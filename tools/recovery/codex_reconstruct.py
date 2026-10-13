#!/usr/bin/env python3

import argparse
import collections
import datetime as dt
import hashlib
import json
import os
import re
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path


PATCH_BLOCK = re.compile(r"\*\*\* Begin Patch.*?\*\*\* End Patch", re.DOTALL)
JS_STRING = re.compile(r'"(?:\\.|[^"\\])*"')
MUTATING_COMMAND = re.compile(
    r"(?:\bapply_patch\b|\bsed\s+-i\b|\bperl\s+-[^\n]*i[^\n]*\b|"
    r"\b(?:cp|mv|rm|install|truncate)\b|\btee\b|(?:^|[;&|]\s*)cat\s+[^\n]*(?:>|<<)|"
    r"\bgit\s+(?:apply|checkout|restore|reset|clean)\b|\b(?:cargo\s+fmt|rustfmt)\b|"
    r"\bpython(?:3)?\b[^\n]*(?:write|open\([^\n]*[\"']w))",
    re.MULTILINE,
)


def parse_time(value):
    if not value:
        return None
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))


def iso(value):
    parsed = parse_time(value) if isinstance(value, str) else value
    return parsed.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def inside_repo(value, repo):
    if not isinstance(value, str):
        return False
    try:
        return Path(value).resolve().is_relative_to(repo)
    except (OSError, RuntimeError, ValueError):
        return value == str(repo) or value.startswith(str(repo) + os.sep)


def read_session(path):
    records = []
    with path.open("r", encoding="utf-8", errors="replace") as source:
        for line_number, line in enumerate(source, 1):
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            record["_line"] = line_number
            records.append(record)
    return records


def session_matches(records, repo):
    for record in records:
        payload = record.get("payload")
        if not isinstance(payload, dict):
            continue
        if inside_repo(payload.get("cwd"), repo):
            return True
        for root in payload.get("workspace_roots") or []:
            value = root.get("root") if isinstance(root, dict) else root
            if inside_repo(value, repo):
                return True
    return False


def nested_strings(value):
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for nested in value.values():
            yield from nested_strings(nested)
    elif isinstance(value, list):
        for nested in value:
            yield from nested_strings(nested)


def argument_strings(arguments):
    yielded = set()

    def emit(value):
        if value not in yielded:
            yielded.add(value)
            return value
        return None

    if isinstance(arguments, str):
        value = emit(arguments)
        if value is not None:
            yield value
        try:
            decoded = json.loads(arguments)
        except (json.JSONDecodeError, TypeError):
            decoded = None
        if decoded is not None:
            for text in nested_strings(decoded):
                value = emit(text)
                if value is not None:
                    yield value
        if "*** Begin Patch" in arguments:
            for match in JS_STRING.finditer(arguments):
                literal = match.group(0)
                if "Begin Patch" not in literal:
                    continue
                try:
                    text = json.loads(literal)
                except json.JSONDecodeError:
                    continue
                value = emit(text)
                if value is not None:
                    yield value
    else:
        for text in nested_strings(arguments):
            value = emit(text)
            if value is not None:
                yield value


def patches_from_arguments(arguments):
    found = []
    seen = set()
    for text in argument_strings(arguments):
        for match in PATCH_BLOCK.finditer(text):
            patch = match.group(0)
            if patch not in seen:
                seen.add(patch)
                found.append(patch)
    return found


def output_failed(output):
    if output is None:
        return False
    if isinstance(output, dict):
        if output.get("isError") is True:
            return True
        if isinstance(output.get("exit_code"), int) and output["exit_code"] != 0:
            return True
    text = output if isinstance(output, str) else json.dumps(output, sort_keys=True)
    return bool(re.search(r"(?:Patch failed|apply_patch.*(?:error|failed)|exit_code[^0-9]*[1-9])", text, re.I))


@dataclass
class EditEvent:
    timestamp: dt.datetime
    transcript: str
    line: int
    call_id: str
    tool: str
    patch: str
    output_seen: bool
    likely_success: bool

    def json(self):
        return {
            "timestamp": iso(self.timestamp),
            "transcript": self.transcript,
            "line": self.line,
            "call_id": self.call_id,
            "tool": self.tool,
            "patch": self.patch,
            "output_seen": self.output_seen,
            "likely_success": self.likely_success,
        }


def deduplicate_edits(edits):
    """Collapse history copied into forked agent transcripts."""
    unique = {}
    for event in edits:
        identity = (event.call_id, event.patch) if event.call_id else (iso(event.timestamp), event.patch)
        existing = unique.get(identity)
        if existing is None:
            unique[identity] = event
            continue
        if event.likely_success and not existing.likely_success:
            unique[identity] = event
        elif event.output_seen and not existing.output_seen:
            unique[identity] = event
    return sorted(unique.values(), key=lambda event: (event.timestamp, event.transcript, event.line))


def scan(codex_root, repo):
    sessions = []
    edits = []
    tool_counts = collections.Counter()
    payload_counts = collections.Counter()
    suspect_commands = []
    for path in sorted(codex_root.rglob("*.jsonl")):
        if path.name in {"history.jsonl", "session_index.jsonl"}:
            continue
        records = read_session(path)
        if not records or not session_matches(records, repo):
            continue
        outputs = {}
        for record in records:
            payload = record.get("payload")
            if not isinstance(payload, dict):
                continue
            payload_type = payload.get("type") or "<none>"
            payload_counts[payload_type] += 1
            if payload_type in {"function_call_output", "custom_tool_call_output"}:
                call_id = payload.get("call_id") or payload.get("id")
                outputs[call_id] = payload.get("output", payload.get("content"))
        first = parse_time(records[0].get("timestamp"))
        last = parse_time(records[-1].get("timestamp"))
        nickname = ""
        calls = 0
        patch_count = 0
        for record in records:
            payload = record.get("payload")
            if not isinstance(payload, dict):
                continue
            nickname = nickname or payload.get("agent_nickname") or ""
            if payload.get("type") not in {"function_call", "custom_tool_call"}:
                continue
            calls += 1
            tool = payload.get("name") or "<unknown>"
            tool_counts[tool] += 1
            call_id = payload.get("call_id") or payload.get("id") or ""
            arguments = payload.get("arguments", payload.get("input", ""))
            patches = patches_from_arguments(arguments)
            patch_count += len(patches)
            timestamp = parse_time(record.get("timestamp"))
            output = outputs.get(call_id)
            for patch in patches:
                edits.append(
                    EditEvent(
                        timestamp=timestamp,
                        transcript=str(path),
                        line=record["_line"],
                        call_id=call_id,
                        tool=tool,
                        patch=patch,
                        output_seen=call_id in outputs,
                        likely_success=call_id in outputs and not output_failed(output),
                    )
                )
            for text in argument_strings(arguments):
                if MUTATING_COMMAND.search(text) and not patches:
                    suspect_commands.append(
                        {
                            "timestamp": iso(timestamp),
                            "transcript": str(path),
                            "line": record["_line"],
                            "call_id": call_id,
                            "tool": tool,
                            "command_sha256": hashlib.sha256(text.encode()).hexdigest(),
                        }
                    )
                    break
        sessions.append(
            {
                "path": str(path),
                "first": iso(first),
                "last": iso(last),
                "nickname": nickname,
                "calls": calls,
                "patches": patch_count,
            }
        )
    edits = deduplicate_edits(edits)
    suspect_commands = list(
        {
            (event["call_id"], event["command_sha256"]): event
            for event in suspect_commands
        }.values()
    )
    suspect_commands.sort(key=lambda event: (event["timestamp"], event["transcript"], event["line"]))
    return sessions, edits, tool_counts, payload_counts, suspect_commands


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as destination:
        json.dump(value, destination, indent=2, sort_keys=True)
        destination.write("\n")


def write_jsonl(path, values):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as destination:
        for value in values:
            json.dump(value, destination, sort_keys=True)
            destination.write("\n")


def message_text(payload):
    content = payload.get("content")
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return ""
    parts = []
    for item in content:
        if not isinstance(item, dict):
            continue
        if item.get("type") in {"input_text", "output_text", "text"}:
            parts.append(item.get("text", ""))
    return "\n".join(parts)


def find_marker_messages(codex_root, repo, expressions):
    patterns = [re.compile(expression, re.I | re.DOTALL) for expression in expressions]
    matches = {}
    for path in sorted(codex_root.rglob("*.jsonl")):
        if path.name in {"history.jsonl", "session_index.jsonl"}:
            continue
        records = read_session(path)
        if not records or not session_matches(records, repo):
            continue
        nickname = ""
        for record in records:
            payload = record.get("payload")
            if isinstance(payload, dict):
                nickname = nickname or payload.get("agent_nickname") or ""
        for record in records:
            payload = record.get("payload")
            if not isinstance(payload, dict):
                continue
            if payload.get("type") != "message" or payload.get("role") != "user":
                continue
            text = message_text(payload)
            matched = [expression for expression, pattern in zip(expressions, patterns) if pattern.search(text)]
            if not matched:
                continue
            key = payload.get("id") or (record.get("timestamp"), text)
            entry = matches.setdefault(
                str(key),
                {
                    "timestamp": record.get("timestamp"),
                    "message_id": payload.get("id", ""),
                    "text": text,
                    "matched": set(),
                    "transcripts": [],
                    "agents": set(),
                },
            )
            entry["matched"].update(matched)
            entry["transcripts"].append(str(path))
            if nickname:
                entry["agents"].add(nickname)
    result = []
    for entry in matches.values():
        entry["matched"] = sorted(entry["matched"])
        entry["transcripts"] = sorted(set(entry["transcripts"]))
        entry["agents"] = sorted(entry["agents"])
        result.append(entry)
    result.sort(key=lambda entry: (entry["timestamp"] or "", entry["message_id"]))
    return result


def parse_patch(patch):
    lines = patch.splitlines()
    sections = []
    current = None
    for line in lines[1:-1]:
        marker = re.match(r"\*\*\* (Update|Add|Delete) File: (.+)", line)
        if marker:
            if current:
                sections.append(current)
            current = {"operation": marker.group(1).lower(), "path": marker.group(2), "lines": []}
            continue
        move = re.match(r"\*\*\* Move to: (.+)", line)
        if move and current:
            current["move_to"] = move.group(1)
            continue
        if current is not None:
            current["lines"].append(line)
    if current:
        sections.append(current)
    return sections


def split_hunks(lines):
    hunks = []
    current = []
    for line in lines:
        if line.startswith("@@"):
            if current:
                hunks.append(current)
            current = []
        else:
            current.append(line)
    if current:
        hunks.append(current)
    return hunks


def hunk_sides(hunk):
    before = []
    after = []
    for line in hunk:
        if line.startswith("+"):
            after.append(line[1:])
        elif line.startswith("-"):
            before.append(line[1:])
        elif line.startswith(" "):
            before.append(line[1:])
            after.append(line[1:])
        elif line == "":
            before.append("")
            after.append("")
        elif line.startswith("\\ No newline"):
            continue
        else:
            before.append(line)
            after.append(line)
    return before, after


def replace_unique(lines, needle, replacement):
    if not needle:
        return None, "empty reverse match"
    hits = []
    width = len(needle)
    for index in range(0, len(lines) - width + 1):
        if lines[index : index + width] == needle:
            hits.append(index)
    if len(hits) != 1:
        return None, f"reverse match count {len(hits)}"
    index = hits[0]
    return lines[:index] + replacement + lines[index + width :], None


def safe_path(root, patch_path):
    candidate = Path(patch_path)
    if candidate.is_absolute():
        try:
            relative = candidate.resolve().relative_to(root.resolve())
        except ValueError:
            return None
    else:
        relative = candidate
    if ".." in relative.parts:
        return None
    return root / relative


def reverse_section(root, section, dry_run):
    path = safe_path(root, section["path"])
    if path is None:
        return False, "path outside repository"
    operation = section["operation"]
    if section.get("move_to"):
        moved = safe_path(root, section["move_to"])
        if moved is None or not moved.exists():
            return False, "moved path missing"
        if not dry_run:
            path.parent.mkdir(parents=True, exist_ok=True)
            moved.rename(path)
    if operation == "add":
        if not path.exists():
            return True, "already absent"
        if not dry_run:
            if path.is_dir():
                shutil.rmtree(path)
            else:
                path.unlink()
        return True, "removed post-cutoff file"
    if operation == "delete":
        content = [line[1:] for line in section["lines"] if line.startswith("-")]
        if not content:
            return False, "deleted content absent from transcript"
        if not dry_run:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("\n".join(content) + "\n", encoding="utf-8")
        return True, "restored deleted file"
    if not path.exists():
        return False, "updated path missing"
    original = path.read_text(encoding="utf-8")
    trailing_newline = original.endswith("\n")
    lines = original.splitlines()
    for hunk in reversed(split_hunks(section["lines"])):
        before, after = hunk_sides(hunk)
        lines, error = replace_unique(lines, after, before)
        if error:
            return False, error
    if not dry_run:
        text = "\n".join(lines)
        if trailing_newline:
            text += "\n"
        path.write_text(text, encoding="utf-8")
    return True, "reversed update"


def copy_repository(repo, destination):
    if destination.exists():
        raise RuntimeError(f"destination already exists: {destination}")
    ignored = {".git", ".recovery", "target", ".cluster-run", ".cluster-all-local", "result", "result-dev"}

    def ignore(directory, names):
        if Path(directory).resolve() == repo.resolve():
            return [name for name in names if name in ignored]
        return []

    shutil.copytree(repo, destination, symlinks=True, ignore=ignore)


def copy_checkpoint(checkpoint, destination):
    if destination.exists():
        raise RuntimeError(f"destination already exists: {destination}")
    shutil.copytree(checkpoint, destination, symlinks=True)
    for path in destination.rglob("*"):
        if not path.is_symlink():
            path.chmod(path.stat().st_mode | 0o200)


def overlay_checkpoint(checkpoint, destination):
    for source in checkpoint.rglob("*"):
        relative = source.relative_to(checkpoint)
        target = destination / relative
        if source.is_dir():
            target.mkdir(parents=True, exist_ok=True)
        elif source.is_symlink():
            target.parent.mkdir(parents=True, exist_ok=True)
            if target.exists() or target.is_symlink():
                target.unlink()
            target.symlink_to(os.readlink(source))
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, target)
            target.chmod(target.stat().st_mode | 0o200)


def replace_entry(source, target):
    if target.is_symlink() or target.is_file():
        target.unlink()
    elif target.is_dir():
        shutil.rmtree(target)
    if source.is_symlink():
        target.symlink_to(os.readlink(source))
    elif source.is_dir():
        shutil.copytree(source, target, symlinks=True)
    else:
        shutil.copy2(source, target)


def compose_snapshot(args):
    shell = Path(args.shell).resolve()
    source_snapshot = Path(args.source_snapshot).resolve()
    destination = Path(args.destination).resolve()
    copy_checkpoint(shell, destination)
    replaced = []
    for source in sorted(source_snapshot.iterdir(), key=lambda path: path.name):
        replace_entry(source, destination / source.name)
        replaced.append(source.name)
    for path in destination.rglob("*"):
        if not path.is_symlink():
            path.chmod(path.stat().st_mode | 0o200)
    report = {
        "destination": str(destination),
        "shell": str(shell),
        "source_snapshot": str(source_snapshot),
        "authoritative_top_level_entries": replaced,
    }
    write_json(Path(args.report), report)
    print(f"destination={destination}")
    print(f"authoritative_top_level_entries={len(replaced)}")
    return 0


def replace_if_present(source, target, replaced):
    if source.exists() or source.is_symlink():
        replace_entry(source, target)
        replaced.append(str(target))


def compose_nix_filtered_snapshot(args):
    """Restore the paths retained by flake.nix's per-crate cleanSource filter."""
    shell = Path(args.shell).resolve()
    source_snapshot = Path(args.source_snapshot).resolve()
    destination = Path(args.destination).resolve()
    copy_checkpoint(shell, destination)
    replaced = []

    for name in ("Cargo.toml", "Cargo.lock", "assets"):
        replace_if_present(source_snapshot / name, destination / name, replaced)
    replace_if_present(
        source_snapshot / "crates/pumpkin-plugin-wit",
        destination / "crates/pumpkin-plugin-wit",
        replaced,
    )

    for group in ("crates", "tools"):
        source_group = source_snapshot / group
        if not source_group.is_dir():
            continue
        for source_package in sorted(source_group.iterdir(), key=lambda path: path.name):
            if not source_package.is_dir():
                continue
            target_package = destination / group / source_package.name
            replace_if_present(
                source_package / "Cargo.toml", target_package / "Cargo.toml", replaced
            )
            source_src = source_package / "src"
            if source_src.is_dir() and any(path.is_file() for path in source_src.rglob("*")):
                replace_entry(source_src, target_package / "src")
                replaced.append(str(target_package / "src"))
                for name in ("build.rs", "benches"):
                    source_entry = source_package / name
                    target_entry = target_package / name
                    if source_entry.exists():
                        replace_entry(source_entry, target_entry)
                        replaced.append(str(target_entry))
                    elif target_entry.exists() or target_entry.is_symlink():
                        if target_entry.is_dir() and not target_entry.is_symlink():
                            shutil.rmtree(target_entry)
                        else:
                            target_entry.unlink()

    for path in destination.rglob("*"):
        if not path.is_symlink():
            path.chmod(path.stat().st_mode | 0o200)
    report = {
        "destination": str(destination),
        "shell": str(shell),
        "nix_filtered_source": str(source_snapshot),
        "replaced_paths": replaced,
    }
    write_json(Path(args.report), report)
    print(f"destination={destination}")
    print(f"replaced_paths={len(replaced)}")
    return 0


def reconstruct(args, edits):
    repo = Path(args.repo).resolve()
    destination = Path(args.destination).resolve()
    cutoff = parse_time(args.cutoff)
    checkpoint_time = parse_time(args.checkpoint_time) if args.checkpoint_time else None
    checkpoint = Path(args.checkpoint).resolve() if args.checkpoint else None
    checkpoint_files = set()
    if checkpoint:
        copy_checkpoint(checkpoint, destination)
        checkpoint_files = {
            str(path.relative_to(checkpoint)) for path in checkpoint.rglob("*") if not path.is_dir()
        }
    else:
        copy_repository(repo, destination)
    report = []
    candidates = [
        event
        for event in edits
        if event.likely_success
        and event.timestamp > cutoff
        and (checkpoint_time is None or event.timestamp <= checkpoint_time)
    ]
    for event in reversed(candidates):
        for section in reversed(parse_patch(event.patch)):
            relative = section["path"]
            if Path(relative).is_absolute():
                try:
                    relative = str(Path(relative).resolve().relative_to(repo))
                except ValueError:
                    relative = section["path"]
            normalized_section = dict(section)
            normalized_section["path"] = relative
            if section.get("move_to"):
                moved = Path(section["move_to"])
                if moved.is_absolute():
                    try:
                        normalized_section["move_to"] = str(moved.resolve().relative_to(repo))
                    except ValueError:
                        normalized_section["move_to"] = section["move_to"]
            ok, message = reverse_section(destination, normalized_section, args.dry_run)
            report.append(
                {
                    "timestamp": iso(event.timestamp),
                    "transcript": event.transcript,
                    "line": event.line,
                    "path": relative,
                    "operation": section["operation"],
                    "ok": ok,
                    "message": message,
                }
            )
    write_json(Path(args.report), report)
    failures = sum(not entry["ok"] for entry in report)
    print(f"destination={destination}")
    print(f"reverse_sections={len(report)}")
    print(f"failures={failures}")
    return 1 if failures else 0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--codex-root", default=str(Path.home() / ".codex"))
    parser.add_argument("--repo", required=True)
    sub = parser.add_subparsers(dest="command", required=True)
    inventory = sub.add_parser("inventory")
    inventory.add_argument("--output-dir", required=True)
    markers = sub.add_parser("markers")
    markers.add_argument("--expression", action="append", required=True)
    markers.add_argument("--output", required=True)
    replay = sub.add_parser("reconstruct")
    replay.add_argument("--cutoff", required=True)
    replay.add_argument("--destination", required=True)
    replay.add_argument("--checkpoint")
    replay.add_argument("--checkpoint-time")
    replay.add_argument("--report", required=True)
    replay.add_argument("--dry-run", action="store_true")
    compose = sub.add_parser("compose")
    compose.add_argument("--shell", required=True)
    compose.add_argument("--source-snapshot", required=True)
    compose.add_argument("--destination", required=True)
    compose.add_argument("--report", required=True)
    compose_filtered = sub.add_parser("compose-nix-filtered")
    compose_filtered.add_argument("--shell", required=True)
    compose_filtered.add_argument("--source-snapshot", required=True)
    compose_filtered.add_argument("--destination", required=True)
    compose_filtered.add_argument("--report", required=True)
    args = parser.parse_args()
    repo = Path(args.repo).resolve()
    sessions, edits, tools, payloads, suspects = scan(Path(args.codex_root), repo)
    if args.command == "inventory":
        output = Path(args.output_dir)
        write_json(output / "sessions.json", sessions)
        write_jsonl(output / "patches.jsonl", [event.json() for event in edits])
        write_json(output / "tool-counts.json", tools)
        write_json(output / "payload-counts.json", payloads)
        write_json(output / "unparsed-mutating-calls.json", suspects)
        print(f"sessions={len(sessions)}")
        print(f"patches={len(edits)}")
        print(f"successful_patches={sum(event.likely_success for event in edits)}")
        print(f"unparsed_mutating_calls={len(suspects)}")
        return 0
    if args.command == "markers":
        matches = find_marker_messages(Path(args.codex_root), repo, args.expression)
        write_json(Path(args.output), matches)
        print(f"matches={len(matches)}")
        for match in matches:
            compact = " ".join(match["text"].split())
            print(f'{match["timestamp"]}\t{match["message_id"]}\t{compact[:240]}')
        return 0
    if args.command == "compose":
        return compose_snapshot(args)
    if args.command == "compose-nix-filtered":
        return compose_nix_filtered_snapshot(args)
    return reconstruct(args, edits)


if __name__ == "__main__":
    sys.exit(main())
