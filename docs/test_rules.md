# Test Rules

Tests must prove behavior, integration, a nontrivial invariant, or a failure mode. A test is forbidden when it merely repeats constants and construction performed directly by the implementation.

For example, this test is forbidden:

```rust
#[test]
fn progress_shows_peer_then_client_percent() {
    let subtitle = lobby_progress_subtitle(75, 25);
    assert_eq!(subtitle.to_pretty_console(), "75% 25%");
    assert_eq!(subtitle.0.style.color, Some(Color::Named(NamedColor::DarkGray)));
    assert_eq!(subtitle.0.extra[0].style.color, Some(Color::Named(NamedColor::White)));
}
```

It calls a formatter with literal values and checks that the same literal values and colors assigned by the formatter are present. It would keep passing if lobby progress were never measured, the subtitle were never sent, packet serialization discarded the child style, or the client displayed the values in the wrong place.

Useful coverage for this behavior must cross a real boundary. A packet-level test may decode the title packet emitted by lobby progression and verify the two independently calculated counters and serialized styles. A live-client test may advance peer acquisition and client acknowledgements separately and verify that the observed subtitle changes while handoff occurs only after client receipt reaches completion.

Apply the same rule throughout the repository:

- Do not test getters by setting a field and reading it back.
- Do not test constructors by comparing their fields with their arguments.
- Do not test an enum mapping by duplicating the same match table in the assertion.
- Do not test a private formatting helper when the requirement concerns a serialized packet or client-visible result.
- Do test boundaries, competing states, ordering, malformed input, convergence, rollback, routing, persistence, and externally observable behavior.
- Prefer one meaningful integration or property test over several implementation-mirroring unit tests.
