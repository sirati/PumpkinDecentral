{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-unstable";

    flake-parts.url = "github:hercules-ci/flake-parts";

    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    flake-compat = {
      url = "github:NixOS/flake-compat";
      flake = false;
    };

    self.submodules = true;
  };

  outputs =
    inputs@{
      flake-parts,
      nixpkgs,
      ...
    }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = nixpkgs.lib.systems.flakeExposed;

      perSystem =
        {
          lib,
          pkgs,
          ...
        }:
        let
          workspaceManifest = (lib.importTOML ./Cargo.toml);
          workspaceVersion = workspaceManifest.workspace.package.version;

          allCrates = [
            "pumpkin"
            "pumpkin-api-macros"
            "pumpkin-auth"
            "pumpkin-codecs"
            "pumpkin-cluster"
            "pumpkin-command"
            "pumpkin-config"
            "pumpkin-data"
            "pumpkin-gametest"
            "pumpkin-inventory"
            "pumpkin-macros"
            "pumpkin-nbt"
            "pumpkin-plugin-api"
            "pumpkin-plugin-runtime"
            "pumpkin-plugin-utils"
            "pumpkin-protocol"
            "pumpkin-util"
            "pumpkin-host-bindings"
            "pumpkin-world"
            "pumpkin-codegen"
            "pumpkin-fuzzer"
          ];

          crateDir = name:
            if name == "pumpkin-codegen" then "tools/pumpkin-codegen"
            else if name == "pumpkin-fuzzer" then "tools/pumpkin-fuzzer"
            else "crates/${name}";

          binOnly = [
            "pumpkin-codegen"
            "pumpkin-fuzzer"
          ];

          bothBinAndLib = [
            "pumpkin"
          ];

          directDeps = crateName:
            let
              m = lib.importTOML (./. + "/${crateDir crateName}/Cargo.toml");
              deps = (m.dependencies or { }) // (m.build-dependencies or { });
              wsDeps = workspaceManifest.workspace.dependencies;
              toEdge = depName: depSpec:
                if builtins.isAttrs depSpec && (depSpec.workspace or false) == true then
                  if builtins.hasAttr depName wsDeps && builtins.isAttrs wsDeps.${depName} && builtins.hasAttr "path" wsDeps.${depName} then
                    depName
                  else
                    null
                else if builtins.isAttrs depSpec && builtins.hasAttr "path" depSpec then
                  let base = builtins.baseNameOf depSpec.path;
                  in if builtins.elem base allCrates then base else null
                else
                  null;
            in
            lib.filter (x: x != null) (lib.mapAttrsToList toEdge deps);

          benchNamesFor = crateName:
            let
              m = lib.importTOML (./. + "/${crateDir crateName}/Cargo.toml");
              benches = m.bench or [ ];
            in
            map (b: b.name) benches;

          closureFor = root:
            let
              go = visited: queue:
                if queue == [ ] then
                  visited
                else
                  let
                    cur = builtins.head queue;
                    rest = builtins.tail queue;
                    already = builtins.elem cur visited;
                    deps = if already then [ ] else directDeps cur;
                    fresh = lib.filter (d: !(builtins.elem d visited) && !(builtins.elem d rest)) deps;
                  in
                  if already then go visited rest else go (visited ++ [ cur ]) (rest ++ fresh);
            in
            go [ ] [ root ];

          crateSrc = crateName:
            let
              closure = closureFor crateName;
              filtered = lib.cleanSourceWith {
                src = ./.;
                filter = path: type:
                  let
                    rel = lib.removePrefix (toString ./. + "/") (toString path);
                    isDir = type == "directory";
                  in
                  if rel == "" then
                    true
                  else if rel == "Cargo.toml" || rel == "Cargo.lock" then
                    true
                  else if lib.hasPrefix "assets/" rel then
                    true
                  else if lib.hasPrefix "crates/pumpkin-plugin-wit/" rel then
                    true
                  else if builtins.match "crates/[^/]+/Cargo.toml" rel != null then
                    true
                  else if builtins.match "tools/[^/]+/Cargo.toml" rel != null then
                    true
                  else if lib.hasPrefix "crates/" rel || lib.hasPrefix "tools/" rel then
                    let
                      parts = lib.splitString "/" rel;
                      dirName = builtins.elemAt parts 1;
                      inClosure = builtins.elem dirName closure;
                      isSrc = builtins.match "crates/[^/]+/src(/.*)?" rel != null || builtins.match "tools/[^/]+/src(/.*)?" rel != null;
                      isBuildRs = builtins.match "crates/[^/]+/build.rs" rel != null || builtins.match "tools/[^/]+/build.rs" rel != null;
                      isBench = builtins.match "crates/[^/]+/benches(/.*)?" rel != null || builtins.match "tools/[^/]+/benches(/.*)?" rel != null;
                    in
                    if isDir then
                      true
                    else if isSrc then
                      inClosure
                    else if isBuildRs then
                      inClosure
                    else if isBench then
                      inClosure
                    else
                      false
                  else if isDir then
                    true
                  else
                    false;
              };
              outside = lib.filter (c: !(builtins.elem c closure)) allCrates;
              stubCmds = lib.concatMapStrings (c:
                let
                  dir = crateDir c;
                  benches = benchNamesFor c;
                  benchStubs = lib.concatMapStrings (b: ''
                    mkdir -p $out/${dir}/benches
                    if [ ! -e $out/${dir}/benches/${b}.rs ]; then : > $out/${dir}/benches/${b}.rs; fi
                  '') benches;
                in if builtins.elem c binOnly then
                  ''
                    mkdir -p $out/${dir}/src
                    if [ ! -e $out/${dir}/src/main.rs ]; then printf 'fn main(){}\n' > $out/${dir}/src/main.rs; fi
                  '' + benchStubs
                else if builtins.elem c bothBinAndLib then
                  ''
                    mkdir -p $out/${dir}/src
                    if [ ! -e $out/${dir}/src/lib.rs ]; then : > $out/${dir}/src/lib.rs; fi
                    if [ ! -e $out/${dir}/src/main.rs ]; then printf 'fn main(){}\n' > $out/${dir}/src/main.rs; fi
                  '' + benchStubs
                else
                  ''
                    mkdir -p $out/${dir}/src
                    if [ ! -e $out/${dir}/src/lib.rs ]; then : > $out/${dir}/src/lib.rs; fi
                  '' + benchStubs) outside;
            in
            pkgs.runCommand "pumpkin-src-${crateName}" { } ''
              cp -r ${filtered} $out
              chmod -R u+w $out
              ${stubCmds}
            '';

          buildCrateFull = crateName: buildType:
            pkgs.rustPlatform.buildRustPackage {
              pname = crateName;
              version = workspaceVersion;

              src = crateSrc crateName;

              cargoLock = {
                lockFile = ./Cargo.lock;
                outputHashes = {
                  "cranelift-assembler-x64-0.136.0-dev" =
                    "sha256-TZkmQ4+wWzb9x8UukZYQs1j05llI8ZmuMyHFXaDwcL0=";
                };
              };

              nativeBuildInputs = [
                pkgs.rustfmt
                pkgs.pkg-config
              ];

              cargoBuildFlags = [
                "--package"
                crateName
              ];

              buildType = buildType;

              CARGO_PROFILE_RELEASE_LTO = "thin";
              CARGO_PROFILE_RELEASE_CODEGEN_UNITS = "16";

              doCheck = false;
            };

          buildCrate = crateName: buildCrateFull crateName "release";
          buildClusterE2e = (buildCrateFull "pumpkin" "debug").overrideAttrs (old: {
            cargoBuildFlags = old.cargoBuildFlags ++ [ "--features=cluster-e2e" ];
          });
          nightlyToolchain = (pkgs.extend inputs.rust-overlay.overlays.default).rust-bin.nightly."2026-09-15".minimal;
          nightlyRustPlatform = pkgs.makeRustPlatform {
            cargo = nightlyToolchain;
            rustc = nightlyToolchain;
          };
          buildAzaleaE2e = nightlyRustPlatform.buildRustPackage {
            pname = "pumpkin-azalea-e2e";
            version = workspaceVersion;
            src = ./tests/azalea-e2e;
            cargoLock = {
              lockFile = ./tests/azalea-e2e/Cargo.lock;
              outputHashes = {
                "azalea-0.16.0+mc26.2" = "sha256-Cy+g4TmUhK0zElUizRZ5Pb5Qvn7SmuJObBZHvsMlQ6w=";
              };
            };
            doCheck = false;
          };
          clusterE2eFeatureGate = pkgs.runCommand "pumpkin-cluster-e2e-feature-gate" {
            nativeBuildInputs = [ pkgs.binutils pkgs.gnugrep ];
          } ''
            if strings ${buildCrate "pumpkin"}/bin/pumpkin | grep -Fq clustertest; then
              echo "normal Pumpkin binary contains the feature-only clustertest command" >&2
              exit 1
            fi
            if ! strings ${buildClusterE2e}/bin/pumpkin | grep -Fq clustertest; then
              echo "cluster-e2e Pumpkin binary does not contain clustertest" >&2
              exit 1
            fi
            touch $out
          '';
        in
        {
          packages = {
            default = buildCrate "pumpkin";
            pumpkin = buildCrate "pumpkin";
            pumpkin-dev = buildCrateFull "pumpkin" "debug";
            pumpkin-cluster-e2e = buildClusterE2e;
            pumpkin-azalea-e2e = buildAzaleaE2e;
          } // lib.genAttrs allCrates (c: buildCrate c);

          checks.pumpkin-cluster-e2e-feature-gate = clusterE2eFeatureGate;

          devShells.default = pkgs.mkShell {
            packages = with pkgs; [
              cargo
              clippy
              rust-analyzer
              rustc
              rustfmt
              pkg-config
            ];
          };

          devShells.azalea = pkgs.mkShell {
            packages = [ nightlyToolchain pkgs.pkg-config ];
          };

          formatter = pkgs.nixfmt-tree;
        };
    };
}
