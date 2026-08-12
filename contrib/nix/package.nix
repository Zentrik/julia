# Build Julia for Windows entirely inside Nix (no manual `make`):
#
#     nix-build contrib/nix/package.nix                   # 64-bit Windows
#     nix-build contrib/nix/package.nix --argstr arch i686
#
# The result is a Windows Julia installation tree (bin/julia.exe, ...),
# ready to be zipped up and copied to a Windows machine.
#
# How network access is handled: Julia's build system downloads its
# dependencies while building (BinaryBuilder tarballs for LLVM, OpenBLAS,
# etc., plus the external stdlibs such as Pkg), but Nix builds run without
# network access.  All downloads are therefore done in a separate
# fixed-output derivation (`depsCache`, essentially `make -C deps getall` +
# `make -C stdlib getall`) whose output hash is pinned below -- the same
# pattern as Go vendorHash / Rust cargoHash.  When dependency versions
# change (deps/*.version, deps/checksums/*, stdlib versions), the pinned
# hash goes stale; refresh it with:
#
#     nix-build contrib/nix/package.nix -A depsCache --check
#
# or set `depsHash` to `null`, build, and copy the "got:" hash from the
# error message into `depsHashes` below.
#
# Note: this file is deliberately usable with plain nix-build rather than
# through flake outputs.  The flake in this directory lives in a
# subdirectory of the repository, so pure flake evaluation cannot reach the
# Julia sources at ../.. -- classic nix expressions can.

{
  nixpkgs ? fetchTarball "https://github.com/NixOS/nixpkgs/archive/3e3f3c7f9977dc123c23ee21e8085ed63daf8c37.tar.gz",
  system ? builtins.currentSystem,
  arch ? "x86_64", # "x86_64" or "i686"
  # The Julia source tree: tracked (+ locally modified) files only, so stray
  # build artifacts in the work tree don't leak into the build.
  src ? builtins.fetchGit ../../.,
  # Target CPU for the system image.  "native" would tune the sysimage to
  # the *build* machine's CPU, which is wrong for a binary that runs
  # elsewhere.
  cpuTarget ? "generic",
  depsHash ? null, # overrides the pinned depsHashes entry below when non-null
}:

let
  tc = import ./toolchain.nix { inherit nixpkgs system arch; };
  inherit (tc) pkgs;
  inherit (pkgs) lib;

  # Fixed-output hashes of `depsCache` per target arch; refresh as described
  # above whenever dependency versions change.
  depsHashes = {
    x86_64 = lib.fakeHash; # TODO: computed on first build; see header comment
    i686 = lib.fakeHash;
  };

  effectiveDepsHash =
    if depsHash != null then depsHash else depsHashes.${arch} or lib.fakeHash;

  version = lib.removeSuffix "\n" (builtins.readFile "${src}/VERSION");

  # Everything the Makefiles download, pre-fetched so the main build can run
  # offline.  Downloads are verified against deps/checksums/* by the
  # Makefiles themselves, which is what makes this derivation reproducible
  # enough to be fixed-output.
  depsCache = pkgs.stdenv.mkDerivation {
    name = "julia-${version}-win-${arch}-depscache";
    inherit src;

    nativeBuildInputs = [
      tc.crossCC
      tc.crossBintools
    ] ++ tc.nativeTools;

    dontConfigure = true;
    dontFixup = true;

    buildPhase = ''
      runHook preBuild
      # `get`, not `getall`: getall is meant for source dists and fetches
      # every dependency there is, including ones that don't exist for this
      # target (e.g. there is no LibUnwind BinaryBuilder artifact for
      # Windows).  `get` fetches exactly the configured DEP_LIBS.
      make -C deps get -j$NIX_BUILD_CORES XC_HOST=${tc.xcHost} NO_GIT=1
      make -C stdlib getall -j$NIX_BUILD_CORES XC_HOST=${tc.xcHost} NO_GIT=1 DEPS_GIT=0
      runHook postBuild
    '';

    installPhase = ''
      runHook preInstall
      mkdir -p $out
      cp -r deps/srccache $out/deps
      cp -r stdlib/srccache $out/stdlib
      runHook postInstall
    '';

    outputHashMode = "recursive";
    outputHashAlgo = "sha256";
    outputHash = effectiveDepsHash;
  };
in

pkgs.stdenv.mkDerivation {
  pname = "julia-win-${arch}";
  inherit version src;

  nativeBuildInputs = [
    tc.crossCC
    tc.crossBintools
    tc.wine
  ] ++ tc.nativeTools;

  configurePhase = ''
    runHook preConfigure

    # Pre-populate the download caches; the Makefiles skip downloads for
    # files that are already present (and checksum-verified).
    cp -r ${depsCache}/deps deps/srccache
    cp -r ${depsCache}/stdlib stdlib/srccache
    chmod -R u+w deps/srccache stdlib/srccache

    cat > Make.user <<EOF
    XC_HOST = ${tc.xcHost}
    NO_GIT = 1
    WINE = ${tc.wineBin}
    JULIA_CPU_TARGET = ${cpuTarget}
    EOF

    # The system image build runs the freshly cross-compiled julia.exe under
    # wine, which wants a writable prefix.
    export HOME=$TMPDIR
    export WINEPREFIX=$TMPDIR/wine
    export WINEDEBUG=-all

    runHook postConfigure
  '';

  buildPhase = ''
    runHook preBuild
    make -j$NIX_BUILD_CORES
    runHook postBuild
  '';

  installPhase = ''
    runHook preInstall
    make install prefix=$PWD/julia-dist
    mkdir -p $out
    cp -a julia-dist/. $out/
    # same pruning of LLVM tools as `make binary-dist` does for Windows
    rm -f $out/bin/llvm* $out/bin/llc.exe $out/bin/lli.exe $out/bin/opt.exe \
          $out/bin/LTO.dll $out/bin/bugpoint.exe $out/bin/macho-dump.exe
    runHook postInstall
  '';

  # The output is a Windows installation tree: no ELF patching, stripping,
  # or shebang rewriting wanted.
  dontFixup = true;

  passthru = {
    inherit depsCache;
    toolchain = tc;
  };

  meta = {
    description = "Julia for Windows (${tc.xcHost}), cross-compiled from Linux";
    platforms = [ system ];
  };
}
