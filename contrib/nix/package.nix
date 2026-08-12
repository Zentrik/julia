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
  # true: download prebuilt dependency binaries (LLVM, OpenBLAS, ...) from
  # BinaryBuilder, matching the composition of the official Julia Windows
  # binaries; the cross toolchain then only compiles Julia itself.
  # false: build all dependencies from source (as nixpkgs does for its
  # native Julia packages); needs the cross gfortran and takes several
  # extra hours.
  useBinaryBuilder ? true,
  depsHash ? null, # overrides the pinned depsHashes entry below when non-null
}:

let
  tc = import ./toolchain.nix { inherit nixpkgs system arch; };
  inherit (tc) pkgs;
  inherit (pkgs) lib;

  # In from-source mode, p7zip is the one exception kept from
  # BinaryBuilder: Julia bundles the standalone 7z.exe as a utility (it is
  # never linked against Julia), and the p7zip codebase is a Unix-only port
  # that cannot be cross-compiled for Windows (mingw predefines _WIN32,
  # which sends it down 7-zip's Windows code paths without the Windows
  # build scaffolding).
  bbFlags = [
    "USE_BINARYBUILDER=${if useBinaryBuilder then "1" else "0"}"
  ] ++ lib.optional (!useBinaryBuilder) "USE_BINARYBUILDER_P7ZIP=1";
  bbFlag = toString bbFlags; # make command-line form
  bbMakeUser = lib.concatStringsSep "\n" bbFlags; # Make.user form (one per line)

  # Fixed-output hashes of `depsCache` per target arch and dependency mode;
  # refresh as described above whenever dependency versions change.
  depsHashes = {
    x86_64-bb = "sha256-UuxX/T2pwDmc1k5J2EKEu/Y/J+1Iht1j8+pZr9GUeWI=";
    # Not computed yet: build with `--arg depsHash null` and copy the hash
    # from the mismatch error here (see header comment).
    x86_64-src = "sha256-cH22zWQD1lSxgdEwfEMXJZ3Zl3pEJwtmYCCzA4nrKQk=";
    i686-bb = "sha256-CiBJR7d3dKTk9SBUjOFQ/kzkqgF07QfXdSsFu2UyzUI=";
    i686-src = lib.fakeHash;
  };

  depsMode = "${arch}-${if useBinaryBuilder then "bb" else "src"}";

  effectiveDepsHash =
    if depsHash != null then depsHash else depsHashes.${depsMode} or lib.fakeHash;

  version = lib.removeSuffix "\n" (builtins.readFile "${src}/VERSION");

  # Everything the Makefiles download, pre-fetched so the main build can run
  # offline.  Downloads are verified against deps/checksums/* by the
  # Makefiles themselves, which is what makes this derivation reproducible
  # enough to be fixed-output.
  depsCache = pkgs.stdenv.mkDerivation {
    name = "julia-${version}-win-${depsMode}-depscache";
    inherit src;

    nativeBuildInputs = [
      tc.crossCC
      tc.crossBintools
    ] ++ lib.optional (!useBinaryBuilder) tc.crossFortran ++ tc.nativeTools;

    dontConfigure = true;
    dontFixup = true;

    buildPhase = ''
      runHook preBuild
      # `get`, not `getall`: getall is meant for source dists and fetches
      # every dependency there is, including ones that don't exist for this
      # target (e.g. there is no LibUnwind BinaryBuilder artifact for
      # Windows).  `get` fetches exactly the configured DEP_LIBS.
      make -C deps get -j$NIX_BUILD_CORES XC_HOST=${tc.xcHost} NO_GIT=1 ${bbFlag}
      make -C stdlib getall -j$NIX_BUILD_CORES XC_HOST=${tc.xcHost} NO_GIT=1 DEPS_GIT=0 ${bbFlag}
      runHook postBuild
    '';

    installPhase = ''
      runHook preInstall
      # Mirror the source-tree layout: the download caches, plus the
      # StdlibArtifacts.toml files that `stdlib getall` fetches *into the
      # source tree* (stdlib/<name>_jll/) rather than into a cache.
      mkdir -p $out/deps $out/stdlib
      cp -r deps/srccache $out/deps/srccache
      cp -r stdlib/srccache $out/stdlib/srccache
      find stdlib -maxdepth 2 -name StdlibArtifacts.toml -exec cp --parents {} $out \;
      runHook postInstall
    '';

    outputHashMode = "recursive";
    outputHashAlgo = "sha256";
    outputHash = effectiveDepsHash;
  };
in

pkgs.stdenv.mkDerivation {
  pname = "julia-win-${arch}${lib.optionalString (!useBinaryBuilder) "-from-source"}";
  inherit version src;

  nativeBuildInputs = [
    tc.crossCC
    tc.crossBintools
    tc.wine
  ] ++ lib.optional (!useBinaryBuilder) tc.crossFortran ++ tc.nativeTools;

  configurePhase = ''
    runHook preConfigure

    # Pre-populate the download caches and pre-fetched StdlibArtifacts.toml
    # files; the Makefiles skip downloads for files that are already present
    # (and checksum-verified).
    cp -r --no-preserve=mode,ownership ${depsCache}/deps/. deps/
    cp -r --no-preserve=mode,ownership ${depsCache}/stdlib/. stdlib/

    cat > Make.user <<EOF
    XC_HOST = ${tc.xcHost}
    NO_GIT = 1
    WINE = ${tc.wineBin}
    JULIA_CPU_TARGET = ${cpuTarget}
    ${bbMakeUser}
    # Visible -L for winpthreads: libtool decides whether -lpthread can be
    # satisfied by a shared library only from -L paths it can see, and the
    # equivalent flag baked into the cc wrapper is invisible to it (without
    # this, libtool-based deps like mpfr silently fall back to static-only
    # builds and the build later fails looking for their DLLs).
    LDFLAGS += -L${tc.winpthreads}/lib
    EOF

    # Several build steps run freshly cross-compiled executables (flisp.exe,
    # julia.exe for the system image) under wine, which wants a writable
    # prefix and -- since wine 9 -- an XDG_RUNTIME_DIR for the wineserver
    # socket; neither exists in the isolated build environment.
    export HOME=$TMPDIR
    export WINEPREFIX=$TMPDIR/wine
    export WINEDEBUG=-all
    export XDG_RUNTIME_DIR=$TMPDIR/xdg-runtime
    mkdir -p -m 700 $XDG_RUNTIME_DIR

    runHook postConfigure
  '';

  buildPhase = ''
    runHook preBuild
    # Build everything with full parallelism except the stdlib package-image
    # precompiles: each of those runs julia.exe under wine, and concurrent
    # wine process storms fail flakily (process spawn errors with no
    # diagnostics), so finish that last stage serially.
    make -j$NIX_BUILD_CORES julia-release
    make -j1
    runHook postBuild
  '';

  installPhase = ''
    runHook preInstall
    # JULIA_INSTALL_DOCS=0: building the HTML docs needs network access (it
    # fetches Documenter & friends through Pkg at build time), which isn't
    # available inside the sandbox; read the manual at docs.julialang.org.
    make install prefix=$PWD/julia-dist JULIA_INSTALL_DOCS=0
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

  # -fstack-clash-protection makes GCC 13 ICE on mingw when emitting SEH
  # unwind info for large stack frames (gcc PR90458, fixed in GCC 14; hit by
  # cli/loader_win_utils.c).  Everything else in nixpkgs' default hardening
  # set is fine with the mingw toolchain.
  hardeningDisable = [ "stackclashprotection" ];

  passthru = {
    inherit depsCache;
    toolchain = tc;
  };

  meta = {
    description = "Julia for Windows (${tc.xcHost}), cross-compiled from Linux";
    platforms = [ system ];
  };
}
