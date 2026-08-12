# The mingw-w64 cross toolchain (POSIX thread model) and the native tools
# needed to build Julia for Windows.  Shared by windows.nix (dev shell) and
# package.nix (full build); see ./README.md.
#
# Why the overlay exists: since NixOS 24.05 nixpkgs builds its mingw-w64 GCC
# with the "mcf" thread model (mcfgthread).  Julia requires the "posix"
# thread model (winpthreads): src/Makefile links with -lpthread, and libuv,
# libjulia and flisp all use the pthread API directly, so an mcf toolchain
# has no pthread.h/libpthread and fails.  The overlay rebuilds the cross
# toolchain with --enable-threads=posix against winpthreads, which is the
# same thing the `*-posix` mingw-w64 compilers on Debian/Ubuntu are.

{
  nixpkgs, # path to a nixpkgs checkout (flake input outPath or fetchTarball)
  system ? builtins.currentSystem,
  arch ? "x86_64", # "x86_64" or "i686"
}:

let
  posixThreadsOverlay = final: prev: {
    # nixpkgs <= 25.05: GCC's thread model comes from the top-level
    # `threadsCross` attribute (keyed on the *target* platform).
    threadsCross =
      prev.lib.optionalAttrs
        (prev.stdenv.targetPlatform.isMinGW && !(prev.stdenv.targetPlatform.useLLVM or false))
        {
          model = "posix";
          # winpthreads.  Called `windows.mingw_w64_pthreads` up to NixOS
          # 25.05 and renamed to `windows.pthreads` on later nixpkgs (where
          # the old name remains as an alias).  Beware that on 25.05
          # `windows.pthreads` is pthreads-win32, a different library, so the
          # order of these fallbacks matters.
          package =
            final.targetPackages.windows.mingw_w64_pthreads or final.windows.mingw_w64_pthreads
              or final.targetPackages.windows.pthreads or final.windows.pthreads;
        };

    # nixpkgs > 25.05 (master as of late 2025): `threadsCross` was replaced
    # by a top-level `threads` attribute keyed on the *host* platform (GCC
    # consumes it as `targetPackages.threads or pkgs.threads`).  Overriding
    # only the old attribute would silently keep the mcf model there, so set
    # both; each nixpkgs version simply ignores the attribute it doesn't
    # consume.
    threads =
      prev.lib.optionalAttrs
        (prev.stdenv.hostPlatform.isMinGW && !(prev.stdenv.hostPlatform.useLLVM or false))
        {
          model = "posix";
          # On nixpkgs new enough to consume `threads`, winpthreads is
          # `windows.pthreads`; keep the old name as a fallback anyway.
          package = final.windows.pthreads or final.windows.mingw_w64_pthreads;
        };
  }
  # Unlike mcfgthread, winpthreads ships a `pthread.h`.  Any mechanism that
  # spreads its include dir through *setup-hook* env vars poisons the
  # native compiler in the same environment: hooks add
  # `-isystem .../winpthreads/include` to the host/build compiler flags,
  # which shadows glibc's pthread.h (winpthreads' pthread.h includes the
  # Windows-only <process.h> and everything burns down).  nixpkgs' posix
  # path has rotted this way since the default moved to mcf, in two places:
  #
  #  1. gcc's own build: dependencies.nix puts threadsCross.package in
  #     depsTargetTarget, whose hooks leak it into NIX_CFLAGS_COMPILE(_FOR_
  #     BUILD) and break host-side components (observed: gcc 13's libcody).
  #     The in-tree target-library builds (libgcc/libstdc++) don't need the
  #     hooks -- they receive winpthreads via the explicit
  #     EXTRA_{FLAGS,LDFLAGS}_FOR_TARGET -- so the dep entry can go.
  #
  #  2. The cc-wrapper: wrapCC puts threadsCross.package in the wrapper's
  #     extraPackages (propagated deps), leaking the same way into any
  #     downstream build environment that also compiles native code -- like
  #     Julia's, which builds host tools with HOSTCC.  Instead, bake
  #     -idirafter/-L flags for winpthreads directly into the wrapper: they
  #     then apply exactly when *this* cross compiler is invoked, and never
  #     contaminate other compilers in the same environment.
  //
    prev.lib.optionalAttrs
      (prev.stdenv.targetPlatform.isMinGW && !(prev.stdenv.targetPlatform.useLLVM or false))
      {
        gcc13 = prev.gcc13.override (old: {
          cc = old.cc.overrideAttrs (_: {
            depsTargetTarget = [ ];
          });
          extraPackages = [ ];
          extraBuildCommands = (old.extraBuildCommands or "") + ''
            echo "-idirafter ${final.threadsCross.package}/include" >> $out/nix-support/cc-cflags
            echo "-L${final.threadsCross.package}/lib" >> $out/nix-support/cc-ldflags
          '';
        });
      };

  crossSystems = {
    # libc = "msvcrt" (not "ucrt"): Julia's official Windows binaries and all
    # BinaryBuilder-provided dependencies link against msvcrt.
    x86_64 = {
      config = "x86_64-w64-mingw32";
      libc = "msvcrt";
    };
    i686 = {
      config = "i686-w64-mingw32";
      libc = "msvcrt";
    };
  };

  # Native package set, used for all tools that run on the build machine.
  pkgs = import nixpkgs { inherit system; };

  # Cross package set; we only take the toolchain out of its buildPackages.
  pkgsWin = import nixpkgs {
    localSystem = { inherit system; };
    crossSystem = crossSystems.${arch};
    overlays = [ posixThreadsOverlay ];
  };
in

rec {
  inherit pkgs pkgsWin arch;

  xcHost = crossSystems.${arch}.config;

  # GCC 13 rather than the nixpkgs default (14): Julia 1.11 sources predate
  # GCC 14, and the libstdc++-6.dll shipped by the (default-on)
  # BinaryBuilder CompilerSupportLibraries is of the GCC 13 era, so code
  # compiled by a newer g++ could require GLIBCXX symbol versions that DLL
  # does not have.
  crossCC = pkgsWin.buildPackages.gcc13;

  # Prefixed ar/as/ld/ranlib/dlltool/windres/... (Make.inc and cli/Makefile
  # invoke these as $(CROSS_COMPILE)ar, $(CROSS_COMPILE)windres, etc.)
  crossBintools = pkgsWin.buildPackages.bintools;

  # Runs the freshly built julia.exe (system image generation) and
  # winepath.exe during the build; see `spawn` in Make.inc.
  wine = if arch == "x86_64" then pkgs.wine64 else pkgs.wine;
  wineBin = "${wine}/bin/${if arch == "x86_64" then "wine64" else "wine"}";

  # Tools that run on the build machine.  A native C/C++ compiler (HOSTCC)
  # is provided by stdenv/mkShell and is not listed here.
  nativeTools = with pkgs; [
    gnumake
    cmake
    m4
    perl
    python3
    gawk
    patch
    pkg-config
    which
    git
    curl
    wget
    p7zip
    cacert # lets the nix-provided curl verify TLS when downloading deps
  ];
}
