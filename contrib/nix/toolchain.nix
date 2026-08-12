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
