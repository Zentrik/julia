# Development shell for cross-compiling Julia from Linux to Windows by hand.
# For a fully-automated build, see ./package.nix instead.  The toolchain
# itself (and the explanation of the posix-threads overlay) lives in
# ./toolchain.nix; usage is documented in ./README.md.

{
  nixpkgs, # path to a nixpkgs checkout (flake input outPath or fetchTarball)
  system ? builtins.currentSystem,
  arch ? "x86_64", # "x86_64" or "i686"
}:

let
  tc = import ./toolchain.nix { inherit nixpkgs system arch; };
  inherit (tc) pkgs;
in

pkgs.mkShell {
  name = "julia-windows-cross-${arch}";

  packages = [
    tc.crossCC
    tc.crossBintools
    tc.wine
  ] ++ tc.nativeTools;

  # -fstack-clash-protection makes GCC 13 ICE on mingw when emitting SEH
  # unwind info for large stack frames (gcc PR90458, fixed in GCC 14; hit by
  # cli/loader_win_utils.c).
  hardeningDisable = [ "stackclashprotection" ];

  env = {
    # Picked up by Make.inc (same as putting them in Make.user):
    # XC_HOST switches the build to a Windows cross-compile, WINE is used to
    # spawn Windows executables during the build.
    XC_HOST = tc.xcHost;
    WINE = tc.wineBin;
  };

  shellHook = ''
    echo "Julia Windows cross-compilation shell (XC_HOST=$XC_HOST)"
    echo "  cross gcc:    $(${tc.xcHost}-gcc -dumpversion) ($(${tc.xcHost}-gcc -v 2>&1 | grep '^Thread model'))"
    echo "  wine:         $WINE"
    echo
    echo "Build with:     make -j\$(nproc)"
    echo "Installer:      make win-extras && make binary-dist && make exe"
  '';
}
