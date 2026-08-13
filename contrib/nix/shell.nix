# Non-flake entry point:
#
#     nix-shell contrib/nix/shell.nix                     # 64-bit Windows
#     nix-shell contrib/nix/shell.nix --argstr arch i686  # 32-bit Windows
#
# See ./README.md for details.
{
  arch ? "x86_64",
  gccVersion ? "13",
  # nixos-25.05 (release-25.05 as of 2026-08-12).  Keep in (rough) sync with
  # the flake.nix input when updating.
  nixpkgs ? fetchTarball "https://github.com/NixOS/nixpkgs/archive/3e3f3c7f9977dc123c23ee21e8085ed63daf8c37.tar.gz",
}:

import ./windows.nix { inherit nixpkgs arch gccVersion; }
