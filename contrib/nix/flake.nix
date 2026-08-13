{
  description = "Cross-compile Julia from Linux to Windows (mingw-w64 with POSIX threads)";

  # Keep the shell.nix pin in sync when bumping this.
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.05";

  outputs =
    { self, nixpkgs }:
    let
      systems = [ "x86_64-linux" ];
      mkShellFor =
        system: arch:
        import ./windows.nix {
          nixpkgs = nixpkgs.outPath;
          inherit system arch;
        };
    in
    {
      devShells = nixpkgs.lib.genAttrs systems (system: rec {
        win64 = mkShellFor system "x86_64";
        win32 = mkShellFor system "i686";
        default = win64;
      });
    };
}
