# Cross-compiling Julia to Windows with Nix

This directory provides a [Nix](https://nixos.org) development shell for
cross-compiling Julia from Linux to Windows, containing a complete
mingw-w64 GCC toolchain, wine, and all other required build tools.

## The pthreads problem, and how this shell solves it

Julia's Windows build requires a mingw-w64 toolchain built with the
**posix thread model** (winpthreads): `src/Makefile` links with
`-lpthread`, and libuv, libjulia and flisp use the pthread API directly.
That is why the [Windows build docs](../../doc/src/devdocs/build/windows.md)
tell Ubuntu users to switch their mingw compilers to the `-posix` variants
with `update-alternatives`.

Since NixOS 24.05, nixpkgs builds its mingw-w64 GCC
(`pkgsCross.mingwW64.stdenv.cc`) with the **mcf thread model**
([mcfgthread](https://github.com/lhmouse/mcfgthread)) instead — check with
`x86_64-w64-mingw32-gcc -v`, which reports `Thread model: mcf`. That
toolchain has no `pthread.h` and no `libpthread`, so the Julia build fails
at compile/link time.

`windows.nix` fixes this with an overlay that overrides nixpkgs'
`threadsCross` attribute, rebuilding the cross GCC with
`--enable-threads=posix` against winpthreads
(`windows.mingw_w64_pthreads`) — the exact equivalent of Debian's
`x86_64-w64-mingw32-gcc-posix`.

## Usage

With flakes enabled (the `path:` prefix avoids copying your whole Julia
checkout into the Nix store):

```sh
nix develop path:./contrib/nix          # 64-bit Windows (also `#win64`)
nix develop path:./contrib/nix#win32    # 32-bit Windows
```

Or without flakes:

```sh
nix-shell contrib/nix/shell.nix                     # 64-bit Windows
nix-shell contrib/nix/shell.nix --argstr arch i686  # 32-bit Windows
```

Then, from the repository root, build as described in the
[Windows build docs](../../doc/src/devdocs/build/windows.md):

```sh
make -j$(nproc)
make win-extras    # necessary before make binary-dist
make binary-dist   # .zip / .tar.gz
make exe           # Windows installer
```

The shell exports `XC_HOST` (`x86_64-w64-mingw32` or `i686-w64-mingw32`)
and `WINE`, which `Make.inc` picks up automatically — no `Make.user` needed
to enable the cross build. Because of that, a plain `make` inside this
shell always cross-compiles; consider a separate build directory
(`make O=build-win64 configure`) if you also build natively from the same
checkout.

## Notes

- **First entry builds the toolchain from source.** The posix-threads
  toolchain differs from what the NixOS binary cache holds, so the first
  `nix develop`/`nix-shell` compiles cross GCC and winpthreads locally
  (roughly 20–60 minutes). It is cached in the Nix store afterwards.
- **Verify the thread model** inside the shell with
  `x86_64-w64-mingw32-gcc -v 2>&1 | grep Thread` — it must say
  `Thread model: posix`.
- **GCC version.** The shell uses GCC 13 (`pkgsWin.buildPackages.gcc13` in
  `windows.nix`) rather than the nixpkgs default, both because this source
  tree predates GCC 14 and because the `libstdc++-6.dll` shipped by the
  BinaryBuilder CompilerSupportLibraries (used at run time) is of the
  GCC 13 era. If you see `GLIBCXX_...' not found` errors from `julia.exe`
  under wine, the compiler is too new for those DLLs.
- **Dependencies come from BinaryBuilder.** The default
  `USE_BINARYBUILDER=1` build downloads prebuilt Windows binaries for LLVM,
  OpenBLAS, etc., so no cross gfortran is needed. Building all deps from
  source (`USE_BINARYBUILDER=0`) would additionally require a cross
  gfortran, which this shell does not provide.
- **Wine noise.** Set `WINEDEBUG=-all` to silence wine's warnings during
  the sysimage build. Wine creates its prefix in `~/.wine` on first use;
  set `WINEPREFIX` to keep it elsewhere.
- **Updating nixpkgs.** Bump the branch in `flake.nix` and the tarball rev
  in `shell.nix` together. The winpthreads package was renamed between
  25.05 (`windows.mingw_w64_pthreads`) and later nixpkgs
  (`windows.pthreads`); the overlay in `windows.nix` handles both, but on
  25.05 `windows.pthreads` is an unrelated library (pthreads-win32), so
  keep the fallback order intact.
