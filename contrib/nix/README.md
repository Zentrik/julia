# Cross-compiling Julia to Windows with Nix

This directory lets you build Julia for Windows from Linux using
[Nix](https://nixos.org) — either fully inside Nix as a package build, or
interactively in a dev shell. It provides a complete mingw-w64 GCC
toolchain with the correct thread model, wine, and all other required
tools.

| File | Purpose |
|---|---|
| `toolchain.nix` | The cross toolchain (posix-threads overlay) and tool set |
| `package.nix` | Full Julia Windows build as a Nix derivation (`nix-build`) |
| `windows.nix` | Interactive dev shell (manual `make`) |
| `flake.nix` / `shell.nix` | Flake and classic entry points for the dev shell |

## The pthreads problem, and how this solves it

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

`toolchain.nix` fixes this with an overlay that overrides nixpkgs'
`threadsCross` attribute, rebuilding the cross GCC with
`--enable-threads=posix` against winpthreads
(`windows.mingw_w64_pthreads`) — the exact equivalent of Debian's
`x86_64-w64-mingw32-gcc-posix`.

## Building Julia as a Nix package

```sh
nix-build contrib/nix/package.nix                   # 64-bit Windows
nix-build contrib/nix/package.nix --argstr arch i686
```

Both the x86_64 and i686 BinaryBuilder-mode builds are validated end to end
in sandboxed Nix builds: the produced `result/bin/julia.exe` runs under wine
(64-bit and 32-bit respectively) with working multithreading
(`wine64 result/bin/julia.exe -t 4 -e 'Threads.@spawn ...'`).

### Building the dependencies from source

```sh
nix-build contrib/nix/package.nix --arg useBinaryBuilder false
```

builds every dependency from source (LLVM, OpenBLAS via the cross
gfortran, SuiteSparse, GMP/MPFR, curl, ...) instead of downloading
BinaryBuilder binaries — the same philosophy as nixpkgs' own from-source
Julia packages, adapted for the Windows cross build.  This takes several
extra hours (LLVM dominates).  Validated end to end for x86_64: the result
runs under wine with working threads, BLAS (through libblastrampoline) and
BigFloat.

One deliberate exception: the bundled standalone `7z.exe` still comes from
BinaryBuilder, because the p7zip codebase is a Unix-only port that cannot
be cross-compiled for Windows.  It is a utility program, never linked
against Julia.  The compiler-support runtime DLLs (libstdc++-6.dll,
libwinpthread-1.dll, libgfortran-5.dll, ...) are bundled from the Nix
cross toolchain itself, so they exactly match the compiler that built
everything.

The from-source mode is only wired up for cross builds from this
directory; it leans on several build-system fixes on this branch
(OpenBLAS cross TARGET, blastrampoline/SuiteSparse DLL naming, curl zstd
detection, CSL bundling of link libraries and runtime DLLs), most of which
had bit-rotted upstream since BinaryBuilder became the default.

`./result` is a Windows Julia installation tree (`bin/julia.exe`, `lib/`,
`share/`); zip it up and copy it to a Windows machine.

Julia's build system normally downloads dependencies *during* the build
(BinaryBuilder tarballs for LLVM, OpenBLAS, …, plus external stdlibs like
Pkg), which Nix's network-isolated builds don't allow. `package.nix`
therefore pre-fetches everything in a **fixed-output derivation** (the
same pattern as Go's `vendorHash`), pinned by the `depsHashes` attribute
in `package.nix`. Whenever dependency versions change in the source tree
(`deps/*.version`, `deps/checksums/`, stdlib versions), that hash goes
stale: rebuild the cache with `depsHash = null` (or `lib.fakeHash`) and
copy the `got:` hash from the mismatch error back into `depsHashes`.

Notes:

- The package is built with plain `nix-build` rather than as a flake
  output: this flake lives in a subdirectory, and pure flake evaluation
  cannot reach the Julia sources at `../..`. (The dev shells below are
  flake outputs, since they don't need the sources.)
- The source tree is taken via `builtins.fetchGit`, i.e. tracked files
  only — a dirty working tree works, but untracked files are invisible to
  the build.
- The system image is compiled by running the freshly built `julia.exe`
  under wine, inside the Nix build. `JULIA_CPU_TARGET` defaults to
  `generic` here (override with `--argstr cpuTarget ...`) so the sysimage
  isn't tuned to the build machine.

## Dev shell (manual builds)

With flakes (the `path:` prefix avoids copying your whole Julia checkout
into the Nix store):

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

- **First build compiles the toolchain from source.** The posix-threads
  toolchain differs from what the NixOS binary cache holds, so the first
  build compiles cross GCC and winpthreads locally (roughly 1–2 hours).
  It is cached in the Nix store afterwards.
- **Verify the thread model** with
  `x86_64-w64-mingw32-gcc -v 2>&1 | grep Thread` — it must say
  `Thread model: posix`.
- **GCC version.** The toolchain uses GCC 13 (`crossCC` in
  `toolchain.nix`) rather than the nixpkgs default, both because this
  source tree predates GCC 14 and because the `libstdc++-6.dll` shipped by
  the BinaryBuilder CompilerSupportLibraries (used at run time) is of the
  GCC 13 era. If you see `GLIBCXX_...' not found` errors from `julia.exe`
  under wine, the compiler is too new for those DLLs.
- **Dependencies come from BinaryBuilder.** The default
  `USE_BINARYBUILDER=1` build downloads prebuilt Windows binaries for LLVM,
  OpenBLAS, etc., so no cross gfortran is needed. Building all deps from
  source (`USE_BINARYBUILDER=0`) would additionally require a cross
  gfortran, which this toolchain does not provide.
- **Wine noise.** In the dev shell, set `WINEDEBUG=-all` to silence wine's
  warnings during the sysimage build (package builds do this already).
  Wine creates its prefix in `~/.wine` on first use; set `WINEPREFIX` to
  keep it elsewhere.
- **Updating nixpkgs.** Bump the branch in `flake.nix` and the tarball rev
  in `shell.nix`/`package.nix` together. The winpthreads package was
  renamed between 25.05 (`windows.mingw_w64_pthreads`) and later nixpkgs
  (`windows.pthreads`); the overlay in `toolchain.nix` handles both, but
  on 25.05 `windows.pthreads` is an unrelated library (pthreads-win32), so
  keep the fallback order intact.
