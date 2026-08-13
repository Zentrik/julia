SRCDIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
BUILDDIR := .
JULIAHOME := $(SRCDIR)
include $(JULIAHOME)/Make.inc
include $(JULIAHOME)/stdlib/stdlib.mk


# set some influential environment variables
# The env var must be a Windows path for julia.exe (cygpath_w), but make
# targets/prereqs need the plain Unix path: under a wine cross-compile the
# converted path contains a drive colon (Z:\...), which is a make syntax
# error in target position on the build host.
JULIA_DEPOT_PATH_HOST := $(build_prefix)/share/julia
export JULIA_DEPOT_PATH := $(shell echo $(call cygpath_w,$(JULIA_DEPOT_PATH_HOST)))
export JULIA_LOAD_PATH := @stdlib$(PATHSEP)$(shell echo $(call cygpath_w,$(JULIAHOME)/stdlib))
unexport JULIA_PROJECT :=
unexport JULIA_BINDIR :=

export JULIA_FALLBACK_REPL := true

default: release
release: $(BUILDDIR)/stdlib/release.image
debug: $(BUILDDIR)/stdlib/debug.image
all: release debug

$(JULIA_DEPOT_PATH_HOST)/compiled:
	mkdir -p $@

print-depot-path:
	@$(call PRINT_JULIA, $(call spawn,$(JULIA_EXECUTABLE)) --startup-file=no -e '@show Base.DEPOT_PATH')

$(BUILDDIR)/stdlib/%.image: $(JULIAHOME)/stdlib/Project.toml $(JULIAHOME)/stdlib/Manifest.toml $(INDEPENDENT_STDLIBS_SRCS) $(JULIA_DEPOT_PATH_HOST)/compiled
	@$(call PRINT_JULIA, JULIA_CPU_TARGET="$(JULIA_CPU_TARGET)" $(call spawn,$(JULIA_EXECUTABLE)) --startup-file=no -e \
		'Base.Precompilation.precompilepkgs(configs=[``=>Base.CacheFlags(debug_level=2, opt_level=3), ``=>Base.CacheFlags(check_bounds=1, debug_level=2, opt_level=3)]; strict=true)')
	touch $@

$(BUILDDIR)/stdlib/release.image: $(build_private_libdir)/sys.$(SHLIB_EXT)
$(BUILDDIR)/stdlib/debug.image: $(build_private_libdir)/sys-debug.$(SHLIB_EXT)

clean:
	rm -rf $(JULIA_DEPOT_PATH_HOST)/compiled
	rm -f $(BUILDDIR)/stdlib/*.image
