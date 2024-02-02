// This file is a part of Julia. License is MIT: https://julialang.org/license

#ifndef JL_HASHING_H
#define JL_HASHING_H

#include "utils.h"
#include "dtypes.h"
#include "analyzer_annotations.h"

#ifdef __cplusplus
extern "C" {
#endif

uint_t nextipow2(uint_t i) JL_NOTSAFEPOINT;
uint32_t int32hash(uint32_t a) JL_NOTSAFEPOINT;
uint64_t int64hash(uint64_t key) JL_NOTSAFEPOINT;
uint32_t int64to32hash(uint64_t key) JL_NOTSAFEPOINT;
#ifdef _P64
#define inthash int64hash
#else
#define inthash int32hash
#endif
JL_DLLEXPORT uint64_t memhash(const char *buf, size_t n) JL_NOTSAFEPOINT;
JL_DLLEXPORT uint64_t memhash_seed(const char *buf, size_t n, uint32_t seed) JL_NOTSAFEPOINT;
JL_DLLEXPORT uint32_t memhash32(const char *buf, size_t n) JL_NOTSAFEPOINT;
JL_DLLEXPORT uint32_t memhash32_seed(const char *buf, size_t n, uint32_t seed) JL_NOTSAFEPOINT;

// AHash
uint64_t folded_multiply(uint64_t s, uint64_t by) {
    __uint128_t result = (__uint128_t)s * (__uint128_t)by;
    return (uint64_t)(result & 0xffffffffffffffff) ^ (uint64_t)(result >> 64);
}
uint64_t update_ahash(uint64_t x, uint64_t h) {
    return folded_multiply(x ^ h, 6364136223846793005);
}
uint64_t finalize_ahash(uint64_t h) {
    h = folded_multiply(h, 0x13198a2e03707344);
    return (h << (h & 63)) | (h >> (8*sizeof(h) - (h & 63))); // rotate (hash & 63) bits to the left
}

#ifdef _P64
STATIC_INLINE uint64_t bitmix(uint64_t h, uint64_t a) JL_NOTSAFEPOINT
{
    return update_ahash(a, h);
}
#else
STATIC_INLINE uint32_t bitmix(uint32_t h, uint32_t a) JL_NOTSAFEPOINT
{
    return int64to32hash((((uint64_t)h) << 32) | (uint64_t)a);
}
#endif
#define bitmix(h, a) (bitmix)((uintptr_t)(h), (uintptr_t)(a))

#ifdef __cplusplus
}
#endif

#endif