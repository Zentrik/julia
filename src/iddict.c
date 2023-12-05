// This file is a part of Julia. License is MIT: https://julialang.org/license

#define hash_size(h) (h->length / 2)

// compute empirical max-probe for a given size
#define max_probe(size) ((size) <= 1024 ? 16 : (size) >> 6)

#define keyhash(k) jl_object_id_(jl_typetagof(k), k)
#define h2index(hv, sz) (size_t)(((hv) & ((sz)-1)) * 2)

static inline int jl_table_assign_bp(jl_genericmemory_t **pa, jl_value_t *key, jl_value_t *val, int rehash);

JL_DLLEXPORT jl_genericmemory_t *jl_idtable_rehash(jl_genericmemory_t *a, size_t newsz)
{
    size_t sz = a->length;
    size_t i;
    jl_value_t **ol = (jl_value_t **) a->ptr;
    jl_genericmemory_t *newa = jl_alloc_memory_any(newsz);
    // keep the original memory in the original slot since we need `ol`
    // to be valid in the loop below.
    JL_GC_PUSH2(&newa, &a);
    for (i = 0; i < sz; i += 2) {
        if (ol[i + 1] != NULL) {
            jl_table_assign_bp(&newa, ol[i], ol[i + 1], 1);
            // it is however necessary here because allocation
            // can (and will) occur in a recursive call inside table_lookup_bp
        }
    }
    JL_GC_POP();
    return newa;
}

static inline int jl_table_assign_bp(jl_genericmemory_t **pa, jl_value_t *key, jl_value_t *val, int rehash)
{
    // pa points to a **un**rooted address
    uint_t hv;
    jl_genericmemory_t *a = *pa;
    size_t orig, index, iter, empty_slot;
    size_t newsz, sz = hash_size(a);
    if (sz == 0) {
        a = jl_alloc_memory_any(HT_N_INLINE);
        sz = hash_size(a);
        *pa = a;
    }
    size_t maxprobe = max_probe(sz);
    _Atomic(jl_value_t*) *tab = (_Atomic(jl_value_t*)*) a->ptr;

    // if (!rehash && count > h->length / 7) {
    //     /* table load factor > 70% */
    //     /* quadruple size, rehash, retry the insert */
    //     /* it's important to grow the table really fast; otherwise we waste */
    //     /* lots of time rehashing all the keys over and over. */
    //     sz = a -> length;
    //     if (sz < HT_N_INLINE)
    //         newsz = HT_N_INLINE;
    //     else if (sz >= (1 << 19) || (sz <= (1 << 8)))
    //         newsz = sz << 1;
    //     else
    //         newsz = sz << 2;
    //     *pa = jl_idtable_rehash(*pa, newsz);

    //     a = *pa;
    //     tab =  (_Atomic(jl_value_t*)*) a->ptr;
    //     sz = hash_size(a);
    //     maxprobe = max_probe(sz);
    // }

    while (1) {
        iter = 0;
        hv = keyhash(key);
        index = h2index(hv, sz);
        size_t two_sz = sz*2;
        orig = index;
        empty_slot = -1;

        size_t probe_current = 0;

        do {
            jl_value_t *k2 = jl_atomic_load_relaxed(&tab[index]);
            if (k2 == NULL) {
                if (empty_slot == -1)
                    empty_slot = index;
                break;
            }
            if (rehash == 0 && jl_egal(key, k2)) {
                if (jl_atomic_load_relaxed(&tab[index + 1]) != NULL) {
                    jl_atomic_store_release(&tab[index + 1], val);
                    jl_gc_wb(a, val);
                    return 0;
                }
                assert(0);
            }
            size_t desired_index = h2index(keyhash(k2), sz);
            size_t current_distance = (index + two_sz - desired_index) & (two_sz - 1);

            if (probe_current > desired_index) {
                jl_value_t *val2 = jl_atomic_load_relaxed(&tab[index + 1]);
                jl_atomic_store_release(&tab[index], key);
                jl_gc_wb(a, key);
                jl_atomic_store_release(&tab[index + 1], val);
                jl_gc_wb(a, val);
                val = val2;
                key = k2;
                probe_current = current_distance;
                // hv = keyhash(k2);
            }

            if (empty_slot == -1 && jl_atomic_load_relaxed(&tab[index + 1]) == NULL) {
                assert(0);
                assert(jl_atomic_load_relaxed(&tab[index]) == jl_nothing);
                empty_slot = index;
            }

            index = (index + 2) & (two_sz - 1);
            // assert(index != orig);
            iter++;
            probe_current += 2;
        } while (iter <= maxprobe && index != orig);

        if (empty_slot != -1) {
            jl_atomic_store_release(&tab[empty_slot], key);
            jl_gc_wb(a, key);
            jl_atomic_store_release(&tab[empty_slot + 1], val);
            jl_gc_wb(a, val);
            return 1;
        }

        /* table full or maxprobe reached */
        /* quadruple size, rehash, retry the insert */
        /* it's important to grow the table really fast; otherwise we waste */
        /* lots of time rehashing all the keys over and over. */
        sz = a -> length;
        if (sz < HT_N_INLINE)
            newsz = HT_N_INLINE;
        else if (sz >= (1 << 19) || (sz <= (1 << 8)))
            newsz = sz << 1;
        else
            newsz = sz << 2;
        *pa = jl_idtable_rehash(*pa, newsz);

        a = *pa;
        tab =  (_Atomic(jl_value_t*)*) a->ptr;
        sz = hash_size(a);
        maxprobe = max_probe(sz);
    }
}

/* returns index of value if key is in hash, otherwise 0 */
static inline size_t jl_table_peek_valueindex(jl_genericmemory_t *a, jl_value_t *key) JL_NOTSAFEPOINT
{
    size_t sz = hash_size(a);
    if (sz == 0)
        return 0;
    size_t maxprobe = max_probe(sz);
    _Atomic(jl_value_t*) *tab = (_Atomic(jl_value_t*)*) a->ptr;
    uint_t hv = keyhash(key);
    size_t index = h2index(hv, sz);
    size_t two_sz = sz * 2;
    size_t orig = index;
    size_t probe_current = 0;
    size_t iter = 0;

    do {
        jl_value_t *k2 = jl_atomic_load_relaxed(&tab[index]); // just to ensure the load doesn't get duplicated
        if (k2 == NULL)
            return 0;

        // size_t desired_index = h2index(keyhash(k2), sz);
        // size_t current_distance = (index + two_sz - desired_index) & (two_sz - 1);
        // if (current_distance < probe_current)
        //     return 0;

        if (jl_egal(key, k2)) {
            if (jl_atomic_load_relaxed(&tab[index + 1]) != NULL)
                return index + 1;
            assert(0);
            // `nothing` is our sentinel value for deletion, so need to keep searching if it's also our search key
            if (key != jl_nothing)
                return 0; // concurrent insertion hasn't completed yet
        }

        index = (index + 2) & (two_sz - 1);
        probe_current += 2;
        iter++;
    } while (iter <= maxprobe && index != orig);

    return 0;
}

/* returns bp if key is in hash, otherwise NULL */
inline _Atomic(jl_value_t*) *jl_table_peek_bp(jl_genericmemory_t *a, jl_value_t *key) JL_NOTSAFEPOINT
{
    _Atomic(jl_value_t*) *tab = (_Atomic(jl_value_t*)*) a->ptr;
    size_t validx = jl_table_peek_valueindex(a, key);
    if (validx != 0)
        return &tab[validx];
    return NULL;
}

JL_DLLEXPORT
jl_genericmemory_t *jl_eqtable_put(jl_genericmemory_t *h, jl_value_t *key, jl_value_t *val, int *p_inserted)
{
    int inserted = jl_table_assign_bp(&h, key, val, 0);
    if (p_inserted)
        *p_inserted = inserted;
    return h;
}

// Note: lookup in the IdDict is permitted concurrently, if you avoid deletions,
// and assuming you do use an external lock around all insertions
JL_DLLEXPORT
jl_value_t *jl_eqtable_get(jl_genericmemory_t *h, jl_value_t *key, jl_value_t *deflt) JL_NOTSAFEPOINT
{
    _Atomic(jl_value_t*) *bp = jl_table_peek_bp(h, key);
    return (bp == NULL) ? deflt : jl_atomic_load_relaxed(bp);
}

jl_value_t *jl_eqtable_getkey(jl_genericmemory_t *h, jl_value_t *key, jl_value_t *deflt) JL_NOTSAFEPOINT
{
    _Atomic(jl_value_t*) *bp = jl_table_peek_bp(h, key);
    return (bp == NULL) ? deflt : jl_atomic_load_relaxed(bp - 1);
}

JL_DLLEXPORT
jl_value_t *jl_eqtable_pop(jl_genericmemory_t *h, jl_value_t *key, jl_value_t *deflt, int *found)
{
    size_t validx = jl_table_peek_valueindex(h, key);
    if (found)
        *found = (validx != 0);
    if (validx == 0)
        return deflt;

    _Atomic(jl_value_t*) *tab = (_Atomic(jl_value_t*)*) h->ptr;
    jl_value_t *val = jl_atomic_load_relaxed(&tab[validx]);

    size_t keyidx = validx-1;
    size_t sz = hash_size(h);
    size_t two_sz = sz*2;
    while (1) {
        jl_atomic_store_relaxed(&tab[keyidx], NULL); // clear the key
        jl_atomic_store_relaxed(&tab[keyidx+1], NULL); // and the value
        size_t next_keyidx = (keyidx + 2) & (two_sz - 1);
        jl_value_t *k2 = jl_atomic_load_relaxed(&tab[next_keyidx]);
        if (k2 == NULL) break; // empty key
        size_t desired_index = h2index(keyhash(k2), sz);
        if (next_keyidx == desired_index) break;
        // backshift
        jl_atomic_store_relaxed(&tab[keyidx], k2);
        jl_atomic_store_relaxed(&tab[keyidx+1], jl_atomic_load_relaxed(&tab[next_keyidx+1]));
        keyidx = next_keyidx;
    }
    return val;
}

JL_DLLEXPORT
size_t jl_eqtable_nextind(jl_genericmemory_t *t, size_t i)
{
    if (i & 1)
        i++;
    size_t alen = t->length;
    while (i < alen && ((void**) t->ptr)[i + 1] == NULL)
        i += 2;
    if (i >= alen)
        return (size_t)-1;
    return i;
}

#undef hash_size
#undef max_probe
#undef h2index
