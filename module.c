/*
  modules and top-level bindings
*/
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdarg.h>
#include <assert.h>
#include <sys/types.h>
#include <limits.h>
#include <errno.h>
#include <math.h>
#ifndef NO_BOEHM_GC
#include <gc.h>
#endif
#include "llt.h"
#include "julia.h"

jl_module_t *jl_system_module;
jl_module_t *jl_user_module;

jl_module_t *jl_new_module(jl_sym_t *name)
{
    jl_module_t *m = (jl_module_t*)allocb(sizeof(jl_module_t));
    m->name = name;
    htable_new(&m->bindings, 0);
    htable_new(&m->modules, 0);
    arraylist_new(&m->imports, 0);
}

jl_binding_t *jl_get_binding(jl_module_t *m, jl_sym_t *var)
{
    jl_binding_t **bp = (jl_binding_t**)ptrhash_bp(&m->bindings, var);
    if (*bp == HT_NOTFOUND) {
        jl_binding_t *b = (jl_binding_t*)allocb(sizeof(jl_binding_t));
        b->name = var;
        b->value = NULL;
        b->type = jl_any_type;
        b->constp = 0;
        b->exportp = 0;
        *bp = b;
    }
    return *bp;
}

int jl_boundp(jl_module_t *m, jl_sym_t *var)
{
    jl_binding_t *b = (jl_binding_t*)ptrhash_get(&m->bindings, var);
    return (b != HT_NOTFOUND && b->value != NULL);
}

void jl_set_global(jl_module_t *m, jl_sym_t *var, jl_value_t *val)
{
    jl_binding_t *bp = jl_get_binding(m, var);
    if (!bp->constp) {
        bp->value = val;
    }
}

void jl_set_const(jl_module_t *m, jl_sym_t *var, jl_value_t *val)
{
    jl_binding_t *bp = jl_get_binding(m, var);
    if (!bp->constp) {
        bp->value = val;
        bp->constp = 1;
    }
}

jl_module_t *jl_add_module(jl_module_t *m, jl_module_t *child);
jl_module_t *jl_get_module(jl_module_t *m, jl_sym_t *name);
jl_module_t *jl_import_module(jl_module_t *to, jl_module_t *from);

jl_value_t **jl_get_bindingp(jl_module_t *m, jl_sym_t *var)
{
    jl_binding_t *b = jl_get_binding(m, var);
    return &b->value;
}

void jl_init_modules()
{
    jl_system_module = jl_new_module(jl_symbol("System"));
    jl_user_module = jl_new_module(jl_symbol("User"));
}
