#include "util/easy_mod_stat.h"
#include <string.h>

typedef struct alloc_stat_header_t {
    mod_stat_t* mod_stat;
    int64_t size;
} alloc_stat_header_t;

#define MOD_STAT_COUNT (1<<16)
mod_stat_t global_mod_stat_table[MOD_STAT_COUNT];
__thread mod_stat_t* easy_cur_mod_stat;
extern void *easy_pool_default_realloc(void *ptr, size_t size);
void* (*realloc_lowlevel)(void*, size_t) = easy_pool_default_realloc;

static uint64_t rand64(uint64_t h)
{
    h ^= h >> 33;
    h *= 0xff51afd7ed558ccd;
    h ^= h >> 33;
    h *= 0xc4ceb9fe1a85ec53;
    h ^= h >> 33;
    return h;
}

static int try_set_slot(mod_stat_t* st, uint64_t id)
{
    uint64_t cur_id = __atomic_load_n(&st->id, __ATOMIC_SEQ_CST);

    if (cur_id != 0) {
        return cur_id == id;
    }
    return __sync_bool_compare_and_swap(&st->id, cur_id, id);
}

static mod_stat_t* get_mod_stat(uint64_t id)
{
    uint64_t i;
    uint64_t h = rand64(id);

    for (i = 0; i < MOD_STAT_COUNT; i++) {
        mod_stat_t* stat = global_mod_stat_table + ((h + i) % MOD_STAT_COUNT);
        if (try_set_slot(stat, id)) {
            return stat;
        }
    }
    return NULL;
}

static mod_stat_t* mod_stat_decode_header(alloc_stat_header_t* h, int64_t* size)
{
    *size = h->size;
    return h->mod_stat;
}

static mod_stat_t* mod_stat_encode_header(alloc_stat_header_t* h, mod_stat_t* stat, int64_t size)
{
    h->size = size;
    return h->mod_stat = stat;
}

static void update_mod_stat(mod_stat_t* stat, int64_t count_inc, int64_t size_inc)
{
    if (stat) {
        __sync_fetch_and_add(&stat->count, count_inc);
        __sync_fetch_and_add(&stat->size, size_inc);
    }
}

void* realloc_with_mod_stat(void* ptr, size_t size)
{
    const int64_t header_size = sizeof(alloc_stat_header_t);
    mod_stat_t* stat = NULL;
    if (ptr) {
        int64_t old_size = 0;
        ptr = (char*)ptr - header_size;
        stat = mod_stat_decode_header((alloc_stat_header_t*)ptr, &old_size);
        update_mod_stat(stat, -1, -old_size);
    }
    ptr = realloc_lowlevel(ptr, size > 0 ? size + header_size : 0);
    if (ptr) {
        stat = mod_stat_encode_header((alloc_stat_header_t*)ptr, easy_cur_mod_stat, size);
        update_mod_stat(stat, 1, size);
        ptr = (char*)ptr + header_size;
    }
    return ptr;
}

mod_stat_t* easy_fetch_mod_stat(uint64_t id)
{
    return get_mod_stat(id);
}
