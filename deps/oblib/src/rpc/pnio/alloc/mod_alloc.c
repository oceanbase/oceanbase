/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef PKT_NIO_MALLOC
#define PKT_NIO_MALLOC(size, label) malloc(size);
#define PKT_NIO_FREE(p)      free(p);
#endif

typedef struct alloc_head_t {
  link_t link;
  int mod;
  int64_t sz;
} alloc_head_t;

static int64_t mod_stat[MOD_MAX_COUNT + 1];
static const char* mod_name[MOD_MAX_COUNT + 1] = {
#define MOD_DEF(name) #name, //keep
#include "mod_define.h"
#undef MOD_DEF
};

static void mod_update(int mod, int64_t sz) {
  FAA(&mod_stat[mod], sz);
}

void mod_report(format_t* f) {
  for(int i = 0; i < MOD_MAX_COUNT; i++) {
    if (mod_stat[i] != 0) {
      format_append(f, "%s:%'8ld ", mod_name[i], mod_stat[i]);
    }
  }
}

static link_queue_t global_free_blocks;
static __attribute__((constructor)) void init_global_free_blocks()
{
  link_queue_init(&global_free_blocks);
}

void* mod_alloc(int64_t sz, int mod) {
#ifndef ALLOC_ERRSIM_FREQ
#define ALLOC_ERRSIM_FREQ 20
#endif
#ifdef PNIO_ERRSIM
  int rand_value = rand();
  if (rand_value % ALLOC_ERRSIM_FREQ == 0) {
    rk_warn("mod_alloc return null. rand_value = %d\n", rand_value);
    errno = ENOMEM;
    return NULL;
  }
#endif
  PNIO_DELAY_WARN(STAT_TIME_GUARD(eloop_malloc_count, eloop_malloc_time));
  const char* label = mod_name[mod];
  alloc_head_t* h = (typeof(h))PKT_NIO_MALLOC(sizeof(*h) + sz, label);
  if (h) {
    h->mod = mod;
    h->sz = sz;
    mod_update(mod, sz);
    h = h + 1;
  }
  return (void*)h;
}

void mod_free(void* p) {
  if (p) {
    alloc_head_t* h = (typeof(h))p - 1;
    mod_update(h->mod, -h->sz);
    PKT_NIO_FREE(h);
  }
}

void* salloc(int64_t sz)
{
  return mod_alloc(sz, MOD_DEFAULT);
}

void sfree(void* p)
{
  mod_free(p);
}
