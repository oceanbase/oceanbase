#ifndef PKT_NIO_MALLOC
#define PKT_NIO_MALLOC(size, label) malloc(size);
#define PKT_NIO_FREE(p)      free(p);
#endif

typedef struct alloc_head_t {
  link_t link;
  int mod;
  int64_t sz;
  int64_t access_time_us;
} alloc_head_t;

static int64_t mod_stat[MOD_MAX_COUNT + 1];
static const char* mod_name[MOD_MAX_COUNT + 1] = {
#define MOD_DEF(name) #name, //keep
#include "mod_define.h"
#undef MOD_DEF
};
#define MEM_CACHE_RESERVE     (1<<16) // 16K
#define MEM_CACHE_UPPER_BOUND (0x1200000 - MEM_CACHE_RESERVE) // 18M-16K
#define MEM_CACHE_LOWER_BOUND (0x400000 - MEM_CACHE_RESERVE) // 4M-16K
#define MEM_CACHE_INTERVAL    (1<<21) // 2M
#define MEM_CACHED_TIME_US    (10000000) // 10s
#define MAX_CACHED_CNT        8
#define MEM_FREELISTS_CNT     (MEM_CACHE_UPPER_BOUND/MEM_CACHE_INTERVAL + 1)
static link_queue_t mem_freelists[MEM_FREELISTS_CNT];
static link_queue_t* locate_freelist(int64_t sz) {
  assert((sz + MEM_CACHE_RESERVE) % MEM_CACHE_INTERVAL == 0);
  return &mem_freelists[sz/MEM_CACHE_INTERVAL];
}
static int64_t upper_align_interval(int64_t input) {
  input = input + MEM_CACHE_RESERVE;
  int64_t ret = ((input + MEM_CACHE_INTERVAL - 1) & ~(MEM_CACHE_INTERVAL - 1)) - MEM_CACHE_RESERVE;
  return ret;
}
void init_mem_freelists() {
  for (int i = 0; i < MEM_FREELISTS_CNT; i++) {
    link_queue_init(&mem_freelists[i]);
  }
}
void refresh_mem_freelists() {
  int64_t cur_time = rk_get_corse_us();
  for (int i = 0; i < MEM_FREELISTS_CNT; i++) {
    link_t* l = NULL;
    int64_t queue_cnt = LOAD(&mem_freelists[i].cnt);
    while (queue_cnt > 0 && NULL != (l = link_queue_pop(&mem_freelists[i]))) {
      alloc_head_t* h = structof(l, alloc_head_t, link);
      if (cur_time - h->access_time_us > MEM_CACHED_TIME_US) {
        PKT_NIO_FREE(h);
      } else {
        link_queue_push(&mem_freelists[i], &h->link);
      }
      queue_cnt --;
    }
  }
}
const char* mem_freelists_str() {
  char str[512] = {'\0'};
  int pos = 0;
  for (int i = 0; i < MEM_FREELISTS_CNT; i++) {
    int64_t queue_cnt = LOAD(&mem_freelists[i].cnt);
    int n = snprintf(str + pos, sizeof(str) - pos, "%ld,", queue_cnt);
    if (n > 0 && pos + n < sizeof(str)) {
      pos += n;
    } else {
      break;
    }
  }
  format_t tf;
  format_init(&tf, sizeof(tf.buf));
  mod_report(&tf);
  return format_sf(&g_log_fbuf, "mem_freelists_str:%s, mod_report:%s", str, format_gets(&tf));
}

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
  alloc_head_t* h = NULL;
  link_t* cache_link = NULL;
  int64_t alloc_sz = sizeof(*h) + sz;
  if (alloc_sz >= MEM_CACHE_LOWER_BOUND && alloc_sz <= MEM_CACHE_UPPER_BOUND) {
    alloc_sz = upper_align_interval(alloc_sz);
    cache_link = link_queue_pop(locate_freelist(alloc_sz));
  }
  if (cache_link) {
    h = structof(cache_link, alloc_head_t, link);
  } else {
    h = (typeof(h))PKT_NIO_MALLOC(alloc_sz, label);
  }
  if (h) {
    h->mod = mod;
    h->sz = alloc_sz;
    h->access_time_us = 0;
    h->link.next = NULL;
    mod_update(mod, h->sz);
    h = h + 1;
  }
  return (void*)h;
}

void mod_free(void* p) {
  if (p) {
    alloc_head_t* h = (typeof(h))p - 1;
    mod_update(h->mod, -h->sz);
    if (h->sz >= MEM_CACHE_LOWER_BOUND && h->sz <= MEM_CACHE_UPPER_BOUND) {
      link_queue_t* free_list = locate_freelist(h->sz);
      int64_t queue_cnt = LOAD(&free_list->cnt);
      if (queue_cnt < MAX_CACHED_CNT) {
        h->access_time_us = rk_get_corse_us();
        link_queue_push(free_list, &h->link);
      } else {
        PKT_NIO_FREE(h);
      }
    } else {
      PKT_NIO_FREE(h);
    }
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
