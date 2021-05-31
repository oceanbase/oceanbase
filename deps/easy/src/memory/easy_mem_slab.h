/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef EASY_MEM_SLAB_H_
#define EASY_MEM_SLAB_H_

#include "easy_define.h"
#include "easy_list.h"
#include "easy_atomic.h"
#include "memory/easy_mem_page.h"

EASY_CPP_START

#define EASY_MEM_SLAB_MIN 512
typedef struct easy_mem_slab_t easy_mem_slab_t;
typedef struct easy_mem_cache_t easy_mem_cache_t;
typedef struct easy_mem_mgr_t easy_mem_mgr_t;
typedef struct easy_mem_sizes_t easy_mem_sizes_t;

struct easy_mem_slab_t {
  easy_list_t list;
  unsigned char* mem;
  uint16_t inuse;
  uint16_t free;
  uint16_t cache_idx;
  uint16_t next_pos[0];
};

struct easy_mem_cache_t {
  easy_list_t slabs_partial;
  easy_list_t slabs_full;
  easy_list_t slabs_free;
  easy_list_t next;

  uint32_t order;
  uint32_t buffer_size;
  uint32_t num;
  uint32_t offset;
  uint32_t free_objects;
  uint32_t free_limit;
  int idx;
  easy_atomic_t lock;
};

struct easy_mem_mgr_t {
  int started;
  int cache_max_num;
  int cache_fix_num;
  int cache_num;
  int64_t max_size;

  easy_list_t list;
  easy_atomic_t lock;
  easy_mem_cache_t* caches;
  easy_mem_zone_t* zone;
};

int easy_mem_slab_init(int start_size, int64_t max_size);
void easy_mem_slab_destroy();
void* easy_mem_slab_realloc(void* ptr, size_t size);
void* easy_mem_cache_alloc(easy_mem_cache_t* cache);
void easy_mem_cache_free(easy_mem_cache_t* cache, void* obj);
easy_mem_cache_t* easy_mem_cache_create(int buffer_size);

EASY_CPP_END

#endif
