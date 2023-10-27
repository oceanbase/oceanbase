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

typedef struct chunk_cache_t
{
  int mod;
  int chunk_bytes;
  fixed_stack_t free_list;
} chunk_cache_t;

extern void chunk_cache_init(chunk_cache_t* cache, int chunk_bytes, int mod);
extern void* chunk_cache_alloc(chunk_cache_t* cache, int64_t sz, int* chunk_size);
extern void chunk_cache_free(void* p);
