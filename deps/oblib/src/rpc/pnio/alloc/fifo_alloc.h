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

typedef struct fifo_alloc_t
{
  chunk_cache_t* chunk_alloc;
  void* cur;
} fifo_alloc_t;
extern void fifo_alloc_init(fifo_alloc_t* alloc, chunk_cache_t* chunk_alloc);
extern void* fifo_alloc(fifo_alloc_t* alloc, int sz);
extern void fifo_free(void* p);
