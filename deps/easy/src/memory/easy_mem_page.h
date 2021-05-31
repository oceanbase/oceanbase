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

#ifndef EASY_MEM_PAGE_H_
#define EASY_MEM_PAGE_H_

#include "easy_define.h"
#include "easy_list.h"
#include "easy_atomic.h"

EASY_CPP_START

#define EASY_MEM_PAGE_SHIFT 16
#define EASY_MEM_PAGE_SIZE (1 << EASY_MEM_PAGE_SHIFT)  // 64K
#define EASY_MEM_MAX_ORDER 12                          // max page size: 128M

typedef struct easy_mem_page_t easy_mem_page_t;
typedef struct easy_mem_area_t easy_mem_area_t;
typedef struct easy_mem_zone_t easy_mem_zone_t;

struct easy_mem_page_t {
  easy_list_t lru;
};

struct easy_mem_area_t {
  easy_list_t free_list;
  int nr_free;
};

struct easy_mem_zone_t {
  unsigned char *mem_start, *mem_last, *mem_end;
  easy_mem_area_t area[EASY_MEM_MAX_ORDER];
  uint32_t max_order;
  int free_pages;
  unsigned char *curr, *curr_end;
  unsigned char page_flags[0];
};

easy_mem_zone_t* easy_mem_zone_create(int64_t max_size);
void easy_mem_zone_destroy(easy_mem_zone_t* zone);
easy_mem_page_t* easy_mem_alloc_pages(easy_mem_zone_t* zone, uint32_t order);
void easy_mem_free_pages(easy_mem_zone_t* zone, easy_mem_page_t* page);

EASY_CPP_END

#endif
