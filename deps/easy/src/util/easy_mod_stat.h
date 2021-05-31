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

#ifndef __EASY_MOD_STAT__
#define __EASY_MOD_STAT__

#include <stdint.h>
#include <sys/types.h>
#include "easy_define.h"
EASY_CPP_START

typedef struct mod_stat_t {
  uint64_t id;
  int64_t count;
  int64_t size;
} mod_stat_t;

extern __thread mod_stat_t* easy_cur_mod_stat;
extern mod_stat_t* easy_fetch_mod_stat(uint64_t id);
extern void* (*realloc_lowlevel)(void*, size_t);
extern void* realloc_with_mod_stat(void* ptr, size_t size);
typedef mod_stat_t easy_mod_stat_t;

EASY_CPP_END
#endif /* __EASY_MOD_STAT__ */
