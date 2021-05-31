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

#ifndef EASY_ARRAY_H_
#define EASY_ARRAY_H_

#include "util/easy_pool.h"
#include "easy_list.h"

EASY_CPP_START

typedef struct easy_array_t {
  easy_pool_t* pool;
  easy_list_t list;
  int object_size;
  int count;
} easy_array_t;

easy_array_t* easy_array_create(int object_size);
void easy_array_destroy(easy_array_t* array);
void* easy_array_alloc(easy_array_t* array);
void easy_array_free(easy_array_t* array, void* ptr);

EASY_CPP_END

#endif
