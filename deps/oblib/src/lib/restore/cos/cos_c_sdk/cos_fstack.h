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

#ifndef LIBCOS_FSTACK_H
#define LIBCOS_FSTACK_H

#include "cos_sys_define.h"


COS_CPP_START

typedef void (*cos_func1_pt)(void*);
typedef void (*cos_func2_pt)();
typedef int (*cos_func3_pt)(void*);
typedef int (*cos_func4_pt)();

typedef union cos_func_u {
    cos_func1_pt func1;
    cos_func2_pt func2;
    cos_func3_pt func3;
    cos_func4_pt func4;
} cos_func_u;

typedef struct cos_fstack_item_t {
    void *data;
    cos_func_u func;
    int order;
} cos_fstack_item_t;

cos_array_header_t *cos_fstack_create(cos_pool_t *p, int size);

cos_fstack_item_t *cos_fstack_pop(cos_array_header_t *fstack);

void cos_fstack_destory(cos_array_header_t *fstack);

void cos_fstack_push(cos_array_header_t *fstack, void *data, cos_func_u func, int order);

COS_CPP_END

#endif
