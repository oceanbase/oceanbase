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

#include "cos_fstack.h"

cos_array_header_t *cos_fstack_create(cos_pool_t *p, int size)
{
    return apr_array_make(p, size, sizeof(cos_fstack_item_t));
}

void cos_fstack_push(cos_array_header_t *fstack, void *data, cos_func_u func, int order)
{
    cos_fstack_item_t *item;

    item = (cos_fstack_item_t*)apr_array_push(fstack);
    item->data = data;
    item->func = func;
    item->order = order;
}

cos_fstack_item_t *cos_fstack_pop(cos_array_header_t *fstack)
{
    cos_fstack_item_t *item;

    item = (cos_fstack_item_t*)apr_array_pop(fstack);
    if (item == NULL) {
        return NULL;
    }

    switch (item->order) {
        case 1:
            item->func.func1(item->data);
            break;
        case 2:
            item->func.func2();
            break;
        case 3:
            item->func.func3(item->data);
            break;
        case 4:
            item->func.func4();
            break;
        default:
            break;
    }

    return item;
}

void cos_fstack_destory(cos_array_header_t *fstack)
{
    while (cos_fstack_pop(fstack) != NULL);
}
