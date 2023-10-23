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

#ifndef SRC_COMMON_META_PROGRAMMING_OB_META_DEFINE_H
#define SRC_COMMON_META_PROGRAMMING_OB_META_DEFINE_H
#include "lib/allocator/ob_allocator.h"
namespace oceanbase
{
namespace common
{
namespace meta
{

struct DummyAllocator : public ObIAllocator
{
virtual void *alloc(const int64_t) override { return nullptr; }
virtual void* alloc(const int64_t, const ObMemAttr &) { return nullptr; }
virtual void free(void *ptr) override {}
static DummyAllocator &get_instance() { static DummyAllocator alloc; return alloc; }
};

}
}
}
#endif