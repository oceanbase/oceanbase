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

#define USING_LOG_PREFIX COMMON
#include "ob_sql_arena_allocator.h"
#include <stdlib.h>
#include "lib/ob_define.h"
#include "lib/oblog/ob_log.h"

namespace oceanbase {
namespace common {
ObSQLArenaAllocator::ObSQLArenaAllocator(const int64_t tenant_id)
    : normal_(ObModIds::OB_SQL_ARENA, OB_MALLOC_BIG_BLOCK_SIZE, tenant_id, ObCtxIds::DEFAULT_CTX_ID),
      large_(ObModIds::OB_SQL_ARENA, OB_MALLOC_BIG_BLOCK_SIZE, tenant_id, ObCtxIds::WORK_AREA),
      threshold_size_total_(DEFAULT_THRESHOLD_SIZE_TOTAL)
{}

ObSQLArenaAllocator::~ObSQLArenaAllocator()
{}

void ObSQLArenaAllocator::set_tenant_id(const uint64_t tenant_id)
{
  normal_.set_tenant_id(tenant_id);
  large_.set_tenant_id(tenant_id);
}

void ObSQLArenaAllocator::set_threshold_size_total(const int threshold_size_total)
{
  threshold_size_total_ = threshold_size_total;
}

void* ObSQLArenaAllocator::alloc(const int64_t size)
{
  void* ptr = nullptr;
  if (size < THRESHOLD_SIZE_ONCE && total() < threshold_size_total_) {
    ptr = normal_.alloc(size);
  } else {
    ptr = large_.alloc(size);
  }
  return ptr;
}

void* ObSQLArenaAllocator::alloc(const int64_t size, const ObMemAttr& attr)
{
  UNUSED(attr);
  return alloc(size);
}

void ObSQLArenaAllocator::free(void* ptr)
{
  UNUSED(ptr);
}

void ObSQLArenaAllocator::reset()
{
  normal_.reset();
  large_.reset();
}

int64_t ObSQLArenaAllocator::total() const
{
  return normal_.total() + large_.total();
}

bool ObSQLArenaAllocator::set_tracer()
{
  return normal_.set_tracer() && large_.set_tracer();
}

bool ObSQLArenaAllocator::revert_tracer()
{
  return normal_.revert_tracer() && large_.revert_tracer();
}

void ObSQLArenaAllocator::reset_remain_one_page()
{
  normal_.reset_remain_one_page();
  large_.reset();
}

};  // end namespace common
};  // end namespace oceanbase
