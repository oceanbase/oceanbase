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

#include "lib/alloc/alloc_func.h"
#include "lib/resource/achunk_mgr.h"
#include "lib/alloc/ob_malloc_allocator.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::lib;

namespace oceanbase
{
namespace lib
{

void set_memory_limit(int64_t bytes)
{
  CHUNK_MGR.set_limit(bytes);
}

int64_t get_memory_limit()
{
  return CHUNK_MGR.get_limit();
}

int64_t get_memory_hold()
{
  return CHUNK_MGR.get_hold();
}

int64_t get_memory_used()
{
  return CHUNK_MGR.get_used();
}

int64_t get_memory_avail()
{
  return get_memory_limit() - get_memory_used();
}

void set_tenant_memory_limit(uint64_t tenant_id, int64_t bytes)
{
  ObMallocAllocator *allocator = ObMallocAllocator::get_instance();
  if (!OB_ISNULL(allocator)) {
    allocator->set_tenant_limit(tenant_id, bytes);
  }
}

int64_t get_tenant_memory_limit(uint64_t tenant_id)
{
  int64_t bytes = 0;
  ObMallocAllocator *allocator = ObMallocAllocator::get_instance();
  if (!OB_ISNULL(allocator)) {
    bytes = allocator->get_tenant_limit(tenant_id);
  }
  return bytes;
}

int64_t get_tenant_memory_hold(uint64_t tenant_id)
{
  int64_t bytes = 0;
  ObMallocAllocator *allocator = ObMallocAllocator::get_instance();
  if (!OB_ISNULL(allocator)) {
    bytes = allocator->get_tenant_hold(tenant_id);
  }
  return bytes;
}

int64_t get_tenant_memory_hold(const uint64_t tenant_id, const uint64_t ctx_id)
{
  int64_t bytes = 0;
  ObMallocAllocator *allocator = ObMallocAllocator::get_instance();
  if (!OB_ISNULL(allocator)) {
    bytes = allocator->get_tenant_ctx_hold(tenant_id, ctx_id);
  }
  return bytes;
}

int64_t get_tenant_cache_hold(uint64_t tenant_id)
{
  int64_t bytes = 0;
  ObMallocAllocator *allocator = ObMallocAllocator::get_instance();
  if (!OB_ISNULL(allocator)) {
    bytes = allocator->get_tenant_cache_hold(tenant_id);
  }
  return bytes;
}

int64_t get_tenant_memory_remain(uint64_t tenant_id)
{
  int64_t bytes = 0;
  ObMallocAllocator *allocator = ObMallocAllocator::get_instance();
  if (!OB_ISNULL(allocator)) {
    bytes = allocator->get_tenant_remain(tenant_id);
  }
  return bytes;
}

void get_tenant_label_memory(
  uint64_t tenant_id, ObLabel &label,
  common::ObLabelItem &item)
{
  ObMallocAllocator *allocator = ObMallocAllocator::get_instance();
  if (!OB_ISNULL(allocator)) {
    allocator->get_tenant_label_usage(tenant_id, label, item);
  }
}

void ob_set_reserved_memory(const int64_t bytes)
{
  ObMallocAllocator *allocator = ObMallocAllocator::get_instance();
  if (!OB_ISNULL(allocator)) {
    allocator->set_reserved(bytes);
  }
}

void ob_set_urgent_memory(const int64_t bytes)
{
  ObMallocAllocator *allocator = ObMallocAllocator::get_instance();
  if (!OB_ISNULL(allocator)) {
    allocator->set_urgent(bytes);
  }
}

int64_t ob_get_reserved_urgent_memory()
{
  int64_t bytes = 0;
  ObMallocAllocator *allocator = ObMallocAllocator::get_instance();
  if (!OB_ISNULL(allocator)) {
    bytes += allocator->get_urgent();
    bytes += allocator->get_reserved();
  }
  return bytes;
}

int set_ctx_limit(uint64_t tenant_id, uint64_t ctx_id, const int64_t limit)
{
  int ret = OB_SUCCESS;
  ObMallocAllocator *alloc = ObMallocAllocator::get_instance();
  if (!OB_ISNULL(alloc)) {
    auto ta = alloc->get_tenant_ctx_allocator(tenant_id, ctx_id);
    if (OB_NOT_NULL(ta)) {
      if (OB_FAIL(ta->set_limit(limit))) {
        LIB_LOG(WARN, "set_limit failed", K(ret), K(limit));
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
    }
  } else {
    ret = OB_NOT_INIT;
  }
  return ret;
}

int set_wa_limit(uint64_t tenant_id, int64_t wa_pctg)
{
  const int64_t tenant_limit = get_tenant_memory_limit(tenant_id);
  // For small tenants, work_area may only have dozens of M, which is unavailable. Give work_area a lower limit
  const int64_t lower_limit = 150L << 20;
  const int64_t wa_limit =
    std::min(static_cast<int64_t>(tenant_limit*0.8),
             std::max(lower_limit, (tenant_limit/100) * wa_pctg));
  return set_ctx_limit(tenant_id, common::ObCtxIds::WORK_AREA, wa_limit);
}

int set_meta_obj_limit(uint64_t tenant_id, int64_t meta_obj_pct_lmt)
{
  const int64_t tenant_limit = get_tenant_memory_limit(tenant_id);
  const int64_t ctx_limit = 0 == meta_obj_pct_lmt ? tenant_limit : (tenant_limit / 100) * meta_obj_pct_lmt;

  return set_ctx_limit(tenant_id, common::ObCtxIds::META_OBJ_CTX_ID, ctx_limit);
}

int set_rpc_limit(uint64_t tenant_id, int64_t rpc_pct_lmt)
{
  const int64_t tenant_limit = get_tenant_memory_limit(tenant_id);
  const int64_t rpc_lmt = (tenant_limit / 100) * rpc_pct_lmt;
  return set_ctx_limit(tenant_id, common::ObCtxIds::RPC_CTX_ID, rpc_lmt);
}

} // end of namespace lib
} // end of namespace oceanbase
