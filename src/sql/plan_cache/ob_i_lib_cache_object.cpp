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

#define USING_LOG_PREFIX SQL_PC
#include "sql/plan_cache/ob_i_lib_cache_object.h"
#include "sql/plan_cache/ob_plan_cache.h"

using namespace oceanbase::common;
using namespace oceanbase::share::schema;

namespace oceanbase
{
namespace sql
{
ObILibCacheObject::ObILibCacheObject(ObLibCacheNameSpace ns, lib::MemoryContext &mem_context)
  : mem_context_(mem_context),
    allocator_(mem_context->get_safe_arena_allocator()),
    ref_count_(0),
    object_id_(OB_INVALID_ID),
    log_del_time_(INT64_MAX),
    added_to_lc_(false),
    ns_(ns),
    tenant_id_(OB_INVALID_ID),
    dynamic_ref_handle_(MAX_HANDLE),
    obj_status_(ObILibCacheObject::ACTIVE)
{
}

void ObILibCacheObject::reset()
{
  ref_count_ = 0;
  object_id_ = OB_INVALID_ID;
  log_del_time_ = INT64_MAX;
  added_to_lc_ = false;
  ns_ = NS_INVALID;
  tenant_id_ = OB_INVALID_ID;
  dynamic_ref_handle_ = MAX_HANDLE;
  obj_status_ = ObILibCacheObject::ACTIVE;
}

void ObILibCacheObject::dump_deleted_log_info(const bool is_debug_log /* = true */) const
{
  if (is_debug_log) {
    SQL_PC_LOG_RET(WARN, OB_SUCCESS, "Dumping Cache Deleted Info",
               K(object_id_),
               K(tenant_id_),
               K(added_to_lc_),
               K(ns_),
               K(get_ref_count()),
               K(log_del_time_),
               K(this));
  } else {
    SQL_PC_LOG(INFO, "Dumping Cache Deleted Info",
               K(object_id_),
               K(tenant_id_),
               K(added_to_lc_),
               K(ns_),
               K(get_ref_count()),
               K(log_del_time_),
               K(this));
  }
}

int ObILibCacheObject::before_cache_evicted()
{
  int ret = OB_SUCCESS;
  LOG_INFO("before_cache_evicted", K(this), KPC(this));
  return ret;
}

int ObILibCacheObject::check_need_add_cache_obj_stat(ObILibCacheCtx &ctx, bool &need_real_add)
{
  UNUSED(ctx);
  int ret = OB_SUCCESS;
  need_real_add = true;
  return ret;
}

int ObILibCacheObject::update_cache_obj_stat(ObILibCacheCtx &ctx)
{
  UNUSED(ctx);
  int ret = OB_SUCCESS;
  return ret;
}

int64_t ObILibCacheObject::inc_ref_count(const CacheRefHandleID ref_handle)
{
  int ret = OB_SUCCESS;
  if (GCONF._enable_plan_cache_mem_diagnosis) {
    ObPlanCache *lib_cache = MTL(ObPlanCache*);
    if (OB_ISNULL(lib_cache)) {
      // ignore ret
      LOG_ERROR("invalid null lib cache", K(ret));
    } else {
      lib_cache->get_ref_handle_mgr().record_ref_op(ref_handle);
    }
  }
  return ATOMIC_AAF(&ref_count_, 1);
}

int64_t ObILibCacheObject::dec_ref_count(const CacheRefHandleID ref_handle)
{
  int ret = OB_SUCCESS;
  if (GCONF._enable_plan_cache_mem_diagnosis) {
    ObPlanCache *lib_cache = MTL(ObPlanCache*);
    if (OB_ISNULL(lib_cache)) {
      // ignore ret
      LOG_ERROR("invalid null lib cache", K(ret));
    } else {
      lib_cache->get_ref_handle_mgr().record_deref_op(ref_handle);
    }
  }
  return ATOMIC_SAF(&ref_count_, 1);
}

} // namespace common
} // namespace oceanbase
