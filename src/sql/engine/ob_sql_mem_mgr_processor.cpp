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

#define USING_LOG_PREFIX SQL_ENG

#include "ob_sql_mem_mgr_processor.h"
#include "observer/omt/ob_tenant_config_mgr.h"

namespace oceanbase {

using namespace omt;

namespace sql {

int ObSqlMemMgrProcessor::init(ObIAllocator* allocator, uint64_t tenant_id, int64_t cache_size,
    const ObPhyOperatorType op_type, const uint64_t op_id, ObExecContext* exec_ctx)
{
  int ret = OB_SUCCESS;
  bool tmp_enable_auto_mem_mgr = false;
  ObTenantSqlMemoryManager* sql_mem_mgr = get_sql_mem_mgr();
  is_auto_mgr_ = false;
  reset();
  tenant_id_ = tenant_id;
  profile_.set_operator_type(op_type);
  profile_.set_operator_id(op_id);
  profile_.set_exec_ctx(exec_ctx);
  if (OB_FAIL(alloc_dir_id(dir_id_))) {
  } else if (OB_NOT_NULL(sql_mem_mgr)) {
    if (sql_mem_mgr->enable_auto_memory_mgr()) {
      tmp_enable_auto_mem_mgr = true;
      if (profile_.get_auto_policy()) {
        // update
        int64_t pre_size = profile_.get_cache_size();
        if (cache_size != pre_size &&
            OB_FAIL(sql_mem_mgr_->update_work_area_profile(allocator, profile_, cache_size - pre_size))) {
          LOG_WARN("failed update work area profile", K(ret), K(cache_size));
        } else {
          profile_.init(cache_size, OB_MALLOC_MIDDLE_BLOCK_SIZE);
          LOG_TRACE(
              "trace update cache size", K(profile_.get_cache_size()), K(profile_.get_expect_size()), K(cache_size));
        }
      } else {
        // first time to register
        profile_.init(cache_size, OB_MALLOC_MIDDLE_BLOCK_SIZE);
        LOG_TRACE("trace register work area profile", K(profile_.get_cache_size()));
      }
      profile_.set_expect_size(OB_INVALID_ID);
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(sql_mem_mgr_->register_work_area_profile(profile_))) {
        LOG_WARN("failed to register workarea profile", K(ret));
      } else if (OB_FAIL(sql_mem_mgr_->get_work_area_size(allocator, profile_))) {
        LOG_WARN("failed to get available mem size", K(ret));
      } else {
        is_auto_mgr_ = profile_.get_auto_policy();
        LOG_TRACE("trace enable sql memory manager",
            K(ret),
            K(profile_.get_cache_size()),
            K(profile_.get_expect_size()),
            K(is_auto_mgr_),
            K(profile_.is_registered()));
      }
    } else {
      profile_.init(cache_size, OB_MALLOC_MIDDLE_BLOCK_SIZE);
      profile_.set_expect_size(OB_INVALID_ID);
      if (OB_FAIL(sql_mem_mgr_->register_work_area_profile(profile_))) {
        LOG_WARN("failed to register workarea profile", K(ret));
      }
      LOG_TRACE("trace only register sql memory manager",
          K(ret),
          K(profile_.get_cache_size()),
          K(profile_.get_expect_size()),
          K(is_auto_mgr_));
    }
  } else {
    profile_.set_expect_size(OB_INVALID_ID);
  }
  int64_t max_mem_size = MAX_SQL_MEM_SIZE;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObSqlWorkareaUtil::get_workarea_size(profile_.get_work_area_type(), tenant_id_, max_mem_size))) {
    LOG_WARN("failed to get workarea size", K(ret), K(tenant_id_), K(max_mem_size));
  }
  if (!profile_.get_auto_policy()) {
    profile_.set_max_bound(max_mem_size);
  }
  // don't register small runners
  origin_max_mem_size_ = max_mem_size;
  default_available_mem_size_ = tmp_enable_auto_mem_mgr ? MAX_SQL_MEM_SIZE : max_mem_size;
  return ret;
}

int ObSqlMemMgrProcessor::get_max_available_mem_size(ObIAllocator* allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_auto_mgr())) {
  } else if (OB_NOT_NULL(sql_mem_mgr_)) {
    if (OB_FAIL(sql_mem_mgr_->get_work_area_size(allocator, profile_))) {
      LOG_WARN("failed to get work area size", K(ret));
    } else {
      LOG_TRACE("trace get max available mem size", K(profile_.get_cache_size()), K(profile_.get_expect_size()));
    }
  }
  return ret;
}

int ObSqlMemMgrProcessor::update_max_available_mem_size_periodically(
    ObIAllocator* allocator, PredFunc predicate, bool& updated)
{
  int ret = OB_SUCCESS;
  updated = false;
  if (predicate(periodic_cnt_)) {
    if (OB_FAIL(get_max_available_mem_size(allocator))) {
      LOG_WARN("failed to get available memory size", K(ret));
    } else {
      updated = true;
      periodic_cnt_ <<= 1;
      LOG_TRACE("trace get max available mem size periodically",
          K(periodic_cnt_),
          K(profile_.get_cache_size()),
          K(profile_.get_expect_size()));
    }
  }
  return ret;
}

int ObSqlMemMgrProcessor::update_cache_size(ObIAllocator* allocator, int64_t cache_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_auto_mgr())) {
  } else if (OB_NOT_NULL(sql_mem_mgr_)) {
    int64_t pre_size = profile_.get_cache_size();
    if (!profile_.get_auto_policy()) {
      LOG_TRACE("unexpected status: profile need register", K(ret));
    } else if (cache_size != pre_size) {
      profile_.init(cache_size, OB_MALLOC_MIDDLE_BLOCK_SIZE);
      if (OB_FAIL(sql_mem_mgr_->update_work_area_profile(allocator, profile_, cache_size - pre_size))) {
        LOG_WARN("failed update work area profile", K(ret), K(cache_size));
      } else {
        LOG_TRACE(
            "trace update cache size", K(profile_.get_cache_size()), K(profile_.get_expect_size()), K(cache_size));
      }
    }
  }
  return ret;
}

int ObSqlMemMgrProcessor::update_used_mem_size(int64_t used_size)
{
  int ret = OB_SUCCESS;
  int64_t delta_size = used_size - profile_.mem_used_;
  if (delta_size > 0) {
    if (OB_NOT_NULL(sql_mem_mgr_) && OB_NOT_NULL(mem_callback_)) {
      mem_callback_->alloc(delta_size);
    }
  } else {
    if (OB_NOT_NULL(sql_mem_mgr_) && OB_NOT_NULL(mem_callback_)) {
      mem_callback_->free(-delta_size);
    }
  }
  profile_.mem_used_ = used_size;
  if (profile_.max_mem_used_ < profile_.mem_used_) {
    profile_.max_mem_used_ = used_size;
  }
  profile_.data_size_ += profile_.delta_size_;
  profile_.pre_mem_used_ = profile_.mem_used_;
  reset_delta_size();
  return ret;
}

int ObSqlMemMgrProcessor::try_upgrade_auto_mgr(ObIAllocator* allocator, int64_t mem_used)
{
  int ret = OB_SUCCESS;
  if (!is_auto_mgr() && OB_NOT_NULL(sql_mem_mgr_)) {
    int64_t max_area_size = max(default_available_mem_size_, MAX_SQL_MEM_SIZE);
    if (sql_mem_mgr_->enable_auto_memory_mgr() && sql_mem_mgr_->get_global_bound_size() < max_area_size) {
    } else if (OB_FAIL(update_used_mem_size(0))) {
      LOG_WARN("failed to update used mem size", K(ret));
    } else if (OB_FAIL(init(allocator,
                   tenant_id_,
                   max_area_size * (EXTEND_RATIO + 100) / 100,
                   profile_.get_operator_type(),
                   profile_.get_operator_id(),
                   profile_.get_exec_ctx()))) {
      LOG_WARN("failed to upgrade sql memory manager", K(ret));
    } else if (is_auto_mgr()) {
      if (OB_FAIL(update_used_mem_size(mem_used))) {
        LOG_WARN("failed to update used mem_size", K(ret));
      }
      LOG_TRACE("trace upgrade auto manager", K(profile_), K(mem_used), K(max_area_size));
    } else if (OB_FAIL(update_used_mem_size(mem_used))) {
      LOG_WARN("failed to update used mem_size", K(ret));
    }
  }
  return ret;
}

// try to extend max memory size to avoid dump or require more memory when need dump
int ObSqlMemMgrProcessor::extend_max_memory_size(
    ObIAllocator* allocator, PredFunc dump_fun, bool& need_dump, int64_t mem_used, int64_t max_times)
{
  int ret = OB_SUCCESS;
  need_dump = true;
  if (OB_FAIL(try_upgrade_auto_mgr(allocator, mem_used))) {
    LOG_WARN("failed to try udgrade auto manager", K(ret));
  } else if (OB_UNLIKELY(!is_auto_mgr())) {
    /* do nothing */
  } else if (OB_NOT_NULL(sql_mem_mgr_)) {
    if (OB_FAIL(get_max_available_mem_size(allocator))) {
      LOG_WARN("failed to get max available memory size", K(ret));
    }
    int64_t times = 0;
    while ((need_dump = dump_fun(profile_.get_expect_size())) && OB_SUCC(ret)) {
      int64_t pre_cache_size = profile_.get_cache_size();
      int64_t pre_expect_size = profile_.get_expect_size();
      if (pre_cache_size > pre_expect_size) {
        if (OB_FAIL(get_max_available_mem_size(allocator))) {
          LOG_WARN("failed to get max available memory size", K(pre_cache_size), K(pre_expect_size), K(ret));
        } else if (profile_.get_cache_size() > profile_.get_expect_size()) {
          LOG_TRACE("trace extend max memory size", K(pre_cache_size), K(pre_expect_size));
          break;
        }
      } else {
        int64_t new_cache_size = profile_.get_cache_size() * (EXTEND_RATIO + 100) / 100;
        if (OB_FAIL(update_cache_size(allocator, new_cache_size))) {
          LOG_WARN("failed to upadte cache size", K(ret), K(new_cache_size));
        } else if (OB_FAIL(get_max_available_mem_size(allocator))) {
          LOG_WARN("failed to get max available memory size",
              K(new_cache_size),
              K(pre_cache_size),
              K(pre_expect_size),
              K(ret));
        } else if (profile_.get_cache_size() > profile_.get_expect_size()) {
          if (OB_FAIL(update_cache_size(allocator, pre_cache_size))) {
            LOG_WARN("failed to get max memory size", K(ret), K(pre_cache_size));
          }
          LOG_TRACE("trace extend max memory size",
              K(pre_cache_size),
              K(pre_expect_size),
              K(profile_.get_cache_size()),
              K(profile_.get_expect_size()));
          break;
        }
      }
      ++times;
      if (0 == times % 16) {
        LOG_INFO(
            "extend max memory size", K(ret), K(times), K(profile_.get_expect_size()), K(profile_.get_cache_size()));
      }
      if (max_times <= times) {
        LOG_TRACE("extend memory size too more times", K(times));
        break;
      }
    }
    LOG_TRACE("extend memory size", K(profile_.get_expect_size()), K(profile_.get_cache_size()));
  }
  return ret;
}

void ObSqlMemMgrProcessor::unregister_profile()
{
  if (OB_NOT_NULL(sql_mem_mgr_)) {
    sql_mem_mgr_->unregister_work_area_profile(profile_);
    destroy();
    LOG_TRACE("trace unregister work area profile", K(profile_));
  }
}

int ObSqlMemMgrProcessor::alloc_dir_id(int64_t& dir_id)
{
  int ret = OB_SUCCESS;
  dir_id = 0;
  if (0 == dir_id_) {
    if (OB_FAIL(ObChunkStoreUtil::alloc_dir_id(dir_id_))) {
      LOG_WARN("failed to alloc dir id", K(ret));
    }
  }
  dir_id = dir_id_;
  return ret;
}

int ObSqlWorkareaUtil::get_workarea_size(const ObSqlWorkAreaType wa_type, const int64_t tenant_id, int64_t& value)
{
  int ret = OB_SUCCESS;
  ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  if (tenant_config.is_valid()) {
    if (HASH_WORK_AREA == wa_type) {
      value = tenant_config->_hash_area_size;
    } else if (SORT_WORK_AREA == wa_type) {
      value = tenant_config->_sort_area_size;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: workarea type", K(wa_type), K(tenant_id));
    }
    LOG_DEBUG("debug workarea size", K(value), K(tenant_id), K(lbt()));
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to init tenant config", K(tenant_id), K(ret));
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
