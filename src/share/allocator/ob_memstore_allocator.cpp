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

#include "ob_shared_memory_allocator_mgr.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/memtable/ob_memtable.h"
#include "lib/utility/ob_print_utils.h"
#include "observer/omt/ob_multi_tenant.h"
#include "observer/ob_server_struct.h"
#include "share/ob_tenant_mgr.h"
#include "storage/tx_storage/ob_tenant_freezer.h"

namespace oceanbase
{
using namespace share;
namespace share
{
int FrozenMemstoreInfoLogger::operator()(ObDLink* link)
{
  int ret = OB_SUCCESS;
  ObMemstoreAllocator::AllocHandle* handle = CONTAINER_OF(link, typeof(*handle), total_list_);
  memtable::ObMemtable& mt = handle->mt_;
  if (handle->is_frozen()) {
    if (OB_FAIL(databuff_print_obj(buf_, limit_, pos_, mt))) {
    } else {
      ret = databuff_printf(buf_, limit_, pos_, ",");
    }
  }
  return ret;
}

int ActiveMemstoreInfoLogger::operator()(ObDLink* link)
{
  int ret = OB_SUCCESS;
  ObMemstoreAllocator::AllocHandle* handle = CONTAINER_OF(link, typeof(*handle), total_list_);
  memtable::ObMemtable& mt = handle->mt_;
  if (handle->is_active()) {
    if (OB_FAIL(databuff_print_obj(buf_, limit_, pos_, mt))) {
    } else {
      ret = databuff_printf(buf_, limit_, pos_, ",");
    }
  }
  return ret;
}

int ObMemstoreAllocator::AllocHandle::init()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  ObMemstoreAllocator &host = MTL(ObSharedMemAllocMgr *)->memstore_allocator();
  (void)host.init_handle(*this);
  return ret;
}

int ObMemstoreAllocator::init()
{
  throttle_tool_ = &(MTL(ObSharedMemAllocMgr *)->share_resource_throttle_tool());
  return arena_.init();
}

void ObMemstoreAllocator::init_handle(AllocHandle& handle)
{
  handle.do_reset();
  handle.set_host(this);
  {
    int64_t nway = nway_per_group();
    LockGuard guard(lock_);
    hlist_.init_handle(handle);
    arena_.update_nway_per_group(nway);
  }
  COMMON_LOG(TRACE, "MTALLOC.init", KP(&handle.mt_));
}

void ObMemstoreAllocator::destroy_handle(AllocHandle& handle)
{
  ObTimeGuard time_guard("ObMemstoreAllocator::destroy_handle", 100 * 1000);
  COMMON_LOG(TRACE, "MTALLOC.destroy", KP(&handle.mt_));
  arena_.free(handle.arena_handle_);
  time_guard.click();
  {
    LockGuard guard(lock_);
    time_guard.click();
    hlist_.destroy_handle(handle);
    time_guard.click();
    if (hlist_.is_empty()) {
      arena_.reset();
    }
    time_guard.click();
  }
  handle.do_reset();
}

void* ObMemstoreAllocator::alloc(AllocHandle& handle, int64_t size, const int64_t expire_ts)
{
  int ret = OB_SUCCESS;
  int64_t align_size = upper_align(size, sizeof(int64_t));
  uint64_t tenant_id = arena_.get_tenant_id();

  bool is_out_of_mem = false;
  if (!handle.is_id_valid()) {
    COMMON_LOG(TRACE, "MTALLOC.first_alloc", KP(&handle.mt_));
    LockGuard guard(lock_);
    if (handle.is_frozen()) {
      ret = OB_EAGAIN;
      if (!handle.mt_.get_offlined()) {
        COMMON_LOG(ERROR, "cannot alloc because allocator is frozen", K(ret), K(handle.mt_));
      } else {
        COMMON_LOG(WARN, "cannot alloc because allocator is frozen", K(ret), K(handle.mt_));
      }
    } else if (!handle.is_id_valid()) {
      handle.set_clock(arena_.retired());
      hlist_.set_active(handle);
    }
  }

  if (OB_SUCC(ret)) {
    storage::ObTenantFreezer *freezer = nullptr;
    if (is_virtual_tenant_id(tenant_id)) {
      // virtual tenant should not have memstore.
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "virtual tenant should not have memstore", K(ret), K(tenant_id));
    } else if (FALSE_IT(freezer = MTL(storage::ObTenantFreezer *))) {
    } else if (OB_FAIL(freezer->check_memstore_full_internal(is_out_of_mem))) {
      COMMON_LOG(ERROR, "fail to check tenant out of mem limit", K(ret), K(tenant_id));
    }
  }

  void *res = nullptr;
  if (OB_FAIL(ret) || is_out_of_mem) {
    if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      STORAGE_LOG(WARN, "this tenant is already out of memstore limit or some thing wrong.", K(tenant_id));
    }
    res = nullptr;
  } else {
    bool is_throttled = false;
    (void)throttle_tool_->alloc_resource<ObMemstoreAllocator>(align_size, expire_ts, is_throttled);
    if (is_throttled) {
      share::memstore_throttled_alloc() += align_size;
    }
    res = arena_.alloc(handle.id_, handle.arena_handle_, align_size);
  }
  return res;
}

void ObMemstoreAllocator::set_frozen(AllocHandle& handle)
{
  COMMON_LOG(TRACE, "MTALLOC.set_frozen", KP(&handle.mt_));
  LockGuard guard(lock_);
  hlist_.set_frozen(handle);
}

static int64_t calc_nway(int64_t cpu, int64_t mem)
{
  return std::min(cpu, mem/20/ObFifoArena::PAGE_SIZE);
}

int64_t ObMemstoreAllocator::nway_per_group()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = arena_.get_tenant_id();
  double min_cpu = 0;
  double max_cpu = 0;
  int64_t max_memory = 0;
  int64_t min_memory = 0;
  omt::ObMultiTenant *omt = GCTX.omt_;

  MTL_SWITCH(tenant_id) {
    storage::ObTenantFreezer *freezer = nullptr;
    if (NULL == omt) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "omt should not be null", K(tenant_id), K(ret));
    } else if (OB_FAIL(omt->get_tenant_cpu(tenant_id, min_cpu, max_cpu))) {
      COMMON_LOG(WARN, "get tenant cpu failed", K(tenant_id), K(ret));
    } else if (FALSE_IT(freezer = MTL(storage::ObTenantFreezer *))) {
    } else if (OB_FAIL(freezer->get_tenant_mem_limit(min_memory, max_memory))) {
      COMMON_LOG(WARN, "get tenant mem limit failed", K(tenant_id), K(ret));
    }
  }
  return OB_SUCCESS == ret? calc_nway((int64_t)max_cpu, min_memory): 0;
}

int ObMemstoreAllocator::set_memstore_threshold()
{
  LockGuard guard(lock_);
  int ret = set_memstore_threshold_without_lock();
  return ret;
}

int ObMemstoreAllocator::set_memstore_threshold_without_lock()
{
  int ret = OB_SUCCESS;
  int64_t memstore_threshold = INT64_MAX;

  storage::ObTenantFreezer *freezer = nullptr;
  if (FALSE_IT(freezer = MTL(storage::ObTenantFreezer *))) {
  } else if (OB_FAIL(freezer->get_tenant_memstore_limit(memstore_threshold))) {
    COMMON_LOG(WARN, "failed to get_tenant_memstore_limit", K(ret));
  } else {
    throttle_tool_->set_resource_limit<ObMemstoreAllocator>(memstore_threshold);
  }
  return ret;
}

int64_t ObMemstoreAllocator::resource_unit_size()
{
  static const int64_t MEMSTORE_RESOURCE_UNIT_SIZE = 2LL * 1024LL * 1024LL; /* 2MB */
  return MEMSTORE_RESOURCE_UNIT_SIZE;
}

void ObMemstoreAllocator::init_throttle_config(int64_t &resource_limit,
                                               int64_t &trigger_percentage,
                                               int64_t &max_duration)
{
  // define some default value
  const int64_t MEMSTORE_THROTTLE_TRIGGER_PERCENTAGE = 60;
  const int64_t MEMSTORE_THROTTLE_MAX_DURATION = 2LL * 60LL * 60LL * 1000LL * 1000LL;  // 2 hours

  int64_t total_memory = lib::get_tenant_memory_limit(MTL_ID());

  // Use tenant config to init throttle config
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (tenant_config.is_valid()) {
    trigger_percentage = tenant_config->writing_throttling_trigger_percentage;
    max_duration = tenant_config->writing_throttling_maximum_duration;
  } else {
    COMMON_LOG_RET(WARN, OB_INVALID_CONFIG, "init throttle config with default value");
    trigger_percentage = MEMSTORE_THROTTLE_TRIGGER_PERCENTAGE;
    max_duration = MEMSTORE_THROTTLE_MAX_DURATION;
  }
  resource_limit = total_memory * MTL(storage::ObTenantFreezer *)->get_memstore_limit_percentage() / 100;
}

void ObMemstoreAllocator::adaptive_update_limit(const int64_t tenant_id,
                                                const int64_t holding_size,
                                                const int64_t config_specify_resource_limit,
                                                int64_t &resource_limit,
                                                int64_t &last_update_limit_ts,
                                                bool &is_updated)
{
  // do nothing
}

};  // namespace share
};  // namespace oceanbase
