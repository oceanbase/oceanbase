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

#define USING_LOG_PREFIX SHARE

#include "ob_lob_ext_info_log_allocator.h"
#include "share/throttle/ob_share_resource_throttle_tool.h"
#include "share/rc/ob_tenant_base.h"
#include "observer/omt/ob_tenant_config_mgr.h"

namespace oceanbase {
namespace share {

int64_t FakeAllocatorForLobExtInfoLog::resource_unit_size()
{
  // same as ObLobExtInfoLogAllocator resource_unit_size
  return OB_MALLOC_MIDDLE_BLOCK_SIZE;/* 64KB */
}

void FakeAllocatorForLobExtInfoLog::init_throttle_config(int64_t &resource_limit,
                                                   int64_t &trigger_percentage,
                                                   int64_t &max_duration)
{
  const int64_t LIMIT_PERCENTAGE = 10;
  const int64_t THROTTLE_TRIGGER_PERCENTAGE = 60;
  const int64_t THROTTLE_MAX_DURATION = 2LL * 60LL * 60LL * 1000LL * 1000LL;  // 2 hours

  int64_t total_memory = lib::get_tenant_memory_limit(MTL_ID());
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (tenant_config.is_valid()) {
    resource_limit = total_memory * LIMIT_PERCENTAGE / 100;
    trigger_percentage = tenant_config->writing_throttling_trigger_percentage;
    max_duration = tenant_config->writing_throttling_maximum_duration;;
  } else {
    resource_limit = total_memory * LIMIT_PERCENTAGE / 100;
    trigger_percentage = THROTTLE_TRIGGER_PERCENTAGE;
    max_duration = THROTTLE_MAX_DURATION;
  }
}

void FakeAllocatorForLobExtInfoLog::adaptive_update_limit(const int64_t tenant_id,
                                                    const int64_t holding_size,
                                                    const int64_t config_specify_resource_limit,
                                                    int64_t &resource_limit,
                                                    int64_t &last_update_limit_ts,
                                                    bool &is_updated)
{
  // do nothing
}

int64_t ObLobExtInfoLogAllocator::resource_unit_size()
{
  return OB_MALLOC_MIDDLE_BLOCK_SIZE;/* 64KB */
}

void ObLobExtInfoLogAllocator::init_throttle_config(int64_t &resource_limit, int64_t &trigger_percentage, int64_t &max_duration)
{
  const int64_t LIMIT_PERCENTAGE = 10;
  const int64_t THROTTLE_TRIGGER_PERCENTAGE = 60;
  const int64_t THROTTLE_MAX_DURATION = 2LL * 60LL * 60LL * 1000LL * 1000LL;  // 2 hours

  int64_t total_memory = lib::get_tenant_memory_limit(MTL_ID());
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (tenant_config.is_valid()) {
    resource_limit = total_memory * LIMIT_PERCENTAGE / 100;
    trigger_percentage = tenant_config->writing_throttling_trigger_percentage;
    max_duration = tenant_config->writing_throttling_maximum_duration;;
  } else {
    resource_limit = total_memory * LIMIT_PERCENTAGE / 100;
    trigger_percentage = THROTTLE_TRIGGER_PERCENTAGE;
    max_duration = THROTTLE_MAX_DURATION;
  }
}

void ObLobExtInfoLogAllocator::adaptive_update_limit(const int64_t tenant_id,
                                                 const int64_t holding_size,
                                                 const int64_t config_specify_resource_limit,
                                                 int64_t &resource_limit,
                                                 int64_t &last_update_limit_ts,
                                                 bool &is_updated)
{
  // do nothing
}

int ObLobExtInfoLogAllocator::init(LobExtInfoLogThrottleTool* throttle_tool)
{
  int ret = OB_SUCCESS;
  if (IS_INIT){
    ret = OB_INIT_TWICE;
    SHARE_LOG(WARN, "init ext info log allocator twice", KR(ret), KP(this));
  } else if (OB_FAIL(allocator_.init(
      lib::ObMallocAllocator::get_instance(),
      OB_MALLOC_MIDDLE_BLOCK_SIZE, /*64KB*/
      lib::ObMemAttr(MTL_ID(), "ExtInfoLog", ObCtxIds::LOB_CTX_ID)))) {
    SHARE_LOG(WARN, "init ext info log allocator failed.", K(ret));
  } else {
    throttle_tool_ = throttle_tool;
    is_inited_ = true;
    SHARE_LOG(WARN, "finish init ext info log allocator", KP(this));
  }
  return ret;
}

void *ObLobExtInfoLogAllocator::alloc(const int64_t size)
{
  int64_t abs_expire_time = THIS_WORKER.get_timeout_ts();
  return alloc(size, abs_expire_time);
}

void *ObLobExtInfoLogAllocator::alloc(const int64_t size, const ObMemAttr &attr)
{
  // not support
  LOG_ERROR_RET(OB_NOT_SUPPORTED, "not support interface");
  return nullptr;
}

void *ObLobExtInfoLogAllocator::alloc(const int64_t size, const int64_t abs_expire_time)
{
  bool is_throttled = false;
  // record alloc resource in throttle tool, but do not throttle immediately
  (void)throttle_tool_->alloc_resource<ObLobExtInfoLogAllocator>(size, abs_expire_time, is_throttled);
  if (OB_UNLIKELY(is_throttled)) {
    // just record alloc size
    share::lob_ext_info_log_throttled_alloc() += size;
  }

  void *obj = allocator_.alloc(size);
  return obj;
}

void ObLobExtInfoLogAllocator::free(void *ptr)
{
  allocator_.free(ptr);
}

ObLobExtInfoLogThrottleGuard::ObLobExtInfoLogThrottleGuard(const int64_t abs_expire_time, LobExtInfoLogThrottleTool* throttle_tool)
  : abs_expire_time_(abs_expire_time), throttle_tool_(throttle_tool)
{
  if (0 == abs_expire_time) {
    abs_expire_time_ =
        ObClockGenerator::getClock() + ObThrottleUnit<ObLobExtInfoLogAllocator>::DEFAULT_MAX_THROTTLE_TIME;
  }
  share::lob_ext_info_log_throttled_alloc() = 0;
}

ObLobExtInfoLogThrottleGuard::~ObLobExtInfoLogThrottleGuard()
{
  ObThrottleInfoGuard share_ti_guard;
  ObThrottleInfoGuard module_ti_guard;

  if (OB_ISNULL(throttle_tool_)) {
    SHARE_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "throttle tool is unexpected nullptr", KP(throttle_tool_));
  } else if (throttle_tool_->is_throttling<ObLobExtInfoLogAllocator>(share_ti_guard, module_ti_guard)) {
    // do throttle
    throttle_tool_->do_throttle<ObLobExtInfoLogAllocator>(abs_expire_time_);

    if (throttle_tool_->still_throttling<ObLobExtInfoLogAllocator>(share_ti_guard, module_ti_guard)) {
      (void)throttle_tool_->skip_throttle<ObLobExtInfoLogAllocator>(
          share::lob_ext_info_log_throttled_alloc(), share_ti_guard, module_ti_guard);

      if (module_ti_guard.is_valid()) {
        module_ti_guard.throttle_info()->reset();
      }
    }

    // reset throttled alloc size
    share::lob_ext_info_log_throttled_alloc() = 0;
  } else {
    // do not need throttle, exit directly
  }
}

}  // namespace share
}  // namespace oceanbase