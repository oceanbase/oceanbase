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

#ifndef OCEABASE_SHARE_THROTTLE_OB_SHARE_RESOURCE_THROTTLE_TOOL_H
#define OCEABASE_SHARE_THROTTLE_OB_SHARE_RESOURCE_THROTTLE_TOOL_H

#include "ob_throttle_unit.h"

#include "common/ob_clock_generator.h"
#include "observer/omt/ob_tenant_config.h"
#include "share/allocator/ob_memstore_allocator.h"
#include "share/allocator/ob_tx_data_allocator.h"
#include "share/allocator/ob_mds_allocator.h"


namespace oceanbase {
namespace share {

template <typename ALLOCATOR>
struct ModuleThrottleTool
{
  ALLOCATOR *allocator_;
  ObThrottleUnit<ALLOCATOR> module_throttle_unit_;
  ModuleThrottleTool<ALLOCATOR>()
      : allocator_(nullptr), module_throttle_unit_(ALLOCATOR::throttle_unit_name(), ALLOCATOR::resource_unit_size()) {}

  TO_STRING_KV(KP(allocator_), K(module_throttle_unit_));
};

// The two stage throttle tool, which manages a share resource throttle unit and some module resource throttle unit
template <typename FakeAllocator, typename ...Args>
class ObShareResourceThrottleTool
{
private:
  static const int64_t DEFAULT_THROTTLE_SLEEP_INTERVAL = 20 * 1000;
private: // Functors for ShareResourceThrottleTool
  struct SumModuleHoldResourceFunctor {
    int64_t sum_;

    SumModuleHoldResourceFunctor() : sum_(0) {}

    template <typename ALLOCATOR,
              typename std::enable_if<
                  std::is_same<typename std::decay<FakeAllocator>::type, typename std::decay<ALLOCATOR>::type>::value,
                  bool>::type = true>
    int operator()(const ModuleThrottleTool<ALLOCATOR> &obj)
    {
      sum_ += 0;
      return OB_SUCCESS;
    }

    template <typename ALLOCATOR,
              typename std::enable_if<
                  !std::is_same<typename std::decay<FakeAllocator>::type, typename std::decay<ALLOCATOR>::type>::value,
                  bool>::type = true>
    int operator()(const ModuleThrottleTool<ALLOCATOR> &obj)
    {
      sum_ += obj.allocator_->hold();
      return OB_SUCCESS;
    }
  };

public:
  ObShareResourceThrottleTool() {}
  ObShareResourceThrottleTool(ObShareResourceThrottleTool &) = delete;
  ObShareResourceThrottleTool &operator= (ObShareResourceThrottleTool &) = delete;

  template <typename HEAD, typename ... OTHERS>
  int init(HEAD *head, OTHERS * ... others);

  template <typename TAIL>
  int init(TAIL *tail);

  template <typename ALLOCATOR>
  void alloc_resource(const int64_t resource_size, const int64_t abs_expire_time, bool &is_throttled);

  template <typename ALLOCATOR>
  bool has_triggered_throttle();

  template <typename ALLOCATOR>
  bool is_throttling(ObThrottleInfoGuard &share_ti_guard, ObThrottleInfoGuard &module_ti_guard);

  template <typename ALLOCATOR>
  bool still_throttling(ObThrottleInfoGuard &share_ti_guard, ObThrottleInfoGuard &module_ti_guard);

  template <typename ALLOCATOR>
  int64_t expected_wait_time(ObThrottleInfoGuard &share_ti_guard, ObThrottleInfoGuard &module_ti_guard);

  template <typename ALLOCATOR>
  void do_throttle(const int64_t abs_expire_time);

  template <typename ALLOCATOR>
  void skip_throttle(const int64_t skip_size);

  template <typename ALLOCATOR>
  void skip_throttle(const int64_t skip_size,
                     ObThrottleInfoGuard &share_ti_guard,
                     ObThrottleInfoGuard &module_ti_guard);

  template <typename ALLOCATOR>
  void set_resource_limit(const int64_t resource_limit);

  template <typename ALLOCATOR>
  void update_throttle_config(const int64_t resource_limit,
                              const int64_t trigger_percentage,
                              const int64_t max_duration,
                              bool &config_changed);

  template <typename ALLOCATOR>
  void enable_adaptive_limit();


  template <typename ALLOCATOR>
  char *unit_name();

  TO_STRING_KV(KP(this), K(module_throttle_tuple_));

private:
  template <typename ALLOCATOR>
  int init_one_(ALLOCATOR *allocator);

private:
  ObTuple<ModuleThrottleTool<FakeAllocator>, ModuleThrottleTool<Args>...> module_throttle_tuple_;

};

}  // namespace share
}  // namespace oceanbase

#ifndef OCEABASE_SHARE_THROTTLE_OB_SHARE_RESOURCE_THROTTLE_TOOL_H_IPP
#define OCEABASE_SHARE_THROTTLE_OB_SHARE_RESOURCE_THROTTLE_TOOL_H_IPP
#include "ob_share_resource_throttle_tool.ipp"
#endif

#endif