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

#ifndef OCEANBASE_SHARE_THROTTLE_OB_THROTTLE_UNIT_H
#define OCEANBASE_SHARE_THROTTLE_OB_THROTTLE_UNIT_H

#include "share/ob_light_hashmap.h"
#include "share/rc/ob_tenant_base.h"
#include "lib/objectpool/ob_concurrency_objpool.h"
#include "lib/alloc/alloc_struct.h"

#include "ob_throttle_info.h"

namespace oceanbase {
namespace share {


/**
 * @brief Generic Resource Throttling Unit
 *
 * This class is used to manage resource throttling to ensure it doesn't exceed a specified resource limit.
 * It includes resource allocation and throttling functionality. When using this throttling class, users need to
 * provide a functor themselves to assist the throttling unit in updating certain values that are essential for
 * throttling, such as the throttle trigger percentage (throttle_trigger_percentage_).
 *
 * If these values do not require dynamic updates for
 * the current throttling module, they can be set as constants within the functor.
 *
 * For Example: TODO : @gengli.wzy compelte this comment
 *
 *
 * using MyClassThrottleUnit = share::ObThrottleUnit<UpdateMyClassThrottleConfigHandle>;
 *
 */
template <typename ALLOCATOR>
class ObThrottleUnit {
public:
  // 2 hours
  static const int64_t DEFAULT_MAX_THROTTLE_TIME = 2LL * 60LL * 60LL * 1000LL * 1000LL;

private:
  // Default resource unit size is 2MB, which used to calculate decay_factor and avaliable resources after some time
  static const int64_t DEFAULT_RESOURCE_UNIT_SIZE = 2L * 1024L * 1024L; /* 2MB */

  // Time interval for advancing the clock (50 microseconds)
  static const int64_t ADVANCE_CLOCK_INTERVAL = 50;// 50us

public:
  // The users can use another unit size to throttle some other resources
  const int64_t RESOURCE_UNIT_SIZE_;

public:
  ObThrottleUnit(const char *throttle_unit_name, const int64_t resource_unit_size = DEFAULT_RESOURCE_UNIT_SIZE)
      : RESOURCE_UNIT_SIZE_(resource_unit_size),
        unit_name_(throttle_unit_name),
        is_inited_(false),
        enable_adaptive_limit_(false),
        config_specify_resource_limit_(0),
        resource_limit_(0),
        sequence_num_(0),
        clock_(0),
        pre_clock_(0),
        throttle_trigger_percentage_(0),
        throttle_max_duration_(0),
        last_advance_clock_ts_us_(0),
        last_print_throttle_info_ts_(0),
        last_update_limit_ts_(0),
        tenant_id_(0),
        decay_factor_(0),
        throttle_info_map_() {}
  ~ObThrottleUnit() {}
  // Disable default constructor and assignment operator
  ObThrottleUnit() = delete;
  ObThrottleUnit &operator=(ObThrottleUnit &rhs) = delete;

  int init();

  /**
   * @brief Acquire queueing sequence and check if throttling is required.
   *
   * @param[in] holding_resource Amount of currently held resources.
   * @param[in] resource_size Size of the requested resource.
   * @param[in] abs_expire_time If is throttled, this function can sleep until abs_expire_time
   * @param[out] is_throttled Indicating if throttling is needed.
   */
  int alloc_resource(const int64_t holding_resource,
                     const int64_t resource_size,
                     const int64_t abs_expire_time,
                     bool &is_throttled);

  /**
   * @brief Check if this throttle unit has triggerd throttle but do not alloc any resource
   *        ATTENTION : This function is different from is_throttling(). is_throttling() only checks if current
   *        thread is throttling, but this function checks if this tenant is throttling
   */
  bool has_triggered_throttle(const int64_t holding_resource);

  /**
   * @brief Check if this throttle unit is throttling status.
   *
   * @param[out] ti_guard The throttle info guard from this unit.
   * @return True if this unit is in throttling status.
   */
  bool is_throttling(ObThrottleInfoGuard &ti_guard);

  /**
   * @brief If the clock has not advanced beyond the queue sequence, throttling is still required.
   *
   * @param[in] ti_guard The guard which hold throttle info(acquired from is_throttling function). If throttle done, the
   * throttle info in guard will be cleared.
   * @param[in] holding_size The holding resource size of allocator.
   * @return True if throttling is still required, false otherwise.
   */
  bool still_throttling(ObThrottleInfoGuard &ti_guard, const int64_t holding_size);

  /**
   * @brief Advance clock once if needed.
   *
   * @param[in] holding_size The holding resource size of allocator.
   */
  void advance_clock(const int64_t holding_size);

  /**
   * @brief skip some throttled sequence if the throttle is not actually executed.
   *
   * @param[in] skip_size The skipped sequence.
   * @param[in] queue_sequence The queue_sequence this throttled thread got.
   */
  void skip_throttle(const int64_t skip_size, const int64_t queue_sequence);

  /**
   * @brief If the clock has not advanced beyond the queue sequence, throttling is still required.
   *
   * @param[in] ti_guard The guard which hold throttle info(acquired from is_throttling function).
   * @param[in] holding_size The holding resource size of allocator.
   * @return True if throttling is still required, false otherwise.
   */
  int64_t expected_wait_time(share::ObThrottleInfoGuard &ti_guard, const int64_t holding_size);

  TO_STRING_KV(K(unit_name_),
               K(is_inited_),
               K(enable_adaptive_limit_),
               K(config_specify_resource_limit_),
               K(resource_limit_),
               K(sequence_num_),
               K(clock_),
               K(pre_clock_),
               K(throttle_trigger_percentage_),
               K(throttle_max_duration_),
               K(last_advance_clock_ts_us_),
               K(last_print_throttle_info_ts_),
               K(last_update_limit_ts_),
               K(decay_factor_));

public: // throttle configs setter
  void enable_adaptive_limit();
  void set_throttle_trigger_percentage(const int64_t throttle_trigger_percentage);
  void set_throttle_max_duration(const int64_t throttle_max_duration);
  void set_resource_limit(const int64_t resource_limit);
  void update_throttle_config(const int64_t resource_limit,
                              const int64_t throttle_trigger_percentage,
                              const int64_t throttle_max_duration,
                              bool &config_changed);

private:
  int get_throttle_info_(const ThrottleID &throttle_id, share::ObThrottleInfoGuard &ti_guard);
  int inner_get_throttle_info_(share::ObThrottleInfo *&throttle_info, const int64_t abs_expire_time);
  void inner_revert_throttle_info_(share::ObThrottleInfo *throttle_info);
  void update_decay_factor_(const bool is_adaptive_update = false);
  void reset_thread_throttle_();
  void set_throttle_info_(const int64_t sequence, const int64_t allocated_size, const int64_t abs_expire_time);
  void inner_set_resource_limit_(const int64_t resource_limit);
  void print_throttle_info_(const int64_t holding_size,
                            const int64_t alloc_size,
                            const int64_t sequence,
                            const int64_t throttle_trigger);
  int64_t avaliable_resource_after_dt_(const int64_t cur_mem_hold, const int64_t trigger_mem_limit, const int64_t dt);

private:
  lib::ObLabel unit_name_;
  bool is_inited_;
  bool enable_adaptive_limit_;
  int64_t config_specify_resource_limit_;
  int64_t resource_limit_;
  int64_t sequence_num_;
  int64_t clock_;
  int64_t pre_clock_;
  int64_t throttle_trigger_percentage_;
  int64_t throttle_max_duration_;
  int64_t last_advance_clock_ts_us_;
  int64_t last_print_throttle_info_ts_;
  int64_t last_update_limit_ts_;
  int64_t tenant_id_;
  double decay_factor_;


  // Save throttle infos created by different threads.
  ObThrottleInfoHashMap throttle_info_map_;
};

}  // namespace share
}  // namespace oceanbase

#ifndef OCEANBASE_SHARE_THROTTLE_OB_THROTTLE_UNIT_H_IPP
#define OCEANBASE_SHARE_THROTTLE_OB_THROTTLE_UNIT_H_IPP
#include "ob_throttle_unit.ipp"
#endif

#endif
