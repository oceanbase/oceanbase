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

#ifndef OCEANBASE_SHARE_DEADLOCK_OB_LCL_SCHEMA_OB_LCL_NODE_H
#define OCEANBASE_SHARE_DEADLOCK_OB_LCL_SCHEMA_OB_LCL_NODE_H

#include "share/deadlock/ob_deadlock_detector_common_define.h"
#include "ob_lcl_parameters.h"
#include "storage/tx/ob_time_wheel.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "lib/atomic/ob_atomic.h"
#include "ob_lcl_utils.h"
#include "lib/function/ob_function.h"
#include "lib/container/ob_se_array.h"

namespace oceanbase
{
namespace unittest
{
class TestObDeadLockDetector;
}
namespace share
{
namespace detector
{

// the code below is aimming to protect print log
// cause print log may read detector's inner status, but may changed by other threads in concurrency
// but there are limits:
// 1. you must not print another detector with the codes below,
//    cause holding_lock just show the locking status of the detector the thread visited.
// 2. you must not visit another detector(call it's function) within locking range,
//    cause holding_lock will be marked true,
//    but you are not actually holding another detector's lock.

class Holding_lock		
{		
public:		
  inline static void set_holding_lock(bool val)		
  {		
    holding_lock_ = val;		
  }		
  inline static bool get_holding_lock()		
  {		
    return holding_lock_;		
  }		
private:		
  RLOCAL_STATIC(bool, holding_lock_);		
};

class LockGuard
{
public:
  LockGuard(common::ObSpinLock &lock) :
  guard_(lock) {
    // no need to consider concurrent problem cause holding_lock is thread-local variable
    Holding_lock::set_holding_lock(true);
  }
  ~LockGuard() {
    // no need to consider concurrent problem cause holding_lock is thread-local variable
    Holding_lock::set_holding_lock(false);
  }
private:
  common::ObSpinLockGuard guard_;
};

#define DETECT_LOG_(level, info_string, args...) \
  do {\
    if (false == Holding_lock::get_holding_lock()) {\
      LockGuard guard(lock_);\
      DETECT_LOG(level, info_string, args);\
    } else {\
      DETECT_LOG(level, info_string, args);\
    }\
  } while(0)

class ObLCLNode : public ObIDeadLockDetector
{
  typedef common::ObArray<ObDependencyResource> BlockList;
  typedef common::ObSEArray<ObDependencyResource, 1> ParentList;
  typedef common::ObSEArray<BlockCallBack, COMMON_BLOCK_SIZE> CallBackList;
  friend class PushStateTask;
  friend class unittest::TestObDeadLockDetector;
public:
  ObLCLNode(const UserBinaryKey &user_key,
            const uint64_t resource_id,
            const DetectCallBack &on_detect_operation,
            const CollectCallBack &on_collect_operation,
            const ObDetectorPriority &priority,
            const uint64_t start_delay,
            const uint32_t count_down_allow_detect,
            const bool auto_activate_when_detected);
  ~ObLCLNode();
public:
  int add_parent(const ObDependencyResource &parent) override;
  void set_timeout(const uint64_t timeout) override;
  int register_timer_task() override;
  void unregister_timer_task() override;
  void dec_count_down_allow_detect() override;
  bool is_successfully_constructed() const { return successfully_constructed_; }
  const ObDetectorPriority &get_priority() const override;// return detector's priority
  // build a directed dependency relationship to other
  int block(const ObDependencyResource &) override;
  int block(const BlockCallBack &) override;
  int get_block_list(common::ObIArray<ObDependencyResource> &cur_list) const override;
  // releace block list
  int replace_block_list(const common::ObIArray<ObDependencyResource> &) override;
  // remove a directed dependency relationship to other
  int activate(const ObDependencyResource &) override;
  // remove all dependency
  int activate_all() override;
  uint64_t get_resource_id() const { return private_label_.get_id(); }
  int process_collect_info_message(const ObDeadLockCollectInfoMessage &) override;
  // handle message for scheme LCL
  int process_lcl_message(const ObLCLMessage &) override;
  TO_STRING_KV(KP(this), K_(self_key), K_(parent_key), KTIME_(timeout_ts), K_(lclv), K_(private_label),
               K_(public_label), K_(detect_callback),
               K_(auto_activate_when_detected), KTIME_(created_time), KTIME_(allow_detect_time),
               K_(is_timer_task_canceled), K_(block_list), K_(parent_list),
               K_(lcl_period), K_(last_send_collect_info_period), K(block_callback_list_.count()),
               K(ATOMIC_LOAD(&count_down_allow_detect_)))
private:
  class PushStateTask : public common::ObTimeWheelTask
  {
  public:
    PushStateTask(ObLCLNode &node);
    void runTimerTask() override;
    uint64_t hash() const override;
    int64_t expected_executed_ts;
    TO_STRING_KV(K_(lcl_node), K(expected_executed_ts));
  private:
    ObLCLNode& lcl_node_;
  } push_state_task_;
private:
  int block_(const ObDependencyResource &);
  int activate_(const ObDependencyResource &);
  int process_msg_as_lclp_msg_(const ObLCLMessage &);
  int process_msg_as_lcls_msg_(const ObLCLMessage &, bool &);
  int add_resource_to_list_(const ObDependencyResource &, BlockList &);
  void get_state_snapshot_(BlockList &, int64_t &, ObLCLLabel &);
  int get_resource_from_callback_list_(BlockList &, common::ObIArray<bool> &);
  int remove_unuseful_callback_(common::ObIArray<bool> &);
  int register_timer_with_necessary_retry_with_lock_();
  int push_state_to_downstreams_with_lock_();
  int add_self_ref_count_();
  void revert_self_ref_count_();
  int check_and_process_completely_collected_msg_with_lock_(
      const common::ObIArray<ObDetectorInnerReportInfo> &);
  int generate_event_id_with_lock_(const common::ObIArray<ObDetectorInnerReportInfo> &,
                                    uint64_t &);
  bool check_dead_loop_with_lock_(const common::ObIArray<ObDetectorInnerReportInfo> &);
  bool if_self_has_lowest_priority_(const common::ObIArray<ObDetectorInnerReportInfo>&);
  int append_report_info_to_msg_(ObDeadLockCollectInfoMessage &, const uint64_t);
  void update_lcl_period_if_necessary_with_lock_();
  bool if_phase_match_(const int64_t, const ObLCLMessage &);
  int broadcast_(const BlockList &, int64_t, const ObLCLLabel &);
  int broadcast_with_lock_(ObDeadLockCollectInfoMessage &);
  // for debug
  DetectCallBack &get_detect_callback_() { return detect_callback_; }
private:
  const UserBinaryKey self_key_;
  UserBinaryKey parent_key_;
  uint64_t timeout_ts_;
  int64_t lclv_;
  ObLCLLabel private_label_;
  ObLCLLabel public_label_;
  DetectCallBack detect_callback_;
  CollectCallBack collect_callback_;
  bool auto_activate_when_detected_;
  int64_t created_time_;
  int64_t allow_detect_time_;
  bool is_timer_task_canceled_;
  BlockList block_list_;
  ParentList parent_list_;
  CallBackList block_callback_list_;
  int64_t lcl_period_;
  int64_t last_report_waiting_for_period_;
  int64_t last_send_collect_info_period_;
  bool successfully_constructed_;
  uint32_t count_down_allow_detect_;
  mutable common::ObSpinLock lock_;
};

}
}
}
#endif
