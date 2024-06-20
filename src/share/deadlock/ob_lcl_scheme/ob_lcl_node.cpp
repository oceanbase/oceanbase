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

#include "lib/atomic/ob_atomic.h"
#include "share/config/ob_server_config.h"
#include <algorithm>
#include "share/ob_occam_time_guard.h"
#include "ob_lcl_utils.h"
#include "share/deadlock/ob_deadlock_parameters.h"
#include "ob_lcl_parameters.h"
#include "ob_lcl_node.h"
#include "ob_lcl_batch_sender_thread.h"
#include "common/ob_clock_generator.h"
#include "share/deadlock/ob_deadlock_detector_mgr.h"
#include "share/deadlock/ob_deadlock_message.h"
#include "share/deadlock/ob_deadlock_inner_table_service.h"
#include "share/deadlock/ob_deadlock_detector_rpc.h"
#include "observer/omt/ob_multi_tenant.h"

namespace oceanbase
{
namespace share
{
namespace detector
{

using namespace common;

TLOCAL(bool, Holding_lock::holding_lock_) = false;

ObLCLNode::ObLCLNode(const UserBinaryKey &user_key,
                     const uint64_t resource_id,
                     const DetectCallBack &on_detect_operation,
                     const CollectCallBack &on_collect_operation,
                     const ObDetectorPriority &priority,
                     const uint64_t start_delay,
                     const uint32_t count_down_allow_detect,
                     const bool auto_activate_when_detected)
  :push_state_task_(*this),
  self_key_(user_key),
  timeout_ts_(0),
  lclv_(0),
  private_label_(resource_id, priority),
  public_label_(private_label_),
  detect_callback_(on_detect_operation),
  collect_callback_(on_collect_operation),
  auto_activate_when_detected_(auto_activate_when_detected),
  is_timer_task_canceled_(false),
  lcl_period_(0),
  last_report_waiting_for_period_(0),
  last_send_collect_info_period_(0),
  count_down_allow_detect_(count_down_allow_detect),
  lock_(ObLatchIds::DEADLOCK_LOCK)
{
  #define PRINT_WRAPPER K(*this), K(resource_id), K(on_detect_operation),\
                        K(priority), K(auto_activate_when_detected)
  successfully_constructed_ = detect_callback_.is_valid() &&
                              on_collect_operation.is_valid();
  created_time_ = ObClockGenerator::getRealClock();
  allow_detect_time_ = created_time_ + start_delay;
  DETECT_LOG_(TRACE, "new detector instance created", PRINT_WRAPPER);
  #undef PRINT_WRAPPER
  ATOMIC_INC(&(ObIDeadLockDetector::total_constructed_count));
}

ObLCLNode::~ObLCLNode()
{
  ATOMIC_INC(&(ObIDeadLockDetector::total_destructed_count));
}

const ObDetectorPriority &ObLCLNode::get_priority() const
{
  return private_label_.get_priority();
}

int ObLCLNode::add_parent(const ObDependencyResource &parent)
{
  int ret = OB_SUCCESS;

  DETECT_TIME_GUARD(100_ms);
  LockGuard lock_guard(lock_);
  for (int64_t idx = 0; idx < parent_list_.count() && OB_SUCC(ret); ++idx) {
    if (parent == parent_list_.at(idx)) {
      ret = OB_ENTRY_EXIST;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(parent_list_.push_back(parent))) {
      DETECT_LOG_(WARN, "push parent to patent list failed", K(*this), K(parent));
    } else {
      DETECT_LOG_(INFO, "push parent to patent list success", K(*this));
    }
  }

  return ret;
}

void ObLCLNode::set_timeout(const uint64_t timeout)
{
  LockGuard lock_guard(lock_);
  timeout_ts_ = ObClockGenerator::getRealClock() + timeout;
}

int ObLCLNode::register_timer_task()
{
  int ret = OB_SUCCESS;

  DETECT_TIME_GUARD(100_ms);
  if (CLICK() && OB_FAIL(add_self_ref_count_())) {
    DETECT_LOG_(WARN, "add self ref count failed, interrupt register process", K(*this));
  } else if (CLICK() && OB_FAIL(register_timer_with_necessary_retry_with_lock_())) {
    // just register next timer task, do not execute tranmist action
    DETECT_LOG_(ERROR, "fail to register timer task", K(*this));
    CLICK();
    revert_self_ref_count_();
  } else {
    DETECT_LOG_(TRACE, "register first timer task successfully", K(*this));
  }

  return ret;
}

void ObLCLNode::unregister_timer_task()
{
  ATOMIC_STORE(&is_timer_task_canceled_, true);
}

void ObLCLNode::dec_count_down_allow_detect()
{
  uint32_t cnt = 0;
  bool cas_failed = false;
  do {
    cnt = ATOMIC_LOAD(&count_down_allow_detect_);
    if (cnt > 0) {
      cas_failed = (ATOMIC_CAS(&count_down_allow_detect_, cnt, cnt - 1) != cnt);
    }
  } while (cnt > 0 && cas_failed);
}

int ObLCLNode::register_timer_with_necessary_retry_with_lock_()
{
  int ret = OB_SUCCESS;
  int64_t delay = 0;
  ObTimeWheel &time_wheel = MTL(ObDeadLockDetectorMgr*)->get_time_wheel();;

  DETECT_TIME_GUARD(100_ms);
  // timewheel execute timer task without lock's protection
  // need add ref count for detector here, otherwise may cause task called after detector destroyed
  LockGuard lock_guard(lock_);// for protecting task_.expected_executed_ts
  int64_t next_expect_ts = push_state_task_.expected_executed_ts;// last executed ts value, actually

  do {
    int64_t cur_ts = ObClockGenerator::getRealClock();
    int64_t baseline_ts = cur_ts > allow_detect_time_ ? cur_ts : allow_detect_time_;
    int64_t lcl_op_interval = ObServerConfig::get_instance()._lcl_op_interval;
    if (lcl_op_interval == 0) {
      ret = OB_NOT_RUNNING;
      break;
    }
    next_expect_ts = ((baseline_ts / lcl_op_interval) + 1) * lcl_op_interval;
    delay = next_expect_ts - ObClockGenerator::getRealClock();
    if (delay < TIMER_SCHEDULE_RESERVE_TIME) {
      delay = TIMER_SCHEDULE_RESERVE_TIME;
    }
    DETECT_LOG_(DEBUG, "delay value is", K(delay), K(*this));
  // the only contidion that could retry, otherwise will dead loop
  } while (CLICK() &&
           OB_INVALID_ARGUMENT ==(ret = time_wheel.schedule(&push_state_task_, delay)) &&
           delay < 0);

  if (OB_SUCCESS != ret) {
    DETECT_LOG_(WARN, "register next task failed", KR(ret), K(*this));
  } else {
    push_state_task_.expected_executed_ts = next_expect_ts;
  }

  return ret;
}

int ObLCLNode::add_self_ref_count_()
{
  int ret = OB_SUCCESS;
  ObIDeadLockDetector *p_detector = nullptr;

  DETECT_TIME_GUARD(100_ms);
  if (CLICK() && OB_FAIL(MTL(ObDeadLockDetectorMgr*)->detector_map_.get(self_key_, p_detector))) {
    // this may happen when register process not done, but user call unregister
    DETECT_LOG_(WARN, "get detector from map failed, which not expected", K(*this));
  } else if (p_detector != this) {
    // this may happen when register process not done,
    // but user call unregister and then call register again
    DETECT_LOG_(WARN, "get detector from map use self_key_, but obj not myself",
                      K(*this), KP(p_detector));
    CLICK();
    MTL(ObDeadLockDetectorMgr*)->detector_map_.revert(p_detector);
  } else {
    // do nothing
  }

  return ret;
}

void ObLCLNode::revert_self_ref_count_()
{
  DETECT_TIME_GUARD(100_ms);
  MTL(ObDeadLockDetectorMgr*)->detector_map_.revert(this);
}

int ObLCLNode::add_resource_to_list_(const ObDependencyResource &resource,
                                     BlockList &block_list)
{
  int ret = OB_SUCCESS;
  int64_t i = 0;

  DETECT_TIME_GUARD(100_ms);
  for (; i < block_list.count(); ++i) {
    if (block_list.at(i).get_addr() == resource.get_addr() &&
        block_list.at(i).get_user_key() == resource.get_user_key()) {
      break;
    }
  }
  CLICK();
  if (i != block_list.count()) {
    ret = OB_ENTRY_EXIST;
  } else {
    ObDependencyResource new_resource(resource.get_addr(), resource.get_user_key());
    if (OB_FAIL(block_list.push_back(resource))) {
      DETECT_LOG_(WARN, "block_list_ push resource failed", K(resource));
    }
  }

  return ret;
}

int ObLCLNode::block(const ObDependencyResource &resource)
{
  DETECT_TIME_GUARD(100_ms);
  LockGuard lock_guard(lock_);
  CLICK();
  return block_(resource);
}

int ObLCLNode::block_(const ObDependencyResource &resource)
{
  #define PRINT_WRAPPER KR(ret), K(block_list_), K(*this)
  int ret = OB_SUCCESS;

  DETECT_TIME_GUARD(100_ms);
  if (false == resource.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    DETECT_LOG_(WARN, "args invalid", PRINT_WRAPPER);
  } else {
    if (OB_FAIL(add_resource_to_list_(resource, block_list_))) {
      DETECT_LOG_(WARN, "block_list_ push resource failed", PRINT_WRAPPER);
    } else {
      DETECT_LOG_(TRACE, "block resource success", PRINT_WRAPPER);
    }
  }

  return ret;
  #undef PRINT_WRAPPER
}

int ObLCLNode::block(const BlockCallBack &func)
{
  int ret = OB_SUCCESS;

  DETECT_TIME_GUARD(100_ms);
  LockGuard lock_guard(lock_);
  if (CLICK() && !func.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    DETECT_LOG_(WARN, "func is invalid, probablly no memory", K(func), K(*this));
  } else if (CLICK() && OB_FAIL(block_callback_list_.push_back(func))) {
    DETECT_LOG_(WARN, "block_callback_list_ push func failed", K(func), K(*this));
  } else if (CLICK() &&
             OB_UNLIKELY(!block_callback_list_.at(block_callback_list_.count() - 1).is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    DETECT_LOG_(ERROR, "copy block callback failed, push a invalid callback to list",
                       K(func), K(*this));
    block_callback_list_.pop_back();
  } else {
    DETECT_LOG_(TRACE, "block callback success", K(*this));
  }

  return ret;
}

int ObLCLNode::activate(const ObDependencyResource &resource)
{
  LockGuard lock_guard(lock_);
  return activate_(resource);
}

int ObLCLNode::activate_(const ObDependencyResource &resource)
{
  #define PRINT_WRAPPER KR(ret), K(resource), K(*this)
  int ret = OB_SUCCESS;
  int64_t idx = 0;
  
  DETECT_TIME_GUARD(100_ms);
  for (; idx < block_list_.count(); ++idx) {
    if (block_list_.at(idx) == resource) {
      break;
    }
  }
  CLICK();
  if (idx == block_list_.count()) {
    ret = OB_ENTRY_NOT_EXIST;
    DETECT_LOG_(WARN, "connot get resource index", PRINT_WRAPPER);
  } else if (OB_FAIL(block_list_.remove(idx))) {
    DETECT_LOG_(WARN, "block_list_ remove resource failed", PRINT_WRAPPER, K(idx));
  } else {
    DETECT_LOG_(INFO, "activate resource success", PRINT_WRAPPER);
  }

  return ret;
  #undef PRINT_WRAPPER
}

int ObLCLNode::activate_all()
{
  #define PRINT_WRAPPER KR(ret), K(*this)
  int ret = OB_SUCCESS;

  DETECT_TIME_GUARD(100_ms);
  LockGuard lock_guard(lock_);
  block_list_.reset();
  block_callback_list_.reset();

  return ret;
  #undef PRINT_WRAPPER
}

int ObLCLNode::get_block_list(common::ObIArray<ObDependencyResource> &cur_list) const
{
  #define PRINT_WRAPPER KR(ret), K(*this)
  int ret = OB_SUCCESS;
  DETECT_TIME_GUARD(100_ms);
  LockGuard lock_guard(lock_);
  if (OB_FAIL(cur_list.assign(block_list_))) {
    DETECT_LOG_(WARN, "fail to get block list", PRINT_WRAPPER);
  }
  return ret;
  #undef PRINT_WRAPPER
}

int ObLCLNode::replace_block_list(const ObIArray<ObDependencyResource> &new_list)
{
  #define PRINT_WRAPPER KR(ret), K(*this), K(new_list)
  int ret = OB_SUCCESS;
  bool need_replace = false;

  DETECT_TIME_GUARD(100_ms);
  {
    CLICK();
    LockGuard lock_guard(lock_);
    if (new_list.count() != block_list_.count()) {
      need_replace = true;
    } else {
      for (int64_t idx1 = 0; idx1 < new_list.count(); ++idx1) {
        bool resource_exist = false;
        for (int64_t idx2 = 0; idx2 < block_list_.count(); ++idx2) {
          if (new_list.at(idx1) == block_list_.at(idx2)) {
            resource_exist = true;
          } else if (new_list.at(idx1) == ObDependencyResource(GCTX.self_addr(), self_key_)) {
            resource_exist = true;
            DETECT_LOG_(ERROR, "replace block list to block myself, should not happen", PRINT_WRAPPER);
          }
        }
        if (!resource_exist) {
          need_replace = true;
          break;
        }
      }
    }
    CLICK();
    if (need_replace) {
      if (OB_FAIL(block_list_.assign(new_list))) {
        DETECT_LOG_(WARN, "error occured while assgning list", PRINT_WRAPPER);
      } else {
        DETECT_LOG_(INFO, "block list is replaced", PRINT_WRAPPER);
      }
    }
  }

  return ret;
  #undef PRINT_WRAPPER
}

int ObLCLNode::get_resource_from_callback_list_(BlockList &blocklist_snapshot,
                                                ObIArray<bool> &remove_idxs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObDependencyResource, 5> resource_array;
  bool need_remove_call_back_flag = false;

  DETECT_TIME_GUARD(100_ms);
  for (int64_t idx = 0; idx < block_callback_list_.count(); ++idx) {
    need_remove_call_back_flag = false;
    resource_array.reset();
    BlockCallBack &call_back = block_callback_list_.at(idx);
    if (!call_back.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      DETECT_LOG_(ERROR, "callback is invalid, which should not happen",
                         KR(ret), K(*this), K(call_back));
    } else if (CLICK() && OB_FAIL(call_back(resource_array, need_remove_call_back_flag))) {
      DETECT_LOG_(WARN, "execute user callback failed", KR(ret), K(*this));
    } else {
      if (resource_array.empty()) {
        ret = OB_ERR_UNEXPECTED;
        DETECT_LOG_(WARN, "user callback return success, but resource array is empty",
                          KR(ret), K(*this));
      } else {
        for (int64_t idx = 0; idx < resource_array.count() && OB_SUCC(ret); ++idx) {
          if (!resource_array[idx].is_valid()) {
            ret = OB_ERR_UNEXPECTED;
            DETECT_LOG_(WARN, "get invalid resource", KR(ret), K(*this));
          } else if (resource_array[idx] == ObDependencyResource(GCTX.self_addr(), self_key_)) {
            ret = OB_ERR_UNEXPECTED;
            DETECT_LOG_(WARN, "result of callback shows block myself, should not happen",
                              KR(ret), K(resource_array[idx]), K(*this));
          } else if (OB_FAIL(blocklist_snapshot.push_back(resource_array[idx]))) {
            DETECT_LOG_(WARN, "blocklist_snapshot push resource failed", KR(ret), K(*this));
          }
        }
      }
    }
    if (CLICK() && OB_FAIL(remove_idxs.push_back(need_remove_call_back_flag))) {// push falg anyway
      DETECT_LOG_(WARN, "remove_flag_list push flag failed", KR(ret), K(*this));
    } else {}
  }
  ret = OB_SUCCESS;
  return ret;
}

int ObLCLNode::remove_unuseful_callback_(ObIArray<bool> &remove_idxs)
{
  int ret = OB_SUCCESS;

  DETECT_TIME_GUARD(100_ms);
  if (false == remove_idxs.empty()) {
    if (remove_idxs.count() == block_callback_list_.count()) {
      CLICK();
      CallBackList new_call_back_list;
      for (int64_t idx = 0; idx < remove_idxs.count() && OB_SUCC(ret); ++idx) {
        if (false == remove_idxs.at(idx)) {
          if (OB_FAIL(new_call_back_list.push_back(block_callback_list_.at(idx)))) {
            DETECT_LOG_(WARN, "push resource to new callback list failed", KR(ret), K(*this));
          } else if (OB_UNLIKELY(!new_call_back_list[new_call_back_list.count() - 1].is_valid())) {
            DETECT_LOG_(WARN, "push callback to new list meet invalid callabck",
                              KR(ret), K(*this), K(idx), K(new_call_back_list),
                              K(block_callback_list_.at(idx)));
          }
        }
      }
      CLICK();
      if (OB_SUCC(ret)) {
        if (OB_FAIL(block_callback_list_.assign(new_call_back_list))) {
          DETECT_LOG_(WARN, "swap old and new callback list failed", KR(ret), K(*this));
        }
      }
    } else {
      DETECT_LOG_(WARN, "can not remove unuseful callback, cause array size not match",
                        KR(ret), K(*this));
    }
  } else {}
  return ret;
}

void ObLCLNode::get_state_snapshot_(BlockList &blocklist_snapshot,
                                   int64_t &lclv_snapshot,
                                   ObLCLLabel &public_label_snapshot)
{
  int ret = OB_SUCCESS;
  ObSEArray<bool, COMMON_BLOCK_SIZE> remove_idxs;

  DETECT_TIME_GUARD(100_ms);
  if (CLICK() && OB_FAIL(blocklist_snapshot.assign(block_list_))) {
    DETECT_LOG_(WARN, "get block list copy failed", KR(ret), K(*this));
  } else if (CLICK() && OB_FAIL(get_resource_from_callback_list_(blocklist_snapshot, remove_idxs))) {
    DETECT_LOG_(WARN, "get resource from callback list failed", KR(ret), K(*this));
  } else if (CLICK() && OB_FAIL(remove_unuseful_callback_(remove_idxs))) {
    DETECT_LOG_(WARN, "could not remove unuseful callbacks", KR(ret), K(*this));
  } else {
    lclv_snapshot = lclv_;
    public_label_snapshot = public_label_;
  }
  return;
}

int ObLCLNode::broadcast_(const BlockList &list,
                          int64_t lclv,
                          const ObLCLLabel &public_label) {
  int ret = OB_SUCCESS;
  ObLCLMessage msg;
  DETECT_TIME_GUARD(100_ms);
  for (int64_t idx = 0; idx < list.count() && OB_SUCC(ret); ++idx) {
    msg.set_args(list.at(idx).get_addr(),
                 list.at(idx).get_user_key(),
                 GCTX.self_addr(),
                 self_key_,
                 lclv,
                 public_label,
                 ObClockGenerator::getRealClock());
    MTL(ObDeadLockDetectorMgr*)->sender_thread_.cache_msg(list.at(idx), msg);
  }
  
  return ret;
}

int ObLCLNode::process_lcl_message(const ObLCLMessage &lcl_msg)
{
  #define PRINT_WRAPPER KR(ret), K(*this), K(lcl_msg)
  int ret = OB_SUCCESS;
  bool detected_flag = false;
  int64_t lclv_snapshot = INVALID_VALUE;
  ObLCLLabel public_label_snapshot;
  BlockList blocklist_snapshot;

  DETECT_TIME_GUARD(100_ms);
  update_lcl_period_if_necessary_with_lock_();
  CLICK();
  const int64_t current_ts = ObClockGenerator::getClock();
  if (CLICK() && !if_phase_match_(current_ts, lcl_msg)) {
    int64_t diff = current_ts - lcl_msg.get_send_ts();
    if (diff > PHASE_TIME / 3) {
      DETECT_LOG_(WARN, "phase not match", K(diff), K(current_ts), K(*this));
    }
  } else {
    {
      CLICK();
      LockGuard guard(lock_);
      CLICK();
      if ((current_ts / PHASE_TIME) % 2 == 0) {// LCLP
        if (CLICK() && OB_SUCC(process_msg_as_lclp_msg_(lcl_msg))) {
          CLICK();
          (void)get_state_snapshot_(blocklist_snapshot, lclv_snapshot, public_label_snapshot);
        }
      } else {// LCLS
        if (CLICK() && OB_SUCC(process_msg_as_lcls_msg_(lcl_msg, detected_flag))) {
          CLICK();
          (void)get_state_snapshot_(blocklist_snapshot, lclv_snapshot, public_label_snapshot);
        }
      }
      ret = OB_SUCCESS;
    }
    if (detected_flag) {
      CLICK();
      ObDeadLockCollectInfoMessage collect_info_message;
      collect_info_message.set_dest_key(self_key_);
      (void)process_collect_info_message(collect_info_message);
    }
  }

  return ret;
  #undef PRINT_WRAPPER
}

int ObLCLNode::process_msg_as_lclp_msg_(const ObLCLMessage &lcl_msg)
{
  int ret = OB_SUCCESS;

  DETECT_TIME_GUARD(100_ms);
  if (lcl_msg.get_lclv() + 1 > lclv_) {
    lclv_ = lcl_msg.get_lclv() + 1;
  }
  // DETECT_LOG_(INFO, "process lclp message done", KR(ret), K(lcl_msg), K(*this));
  return ret;
}

int ObLCLNode::process_msg_as_lcls_msg_(const ObLCLMessage &lcl_msg,
                                        bool &detected_flag)
{
  int ret = OB_SUCCESS;

  DETECT_TIME_GUARD(100_ms);
  if (lcl_msg.get_lclv() >= lclv_) {
    lclv_ = lcl_msg.get_lclv();
    if (lcl_msg.get_lclv() == lclv_ &&
        lcl_msg.get_label() == private_label_) {
      if (last_send_collect_info_period_ == lcl_period_) {
        ret = OB_EAGAIN;
        if (REACH_TIME_INTERVAL(PHASE_TIME)) {
          DETECT_LOG_(INFO, "collect info msg has been send in this period, giving up this time",
                            K(lcl_msg), K(*this)); 
        }
      } else {
        DETECT_LOG_(INFO, "detect cycle", K(lcl_msg), K(*this));
        last_send_collect_info_period_ = lcl_period_;
        detected_flag = true;
      }
    } else if (lcl_msg.get_label() < public_label_) {
      public_label_ = lcl_msg.get_label();
    }
  }
  // DETECT_LOG_(INFO, "process lcls message done", KR(ret), K(lcl_msg), K(*this));
  return ret;
}

int ObLCLNode::process_collect_info_message(const ObDeadLockCollectInfoMessage &msg)
{
  CHECK_ARGS(msg);
  #define PRINT_WRAPPER KR(ret), K(*this), K(msg_copy)
  int ret = OB_SUCCESS;
  uint64_t event_id = 0;

  ObDeadLockCollectInfoMessage msg_copy;
  if (OB_FAIL(msg_copy.assign(msg))) {
    DETECT_LOG_(WARN, "fail to copy message", PRINT_WRAPPER);
  } else {
    const ObSArray<ObDetectorInnerReportInfo> &collected_info = msg_copy.get_collected_info();
    if (!collected_info.empty()) {
      int64_t detector_id = private_label_.get_id();
      const UserBinaryKey &victim = collected_info[0].get_user_key();
      DETECT_LOG_(INFO, "witness deadlock", KP(this), K(detector_id), K_(self_key), K(victim));
    }
    DETECT_TIME_GUARD(100_ms);
    DETECT_LOG_(INFO, "reveive collect info message", K(collected_info.count()), PRINT_WRAPPER);
    if (CLICK() && OB_SUCC(check_and_process_completely_collected_msg_with_lock_(collected_info))) {
      DETECT_LOG_(INFO, "collect info done", PRINT_WRAPPER);
      CLICK();
      (void) ObDeadLockInnerTableService::insert_all(collected_info);
    } else if (CLICK() && check_dead_loop_with_lock_(collected_info)) {
      DETECT_LOG_(INFO, "message dead loop, just drop this message", PRINT_WRAPPER);
    } else if (CLICK() && OB_FAIL(generate_event_id_with_lock_(collected_info, event_id))) {
      DETECT_LOG_(WARN, "generate event id failed", PRINT_WRAPPER);
    } else if (CLICK() && OB_FAIL(append_report_info_to_msg_(msg_copy, event_id))) {
      DETECT_LOG_(WARN, "append report info to collect info message failed", PRINT_WRAPPER);
    } else if (CLICK() && OB_FAIL(broadcast_with_lock_(msg_copy))) {
      DETECT_LOG_(WARN, "keep boardcasting collect info msg failed", PRINT_WRAPPER);
    } else {
      DETECT_LOG_(INFO, "successfully keep broadcasting collect info msg", PRINT_WRAPPER);
    }
  }

  return ret;
  #undef PRINT_WRAPPER
}

int ObLCLNode::check_and_process_completely_collected_msg_with_lock_(
               const ObIArray<ObDetectorInnerReportInfo> &collected_info)
{
  int ret = OB_SUCCESS;

  DETECT_TIME_GUARD(100_ms);
  if (!collected_info.empty() &&
      collected_info.at(0).get_addr() == GCTX.self_addr() &&
      collected_info.at(0).get_detector_id() == private_label_.get_id()) {
    if (CLICK() && !if_self_has_lowest_priority_(collected_info)) {
      DETECT_LOG_(WARN, "killed node do not have the lowest priority in this cycle",
                       K(collected_info.count()), K(collected_info), K(*this));
    } else {
      CLICK();
      LockGuard guard(lock_);
      if (CLICK() && OB_FAIL(detect_callback_(collected_info, 0))) {
        DETECT_LOG_(WARN, "execute user call back failed", K(*this));
      } else {
        CLICK();
        DETECT_LOG_(INFO, "deadlock detected",
                          K(collected_info.count()), K(collected_info), K(*this));
        if (auto_activate_when_detected_) {
          int64_t next_idx = 1;
          if (collected_info.count() == next_idx) {
            next_idx = 0;
          }
          ObDependencyResource peer_resource(collected_info.at(next_idx).get_addr(),
                                             collected_info.at(next_idx).get_user_key());
          int64_t peer_resource_idx = -1;
          for (int64_t idx = 0; idx < block_list_.count(); ++idx) {
            if (peer_resource == block_list_.at(idx)) {
              peer_resource_idx = idx;
              break;
            }
          }
          if (peer_resource_idx != -1) {
            block_list_.remove(peer_resource_idx);
          }
        }
      }
    }
    ret = OB_SUCCESS;
  } else {
    ret = OB_EAGAIN;
  }

  return ret;
}

bool ObLCLNode::if_self_has_lowest_priority_(
                const ObIArray<ObDetectorInnerReportInfo> &collected_info)
{
  DETECT_TIME_GUARD(100_ms);
  ObDetectorPriority lowest_priority(PRIORITY_RANGE::EXTREMELY_HIGH, UINT64_MAX);
  int64_t lowest_priority_idx = -1;
  for (int64_t i = 0; i < collected_info.count(); ++i) {
    if (collected_info.at(i).get_priority() < lowest_priority) {
      lowest_priority_idx = i;
      lowest_priority = collected_info.at(i).get_priority();
    }
  }
  return lowest_priority_idx == 0;
}

int ObLCLNode::generate_event_id_with_lock_(const ObIArray<ObDetectorInnerReportInfo>
                                                   &collected_info,
                                             uint64_t &event_id)
{
  DETECT_TIME_GUARD(100_ms);
  event_id = 0;
  LockGuard guard(lock_);
  if (collected_info.empty()) {
    event_id = murmurhash(&GCTX.self_addr(),
                          sizeof(GCTX.self_addr()),
                          event_id);
    uint64_t id = private_label_.get_id();
    event_id = murmurhash(&id, sizeof(id), event_id);
    int64_t create_time = created_time_;
    event_id = murmurhash(&create_time, sizeof(create_time), event_id);
  } else {
    event_id = collected_info.at(0).get_event_id();
  }
  return OB_SUCCESS;
}

bool ObLCLNode::check_dead_loop_with_lock_(const ObIArray<ObDetectorInnerReportInfo> &collected_info)
{
  int ret = OB_SUCCESS;
  int64_t idx = 0;

  DETECT_TIME_GUARD(100_ms);
  LockGuard guard(lock_);
  for (; idx < collected_info.count() && OB_SUCC(ret); ++idx) {
    if (collected_info.at(idx).get_addr() == GCTX.self_addr() &&
        collected_info.at(idx).get_detector_id() == private_label_.get_id()) {
      DETECT_LOG_(INFO, "dead loop occured", K(collected_info), K(*this));
      break;
    }
  }
  return idx != collected_info.count();
}

int ObLCLNode::append_report_info_to_msg_(ObDeadLockCollectInfoMessage &msg,
                                          const uint64_t event_id)
{
  int ret = OB_SUCCESS;
  ObDetectorInnerReportInfo inner_report_info;
  ObDetectorUserReportInfo user_report_info;

  DETECT_TIME_GUARD(100_ms);
  if (CLICK() && OB_FAIL(collect_callback_(user_report_info))) {
    DETECT_LOG_(WARN, "execute collect callback failed", K(*this));
  } else if (CLICK() && OB_UNLIKELY(false == user_report_info.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    DETECT_LOG_(WARN, "execute collect callback success, but user_report_info invalid", K(*this));
  } else {
    CLICK();
    LockGuard guard(lock_);
    if (CLICK() && 
       OB_FAIL(inner_report_info.set_args(self_key_, GCTX.self_addr(),
                                          private_label_.get_id(), ObClockGenerator::getRealClock(),
                                          created_time_, event_id,
                                          msg.get_collected_info().empty() ?
                                          ROLE::VICTIM : ROLE::WITNESS,
                                          allow_detect_time_ - created_time_,
                                          private_label_.get_priority(),
                                          user_report_info))) {
      DETECT_LOG_(WARN, "construct inner report info failed", K(*this), K(msg));
    } else if (CLICK() && OB_FAIL(msg.append(inner_report_info))) {
      DETECT_LOG_(WARN, "append msg to collect info message failed", K(*this), K(msg));
    }
  }
  return ret;
}

int ObLCLNode::broadcast_with_lock_(ObDeadLockCollectInfoMessage &msg)
{
  int ret = OB_SUCCESS;
  UserBinaryKey self_key_copy;
  BlockList block_list_copy;

  DETECT_TIME_GUARD(100_ms);
  {
    int64_t unused_1;
    ObLCLLabel uneused_2;
    LockGuard guard(lock_);
    self_key_copy = self_key_;
    block_list_copy.assign(block_list_);
    get_state_snapshot_(block_list_copy, unused_1, uneused_2);
  }
  CLICK();
  for (int64_t idx = 0; idx < block_list_copy.count() && OB_SUCC(ret); ++idx) {
    msg.set_dest_key(block_list_copy.at(idx).get_user_key());
    if (CLICK() &&
        OB_FAIL(MTL(ObDeadLockDetectorMgr*)->get_rpc().
                post_collect_info_message(block_list_copy.at(idx).get_addr(), msg))) {
      DETECT_LOG_(WARN, "send collect info message failed", KR(ret), K(msg));
    }
  }
  return ret;
}

int ObLCLNode::push_state_to_downstreams_with_lock_()
{
  int ret = OB_SUCCESS;
  int64_t lclv_snapshot = INVALID_VALUE;
  ObLCLLabel public_label_snapshot;
  BlockList blocklist_snapshot;

  DETECT_TIME_GUARD(100_ms);
  {
    LockGuard guard(lock_);
    CLICK();
    get_state_snapshot_(blocklist_snapshot, lclv_snapshot, public_label_snapshot);
  }

  CLICK();
  if (ATOMIC_LOAD(&count_down_allow_detect_) != 0) {
    DETECT_LOG_(INFO, "not allow do detect cause count_down_allow_detect_ is not dec to 0 yet",
                      K(blocklist_snapshot), K(lclv_snapshot), K(public_label_snapshot),
                      K(*this));
  } else if (OB_FAIL(broadcast_(blocklist_snapshot, lclv_snapshot, public_label_snapshot))) {
    DETECT_LOG_(WARN, "boardcast failed",
                      K(blocklist_snapshot), K(lclv_snapshot), K(public_label_snapshot),
                      K(*this));
  } else if (lcl_period_ > last_report_waiting_for_period_) {
    LockGuard guard(lock_);
    last_report_waiting_for_period_ = lcl_period_;
    if (blocklist_snapshot.empty()) {
      DETECT_LOG_(WARN, "not waiting", K(*this), K_(last_report_waiting_for_period));
    } else {
      int64_t detector_id = private_label_.get_id();
      DETECT_LOG_(INFO, "waiting for", K_(self_key), K(blocklist_snapshot), KPC(this));
    }

    if (!parent_list_.empty()) {
      ObDeadLockNotifyParentMessage notify_msg;
      for (int64_t idx = 0; idx < parent_list_.count(); ++idx) {
        notify_msg.set_args(parent_list_.at(idx).get_addr(),
                            parent_list_.at(idx).get_user_key(),
                            GCTX.self_addr(),
                            self_key_);
        if (OB_FAIL(MTL(ObDeadLockDetectorMgr*)->get_rpc().
                    post_notify_parent_message(parent_list_.at(idx).get_addr(), notify_msg))) {
          DETECT_LOG_(WARN, "post notify parent message failed", KR(ret), K(notify_msg), K(*this));
        }
      }
    }
  }

  return ret;
}

void ObLCLNode::update_lcl_period_if_necessary_with_lock_()
{
  int ret = OB_SUCCESS;
  DETECT_TIME_GUARD(10_ms);
  int64_t current_ts = ObClockGenerator::getRealClock();
  int64_t new_period_ = current_ts / PERIOD;
  int64_t timeout_ts = 0;

  {
    LockGuard guard(lock_);
    timeout_ts = timeout_ts_;
    if (new_period_ > lcl_period_) {
      lcl_period_ = new_period_;
      public_label_ = private_label_;
    }
  }
  if (timeout_ts != 0 && ObClockGenerator::getRealClock() > timeout_ts) {
    if (OB_FAIL(MTL(ObDeadLockDetectorMgr*)->unregister_key_(self_key_))) {
      DETECT_LOG_(WARN, "fail to gc lcl node", K(*this), KR(ret), K(timeout_ts));
    }
  }
}

bool ObLCLNode::if_phase_match_(const int64_t ts,
                                const ObLCLMessage &msg) {
  bool ret = true;
  int64_t my_phase = ts / PHASE_TIME;
  int64_t msg_phase = msg.get_send_ts() / PHASE_TIME;

  DETECT_TIME_GUARD(10_ms);
  if (my_phase != msg_phase) {
    ret = false;
  }
  return ret;
}

ObLCLNode::PushStateTask::PushStateTask(ObLCLNode &node) : expected_executed_ts(0), lcl_node_(node) {}

void ObLCLNode::PushStateTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t current_ts = ObClockGenerator::getRealClock();

  DETECT_TIME_GUARD(100_ms);
  int64_t warn_threshold_ts = lcl_node_.timeout_ts_ ?
                              lcl_node_.timeout_ts_ + 10_min :
                              lcl_node_.created_time_ + 1_hour;
  if (current_ts > warn_threshold_ts) {
    DETECT_LOG(WARN, "long lived lcl node, maybe leaked", K(*this));
  }
  if (false == ATOMIC_LOAD(&lcl_node_.is_timer_task_canceled_)) {
    if (expected_executed_ts > current_ts) {
      DETECT_LOG(WARN, "schedule error", K(current_ts), K(expected_executed_ts));
    } else if (current_ts - expected_executed_ts > 100 * 1000) {// 100ms
      if (REACH_TIME_INTERVAL(100 * 1000)) {// 100ms
        DETECT_LOG(WARN, "task scheduled out of range", K(current_ts), K(expected_executed_ts));
      }
    }
    
    CLICK();
    (void)lcl_node_.update_lcl_period_if_necessary_with_lock_();

    if (CLICK() && OB_TMP_FAIL(lcl_node_.push_state_to_downstreams_with_lock_())) {
      DETECT_LOG(WARN, "push state to downstreams failed",
                     K(tmp_ret), K(current_ts), K(expected_executed_ts), K(*this));
    }

    if (CLICK() && OB_TMP_FAIL(lcl_node_.register_timer_with_necessary_retry_with_lock_())) {
      DETECT_LOG(ERROR, "register timer task with retry failed", K(tmp_ret), K(*this));
    } else {}
  } else {
    // may destory itself here, make sure it is the last action of this function
    lcl_node_.revert_self_ref_count_();
  }
}

uint64_t ObLCLNode::PushStateTask::hash() const
{
  uint64_t id = lcl_node_.private_label_.get_id();
  return murmurhash(&id, sizeof(id), 0);
}

}
}
}
