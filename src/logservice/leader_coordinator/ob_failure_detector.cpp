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

#include "share/ob_occam_time_guard.h"
#include "lib/ob_define.h"
#include "lib/function/ob_function.h"
#include "lib/string/ob_string.h"
#include "share/ob_occam_timer.h"
#include "ob_failure_detector.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_se_array.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "share/rc/ob_tenant_base.h"
#include "ob_leader_coordinator.h"
#include <utility>
#include "common_define.h"
#include "ob_leader_coordinator.h"
#include "observer/ob_server_event_history_table_operator.h"

namespace oceanbase
{
namespace logservice
{
namespace coordinator
{

using namespace common;

ObFailureDetector::ObFailureDetector() : coordinator_(nullptr) {}

constexpr int VALUE_BUFFER_SIZE = 512;

int ObFailureDetector::init(ObLeaderCoordinator *coordinator)
{
  LC_TIME_GUARD(1_s);
  int ret = OB_SUCCESS;
  event_.reset();
  recover_detect_operation_.reset();
  if (OB_NOT_NULL(coordinator_)) {
    ret = OB_INIT_TWICE;
    COORDINATOR_LOG(ERROR, "has been inited", KR(ret), K(MTL_ID()), KP_(coordinator));
  } else if (OB_ISNULL(coordinator)) {
    ret = OB_INVALID_ARGUMENT;
    COORDINATOR_LOG(ERROR, "coordinator is nullptr", KR(ret), K(MTL_ID()));
  } else {
    coordinator_ = coordinator;
    if (CLICK_FAIL(coordinator_->timer_.schedule_task_repeat(task_handle_, 1_s, [this]() {
      detect_recover();
      return false;
    }))) {
      COORDINATOR_LOG(ERROR, "fail to schedule task", KR(ret), K(MTL_ID()));
    } else {
      COORDINATOR_LOG(INFO, "failure detector init success", KR(ret), K(MTL_ID()), K(lbt()));
    }
  }
  return ret;
}

void ObFailureDetector::destroy()
{
  LC_TIME_GUARD(1_s);
  task_handle_.stop_and_wait();
  COORDINATOR_LOG(INFO, "failure detector destroyed", K(MTL_ID()), K(lbt()));
}

int ObFailureDetector::mtl_init(ObFailureDetector *&p_failure_detector)
{
  LC_TIME_GUARD(1_s);
  int ret = OB_SUCCESS;
  ObLeaderCoordinator *p_coordinator = MTL(ObLeaderCoordinator *);
  if (OB_ISNULL(p_coordinator)) {
    ret = OB_INVALID_ARGUMENT;
    COORDINATOR_LOG(ERROR, "invalid argument", KR(ret), K(MTL_ID()));
  } else if (CLICK_FAIL(p_failure_detector->init(p_coordinator))) {
    COORDINATOR_LOG(ERROR, "init failure detector failed", KR(ret), K(MTL_ID()));
  }
  return ret;
}

void ObFailureDetector::detect_recover()
{
  LC_TIME_GUARD(1_s);
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  ObArray<FailureEvent> temp_event_;
  ObArray<ObFunction<bool()>> temp_recover_detect_operation_;
  if (!event_.empty()) {
    COORDINATOR_LOG(INFO, "doing detect revocer operation", K(MTL_ID()), K_(event), K_(recover_detect_operation));
  }
  for (int64_t idx = 0; idx < event_.count() && OB_SUCC(ret); ++idx) {
    if (recover_detect_operation_[idx].is_valid()) {
      LC_TIME_GUARD(10_ms);
      bool recover_flag = recover_detect_operation_[idx]();
      if (recover_flag) {
        COORDINATOR_LOG(INFO, "revocer event detected", K(MTL_ID()), K(event_[idx]));
        (void) insert_event_to_table_(event_[idx], recover_detect_operation_[idx], "DETECT REVOCER");
      } else {
        if (CLICK_FAIL(temp_event_.push_back(event_[idx]))) {
          COORDINATOR_LOG(INFO, "fail to push event to temp_event_", KR(ret), K(MTL_ID()), K(event_[idx]));
        } else if (CLICK_FAIL(temp_recover_detect_operation_.push_back(recover_detect_operation_[idx]))) {
          COORDINATOR_LOG(INFO, "fail to push detect operation to temp_recover_detect_operation_", KR(ret), K(MTL_ID()), K(event_[idx]));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (temp_event_.count() != event_.count()) {
      if (CLICK_FAIL(event_.assign(temp_event_))) {
        COORDINATOR_LOG(WARN, "replace event array failed", KR(ret), K(MTL_ID()));
      } else if (CLICK_FAIL(recover_detect_operation_.assign(temp_recover_detect_operation_))) {
        COORDINATOR_LOG(WARN, "replace recover detect operation array failed", KR(ret), K(MTL_ID()));
      }
    }
  }
}

int ObFailureDetector::add_failure_event(const FailureEvent &event)
{
  LC_TIME_GUARD(1_s);
  int ret = OB_SUCCESS;
  int64_t idx = 0;
  ObSpinLockGuard guard(lock_);
  for (; idx < event_.count(); ++idx) {
    if (event_[idx] == event) {
      break;
    }
  }
  if (idx != event_.count()) {
    ret = OB_ENTRY_EXIST;
    COORDINATOR_LOG(WARN, "this failure event has been exist", KR(ret), K(MTL_ID()), K(event), K(event_));
  } else if (CLICK_FAIL(event_.push_back(event))) {
    COORDINATOR_LOG(WARN, "fail to push event to failure detector", KR(ret), K(MTL_ID()), K(event), K(event_));
  } else if (CLICK_FAIL(recover_detect_operation_.push_back(ObFunction<bool()>()))) {
    COORDINATOR_LOG(WARN, "fail to push default recover operation to failure detector", KR(ret), K(MTL_ID()), K(event), K(event_));
    event_.pop_back();
  } else {
    COORDINATOR_LOG(INFO, "success report a failure event without recover detect operation", KR(ret), K(MTL_ID()), K(event), K(event_));
    (void) insert_event_to_table_(event, ObFunction<bool()>(), event.info_.get_ob_string());
  }
  return ret;
}

int ObFailureDetector::add_failure_event(const FailureEvent &event, const ObFunction<bool()> &recover_detect_operation)
{
  LC_TIME_GUARD(1_s);
  int ret = OB_SUCCESS;
  int64_t idx = 0;
  ObSpinLockGuard guard(lock_);
  for (; idx < event_.count(); ++idx) {
    if (event_[idx] == event) {
      break;
    }
  }
  if (idx != event_.count()) {
    ret = OB_ENTRY_EXIST;
    COORDINATOR_LOG(WARN, "this failure event has been exist", KR(ret), K(MTL_ID()), K(event), K(event_));
  } else if (CLICK_FAIL(event_.push_back(event))) {
    COORDINATOR_LOG(WARN, "fail to push event to failure detector", KR(ret), K(MTL_ID()), K(event), K(event_));
  } else if (CLICK_FAIL(recover_detect_operation_.push_back(recover_detect_operation))) {
    COORDINATOR_LOG(INFO, "fail to push recover operation to failure detector", KR(ret), K(MTL_ID()), K(event), K(event_));
    event_.pop_back();
  } else {
    COORDINATOR_LOG(INFO, "success report a failure event with recover detect operation", KR(ret), K(MTL_ID()), K(event), K(event_));
    (void) insert_event_to_table_(event, recover_detect_operation, event.info_.get_ob_string());
  }
  return ret;
}

int ObFailureDetector::remove_failure_event(const FailureEvent &event)
{
  LC_TIME_GUARD(1_s);
  int ret = OB_SUCCESS;
  int64_t idx = 0;
  ObSpinLockGuard guard(lock_);
  for (; idx < event_.count(); ++idx) {
    if (event_[idx] == event) {
      break;
    }
  }
  if (idx == event_.count()) {
    ret = OB_ENTRY_NOT_EXIST;
    COORDINATOR_LOG(WARN, "this failure event not exist", KR(ret), K(MTL_ID()), K(event), K(event_));
  } else {
    (void) insert_event_to_table_(event_[idx], recover_detect_operation_[idx], "REMOVE FAILURE");
    if (CLICK_FAIL(event_.remove(idx))) {
      COORDINATOR_LOG(WARN, "remove event failed", KR(ret), K(MTL_ID()), K(event), K(event_));
    } else if (CLICK_FAIL(recover_detect_operation_.remove(idx))) {
      COORDINATOR_LOG(WARN, "remove operation failed", KR(ret), K(MTL_ID()), K(event), K(event_));
    } else {
      COORDINATOR_LOG(INFO, "user remove failure event success", KR(ret), K(MTL_ID()), K(event), K(event_));
    }
  }
  return ret;
}

int ObFailureDetector::insert_event_to_table_(const FailureEvent &event, const ObFunction<bool()> &recover_operation, ObString info)
{
  LC_TIME_GUARD(1_s);
  #define PRINT_WRAPPER KR(ret), K(MTL_ID()), K(event), K(recover_operation)
  int ret = OB_SUCCESS;
  if (CLICK_FAIL(SERVER_EVENT_ADD("FAILURE_DETECTOR",
                               common::to_cstring(info),
                               "FAILURE_MODULE",
                               obj_to_cstring(event.module_),
                               "FAILURE_TYPE",
                               obj_to_cstring(event.type_),
                               "AUTO_RECOVER",
                               common::to_cstring(recover_operation.is_valid())))) {
    COORDINATOR_LOG_(WARN, "insert into __all_server_event_history failed");
  } else {
    COORDINATOR_LOG_(INFO, "insert into __all_server_event_history success");
  }
  return ret;
  #undef PRINT_WRAPPER
}

int ObFailureDetector::get_specified_level_event(FailureLevel level, ObIArray<FailureEvent> &results)
{
  LC_TIME_GUARD(1_s);
  #define PRINT_WRAPPER KR(ret), K(MTL_ID()), K(level), K(results)
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  results.reset();
  for (int64_t idx = 0; idx < event_.count(); ++idx) {
    if (event_.at(idx).get_failure_level() == level) {
      if (CLICK_FAIL(results.push_back(event_.at(idx)))) {
        COORDINATOR_LOG_(WARN, "fail to push back event to results");
      }
    }
  }
  if (CLICK_FAIL(ret)) {
    COORDINATOR_LOG_(WARN, "fail to get specified level failure event");
  }
  return ret;
  #undef PRINT_WRAPPER
}

}
}
}