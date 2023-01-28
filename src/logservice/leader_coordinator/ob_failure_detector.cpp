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
#include "share/io/ob_io_struct.h"
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
#include "logservice/ob_log_service.h"
#include "observer/ob_server_event_history_table_operator.h"

namespace oceanbase
{
namespace logservice
{
namespace coordinator
{

using namespace common;

ObFailureDetector::ObFailureDetector()
    : coordinator_(nullptr),
      has_add_clog_hang_event_(false),
      has_add_slog_hang_event_(false),
      has_add_sstable_hang_event_(false),
      has_add_clog_full_event_(false),
      lock_(common::ObLatchIds::ELECTION_LOCK)
{}

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
    if (CLICK_FAIL(coordinator_->failure_detect_timer_.schedule_task_repeat(failure_task_handle_, 100_ms, [this]() {
      detect_failure();
      return false;
    }))) {
      COORDINATOR_LOG(ERROR, "fail to schedule failure detect task", KR(ret), K(MTL_ID()));
    } else if (CLICK_FAIL(coordinator_->recovery_detect_timer_.schedule_task_repeat(recovery_task_handle_, 1_s, [this]() {
      detect_recover();
      return false;
    }))) {
      COORDINATOR_LOG(ERROR, "fail to schedule recovery detect task", KR(ret), K(MTL_ID()));
    } else {
      COORDINATOR_LOG(INFO, "failure detector init success", KR(ret), K(MTL_ID()), K(lbt()));
    }
  }
  return ret;
}

void ObFailureDetector::destroy()
{
  LC_TIME_GUARD(1_s);
  failure_task_handle_.stop_and_wait();
  recovery_task_handle_.stop_and_wait();
  has_add_clog_hang_event_ = false;
  has_add_slog_hang_event_ = false;
  has_add_sstable_hang_event_ = false;
  has_add_clog_full_event_ = false;
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
          COORDINATOR_LOG(WARN, "fail to push event to temp_event_", KR(ret), K(MTL_ID()), K(event_[idx]));
        } else if (CLICK_FAIL(temp_recover_detect_operation_.push_back(recover_detect_operation_[idx]))) {
          temp_event_.pop_back();
          COORDINATOR_LOG(WARN, "fail to push detect operation to temp_recover_detect_operation_", KR(ret), K(MTL_ID()), K(event_[idx]));
        }
      }
    } else {
      if (CLICK_FAIL(temp_event_.push_back(event_[idx]))) {
        COORDINATOR_LOG(WARN, "fail to push event to temp_event_", KR(ret), K(MTL_ID()), K(event_[idx]));
      } else if (CLICK_FAIL(temp_recover_detect_operation_.push_back(recover_detect_operation_[idx]))) {
        temp_event_.pop_back();
        COORDINATOR_LOG(WARN, "fail to push detect operation to temp_recover_detect_operation_", KR(ret), K(MTL_ID()), K(event_[idx]));
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

void ObFailureDetector::detect_failure()
{
  LC_TIME_GUARD(1_s);
  // clog disk hang check
  detect_palf_hang_failure_();
  // slog writter hang check
  detect_slog_writter_hang_failure_();
  // sstable hang check
  detect_sstable_io_failure_();
  // clog disk full check
  detect_palf_disk_full_();
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

bool ObFailureDetector::is_clog_disk_has_fatal_error()
{
  return ATOMIC_LOAD(&has_add_clog_hang_event_)
         || ATOMIC_LOAD(&has_add_clog_full_event_);
}

bool ObFailureDetector::is_data_disk_has_fatal_error()
{
  return ATOMIC_LOAD(&has_add_slog_hang_event_)
         || ATOMIC_LOAD(&has_add_sstable_hang_event_);
}

void ObFailureDetector::detect_palf_hang_failure_()
{
  LC_TIME_GUARD(1_s);
  int ret = OB_SUCCESS;
  bool is_clog_disk_hang = false;
  int64_t clog_disk_last_working_time = OB_INVALID_TIMESTAMP;
  const int64_t now = ObTimeUtility::current_time();
  ObLogService *log_service = MTL(ObLogService*);
  FailureEvent clog_disk_hang_event(FailureType::PROCESS_HANG, FailureModule::LOG, FailureLevel::FATAL);
  if (OB_FAIL(clog_disk_hang_event.set_info("clog disk hang event"))) {
    COORDINATOR_LOG(ERROR, "clog_disk_hang_event set_info failed", K(ret));
  } else if (OB_FAIL(log_service->get_io_start_time(clog_disk_last_working_time))) {
    COORDINATOR_LOG(WARN, "get_io_start_time failed", K(ret));
  } else if (FALSE_IT(is_clog_disk_hang = (OB_INVALID_TIMESTAMP != clog_disk_last_working_time
                      && now - clog_disk_last_working_time > IO_HANG_TIME_THRESHOLD_US))) {
  } else if (false == ATOMIC_LOAD(&has_add_clog_hang_event_)) {
    if (!is_clog_disk_hang) {
      // log disk does not hang, skip.
    } else if (OB_FAIL(add_failure_event(clog_disk_hang_event))) {
      COORDINATOR_LOG(ERROR, "add_failure_event failed", K(ret), K(clog_disk_hang_event));
    } else {
      ATOMIC_SET(&has_add_clog_hang_event_, true);
      COORDINATOR_LOG(WARN, "clog disk may be hang, add failure event", K(ret), K(clog_disk_hang_event),
                        K(clog_disk_last_working_time), "hang time", now - clog_disk_last_working_time);
    }
  } else {
    if (is_clog_disk_hang) {
      // IO worker has not recoverd, cannot remove failure_event.
    } else if (OB_FAIL(remove_failure_event(clog_disk_hang_event))) {
      COORDINATOR_LOG(ERROR, "remove_failure_event failed", K(ret), K(clog_disk_hang_event));
    } else {
      ATOMIC_SET(&has_add_clog_hang_event_, false);
      COORDINATOR_LOG(INFO, "clog disk has recoverd, remove failure event", K(ret), K(clog_disk_hang_event));
    }
  }
}

void ObFailureDetector::detect_slog_writter_hang_failure_()
{
  LC_TIME_GUARD(1_s);
  int ret = OB_SUCCESS;
  bool is_slog_writter_hang = false;
  int64_t slog_writter_last_working_time = OB_INVALID_TIMESTAMP;
  const int64_t now = ObTimeUtility::current_time();
  ObStorageLogger *storage_logger = MTL(ObStorageLogger*);
  FailureEvent slog_writter_hang_event(FailureType::PROCESS_HANG, FailureModule::STORAGE, FailureLevel::FATAL);
  if (OB_FAIL(slog_writter_hang_event.set_info("slog writter hang event"))) {
    COORDINATOR_LOG(ERROR, "slog_writter_hang_event set_info failed", K(ret));
  } else if (FALSE_IT(slog_writter_last_working_time = storage_logger->get_pwrite_ts())) {
  } else if (FALSE_IT(is_slog_writter_hang = (0 != slog_writter_last_working_time
                      && now - slog_writter_last_working_time > GCONF.data_storage_warning_tolerance_time))) {
  } else if (false == ATOMIC_LOAD(&has_add_slog_hang_event_)) {
    if (!is_slog_writter_hang) {
      // slog writter is normal, skip.
    } else if (OB_FAIL(add_failure_event(slog_writter_hang_event))) {
      COORDINATOR_LOG(ERROR, "add_failure_event failed", K(ret), K(slog_writter_hang_event));
    } else {
      ATOMIC_SET(&has_add_slog_hang_event_, true);
      COORDINATOR_LOG(WARN, "slog writter may be hang, add failure event", K(ret), K(slog_writter_hang_event),
                        K(slog_writter_last_working_time), "hang time", now - slog_writter_last_working_time);
    }
  } else {
    if (is_slog_writter_hang) {
      // slog writter still hangs, cannot remove failure_event.
    } else if (OB_FAIL(remove_failure_event(slog_writter_hang_event))) {
      COORDINATOR_LOG(ERROR, "remove_failure_event failed", K(ret), K(slog_writter_hang_event));
    } else {
      ATOMIC_SET(&has_add_slog_hang_event_, false);
      COORDINATOR_LOG(INFO, "slog writter has recoverd, remove failure event", K(ret), K(slog_writter_hang_event));
    }
  }
}

void ObFailureDetector::detect_sstable_io_failure_()
{
  LC_TIME_GUARD(1_s);
  int ret = OB_SUCCESS;
  ObDeviceHealthStatus data_disk_status;
  int64_t data_disk_error_start_ts = OB_INVALID_TIMESTAMP;
  const int64_t now = ObTimeUtility::current_time();
  FailureEvent sstable_io_hang_event(FailureType::PROCESS_HANG, FailureModule::STORAGE, FailureLevel::FATAL);
  if (OB_FAIL(sstable_io_hang_event.set_info("sstable io hang event"))) {
    COORDINATOR_LOG(ERROR, "sstable_io_hang_event set_info failed", K(ret));
  } else if (OB_FAIL(OB_IO_MANAGER.get_device_health_detector().get_device_health_status(data_disk_status,
                                                                                         data_disk_error_start_ts))) {
    COORDINATOR_LOG(WARN, "get_device_health_status failed", K(ret));
  } else if (false == ATOMIC_LOAD(&has_add_sstable_hang_event_)) {
    // TODO: modify statement if new ObDeviceHealthStatus is added
    if (ObDeviceHealthStatus::DEVICE_HEALTH_NORMAL == data_disk_status) {
      // data disk does not hang, skip.
    } else if (OB_FAIL(add_failure_event(sstable_io_hang_event))) {
      COORDINATOR_LOG(ERROR, "add_failure_event failed", K(ret), K(sstable_io_hang_event));
    } else {
      ATOMIC_SET(&has_add_sstable_hang_event_, true);
      COORDINATOR_LOG(WARN, "data disk may be hang, add failure event", K(ret), K(sstable_io_hang_event),
                        K(data_disk_error_start_ts));
    }
  } else {
    if (ObDeviceHealthStatus::DEVICE_HEALTH_NORMAL != data_disk_status) {
      // data disk does not recoverd, cannot remove failure_event.
    } else if (OB_FAIL(remove_failure_event(sstable_io_hang_event))) {
      COORDINATOR_LOG(ERROR, "remove_failure_event failed", K(ret), K(sstable_io_hang_event));
    } else {
      ATOMIC_SET(&has_add_sstable_hang_event_, false);
      COORDINATOR_LOG(INFO, "data disk has recoverd, remove failure event", K(ret), K(sstable_io_hang_event));
    }
  }
}

void ObFailureDetector::detect_palf_disk_full_()
{
  LC_TIME_GUARD(1_s);
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();
  bool is_disk_enough = true;
  ObLogService *log_service = MTL(ObLogService*);
  FailureEvent clog_disk_full_event(FailureType::RESOURCE_NOT_ENOUGH, FailureModule::LOG, FailureLevel::FATAL);
  if (OB_FAIL(clog_disk_full_event.set_info("clog disk full event"))) {
    COORDINATOR_LOG(ERROR, "clog_disk_full_event set_info failed", K(ret));
  } else if (OB_FAIL(log_service->check_disk_space_enough(is_disk_enough))) {
    COORDINATOR_LOG(WARN, "check_disk_space_enough failed", K(ret));
  } else if (false == ATOMIC_LOAD(&has_add_clog_full_event_)) {
    if (is_disk_enough) {
      // clog disk is not full, skip.
    } else if (OB_FAIL(add_failure_event(clog_disk_full_event))) {
      COORDINATOR_LOG(ERROR, "add_failure_event failed", K(ret), K(clog_disk_full_event));
    } else {
      ATOMIC_SET(&has_add_clog_full_event_, true);
      COORDINATOR_LOG(WARN, "clog disk is full, add failure event", K(ret), K(clog_disk_full_event),
                        K(now));
    }
  } else {
    if (!is_disk_enough) {
      // clog disk is still full, cannot remove failure_event.
    } else if (OB_FAIL(remove_failure_event(clog_disk_full_event))) {
      COORDINATOR_LOG(ERROR, "remove_failure_event failed", K(ret), K(clog_disk_full_event));
    } else {
      ATOMIC_SET(&has_add_clog_full_event_, false);
      COORDINATOR_LOG(INFO, "clog disk has left space, remove failure event", K(ret), K(clog_disk_full_event));
    }
  }
}

}
}
}
