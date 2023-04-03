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

#include "lib/ob_errno.h"
#include "share/ob_occam_time_guard.h"
#include "ob_lcl_batch_sender_thread.h"
#include "lib/atomic/ob_atomic.h"
#include "common/ob_clock_generator.h"
#include "lib/oblog/ob_log_module.h"
#include "share/deadlock/ob_deadlock_detector_mgr.h"
#include "ob_lcl_parameters.h"
#include "share/deadlock/ob_deadlock_arg_checker.h"
#include "share/deadlock/ob_deadlock_detector_rpc.h"
#include <cstdlib>
#include <exception>

namespace oceanbase
{
namespace share
{
namespace detector
{

using namespace common;
extern const char * MEMORY_LABEL;

bool ObLCLBatchSenderThread::RemoveIfOp::operator()(const ObDependencyResource &key,
                                                    ObLCLMessage &lcl_msg)
{
  UNUSED(key);
  bool ret = true;
  int temp_ret = OB_SUCCESS;

  DETECT_TIME_GUARD(100_ms);
  if (lcl_message_list_.count() >= LCL_MSG_CACHE_LIMIT) {
    temp_ret = OB_BUF_NOT_ENOUGH;
    ret = false;
    DETECT_LOG_RET(WARN, temp_ret, "LCL message fetch failed",
                         KR(temp_ret), K(lcl_msg));
  } else if (OB_SUCCESS != (temp_ret = lcl_message_list_.push_back(lcl_msg))) {
    ret = false;
    DETECT_LOG_RET(WARN, temp_ret, "push lcl message to lcl_message_list failed",
                         KR(temp_ret), K(lcl_msg));
  }
  return ret;
}

bool ObLCLBatchSenderThread::MergeOp::operator()(const ObDependencyResource &key,
                                                 ObLCLMessage &value)
{
  UNUSED(key);
  int temp_ret = OB_SUCCESS;

  DETECT_TIME_GUARD(100_ms);
  if (OB_SUCCESS != (temp_ret = value.merge(lcl_message_))) {
    DETECT_LOG_RET(WARN, temp_ret, "merge msg failed", K(temp_ret), K(value), K(lcl_message_));
  }
  return true;
}

int ObLCLBatchSenderThread::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(share::ObThreadPool::init())) {
    DETECT_LOG(WARN, "init thread failed", K(ret), KP(this), K(MTL_ID()));
  } else if (OB_FAIL(lcl_msg_map_.init("LCLSender", MTL_ID()))) {
    DETECT_LOG(WARN, "init thread failed", K(ret), KP(this), K(MTL_ID()));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObLCLBatchSenderThread::start()
{ 
  CHECK_INIT();
  int ret = OB_SUCCESS;
  ATOMIC_STORE(&is_running_, true);
  set_run_wrapper(MTL_CTX());
  if (OB_FAIL(ObThreadPool::start())) {
    ATOMIC_STORE(&is_running_, false);
    DETECT_LOG(ERROR, "satrt ObLCLBatchSenderThread failed", KR(ret));
  }
  return ret;
}

int ObLCLBatchSenderThread::cache_msg(const ObDependencyResource &key, const ObLCLMessage &lcl_msg)
{
  #define PRINT_WRAPPER KR(ret), K(key), K(lcl_msg), K(can_insert), K(random_drop_percentage)
  int ret = OB_SUCCESS;
  CHECK_INIT_AND_START();
  ObLCLBatchSenderThread::MergeOp op(lcl_msg);
  int64_t random_drop_percentage = 0;
  DETECT_TIME_GUARD(100_ms);
  int64_t msg_count = lcl_msg_map_.count();
  bool can_insert = false;
  if (msg_count < LCL_MSG_CACHE_LIMIT / 2) {// always keep
    can_insert = true;
  } else if (msg_count < LCL_MSG_CACHE_LIMIT) {// random drop
    int64_t keep_threshold = LCL_MSG_CACHE_LIMIT / 2;
    // more keeping messages means higher probability to drop new appended one
    // if reach LCL_MSG_CACHE_LIMIT, definitely drop
    random_drop_percentage = (msg_count - keep_threshold) * 100 / keep_threshold;
    can_insert = distribution_(random_generator_) > random_drop_percentage;
  } else {// always drop
    can_insert = false;
    random_drop_percentage = 100;
  }
  if (OB_FAIL(insert_or_merge_(key, lcl_msg, can_insert))) {
    DETECT_LOG(WARN, "lcl message is droped", PRINT_WRAPPER);
  }
  return ret;
  #undef PRINT_WRAPPER
}

int ObLCLBatchSenderThread::insert_or_merge_(const ObDependencyResource &key,
                                             const ObLCLMessage &lcl_message,
                                             const bool can_insert)
{
  #define PRINT_WRAPPER KR(ret), K(key), K(lcl_message), K(can_insert), K(msg_count)
  DETECT_TIME_GUARD(100_ms);
  int ret = OB_SUCCESS;
  ObLCLBatchSenderThread::MergeOp op(lcl_message);
  int64_t msg_count = lcl_msg_map_.count();
  do {// there may be concurrent problem, so need retry until success or meet can't handle failure
    if (OB_SUCCESS != ret) {
      DETECT_LOG(INFO, "try again", PRINT_WRAPPER);
    }
    if (can_insert) {// try insert first, if exist, try update merge then
      if (OB_SUCC(lcl_msg_map_.insert(key, lcl_message))) {
      } else if (OB_ENTRY_EXIST != ret) {
        DETECT_LOG(WARN, "this error can't handle", PRINT_WRAPPER);
        break;
      } else if (OB_SUCC(lcl_msg_map_.operate(key, op))) {
      } else if (OB_ENTRY_NOT_EXIST != ret) {
        DETECT_LOG(WARN, "this error can't handle", PRINT_WRAPPER);
      }
    } else {// just try update merge
      if (OB_FAIL(lcl_msg_map_.operate(key, op))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_BUF_NOT_ENOUGH;
        }
      }
      break;// no matter success or not, no retry
    }
  } while (CLICK() && (OB_ENTRY_NOT_EXIST == ret) && ATOMIC_LOAD(&is_running_));
  return ret;
  #undef PRINT_WRAPPER
}

void ObLCLBatchSenderThread::stop()
{
  ObThreadPool::stop();
  ATOMIC_STORE(&is_running_, false);
  DETECT_LOG(INFO, "ObLCLBatchSenderThread stop");
}

void ObLCLBatchSenderThread::wait()
{
  ObThreadPool::wait();
  DETECT_LOG(INFO, "ObLCLBatchSenderThread wait");
}

void ObLCLBatchSenderThread::destroy()
{
  (void) stop();
  (void) wait();
  lcl_msg_map_.destroy();
}

void ObLCLBatchSenderThread::record_summary_info_and_logout_when_necessary_(int64_t begin_ts,
                                                                            int64_t end_ts,
                                                                            int64_t diff)
{
  DETECT_TIME_GUARD(100_ms);
  int64_t _lcl_op_interval = ObServerConfig::get_instance()._lcl_op_interval;
  if (diff > _lcl_op_interval) {
    ++over_night_times_;
    DETECT_LOG_RET(WARN, common::OB_ERR_UNEXPECTED, "ObLCLBatchSenderThread is too busy",
                      K(end_ts), K(begin_ts), K(diff), K(*this));
  }

  total_record_time_ += diff < _lcl_op_interval ? _lcl_op_interval : diff;
  total_busy_time_ += diff;

  if (total_record_time_ > 5L * 1000L * 1000L) {// 5s
    int duty_ratio_percentage = double(total_busy_time_) / total_record_time_ * 100;
    int64_t total_constructed_detector = ATOMIC_LOAD(&ObIDeadLockDetector::total_constructed_count);
    int64_t total_destructed_detector = ATOMIC_LOAD(&ObIDeadLockDetector::total_destructed_count);
    int64_t total_alived_detector = total_constructed_detector - total_destructed_detector;
    DETECT_LOG(INFO, "ObLCLBatchSenderThread periodic report summary info", K(duty_ratio_percentage),
                      K(total_constructed_detector), K(total_destructed_detector),
                      K(total_alived_detector), K(_lcl_op_interval), K(lcl_msg_map_.count()), K(*this));
    total_record_time_ = 0;
    total_busy_time_ = 0;
    over_night_times_ = 0;
  }
}

void ObLCLBatchSenderThread::run1()
{
  int ret = OB_SUCCESS;

  int64_t begin_ts = 0;
  int64_t end_ts = 0;
  int64_t diff = 0;

  ObArray<ObLCLMessage> mock_lcl_message_list;
  mock_lcl_message_list.set_label("LCLArray");
  ObLCLBatchSenderThread::RemoveIfOp op(mock_lcl_message_list);
  lib::set_thread_name("LCLSender");
  while(ATOMIC_LOAD(&is_running_)) {
    IGNORE_RETURN lib::Thread::update_loop_ts();
    int64_t _lcl_op_interval = ObServerConfig::get_instance()._lcl_op_interval;
    if (_lcl_op_interval == 0) {// 0 means deadlock is disabled
      if (lcl_msg_map_.count() != 0) {
        lcl_msg_map_.clear();
      }
      // if deadlock is disabled, just sleep for a while and check if there is a change
      ob_usleep(1000 * 1000);// detect deadlock open or not every 1s
      continue;
    }

    {
      DETECT_TIME_GUARD(50_ms < _lcl_op_interval ? 50_ms : _lcl_op_interval);
      begin_ts = ObClockGenerator::getRealClock();
      mock_lcl_message_list.reset();
      if (ATOMIC_LOAD(&allow_send_)) {
        if (OB_FAIL(lcl_msg_map_.remove_if(op))) {
          DETECT_LOG(WARN, "can't fill mock_lcl_message_list", KR(ret));
          lcl_msg_map_.reset();// if fetch failed, remove all
        }
        CLICK();
        for (int64_t idx = 0; idx < mock_lcl_message_list.count(); ++idx) {
          const ObLCLMessage &msg = mock_lcl_message_list.at(idx);
          if (OB_ISNULL(mgr_)) {
          } else if (OB_FAIL(mgr_->get_rpc().post_lcl_message(msg.get_addr(), msg))) {
            DETECT_LOG(WARN, "send LCL msg failed", KR(ret), K(msg));
            CLICK();
          } else {
            DETECT_LOG(DEBUG, "send LCL msg success", K(msg));
          }
        }
      }
    }
    
    end_ts = ObClockGenerator::getRealClock();
    diff = end_ts - begin_ts;
    record_summary_info_and_logout_when_necessary_(begin_ts, end_ts, diff);

    if (diff < _lcl_op_interval) {
      ob_usleep(static_cast<uint32_t>(_lcl_op_interval - diff));
      // DETECT_LOG(DEBUG, "scan done", K(diff), K(*this));
    }
  }
  DETECT_LOG(INFO, "ObLCLBatchSenderThread not running anymore");
}

}
}
}
