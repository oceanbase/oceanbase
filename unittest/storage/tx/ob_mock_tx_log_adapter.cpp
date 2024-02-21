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

#include "ob_mock_tx_log_adapter.h"
#include "logservice/ob_log_handler.h"
#include "storage/tx/ob_trans_submit_log_cb.h"
#include "logservice/ob_log_base_type.h"

namespace oceanbase
{

using namespace palf;
using namespace logservice;
namespace transaction
{
int MockTxLogAdapter::init(ObITxLogParam *param)
{
  int ret = OB_SUCCESS;

  ObSpinLockGuard file_guard(log_file_lock_);
  ObSpinLockGuard cbs_guard(cbs_lock_);

  // NULL pointer will core in unittest
  MockTxLogParam *mock_param = static_cast<MockTxLogParam *>(param);
  submit_config_ = *mock_param;
  max_allocated_log_ts_ = 0;
  // max_success_log_ts = 0;
  mock_log_file_.clear();
  waiting_cbs_.clear();
  is_running_ = false;

  ObSimpleThreadPool::init(submit_config_.cb_thread_cnt_, 8000000);
  timer_.init("TransTimeWheel");
  return ret;
}

int MockTxLogAdapter::start()
{
  int ret = OB_SUCCESS;
  ATOMIC_STORE(&is_running_, true);

  timer_.start();
  task_ptr_ =(ObITimeoutTask*)malloc(sizeof(MockCbTimeoutTask));
  new (task_ptr_) MockCbTimeoutTask();
  ((MockCbTimeoutTask*)task_ptr_)->init(this);
  timer_.register_timeout_task(*task_ptr_, submit_config_.cb_time_);


  CB_CNT_ = 0;
  return ret;
}

void MockTxLogAdapter::stop()
{
  if (submit_config_.wait_all_cb_) {
    while (is_cbs_finish_()) {
      usleep(10000); // 10 ms
      TRANS_LOG(INFO, "Wait cbs finish");
    }
  }
  ATOMIC_STORE(&is_running_, false);
  timer_.stop();
  ObSimpleThreadPool::stop();
}

void MockTxLogAdapter::wait()
{
  timer_.wait();
  ObSimpleThreadPool::wait();
}

void MockTxLogAdapter::destroy()
{
  // do nothing
  timer_.destroy();
  ObSimpleThreadPool::destroy();
}

int MockTxLogAdapter::push(void *task) { return ObSimpleThreadPool::push(task); }

void MockTxLogAdapter::handle(void *task)
{
  AppendCb *cb = static_cast<AppendCb *>(task);

  TRANS_LOG(INFO, "Start handle cb", K(CB_CNT_));
  // nullptr of cb will core
  if (nullptr != cb) {
    if (ATOMIC_LOAD(&is_running_)) {
      cb->on_success();
    } else {
      cb->on_failure();
    }
    ATOMIC_INC(&CB_CNT_);
  }
  TRANS_LOG(INFO, "End handle cb", K(CB_CNT_));
}

int MockTxLogAdapter::submit_log(const char *buf,
                                 const int64_t size,
                                 const share::SCN &base_ts,
                                 ObTxBaseLogCb *cb,
                                 const bool need_nonblock,
                                 const int64_t retry_timeout_us)
{
  int ret = OB_SUCCESS;
  int64_t ts = 0;
  palf::LSN lsn;
  share::SCN scn;
  UNUSED(need_nonblock);
  if (ATOMIC_LOAD(&is_running_)) {

    {
      ObSpinLockGuard file_guard(log_file_lock_);
      std::string log_buf(buf, size);
      if (base_ts.get_val_for_gts() > max_allocated_log_ts_) {
        max_allocated_log_ts_ = base_ts.get_val_for_gts();
      }
      ts = ++max_allocated_log_ts_;
      scn.convert_for_gts(ts);
      lsn = palf::LSN(++lsn_);
      cb->set_log_ts(scn);
      cb->set_lsn(lsn);
      mock_log_file_[ts] = log_buf;
    }

    {
      ObSpinLockGuard cbs_guard(cbs_lock_);
      waiting_cbs_.push_back(cb);
    }
  } else {
    ret = OB_NOT_MASTER;
  }
  return ret;
}

int MockTxLogAdapter::get_role(bool &is_leader, int64_t &epoch)
{
  int ret = OB_SUCCESS;

  if (ATOMIC_LOAD(&is_running_)) {
    is_leader = true;
    epoch = 1;
  } else {
    is_leader = false;
  }
  return ret;
}

bool MockTxLogAdapter::is_cbs_finish_()
{
  ObSpinLockGuard cbs_guard(cbs_lock_);

  return waiting_cbs_.empty();
}

void MockTxLogAdapter::invoke_all_cbs()
{
  ObSpinLockGuard cbs_guard(cbs_lock_);

  TRANS_LOG(INFO,"Start push all cbs",K(waiting_cbs_.size()));
  AppendCb *tmp_cb = nullptr;
  while (!waiting_cbs_.empty()) {
    if (submit_config_.is_asc_cbs_) {
      tmp_cb = waiting_cbs_.front();
      push(static_cast<void *>(tmp_cb));
      waiting_cbs_.pop_front();
    } else {
      tmp_cb = waiting_cbs_.back();
      push(static_cast<void *>(tmp_cb));
      waiting_cbs_.pop_back();
    }
  }
  TRANS_LOG(INFO,"End push all cbs",K(waiting_cbs_.size()));

  timer_.unregister_timeout_task(*task_ptr_);
  timer_.register_timeout_task(*task_ptr_, submit_config_.cb_time_);
}

bool MockTxLogAdapter::get_log(int64_t log_ts, std::string &log_string)
{
  bool has_log = false;
  ObSpinLockGuard file_guard(log_file_lock_);
  if(mock_log_file_.find(log_ts) != mock_log_file_.end())
  {
    log_string = mock_log_file_[log_ts];
    has_log = true;
  }
  return has_log;
}

int MockTxLogAdapter::get_next_log(int64_t log_ts, std::string &log_string, int64_t &next_log_ts)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard file_guard(log_file_lock_);

  std::map<int64_t, std::string>::iterator file_iter = mock_log_file_.find(log_ts);

  if (file_iter == mock_log_file_.end()) {
    file_iter = mock_log_file_.begin();
  }

  file_iter++;

  if (file_iter == mock_log_file_.end()) {
    ret = OB_HASH_NOT_EXIST;
  } else {
    log_string = file_iter->second;
    next_log_ts = file_iter->first;
  }

  return ret;
}

int64_t MockTxLogAdapter::get_cb_cnt()
{
  return ATOMIC_LOAD(&CB_CNT_);
}


// int MockReplayMgr::init(MockTxLogAdapter * log_adapter)
// {
//   int ret = OB_SUCCESS;
//
//   log_adapter_ptr_ = log_adapter;
//
//   return ret;
// }
//
// int MockReplayMgr::start()
// {
//   int ret = OB_SUCCESS;
//
//   ret = lib::ThreadPool::start();
//
//   return ret;
// }
//
// void MockReplayMgr::stop()
// {
//   lib::ThreadPool::stop();
// }
//
// void MockReplayMgr::wait()
// {
//   lib::ThreadPool::wait();
// }
//
// void MockReplayMgr::destroy()
// {
//   lib::ThreadPool::destroy();
// }
//
// void MockReplayMgr::run1()
// {
//   int ret = OB_SUCCESS;
//   int64_t cur_time = 0;
//   int64_t time_used = 0;
//   std::string tmp_log_string;
//
//   palf::LSN tmp_lsn;
//   while (!has_set_stop()) {
//     cur_time = ObTimeUtility::current_time();
//
//     for (auto iter = replay_target_list_.begin(); iter != replay_target_list_.end(); iter++) {
//
//       while (OB_SUCC(log_adapter_ptr_->get_next_log(iter->replay_success_ts_, iter->replaying_log_,
//                                                     iter->replaying_ts_))) {
//
//         if (OB_FAIL(iter->replay_target_->replay(tmp_log_string.c_str(), tmp_log_string.size(),
//                                                  tmp_lsn, iter->replaying_ts_))) {
//           TRANS_LOG(WARN, "replay one log error", KP(iter->replay_target_), K(iter->replaying_ts_));
//         } else {
//           iter->replay_success_ts_ = iter->replaying_ts_;
//         }
//       }
//
//       if (ret != OB_HASH_NOT_EXIST) {
//         TRANS_LOG(WARN, "replay error", K(ret), K(iter->replay_success_ts_),
//                   K(iter->replaying_ts_));
//       }
//     }
//     time_used = ObTimeUtility::current_time() - cur_time;
//     if (time_used < log_adapter_ptr_->get_cb_time()) {
//       usleep(log_adapter_ptr_->get_cb_time() - time_used);
//     }
//   }
// }
//
// void MockReplayMgr::register_replay_target(logservice::ObIReplaySubHandler *replay_target)
// {
//   MockReplayInfo tmp_replay_info;
//   tmp_replay_info.replay_target_ = replay_target;
//
//   replay_target_list_.push_back(tmp_replay_info);
//
// }
//
// void MockReplayMgr::unregister_replay_target(logservice::ObIReplaySubHandler *replay_target)
// {
//   auto iter = replay_target_list_.begin();
//   for (; iter != replay_target_list_.end(); iter++) {
//
//     if (iter->replay_target_ == replay_target) {
//       break;
//     }
//   }
//
//   if (iter != replay_target_list_.end()) {
//     replay_target_list_.erase(iter);
//   }
// }

} // namespace transaction
} // namespace oceanbase
