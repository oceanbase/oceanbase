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

#define USING_LOG_PREFIX RS

#include "ob_rs_reentrant_thread.h"
#include "share/ob_errno.h"

namespace oceanbase {
using namespace common;

namespace rootserver {

CheckThreadSet ObRsReentrantThread::check_thread_set_;

ObRsReentrantThread::ObRsReentrantThread() : last_run_timestamp_(-1)
{}

ObRsReentrantThread::ObRsReentrantThread(bool need_check) : last_run_timestamp_(need_check ? 0 : -1)
{}

ObRsReentrantThread::~ObRsReentrantThread()
{}

void ObRsReentrantThread::update_last_run_timestamp()
{
  if (ATOMIC_LOAD(&last_run_timestamp_) != -1) {
    ATOMIC_STORE(&last_run_timestamp_, ObTimeUtility::current_time());
  }
}

bool ObRsReentrantThread::need_monitor_check() const
{
  bool ret = false;
  int64_t last_run_timestamp = get_last_run_timestamp();
  int64_t schedule_interval = get_schedule_interval();
  if (schedule_interval >= 0 && last_run_timestamp > 0 &&
      last_run_timestamp + schedule_interval + MAX_THREAD_SCHEDULE_OVERRUN_TIME < ObTimeUtility::current_time()) {
    ret = true;
  }
  return ret;
}

void ObRsReentrantThread::stop()
{
  ObReentrantThread::stop();
}

void ObRsReentrantThread::wait()
{
  ObReentrantThread::wait();
  if (get_last_run_timestamp() != -1) {
    ATOMIC_STORE(&last_run_timestamp_, 0);
  }
}

int ObRsReentrantThread::create(const int64_t thread_cnt, const char* thread_name)
{
  int ret = OB_SUCCESS;
  if (last_run_timestamp_ != -1 && OB_FAIL(ObRsReentrantThread::check_thread_set_.add(this))) {
    LOG_WARN("rs_monitor_check : fail to add check thread set", KR(ret));
  } else if (OB_FAIL(share::ObReentrantThread::create(thread_cnt, thread_name))) {
    LOG_WARN("fail to create reentraint thread", KR(ret), K(thread_name));
  } else if (last_run_timestamp_ != -1) {
    LOG_INFO("rs_monitor_check : reentrant thread check register success", K(thread_name));
  }
  return ret;
}

int ObRsReentrantThread::destroy()
{
  int ret = OB_SUCCESS;
  const char* thread_name = get_thread_name();
  if (last_run_timestamp_ != -1 && OB_FAIL(ObRsReentrantThread::check_thread_set_.remove(this))) {
    LOG_WARN("rs_monitor_check : fail to remove check thread set", KR(ret), K(thread_name));
  } else if (OB_FAIL(share::ObReentrantThread::destroy())) {
    LOG_INFO("fail to destroy reentraint thread", KR(ret), K(thread_name));
  } else if (last_run_timestamp_ != -1) {
    LOG_INFO("rs_monitor_check : reentrant thread check unregister success", K(thread_name), K_(last_run_timestamp));
  }
  return ret;
}

int64_t ObRsReentrantThread::get_last_run_timestamp() const
{
  return ATOMIC_LOAD(&last_run_timestamp_);
}

void ObRsReentrantThread::check_alert(const ObRsReentrantThread& thread)
{
  if (thread.need_monitor_check()) {
    const char* thread_name = thread.get_thread_name();
    int64_t last_run_timestamp = thread.get_last_run_timestamp();
    int64_t last_run_interval = ObTimeUtility::current_time() - last_run_timestamp;
    int64_t schedule_interval = thread.get_schedule_interval();
    int64_t check_interval = schedule_interval + MAX_THREAD_SCHEDULE_OVERRUN_TIME;
    LOG_ERROR("rs_monitor_check : thread hang",
        K(thread_name),
        K(last_run_timestamp),
        K(last_run_interval),
        K(check_interval),
        K(schedule_interval));
  }
}

CheckThreadSet::CheckThreadSet() : len_(0)
{
  memset(arr_, 0, sizeof(arr_));
}

CheckThreadSet::~CheckThreadSet()
{}

void CheckThreadSet::reset()
{
  SpinWLockGuard guard(rwlock_);
  len_ = 0;
}

int CheckThreadSet::remove(ObRsReentrantThread* thread)
{
  int ret = OB_SUCCESS;
  bool found = false;
  SpinWLockGuard guard(rwlock_);
  if (OB_ISNULL(thread)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    for (int i = 0; i < len_; i++) {
      if (arr_[i] == thread) {
        found = true;
      }
      if (found == true && i + 1 < len_) {
        arr_[i] = arr_[i + 1];
      }
    }
    if (found == true) {
      len_--;
    } else {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

int CheckThreadSet::add(ObRsReentrantThread* thread)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(rwlock_);
  if (OB_ISNULL(thread)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    for (int i = 0; i < len_; i++) {
      if (arr_[i] == thread) {
        ret = OB_ENTRY_EXIST;
        break;
      }
    }
    if (OB_SUCC(ret)) {
      if (len_ < MAX_LEN) {
        arr_[len_] = thread;
        len_++;
      } else {
        ret = OB_ARRAY_OUT_OF_RANGE;
      }
    } else if (OB_UNLIKELY(ret == OB_ENTRY_EXIST)) {
      // There are existing items when adding elements, which should not be an exception
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

void CheckThreadSet::loop_operation(void (*func)(const ObRsReentrantThread&))
{
  SpinRLockGuard guard(rwlock_);
  for (int i = 0; i < len_; i++) {
    if (OB_ISNULL(arr_[i])) {
      LOG_WARN("rs_monitor_check : arr[i] is NULL", K(i));
      continue;
    }
    func(*(arr_[i]));
  }
}

}  // namespace rootserver
}  // namespace oceanbase
