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

#include "lib/thread/ob_reentrant_thread.h"
#include "lib/thread/ob_thread_name.h"

#include <sys/ptrace.h>
#include "lib/ob_define.h"

namespace oceanbase {
using namespace lib;
using namespace common;
namespace share {
ObReentrantThread::ObReentrantThread()
    : stop_(true), created_(false), running_cnt_(0), blocking_runnign_cnt_(0), thread_name_("")
{}

ObReentrantThread::~ObReentrantThread()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(destroy())) {
    LOG_WARN("destroy failed", K(ret));
  }
}

int ObReentrantThread::create(const int64_t thread_cnt, const char* thread_name)
{
  int ret = OB_SUCCESS;
  if (created_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already created", K(ret));
  } else if (thread_cnt <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(thread_cnt));
  } else if (OB_FAIL(cond_.init(ObWaitEventIds::REENTRANT_THREAD_COND_WAIT))) {
    LOG_WARN("fail to init cond, ", K(ret));
  } else {
    thread_name_ = thread_name;
    ThreadPool::set_thread_count(thread_cnt);
    created_ = true;
    if (OB_SUCC(ThreadPool::start())) {
      // wait all thread started
      ObThreadCondGuard guard(cond_);
      while (thread_cnt != blocking_runnign_cnt_) {
        cond_.wait();
      }
    }
  }
  return ret;
}

int ObReentrantThread::destroy()
{
  int ret = OB_SUCCESS;
  if (created_) {
    stop();
    {
      ObThreadCondGuard guard(cond_);
      created_ = false;
      int tmp_ret = cond_.broadcast();
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("condition broadcast failed", K(tmp_ret));
      }
    }
    ThreadPool::wait();
    cond_.destroy();
  }
  return ret;
}

int ObReentrantThread::start()
{
  int ret = OB_SUCCESS;
  if (!created_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    if (stop_) {
      ObThreadCondGuard guard(cond_);
      stop_ = false;
      int tmp_ret = cond_.broadcast();
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("condition broadcast failed", K(tmp_ret));
      }
    }
  }
  return ret;
}

void ObReentrantThread::stop()
{
  int ret = OB_SUCCESS;
  if (!created_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObThreadCondGuard guard(cond_);
    stop_ = true;
    ThreadPool::stop();
    int tmp_ret = cond_.broadcast();
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("condition broadcast failed", K(tmp_ret));
    }
  }
}

void ObReentrantThread::wait()
{
  int ret = OB_SUCCESS;
  if (!created_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObThreadCondGuard guard(cond_);
    if (running_cnt_ < 0) {
      ret = OB_INNER_STAT_ERROR;
      LOG_WARN("inner status error", K(ret), K_(running_cnt));
    } else {
      while (running_cnt_ > 0) {
        cond_.wait();
      }
    }
  }
}

void ObReentrantThread::run1()
{
  int ret = OB_SUCCESS;
  const uint64_t idx = get_thread_idx();
  LOG_INFO("new reentrant thread created", K(idx));
  if (OB_NOT_NULL(thread_name_)) {
    if (1 == ThreadPool::get_thread_count()) {
      lib::set_thread_name(thread_name_);
    } else {
      lib::set_thread_name(thread_name_, idx);
    }
  }
  if (OB_FAIL(before_blocking_run())) {
    LOG_WARN("Failed to do before run", K(ret));
  } else if (OB_FAIL(blocking_run())) {
    LOG_WARN("blocking run failed", K(ret));
  } else if (OB_FAIL(after_blocking_run())) {
    LOG_WARN("Failed to do after run", K(ret));
  } else {
  }  // do nothing
  LOG_INFO("reentrant thread exited", K(idx));
}

int ObReentrantThread::blocking_run()
{
  int ret = OB_SUCCESS;
  // may be executed before created, can not check created_ here
  {
    ObThreadCondGuard guard(cond_);
    if (blocking_runnign_cnt_ < 0 || running_cnt_ < 0) {
      ret = OB_INNER_STAT_ERROR;
      LOG_WARN("inner status error", K(ret), K_(blocking_runnign_cnt), K_(running_cnt));
    } else {
      blocking_runnign_cnt_++;
      if (blocking_runnign_cnt_ >= get_thread_count()) {
        int tmp_ret = cond_.broadcast();
        if (OB_SUCCESS != tmp_ret) {
          LOG_WARN("condition broadcast failed", K(tmp_ret));
        }
      }
    }
  }
  while (OB_SUCC(ret)) {
    bool need_run = false;
    {
      ObThreadCondGuard guard(cond_);
      if (!created_) {
        break;
      }
      static const int64_t WAIT_TIME_MS = 3000;
      if (stop_) {
        cond_.wait(WAIT_TIME_MS);
      } else {
        need_run = true;
        running_cnt_++;
      }
    }
    if (need_run) {
      run2();
      ObThreadCondGuard guard(cond_);
      running_cnt_--;
      int tmp_ret = cond_.broadcast();
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("condition broadcast failed", K(tmp_ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObThreadCondGuard guard(cond_);
    blocking_runnign_cnt_--;
  }
  return ret;
}

void ObReentrantThread::nothing()
{}

}  // end namespace share
}  // end namespace oceanbase
