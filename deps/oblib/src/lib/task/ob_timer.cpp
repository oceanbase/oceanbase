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

#include "ob_timer.h"
#include "lib/task/ob_timer_monitor.h"
#include "lib/thread/ob_thread_name.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/ash/ob_active_session_guard.h"
#include "lib/allocator/ob_malloc.h"

namespace oceanbase
{
namespace common
{
using namespace obutil;
using namespace lib;

int ObTimer::init(const char* timer_name, const ObMemAttr &attr)
{
  UNUSED(attr);
  int ret = OB_SUCCESS;
  if (ObTimerService::get_instance().is_never_started()) {
    if (OB_FAIL(ObTimerService::get_instance().start())) {
      OB_LOG(ERROR, "start global ObTimerService failed", K(ret));
      ob_abort();
    }
  }
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else {
    is_inited_ = true;
    is_stopped_ = false;
    ObTimerUtil::copy_buff(timer_name_, sizeof(timer_name_), timer_name);
  }
  return ret;
}

ObTimer::~ObTimer()
{
  destroy();
}

int ObTimer::start()
{
  int ret = OB_SUCCESS;
  IRunWrapper *expect_wrapper = Threads::get_expect_run_wrapper();
  if (expect_wrapper != nullptr && expect_wrapper != run_wrapper_) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(ERROR, "ObTimer::start tenant ctx not match", KP(expect_wrapper), KP(run_wrapper_));
    ob_abort();
  } else if (!is_inited_) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "timer is not yet initialized", K(ret));
  } else if (is_stopped_) {
    is_stopped_ = false;
  } else {}
  return ret;
}

void ObTimer::stop()
{
  if (is_inited_) {
    if (!is_stopped_) {
      is_stopped_ = true;
    }
  }
  cancel_all();
  wait();
}

void ObTimer::wait()
{
  int err = OB_SUCCESS;
  if (!is_inited_) {
    OB_LOG_RET(WARN, OB_NOT_INIT, "timer is not yet initialized", K(ret));
  } else if (nullptr == timer_service_) {
    OB_LOG_RET(WARN, OB_ERR_NULL_VALUE, "timer_service is NULL", K(ret));
  } else if (OB_SUCCESS != (err = timer_service_->wait_task(this, nullptr))) {
    OB_LOG_RET(WARN, err, "timer_service_.wait_task failed", K(ret));
  } else {}
}

void ObTimer::destroy()
{
  if (is_inited_) {
    is_stopped_ = true;
    cancel_all();
    wait();
    is_inited_ = false;
    timer_service_ = nullptr;
    timer_name_[0] = '\0';
  }
}

int ObTimer::set_run_wrapper(lib::IRunWrapper *run_wrapper)
{
  int ret = OB_SUCCESS;
  if (nullptr != run_wrapper)
  {
    ObTimerService *service = run_wrapper->get_timer_service();
    if (nullptr == service) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(ERROR, "timer service got from run_wrapper is NULL", K(ret));
    } else if (nullptr != timer_service_
        && timer_service_->get_tenant_id() != ObTimerService::get_instance().get_tenant_id()) {
      ret = OB_OP_NOT_ALLOW;
      OB_LOG(ERROR, "can not change timer service from some tenant to others", K(ret));
    } else {
      timer_service_ = service;
      run_wrapper_ = run_wrapper;
    }
  }
  return ret;
}

bool ObTimer::task_exist(const ObTimerTask &task)
{
  bool ret = false;
  if (is_inited_ && nullptr != timer_service_) {
    ret = timer_service_->task_exist(this, task);
  }
  return ret;
}

int ObTimer::schedule(ObTimerTask &task, const int64_t delay, const bool repeate /*=false*/, const bool immediate /*=false*/)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "timer is not yet initialized", K(ret), K(task));
  } else if (is_stopped_) {
    ret = OB_CANCELED;
      OB_LOG(WARN, "schedule task on stopped timer", K(ret), K(task));
  } else if (nullptr == timer_service_) {
    ret = OB_ERR_NULL_VALUE;
    OB_LOG(WARN, "timer_service is NULL", K(ret), K(task));
  } else if (OB_FAIL(timer_service_->schedule_task(this, task, delay, repeate, immediate))) {
    OB_LOG(WARN, "timer_service_.schedule_task failed", K(ret), K(task));
  } else {}
  return ret;
}

int ObTimer::schedule_repeate_task_immediately(ObTimerTask &task, const int64_t delay)
{
  return schedule(task, delay, true, true);
}

int ObTimer::cancel_task(const ObTimerTask &task)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "timer is not yet initialized", K(ret), K(task));
  } else if (nullptr == timer_service_) {
    ret = OB_ERR_NULL_VALUE;
    OB_LOG(WARN, "timer_service is NULL", K(ret), K(task));
  } else if (OB_FAIL(timer_service_->cancel_task(this, &task))) {
    OB_LOG(WARN, "timer_service_.cancel_task failed", K(ret), K(task));
  } else {}
  return ret;
}

int ObTimer::wait_task(const ObTimerTask &task)
{
  int ret = OB_SUCCESS;
  ObBKGDSessInActiveGuard inactive_guard;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "timer is not yet initialized", K(ret), K(task));
  } else if (nullptr == timer_service_) {
    ret = OB_ERR_NULL_VALUE;
    OB_LOG(WARN, "timer_service is NULL", K(ret), K(task));
  } else if (OB_FAIL(timer_service_->wait_task(this, &task))) {
    OB_LOG(WARN, "timer_service_.wait_task failed", K(ret), K(task));
  } else {}
  return ret;
}

int ObTimer::cancel(const ObTimerTask &task)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "timer is not yet initialized", K(ret), K(task));
  } else if (nullptr == timer_service_) {
    ret = OB_ERR_NULL_VALUE;
    OB_LOG(WARN, "timer_service is NULL", K(ret), K(task));
  } else if (OB_FAIL(timer_service_->cancel_task(this, &task))) {
    OB_LOG(WARN, "timer_service_.cancel_task failed", K(ret), K(task));
  } else {}
  return ret;
}

void ObTimer::cancel_all()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "timer is not yet initialized", K(ret));
  } else if (nullptr == timer_service_) {
    ret = OB_ERR_NULL_VALUE;
    OB_LOG(WARN, "timer_service is NULL", K(ret));
  } else if (OB_FAIL(timer_service_->cancel_task(this, nullptr))) {
    OB_LOG(WARN, "timer_service_.cancel_task failed", K(ret));
  } else {
    wait();
  }
}

bool ObTimer::inited() const
{
  return is_inited_;
}

} /* common */
} /* chunkserver */
