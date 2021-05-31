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

#define USING_LOG_PREFIX COMMON
#include "share/ob_force_print_log.h"
#include "share/scheduler/ob_dag_worker.h"
#include "share/scheduler/ob_dag.h"
#include "share/scheduler/ob_tenant_thread_pool.h"
#include "share/rc/ob_context.h"
#include "lib/profile/ob_trace_id.h"
#include "observer/omt/ob_tenant.h"

namespace oceanbase {
using namespace lib;
using namespace common;
using namespace omt;
namespace share {

/*************************************ObDagWorkerNew***********************************/

ObDagWorkerNew::ObDagWorkerNew() : is_inited_(false), switch_flag_(SwitchFlagType::SF_INIT), task_(NULL)
{}

ObDagWorkerNew::~ObDagWorkerNew()
{
  reset();
  stop_worker();
}

int ObDagWorkerNew::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "dag worker is inited twice", K(ret));
  } else if (OB_FAIL(cond_.init(ObWaitEventIds::DAG_WORKER_COND_WAIT))) {  // init cond_
    COMMON_LOG(WARN, "failed to init cond", K(ret));
  } else if (OB_FAIL(start())) {  // start thread
    COMMON_LOG(WARN, "failed to start dag worker", K(ret));
  } else {
    is_inited_ = true;
  }
  if (!is_inited_) {
    destroy();
    COMMON_LOG(WARN, "failed to init ObDagWorkerNew", K(ret));
  } else {
    COMMON_LOG(INFO, "ObDagWorkerNew is inited", K(ret));
  }
  return ret;
}
// reset basic settings
void ObDagWorkerNew::reset()
{
  if (is_inited_) {
    switch_flag_ = SwitchFlagType::SF_INIT;
    task_ = NULL;
  }
}

void ObDagWorkerNew::stop_worker()
{
  is_inited_ = false;
  stop();
  notify();
  wait();
}

void ObDagWorkerNew::notify()
{
  ObThreadCondGuard guard(cond_);
  cond_.signal();
}

void ObDagWorkerNew::set_switch_flag(int64_t switch_flag)
{
  ObThreadCondGuard guard(cond_);
  switch_flag_ = switch_flag;
}

int64_t ObDagWorkerNew::get_switch_flag()
{
  ObThreadCondGuard guard(cond_);
  return switch_flag_;
}

void ObDagWorkerNew::set_task(ObITaskNew* task)
{
  ObThreadCondGuard guard(cond_);
  task_ = task;
}
ObITaskNew* ObDagWorkerNew::get_task()
{
  ObThreadCondGuard guard(cond_);
  return task_;
}

void ObDagWorkerNew::resume()  // wake up
{
  notify();
}

int ObDagWorkerNew::pause_task()
{
  int ret = OB_SUCCESS;
  ObTenantThreadPool* thread_pool = NULL;
  if (OB_ISNULL(task_)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "task is null", K(*this), K(switch_flag_));
  } else if (OB_ISNULL(task_->get_dag()) || OB_ISNULL(thread_pool = task_->get_dag()->get_tenant_thread_pool())) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "dag or tenant_thread_pool is null", K(*this), K(task_->get_dag()));
  } else if (OB_FAIL(
                 thread_pool->pause_task(task_, ObITaskNew::THIS_TASK_PAUSE))) {  // call switch_task to deal with task
    COMMON_LOG(WARN, "switch task fail", K(ret), K(*this), K_(task));
  } else {
    COMMON_LOG(INFO, "pause task success", K(*this), K_(task));
    ObThreadCondGuard guard(cond_);
    while (ObITaskNew::TASK_STATUS_RUNNING != task_->get_status()) {  // wait to be waken up
      cond_.wait(SLEEP_TIME_MS);
    }
  }
  return ret;
}

int ObDagWorkerNew::switch_run_permission()
{
  int ret = OB_SUCCESS;
  ObTenantThreadPool* thread_pool = NULL;
  if (OB_ISNULL(task_)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "task is null", K(*this), K(switch_flag_));
  } else if (OB_ISNULL(task_->get_dag()) || OB_ISNULL(thread_pool = task_->get_dag()->get_tenant_thread_pool())) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "dag or tenant_thread_pool is null", K(*this), K(task_->get_dag()));
  } else if (OB_FAIL(
                 thread_pool->switch_task(task_, ObITaskNew::THIS_TASK_PAUSE))) {  // call switch_task to deal with task
    COMMON_LOG(WARN, "switch task fail", K(ret), K(*this), K_(task));
  } else {
    COMMON_LOG(INFO, "switch_run_permission success", K(*this), K_(task));
    ObThreadCondGuard guard(cond_);
    while (ObITaskNew::TASK_STATUS_RUNNING != task_->get_status()) {  // wait to be waken up
      cond_.wait(SLEEP_TIME_MS);
    }
  }
  return ret;
}

int ObDagWorkerNew::check_flag()
{
  int ret = OB_SUCCESS;
  switch (switch_flag_) {
    case SwitchFlagType::SF_INIT: {  // init
      break;
    }
    case SwitchFlagType::SF_SWITCH: {  // switch to task_->next_task_
      if (OB_ISNULL(task_)) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "invalid task_", K(ret), K_(task));
      } else {
        if (OB_FAIL(switch_run_permission())) {  // stop running
          COMMON_LOG(WARN, "switch run permission error", K(ret), K_(task));
        }
      }
      break;
    }
    case SwitchFlagType::SF_CONTINUE: {  // continue running
      break;
    }
    case SwitchFlagType::SF_PAUSE: {
      if (OB_ISNULL(task_)) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "invalid task_", K(ret), K(task_));
      } else {
        if (OB_FAIL(pause_task())) {  // stop running
          COMMON_LOG(WARN, "pause worker fail", K(ret), K(task_));
          ob_abort();
        } else {
          COMMON_LOG(INFO, "pause worker success", K(ret), K(task_));
        }
      }
      break;
    }
    default: {
      COMMON_LOG(WARN, "invalid switch_flag_", K(ret), K(switch_flag_));
    }
  }
  return ret;
}

void ObDagWorkerNew::run1()
{
  COMMON_LOG(INFO, "run1", K(*task_), K_(switch_flag));
  int ret = OB_SUCCESS;
  while (!has_set_stop()) {  // not stoped
    ret = OB_SUCCESS;
    if (OB_FAIL(check_flag())) {  // check flag
      COMMON_LOG(WARN, "check flag failed", K(*task_), K_(switch_flag));
    }
    if (SF_CONTINUE == switch_flag_ && NULL != task_) {
      if (OB_FAIL(task_->do_work())) {  // call do_work to run task
        COMMON_LOG(WARN, "failed to do work", K(ret), K(*task_));
      }
    } else {
      ObThreadCondGuard guard(cond_);
      while (NULL == task_ && !has_set_stop() && SF_CONTINUE != switch_flag_) {
        cond_.wait(SLEEP_TIME_MS);
      }
    }
  }
}

void ObDagWorkerNew::yield(void* ptr)
{
  ObDagWorkerNew* worker = static_cast<ObDagWorkerNew*>(ptr);
  if (OB_NOT_NULL(ptr) && SF_INIT != worker->switch_flag_) {
    worker->check_flag();  // check flag
  }
}

}  // namespace share
}  // namespace oceanbase
