/**
 * Copyright (c) 2024 OceanBase
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

#include "rootserver/mview/ob_mview_pending_task_scheduler.h"
#include "rootserver/mview/ob_mview_pending_task_manager.h"
#include "share/ob_thread_mgr.h"
#include "share/config/ob_server_config.h"
#include "observer/omt/ob_tenant_config_mgr.h"

namespace oceanbase
{
namespace rootserver
{
using namespace common;

ObMviewPendingTaskScheduler::ObMviewPendingTaskScheduler()
  : tg_id_(-1),
    idle_cond_(),
    has_pending_work_(false),
    manager_(NULL),
    is_started_(false),
    is_inited_(false)
{
}

ObMviewPendingTaskScheduler::~ObMviewPendingTaskScheduler()
{
  destroy();
}

int ObMviewPendingTaskScheduler::init(ObMViewPendingTaskManager &manager)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("pending task scheduler init twice", KR(ret));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::MViewSched, tg_id_))) {
    LOG_WARN("create tenant tg failed", KR(ret));
  } else if (OB_FAIL(TG_SET_RUNNABLE(tg_id_, *this))) {
    LOG_WARN("set runnable failed", KR(ret), K_(tg_id));
  } else if (OB_FAIL(idle_cond_.init(ObWaitEventIds::THREAD_IDLING_COND_WAIT))) {
    LOG_WARN("init idle cond failed", KR(ret));
  } else {
    manager_ = &manager;
    is_inited_ = true;
  }
  if (OB_FAIL(ret) && tg_id_ != -1) {
    TG_DESTROY(tg_id_);
    tg_id_ = -1;
  }
  return ret;
}

int ObMviewPendingTaskScheduler::start()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("pending task scheduler not init", KR(ret));
  } else if (!is_started_) {
    // First switch_to_leader: physically create the thread, then logical_start
    // to clear ObReentrantThread::stop_ (initially true) so blocking_run()
    // enters run2() instead of waiting on the condition variable indefinitely.
    if (OB_FAIL(TG_START(tg_id_))) {
      LOG_WARN("start pending task scheduler failed", KR(ret), K_(tg_id));
    } else if (OB_FAIL(TG_REENTRANT_LOGICAL_START(tg_id_))) {
      LOG_WARN("logical start pending task scheduler failed", KR(ret), K_(tg_id));
      TG_STOP(tg_id_);
      wakeup();
      TG_WAIT(tg_id_);
    } else {
      is_started_ = true;
    }
  } else if (OB_FAIL(TG_REENTRANT_LOGICAL_START(tg_id_))) {
    // Subsequent switch_to_leader: logically restart (clears has_set_stop, re-runs run1)
    LOG_WARN("logical start pending task scheduler failed", KR(ret), K_(tg_id));
  } else {
    wakeup();
  }
  return ret;
}

void ObMviewPendingTaskScheduler::stop()
{
  // Only act when the thread has been physically started; if stop() is called
  // before the first start() (e.g. switch_to_follower_forcedly during init),
  // skip the logical stop to avoid pre-setting has_set_stop()=true, which would
  // cause the thread to exit run1() immediately on the first TG_START.
  if (OB_LIKELY(is_inited_) && is_started_) {
    TG_REENTRANT_LOGICAL_STOP(tg_id_);
    wakeup();
  }
}

void ObMviewPendingTaskScheduler::wait()
{
  // TG_REENTRANT_LOGICAL_WAIT blocks until run1() exits but does NOT call
  // destroy(), keeping th_ valid for the next TG_REENTRANT_LOGICAL_START.
  if (OB_LIKELY(is_inited_) && is_started_) {
    TG_REENTRANT_LOGICAL_WAIT(tg_id_);
  }
}

void ObMviewPendingTaskScheduler::destroy()
{
  if (OB_LIKELY(is_inited_)) {
    TG_STOP(tg_id_);
    wakeup();
    TG_WAIT(tg_id_);
    TG_DESTROY(tg_id_);
    tg_id_ = -1;
    is_started_ = false;
    idle_cond_.destroy();
    manager_ = NULL;
    is_inited_ = false;
  }
}

void ObMviewPendingTaskScheduler::wakeup()
{
  if (OB_LIKELY(is_inited_)) {
    ObThreadCondGuard guard(idle_cond_);
    has_pending_work_ = true;
    idle_cond_.signal();
  }
}

int ObMviewPendingTaskScheduler::check_concurrent_limit(uint64_t tenant_id) const
{
  int ret = OB_SUCCESS;
  int64_t max_concurrent = 10;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  if (tenant_config.is_valid()) {
    max_concurrent = tenant_config->_mview_refresh_concurrency;
  }
  const int64_t running_cnt = manager_->get_total_running_cnt();
  if (running_cnt >= max_concurrent) {
    ret = OB_EAGAIN;
    LOG_INFO("mview concurrent task limit reached, will retry later",
             K(running_cnt), K(max_concurrent), K(tenant_id));
  }
  return ret;
}

int ObMviewPendingTaskScheduler::process_one_task()
{
  int ret = OB_SUCCESS;
  ObMViewPendingTask task;
  ObAddr leader_addr;
  int task_ret = OB_SUCCESS;
  if (OB_ISNULL(manager_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("manager is null", KR(ret), K(manager_));
  } else if (OB_FAIL(check_concurrent_limit(MTL_ID()))) {
    if (OB_EAGAIN != ret) {
      LOG_WARN("check concurrent limit failed", KR(ret));
    }
  } else if (OB_FAIL(manager_->peek_task(task))) {
    if (OB_EAGAIN != ret) {
      LOG_WARN("peek task failed", KR(ret));
    }
  } else if (OB_FAIL(manager_->get_mview_leader_addr(task.tenant_id_, task.mview_id_,
                                                     leader_addr))) {
    LOG_WARN("get mview leader addr failed", KR(ret), K(task));
  } else if (OB_FAIL(manager_->mark_task_running(task.tenant_id_,
                                                 task.refresh_id_,
                                                 task.mview_id_,
                                                 leader_addr))) {
    if (OB_EAGAIN == ret) {
      // Table CAS PENDING→RUNNING failed: on-disk status diverged from memory. Read the
      // actual disk status and align memory (plus any side effect — recovery / retry /
      // recycle) so peek_task no longer returns this entry and the scheduler does not
      // busy-loop.
      int tmp_ret = OB_SUCCESS;
      ret = OB_SUCCESS;
      LOG_WARN("mark task running cas failed, resync memory from disk", K(task));
      if (OB_TMP_FAIL(manager_->resync_task_from_disk(task.tenant_id_,
                                                      task.refresh_id_,
                                                      task.mview_id_))) {
        LOG_WARN("fail to resync task from disk", KR(tmp_ret), K(task));
      }
    } else {
      LOG_WARN("mark task running failed", KR(ret), K(task));
    }
  } else if (OB_FAIL(manager_->run_task(task.tenant_id_,
                                        task.refresh_id_,
                                        task.mview_id_,
                                        task.target_data_sync_scn_,
                                        task.refresh_method_,
                                        task.refresh_parallel_,
                                        task.retry_count_,
                                        task.is_nested_refresh(),
                                        leader_addr))) {
    task_ret = ret;
    ret = OB_SUCCESS;
    if (OB_FAIL(manager_->finalize_task(task.tenant_id_,
                                        task.refresh_id_,
                                        task.mview_id_,
                                        task_ret))) {
      LOG_WARN("finalize task failed after run_task dispatch error", KR(ret), K(task), K(task_ret));
    } else {
      LOG_INFO("pending task finished with failure", K(task_ret), K(task));
    }
  }
  return ret;
}

void ObMviewPendingTaskScheduler::idle_wait()
{
  ObBKGDSessInActiveGuard inactive_guard;
  ObThreadCondGuard guard(idle_cond_);
  if (!has_pending_work_) {
    idle_cond_.wait(10 * 1000);
  }
  has_pending_work_ = false;
}

void ObMviewPendingTaskScheduler::run1()
{
  lib::set_thread_name("MViewSched");
  ObCurTraceId::TraceId trace_id;
  trace_id.init(GCONF.self_addr_);
  ObTraceIdGuard trace_id_guard(trace_id);
  bool reload_done = false;
  while (!has_set_stop() && !reload_done) {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(manager_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("manager is null", KR(ret));
      reload_done = true;
    } else if (OB_FAIL(manager_->reload_tasks())) {
      LOG_WARN("fail to reload tasks on startup, will retry", KR(ret));
      idle_wait();
    } else {
      manager_->on_reload_done();
      LOG_INFO("reload tasks done", KR(ret));
      reload_done = true;
    }
  }
  while (!has_set_stop()) {
    int ret = OB_SUCCESS;
    bool need_idle = false;
    if (OB_FAIL(manager_->process_task_results(
            ObMViewPendingTaskManager::MAX_PROCESS_RPC_RESULT_CNT))) {
      LOG_WARN("process task results failed", KR(ret));
      ret = OB_SUCCESS;
    }
    while (!has_set_stop() && !need_idle) {
      if (OB_FAIL(process_one_task())) {
        if (OB_EAGAIN == ret) {
          need_idle = true;
        } else {
          LOG_WARN("process one pending task failed", KR(ret));
        }
        ret = OB_SUCCESS;
      }
    }
    if (!has_set_stop() && need_idle && !manager_->has_pending_results()) {
      idle_wait();
    }
  }
}

} // namespace rootserver
} // namespace oceanbase
