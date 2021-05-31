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

#define USING_LOG_PREFIX SQL
#include "ob_px_admission.h"
#include "share/config/ob_server_config.h"
#include "observer/mysql/obmp_query.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

// assign worker if we can satisfy req_cnt
bool ObPxAdmission::admit(int64_t req_cnt, int64_t& admit_cnt)
{
  auto stat = MTL_GET(ObPxPoolStat*);
  if (OB_ISNULL(stat)) {
    admit_cnt = 0;
    LOG_ERROR("can't get stat from MTL");
  } else {
    stat->acquire_parallel_servers_target(req_cnt, admit_cnt);
  }
  LOG_TRACE("after enter admission", K(req_cnt), K(admit_cnt));
  return 0 != admit_cnt;
}

void ObPxAdmission::release(int64_t req_cnt)
{
  auto stat = MTL_GET(ObPxPoolStat*);
  if (OB_ISNULL(stat)) {
    LOG_ERROR("can't get stat from MTL");
  } else {
    stat->release_parallel_servers_target(req_cnt);
  }
  LOG_TRACE("after exit admission", K(req_cnt));
}

int ObPxAdmission::enter_query_admission(
    ObSQLSessionInfo& session, ObExecContext& exec_ctx, ObPhysicalPlan& plan, int64_t& worker_count)
{
  int ret = OB_SUCCESS;
  int64_t calculated_px_worker_count = plan.get_expected_worker_count();
  int64_t req_worker_count = 0;

  if (1 == plan.get_px_dop()) {
    // no check for single dop scheduler,  which execute in RPC thread, no need px thread
    req_worker_count = 0;
  } else {
    // expected_worker_count_ always be -1 for plan cache
    req_worker_count = calculated_px_worker_count;
  }

  if (plan.is_use_px() && req_worker_count > 0) {
    int64_t admit_worker_count = 0;
    // Strategy:
    //       1. Large query for px execution, (see THIS_WORKER.check_large_query_quota)
    //       2. Wait thread until timeout
    if (plan.get_query_hint().query_timeout_ > 0) {
      THIS_WORKER.set_timeout_ts(session.get_query_start_time() + plan.get_query_hint().query_timeout_);
    }
    if (OB_FAIL(THIS_WORKER.check_status())) {
      LOG_WARN("fail check query status", K(ret));
    } else if (!ObPxAdmission::admit(req_worker_count, admit_worker_count)) {
      plan.inc_delayed_px_querys();
      THIS_WORKER.set_retry_flag();
      ret = OB_EAGAIN;
      LOG_INFO("It's a px query, out of px worker resource, "
               "need delay, do not need disconnect",
          K(req_worker_count),
          K(admit_worker_count),
          K(plan.get_px_dop()),
          K(plan.get_plan_id()),
          K(ret));
    } else {
      worker_count = admit_worker_count;
      ObPhysicalPlanCtx* plan_ctx = GET_PHY_PLAN_CTX(exec_ctx);
      ObTaskExecutorCtx* task_exec_ctx = GET_TASK_EXECUTOR_CTX(exec_ctx);
      if (OB_ISNULL(plan_ctx) || OB_ISNULL(task_exec_ctx)) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        plan_ctx->set_worker_count(worker_count);
        task_exec_ctx->set_expected_worker_cnt(req_worker_count);
        task_exec_ctx->set_allocated_worker_cnt(worker_count);
      }
      LOG_TRACE("PX admission set the plan worker count", K(req_worker_count), K(worker_count));
    }
  } else {
    worker_count = 0;
    // work count in task_exec_ctx default 0
  }
  return ret;
}

void ObPxAdmission::exit_query_admission(int64_t worker_count)
{
  if (OB_UNLIKELY(worker_count > 0)) {
    ObPxAdmission::release(worker_count);
  }
}

void ObPxPoolStat::acquire_max_parallel_servers(int64_t max, int64_t min, int64_t& acquired_cnt)
{
  SpinWLockGuard guard(max_parallel_servers_lock_);
  int64_t free = pool_size_ - max_parallel_servers_used_;
  if (OB_UNLIKELY(free < min)) {
    acquired_cnt = 0;
    LOG_TRACE("can't acquire any thread res", K(free), K(pool_size_), K(max_parallel_servers_used_), K(min), K(max));
  } else {
    acquired_cnt = std::min(free, max);
    max_parallel_servers_used_ += acquired_cnt;
  }
}

void ObPxPoolStat::release_max_parallel_servers(int64_t acquired_cnt)
{
  SpinWLockGuard guard(max_parallel_servers_lock_);
  max_parallel_servers_used_ -= acquired_cnt;
}

void ObPxPoolStat::set_pool_size(int64_t size)
{
  SpinWLockGuard guard(max_parallel_servers_lock_);
  pool_size_ = size;
}

void ObPxPoolStat::acquire_parallel_servers_target(int64_t max, int64_t& acquired_cnt)
{
  SpinWLockGuard guard(parallel_servers_target_lock_);
  if (0 == parallel_servers_target_used_) {
    acquired_cnt = std::min(max, target_);
    parallel_servers_target_used_ += acquired_cnt;
  } else if (max <= target_ - parallel_servers_target_used_) {
    acquired_cnt = max;
    parallel_servers_target_used_ += acquired_cnt;
  } else {
    acquired_cnt = 0;
    LOG_TRACE("can't acquire any thread res", K(max), K(target_), K(pool_size_), K(parallel_servers_target_used_));
  }
}

void ObPxPoolStat::release_parallel_servers_target(int64_t acquired_cnt)
{
  if (acquired_cnt > 0) {
    SpinWLockGuard guard(parallel_servers_target_lock_);
    parallel_servers_target_used_ -= acquired_cnt;
  }
}

void ObPxPoolStat::set_target(int64_t size)
{
  SpinWLockGuard guard(parallel_servers_target_lock_);
  target_ = size;
}

void ObPxSubAdmission::acquire(int64_t max, int64_t min, int64_t& acquired_cnt)
{
  UNUSED(min);
  /*
  auto stat = MTL_GET(ObPxPoolStat*);
  if (OB_ISNULL(stat)) {
    acquired_cnt = 0; // can not acquire any
    LOG_ERROR("can't get stat from MTL");
  } else {
    stat->acquire_max_parallel_servers(max, min, acquired_cnt);
    if (acquired_cnt <= 0) {
      LOG_WARN("no free thread resource",
               "tenant_id", oceanbase::lib::current_tenant_id(),
               K(*stat));
    }
  }
  */
  acquired_cnt = max;
}

void ObPxSubAdmission::release(int64_t acquired_cnt)
{
  UNUSED(acquired_cnt);
  /*
  auto stat = MTL_GET(ObPxPoolStat*);
  if (OB_ISNULL(stat)) {
    LOG_ERROR("can't get stat from MTL");
  } else {
    stat->release_max_parallel_servers(acquired_cnt);
  }
  */
}
