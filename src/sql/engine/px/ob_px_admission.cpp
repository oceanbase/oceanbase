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
#include "observer/omt/ob_tenant_config_mgr.h"
#include "observer/omt/ob_th_worker.h"
#include "observer/omt/ob_tenant.h"
#include "ob_px_target_mgr.h"
#include "ob_px_util.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

int ObPxAdmission::get_parallel_session_target(ObSQLSessionInfo &session,
                                               int64_t minimal_session_target,
                                               int64_t &session_target)
{
  int ret = OB_SUCCESS;
  int64_t parallel_servers_target = INT64_MAX; // default to unlimited
  session_target = INT64_MAX; // default to unlimited
  uint64_t tenant_id = session.get_effective_tenant_id();
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  if (OB_FAIL(OB_PX_TARGET_MGR.get_parallel_servers_target(tenant_id, parallel_servers_target))) {
    LOG_WARN("get parallel_servers_target failed", K(ret));
  } else if (OB_LIKELY(tenant_config.is_valid())) {
    int64_t pmas = tenant_config->_parallel_max_active_sessions;
    int64_t parallel_session_count;
    if (OB_FAIL(OB_PX_TARGET_MGR.get_parallel_session_count(tenant_id, parallel_session_count))) {
      LOG_WARN("get parallel_px_session failed", K(ret));
    } else if (pmas > 0 && parallel_servers_target != INT64_MAX && parallel_session_count > 0) {
      // when pmas is TOO large, session target could be less than one,
      // this is not good! We ensure this query can run with minimal threads here
      session_target = std::max(parallel_servers_target / pmas, minimal_session_target);
    }
  } else if (OB_UNLIKELY(minimal_session_target > parallel_servers_target)) {
    ret = OB_ERR_PARALLEL_SERVERS_TARGET_NOT_ENOUGH;
    LOG_WARN("minimal_session_target is more than parallel_servers_target", K(ret),
                                      K(minimal_session_target), K(parallel_servers_target));
  } else {
    // tenant_config is invalid, use parallel_servers_target
    session_target = parallel_servers_target;
  }
  LOG_TRACE("PX get parallel session target", K(tenant_id), K(tenant_config.is_valid()),
                        K(parallel_servers_target), K(minimal_session_target), K(session_target));
  return ret;
}

// 如果当前剩余线程数能满足 req_cnt，则分配线程给请求
// 但考虑到系统空闲时，要允许第一个请求执行，需要处理下面的特殊情况：
//   如果 请求的线程数 req_cnt 大于 limit，并且当前没有其它 px 请求（used = 0）
//   那么 就把所有线程分配给这个请求 (admit_cnt = req_cnt, used = limit)
//
//   推论：一个需要**过量**线程的请求，只会在系统空闲下来之后才会被调度
int64_t ObPxAdmission::admit(ObSQLSessionInfo &session, ObExecContext &exec_ctx,
                             int64_t wait_time_us, int64_t session_target,
                             ObHashMap<ObAddr, int64_t> &worker_map,
                             int64_t req_cnt, int64_t &admit_cnt)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = session.get_effective_tenant_id();
  uint64_t admission_version = UINT64_MAX;
  // when pmas enabled, block thread until got expected thread resource
  int64_t left_time_us = wait_time_us;
  int64_t start_time_us = ObClockGenerator::getClock();
  bool need_retry = false;
  do {
    if (OB_FAIL(THIS_WORKER.check_status())) {
      LOG_WARN("fail check query status", K(ret));
    } else if (OB_FAIL(OB_PX_TARGET_MGR.apply_target(tenant_id, worker_map, wait_time_us, session_target, req_cnt, admit_cnt, admission_version))) {
      LOG_WARN("apply target failed", K(ret), K(tenant_id), K(req_cnt));
    } else if (0 != admit_cnt) {
      exec_ctx.set_admission_version(admission_version);
      LOG_TRACE("after enter admission", K(ret), K(req_cnt), K(admit_cnt));
    }
    left_time_us = wait_time_us - (ObClockGenerator::getClock() - start_time_us);
    if (OB_SUCC(ret) && 0 == admit_cnt && left_time_us > 0) {
      if (!need_retry) {
        // only print once
        LOG_INFO("Not enough PX thread to execute query."
                 "should wait and re-acquire thread resource from target queue",
                 K(req_cnt), K(left_time_us));
        // fake one retry record, not really a query retry
        session.get_retry_info_for_update().set_last_query_retry_err(OB_ERR_INSUFFICIENT_PX_WORKER);
      }
      need_retry = true;
    } else {
      need_retry = false;
    }
  } while (need_retry);
  return ret;
}

int ObPxAdmission::enter_query_admission(ObSQLSessionInfo &session,
                                         ObExecContext &exec_ctx,
                                         sql::stmt::StmtType stmt_type,
                                         ObPhysicalPlan &plan)
{
  int ret = OB_SUCCESS;
  // 对于只有dop=1 的场景跳过检查，因为这种场景走 RPC 线程，不消耗 PX 线程
  //
  if (stmt::T_EXPLAIN != stmt_type
      && plan.is_use_px()
      && 1 != plan.get_px_dop()
      && plan.get_expected_worker_count() > 0) {
    // use for appointment
    const auto &req_px_worker_map = plan.get_expected_worker_map();
    ObHashMap<ObAddr, int64_t> &acl_px_worker_map = exec_ctx.get_admission_addr_map();
    if (acl_px_worker_map.created()) {
      acl_px_worker_map.clear();
    } else if (OB_FAIL(acl_px_worker_map.create(hash::cal_next_prime(10), ObModIds::OB_SQL_PX, ObModIds::OB_SQL_PX))){
      LOG_WARN("create hash map failed", K(ret));
    }
    if (OB_SUCC(ret)) {
      for (auto it = req_px_worker_map.begin();
          OB_SUCC(ret) && it != req_px_worker_map.end(); ++it) {
        if (OB_FAIL(acl_px_worker_map.set_refactored(it->first, it->second))){
          LOG_WARN("set refactored failed", K(ret), K(it->first), K(it->second));
        }
      }
      // use for exec
      int64_t req_worker_count = plan.get_expected_worker_count();
      int64_t minimal_px_worker_count = plan.get_minimal_worker_count();
      int64_t admit_worker_count = 0;
      // 如果一直得不到线程资源，需要超时退出。
      // 下面处理带 timeout hint 的情景
      if (plan.get_phy_plan_hint().query_timeout_ > 0) {
        THIS_WORKER.set_timeout_ts(
            session.get_query_start_time() + plan.get_phy_plan_hint().query_timeout_);
      }
      int64_t wait_time_us = THIS_WORKER.get_timeout_remain();
      int64_t session_target = INT64_MAX;
      if (OB_FAIL(get_parallel_session_target(session, minimal_px_worker_count, session_target))) {
        LOG_WARN("fail get session target", K(ret));
      } else if (OB_FAIL(THIS_WORKER.check_status())) {
        LOG_WARN("fail check query status", K(ret));
      } else if (OB_FAIL(ObPxAdmission::admit(session, exec_ctx,
                                              wait_time_us, session_target,
                                              acl_px_worker_map, req_worker_count, admit_worker_count))) {
        LOG_WARN("fail do px admission",
                K(ret), K(wait_time_us), K(session_target));
      } else if (admit_worker_count <= 0) {
        plan.inc_delayed_px_querys();
        ret = OB_ERR_INSUFFICIENT_PX_WORKER;
        LOG_INFO("It's a px query, out of px worker resource, "
                "need delay, do not need disconnect",
                K(admit_worker_count),
                K(plan.get_px_dop()),
                K(plan.get_plan_id()),
                K(ret));
      } else {
        ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(exec_ctx);
        ObTaskExecutorCtx *task_exec_ctx = GET_TASK_EXECUTOR_CTX(exec_ctx);
        if (OB_ISNULL(plan_ctx) || OB_ISNULL(task_exec_ctx)) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          plan_ctx->set_worker_count(admit_worker_count);
          // 表示 optimizer 计算的数量
          task_exec_ctx->set_expected_worker_cnt(req_worker_count);
          task_exec_ctx->set_minimal_worker_cnt(minimal_px_worker_count);
          // 表示 admission 根据当前资源排队情况实际分配的数量
          task_exec_ctx->set_admited_worker_cnt(admit_worker_count);
        }
        LOG_TRACE("PX admission set the plan worker count", K(req_worker_count), K(minimal_px_worker_count), K(admit_worker_count));
      }
    }
  }
  return ret;
}

void ObPxAdmission::exit_query_admission(ObSQLSessionInfo &session,
                                         ObExecContext &exec_ctx,
                                         sql::stmt::StmtType stmt_type,
                                         ObPhysicalPlan &plan)
{
  if (stmt::T_EXPLAIN != stmt_type
      && plan.is_use_px()
      && 1 != plan.get_px_dop()
      && exec_ctx.get_admission_version() != UINT64_MAX) {
    int ret = OB_SUCCESS;
    uint64_t tenant_id = session.get_effective_tenant_id();
    hash::ObHashMap<ObAddr, int64_t> &addr_map = exec_ctx.get_admission_addr_map();
    if (OB_FAIL(OB_PX_TARGET_MGR.release_target(tenant_id,
                                                addr_map,
                                                exec_ctx.get_admission_version()))) {
      LOG_WARN("release target failed", K(ret), K(tenant_id), K(exec_ctx.get_admission_version()));
    }
    (void)addr_map.destroy();
    LOG_DEBUG("release resource, notify wait threads");
  }
}

// 供给 SQC 端使用的 Admission 模块
// 每个租户一个资源池
void ObPxSubAdmission::acquire(int64_t max, int64_t min, int64_t &acquired_cnt)
{
  UNUSED(min);
  oceanbase::omt::ObTenant *tenant = nullptr;
  oceanbase::omt::ObThWorker *worker = nullptr;
  int64_t upper_bound = 1;
  if (nullptr == (worker = THIS_THWORKER_SAFE)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "Oooops! can't find tenant. Unexpected!", K(max), K(min));
  } else if (nullptr == (tenant = worker->get_tenant())) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "Oooops! can't find tenant. Unexpected!", KP(worker), K(max), K(min));
  } else {
    oceanbase::omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant->id()));
    if (!tenant_config.is_valid()) {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "get tenant config failed, use default cpu_quota_concurrency");
      upper_bound = tenant->unit_min_cpu() * 4;
    } else {
      upper_bound = tenant->unit_min_cpu() * tenant_config->px_workers_per_cpu_quota;
    }
  }
  acquired_cnt = std::min(max, upper_bound);
}

void ObPxSubAdmission::release(int64_t acquired_cnt)
{
  UNUSED(acquired_cnt);
}
