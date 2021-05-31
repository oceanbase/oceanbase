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

#define USING_LOG_PREFIX SQL_EXE

#include "sql/executor/ob_executor.h"
#include "lib/stat/ob_diagnose_info.h"
#include "sql/executor/ob_distributed_scheduler.h"
#include "sql/executor/ob_remote_scheduler.h"
#include "sql/executor/ob_local_scheduler.h"
#include "sql/executor/ob_task_executor_ctx.h"
#include "sql/executor/ob_execute_result.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_operator.h"
#include "sql/engine/table/ob_table_scan.h"
#include "lib/profile/ob_perf_event.h"
#include "sql/executor/ob_transmit.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;

ObExecutor::ObExecutor() : inited_(false), phy_plan_(NULL), execution_id_(OB_INVALID_ID)
{
  /* add your code here */
}

int ObExecutor::init(ObPhysicalPlan* plan)
{
  int ret = OB_SUCCESS;
  if (true == inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("executor is inited twice", K(ret));
  } else if (OB_ISNULL(plan)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("plan is NULL", K(ret));
  } else {
    phy_plan_ = plan;
    inited_ = true;
  }
  return ret;
}

void ObExecutor::reset()
{
  inited_ = false;
  phy_plan_ = NULL;
  execution_id_ = OB_INVALID_ID;
}

int ObExecutor::execute_plan(ObExecContext& ctx)
{
  NG_TRACE(exec_plan_begin);
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx& task_exec_ctx = ctx.get_task_exec_ctx();
  ObExecuteResult& exec_result = task_exec_ctx.get_execute_result();
  ObSQLSessionInfo* session_info = ctx.get_my_session();

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(session_info)) {
    ret = OB_NOT_INIT;
    LOG_WARN("session info is NULL", K(ret));
  } else if (OB_ISNULL(phy_plan_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("phy_plan_ is NULL", K(ret));
  } else if (OB_FAIL(session_info->set_cur_phy_plan(phy_plan_))) {
    LOG_WARN("set extra serialize vars", K(ret));
  } else if (session_info->use_static_typing_engine() &&
             OB_FAIL(phy_plan_->get_expr_frame_info().pre_alloc_exec_memory(ctx))) {
    LOG_WARN("fail to pre allocate memory", K(ret), K(phy_plan_->get_expr_frame_info()));
  } else if (session_info->use_static_typing_engine() && OB_FAIL(ctx.init_eval_ctx())) {
    LOG_WARN("init eval ctx failed", K(ret));
  } else {
    ObPhyPlanType execute_type = phy_plan_->get_plan_type();

    if (phy_plan_->get_need_serial_exec()) {
      session_info->set_need_serial_exec(phy_plan_->get_need_serial_exec());
    }
    // consider this case:
    // MULTI PART INSERT (remote)
    //   SELECT (local)
    // the plan type is OB_PHY_PLAN_DISTRIBUTED, but need schedule as LOCAL plan.
    if (execute_type != OB_PHY_PLAN_LOCAL && phy_plan_->is_require_local_execution()) {
      execute_type = OB_PHY_PLAN_LOCAL;
      LOG_TRACE("change the plan execution type", "fact", execute_type, K(phy_plan_->get_plan_type()));
    }

    switch (execute_type) {
      case OB_PHY_PLAN_LOCAL: {
        EVENT_INC(SQL_LOCAL_COUNT);
        if (NULL == phy_plan_->get_root_op_spec()) {
          // root operator spec is NULL, old plan
          exec_result.set_root_op(phy_plan_->get_main_query());
        } else {
          ObOperator* op = NULL;
          if (OB_FAIL(phy_plan_->get_root_op_spec()->create_operator(ctx, op))) {
            LOG_WARN("create operator from spec failed", K(ret));
          } else if (OB_ISNULL(op)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("created operator is NULL", K(ret));
          } else {
            exec_result.set_static_engine_root(op);
          }
        }
        break;
      }
      case OB_PHY_PLAN_REMOTE:
        EVENT_INC(SQL_REMOTE_COUNT);
        ret = execute_remote_single_partition_plan(ctx);
        break;
      case OB_PHY_PLAN_DISTRIBUTED:
        EVENT_INC(SQL_DISTRIBUTED_COUNT);
        if (phy_plan_->is_use_px()) {
          // ObPxCoord will do schedule job.
          if (NULL != phy_plan_->get_root_op_spec()) {
            ret = execute_static_cg_px_plan(ctx);
          } else {
            ret = execute_old_px_plan(ctx);
          }
        } else {
          if (OB_FAIL(task_exec_ctx.reset_and_init_stream_handler())) {
            LOG_WARN("init stream handler failed", K(ret));
          } else {
            // user var & distributed => not supported
            if (phy_plan_->is_contains_assignment()) {
              ret = OB_ERR_DISTRIBUTED_NOT_SUPPORTED;
              LOG_USER_ERROR(OB_ERR_DISTRIBUTED_NOT_SUPPORTED, "user variable assignment in distributed plan");
            } else {
              ret = execute_distributed_plan(ctx);
            }
          }
        }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        break;
    }
  }
  NG_TRACE(exec_plan_end);
  return ret;
}

int ObExecutor::execute_local_single_partition_plan(ObExecContext& ctx)
{
  int ret = OB_SUCCESS;
  ObLocalScheduler scheduler;
  if (OB_ISNULL(phy_plan_)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    ret = scheduler.schedule(ctx, phy_plan_);
  }
  return ret;
}

int ObExecutor::execute_remote_single_partition_plan(ObExecContext& ctx)
{
  ObRemoteScheduler scheduler;
  return scheduler.schedule(ctx, phy_plan_);
}

int ObExecutor::execute_distributed_plan(ObExecContext& ctx)
{
  int ret = OB_SUCCESS;
  OB_ASSERT(NULL != phy_plan_);
  ObDistributedSchedulerManager* scheduler_manager = NULL;
  ObPhysicalPlanCtx* phy_plan_ctx = ctx.get_physical_plan_ctx();
  int64_t remain_time_us = 0;
  int64_t now = ::oceanbase::common::ObTimeUtility::current_time();
  if (OB_ISNULL(phy_plan_) || OB_ISNULL(phy_plan_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plan or context is NULL", K_(phy_plan), K(phy_plan_ctx));
  } else if (OB_UNLIKELY((remain_time_us = phy_plan_ctx->get_timeout_timestamp() - now) <= 0)) {
    ret = OB_TIMEOUT;
    LOG_WARN("timeout", K(ret), K(remain_time_us), K(now), "timeout_timestamp", phy_plan_ctx->get_timeout_timestamp());
  } else if (OB_ISNULL(scheduler_manager = ObDistributedSchedulerManager::get_instance())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get ObDistributedSchedulerManager instance", K(ret));
  } else if (OB_FAIL(scheduler_manager->alloc_scheduler(ctx, execution_id_))) {
    execution_id_ = OB_INVALID_ID;
    LOG_WARN("fail to alloc scheduler", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == execution_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("succeed to alloc but execution id is invalid", K(execution_id_), K(ret));
  } else if (OB_FAIL(scheduler_manager->parse_jobs_and_start_sche_thread(
                 execution_id_, ctx, phy_plan_, phy_plan_ctx->get_timeout_timestamp()))) {
    LOG_WARN("fail to schedule", K(ret));
  } else {
    ctx.set_execution_id(execution_id_);
  }
  return ret;
}

int ObExecutor::execute_static_cg_px_plan(ObExecContext& ctx)
{
  int ret = OB_SUCCESS;
  ObOperator* op = NULL;
  if (OB_FAIL(phy_plan_->get_root_op_spec()->create_op_input(ctx))) {
    LOG_WARN("create input from spec failed", K(ret));
  } else if (OB_FAIL(phy_plan_->get_root_op_spec()->create_operator(ctx, op))) {
    LOG_WARN("create operator from spec failed", K(ret));
  } else if (OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("created operator is NULL", K(ret));
  } else {
    /**
     * FIXME
     * these codes are ugly
     */
    ObSEArray<const ObTableScanSpec*, 8> scan_ops;
    // pre query range and init scan input (for compatible)
    if (OB_FAIL(ObTaskSpliter::find_scan_ops(scan_ops, *phy_plan_->get_root_op_spec()))) {
      LOG_WARN("fail get scan ops", K(ret));
    } else {
      ARRAY_FOREACH_X(scan_ops, idx, cnt, OB_SUCC(ret))
      {
        if (OB_ISNULL(scan_ops.at(idx))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL scan op ptr unexpected", K(ret));
        } else {
          ObOperatorKit* kit = ctx.get_operator_kit(scan_ops.at(idx)->get_id());
          if (OB_ISNULL(kit) || OB_ISNULL(kit->input_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("operator is NULL", K(ret), KP(kit));
          } else {
            ObTableScanOpInput* scan_input = static_cast<ObTableScanOpInput*>(kit->input_);
            // hard code idx to 0
            scan_input->set_location_idx(0);
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    ctx.get_task_executor_ctx()->get_execute_result().set_static_engine_root(op);
  }
  return ret;
}

int ObExecutor::execute_old_px_plan(ObExecContext& ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObJobControl::alloc_phy_op_input(ctx, phy_plan_->get_main_query()))) {
    LOG_WARN("fail alloc all op input", K(ret));
  } else {
    ObSEArray<const ObTableScan*, 8> scan_ops;
    // pre query range and init scan input (for compatible)
    if (OB_FAIL(ObTaskSpliter::find_scan_ops(scan_ops, *phy_plan_->get_main_query()))) {
      LOG_WARN("fail get scan ops", K(ret));
    } else {
      ARRAY_FOREACH_X(scan_ops, idx, cnt, OB_SUCC(ret))
      {
        ObTableScanInput* tsc_input = NULL;
        if (OB_ISNULL(scan_ops.at(idx))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL scan op ptr unexpected", K(ret));
        } else if (OB_ISNULL(tsc_input = GET_PHY_OP_INPUT(ObTableScanInput, ctx, scan_ops.at(idx)->get_id()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("can't get tsc op input", K(ret));
        } else {
          // hard code idx to 0
          tsc_input->set_location_idx(0);
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    ctx.get_task_exec_ctx().get_execute_result().set_root_op(phy_plan_->get_main_query());
  }
  return ret;
}

int ObExecutor::close(ObExecContext& ctx)
{
  // close() may be called anytime, so ignore inited_.
  int ret = OB_SUCCESS;
  ObSQLSessionInfo* session_info = ctx.get_my_session();
  if (OB_LIKELY(NULL != session_info)) {
    session_info->reset_cur_phy_plan_to_null();
  }
  if (OB_LIKELY(NULL != phy_plan_)) {
    ObPhyPlanType execute_type = phy_plan_->get_plan_type();
    switch (execute_type) {
      case OB_PHY_PLAN_LOCAL:
      case OB_PHY_PLAN_REMOTE:
        break;
      case OB_PHY_PLAN_DISTRIBUTED: {
        int free_ret = OB_SUCCESS;
        ObDistributedSchedulerManager* scheduler_manager = NULL;
        if (phy_plan_->is_use_px()) {
          // do nothing
        } else if (OB_UNLIKELY(OB_INVALID_ID == execution_id_)) {
          // fail to alloc distributed scheduler, do nothing
          LOG_WARN("fail to alloc distributed scheduler, do nothing", K(ret), K(execution_id_));
        } else if (OB_UNLIKELY(NULL == (scheduler_manager = ObDistributedSchedulerManager::get_instance()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("fail to get ObDistributedSchedulerManager instance", K(ret));
        } else {
          if (OB_FAIL(scheduler_manager->close_scheduler(ctx, execution_id_))) {
            LOG_WARN("fail to close scheduler", K(ret), K(execution_id_));
          }
          if (OB_SUCCESS != (free_ret = scheduler_manager->free_scheduler(execution_id_))) {
            ret = (OB_SUCCESS == ret) ? free_ret : ret;
            LOG_ERROR("fail to free scheduler", K(ret), K(free_ret));
          }
        }
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid execute_type", K(ret), K(execute_type));
        break;
    }
  }
  return ret;
}
