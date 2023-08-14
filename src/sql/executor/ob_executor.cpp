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
#include "sql/executor/ob_remote_scheduler.h"
#include "sql/executor/ob_task_executor_ctx.h"
#include "sql/executor/ob_execute_result.h"
#include "sql/executor/ob_task_spliter.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_operator.h"
#include "lib/profile/ob_perf_event.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;

int ObExecutor::init(ObPhysicalPlan *plan)
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

int ObExecutor::execute_plan(ObExecContext &ctx)
{
  NG_TRACE(exec_plan_begin);
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx &task_exec_ctx = ctx.get_task_exec_ctx();
  ObExecuteResult &exec_result = task_exec_ctx.get_execute_result();
  ObSQLSessionInfo *session_info = ctx.get_my_session();
  ObPhysicalPlanCtx *plan_ctx = ctx.get_physical_plan_ctx();
  int64_t batched_stmt_cnt = ctx.get_sql_ctx()->multi_stmt_item_.get_batched_stmt_cnt();
  ctx.set_use_temp_expr_ctx_cache(true);
  // If the batch execution is rewritten by insert multi values, there is no need to repack multiple times
  if (ctx.get_sql_ctx()->is_do_insert_batch_opt()) {
    batched_stmt_cnt = 0;
  }
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
  } else if (OB_FAIL(phy_plan_->get_expr_frame_info()
                                 .pre_alloc_exec_memory(ctx))) {
    LOG_WARN("fail to pre allocate memory", K(ret), K(phy_plan_->get_expr_frame_info()));
  } else if (batched_stmt_cnt > 0
      && OB_FAIL(plan_ctx->create_implicit_cursor_infos(batched_stmt_cnt))) {
    LOG_WARN("create implicit cursor infos failed", K(ret), K(batched_stmt_cnt));
  } else {
    ObPhyPlanType execute_type = phy_plan_->get_plan_type();

    // 特殊处理如下case：
    // MULTI PART INSERT (remote)
    //   SELECT (local)
    // 这样的计划在优化器生成阶段，plan type是OB_PHY_PLAN_DISTRIBUTED，
    // 但是需要使用local的方式进行执行调度
    if (execute_type != OB_PHY_PLAN_LOCAL && phy_plan_->is_require_local_execution()) {
      execute_type = OB_PHY_PLAN_LOCAL;
      LOG_TRACE("change the plan execution type",
          "fact", execute_type, K(phy_plan_->get_plan_type()));
    }

    switch (execute_type) {
      case OB_PHY_PLAN_LOCAL: {
        EVENT_INC(SQL_LOCAL_COUNT);
        ObOperator *op = NULL;
        if (OB_FAIL(phy_plan_->get_root_op_spec()->create_operator(ctx, op))) {
          LOG_WARN("create operator from spec failed", K(ret));
        } else if (OB_ISNULL(op)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("created operator is NULL", K(ret));
        } else {
          exec_result.set_static_engine_root(op);
        }
        break;
      }
      case OB_PHY_PLAN_REMOTE:
        EVENT_INC(SQL_REMOTE_COUNT);
        ret = execute_remote_single_partition_plan(ctx);
        break;
      case OB_PHY_PLAN_DISTRIBUTED:
        EVENT_INC(SQL_DISTRIBUTED_COUNT);
        // PX 特殊路径
        // PX 模式下，调度工作由 ObPxCoord 算子负责
        ret = execute_static_cg_px_plan(ctx);
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        break;
    }
  }
  NG_TRACE(exec_plan_end);
  return ret;
}

int ObExecutor::execute_remote_single_partition_plan(ObExecContext &ctx)
{
  ObRemoteScheduler scheduler;
  return scheduler.schedule(ctx, phy_plan_);
}

int ObExecutor::execute_static_cg_px_plan(ObExecContext &ctx)
{
  int ret = OB_SUCCESS;
  ObOperator *op = NULL;
  if (OB_FAIL(phy_plan_->get_root_op_spec()->create_op_input(ctx))) {
    LOG_WARN("create input from spec failed", K(ret));
  } else if (OB_FAIL(phy_plan_->get_root_op_spec()->create_operator(ctx, op))) {
    LOG_WARN("create operator from spec failed", K(ret));
  } else if (OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("created operator is NULL", K(ret));
  } else {
    ctx.get_task_executor_ctx()
        ->get_execute_result()
        .set_static_engine_root(op);
  }
  return ret;
}

int ObExecutor::close(ObExecContext &ctx)
{
  // close函数要设计成不管什么时候调都可以，因此不管inited_的值
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session_info = ctx.get_my_session();
  if (OB_LIKELY(NULL != session_info)) {
    //将session中的cur_phy_plan_重置为NULL
    session_info->reset_cur_phy_plan_to_null();
  }
  return ret;
}
