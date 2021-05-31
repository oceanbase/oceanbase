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

#define USING_LOG_PREFIX SQL_ENG
#include "ob_px_task_process.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/engine/px/ob_px_dtl_msg.h"
#include "sql/engine/px/ob_granule_iterator.h"
#include "sql/engine/px/ob_granule_iterator_op.h"
#include "sql/engine/px/ob_px_exchange.h"
#include "sql/dtl/ob_dtl_channel_group.h"
#include "observer/ob_server.h"
#include "sql/executor/ob_task_executor_ctx.h"
#include "lib/stat/ob_session_stat.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/executor/ob_executor_rpc_processor.h"
#include "sql/engine/px/ob_px_worker_stat.h"
#include "sql/engine/px/ob_px_interruption.h"
#include "share/rc/ob_context.h"
#include "sql/engine/px/ob_px_sqc_handler.h"
#include "sql/engine/px/exchange/ob_px_transmit.h"
#include "sql/engine/px/exchange/ob_px_receive.h"
#include "sql/engine/px/exchange/ob_px_transmit_op.h"
#include "sql/engine/px/exchange/ob_px_receive_op.h"
#include "sql/engine/dml/ob_table_insert_op.h"
#include "sql/ob_sql_mock_schema_utils.h"
#include "sql/engine/px/ob_px_basic_info.h"
#include "sql/engine/pdml/static/ob_px_multi_part_modify_op.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::lib;
using namespace oceanbase::sql;
using namespace oceanbase::sql::dtl;

ObPxTaskProcess::ObPxTaskProcess(const observer::ObGlobalContext& gctx, ObPxRpcInitTaskArgs& arg)
    : gctx_(gctx),
      arg_(arg),
      sql_ctx_(),
      schema_guard_(),
      vt_iter_factory_(*gctx.vt_iter_creator_),
      enqueue_timestamp_(0),
      process_timestamp_(0),
      exec_start_timestamp_(0),
      exec_end_timestamp_(0),
      is_oracle_mode_(false),
      partition_location_cache_()
{}

ObPxTaskProcess::~ObPxTaskProcess()
{}

int ObPxTaskProcess::check_inner_stat()
{
  int ret = OB_SUCCESS;
  ObExecContext* exec_ctx = NULL;
  ObSQLSessionInfo* session = NULL;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  if (arg_.is_invalid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("des phy plan is null", K(ret), K(arg_.des_phy_plan_), K(arg_.op_root_), K(arg_.op_spec_root_));
  } else if (OB_ISNULL(exec_ctx = arg_.exec_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exec ctx is NULL", K(ret), K(exec_ctx));
  } else if (OB_ISNULL(session = exec_ctx->get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret), K(exec_ctx));
  } else if (OB_ISNULL(plan_ctx = exec_ctx->get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("phy plan ctx is NULL", K(ret), K(exec_ctx));
  } else {
    // In order to determine the task timeout as soon as possible,
    // the timeout period for starting the task is very short, about 10ms
    // This will cause any internal query initiated in the after process to quickly time out 10ms
    // So here you need to reset the timeout to the actual query timeout time
    THIS_WORKER.set_timeout_ts(plan_ctx->get_timeout_timestamp());
    // Set up the diagnosis function environment
    ObSessionStatEstGuard stat_est_guard(session->get_effective_tenant_id(), session->get_sessid());
  }
  return ret;
}

void ObPxTaskProcess::operator()(void)
{
  (void)process();
}

// The Tenant Px Pool will invoke this function
void ObPxTaskProcess::run()
{
  int ret = OB_SUCCESS;
  share::CompatModeGuard g(is_oracle_mode() ? share::ObWorker::CompatMode::ORACLE : share::ObWorker::CompatMode::MYSQL);

  LOG_TRACE("Px task get sql mode", K(is_oracle_mode()));

  LOG_TRACE("begin process task", KP(this));
  ObPxWorkerStat stat;
  stat.init(get_session_id(),
      get_tenant_id(),
      ObCurTraceId::get(),
      get_qc_id(),
      get_sqc_id(),
      get_worker_id(),
      get_dfo_id(),
      ObTimeUtility::current_time(),
      GETTID());
  ObPxWorkerStatList::instance().push(stat);
  ret = process();
  ObPxWorkerStatList::instance().remove(stat);
  LOG_TRACE("end process task", KP(this), K(ret));
  // Tenant Px Pool don't have any feedback, so all error should be handled by interruption.
  UNUSED(ret);
}

int ObPxTaskProcess::process()
{
  int ret = OB_SUCCESS;

  process_timestamp_ = ObTimeUtility::current_time();

  ObExecRecord exec_record;
  ObExecTimestamp exec_timestamp;
  ObWaitEventDesc max_wait_desc;
  ObWaitEventStat total_wait_desc;
  ObSQLSessionInfo* session = (NULL == arg_.exec_ctx_ ? NULL : arg_.exec_ctx_->get_my_session());
  const bool enable_perf_event = lib::is_diagnose_info_enabled();
  bool enable_sql_audit = GCONF.enable_sql_audit;
  if (OB_ISNULL(session)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("session is NULL", K(ret));
  } else if (OB_FAIL(session->store_query_string(ObString::make_string("PX DFO EXECUTING")))) {
    LOG_WARN("store query string to session failed", K(ret));
  } else {
    ObWorkerSessionGuard worker_session_guard(session);
    ObSQLSessionInfo::LockGuard lock_guard(session->get_query_lock());
    session->set_peer_addr(arg_.task_.get_sqc_addr());
    session->set_cur_phy_plan(arg_.des_phy_plan_);
    session->set_thread_id(GETTID());
    arg_.exec_ctx_->set_sqc_handler(arg_.sqc_handler_);

    ObMaxWaitGuard max_wait_guard(enable_perf_event ? &max_wait_desc : NULL);
    ObTotalWaitGuard total_wait_guard(enable_perf_event ? &total_wait_desc : NULL);
    enable_sql_audit = enable_sql_audit && session->get_local_ob_enable_sql_audit();
    if (enable_perf_event) {
      if (enable_sql_audit) {
        exec_record.record_start();
      }
      exec_start_timestamp_ = ObTimeUtility::current_time();
      set_enqueue_timestamp(exec_start_timestamp_);
      arg_.sqc_task_ptr_->task_monitor_info_.record_exec_time_begin();
    }

    if (OB_FAIL(do_process())) {
      LOG_WARN("failed to process", K(get_tenant_id()), K(ret));
    }

    if (enable_perf_event) {
      exec_end_timestamp_ = ObTimeUtility::current_time();
      if (enable_sql_audit) {
        exec_record.record_end();
      }
      record_exec_timestamp(true, exec_timestamp);
      if (NULL != arg_.sqc_task_ptr_) {
        arg_.sqc_task_ptr_->task_monitor_info_.record_exec_time_end();
        arg_.sqc_task_ptr_->task_monitor_info_.record_sched_exec_time_end();
      }
    }

    if (enable_sql_audit) {
      ObPhysicalPlan* phy_plan = arg_.des_phy_plan_;
      if (OB_ISNULL(phy_plan)) {
        LOG_WARN("invalid argument", K(ret), K(phy_plan));
      } else {
        ObAuditRecordData& audit_record = session->get_audit_record();
        audit_record.seq_ = 0;  // don't use now
        audit_record.status_ = (OB_SUCCESS == ret || common::OB_ITER_END == ret) ? obmysql::REQUEST_SUCC : ret;
        audit_record.execution_id_ = GCTX.sql_engine_->get_execution_id();
        audit_record.qc_id_ = get_qc_id();
        audit_record.dfo_id_ = get_dfo_id();
        audit_record.sqc_id_ = get_sqc_id();
        audit_record.worker_id_ = get_worker_id();
        // audit_record.client_addr_ = arg_.task_.sqc_addr_;
        audit_record.affected_rows_ = 0;
        audit_record.return_rows_ = 0;

        exec_record.max_wait_event_ = max_wait_desc;
        exec_record.wait_time_end_ = total_wait_desc.time_waited_;
        exec_record.wait_count_end_ = total_wait_desc.total_waits_;

        audit_record.plan_type_ = phy_plan->get_plan_type();
        audit_record.is_executor_rpc_ = true;
        audit_record.is_inner_sql_ = session->is_inner();
        audit_record.is_hit_plan_cache_ = true;
        audit_record.is_multi_stmt_ = false;

        audit_record.exec_timestamp_ = exec_timestamp;
        audit_record.exec_record_ = exec_record;

        // Accumulated time of update phase
        audit_record.update_stage_stat();

        ObSQLUtils::handle_audit_record(false, EXECUTE_DIST, *session, *arg_.exec_ctx_);
      }
    }
  }

  // please let this function be the end of this code block.
  release();

  return ret;
}

int ObPxTaskProcess::execute(ObPhyOperator& root)
{
  int ret = OB_SUCCESS;
  ObExecContext& ctx = *arg_.exec_ctx_;
  int close_ret = OB_SUCCESS;
  // root op is transmit,not need to call get_next_row,
  // open() driver next operator get_next_row() method
  if (OB_FAIL(root.open(ctx))) {
    LOG_WARN("fail open dfo op", K(ret));
  } else if (OB_ISNULL(ctx.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("phy plan ctx is null", K(ret));
  } else if (OB_ISNULL(arg_.sqc_task_ptr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the sqc task ptr is null", K(ret));
  } else {
    // when pdml,after each task execute,have affected rows
    // need save affected row to sqc task
    arg_.sqc_task_ptr_->set_affected_rows(ctx.get_physical_plan_ctx()->get_affected_rows());
    arg_.sqc_task_ptr_->dml_row_info_.set_px_dml_row_info(*ctx.get_physical_plan_ctx());
    LOG_TRACE("the affected row from sqc task", K(arg_.sqc_task_ptr_->get_affected_rows()));
  }
  if (OB_SUCC(ret)) {
    // collect metric to (transmit, receive) operator
    ObSQLSessionInfo* session = NULL;
    if (OB_ISNULL(session = ctx.get_my_session())) {
      LOG_TRACE("session is NULL", K(ret), K(ctx));
    } else if (nullptr != arg_.sqc_task_ptr_ && GCONF.enable_sql_audit && session->get_local_ob_enable_sql_audit()) {
      int collect_ret = OB_SUCCESS;
      OpPreCloseProcessor pre_close_proc(*arg_.sqc_task_ptr_);
      ObPxOperatorVisitor visitor;
      if (OB_SUCCESS != (collect_ret = visitor.visit(ctx, *arg_.op_root_, pre_close_proc))) {
        LOG_WARN("failed to collect metric", K(ret), K(*arg_.sqc_task_ptr_));
      }
    } else {
      bool enable_sql_audit = GCONF.enable_sql_audit;
      LOG_TRACE("disable trace sql audit", K(ret), K(enable_sql_audit), K(session->get_local_ob_enable_sql_audit()));
    }
  }
  if (OB_SUCCESS != (close_ret = root.close(ctx))) {
    LOG_WARN("fail close dfo op", K(ret), K(close_ret));
    ret = OB_SUCCESS == ret ? close_ret : ret;
  }
  LOG_TRACE("finish open & close task ops", K(ret), K(close_ret));
  return ret;
}

int ObPxTaskProcess::execute(ObOpSpec& root_spec)
{
  int ret = OB_SUCCESS;
  ObExecContext& ctx = *arg_.exec_ctx_;
  int close_ret = OB_SUCCESS;
  // root op is transmit,not need call get_next_row,
  // open method driver next operat's get_next_row
  ObOperatorKit* kit = ctx.get_operator_kit(root_spec.id_);
  if (OB_ISNULL(kit) || OB_ISNULL(kit->op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operator is NULL", K(ret), KP(kit));
  } else if (root_spec.type_ != kit->spec_->type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("is not subplan filter operator",
        K(ret),
        "spec",
        kit->spec_,
        "root operator type",
        root_spec.type_,
        "kit root operator type",
        kit->spec_->type_);
  } else if (!ctx.get_my_session()->use_static_typing_engine()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("It's must be static typing engine", K(ret), KP(kit));
  } else if (nullptr == ctx.get_eval_ctx()->frames_ || ctx.get_eval_ctx()->frames_ != ctx.get_frames()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("frames is null", K(ret));
  } else {
    LOG_TRACE(
        "trace run op spec root", K(&ctx), K(ctx.get_frames()), K(ctx.get_my_session()->use_static_typing_engine()));
    ObOperator* root = kit->op_;
    if (OB_FAIL(root->open())) {
      LOG_WARN("fail open dfo op", K(ret));
    } else if (OB_ISNULL(ctx.get_physical_plan_ctx())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("phy plan ctx is null", K(ret));
    } else if (OB_ISNULL(arg_.sqc_task_ptr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the sqc task ptr is null", K(ret));
    } else {
      // when pdml,after each task execute,have affected rows
      // need save affected row to sqc task
      arg_.sqc_task_ptr_->set_affected_rows(ctx.get_physical_plan_ctx()->get_affected_rows());
      arg_.sqc_task_ptr_->dml_row_info_.set_px_dml_row_info(*ctx.get_physical_plan_ctx());
      LOG_TRACE("the affected row from sqc task", K(arg_.sqc_task_ptr_->get_affected_rows()));
    }
    if (OB_SUCC(ret)) {
      ObSQLSessionInfo* session = NULL;
      if (OB_ISNULL(session = ctx.get_my_session())) {
        LOG_TRACE("session is NULL", K(ret), K(ctx));
      } else if (nullptr != arg_.sqc_task_ptr_ && GCONF.enable_sql_audit && session->get_local_ob_enable_sql_audit()) {
        int collect_ret = OB_SUCCESS;
        OpPreCloseProcessor pre_close_proc(*arg_.sqc_task_ptr_);
        ObPxOperatorVisitor visitor;
        if (OB_SUCCESS != (collect_ret = visitor.visit(ctx, *arg_.op_spec_root_, pre_close_proc))) {
          LOG_WARN("failed to collect metric", K(ret), K(*arg_.sqc_task_ptr_));
        }
      } else {
        bool enable_sql_audit = GCONF.enable_sql_audit;
        LOG_TRACE("disable trace sql audit", K(ret), K(enable_sql_audit), K(session->get_local_ob_enable_sql_audit()));
      }
    }
    if (OB_SUCCESS != (close_ret = root->close())) {
      LOG_WARN("fail close dfo op", K(ret), K(close_ret));
      ret = OB_SUCCESS == ret ? close_ret : ret;
    }
    LOG_TRACE("finish open & close task ops", K(ret), K(close_ret));
  }
  return ret;
}

int ObPxTaskProcess::do_process()
{
  LOG_TRACE("[CMD] run task", "task", arg_.task_);

  int ret = OB_SUCCESS;
  int64_t task_id = arg_.task_.get_task_id();
  int64_t dfo_id = arg_.task_.get_dfo_id();
  int64_t sqc_id = arg_.task_.get_sqc_id();

  if (NULL != arg_.sqc_task_ptr_) {
    arg_.sqc_task_ptr_->set_task_co_id(lib::CO_ID());
    arg_.sqc_task_ptr_->set_task_state(SQC_TASK_START);
  }

  if (OB_INVALID_ID == task_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid task id from sqc", K(ret));
  } else if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check task processor inner stat fail", K(ret));
  } else {
    // 1. build env
    //   - reference: sql/executor/ob_executor_rpc_processor.cpp
    //   - exec, stat
    // With the execution parameters of the operators in the input,
    // If the input is filled in before sending in the old frame,
    // Inside the plan, and then serialized and sent out.
    //
    // 2. init GranuleIterator
    //   - It has been initialized by GI Input on the Sqc side, and nothing is done here
    // 3. init Receive(if any), Transmit operator execution parameters:
    //   - Data Channel Info, It has been set up through Recv/Trans Input on the Sqc side
    //   - Task Id Number needs to be set
    // 4. execute
    LOG_TRACE("TIMERECORD ",
        "reserve:=0 name:=TASK dfoid:",
        dfo_id,
        "sqcid:",
        sqc_id,
        "taskid:",
        task_id,
        "start:",
        ObTimeUtility::current_time(),
        "addr:",
        arg_.task_.get_exec_addr());
    LOG_DEBUG("begin to execute sql task", K(task_id));
    LOG_DEBUG("async task physical plan", "phy_plan", arg_.des_phy_plan_);

    // set mock schema guard
    ObSQLMockSchemaGuard mock_schema_guard;
    // prepare mock schemas
    if (OB_SUCC(ret) &&
        OB_FAIL(ObSQLMockSchemaUtils::prepare_mocked_schemas(arg_.des_phy_plan_->get_mock_rowid_tables()))) {
      LOG_WARN("failed to prepare mocked schemas", K(ret));
    }
    if (OB_SUCC(ret)) {
      ObTaskExecutorCtx* executor_ctx = NULL;
      if (OB_ISNULL(gctx_.schema_service_) || OB_ISNULL(gctx_.sql_engine_) || OB_ISNULL(gctx_.executor_rpc_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr",
            K(ret),
            KP(gctx_.schema_service_),
            KP(gctx_.sql_engine_),
            KP(arg_.exec_ctx_),
            KP(gctx_.executor_rpc_));
      } else if (OB_FAIL(gctx_.schema_service_->get_tenant_schema_guard(
                     arg_.exec_ctx_->get_my_session()->get_effective_tenant_id(), schema_guard_))) {
        LOG_WARN("fail to get schema guard", K(ret));
      } else {
        // Initialization of the parameters of the virtual table for remote execution
        ObVirtualTableCtx vt_ctx;
        ObExecContext& exec_ctx = *arg_.exec_ctx_;
        vt_ctx.vt_iter_factory_ = &vt_iter_factory_;
        vt_ctx.session_ = exec_ctx.get_my_session();
        vt_ctx.schema_guard_ = &schema_guard_;
        vt_ctx.partition_table_operator_ = gctx_.pt_operator_;
        sql_ctx_.session_info_ = exec_ctx.get_my_session();
        sql_ctx_.schema_guard_ = &schema_guard_;
        exec_ctx.set_addr(gctx_.self_addr_);
        exec_ctx.set_plan_cache_manager(gctx_.sql_engine_->get_plan_cache_manager());
        exec_ctx.set_sql_proxy(gctx_.sql_proxy_);
        exec_ctx.set_virtual_table_ctx(vt_ctx);
        exec_ctx.set_session_mgr(gctx_.session_mgr_);
        exec_ctx.set_sql_ctx(&sql_ctx_);
        if (OB_ISNULL(executor_ctx = exec_ctx.get_task_executor_ctx())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("task executor ctx is NULL", K(ret));
        } else {
          executor_ctx->set_partition_service(gctx_.par_ser_);
          executor_ctx->set_vt_partition_service(gctx_.vt_par_ser_);
          executor_ctx->schema_service_ = gctx_.schema_service_;
          executor_ctx->set_task_executor_rpc(*gctx_.executor_rpc_);
          partition_location_cache_.init(gctx_.location_cache_, gctx_.self_addr_, &schema_guard_);
          executor_ctx->set_partition_location_cache(&partition_location_cache_);
        }
      }
    }

    if (OB_SUCC(ret)) {
      // set task id to transmit input, receive input, gi input in dfo
      OpPreparation setter;
      ObPxOperatorVisitor visitor;  // traverse all Operator in DFO
      setter.set_task_id(task_id);
      setter.set_dfo_id(dfo_id);
      setter.set_sqc_id(sqc_id);
      setter.set_exec_ctx(arg_.exec_ctx_);
      if (nullptr != arg_.op_spec_root_) {
        // why set it here,because when we create operator,need set eval_ctx
        // so must eval_ctx created before building operator
        ObOperator* op = nullptr;
        if (OB_FAIL(arg_.exec_ctx_->init_eval_ctx())) {
          LOG_WARN("init eval ctx failed", K(ret));
        } else if (OB_FAIL(arg_.op_spec_root_->create_operator(*arg_.exec_ctx_, op))) {
          LOG_WARN("create operator from spec failed", K(ret));
        } else if (OB_FAIL(visitor.visit(*arg_.exec_ctx_, *arg_.op_spec_root_, setter))) {
          LOG_WARN("fail apply task id to dfo exchanges", K(task_id), K(ret));
        } else if (OB_ISNULL(op)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected status: op root is null", K(ret));
        } else {
          arg_.static_engine_root_ = op;
        }
      } else if (nullptr != arg_.op_root_) {
        if (OB_FAIL(visitor.visit(*arg_.exec_ctx_, *arg_.op_root_, setter))) {
          LOG_WARN("fail apply task id to dfo exchanges", K(task_id), K(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected status: op root is null", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (nullptr != arg_.op_root_) {
        if (OB_FAIL(execute(*arg_.op_root_))) {
          LOG_WARN("failed to execute plan", K(ret));
        }
      } else if (nullptr != arg_.op_spec_root_) {
        if (OB_FAIL(execute(*arg_.op_spec_root_))) {
          LOG_WARN("failed to execute plan", K(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected status: no root operator", K(ret));
      }
    }
  }
  LOG_TRACE("TIMERECORD ",
      "reserve:=0 name:=TASK dfoid:",
      dfo_id,
      "sqcid:",
      sqc_id,
      "taskid:",
      task_id,
      "end:",
      ObTimeUtility::current_time(),
      "addr:",
      arg_.task_.get_exec_addr());

  // Task and Sqc in different thread,task need Communicate with sqc
  if (NULL != arg_.sqc_task_ptr_) {
    arg_.sqc_task_ptr_->set_result(ret);
    if (OB_SUCC(ret)) {
      // nop
    } else if (IS_INTERRUPTED()) {
      // It is currently interrupted by QC, if no more interrupts are sent to QC, exit.
    } else {
      (void)ObInterruptUtil::interrupt_qc(arg_.task_, ret);
    }
  }

  LOG_TRACE("notify SQC task exit", K(dfo_id), K(sqc_id), K(task_id), K(ret));

  return ret;
}

/**
 *  Attention, we must call the function at the end of process()
 */
void ObPxTaskProcess::release()
{
  if (NULL != arg_.sqc_task_ptr_) {
    arg_.sqc_task_ptr_->set_task_state(SQC_TASK_EXIT);
  } else {
    LOG_ERROR("Unexpected px task process", K(arg_.sqc_task_ptr_));
  }
}

int ObPxTaskProcess::OpPreparation::apply(ObExecContext& ctx, ObPhyOperator& op)
{
  int ret = OB_SUCCESS;
  if (task_id_ < 0) {
    ret = OB_NOT_INIT;
    LOG_WARN("task id not init", K_(task_id), K(ret));
  } else if (IS_PX_TRANSMIT(op.get_type()) || IS_PX_RECEIVE(op.get_type())) {
    ObPxExchangeInput* input = GET_PHY_OP_INPUT(ObPxExchangeInput, ctx, op.get_id());
    if (OB_ISNULL(input)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("input not found for op", "op_id", op.get_id(), K(ret));
    } else {
      input->set_task_id(task_id_);
      input->set_sqc_id(sqc_id_);
      input->set_dfo_id(dfo_id_);
    }
  } else if (PHY_GRANULE_ITERATOR == op.get_type()) {
    ObGIInput* input = GET_PHY_OP_INPUT(ObGIInput, ctx, op.get_id());
    ObGranuleIterator* gi = static_cast<ObGranuleIterator*>(&op);
    if (OB_ISNULL(input)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("input not found for op", "op_id", op.get_id(), K(ret));
    } else if (gi->pwj_gi() && on_set_tscs_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the partition-wise join's subplan contain a gi operator", K(*gi), K(ret));
    } else {
      input->set_worker_id(task_id_);
      if (gi->pwj_gi()) {
        pw_gi_ = gi;
        on_set_tscs_ = true;
      }
    }
  } else if (PHY_TABLE_SCAN == op.get_type() && on_set_tscs_) {
    if (OB_ISNULL(pw_gi_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("gi is null", K(ret));
    } else if (OB_FAIL(tsc_ops_.push_back(static_cast<ObTableScan*>(&op)))) {
      LOG_WARN("add tsc to gi failed", K(ret));
    }
  } else if (IS_DML(op.get_type()) && on_set_tscs_) {
    if (OB_ISNULL(pw_gi_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("gi is null", K(ret));
    } else {
      LOG_TRACE("set partition wise insert op");
      dml_op_ = static_cast<ObTableModify*>(&op);
    }
  } else if (IS_PX_MODIFY(op.get_type())) {
    ObPxModifyInput* input = GET_PHY_OP_INPUT(ObPxModifyInput, ctx, op.get_id());
    if (OB_ISNULL(input)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("input not found for op", "op_id", op.get_id(), K(ret));
    } else {
      input->set_task_id(task_id_);
      input->set_sqc_id(sqc_id_);
      input->set_dfo_id(dfo_id_);
    }
  }

  return ret;
}

int ObPxTaskProcess::OpPreparation::apply(ObExecContext& ctx, ObOpSpec& op)
{
  int ret = OB_SUCCESS;
  ObOperatorKit* kit = ctx.get_operator_kit(op.id_);
  if (OB_ISNULL(kit) || OB_ISNULL(kit->op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null ptr", K(ret), K(op.id_));
  } else if (task_id_ < 0) {
    ret = OB_NOT_INIT;
    LOG_WARN("task id not init", K_(task_id), K(ret));
  } else if (IS_PX_TRANSMIT(op.type_) || IS_PX_RECEIVE(op.type_)) {
    if (OB_ISNULL(kit->input_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("operator is NULL", K(ret), KP(kit));
    } else {
      ObPxExchangeOpInput* input = static_cast<ObPxExchangeOpInput*>(kit->input_);
      if (OB_ISNULL(input)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("input not found for op", "op_id", op.id_, K(ret));
      } else {
        input->set_task_id(task_id_);
        input->set_sqc_id(sqc_id_);
        input->set_dfo_id(dfo_id_);
      }
    }
  } else if (PHY_GRANULE_ITERATOR == op.type_) {
    if (OB_ISNULL(kit->input_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("operator is NULL", K(ret), KP(kit));
    } else if (PHY_GRANULE_ITERATOR != kit->spec_->type_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid type", K(ret), KP(kit->spec_->type_));
    } else {
      ObGIOpInput* input = static_cast<ObGIOpInput*>(kit->input_);
      ObGranuleIteratorSpec* gi =
          static_cast<ObGranuleIteratorSpec*>(const_cast<oceanbase::sql::ObOpSpec*>(kit->spec_));
      if (OB_ISNULL(input)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("input not found for op", "op_id", op.id_, K(ret));
      } else if (gi->pwj_gi() && on_set_tscs_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the partition-wise join's subplan contain a gi operator", K(*gi), K(ret));
      } else {
        input->set_worker_id(task_id_);
        if (gi->pwj_gi()) {
          pw_gi_spec_ = gi;
          on_set_tscs_ = true;
        }
      }
    }
  } else if (PHY_TABLE_SCAN == op.type_ && on_set_tscs_) {
    if (OB_ISNULL(pw_gi_spec_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("gi is null", K(ret));
    } else if (OB_FAIL(tsc_op_specs_.push_back(static_cast<ObTableScanSpec*>(&op)))) {
      LOG_WARN("add tsc to gi failed", K(ret));
    }
  } else if (IS_DML(op.type_) && on_set_tscs_) {
    if (OB_ISNULL(pw_gi_spec_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("gi is null", K(ret));
    } else {
      LOG_TRACE("set partition wise insert op");
      dml_spec_ = static_cast<ObTableModifySpec*>(&op);
    }
  } else if (IS_PX_MODIFY(op.get_type())) {
    if (OB_ISNULL(kit->input_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("operator is NULL", K(ret), K(op.id_), KP(kit));
    } else {
      ObPxMultiPartModifyOpInput* input = static_cast<ObPxMultiPartModifyOpInput*>(kit->input_);
      input->set_task_id(task_id_);
      input->set_sqc_id(sqc_id_);
      input->set_dfo_id(dfo_id_);
    }
  }
  return ret;
}

int ObPxTaskProcess::OpPreparation::reset(ObPhyOperator& op)
{
  int ret = OB_SUCCESS;
  if (PHY_GRANULE_ITERATOR == op.get_type()) {
    ObGranuleIterator* gi = static_cast<ObGranuleIterator*>(&op);
    if (gi->pwj_gi()) {
      if (pw_gi_ == nullptr || !on_set_tscs_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Invalid state", K(pw_gi_), K(on_set_tscs_));
      } else if (OB_FAIL(pw_gi_->set_tscs(tsc_ops_))) {
        LOG_WARN("Set tsc failed", K(ret));
      } else if (OB_FAIL(pw_gi_->set_dml_op(dml_op_))) {
        LOG_WARN("set insert op failed", K(ret));
      }
      tsc_ops_.reset();
      dml_op_ = nullptr;
      pw_gi_ = nullptr;
      on_set_tscs_ = false;
    }
  }
  return ret;
}

int ObPxTaskProcess::OpPreparation::reset(ObOpSpec& op)
{
  int ret = OB_SUCCESS;
  if (PHY_GRANULE_ITERATOR == op.type_) {
    ObGranuleIteratorSpec* gi = static_cast<ObGranuleIteratorSpec*>(&op);
    if (gi->pwj_gi()) {
      if (pw_gi_spec_ == nullptr || !on_set_tscs_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Invalid state", K(pw_gi_spec_), K(on_set_tscs_));
      } else {
        ObOperatorKit* kit = ctx_->get_operator_kit(op.id_);
        if (OB_ISNULL(kit) || OB_ISNULL(kit->op_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("operator is NULL", K(ret), KP(kit));
        } else if (PHY_GRANULE_ITERATOR != kit->spec_->type_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("operator is NULL", K(ret), KP(kit->spec_->type_));
        } else {
          ObGranuleIteratorOp* gi_op = static_cast<ObGranuleIteratorOp*>(kit->op_);
          if (OB_FAIL(gi_op->set_tscs(tsc_op_specs_))) {
            LOG_WARN("Set tsc failed", K(ret));
          } else if (OB_FAIL(gi_op->set_dml_op(dml_spec_))) {
            LOG_WARN("set insert op failed", K(ret));
          }
        }
      }
      tsc_op_specs_.reset();
      dml_spec_ = nullptr;
      pw_gi_spec_ = nullptr;
      on_set_tscs_ = false;
    }
  }
  return ret;
}

int ObPxTaskProcess::OpPreCloseProcessor::apply(ObExecContext& ctx, ObPhyOperator& op)
{
  int ret = OB_SUCCESS;
  ObOpMetric* metric = nullptr;
  if (IS_PX_TRANSMIT(op.get_type())) {
    ObPxTransmit::ObPxTransmitCtx* transmit_ctx = NULL;
    if (OB_ISNULL(transmit_ctx = GET_PHY_OPERATOR_CTX(ObPxTransmit::ObPxTransmitCtx, ctx, op.get_id()))) {
      LOG_TRACE("transmit context is null");
    } else {
      metric = &transmit_ctx->get_op_metric();
      if (OB_FAIL(ObPxChannelUtil::set_transmit_metric(transmit_ctx->get_task_channels(), *metric))) {
        LOG_WARN("failed to set transmit metric", K(*metric));
      } else {
        metric->set_type(ObOpMetric::MetricType::OP);
      }
    }
  } else if (IS_PX_RECEIVE(op.get_type())) {
    ObPxReceive::ObPxReceiveCtx* recv_ctx = NULL;
    if (OB_ISNULL(recv_ctx = GET_PHY_OPERATOR_CTX(ObPxReceive::ObPxReceiveCtx, ctx, op.get_id()))) {
      LOG_TRACE("receive context is null");
    } else {
      metric = &recv_ctx->get_op_metric();
      if (OB_FAIL(ObPxChannelUtil::set_transmit_metric(recv_ctx->get_task_channels(), *metric))) {
        LOG_WARN("failed to set transmit metric", K(*metric));
      } else {
        metric->set_type(ObOpMetric::MetricType::OP);
      }
    }
  }
  if (nullptr != metric) {
    LOG_TRACE("trace to fill metric", K(*metric));
    ObPxTaskMonitorInfo& monitor_info = task_.get_task_monitor_info();
    if (OB_FAIL(monitor_info.add_op_metric(*metric))) {
      LOG_WARN("failed to add op metric", K(*metric));
    } else {
      if (0 < monitor_info.get_op_metrics().count()) {
        LOG_TRACE("trace op metric", K(task_), K(monitor_info));
      }
    }
  }
  return ret;
}

int ObPxTaskProcess::OpPreCloseProcessor::apply(ObExecContext& ctx, ObOpSpec& op)
{
  int ret = OB_SUCCESS;
  ObOpMetric* metric = nullptr;
  if (IS_PX_TRANSMIT(op.get_type())) {
    ObOperatorKit* kit = ctx.get_operator_kit(op.id_);
    if (OB_ISNULL(kit) || OB_ISNULL(kit->op_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("operator is NULL", K(ret), KP(kit));
    } else if (!IS_PX_TRANSMIT(kit->spec_->type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("is not subplan filter operator", K(ret), "spec", kit->spec_);
    } else {
      ObPxTransmitOp* transmit_op = static_cast<ObPxTransmitOp*>(const_cast<ObOperator*>(kit->op_));
      metric = &transmit_op->get_op_metric();
      if (OB_FAIL(ObPxChannelUtil::set_transmit_metric(transmit_op->get_task_channels(), *metric))) {
        LOG_WARN("failed to set transmit metric", K(*metric));
      } else {
        metric->set_type(ObOpMetric::MetricType::OP);
      }
    }
  } else if (IS_PX_RECEIVE(op.type_)) {
    ObOperatorKit* kit = ctx.get_operator_kit(op.id_);
    if (OB_ISNULL(kit) || OB_ISNULL(kit->op_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("operator is NULL", K(ret), KP(kit));
    } else if (!IS_PX_RECEIVE(kit->spec_->type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("is not subplan filter operator", K(ret), "spec", kit->spec_);
    } else {
      ObPxReceiveOp* receive_op = static_cast<ObPxReceiveOp*>(kit->op_);
      metric = &receive_op->get_op_metric();
      if (OB_FAIL(ObPxChannelUtil::set_transmit_metric(receive_op->get_task_channels(), *metric))) {
        LOG_WARN("failed to set transmit metric", K(*metric));
      } else {
        metric->set_type(ObOpMetric::MetricType::OP);
      }
    }
  }
  if (nullptr != metric) {
    LOG_TRACE("trace to fill metric", K(*metric));
    ObPxTaskMonitorInfo& monitor_info = task_.get_task_monitor_info();
    if (OB_FAIL(monitor_info.add_op_metric(*metric))) {
      LOG_WARN("failed to add op metric", K(*metric));
    } else {
      if (0 < monitor_info.get_op_metrics().count()) {
        LOG_TRACE("trace op metric", K(task_), K(monitor_info));
      }
    }
  }
  return ret;
}

int ObPxTaskProcess::OpPreCloseProcessor::reset(ObPhyOperator& op)
{
  UNUSED(op);
  return OB_SUCCESS;
}

int ObPxTaskProcess::OpPreCloseProcessor::reset(ObOpSpec& op)
{
  UNUSED(op);
  return OB_SUCCESS;
}

uint64_t ObPxTaskProcess::get_session_id() const
{
  uint64_t session_id = 0;
  ObExecContext* exec_ctx = NULL;
  ObSQLSessionInfo* session = NULL;
  if (OB_ISNULL(exec_ctx = arg_.exec_ctx_)) {
    LOG_WARN("exec ctx is NULL", K(exec_ctx));
  } else if (OB_ISNULL(session = exec_ctx->get_my_session())) {
    LOG_WARN("session is NULL", K(exec_ctx));
  } else {
    session_id = session->get_sessid();
  }
  return session_id;
}

uint64_t ObPxTaskProcess::get_tenant_id() const
{
  uint64_t tenant_id = 0;
  ObExecContext* exec_ctx = NULL;
  ObSQLSessionInfo* session = NULL;
  if (OB_ISNULL(exec_ctx = arg_.exec_ctx_)) {
    LOG_WARN("exec ctx is NULL", K(exec_ctx));
  } else if (OB_ISNULL(session = exec_ctx->get_my_session())) {
    LOG_WARN("session is NULL", K(exec_ctx));
  } else {
    tenant_id = session->get_effective_tenant_id();
  }
  return tenant_id;
}
