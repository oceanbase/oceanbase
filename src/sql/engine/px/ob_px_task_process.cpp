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
#include "sql/engine/px/ob_granule_iterator_op.h"
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
#include "sql/engine/px/exchange/ob_px_transmit_op.h"
#include "sql/engine/px/exchange/ob_px_receive_op.h"
#include "sql/engine/basic/ob_temp_table_insert_op.h"
#include "sql/engine/basic/ob_temp_table_insert_vec_op.h"
#include "sql/engine/dml/ob_table_insert_op.h"
#include "sql/engine/join/ob_hash_join_op.h"
#include "sql/engine/window_function/ob_window_function_op.h"
#include "sql/engine/px/ob_px_basic_info.h"
#include "sql/engine/pdml/static/ob_px_multi_part_insert_op.h"
#include "sql/engine/join/ob_join_filter_op.h"
#include "sql/engine/px/ob_granule_pump.h"
#include "sql/engine/join/hash_join/ob_hash_join_vec_op.h"
#include "sql/engine/basic/ob_select_into_op.h"
#include "observer/mysql/obmp_base.h"
#include "lib/alloc/ob_malloc_callback.h"
#include "sql/engine/window_function/ob_window_function_vec_op.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::lib;
using namespace oceanbase::sql;
using namespace oceanbase::sql::dtl;

ObPxTaskProcess::ObPxTaskProcess(const observer::ObGlobalContext &gctx, ObPxRpcInitTaskArgs &arg)
  : gctx_(gctx), arg_(arg),
    schema_guard_(share::schema::ObSchemaMgrItem::MOD_PX_TASK_PROCESSS),
    vt_iter_factory_(*gctx.vt_iter_creator_),
    enqueue_timestamp_(0), process_timestamp_(0), exec_start_timestamp_(0), exec_end_timestamp_(0),
    is_oracle_mode_(false)
{
}

ObPxTaskProcess::~ObPxTaskProcess()
{
}

int ObPxTaskProcess::check_inner_stat()
{
  int ret = OB_SUCCESS;
  ObExecContext *exec_ctx = NULL;
  ObSQLSessionInfo *session = NULL;
  ObPhysicalPlanCtx *plan_ctx = NULL;
  if (arg_.is_invalid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("des phy plan is null", K(ret), K(arg_.des_phy_plan_),
      K(arg_.op_spec_root_));
  } else if (OB_ISNULL(exec_ctx = arg_.exec_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exec ctx is NULL", K(ret), K(exec_ctx));
  } else if (OB_ISNULL(session = exec_ctx->get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret), K(exec_ctx));
  } else if (OB_ISNULL(plan_ctx = exec_ctx->get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("phy plan ctx is NULL", K(ret), K(exec_ctx));
  } else if (OB_ISNULL(arg_.get_sqc_handler())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sqc hanlder is null", K(ret));
  } else {
    // 为了尽快确定任务超时，启动 task 的超时时间很短，10ms 左右
    // 这会导致 after process 中发起任意内部查询都会很快 10ms 超时
    // 所以这里需要重新设置超时为实际 query 超时时间
    THIS_WORKER.set_timeout_ts(plan_ctx->get_timeout_timestamp());
  }
  return ret;
}

void ObPxTaskProcess::operator()(void)
{
  (void) process();
}

// The Tenant Px Pool will invoke this function
void ObPxTaskProcess::run()
{
  int ret = OB_SUCCESS;
  lib::CompatModeGuard g(is_oracle_mode() ?
      lib::Worker::CompatMode::ORACLE : lib::Worker::CompatMode::MYSQL);

  LOG_TRACE("Px task get sql mode", K(is_oracle_mode()));

  LOG_TRACE("begin process task",
            KP(this));
  ObPxWorkerStat stat;
  stat.init(get_session_id(),
            get_tenant_id(),
            *ObCurTraceId::get_trace_id(),
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
  ACTIVE_SESSION_FLAG_SETTER_GUARD(in_px_execution);
  int ret = OB_SUCCESS;
  common::ob_setup_default_tsi_warning_buffer();
  common::ob_reset_tsi_warning_buffer();
  enqueue_timestamp_ = ObTimeUtility::current_time();
  process_timestamp_ = enqueue_timestamp_;
  ObExecRecord exec_record;
  ObExecTimestamp exec_timestamp;
  ObWaitEventDesc max_wait_desc;
  ObWaitEventStat total_wait_desc;
  ObSQLSessionInfo *session = (NULL == arg_.exec_ctx_
                               ? NULL
                               : arg_.exec_ctx_->get_my_session());
  ObPxSqcHandler *sqc_handler = arg_.sqc_handler_;
  if (OB_ISNULL(session)  || OB_ISNULL(sqc_handler)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("session or sqc_handler is NULL", K(ret));
  } else if (OB_FAIL(session->store_query_string(ObString::make_string("PX DFO EXECUTING")))) {
    LOG_WARN("store query string to session failed", K(ret));
  } else {
    // 设置诊断功能环境
    ObPxRpcInitSqcArgs &arg = arg_.sqc_handler_->get_sqc_init_arg();
    SQL_INFO_GUARD(arg.sqc_.get_monitoring_info().cur_sql_, session->get_cur_sql_id());
    const bool enable_perf_event = lib::is_diagnose_info_enabled();
    const bool enable_sql_audit =
        GCONF.enable_sql_audit && session->get_local_ob_enable_sql_audit();
    ObAuditRecordData &audit_record = session->get_raw_audit_record();
    ObWorkerSessionGuard worker_session_guard(session);
    ObSQLSessionInfo::LockGuard lock_guard(session->get_query_lock());
    ObSessionStatEstGuard stat_est_guard(session->get_effective_tenant_id(), session->get_sessid());
    session->set_current_trace_id(ObCurTraceId::get_trace_id());
    session->get_raw_audit_record().request_memory_used_ = 0;
    observer::ObProcessMallocCallback pmcb(0,
          session->get_raw_audit_record().request_memory_used_);
    lib::ObMallocCallbackGuard guard(pmcb);
    session->set_peer_addr(arg_.task_.get_sqc_addr());
    session->set_cur_phy_plan(arg_.des_phy_plan_);
    session->set_thread_id(GETTID());
    arg_.exec_ctx_->reference_my_plan(arg_.des_phy_plan_);
    arg_.exec_ctx_->set_sqc_handler(arg_.sqc_handler_);
    arg_.exec_ctx_->set_px_task_id(arg_.task_.get_task_id());
    arg_.exec_ctx_->set_px_sqc_id(arg_.task_.get_sqc_id());
    arg_.exec_ctx_->set_branch_id(arg_.task_.get_branch_id());
    ObMaxWaitGuard max_wait_guard(enable_perf_event ? &max_wait_desc : NULL);
    ObTotalWaitGuard total_wait_guard(enable_perf_event ? &total_wait_desc : NULL);

    if (enable_perf_event) {
      exec_record.record_start();
    }

    //监控项统计开始
    exec_start_timestamp_ = enqueue_timestamp_;

    if (OB_FAIL(do_process())) {
      LOG_WARN("failed to process", K(get_tenant_id()), K(ret), K(get_qc_id()), K(get_dfo_id()));
    }

    //监控项统计结束
    exec_end_timestamp_ = ObTimeUtility::current_time();

    // some statistics must be recorded for plan stat, even though sql audit disabled
    record_exec_timestamp(true, exec_timestamp);
    audit_record.exec_timestamp_ = exec_timestamp;
    audit_record.exec_timestamp_.update_stage_time();

    if (enable_perf_event) {
      exec_record.record_end();
      exec_record.max_wait_event_ = max_wait_desc;
      exec_record.wait_time_end_ = total_wait_desc.time_waited_;
      exec_record.wait_count_end_ = total_wait_desc.total_waits_;
      audit_record.exec_record_ = exec_record;
      audit_record.update_event_stage_state();
    }

    if (enable_sql_audit) {
      if (OB_ISNULL(arg_.sqc_task_ptr_)){
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the sqc task ptr is null", K(ret));
      } else {
        arg_.sqc_task_ptr_->set_memstore_read_row_count(exec_record.get_memstore_read_row_count());
        arg_.sqc_task_ptr_->set_ssstore_read_row_count(exec_record.get_ssstore_read_row_count());
      }
    }

    if (enable_sql_audit) {
      ObPhysicalPlan *phy_plan = arg_.des_phy_plan_;
      if ( OB_ISNULL(phy_plan)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid argument", K(ret), K(phy_plan));
      } else {
        audit_record.try_cnt_++;
        audit_record.seq_ = 0;  //don't use now
        audit_record.status_ = (OB_SUCCESS == ret || common::OB_ITER_END == ret)
            ? obmysql::REQUEST_SUCC : ret;
        session->get_cur_sql_id(audit_record.sql_id_, OB_MAX_SQL_ID_LENGTH + 1);
        audit_record.db_id_ = session->get_database_id();
        audit_record.user_group_ = THIS_WORKER.get_group_id();
        audit_record.execution_id_ = GCTX.sql_engine_->get_execution_id();
        audit_record.client_addr_ = session->get_client_addr();
        audit_record.user_client_addr_ = session->get_user_client_addr();
        audit_record.qc_id_ = get_qc_id();
        audit_record.dfo_id_ = get_dfo_id();
        audit_record.sqc_id_ = get_sqc_id();
        audit_record.worker_id_ = get_worker_id();
        //audit_record.client_addr_ = arg_.task_.sqc_addr_;
        audit_record.affected_rows_ = 0;
        audit_record.return_rows_ = 0;

        audit_record.plan_id_ =  phy_plan->get_plan_id();
        audit_record.plan_type_ =  phy_plan->get_plan_type();
        audit_record.is_executor_rpc_ = true;
        audit_record.is_inner_sql_ = session->is_inner();
        audit_record.is_hit_plan_cache_ = true;
        audit_record.is_multi_stmt_ = false;
        audit_record.is_perf_event_closed_ = !lib::is_diagnose_info_enabled();
        audit_record.total_memstore_read_row_count_ = exec_record.get_memstore_read_row_count();
        audit_record.total_ssstore_read_row_count_ = exec_record.get_ssstore_read_row_count();
      }
    }
    ObSQLUtils::handle_audit_record(false, EXECUTE_DIST, *session);
  }
  release();
  return ret;
}

int ObPxTaskProcess::execute(ObOpSpec &root_spec)
{
  int ret = OB_SUCCESS;
  ObExecContext &ctx = *arg_.exec_ctx_;
  int close_ret = OB_SUCCESS;
  // root op 是 transmit，不需要调用 get_next_row，由 open
  // 内部驱动下层算子的 get_next_row
  ObOperatorKit *kit = ctx.get_operator_kit(root_spec.id_);
  if (OB_ISNULL(kit) || OB_ISNULL(kit->op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operator is NULL", K(ret), KP(kit));
  } else if (root_spec.type_ != kit->spec_->type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("is not subplan filter operator", K(ret),
              "spec", kit->spec_,
              "root operator type", root_spec.type_,
              "kit root operator type", kit->spec_->type_);
  } else if (nullptr == kit->op_->get_eval_ctx().frames_
             || kit->op_->get_eval_ctx().frames_ != ctx.get_frames()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("frames is null", K(ret));
  } else {
    ObOperator *root = kit->op_;
    int64_t batch_count = arg_.get_sqc_handler()->
      get_sqc_init_arg().sqc_.get_rescan_batch_params().get_count();
    bool need_fill_batch_info = false;
    //  这里统一处理了 batch rescan 和非 batch rescan 的场景
    //  一般场景下，传入的参数是 batch_count = 0
    //  为了复用下面的循环代码,将batch_count 改为1, 但无需初始化rescan 参数
    if (batch_count < 1) {
      batch_count = 1;
    } else {
      need_fill_batch_info = true;
    }
    LOG_TRACE("trace run op spec root", K(&ctx), K(ctx.get_frames()),
              K(batch_count), K(need_fill_batch_info), K(root_spec.get_id()), K(&(root->get_exec_ctx())));
    CK(IS_PX_TRANSMIT(root_spec.get_type()));
    for (int i = 0; i < batch_count && OB_SUCC(ret); ++i) {
      if (need_fill_batch_info) {
        if (OB_FAIL(ctx.fill_px_batch_info(arg_.get_sqc_handler()->
          get_sqc_init_arg().sqc_.get_rescan_batch_params(), i,
          arg_.des_phy_plan_->get_expr_frame_info().rt_exprs_))) {
          LOG_WARN("fail to fill batch info", K(ret));
        } else if (OB_FAIL(arg_.get_sqc_handler()->get_sub_coord().
          get_sqc_ctx().gi_pump_.regenerate_gi_task())) {
          LOG_WARN("fail to generate gi task array", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (0 == i) {
        if (OB_FAIL(root->open())) {
          LOG_WARN("fail open dfo op", K(ret));
        } else if (batch_count > 1) {
          static_cast<ObPxTransmitOp *>(root)->set_batch_param_remain(true);
        }
      } else if (0 != i) {
        if (OB_FAIL(root->rescan())) {
          LOG_WARN("fail to rescan dfo op", K(ret));
        } else if (i + 1 == batch_count) {
          static_cast<ObPxTransmitOp *>(root)->set_batch_param_remain(false);
        }
      }
      OZ(static_cast<ObPxTransmitOp *>(root)->transmit());
    }

    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(ctx.get_physical_plan_ctx())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("phy plan ctx is null", K(ret));
    } else if (OB_ISNULL(arg_.sqc_task_ptr_)){
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the sqc task ptr is null", K(ret));
    } else {
      // pdml情况下，每一个task执行结束以后，会有affected rows
      // 需要将对应的affected row存储到sqc task中
      arg_.sqc_task_ptr_->set_affected_rows(ctx.get_physical_plan_ctx()->get_affected_rows());
      arg_.sqc_task_ptr_->dml_row_info_.set_px_dml_row_info(*ctx.get_physical_plan_ctx());
      LOG_TRACE("the affected row from sqc task", K(arg_.sqc_task_ptr_->get_affected_rows()));
    }
    // record ret code
    if (OB_FAIL(ret)) {
      OpPostparation setter(ret);
      ObPxOperatorVisitor visitor; // 遍历 DFO 所有 Operator
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = visitor.visit(*arg_.exec_ctx_, *arg_.op_spec_root_, setter))) {
        ret = OB_SUCCESS == ret ? tmp_ret : ret;
        LOG_WARN("failed to apply error code", K(ret), K(tmp_ret));
      }
      if (OB_SUCCESS != (tmp_ret = ObInterruptUtil::interrupt_tasks(arg_.get_sqc_handler()->get_sqc_init_arg().sqc_,
                                              OB_GOT_SIGNAL_ABORTING))) {
        LOG_WARN("interrupt_tasks failed", K(tmp_ret));
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

ERRSIM_POINT_DEF(ERRSIM_INTERRUPT_QC_FAILED)
int ObPxTaskProcess::do_process()
{
  LOG_TRACE("[CMD] run task", "task", arg_.task_);

  int ret = OB_SUCCESS;
  int64_t task_id = arg_.task_.get_task_id();
  int64_t dfo_id = arg_.task_.get_dfo_id();
  int64_t sqc_id = arg_.task_.get_sqc_id();

  if (NULL != arg_.sqc_task_ptr_) {
    arg_.sqc_task_ptr_->set_task_state(SQC_TASK_START);
  }

  if (OB_INVALID_ID == task_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid task id from sqc", K(ret));
  } else if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check task processor inner stat fail", K(ret));
  } else {
    // 1. 构建执行环境
    //   - 参考 sql/executor/ob_executor_rpc_processor.cpp
    //   - exec, stat 等
    // 在input里面带有算子们的执行参数，
    // 如果input在旧框架里面是在发送之前填充到，
    // 计划里面，然后序列化发送出去的。
    //
    // 2. 初始化 GranuleIterator 的输入
    //   - 在Sqc端已经通过 GI Input 初始化好，这里不做任何事情
    // 3. 初始化 Receive(if any), Transmit 算子执行参数:
    //   - Data Channel Info 在Sqc端已经通过 Recv/Trans Input 设置好
    //   - Task Id 编号需要设置
    // 4. 执行
    LOG_TRACE("TIMERECORD ", "reserve:=0 name:=TASK dfoid:",dfo_id,"sqcid:",
             sqc_id,"taskid:", task_id,"start:", ObTimeUtility::current_time(),
             "addr:", arg_.task_.get_exec_addr());
    LOG_DEBUG("begin to execute sql task", K(task_id));
    LOG_DEBUG("async task physical plan", "phy_plan", arg_.des_phy_plan_);

    if (OB_SUCC(ret)) {
      ObTaskExecutorCtx *executor_ctx = NULL;
      if (OB_ISNULL(gctx_.schema_service_) || OB_ISNULL(gctx_.sql_engine_)
          || OB_ISNULL(gctx_.executor_rpc_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(ret),
                  KP(gctx_.schema_service_),
                  KP(gctx_.sql_engine_),
                  KP(arg_.exec_ctx_),
                  KP(gctx_.executor_rpc_));
      } else if (OB_FAIL(gctx_.schema_service_->get_tenant_schema_guard(
                  arg_.exec_ctx_->get_my_session()->get_effective_tenant_id(),
                  schema_guard_))) {
        LOG_WARN("fail to get schema guard", K(ret));
      } else {
        // 用于远端执行的虚拟表的参数的初始化
        ObVirtualTableCtx vt_ctx;
        ObExecContext &exec_ctx = *arg_.exec_ctx_;
        vt_ctx.vt_iter_factory_ = &vt_iter_factory_;
        vt_ctx.session_ = exec_ctx.get_my_session();
        vt_ctx.schema_guard_ = &schema_guard_;
        exec_ctx.get_sql_ctx()->schema_guard_ = &schema_guard_;
        exec_ctx.set_sql_proxy(gctx_.sql_proxy_);
        exec_ctx.set_virtual_table_ctx(vt_ctx);
        if (OB_ISNULL(executor_ctx = exec_ctx.get_task_executor_ctx())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("task executor ctx is NULL", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      // 向 DFO 中的 transmit input, receive input, gi input 里设置 task id
      OpPreparation setter;
      ObPxOperatorVisitor visitor; // 遍历 DFO 所有 Operator
      setter.set_task_id(task_id);
      setter.set_dfo_id(dfo_id);
      setter.set_sqc_id(sqc_id);
      setter.set_exec_ctx(arg_.exec_ctx_);
      setter.set_px_task(arg_.sqc_task_ptr_);
      if (nullptr != arg_.op_spec_root_) {
        // 为什么放在这里，主要是因为operator创建时候，需要赋值eval_ctx
        // 所以必须eval_ctx在operator create之前创建
        ObOperator *op = nullptr;
        if (OB_FAIL(arg_.op_spec_root_->create_operator(*arg_.exec_ctx_, op))) {
          LOG_WARN("create operator from spec failed", K(ret));
        } else if (OB_FAIL(visitor.visit(*arg_.exec_ctx_, *arg_.op_spec_root_, setter))) {
          LOG_WARN("fail apply task id to dfo exchanges", K(task_id), K(ret));
        } else if (OB_ISNULL(op)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected status: op root is null", K(ret));
        } else {
          arg_.static_engine_root_ = op;
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected status: op root is null", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_NOT_NULL(arg_.sqc_handler_) && OB_NOT_NULL(arg_.exec_ctx_)) {
        ObIArray<ObSqlTempTableCtx> &ctx = arg_.sqc_handler_->get_sqc_init_arg().sqc_.get_temp_table_ctx();
        if (OB_FAIL(arg_.exec_ctx_->get_temp_table_ctx().assign(ctx))) {
          LOG_WARN("failed to assign temp table ctx", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (nullptr != arg_.op_spec_root_) {
        const ObPxSqcMeta &sqc_meta = arg_.sqc_handler_->get_sqc_init_arg().sqc_;
        // show monitoring information from qc
        LOG_DEBUG("receive monitoring information", K(sqc_meta.get_monitoring_info()));
        ObExtraServerAliveCheck qc_alive_checker(sqc_meta.get_qc_addr(),
          arg_.exec_ctx_->get_my_session()->get_process_query_time());
        ObExtraServerAliveCheck::Guard check_guard(*arg_.exec_ctx_, qc_alive_checker);
        if (OB_FAIL(execute(*arg_.op_spec_root_))) {
          LOG_WARN("failed to execute plan", K(ret), K(arg_.op_spec_root_->id_));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected status: no root operator", K(ret));
      }
    }
  }

  // for forward warning msg and user error msg
  (void)record_user_error_msg(ret);
  // for transaction
  (void)record_tx_desc();
  // for exec feedback info
  (void)record_exec_feedback_info();

  LOG_TRACE("TIMERECORD ", "reserve:=0 name:=TASK dfoid:",dfo_id,"sqcid:",
           sqc_id,"taskid:", task_id,"end:", ObTimeUtility::current_time(),
           "addr:", arg_.task_.get_exec_addr());

  // Task 和 Sqc 在两个不同线程中时，task 需要和 sqc 通信
  if (NULL != arg_.sqc_task_ptr_) {
    arg_.sqc_task_ptr_->set_result(ret);
    if (OB_NOT_NULL(arg_.exec_ctx_)) {
      int das_retry_rc = DAS_CTX(*arg_.exec_ctx_).get_location_router().get_last_errno();
      arg_.sqc_task_ptr_->set_das_retry_rc(das_retry_rc);
    }
    if (OB_SUCC(ret)) {
      // nop
    } else if (IS_INTERRUPTED()) {
      //当前是被QC中断的，不再向QC发送中断，退出即可。
    } else if (arg_.get_sqc_handler()->get_sqc_init_arg().sqc_.is_ignore_vtable_error()
               && ObVirtualTableErrorWhitelist::should_ignore_vtable_error(ret)) {
      // 忽略虚拟表错误
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS == ERRSIM_INTERRUPT_QC_FAILED) {
        if (OB_SUCCESS != (tmp_ret = ObInterruptUtil::interrupt_qc(arg_.task_, ret, arg_.exec_ctx_))) {
          LOG_WARN("interrupt_qc failed", K(tmp_ret));
        }
      }
    }
  }

  LOG_TRACE("notify SQC task exit", K(dfo_id), K(sqc_id), K(task_id), K(ret));

  return ret;
}

int ObPxTaskProcess::record_user_error_msg(int retcode)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(arg_.sqc_task_ptr_));
  if (OB_SUCC(ret)) {
    ObPxUserErrorMsg &rcode =  arg_.sqc_task_ptr_->get_err_msg();
    rcode.rcode_ = retcode;
    common::ObWarningBuffer *wb = common::ob_get_tsi_warning_buffer();
    if (wb) {
      if (retcode != common::OB_SUCCESS) {
        (void)snprintf(rcode.msg_, common::OB_MAX_ERROR_MSG_LEN, "%s", wb->get_err_msg());
      }
      //always add warning buffer
      bool not_null = true;
      for (uint32_t idx = 0; OB_SUCC(ret) && not_null && idx < wb->get_readable_warning_count(); idx++) {
        const common::ObWarningBuffer::WarningItem *item = wb->get_warning_item(idx);
        if (item != NULL) {
          if (OB_FAIL(rcode.warnings_.push_back(*item))) {
            RPC_OBRPC_LOG(WARN, "Failed to add warning", K(ret));
          }
        } else {
          not_null = false;
        }
      }
    }
  }
  return ret;
}

int ObPxTaskProcess::record_exec_feedback_info()
{
  int ret = OB_SUCCESS;
  ObExecContext *cur_exec_ctx = nullptr;
  CK (OB_NOT_NULL(arg_.sqc_task_ptr_));
  CK (OB_NOT_NULL(cur_exec_ctx = arg_.exec_ctx_));
  if (OB_SUCC(ret)) {
    ObExecFeedbackInfo &fb_info = cur_exec_ctx->get_feedback_info();
    if (fb_info.get_feedback_nodes().count() > 0 &&
        fb_info.is_valid()) {
      OZ(arg_.sqc_task_ptr_->get_feedback_info().assign(fb_info));
    }
  }
  return ret;
}

int ObPxTaskProcess::record_tx_desc()
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *cur_session = NULL;
  ObExecContext *cur_exec_ctx = NULL;
  CK (OB_NOT_NULL(arg_.sqc_task_ptr_));
  CK (OB_NOT_NULL(cur_exec_ctx = arg_.exec_ctx_));
  CK (OB_NOT_NULL(cur_session = cur_exec_ctx->get_my_session()));
  if (OB_SUCC(ret) && !arg_.sqc_task_ptr_->is_use_local_thread()) {
    // move session's tx_desc to task, accumulate when sqc report
    ObSQLSessionInfo::LockGuard guard(cur_session->get_thread_data_lock());
    transaction::ObTxDesc *&cur_tx_desc = cur_session->get_tx_desc();
    if (OB_NOT_NULL(cur_tx_desc)) {
      transaction::ObTxDesc *&task_tx_desc = arg_.sqc_task_ptr_->get_tx_desc();
      task_tx_desc = cur_tx_desc;
      cur_tx_desc = NULL;
    }
  }
  return ret;
}
/**
 *  Attention, we must call the function at the end of process()
 */
void ObPxTaskProcess::release() {
  if (NULL != arg_.sqc_task_ptr_) {
    arg_.sqc_task_ptr_->set_task_state(SQC_TASK_EXIT);
  } else {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "Unexpected px task process", K(arg_.sqc_task_ptr_));
  }
}

int ObPxTaskProcess::OpPreparation::apply(ObExecContext &ctx,
                                          ObOpSpec &op)
{
  int ret = OB_SUCCESS;
  ObOperatorKit *kit = ctx.get_operator_kit(op.id_);
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
      ObPxExchangeOpInput *input = static_cast<ObPxExchangeOpInput*>(kit->input_);
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
      ObGIOpInput *input = static_cast<ObGIOpInput*>(kit->input_);
      ObGranuleIteratorSpec *gi = static_cast<ObGranuleIteratorSpec *>(
        const_cast<oceanbase::sql::ObOpSpec*>(kit->spec_));
      if (OB_ISNULL(input)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("input not found for op", "op_id", op.id_, K(ret));
      } else if (ObGranuleUtil::pwj_gi(gi->gi_attri_flag_) && on_set_tscs_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the partition-wise join's subplan contain a gi operator", K(*gi), K(ret));
      } else {
        input->set_worker_id(task_id_);
        input->set_px_sequence_id(task_->px_int_id_.px_interrupt_id_.first_);
        if (OB_NOT_NULL(ctx.get_my_session())) {
          input->set_rf_max_wait_time(ctx.get_my_session()->get_runtime_filter_wait_time_ms());
        }
        if (ObGranuleUtil::pwj_gi(gi->gi_attri_flag_)) {
          pw_gi_spec_ = gi;
          on_set_tscs_ = true;
        }
      }
    }
  } else if ((PHY_TABLE_SCAN == op.type_ ||
              PHY_ROW_SAMPLE_SCAN == op.type_ ||
              PHY_BLOCK_SAMPLE_SCAN == op.type_) && on_set_tscs_) {
    if (OB_ISNULL(pw_gi_spec_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("gi is null", K(ret));
    } else if (static_cast<ObTableScanSpec&>(op).use_dist_das()) {
      // avoid das tsc collected and processed by gi
    } else if (OB_FAIL(tsc_op_specs_.push_back(static_cast<ObTableScanSpec *>(&op)))) {
      LOG_WARN("add tsc to gi failed", K(ret));
    }
  } else if (IS_DML(op.type_) && on_set_tscs_) {
    if (OB_ISNULL(pw_gi_spec_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("gi is null", K(ret));
    } else  {
      LOG_TRACE("set partition wise insert op");
      dml_spec_ = static_cast<ObTableModifySpec *>(&op);
    }
  } else if (IS_PX_MODIFY(op.get_type())) {
    if (OB_ISNULL(kit->input_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("operator is NULL", K(ret), K(op.id_), KP(kit));
    } else {
      ObPxMultiPartModifyOpInput *input = static_cast<ObPxMultiPartModifyOpInput *>(kit->input_);
      input->set_task_id(task_id_);
      input->set_sqc_id(sqc_id_);
      input->set_dfo_id(dfo_id_);
    }
  } else if (IS_PX_JOIN_FILTER(op.get_type())) {
    ObJoinFilterSpec *filter_spec = reinterpret_cast<ObJoinFilterSpec *>(&op);
    if (OB_ISNULL(kit->input_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("operator is NULL", K(ret), K(op.id_), KP(kit));
    } else {
      ObJoinFilterOpInput *input = static_cast<ObJoinFilterOpInput *>(kit->input_);
      if (!filter_spec->is_shared_join_filter()) {
        input->set_task_id(task_id_);
      }
    }
  } else if (PHY_TEMP_TABLE_INSERT == op.type_) {
    ObOperatorKit *kit = ctx.get_operator_kit(op.id_);
    if (OB_ISNULL(kit) || OB_ISNULL(kit->op_) || OB_ISNULL(kit->input_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("operator is NULL", K(ret), KP(kit));
    } else if (PHY_TEMP_TABLE_INSERT != kit->spec_->type_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("is not temp table insert operator", K(ret),
               "spec", kit->spec_);
    } else {
      ObTempTableInsertOp *insert_op = static_cast<ObTempTableInsertOp*>(kit->op_);
      insert_op->set_px_task(task_);
      ObTempTableInsertOpInput *input = static_cast<ObTempTableInsertOpInput *>(kit->input_);
      input->qc_id_ = NULL == task_ ? OB_INVALID_ID : task_->qc_id_;
      input->sqc_id_ = sqc_id_;
      input->dfo_id_ = dfo_id_;
    }
  } else if (PHY_VEC_TEMP_TABLE_INSERT == op.type_) {
    ObOperatorKit *kit = ctx.get_operator_kit(op.id_);
    if (OB_ISNULL(kit) || OB_ISNULL(kit->op_) || OB_ISNULL(kit->input_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("operator is NULL", K(ret), KP(kit));
    } else if (PHY_VEC_TEMP_TABLE_INSERT != kit->spec_->type_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("is not temp table insert operator", K(ret),
               "spec", kit->spec_);
    } else {
      ObTempTableInsertVecOp *insert_op = static_cast<ObTempTableInsertVecOp*>(kit->op_);
      insert_op->set_px_task(task_);
      ObTempTableInsertVecOpInput *input = static_cast<ObTempTableInsertVecOpInput *>(kit->input_);
      input->qc_id_ = NULL == task_ ? OB_INVALID_ID : task_->qc_id_;
      input->sqc_id_ = sqc_id_;
      input->dfo_id_ = dfo_id_;
    }
  } else if (PHY_HASH_JOIN == op.type_) {
    if (OB_ISNULL(kit->input_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("operator is NULL", K(ret), KP(kit));
    } else {
      ObHashJoinSpec &hj_spec = static_cast<ObHashJoinSpec&>(op);
      ObHashJoinInput *input = static_cast<ObHashJoinInput*>(kit->input_);
      if (OB_ISNULL(input)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("input not found for op", "op_id", op.id_, K(ret));
      } else if (hj_spec.is_shared_ht_) {
        input->set_task_id(task_id_);
        LOG_TRACE("debug pre apply info", K(task_id_), K(op.id_));
      }
    }
  } else if (PHY_VEC_HASH_JOIN == op.type_) {
    if (OB_ISNULL(kit->input_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("operator is NULL", K(ret), KP(kit));
    } else {
      ObHashJoinVecSpec &hj_spec = static_cast<ObHashJoinVecSpec&>(op);
      ObHashJoinVecInput *input = static_cast<ObHashJoinVecInput*>(kit->input_);
      if (OB_ISNULL(input)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("input not found for op", "op_id", op.id_, K(ret));
      } else if (hj_spec.is_shared_ht_) {
        input->set_task_id(task_id_);
        LOG_TRACE("debug pre apply info", K(task_id_), K(op.id_));
      }
    }
  } else if (PHY_SELECT_INTO == op.type_) {
    if (OB_ISNULL(kit->input_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("operator is NULL", K(ret), KP(kit));
    } else {
      ObSelectIntoOpInput *input = static_cast<ObSelectIntoOpInput*>(kit->input_);
      if (OB_ISNULL(input)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("input not found for op", "op_id", op.id_, K(ret));
      } else {
        input->set_task_id(task_id_);
        input->set_sqc_id(sqc_id_);
      }
    }
  }
  return ret;
}

int ObPxTaskProcess::OpPreparation::reset(ObOpSpec &op)
{
  int ret = OB_SUCCESS;
  if (PHY_GRANULE_ITERATOR == op.type_) {
    ObGranuleIteratorSpec *gi = static_cast<ObGranuleIteratorSpec *>(&op);
    if ((ObGranuleUtil::pwj_gi(gi->gi_attri_flag_))) {
      if (pw_gi_spec_ == nullptr || !on_set_tscs_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Invalid state", K(pw_gi_spec_), K(on_set_tscs_));
      } else {
        ObOperatorKit *kit = ctx_->get_operator_kit(op.id_);
        if (OB_ISNULL(kit) || OB_ISNULL(kit->op_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("operator is NULL", K(ret), KP(kit));
        } else if (PHY_GRANULE_ITERATOR != kit->spec_->type_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("operator is NULL", K(ret), KP(kit->spec_->type_));
        } else {
          ObGranuleIteratorOp *gi_op = static_cast<ObGranuleIteratorOp*>(kit->op_);
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

int ObPxTaskProcess::OpPostparation::apply(ObExecContext &ctx, ObOpSpec &op)
{
  int ret = OB_SUCCESS;
  ObOperatorKit *kit = ctx.get_operator_kit(op.id_);
  if (OB_ISNULL(kit) || OB_ISNULL(kit->op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null ptr", K(ret), K(op.id_));
  } else if (PHY_HASH_JOIN == op.type_) {
    if (OB_ISNULL(kit->input_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("operator is NULL", K(ret), KP(kit));
    } else {
      ObHashJoinSpec &hj_spec = static_cast<ObHashJoinSpec&>(op);
      ObHashJoinInput *input = static_cast<ObHashJoinInput*>(kit->input_);
      if (OB_ISNULL(input)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("input not found for op", "op_id", op.id_, K(ret));
      } else if (hj_spec.is_shared_ht_ && OB_SUCCESS != ret_) {
        input->set_error_code(ret_);
        LOG_TRACE("debug post apply info", K(ret_));
      } else {
        LOG_TRACE("debug post apply info", K(ret_));
      }
    }
  } else if (PHY_VEC_HASH_JOIN == op.type_) {
    if (OB_ISNULL(kit->input_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("operator is NULL", K(ret), KP(kit));
    } else {
      ObHashJoinVecSpec &hj_spec = static_cast<ObHashJoinVecSpec&>(op);
      ObHashJoinVecInput *input = static_cast<ObHashJoinVecInput*>(kit->input_);
      if (OB_ISNULL(input)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("input not found for op", "op_id", op.id_, K(ret));
      } else if (hj_spec.is_shared_ht_ && OB_SUCCESS != ret_) {
        input->set_error_code(ret_);
        LOG_TRACE("debug post apply info", K(ret_));
      } else {
        LOG_TRACE("debug post apply info", K(ret_));
      }
    }
  } else if (PHY_WINDOW_FUNCTION == op.type_) {
    if (OB_ISNULL(kit->input_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("operator is NULL", K(ret), KP(kit));
    } else {
      ObWindowFunctionSpec &wf_spec = static_cast<ObWindowFunctionSpec&>(op);
      ObWindowFunctionOpInput *input = static_cast<ObWindowFunctionOpInput*>(kit->input_);
      if (OB_ISNULL(input)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("input not found for op", "op_id", op.id_, K(ret));
      } else if (wf_spec.is_participator() && OB_SUCCESS != ret_) {
        input->set_error_code(ret_);
        LOG_TRACE("debug post apply info", K(ret_));
      } else {
        LOG_TRACE("debug post apply info", K(ret_));
      }
    }
  } else if (PHY_VEC_WINDOW_FUNCTION == op.type_) {
    if (OB_ISNULL(kit->input_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("operator is null", K(ret), K(kit));
    } else {
      ObWindowFunctionVecSpec &wf_spec = static_cast<ObWindowFunctionVecSpec &>(op);
      ObWindowFunctionOpInput *input = static_cast<ObWindowFunctionOpInput *>(kit->input_);
      if (OB_ISNULL(input)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("input is null", K(ret));
      } else if (wf_spec.is_participator() && OB_SUCCESS != ret_) {
        input->set_error_code(ret_);
        LOG_TRACE("debug post apply info", K(ret_));
      } else {
        LOG_TRACE("debug post apply info", K(ret_));
      }
    }
  } else if (PHY_PX_MULTI_PART_INSERT == op.get_type()) {
    if (OB_ISNULL(kit->input_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("operator is NULL", K(ret), KP(kit));
    } else {
      ObPxMultiPartInsertOpInput *input = static_cast<ObPxMultiPartInsertOpInput *>(kit->input_);
      if (OB_ISNULL(input)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("input not found for op", "op_id", op.id_, K(ret));
      } else if (OB_SUCCESS != ret_) {
        input->set_error_code(ret_);
        LOG_TRACE("debug post apply info", K(ret_));
      } else {
        LOG_TRACE("debug post apply info", K(ret_));
      }
    }
  }
  return ret;
}

int ObPxTaskProcess::OpPostparation::reset(ObOpSpec &op)
{
  int ret = OB_SUCCESS;
  UNUSED(op);
  return ret;
}

uint64_t ObPxTaskProcess::get_session_id() const
{
  uint64_t session_id = 0;
  ObExecContext *exec_ctx = NULL;
  ObSQLSessionInfo *session = NULL;
  if (OB_ISNULL(exec_ctx = arg_.exec_ctx_)) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "exec ctx is NULL", K(exec_ctx));
  } else if (OB_ISNULL(session = exec_ctx->get_my_session())) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "session is NULL", K(exec_ctx));
  } else {
    session_id = session->get_sessid();
  }
  return session_id;
}

uint64_t ObPxTaskProcess::get_tenant_id() const
{
  uint64_t tenant_id = 0;
  ObExecContext *exec_ctx = NULL;
  ObSQLSessionInfo *session = NULL;
  if (OB_ISNULL(exec_ctx = arg_.exec_ctx_)) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "exec ctx is NULL", K(exec_ctx));
  } else if (OB_ISNULL(session = exec_ctx->get_my_session())) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "session is NULL", K(exec_ctx));
  } else {
    tenant_id = session->get_effective_tenant_id();
  }
  return tenant_id;
}

