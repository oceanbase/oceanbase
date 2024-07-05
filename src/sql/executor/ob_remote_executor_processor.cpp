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
#include "sql/executor/ob_remote_executor_processor.h"
#include "lib/stat/ob_session_stat.h"
#include "lib/utility/ob_tracepoint.h"
#include "lib/oblog/ob_warning_buffer.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "sql/ob_sql_trans_util.h"
#include "sql/ob_end_trans_callback.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/session/ob_sql_session_mgr.h"
#include "sql/monitor/ob_exec_stat_collector.h"
#include "observer/mysql/ob_mysql_request_manager.h"
#include "observer/mysql/obmp_base.h"
#include "observer/ob_req_time_service.h"
#include "observer/ob_server.h"
#include "observer/ob_server.h"
#include "lib/stat/ob_session_stat.h"
#include "sql/ob_sql.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/tx/ob_trans_service.h"
#include "sql/engine/expr/ob_expr_last_refresh_scn.h"

namespace oceanbase
{
using namespace obrpc;
using namespace common;
using namespace observer;
namespace sql
{
template <typename T>
int ObRemoteBaseExecuteP<T>::base_init()
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx &executor_ctx = exec_ctx_.get_task_exec_ctx();

  exec_ctx_.get_sql_ctx()->schema_guard_ = &schema_guard_;
  exec_ctx_.get_sql_ctx()->is_remote_sql_ = true;
  exec_ctx_.get_sql_ctx()->retry_times_ = 0;
  executor_ctx.schema_service_ = gctx_.schema_service_;
  executor_ctx.set_min_cluster_version(GET_MIN_CLUSTER_VERSION());
  return ret;
}

template<typename T>
int ObRemoteBaseExecuteP<T>::base_before_process(int64_t tenant_schema_version,
                                                 int64_t sys_schema_version,
                                                 const DependenyTableStore &dependency_tables)
{
  ObActiveSessionGuard::get_stat().in_sql_execution_ = true;
  bool table_version_equal = false;
  int ret = OB_SUCCESS;
  process_timestamp_ = ObTimeUtility::current_time();
  ObSQLSessionInfo *session_info = exec_ctx_.get_my_session();
  ObTaskExecutorCtx &executor_ctx = exec_ctx_.get_task_exec_ctx();
  ObVirtualTableCtx vt_ctx;
  int64_t tenant_local_version = -1;
  int64_t sys_local_version = -1;
  int64_t query_timeout = 0;
  uint64_t tenant_id = 0;

  if (OB_ISNULL(gctx_.schema_service_) || OB_ISNULL(gctx_.sql_engine_)
      || OB_ISNULL(gctx_.executor_rpc_) || OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("schema service or sql engine is NULL", K(ret),
              K(gctx_.schema_service_), K(gctx_.sql_engine_));
  } else if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_1_0_0 &&
      (tenant_schema_version == OB_INVALID_VERSION || sys_schema_version == OB_INVALID_VERSION)) {
    // For 4.1 and later versions, it is not allowed to pass schema_version as -1
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_schema_version and sys_schema_version", K(ret),
        K(tenant_schema_version), K(sys_schema_version));
  } else if (FALSE_IT(tenant_id = session_info->get_effective_tenant_id())) {
    // record tanent_id
  } else if (tenant_id == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tanent_id", K(ret));
  } else if (FALSE_IT(THIS_WORKER.set_compatibility_mode(
      ORACLE_MODE == session_info->get_compatibility_mode() ?
          lib::Worker::CompatMode::ORACLE : lib::Worker::CompatMode::MYSQL))) {
    //设置RPC work线程的租户兼容模式
  } else if (OB_FAIL(gctx_.schema_service_->get_tenant_refreshed_schema_version(tenant_id, tenant_local_version))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      // 本地schema可能还没刷出来
      tenant_local_version = OB_INVALID_VERSION;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get tenant refreshed schema version", K(ret), K(tenant_id));
    }
  } else if (OB_FAIL(gctx_.schema_service_->get_tenant_refreshed_schema_version(
      OB_SYS_TENANT_ID, sys_local_version))) {
    LOG_WARN("fail to get systenant refresh schema version", K(ret));
  }

  if (OB_FAIL(ret)) {
    //do nothing
  } else if (sys_schema_version > sys_local_version) {
    if (OB_FAIL(try_refresh_schema_(OB_SYS_TENANT_ID, sys_schema_version, session_info->is_inner()))) {
      LOG_WARN("fail to try refresh systenant schema", KR(ret), K(sys_schema_version), K(sys_local_version));
    }
  }

  if (OB_SUCC(ret)) {
    // First fetch the latest local schema_guard
    if (OB_FAIL(gctx_.schema_service_->get_tenant_schema_guard(
        tenant_id, schema_guard_, tenant_local_version, sys_local_version))) {
      LOG_WARN("fail to get schema guard", K(ret), K(tenant_local_version),
          K(sys_local_version), K(tenant_schema_version), K(sys_schema_version));
    }
  }

  if (OB_SUCC(ret) && tenant_schema_version != tenant_local_version) {
    // You must have obtained schema_guard_ before,
    // and obtained the latest schema version of the current server
    if (OB_FAIL(ObSQLUtils::check_table_version(table_version_equal, dependency_tables, schema_guard_))) {
      LOG_WARN("fail to check table_version", K(ret), K(dependency_tables));
    } else if (!table_version_equal) {
      if (tenant_schema_version == OB_INVALID_VERSION) {
        // When the table version is inconsistent, the schema_version sent from the control terminal is -1,
        // and you need to retry until the table version is consistent
        ret = OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH;
        LOG_WARN("dependency table version is not equal, need retry", K(ret));
      } else if (tenant_schema_version > tenant_local_version) {
        // The local schema version is behind. At this point,
        // you need to refresh the schema version and reacquire schema_guard
        if (OB_FAIL(try_refresh_schema_(tenant_id, tenant_schema_version, session_info->is_inner()))) {
          LOG_WARN("fail to try refresh tenant schema", KR(ret), K(tenant_id),
                    K(tenant_schema_version), K(tenant_local_version));
        } else if (OB_FAIL(gctx_.schema_service_->get_tenant_schema_guard(
            tenant_id, schema_guard_, tenant_schema_version, sys_schema_version))) {
          LOG_WARN("fail to get schema guard", K(ret), K(tenant_schema_version), K(sys_schema_version));
        }
      } else if (tenant_schema_version <= tenant_local_version) {
        // In this scenario, schema_guard needs to be taken again
        if (OB_FAIL(gctx_.schema_service_->get_tenant_schema_guard(
            tenant_id, schema_guard_, tenant_schema_version, sys_schema_version))) {
         LOG_WARN("fail to get schema guard", K(ret), K(tenant_schema_version), K(sys_schema_version));
        }
      }
    } else {
      // If table_version_equal == true, the table schema is consistent,
      // At this point, there is no need to reacquire schema_guard, the current schema_guard is available
    }
  }

  int64_t local_tenant_schema_version = -1;
  if (OB_FAIL(ret)) {
    //do nothing
  } else if (OB_FAIL(schema_guard_.get_schema_version(tenant_id, local_tenant_schema_version))) {
    LOG_WARN("get schema version from schema_guard failed", K(ret));
  } else if (OB_FAIL(session_info->get_query_timeout(query_timeout))) {
    LOG_WARN("get query timeout failed", K(ret));
  } else {
    THIS_WORKER.set_timeout_ts(query_timeout + session_info->get_query_start_time());
    session_info->set_peer_addr(ObRpcProcessor<T>::arg_.get_ctrl_server());
    exec_ctx_.set_my_session(session_info);
    exec_ctx_.show_session();
    exec_ctx_.get_sql_ctx()->session_info_ = session_info;
    exec_ctx_.set_mem_attr(ObMemAttr(tenant_id, ObModIds::OB_SQL_EXEC_CONTEXT, ObCtxIds::EXECUTE_CTX_ID));
    ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(exec_ctx_);
    plan_ctx->set_rich_format(session_info->use_rich_format());
    exec_ctx_.hide_session(); // don't show remote session in show processlist
    vt_ctx.session_ = session_info;
    vt_ctx.vt_iter_factory_ = &vt_iter_factory_;
    vt_ctx.schema_guard_ = &schema_guard_;
    exec_ctx_.set_virtual_table_ctx(vt_ctx);
  }
  LOG_TRACE("print tenant_schema_version for remote_execute", K(tenant_schema_version), K(tenant_local_version),
      K(local_tenant_schema_version), K(table_version_equal), K(ret), K(sys_schema_version), K(sys_local_version));
  if (OB_FAIL(ret)) {
    if (local_tenant_schema_version != tenant_schema_version) {
      if (is_schema_error(ret)) {
        ret = OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH; // 重写错误码，使得scheduler端能等待远端schema刷新并重试
      }
    } else if (ret == OB_TENANT_NOT_EXIST) {
      // fix bug:
      // 控制端重启observer，导致租户schema没刷出来，发送过来的schema_version异常, 让对端重试
      ret = OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH;
    } else {
      if (OB_SCHEMA_ERROR == ret || OB_SCHEMA_EAGAIN == ret) {
        ret = OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH; // 针对OB_SCHEMA_ERROR 和OB_SCHEMA_EAGAIN这两个错误码，远程执行暂时先考虑重写，等待远端schema刷新并重试
      }
    }
    // overwrite ret to make sure sql will retry
    if (OB_NOT_NULL(session_info)
        && OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH == ret
        && GSCHEMASERVICE.is_schema_error_need_retry(NULL, tenant_id)) {
      ret = OB_ERR_REMOTE_SCHEMA_NOT_FULL;
    }
  }
  return ret;
}

template<typename T>
int ObRemoteBaseExecuteP<T>::auto_start_phy_trans()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(exec_ctx_);
  ObSQLSessionInfo *my_session = GET_MY_SESSION(exec_ctx_);
  if (OB_ISNULL(plan_ctx) || OB_ISNULL(my_session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("plan_ctx or my_session is NULL", K(ret), K(plan_ctx), K(my_session));
  } else {
    // 远程单partition并且是autocommit的情况下才运行start_trans和start_stmt
    // bool in_trans = my_session->is_in_transaction();
    bool ac = true;
    my_session->set_early_lock_release(plan_ctx->get_phy_plan()->stat_.enable_early_lock_release_);
    if (OB_FAIL(my_session->get_autocommit(ac))) {
      LOG_WARN("fail to get autocommit", K(ret));
    } else
    //这里不能用本地生成的执行计划，因为其plan type是local
    // if (ObSqlTransUtil::is_remote_trans(ac, in_trans, OB_PHY_PLAN_REMOTE)) {
    // NOTE: autocommit modfied by PL cannot sync to remote,
    // use has_start_stmt to test transaction prepared on original node
    if (false == my_session->has_start_stmt()) {
      ObSQLSessionInfo::LockGuard data_lock_guard(my_session->get_thread_data_lock());
      //start Tx on remote node, we need release txDesc deserilized by session
      if (OB_NOT_NULL(my_session->get_tx_desc())) {
        MTL(transaction::ObTransService*)->release_tx(*my_session->get_tx_desc());
        my_session->get_tx_desc() = NULL;
      }
      if (trans_state_.is_start_stmt_executed()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("start_stmt is executed", K(ret));
      } else if (OB_FAIL(ObSqlTransControl::start_stmt(exec_ctx_))) {
          LOG_WARN("fail to start stmt", K(ret));
      } else {
        trans_state_.set_start_stmt_executed(OB_SUCC(ret));
      }
    }

    if (OB_UNLIKELY(DAS_CTX(exec_ctx_).get_table_loc_list().empty())) {
      //uncertain plan的分区信息在执行前可能为空，因此如果是uncertain plan，这里直接跳过
      if (OB_PHY_PLAN_UNCERTAIN != plan_ctx->get_phy_plan()->get_location_type()
          && !plan_ctx->get_phy_plan()->use_das()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("remote plan has empty participant", K(ret));
      }
    }
  }
  return ret;
}

/// 将结果同步回复给客户端
template<typename T>
int ObRemoteBaseExecuteP<T>::sync_send_result(ObExecContext &exec_ctx,
                                              const ObPhysicalPlan &plan,
                                              ObScanner &scanner)
{
  int ret = OB_SUCCESS;
  const ObNewRow *row = NULL;
  int last_row_used = true;// scanner满时标记缓存上一行
  int64_t total_row_cnt = 0;
  ObOperator *se_op = NULL;
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(exec_ctx_);
  ObSQLSessionInfo *my_session = GET_MY_SESSION(exec_ctx_);
  if (OB_ISNULL(my_session) || OB_ISNULL(plan_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid argument", K(plan_ctx), K(exec_ctx.get_my_session()));
  } else {
    const ObOpSpec *spec = NULL;
    ObOperatorKit *kit = NULL;
    if (OB_ISNULL(spec = plan.get_root_op_spec())
        || OB_ISNULL(kit = exec_ctx.get_kit_store().get_operator_kit(spec->id_))
        || OB_ISNULL(se_op = kit->op_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("root op spec is null", K(ret));
    }
  }
  ObBatchRowIter br_it_(se_op);
  if (OB_SUCC(ret) && plan.with_rows()) {
    if (OB_FAIL(se_op->is_vectorized() ? br_it_.get_next_row() : se_op->get_next_row())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to get next row", K(ret));
      }
    }
    while (OB_SUCC(ret) && OB_SUCC(exec_ctx.check_status())) {
      scanner.set_tenant_id(exec_ctx.get_my_session()->get_effective_tenant_id());
      bool need_flush = false;
      ObEvalCtx::BatchInfoScopeGuard batch_info_guard(se_op->get_eval_ctx());
      bool added = false;
      if (se_op->is_vectorized()) {
        batch_info_guard.set_batch_size(br_it_.get_brs()->size_);
        batch_info_guard.set_batch_idx(br_it_.cur_idx());
      }
      if (OB_FAIL(scanner.try_add_row(se_op->get_spec().output_,
                                      &se_op->get_eval_ctx(),
                                      added))) {
        LOG_WARN("fail add row to scanner", K(ret));
      } else if (!added) {
        need_flush = true;
      }
      if (OB_SUCC(ret)) {
        if (need_flush) {
          last_row_used = false;
          //set user var map
          if (OB_FAIL(scanner.set_session_var_map(exec_ctx.get_my_session()))) {
            LOG_WARN("set user var to scanner failed");
          } else {
            has_send_result_ = true;
            // override error code
            if (OB_FAIL(ObRpcProcessor<T>::flush(THIS_WORKER.get_timeout_remain(), &ObRpcProcessor<T>::arg_.get_ctrl_server()))) {
              LOG_WARN("fail to flush", K(ret));
            } else {
              // 超过1个scanner的情况，每次发送都打印一条日志
              NG_TRACE_EXT(scanner, OB_ID(row_count), scanner.get_row_count(),
                           OB_ID(total_count), total_row_cnt);
              // 数据发送完成后，scanner可以重用
              scanner.reuse();
            }
          }
        } else {
          LOG_DEBUG("scanner add row", K(ret));
          last_row_used = true;
          total_row_cnt++;
        }
      }
      // 如果上次读取的数据还没有吐出到scanner，则跳过本次获取row
      if (OB_SUCC(ret)) {
        if (last_row_used) {
          if (OB_FAIL(se_op->is_vectorized() ? br_it_.get_next_row() : se_op->get_next_row())) {
            if (OB_UNLIKELY(OB_ITER_END != ret)) {
              LOG_WARN("failed to get next row", K(ret));
            } else {}
          } else {}
        }
      }
    } // while end
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  if (OB_SUCC(ret)) {
    //所有需要通过scanner传回的数据先存入collector
    if (OB_FAIL(scanner.set_session_var_map(exec_ctx.get_my_session()))) {
      LOG_WARN("set user var to scanner failed");
    } else {
      scanner.set_found_rows(plan_ctx->get_found_rows());
      if (OB_FAIL(scanner.set_row_matched_count(plan_ctx->get_row_matched_count()))) {
        LOG_WARN("fail to set row matched count", K(ret), K(plan_ctx->get_row_matched_count()));
      } else if (OB_FAIL(scanner.set_row_duplicated_count(plan_ctx->get_row_duplicated_count()))) {
        LOG_WARN("fail to set row duplicated count",
                 K(ret), K(plan_ctx->get_row_duplicated_count()));
      } else {
        // 对于INSERT UPDATE等，立即设置affected row，对于select，这里总为0
        scanner.set_affected_rows(plan_ctx->get_affected_rows());
        //scanner.set_force_rollback(plan_ctx->is_force_rollback());
      }

      scanner.set_memstore_read_row_count(
        my_session->get_raw_audit_record().exec_record_.get_cur_memstore_read_row_count());
      scanner.set_ssstore_read_row_count(
        my_session->get_raw_audit_record().exec_record_.get_cur_ssstore_read_row_count());
      // set last_insert_id no matter success or fail after open
      scanner.set_last_insert_id_to_client(plan_ctx->calc_last_insert_id_to_client());
      scanner.set_last_insert_id_session(plan_ctx->calc_last_insert_id_session());
      scanner.set_last_insert_id_changed(plan_ctx->get_last_insert_id_changed());
      if (!plan_ctx->is_result_accurate()) {
        scanner.set_is_result_accurate(plan_ctx->is_result_accurate());
      }
      OZ(scanner.assign_implicit_cursor(plan_ctx->get_implicit_cursor_infos()));
      if (OB_SUCC(ret)) {
        if (OB_FAIL(scanner.get_table_row_counts().assign(plan_ctx->get_table_row_count_list()))) {
          LOG_WARN("fail to set scan rowcount stat", K(ret));
        }
        LOG_DEBUG("table row counts",
                  K(scanner.get_table_row_counts()),
                  K(plan_ctx->get_table_row_count_list()));
      }
      NG_TRACE_EXT(remote_result, OB_ID(found_rows), plan_ctx->get_found_rows(),
                   OB_ID(affected_rows), scanner.get_affected_rows(),
                   OB_ID(last_insert_id), scanner.get_last_insert_id_session(),
                   OB_ID(last_insert_id), scanner.get_last_insert_id_to_client(),
                   OB_ID(last_insert_id), scanner.get_last_insert_id_changed());
    }
  }
  // TODO:
  // 如果发生了错误，如何主动通知客户端，使其提前终止
  //

  return ret;
}

template<typename T>
int ObRemoteBaseExecuteP<T>::auto_end_phy_trans(bool is_rollback)
{
  int ret = OB_SUCCESS;
  int end_ret = OB_SUCCESS;
  bool ac = false;
  ObSQLSessionInfo *my_session = GET_MY_SESSION(exec_ctx_);
  // NOTE: 事务状态已经在auto_start_phy_trans()里面改变过了，
  // 不可以再调用ObSqlTransUtil::is_remote_trans来判断。
  // 所以依据内部的trans_state_来判断即可
  if (trans_state_.is_start_stmt_executed() &&
      trans_state_.is_start_stmt_success()) {
    if (OB_SUCCESS != (end_ret = ObSqlTransControl::end_stmt(
                exec_ctx_, is_rollback || OB_SUCCESS != ret))) {
      ret = (OB_SUCCESS == ret) ? end_ret : ret;
      LOG_WARN("fail to end stmt", K(ret), K(end_ret), K(is_rollback), K(exec_ctx_));
    }
    trans_state_.clear_start_stmt_executed();
    is_rollback = (is_rollback || OB_SUCCESS != ret);
    if (OB_SUCCESS != (end_ret = ObSqlTransControl::implicit_end_trans(
                      exec_ctx_, is_rollback))) {
      ret = (OB_SUCCESS == ret) ? end_ret : ret;
      LOG_WARN("fail end implicit trans", K(is_rollback), K(ret), K_(exec_ctx));
    }
  }
  trans_state_.reset();
  return ret;
}

template<typename T>
int ObRemoteBaseExecuteP<T>::execute_remote_plan(ObExecContext &exec_ctx,
                                                 const ObPhysicalPlan &plan)
{
  int ret = OB_SUCCESS;
  int last_err = OB_SUCCESS;
  int close_ret = OB_SUCCESS;
  ObSQLSessionInfo *session = exec_ctx.get_my_session();
  ObPhysicalPlanCtx *plan_ctx = exec_ctx.get_physical_plan_ctx();
  ObOperator *se_op = nullptr; // static engine operator
  exec_ctx.set_use_temp_expr_ctx_cache(true);
  FLTSpanGuard(remote_execute);
  if (OB_ISNULL(plan_ctx) || OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("op is NULL", K(ret), K(plan_ctx), K(session));
  }
  if (OB_SUCC(ret)) {
    int64_t retry_times = 0;
    do {
      if (!is_execute_remote_plan()) {
        // remote execute using send sql_info, should init_phy_op
        if (OB_FAIL(exec_ctx.init_phy_op(plan.get_phy_operator_size()))) {
          LOG_WARN("init physical operator ctx buckets failed",
                   K(ret), K(plan.get_phy_operator_size()));
        } else if (OB_FAIL(exec_ctx.init_expr_op(plan.get_expr_operator_size()))) {
          LOG_WARN("init expr operator ctx buckets failed", K(ret), K(plan.get_expr_operator_size()));
        }
      }
      // ========================== 开启事务 ===========================
      if (OB_FAIL(ret)) {

      } else if (OB_LIKELY(plan.is_need_trans()) && OB_FAIL(auto_start_phy_trans())) {
        LOG_WARN("fail to begin trans", K(ret));
      } else {
        if (OB_FAIL(plan.get_expr_frame_info().pre_alloc_exec_memory(exec_ctx))) {
          LOG_WARN("fail to pre allocate memory", K(ret), K(plan.get_expr_frame_info()));
        } else if (OB_ISNULL(plan.get_root_op_spec())) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument",
                   K(plan.get_root_op_spec()), K(ret));
        } else if (OB_FAIL(plan.get_root_op_spec()->create_operator(exec_ctx, se_op))) {
          LOG_WARN("create operator from spec failed", K(ret));
        } else if (OB_ISNULL(se_op)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("created operator is NULL", K(ret));
        } else if (OB_FAIL(plan_ctx->reserve_param_space(plan.get_param_count()))) {
          LOG_WARN("reserve rescan param space failed", K(ret), K(plan.get_param_count()));
        } else if (!plan.get_mview_ids().empty() && plan_ctx->get_mview_ids().empty()
                   && OB_FAIL(ObExprLastRefreshScn::set_last_refresh_scns(plan.get_mview_ids(),
                                                                          exec_ctx.get_sql_proxy(),
                                                                          exec_ctx.get_my_session(),
                                                                          exec_ctx.get_das_ctx().get_snapshot().core_.version_,
                                                                          plan_ctx->get_mview_ids(),
                                                                          plan_ctx->get_last_refresh_scns()))) {
          LOG_WARN("fail to set last_refresh_scns", K(ret), K(plan.get_mview_ids()));
        } else {
          if (OB_FAIL(se_op->open())) {
            LOG_WARN("fail open task", K(ret));
          } else if (OB_FAIL(send_result_to_controller(exec_ctx_, plan))) {
            LOG_WARN("sync send result failed", K(ret));
          }
          if (OB_SUCCESS != (close_ret = se_op->close())) {
            LOG_WARN("fail close task", K(close_ret));
          }
          ret = (OB_SUCCESS == ret) ? close_ret : ret;
        }
        NG_TRACE_EXT(process_ret, OB_ID(process_ret), ret);
      }

      // ========================== 结束事务 ===========================
      // 注意!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
      // 改代码的时候要小心，不要把结束事务这一段包进if(OB_SUCC(ret))之类条件的大括号里面，
      // 不管ret是什么，只要调用了auto_start_phy_trans都必须要调用auto_end_phy_trans
      bool is_rollback = false;
      // 如果必要，结束事务
      if (OB_FAIL(ret)) {
        is_rollback = true;
      }
      int trans_end_ret = OB_SUCCESS;
      if (OB_SUCCESS != (trans_end_ret = auto_end_phy_trans(is_rollback))) {
        LOG_WARN("fail to end trans", K(trans_end_ret));
      }
      ret = (OB_SUCCESS == ret) ? trans_end_ret : ret;
      if (OB_FAIL(ret)) {
        exec_ctx_.reset_op_env();
        if (has_send_result_) {
          clean_result_buffer();
        }
      }
    } while (query_can_retry_in_remote(last_err, ret, *session, retry_times));
  }
  return ret;
}

template<typename T>
bool ObRemoteBaseExecuteP<T>::query_can_retry_in_remote(int &last_err,
                                                        int &err,
                                                        ObSQLSessionInfo &session,
                                                        int64_t &retry_times)
{
  bool bret = false;
  bool ac = session.get_local_autocommit();
  bool in_trans = session.get_in_transaction();
  if (is_execute_remote_plan()) {
    bret = false;  // remote execute using remote_phy_plan, should not retry
  } else {
    if (ObSqlTransUtil::is_remote_trans(ac, in_trans, OB_PHY_PLAN_REMOTE) && retry_times <= 100) {
      //暂时拍脑袋决定重试100次不成功就直接返回，不要让控制端等太久的时间
      if (is_try_lock_row_err(err)) {
        bret = true;
      } else if (is_transaction_set_violation_err(err) || is_snapshot_discarded_err(err)) {
        if (!ObSqlTransControl::is_isolation_RR_or_SE(session.get_tx_isolation())) {
          //不是Serializable隔离级别，可以在远端重试
          bret = true;
        }
      }
      if (bret) {
        //重试之前保存当前错误码
        last_err = err;
        if (OB_SUCCESS != (err = exec_ctx_.check_status())) {
          bret = false;
          LOG_WARN_RET(err, "cehck execute status failed", K(err), K(last_err), K(bret));
          err = last_err; //返回真实值
        } else {
          LOG_INFO("query retry in remote", K(retry_times), K(last_err));
          ++retry_times;
          int64_t base_sleep_us = 1000;
          ob_usleep(base_sleep_us * retry_times);
        }
      }
    }
    if (!bret && is_try_lock_row_err(last_err) && is_timeout_err(err)) {
      //锁冲突重试到超时，最后直接给用户报LOCK CONFLICT错误
      LOG_WARN_RET(err, "retry query until query timeout", K(last_err), K(err));
      err = OB_ERR_EXCLUSIVE_LOCK_CONFLICT;
    }
  }
  return bret;
}

template<typename T>
void ObRemoteBaseExecuteP<T>::record_sql_audit_and_plan_stat(
                                               const ObPhysicalPlan *plan,
                                               ObSQLSessionInfo *session)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(session));
  } else {
    const bool enable_sql_audit =
        GCONF.enable_sql_audit && session->get_local_ob_enable_sql_audit();
    if (enable_sql_audit) {
      ObAuditRecordData &audit_record = session->get_raw_audit_record();
      audit_record.try_cnt_++;
      audit_record.seq_ = 0;  //don't use now
      audit_record.status_ =
          (OB_SUCCESS == ret || common::OB_ITER_END == ret) ? obmysql::REQUEST_SUCC : ret;
      if (OB_NOT_NULL(plan)) {
        const ObString &sql_id = plan->get_sql_id_string();
        int64_t length = sql_id.length();
        MEMCPY(audit_record.sql_id_, sql_id.ptr(), std::min(length, OB_MAX_SQL_ID_LENGTH));
        audit_record.sql_id_[OB_MAX_SQL_ID_LENGTH] = '\0';
      } else {
        session->get_cur_sql_id(audit_record.sql_id_, OB_MAX_SQL_ID_LENGTH + 1);
      }
      audit_record.db_id_ = session->get_database_id();
      audit_record.execution_id_ = session->get_current_execution_id();
      audit_record.client_addr_ = session->get_client_addr();
      audit_record.user_client_addr_ = session->get_user_client_addr();
      audit_record.user_group_ = THIS_WORKER.get_group_id();
      audit_record.affected_rows_ = 0;
      audit_record.return_rows_ = 0;

      audit_record.plan_id_ = plan != nullptr ? plan->get_plan_id() : 0;
      audit_record.plan_type_ = plan != nullptr ? plan->get_plan_type() : OB_PHY_PLAN_UNINITIALIZED;
      audit_record.is_executor_rpc_ = true;
      audit_record.is_inner_sql_ = session->is_inner();
      audit_record.is_hit_plan_cache_ = true;
      audit_record.is_multi_stmt_ = false;
      audit_record.is_perf_event_closed_ = !lib::is_diagnose_info_enabled();

      //统计plan相关的信息
      ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(exec_ctx_);
      ObIArray<ObTableRowCount> *table_row_count_list = NULL;
      if (NULL != plan_ctx) {
        audit_record.consistency_level_ = plan_ctx->get_consistency_level();
        audit_record.table_scan_stat_ = plan_ctx->get_table_scan_stat();
        table_row_count_list = &(plan_ctx->get_table_row_count_list());
      }
      if (NULL != plan) {
        ObPhysicalPlan *mutable_plan = const_cast<ObPhysicalPlan*>(plan);
        if (!exec_ctx_.get_sql_ctx()->self_add_plan_ && exec_ctx_.get_sql_ctx()->plan_cache_hit_) {
          mutable_plan->update_plan_stat(audit_record,
                                        false, // false mean not first update plan stat
                                        table_row_count_list);
        } else if (exec_ctx_.get_sql_ctx()->self_add_plan_ && !exec_ctx_.get_sql_ctx()->plan_cache_hit_) {
          mutable_plan->update_plan_stat(audit_record,
                                        true,
                                        table_row_count_list);

        }
      }
    }
    ObSQLUtils::handle_audit_record(false, EXECUTE_REMOTE, *session);
  }

}

template<typename T>
int ObRemoteBaseExecuteP<T>::execute_with_sql(ObRemoteTask &task)
{
  int ret = OB_SUCCESS;
  NG_TRACE(exec_remote_plan_begin);
  ObExecRecord exec_record;
  ObExecTimestamp exec_timestamp;
  exec_timestamp.exec_type_ = RpcProcessor;
  int64_t local_tenant_schema_version = -1;
  ObSQLSessionInfo *session = NULL;
  const bool enable_perf_event = lib::is_diagnose_info_enabled();
  bool enable_sql_audit = GCONF.enable_sql_audit;
  ObPhysicalPlan *plan = nullptr;
  ObPhysicalPlanCtx *plan_ctx = nullptr;
  exec_ctx_.set_use_temp_expr_ctx_cache(false);
  int inject_err_no = EVENT_CALL(EventTable::EN_REMOTE_EXEC_ERR);
  if (0 != inject_err_no) {
    ret = inject_err_no;
    LOG_WARN("Injection OB_LOCATION_NOT_EXIST error", K(ret));
  } else if (OB_ISNULL(session = exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("session is NULL", K(ret), K(task));
  } else if (OB_ISNULL(plan_ctx = GET_PHY_PLAN_CTX(exec_ctx_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("plan_ctx is NULL", K(ret), K(plan_ctx));
  } else if (OB_ISNULL(gctx_.sql_engine_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("sql engine is NULL", K(ret), K(gctx_.sql_engine_));
  } else if (OB_FAIL(schema_guard_.get_schema_version(session->get_effective_tenant_id(), local_tenant_schema_version))) {
    LOG_WARN("get schema version from schema_guard failed", K(ret));
  } else {
    enable_sql_audit = enable_sql_audit && session->get_local_ob_enable_sql_audit();
  }

  // 设置诊断功能环境
  if (OB_SUCC(ret)) {
    ObSessionStatEstGuard stat_est_guard(session->get_effective_tenant_id(), session->get_sessid());
    SQL_INFO_GUARD(task.get_remote_sql_info()->remote_sql_, session->get_cur_sql_id());
    // 初始化ObTask的执行环节
    //
    //
    // 执行ObTask, 处理结果通过Result返回
    ObWaitEventDesc max_wait_desc;
    ObWaitEventStat total_wait_desc;
    ObDiagnoseSessionInfo *di = ObDiagnoseSessionInfo::get_local_diagnose_info();
    {
      ObMaxWaitGuard max_wait_guard(enable_perf_event ? &max_wait_desc : NULL, di);
      ObTotalWaitGuard total_wait_guard(enable_perf_event ? &total_wait_desc : NULL, di);
      if (enable_perf_event) {
        exec_record.record_start();
      }
      if (OB_FAIL(gctx_.sql_engine_->handle_remote_query(plan_ctx->get_remote_sql_info(),
                                                         *exec_ctx_.get_sql_ctx(),
                                                         exec_ctx_,
                                                         guard_))) {
        LOG_WARN("handle remote query failed", K(ret), K(task));
      } else if (FALSE_IT(plan = static_cast<ObPhysicalPlan*>(guard_.get_cache_obj()))) {
        // do nothing
      } else if (OB_ISNULL(plan)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("plan is null", K(ret));
      }
      //监控项统计开始
      exec_start_timestamp_ = ObTimeUtility::current_time();
      exec_ctx_.set_plan_start_time(exec_start_timestamp_);
      ObAuditRecordData &audit_record = session->get_raw_audit_record();

      if (OB_SUCC(ret)) {
        NG_TRACE_EXT(execute_task, OB_ID(task), task, OB_ID(stmt_type), plan->get_stmt_type());
        ObWorkerSessionGuard worker_session_guard(session);
        ObSQLSessionInfo::LockGuard lock_guard(session->get_query_lock());
        audit_record.request_memory_used_ = 0;
        observer::ObProcessMallocCallback pmcb(0, audit_record.request_memory_used_);
        lib::ObMallocCallbackGuard guard(pmcb);
        session->set_peer_addr(task.get_ctrl_server());
        NG_TRACE_EXT(execute_task, OB_ID(task), task, OB_ID(stmt_type), plan->get_stmt_type());
        if (OB_FAIL(execute_remote_plan(exec_ctx_, *plan))) {
          LOG_WARN("execute remote plan failed", K(ret), K(task), K(exec_ctx_.get_das_ctx().get_snapshot()));
        } else if (plan->try_record_plan_info()) {
          if(exec_ctx_.get_feedback_info().is_valid() &&
             plan->get_logical_plan().is_valid() &&
             OB_FAIL(plan->set_feedback_info(exec_ctx_))) {
            LOG_WARN("failed to set feedback info", K(ret));
          } else {
            plan->set_record_plan_info(false);
          }
        }
      }
      // ===============================================================
      //处理重试
      if (OB_FAIL(ret)) {
        if (local_tenant_schema_version != task.get_tenant_schema_version()) {
          if (is_schema_error(ret)) {
            ret = OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH; // 重写错误码，使得scheduler端能等待远端schema刷新并重试
          }
        } else {
          if (OB_SCHEMA_ERROR == ret || OB_SCHEMA_EAGAIN == ret) {
            ret = OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH; // 针对OB_SCHEMA_ERROR 和OB_SCHEMA_EAGAIN这两个错误码，远程执行暂时先考虑重写，等待远端schema刷新并重试
          }
        }
        // overwrite ret to make sure sql will retry
        if (OB_NOT_NULL(session)
            && OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH == ret
            && GSCHEMASERVICE.is_schema_error_need_retry(
               NULL, session->get_effective_tenant_id())) {
          ret = OB_ERR_REMOTE_SCHEMA_NOT_FULL;
        }
        DAS_CTX(exec_ctx_).get_location_router().refresh_location_cache_by_errno(true, ret);
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

      //此处代码要放在scanner.set_err_code(ret)代码前,避免ret被都写成了OB_SUCCESS
      record_sql_audit_and_plan_stat(plan, session);
    }
  }
  if (OB_NOT_NULL(plan)) {
    if (plan->is_limited_concurrent_num()) {
      plan->dec_concurrent_num();
    }
  }
  NG_TRACE(exec_remote_plan_end);
  //执行相关的错误信息记录在exec_errcode_中，通过scanner向控制端返回，所以这里给RPC框架返回成功
  return ret;
}

template<typename T>
int ObRemoteBaseExecuteP<T>::base_before_response(common::ObScanner &scanner)
{
  ObSQLSessionInfo *session = exec_ctx_.get_my_session();
  // if ac=1 remote plan, transaction started on remote node
  // has terminated when arrive here, the tx_desc should be NULL
  if (OB_NOT_NULL(session) && OB_NOT_NULL(session->get_tx_desc())) {
    int get_ret = MTL(transaction::ObTransService*)
      ->get_tx_exec_result(*session->get_tx_desc(), scanner.get_trans_result());
    if (OB_SUCCESS != get_ret) {
      LOG_WARN_RET(get_ret, "failed to get trans result",
               K(get_ret),
               "scanner_trans_result", scanner.get_trans_result(),
               "tx_desc", *session->get_tx_desc());
      exec_errcode_ = (OB_SUCCESS == exec_errcode_) ? get_ret : exec_errcode_;
    } else {
      LOG_TRACE("get trans_result",
                "cur_stmt", session->get_current_query_string(),
                "scanner_trans_result", scanner.get_trans_result(),
                "tx_desc", *session->get_tx_desc());
    }
  }
  if (OB_SUCCESS == exec_errcode_ && is_execute_remote_plan()) {
    ObExecFeedbackInfo &fb_info = scanner.get_feedback_info();
    exec_errcode_ = fb_info.assign(exec_ctx_.get_feedback_info());
  }
  exec_ctx_.hide_session();
  // return a scanner no matter success or fail, set err_code to scanner
  if (OB_SUCCESS != exec_errcode_) {
    bool has_store_pl_msg = false;
    scanner.set_err_code(exec_errcode_);
    if (OB_NOT_NULL(session) && 0 < session->get_pl_exact_err_msg().length()) {
      ObSqlString msg;
      const int64_t max_msg_buf = 8192 - 256;   // same as MAX_MSGBUF_SIZE
      ObString str = 0 < ob_get_tsi_err_msg(exec_errcode_).length()
                       ? ob_get_tsi_err_msg(exec_errcode_)
                       : ob_errpkt_strerror(exec_errcode_, lib::is_oracle_mode());
      if (OB_SUCCESS == msg.append(str)
            && OB_SUCCESS == msg.append(session->get_pl_exact_err_msg().ptr())
            && max_msg_buf > msg.length()) {
        scanner.store_err_msg(msg.string());
        session->get_pl_exact_err_msg().reset();
        has_store_pl_msg = true;
      }
      if (!has_store_pl_msg) {
        LOG_WARN_RET(OB_ERROR, "get pl msg fail.", K(exec_errcode_));
      }
    }
    if (!has_store_pl_msg) {
      scanner.store_err_msg(ob_get_tsi_err_msg(exec_errcode_));
    }
    exec_errcode_ = OB_SUCCESS;
  }
  return OB_SUCCESS;
}

template<typename T>
int ObRemoteBaseExecuteP<T>::base_after_process()
{
  int ret = OB_SUCCESS;
  if (exec_end_timestamp_ - exec_start_timestamp_ >=
      ObServerConfig::get_instance().trace_log_slow_query_watermark) {
    //slow mini task, print trace info
    FORCE_PRINT_TRACE(THE_TRACE, "[slow remote task]");
  }
  ObActiveSessionGuard::get_stat().in_sql_execution_ = false;
  return ret;
}

template<typename T>
void ObRemoteBaseExecuteP<T>::base_cleanup()
{
  ObActiveSessionGuard::setup_default_ash();
  exec_ctx_.cleanup_session();
  exec_errcode_ = OB_SUCCESS;
  obrpc::ObRpcProcessor<T>::cleanup();
  return;
}

template<typename T>
int ObRemoteBaseExecuteP<T>::try_refresh_schema_(const uint64_t tenant_id,
                                                 const int64_t schema_version,
                                                 const bool is_inner_sql)
{
  int ret = OB_SUCCESS;
  const int64_t timeout_remain = THIS_WORKER.get_timeout_remain();
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("arg is invalid", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(timeout_remain <= 0)) {
    ret = OB_TIMEOUT;
    LOG_WARN("THIS_WORKER is timeout", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(gctx_.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is NULL", KR(ret), K(tenant_id));
  } else {
    const int64_t orig_timeout_ts = THIS_WORKER.get_timeout_ts();
    const int64_t try_refresh_time = is_inner_sql ? timeout_remain : std::min(10 * 1000L, timeout_remain);
    THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + try_refresh_time);
    if (OB_FAIL(gctx_.schema_service_->async_refresh_schema(
                tenant_id, schema_version))) {
      LOG_WARN("fail to refresh schema", KR(ret), K(tenant_id),
                                         K(schema_version), K(try_refresh_time));
    }
    THIS_WORKER.set_timeout_ts(orig_timeout_ts);
    if (OB_TIMEOUT == ret
        && THIS_WORKER.is_timeout_ts_valid()
        && !THIS_WORKER.is_timeout()) {
      ret = OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH;
      LOG_WARN("fail to refresh schema in try refresh time", KR(ret), K(tenant_id),
                K(schema_version), K(try_refresh_time));
    }
  }
  return ret;
}

int ObRpcRemoteExecuteP::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(base_init())) {
    LOG_WARN("init remote base execute context failed", K(ret));
  }
  if (OB_SUCC(ret)) {
    uint64_t mtl_id = MTL_ID();
    mtl_id = (mtl_id == OB_INVALID_TENANT_ID ?
                        OB_SERVER_TENANT_ID :
                        mtl_id);

    result_.set_tenant_id(mtl_id);
    if (OB_FAIL(result_.init(DEFAULT_MAX_REMOTE_EXEC_PACKET_LENGTH))) {
      LOG_WARN("fail to init result", K(ret));
    } else {
      arg_.set_deserialize_param(exec_ctx_, phy_plan_);
    }
  }
  return ret;
}

int ObRpcRemoteExecuteP::send_result_to_controller(ObExecContext &exec_ctx,
                                                   const ObPhysicalPlan &plan)
{
  ObScanner &scanner = result_;
  return sync_send_result(exec_ctx, plan, scanner);
}

int ObRpcRemoteExecuteP::before_process()
{
  int ret = OB_SUCCESS;
  ObTask &task = arg_;
  ObTaskExecutorCtx &executor_ctx = exec_ctx_.get_task_exec_ctx();
  if (OB_FAIL(base_before_process(executor_ctx.get_query_tenant_begin_schema_version(),
                                  executor_ctx.get_query_sys_begin_schema_version(),
                                  task.get_des_phy_plan().get_dependency_table()))) {
    LOG_WARN("exec base before process failed", K(ret));
  } else if (OB_FAIL(task.load_code())) {
    LOG_WARN("fail to load code", K(ret));
  }
  return ret;
}

int ObRpcRemoteExecuteP::process()
{
  int &ret = exec_errcode_;
  int64_t local_tenant_schema_version = -1;
  ret = OB_SUCCESS;
  NG_TRACE(exec_remote_plan_begin);
  ObExecRecord exec_record;
  ObExecTimestamp exec_timestamp;
  exec_timestamp.exec_type_ = RpcProcessor;
  // arg_是一个ObTask对象
  ObTask &task = arg_;
  ObExecContext *exec_ctx = NULL;
  ObSQLSessionInfo *session = NULL;
  const bool enable_perf_event = lib::is_diagnose_info_enabled();
  bool enable_sql_audit = GCONF.enable_sql_audit;
  if (OB_ISNULL(exec_ctx = task.get_exec_context())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("exec ctx is NULL", K(ret), K(task));
  } else if (OB_ISNULL(session = exec_ctx->get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("session is NULL", K(ret), K(task));
  } else if (OB_ISNULL(gctx_.sql_engine_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("sql engine is NULL", K(ret), K(gctx_.sql_engine_));
  } else if (OB_FAIL(schema_guard_.get_schema_version(session->get_effective_tenant_id(), local_tenant_schema_version))) {
    LOG_WARN("get schema version from schema_guard failed", K(ret));
  } else {
    enable_sql_audit = enable_sql_audit && session->get_local_ob_enable_sql_audit();
    LOG_DEBUG("des_plan", K(task.get_des_phy_plan()));
  }

  // 设置诊断功能环境
  if (OB_SUCC(ret)) {
    ObSessionStatEstGuard stat_est_guard(
        session->get_effective_tenant_id(),
        session->get_sessid());
    SQL_INFO_GUARD(task.get_sql_string(), session->get_cur_sql_id());
    // 初始化ObTask的执行环节
    //
    //
    // 执行ObTask, 处理结果通过Result返回
    ObOpSpec *op_spec = task.get_root_spec();
    ObPhysicalPlanCtx *plan_ctx = NULL;
    ObWaitEventDesc max_wait_desc;
    ObWaitEventStat total_wait_desc;
    ObDiagnoseSessionInfo *di = ObDiagnoseSessionInfo::get_local_diagnose_info();
    {
      ObMaxWaitGuard max_wait_guard(enable_perf_event ? &max_wait_desc : NULL, di);
      ObTotalWaitGuard total_wait_guard(enable_perf_event ? &total_wait_desc : NULL, di);
      if (enable_perf_event) {
        exec_record.record_start(di);
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(plan_ctx = GET_PHY_PLAN_CTX(exec_ctx_))) {
        LOG_ERROR("plan_ctx is NULL");
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_ISNULL(op_spec)) {
        LOG_ERROR("op_spec is NULL");
        ret = OB_ERR_UNEXPECTED;
      }
      //监控项统计开始
      exec_start_timestamp_ = ObTimeUtility::current_time();
      exec_ctx->set_plan_start_time(exec_start_timestamp_);
      ObAuditRecordData &audit_record = session->get_raw_audit_record();

      if (OB_SUCC(ret)) {
        NG_TRACE_EXT(execute_task, OB_ID(task), task, OB_ID(stmt_type), task.get_des_phy_plan().get_stmt_type());
        ObWorkerSessionGuard worker_session_guard(session);
        ObSQLSessionInfo::LockGuard lock_guard(session->get_query_lock());
        audit_record.request_memory_used_ = 0;
        observer::ObProcessMallocCallback pmcb(0, audit_record.request_memory_used_);
        lib::ObMallocCallbackGuard guard(pmcb);
        NG_TRACE_EXT(execute_task, OB_ID(task), task,
                     OB_ID(stmt_type), task.get_des_phy_plan().get_stmt_type());
        phy_plan_.set_next_phy_operator_id(exec_ctx_.get_phy_op_size());
        phy_plan_.set_next_expr_operator_id(exec_ctx_.get_expr_op_size());

        if (OB_FAIL(execute_remote_plan(exec_ctx_, phy_plan_))) {
          LOG_WARN("execute remote plan failed", K(ret), K(local_tenant_schema_version), K(exec_ctx_.get_task_exec_ctx().get_query_tenant_begin_schema_version()));
        }

        NG_TRACE_EXT(process_ret, OB_ID(process_ret), ret);
      }
      if (OB_FAIL(ret)) {
        if (local_tenant_schema_version != exec_ctx_.get_task_exec_ctx().get_query_tenant_begin_schema_version()) {
          if (is_schema_error(ret)) {
            ret = OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH; // 重写错误码，使得scheduler端能等待远端schema刷新并重试
          }
        } else {
          if (OB_SCHEMA_ERROR == ret || OB_SCHEMA_EAGAIN == ret) {
            ret = OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH; // 针对OB_SCHEMA_ERROR 和OB_SCHEMA_EAGAIN这两个错误码，远程执行暂时先考虑重写，等待远端schema刷新并重试
          }
        }
        // overwrite ret to make sure sql will retry
        if (OB_NOT_NULL(session)
            && OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH == ret
            && GSCHEMASERVICE.is_schema_error_need_retry(
               NULL, session->get_effective_tenant_id())) {
          ret = OB_ERR_REMOTE_SCHEMA_NOT_FULL;
        }
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

      //此处代码要放在scanner.set_err_code(ret)代码前,避免ret被都写成了OB_SUCCESS
      record_sql_audit_and_plan_stat(&phy_plan_, session);
    }
  }

  //vt_iter_factory_.reuse();
  phy_plan_.destroy();
  NG_TRACE(exec_remote_plan_end);
  //执行相关的错误信息记录在exec_errcode_中，通过scanner向控制端返回，所以这里给RPC框架返回成功
  return OB_SUCCESS;
}

int ObRpcRemoteExecuteP::before_response(int error_code)
{
  UNUSED(error_code);
  ObScanner &scanner = result_;
  return base_before_response(scanner);
}

int ObRpcRemoteExecuteP::after_process(int error_code)
{
  UNUSED(error_code);
  return base_after_process();
}

void ObRpcRemoteExecuteP::cleanup()
{
  base_cleanup();
  return;
}

void ObRpcRemoteExecuteP::clean_result_buffer()
{
  ObScanner &scanner = result_;
  scanner.reuse();
}

////////////
/// remote sync with sql
int ObRpcRemoteSyncExecuteP::init()
{
  int ret = OB_SUCCESS;
  ObRemoteTask &task = arg_;
  if (OB_FAIL(base_init())) {
    LOG_WARN("init remote base execute context failed", K(ret));
  }
  if (OB_SUCC(ret)) {
    result_.set_tenant_id(MTL_ID());
    if (OB_FAIL(result_.init(DEFAULT_MAX_REMOTE_EXEC_PACKET_LENGTH))) {
      LOG_WARN("fail to init result", K(ret));
    } else if (OB_FAIL(exec_ctx_.create_physical_plan_ctx())) {
      LOG_WARN("create physical plan ctx failed", K(ret));
    } else {
      ObPhysicalPlanCtx *plan_ctx = exec_ctx_.get_physical_plan_ctx();
      plan_ctx->get_remote_sql_info().ps_params_ = &plan_ctx->get_param_store_for_update();
      task.set_remote_sql_info(&plan_ctx->get_remote_sql_info());
      task.set_exec_ctx(&exec_ctx_);
    }
  }
  return ret;
}

int ObRpcRemoteSyncExecuteP::before_process()
{
  int ret = OB_SUCCESS;
  ObRemoteTask &task = arg_;
  if (OB_FAIL(base_before_process(task.get_tenant_schema_version(),
                                  task.get_sys_schema_version(),
                                  task.get_dependency_tables()))) {
    LOG_WARN("base before process failed", K(ret));
  }
  return ret;
}

int ObRpcRemoteSyncExecuteP::send_result_to_controller(ObExecContext &exec_ctx,
                                                       const ObPhysicalPlan &plan)
{
  ObScanner &scanner = result_;
  return sync_send_result(exec_ctx, plan, scanner);
}

int ObRpcRemoteSyncExecuteP::process()
{
  int &ret = exec_errcode_;
  // arg_是一个ObRemoteTask对象
  ObRemoteTask &task = arg_;
  ret = execute_with_sql(task);
  //执行相关的错误信息记录在exec_errcode_中，通过scanner向控制端返回，所以这里给RPC框架返回成功
  return OB_SUCCESS;
}

int ObRpcRemoteSyncExecuteP::before_response(int error_code)
{
  UNUSED(error_code);
  ObScanner &scanner = result_;
  return base_before_response(scanner);
}

int ObRpcRemoteSyncExecuteP::after_process(int error_code)
{
  UNUSED(error_code);
  return base_after_process();
}

void ObRpcRemoteSyncExecuteP::cleanup()
{
  base_cleanup();
  return;
}

void ObRpcRemoteSyncExecuteP::clean_result_buffer()
{
  ObScanner &scanner = result_;
  scanner.reuse();
}
}  // namespace sql
}  // namespace oceanbase
