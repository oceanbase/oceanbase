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
#include "storage/ob_partition_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "sql/ob_sql_trans_util.h"
#include "sql/ob_end_trans_callback.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/session/ob_sql_session_mgr.h"
#include "sql/plan_cache/ob_cache_object_factory.h"
#include "sql/monitor/ob_exec_stat_collector.h"
#include "observer/mysql/ob_mysql_request_manager.h"
#include "observer/ob_req_time_service.h"
#include "observer/ob_server.h"
#include "observer/ob_server.h"
#include "lib/stat/ob_session_stat.h"
#include "sql/ob_sql.h"
#include "share/scheduler/ob_dag_scheduler.h"

namespace oceanbase {
using namespace obrpc;
using namespace common;
using namespace observer;
namespace sql {
template <typename T>
int ObRemoteBaseExecuteP<T>::base_init()
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx& executor_ctx = exec_ctx_.get_task_exec_ctx();

  sql_ctx_.session_info_ = exec_ctx_.get_my_session();
  sql_ctx_.schema_guard_ = &schema_guard_;
  sql_ctx_.is_remote_sql_ = true;
  sql_ctx_.partition_location_cache_ = &partition_location_cache_;
  sql_ctx_.sql_proxy_ = gctx_.sql_proxy_;
  sql_ctx_.vt_iter_factory_ = &vt_iter_factory_;
  sql_ctx_.partition_table_operator_ = gctx_.pt_operator_;
  sql_ctx_.session_mgr_ = gctx_.session_mgr_;
  sql_ctx_.merged_version_ = *(gctx_.merged_version_);
  sql_ctx_.part_mgr_ = gctx_.part_mgr_;
  sql_ctx_.retry_times_ = 0;
  exec_ctx_.set_addr(gctx_.self_addr_);
  exec_ctx_.set_plan_cache_manager(gctx_.sql_engine_->get_plan_cache_manager());
  exec_ctx_.set_sql_proxy(gctx_.sql_proxy_);
  exec_ctx_.set_session_mgr(gctx_.session_mgr_);
  exec_ctx_.set_sql_ctx(&sql_ctx_);
  executor_ctx.set_partition_service(gctx_.par_ser_);
  executor_ctx.set_vt_partition_service(gctx_.vt_par_ser_);
  executor_ctx.schema_service_ = gctx_.schema_service_;
  executor_ctx.set_task_executor_rpc(*gctx_.executor_rpc_);
  executor_ctx.set_min_cluster_version(GET_MIN_CLUSTER_VERSION());
  partition_location_cache_.set_task_exec_ctx(&executor_ctx);
  return ret;
}

template <typename T>
int ObRemoteBaseExecuteP<T>::base_before_process(
    int64_t tenant_schema_version, int64_t sys_schema_version, const DependenyTableStore& dependency_tables)
{
  bool table_version_equal = false;
  int ret = OB_SUCCESS;
  process_timestamp_ = ObTimeUtility::current_time();
  ObSQLSessionInfo* session_info = exec_ctx_.get_my_session();
  ObTaskExecutorCtx& executor_ctx = exec_ctx_.get_task_exec_ctx();
  ObVirtualTableCtx vt_ctx;
  int64_t tenant_local_version = -1;
  int64_t sys_local_version = -1;
  int64_t query_timeout = 0;
  if (OB_ISNULL(gctx_.schema_service_) || OB_ISNULL(gctx_.sql_engine_) || OB_ISNULL(gctx_.executor_rpc_) ||
      OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("schema service or sql engine is NULL", K(ret), K(gctx_.schema_service_), K(gctx_.sql_engine_));
  } else if (FALSE_IT(THIS_WORKER.set_compatibility_mode(ORACLE_MODE == session_info->get_compatibility_mode()
                                                             ? share::ObWorker::CompatMode::ORACLE
                                                             : share::ObWorker::CompatMode::MYSQL))) {
  } else if (OB_FAIL(gctx_.schema_service_->get_tenant_refreshed_schema_version(
                 session_info->get_effective_tenant_id(), tenant_local_version))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      tenant_local_version = OB_INVALID_VERSION;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get tenant refreshed schema version", K(ret), K(session_info->get_effective_tenant_id()));
    }
  } else if (OB_FAIL(gctx_.schema_service_->get_tenant_refreshed_schema_version(OB_SYS_TENANT_ID, sys_local_version))) {
    LOG_WARN("fail to get systenant refresh schema version", K(ret));
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (sys_schema_version > sys_local_version) {
    if (OB_FAIL(gctx_.schema_service_->async_refresh_schema(OB_SYS_TENANT_ID, sys_schema_version))) {
      LOG_WARN("fail to push back effective_tenant_id", K(ret), K(sys_schema_version), K(sys_local_version));
    }
  }

  if (OB_SUCC(ret) && tenant_schema_version != tenant_local_version) {
    if (OB_FAIL(
            gctx_.schema_service_->get_tenant_schema_guard(session_info->get_effective_tenant_id(), schema_guard_))) {
      LOG_WARN("fail to get schema guard",
          K(ret),
          K(tenant_local_version),
          K(sys_local_version),
          K(tenant_schema_version),
          K(sys_schema_version));
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_SUCC(sql::ObSQLUtils::check_table_version(table_version_equal, dependency_tables, schema_guard_))) {
      if (!table_version_equal) {
        if (tenant_schema_version > tenant_local_version) {
          if (OB_FAIL(gctx_.schema_service_->async_refresh_schema(
                  session_info->get_effective_tenant_id(), tenant_schema_version))) {
            LOG_WARN("fail to push back effective_tenant_id",
                K(ret),
                K(session_info->get_effective_tenant_id()),
                K(tenant_schema_version),
                K(tenant_local_version));
          }
        }
      }
    } else {
      ret = OB_SUCCESS;
      LOG_INFO("fail to check table_schema_version", K(ret));
      if (tenant_schema_version > tenant_local_version) {
        if (OB_FAIL(gctx_.schema_service_->async_refresh_schema(
                session_info->get_effective_tenant_id(), tenant_schema_version))) {
          LOG_WARN("fail to push back effective_tenant_id",
              K(ret),
              K(session_info->get_effective_tenant_id()),
              K(tenant_schema_version),
              K(tenant_local_version));
        }
      }
    }
  }

  if (OB_SUCC(ret) && !table_version_equal) {
    if (OB_FAIL(gctx_.schema_service_->get_tenant_schema_guard(
            session_info->get_effective_tenant_id(), schema_guard_, tenant_schema_version, sys_schema_version))) {
      LOG_WARN("fail to get schema guard", K(ret), K(tenant_schema_version), K(sys_schema_version));
    }
  }

  int64_t local_tenant_schema_version = -1;
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(schema_guard_.get_schema_version(
                 session_info->get_effective_tenant_id(), local_tenant_schema_version))) {
    LOG_WARN("get schema version from schema_guard failed", K(ret));
  } else if (OB_FAIL(session_info->get_query_timeout(query_timeout))) {
    LOG_WARN("get query timeout failed", K(ret));
  } else {
    THIS_WORKER.set_timeout_ts(query_timeout + session_info->get_query_start_time());
    partition_location_cache_.init(gctx_.location_cache_, gctx_.self_addr_, &schema_guard_);
    executor_ctx.set_partition_location_cache(&partition_location_cache_);
    session_info->set_plan_cache_manager(gctx_.sql_engine_->get_plan_cache_manager());
    session_info->set_peer_addr(ObRpcProcessor<T>::arg_.get_ctrl_server());
    exec_ctx_.set_my_session(session_info);
    exec_ctx_.show_session();
    sql_ctx_.session_info_ = session_info;
    vt_ctx.session_ = session_info;
    vt_ctx.vt_iter_factory_ = &vt_iter_factory_;
    vt_ctx.schema_guard_ = &schema_guard_;
    vt_ctx.partition_table_operator_ = gctx_.pt_operator_;
    exec_ctx_.set_virtual_table_ctx(vt_ctx);
  }

  if (OB_FAIL(ret)) {
    if (local_tenant_schema_version != tenant_schema_version) {
      if (is_schema_error(ret)) {
        ret = OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH;
      }
    } else {
      if (OB_SCHEMA_ERROR == ret || OB_SCHEMA_EAGAIN == ret) {
        ret = OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH;
      }
    }
  }
  return ret;
}

template <typename T>
int ObRemoteBaseExecuteP<T>::auto_start_phy_trans(ObPartitionLeaderArray& leader_parts)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = GET_PHY_PLAN_CTX(exec_ctx_);
  ObSQLSessionInfo* my_session = GET_MY_SESSION(exec_ctx_);
  if (OB_ISNULL(plan_ctx) || OB_ISNULL(my_session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("plan_ctx or my_session is NULL", K(ret), K(plan_ctx), K(my_session));
  } else {
    bool in_trans = my_session->get_in_transaction();
    bool ac = true;
    my_session->set_early_lock_release(plan_ctx->get_phy_plan()->stat_.enable_early_lock_release_);
    if (OB_FAIL(my_session->get_autocommit(ac))) {
      LOG_WARN("fail to get autocommit", K(ret));
    } else if (OB_FAIL(ObSqlTransControl::get_participants(exec_ctx_, leader_parts))) {
      LOG_WARN("fail to get participants", K(ret));
    } else {
      if (ObSqlTransUtil::is_remote_trans(ac, in_trans, OB_PHY_PLAN_REMOTE)) {
        if (trans_state_.is_start_trans_executed()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("start_trans is executed", K(ret));
        } else if (OB_FAIL(ObSqlTransControl::on_plan_start(exec_ctx_, true))) {
          LOG_WARN("fail to start trans", K(ret));
        }
        trans_state_.set_start_trans_executed(OB_SUCC(ret));
        if (OB_SUCC(ret)) {
          if (trans_state_.is_start_stmt_executed()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("start_stmt is executed", K(ret));
          } else if (OB_FAIL(ObSqlTransControl::start_stmt(exec_ctx_, leader_parts, true))) {
            LOG_WARN("fail to start stmt", K(ret), K(leader_parts));
          }
          trans_state_.set_start_stmt_executed(OB_SUCC(ret));
        }
      }

      if (OB_SUCC(ret)) {
        if (true == trans_state_.is_start_participant_executed()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("start_participant is executed", K(ret));
        } else if (OB_UNLIKELY(leader_parts.get_partitions().empty())) {
          if (OB_PHY_PLAN_UNCERTAIN != plan_ctx->get_phy_plan()->get_location_type()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("remote plan has empty participant", K(ret));
          }
        } else {
          if (OB_FAIL(ObSqlTransControl::start_participant(exec_ctx_, leader_parts.get_partitions(), true))) {
            LOG_WARN("fail to start participant", K(ret));
          }
          trans_state_.set_start_participant_executed(OB_SUCC(ret));
        }
      }
    }
  }
  return ret;
}

template <typename T>
int ObRemoteBaseExecuteP<T>::sync_send_result(ObExecContext& exec_ctx, const ObPhysicalPlan& plan, ObScanner& scanner)
{
  int ret = OB_SUCCESS;
  const ObNewRow* row = NULL;
  int last_row_used = true;
  int64_t total_row_cnt = 0;
  bool is_static_engine = false;
  ObOperator* se_op = NULL;
  ObPhyOperator* op = NULL;
  ObPhysicalPlanCtx* plan_ctx = GET_PHY_PLAN_CTX(exec_ctx_);
  ObSQLSessionInfo* my_session = GET_MY_SESSION(exec_ctx_);
  if (OB_ISNULL(my_session) || OB_ISNULL(plan_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid argument", K(plan_ctx), K(exec_ctx.get_my_session()));
  } else {
    is_static_engine = my_session->use_static_typing_engine();
    if (is_static_engine) {
      const ObOpSpec* spec = NULL;
      ObOperatorKit* kit = NULL;
      if (OB_ISNULL(spec = plan.get_root_op_spec()) ||
          OB_ISNULL(kit = exec_ctx.get_kit_store().get_operator_kit(spec->id_)) || OB_ISNULL(se_op = kit->op_)) {
        LOG_WARN("root op spec is null", K(ret));
      }
    } else {
      op = plan.get_main_query();
    }
  }
  if (OB_SUCC(ret) && plan.with_rows()) {
    if (is_static_engine && OB_FAIL(se_op->get_next_row())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to get next row", K(ret));
      }
    } else if (!is_static_engine && OB_FAIL(op->get_next_row(exec_ctx, row))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to get next row", K(ret));
      }
    }
    while (OB_SUCC(ret) && OB_SUCC(exec_ctx.check_status())) {
      scanner.set_tenant_id(exec_ctx.get_my_session()->get_effective_tenant_id());
      bool need_flush = false;
      if (is_static_engine) {
        bool added = false;
        if (OB_FAIL(scanner.try_add_row(se_op->get_spec().output_, exec_ctx.get_eval_ctx(), added))) {
          LOG_WARN("fail add row to scanner", K(ret));
        } else if (!added) {
          need_flush = true;
        }
      } else {
        if (OB_ISNULL(row)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("row is NULL", K(ret));
        } else if (OB_FAIL(scanner.add_row(*row))) {
          if (OB_UNLIKELY(OB_SIZE_OVERFLOW != ret)) {
            // error happened, will break
            LOG_WARN("fail add row to scanner", K(ret));
          } else {
            ret = OB_SUCCESS;
            need_flush = true;
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (need_flush) {
          last_row_used = false;
          // set user var map
          if (OB_FAIL(scanner.set_session_var_map(exec_ctx.get_my_session()))) {
            LOG_WARN("set user var to scanner failed");
          } else {
            has_send_result_ = true;
            // override error code
            if (OB_FAIL(ObRpcProcessor<T>::flush(THIS_WORKER.get_timeout_remain()))) {
              LOG_WARN("fail to flush", K(ret));
            } else {
              NG_TRACE_EXT(scanner, OB_ID(row_count), scanner.get_row_count(), OB_ID(total_count), total_row_cnt);
              scanner.reuse();
            }
          }
        } else {
          LOG_DEBUG("scanner add row", K(ret));
          last_row_used = true;
          total_row_cnt++;
        }
      }
      if (OB_SUCC(ret)) {
        if (last_row_used) {
          if (is_static_engine && OB_FAIL(se_op->get_next_row())) {
            if (OB_UNLIKELY(OB_ITER_END != ret)) {
              LOG_WARN("failed to get next row", K(ret));
            } else {
            }
          } else if (!is_static_engine && OB_FAIL(op->get_next_row(exec_ctx, row))) {
            if (OB_UNLIKELY(OB_ITER_END != ret)) {
              LOG_WARN("failed to get next row", K(ret));
            } else {
            }
          } else {
          }
        }
      }
    }  // while end
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(scanner.set_session_var_map(exec_ctx.get_my_session()))) {
      LOG_WARN("set user var to scanner failed");
    } else {
      scanner.set_found_rows(plan_ctx->get_found_rows());
      if (OB_FAIL(scanner.set_row_matched_count(plan_ctx->get_row_matched_count()))) {
        LOG_WARN("fail to set row matched count", K(ret), K(plan_ctx->get_row_matched_count()));
      } else if (OB_FAIL(scanner.set_row_duplicated_count(plan_ctx->get_row_duplicated_count()))) {
        LOG_WARN("fail to set row duplicated count", K(ret), K(plan_ctx->get_row_duplicated_count()));
      } else {
        scanner.set_affected_rows(plan_ctx->get_affected_rows());
      }

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
        LOG_DEBUG("table row counts", K(scanner.get_table_row_counts()), K(plan_ctx->get_table_row_count_list()));
      }
      NG_TRACE_EXT(remote_result,
          OB_ID(found_rows),
          plan_ctx->get_found_rows(),
          OB_ID(affected_rows),
          scanner.get_affected_rows(),
          OB_ID(last_insert_id),
          scanner.get_last_insert_id_session(),
          OB_ID(last_insert_id),
          scanner.get_last_insert_id_to_client(),
          OB_ID(last_insert_id),
          scanner.get_last_insert_id_changed());
    }
  }

  return ret;
}

template <typename T>
int ObRemoteBaseExecuteP<T>::auto_end_phy_trans(bool is_rollback, const ObPartitionArray& participants)
{
  int ret = OB_SUCCESS;
  int end_ret = OB_SUCCESS;

  if (trans_state_.is_start_participant_executed() && trans_state_.is_start_participant_success()) {
    if (OB_SUCCESS != (end_ret = ObSqlTransControl::end_participant(exec_ctx_, is_rollback, participants))) {
      ret = (OB_SUCCESS == ret) ? end_ret : ret;
      LOG_WARN("fail to end participant", K(ret), K(end_ret), K(is_rollback), K(participants), K(exec_ctx_));
    }
    trans_state_.clear_start_participant_executed();
  }

  if (trans_state_.is_start_stmt_executed() && trans_state_.is_start_stmt_success()) {
    if (OB_SUCCESS != (end_ret = ObSqlTransControl::end_stmt(exec_ctx_, is_rollback || OB_SUCCESS != ret))) {
      ret = (OB_SUCCESS == ret) ? end_ret : ret;
      LOG_WARN("fail to end stmt", K(ret), K(end_ret), K(is_rollback), K(exec_ctx_));
    }
    trans_state_.clear_start_stmt_executed();
  }
  if (trans_state_.is_start_trans_executed() && trans_state_.is_start_trans_success()) {
    ObSQLSessionInfo* my_session = GET_MY_SESSION(exec_ctx_);
    if (OB_ISNULL(my_session)) {
      if (OB_SUCC(ret)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("ret is OB_SUCCESS, but session is NULL", K(ret));
      } else {
        LOG_ERROR("ret is not OB_SUCCESS, and session is NULL", K(ret));
      }
    } else {
      bool in_trans = my_session->get_in_transaction();
      bool ac = false;
      if (OB_FAIL(my_session->get_autocommit(ac))) {
        LOG_WARN("fail to get autocommit", K(ret));
      } else if (ObSqlTransUtil::plan_can_end_trans(ac, in_trans)) {
        if (!my_session->is_standalone_stmt()) {
          ObEndTransSyncCallback callback;
          is_rollback = (is_rollback || OB_SUCCESS != ret);
          if (OB_FAIL(callback.init(&(my_session->get_trans_desc()), my_session))) {
            LOG_WARN("fail init callback", K(ret));
          } else {
            int wait_ret = OB_SUCCESS;
            if (OB_SUCCESS != (end_ret = ObSqlTransControl::implicit_end_trans(
                                   exec_ctx_, is_rollback, callback))) {  // implicit commit, no rollback
              ret = (OB_SUCCESS == ret) ? end_ret : ret;
              LOG_WARN("fail end implicit trans", K(is_rollback), K(ret));
            }
            if (OB_UNLIKELY(OB_SUCCESS != (wait_ret = callback.wait()))) {
              LOG_WARN("sync end trans callback return an error!",
                  K(ret),
                  K(wait_ret),
                  K(is_rollback),
                  K(my_session->get_trans_desc()));
            }
            ret = OB_SUCCESS != ret ? ret : wait_ret;
          }
        }
        trans_state_.clear_start_trans_executed();
      } else {
      }
    }
  }
  trans_state_.reset();

  return ret;
}

template <typename T>
int ObRemoteBaseExecuteP<T>::execute_remote_plan(ObExecContext& exec_ctx, const ObPhysicalPlan& plan)
{
  int ret = OB_SUCCESS;
  int last_err = OB_SUCCESS;
  int close_ret = OB_SUCCESS;
  int64_t execution_id = 0;
  ObSQLSessionInfo* session = exec_ctx.get_my_session();
  bool enable_sql_audit = GCONF.enable_sql_audit;
  ObPartitionLeaderArray pla;
  ObPhysicalPlanCtx* plan_ctx = exec_ctx.get_physical_plan_ctx();
  bool is_static_engine = false;
  ObPhyOperator* op = nullptr;
  ObOperator* se_op = nullptr;  // static engine operator
  ObSQLMockSchemaGuard mock_schema_guard;
  if (OB_FAIL(ObSQLMockSchemaUtils::prepare_mocked_schemas(plan.get_mock_rowid_tables()))) {
    LOG_WARN("failed to mock rowid tables", K(ret));
  } else if (OB_ISNULL(plan_ctx) || OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("op is NULL", K(ret), K(op), K(plan_ctx), K(session));
  } else {
    is_static_engine = session->use_static_typing_engine();
  }
  if (OB_SUCC(ret)) {
    int64_t retry_times = 0;
    do {
      if (!is_execute_remote_plan()) {
        // remote execute using send sql_info, should init_phy_op
        if (OB_FAIL(exec_ctx.init_phy_op(plan.get_phy_operator_size()))) {
          LOG_WARN("init physical operator ctx buckets failed", K(ret), K(plan.get_phy_operator_size()));
        } else if (OB_FAIL(exec_ctx.init_expr_op(plan.get_expr_operator_size()))) {
          LOG_WARN("init expr operator ctx buckets failed", K(ret), K(plan.get_expr_operator_size()));
        }
      }
      if (OB_FAIL(ret)) {

      } else if (OB_LIKELY(plan.is_need_trans()) && OB_FAIL(auto_start_phy_trans(pla))) {
        LOG_WARN("fail to begin trans", K(ret));
      } else {
        if (is_static_engine) {
          if (OB_FAIL(plan.get_expr_frame_info().pre_alloc_exec_memory(exec_ctx))) {
            LOG_WARN("fail to pre allocate memory", K(ret), K(plan.get_expr_frame_info()));
          } else if (OB_FAIL(exec_ctx.init_eval_ctx())) {
            LOG_WARN("init eval ctx failed", K(ret));
          } else if (OB_ISNULL(plan.get_root_op_spec())) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid argument", K(is_static_engine), K(plan.get_root_op_spec()), K(plan.get_main_query()));
          } else if (OB_FAIL(plan.get_root_op_spec()->create_operator(exec_ctx, se_op))) {
            LOG_WARN("create operator from spec failed", K(ret));
          } else if (OB_ISNULL(se_op)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("created operator is NULL", K(ret));
          } else {
            if (OB_FAIL(se_op->open())) {
              LOG_WARN("fail open task", K(ret));
            } else if (OB_FAIL(send_result_to_controller(exec_ctx_, plan))) {
              LOG_WARN("sync send result failed", K(ret));
            }
            if (OB_SUCCESS != (close_ret = se_op->close())) {
              LOG_WARN("fail close task", K(close_ret));
            }
          }
        } else {
          if (OB_ISNULL(op = plan.get_main_query())) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid argument", KP(op), K(is_static_engine));
          } else if (OB_FAIL(plan_ctx->reserve_param_space(plan.get_param_count()))) {
            LOG_WARN("reserve rescan param space failed", K(ret), K(plan.get_param_count()));
          } else {
            if (OB_FAIL(op->open(exec_ctx_))) {
              LOG_WARN("fail open task", K(ret));
            } else if (OB_FAIL(send_result_to_controller(exec_ctx_, plan))) {
              LOG_WARN("send result to controller failed", K(ret));
            }
            if (OB_SUCCESS != (close_ret = op->close(exec_ctx_))) {
              LOG_WARN("fail close task", K(close_ret));
            }
          }
        }
        NG_TRACE_EXT(process_ret, OB_ID(process_ret), ret);
      }

      bool is_rollback = false;
      if (OB_FAIL(ret)) {
        is_rollback = true;
      }
      int trans_end_ret = OB_SUCCESS;
      if (OB_SUCCESS != (trans_end_ret = auto_end_phy_trans(is_rollback, pla.get_partitions()))) {
        LOG_WARN("fail to end trans", K(trans_end_ret));
      }
      ret = (OB_SUCCESS == ret) ? trans_end_ret : ret;
      if (OB_FAIL(ret)) {
        pla.reset();
        exec_ctx_.reset_op_env();
        if (has_send_result_) {
          clean_result_buffer();
        }
      }
    } while (query_can_retry_in_remote(last_err, ret, *session, retry_times));
  }
  return ret;
}

template <typename T>
bool ObRemoteBaseExecuteP<T>::query_can_retry_in_remote(
    int& last_err, int& err, ObSQLSessionInfo& session, int64_t& retry_times)
{
  bool bret = false;
  bool ac = session.get_local_autocommit();
  bool in_trans = session.get_in_transaction();
  if (is_execute_remote_plan()) {
    bret = false;  // remote execute using remote_phy_plan, should not retry
  } else {
    if (ObSqlTransUtil::is_remote_trans(ac, in_trans, OB_PHY_PLAN_REMOTE) && retry_times <= 100) {
      if (is_try_lock_row_err(err)) {
        bret = true;
      } else if (is_transaction_set_violation_err(err) || is_snapshot_discarded_err(err)) {
        if (!ObSqlTransControl::is_isolation_RR_or_SE(session.get_tx_isolation())) {
          bret = true;
        }
      }
      if (bret) {
        last_err = err;
        if (OB_SUCCESS != (err = exec_ctx_.check_status())) {
          bret = false;
          LOG_WARN("cehck execute status failed", K(err), K(last_err), K(bret));
          err = last_err;
        } else {
          LOG_INFO("query retry in remote", K(retry_times), K(last_err));
          ++retry_times;
          int64_t base_sleep_us = 1000;
          usleep(base_sleep_us * retry_times);
        }
      }
    }
    if (!bret && is_try_lock_row_err(last_err) && is_timeout_err(err)) {
      LOG_WARN("retry query until query timeout", K(last_err), K(err));
      err = OB_ERR_EXCLUSIVE_LOCK_CONFLICT;
    }
  }
  return bret;
}

template <typename T>
void ObRemoteBaseExecuteP<T>::record_sql_audit_and_plan_stat(const ObPhysicalPlan* plan, ObSQLSessionInfo* session,
    ObExecRecord exec_record, ObExecTimestamp exec_timestamp, ObWaitEventDesc& max_wait_desc,
    ObWaitEventStat& total_wait_desc)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(session));
  } else {
    exec_timestamp.exec_type_ = RpcProcessor;
    ObAuditRecordData& audit_record = session->get_audit_record();
    audit_record.seq_ = 0;  // don't use now
    audit_record.status_ = (OB_SUCCESS == ret || common::OB_ITER_END == ret) ? obmysql::REQUEST_SUCC : ret;
    audit_record.execution_id_ = session->get_current_execution_id();
    audit_record.client_addr_ = ObRpcProcessor<T>::arg_.get_ctrl_server();
    audit_record.user_client_addr_ = ObRpcProcessor<T>::arg_.get_ctrl_server();
    audit_record.user_group_ = THIS_WORKER.get_group_id();
    audit_record.affected_rows_ = 0;
    audit_record.return_rows_ = 0;

    exec_record.max_wait_event_ = max_wait_desc;
    exec_record.wait_time_end_ = total_wait_desc.time_waited_;
    exec_record.wait_count_end_ = total_wait_desc.total_waits_;

    audit_record.plan_type_ = plan != nullptr ? plan->get_plan_type() : OB_PHY_PLAN_UNINITIALIZED;
    audit_record.is_executor_rpc_ = true;
    audit_record.is_inner_sql_ = session->is_inner();
    audit_record.is_hit_plan_cache_ = true;
    audit_record.is_multi_stmt_ = false;

    audit_record.exec_timestamp_ = exec_timestamp;
    audit_record.exec_record_ = exec_record;

    audit_record.update_stage_stat();

    ObPhysicalPlanCtx* plan_ctx = GET_PHY_PLAN_CTX(exec_ctx_);
    ObIArray<ObTableRowCount>* table_row_count_list = NULL;
    if (NULL != plan_ctx) {
      audit_record.consistency_level_ = plan_ctx->get_consistency_level();
      audit_record.table_scan_stat_ = plan_ctx->get_table_scan_stat();
      table_row_count_list = &(plan_ctx->get_table_row_count_list());
    }
    if (NULL != plan) {
      ObPhysicalPlan* mutable_plan = const_cast<ObPhysicalPlan*>(plan);
      if (!sql_ctx_.self_add_plan_ && sql_ctx_.plan_cache_hit_) {
        mutable_plan->update_plan_stat(audit_record,
            false,  // false mean not first update plan stat
            exec_ctx_.get_is_evolution(),
            table_row_count_list);
      } else if (sql_ctx_.self_add_plan_ && !sql_ctx_.plan_cache_hit_) {
        mutable_plan->update_plan_stat(audit_record, true, exec_ctx_.get_is_evolution(), table_row_count_list);
      }
    }
    ObSQLUtils::handle_audit_record(false, EXECUTE_REMOTE, *session, exec_ctx_);
  }
}

template <typename T>
int ObRemoteBaseExecuteP<T>::execute_with_sql(ObRemoteTask& task)
{
  int ret = OB_SUCCESS;
  NG_TRACE(exec_remote_plan_begin);
  int close_ret = OB_SUCCESS;
  ObExecRecord exec_record;
  int64_t local_tenant_schema_version = -1;
  ObExecTimestamp exec_timestamp;
  exec_timestamp.exec_type_ = RpcProcessor;
  ObSQLSessionInfo* session = NULL;
  const bool enable_perf_event = lib::is_diagnose_info_enabled();
  bool enable_sql_audit = GCONF.enable_sql_audit;
  ObPhysicalPlan* plan = nullptr;
  ObPhysicalPlanCtx* plan_ctx = nullptr;
  CacheRefHandleID cache_handle_id = MAX_HANDLE;
  if (OB_ISNULL(session = exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("session is NULL", K(ret), K(task));
  } else if (OB_ISNULL(plan_ctx = GET_PHY_PLAN_CTX(exec_ctx_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("plan_ctx is NULL", K(ret), K(plan_ctx));
  } else if (OB_ISNULL(gctx_.sql_engine_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("sql engine is NULL", K(ret), K(gctx_.sql_engine_));
  } else if (OB_FAIL(
                 schema_guard_.get_schema_version(session->get_effective_tenant_id(), local_tenant_schema_version))) {
    LOG_WARN("get schema version from schema_guard failed", K(ret));
  } else {
    enable_sql_audit = enable_sql_audit && session->get_local_ob_enable_sql_audit();
  }

  if (OB_SUCC(ret)) {
    ObReqTimeGuard req_timeinfo_guard;
    ObSessionStatEstGuard stat_est_guard(session->get_effective_tenant_id(), session->get_sessid());
    ObPhyOperator* op = NULL;
    ObWaitEventDesc max_wait_desc;
    ObWaitEventStat total_wait_desc;
    ObDiagnoseSessionInfo* di = ObDiagnoseSessionInfo::get_local_diagnose_info();
    {
      ObMaxWaitGuard max_wait_guard(enable_perf_event ? &max_wait_desc : NULL, di);
      ObTotalWaitGuard total_wait_guard(enable_perf_event ? &total_wait_desc : NULL, di);
      if (enable_sql_audit) {
        exec_record.record_start();
      }
      if (OB_FAIL(gctx_.sql_engine_->handle_remote_query(
              plan_ctx->get_remote_sql_info(), sql_ctx_, exec_ctx_, plan, cache_handle_id))) {
        LOG_WARN("handle remote query failed", K(ret), K(task));
      } else if (OB_ISNULL(plan)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("plan is null", K(ret));
      }
      if (enable_perf_event) {
        exec_start_timestamp_ = ObTimeUtility::current_time();
      }

      if (OB_SUCC(ret)) {
        NG_TRACE_EXT(execute_task, OB_ID(task), task, OB_ID(stmt_type), plan->get_stmt_type());
        ObWorkerSessionGuard worker_session_guard(session);
        ObSQLSessionInfo::LockGuard lock_guard(session->get_query_lock());
        session->set_peer_addr(task.get_ctrl_server());
        NG_TRACE_EXT(execute_task, OB_ID(task), task, OB_ID(stmt_type), plan->get_stmt_type());
        if (OB_FAIL(execute_remote_plan(exec_ctx_, *plan))) {
          LOG_WARN("execute remote plan failed", K(ret), K(task));
        }
      }
      if (OB_FAIL(ret)) {
        if (local_tenant_schema_version != task.get_tenant_schema_version()) {
          if (is_schema_error(ret)) {
            ret = OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH;
          }
        } else {
          if (OB_SCHEMA_ERROR == ret || OB_SCHEMA_EAGAIN == ret) {
            ret = OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH;
          }
        }
        if (is_master_changed_error(ret) || is_partition_change_error(ret) || is_get_location_timeout_error(ret)) {
          ObTaskExecutorCtx& task_exec_ctx = exec_ctx_.get_task_exec_ctx();
          LOG_DEBUG("remote execute failed, begin to refresh location cache nonblocking", K(ret));
          int refresh_err = ObTaskExecutorCtxUtil::refresh_location_cache(task_exec_ctx, true);
          if (OB_SUCCESS != refresh_err) {
            LOG_WARN("refresh location cache failed", K(ret), K(refresh_err));
          }
        }
      }
      if (enable_perf_event) {
        exec_end_timestamp_ = ObTimeUtility::current_time();
        if (enable_sql_audit) {
          exec_record.record_end();
        }
        record_exec_timestamp(true, exec_timestamp);
      }

      int record_ret = OB_SUCCESS;
      record_sql_audit_and_plan_stat(plan, session, exec_record, exec_timestamp, max_wait_desc, total_wait_desc);
    }
  }
  if (OB_NOT_NULL(plan)) {
    if (plan->is_limited_concurrent_num()) {
      plan->dec_concurrent_num();
    }
    ObCacheObjectFactory::free(plan, cache_handle_id);
    plan = NULL;
  }
  NG_TRACE(exec_remote_plan_end);
  return ret;
}

template <typename T>
int ObRemoteBaseExecuteP<T>::base_before_response(common::ObScanner& scanner)
{
  /**
   * call merge_result() no matter ret is OB_SUCCESS or not, otherwise only part of
   * transaction will be rollbacked if any previous operation failed.
   */
  ObSQLSessionInfo* session = exec_ctx_.get_my_session();
  if (OB_NOT_NULL(session)) {
    int merge_ret = scanner.get_trans_result().merge_result(session->get_trans_result());
    if (OB_SUCCESS != merge_ret) {
      LOG_WARN("failed to merge trans result",
          K(merge_ret),
          "scanner_trans_result",
          scanner.get_trans_result(),
          "session_trans_result",
          session->get_trans_result());
      exec_errcode_ = (OB_SUCCESS == exec_errcode_) ? merge_ret : exec_errcode_;
    } else {
      LOG_DEBUG("process trans_result",
          "cur_stmt",
          session->get_current_query_string(),
          K(scanner.get_trans_result()),
          K(session->get_trans_result()));
    }
  }
  exec_ctx_.hide_session();
  // return a scanner no matter success or fail, set err_code to scanner
  if (OB_SUCCESS != exec_errcode_) {
    scanner.set_err_code(exec_errcode_);
    scanner.store_err_msg(ob_get_tsi_err_msg(exec_errcode_));
    exec_errcode_ = OB_SUCCESS;
  }
  return OB_SUCCESS;
}

template <typename T>
int ObRemoteBaseExecuteP<T>::base_after_process()
{
  int ret = OB_SUCCESS;
  if (exec_end_timestamp_ - exec_start_timestamp_ >= ObServerConfig::get_instance().trace_log_slow_query_watermark) {
    // slow mini task, print trace info
    FORCE_PRINT_TRACE(THE_TRACE, "[slow remote task]");
  }
  return ret;
}

template <typename T>
void ObRemoteBaseExecuteP<T>::base_cleanup()
{
  exec_ctx_.cleanup_session();
  exec_errcode_ = OB_SUCCESS;
  obrpc::ObRpcProcessor<T>::cleanup();
  return;
}

int ObRpcRemoteExecuteP::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(base_init())) {
    LOG_WARN("init remote base execute context failed", K(ret));
  } else if (OB_FAIL(result_.init())) {
    LOG_WARN("fail to init result", K(ret));
  } else {
    arg_.set_deserialize_param(exec_ctx_, phy_plan_);
  }
  return ret;
}

int ObRpcRemoteExecuteP::send_result_to_controller(ObExecContext& exec_ctx, const ObPhysicalPlan& plan)
{
  ObScanner& scanner = result_;
  return sync_send_result(exec_ctx, plan, scanner);
}

int ObRpcRemoteExecuteP::before_process()
{
  int ret = OB_SUCCESS;
  ObTask& task = arg_;
  ObTaskExecutorCtx& executor_ctx = exec_ctx_.get_task_exec_ctx();
  if (OB_FAIL(base_before_process(executor_ctx.get_query_tenant_begin_schema_version(),
          executor_ctx.get_query_sys_begin_schema_version(),
          task.get_des_phy_plan().get_dependency_table()))) {
    LOG_WARN("exec base before process failed", K(ret));
  }
  return ret;
}

int ObRpcRemoteExecuteP::process()
{
  int& ret = exec_errcode_;
  int64_t local_tenant_schema_version = -1;
  ret = OB_SUCCESS;
  NG_TRACE(exec_remote_plan_begin);
  int close_ret = OB_SUCCESS;
  ObExecRecord exec_record;
  ObExecTimestamp exec_timestamp;
  exec_timestamp.exec_type_ = RpcProcessor;
  ObTask& task = arg_;
  ObExecContext* exec_ctx = NULL;
  ObSQLSessionInfo* session = NULL;
  const bool enable_perf_event = lib::is_diagnose_info_enabled();
  bool enable_sql_audit = GCONF.enable_sql_audit;
  ObPartitionLeaderArray pla;
  if (OB_ISNULL(exec_ctx = task.get_exec_context())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("exec ctx is NULL", K(ret), K(task));
  } else if (OB_ISNULL(session = exec_ctx->get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("session is NULL", K(ret), K(task));
  } else if (OB_ISNULL(gctx_.sql_engine_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("sql engine is NULL", K(ret), K(gctx_.sql_engine_));
  } else if (OB_FAIL(
                 schema_guard_.get_schema_version(session->get_effective_tenant_id(), local_tenant_schema_version))) {
    LOG_WARN("get schema version from schema_guard failed", K(ret));
  } else {
    enable_sql_audit = enable_sql_audit && session->get_local_ob_enable_sql_audit();
    LOG_DEBUG("des_plan", K(task.get_des_phy_plan()));
  }

  if (OB_SUCC(ret)) {
    ObSessionStatEstGuard stat_est_guard(session->get_effective_tenant_id(), session->get_sessid());
    ObPhyOperator* op = task.get_root_op();
    ObPhysicalPlanCtx* plan_ctx = NULL;
    ObWaitEventDesc max_wait_desc;
    ObWaitEventStat total_wait_desc;
    ObDiagnoseSessionInfo* di = ObDiagnoseSessionInfo::get_local_diagnose_info();
    {
      ObMaxWaitGuard max_wait_guard(enable_perf_event ? &max_wait_desc : NULL, di);
      ObTotalWaitGuard total_wait_guard(enable_perf_event ? &total_wait_desc : NULL, di);
      if (enable_sql_audit) {
        exec_record.record_start(di);
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(plan_ctx = GET_PHY_PLAN_CTX(exec_ctx_))) {
        LOG_ERROR("plan_ctx is NULL");
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_ISNULL(op)) {
        LOG_ERROR("op is NULL");
        ret = OB_ERR_UNEXPECTED;
      }
      if (enable_perf_event) {
        exec_start_timestamp_ = ObTimeUtility::current_time();
      }

      if (OB_SUCC(ret)) {
        NG_TRACE_EXT(execute_task, OB_ID(task), task, OB_ID(stmt_type), task.get_des_phy_plan().get_stmt_type());
        ObWorkerSessionGuard worker_session_guard(session);
        ObSQLSessionInfo::LockGuard lock_guard(session->get_query_lock());
        NG_TRACE_EXT(execute_task, OB_ID(task), task, OB_ID(stmt_type), task.get_des_phy_plan().get_stmt_type());
        phy_plan_.set_next_phy_operator_id(exec_ctx_.get_phy_op_size());
        phy_plan_.set_next_expr_operator_id(exec_ctx_.get_expr_op_size());
        if (OB_FAIL(execute_remote_plan(exec_ctx_, phy_plan_))) {
          LOG_WARN("execute remote plan failed", K(ret));
        }

        NG_TRACE_EXT(process_ret, OB_ID(process_ret), ret);
      }
      if (OB_FAIL(ret)) {
        if (local_tenant_schema_version != exec_ctx_.get_task_exec_ctx().get_query_tenant_begin_schema_version()) {
          if (is_schema_error(ret)) {
            ret = OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH;
          }
        } else {
          if (OB_SCHEMA_ERROR == ret || OB_SCHEMA_EAGAIN == ret) {
            ret = OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH;
          }
        }
      }
      if (enable_perf_event) {
        exec_end_timestamp_ = ObTimeUtility::current_time();
        if (enable_sql_audit) {
          exec_record.record_end(di);
        }
      }
      record_exec_timestamp(true, exec_timestamp);

      int record_ret = OB_SUCCESS;
      if (enable_sql_audit) {
        record_sql_audit_and_plan_stat(
            &phy_plan_, session, exec_record, exec_timestamp, max_wait_desc, total_wait_desc);
      }
    }
  }

  phy_plan_.destroy();
  NG_TRACE(exec_remote_plan_end);
  return OB_SUCCESS;
}

int ObRpcRemoteExecuteP::before_response()
{
  ObScanner& scanner = result_;
  return base_before_response(scanner);
}

int ObRpcRemoteExecuteP::after_process()
{
  return base_after_process();
}

void ObRpcRemoteExecuteP::cleanup()
{
  base_cleanup();
  return;
}

int ObRpcRemoteExecuteP::get_participants(ObPartitionLeaderArray& pla)
{
  int ret = OB_SUCCESS;
  pla.reset();
  ObPartitionLeaderArray participants;
  const ObTask& task = arg_;
  const ObPartitionIArray& pkeys = task.get_partition_keys();
  ObSQLSessionInfo* my_session = GET_MY_SESSION(exec_ctx_);
  ObPartitionType type;
  if (OB_ISNULL(my_session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("my_session is NULL", K(ret), K(my_session));
  } else if (pkeys.count() > 0) {
    ObTaskExecutorCtx& task_exec_ctx = exec_ctx_.get_task_exec_ctx();
    const ObIArray<ObPhyTableLocation>& table_locations = task_exec_ctx.get_table_locations();
    for (int64_t i = 0; OB_SUCC(ret) && i < pkeys.count(); ++i) {
      const ObPartitionKey& pkey = pkeys.at(i);
      type = ObPhyTableLocation::get_partition_type(pkey, table_locations, my_session->get_is_in_retry_for_dup_tbl());
      if (is_virtual_table(pkey.get_table_id())) {
      } else if (OB_FAIL(
                     ObSqlTransControl::append_participant_to_array_distinctly(pla, pkey, type, gctx_.self_addr_))) {
        LOG_WARN("fail to add participant", K(ret), K(pkey), K(gctx_.self_addr_));
      }
    }
  } else {
    if (OB_FAIL(ObSqlTransControl::get_participants(exec_ctx_, pla))) {
      LOG_WARN("fail to get participants", K(ret));
    }
  }
  return ret;
}

void ObRpcRemoteExecuteP::clean_result_buffer()
{
  ObScanner& scanner = result_;
  scanner.reuse();
}

////////////
/// remote sync with sql
int ObRpcRemoteSyncExecuteP::init()
{
  int ret = OB_SUCCESS;
  ObRemoteTask& task = arg_;
  if (OB_FAIL(base_init())) {
    LOG_WARN("init remote base execute context failed", K(ret));
  } else if (OB_FAIL(result_.init())) {
    LOG_WARN("fail to init result", K(ret));
  } else if (OB_FAIL(exec_ctx_.create_physical_plan_ctx())) {
    LOG_WARN("create physical plan ctx failed", K(ret));
  } else {
    ObPhysicalPlanCtx* plan_ctx = exec_ctx_.get_physical_plan_ctx();
    plan_ctx->get_remote_sql_info().ps_params_ = &plan_ctx->get_param_store_for_update();
    task.set_remote_sql_info(&plan_ctx->get_remote_sql_info());
    task.set_exec_ctx(&exec_ctx_);
  }
  return ret;
}

int ObRpcRemoteSyncExecuteP::before_process()
{
  int ret = OB_SUCCESS;
  ObRemoteTask& task = arg_;
  if (OB_FAIL(base_before_process(
          task.get_tenant_schema_version(), task.get_sys_schema_version(), task.get_dependency_tables()))) {
    LOG_WARN("base before process failed", K(ret));
  }
  return ret;
}

int ObRpcRemoteSyncExecuteP::send_result_to_controller(ObExecContext& exec_ctx, const ObPhysicalPlan& plan)
{
  ObScanner& scanner = result_;
  return sync_send_result(exec_ctx, plan, scanner);
}

int ObRpcRemoteSyncExecuteP::process()
{
  int& ret = exec_errcode_;
  ObRemoteTask& task = arg_;
  ret = execute_with_sql(task);
  return OB_SUCCESS;
}

int ObRpcRemoteSyncExecuteP::before_response()
{
  ObScanner& scanner = result_;
  return base_before_response(scanner);
}

int ObRpcRemoteSyncExecuteP::after_process()
{
  return base_after_process();
}

void ObRpcRemoteSyncExecuteP::cleanup()
{
  base_cleanup();
  return;
}

void ObRpcRemoteSyncExecuteP::clean_result_buffer()
{
  ObScanner& scanner = result_;
  scanner.reuse();
}

////////////
/// remote async with sql
int ObRpcRemoteASyncExecuteP::init()
{
  int ret = OB_SUCCESS;
  ObRemoteTask& task = arg_;
  if (OB_FAIL(base_init())) {
    LOG_WARN("init remote base execute context failed", K(ret));
  } else if (OB_FAIL(remote_result_.init())) {
    LOG_WARN("init remote result failed", K(ret));
  } else if (OB_FAIL(exec_ctx_.create_physical_plan_ctx())) {
    LOG_WARN("create physical plan ctx failed", K(ret));
  } else {
    ObPhysicalPlanCtx* plan_ctx = exec_ctx_.get_physical_plan_ctx();
    plan_ctx->get_remote_sql_info().ps_params_ = &plan_ctx->get_param_store_for_update();
    task.set_remote_sql_info(&plan_ctx->get_remote_sql_info());
    task.set_exec_ctx(&exec_ctx_);
  }
  LOG_DEBUG("init async execute processor", K(ret));
  return ret;
}

int ObRpcRemoteASyncExecuteP::before_process()
{
  int ret = OB_SUCCESS;
  ObRemoteTask& task = arg_;
  if (OB_FAIL(base_before_process(
          task.get_tenant_schema_version(), task.get_sys_schema_version(), task.get_dependency_tables()))) {
    LOG_WARN("base before process failed", K(ret));
  } else {
    remote_result_.get_task_id() = task.get_task_id();
  }
  LOG_DEBUG("before async execute processor", K(ret), K(task));
  return ret;
}

int ObRpcRemoteASyncExecuteP::process()
{
  int& ret = exec_errcode_;
  ObRemoteTask& task = arg_;
  ret = execute_with_sql(task);
  LOG_DEBUG("async execute processor", K(ret), K(task));
  return OB_SUCCESS;
}

int ObRpcRemoteASyncExecuteP::send_result_to_controller(ObExecContext& exec_ctx, const ObPhysicalPlan& plan)
{
  int ret = OB_SUCCESS;
  const ObNewRow* row = NULL;
  bool buffer_enough = false;
  ObPhysicalPlanCtx* plan_ctx = GET_PHY_PLAN_CTX(exec_ctx_);
  int64_t total_row_cnt = 0;
  ObScanner& scanner = remote_result_.get_scanner();
  bool is_static_engine = false;
  ObPhyOperator* op = NULL;
  ObOperator* se_op = NULL;
  if (OB_ISNULL(plan_ctx) || OB_ISNULL(exec_ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid argument", K(plan_ctx), K(op), K(exec_ctx.get_my_session()));
  } else {
    is_static_engine = exec_ctx.get_my_session()->use_static_typing_engine();
    if (is_static_engine) {
      const ObOpSpec* spec = NULL;
      ObOperatorKit* kit = NULL;
      if (OB_ISNULL(spec = plan.get_root_op_spec()) ||
          OB_ISNULL(kit = exec_ctx.get_kit_store().get_operator_kit(spec->id_)) || OB_ISNULL(se_op = kit->op_)) {
        LOG_WARN("root op spec is null", K(ret));
      }
    } else {
      op = plan.get_main_query();
    }
  }
  if (OB_SUCC(ret) && plan.with_rows()) {
    scanner.set_tenant_id(exec_ctx.get_my_session()->get_effective_tenant_id());
    while (OB_SUCC(ret) && !buffer_enough && OB_SUCC(exec_ctx.check_status())) {
      if (is_static_engine) {
        bool added = false;
        if (OB_FAIL(se_op->get_next_row())) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("failed to get next row", K(ret));
          }
        } else if (OB_FAIL(scanner.try_add_row(se_op->get_spec().output_, exec_ctx.get_eval_ctx(), added))) {
          LOG_WARN("fail add row to scanner", K(ret));
        } else if (!added) {
          buffer_enough = true;
        }
      } else {
        if (OB_FAIL(op->get_next_row(exec_ctx, row))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("get next row from root operator failed", K(ret));
          }
        } else if (OB_FAIL(scanner.add_row(*row))) {
          if (OB_UNLIKELY(OB_SIZE_OVERFLOW != ret)) {
            // error happened, will break
            LOG_WARN("fail add row to scanner", K(ret));
            // break;
          } else {
            buffer_enough = true;
          }
        }
      }
      if (buffer_enough && OB_SIZE_OVERFLOW == ret) {
        if (OB_FAIL(scanner.set_session_var_map(exec_ctx.get_my_session()))) {
          LOG_WARN("set user var to scanner failed");
        }
      }
    }  // while end
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  if (OB_SUCC(ret)) {
    if (buffer_enough) {
      remote_result_.set_has_more(true);
    } else if (OB_FAIL(scanner.set_session_var_map(exec_ctx.get_my_session()))) {
      LOG_WARN("set user var to scanner failed");
    } else {
      scanner.set_found_rows(plan_ctx->get_found_rows());
      if (OB_FAIL(scanner.set_row_matched_count(plan_ctx->get_row_matched_count()))) {
        LOG_WARN("fail to set row matched count", K(ret), K(plan_ctx->get_row_matched_count()));
      } else if (OB_FAIL(scanner.set_row_duplicated_count(plan_ctx->get_row_duplicated_count()))) {
        LOG_WARN("fail to set row duplicated count", K(ret), K(plan_ctx->get_row_duplicated_count()));
      } else {
        scanner.set_affected_rows(plan_ctx->get_affected_rows());
        // scanner.set_force_rollback(plan_ctx->is_force_rollback());
      }

      // set last_insert_id no matter success or fail after open
      scanner.set_last_insert_id_to_client(plan_ctx->calc_last_insert_id_to_client());
      scanner.set_last_insert_id_session(plan_ctx->calc_last_insert_id_session());
      scanner.set_last_insert_id_changed(plan_ctx->get_last_insert_id_changed());
      if (!plan_ctx->is_result_accurate()) {
        scanner.set_is_result_accurate(plan_ctx->is_result_accurate());
      }
      OZ(scanner.assign_implicit_cursor(plan_ctx->get_implicit_cursor_infos()));
      ObSQLSessionInfo* session = exec_ctx_.get_my_session();
      if (OB_SUCC(ret) && OB_NOT_NULL(session)) {
        int merge_ret = scanner.get_trans_result().merge_result(session->get_trans_result());
        if (OB_SUCCESS != merge_ret) {
          LOG_WARN("failed to merge trans result",
              K(merge_ret),
              "scanner_trans_result",
              scanner.get_trans_result(),
              "session_trans_result",
              session->get_trans_result());
          exec_errcode_ = (OB_SUCCESS == exec_errcode_) ? merge_ret : exec_errcode_;
        } else {
          LOG_DEBUG("process trans_result",
              "cur_stmt",
              session->get_current_query_string(),
              K(scanner.get_trans_result()),
              K(session->get_trans_result()));
        }
      }
      if (OB_SUCC(ret) && !ObStmt::is_dml_write_stmt(plan.get_stmt_type())) {
        if (OB_FAIL(send_remote_result(remote_result_))) {
          ObRemoteTask& task = arg_;
          LOG_WARN("send remote result failed", K(ret), K(task));
          if (is_static_engine) {
            remote_result_.get_scanner().get_datum_store().reset();
          } else {
            remote_result_.get_scanner().get_row_store().reset();
          }
        }
      }
      NG_TRACE_EXT(remote_result,
          OB_ID(found_rows),
          plan_ctx->get_found_rows(),
          OB_ID(affected_rows),
          scanner.get_affected_rows(),
          OB_ID(last_insert_id),
          scanner.get_last_insert_id_session(),
          OB_ID(last_insert_id),
          scanner.get_last_insert_id_to_client(),
          OB_ID(last_insert_id),
          scanner.get_last_insert_id_changed());
    }
  }
  return ret;
}

int ObRpcRemoteASyncExecuteP::send_remote_result(ObRemoteResult& remote_result)
{
  int ret = OB_SUCCESS;
  ObRemoteTask& task = arg_;
  ObSQLSessionInfo* session = exec_ctx_.get_my_session();
  ObPhysicalPlanCtx* plan_ctx = exec_ctx_.get_physical_plan_ctx();
  ObTaskExecutorCtx& task_exec_ctx = exec_ctx_.get_task_exec_ctx();
  ObExecutorRpcImpl* rpc = task_exec_ctx.get_task_executor_rpc();
  ObWarningBuffer* wb = ob_get_tsi_warning_buffer();
  if (OB_ISNULL(rpc) || OB_ISNULL(plan_ctx) || OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), K(rpc), K(plan_ctx), K(session));
  } else if (wb != nullptr && OB_FAIL(remote_result.get_scanner().store_warning_msg(*wb))) {
    LOG_WARN("store warning msg failed", K(ret));
  } else {
    //    ObExecutorRpcCtx rpc_ctx(session->get_rpc_tenant_id(),
    //                             plan_ctx->get_timeout_timestamp(),
    //                             task_exec_ctx.get_min_cluster_version(),
    //                             &session->get_retry_info_for_update(),
    //                             session,
    //                             plan_ctx->is_plain_select_stmt());
    //    if (OB_FAIL(rpc->remote_post_result_async(rpc_ctx,
    //                                              remote_result,
    //                                              task.get_ctrl_server(),
    //                                              has_send_result_))) {
    //      LOG_WARN("remote post result async failed", K(ret), K(task));
    //    }
    if (OB_FAIL(rpc->remote_batch_post_result(session->get_rpc_tenant_id(),
            task.get_ctrl_server(),
            session->get_local_ob_org_cluster_id(),
            remote_result,
            has_send_result_))) {
      LOG_WARN("remote batch post result failed", K(ret), K(task), K(session->get_rpc_tenant_id()));
    }
  }
  return ret;
}

int ObRpcRemoteASyncExecuteP::before_response()
{
  int ret = OB_SUCCESS;
  base_before_response(remote_result_.get_scanner());
  if (!has_send_result_) {
    if (OB_FAIL(send_remote_result(remote_result_))) {
      ObRemoteTask& task = arg_;
      LOG_WARN("send remote result failed", K(ret), K(task));
    }
  }
  return ret;
}

int ObRpcRemoteASyncExecuteP::after_process()
{
  return base_after_process();
}

void ObRpcRemoteASyncExecuteP::cleanup()
{
  if (is_from_batch_) {
    exec_ctx_.cleanup_session();
    exec_errcode_ = OB_SUCCESS;
  } else {
    base_cleanup();
  }
  return;
}

void ObRpcRemoteASyncExecuteP::clean_result_buffer()
{
  remote_result_.get_scanner().reuse();
}

////////////////////post result to controller
int ObRpcRemotePostResultP::init()
{
  int ret = OB_SUCCESS;
  ObRemoteResult& remote_result = arg_;
  if (OB_FAIL(remote_result.init())) {
    LOG_WARN("fail to init result", K(ret));
  }
  return ret;
}

int ObRpcRemotePostResultP::process()
{
  int ret = OB_SUCCESS;
  THIS_WORKER.disable_retry();
  ObRemoteResult& remote_result = arg_;
  ObRemoteTaskCtx* task_ctx = nullptr;
  ObQueryExecCtx* query_ctx = nullptr;
  ObSQLSessionInfo* session_ref = nullptr;
  int64_t query_timeout = 0;
  if (OB_ISNULL(gctx_.query_ctx_mgr_) || OB_ISNULL(gctx_.session_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("global context is invalid", K(ret), K(gctx_.query_ctx_mgr_), K(gctx_.session_mgr_));
  } else if (OB_FAIL(gctx_.query_ctx_mgr_->get_task_exec_ctx(
                 remote_result.get_task_id().get_ob_execution_id(), task_ctx))) {
    LOG_WARN("get query execution context failed, maybe timeout", K(ret));
  } else if (OB_ISNULL(query_ctx = task_ctx->get_query_exec_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query context is null", K(ret));
  } else if (FALSE_IT(session_ref = &query_ctx->get_result_set().get_session())) {
    // do nothing
  } else if (OB_FAIL(gctx_.session_mgr_->inc_session_ref(session_ref))) {
    LOG_WARN("pin session failed", K(ret));
  } else if (OB_FAIL(session_ref->get_query_timeout(query_timeout))) {
    LOG_WARN("get query timeout failed", K(ret));
  } else {
    THIS_WORKER.set_compatibility_mode(ORACLE_MODE == session_ref->get_compatibility_mode()
                                           ? share::ObWorker::CompatMode::ORACLE
                                           : share::ObWorker::CompatMode::MYSQL);
    THIS_WORKER.set_timeout_ts(query_timeout + session_ref->get_query_start_time());
    ObMySQLResultSet& result = query_ctx->get_result_set();
    ObSQLSessionInfo& session = *session_ref;
    ob_setup_tsi_warning_buffer(&session.get_warnings_buffer());
    ObSQLSessionInfo::LockGuard lock_guard(session.get_query_lock());
    if (!task_ctx->is_exiting()) {
      ObPhysicalPlan* phy_plan = NULL;
      ObAsyncExecuteResult async_exec_result;
      remote_result.get_scanner().log_user_error_and_warn();
      if (OB_ISNULL(phy_plan = result.get_physical_plan())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret), KP(phy_plan));
      } else if (OB_FAIL(remote_result.get_scanner().get_err_code())) {
        const char* err_msg = remote_result.get_scanner().get_err_msg();
        LOG_WARN("error occurred during remote execution",
            K(ret),
            K(err_msg),
            K(remote_result.get_task_id()),
            K(task_ctx->get_runner_svr()));
      } else {
        async_exec_result.set_static_engine_spec(phy_plan->get_root_op_spec());
        result.set_exec_result(&async_exec_result);
        if (remote_result.has_more()) {}
        async_exec_result.set_result_stream(&remote_result.get_scanner(), result.get_field_cnt());
      }
      int saved_ret = ret;
      if (OB_FAIL(session.get_trans_result().merge_result(remote_result.get_scanner().get_trans_result()))) {
        LOG_WARN("fail to merge trans result",
            K(ret),
            "session_trans_result",
            session.get_trans_result(),
            "scanner_trans_result",
            remote_result.get_scanner().get_trans_result());
      } else {
        LOG_DEBUG("execute trans_result",
            "session_trans_result",
            session.get_trans_result(),
            "scanner_trans_result",
            remote_result.get_scanner().get_trans_result());
      }
      ret = (OB_SUCCESS != saved_ret ? saved_ret : ret);
      if (result.get_errcode() == OB_SUCCESS) {
        result.set_errcode(ret);
      }
      if (OB_FAIL(task_ctx->get_remote_plan_driver().response_result(result))) {
        LOG_WARN("response result failed", K(ret));
      }
      bool need_retry = (RETRY_TYPE_NONE != task_ctx->get_retry_ctrl().get_retry_type());
      rpc::ObRequest* mysql_request = task_ctx->get_mysql_request();
      if (!need_retry) {
        int fret = OB_SUCCESS;
        if (OB_SUCCESS != (fret = task_ctx->get_mppacket_sender().flush_buffer(true))) {
          LOG_WARN("flush the last buffer to client failed", K(ret), K(fret));
        }
        ret = OB_SUCC(ret) ? fret : ret;
      }
      task_ctx->set_is_exiting(true);
      gctx_.query_ctx_mgr_->revert_task_exec_ctx(task_ctx);
      task_ctx = nullptr;
      int dret = OB_SUCCESS;
      if (OB_SUCCESS !=
          (dret = gctx_.query_ctx_mgr_->drop_task_exec_ctx(remote_result.get_task_id().get_ob_execution_id()))) {
        LOG_WARN("drop query exec ctx failed", K(ret), K(dret), K(remote_result.get_task_id()));
      }
      ret = OB_SUCC(ret) ? dret : ret;
      if (need_retry) {
        if (OB_ISNULL(mysql_request)) {
          LOG_WARN("mysql request is null", K(ret), K(need_retry));
        } else {
          OBSERVER.get_net_frame().get_deliver().deliver(*mysql_request);
        }
      }
      session.set_show_warnings_buf(ret);
    }
  }
  if (session_ref != nullptr && gctx_.session_mgr_ != nullptr) {
    gctx_.session_mgr_->revert_session(session_ref);
    session_ref = nullptr;
  }
  ob_setup_tsi_warning_buffer(nullptr);
  LOG_DEBUG("process remote task result", K(ret), K(remote_result));
  return OB_SUCCESS;
}
}  // namespace sql
}  // namespace oceanbase
