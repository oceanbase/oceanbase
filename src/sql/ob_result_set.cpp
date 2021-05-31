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

#define USING_LOG_PREFIX SQL_SESSION
#include "sql/ob_result_set.h"
#include "lib/oblog/ob_trace_log.h"
#include "lib/container/ob_id_set.h"
#include "lib/charset/ob_charset.h"
#include "lib/utility/ob_macro_utils.h"
#include "rpc/obmysql/ob_mysql_global.h"
#include "rpc/obmysql/ob_mysql_field.h"
#include "lib/oblog/ob_log_module.h"
#include "storage/ob_partition_service.h"
#include "engine/ob_physical_plan.h"
#include "sql/parser/parse_malloc.h"
#include "share/system_variable/ob_system_variable.h"
#include "share/system_variable/ob_system_variable_alias.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/resolver/ob_cmd.h"
#include "sql/engine/px/ob_px_admission.h"
#include "sql/executor/ob_executor.h"
#include "sql/executor/ob_cmd_executor.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/optimizer/ob_log_plan_factory.h"
#include "sql/ob_sql_trans_util.h"
#include "sql/ob_end_trans_callback.h"
#include "sql/session/ob_sql_session_info.h"
#include "lib/profile/ob_perf_event.h"
#include "sql/engine/basic/ob_limit.h"
#include "sql/plan_cache/ob_cache_object_factory.h"
#include "sql/ob_sql_mock_schema_utils.h"
#include "share/ob_cluster_version.h"
#include "storage/transaction/ob_trans_define.h"
#include "observer/ob_server_struct.h"
#include "storage/transaction/ob_weak_read_service.h"    // ObWeakReadService
#include "storage/transaction/ob_i_weak_read_service.h"  // WRS_LEVEL_SERVER
#include "storage/transaction/ob_weak_read_util.h"       // ObWeakReadUtil
#include <cctype>

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::storage;
using namespace oceanbase::transaction;

ObResultSet::~ObResultSet()
{
  bool is_remote_sql = false;
  if (OB_NOT_NULL(get_exec_context().get_sql_ctx())) {
    is_remote_sql = get_exec_context().get_sql_ctx()->is_remote_sql_;
  }
  if (NULL != physical_plan_ && !is_remote_sql) {
    // LOG_DEBUG("destruct physical plan cons from assign", K(physical_plan_));
    if (OB_UNLIKELY(physical_plan_->is_limited_concurrent_num())) {
      physical_plan_->dec_concurrent_num();
    }
    ObCacheObjectFactory::free(physical_plan_, ref_handle_id_);
    physical_plan_ = NULL;
  }
}

int ObResultSet::open_cmd()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cmd_)) {
    LOG_ERROR("cmd and physical_plan both not init", K(stmt_type_));
    ret = common::OB_NOT_INIT;
  } else if (OB_FAIL(init_cmd_exec_context(get_exec_context()))) {
    LOG_WARN("fail init exec context", K(ret), K_(stmt_type));
  } else if (OB_FAIL(on_cmd_execute())) {
    LOG_WARN("fail start cmd trans", K(ret), K_(stmt_type));
  } else if (OB_FAIL(ObCmdExecutor::execute(get_exec_context(), *cmd_))) {
    SQL_LOG(WARN, "execute cmd failed", K(ret));
  }
  return ret;
}

OB_INLINE int ObResultSet::open_plan()
{
  int ret = OB_SUCCESS;
  // ObLimit *limit_opt = NULL;
  if (OB_ISNULL(physical_plan_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid physical plan", K(physical_plan_));
  } else if (OB_FAIL(prepare_mock_schemas())) {
    LOG_WARN("failed to prepare mock schemas", K(ret));
  } else {
    has_top_limit_ = physical_plan_->has_top_limit();
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObPxAdmission::enter_query_admission(
              my_session_, get_exec_context(), *get_physical_plan(), worker_count_))) {
        // query is not admitted to run
        if (OB_EAGAIN == ret) {
          ret = OB_ERR_SCHEDULER_THREAD_NOT_ENOUGH;
          LOG_DEBUG("Query is not admitted to run, try again", K(ret));
        } else {
          LOG_WARN("Fail to get admission to use px", K(ret));
        }
      } else if (THIS_WORKER.is_timeout()) {
        ret = OB_TIMEOUT;
        LOG_WARN("query is timeout",
            K(ret),
            "timeout_ts",
            THIS_WORKER.get_timeout_ts(),
            "start_time",
            my_session_.get_query_start_time());
      } else if (stmt::T_PREPARE != stmt_type_) {
        if (OB_FAIL(auto_start_plan_trans())) {
          LOG_WARN("fail start trans", K(ret));
        } else {
          int64_t retry = 0;
          if (OB_SUCC(ret)) {
            do {
              ret = do_open_plan(get_exec_context());
            } while (transaction_set_violation_and_retry(ret, retry));
          }
        }
      }
    }

    if (OB_FAIL(ret)) {
      physical_plan_->set_is_last_open_succ(false);
    } else {
      physical_plan_->set_is_last_open_succ(true);
    }
  }
  return ret;
}

int ObResultSet::sync_open()
{
  int ret = OB_SUCCESS;
  OZ(execute());
  OZ(open());
  return ret;
}

int ObResultSet::execute()
{
  int ret = common::OB_SUCCESS;
  //  if (stmt::T_PREPARE != stmt_type_
  //      && stmt::T_DEALLOCATE != stmt_type_) {

  my_session_.set_first_stmt_type(stmt_type_);
  if (NULL != physical_plan_) {
    ret = open_plan();
  } else if (NULL != cmd_) {
    ret = open_cmd();
  } else {
    // inner sql executor, no plan or cmd. do nothing.
  }
  //  } else {
  //    //T_PREPARE//T_DEALLOCATE do nothing
  //  }
  if (OB_TRANS_XA_BRANCH_FAIL == ret) {
    LOG_WARN("branch fail in global transaction", K(my_session_.get_trans_desc()));
    my_session_.reset_first_stmt_type();
    my_session_.reset_tx_variable();
    my_session_.set_early_lock_release(false);
    my_session_.get_trans_desc().get_standalone_stmt_desc().reset();
  }
  set_errcode(ret);

  return ret;
}

int ObResultSet::open()
{
  int ret = OB_SUCCESS;
  if (NULL != physical_plan_) {
    if (OB_ISNULL(exec_result_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("exec result is null", K(ret));
    } else if (OB_FAIL(exec_result_->open(get_exec_context()))) {
      if (OB_TRANSACTION_SET_VIOLATION != ret && OB_TRY_LOCK_ROW_CONFLICT != ret) {
        SQL_LOG(WARN, "fail open main query", K(ret));
      }
    } else if (OB_FAIL(drive_pdml_query())) {
      LOG_WARN("fail do px dml query", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (!is_inner_result_set_ && OB_FAIL(set_mysql_info())) {
      SQL_LOG(WARN, "fail to get mysql info", K(ret));
    } else if (NULL != get_exec_context().get_physical_plan_ctx()) {
      SQL_LOG(DEBUG, "get affected row", K(get_exec_context().get_physical_plan_ctx()->get_affected_rows()));
      set_affected_rows(get_exec_context().get_physical_plan_ctx()->get_affected_rows());
    }
  }
  set_errcode(ret);
  return ret;
}

int ObResultSet::on_cmd_execute()
{
  int ret = OB_SUCCESS;

  bool ac = true;
  if (OB_ISNULL(cmd_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid inner state", K(cmd_));
  } else if (OB_FAIL(my_session_.get_autocommit(ac))) {
    LOG_WARN("fail to get autocommit", K(ret));
  } else {
    bool in_trans = my_session_.get_in_transaction();

    if (ObSqlTransUtil::cmd_need_new_trans(ac, in_trans)) {
      if (cmd_->cause_implicit_commit()) {
        ObEndTransSyncCallback callback;
        if (OB_FAIL(callback.init(&(my_session_.get_trans_desc()), &my_session_))) {
          SQL_ENG_LOG(WARN, "fail init callback", K(ret));
        } else {
          int wait_ret = OB_SUCCESS;
          if (OB_FAIL(ObSqlTransControl::implicit_end_trans(get_exec_context(), false, callback))) {
            // implicit commit, no rollback
            SQL_ENG_LOG(WARN, "fail end implicit trans on cmd execute", K(ret));
            // Convention: callback must not be called when end_trans
            // returns an error, callback must be called when it returns SUCC
          }
          // No matter what the return code is, wait
          if (OB_UNLIKELY(OB_SUCCESS != (wait_ret = callback.wait()))) {
            LOG_WARN("sync end trans callback return an error!", K(ret), K(wait_ret), K(my_session_.get_trans_desc()));
          }
          ret = OB_SUCCESS != ret ? ret : wait_ret;
        }
      }
    }
  }
  return ret;
}

OB_INLINE int ObResultSet::auto_start_plan_trans()
{
  int ret = OB_SUCCESS;
  bool ac = true;
  uint64_t tenant_id = my_session_.get_effective_tenant_id();
  ObIWeakReadService* wrs = GCTX.weak_read_service_;
  if (OB_ISNULL(physical_plan_) || OB_ISNULL(wrs)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid inner state", K(physical_plan_), K(wrs));
  } else if (OB_FAIL(my_session_.get_autocommit(ac))) {
    LOG_WARN("fail to get autocommit", K(ret));
  } else {
    bool in_trans = my_session_.get_in_transaction();
    if (ObSqlTransUtil::is_remote_trans(ac, in_trans, physical_plan_->get_plan_type())) {
      if (get_exec_context().get_min_cluster_version() < CLUSTER_VERSION_2200 &&
          (ObWeakReadUtil::enable_monotonic_weak_read(tenant_id) || my_session_.is_inner())) {
        int64_t read_snapshot = 0;
        if (OB_FAIL(wrs->get_server_version(tenant_id, read_snapshot))) {
          if (OB_TENANT_NOT_IN_SERVER == ret) {
            LOG_WARN("tenant not in server. get weak read server version fail", K(ret), K(tenant_id), K_(my_session));

            // Generate a minimal readable version number
            read_snapshot = ObWeakReadUtil::generate_min_weak_read_version(tenant_id);
            LOG_INFO(
                "generate min readable weak read snapshot version", K(read_snapshot), K(tenant_id), K_(my_session));

            ret = OB_SUCCESS;
          } else {
            LOG_WARN("get weak read snapshot version fail", K(ret), K(tenant_id), K_(my_session));
          }
        } else {
        }

        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(my_session_.update_safe_weak_read_snapshot(
                       tenant_id, read_snapshot, ObBasicSessionInfo::AUTO_PLAN))) {
          LOG_WARN("update safe weak read snapshot version error", K(ret), K(read_snapshot), K_(my_session));
        } else {
          if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
            LOG_INFO("update safe_weak_read_snapshot success in autocommit sp_trans", K(read_snapshot), K_(my_session));
          }
        }
      } else {
        // do nothing
      }
    } else if (OB_LIKELY(physical_plan_->is_need_trans())) {
      if (OB_UNLIKELY(get_trans_state().is_start_trans_executed())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid trans state", K(get_trans_state().is_start_trans_executed()));
      } else {
        if (OB_SUCCESS != (ret = ObSqlTransControl::on_plan_start(get_exec_context()))) {
          SQL_LOG(WARN, "fail start trans", K(ret));
        }
        get_trans_state().set_start_trans_executed(OB_SUCC(ret));
        // Clean up the end trans execution state, which is used in asynchronous callback scenarios
        get_trans_state().clear_end_trans_executed();
      }
    }
  }
  return ret;
}

int ObResultSet::start_stmt()
{
  NG_TRACE(sql_start_stmt_begin);
  int ret = OB_SUCCESS;
  bool ac = true;
  if (my_session_.is_standalone_stmt()) {
    // do nothing
  } else if (OB_ISNULL(physical_plan_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid inner state", K(physical_plan_));
  } else if (OB_FAIL(my_session_.get_autocommit(ac))) {
    LOG_WARN("fail to get autocommit", K(ret));
  } else {
    bool in_trans = my_session_.get_in_transaction();

    // 1. Regardless of whether it is in a transaction, as long as it is not
    //    select and the plan is REMOTE, the miss is reported to the client
    // 2. feedback this misshit to obproxy (bug#6255177)
    // 3. For multi-stmt, only the first partition hit information is fed back to the client
    // 4. The retry situation needs to be considered. What needs to be fed
    //    back to the client is the first partition hit with a successful retry.
    if (stmt::T_SELECT != stmt_type_) {
      my_session_.partition_hit().try_set_bool(OB_PHY_PLAN_REMOTE != physical_plan_->get_plan_type());
    }

    if (ObSqlTransUtil::is_remote_trans(ac, in_trans, physical_plan_->get_plan_type())) {
      get_exec_context().set_need_change_timeout_ret(false);
    } else if (OB_LIKELY(physical_plan_->is_need_trans())) {
      ObPartitionLeaderArray participants;
      if (get_trans_state().is_start_stmt_executed()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid transaction state", K(get_trans_state().is_start_stmt_executed()));
      } else if (OB_FAIL(ObSqlTransControl::get_participants(get_exec_context(), participants))) {
        SQL_LOG(WARN, "fail to get participants", K(ret));
      } else {
        if (OB_FAIL(ObSqlTransControl::start_stmt(get_exec_context(), participants))) {
          SQL_LOG(WARN, "fail to start stmt", K(ret), K(participants), K(physical_plan_->get_dependency_table()));
        } else {
          get_exec_context().set_need_change_timeout_ret(false);
        }
        get_trans_state().set_start_stmt_executed(OB_SUCC(ret));
      }
    }
  }
  NG_TRACE(sql_start_stmt_end);
  return ret;
}

int ObResultSet::end_stmt(const bool is_rollback)
{
  int ret = OB_SUCCESS;
  NG_TRACE(start_end_stmt);
  if (get_trans_state().is_start_stmt_executed() && get_trans_state().is_start_stmt_success()) {
    if (OB_ISNULL(physical_plan_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid inner state", K(physical_plan_));
    } else if (physical_plan_->is_need_trans()) {
      if (OB_FAIL(ObSqlTransControl::end_stmt(get_exec_context(), is_rollback))) {
        SQL_LOG(WARN, "fail to end stmt", K(ret), K(is_rollback));
      }
    }
    get_trans_state().clear_start_stmt_executed();
  }

  NG_TRACE(end_stmt);
  return ret;
}

OB_INLINE int ObResultSet::start_participant()
{
  NG_TRACE(sql_start_participant_begin);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(physical_plan_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid physical plan", K(physical_plan_));
  } else if (OB_UNLIKELY(!physical_plan_->is_need_trans())) {
  } else if (OB_UNLIKELY(get_trans_state().is_start_participant_executed())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid transaction state", K(get_trans_state().is_start_participant_executed()));
  } else {
    const ObPhyPlanType& plan_loc_type = physical_plan_->get_plan_type();
    switch (plan_loc_type) {
      case OB_PHY_PLAN_LOCAL:
      case OB_PHY_PLAN_DISTRIBUTED: {
        ObPartitionArray& participants = get_trans_state().get_participants();
        if (OB_PHY_PLAN_LOCAL == plan_loc_type) {
          // local plan, handle all participants
          if (OB_FAIL(ObSqlTransControl::get_participants(get_exec_context(), participants))) {
            LOG_WARN("fail to get participants", K(ret));
          }
        } else if (OB_PHY_PLAN_DISTRIBUTED == plan_loc_type) {
          // Distributed plan, only process participants
          // of root job locally, and process others on corresponding machines
          const ObPhyOperator* root_op = physical_plan_->get_main_query();
          if (OB_ISNULL(root_op)) {
            const ObOperator* root_op_v2 = nullptr;
            ObExecuteResult* exec_result = static_cast<ObExecuteResult*>(exec_result_);
            if (OB_ISNULL(root_op_v2 = exec_result->get_static_engine_root())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("root operator is null", K(ret));
            } else if (OB_FAIL(ObSqlTransControl::get_root_job_participants(
                           get_exec_context(), *root_op_v2, participants))) {
              LOG_WARN("fail to get root participants", K(ret));
            }
          } else if (OB_FAIL(
                         ObSqlTransControl::get_root_job_participants(get_exec_context(), *root_op, participants))) {
            LOG_WARN("fail to get root job participants", K(ret), KPC(root_op));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("unexpected plan location type", K(ret), K(plan_loc_type));
        }
        if (OB_SUCC(ret) && OB_LIKELY(participants.count() > 0)) {
          if (OB_FAIL(ObSqlTransControl::start_participant(get_exec_context(), participants))) {
            LOG_DEBUG("fail to start participant", K(ret), K(participants));
          }
          get_trans_state().set_start_participant_executed(OB_SUCC(ret));
        }
        break;
      }
      case OB_PHY_PLAN_REMOTE: {
        // do every thing in remote
        break;
      }
      default: {
        break;
      }
    }
  }
  NG_TRACE(sql_start_participant_end);
  return ret;
}

OB_INLINE int ObResultSet::end_participant(const bool is_rollback)
{
  int ret = OB_SUCCESS;
  NG_TRACE(end_participant_begin);
  if (get_trans_state().is_start_participant_executed() && get_trans_state().is_start_participant_success()) {
    if (OB_ISNULL(physical_plan_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid physical plan", K(physical_plan_));
    } else {
      switch (physical_plan_->get_plan_type()) {
        case OB_PHY_PLAN_LOCAL:
        case OB_PHY_PLAN_DISTRIBUTED: {
          if (OB_LIKELY(physical_plan_->is_need_trans())) {
            // participants have been stored in get_trans_state().get_participants() when start_paricipant() was called
            const ObPartitionArray& participants = get_trans_state().get_participants();
            if (OB_LIKELY(participants.count() > 0)) {
              if (OB_FAIL(ObSqlTransControl::end_participant(get_exec_context(), is_rollback, participants))) {
                LOG_WARN("fail to end participant", K(ret), K(is_rollback), K(participants));
              }
            }
          }
          break;
        }
        case OB_PHY_PLAN_REMOTE: {
          // do every thing remote
          break;
        }
        default: {
          break;
        }
      }
    }
  }
  if (get_trans_state().is_start_participant_executed()) {
    get_trans_state().clear_start_participant_executed();
  }
  NG_TRACE(end_participant_end);
  return ret;
}

int ObResultSet::get_next_row(const common::ObNewRow*& row)
{
  int& ret = errcode_;
  if (OB_LIKELY(NULL != physical_plan_)) {  // take this branch more frequently
    if (OB_ISNULL(exec_result_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("exec result is null", K(ret));
    } else if (OB_FAIL(exec_result_->get_next_row(get_exec_context(), row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next row from exec result failed", K(ret));
      }
    } else {
      return_rows_++;
    }
  } else if (NULL != cmd_) {
    if (is_pl_stmt(static_cast<stmt::StmtType>(cmd_->get_cmd_type()))) {
      ret = OB_NOT_SUPPORTED;
    } else {
      ret = OB_ERR_UNEXPECTED;
      _OB_LOG(ERROR, "should not call get_next_row in CMD SQL");
    }
  } else {
    _OB_LOG(ERROR, "phy_plan not init");
    ret = OB_NOT_INIT;
  }
  if (OB_TRANS_XA_BRANCH_FAIL == ret) {
    LOG_WARN("branch fail in global transaction", K(my_session_.get_trans_desc()));
    my_session_.reset_first_stmt_type();
    my_session_.reset_tx_variable();
    my_session_.set_early_lock_release(false);
    my_session_.get_trans_desc().get_standalone_stmt_desc().reset();
  }
  return ret;
}

// ObPhysicalPlan *ObResultSet::get_physical_plan()
//{
//  return physical_plan_;
//}

// The conditions that trigger this error: A and B two SQLs,
// and a few rows of data were modified at the same time (the modified content has intersection).
// A: update tbl set a = a + 1;
// B: update tbl set a = a + 1;
// If B completes the entire update operation after A reads a and before update a,
// the update of B will be lost, and the final result is a=1
//
// Solution: Check the version number before updating the data,
// and if the version number is inconsistent, throw an OB_TRANSACTION_SET_VIOLATION error,
// and the SQL layer will try again here to ensure that the read and write
// version numbers are consistent.
bool ObResultSet::transaction_set_violation_and_retry(int& err, int64_t& retry_times)
{
  bool bret = false;
  ObSqlCtx* sql_ctx = get_exec_context().get_sql_ctx();
  bool is_batched_stmt = false;
  if (sql_ctx != nullptr) {
    is_batched_stmt = sql_ctx->multi_stmt_item_.is_batched_multi_stmt();
  }
  if ((OB_SNAPSHOT_DISCARDED == err || OB_TRANSACTION_SET_VIOLATION == err) &&
      retry_times < TRANSACTION_SET_VIOLATION_MAX_RETRY && !is_batched_stmt) {
    int32_t isolation = my_session_.get_tx_isolation();
    bool is_isolation_RR_or_SE =
        (isolation == ObTransIsolation::REPEATABLE_READ || isolation == ObTransIsolation::SERIALIZABLE);
    // bug#6361189  pass err to force rollback stmt in do_close_plan()
    if (OB_TRANSACTION_SET_VIOLATION == err && 0 == retry_times && !is_isolation_RR_or_SE) {
      // When TSC error retry, WARN log will be printed only for the first time
      LOG_WARN("transaction set consistency violation, will retry");
    }
    int ret = do_close_plan(err, get_exec_context());
    ObPhysicalPlanCtx* plan_ctx = get_exec_context().get_physical_plan_ctx();
    if (OB_NOT_NULL(plan_ctx)) {
      plan_ctx->reset_for_quick_retry();
    }
    if (OB_SUCCESS != ret) {
      LOG_WARN("failed to close plan", K(err), K(ret));
    } else {
      // OB_SNAPSHOT_DISCARDED should not retry now, see:
      // so we remove this condition: OB_TRANSACTION_SET_VIOLATION == err
      if (/*OB_TRANSACTION_SET_VIOLATION == err &&*/ is_isolation_RR_or_SE) {
        // rewrite err in ObQueryRetryCtrl::test_and_save_retry_state().
        // err = OB_TRANS_CANNOT_SERIALIZE;
        bret = false;
      } else {
        ++retry_times;
        bret = true;
      }
    }
    LOG_DEBUG("transaction set consistency violation and retry", "retry", bret, K(retry_times), K(err));
  }
  return bret;
}

OB_INLINE int ObResultSet::do_open_plan(ObExecContext& ctx)
{
  NG_TRACE_EXT(do_open_plan_begin, OB_ID(plan_id), physical_plan_->get_plan_id());
  int ret = OB_SUCCESS;
  ctx.reset_op_env();
  exec_result_ = &(ctx.get_task_exec_ctx().get_execute_result());
  if (stmt::T_PREPARE != stmt_type_) {
    if (OB_FAIL(ctx.init_phy_op(physical_plan_->get_phy_operator_size()))) {
      LOG_WARN("fail init exec phy op ctx", K(ret));
    } else if (OB_FAIL(ctx.init_expr_op(physical_plan_->get_expr_operator_size()))) {
      LOG_WARN("fail init exec expr op ctx", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(start_stmt())) {
    LOG_WARN("fail start stmt", K(ret));
  } else {
    /* Set exec_result_ to the runtime environment of the executor to return data */
    if (OB_FAIL(executor_.init(physical_plan_))) {
      SQL_LOG(WARN, "fail to init executor", K(ret), K(physical_plan_));
    } else if (OB_FAIL(executor_.execute_plan(ctx))) {
      SQL_LOG(WARN, "fail execute plan", K(ret));
    } else if (OB_FAIL(start_participant())) {
      if (OB_REPLICA_NOT_READABLE != ret) {
        LOG_WARN("fail to start participant", K(ret));
      }
    }
  }
  NG_TRACE(do_open_plan_end);
  return ret;
}

int ObResultSet::set_mysql_info()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = get_exec_context().get_physical_plan_ctx();
  int64_t pos = 0;
  if (OB_ISNULL(plan_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_LOG(WARN, "fail to get physical plan ctx");
  } else if (stmt::T_UPDATE == get_stmt_type()) {
    int result_len = snprintf(message_ + pos,
        MSG_SIZE - pos,
        OB_UPDATE_MSG_FMT,
        plan_ctx->get_row_matched_count(),
        plan_ctx->get_row_duplicated_count(),
        warning_count_);
    if (OB_UNLIKELY(result_len < 0) || OB_UNLIKELY(result_len >= MSG_SIZE - pos)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to snprintf to buff", K(ret));
    }
  } else if (stmt::T_REPLACE == get_stmt_type() || stmt::T_INSERT == get_stmt_type()) {
    if (plan_ctx->get_row_matched_count() <= 1) {
      // nothing to do
    } else {
      int result_len = snprintf(message_ + pos,
          MSG_SIZE - pos,
          OB_INSERT_MSG_FMT,
          plan_ctx->get_row_matched_count(),
          plan_ctx->get_row_duplicated_count(),
          warning_count_);
      if (OB_UNLIKELY(result_len < 0) || OB_UNLIKELY(result_len >= MSG_SIZE - pos)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to snprintf to buff", K(ret));
      }
    }
  } else if (stmt::T_LOAD_DATA == get_stmt_type()) {
    int result_len = snprintf(message_ + pos,
        MSG_SIZE - pos,
        OB_LOAD_DATA_MSG_FMT,
        plan_ctx->get_row_matched_count(),
        plan_ctx->get_row_deleted_count(),
        plan_ctx->get_row_duplicated_count(),
        warning_count_);
    if (OB_UNLIKELY(result_len < 0) || OB_UNLIKELY(result_len >= MSG_SIZE - pos)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to snprintf to buff", K(ret));
    }
  } else {
    // nothing to do
  }
  return ret;
}

OB_INLINE void ObResultSet::store_affected_rows(ObPhysicalPlanCtx& plan_ctx)
{
  int64_t affected_row = 0;
  if (!ObStmt::is_dml_stmt(get_stmt_type())) {
    affected_row = 0;
  } else if (stmt::T_SELECT == get_stmt_type()) {
    affected_row = share::is_oracle_mode() ? plan_ctx.get_affected_rows() : -1;
  } else {
    affected_row = get_affected_rows();
  }
  NG_TRACE_EXT(affected_rows, OB_ID(affected_rows), affected_row);
  my_session_.set_affected_rows(affected_row);
}

OB_INLINE void ObResultSet::store_found_rows(ObPhysicalPlanCtx& plan_ctx)
{
  int64_t rows = 1;
  if (plan_ctx.is_affect_found_row()) {
    if (OB_UNLIKELY(stmt::T_EXPLAIN == get_stmt_type())) {
      rows = 0;
      my_session_.set_found_rows(rows);
    } else {
      int64_t found_rows = -1;
      found_rows = plan_ctx.get_found_rows();
      rows = found_rows == 0 ? return_rows_ : found_rows;
      my_session_.set_found_rows(rows);
      NG_TRACE_EXT(store_found_rows, OB_ID(found_rows), found_rows, OB_ID(return_rows), return_rows_);
    }
  }
  return;
}

int ObResultSet::update_last_insert_id()
{
  return store_last_insert_id(get_exec_context());
}

int ObResultSet::update_is_result_accurate()
{
  int ret = OB_SUCCESS;
  if (stmt::T_SELECT == stmt_type_) {
    ObPhysicalPlanCtx* plan_ctx = get_exec_context().get_physical_plan_ctx();
    bool old_is_result_accurate = true;
    if (OB_ISNULL(plan_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "get plan ctx is NULL", K(ret));
    } else if (OB_FAIL(my_session_.get_is_result_accurate(old_is_result_accurate))) {
      SQL_LOG(WARN, "faile to get is_result_accurate", K(ret));
    } else {
      bool is_result_accurate = plan_ctx->is_result_accurate();
      SQL_LOG(DEBUG, "debug is_result_accurate for session", K(is_result_accurate), K(old_is_result_accurate), K(ret));
      if (is_result_accurate != old_is_result_accurate) {
        if (OB_FAIL(my_session_.update_sys_variable(SYS_VAR_IS_RESULT_ACCURATE, is_result_accurate ? 1 : 0))) {
          LOG_WARN("fail to update result accurate", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObResultSet::store_last_insert_id(ObExecContext& ctx)
{
  int ret = OB_SUCCESS;
  if (ObStmt::is_dml_stmt(stmt_type_) && OB_LIKELY(NULL != physical_plan_) &&
      OB_LIKELY(!physical_plan_->is_affected_last_insert_id())) {
    // nothing to do
  } else {
    ObPhysicalPlanCtx* plan_ctx = ctx.get_physical_plan_ctx();
    if (OB_ISNULL(plan_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "get plan ctx is NULL", K(plan_ctx));
    } else {
      uint64_t last_insert_id_session = plan_ctx->calc_last_insert_id_session();
      SQL_LOG(DEBUG, "debug last_insert_id for session", K(last_insert_id_session), K(ret));
      SQL_LOG(DEBUG,
          "debug last_insert_id changed",
          K(ret),
          "last_insert_id_changed",
          plan_ctx->get_last_insert_id_changed());

      if (plan_ctx->get_last_insert_id_changed()) {
        ObObj last_insert_id;
        last_insert_id.set_uint64(last_insert_id_session);
        if (OB_FAIL(my_session_.update_sys_variable(SYS_VAR_LAST_INSERT_ID, last_insert_id))) {
          LOG_WARN("fail to update last_insert_id", K(ret));
        } else if (OB_FAIL(my_session_.update_sys_variable(SYS_VAR_IDENTITY, last_insert_id))) {
          LOG_WARN("succ update last_insert_id, but fail to update identity", K(ret));
        } else {
          NG_TRACE_EXT(last_insert_id, OB_ID(last_insert_id), last_insert_id_session);
        }
      }

      if (OB_SUCC(ret)) {
        // TODO when does observer should return last_insert_id to client?
        set_last_insert_id_to_client(plan_ctx->calc_last_insert_id_to_client());
        SQL_LOG(DEBUG, "zixu debug", K(ret), "last_insert_id_to_client", get_last_insert_id_to_client());
      }
    }
  }
  return ret;
}
bool ObResultSet::need_rollback(int ret, int errcode, bool is_error_ignored) const
{
  bool bret = false;
  if (OB_SUCCESS != ret || (errcode != OB_SUCCESS && errcode != OB_ITER_END) || is_error_ignored) {
    bret = true;
  }
  return bret;
}

OB_INLINE int ObResultSet::do_close_plan(int errcode, ObExecContext& ctx)
{
  int ret = common::OB_SUCCESS;
  int pret = OB_SUCCESS;
  int sret = OB_SUCCESS;
  NG_TRACE(close_plan_begin);
  if (OB_LIKELY(NULL != physical_plan_)) {
    if (OB_ISNULL(exec_result_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("exec result is null", K(ret));
    } else if (OB_FAIL(exec_result_->close(ctx))) {
      SQL_LOG(WARN, "fail close main query", K(ret));
    }
    // Notify all tasks of the plan to delete the corresponding intermediate results
    int close_ret = OB_SUCCESS;
    ObPhysicalPlanCtx* plan_ctx = NULL;
    if (OB_ISNULL(plan_ctx = get_exec_context().get_physical_plan_ctx())) {
      close_ret = OB_ERR_UNEXPECTED;
      LOG_WARN("physical plan ctx is null");
    } else if (OB_SUCCESS != (close_ret = executor_.close(ctx))) {
      SQL_LOG(WARN, "fail to close executor", K(ret), K(close_ret));
    }
    // reset_op_ctx anyway
    bool err_ignored = false;
    if (OB_ISNULL(plan_ctx)) {
      LOG_ERROR("plan is not NULL, but plan ctx is NULL", K(ret), K(errcode));
    } else {
      err_ignored = plan_ctx->is_error_ignored();
    }
    bool rollback = need_rollback(ret, errcode, err_ignored);
    pret = end_participant(rollback);
    sret = end_stmt(rollback || OB_SUCCESS != pret);
    // SQL_LOG(INFO, "end_stmt err code", K_(errcode), K(ret), K(pret), K(sret));
    // if branch fail is returned from end_stmt, then return it first
    if (OB_TRANS_XA_BRANCH_FAIL == sret) {
      ret = OB_TRANS_XA_BRANCH_FAIL;
    } else if (OB_FAIL(ret)) {
      // nop
    } else if (OB_SUCCESS != pret) {
      ret = pret;
    } else if (OB_SUCCESS != sret) {
      ret = sret;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
  }

  // reset executor_ anyway
  executor_.reset();

  ObPxAdmission::exit_query_admission(worker_count_);

  NG_TRACE(close_plan_end);
  return ret;
}

int ObResultSet::close(bool need_retry)
{
  int ret = OB_SUCCESS;
  int do_close_plan_ret = OB_SUCCESS;
  if (OB_LIKELY(NULL != physical_plan_)) {
    if (OB_FAIL(my_session_.reset_tx_variable_if_remote_trans(physical_plan_->get_plan_type()))) {
      LOG_WARN("fail to reset tx_read_only if it is remote trans", K(ret));
    }
    // do_close_plan anyway
    if (OB_UNLIKELY(OB_SUCCESS != (do_close_plan_ret = do_close_plan(errcode_, get_exec_context())))) {
      SQL_LOG(WARN, "fail close main query", K(ret), K(do_close_plan_ret));
    }
    if (OB_SUCC(ret)) {
      ret = do_close_plan_ret;
    }
  } else if (NULL != cmd_) {
    ret = OB_SUCCESS;  // cmd mode always return true in close phase
  } else {
    // inner sql executor, no plan or cmd. do nothing.
  }

  // Record plan id to session for the next show plan
  if (OB_LIKELY(NULL != physical_plan_)) {
    if (OB_LIKELY(physical_plan_->get_literal_stmt_type() != stmt::T_SHOW_TRACE)) {
      my_session_.set_last_plan_id(physical_plan_->get_plan_id());
    }
  } else {
    my_session_.set_last_plan_id(OB_INVALID_ID);
  }

  // close result set anyway
  if (OB_SUCCESS != errcode_ && OB_ITER_END != errcode_) {
    ret = errcode_;
  }
  if (OB_SUCC(ret)) {
    ObPhysicalPlanCtx* plan_ctx = get_exec_context().get_physical_plan_ctx();
    if (OB_ISNULL(plan_ctx)) {
      ret = OB_NOT_INIT;
      LOG_WARN("result set isn't init", K(ret));
    } else {
      store_affected_rows(*plan_ctx);
      store_found_rows(*plan_ctx);
    }
  }

  if (OB_SUCC(ret) && get_stmt_type() == stmt::T_SELECT) {
    if (OB_FAIL(update_is_result_accurate())) {
      SQL_LOG(WARN, "failed to update is result_accurate", K(ret));
    }
  }
  // set last_insert_id
  int ins_ret = OB_SUCCESS;
  if (OB_SUCCESS != ret && get_stmt_type() != stmt::T_INSERT && get_stmt_type() != stmt::T_REPLACE) {
    // ignore when OB_SUCCESS != ret and stmt like select/update/delete... executed
  } else if (OB_SUCCESS != (ins_ret = store_last_insert_id(get_exec_context()))) {
    SQL_LOG(WARN, "failed to store last_insert_id", K(ret), K(ins_ret));
  }

  if (OB_SUCC(ret)) {
    ret = ins_ret;
  }
  int prev_ret = ret;
  bool async = false;  // for debug purpose
  ret = auto_end_plan_trans(ret, need_retry, async);
  if (OB_TRANS_XA_BRANCH_FAIL == do_close_plan_ret) {
    LOG_WARN("branch fail in global transaction", K(my_session_.get_trans_desc()));
    my_session_.reset_first_stmt_type();
    my_session_.reset_tx_variable();
    my_session_.set_early_lock_release(false);
    my_session_.get_trans_desc().get_standalone_stmt_desc().reset();
  }
  NG_TRACE_EXT(result_set_close,
      OB_ID(ret),
      ret,
      OB_ID(arg1),
      prev_ret,
      OB_ID(arg2),
      ins_ret,
      OB_ID(arg3),
      errcode_,
      OB_ID(async),
      async);
  return ret;
}

// Call end_trans asynchronously
OB_INLINE int ObResultSet::auto_end_plan_trans(int ret, bool need_retry, bool& async)
{
  NG_TRACE(auto_end_plan_begin);
  if (get_trans_state().is_start_trans_executed() && get_trans_state().is_start_trans_success() && need_end_trans()) {
    ObPhysicalPlanCtx* plan_ctx = NULL;
    if (OB_ISNULL(plan_ctx = get_exec_context().get_physical_plan_ctx())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("physical plan ctx is null, won't call end trans!!!", K(ret));
    } else {
      // bool is_rollback = (OB_FAIL(ret) || plan_ctx->is_force_rollback());
      bool is_rollback = need_rollback(OB_SUCCESS, ret, plan_ctx->is_error_ignored());
      if (OB_LIKELY(false == is_end_trans_async()) || OB_LIKELY(false == is_user_sql_)) {
        ObEndTransSyncCallback callback;
        callback.set_last_error(ret);
        if (OB_FAIL(callback.init(&(my_session_.get_trans_desc()), &my_session_))) {
          SQL_ENG_LOG(WARN, "fail init callback", K(ret));
        } else {
          int wait_ret = OB_SUCCESS;
          if (OB_FAIL(ObSqlTransControl::implicit_end_trans(get_exec_context(), is_rollback, callback))) {
            SQL_LOG(WARN, "fail end trans", K(ret));
          }
          // wait no matter return code
          if (OB_UNLIKELY(OB_SUCCESS != (wait_ret = callback.wait()))) {
            if (OB_REPLICA_NOT_READABLE != wait_ret) {
              LOG_WARN("sync end trans callback return an error!",
                  K(ret),
                  K(wait_ret),
                  K(is_rollback),
                  K(my_session_.get_trans_desc()));
            }
          }
          ret = OB_SUCCESS != ret ? ret : wait_ret;
        }
        async = false;
      } else {
        ObEndTransAsyncCallback& cb = my_session_.get_end_trans_cb();
        cb.set_last_error(ret);
        ret = ObSqlTransControl::implicit_end_trans(get_exec_context(), is_rollback, cb);
        get_trans_state().set_end_trans_executed(OB_SUCC(ret));
        async = true;
      }
      my_session_.get_trans_desc().get_standalone_stmt_desc().reset();
      get_trans_state().clear_start_trans_executed();
    }
  } else if (OB_SUCCESS != ret) {
    if (lib::is_oracle_mode() && is_user_sql() && !need_retry && !my_session_.has_set_trans_var() &&
        my_session_.is_isolation_serializable()) {
      bool need_end_trans =
          // The first statement of the current transaction failed,
          // and the entire transaction needs to be rolled back
          (is_dml_stmt(stmt_type_) && !my_session_.has_any_dml_succ()) ||
          (is_pl_stmt(stmt_type_) && !my_session_.has_any_pl_succ() && is_pl_stmt(my_session_.get_first_stmt_type()));
      if (need_end_trans && OB_FAIL(ObSqlTransControl::explicit_end_trans(get_exec_context(), true))) {
        LOG_WARN("failed to explicit end trans", K(ret));
      }
    }
    if (my_session_.is_standalone_stmt()) {
      if (my_session_.get_local_autocommit()) {
        my_session_.get_trans_desc().get_standalone_stmt_desc().reset();
        my_session_.reset_tx_variable();
      } else {
        my_session_.get_trans_desc().get_standalone_stmt_desc().set_standalone_stmt_end();
      }
    }
  } else {
    if (lib::is_oracle_mode() && is_user_sql()) {
      if (is_dml_stmt(stmt_type_)) {
        my_session_.set_has_any_dml_succ(true);
      } else if (is_pl_stmt(stmt_type_)) {
        my_session_.set_has_any_pl_succ(true);
      }
    }
    if (my_session_.is_standalone_stmt()) {
      if (my_session_.get_local_autocommit()) {
        my_session_.get_trans_desc().get_standalone_stmt_desc().reset();
        my_session_.reset_tx_variable();
      } else {
        my_session_.get_trans_desc().get_standalone_stmt_desc().set_standalone_stmt_end();
      }
    }
  }
  NG_TRACE(auto_end_plan_end);
  return ret;
}

void ObResultSet::set_statement_name(const common::ObString name)
{
  statement_name_ = name;
}

int ObResultSet::from_plan(const ObPhysicalPlan& phy_plan, const ObIArray<ObPCParam*>& raw_params)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  if (OB_ISNULL(plan_ctx = get_exec_context().get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("physical plan ctx is null or ref handle is invalid", K(ret), K(plan_ctx));
  } else if (phy_plan.contain_paramed_column_field() && OB_FAIL(copy_field_columns(phy_plan))) {
    LOG_WARN("failed to copy field columns", K(ret));
  } else if (phy_plan.contain_paramed_column_field() && OB_FAIL(construct_field_name(raw_params, false))) {
    LOG_WARN("failed to construct field name", K(ret));
  } else {
    p_field_columns_ = phy_plan.contain_paramed_column_field() ? &field_columns_ : &phy_plan.get_field_columns();
    p_param_columns_ = &phy_plan.get_param_fields();
    stmt_type_ = phy_plan.get_stmt_type();
    literal_stmt_type_ = phy_plan.get_literal_stmt_type();
    is_returning_ = phy_plan.is_returning();
    plan_ctx->set_is_affect_found_row(phy_plan.is_affect_found_row());
  }
  return ret;
}

int ObResultSet::to_plan(const bool is_ps_mode, ObPhysicalPlan* phy_plan)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(phy_plan)) {
    LOG_ERROR("invalid argument", K(phy_plan));
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (OB_FAIL(phy_plan->set_field_columns(field_columns_))) {
      LOG_WARN("Failed to copy field info to plan", K(ret));
    } else if (is_ps_mode && OB_FAIL(phy_plan->set_param_fields(param_columns_))) {
      // param fields is only needed ps mode
      LOG_WARN("failed to copy param field to plan", K(ret));
    }
  }

  return ret;
}

int ObResultSet::get_read_consistency(ObConsistencyLevel& consistency)
{
  consistency = INVALID_CONSISTENCY;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(physical_plan_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("physical_plan", K_(physical_plan), K(ret));
    ret = OB_ERR_UNEXPECTED;
  } else {
    const ObQueryHint& query_hint = physical_plan_->get_query_hint();
    if (stmt::T_SELECT == stmt_type_) {
      if (OB_UNLIKELY(query_hint.read_consistency_ != INVALID_CONSISTENCY)) {
        consistency = query_hint.read_consistency_;
      } else {
        consistency = my_session_.get_consistency_level();
      }
    } else {
      consistency = STRONG;
    }
  }
  return ret;
}

int ObResultSet::init_cmd_exec_context(ObExecContext& exec_ctx)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = GET_PHY_PLAN_CTX(exec_ctx);
  void* buf = NULL;
  if (OB_ISNULL(cmd_) || OB_ISNULL(plan_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("cmd or ctx is NULL", K(ret), K(cmd_), K(plan_ctx));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(buf = get_mem_pool().alloc(sizeof(ObNewRow)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(sizeof(ObNewRow)), K(ret));
  } else {
    exec_ctx.set_output_row(new (buf) ObNewRow());
    exec_ctx.set_field_columns(&field_columns_);
    int64_t plan_timeout = 0;
    if (OB_FAIL(my_session_.get_query_timeout(plan_timeout))) {
      LOG_WARN("fail to get query timeout", K(ret));
    } else {
      int64_t start_time = my_session_.get_query_start_time();
      plan_ctx->set_timeout_timestamp(start_time + plan_timeout);
      THIS_WORKER.set_timeout_ts(plan_ctx->get_timeout_timestamp());
    }
  }
  return ret;
}

int ObResultSet::refresh_location_cache(bool is_nonblock)
{
  return ObTaskExecutorCtxUtil::refresh_location_cache(get_exec_context().get_task_exec_ctx(), is_nonblock);
}

int ObResultSet::check_and_nonblock_refresh_location_cache()
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx& task_exec_ctx = get_exec_context().get_task_exec_ctx();
  ObIPartitionLocationCache* cache = task_exec_ctx.get_partition_location_cache();
  if (OB_ISNULL(cache)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("cache is NULL", K(ret));
  } else {
    if (task_exec_ctx.is_need_renew_location_cache()) {
      const int64_t expire_renew_time = INT64_MAX;
      const common::ObList<common::ObPartitionKey, common::ObIAllocator>& part_keys =
          task_exec_ctx.get_need_renew_partition_keys();
      FOREACH_X(it, part_keys, OB_SUCC(ret))
      {
        bool is_limited = false;
        if (OB_FAIL(cache->nonblock_renew_with_limiter(*it, expire_renew_time, is_limited))) {
          LOG_WARN("LOCATION: fail to renew", K(ret), K(*it), K(expire_renew_time), K(is_limited));
        } else {
#if !defined(NDEBUG)
          LOG_INFO("LOCATION: noblock renew with limiter", "key", *it);
#endif
        }
      }
    }
  }
  return ret;
}

bool ObResultSet::need_end_trans_callback() const
{
  int ret = OB_SUCCESS;
  bool need = false;
  if (stmt::T_SELECT == get_stmt_type()) {
    // callbacks are not always taken, regardless of the transaction status
    need = false;
  } else if (is_returning_) {
    need = false;
  } else if (my_session_.get_has_temp_table_flag() ||
             my_session_.get_trans_desc().is_trx_level_temporary_table_involved()) {
    need = false;
  } else if (stmt::T_END_TRANS == get_stmt_type()) {
    need = true;
  } else {
    bool in_trans = my_session_.get_in_transaction();
    bool ac = true;
    if (OB_FAIL(my_session_.get_autocommit(ac))) {
      LOG_ERROR("fail to get autocommit", K(ret));
    } else {
    }
    if (stmt::T_ANONYMOUS_BLOCK == get_stmt_type()) {
      need = ac && !my_session_.get_in_transaction() && !is_with_rows();
    } else if (OB_LIKELY(NULL != physical_plan_) && OB_LIKELY(physical_plan_->is_need_trans())) {
      need = (true == ObSqlTransUtil::plan_can_end_trans(ac, in_trans)) &&
             (false == ObSqlTransUtil::is_remote_trans(ac, in_trans, physical_plan_->get_plan_type()));
    }
  }
  return need;
}

bool ObResultSet::need_end_trans() const
{
  int ret = OB_SUCCESS;
  bool need = false;
  bool in_trans = my_session_.get_in_transaction();
  bool ac = true;
  if (OB_FAIL(my_session_.get_autocommit(ac))) {
    LOG_ERROR("fail to get autocommit", K(ret));
  }
  if (OB_LIKELY(NULL != physical_plan_) && OB_LIKELY(physical_plan_->is_need_trans())) {
    need = !my_session_.is_standalone_stmt() && (true == ObSqlTransUtil::plan_can_end_trans(ac, in_trans)) &&
           (false == ObSqlTransUtil::is_remote_trans(ac, in_trans, physical_plan_->get_plan_type()));
  }
  return need;
}

int ObResultSet::ExternalRetrieveInfo::check_into_exprs(
    ObStmt& stmt, ObArray<ObDataType>& basic_types, ObBitSet<>& basic_into)
{
  int ret = OB_SUCCESS;
  CK(stmt.is_select_stmt() || stmt.is_insert_stmt() || stmt.is_update_stmt() || stmt.is_delete_stmt());

  if (OB_SUCC(ret)) {
    if (stmt.is_select_stmt()) {
      ObSelectStmt& select_stmt = static_cast<ObSelectStmt&>(stmt);
      if (basic_types.count() != select_stmt.get_select_item_size()) {
        ret = OB_ERR_TOO_MANY_VALUES;
        LOG_WARN("ORA-00913: too many values", K(ret), K(basic_types.count()), K(select_stmt.get_select_item_size()));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt.get_select_item_size(); ++i) {
        if (basic_into.has_member(i)) {
        } else if (OB_ISNULL(select_stmt.get_select_item(i).expr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("select expr is NULL", K(i), K(ret));
        } else if (select_stmt.get_select_item(i).expr_->get_result_type().get_obj_meta() !=
                       basic_types.at(i).get_meta_type() ||
                   select_stmt.get_select_item(i).expr_->get_result_type().get_accuracy() !=
                       basic_types.at(i).get_accuracy()) {
          LOG_DEBUG("select item type and into expr type are not match",
              K(i),
              K(select_stmt.get_select_item(i).expr_->get_result_type()),
              K(basic_types.at(i)),
              K(ret));
        }
      }
    } else {
      ObDelUpdStmt& dml_stmt = static_cast<ObDelUpdStmt&>(stmt);
      const ObIArray<ObRawExpr*>& returning_exprs = dml_stmt.get_returning_exprs();
      if (basic_types.count() != returning_exprs.count()) {
        ret = OB_ERR_TOO_MANY_VALUES;
        LOG_WARN("ORA-00913: too many values", K(ret), K(basic_types.count()), K(returning_exprs.count()));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < returning_exprs.count(); ++i) {
        if (basic_into.has_member(i)) {
        } else if (OB_ISNULL(returning_exprs.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("returning expr is NULL", K(i), K(ret));
        } else if (returning_exprs.at(i)->get_result_type().get_obj_meta() != basic_types.at(i).get_meta_type() ||
                   returning_exprs.at(i)->get_result_type().get_accuracy() != basic_types.at(i).get_accuracy()) {
          LOG_DEBUG("select item type and into expr type are not match",
              K(i),
              K(returning_exprs.at(i)->get_result_type()),
              K(basic_types.at(i)),
              K(ret));
        }
      }
    }
  }
  return ret;
}

int ObResultSet::ExternalRetrieveInfo::build(
    ObStmt& stmt, ObSQLSessionInfo& session_info, common::ObIArray<std::pair<ObRawExpr*, ObConstRawExpr*>>& param_info)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret)) {
    if (param_info.empty() && into_exprs_.empty()) {
      if (stmt.is_dml_stmt()) {
        OZ(ObSQLUtils::reconstruct_sql(allocator_, &stmt, stmt.get_sql_stmt(), session_info.create_obj_print_params()));
      } else {
        // other stmt do not need reconstruct.
      }
    } else {
      OZ(ObSQLUtils::reconstruct_sql(allocator_, &stmt, stmt.get_sql_stmt(), session_info.create_obj_print_params()));
      external_params_.set_capacity(param_info.count());
      for (int64_t i = 0; OB_SUCC(ret) && i < param_info.count(); ++i) {
        if (OB_ISNULL(param_info.at(i).first) || OB_ISNULL(param_info.at(i).second)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param expr is NULL", K(i), K(param_info.at(i).first), K(param_info.at(i).second), K(ret));
        } else if (OB_FAIL(external_params_.push_back(param_info.at(i).first))) {
          LOG_WARN("push back error", K(i), K(param_info.at(i).first), K(ret));
        } else if (T_QUESTIONMARK == param_info.at(i).first->get_expr_type()) {
          ObConstRawExpr* const_expr = static_cast<ObConstRawExpr*>(param_info.at(i).first);
          if (OB_FAIL(param_info.at(i).second->get_value().apply(const_expr->get_value()))) {
            LOG_WARN("apply error", K(const_expr->get_value()), K(param_info.at(i).second->get_value()), K(ret));
          }
        } else {
          // If it is not a local variable of PL, you need to change the value of
          // QUESTIONMARK to an invalid value, so as not to confuse the proxy
          param_info.at(i).second->get_value().set_unknown(OB_INVALID_INDEX);
          param_info.at(i).second->set_expr_obj_meta(param_info.at(i).second->get_value().get_meta());
        }
      }
      OZ(ObSQLUtils::reconstruct_sql(allocator_, &stmt, route_sql_, session_info.create_obj_print_params()));
    }
  }
  LOG_INFO("reconstruct sql:", K(stmt.get_prepare_param_count()), K(stmt.get_sql_stmt()), K(route_sql_), K(ret));
  return ret;
}

int ObResultSet::drive_pdml_query()
{
  int ret = OB_SUCCESS;
  if (get_physical_plan()->is_returning() || get_physical_plan()->is_local_or_remote_plan()) {
    // nothing
  } else if (get_physical_plan()->is_use_px() && !get_physical_plan()->is_use_pdml()) {
    // In the case of DML + PX, you need to call get_next_row to drive the PX framework
    stmt::StmtType type = get_physical_plan()->get_stmt_type();
    if (type == stmt::T_INSERT || type == stmt::T_DELETE || type == stmt::T_UPDATE || type == stmt::T_REPLACE ||
        type == stmt::T_MERGE) {
      const ObNewRow* row = nullptr;
      if (OB_LIKELY(OB_ITER_END == (ret = get_next_row(row)))) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("do dml query failed", K(ret));
      }
    }
  } else if (get_physical_plan()->is_use_pdml()) {
    const ObNewRow* row = nullptr;
    if (OB_LIKELY(OB_ITER_END == (ret = get_next_row(row)))) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("do pdml query failed", K(ret));
    }
  }

  return ret;
}

int ObResultSet::copy_field_columns(const ObPhysicalPlan& plan)
{
  int ret = OB_SUCCESS;
  field_columns_.reset();

  int64_t N = plan.get_field_columns().count();
  ObField field;
  if (N > 0 && OB_FAIL(field_columns_.reserve(N))) {
    LOG_WARN("failed to reserve field column array", K(ret), K(N));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
    const ObField& ofield = plan.get_field_columns().at(i);
    if (OB_FAIL(field.deep_copy(ofield, &get_mem_pool()))) {
      LOG_WARN("deep copy field failed", K(ret));
    } else if (OB_FAIL(field_columns_.push_back(field))) {
      LOG_WARN("push back field column failed", K(ret));
    } else {
      LOG_DEBUG("succs to copy field", K(field));
    }
  }
  return ret;
}

int ObResultSet::construct_field_name(const common::ObIArray<ObPCParam*>& raw_params, const bool is_first_parse)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < field_columns_.count(); i++) {
    if (OB_FAIL(construct_display_field_name(field_columns_.at(i), raw_params, is_first_parse))) {
      LOG_WARN("failed to construct display name", K(ret), K(field_columns_.at(i)));
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObResultSet::construct_display_field_name(
    common::ObField& field, const ObIArray<ObPCParam*>& raw_params, const bool is_first_parse)
{
  int ret = OB_SUCCESS;
  char* buf = nullptr;
  int32_t buf_len = MAX_COLUMN_CHAR_LENGTH * 2;
  int32_t pos = 0;
  int32_t name_pos = 0;

  if (!field.is_paramed_select_item_ || NULL == field.paramed_ctx_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(field.is_paramed_select_item_), K(field.paramed_ctx_));
  } else if (0 == field.paramed_ctx_->paramed_cname_.length()) {
    // 1. The length of the parameterized cname is 0, indicating that the column name is specified
    // 2. The alias is specified, and the alias is stored in cname_, so you can use it directly
    // do nothing
  } else if (OB_ISNULL(buf = static_cast<char*>(get_mem_pool().alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret), K(buf_len));
  } else {
    LOG_DEBUG("construct display field name", K(field));
#define PARAM_CTX field.paramed_ctx_
    for (int64_t i = 0; OB_SUCC(ret) && pos <= buf_len && i < PARAM_CTX->param_idxs_.count(); i++) {
      int64_t idx = PARAM_CTX->param_idxs_.at(i);
      if (idx >= raw_params.count()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid index", K(i), K(raw_params.count()));
      } else if (OB_ISNULL(raw_params.at(idx)) || OB_ISNULL(raw_params.at(idx)->node_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(raw_params.at(idx)), K(raw_params.at(idx)->node_));
      } else {
        int32_t len = (int32_t)PARAM_CTX->param_str_offsets_.at(i) - name_pos;
        len = std::min(buf_len - pos, len);
        if (len > 0) {
          MEMCPY(buf + pos, PARAM_CTX->paramed_cname_.ptr() + name_pos, len);
        }
        name_pos = (int32_t)PARAM_CTX->param_str_offsets_.at(i) + 1;  // skip '?'
        pos += len;

        if (is_first_parse && PARAM_CTX->neg_param_idxs_.has_member(idx) && pos < buf_len) {
          buf[pos++] = '-';  // insert `-` for negative value
        }
        int64_t copy_str_len = 0;
        const char* copy_str = NULL;
        if (1 == raw_params.at(idx)->node_->is_copy_raw_text_) {
          copy_str_len = raw_params.at(idx)->node_->text_len_;
          copy_str = raw_params.at(idx)->node_->raw_text_;
        } else if (PARAM_CTX->esc_str_flag_) {
          if (share::is_mysql_mode() && 1 == raw_params.at(idx)->node_->num_child_) {
            LOG_DEBUG("concat str node");
            if (OB_ISNULL(raw_params.at(idx)->node_->children_) || OB_ISNULL(raw_params.at(idx)->node_->children_[0]) ||
                T_CONCAT_STRING != raw_params.at(idx)->node_->children_[0]->type_) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invalid argument", K(ret));
            } else {
              copy_str_len = raw_params.at(idx)->node_->children_[0]->str_len_;
              copy_str = raw_params.at(idx)->node_->children_[0]->str_value_;
            }
          } else {
            copy_str_len = raw_params.at(idx)->node_->str_len_;
            copy_str = raw_params.at(idx)->node_->str_value_;
            if (share::is_oracle_mode() && pos < buf_len) {
              buf[pos++] = '\'';
            }
          }
        } else {
          copy_str_len = raw_params.at(idx)->node_->text_len_;
          copy_str = raw_params.at(idx)->node_->raw_text_;
        }

        if (OB_SUCC(ret)) {
          len = std::min(buf_len - pos, (int32_t)copy_str_len);
          if (len > 0) {
            MEMCPY(buf + pos, copy_str, len);
          }

          if (len > 0 && 1 == raw_params.at(idx)->node_->is_copy_raw_text_ && share::is_oracle_mode()) {
            // If you copy raw_text_ directly, you may need to convert case and remove spaces
            len = (int32_t)remove_extra_space(buf + pos, len);
            len = (int32_t)ObCharset::caseup(ObCollationType::CS_TYPE_UTF8MB4_BIN, buf + pos, len, buf + pos, len);
          }
          pos += len;

          if (PARAM_CTX->esc_str_flag_ && share::is_oracle_mode() && pos < buf_len) {
            buf[pos++] = '\'';
          }
        }
      }
    }  // for end

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (name_pos < PARAM_CTX->paramed_cname_.length()) {
      int32_t len = std::min((int32_t)PARAM_CTX->paramed_cname_.length() - name_pos, buf_len - pos);
      if (len > 0) {
        MEMCPY(buf + pos, PARAM_CTX->paramed_cname_.ptr() + name_pos, len);
      }
      pos += len;
    }

    if (OB_FAIL(ret) || 0 == PARAM_CTX->param_idxs_.count()) {
      // do nothing
    } else if (share::is_oracle_mode()) {
      pos = (int32_t)remove_extra_space(buf, pos);
      pos = (int32_t)ObCharset::caseup(ObCollationType::CS_TYPE_UTF8MB4_BIN, buf, pos, buf, pos);
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (share::is_mysql_mode() && OB_FAIL(make_final_field_name(buf, pos, field.cname_))) {
      LOG_WARN("failed to make final field name", K(ret));
    } else if (share::is_oracle_mode()) {
      pos = pos < MAX_COLUMN_CHAR_LENGTH ? pos : MAX_COLUMN_CHAR_LENGTH;
      // field.cname_.assign(buf, pos);
      if (OB_FAIL(ob_write_string(get_mem_pool(), ObString(pos, buf), field.cname_))) {
        LOG_WARN("failed to copy paramed cname", K(ret));
      }
    } else {
      // do nothing
    }
#undef PARAM_CTX
  }
  return ret;
}

int64_t ObResultSet::remove_extra_space(char* buff, int64_t len)
{
  int64_t length = 0;
  if (OB_LIKELY(NULL != buff)) {
    for (int64_t i = 0; i < len; i++) {
      if (!std::isspace(buff[i])) {
        buff[length++] = buff[i];
      }
    }
  }
  return length;
}

void ObResultSet::replace_lob_type(const ObSQLSessionInfo& session, const ObField& field, obmysql::ObMySQLField& mfield)
{
  bool is_use_lob_locator = session.is_client_use_lob_locator();
  if (lib::is_oracle_mode()) {
    if (is_lob_locator(field.type_.get_type()) || is_lob(field.type_.get_type())) {
      if (!is_use_lob_locator && is_lob_locator(field.type_.get_type())) {
        mfield.type_ = obmysql::EMySQLFieldType::MYSQL_TYPE_LONG_BLOB;
      } else if (is_use_lob_locator) {
        if (CS_TYPE_BINARY == field.charsetnr_) {
          mfield.type_ = obmysql::EMySQLFieldType::MYSQL_TYPE_ORA_BLOB;
        } else {
          mfield.type_ = obmysql::EMySQLFieldType::MYSQL_TYPE_ORA_CLOB;
        }
      }
    }
    LOG_TRACE("init field", K(is_use_lob_locator), K(field), K(mfield.type_));
  }
}

int ObResultSet::make_final_field_name(char* buf, int64_t len, common::ObString& field_name)
{
  int ret = OB_SUCCESS;
  ObCollationType cs_type = common::CS_TYPE_INVALID;
  if (OB_ISNULL(buf) || 0 == len) {
    field_name.assign(buf, static_cast<int32_t>(len));
  } else if (OB_FAIL(my_session_.get_collation_connection(cs_type))) {
    LOG_WARN("failed to get collation_connection", K(ret));
  } else {
    while (len && !ObCharset::is_graph(cs_type, *buf)) {
      buf++;
      len--;
    }
    len = len < MAX_COLUMN_CHAR_LENGTH ? len : MAX_COLUMN_CHAR_LENGTH;
    field_name.assign(buf, static_cast<int32_t>(len));
  }
  return ret;
}

uint64_t ObResultSet::get_field_cnt() const
{
  int64_t cnt = 0;
  uint64_t ret = 0;
  if (OB_ISNULL(get_field_columns())) {
    LOG_ERROR("unexpected error. field columns is null");
    right_to_die_or_duty_to_live();
  }
  cnt = get_field_columns()->count();
  if (cnt >= 0) {
    ret = static_cast<uint64_t>(cnt);
  }
  return ret;
}

bool ObResultSet::has_implicit_cursor() const
{
  bool bret = false;
  if (get_exec_context().get_physical_plan_ctx() != nullptr) {
    bret = !get_exec_context().get_physical_plan_ctx()->get_implicit_cursor_infos().empty();
  }
  return bret;
}

int ObResultSet::switch_implicit_cursor()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = get_exec_context().get_physical_plan_ctx();
  if (OB_ISNULL(plan_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plan_ctx is null", K(ret));
  } else if (OB_FAIL(plan_ctx->switch_implicit_cursor())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("cursor_idx is invalid", K(ret));
    }
  } else {
    set_affected_rows(plan_ctx->get_affected_rows());
    memset(message_, 0, sizeof(message_));
    if (OB_FAIL(set_mysql_info())) {
      LOG_WARN("set mysql info failed", K(ret));
    }
  }
  return ret;
}

bool ObResultSet::is_cursor_end() const
{
  bool bret = false;
  ObPhysicalPlanCtx* plan_ctx = get_exec_context().get_physical_plan_ctx();
  if (plan_ctx != nullptr) {
    bret = (plan_ctx->get_cur_stmt_id() >= plan_ctx->get_implicit_cursor_infos().count());
  }
  return bret;
}

int ObResultSet::prepare_mock_schemas()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_ISNULL(physical_plan_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid null plan", K(ret));
  } else if (physical_plan_->get_mock_rowid_tables().count() <= 0) {
    // do nothing
  } else if (OB_ISNULL(get_exec_context().get_sql_ctx()) ||
             OB_ISNULL(get_exec_context().get_sql_ctx()->schema_guard_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invliad arguments", K(ret));
  } else {
    ret = ObSQLMockSchemaUtils::prepare_mocked_schemas(physical_plan_->get_mock_rowid_tables());
  }
  return ret;
}
