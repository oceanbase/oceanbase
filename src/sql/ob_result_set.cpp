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
#include "sql/ob_result_set.h"
#include "lib/oblog/ob_trace_log.h"
#include "lib/charset/ob_charset.h"
#include "lib/utility/ob_macro_utils.h"
#include "rpc/obmysql/ob_mysql_global.h"
#include "rpc/obmysql/ob_mysql_field.h"
#include "lib/oblog/ob_log_module.h"
#include "engine/ob_physical_plan.h"
#include "sql/parser/parse_malloc.h"
#include "share/system_variable/ob_system_variable.h"
#include "share/system_variable/ob_system_variable_alias.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/resolver/ob_cmd.h"
#include "sql/engine/px/ob_px_admission.h"
#include "sql/engine/cmd/ob_table_direct_insert_service.h"
#include "sql/executor/ob_executor.h"
#include "sql/executor/ob_cmd_executor.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/resolver/cmd/ob_call_procedure_stmt.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/optimizer/ob_log_plan_factory.h"
#include "sql/ob_sql_trans_util.h"
#include "sql/ob_end_trans_callback.h"
#include "sql/session/ob_sql_session_info.h"
#include "lib/profile/ob_perf_event.h"
#include "sql/plan_cache/ob_cache_object_factory.h"
#include "share/ob_cluster_version.h"
#include "storage/tx/ob_trans_define.h"
#include "pl/ob_pl_user_type.h"
#include "pl/ob_pl_stmt.h"
#include "observer/ob_server_struct.h"
#include "storage/tx/wrs/ob_weak_read_service.h"       // ObWeakReadService
#include "storage/tx/wrs/ob_i_weak_read_service.h"     // WRS_LEVEL_SERVER
#include "storage/tx/wrs/ob_weak_read_util.h"          // ObWeakReadUtil
#include "observer/ob_req_time_service.h"
#include "sql/dblink/ob_dblink_utils.h"
#include "sql/dblink/ob_tm_service.h"
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
  ObPhysicalPlan* physical_plan = get_physical_plan();
  if (OB_NOT_NULL(physical_plan) && !is_remote_sql
      && OB_UNLIKELY(physical_plan->is_limited_concurrent_num())) {
    physical_plan->dec_concurrent_num();
  }
  // when ObExecContext is destroyed, it also depends on the physical plan, so need to ensure
  // that inner_exec_ctx_ is destroyed before cache_obj_guard_
  if (NULL != inner_exec_ctx_) {
    inner_exec_ctx_->~ObExecContext();
    inner_exec_ctx_ = NULL;
  }
  ObPlanCache *pc = my_session_.get_plan_cache_directly();
  if (OB_NOT_NULL(pc)) {
    cache_obj_guard_.force_early_release(pc);
  }
  // Always called at the end of the ObResultSet destructor
  update_end_time();
  is_init_ = false;
}

int ObResultSet::open_cmd()
{
  int ret = OB_SUCCESS;
  FLTSpanGuard(cmd_open);
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
  //ObLimit *limit_opt = NULL;
  ObPhysicalPlan* physical_plan_ = static_cast<ObPhysicalPlan*>(cache_obj_guard_.get_cache_obj());
  if (OB_ISNULL(physical_plan_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid physical plan", K(physical_plan_));
  } else {
    has_top_limit_ = physical_plan_->has_top_limit();
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObPxAdmission::enter_query_admission(my_session_,
                                                       get_exec_context(),
                                                       get_stmt_type(),
                                                       *get_physical_plan()))) {
        // query is not admitted to run
        // Note: explain statement's phy plan is target query's plan, don't enable admission test
        LOG_DEBUG("Query is not admitted to run, try again", K(ret));
      } else if (THIS_WORKER.is_timeout()) {
        // packet有可能在队列里面呆的时间过长，到这里已经超时，
        // 如果这里不检查，则会继续运行，很可能进入到其他模块里面，
        // 其他模块检测到超时，引起其他模块的疑惑，因此在这里加上超时检查。
        // 之所以在这个位置才检查，是为了考虑hint中的超时时间，
        // 因为在init_plan_exec_context函数里面才将hint的超时时间设进THIS_WORKER中。
        ret = OB_TIMEOUT;
        LOG_WARN("query is timeout", K(ret),
                 "timeout_ts", THIS_WORKER.get_timeout_ts(),
                 "start_time", my_session_.get_query_start_time());
      } else if (stmt::T_PREPARE != stmt_type_) {
        int64_t retry = 0;
        if (OB_SUCC(ret)) {
          do {
            ret = do_open_plan(get_exec_context());
          } while (transaction_set_violation_and_retry(ret, retry));
        }
      }
    }
  }

  return ret;
}

int ObResultSet::open()
{
  int ret = OB_SUCCESS;
  my_session_.set_process_query_time(ObTimeUtility::current_time());
  LinkExecCtxGuard link_guard(my_session_, get_exec_context());
  FLTSpanGuard(open);
  if (lib::is_oracle_mode() &&
      get_exec_context().get_nested_level() >= OB_MAX_RECURSIVE_SQL_LEVELS) {
    ret = OB_ERR_RECURSIVE_SQL_LEVELS_EXCEEDED;
    LOG_ORACLE_USER_ERROR(OB_ERR_RECURSIVE_SQL_LEVELS_EXCEEDED, OB_MAX_RECURSIVE_SQL_LEVELS);
  } else if (OB_FAIL(execute())) {
    LOG_WARN("execute plan failed", K(ret));
  } else if (OB_FAIL(open_result())) {
    LOG_WARN("open result set failed", K(ret));
  }

  if (OB_SUCC(ret)) {
    ObPhysicalPlan* physical_plan_ = static_cast<ObPhysicalPlan*>(cache_obj_guard_.get_cache_obj());
    if (OB_NOT_NULL(cmd_)) {
      // cmd not set
    } else if (ret != OB_NOT_INIT &&
        OB_ISNULL(physical_plan_)) {
      LOG_WARN("empty physical plan", K(ret));
    } else if (OB_FAIL(ret)) {
      physical_plan_->set_is_last_exec_succ(false);
    } else {
      physical_plan_->set_is_last_exec_succ(true);
    }
  }

  return ret;
}

int ObResultSet::execute()
{
  int ret = common::OB_SUCCESS;
  ObPhysicalPlan* physical_plan_ = static_cast<ObPhysicalPlan*>(cache_obj_guard_.get_cache_obj());
//  if (stmt::T_PREPARE != stmt_type_
//      && stmt::T_DEALLOCATE != stmt_type_) {
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
  set_errcode(ret);

  return ret;
}

int ObResultSet::open_result()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlan* physical_plan_ = static_cast<ObPhysicalPlan*>(cache_obj_guard_.get_cache_obj());
  if (NULL != physical_plan_) {
    if (OB_ISNULL(exec_result_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("exec result is null", K(ret));
    } else if (OB_FAIL(exec_result_->open(get_exec_context()))) {
      if (OB_TRANSACTION_SET_VIOLATION != ret && OB_TRY_LOCK_ROW_CONFLICT != ret) {
        SQL_LOG(WARN, "fail open main query", K(ret));
      }
    } else if (OB_FAIL(drive_dml_query())) {
      LOG_WARN("fail to drive dml query", K(ret));
    } else if ((stmt::T_INSERT == get_stmt_type())
        && (ObTableDirectInsertService::is_direct_insert(*physical_plan_))) {
      // for insert /*+ append */ into select clause
      if (OB_FAIL(ObTableDirectInsertService::commit_direct_insert(get_exec_context(), *physical_plan_))) {
        LOG_WARN("fail to commit direct insert", KR(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (!is_inner_result_set_ && OB_FAIL(set_mysql_info())) {
      SQL_LOG(WARN, "fail to get mysql info", K(ret));
    } else if (NULL != get_exec_context().get_physical_plan_ctx()) {
      SQL_LOG(DEBUG, "get affected row", K(get_stmt_type()),
              K(get_exec_context().get_physical_plan_ctx()->get_affected_rows()));
        set_affected_rows(get_exec_context().get_physical_plan_ctx()->get_affected_rows());
    }
    if (OB_SUCC(ret) && get_stmt_type() == stmt::T_ANONYMOUS_BLOCK) {
      // Compatible with oracle anonymous block affect rows setting
      set_affected_rows(1);
    }
  }
  set_errcode(ret);
  return ret;
}

/* on_cmd_execute - prepare before start execute cmd
 *
 * some command implicit commit current transaction state,
 * include release savepoints
 */
int ObResultSet::on_cmd_execute()
{
  int ret = OB_SUCCESS;

  bool ac = true;
  if (OB_ISNULL(cmd_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid inner state", K(cmd_));
  } else if (cmd_->cause_implicit_commit()) {
    if (my_session_.is_in_transaction() && my_session_.associated_xa()) {
      int tmp_ret = OB_SUCCESS;
      transaction::ObTxDesc *tx_desc = my_session_.get_tx_desc();
      const transaction::ObXATransID xid = my_session_.get_xid();
      const transaction::ObGlobalTxType global_tx_type = tx_desc->get_global_tx_type(xid);
      if (transaction::ObGlobalTxType::XA_TRANS == global_tx_type) {
        // commit is not allowed in xa trans
        ret = OB_TRANS_XA_ERR_COMMIT;
        LOG_WARN("COMMIT is not allowed in a xa trans", K(ret), K(xid), K(global_tx_type),
            KPC(tx_desc));
      } else if (transaction::ObGlobalTxType::DBLINK_TRANS == global_tx_type) {
        transaction::ObTransID tx_id;
        if (OB_FAIL(ObTMService::tm_commit(get_exec_context(), tx_id))) {
          LOG_WARN("fail to do commit for dblink trans", K(ret), K(tx_id), K(xid),
              K(global_tx_type));
        }
        my_session_.restore_auto_commit();
        const bool force_disconnect = false;
        if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = my_session_.get_dblink_context().clean_dblink_conn(force_disconnect)))) {
          LOG_WARN("dblink transaction failed to release dblink connections", K(tmp_ret), K(tx_id), K(xid));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected global trans type", K(ret), K(xid), K(global_tx_type), KPC(tx_desc));
      }
      get_exec_context().set_need_disconnect(false);
    } else {
      // implicit end transaction and start transaction will not clear next scope transaction settings by:
      // a. set by `set transaction read only`
      // b. set by `set transaction isolation level XXX`
      const int cmd_type = cmd_->get_cmd_type();
      bool keep_trans_variable = (cmd_type == stmt::T_START_TRANS);
      if (OB_FAIL(ObSqlTransControl::implicit_end_trans(get_exec_context(), false, NULL, !keep_trans_variable))) {
        LOG_WARN("fail end implicit trans on cmd execute", K(ret));
      } else if (my_session_.need_recheck_txn_readonly() && my_session_.get_tx_read_only()) {
        ret = OB_ERR_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION;
        LOG_WARN("cmd can not execute because txn is read only", K(ret), K(cmd_type));
      }
    }
  }
  return ret;
}

// open transaction if need (eg. ac=1 DML)
int ObResultSet::start_stmt()
{
  NG_TRACE(sql_start_stmt_begin);
  int ret = OB_SUCCESS;
  bool ac = true;
  ObPhysicalPlan* phy_plan = static_cast<ObPhysicalPlan*>(cache_obj_guard_.get_cache_obj());
  if (OB_ISNULL(phy_plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid inner state", K(phy_plan));
  } else if (OB_ISNULL(phy_plan->get_root_op_spec())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("root_op_spec of phy_plan is NULL", K(phy_plan), K(ret));
  } else if (OB_FAIL(my_session_.get_autocommit(ac))) {
    LOG_WARN("fail to get autocommit", K(ret));
  } else if (phy_plan->is_link_dml_plan() || phy_plan->has_link_sfd()) {
    if (ac) {
      my_session_.set_autocommit(false);
      my_session_.set_restore_auto_commit(); // auto commit will be restored at tm_rollback or tm_commit;
    }
    LOG_DEBUG("dblink xa trascaction need skip start_stmt()", K(PHY_LINK_DML == phy_plan->get_root_op_spec()->type_), K(my_session_.get_dblink_context().is_dblink_xa_tras()));
  } else {
    if (-1 != phy_plan->tx_id_) {
      const transaction::ObTransID tx_id(phy_plan->tx_id_);
      if (OB_FAIL(sql::ObTMService::recover_tx_for_callback(tx_id, get_exec_context()))) {
        LOG_WARN("failed to recover dblink xa transaction", K(ret), K(tx_id));
      } else {
        need_revert_tx_ = true;
        LOG_DEBUG("succ to recover dblink xa transaction", K(tx_id));
      }
    } else {
      LOG_DEBUG("recover dblink xa skip", K(phy_plan->tx_id_));
    }
    bool in_trans = my_session_.get_in_transaction();

    // 1. 无论是否处于事务中，只要不是select并且plan为REMOTE的，就反馈给客户端不命中
    // 2. feedback this misshit to obproxy (bug#6255177)
    // 3. 对于multi-stmt，只反馈首个partition hit信息给客户端
    // 4. 需要考虑到重试的情况，需要反馈给客户端的是首个重试成功的partition hit。
    if (OB_SUCC(ret) && stmt::T_SELECT != stmt_type_) {
      my_session_.partition_hit().try_set_bool(
          OB_PHY_PLAN_REMOTE != phy_plan->get_plan_type());
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (ObSqlTransUtil::is_remote_trans(
                ac, in_trans, phy_plan->get_plan_type())) {
      // pass
    } else if (OB_LIKELY(phy_plan->is_need_trans())) {
      if (get_trans_state().is_start_stmt_executed()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid transaction state", K(get_trans_state().is_start_stmt_executed()));
      } else if (OB_FAIL(ObSqlTransControl::start_stmt(get_exec_context()))) {
        SQL_LOG(WARN, "fail to start stmt", K(ret),
                K(phy_plan->get_dependency_table()));
      } else {
        auto literal_stmt_type = literal_stmt_type_ != stmt::T_NONE ? literal_stmt_type_ : stmt_type_;
        my_session_.set_first_need_txn_stmt_type(literal_stmt_type);
      }
      get_trans_state().set_start_stmt_executed(OB_SUCC(ret));
    }
  }
  NG_TRACE(sql_start_stmt_end);
  return ret;
}

int ObResultSet::end_stmt(const bool is_rollback)
{
  int ret = OB_SUCCESS;
  NG_TRACE(start_end_stmt);
  if (get_trans_state().is_start_stmt_executed()
      && get_trans_state().is_start_stmt_success()) {
    ObPhysicalPlan* physical_plan_ = static_cast<ObPhysicalPlan*>(cache_obj_guard_.get_cache_obj());
    if (OB_ISNULL(physical_plan_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid inner state", K(physical_plan_));
    } else if (physical_plan_->is_need_trans()) {
      if (OB_FAIL(ObSqlTransControl::end_stmt(get_exec_context(), is_rollback))) {
        SQL_LOG(WARN, "fail to end stmt", K(ret), K(is_rollback));
      }
    }
    get_trans_state().clear_start_stmt_executed();
  } else {
    // do nothing
  }
  if (need_revert_tx_) { // ignore ret
    int tmp_ret = sql::ObTMService::revert_tx_for_callback(get_exec_context());
    need_revert_tx_ = false;
    LOG_DEBUG("revert tx for callback", K(tmp_ret));
  }
  NG_TRACE(end_stmt);
  return ret;
}

//@notice: this interface can not be used in the interface of ObResultSet
//otherwise, will cause the exec context reference mistake in session info
//see the call reference in LinkExecCtxGuard
int ObResultSet::get_next_row(const common::ObNewRow *&row)
{
  LinkExecCtxGuard link_guard(my_session_, get_exec_context());
  return inner_get_next_row(row);
}

OB_INLINE int ObResultSet::inner_get_next_row(const common::ObNewRow *&row)
{
  int &ret = errcode_;
  ObPhysicalPlan* physical_plan_ = static_cast<ObPhysicalPlan*>(cache_obj_guard_.get_cache_obj());
  // last_exec_succ default values is true
  if (OB_LIKELY(NULL != physical_plan_)) { // take this branch more frequently
    if (OB_ISNULL(exec_result_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("exec result is null", K(ret));
    } else if (OB_FAIL(exec_result_->get_next_row(get_exec_context(), row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next row from exec result failed", K(ret));
        // marked last execute status
        physical_plan_->set_is_last_exec_succ(false);
      }
    } else {
      return_rows_++;
    }
  } else if (NULL != cmd_) {
    if (is_pl_stmt(static_cast<stmt::StmtType>(cmd_->get_cmd_type()))) {
      if (return_rows_ > 0) {
        errcode_ = OB_ITER_END;
      } else {
        row = get_exec_context().get_output_row();
        return_rows_++;
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      _OB_LOG(ERROR, "should not call get_next_row in CMD SQL");
    }
  } else {
    _OB_LOG(ERROR, "phy_plan not init");
    ret = OB_NOT_INIT;
  }
  //Save the current execution state to determine whether to refresh location
  //and perform other necessary cleanup operations when the statement exits.
  DAS_CTX(get_exec_context()).get_location_router().save_cur_exec_status(ret);

  return ret;
}

// 触发本错误的条件： A、B两个SQL，同时修改了某几行数据（修改内容有交集）。
// 微观上，修改操作要先读出符合条件的行，然后再更新。在读的时候，会记录一个版本号，
// 更新的时候，会检查版本号是否有变化。如果有变化，则说明在读之后、写之前，数据被其它
// 更新操作修改了。这个时候如果强行更新，则会违反一致性。举个例子：
// tbl里包含一行数据 0，希望通过A、B两个并发的操作让它变成2：
// A: update tbl set a = a + 1;
// B: update tbl set a = a + 1;
// 如果在A读a之后、更新a之前，B完成了整个更新操作，则B的更新会丢失，最终结果为a=1
//
// 解决方案：在更新数据之前检查版本号，发现版本号不一致，则抛出OB_TRANSACTION_SET_VIOLATION
// 错误，由SQL层在这里重试，保证读写版本号一致。
//
// Note: 由于本重试不涉及到刷新Schema或更新Location Cache，所以无需做整个语句级重试，
// 执行级别的重试即可。
bool ObResultSet::transaction_set_violation_and_retry(int &err, int64_t &retry_times)
{
  bool bret = false;
  ObSqlCtx *sql_ctx = get_exec_context().get_sql_ctx();
  bool is_batched_stmt = false;
  if (sql_ctx != nullptr) {
    is_batched_stmt = sql_ctx->is_batch_params_execute();
  }
  if ((OB_SNAPSHOT_DISCARDED == err
       || OB_TRANSACTION_SET_VIOLATION == err)
      && retry_times < TRANSACTION_SET_VIOLATION_MAX_RETRY
      && !is_batched_stmt) {
    //batched stmt本身是一种优化，其中cursor状态在快速重路径中无法维护，不走快速重试路径
    ObTxIsolationLevel isolation = my_session_.get_tx_isolation();
    bool is_isolation_RR_or_SE = (isolation == ObTxIsolationLevel::RR
                                  || isolation == ObTxIsolationLevel::SERIAL);
    // bug#6361189  pass err to force rollback stmt in do_close_plan()
    if (OB_TRANSACTION_SET_VIOLATION == err && 0 == retry_times && !is_isolation_RR_or_SE) {
      // TSC错误重试时，只在第一次打印WARN日志
      LOG_WARN_RET(err, "transaction set consistency violation, will retry");
    }
    int ret = do_close_plan(err, get_exec_context());
    ObPhysicalPlanCtx *plan_ctx = get_exec_context().get_physical_plan_ctx();
    if (OB_NOT_NULL(plan_ctx)) {
      plan_ctx->reset_for_quick_retry();
    }
    if (OB_SUCCESS != ret) {
      // 注意：如果do_close失败，会影响到TSC冲突重试，导致本该重试却实际没有重试的问题
      // 这里如果出现bug，则需要看每个涉及到的Operator的close实现
      LOG_WARN("failed to close plan", K(err), K(ret));
    } else {
      // OB_SNAPSHOT_DISCARDED should not retry now, see:
      //
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
    LOG_DEBUG("transaction set consistency violation and retry",
              "retry", bret, K(retry_times), K(err));
  }
  return bret;
}

OB_INLINE int ObResultSet::do_open_plan(ObExecContext &ctx)
{
  ObPhysicalPlan* physical_plan_ = static_cast<ObPhysicalPlan*>(cache_obj_guard_.get_cache_obj());
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

  if (OB_SUCC(ret) && my_session_.get_ddl_info().is_ddl() && stmt::T_INSERT == get_stmt_type()) {
    if (OB_FAIL(ObDDLUtil::clear_ddl_checksum(physical_plan_))) {
      LOG_WARN("fail to clear ddl checksum", K(ret));
    }
  }

  // for insert /*+ append */ into select clause
  if (OB_SUCC(ret)
      && (stmt::T_INSERT == get_stmt_type())
      && (ObTableDirectInsertService::is_direct_insert(*physical_plan_))) {
    if (OB_FAIL(ObTableDirectInsertService::start_direct_insert(ctx, *physical_plan_))) {
      LOG_WARN("fail to start direct insert", KR(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(start_stmt())) {
    LOG_WARN("fail start stmt", K(ret));
  } else {
    /* 将exec_result_设置到executor的运行时环境中，用于返回数据 */
    /* 执行plan,
     * 无论是本地、远程、还是分布式plan，除RootJob外，其余都会在execute_plan函数返回之前执行完毕
     * exec_result_负责执行最后一个Job: RootJob
     **/
    if (OB_FAIL(executor_.init(physical_plan_))) {
      SQL_LOG(WARN, "fail to init executor", K(ret), K(physical_plan_));
    } else if (OB_FAIL(executor_.execute_plan(ctx))) {
      SQL_LOG(WARN, "fail execute plan", K(ret));
    }
  }
  NG_TRACE(do_open_plan_end);
  return ret;
}

int ObResultSet::set_mysql_info()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = get_exec_context().get_physical_plan_ctx();
  int64_t pos = 0;
  if (OB_ISNULL(plan_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_LOG(WARN, "fail to get physical plan ctx");
  } else if (stmt::T_UPDATE == get_stmt_type()) {
    int result_len = snprintf(message_ + pos, MSG_SIZE - pos, OB_UPDATE_MSG_FMT, plan_ctx->get_row_matched_count(),
                              plan_ctx->get_row_duplicated_count(), warning_count_);
    if (OB_UNLIKELY(result_len < 0) || OB_UNLIKELY(result_len >= MSG_SIZE - pos)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to snprintf to buff", K(ret));
    }
  } else if (stmt::T_REPLACE == get_stmt_type()
             || stmt::T_INSERT == get_stmt_type()) {
    if (plan_ctx->get_row_matched_count() <= 1) {
      //nothing to do
    } else {
      int result_len = snprintf(message_ + pos, MSG_SIZE - pos, OB_INSERT_MSG_FMT, plan_ctx->get_row_matched_count(),
                                plan_ctx->get_row_duplicated_count(), warning_count_);
      if (OB_UNLIKELY(result_len < 0) || OB_UNLIKELY(result_len >= MSG_SIZE - pos)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to snprintf to buff", K(ret));
      }
    }
  } else if (stmt::T_LOAD_DATA == get_stmt_type()) {
    int result_len = snprintf(message_ + pos, MSG_SIZE - pos, OB_LOAD_DATA_MSG_FMT, plan_ctx->get_row_matched_count(),
                              plan_ctx->get_row_deleted_count(), plan_ctx->get_row_duplicated_count(), warning_count_);
    if (OB_UNLIKELY(result_len < 0) || OB_UNLIKELY(result_len >= MSG_SIZE - pos)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to snprintf to buff", K(ret));
    }
  } else {
    //nothing to do
  }
  return ret;
}

OB_INLINE void ObResultSet::store_affected_rows(ObPhysicalPlanCtx &plan_ctx)
{
  int64_t affected_row = 0;
  if (!ObStmt::is_dml_stmt(get_stmt_type())
      && (lib::is_oracle_mode() || !is_pl_stmt(get_stmt_type()))) {
    affected_row = 0;
  } else if (stmt::T_SELECT == get_stmt_type()) {
    affected_row = plan_ctx.get_affected_rows();
    if (lib::is_mysql_mode() && 0 == affected_row) {
      affected_row = -1;
    }
  } else {
    affected_row = get_affected_rows();
  }
  NG_TRACE_EXT(affected_rows, OB_ID(affected_rows), affected_row);
  my_session_.set_affected_rows(affected_row);
}

OB_INLINE void ObResultSet::store_found_rows(ObPhysicalPlanCtx &plan_ctx)
{
  int64_t rows = 1;
  //如果是execute语句的话，则需要判断对应prepare语句的类型，如果是select的话，则会影响found_rows的值；
  //to do by rongxuan.lc 20151127
  if (plan_ctx.is_affect_found_row()) {
    if (OB_UNLIKELY(stmt::T_EXPLAIN == get_stmt_type())) {
      rows = 0;
      my_session_.set_found_rows(rows);
    } else {
      int64_t found_rows = -1;
      found_rows = plan_ctx.get_found_rows();
      rows = found_rows == 0 ? return_rows_ : found_rows;
      my_session_.set_found_rows(rows);
      NG_TRACE_EXT(store_found_rows,
                   OB_ID(found_rows), found_rows,
                   OB_ID(return_rows), return_rows_);
    }
  }
  return;
}


int ObResultSet::update_last_insert_id()
{
  return store_last_insert_id(get_exec_context());
}

int ObResultSet::update_last_insert_id_to_client()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = get_exec_context().get_physical_plan_ctx();
  if (OB_ISNULL(plan_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_LOG(WARN, "get plan ctx is NULL", K(ret));
  } else {
    set_last_insert_id_to_client(plan_ctx->calc_last_insert_id_to_client());
  }
  return ret;
}

int ObResultSet::update_is_result_accurate()
{
  int ret = OB_SUCCESS;
  if (stmt::T_SELECT == stmt_type_) {
    ObPhysicalPlanCtx *plan_ctx = get_exec_context().get_physical_plan_ctx();
    bool old_is_result_accurate = true;
    if (OB_ISNULL(plan_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "get plan ctx is NULL", K(ret));
    } else if (OB_FAIL(my_session_.get_is_result_accurate(old_is_result_accurate))) {
      SQL_LOG(WARN, "faile to get is_result_accurate", K(ret));
    } else {
      bool is_result_accurate = plan_ctx->is_result_accurate();
      SQL_LOG(DEBUG, "debug is_result_accurate for session", K(is_result_accurate),
              K(old_is_result_accurate), K(ret));
      if (is_result_accurate != old_is_result_accurate) {
        // FIXME @qianfu 暂时写成update_sys_variable函数，以后再实现一个可以传入ObSysVarClassType并且
        // 和set系统变量语句走同一套逻辑的函数，在这里调用
        if (OB_FAIL(my_session_.update_sys_variable(SYS_VAR_IS_RESULT_ACCURATE, is_result_accurate ? 1 : 0))) {
          LOG_WARN("fail to update result accurate", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObResultSet::store_last_insert_id(ObExecContext &ctx)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlan* physical_plan_ = static_cast<ObPhysicalPlan*>(cache_obj_guard_.get_cache_obj());
  if (ObStmt::is_dml_stmt(stmt_type_)
      && OB_LIKELY(NULL != physical_plan_)
      && OB_LIKELY(!physical_plan_->is_affected_last_insert_id())) {
    //nothing to do
  } else {
    ObPhysicalPlanCtx *plan_ctx = ctx.get_physical_plan_ctx();
    if (OB_ISNULL(plan_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "get plan ctx is NULL", K(plan_ctx));
    } else {
      uint64_t last_insert_id_session = plan_ctx->calc_last_insert_id_session();
      SQL_LOG(DEBUG, "debug last_insert_id for session", K(last_insert_id_session), K(ret));
      SQL_LOG(DEBUG, "debug last_insert_id changed",  K(ret),
              "last_insert_id_changed", plan_ctx->get_last_insert_id_changed());

      if (plan_ctx->get_last_insert_id_changed()) {
        ObObj last_insert_id;
        last_insert_id.set_uint64(last_insert_id_session);
        // FIXME @qianfu 暂时写成update_sys_variable函数，以后再实现一个可以传入ObSysVarClassType并且
        // 和set系统变量语句走同一套逻辑的函数，在这里调用
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
        SQL_LOG(DEBUG, "zixu debug", K(ret),
                "last_insert_id_to_client", get_last_insert_id_to_client());
      }
    }
  }
  return ret;
}
bool ObResultSet::need_rollback(int ret, int errcode, bool is_error_ignored) const
{
  bool bret = false;
  if (OB_SUCCESS != ret //当前close执行语句出错
      || (errcode != OB_SUCCESS && errcode != OB_ITER_END)  //errcode是open阶段特意保存下来的错误
      || is_error_ignored) {  //曾经出错，但是被忽略了；
    bret = true;
  }
  return bret;
}

OB_INLINE int ObResultSet::do_close_plan(int errcode, ObExecContext &ctx)
{
  int ret = common::OB_SUCCESS;
  int pret = OB_SUCCESS;
  int sret = OB_SUCCESS;
  NG_TRACE(close_plan_begin);
  ObPhysicalPlan* physical_plan_ = static_cast<ObPhysicalPlan*>(cache_obj_guard_.get_cache_obj());
  if (OB_LIKELY(NULL != physical_plan_)) {

    // executor_.close要在exec_result_.close之后，因为主线程get_next_row的时候会占着锁，
    // exec_result_.close会释放锁，而executor_.close需要拿写锁来删掉该scheduler。
    // FIXME qianfu.zpf 这里本来应该要调度线程结束才调用exec_result_.close的。调度线程给主线程push完最后一个
    // task结果之后主线程就会往下走，有可能在调度线程结束之前主线程就调用了exec_result_.close，
    // 但是由于目前调度线程给主线程push完最后一个task结果之后, 调度线程不会用到exec_result_.close中释放的变量，
    // 因此目前这样写暂时是没有问题的。
    // 以后要修正这里的逻辑。

    int old_errcode = ctx.get_errcode();
    if (OB_SUCCESS == old_errcode) {
      // record error code generated in open-phase for after stmt trigger
      ctx.set_errcode(errcode);
    }
    if (OB_ISNULL(exec_result_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("exec result is null", K(ret));
    } else if (OB_FAIL(exec_result_->close(ctx))) {
      SQL_LOG(WARN, "fail close main query", K(ret));
    }
    // whether `close` is successful or not, restore ctx.errcode_
    ctx.set_errcode(old_errcode);
    // 通知该plan的所有task删掉对应的中间结果
    int close_ret = OB_SUCCESS;
    ObPhysicalPlanCtx *plan_ctx = NULL;
    if (OB_ISNULL(plan_ctx = get_exec_context().get_physical_plan_ctx())) {
      close_ret = OB_ERR_UNEXPECTED;
      LOG_WARN("physical plan ctx is null");
    } else if (OB_SUCCESS != (close_ret = executor_.close(ctx))) { // executor_.close里面会等到调度线程结束才返回。
      SQL_LOG(WARN, "fail to close executor", K(ret), K(close_ret));
    }

    ObPxAdmission::exit_query_admission(my_session_, get_exec_context(), get_stmt_type(), *get_physical_plan());
    // Finishing direct-insert must be executed after ObPxTargetMgr::release_target()
    if ((OB_SUCCESS == close_ret)
        && (OB_SUCCESS == errcode || OB_ITER_END == errcode)
        && (stmt::T_INSERT == get_stmt_type())
        && (ObTableDirectInsertService::is_direct_insert(*physical_plan_))) {
      // for insert /*+ append */ into select clause
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(ObTableDirectInsertService::finish_direct_insert(ctx, *physical_plan_))) {
        errcode_ = tmp_ret; // record error code
        errcode = tmp_ret;
        LOG_WARN("fail to finish direct insert", KR(tmp_ret));
      }
    }
//    // 必须要在executor_.execute_plan运行之后再调用exec_result_的一系列函数。
//    if (OB_FAIL(exec_result_.close(ctx))) {
//      SQL_LOG(WARN, "fail close main query", K(ret));
//    }
    // 无论如何都reset_op_ctx
    bool err_ignored = false;
    if (OB_ISNULL(plan_ctx)) {
      LOG_ERROR("plan is not NULL, but plan ctx is NULL", K(ret), K(errcode));
    } else {
      err_ignored = plan_ctx->is_error_ignored();
    }
    bool rollback = need_rollback(ret, errcode, err_ignored);
    get_exec_context().set_errcode(errcode);
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
    if (OB_SUCC(ret)) {
      if (physical_plan_->need_record_plan_info()) {
        if (ctx.get_feedback_info().is_valid() &&
            physical_plan_->get_logical_plan().is_valid() &&
            OB_FAIL(physical_plan_->set_feedback_info(ctx))) {
          LOG_WARN("failed to set feedback info", K(ret));
        } else {
          physical_plan_->set_record_plan_info(false);
        }
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
  }

  // 无论如何都reset掉executor_，否则前面调executor_.init的时候可能会报init twice
  executor_.reset();

  NG_TRACE(close_plan_end);
  return ret;
}

int ObResultSet::close(int &client_ret)
{
  int ret = OB_SUCCESS;
  LinkExecCtxGuard link_guard(my_session_, get_exec_context());

  FLTSpanGuard(close);
  int do_close_plan_ret = OB_SUCCESS;
  ObPhysicalPlan* physical_plan_ = static_cast<ObPhysicalPlan*>(cache_obj_guard_.get_cache_obj());
  if (OB_LIKELY(NULL != physical_plan_)) {
    if (OB_FAIL(my_session_.reset_tx_variable_if_remote_trans(
                physical_plan_->get_plan_type()))) {
      LOG_WARN("fail to reset tx_read_only if it is remote trans", K(ret));
    } else {
      my_session_.set_last_plan_id(physical_plan_->get_plan_id());
    }
    // 无论如何必须执行do_close_plan
    if (OB_UNLIKELY(OB_SUCCESS != (do_close_plan_ret = do_close_plan(errcode_,
                                                                     get_exec_context())))) {
      SQL_LOG(WARN, "fail close main query", K(ret), K(do_close_plan_ret));
    }
    if (OB_SUCC(ret)) {
      ret = do_close_plan_ret;
    }
  } else if (NULL != cmd_) {
    ret = OB_SUCCESS; // cmd mode always return true in close phase
  } else {
    // inner sql executor, no plan or cmd. do nothing.
  }

  // open, get_next_row无论是否成功，close总是要调用
  // 下面的逻辑保证对外吐出的是首次出现的错误码。 by xiaochu.yh
  // 保存close之前的错误码，用来判断本次stmt是否需要回滚； by rongxuan.lc
  if (OB_SUCCESS != errcode_ && OB_ITER_END != errcode_) {
    ret = errcode_;
  }
  if (OB_SUCC(ret)) {
    ObPhysicalPlanCtx *plan_ctx = get_exec_context().get_physical_plan_ctx();
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
  if (OB_SUCCESS != ret
      && get_stmt_type() != stmt::T_INSERT
      && get_stmt_type() != stmt::T_REPLACE) {
    // ignore when OB_SUCCESS != ret and stmt like select/update/delete... executed
  } else if (OB_SUCCESS != (ins_ret = store_last_insert_id(get_exec_context()))) {
      SQL_LOG(WARN, "failed to store last_insert_id", K(ret), K(ins_ret));
  }
  if (OB_SUCC(ret)) {
    ret = ins_ret;
  }

  if (OB_SUCC(ret)) {
    if (!get_exec_context().get_das_ctx().is_partition_hit()) {
      my_session_.partition_hit().try_set_bool(false);
    }
  }

  int prev_ret = ret;
  bool async = false; // for debug purpose
  if (OB_TRANS_XA_BRANCH_FAIL == ret) {
    if (my_session_.associated_xa()) {
      //兼容oracle，这里需要重置session状态
      LOG_WARN("branch fail in global transaction", KPC(my_session_.get_tx_desc()));
      ObSqlTransControl::clear_xa_branch(my_session_.get_xid(), my_session_.get_tx_desc());
      my_session_.reset_tx_variable();
      my_session_.disassociate_xa();
    }
  } else if (OB_NOT_NULL(physical_plan_)) {
    //Because of the async close result we need set the partition_hit flag
    //to the call back param, than close the result.
    //But the das framwork set the partition_hit after result is closed.
    //So we need to set the partition info at here.
    if (is_end_trans_async()) {
      ObCurTraceId::TraceId *cur_trace_id = NULL;
      if (OB_ISNULL(cur_trace_id = ObCurTraceId::get_trace_id())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("current trace id is NULL", K(ret));
        set_end_trans_async(false);
      } else {
        observer::ObSqlEndTransCb &sql_end_cb = my_session_.get_mysql_end_trans_cb();
        ObEndTransCbPacketParam pkt_param;
        int fill_ret = OB_SUCCESS;
        fill_ret = sql_end_cb.set_packet_param(pkt_param.fill(*this, my_session_, *cur_trace_id));
        if (OB_SUCCESS != fill_ret) {
          LOG_WARN("fail set packet param", K(ret));
          set_end_trans_async(false);
        }
      }
    }
    ret = auto_end_plan_trans(*physical_plan_, ret, async);
  }

  if (is_user_sql_ && my_session_.need_reset_package()) {
    // need_reset_package is set, it must be reset package, wether exec succ or not.
    int tmp_ret = my_session_.reset_all_package_state_by_dbms_session(true);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("reset all package fail. ", K(tmp_ret), K(ret));
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    }
  }
  // notify close fail to listener
  int err = OB_SUCCESS != do_close_plan_ret ? do_close_plan_ret : ret;
  if (OB_SUCCESS != err && err != errcode_ && close_fail_cb_.is_valid()) {
    close_fail_cb_(err, client_ret);
  }
  //Save the current execution state to determine whether to refresh location
  //and perform other necessary cleanup operations when the statement exits.
  DAS_CTX(get_exec_context()).get_location_router().save_cur_exec_status(ret);
  //NG_TRACE_EXT(result_set_close, OB_ID(ret), ret, OB_ID(arg1), prev_ret,
               //OB_ID(arg2), ins_ret, OB_ID(arg3), errcode_, OB_ID(async), async);
  return ret;  // 后面所有的操作都通过callback来完成
}

// 异步调用end_trans
// 1. 仅仅针对AC=1的phy plan，不针对cmd (除了还没实现的commit/rollback)
// 2. TODO:对于commit/rollback这个cmd，后面也需要走这个路径。现在还是走同步Callback。
OB_INLINE int ObResultSet::auto_end_plan_trans(ObPhysicalPlan& plan,
                                               int ret,
                                               bool &async)
{
  NG_TRACE(auto_end_plan_begin);
  bool in_trans = my_session_.is_in_transaction();
  bool ac = true;
  bool explicit_trans = my_session_.has_explicit_start_trans();
  bool is_rollback = false;
  my_session_.get_autocommit(ac);
  async = false;
  LOG_TRACE("auto_end_plan_trans.start", K(ret),
            K(in_trans), K(ac), K(explicit_trans),
            K(is_async_end_trans_submitted()));
  // explicit start trans will disable auto-commit
  if (!explicit_trans && ac) {
    // Query like `select 1` will keep next scope set transaction xxx valid
    // for example:
    // set session transaction read only;
    // set @@session.autocommit=1;
    // set transaction read write;
    // select 1;
    // insert into t values(1); -- this will be success
    //
    // so, can not reset these transaction variables
    //
    // must always commit/rollback the transactional state in `ObTxDesc`
    // for example:
    // set session transaction isolation level SERIALIZABLE
    // -- UDF with: select count(1) from t1;
    // select UDF1() from dual; -- PL will remain transctional state after run UDF1
    //
    // after execute UDF1, snapshot is kept in ObTxDesc, must cleanup before run
    // other Query
    bool reset_tx_variable = plan.is_need_trans();
    ObPhysicalPlanCtx *plan_ctx = NULL;
    if (OB_ISNULL(plan_ctx = get_exec_context().get_physical_plan_ctx())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("physical plan ctx is null, won't call end trans!!!", K(ret));
    } else {
      //bool is_rollback = (OB_FAIL(ret) || plan_ctx->is_force_rollback());
      is_rollback = need_rollback(OB_SUCCESS, ret, plan_ctx->is_error_ignored());
      // 对于UPDATE等异步提交的语句，如果需要重试，那么中途的rollback也走同步接口
      if (OB_LIKELY(false == is_end_trans_async()) || OB_LIKELY(false == is_user_sql_)) {
        // 如果没有设置end_trans_cb，就走同步接口。这个主要是为了InnerSQL提供的。
        // 因为InnerSQL没有走Obmp_query接口，而是直接操作ResultSet
        int save_ret = ret;
        if (OB_FAIL(ObSqlTransControl::implicit_end_trans(get_exec_context(),
                                                          is_rollback,
                                                          NULL,
                                                          reset_tx_variable))) {
          if (OB_REPLICA_NOT_READABLE != ret) {
              LOG_WARN("sync end trans callback return an error!", K(ret),
                       K(is_rollback), KPC(my_session_.get_tx_desc()));
            }
        }
        ret = OB_SUCCESS != save_ret? save_ret : ret;
        async = false;
      } else {
        // 外部SQL请求，走异步接口
#ifdef OB_BUILD_AUDIT_SECURITY
        {
          // do security audit before async end trans to avoid deadlock of query_lock.
          //
          // don't need to set ret
          ObSqlCtx *sql_ctx = get_exec_context().get_sql_ctx();
          if (OB_ISNULL(sql_ctx)) {
            LOG_WARN("sql_ctx is null when handle security audit");
          } else {
            ObSecurityAuditUtils::handle_security_audit(*this,
                                                        sql_ctx->schema_guard_,
                                                        sql_ctx->cur_stmt_,
                                                        ObString::make_empty_string(),
                                                        ret);
          }
        }
#endif
        int save_ret = ret;
        my_session_.get_end_trans_cb().set_last_error(ret);
        ret = ObSqlTransControl::implicit_end_trans(get_exec_context(),
                                                    is_rollback,
                                                    &my_session_.get_end_trans_cb(),
                                                    reset_tx_variable);
        // NOTE: async callback client will not issued if:
        // 1) it is a rollback, which will succeed immediately
        // 2) the commit submit/starting failed, in this case
        //    the connection will be released
        // so the error code should not override, and return to plan-driver
        // to decide whether a packet should be sent to client
        ret = save_ret == OB_SUCCESS ? ret : save_ret;
        async = true;
      }
      get_trans_state().clear_start_trans_executed();
    }
  }
  NG_TRACE(auto_end_plan_end);
  LOG_TRACE("auto_end_plan_trans.end", K(ret),
            K(in_trans), K(ac), K(explicit_trans), K(plan.is_need_trans()),
            K(is_rollback),  K(async),
            K(is_async_end_trans_submitted()));
  return ret;
}

void ObResultSet::set_statement_name(const common::ObString name)
{
  statement_name_ = name;
}

int ObResultSet::from_plan(const ObPhysicalPlan &phy_plan, const ObIArray<ObPCParam *> &raw_params)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = NULL;
  if (OB_ISNULL(plan_ctx = get_exec_context().get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("physical plan ctx is null or ref handle is invalid",
             K(ret), K(plan_ctx));
  } else if (phy_plan.contain_paramed_column_field()
             && OB_FAIL(copy_field_columns(phy_plan))) {
    // 因为会修改ObField中的cname_，所以这里深拷计划中的field_columns_
    LOG_WARN("failed to copy field columns", K(ret));
  } else if (phy_plan.contain_paramed_column_field()
             && OB_FAIL(construct_field_name(raw_params, false))) {
    LOG_WARN("failed to construct field name", K(ret));
  } else {
    int64_t ps_param_count = plan_ctx->get_orig_question_mark_cnt();
    p_field_columns_ = phy_plan.contain_paramed_column_field()
                                  ? &field_columns_
                                  : &phy_plan.get_field_columns();
    p_returning_param_columns_ = &phy_plan.get_returning_param_fields();
    stmt_type_ = phy_plan.get_stmt_type();
    literal_stmt_type_ = phy_plan.get_literal_stmt_type();
    is_returning_ = phy_plan.is_returning();
    plan_ctx->set_is_affect_found_row(phy_plan.is_affect_found_row());
    if (is_ps_protocol() && ps_param_count != phy_plan.get_param_fields().count()) {
      if (OB_FAIL(reserve_param_columns(ps_param_count))) {
        LOG_WARN("reserve param columns failed", K(ret), K(ps_param_count));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < ps_param_count; ++i) {
        ObField param_field;
        param_field.type_.set_type(ObIntType); // @bug
        param_field.cname_ = ObString::make_string("?");
        OZ (add_param_column(param_field), K(param_field), K(i), K(ps_param_count));
      }
      LOG_DEBUG("reset param count ", K(ps_param_count), K(plan_ctx->get_orig_question_mark_cnt()),
        K(phy_plan.get_returning_param_fields().count()), K(phy_plan.get_param_fields().count()));
    } else {
      p_param_columns_ = &phy_plan.get_param_fields();
    }
  }
  return ret;
}

int ObResultSet::to_plan(const PlanCacheMode mode, ObPhysicalPlan *phy_plan)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(phy_plan)) {
    LOG_ERROR("invalid argument", K(phy_plan));
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (OB_FAIL(phy_plan->set_field_columns(field_columns_))) {
      LOG_WARN("Failed to copy field info to plan", K(ret));
    } else if ((PC_PS_MODE == mode || PC_PL_MODE == mode)
               && OB_FAIL(phy_plan->set_param_fields(param_columns_))) {
      // param fields is only needed ps mode
      LOG_WARN("failed to copy param field to plan", K(ret));
    } else if ((PC_PS_MODE == mode || PC_PL_MODE == mode)
               && OB_FAIL(phy_plan->set_returning_param_fields(returning_param_columns_))) {
      // returning param fields is only needed ps mode
      LOG_WARN("failed to copy returning param field to plan", K(ret));
    }
  }

  return ret;
}

int ObResultSet::get_read_consistency(ObConsistencyLevel &consistency)
{
  consistency = INVALID_CONSISTENCY;
  int ret = OB_SUCCESS;
  ObPhysicalPlan* physical_plan_ = static_cast<ObPhysicalPlan*>(cache_obj_guard_.get_cache_obj());
  if (OB_ISNULL(physical_plan_)
      || OB_ISNULL(exec_ctx_)
      || OB_ISNULL(exec_ctx_->get_sql_ctx())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("physical_plan", K_(physical_plan), K(exec_ctx_->get_sql_ctx()), K(ret));
  } else {
    const ObPhyPlanHint &phy_hint = physical_plan_->get_phy_plan_hint();
    if (stmt::T_SELECT == stmt_type_) { // select才有weak
      if (exec_ctx_->get_sql_ctx()->is_protocol_weak_read_) {
        consistency = WEAK;
      } else if (OB_UNLIKELY(phy_hint.read_consistency_ != INVALID_CONSISTENCY)) {
        consistency = phy_hint.read_consistency_;
      } else {
        consistency = my_session_.get_consistency_level();
      }
    } else {
      consistency = STRONG;
    }
  }
  return ret;
}

int ObResultSet::init_cmd_exec_context(ObExecContext &exec_ctx)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(exec_ctx);
  void *buf = NULL;
  if (OB_ISNULL(cmd_) || OB_ISNULL(plan_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("cmd or ctx is NULL", K(ret), K(cmd_), K(plan_ctx));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(buf = get_mem_pool().alloc(sizeof(ObNewRow)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(sizeof(ObNewRow)), K(ret));
  } else {
    exec_ctx.set_output_row(new(buf)ObNewRow());
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

// obmp_query中重试整个SQL之前，可能需要调用本接口来刷新Location，以避免总是发给了错误的服务器
void ObResultSet::refresh_location_cache_by_errno(bool is_nonblock, int err)
{
  DAS_CTX(get_exec_context()).get_location_router().refresh_location_cache_by_errno(is_nonblock, err);
}

void ObResultSet::force_refresh_location_cache(bool is_nonblock, int err)
{
  DAS_CTX(get_exec_context()).get_location_router().force_refresh_location_cache(is_nonblock, err);
}

// 告诉mysql是否要传入一个EndTransCallback
bool ObResultSet::need_end_trans_callback() const
{
  int ret = OB_SUCCESS;
  bool need = false;
  ObPhysicalPlan* physical_plan_ = static_cast<ObPhysicalPlan*>(cache_obj_guard_.get_cache_obj());
  if (stmt::T_SELECT == get_stmt_type()) {
    // 对于select语句，总不走callback，无论事务状态如何
    need = false;
  } else if (is_returning_) {
    need = false;
  } else if (my_session_.get_has_temp_table_flag()
             || my_session_.has_tx_level_temp_table()) {
    need = false;
  } else if (stmt::T_END_TRANS == get_stmt_type()) {
    need = true;
  } else {
    bool explicit_start_trans = my_session_.has_explicit_start_trans();
    bool ac = true;
    if (OB_FAIL(my_session_.get_autocommit(ac))) {
      LOG_ERROR("fail to get autocommit", K(ret));
    } else {}
    if (stmt::T_ANONYMOUS_BLOCK == get_stmt_type() && is_oracle_mode()) {
      need = ac && !explicit_start_trans && !is_with_rows();
    } else if (OB_LIKELY(NULL != physical_plan_) &&
               OB_LIKELY(physical_plan_->is_need_trans()) &&
               !physical_plan_->is_link_dml_plan()) {
      need = (true == ObSqlTransUtil::plan_can_end_trans(ac, explicit_start_trans)) &&
          (false == ObSqlTransUtil::is_remote_trans(ac, explicit_start_trans, physical_plan_->get_plan_type()));
    }
  }
  return need;
}

int ObResultSet::ExternalRetrieveInfo::build_into_exprs(
        ObStmt &stmt, pl::ObPLBlockNS *ns, bool is_dynamic_sql)
{
  int ret = OB_SUCCESS;
  if (stmt.is_select_stmt()) {
    ObSelectIntoItem *into_item = (static_cast<ObSelectStmt&>(stmt)).get_select_into();
    if (OB_NOT_NULL(into_item)) {
      OZ (into_exprs_.assign(into_item->pl_vars_));
    }
    is_select_for_update_ = (static_cast<ObSelectStmt&>(stmt)).has_for_update();
    has_hidden_rowid_ = (static_cast<ObSelectStmt&>(stmt)).has_hidden_rowid();
  } else if (stmt.is_insert_stmt() || stmt.is_update_stmt() || stmt.is_delete_stmt()) {
    ObDelUpdStmt &dml_stmt = static_cast<ObDelUpdStmt&>(stmt);
    OZ (into_exprs_.assign(dml_stmt.get_returning_into_exprs()));
  }

  return ret;
}

int ObResultSet::ExternalRetrieveInfo::check_into_exprs(ObStmt &stmt,
                                                        ObArray<ObDataType> &basic_types,
                                                        ObBitSet<> &basic_into)
{
  int ret = OB_SUCCESS;
  CK (stmt.is_select_stmt()
      || stmt.is_insert_stmt()
      || stmt.is_update_stmt()
      || stmt.is_delete_stmt());

  if (OB_SUCC(ret)) {
    if (stmt.is_select_stmt()) {
      ObSelectStmt &select_stmt = static_cast<ObSelectStmt&>(stmt);
      if (basic_types.count() != select_stmt.get_select_item_size()
          && into_exprs_.count() != select_stmt.get_select_item_size()) {
        if (basic_types.count() > select_stmt.get_select_item_size()
            || into_exprs_.count() > select_stmt.get_select_item_size()) {
          ret = OB_ERR_TOO_MANY_VALUES;
          LOG_WARN("ORA-00913: too many values",
                   K(ret), K(basic_types.count()), K(select_stmt.get_select_item_size()));
        } else if (basic_types.count() < select_stmt.get_select_item_size()
                   || into_exprs_.count() < select_stmt.get_select_item_size()) {
          ret = OB_ERR_NOT_ENOUGH_VALUES;
          LOG_WARN("not enough values",
                   K(ret), K(basic_types.count()), K(select_stmt.get_select_item_size()));
        }
      }
    } else {
      ObDelUpdStmt &dml_stmt = static_cast<ObDelUpdStmt&>(stmt);
      const ObIArray<ObRawExpr*> &returning_exprs = dml_stmt.get_returning_exprs();
      if (basic_types.count() > returning_exprs.count()) {
        ret = OB_ERR_TOO_MANY_VALUES;
        LOG_WARN("ORA-00913: too many values",
                 K(ret), K(basic_types.count()), K(returning_exprs.count()));
      } else if (basic_types.count() < returning_exprs.count()) {
        ret = OB_ERR_NOT_ENOUGH_VALUES;
        LOG_WARN("not enough values",
                 K(ret), K(basic_types.count()), K(returning_exprs.count()));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < returning_exprs.count(); ++i) {
        if (basic_into.has_member(i)) { // 往基本类型里into，不必检查类型匹配，存储之前做强转
        } else if (OB_ISNULL(returning_exprs.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("returning expr is NULL", K(i), K(ret));
        } else if (returning_exprs.at(i)->get_result_type().get_obj_meta() != basic_types.at(i).get_meta_type()
            || returning_exprs.at(i)->get_result_type().get_accuracy() != basic_types.at(i).get_accuracy()) {
          LOG_DEBUG("select item type and into expr type are not match", K(i), K(returning_exprs.at(i)->get_result_type()), K(basic_types.at(i)), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObResultSet::ExternalRetrieveInfo::recount_dynamic_param_info(
  common::ObIArray<std::pair<ObRawExpr*,ObConstRawExpr*>> &param_info)
{
  int ret = OB_SUCCESS;
  int64_t current_position = INT64_MAX;
  for (int64_t  i = 0; OB_SUCC(ret) && i < into_exprs_.count(); ++i) {
    ObRawExpr *into = into_exprs_.at(i);
    if (OB_NOT_NULL(into)
        && T_QUESTIONMARK == into->get_expr_type()
        && static_cast<ObConstRawExpr *>(into)->get_value().get_unknown() < current_position) {
      current_position = static_cast<ObConstRawExpr *>(into)->get_value().get_unknown();
    }
  }

  // sort
  ObSEArray<ObConstRawExpr *, 4> recount_params;
  for (int64_t i = 0;
       current_position != INT64_MAX && OB_SUCC(ret) && i < param_info.count(); ++i) {
    ObRawExpr *param = param_info.at(i).first;
    if (OB_NOT_NULL(param)
        && T_QUESTIONMARK == param->get_expr_type()
        && static_cast<ObConstRawExpr *>(param)->get_value().get_unknown() > current_position) {
      OZ (recount_params.push_back(static_cast<ObConstRawExpr *>(param)));
    }
  }
  if (OB_SUCC(ret) && recount_params.count() >= 2) {
    std::sort(recount_params.begin(), recount_params.end(),
        [](ObConstRawExpr *a, ObConstRawExpr *b) {
          return a->get_value().get_unknown() < b->get_value().get_unknown();
        });
  }

  for (int64_t i = 0;
      current_position != INT64_MAX && OB_SUCC(ret) && i < recount_params.count(); ++i) {
    recount_params.at(i)->get_value().set_unknown(current_position);
    current_position ++;
  }
  return ret;
}

int ObResultSet::ExternalRetrieveInfo::build(
  ObStmt &stmt,
  ObSQLSessionInfo &session_info,
  pl::ObPLBlockNS *ns,
  bool is_dynamic_sql,
  common::ObIArray<std::pair<ObRawExpr*,ObConstRawExpr*>> &param_info)
{
  int ret = OB_SUCCESS;
  OZ (build_into_exprs(stmt, ns, is_dynamic_sql));
  CK (OB_NOT_NULL(session_info.get_cur_exec_ctx()));
  CK (OB_NOT_NULL(session_info.get_cur_exec_ctx()->get_sql_ctx()));
  OX (is_bulk_ = session_info.get_cur_exec_ctx()->get_sql_ctx()->is_bulk_);
  OX (session_info.get_cur_exec_ctx()->get_sql_ctx()->is_bulk_ = false);
  if (stmt.is_dml_stmt()) {
    OX (has_link_table_ = static_cast<ObDMLStmt&>(stmt).has_link_table());
  }
  if (OB_SUCC(ret)) {
    ObSchemaGetterGuard *schema_guard = NULL;
    if (OB_ISNULL(stmt.get_query_ctx()) ||
        OB_ISNULL(schema_guard = stmt.get_query_ctx()->sql_schema_guard_.get_schema_guard())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("query_ctx is null", K(ret));
    } else if (param_info.empty() && into_exprs_.empty()) {
      if (stmt.is_dml_stmt()) {
        OZ (ObSQLUtils::reconstruct_sql(allocator_, &stmt, stmt.get_query_ctx()->get_sql_stmt(),
                                        schema_guard, session_info.create_obj_print_params()));
      } else {
        // other stmt do not need reconstruct.
      }
    } else {
      if (is_dynamic_sql && !into_exprs_.empty()) {
        OZ (recount_dynamic_param_info(param_info));
      }
      OZ (ObSQLUtils::reconstruct_sql(allocator_, &stmt, stmt.get_query_ctx()->get_sql_stmt(),
                                      schema_guard, session_info.create_obj_print_params()));
      /*
       * stmt里的？是按照表达式resolve的顺序编号的，这个顺序在prepare阶段需要依赖的
       * 但是route_sql里的？需要按照入参在符号表里的下标进行编号，这个编号是proxy做路由的时候依赖的，所以这里要改掉stmt里QUESTIONMARK的值
       */
      external_params_.set_capacity(param_info.count());
      for (int64_t i = 0; OB_SUCC(ret) && i < param_info.count(); ++i) {
        if (OB_ISNULL(param_info.at(i).first) || OB_ISNULL(param_info.at(i).second)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param expr is NULL", K(i), K(param_info.at(i).first), K(param_info.at(i).second), K(ret));
        } else if (OB_FAIL(external_params_.push_back(param_info.at(i).first))) {
          LOG_WARN("push back error", K(i), K(param_info.at(i).first), K(ret));
        } else if (T_QUESTIONMARK == param_info.at(i).first->get_expr_type()) {
          ObConstRawExpr *const_expr = static_cast<ObConstRawExpr *>(param_info.at(i).first);
          if (OB_FAIL(param_info.at(i).second->get_value().apply(const_expr->get_value()))) {
            LOG_WARN("apply error", K(const_expr->get_value()), K(param_info.at(i).second->get_value()), K(ret));
          }
        } else {
          //如果不是PL的local变量，需要把QUESTIONMARK的值改为无效值，以免使proxy混淆
          param_info.at(i).second->get_value().set_unknown(OB_INVALID_INDEX);
          param_info.at(i).second->set_expr_obj_meta(
                              param_info.at(i).second->get_value().get_meta());
          if (stmt_sql_.empty()) {
            OZ (ob_write_string(allocator_, stmt.get_query_ctx()->get_sql_stmt(), stmt_sql_));
          }
        }
      }
      OZ (ObSQLUtils::reconstruct_sql(allocator_, &stmt, route_sql_,
                                      schema_guard, session_info.create_obj_print_params()));
    }
  }
  LOG_INFO("reconstruct sql:", K(stmt.get_query_ctx()->get_prepare_param_count()),
                               K(stmt.get_query_ctx()->get_sql_stmt()), K(route_sql_), K(ret));
  return ret;
}

int ObResultSet::drive_dml_query()
{
  // DML使用PX执行框架，在非returning情况下，需要主动
  // 调用get_next_row才能驱动整个框架的执行
  int ret = OB_SUCCESS;
  if (get_physical_plan()->is_returning()
      || (get_physical_plan()->is_local_or_remote_plan() && !get_physical_plan()->need_drive_dml_query_)) {
    //1. dml returning will drive dml write through result.get_next_row()
    //2. partial dml query will drive dml write through operator open
    //3. partial dml query need to drive dml write through result.drive_dml_query,
    //use the flag result.drive_dml_query to distinguish situation 2,3
  } else if (get_physical_plan()->need_drive_dml_query_) {
    const ObNewRow *row = nullptr;
    if (OB_LIKELY(OB_ITER_END == (ret = inner_get_next_row(row)))) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("do dml query failed", K(ret));
    }
  } else if (get_physical_plan()->is_use_px() &&
             !get_physical_plan()->is_use_pdml()) {
    // DML + PX情况下，需要调用get_next_row 来驱动PX框架
    stmt::StmtType type = get_physical_plan()->get_stmt_type();
    if (type == stmt::T_INSERT ||
        type == stmt::T_DELETE ||
        type == stmt::T_UPDATE ||
        type == stmt::T_REPLACE ||
        type == stmt::T_MERGE) {
      const ObNewRow *row = nullptr;
      // 非returning类型的PX+DML的驱动需要主动调用 get_next_row
      if (OB_LIKELY(OB_ITER_END == (ret = inner_get_next_row(row)))) {
        // 外层调用已经处理了对应的affected rows
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("do dml query failed", K(ret));
      }
    }
  } else if (get_physical_plan()->is_use_pdml()) {
    const ObNewRow *row = nullptr;
    // pdml+非returning情况下，需要调用get_next_row来驱动PX框架
    if (OB_LIKELY(OB_ITER_END == (ret = inner_get_next_row(row)))) {
      // 外层调用已经处理了对应的affected rows
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("do pdml query failed", K(ret));
    }
  }

  return ret;
}

int ObResultSet::copy_field_columns(const ObPhysicalPlan &plan)
{
  int ret = OB_SUCCESS;
  field_columns_.reset();

  int64_t N = plan.get_field_columns().count();
  ObField field;
  if (N > 0 && OB_FAIL(field_columns_.reserve(N))) {
    LOG_WARN("failed to reserve field column array", K(ret), K(N));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
    const ObField &ofield = plan.get_field_columns().at(i);
    if (OB_FAIL(field.deep_copy(ofield, &get_mem_pool()))) {
      LOG_WARN("deep copy field failed", K(ret));
    } else if (OB_FAIL(field_columns_.push_back(field))) {
      LOG_WARN("push back field column failed", K(ret));
    } else {
      LOG_DEBUG("success to copy field", K(field));
    }
  }
  return ret;
}

int ObResultSet::construct_field_name(const common::ObIArray<ObPCParam *> &raw_params,
                                      const bool is_first_parse)
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

int ObResultSet::construct_display_field_name(common::ObField &field,
                                              const ObIArray<ObPCParam *> &raw_params,
                                              const bool is_first_parse)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  // mysql模式下，select '\t\t\t\t aaa'会去掉头部的转义字符以及空格，最后的长度不能大于MAX_COLUMN_CHAR_LENGTH
  // 所以这里多给一些buffer来处理这种情况
  // 如果转义字符多了还是会有问题
  int32_t buf_len = MAX_COLUMN_CHAR_LENGTH * 2;
  int32_t pos = 0;
  int32_t name_pos = 0;

  if (!field.is_paramed_select_item_ || NULL == field.paramed_ctx_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(field.is_paramed_select_item_), K(field.paramed_ctx_));
  } else if (0 == field.paramed_ctx_->paramed_cname_.length()) {
    // 1. 参数化的cname长度为0，说明指定了列名
    // 2. 指定了别名，别名存在cname_里，直接使用即可
    // do nothing
  } else if (OB_ISNULL(buf = static_cast<char *>(get_mem_pool().alloc(buf_len)))) {
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
        name_pos = (int32_t)PARAM_CTX->param_str_offsets_.at(i) + 1; // skip '?'
        pos += len;

        if (is_first_parse && PARAM_CTX->neg_param_idxs_.has_member(idx) && pos < buf_len) {
          buf[pos++] = '-'; // insert `-` for negative value
        }
        int64_t copy_str_len = 0;
        const char *copy_str = NULL;

        // 1.
        // 如果有词法分析得到常量节点有is_copy_raw_text_标记，则是有前缀的常量，比如
        // select date'2012-12-12' from dual, column显示为date'2012-12-12',
        // 词法分析得到的常量节点的raw_text为date'2012-12-12'，
        // 所以直接拷贝raw_text，在oracle模式下需要还需要去空格以及转换大小写
        // 2.
        // 如果esc_str_flag_被标记，那么投影列是是常量字符串，其内部字符串需要转义，
        // 常量节点的str_value是转义好的字符串，直接使用即可，
        // 但是在mysql模式下，字符串开头的某些转义字符不会显示，ObResultSet::make_final_field_name会处理
        // oracle模式下，需要在加上单引号
        // 比如MySQL模式：select '\'hello' from dual,列名显示'hello
        //                select '\thello' from dual,列名显示hello (去掉了左边的转义字符)
        // Oracle模式：select 'hello' from dual, 列名显示 'hello', (带引号)
        //             select '''hello' from dual, 列名显示 ''hello'
        // 3.
        // mysql模式下, 有以下对于以下sql：
        // select 'a' 'abc' from dual
        // 行为是a作为column name，’abc‘作为值，但是参数化后的sql为select ? from dual
        // 在词法节点两个字符串被concat，整体作为一个varchar节点，但是该节点有一个T_CONCAT_STRING子节点，节点的str_val保存的是column name
        // 所以这种情况下，直接去T_CONCAT_STRING的str_val作为列名
        if (1 == raw_params.at(idx)->node_->is_copy_raw_text_) {
          copy_str_len = raw_params.at(idx)->node_->text_len_;
          copy_str = raw_params.at(idx)->node_->raw_text_;
        } else if (PARAM_CTX->esc_str_flag_) {
          if (lib::is_mysql_mode()
              && 1 == raw_params.at(idx)->node_->num_child_) {
            LOG_DEBUG("concat str node");
            if (OB_ISNULL(raw_params.at(idx)->node_->children_)
                || OB_ISNULL(raw_params.at(idx)->node_->children_[0])
                || T_CONCAT_STRING != raw_params.at(idx)->node_->children_[0]->type_) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invalid argument", K(ret));
            } else {
              copy_str_len = raw_params.at(idx)->node_->children_[0]->str_len_;
              copy_str = raw_params.at(idx)->node_->children_[0]->str_value_;
            }
          } else {
            copy_str_len = raw_params.at(idx)->node_->str_len_;
            copy_str = raw_params.at(idx)->node_->str_value_;
            if (lib::is_oracle_mode() && pos < buf_len) {
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

          if (len > 0
              && 1 == raw_params.at(idx)->node_->is_copy_raw_text_
              && lib::is_oracle_mode()) {
            // 如果直接拷贝了raw_text_，可能需要转换大小写和去空格
            ObCollationType col_type = static_cast<ObCollationType>(field.charsetnr_);
            len = (int32_t)remove_extra_space(buf + pos, len);
            ObString str(len, (const char *)buf+pos);
            ObString dest;
            if (OB_FAIL(ObCharset::caseup(col_type, str, dest, get_mem_pool()))) {
              LOG_WARN("failed to do charset caseup", K(ret));
            } else {
              len = dest.length();
              MEMCPY(buf+pos, dest.ptr(), len);
            }
            get_mem_pool().free(dest.ptr());
            dest.reset();
            str.reset();
          }
          pos += len;

          if (PARAM_CTX->esc_str_flag_ && lib::is_oracle_mode() && pos < buf_len) {
            buf[pos++] = '\'';
          }
        }
      }
    } // for end

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (name_pos < PARAM_CTX->paramed_cname_.length()) { // 如果最后一个常量后面还有字符串
      int32_t len = std::min((int32_t)PARAM_CTX->paramed_cname_.length() - name_pos, buf_len - pos);
      if (len > 0) {
        MEMCPY(buf + pos, PARAM_CTX->paramed_cname_.ptr() + name_pos, len);
      }
      pos += len;
    }

    if (OB_FAIL(ret) || 0 == PARAM_CTX->param_idxs_.count()) { // 如果没有任何参数化信息，不做任何处理
      // do nothing
    } else if (lib::is_oracle_mode()) {
      // 如果oracle模式下cp_str_value_flag_为true，说明有alias name，直接使用即可(空格也不用去)
      // 否则，oracle模式下需要转换大写,去掉多余的空格
      ObCollationType col_type = static_cast<ObCollationType>(field.charsetnr_);
      pos = (int32_t)remove_extra_space(buf, pos);
      ObString str(pos, (const char *)buf);
      ObString dest;
      if (OB_FAIL(ObCharset::caseup(col_type, str, dest, get_mem_pool()))) {
        LOG_WARN("failed to do charset caseup", K(ret));
      } else {
        pos = dest.length();
        MEMCPY(buf, dest.ptr(), pos);
      }
      get_mem_pool().free(dest.ptr());
      dest.reset();
      str.reset();
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (lib::is_mysql_mode() && OB_FAIL(make_final_field_name(buf, pos, field.cname_))) {
      LOG_WARN("failed to make final field name", K(ret));
    } else if (lib::is_oracle_mode()) {
      pos = pos < MAX_COLUMN_CHAR_LENGTH ? pos : MAX_COLUMN_CHAR_LENGTH;
      //field.cname_.assign(buf, pos);
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

int64_t ObResultSet::remove_extra_space(char *buff, int64_t len)
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

void ObResultSet::replace_lob_type(const ObSQLSessionInfo &session,
                                  const ObField &field,
                                  obmysql::ObMySQLField &mfield)
{
  bool is_use_lob_locator = session.is_client_use_lob_locator();
  if (lib::is_oracle_mode()) {
    // 如果客户端不用lob locator, 而server返回了lob locator数据类型
    // 则在field中改为老的方式的type:MYSQL_TYPE_LONG_BLOB;
    // 如果客户端使用lob locator, 则根据collation, 判断是clob还是blob;
    // 然后返回blob/clob的type
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
  } else {
    if (mfield.type_ == obmysql::EMySQLFieldType::MYSQL_TYPE_TINY_BLOB ||
        mfield.type_ == obmysql::EMySQLFieldType::MYSQL_TYPE_MEDIUM_BLOB ||
        mfield.type_ == obmysql::EMySQLFieldType::MYSQL_TYPE_LONG_BLOB) {
      // compat mysql-jdbc
      // for 5.x, always return MYSQL_TYPE_BLOB
      // for 8.x always return MYSQL_TYPE_BLOB, and do text type judge in mysql-jdbc by length
      mfield.type_ = obmysql::EMySQLFieldType::MYSQL_TYPE_BLOB;
    }
  }
}

int ObResultSet::make_final_field_name(char *buf, int64_t len, common::ObString &field_name)
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

int ObResultSet::switch_implicit_cursor(int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  affected_rows = 0;
  ObPhysicalPlanCtx *plan_ctx = get_exec_context().get_physical_plan_ctx();
  if (OB_ISNULL(plan_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plan_ctx is null", K(ret));
  } else if (OB_FAIL(plan_ctx->switch_implicit_cursor())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("cursor_idx is invalid", K(ret));
    }
  } else {
    affected_rows = plan_ctx->get_affected_rows();
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
  ObPhysicalPlanCtx *plan_ctx = get_exec_context().get_physical_plan_ctx();
  if (plan_ctx != nullptr) {
    bret = (plan_ctx->get_cur_stmt_id() >= plan_ctx->get_implicit_cursor_infos().count());
  }
  return bret;
}

ObRemoteResultSet::ObRemoteResultSet(common::ObIAllocator &allocator)
    : mem_pool_(allocator), remote_resp_handler_(NULL), field_columns_(),
      scanner_(NULL),
      scanner_iter_(),
      all_data_empty_(false),
      cur_data_empty_(true),
      first_response_received_(false),
      found_rows_(0),
      stmt_type_(stmt::T_NONE)
{}

ObRemoteResultSet::~ObRemoteResultSet()
{
  if (NULL != remote_resp_handler_) {
    remote_resp_handler_->~ObInnerSqlRpcStreamHandle();
    remote_resp_handler_ = NULL;
  }
  mem_pool_.reset();
}

int ObRemoteResultSet::reset_and_init_remote_resp_handler()
{
  int ret = OB_SUCCESS;

  if (NULL != remote_resp_handler_) {
    remote_resp_handler_->~ObInnerSqlRpcStreamHandle();
    remote_resp_handler_ = NULL;
  }
  ObInnerSqlRpcStreamHandle *buffer = NULL;
  if (OB_ISNULL(buffer = static_cast<ObInnerSqlRpcStreamHandle*>(
                mem_pool_.alloc(sizeof(ObInnerSqlRpcStreamHandle))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for ObInnerSqlRpcStreamHandle", K(ret));
  } else {
    remote_resp_handler_ = new (buffer) ObInnerSqlRpcStreamHandle();
  }

  return ret;
}

int ObRemoteResultSet::copy_field_columns(
    const common::ObSArray<common::ObField> &src_field_columns)
{
  int ret = OB_SUCCESS;
  field_columns_.reset();

  int64_t N = src_field_columns.count();
  ObField field;
  if (N > 0 && OB_FAIL(field_columns_.reserve(N))) {
    LOG_WARN("failed to reserve field column array", K(ret), K(N));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
    const ObField &obfield = src_field_columns.at(i);
    if (OB_FAIL(field.deep_copy(obfield, &get_mem_pool()))) {
      LOG_WARN("deep copy field failed", K(ret));
    } else if (OB_FAIL(field_columns_.push_back(field))) {
      LOG_WARN("push back field column failed", K(ret));
    } else {
      LOG_DEBUG("succs to copy field", K(field));
    }
  }

  return ret;
}

int ObRemoteResultSet::setup_next_scanner()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(remote_resp_handler_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("resp_handler is NULL", K(ret));
  } else {
    ObInnerSQLTransmitResult *transmit_result= NULL;

    if (!first_response_received_) { /* has not gotten the first scanner response */
      if (OB_ISNULL(transmit_result = remote_resp_handler_->get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("transmit_result is NULL", K(ret));
      } else if (OB_FAIL(transmit_result->get_err_code())) {
        LOG_WARN("while fetching first scanner, the remote rcode is not OB_SUCCESS", K(ret));
      } else {
        scanner_ = &transmit_result->get_scanner();
        scanner_iter_ = scanner_->begin();
        first_response_received_ = true; /* has gotten the first scanner response already */
        found_rows_ += scanner_->get_found_rows();
        stmt_type_ = transmit_result->get_stmt_type();
        const common::ObSArray<common::ObField> &src_field_columns =
              transmit_result->get_field_columns();
        if (0 >= src_field_columns.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("the count of field_columns is unexpected",
                   K(ret), K(src_field_columns.count()));
        } else if (OB_FAIL(copy_field_columns(src_field_columns))) {
          LOG_WARN("copy_field_columns failed", K(ret), K(src_field_columns.count()));
        } else {
          const int64_t column_count = field_columns_.count();
          if (column_count <= 0) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("column_count is invalid", K(column_count));
          }
        }
      }
    } else { /* successor request will use handle to send get_more to the resource observer */
      ObInnerSqlRpcStreamHandle::InnerSQLSSHandle &handle = remote_resp_handler_->get_handle();
      if (handle.has_more()) {
        if (OB_FAIL(remote_resp_handler_->reset_and_init_scanner())) {
          LOG_WARN("fail reset and init result", K(ret));
        } else if (OB_ISNULL(transmit_result = remote_resp_handler_->get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("succ to alloc result, but result scanner is NULL", K(ret));
        } else if (OB_FAIL(handle.get_more(*transmit_result))) {
          LOG_WARN("fail wait response", K(ret));
        } else {
          scanner_ = &transmit_result->get_scanner();
          scanner_iter_ = scanner_->begin();
          found_rows_ += scanner_->get_found_rows();
        }
      } else {
        ret = OB_ITER_END;
        LOG_DEBUG("no more scanners in the handle", K(ret));
      }
    }
  }

  return ret;
}

int ObRemoteResultSet::get_next_row_from_cur_scanner(const common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObNewRow *new_row = NULL;

  if (OB_FAIL(scanner_iter_.get_next_row(new_row, NULL))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail get next row", K(ret));
    }
  } else {
    row = new_row;
  }

  return ret;
}

int ObRemoteResultSet::get_next_row(const common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  bool has_got_a_row = false;

  while (OB_SUCC(ret) && false == has_got_a_row) {
    if (all_data_empty_) { // has no more data
      ret = OB_ITER_END;
    } else if (cur_data_empty_) { // current scanner has no more data
      if (OB_FAIL(setup_next_scanner())) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to setup next scanner", K(ret));
        } else {
          all_data_empty_ = true;
        }
      } else {
        cur_data_empty_ = false;
      }
    } else { // current scanner has new rows
      if (OB_FAIL(get_next_row_from_cur_scanner(row))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to get next row from cur scanner", K(ret));
        } else {
          cur_data_empty_ = true;
          ret = OB_SUCCESS;
        }
      } else {
        has_got_a_row = true; // get one row and break
      }
    }
  }

  return ret;
}

int ObRemoteResultSet::close()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(remote_resp_handler_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("resp_handler is NULL", K(ret));
  } else {
    ObInnerSqlRpcStreamHandle::InnerSQLSSHandle &handle = remote_resp_handler_->get_handle();
    if (handle.has_more()) {
      if (OB_FAIL(handle.abort())) {
        LOG_WARN("fail to abort", K(ret));
      }
    }
  }

  return ret;
}
