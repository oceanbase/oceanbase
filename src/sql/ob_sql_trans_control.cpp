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

#include "common/ob_clock_generator.h"
#include "lib/ob_errno.h"
#include "share/rc/ob_tenant_base.h"
#define USING_LOG_PREFIX SQL_EXE

#include "share/ob_schema_status_proxy.h"   // ObSchemaStatusProxy
#include "share/schema/ob_tenant_schema_service.h"
#include "sql/ob_sql_trans_control.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/parser/parse_malloc.h"
#include "sql/resolver/ob_stmt.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/ob_sql_trans_util.h"
#include "sql/ob_end_trans_callback.h"
#include "lib/oblog/ob_trace_log.h"
#include "storage/tx/ob_trans_service.h"
#include "storage/tx/ob_xa_service.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/tablelock/ob_table_lock_service.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/executor/ob_task_spliter.h"
#include "lib/profile/ob_perf_event.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_server.h"
#include "storage/tx/wrs/ob_weak_read_util.h"        //ObWeakReadUtil
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/ls/ob_ls_get_mod.h"
#include "storage/tablet/ob_tablet.h"
#include "sql/das/ob_das_dml_ctx_define.h"
#include "share/deadlock/ob_deadlock_detector_mgr.h"
#include "sql/engine/cmd/ob_table_direct_insert_ctx.h"
#include "storage/memtable/ob_lock_wait_mgr.h"

#ifdef CHECK_SESSION
#error "redefine macro CHECK_SESSION"
#else
#define CHECK_SESSION(session) \
  if (OB_SUCC(ret) && session->is_zombie()) {                   \
    ret = OB_ERR_SESSION_INTERRUPTED;                           \
    LOG_WARN("session has been killed", KR(ret), KPC(session)); \
  }
#endif
#define CHECK_TX_FREE_ROUTE(exec_ctx, session, ...)                     \
  if (OB_SUCC(ret) && session->is_txn_free_route_temp()) {              \
    __VA_ARGS__;                                                        \
    ret = OB_ERR_UNEXPECTED;                                            \
    exec_ctx.set_need_disconnect(true);                                 \
    TRANS_LOG(ERROR, "trans act on txn temporary node", KR(ret),        \
              K(session->get_txn_free_route_ctx()),                     \
              K(session->get_tx_id()), KPC(session));                   \
    if (session->get_tx_desc()) {                                       \
      session->get_tx_desc()->dump_and_print_trace();                   \
    }                                                                   \
  }

namespace oceanbase
{
using namespace common;
using namespace transaction;
using namespace share;
using namespace share::schema;
using namespace share::detector;
namespace sql
{
static int get_tx_service(ObBasicSessionInfo *session,
                          ObTransService *&txs)
{
  int ret = OB_SUCCESS;
  uint64_t effective_tenant_id = session->get_effective_tenant_id();
  if (OB_NOT_NULL(session->get_tx_desc())) {
    uint64_t tx_tenant_id = session->get_tx_desc()->get_tenant_id();
    if (effective_tenant_id != tx_tenant_id) {
      ret = OB_TENANT_ID_NOT_MATCH;
      LOG_ERROR("effective_tenant_id not equals to tx_tenant_id", K(ret), K(effective_tenant_id), K(tx_tenant_id), KPC(session));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(txs = MTL_WITH_CHECK_TENANT(ObTransService*, effective_tenant_id))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("get_tx_service", K(ret), K(effective_tenant_id), K(MTL_ID()));
    }
  }
  return ret;
}

static inline int get_lock_service(uint64_t tenant_id, tablelock::ObTableLockService *&lock_service)
{
  int ret = OB_SUCCESS;
  lock_service = MTL_WITH_CHECK_TENANT(tablelock::ObTableLockService*, tenant_id);
  if (OB_ISNULL(lock_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get_lock_service", K(ret), K(tenant_id), K(MTL_ID()));
  }
  return ret;
}

static int get_org_cluster_id_(ObSQLSessionInfo*, int64_t &);
static inline int build_tx_param_(ObSQLSessionInfo *session, ObTxParam &p, const bool *readonly = nullptr)
{
  int ret = OB_SUCCESS;
  int64_t org_cluster_id = OB_INVALID_ORG_CLUSTER_ID;
  OZ (get_org_cluster_id_(session, org_cluster_id));
  int64_t tx_timeout_us = 0;
  session->get_tx_timeout(tx_timeout_us);

  p.timeout_us_ = tx_timeout_us;
  p.lock_timeout_us_ = session->get_trx_lock_timeout();
  bool ro = OB_NOT_NULL(readonly) ? *readonly : session->get_tx_read_only();
  p.access_mode_ = ro ? ObTxAccessMode::RD_ONLY : ObTxAccessMode::RW;
  p.isolation_ = session->get_tx_isolation();
  p.cluster_id_ = org_cluster_id;

  return ret;
}

int ObSqlTransControl::create_stash_savepoint(ObExecContext &ctx, const ObString &name)
{
  int ret = OB_SUCCESS;
  ObTransService *txs = NULL;
  ObSQLSessionInfo *session = GET_MY_SESSION(ctx);
  CK (OB_NOT_NULL(session));
  OZ (get_tx_service(session, txs));
  OZ (acquire_tx_if_need_(txs, *session));
  OZ (txs->create_stash_savepoint(*session->get_tx_desc(), name));
  return ret;
}

int ObSqlTransControl::explicit_start_trans(ObExecContext &ctx, const bool read_only, const ObString hint)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(ctx);
  ObSQLSessionInfo *session = GET_MY_SESSION(ctx);
  ObTransService *txs = NULL;
  uint64_t tenant_id = 0;
  ObTransID tx_id;
  bool cleanup = true;

  CK (OB_NOT_NULL(plan_ctx), OB_NOT_NULL(session));
  CHECK_SESSION(session);
  CHECK_TX_FREE_ROUTE(ctx, session, cleanup = false);
  if (OB_SUCC(ret) && session->is_in_transaction()) {
    ret = OB_ERR_UNEXPECTED;
    cleanup = false;
    LOG_ERROR("nested start transaction not allowed", KR(ret), K(ctx));
  }
  OX (tenant_id = session->get_effective_tenant_id());
  OZ (get_tx_service(session, txs), tenant_id);

  if (OB_SUCC(ret) && OB_NOT_NULL(session->get_tx_desc())) {
    ObSQLSessionInfo::LockGuard data_lock_guard(session->get_thread_data_lock());
    ObTxDesc *tx_desc = session->get_tx_desc();
    if (tx_desc->get_tenant_id() != tenant_id) {
      LOG_ERROR("switch tenant but hold tx_desc", K(tenant_id), KPC(tx_desc));
    }
    txs->release_tx(*tx_desc);
    session->get_tx_desc() = NULL;
  }

  ObTxParam &tx_param = plan_ctx->get_trans_param();
  OZ (build_tx_param_(session, tx_param, &read_only));
  OZ (txs->acquire_tx(session->get_tx_desc(), session->get_sessid(), session->get_data_version()));
  OZ (txs->start_tx(*session->get_tx_desc(), tx_param), tx_param);
  OX (tx_id = session->get_tx_desc()->get_tx_id());

  if (OB_FAIL(ret) && cleanup && OB_NOT_NULL(txs) && OB_NOT_NULL(session->get_tx_desc())) {
    ObSQLSessionInfo::LockGuard data_lock_guard(session->get_thread_data_lock());
    txs->release_tx(*session->get_tx_desc());
    session->get_tx_desc() = NULL;
  }
  OX (session->get_raw_audit_record().trans_id_ = session->get_tx_id());
  OX (session->get_raw_audit_record().seq_num_ = ObSequence::get_max_seq_no());
  NG_TRACE_EXT(start_trans, OB_ID(ret), ret,
               OB_ID(trans_id), tx_id.get_id(),
               OB_ID(timeout), tx_param.timeout_us_,
               OB_ID(start_time), session ? session->get_query_start_time() : 0);

  if (hint.length()) {
    LOG_INFO("explicit start trans with hint", "trans_id", tx_id,
             K(ret), K(hint), K(read_only), "session_id", (session ? session->get_sessid() : 0));
  }
#ifndef NDEBUG
  LOG_INFO("start_trans", K(ret), K(tx_id), KPC(session), K(read_only), K(ctx.get_execution_id()));
#endif
  return ret;
}

int ObSqlTransControl::implicit_end_trans(ObExecContext &exec_ctx,
                                          const bool is_rollback,
                                          ObEndTransAsyncCallback *callback,
                                          bool reset_trans_variable)
{
  int ret = OB_SUCCESS;
#ifndef NDEBUG
  LOG_INFO("implicit end trans", K(is_rollback), K(exec_ctx.get_execution_id()), KP(callback));
#endif
  ObSQLSessionInfo *session = GET_MY_SESSION(exec_ctx);
  CK (OB_NOT_NULL(session));
  if (OB_SUCCESS != ret) {
    // do nothing
  } else if (!session->is_inner() && session->associated_xa()) {
    // NOTE that not support dblink trans in this interface
    // PLEASE handle implicit cases for dblink trans instead of this interface
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("executing do end trans in xa", K(ret), K(session->get_xid()));
  }
  int64_t tx_id = 0;
  OX (tx_id = session->get_tx_id().get_id());
  CHECK_TX_FREE_ROUTE(exec_ctx, session);
  FLTSpanGuard(end_transaction);
  OZ(end_trans(exec_ctx, is_rollback, false, callback, reset_trans_variable));
  FLT_SET_TAG(trans_id, tx_id);
  return ret;
}

int ObSqlTransControl::explicit_end_trans(ObExecContext &exec_ctx, const bool is_rollback, const ObString hint)
{
  int ret = OB_SUCCESS;
#ifndef NDEBUG
  LOG_INFO("explicit end trans", K(is_rollback), K(exec_ctx.get_execution_id()));
#endif
  FLTSpanGuard(end_transaction);
  ObTransID txn_id;
  ObEndTransAsyncCallback *callback = NULL;
  ObSQLSessionInfo *session = GET_MY_SESSION(exec_ctx);
  CK (OB_NOT_NULL(session));
  if (OB_SUCC(ret) && session->get_tx_desc()) {
    txn_id = session->get_tx_desc()->tid();
  }
  CHECK_TX_FREE_ROUTE(exec_ctx, session);
  if (exec_ctx.is_end_trans_async()) {
    CK (OB_NOT_NULL(callback = &session->get_end_trans_cb()));
  }
  OZ (end_trans(exec_ctx, is_rollback, true, callback));
  OX (session->get_raw_audit_record().seq_num_ = ObSequence::get_max_seq_no());
  FLT_SET_TAG(trans_id, txn_id.get_id());
  if (hint.length()) {
    LOG_INFO("explicit end trans with hint",
             "trans_id", txn_id, "action", (is_rollback ? "ROLLBACK" : "COMMIT"),
             K(ret), K(hint), "session_id", session->get_sessid());
  }
  return ret;
}

int ObSqlTransControl::end_trans(ObExecContext &exec_ctx,
                                 const bool is_rollback,
                                 const bool is_explicit,
                                 ObEndTransAsyncCallback *callback,
                                 bool reset_trans_variable)
{
  int ret = OB_SUCCESS;
  bool sync = false;
  DISABLE_SQL_MEMLEAK_GUARD;
  ObSQLSessionInfo *session = GET_MY_SESSION(exec_ctx);
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(exec_ctx);
#ifndef NDEBUG
  LOG_INFO("end_trans", K(session->is_in_transaction()),
                        K(session->has_explicit_start_trans()),
                        K(exec_ctx.get_execution_id()),
                        KPC(session->get_tx_desc()),
                        KP(callback));
#endif
  if (OB_ISNULL(session) || OB_ISNULL(plan_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(ret), KPC(session), KP(plan_ctx));
  } else if (OB_NOT_NULL(callback)) {
      callback->set_is_need_rollback(is_rollback);
      callback->set_end_trans_type(is_explicit ?
                                   ObExclusiveEndTransCallback::END_TRANS_TYPE_EXPLICIT :
                                   ObExclusiveEndTransCallback::END_TRANS_TYPE_IMPLICIT);
  }

  if (OB_FAIL(ret)) {
  } else if (!session->is_in_transaction()) {
    if (!is_rollback && OB_NOT_NULL(callback)) {
      if (OB_FAIL(inc_session_ref(session))) {
        LOG_WARN("fail to inc session ref", K(ret));
      } else {
        callback->handout();
      }
      callback->callback(OB_SUCCESS);
    } else {
      reset_session_tx_state(session, true, reset_trans_variable);
      exec_ctx.set_need_disconnect(false);
    }
  } else {
    // add tx id to AuditRecord
    session->get_raw_audit_record().trans_id_ = session->get_tx_id();
    int64_t expire_ts = get_stmt_expire_ts(plan_ctx, *session);
    if (OB_FAIL(do_end_trans_(session,
                              is_rollback,
                              is_explicit,
                              expire_ts,
                              callback))) {
    }
    bool need_disconnect = false;
    ObSQLUtils::check_if_need_disconnect_after_end_trans(ret,
                                                         is_rollback,
                                                         is_explicit,
                                                         need_disconnect);
    exec_ctx.set_need_disconnect(need_disconnect);
    if (is_rollback || OB_FAIL(ret) || !callback) {
      bool reuse_tx = OB_SUCCESS == ret
        || OB_TRANS_COMMITED == ret
        || OB_TRANS_ROLLBACKED == ret;
      reset_session_tx_state(session, reuse_tx, reset_trans_variable);
    }
  }
  if (callback && !is_rollback) {
    exec_ctx.get_trans_state().set_end_trans_executed(OB_SUCC(ret));
  }
  return ret;
}

int ObSqlTransControl::kill_query_session(ObSQLSessionInfo &session,
                                          const ObSQLSessionState &status)
{
  int ret = OB_SUCCESS;
  if (session.get_in_transaction()) {
    ObTxDesc *tx_desc = session.get_tx_desc();
    uint64_t tx_tenant_id = tx_desc->get_tenant_id();
    MTL_SWITCH(tx_tenant_id) {
      ObTransService *txs = NULL;
      CK(OB_NOT_NULL(txs = MTL_WITH_CHECK_TENANT(ObTransService*, tx_tenant_id)));
      OZ(txs->interrupt(*tx_desc, OB_ERR_QUERY_INTERRUPTED),
         tx_desc->get_tx_id(), status);
      LOG_INFO("kill_query_session", K(ret), K(session), K(tx_desc->get_tx_id()),
               "session_status", status);
    }
  }
  return ret;
}

int ObSqlTransControl::kill_idle_timeout_tx(ObSQLSessionInfo *session)
{
  int ret = OB_SUCCESS;
  if (!session->can_txn_free_route()) {
    ret = kill_tx(session, OB_TRANS_IDLE_TIMEOUT);
  }
  return ret;
}

int ObSqlTransControl::kill_tx(ObSQLSessionInfo *session, int cause)
{
  int ret = OB_SUCCESS;
  if (!session->get_is_deserialized() && session->is_in_transaction()) {
    uint32_t session_id = session->get_sessid();
    if (cause >= 0) {
      LOG_INFO("begin to kill tx", "caused_by", ObTxAbortCauseNames::of(cause), K(cause), K(session_id), KPC(session));
    } else {
      LOG_INFO("begin to kill tx", "caused_by", common::ob_error_name(cause), K(cause), K(session_id), KPC(session));
    }
    ObTxDesc *tx_desc = session->get_tx_desc();
    uint64_t tx_tenant_id = tx_desc->get_tenant_id();
    const ObTransID tx_id = tx_desc->get_tx_id();
    bool tx_free_route_tmp = session->is_txn_free_route_temp();
    MTL_SWITCH(tx_tenant_id) {
      ObSQLSessionInfo::LockGuard data_lock_guard(session->get_thread_data_lock());
      if (tx_free_route_tmp) {
        // if XA-txn is on this server, we have acquired its ref, release ref
        // and disassocate with session
        if (tx_desc->is_xa_trans() && tx_desc->get_addr() == GCONF.self_addr_) {
          ObTransService *txs = MTL(ObTransService*);
          CK (OB_NOT_NULL(txs), session_id, tx_id);
          OZ (txs->release_tx_ref(*tx_desc), session_id, tx_id);
          session->get_tx_desc() = NULL;
        }
      } else  if (tx_desc->is_xa_trans()) {
        const transaction::ObXATransID xid = session->get_xid();
        const transaction::ObGlobalTxType global_tx_type = tx_desc->get_global_tx_type(xid);
        ObXAService *xas = MTL(ObXAService *);
        CK (OB_NOT_NULL(xas));
        if (transaction::ObGlobalTxType::XA_TRANS == global_tx_type) {
          OZ (xas->handle_terminate_for_xa_branch(session->get_xid(), tx_desc, session->get_xa_end_timeout_seconds()),
              xid, global_tx_type, session_id, tx_id);
          // currently, tx_desc is NULL
        } else if (transaction::ObGlobalTxType::DBLINK_TRANS == global_tx_type) {
          OZ (xas->rollback_for_dblink_trans(tx_desc), ret, xid, global_tx_type, tx_id);
          // currently, tx_desc is NULL
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected global trans type", K(ret), K(xid), K(global_tx_type), K(tx_id));
        }
        session->get_tx_desc() = NULL;
      } else {
        ObTransService *txs = NULL;
        CK(OB_NOT_NULL(txs = MTL_WITH_CHECK_TENANT(ObTransService*, tx_tenant_id)));
        OZ(txs->abort_tx(*tx_desc, cause), *session, tx_desc->get_tx_id());
      }
      // NOTE that the tx_desc is set to NULL in xa case, DO NOT print anything in tx_desc
      LOG_INFO("kill tx done", K(ret), K(cause), K(session_id), K(tx_id), K(tx_free_route_tmp));
    }
  }
  return ret;
}

int ObSqlTransControl::rollback_trans(ObSQLSessionInfo *session,
                                      bool &need_disconnect)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("", K(ret), K(session));
  } else if (OB_NOT_NULL(session->get_tx_desc())) {
    need_disconnect = false;
    if (OB_FAIL(do_end_trans_(session, true, false, INT64_MAX, NULL))) {
      LOG_WARN("fail rollback trans", K(ret), KPC(session->get_tx_desc()));
      ObSQLUtils::check_if_need_disconnect_after_end_trans(
          ret, true, false, need_disconnect);
    }
    reset_session_tx_state(session);
  } else {
    reset_session_tx_state(session);
  }
  return ret;
}

int ObSqlTransControl::do_end_trans_(ObSQLSessionInfo *session,
                                     const bool is_rollback,
                                     const bool is_explicit,
                                     const int64_t expire_ts,
                                     ObEndTransAsyncCallback *callback)
{
  int ret = OB_SUCCESS;
  ObTxDesc *&tx_ptr = session->get_tx_desc();
  bool is_detector_exist = false;
  int tmp_ret = OB_SUCCESS;
  const int64_t lcl_op_interval = GCONF._lcl_op_interval;
  if (lcl_op_interval <= 0) {
    // do nothing
  } else if (OB_ISNULL(MTL(share::detector::ObDeadLockDetectorMgr*))) {
    tmp_ret = OB_BAD_NULL_ERROR;
    DETECT_LOG(WARN, "MTL ObDeadLockDetectorMgr is NULL", K(tmp_ret), K(tx_ptr->tid()));
  } else if (OB_TMP_FAIL(MTL(share::detector::ObDeadLockDetectorMgr*)->
                         check_detector_exist(tx_ptr->tid(), is_detector_exist))) {
    DETECT_LOG(WARN, "fail to check detector exist, may causing detector leak", K(tmp_ret),
               K(tx_ptr->tid()));
  } else if (is_detector_exist) {
    ObTransDeadlockDetectorAdapter::unregister_from_deadlock_detector(tx_ptr->tid(),
                                    ObTransDeadlockDetectorAdapter::UnregisterPath::DO_END_TRANS);
  }
  if (!session->is_inner() && session->associated_xa() && !is_explicit) {
    ret = OB_TRANS_XA_RMFAIL;
    LOG_ERROR("executing do end trans in xa", K(ret), K(session->get_xid()), KPC(tx_ptr));
  } else {
    /*
     * normal transaction control
     *
     * call convention:
     * if trans_service.end_trans failed:
     * 1) tx will be aborted (if tx exist and not terminated)
     * 2) the callback will not been called
     */
    ObTransService *txs = NULL;
    uint64_t tenant_id = session->get_effective_tenant_id();
    const common::ObString &trace_info = session->get_ob_trace_info();
    if (OB_FAIL(get_tx_service(session, txs))) {
      LOG_ERROR("fail to get trans service", K(ret), K(tenant_id));
    } else if (is_rollback) {
      ret = txs->rollback_tx(*tx_ptr);
    } else if (callback) {
      if (OB_FAIL(inc_session_ref(session))) {
        LOG_WARN("fail to inc session ref", K(ret));
      } else {
        callback->handout();
        // Add ASH flags to async commit of transactions
        // In the end of async commit in func named ` ObEndTransAsyncCallback::callback() `,
        // set the ash flag named  `in_committing_` to false.
        ObActiveSessionGuard::get_stat().in_committing_ = true;
        if(OB_FAIL(txs->submit_commit_tx(*tx_ptr, expire_ts, *callback, &trace_info))) {
          LOG_WARN("submit commit tx fail", K(ret), KP(callback), K(expire_ts), KPC(tx_ptr));
          GCTX.session_mgr_->revert_session(session);
          callback->handin();
        }
      }
    } else {
      ACTIVE_SESSION_FLAG_SETTER_GUARD(in_committing);
      if (OB_FAIL(txs->commit_tx(*tx_ptr, expire_ts, &trace_info))) {
        LOG_WARN("sync commit tx fail", K(ret), K(expire_ts), KPC(tx_ptr));
      }
    }
  }

  bool print_log = OB_FAIL(ret);
#ifndef NDEBUG
 print_log = true;
#endif
 if (print_log) {
   LOG_INFO("do_end_trans", K(ret),
            KPC(tx_ptr),
            K(is_rollback),
            K(expire_ts),
            K(is_explicit),
            KP(callback));
 }
 return ret;
}

int ObSqlTransControl::decide_trans_read_interface_specs(
    const ObConsistencyLevel &sql_consistency_level,
    ObTxConsistencyType &trans_consistency_type)
{
  int ret = OB_SUCCESS;
  if (sql_consistency_level == STRONG) {
    trans_consistency_type = ObTxConsistencyType::CURRENT_READ;
  } else if (sql_consistency_level == WEAK || sql_consistency_level == FROZEN){
    trans_consistency_type = ObTxConsistencyType::BOUNDED_STALENESS_READ;
  } else {
    ret = OB_INVALID_ARGUMENT;
    SQL_LOG(ERROR, "invalid consistency_level", K(sql_consistency_level));
  }
  return ret;
}

int ObSqlTransControl::start_stmt(ObExecContext &exec_ctx)
{
  int ret = OB_SUCCESS;
  memtable::advance_tlocal_request_lock_wait_stat(rpc::RequestLockWaitStat::RequestStat::START);
  DISABLE_SQL_MEMLEAK_GUARD;
  ObSQLSessionInfo *session = GET_MY_SESSION(exec_ctx);
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(exec_ctx);
  const ObPhysicalPlan *plan = plan_ctx->get_phy_plan();
  ObDASCtx &das_ctx = DAS_CTX(exec_ctx);
  ObTransService *txs = NULL;
  uint64_t tenant_id = 0;
  CK (OB_NOT_NULL(session), OB_NOT_NULL(plan_ctx), OB_NOT_NULL(plan));
  OX (tenant_id = session->get_effective_tenant_id());
  OX (session->get_trans_result().reset());
  OZ (get_tx_service(session, txs), tenant_id);
  OZ (acquire_tx_if_need_(txs, *session));
  OZ (stmt_sanity_check_(session, plan, plan_ctx));
  bool start_hook = false;
  if (!ObSQLUtils::is_nested_sql(&exec_ctx)) {
    OZ (txs->sql_stmt_start_hook(session->get_xid(), *session->get_tx_desc(), session->get_sessid(), get_real_session_id(*session)));
    if (OB_SUCC(ret)) {
      start_hook = true;
      OX (session->get_tx_desc()->clear_interrupt());
    }
  }
  if (OB_SUCC(ret)
      && txs->get_tx_elr_util().check_and_update_tx_elr_info(*session->get_tx_desc())) {
    LOG_WARN("check and update tx elr info", K(ret), KPC(session->get_tx_desc()));
  }
  uint32_t session_id = 0;
  ObTxDesc *tx_desc = NULL;
  bool is_plain_select = false;
  int64_t nested_level = 0;
  OX (nested_level = exec_ctx.get_nested_level());
  OX (session_id = session->get_sessid());
  OX (tx_desc = session->get_tx_desc());
  OX (is_plain_select = plan->is_plain_select());
  OX (tx_desc->clear_interrupt());
  if (OB_SUCC(ret) && !is_plain_select) {
    OZ (stmt_setup_savepoint_(session, das_ctx, plan_ctx, txs, nested_level), session_id, *tx_desc);
  }

  OZ (stmt_setup_snapshot_(session, das_ctx, plan, plan_ctx, txs), session_id, *tx_desc);
  // add tx id to AuditRecord
  OX (session->get_raw_audit_record().trans_id_ = session->get_tx_id());

  // add snapshot info to AuditRecord
  if (OB_SUCC(ret)) {
    ObAuditRecordData &audit_record = session->get_raw_audit_record();
    ObTxReadSnapshot &snapshot = das_ctx.get_snapshot();
    (void)snapshot.format_source_for_display(audit_record.snapshot_source_, sizeof(audit_record.snapshot_source_));
    audit_record.snapshot_ = {
      .version_ = snapshot.core_.version_,
      .tx_id_ = snapshot.core_.tx_id_.get_id(),
      .scn_ = static_cast<int64_t>(snapshot.core_.scn_.cast_to_int()),
      .source_ = audit_record.snapshot_source_
    };
    audit_record.seq_num_ = ObSequence::get_max_seq_no();
  }
  if (OB_SUCC(ret) && !session->has_start_stmt()) {
    OZ (session->set_start_stmt());
  }
  if (plan->is_contain_oracle_trx_level_temporary_table()) {
    OX (tx_desc->set_with_temporary_table());
  }
  if (OB_FAIL(ret) && start_hook) {
    int tmp_ret = txs->sql_stmt_end_hook(session->get_xid(), *session->get_tx_desc());
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("call sql stmt end hook fail", K(tmp_ret));
    }
  }

  if (OB_SUCC(ret)
      && !ObSQLUtils::is_nested_sql(&exec_ctx)
      && das_ctx.get_snapshot().core_.version_.is_valid()) {
    // maintain the read snapshot version on session for multi-version garbage
    // colloecor. It is maintained for all cases except remote exection with ac
    // = 1. So we need carefully design the version for the corner case.
    session->set_reserved_snapshot_version(das_ctx.get_snapshot().core_.version_);
  }

bool print_log = false;
#ifndef NDEBUG
 print_log = true;
#else
 if (OB_FAIL(ret)) { print_log = true; }
#endif
 if (print_log) {
    bool auto_commit = false;
    session->get_autocommit(auto_commit);
    ObPhyPlanType plan_type = plan->get_location_type();
    stmt::StmtType stmt_type = plan->get_stmt_type();
    bool has_for_update = plan->has_for_update();
    bool use_das = plan->use_das();
    ObTxExecResult &trans_result = session->get_trans_result();
    int64_t query_start_time = session->get_query_start_time();
    ObTxReadSnapshot &snapshot = das_ctx.get_snapshot();
    ObTxSEQ savepoint = das_ctx.get_savepoint();
    LOG_INFO("start stmt", K(ret),
             K(auto_commit),
             K(session_id),
             K(snapshot),
             K(savepoint),
             KPC(tx_desc),
             K(plan_type),
             K(stmt_type),
             K(has_for_update),
             K(query_start_time),
             K(use_das),
             K(nested_level),
             KPC(session),
             K(plan),
             "consistency_level_in_plan_ctx", plan_ctx->get_consistency_level(),
             K(trans_result));
  }
  return ret;
}

int ObSqlTransControl::dblink_xa_prepare(ObExecContext &exec_ctx)
{
  int ret = OB_SUCCESS;
  uint16_t charset_id = 0;
  uint16_t ncharset_id = 0;
  uint64_t tenant_id = -1;
  uint32_t sessid = -1;
  uint32_t tm_sessid = 0;
  common::ObDbLinkProxy *dblink_proxy = GCTX.dblink_proxy_;
  sql::ObSQLSessionMgr *session_mgr = GCTX.session_mgr_;
  ObSQLSessionInfo *session = GET_MY_SESSION(exec_ctx);
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(exec_ctx);
  const ObPhysicalPlan *phy_plan = plan_ctx->get_phy_plan();
  common::ObArenaAllocator allocator;
  ObSchemaGetterGuard schema_guard;
  if (OB_ISNULL(session) || OB_ISNULL(dblink_proxy) || OB_ISNULL(phy_plan) || OB_ISNULL(session_mgr) || OB_ISNULL(plan_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ptr", K(ret), KP(session), KP(dblink_proxy), KP(phy_plan), KP(session_mgr), KP(plan_ctx));
  } else if (lib::is_oracle_mode() &&
             !plan_ctx->get_dblink_ids().empty() &&
             plan_ctx->get_main_xa_trans_branch()) {
    if (OB_FAIL(ObDblinkService::get_charset_id(session, charset_id, ncharset_id))) {
      LOG_WARN("failed to get session charset id", K(ret));
    } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(session->get_effective_tenant_id(), schema_guard))) {
      LOG_WARN("failed to get schema guard", K(ret), K(session->get_effective_tenant_id()));
    } else {
      ObIArray<uint64_t> &dblink_ids = plan_ctx->get_dblink_ids();
      tenant_id = session->get_effective_tenant_id();
      sessid = session->get_sessid();
      for(int64_t i = 0; OB_SUCC(ret) && i < dblink_ids.count(); ++i) {
        uint64_t dblink_id = dblink_ids.at(i);
        const ObDbLinkSchema *dblink_schema = NULL;
        common::sqlclient::ObISQLConnection *dblink_conn = NULL;
        if (OB_FAIL(schema_guard.get_dblink_schema(tenant_id, dblink_id, dblink_schema))) {
          LOG_WARN("failed to get dblink schema", K(ret), K(tenant_id), K(dblink_id));
        } else if (OB_ISNULL(dblink_schema)) {
          ret = OB_DBLINK_NOT_EXIST_TO_ACCESS;
          LOG_WARN("dblink schema is NULL", K(ret), K(dblink_id));
        } else if (OB_FAIL(session->get_dblink_context().get_dblink_conn(dblink_id, dblink_conn))) {
          LOG_WARN("failed to get dblink connection from session", K(dblink_id), K(sessid), K(ret));
        } else if (OB_NOT_NULL(dblink_conn)) {
          ObDblinkCtxInSession::revert_dblink_conn(dblink_conn); // release rlock locked by get_dblink_conn
        } else {
          common::sqlclient::dblink_param_ctx dblink_param_ctx;
          if (OB_FAIL(ObDblinkService::init_dblink_param_ctx(dblink_param_ctx,
                                                             session,
                                                             allocator, //useless in oracle mode
                                                             dblink_id,
                                                             static_cast<common::sqlclient::DblinkDriverProto>(dblink_schema->get_driver_proto())))) {
            LOG_WARN("failed to init dblink param ctx", K(ret), K(dblink_param_ctx), K(dblink_id));
          } else if (OB_FAIL(dblink_proxy->create_dblink_pool(dblink_param_ctx,
                                                        dblink_schema->get_host_addr(),
                                                        dblink_schema->get_tenant_name(),
                                                        dblink_schema->get_user_name(),
                                                        dblink_schema->get_plain_password(),
                                                        dblink_schema->get_database_name(),
                                                        dblink_schema->get_conn_string(),
                                                        dblink_schema->get_cluster_name()))) {
            LOG_WARN("failed to create dblink pool", K(ret));
          } else if (OB_FAIL(dblink_proxy->acquire_dblink(dblink_param_ctx, dblink_conn))) {
            LOG_WARN("failed to acquire dblink", K(ret), K(dblink_param_ctx));
          } else if (OB_FAIL(session->get_dblink_context().register_dblink_conn_pool(dblink_conn->get_common_server_pool()))) {
            LOG_WARN("failed to register dblink conn pool to current session", K(ret));
          } else if (OB_FAIL(session->get_dblink_context().set_dblink_conn(dblink_conn))) {
            LOG_WARN("failed to set dblink connection to session", K(session), K(sessid), K(ret));
          } else if (OB_FAIL(ObTMService::tm_rm_start(exec_ctx,
                                                      static_cast<common::sqlclient::DblinkDriverProto>(dblink_schema->get_driver_proto()),
                                                      dblink_conn,
                                                      session->get_dblink_context().get_tx_id()))) {
            LOG_WARN("failed to tm_rm_start", K(ret), K(dblink_id), K(dblink_conn), K(sessid), K(static_cast<common::sqlclient::DblinkDriverProto>(dblink_schema->get_driver_proto())));
          } else {
            LOG_TRACE("link succ to prepare xa connection", KP(plan_ctx), K(ret), K(dblink_id), K(session->get_dblink_context().get_tx_id()));
          }
        }
      }
    }
  }
  if (OB_NOT_NULL(plan_ctx)) {
    plan_ctx->get_dblink_ids().reset();
  }
  return ret;
}

int ObSqlTransControl::stmt_sanity_check_(ObSQLSessionInfo *session,
                                          const ObPhysicalPlan *plan,
                                          ObPhysicalPlanCtx *plan_ctx)
{
  int ret = OB_SUCCESS;
  ObConsistencyLevel current_consist_level = plan_ctx->get_consistency_level();
  CK (current_consist_level != ObConsistencyLevel::INVALID_CONSISTENCY);
  const bool contain_inner_table = plan->is_contain_inner_table();

  // adjust stmt's consistency level
  if (OB_SUCC(ret)) {
    // Weak read statement with inner table should be converted to strong read.
    // For example, schema refresh statement;
    if (contain_inner_table ||
      (!plan->is_plain_select() && current_consist_level != ObConsistencyLevel::STRONG)) {
      plan_ctx->set_consistency_level(ObConsistencyLevel::STRONG);
    }
  }

  if (OB_SUCC(ret) && session->is_in_transaction()) {
    // check consistency type volatile
    ObConsistencyLevel current_consist_level = plan_ctx->get_consistency_level();
    if (current_consist_level == ObConsistencyLevel::WEAK) {
      // read write transaction
      if (!session->get_tx_desc()->is_clean()) {
        plan_ctx->set_consistency_level(ObConsistencyLevel::STRONG);
      }
    }

    // check isolation with consistency type
    ObTxIsolationLevel iso = session->get_tx_desc()->get_isolation_level();
    ObConsistencyLevel cl = plan_ctx->get_consistency_level();
    if (ObConsistencyLevel::WEAK == cl &&
      (iso == ObTxIsolationLevel::SERIAL || iso == ObTxIsolationLevel::RR)) {
      ret = OB_NOT_SUPPORTED;
      TRANS_LOG(ERROR, "statement of weak consistency is not allowed under SERIALIZABLE isolation",
                KR(ret), "trans_id", session->get_tx_id(), "consistency_level", cl);
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "weak consistency under SERIALIZABLE and REPEATABLE-READ isolation level");
    }
  }
  if (OB_SUCC(ret)
      && !plan_ctx->check_consistency_level_validation(contain_inner_table)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected consistency level", K(ret), K(contain_inner_table),
                                                    "current_level", current_consist_level,
                                                    "plan_ctx_level", plan_ctx->get_consistency_level());
  }

  return ret;
}

int ObSqlTransControl::stmt_setup_snapshot_(ObSQLSessionInfo *session,
                                            ObDASCtx &das_ctx,
                                            const ObPhysicalPlan *plan,
                                            const ObPhysicalPlanCtx *plan_ctx,
                                            ObTransService *txs)
{
  int ret = OB_SUCCESS;
  ObConsistencyLevel cl = plan_ctx->get_consistency_level();
  ObTxReadSnapshot &snapshot = das_ctx.get_snapshot();
  if (cl == ObConsistencyLevel::WEAK || cl == ObConsistencyLevel::FROZEN) {
    SCN snapshot_version = SCN::min_scn();
    const bool local_single_ls = plan->is_local_plan() &&
                                 OB_PHY_PLAN_LOCAL == plan->get_location_type();
    if (OB_FAIL(txs->get_weak_read_snapshot_version(session->get_ob_max_read_stale_time(),
                                                    local_single_ls,
                                                    snapshot_version))) {
      TRANS_LOG(WARN, "get weak read snapshot fail", KPC(txs));
      int64_t stale_time = session->get_ob_max_read_stale_time();
      int64_t refresh_interval = GCONF.weak_read_version_refresh_interval;
      if (stale_time > 0 && refresh_interval > stale_time) {
        TRANS_LOG(WARN, "weak_read_version_refresh_interval is larger than ob_max_read_stale_time ",
                  K(refresh_interval), K(stale_time), KPC(txs));
      }
    } else {
      snapshot.init_weak_read(snapshot_version);
    }
    // 1) acquire snapshot version when insert operator is executed
    // 2) don't resolve RR and SERIALIZABLE isolation scenario temporarily, because of remote stmt plan
  } else if (plan->is_plain_insert()
          && session->get_tx_isolation() != ObTxIsolationLevel::SERIAL
          && session->get_tx_isolation() != ObTxIsolationLevel::RR) {
    ObTxDesc &tx_desc = *session->get_tx_desc();
    snapshot.init_none_read();
    snapshot.core_.tx_id_ = tx_desc.get_tx_id();
    snapshot.core_.scn_ = tx_desc.get_tx_seq();
  } else {
    ObTxDesc &tx_desc = *session->get_tx_desc();
    int64_t stmt_expire_ts = get_stmt_expire_ts(plan_ctx, *session);
    share::ObLSID first_ls_id;
    bool local_single_ls_plan = false;
    bool is_single_tablet = false;
    const bool local_single_ls_plan_maybe = plan->is_local_plan() &&
                                            OB_PHY_PLAN_LOCAL == plan->get_location_type();
    if (local_single_ls_plan_maybe) {
      if (OB_FAIL(get_first_lsid(das_ctx, first_ls_id, is_single_tablet))) {
      } else if (!first_ls_id.is_valid()) {
        // do nothing
      // get_ls_read_snapshot may degenerate into get_gts, so it can be used even if the ls is not local.
      // This is mainly to solve the problem of strong reading performance in some single-tablet scenarios.
      } else if (OB_FAIL(txs->get_ls_read_snapshot(tx_desc,
                                                   session->get_tx_isolation(),
                                                   first_ls_id,
                                                   stmt_expire_ts,
                                                   snapshot))) {
      } else if (is_single_tablet) {
        // performance for single tablet scenario
        local_single_ls_plan = true;
      } else {
        local_single_ls_plan = has_same_lsid(das_ctx, snapshot, first_ls_id);
      }
    }
    if (OB_SUCC(ret) && !local_single_ls_plan) {
      ret = txs->get_read_snapshot(tx_desc,
                                   session->get_tx_isolation(),
                                   stmt_expire_ts,
                                   snapshot);
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to get snapshot", K(ret), K(local_single_ls_plan), K(first_ls_id), KPC(session));
    }
  }
  return ret;
}

int ObSqlTransControl::stmt_refresh_snapshot(ObExecContext &exec_ctx) {
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = GET_MY_SESSION(exec_ctx);
  ObDASCtx &das_ctx = DAS_CTX(exec_ctx);
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(exec_ctx);
  const ObPhysicalPlan *plan = plan_ctx->get_phy_plan();
  ObTransService *txs = NULL;
  if (sql::stmt::T_INSERT == plan->get_stmt_type() || sql::stmt::T_INSERT_ALL == plan->get_stmt_type()) {
    //NOTE: oracle insert and insert all stmt can't see the evaluated results of before stmt trigger, no need to refresh snapshot
  } else if (OB_FAIL(get_tx_service(session, txs))) {
    LOG_WARN("failed to get transaction service", K(ret));
  } else if (OB_FAIL(stmt_setup_snapshot_(session, das_ctx, plan, plan_ctx, txs))) {
    LOG_WARN("failed to set snapshot", K(ret));
  }
  return ret;
}

int ObSqlTransControl::set_fk_check_snapshot(ObExecContext &exec_ctx)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = GET_MY_SESSION(exec_ctx);
  ObDASCtx &das_ctx = DAS_CTX(exec_ctx);
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(exec_ctx);
  const ObPhysicalPlan *plan = plan_ctx->get_phy_plan();
  // insert stmt does not set snapshot by default, set snapshopt for foreign key check induced by insert heres
  if (plan->is_plain_insert()) {
    ObTransService *txs = NULL;
    ObTxReadSnapshot &snapshot = das_ctx.get_snapshot();
    ObTxDesc &tx_desc = *session->get_tx_desc();
    int64_t stmt_expire_ts = get_stmt_expire_ts(plan_ctx, *session);
    share::ObLSID local_ls_id;
    bool local_single_ls_plan = plan->is_local_plan()
      && OB_PHY_PLAN_LOCAL == plan->get_location_type()
      && has_same_lsid(das_ctx, snapshot, local_ls_id);
    if (OB_FAIL(get_tx_service(session, txs))) {
      LOG_WARN("failed to get transaction service", K(ret));
    } else {
      if (local_single_ls_plan) {
        ret = txs->get_ls_read_snapshot(tx_desc,
                                        session->get_tx_isolation(),
                                        local_ls_id,
                                        stmt_expire_ts,
                                        snapshot);
      } else {
        ret = txs->get_read_snapshot(tx_desc,
                                      session->get_tx_isolation(),
                                      stmt_expire_ts,
                                      snapshot);
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("fail to get snapshot", K(ret), K(local_ls_id), KPC(session));
      }
    }
  }
  return ret;
}

int ObSqlTransControl::stmt_setup_savepoint_(ObSQLSessionInfo *session,
                                             ObDASCtx &das_ctx,
                                             ObPhysicalPlanCtx *plan_ctx,
                                             ObTransService* txs,
                                             const int64_t nested_level)
{
  int ret = OB_SUCCESS;
  ObTxParam &tx_param = plan_ctx->get_trans_param();
  OZ (build_tx_param_(session, tx_param));
  ObTxDesc &tx = *session->get_tx_desc();
  ObTxSEQ savepoint;
  OZ (txs->create_implicit_savepoint(tx, tx_param, savepoint, nested_level == 0), tx, tx_param);
  OX (das_ctx.set_savepoint(savepoint));
  return ret;
}

#define CHECK_TXN_FREE_ROUTE_ALLOWED()                                  \
  if (OB_SUCC(ret) && !session->is_inner() && session->is_txn_free_route_temp()) { \
    ret = OB_TRANS_FREE_ROUTE_NOT_SUPPORTED;                            \
    LOG_WARN("current stmt is not allowed executed on txn tmp node", K(ret), \
             K(session->get_txn_free_route_ctx()), KPC(session));       \
  }
int ObSqlTransControl::create_savepoint(ObExecContext &exec_ctx,
                                        const ObString &sp_name,
                                        const bool user_create)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = GET_MY_SESSION(exec_ctx);
  ObTransService *txs = NULL;
  CK (OB_NOT_NULL(session));
  CHECK_SESSION (session);
  CHECK_TXN_FREE_ROUTE_ALLOWED();
  OZ (get_tx_service(session, txs));
  OZ (acquire_tx_if_need_(txs, *session));
  bool start_hook = false;
  OZ(start_hook_if_need_(*session, txs, start_hook));
  OZ (txs->create_explicit_savepoint(*session->get_tx_desc(), sp_name, get_real_session_id(*session), user_create), sp_name);
  if (start_hook) {
    int tmp_ret = txs->sql_stmt_end_hook(session->get_xid(), *session->get_tx_desc());
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("call sql stmt end hook fail", K(tmp_ret));
      ret = COVER_SUCC(tmp_ret);
    }
  }
  if (user_create) {
    OX(session->get_raw_audit_record().seq_num_ = ObSequence::get_max_seq_no());
  }
  return ret;
}

uint32_t ObSqlTransControl::get_real_session_id(ObSQLSessionInfo &session)
{
  return session.get_xid().empty() ? 0 : (session.get_proxy_sessid() != 0 ? session.get_proxy_sessid() : session.get_sessid());
}

int ObSqlTransControl::get_first_lsid(const ObDASCtx &das_ctx, share::ObLSID &first_lsid, bool &is_single_tablet)
{
  int ret = OB_SUCCESS;
  const DASTableLocList &table_locs = das_ctx.get_table_loc_list();
  if (!table_locs.empty()) {
    const ObDASTableLoc *first_table_loc = table_locs.get_first();
    const DASTabletLocList &tablet_locs = first_table_loc->get_tablet_locs();
    if (!tablet_locs.empty()) {
      const ObDASTabletLoc *tablet_loc = tablet_locs.get_first();
      first_lsid = tablet_loc->ls_id_;
    }
    is_single_tablet = (1 == table_locs.size() && 1 == tablet_locs.size());
  }
  return ret;
}

bool ObSqlTransControl::has_same_lsid(const ObDASCtx &das_ctx,
                                      const ObTxReadSnapshot &snapshot,
                                      share::ObLSID &first_lsid)
{
  int ret = OB_SUCCESS;
  bool bret = true;
  ObLSHandle ls_handle;
  const share::SCN snapshot_version = snapshot.core_.version_;
  const DASTableLocList &table_locs = das_ctx.get_table_loc_list();
  FOREACH_X(table_node, table_locs, bret) {
    ObDASTableLoc *table_loc = *table_node;
    for (DASTabletLocListIter tablet_node = table_loc->tablet_locs_begin();
         bret && tablet_node != table_loc->tablet_locs_end(); ++tablet_node) {
      ObDASTabletLoc *tablet_loc = *tablet_node;
      const ObTabletID tablet_id = tablet_loc->tablet_id_;
      if (first_lsid != tablet_loc->ls_id_) {
        bret = false;
      }
      if (bret && !ls_handle.is_valid()) {
        ObLSService *ls_svr = NULL;
        if (OB_ISNULL(ls_svr = MTL(ObLSService *))) {
          bret = false;
        } else if (OB_FAIL(ls_svr->get_ls(first_lsid, ls_handle, ObLSGetMod::TRANS_MOD))) {
          bret = false;
        } else {
          // do nothing
        }
      }
      if (bret) {
        ObLS *ls = NULL;
        ObLSTabletService *ls_tablet_service = NULL;
        if (OB_ISNULL(ls = ls_handle.get_ls())) {
          bret = false;
        } else if (OB_ISNULL(ls_tablet_service = ls->get_tablet_svr())) {
          bret = false;
        } else {
          ObTablet *tablet = NULL;
          ObTabletHandle tablet_handle;
          if (OB_FAIL(ls_tablet_service->get_tablet(tablet_id, tablet_handle, 100 * 1000))) {
            bret = false;
          } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
            bret = false;
          } else if (!tablet->get_tablet_meta().transfer_info_.is_valid()) {
            bret = false;
          } else if (tablet->get_tablet_meta().transfer_info_.transfer_start_scn_ >= snapshot_version) {
            bret = false;
          } else {
            // do nothing
          }
        }
      }
      if (bret && common::ObRole::FOLLOWER == snapshot.snapshot_ls_role_) {
        ObLS *ls = NULL;
        if (OB_ISNULL(ls = ls_handle.get_ls())) {
          bret = false;
          LOG_WARN("invalid ls", K(bret), K(first_lsid), K(snapshot));
        } else if (!(ls->get_dup_table_ls_handler()->is_dup_tablet(tablet_id))) {
          bret = false;
          LOG_WARN("There is a normal tablet, retry to acquire snapshot with gts", K(bret), K(first_lsid),
                   K(snapshot), K(tablet_loc));
        }
      }
    }
  }
  return bret;
}

int ObSqlTransControl::start_hook_if_need_(ObSQLSessionInfo &session,
                                           ObTransService *txs,
                                           bool &start_hook)
{
  int ret = OB_SUCCESS;
  if (!session.get_tx_desc()->is_shadow() && !session.has_start_stmt() &&
      OB_SUCC(txs->sql_stmt_start_hook(session.get_xid(), *session.get_tx_desc(), session.get_sessid(), get_real_session_id(session)))) {
    start_hook = true;
  }
  return ret;
}

int ObSqlTransControl::rollback_savepoint(ObExecContext &exec_ctx,
                                          const ObString &sp_name)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = GET_MY_SESSION(exec_ctx);
  const ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(exec_ctx);
  ObTransService *txs = NULL;
  int64_t stmt_expire_ts = 0;

  CK (OB_NOT_NULL(session), OB_NOT_NULL(plan_ctx));
  CHECK_SESSION (session);
  CHECK_TXN_FREE_ROUTE_ALLOWED();
  OZ (get_tx_service(session, txs));
  OZ (acquire_tx_if_need_(txs, *session));
  OX (stmt_expire_ts = get_stmt_expire_ts(plan_ctx, *session));
  bool start_hook = false;
  OZ(start_hook_if_need_(*session, txs, start_hook));
  OZ (txs->rollback_to_explicit_savepoint(*session->get_tx_desc(), sp_name, stmt_expire_ts, get_real_session_id(*session)), sp_name);
  if (0 == session->get_raw_audit_record().seq_num_) {
    OX (session->get_raw_audit_record().seq_num_ = ObSequence::get_max_seq_no());
  }
  if (start_hook) {
    int tmp_ret = txs->sql_stmt_end_hook(session->get_xid(), *session->get_tx_desc());
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("call sql stmt end hook fail", K(tmp_ret));
      ret = COVER_SUCC(tmp_ret);
    }
  }
  return ret;
}

int ObSqlTransControl::release_stash_savepoint(ObExecContext &exec_ctx,
                                               const ObString &sp_name)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = GET_MY_SESSION(exec_ctx);
  ObTransService *txs = NULL;
  CK (OB_NOT_NULL(session));
  // NOTE: should _NOT_ check session is zombie, because the stash savepoint
  // should be release before query quit
  // CHECK_SESSION (session);
  OZ (get_tx_service(session, txs), *session);
  OZ (acquire_tx_if_need_(txs, *session));
  OZ (txs->release_explicit_savepoint(*session->get_tx_desc(), sp_name, get_real_session_id(*session)), *session, sp_name);
  return ret;
}

int ObSqlTransControl::release_savepoint(ObExecContext &exec_ctx,
                                         const ObString &sp_name)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = GET_MY_SESSION(exec_ctx);
  ObTransService *txs = NULL;
  CK (OB_NOT_NULL(session));
  CHECK_SESSION (session);
  CHECK_TXN_FREE_ROUTE_ALLOWED();
  OZ (get_tx_service(session, txs), *session);
  OZ (acquire_tx_if_need_(txs, *session));
  bool start_hook = false;
  OZ(start_hook_if_need_(*session, txs, start_hook));
  OZ (txs->release_explicit_savepoint(*session->get_tx_desc(), sp_name, get_real_session_id(*session)), *session, sp_name);
  if (start_hook) {
    int tmp_ret = txs->sql_stmt_end_hook(session->get_xid(), *session->get_tx_desc());
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("call sql stmt end hook fail", K(tmp_ret));
      ret = COVER_SUCC(tmp_ret);
    }
  }
  return ret;
}

int ObSqlTransControl::xa_rollback_all_changes(ObExecContext &exec_ctx)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = GET_MY_SESSION(exec_ctx);
  const ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(exec_ctx);
  int64_t stmt_expire_ts = 0;

  CK (OB_NOT_NULL(session), OB_NOT_NULL(plan_ctx));
  if (OB_SUCC(ret) && (!session->is_in_transaction() || !session->associated_xa())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("called in wrong context", KR(ret), KPC(session->get_tx_desc()));
  }
  CHECK_SESSION (session);
  OX (stmt_expire_ts = get_stmt_expire_ts(plan_ctx, *session));
  ObXAService * xa_service = MTL(ObXAService*);
  CK (OB_NOT_NULL(xa_service));
  OZ (xa_service->xa_rollback_all_changes(session->get_xid(),
                                          session->get_tx_desc(),
                                          stmt_expire_ts),
      PC(session->get_tx_desc()));
  return ret;
}

int ObSqlTransControl::end_stmt(ObExecContext &exec_ctx, const bool rollback)
{
  int ret = OB_SUCCESS;
  DISABLE_SQL_MEMLEAK_GUARD;
  ObSQLSessionInfo *session = GET_MY_SESSION(exec_ctx);
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(exec_ctx);
  const ObPhysicalPlan *plan = NULL;
  ObDASCtx &das_ctx = DAS_CTX(exec_ctx);
  ObTransService *txs = NULL;
  ObTxDesc *tx_desc = NULL;
  stmt::StmtType stmt_type = stmt::StmtType::T_NONE;
  bool is_plain_select = false;
  ObTxSEQ savepoint = das_ctx.get_savepoint();
  int exec_errcode = exec_ctx.get_errcode();
  int64_t tx_id = 0;

  CK (OB_NOT_NULL(session), OB_NOT_NULL(plan_ctx));
  CK (OB_NOT_NULL(plan = plan_ctx->get_phy_plan()));
  OX (tx_desc = session->get_tx_desc());
  OX (stmt_type = plan->get_stmt_type());
  OX (is_plain_select = plan->is_plain_select());
  OZ (get_tx_service(session, txs), *session);
  // plain select stmt don't require txn descriptor
  if (OB_SUCC(ret) && !is_plain_select) {
    CK (OB_NOT_NULL(tx_desc));
    ObTransID tx_id_before_rollback;
    OX (tx_id_before_rollback = tx_desc->get_tx_id());
    tx_id = tx_id_before_rollback.get_id();
    OX (ObTransDeadlockDetectorAdapter::maintain_deadlock_info_when_end_stmt(exec_ctx, rollback));
    ObTxExecResult &tx_result = session->get_trans_result();
    if (OB_FAIL(ret)) {
    } else if (OB_E(EventTable::EN_TX_RESULT_INCOMPLETE, session->get_sessid()) tx_result.is_incomplete()) {
      if (!rollback) {
        LOG_ERROR("trans result incomplete, but rollback not issued");
      }
      OZ (txs->abort_tx(*tx_desc, ObTxAbortCause::TX_RESULT_INCOMPLETE));
      ret = OB_TRANS_NEED_ROLLBACK;
      LOG_WARN("trans result incomplete, trans aborted", K(ret));
    } else if (plan->get_enable_append()
               && plan->get_enable_inc_direct_load()
               && OB_UNLIKELY(OB_SUCCESS != exec_errcode)) {
      if (!rollback) {
        LOG_ERROR("direct load failed, but rollback not issued");
      }
      OZ (txs->abort_tx(*tx_desc, ObTxAbortCause::TX_RESULT_INCOMPLETE));
      ret = OB_TRANS_NEED_ROLLBACK;
      LOG_WARN("direct load failed, trans aborted", KR(ret));
    } else if (rollback) {
      int64_t stmt_expire_ts = get_stmt_expire_ts(plan_ctx, *session);
      const share::ObLSArray &touched_ls = tx_result.get_touched_ls();
      OZ (txs->rollback_to_implicit_savepoint(*tx_desc, savepoint, stmt_expire_ts, &touched_ls, exec_errcode),
          savepoint, stmt_expire_ts, touched_ls);
      // prioritize returning session error code
      if (session->is_terminate(ret)) {
        LOG_INFO("trans has terminated when end stmt", K(ret), K(tx_id_before_rollback));
      }
    }
    // this may happend cause tx may implicit aborted
    // (for example: first write sql of implicit started trans meet lock conflict)
    // and if associated detector is created, must clean it also
    if (OB_NOT_NULL(tx_desc) && tx_desc->get_tx_id() != tx_id_before_rollback) {
      ObTransDeadlockDetectorAdapter::
      unregister_from_deadlock_detector(tx_id_before_rollback,
                                        ObTransDeadlockDetectorAdapter::
                                        UnregisterPath::TX_ROLLBACK_IN_END_STMT);
    }
  }
  // call end stmt hook
  if (OB_NOT_NULL(tx_desc) && OB_NOT_NULL(txs) && OB_NOT_NULL(session) && !ObSQLUtils::is_nested_sql(&exec_ctx)) {
    int tmp_ret = txs->sql_stmt_end_hook(session->get_xid(), *tx_desc);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("call sql stmt end hook fail", K(tmp_ret));
      ret = COVER_SUCC(tmp_ret);
    }
  }

  if (!ObSQLUtils::is_nested_sql(&exec_ctx) && OB_NOT_NULL(session)) {
    int tmp_ret = session->set_end_stmt();
    if (OB_SUCCESS != tmp_ret) {
      LOG_ERROR("set_end_stmt fail", K(tmp_ret));
    }
    ret = COVER_SUCC(tmp_ret);
  }

  if (OB_SUCC(ret) && !ObSQLUtils::is_nested_sql(&exec_ctx)) {
    session->reset_reserved_snapshot_version();
  }

  bool print_log = false;
#ifndef NDEBUG
  print_log = true;
#else
  if (OB_FAIL(ret) || rollback) { print_log = true; }
#endif
  if (print_log
      && OB_NOT_NULL(session)
      && (OB_TRY_LOCK_ROW_CONFLICT != exec_ctx.get_errcode()
          || REACH_TIME_INTERVAL(1 * 1000 * 1000))) {
    LOG_INFO("end stmt", K(ret),
             "tx_id", tx_id,
             "plain_select", is_plain_select,
             "stmt_type", stmt_type,
             K(savepoint),
             "tx_desc", PC(session->get_tx_desc()),
             "trans_result", session->get_trans_result(),
             K(rollback),
             KPC(session),
             K(exec_ctx.get_errcode()));
  }
  if (OB_NOT_NULL(session)) {
    session->get_trans_result().reset();
  }
  memtable::advance_tlocal_request_lock_wait_stat(rpc::RequestLockWaitStat::RequestStat::END);
  return ret;
}

int ObSqlTransControl::inc_session_ref(const ObSQLSessionInfo *session)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(GCTX.session_mgr_));
  OZ (GCTX.session_mgr_->inc_session_ref(session));
  return ret;
}

bool ObSqlTransControl::is_isolation_RR_or_SE(ObTxIsolationLevel isolation)
{
  return (isolation == ObTxIsolationLevel::RR
          || isolation == ObTxIsolationLevel::SERIAL);
}

int ObSqlTransControl::create_anonymous_savepoint(ObExecContext &exec_ctx, ObTxSEQ &savepoint)
{
  int ret = OB_SUCCESS;
  ObTransService *txs = NULL;
  ObSQLSessionInfo *session = GET_MY_SESSION(exec_ctx);
  CK (OB_NOT_NULL(session));
  OZ (get_tx_service(session, txs));
  CK (OB_NOT_NULL(session->get_tx_desc()));
  ObTxParam tx_param;
  const int16_t branch_id = DAS_CTX(exec_ctx).get_write_branch_id();
  OZ (txs->create_branch_savepoint(*session->get_tx_desc(), branch_id, savepoint), *session->get_tx_desc());
  return ret;
}

int ObSqlTransControl::create_anonymous_savepoint(ObTxDesc &tx_desc, ObTxSEQ &savepoint)
{
  int ret = OB_SUCCESS;
  ObTransService *txs = NULL;
  if (OB_ISNULL(txs = MTL_WITH_CHECK_TENANT(ObTransService*, tx_desc.get_tenant_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get_tx_service", K(ret), K(tx_desc.get_tenant_id()), K(MTL_ID()));
  }
  OZ (txs->create_in_txn_implicit_savepoint(tx_desc, savepoint));
  return ret;
}

int ObSqlTransControl::rollback_savepoint(ObExecContext &exec_ctx, const ObTxSEQ savepoint)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = GET_MY_SESSION(exec_ctx);
  const ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(exec_ctx);
  ObTransService *txs = NULL;
  int64_t expire_ts = 0;

  CK (OB_NOT_NULL(session), OB_NOT_NULL(plan_ctx));
  OZ (get_tx_service(session, txs));
  CK (OB_NOT_NULL(session->get_tx_desc()));
  OX (expire_ts = get_stmt_expire_ts(plan_ctx, *session));
  OZ (txs->rollback_to_implicit_savepoint(*session->get_tx_desc(), savepoint, expire_ts, nullptr));
  return ret;
}
/*
 * Ask Transaction Layer accumulated transaction state need collected
 * to Transaction Manager
 * @trans_result : managed by SQL layer and maybe non-empty before pass down.
 */
int ObSqlTransControl::get_trans_result(ObExecContext &exec_ctx, ObTxExecResult &trans_result)
{
  int ret = OB_SUCCESS;
  ObTransService *txs = NULL;
  ObSQLSessionInfo *session = NULL;
  CK (OB_NOT_NULL(session = exec_ctx.get_my_session()));
  OZ (get_tx_service(session, txs));
  if (OB_SUCC(ret) && session->is_in_transaction()) {
    OZ(txs->collect_tx_exec_result(*session->get_tx_desc(), trans_result));
    int64_t tx_id = session->get_tx_id();
    NG_TRACE_EXT(get_trans_result, OB_ID(ret), ret, OB_ID(trans_id), tx_id);
  }
  return ret;
}

int ObSqlTransControl::get_trans_result(ObExecContext &exec_ctx)
{
  return get_trans_result(exec_ctx, exec_ctx.get_my_session()->get_trans_result());
}

int ObSqlTransControl::reset_session_tx_state(ObBasicSessionInfo *session,
                                              bool reuse_tx_desc,
                                              bool reset_trans_variable,
                                              const uint64_t data_version)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("reset session tx state", KPC(session->get_tx_desc()), K(lbt()));
  if (OB_NOT_NULL(session->get_tx_desc())) {
    ObSQLSessionInfo::LockGuard data_lock_guard(session->get_thread_data_lock());
    ObTxDesc &tx_desc = *session->get_tx_desc();
    ObTransID tx_id = tx_desc.get_tx_id();
    uint64_t effect_tid = session->get_effective_tenant_id();
    MTL_SWITCH(effect_tid) {
      ObTransService *txs = NULL;
      OZ (get_tx_service(session, txs), *session, tx_desc);
      if (reuse_tx_desc) {
        if (OB_FAIL(txs->reuse_tx(tx_desc, data_version))) {
          LOG_ERROR("reuse txn descriptor fail, will release it", K(ret), KPC(session), K(tx_desc));
          OZ (txs->release_tx(tx_desc), tx_id);
          session->get_tx_desc() = NULL;
        }
      } else {
        OZ (txs->release_tx(tx_desc), *session, tx_id, tx_desc);
        session->get_tx_desc() = NULL;
      }
    }
  }
  session->get_trans_result().reset();
  session->reset_tx_variable(reset_trans_variable);
  return ret;
}

int ObSqlTransControl::reset_session_tx_state(ObSQLSessionInfo *session, bool reuse_tx_desc, bool reset_trans_variable)
{
  int temp_ret = OB_SUCCESS;
  // cleanup txn level temp tables if this is the txn start node
  ObTxDesc *tx_desc = session->get_tx_desc();
  if (OB_NOT_NULL(tx_desc)
      && tx_desc->with_temporary_table()
      && tx_desc->get_addr() == GCONF.self_addr_) {
    temp_ret = session->drop_temp_tables(false);
    if (OB_SUCCESS != temp_ret) {
      LOG_WARN_RET(temp_ret, "trx level temporary table clean failed", KR(temp_ret));
    }
  }
  int ret = reset_session_tx_state(static_cast<ObBasicSessionInfo*>(session), reuse_tx_desc,
      reset_trans_variable, session->get_data_version());
  return COVER_SUCC(temp_ret);
}

static int get_org_cluster_id_(ObSQLSessionInfo *session, int64_t &org_cluster_id) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(session->get_ob_org_cluster_id(org_cluster_id))) {
    LOG_WARN("fail to get ob_org_cluster_id", K(ret));
  } else if (OB_INVALID_ORG_CLUSTER_ID == org_cluster_id ||
             OB_INVALID_CLUSTER_ID == org_cluster_id) {
    org_cluster_id = ObServerConfig::get_instance().cluster_id;
    // 如果没设置ob_org_cluster_id（0为非法值，认为没有设置），则设为当前集群的cluster_id。
    // 如果配置项中没设置cluster_id，则ObServerConfig::get_instance().cluster_id会拿到默认值-1。
    // 配置项中没设置cluster_id的话observer是起不来的，因此这里org_cluster_id不会为-1。
    // 保险起见，这里判断org_cluster_id为0或者-1都将其设为ObServerConfig::get_instance().cluster_id。
    if (org_cluster_id < OB_MIN_CLUSTER_ID
        || org_cluster_id > OB_MAX_CLUSTER_ID) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("org_cluster_id is set to cluster_id, but it is out of range",
                K(ret), K(org_cluster_id), K(OB_MIN_CLUSTER_ID), K(OB_MAX_CLUSTER_ID));
    }
  }
  return ret;
}

int ObSqlTransControl::acquire_tx_if_need_(ObTransService *txs, ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session.get_tx_desc())) {
    OZ (txs->acquire_tx(session.get_tx_desc(), session.get_sessid(), session.get_data_version()), session);
  }
  return ret;
}

int ObSqlTransControl::lock_table(ObExecContext &exec_ctx,
                                  const uint64_t table_id,
                                  const ObIArray<ObObjectID> &part_ids,
                                  const ObTableLockMode lock_mode,
                                  const int64_t wait_lock_seconds)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = GET_MY_SESSION(exec_ctx);
  const ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(exec_ctx);
  ObTransService *txs = NULL;
  tablelock::ObTableLockService *lock_service = NULL;

  CK (OB_NOT_NULL(session), OB_NOT_NULL(plan_ctx));
  CHECK_SESSION (session);
  OZ (get_tx_service(session, txs));
  OZ (get_lock_service(session->get_effective_tenant_id(), lock_service));
  if (OB_SUCC(ret) && OB_ISNULL(session->get_tx_desc())) {
    OZ (txs->acquire_tx(session->get_tx_desc(), session->get_sessid(), session->get_data_version()), *session);
  }
  ObTxParam tx_param;
  OZ (build_tx_param_(session, tx_param));
  // calculate lock table timeout
  int64_t lock_timeout_us = 0;
  {
    int64_t stmt_expire_ts = 0;
    int64_t tx_expire_ts = 0;
    int64_t lock_wait_expire_ts = 0;
    OX (stmt_expire_ts = get_stmt_expire_ts(plan_ctx, *session));
    OZ (get_trans_expire_ts(*session, tx_expire_ts));

    if (wait_lock_seconds < 0) {
      // It means that there's no opt about wait or no wait,
      // so we just use the deafult timeout config here.
      OX (lock_timeout_us = MAX(200L, MIN(stmt_expire_ts, tx_expire_ts) -
                                         ObTimeUtility::current_time()));
    } else {
      // The priority of stmt_expire_ts and tx_expire_ts is higher than
      // wait N. So if the statement or transaction is timeout, it should
      // return error code, rather than wait until N seconds.
      lock_wait_expire_ts =
        MIN3(session->get_query_start_time() + wait_lock_seconds * 1000 * 1000, stmt_expire_ts, tx_expire_ts);
      OX (lock_timeout_us = lock_wait_expire_ts - ObTimeUtility::current_time());
      lock_timeout_us = lock_timeout_us < 0 ? 0 : lock_timeout_us;
    }
  }
  if (part_ids.empty()) {
    ObLockTableRequest arg;
    arg.table_id_ = table_id;
    arg.owner_id_.set_default();
    arg.lock_mode_ = lock_mode;
    arg.op_type_ = ObTableLockOpType::IN_TRANS_COMMON_LOCK;
    arg.timeout_us_ = lock_timeout_us;
    arg.is_from_sql_ = true;

    OZ (lock_service->lock_table(*session->get_tx_desc(),
                                tx_param,
                                arg),
        tx_param, table_id, lock_mode, lock_timeout_us);
  } else {
    ObLockPartitionRequest arg;
    arg.table_id_ = table_id;
    arg.owner_id_.set_default();
    arg.lock_mode_ = lock_mode;
    arg.op_type_ = ObTableLockOpType::IN_TRANS_COMMON_LOCK;
    arg.timeout_us_ = lock_timeout_us;
    arg.is_from_sql_ = true;

    for (int64_t i = 0; i < part_ids.count() && OB_SUCC(ret); ++i) {
      arg.part_object_id_ = part_ids.at(i);
      OZ(lock_service->lock_partition_or_subpartition(*session->get_tx_desc(),
                                                      tx_param, arg),
         tx_param, table_id, lock_mode, lock_timeout_us);
    }
  }

  return ret;
}

void ObSqlTransControl::clear_xa_branch(const ObXATransID &xid, ObTxDesc *&tx_desc)
{
  MTL(ObXAService *)->clear_xa_branch(xid, tx_desc);
}


int ObSqlTransControl::check_ls_readable(const uint64_t tenant_id,
                                         const share::ObLSID &ls_id,
                                         const common::ObAddr &addr,
                                         const int64_t max_stale_time_us,
                                         bool &can_read)
{
  int ret = OB_SUCCESS;
  can_read = false;

  if (!ls_id.is_valid()
      || !addr.is_valid()
      || max_stale_time_us == 0
      || max_stale_time_us <= -2) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ls_id), K(addr), K(max_stale_time_us));
  } else if (observer::ObServer::get_instance().get_self() == addr && max_stale_time_us > 0) {
    storage::ObLSService *ls_svr =  MTL(storage::ObLSService *);
    storage::ObLSHandle handle;
    ObLS *ls = nullptr;

    if (OB_ISNULL(ls_svr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log stream service is NULL", K(ret));
    } else if (OB_FAIL(ls_svr->get_ls(ls_id, handle, ObLSGetMod::TRANS_MOD))) {
      LOG_WARN("get id service log stream failed");
    } else if (OB_ISNULL(ls = handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("id service log stream not exist");
    } else if (ObTimeUtility::current_time() - max_stale_time_us
         < ls->get_ls_wrs_handler()->get_ls_weak_read_ts().convert_to_ts()) {
      can_read = true;
    } else if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
      LOG_WARN("log stream unreadable", K(ls_id), K(addr), K(max_stale_time_us));
    }
  } else {
    ObBLKey blk;
    bool in_black_list = false;
    if (OB_FAIL(blk.init(addr, tenant_id, ls_id))) {
      LOG_WARN("ObBLKey init error", K(ret), K(addr), K(tenant_id), K(ls_id));
    } else if (OB_FAIL(ObBLService::get_instance().check_in_black_list(blk, in_black_list))) {
      LOG_WARN("check in black list error", K(ret), K(blk));
    } else {
      can_read = (in_black_list ? false : true);
      if (!can_read && REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
        LOG_WARN("log stream unreadable", K(ls_id), K(blk), K(in_black_list));
      }
    }
  }
  return ret;
}

#define DELEGATE_TO_TXN(name)                                           \
  int ObSqlTransControl::update_txn_##name##_state(ObSQLSessionInfo &session, const char* buf, const int64_t len, int64_t &pos) \
  {                                                                     \
    int ret = OB_SUCCESS;                                               \
    ObSQLSessionInfo::LockGuard data_lock_guard(session.get_thread_data_lock()); \
    ObTransService *txs = NULL;                                         \
    OZ (get_tx_service(&session, txs));                                 \
    ObTxDesc *&tx_desc = session.get_tx_desc();                          \
    bool has_tx_desc = OB_NOT_NULL(tx_desc);                            \
    ObTransID prev_tx_id;                                               \
    if (has_tx_desc) { prev_tx_id =  session.get_tx_id(); }             \
    OZ (txs->txn_free_route__update_##name##_state(session.get_sessid(), tx_desc, session.get_txn_free_route_ctx(), buf, len, pos, session.get_data_version()), session); \
    if (OB_SUCC(ret) && has_tx_desc && (OB_ISNULL(tx_desc) || tx_desc->get_tx_id() !=  prev_tx_id)) { \
      session.reset_tx_variable();                                      \
    }                                                                   \
    LOG_DEBUG("update-txn-state", K(ret), K(session), K(prev_tx_id), KPC(tx_desc)); \
    if (OB_FAIL(ret)) {                                                 \
      LOG_WARN("update txn state fail", K(ret), "state", #name,         \
               K(session.get_txn_free_route_ctx()),                     \
               K(session), K(prev_tx_id), KPC(tx_desc));                \
    }                                                                   \
    return ret;                                                         \
  }                                                                     \
  int ObSqlTransControl::serialize_txn_##name##_state(ObSQLSessionInfo &session, char* buf, const int64_t len, int64_t &pos) \
  {                                                                     \
    int ret = OB_SUCCESS;                                               \
    MTL_SWITCH(session.get_effective_tenant_id()) {                     \
      ObTransService *txs = NULL;                                       \
      OZ (get_tx_service(&session, txs));                               \
      OZ (txs->txn_free_route__serialize_##name##_state(session.get_sessid(), session.get_tx_desc(), session.get_txn_free_route_ctx(), buf, len, pos)); \
    }                                                                   \
    LOG_DEBUG("serialize-txn-state", K(session));                       \
    return ret;                                                         \
  }                                                                     \
  int64_t ObSqlTransControl::get_txn_##name##_state_serialize_size(ObSQLSessionInfo &session) \
  {                                                                     \
    int ret = OB_SUCCESS;                                               \
    int64_t size = -1;                                                  \
    ObTransService *txs = NULL;                                         \
    MTL_SWITCH(session.get_effective_tenant_id()) {                     \
      OZ (get_tx_service(&session, txs));                               \
      if (OB_SUCC(ret)) {                                               \
        size = txs->txn_free_route__get_##name##_state_serialize_size(session.get_tx_desc(), session.get_txn_free_route_ctx()); \
      }                                                                 \
    }                                                                   \
    LOG_DEBUG("get-serialize-size-txn-state", K(session));              \
    return size;                                                        \
  }                                                                     \
  int64_t ObSqlTransControl::get_fetch_txn_##name##_state_size(ObSQLSessionInfo& sess) \
  { return ObTransService::txn_free_route__get_##name##_state_size(sess.get_tx_desc()); } \
  int ObSqlTransControl::fetch_txn_##name##_state(ObSQLSessionInfo &sess, char *buf, const int64_t len, int64_t &pos) \
  { return ObTransService::txn_free_route__get_##name##_state(sess.get_tx_desc(), sess.get_txn_free_route_ctx(), buf, len, pos); } \
  int ObSqlTransControl::cmp_txn_##name##_state(const char* cur_buf, int64_t cur_len, const char* last_buf, int64_t last_len) \
  { return ObTransService::txn_free_route__cmp_##name##_state(cur_buf, cur_len, last_buf, last_len); } \
  void ObSqlTransControl::display_txn_##name##_state(ObSQLSessionInfo &sess, const char* local_buf, const int64_t local_len, const char* remote_buf, const int64_t remote_len) \
  {                                                                     \
    ObTransService::txn_free_route__display_##name##_state("LOAL", local_buf, local_len); \
    ObTransService::txn_free_route__display_##name##_state("REMOTE", remote_buf, remote_len); \
    ObTxDesc *tx = sess.get_tx_desc();                                  \
    if (OB_NOT_NULL(tx)) {                                              \
      tx->print_trace();                                                \
    }                                                                   \
  }

DELEGATE_TO_TXN(static);
DELEGATE_TO_TXN(dynamic);
DELEGATE_TO_TXN(parts);
DELEGATE_TO_TXN(extra);

#undef DELEGATE_TO_TXN

int ObSqlTransControl::calc_txn_free_route(ObSQLSessionInfo &session, ObTxnFreeRouteCtx &txn_free_route_ctx)
{
  int ret = OB_SUCCESS;
  ObTransService *txs = NULL;
  MTL_SWITCH(session.get_effective_tenant_id()) {
    OZ (get_tx_service(&session, txs));
    OZ (txs->calc_txn_free_route(session.get_tx_desc(), txn_free_route_ctx));
  }
  return ret;
}

int ObSqlTransControl::check_free_route_tx_alive(ObSQLSessionInfo &session, ObTxnFreeRouteCtx &txn_free_route_ctx)
{
  int ret = OB_SUCCESS;
  ObTxDesc *tx = session.get_tx_desc();
  if (OB_NOT_NULL(tx)) {
    MTL_SWITCH(tx->get_tenant_id()) {
      ObTransService *txs = MTL(ObTransService*);
      CK (OB_NOT_NULL(txs));
      OZ (txs->tx_free_route_check_alive(txn_free_route_ctx, *tx, session.get_sessid()));
    }
  }
  return ret;
}

int ObSqlTransControl::alloc_branch_id(ObExecContext &exec_ctx, const int64_t count, int16_t &branch_id)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = GET_MY_SESSION(exec_ctx);
  ObTxDesc *tx_desc = NULL;
  CK (OB_NOT_NULL(session));
  CK (OB_NOT_NULL(tx_desc = session->get_tx_desc()));
  OZ (tx_desc->alloc_branch_id(count, branch_id));
  return ret;
}

int ObSqlTransControl::reset_trans_for_autocommit_lock_conflict(ObExecContext &exec_ctx)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = GET_MY_SESSION(exec_ctx);
  ObTxDesc *tx_desc = NULL;
  CK (OB_NOT_NULL(session));
  CK (OB_NOT_NULL(tx_desc = session->get_tx_desc()));
  OZ (tx_desc->clear_state_for_autocommit_retry());
  return ret;
}

}/* ns sql*/
}/* ns oceanbase */
