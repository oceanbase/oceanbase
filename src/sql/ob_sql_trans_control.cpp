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
#include "sql/das/ob_das_dml_ctx_define.h"
#include "share/deadlock/ob_deadlock_detector_mgr.h"

#ifdef CHECK_SESSION
#error "redefine macro CHECK_SESSION"
#else
#define CHECK_SESSION(session) \
  if (OB_SUCC(ret) && session->is_zombie()) {                   \
    ret = OB_ERR_SESSION_INTERRUPTED;                           \
    LOG_WARN("session has been killed", KR(ret), KPC(session)); \
  }
#endif

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
                          transaction::ObTransService *&txs)
{
  int ret = OB_SUCCESS;
  auto effective_tenant_id = session->get_effective_tenant_id();
  if (session->get_tx_desc() != NULL) {
    auto tx_tenant_id = session->get_tx_desc()->get_tenant_id();
    if (effective_tenant_id != tx_tenant_id) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("effective_tenant_id not equals to tx_tenant_id", K(ret), K(effective_tenant_id), K(tx_tenant_id));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(txs = MTL_WITH_CHECK_TENANT(transaction::ObTransService*, effective_tenant_id))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("get_tx_service", K(ret), K(effective_tenant_id), K(MTL_ID()));
    }
  }
  return ret;
}

static inline int get_lock_service(uint64_t tenant_id,
                            transaction::tablelock::ObTableLockService *&lock_service)
{
  int ret = OB_SUCCESS;
  lock_service = MTL_WITH_CHECK_TENANT(transaction::tablelock::ObTableLockService*,
                                       tenant_id);
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
  transaction::ObTransService *txs = NULL;
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
  transaction::ObTransService *txs = NULL;
  uint64_t tenant_id = 0;
  ObTransID tx_id;

  CK (OB_NOT_NULL(plan_ctx), OB_NOT_NULL(session));
  CHECK_SESSION(session);
  if (OB_SUCC(ret) && session->is_in_transaction()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("nested start transaction not allowed", KR(ret), K(ctx));
  }
  OX (tenant_id = session->get_effective_tenant_id());
  OZ (get_tx_service(session, txs), tenant_id);

  if (OB_SUCC(ret) && OB_NOT_NULL(session->get_tx_desc())) {
    auto *tx_desc = session->get_tx_desc();
    if (tx_desc->get_tenant_id() != tenant_id) {
      LOG_ERROR("switch tenant but hold tx_desc", K(tenant_id), KPC(tx_desc));
    }
    txs->release_tx(*tx_desc);
    session->get_tx_desc() = NULL;
  }

  ObTxParam &tx_param = plan_ctx->get_trans_param();
  OZ (build_tx_param_(session, tx_param, &read_only));
  OZ (txs->acquire_tx(session->get_tx_desc(), session->get_sessid()));
  OZ (txs->start_tx(*session->get_tx_desc(), tx_param), tx_param);
  OX (tx_id = session->get_tx_desc()->get_tx_id());
  OX (session->set_explicit_start_trans(true));

  if (OB_FAIL(ret) && OB_NOT_NULL(txs) && OB_NOT_NULL(session->get_tx_desc())) {
    txs->release_tx(*session->get_tx_desc());
    session->get_tx_desc() = NULL;
  }

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
                                          ObEndTransAsyncCallback *callback)
{
  int ret = OB_SUCCESS;
#ifndef NDEBUG
  LOG_INFO("implicit end trans", K(is_rollback), K(exec_ctx.get_execution_id()), KP(callback));
#endif
  int64_t t_id = 0;
  if (OB_ISNULL(GET_MY_SESSION(exec_ctx))) {
    // do nothing
  } else if (OB_ISNULL(GET_MY_SESSION(exec_ctx)->get_tx_desc())) {
    // do nothing
  } else {
    t_id = GET_MY_SESSION(exec_ctx)->get_tx_desc()->tid().get_id();
  }

  FLTSpanGuard(end_transaction);
  OZ(end_trans(exec_ctx, is_rollback, false, callback));

  FLT_SET_TAG(trans_id, t_id);
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

  if (exec_ctx.is_end_trans_async()) {
    CK (OB_NOT_NULL(callback = &session->get_end_trans_cb()));
  }
  OZ (end_trans(exec_ctx, is_rollback, true, callback));
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
                                 ObEndTransAsyncCallback *callback)
{
  int ret = OB_SUCCESS;
  bool sync = false;
  ObSQLSessionInfo *session = GET_MY_SESSION(exec_ctx);
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(exec_ctx);
#ifndef NDEBUG
  LOG_INFO("end_trans", K(session->is_in_transaction()),
                        K(session->has_explicit_start_trans()),
                        K(exec_ctx.get_execution_id()),
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
      reset_session_tx_state(session, true);
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
      reset_session_tx_state(session, reuse_tx);
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
    transaction::ObTxDesc *tx_desc = session.get_tx_desc();
    auto tx_tenant_id = tx_desc->get_tenant_id();
    MTL_SWITCH(tx_tenant_id) {
      transaction::ObTransService *txs = NULL;
      CK(OB_NOT_NULL(txs = MTL_WITH_CHECK_TENANT(transaction::ObTransService*,
                                                 tx_tenant_id)));
      OZ(txs->interrupt(*tx_desc, OB_ERR_QUERY_INTERRUPTED),
         tx_desc->get_tx_id(), status);
      LOG_INFO("kill_query_session", K(ret), K(session), K(tx_desc->get_tx_id()),
               "session_status", status);
    }
  }
  return ret;
}

int ObSqlTransControl::kill_tx(ObSQLSessionInfo *session, int cause)
{
  auto session_id = session->get_sessid();
  LOG_INFO("begin to kill tx", K(cause), K(session_id), KPC(session));
  int ret = OB_SUCCESS;
  if (session->is_in_transaction()) {
    transaction::ObTxDesc *tx_desc = session->get_tx_desc();
    auto tx_tenant_id = tx_desc->get_tenant_id();
    const ObTransID tx_id = tx_desc->get_tx_id();
    MTL_SWITCH(tx_tenant_id) {
      if (tx_desc->is_xa_trans()) {
        if (OB_FAIL(MTL(transaction::ObXAService *)->handle_terminate_for_xa_branch(
                session->get_xid(), tx_desc, session->get_xa_end_timeout_seconds()))) {
          LOG_WARN("rollback xa trans fail", K(ret), K(session_id), KPC(tx_desc));
        }
        session->get_tx_desc() = NULL;
      } else {
        transaction::ObTransService *txs = NULL;
        CK(OB_NOT_NULL(txs = MTL_WITH_CHECK_TENANT(transaction::ObTransService*,
                                                   tx_tenant_id)));
        OZ(txs->abort_tx(*tx_desc, cause), *session, tx_desc->get_tx_id());
      }
      LOG_INFO("kill tx done", K(ret), K(cause), K(session_id), K(tx_id));
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
  transaction::ObTxDesc *&tx_ptr = session->get_tx_desc();
  if (session->is_registered_to_deadlock()) {
    if (OB_SUCC(MTL(share::detector::ObDeadLockDetectorMgr*)->unregister_key(tx_ptr->tid()))) {
      DETECT_LOG(INFO, "unregister deadlock detector in do end trans", KPC(tx_ptr));
    } else {
      DETECT_LOG(WARN, "unregister deadlock detector in do end trans failed", KPC(tx_ptr));
    }
    session->set_registered_to_deadlock(false);
  }
  if (session->associated_xa() && !is_explicit) {
    ret = OB_TRANS_XA_RMFAIL;
  } else {
    /*
     * normal transaction control
     *
     * call convention:
     * if trans_service.end_trans failed:
     * 1) tx will be aborted (if tx exist and not terminated)
     * 2) the callback will not been called
     */
    transaction::ObTransService *txs = NULL;
    uint64_t tenant_id = session->get_effective_tenant_id();
    auto &trace_info = session->get_ob_trace_info();
    if (OB_FAIL(get_tx_service(session, txs))) {
      LOG_ERROR("fail to get trans service", K(ret), K(tenant_id));
    } else if (is_rollback) {
      ret = txs->rollback_tx(*tx_ptr);
    } else if (callback) {
      if (OB_FAIL(inc_session_ref(session))) {
        LOG_WARN("fail to inc session ref", K(ret));
      } else {
        callback->handout();
        if(OB_FAIL(txs->submit_commit_tx(*tx_ptr, expire_ts, *callback, &trace_info))) {
          LOG_WARN("submit commit tx fail", K(ret), KP(callback), K(expire_ts), KPC(tx_ptr));
          GCTX.session_mgr_->revert_session(session);
          callback->handin();
        }
      }
    } else if (OB_FAIL(txs->commit_tx(*tx_ptr, expire_ts, &trace_info))) {
      LOG_WARN("sync commit tx fail", K(ret), K(expire_ts), KPC(tx_ptr));
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
  ObSQLSessionInfo *session = GET_MY_SESSION(exec_ctx);
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(exec_ctx);
  const ObPhysicalPlan *plan = plan_ctx->get_phy_plan();
  ObDASCtx &das_ctx = DAS_CTX(exec_ctx);
  transaction::ObTransService *txs = NULL;
  uint64_t tenant_id = 0;

  CK (OB_NOT_NULL(session), OB_NOT_NULL(plan_ctx), OB_NOT_NULL(plan));
  OX (tenant_id = session->get_effective_tenant_id());
  OX (session->get_trans_result().reset());
  OZ (get_tx_service(session, txs), tenant_id);
  OZ (acquire_tx_if_need_(txs, *session));
  OZ (stmt_sanity_check_(session, plan, plan_ctx));
  OZ (txs->sql_stmt_start_hook(session->get_xid(), *session->get_tx_desc()));
  if (OB_SUCC(ret)
      && txs->get_tx_elr_util().check_and_update_tx_elr_info(
                                         *session->get_tx_desc(),
                                         session->get_early_lock_release())) {
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
  if (OB_SUCC(ret) && !is_plain_select) {
    OZ (stmt_setup_savepoint_(session, das_ctx, plan_ctx, txs, nested_level), session_id, *tx_desc);
  }

  OZ (stmt_setup_snapshot_(session, das_ctx, plan, plan_ctx, txs), session_id, *tx_desc);
  // add tx id to AuditRecord
  OX (session->get_raw_audit_record().trans_id_ = session->get_tx_id());

  // add snapshot info to AuditRecord
  if (OB_SUCC(ret)) {
    ObAuditRecordData &audit_record = session->get_raw_audit_record();
    auto &snapshot = das_ctx.get_snapshot();
    auto &ar_snapshot = audit_record.snapshot_;
    ar_snapshot.version_ = snapshot.core_.version_;
    ar_snapshot.tx_id_ = snapshot.core_.tx_id_.get_id();
    ar_snapshot.scn_ = snapshot.core_.scn_;
    ar_snapshot.source_ = snapshot.get_source_name().ptr();
  }
  if (OB_SUCC(ret) && !session->has_start_stmt()) {
    OZ (session->set_start_stmt());
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
    auto plan_type = plan->get_location_type();
    auto stmt_type = plan->get_stmt_type();
    auto has_for_update = plan->has_for_update();
    auto use_das = plan->use_das();
    auto &trans_result = session->get_trans_result();
    auto query_start_time = session->get_query_start_time();
    auto &snapshot = das_ctx.get_snapshot();
    auto savepoint = das_ctx.get_savepoint();
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

int ObSqlTransControl::stmt_sanity_check_(ObSQLSessionInfo *session,
                                          const ObPhysicalPlan *plan,
                                          ObPhysicalPlanCtx *plan_ctx)
{
  int ret = OB_SUCCESS;
  auto current_consist_level = plan_ctx->get_consistency_level();
  CK (current_consist_level != ObConsistencyLevel::INVALID_CONSISTENCY);
  bool is_plain_select = plan->is_plain_select();

  // adjust stmt's consistency level
  if (OB_SUCC(ret)) {
    // Weak read statement with inner table should be converted to strong read.
    // For example, schema refresh statement;
    if (plan->is_contain_inner_table() ||
       (!is_plain_select && current_consist_level != ObConsistencyLevel::STRONG)) {
      plan_ctx->set_consistency_level(ObConsistencyLevel::STRONG);
    }
  }

  // check isolation with consistency type
  if (OB_SUCC(ret) && session->is_in_transaction()) {
    auto iso = session->get_tx_desc()->get_isolation_level();
    auto cl = plan_ctx->get_consistency_level();
    if (ObConsistencyLevel::WEAK == cl && (iso == ObTxIsolationLevel::SERIAL || iso == ObTxIsolationLevel::RR)) {
      ret = OB_NOT_SUPPORTED;
      TRANS_LOG(ERROR, "statement of weak consistency is not allowed under SERIALIZABLE isolation",
                KR(ret), "trans_id", session->get_tx_id(), "consistency_level", cl);
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "weak consistency under SERIALIZABLE and REPEATABLE-READ isolation level");
    }
  }
  return ret;
}

int ObSqlTransControl::stmt_setup_snapshot_(ObSQLSessionInfo *session,
                                            ObDASCtx &das_ctx,
                                            const ObPhysicalPlan *plan,
                                            const ObPhysicalPlanCtx *plan_ctx,
                                            transaction::ObTransService *txs)
{
  int ret = OB_SUCCESS;
  auto cl = plan_ctx->get_consistency_level();
  auto &snapshot = das_ctx.get_snapshot();
  if (session->get_read_snapshot_version() > 0) {
    snapshot.init_special_read(session->get_read_snapshot_version());
    // Weak read statement with inner table should be converted to strong read.
    // For example, schema refresh statement;
  } else if (cl == ObConsistencyLevel::WEAK || cl == ObConsistencyLevel::FROZEN) {
    int64_t snapshot_version = 0;
    if (OB_FAIL(txs->get_weak_read_snapshot_version(snapshot_version))) {
      TRANS_LOG(WARN, "get weak read snapshot fail", KPC(txs));
    } else {
      snapshot.init_weak_read(snapshot_version);
    }
    // 1) acquire snapshot verison when insert operator is executed
    // 2) don't resolve RR and SERIALIZABLE isolation scenario temporarily, becouse of remote stmt plan
  } else if (plan->is_plain_insert()
          && session->get_tx_isolation() != ObTxIsolationLevel::SERIAL
          && session->get_tx_isolation() != ObTxIsolationLevel::RR) {
    snapshot.init_none_read();
  } else {
    auto &tx_desc = *session->get_tx_desc();
    int64_t stmt_expire_ts = get_stmt_expire_ts(plan_ctx, *session);
    share::ObLSID local_ls_id;
    bool local_single_ls_plan = plan->is_local_plan()
      && OB_PHY_PLAN_LOCAL == plan->get_location_type()
      && das_ctx.has_same_lsid(&local_ls_id);
    if (local_single_ls_plan && !tx_desc.is_can_elr()) {
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
  return ret;
}

int ObSqlTransControl::stmt_refresh_snapshot(ObExecContext &exec_ctx) {
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = GET_MY_SESSION(exec_ctx);
  ObDASCtx &das_ctx = DAS_CTX(exec_ctx);
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(exec_ctx);
  const ObPhysicalPlan *plan = plan_ctx->get_phy_plan();
  transaction::ObTransService *txs = NULL;
  if (sql::stmt::T_INSERT == plan->get_stmt_type() || sql::stmt::T_INSERT_ALL == plan->get_stmt_type()) {
    //NOTE: oracle insert and insert all stmt can't see the evaluated results of before stmt trigger, no need to refresh snapshot
  } else if (OB_FAIL(get_tx_service(session, txs))) {
    LOG_WARN("failed to get transaction service", K(ret));
  } else if (OB_FAIL(stmt_setup_snapshot_(session, das_ctx, plan, plan_ctx, txs))) {
    LOG_WARN("failed to set sanpshot", K(ret));
  }
  return ret;
}

int ObSqlTransControl::stmt_setup_savepoint_(ObSQLSessionInfo *session,
                                             ObDASCtx &das_ctx,
                                             ObPhysicalPlanCtx *plan_ctx,
                                             transaction::ObTransService* txs,
                                             const int64_t nested_level)
{
  int ret = OB_SUCCESS;
  ObTxParam &tx_param = plan_ctx->get_trans_param();
  OZ (build_tx_param_(session, tx_param));
  auto &tx = *session->get_tx_desc();
  int64_t savepoint = 0;
  OZ (txs->create_implicit_savepoint(tx, tx_param, savepoint, nested_level == 0), tx, tx_param);
  OX (das_ctx.set_savepoint(savepoint));
  return ret;
}

int ObSqlTransControl::create_savepoint(ObExecContext &exec_ctx,
                                        const ObString &sp_name)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = GET_MY_SESSION(exec_ctx);
  transaction::ObTransService *txs = NULL;
  CK (OB_NOT_NULL(session));
  CHECK_SESSION (session);
  OZ (get_tx_service(session, txs));
  OZ (acquire_tx_if_need_(txs, *session));
  OZ (txs->create_explicit_savepoint(*session->get_tx_desc(), sp_name), sp_name);
  return ret;
}

int ObSqlTransControl::rollback_savepoint(ObExecContext &exec_ctx,
                                          const ObString &sp_name)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = GET_MY_SESSION(exec_ctx);
  const ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(exec_ctx);
  transaction::ObTransService *txs = NULL;
  int64_t stmt_expire_ts = 0;

  CK (OB_NOT_NULL(session), OB_NOT_NULL(plan_ctx));
  CHECK_SESSION (session);
  OZ (get_tx_service(session, txs));
  OZ (acquire_tx_if_need_(txs, *session));
  OX (stmt_expire_ts = get_stmt_expire_ts(plan_ctx, *session));
  OZ (txs->rollback_to_explicit_savepoint(*session->get_tx_desc(), sp_name, stmt_expire_ts), sp_name);
  return ret;
}

int ObSqlTransControl::release_savepoint(ObExecContext &exec_ctx,
                                         const ObString &sp_name)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = GET_MY_SESSION(exec_ctx);
  transaction::ObTransService *txs = NULL;
  CK (OB_NOT_NULL(session));
  CHECK_SESSION (session);
  OZ (get_tx_service(session, txs), *session);
  OZ (acquire_tx_if_need_(txs, *session));
  OZ (txs->release_explicit_savepoint(*session->get_tx_desc(), sp_name), *session, sp_name);
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
  transaction::ObXAService * xa_service = MTL(transaction::ObXAService*);
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
  ObSQLSessionInfo *session = GET_MY_SESSION(exec_ctx);
  auto *plan_ctx = GET_PHY_PLAN_CTX(exec_ctx);
  const ObPhysicalPlan *plan = NULL;
  ObDASCtx &das_ctx = DAS_CTX(exec_ctx);
  transaction::ObTransService *txs = NULL;
  transaction::ObTxDesc *tx_desc = NULL;
  sql::stmt::StmtType stmt_type = sql::stmt::StmtType::T_NONE;
  bool is_plain_select = false;
  int64_t savepoint = das_ctx.get_savepoint();

  CK (OB_NOT_NULL(session), OB_NOT_NULL(plan_ctx));
  CK (OB_NOT_NULL(plan = plan_ctx->get_phy_plan()));
  OX (tx_desc = session->get_tx_desc());
  OX (stmt_type = plan->get_stmt_type());
  OX (is_plain_select = plan->is_plain_select());
  OZ (get_tx_service(session, txs), *session);
  // plain select stmt don't require txn descriptor
  if (OB_SUCC(ret) && !is_plain_select) {
    CK (OB_NOT_NULL(tx_desc));
    auto &tx_result = session->get_trans_result();
    if (OB_FAIL(ret)) {
    } else if (tx_result.is_incomplete()) {
      if (!rollback) {
        LOG_ERROR("trans result incomplete, but rollback not issued");
      }
      OZ (txs->abort_tx(*tx_desc, ObTxAbortCause::TX_RESULT_INCOMPLETE));
      ret = OB_TRANS_NEED_ROLLBACK;
      LOG_WARN("trans result incomplete, trans aborted", K(ret));
    } else if (rollback) {
      auto stmt_expire_ts = get_stmt_expire_ts(plan_ctx, *session);
      auto &touched_ls = tx_result.get_touched_ls();
      OZ (txs->rollback_to_implicit_savepoint(*tx_desc, savepoint, stmt_expire_ts, &touched_ls),
          savepoint, stmt_expire_ts, touched_ls);
    }
  }
  // call end stmt hook
  if (OB_NOT_NULL(tx_desc) && OB_NOT_NULL(txs) && OB_NOT_NULL(session)) {
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

  if (!is_plain_select) {
    ObTransDeadlockDetectorAdapter::maintain_deadlock_info_when_end_stmt(exec_ctx, rollback);
  }

  bool print_log = false;
#ifndef NDEBUG
  print_log = true;
#else
  if (OB_FAIL(ret) || rollback) { print_log = true; }
#endif
  if (print_log && OB_NOT_NULL(session)) {
    LOG_INFO("end stmt", K(ret),
             "plain_select", is_plain_select,
             "stmt_type", stmt_type,
             K(savepoint),
             "tx_desc", PC(session->get_tx_desc()),
             "trans_result", session->get_trans_result(),
             K(rollback),
             KPC(session));
  }
  if (OB_NOT_NULL(session)) {
    session->get_trans_result().reset();
  }
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

int ObSqlTransControl::create_anonymous_savepoint(ObExecContext &exec_ctx, int64_t &savepoint)
{
  int ret = OB_SUCCESS;
  transaction::ObTransService *txs = NULL;
  ObSQLSessionInfo *session = GET_MY_SESSION(exec_ctx);
  CK (OB_NOT_NULL(session));
  OZ (get_tx_service(session, txs));
  ObTxParam tx_param;
  OZ (build_tx_param_(session, tx_param));
  OZ (txs->create_implicit_savepoint(*session->get_tx_desc(), tx_param, savepoint), *session->get_tx_desc());
  return ret;
}

int ObSqlTransControl::create_anonymous_savepoint(transaction::ObTxDesc &tx_desc, int64_t &savepoint)
{
  int ret = OB_SUCCESS;
  transaction::ObTransService *txs = NULL;
  if (OB_ISNULL(txs = MTL_WITH_CHECK_TENANT(transaction::ObTransService*, tx_desc.get_tenant_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get_tx_service", K(ret), K(tx_desc.get_tenant_id()), K(MTL_ID()));
  }
  OZ (txs->create_in_txn_implicit_savepoint(tx_desc, savepoint));
  return ret;
}

int ObSqlTransControl::rollback_savepoint(ObExecContext &exec_ctx, const int64_t savepoint)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = GET_MY_SESSION(exec_ctx);
  const ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(exec_ctx);
  transaction::ObTransService *txs = NULL;
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
int ObSqlTransControl::get_trans_result(ObExecContext &exec_ctx,
                                        transaction::ObTxExecResult &trans_result)
{
  int ret = OB_SUCCESS;
  transaction::ObTransService *txs = NULL;
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

int ObSqlTransControl::reset_session_tx_state(ObBasicSessionInfo *session, bool reuse_tx_desc)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("reset session tx state", KPC(session->get_tx_desc()), K(lbt()));
  if (OB_NOT_NULL(session->get_tx_desc())) {
    auto &tx_desc = *session->get_tx_desc();
    auto effect_tid = session->get_effective_tenant_id();
    MTL_SWITCH(effect_tid) {
      transaction::ObTransService *txs = NULL;
      OZ (get_tx_service(session, txs), *session, tx_desc);
      if (reuse_tx_desc) {
        if (OB_FAIL(txs->reuse_tx(tx_desc))) {
          LOG_ERROR("reuse txn descriptor fail, will release it", K(ret), KPC(session), K(tx_desc));
          OZ (txs->release_tx(tx_desc));
          OX (session->get_tx_desc() = NULL);
        }
      } else {
        OZ (txs->release_tx(tx_desc), *session, tx_desc);
        OX (session->get_tx_desc() = NULL);
      }
    }
  }
  session->reset_first_need_txn_stmt_type();
  session->get_trans_result().reset();
  session->reset_tx_variable();
  return ret;
}

int ObSqlTransControl::reset_session_tx_state(ObSQLSessionInfo *session, bool reuse_tx_desc)
{
  int temp_ret = OB_SUCCESS;
  // cleanup temp tables created during txn progress
  if (session->has_tx_level_temp_table()) {
    temp_ret = session->drop_temp_tables(false);
    if (OB_SUCCESS != temp_ret) {
      LOG_WARN("trx level temporary table clean failed", KR(temp_ret));
    }
  }
  int ret = reset_session_tx_state(static_cast<ObBasicSessionInfo*>(session), reuse_tx_desc);
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

int ObSqlTransControl::acquire_tx_if_need_(transaction::ObTransService *txs, ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session.get_tx_desc())) {
    OZ (txs->acquire_tx(session.get_tx_desc(), session.get_sessid()), session);
  }
  return ret;
}

int ObSqlTransControl::lock_table(ObExecContext &exec_ctx,
                                  const uint64_t table_id,
                                  const ObTableLockMode lock_mode)
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
    OZ (txs->acquire_tx(session->get_tx_desc(), session->get_sessid()), *session);
  }
  ObTxParam tx_param;
  OZ (build_tx_param_(session, tx_param));
  // calculate lock table timeout
  int64_t lock_timeout_us = 0;
  {
    int64_t stmt_expire_ts = 0;
    int64_t tx_expire_ts = 0;
    OX (stmt_expire_ts = get_stmt_expire_ts(plan_ctx, *session));
    OZ (get_trans_expire_ts(*session, tx_expire_ts));
    OX (lock_timeout_us = MAX(200L, MIN(stmt_expire_ts, tx_expire_ts) - ObTimeUtility::current_time()));
  }
  OZ (lock_service->lock_table(*session->get_tx_desc(),
                               tx_param,
                               table_id,
                               lock_mode,
                               lock_timeout_us),
      tx_param, table_id, lock_mode, lock_timeout_us);
  return ret;
}

void ObSqlTransControl::clear_xa_branch(const ObXATransID &xid, ObTxDesc *&tx_desc)
{
  MTL(transaction::ObXAService *)->clear_xa_branch(xid, tx_desc);
}

}/* ns sql*/
}/* ns oceanbase */
