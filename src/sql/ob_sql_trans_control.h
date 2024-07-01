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

#ifndef OCEANBASE_SQL_TRANS_CONTROL_
#define OCEANBASE_SQL_TRANS_CONTROL_
#include "share/ob_define.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/tx/ob_trans_deadlock_adapter.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/session/ob_sql_session_mgr.h"
#include "storage/tx/ob_clog_encrypt_info.h"
#include "storage/tablelock/ob_table_lock_common.h"

namespace oceanbase
{
namespace transaction
{
class ObStartTransParam;
class ObTxDesc;
}

namespace share
{
namespace schema
{
class ObTableSchema;
}
}
namespace sql
{
class ObExecContext;
class ObPhysicalPlan;
class ObPhyOperator;
class ObStmt;
class ObPhysicalPlanCtx;
class ObSQLSessionInfo;
class ObIEndTransCallback;
class ObEndTransAsyncCallback;
class ObNullEndTransCallback;
class ObIDASTaskOp;

class TransState
{
private:
  /* 两位表示一个动作：低位表示是否执行，高位表示是否成功 */
  static const uint32_t START_TRANS_EXECUTED_BIT   = (1 << 0);
  static const uint32_t END_TRANS_EXECUTED_BIT     = (1 << 2);
  static const uint32_t START_STMT_EXECUTED_BIT    = (1 << 4);
  static const uint32_t END_STMT_EXECUTED_BIT      = (1 << 6);
  static const uint32_t START_PART_EXECUTED_BIT    = (1 << 8);
  static const uint32_t END_PART_EXECUTED_BIT      = (1 << 10);
  static const uint32_t START_TRANS_SUCC_BIT       = (1 << 1);
  static const uint32_t END_TRANS_SUCC_BIT         = (1 << 3);
  static const uint32_t START_STMT_SUCC_BIT        = (1 << 5);
  static const uint32_t END_STMT_SUCC_BIT          = (1 << 7);
  static const uint32_t START_PART_SUCC_BIT        = (1 << 9);
  static const uint32_t END_PART_SUCC_BIT          = (1 << 11);
  static const uint32_t START_TRANS_EXECUTED_SHIFT = 1;
  static const uint32_t END_TRANS_EXECUTED_SHIFT   = 3;
  static const uint32_t START_STMT_EXECUTED_SHIFT  = 5;
  static const uint32_t END_STMT_EXECUTED_SHIFT    = 7;
  static const uint32_t START_PART_EXECUTED_SHIFT  = 9;
  static const uint32_t END_PART_EXECUTED_SHIFT    = 11;
  static const uint32_t START_TRANS_EXECUTED_MASK = (0xFFFFFFFF ^ (0x3 << 0));
  static const uint32_t END_TRANS_EXECUTED_MASK   = (0xFFFFFFFF ^ (0x3 << 2));
  static const uint32_t START_STMT_EXECUTED_MASK  = (0xFFFFFFFF ^ (0x3 << 4));
  static const uint32_t END_STMT_EXECUTED_MASK    = (0xFFFFFFFF ^ (0x3 << 6));
  static const uint32_t START_PART_EXECUTED_MASK  = (0xFFFFFFFF ^ (0x3 << 8));
  static const uint32_t END_PART_EXECUTED_MASK    = (0xFFFFFFFF ^ (0x3 << 10));
public:
  TransState() : state_(0) {}
  ~TransState() {}
  void set_start_trans_executed(bool is_succ)
  { state_ = ((state_ | START_TRANS_EXECUTED_BIT) | (is_succ << START_TRANS_EXECUTED_SHIFT)); }
  void set_end_trans_executed(bool is_succ)
  { state_ = ((state_ | END_TRANS_EXECUTED_BIT) | (is_succ << END_TRANS_EXECUTED_SHIFT)); }
  void set_start_stmt_executed(bool is_succ)
  { state_ = ((state_ | START_STMT_EXECUTED_BIT) | (is_succ << START_STMT_EXECUTED_SHIFT)); }
  void set_end_stmt_executed(bool is_succ)
  { state_ = ((state_ | END_STMT_EXECUTED_BIT) | (is_succ << END_STMT_EXECUTED_SHIFT)); }
  void set_start_participant_executed(bool is_succ)
  { state_ = ((state_ | START_PART_EXECUTED_BIT) | (is_succ << START_PART_EXECUTED_SHIFT)); }
  void set_end_participant_executed(bool is_succ)
  { state_ = ((state_ | END_PART_EXECUTED_BIT) | (is_succ << END_PART_EXECUTED_SHIFT)); }

  void clear_start_trans_executed()
  { state_ = (state_ & START_TRANS_EXECUTED_MASK); }
  void clear_start_stmt_executed()
  { state_ = (state_ & START_STMT_EXECUTED_MASK); }
  void clear_start_participant_executed()
  { state_ = (state_ & START_PART_EXECUTED_MASK); }
  void clear_end_trans_executed()
  { state_ = (state_ & END_TRANS_EXECUTED_MASK); }
  void clear_end_stmt_executed()
  { state_ = (state_ & END_STMT_EXECUTED_MASK); }
  void clear_end_participant_executed()
  { state_ = (state_ & END_PART_EXECUTED_MASK); }

  bool is_start_trans_executed() const
  { return state_ & START_TRANS_EXECUTED_BIT; }
  bool is_end_trans_executed() const
  { return state_ & END_TRANS_EXECUTED_BIT; }
  bool is_start_stmt_executed() const
  { return state_ & START_STMT_EXECUTED_BIT; }
  bool is_end_stmt_executed() const
  { return state_ & END_STMT_EXECUTED_BIT; }
  bool is_start_participant_executed() const
  { return state_ & START_PART_EXECUTED_BIT; }
  bool is_end_participant_executed() const
  { return state_ & END_PART_EXECUTED_BIT; }
  bool is_start_trans_success() const
  { return state_ & START_TRANS_SUCC_BIT; }
  bool is_end_trans_success() const
  { return state_ & END_TRANS_SUCC_BIT; }
  bool is_start_stmt_success() const
  { return state_ & START_STMT_SUCC_BIT; }
  bool is_end_stmt_success() const
  { return state_ & END_STMT_SUCC_BIT; }
  bool is_start_participant_success() const
  { return state_ & START_PART_SUCC_BIT; }
  bool is_end_participant_success() const
  { return state_ & END_PART_SUCC_BIT; }

  void reset()
  { state_ = 0; }

private:
  uint32_t state_;
  // cached for start_stmt, start_participants, end_participants
};

/* 内部类，仅用于本文件。将SQL层的Consistency转化为事务层的Consistency。
 * 事务层对Consistency只有STRONg/WEAK的概念，没有FROZEN的概念。
 **/
class ObConsistencyLevelAdaptor
{
public:
  explicit ObConsistencyLevelAdaptor(common::ObConsistencyLevel sql_consistency) {
    switch(sql_consistency) {
      case common::STRONG:
        trans_consistency_ = transaction::ObTransConsistencyLevel::STRONG;
        break;
      case common::WEAK:
      case common::FROZEN:
        trans_consistency_ = transaction::ObTransConsistencyLevel::WEAK;
        break;
      default:
        trans_consistency_ = transaction::ObTransConsistencyLevel::UNKNOWN;
    }
  }
  int64_t get_consistency() {
    return trans_consistency_;
  }
private:
  int64_t trans_consistency_;
};

class ObSqlTransControl
{
public:
  static int reset_session_tx_state(ObSQLSessionInfo *session, bool reuse_tx_desc = false, bool active_tx_end = true);
  static int reset_session_tx_state(ObBasicSessionInfo *session,
                                    bool reuse_tx_desc = false,
                                    bool active_tx_end = true,
                                    const uint64_t data_version = 0);
  static int create_stash_savepoint(ObExecContext &exec_ctx, const ObString &name);
  static int release_stash_savepoint(ObExecContext &exec_ctx, const ObString &name);
  static int explicit_start_trans(ObExecContext &exec_ctx, const bool read_only, const ObString hint = ObString());
  static int explicit_end_trans(ObExecContext &exec_ctx, const bool is_rollback, const ObString hint = ObString());
  static int implicit_end_trans(ObExecContext &exec_ctx,
                                const bool is_rollback,
                                ObEndTransAsyncCallback *callback = NULL,
                                bool reset_trans_variable = true);
  static int end_trans(ObExecContext &exec_ctx,
                       const bool is_rollback,
                       const bool is_explicit,
                       ObEndTransAsyncCallback *callback = NULL,
                       bool reset_trans_variable = true);
  static int rollback_trans(ObSQLSessionInfo *session,
                            bool &need_disconnect);
  static int do_end_trans_(ObSQLSessionInfo *session,
                           const bool is_rollback,
                           const bool is_explicit,
                           const int64_t expire_ts,
                           ObEndTransAsyncCallback *callback);
  static int start_stmt(ObExecContext &ctx);
  static int dblink_xa_prepare(ObExecContext &exec_ctx);
  static int stmt_sanity_check_(ObSQLSessionInfo *session,
                                const ObPhysicalPlan *plan,
                                ObPhysicalPlanCtx *plan_ctx);
  static int stmt_setup_snapshot_(ObSQLSessionInfo *session,
                                  ObDASCtx &das_ctx,
                                  const ObPhysicalPlan *plan,
                                  const ObPhysicalPlanCtx *plan_ctx,
                                  transaction::ObTransService *txs);
  static int stmt_refresh_snapshot(ObExecContext &ctx);
  static int set_fk_check_snapshot(ObExecContext &exec_ctx);
  static int stmt_setup_savepoint_(ObSQLSessionInfo *session,
                                   ObDASCtx &das_ctx,
                                   ObPhysicalPlanCtx *plan_ctx,
                                   transaction::ObTransService* txs,
                                   const int64_t nested_level);
  static int end_stmt(ObExecContext &exec_ctx, const bool is_rollback);
  static int alloc_branch_id(ObExecContext &exec_ctx, const int64_t count, int16_t &branch_id);
  static int kill_query_session(ObSQLSessionInfo &session, const ObSQLSessionState &status);
  static int kill_tx(ObSQLSessionInfo *session, int cause);
  static int kill_idle_timeout_tx(ObSQLSessionInfo *session);
  static int kill_deadlock_tx(ObSQLSessionInfo *session)
  {
    using namespace oceanbase::transaction;
    return kill_tx(session, OB_DEAD_LOCK);
  }
  static int kill_tx_on_session_killed(ObSQLSessionInfo *session)
  {
    using namespace oceanbase::transaction;
    return kill_tx(session, OB_SESSION_KILLED);
  }
  static int kill_tx_on_session_disconnect(ObSQLSessionInfo *session)
  {
    using namespace oceanbase::transaction;
    return kill_tx(session, static_cast<int>(ObTxAbortCause::SESSION_DISCONNECT));
  }
  static int create_savepoint(ObExecContext &exec_ctx, const common::ObString &sp_name, const bool user_create = false);
  static int rollback_savepoint(ObExecContext &exec_ctx, const common::ObString &sp_name);
  static int release_savepoint(ObExecContext &exec_ctx, const common::ObString &sp_name);
  static int xa_rollback_all_changes(ObExecContext &exec_ctx);
public:
  static int decide_trans_read_interface_specs(
    const common::ObConsistencyLevel &sql_consistency_level,
    transaction::ObTxConsistencyType &trans_consistency_type);
  static bool is_isolation_RR_or_SE(transaction::ObTxIsolationLevel isolation);
  static int get_trans_result(ObExecContext &exec_ctx);
  static int get_trans_result(ObExecContext &exec_ctx, transaction::ObTxExecResult &trans_result);
  static int lock_table(ObExecContext &exec_ctx,
                        const uint64_t table_id,
                        const ObIArray<ObObjectID> &part_ids,
                        const transaction::tablelock::ObTableLockMode lock_mode,
                        const int64_t wait_lock_seconds);
  static void clear_xa_branch(const transaction::ObXATransID &xid, transaction::ObTxDesc *&tx_desc);
  static int check_ls_readable(const uint64_t tenant_id,
                               const share::ObLSID &ls_id,
                               const common::ObAddr &addr,
                               const int64_t max_stale_time_us,
                               bool &can_read);
private:
  DISALLOW_COPY_AND_ASSIGN(ObSqlTransControl);
  static int get_trans_expire_ts(const ObSQLSessionInfo &session,
                                         int64_t &trans_timeout_ts);
  static int64_t get_stmt_expire_ts(const ObPhysicalPlanCtx *plan_ctx,
                                           const ObSQLSessionInfo &session);
  static int inc_session_ref(const ObSQLSessionInfo *session);
  static int acquire_tx_if_need_(transaction::ObTransService *txs, ObSQLSessionInfo &session);
  static int start_hook_if_need_(ObSQLSessionInfo &session,
                                 transaction::ObTransService *txs,
                                 bool &start_hook);
  static uint32_t get_real_session_id(ObSQLSessionInfo &session);
  static int get_first_lsid(const ObDASCtx &das_ctx, share::ObLSID &first_lsid, bool &is_single_tablet);
  static bool has_same_lsid(const ObDASCtx &das_ctx,
                            const transaction::ObTxReadSnapshot &snapshot,
                            share::ObLSID &first_lsid);
public:
  /*
   * create a savepoint without name
   * it was extremely lighweight and fast
   *
   * capability:
   *   only support inner stmt savepoint, and can not been used to cross stmt rollback
   */
  static int create_anonymous_savepoint(ObExecContext &exec_ctx, transaction::ObTxSEQ &savepoint);
  static int create_anonymous_savepoint(transaction::ObTxDesc &tx_desc, transaction::ObTxSEQ &savepoint);
  /*
   * rollback to savepoint
   *
   * [convention]:
   *   transaction layer use trans_result (which maintained by SQL-engine) to decide rollback participants
   *   therefore if trans_result not been collected completed, trans_result.incomplete flag must be set
   *   before do rollback.
   *   and if trans_result was incomplete, SQL-engine should pass the participants to transaction layer
   *   (the participants was calculated from table-locations inner this function)
   *
   * for example: the sql-task executed timeout and its result was unknown, and then do rollback_savepoint;
   *   in this case, the trans_result was incomplete, the flag must been set.
   */
  static int rollback_savepoint(ObExecContext &exec_ctx, const transaction::ObTxSEQ savepoint);

  //
  // Transaction free route relative
  //
#define SQL_TRANS_CONTROL_TXN_FREE_ROUTE_INTERFACE_(name)               \
  /* called when receive request to update txn state */                 \
  static int update_txn_##name##_state(ObSQLSessionInfo &session, const char* buf, const int64_t len, int64_t &pos); \
  /* called when response client to serialize txn state which has changed */ \
  static int64_t get_txn_##name##_state_serialize_size(ObSQLSessionInfo &session); \
  static int serialize_txn_##name##_state(ObSQLSessionInfo &session, char* buf, const int64_t len, int64_t &pos); \
  static int64_t get_fetch_txn_##name##_state_size(ObSQLSessionInfo& sess); \
  static int fetch_txn_##name##_state(ObSQLSessionInfo &sess, char *buf, const int64_t length, int64_t &pos); \
  static int cmp_txn_##name##_state(const char* cur_buf, int64_t cur_len, const char* last_buf, int64_t last_len); \
  static void display_txn_##name##_state(ObSQLSessionInfo &sess, const char* cur_buf, const int64_t cur_len, const char* last_buf, const int64_t last_len);
#define SQL_TRANS_CONTROL_TXN_FREE_ROUTE_INTERFACE(name)  SQL_TRANS_CONTROL_TXN_FREE_ROUTE_INTERFACE_(name)
  LST_DO(SQL_TRANS_CONTROL_TXN_FREE_ROUTE_INTERFACE, (), static, dynamic, parts, extra)
  // called when response client to decide whether need allow free route and whether state need to be returned
  static int calc_txn_free_route(ObSQLSessionInfo &session, transaction::ObTxnFreeRouteCtx &txn_free_route_ctx);
  static int check_free_route_tx_alive(ObSQLSessionInfo &session, transaction::ObTxnFreeRouteCtx &txn_free_rotue_ctx);

  // when lock conflict, stmt will do retry, we do not rollback current transaction
  // but clean the transaction level snapshot it exist
  static int reset_trans_for_autocommit_lock_conflict(ObExecContext &exec_ctx);
};

inline int ObSqlTransControl::get_trans_expire_ts(const ObSQLSessionInfo &my_session,
                                                  int64_t &trans_timeout_ts)
{
  int ret = common::OB_SUCCESS;
  int64_t tx_timeout = 0;
  if (OB_FAIL(my_session.get_tx_timeout(tx_timeout))) {
    SQL_LOG(WARN, "fail to get tx timeout", K(ret));
  } else {
    trans_timeout_ts = my_session.get_query_start_time() + tx_timeout;
  }
  return ret;
}

inline int64_t ObSqlTransControl::get_stmt_expire_ts(const ObPhysicalPlanCtx *plan_ctx,
                                                     const ObSQLSessionInfo &session)
{
  // NOTE: this is QUERY's timeout setting(not TRANSACTION's timeout),
  // either from Hint 'query_timeout' or from session's query_timeout setting.
  if (OB_NOT_NULL(plan_ctx) && OB_NOT_NULL(plan_ctx->get_phy_plan())) {
    // if plan not null, it is a Query, otherwise it is a Command
    return plan_ctx->get_trans_timeout_timestamp();
  }
  int ret = common::OB_SUCCESS;
  int64_t query_timeout = 0;
  if (OB_FAIL(session.get_query_timeout(query_timeout))) {
    SQL_LOG(ERROR, "fail to get query timeout", K(ret));
    query_timeout = 1000 * 1000; // default timeout 1s
  }
  return session.get_query_start_time() + query_timeout;
}

}
}
#endif /* OCEANBASE_SQL_TRANS_CONTROL_ */
//// end of header file
