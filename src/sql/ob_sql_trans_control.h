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
#include "common/ob_partition_key.h"
#include "storage/transaction/ob_trans_define.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/session/ob_sql_session_mgr.h"

namespace oceanbase {
namespace transaction {
class ObStartTransParam;
class ObTransDesc;
}  // namespace transaction
namespace storage {
class ObPartitionService;
}

namespace share {
namespace schema {
class ObTableSchema;
}
}  // namespace share
namespace sql {
class ObExecContext;
class ObPhysicalPlan;
class ObPhyOperator;
class ObStmt;
class ObPhysicalPlanCtx;
class ObSQLSessionInfo;
class ObIEndTransCallback;
class ObNullEndTransCallback;
class ObExclusiveEndTransCallback;

class TransState {
private:
  static const uint32_t START_TRANS_EXECUTED_BIT = (1 << 0);
  static const uint32_t END_TRANS_EXECUTED_BIT = (1 << 2);
  static const uint32_t START_STMT_EXECUTED_BIT = (1 << 4);
  static const uint32_t END_STMT_EXECUTED_BIT = (1 << 6);
  static const uint32_t START_PART_EXECUTED_BIT = (1 << 8);
  static const uint32_t END_PART_EXECUTED_BIT = (1 << 10);
  static const uint32_t START_TRANS_SUCC_BIT = (1 << 1);
  static const uint32_t END_TRANS_SUCC_BIT = (1 << 3);
  static const uint32_t START_STMT_SUCC_BIT = (1 << 5);
  static const uint32_t END_STMT_SUCC_BIT = (1 << 7);
  static const uint32_t START_PART_SUCC_BIT = (1 << 9);
  static const uint32_t END_PART_SUCC_BIT = (1 << 11);
  static const uint32_t START_TRANS_EXECUTED_SHIFT = 1;
  static const uint32_t END_TRANS_EXECUTED_SHIFT = 3;
  static const uint32_t START_STMT_EXECUTED_SHIFT = 5;
  static const uint32_t END_STMT_EXECUTED_SHIFT = 7;
  static const uint32_t START_PART_EXECUTED_SHIFT = 9;
  static const uint32_t END_PART_EXECUTED_SHIFT = 11;
  static const uint32_t START_TRANS_EXECUTED_MASK = (0xFFFFFFFF ^ (0x3 << 0));
  static const uint32_t END_TRANS_EXECUTED_MASK = (0xFFFFFFFF ^ (0x3 << 2));
  static const uint32_t START_STMT_EXECUTED_MASK = (0xFFFFFFFF ^ (0x3 << 4));
  static const uint32_t END_STMT_EXECUTED_MASK = (0xFFFFFFFF ^ (0x3 << 6));
  static const uint32_t START_PART_EXECUTED_MASK = (0xFFFFFFFF ^ (0x3 << 8));
  static const uint32_t END_PART_EXECUTED_MASK = (0xFFFFFFFF ^ (0x3 << 10));

public:
  TransState() : state_(0)
  {}
  ~TransState()
  {}
  void set_start_trans_executed(bool is_succ)
  {
    state_ = ((state_ | START_TRANS_EXECUTED_BIT) | (is_succ << START_TRANS_EXECUTED_SHIFT));
  }
  void set_end_trans_executed(bool is_succ)
  {
    state_ = ((state_ | END_TRANS_EXECUTED_BIT) | (is_succ << END_TRANS_EXECUTED_SHIFT));
  }
  void set_start_stmt_executed(bool is_succ)
  {
    state_ = ((state_ | START_STMT_EXECUTED_BIT) | (is_succ << START_STMT_EXECUTED_SHIFT));
  }
  void set_end_stmt_executed(bool is_succ)
  {
    state_ = ((state_ | END_STMT_EXECUTED_BIT) | (is_succ << END_STMT_EXECUTED_SHIFT));
  }
  void set_start_participant_executed(bool is_succ)
  {
    state_ = ((state_ | START_PART_EXECUTED_BIT) | (is_succ << START_PART_EXECUTED_SHIFT));
  }
  void set_end_participant_executed(bool is_succ)
  {
    state_ = ((state_ | END_PART_EXECUTED_BIT) | (is_succ << END_PART_EXECUTED_SHIFT));
  }

  void clear_start_trans_executed()
  {
    state_ = (state_ & START_TRANS_EXECUTED_MASK);
  }
  void clear_start_stmt_executed()
  {
    state_ = (state_ & START_STMT_EXECUTED_MASK);
  }
  void clear_start_participant_executed()
  {
    state_ = (state_ & START_PART_EXECUTED_MASK);
  }
  void clear_end_trans_executed()
  {
    state_ = (state_ & END_TRANS_EXECUTED_MASK);
  }
  void clear_end_stmt_executed()
  {
    state_ = (state_ & END_STMT_EXECUTED_MASK);
  }
  void clear_end_participant_executed()
  {
    state_ = (state_ & END_PART_EXECUTED_MASK);
  }

  bool is_start_trans_executed() const
  {
    return state_ & START_TRANS_EXECUTED_BIT;
  }
  bool is_end_trans_executed() const
  {
    return state_ & END_TRANS_EXECUTED_BIT;
  }
  bool is_start_stmt_executed() const
  {
    return state_ & START_STMT_EXECUTED_BIT;
  }
  bool is_end_stmt_executed() const
  {
    return state_ & END_STMT_EXECUTED_BIT;
  }
  bool is_start_participant_executed() const
  {
    return state_ & START_PART_EXECUTED_BIT;
  }
  bool is_end_participant_executed() const
  {
    return state_ & END_PART_EXECUTED_BIT;
  }
  bool is_start_trans_success() const
  {
    return state_ & START_TRANS_SUCC_BIT;
  }
  bool is_end_trans_success() const
  {
    return state_ & END_TRANS_SUCC_BIT;
  }
  bool is_start_stmt_success() const
  {
    return state_ & START_STMT_SUCC_BIT;
  }
  bool is_end_stmt_success() const
  {
    return state_ & END_STMT_SUCC_BIT;
  }
  bool is_start_participant_success() const
  {
    return state_ & START_PART_SUCC_BIT;
  }
  bool is_end_participant_success() const
  {
    return state_ & END_PART_SUCC_BIT;
  }

  void reset()
  {
    state_ = 0;
  }

  const common::ObPartitionArray& get_participants() const
  {
    return participants_;
  }
  common::ObPartitionArray& get_participants()
  {
    return participants_;
  }

private:
  uint32_t state_;
  // cached for start_stmt, start_participants, end_participants
  common::ObPartitionArray participants_;
};

class ObConsistencyLevelAdaptor {
public:
  explicit ObConsistencyLevelAdaptor(common::ObConsistencyLevel sql_consistency)
  {
    switch (sql_consistency) {
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
  int64_t get_consistency()
  {
    return trans_consistency_;
  }

private:
  int64_t trans_consistency_;
};

class ObSqlTransControl {
public:
  static int on_plan_start(ObExecContext& exec_ctx, bool is_remote = false);
  // static int on_plan_end(ObExecContext &exec_ctx, bool is_rollback, ObExclusiveEndTransCallback &callback);

  static int explicit_start_trans(ObExecContext& exec_ctx, const bool read_only);
  static int explicit_start_trans(ObExecContext& exec_ctx, transaction::ObStartTransParam& start_trans_param);
  static int implicit_end_trans(ObExecContext& exec_ctx, const bool is_rollback, ObExclusiveEndTransCallback& callback);
  static int explicit_end_trans(ObExecContext& exec_ctx, const bool is_rollback);
  static int explicit_end_trans(ObExecContext& exec_ctx, const bool is_rollback, ObEndTransSyncCallback& callback);
  static int explicit_end_trans(ObExecContext& exec_ctx, const bool is_rollback, ObEndTransAsyncCallback& callback);

  // call by OB_MYSQL_COM_CHANGE_USER, always to rollback the transaction if exists
  // call by ObSQLSessionMgr::SessionReclaimCallback::reclaim_value
  static int end_trans(
      storage::ObPartitionService* partition_service, ObSQLSessionInfo* session, bool& has_called_txs_end_trans);
  /** SQL layer transaction interface
   *
   * 1. Call background
   *
   * AutoCommit=1:
   *
   * Local:StartTrans,EndTrans in ObResultSet's Open,Close;
   *        StartStmt,EndStmt,in ObResultSet's Open,Close;
   *        StartParticipant,EndParticipant in ObResultSet's Open,Close
   * Remote:StartTrans,EndTrans in ObRpcTaskExecuteP's before_process,after_process;
   *        StartStmt,EndStmt,in ObRpcTaskExecuteP's before_process,after_process;
   *        StartParticipant,EndParticipant in ObRpcTaskExecuteP's before_process,after_process
   * Distribute:StartTrans,EndTrans in ObResultSet's Open,Close;
   *        StartStmt,EndStmt,in ObResultSet's Open,Close;
   *        StartParticipant,EndParticipant in ObResultSet's Open,Close,
   *        and in ObRpcTaskExecuteP's before_process,after_process
   *
   *
   *
   *
   * 2. arguments
   *
   * ObTransDesc:ObExecContext->ObPhysicalPlanCtx, need to serialize it;
   * Participants:All Participant information should be recorded in TaskInfo
   * tenant_id:ObExecContext->ObSQLSessionInfo
   * Considering that there are multiple PhysicalPlans for multi-statement transactions,
   * but only one SessionInfo,
   * and each statement may be executed across computers.
   * Session does not have a corresponding data structure at the remote end.
   * Therefore, before each SQL statement is executed,
   * TransDesc must be taken from SessionInfo and the execution ends. After that,
   * TransDesc will write SessionInfo.
   */
  static int start_trans(
      ObExecContext& exec_ctx, transaction::ObStartTransParam& start_trans_param, bool is_remote = false);

  static int start_stmt(ObExecContext& ctx, const common::ObPartitionLeaderArray& pla, bool is_remote = false);

  static int start_participant(
      ObExecContext& exec_ctx, const common::ObPartitionArray& participants, bool is_remote = false);

  static int end_trans(
      ObExecContext& exec_ctx, const bool is_rollback, const bool is_explicit, ObExclusiveEndTransCallback& callback);
  static int kill_query_session(
      storage::ObPartitionService* ps, ObSQLSessionInfo& session, const ObSQLSessionState& status);
  static int kill_active_trx(storage::ObPartitionService* ps, ObSQLSessionInfo* session);
  static int end_stmt(ObExecContext& exec_ctx, const bool is_rollback);

  static int end_participant(
      ObExecContext& exec_ctx, const bool is_rollback, const common::ObPartitionArray& participants);
  static int get_discard_participants(const common::ObPartitionArray& all_partitions,
      const common::ObPartitionArray& response_partitions, common::ObPartitionArray& discard_partitions);
  static int create_savepoint(ObExecContext& exec_ctx, const common::ObString& sp_name);
  static int rollback_savepoint(ObExecContext& exec_ctx, const common::ObString& sp_name);
  static int release_savepoint(ObExecContext& exec_ctx, const common::ObString& sp_name);
  static int xa_rollback_all_changes(ObExecContext& exec_ctx);
  static int start_cursor_stmt(ObExecContext& exec_ctx);

public:
  static int get_participants(ObExecContext& exec_ctx, common::ObPartitionArray& participants);
  static int get_root_job_participants(
      ObExecContext& exec_ctx, const ObPhyOperator& root_job_root_op, common::ObPartitionArray& participants);
  static int get_root_job_participants(
      ObExecContext& exec_ctx, const ObOperator& root_job_root_op, common::ObPartitionArray& participants);
  static int get_single_participant(ObExecContext& exec_ctx, common::ObPartitionArray& participants);

  static int get_participants(ObExecContext& exec_ctx, common::ObPartitionLeaderArray& pla);
  static int get_participants(const common::ObIArray<ObPhyTableLocation>& table_locations,
      common::ObPartitionLeaderArray& pla, bool is_retry_for_dup_tbl);
  static int append_participant_by_table_loc(
      common::ObPartitionIArray& participants, const ObPhyTableLocation& table_loc);
  static int append_participant_to_array_distinctly(
      common::ObPartitionIArray& participants, const common::ObPartitionKey& key);
  static int append_participant_to_array_distinctly(common::ObPartitionLeaderArray& pla,
      const common::ObPartitionKey& key, const common::ObPartitionType type, const common::ObAddr& svr);
  static bool is_weak_consistency_level(const common::ObConsistencyLevel& consistency_level);
  static int decide_trans_read_interface_specs(const char* module, const ObSQLSessionInfo& session,
      const stmt::StmtType& stmt_type, const stmt::StmtType& literal_stmt_type,
      const bool& is_contain_select_for_update, const bool& is_contain_inner_table,
      const common::ObConsistencyLevel& sql_consistency_level, const bool need_consistent_snapshot,
      int32_t& trans_consistency_level, int32_t& trans_consistency_type, int32_t& read_snapshot_type);

  static int get_stmt_snapshot_info(ObExecContext& exec_ctx, const bool is_cursor, transaction::ObTransDesc& trans_desc,
      transaction::ObTransSnapInfo& snap_info);
  static int start_standalone_stmt(
      ObExecContext& exec_ctx, ObSQLSessionInfo& session_info, ObPhysicalPlanCtx& phy_plan_ctx, bool is_remote);
  static bool is_isolation_RR_or_SE(int32_t isolation);

private:
  DISALLOW_COPY_AND_ASSIGN(ObSqlTransControl);
  /* functions */
  static int implicit_start_trans(ObExecContext& exec_ctx, bool is_remote = false);

  inline static int get_trans_timeout_ts(const ObSQLSessionInfo& my_session, int64_t& trans_timeout_ts);
  inline static int get_stmt_timeout_ts(const ObSQLSessionInfo& my_session, int64_t& stmt_timeout_ts);
  inline static int64_t get_stmt_timeout_ts(const ObPhysicalPlanCtx& plan_ctx);
  static int inc_session_ref(const ObSQLSessionInfo* my_session);
  static int merge_stmt_partitions(ObExecContext& exec_ctx, ObSQLSessionInfo& session);
  static int set_trans_param_(ObSQLSessionInfo& my_session, const ObPhysicalPlan& phy_plan,
      const common::ObConsistencyLevel sql_consistency_level, int32_t& trans_consistency_level,
      int64_t& auto_spec_snapshot_version);
  static int specify_stmt_snapshot_version_for_slave_cluster_sql_(const ObSQLSessionInfo& session,
      const stmt::StmtType& literal_stmt_type, const bool& is_contain_inner_table, const int32_t consistency_type,
      const int32_t read_snapshot_type, int64_t& auto_spec_snapshot_version);
  static int trans_param_compat_with_cluster_before_2200_(transaction::ObTransDesc& trans_desc);
  static int get_pg_key_(
      const ObIArray<ObPhyTableLocation>& table_locations, const ObPartitionKey& pkey, ObPartitionKey& pg_key);
  static int change_pkeys_to_pgs_(
      const ObIArray<ObPhyTableLocation>& table_locations, const ObPartitionArray& pkeys, ObPartitionArray& pg_keys);
  static int change_pla_info_(const ObIArray<ObPhyTableLocation>& table_locations, const ObPartitionLeaderArray& pla,
      ObPartitionLeaderArray& out_pla);
  static bool check_fast_select_read_uncommited(
      transaction::ObTransDesc& trans_desc, const common::ObPartitionLeaderArray& pla);
  static int update_safe_weak_read_snapshot(
      bool is_bounded_staleness_read, ObSQLSessionInfo& session_info, int snapshot_type, int64_t snapshot_version);
};

inline int ObSqlTransControl::get_trans_timeout_ts(const ObSQLSessionInfo& my_session, int64_t& trans_timeout_ts)
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

inline int ObSqlTransControl::get_stmt_timeout_ts(const ObSQLSessionInfo& my_session, int64_t& stmt_timeout_ts)
{
  int ret = common::OB_SUCCESS;
  int64_t query_timeout = 0;
  if (OB_FAIL(my_session.get_query_timeout(query_timeout))) {
    SQL_LOG(WARN, "fail to get query timeout", K(ret));
  } else {
    stmt_timeout_ts = my_session.get_query_start_time() + query_timeout;
  }
  return ret;
}

inline int64_t ObSqlTransControl::get_stmt_timeout_ts(const ObPhysicalPlanCtx& plan_ctx)
{
  return plan_ctx.get_trans_timeout_timestamp();
}

}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_TRANS_CONTROL_ */
//// end of header file
