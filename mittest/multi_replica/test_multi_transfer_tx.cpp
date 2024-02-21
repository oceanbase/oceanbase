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
#include <gtest/gtest.h>
#define USING_LOG_PREFIX SERVER
#define protected public
#define private public
#include "env/ob_multi_replica_test_base.h"
#include "env/ob_multi_replica_util.h"
#include "mittest/env/ob_simple_server_helper.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/compaction/ob_tablet_merge_task.h"
#include "storage/compaction/ob_tablet_merge_ctx.h"
#include "storage/high_availability/ob_tablet_backfill_tx.h"

#define CUR_TEST_CASE_NAME ObTestMultiTransferTx
DEFINE_MULTI_ZONE_TEST_CASE_CLASS
MULTI_REPLICA_TEST_MAIN_FUNCTION(test_multi_transfer_tx_);

static ObTabletID MULTI_TRANSFER_TX_CHOOSEN_TABLET_ID;
static oceanbase::transaction::ObTransID MULTI_TRANSFER_TX_CHOOSEN_TX_ID;

namespace oceanbase
{

namespace transaction
{
int ObPartTransCtx::replay_rollback_to(const ObTxRollbackToLog &log,
                                       const palf::LSN &offset,
                                       const SCN &timestamp,
                                       const int64_t &part_log_no,
                                       const bool is_tx_log_queue,
                                       const bool pre_barrier)
{
  int ret = OB_SUCCESS;
  common::ObTimeGuard timeguard("replay_rollback_to", 10 * 1000);
  // int64_t start = ObTimeUtility::fast_current_time();
  CtxLockGuard guard(lock_);

  if (trans_id_ == MULTI_TRANSFER_TX_CHOOSEN_TX_ID) {
    TRANS_LOG(INFO, "qianchen debuf2", KPC(this));
    return OB_EAGAIN;
  }

  bool need_replay = true;
  ObTxSEQ from = log.get_from();
  ObTxSEQ to = log.get_to();
  if (OB_UNLIKELY(from.get_branch() != to.get_branch())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "invalid savepoint", K(log));
  }
  //
  // the log is replay in txn log queue
  // for parallel replay, a global savepoint after the serial final log
  // must set the pre-barrier replay flag
  // some branch savepoint also need this, but we can't distinguish
  // hence only sanity check for global savepoint
  //
  else if (is_tx_log_queue) {
    if (is_parallel_logging()             // has enter parallel logging
        && to.get_branch() == 0           // is a global savepoint
        && timestamp > exec_info_.serial_final_scn_  // it is after the serial final log
        && !pre_barrier) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "missing pre barrier flag for parallel replay", KR(ret), K(*this));
      usleep(5000);
      ob_abort();
    } else if (OB_FAIL(check_replay_avaliable_(offset, timestamp, part_log_no, need_replay))) {
      TRANS_LOG(WARN, "check replay available failed", KR(ret), K(offset), K(timestamp), K(*this));
    } else if (!need_replay) {
      TRANS_LOG(INFO, "need not replay log", K(log), K(timestamp), K(offset), K(*this));
    } else if (OB_FAIL((update_replaying_log_no_(timestamp, part_log_no)))) {
      TRANS_LOG(WARN, "update replaying log no failed", K(ret), K(timestamp), K(part_log_no));
    }
  } else if (!ctx_tx_data_.get_start_log_ts().is_valid() && OB_FAIL(ctx_tx_data_.set_start_log_ts(timestamp))) {
    // update start_log_ts for branch savepoint, because it may replayed before first log in txn queue
    TRANS_LOG(WARN, "set tx data start log ts fail", K(ret), K(timestamp), KPC(this));
  }

  //
  // Step1, add Undo into TxData, both for parallel replay and serial replay
  //
  if (OB_SUCC(ret) && need_replay && OB_FAIL(rollback_to_savepoint_(log.get_from(), log.get_to(), timestamp))) {
    TRANS_LOG(WARN, "replay savepoint_rollback fail", K(ret), K(log), K(offset), K(timestamp),
              KPC(this));
  }

  // this is compatible code, since 4.3, redo_lsn not collect during replay
  if (OB_SUCC(ret) && OB_FAIL(check_and_merge_redo_lsns_(offset))) {
    TRANS_LOG(WARN, "check and merge redo lsns failed", K(ret), K(trans_id_), K(timestamp), K(offset));
  }

  //
  // Step2, remove TxNode(s)
  //
  if (OB_SUCC(ret) && !need_replay) {
    if (OB_FAIL(mt_ctx_.rollback(log.get_to(), log.get_from(), timestamp))) {
      TRANS_LOG(WARN, "mt ctx rollback fail", K(ret), K(log), KPC(this));
    }
  }

  if (OB_FAIL(ret) && OB_EAGAIN != ret) {
    TRANS_LOG(WARN, "[Replay Tx] Replay RollbackToLog in TxCtx Failed", K(timestamp), K(offset),
              K(ret), K(need_replay), K(log), KPC(this));
  } else {
#ifndef NDEBUG
    TRANS_LOG(INFO, "[Replay Tx] Replay RollbackToLog in TxCtx", K(timestamp), K(offset), K(ret),
              K(need_replay), K(log), KPC(this));
#endif
  }

  if (OB_EAGAIN != ret) {
    REC_TRANS_TRACE_EXT(tlog_,
                        replay_rollback_to,
                        OB_ID(ret),
                        ret,
                        OB_ID(used),
                        timeguard.get_diff(),
                        OB_Y(need_replay),
                        OB_ID(offset),
                        offset.val_,
                        OB_ID(t),
                        timestamp,
                        OB_ID(ref),
                        get_ref());
  }

  return ret;
}
}

namespace storage
{
int ObTransferWorkerMgr::do_transfer_backfill_tx_(const ObTransferBackfillTXParam &param)
{
  int ret = OB_SUCCESS;
  STORAGE_LOG(INFO, "qianchen debuf3", K(param));
  return ret;
}
}

namespace compaction
{
int ObTabletMergeFinishTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTaskController::get().switch_task(share::ObTaskType::DATA_MAINTAIN);
  ObTabletMergeCtx *ctx_ptr = nullptr;
  DEBUG_SYNC(MERGE_PARTITION_FINISH_TASK);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited yet", K(ret));
  } else if (OB_UNLIKELY(nullptr == merge_dag_
      || (nullptr == (ctx_ptr = static_cast<ObTabletMergeCtx *>(merge_dag_->get_ctx()))))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "get unexpected null ctx", K(ret));
  } else if (FALSE_IT(SET_MEM_CTX(ctx_ptr->mem_ctx_))) {
  } else if (ctx_ptr->get_tablet_id() == MULTI_TRANSFER_TX_CHOOSEN_TABLET_ID) {
    STORAGE_LOG(WARN, "qianchen debuf find tablet", KR(ret), KPC(ctx_ptr));
    ret = OB_EAGAIN;
  } else if (OB_FAIL(ctx_ptr->update_tablet_after_merge())) {
    STORAGE_LOG(WARN, "failed to update tablet after merge", KR(ret), KPC(ctx_ptr));
  }
  if (OB_FAIL(ret)) {
    STORAGE_LOG(WARN, "sstable merge failed", K(ret), KPC(ctx_ptr), "task", *(static_cast<ObITask *>(this)));
  } else {
    ObITable *sstable = ctx_ptr->merged_table_handle_.get_table();
    // ATTENTION! Critical diagnostic log, DO NOT CHANGE!!!
    STORAGE_LOG(INFO, "sstable merge finish", K(ret), "merge_info", ctx_ptr->get_merge_info(),
        KPC(sstable), "mem_peak", ctx_ptr->mem_ctx_.get_total_mem_peak(), "compat_mode", merge_dag_->get_compat_mode(),
        "time_guard", ctx_ptr->info_collector_.time_guard_);
  }
  return ret;
}
}

namespace unittest
{

#define EXE_SQL(sql_str)                                            \
  ASSERT_EQ(OB_SUCCESS, sql.assign(sql_str));                       \
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

#define EXE_SQL_FMT(...)                                            \
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(__VA_ARGS__));               \
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));


void get_tablet_info_with_table_name(common::ObMySQLProxy &sql_proxy,
                                     const char *name,
                                     int64_t &table_id,
                                     int64_t &object_id,
                                     int64_t &tablet_id,
                                     int64_t &ls_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;

  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("SELECT table_id, object_id, tablet_id, ls_id FROM oceanbase.DBA_OB_TABLE_LOCATIONS WHERE TABLE_NAME= '%s';", name));
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
    sqlclient::ObMySQLResult *result = res.get_result();
    ASSERT_NE(nullptr, result);
    ASSERT_EQ(OB_SUCCESS, result->next());
    ASSERT_EQ(OB_SUCCESS, result->get_int("table_id", table_id));
    ASSERT_EQ(OB_SUCCESS, result->get_int("object_id", object_id));
    ASSERT_EQ(OB_SUCCESS, result->get_int("tablet_id", tablet_id));
    ASSERT_EQ(OB_SUCCESS, result->get_int("ls_id", ls_id));
  }
}

TEST_F(GET_ZONE_TEST_CLASS_NAME(1), create_test_env)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;

  // ============================== Phase1. create tenant ==============================
  uint64_t tenant_id;
  SERVER_LOG(INFO, "create_tenant start");
  ASSERT_EQ(OB_SUCCESS, create_tenant(DEFAULT_TEST_TENANT_NAME,
                                      "2G",
                                      "2G",
                                      false,
                                      "zone1, zone2"));
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id));
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
  SERVER_LOG(INFO, "create_tenant end", K(tenant_id));

  SERVER_LOG(INFO, "[ObMultiReplicaTestBase] create test tenant success", K(tenant_id));
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  sqlclient::ObISQLConnection *connection_qc = nullptr;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(connection_qc));
  ASSERT_NE(nullptr, connection_qc);
  WRITE_SQL_BY_CONN(connection_qc, "alter system set partition_balance_schedule_interval = '10s';");
  WRITE_SQL_BY_CONN(connection_qc, "alter system set _enable_active_txn_transfer = True;");

  // ============================== Phase2. create table ==============================
  TRANS_LOG(INFO, "create table qcc1 start");
  EXE_SQL("create table qcc1 (a int)");
  TRANS_LOG(INFO, "create_table qcc1 end");

  TRANS_LOG(INFO, "create table qcc2 start");
  EXE_SQL("create table qcc2 (a int)");
  TRANS_LOG(INFO, "create_table qcc2 end");
  usleep(3 * 1000 * 1000);
  ObLSID loc1, loc2;
  ASSERT_EQ(0, SSH::select_table_loc(tenant_id, "qcc1", loc1));
  ASSERT_EQ(0, SSH::select_table_loc(tenant_id, "qcc2", loc2));
  int64_t table1, table2;
  int64_t object1, object2;
  int64_t tablet1, tablet2;
  int64_t ls1, ls2;
  get_tablet_info_with_table_name(sql_proxy, "qcc1", table1, object1, tablet1, ls1);
  get_tablet_info_with_table_name(sql_proxy, "qcc2", table2, object2, tablet2, ls2);
  fprintf(stdout, "qcc is created successfully, loc1: %ld, loc2: %ld, table1: %ld, table2: %ld, tablet1: %ld, tablet2: %ld, ls1: %ld, ls2: %ld\n",
          loc1.id(), loc2.id(), table1, table2, tablet1, tablet2, ls1, ls2);
  ASSERT_NE(loc1, loc2);
  EXE_SQL("create tablegroup tg1 sharding='NONE';");

  MULTI_TRANSFER_TX_CHOOSEN_TABLET_ID = tablet2;

  // ============================== Phase5. start the user txn ==============================
  sqlclient::ObISQLConnection *user_connection = nullptr;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(user_connection));
  ASSERT_NE(nullptr, user_connection);

  WRITE_SQL_BY_CONN(user_connection, "set SESSION ob_trx_timeout = 10000000000");
  WRITE_SQL_BY_CONN(user_connection, "set SESSION ob_trx_idle_timeout = 10000000000");
  WRITE_SQL_BY_CONN(user_connection, "set SESSION ob_query_timeout = 10000000000");

  TRANS_LOG(INFO, "start the txn");
  WRITE_SQL_BY_CONN(user_connection, "begin;");
  WRITE_SQL_BY_CONN(user_connection, "savepoint qc1");
  WRITE_SQL_BY_CONN(user_connection, "insert into qcc1 values(1);");
  WRITE_SQL_BY_CONN(user_connection, "insert into qcc2 values(1);");

  ObTransID tx_id;
  ASSERT_EQ(0, SSH::find_tx(user_connection, tx_id));
  MULTI_TRANSFER_TX_CHOOSEN_TX_ID = tx_id;
  fprintf(stdout, "starting the user txn, %lu\n", tx_id.get_id());
  TRANS_LOG(INFO, "starting the user txn", K(tx_id));

  // ============================== Phase5. start the transfer ==============================
  EXE_SQL("alter tablegroup tg1 add qcc1,qcc2;");
  usleep(1 * 1000 * 1000);
  int64_t begin_time = ObTimeUtility::current_time();
  while (true) {
    ASSERT_EQ(0, SSH::select_table_loc(tenant_id, "qcc1", loc1));
    ASSERT_EQ(0, SSH::select_table_loc(tenant_id, "qcc2", loc2));
    if (loc1 == loc2) {
      fprintf(stdout, "succeed wait for balancer\n");
      break;
    } else if (ObTimeUtility::current_time() - begin_time > 300 * 1000 * 1000) {
      fprintf(stdout, "ERROR: fail to wait for balancer\n");
      break;
    } else {
      usleep(1 * 1000 * 1000);
      fprintf(stdout, "wait for balancer\n");
    }
  }

  ASSERT_EQ(loc1, loc2);

  WRITE_SQL_BY_CONN(user_connection, "rollback to qc1");

  HEAP_VAR(ObMySQLProxy::MySQLResult, res)
  {
    ASSERT_EQ(OB_SUCCESS,
              user_connection->execute_read(OB_SYS_TENANT_ID,
                                          "select /*+log_level(debug)*/* from qcc2;",
                                          res));
    common::sqlclient::ObMySQLResult *result = res.mysql_result();
    ASSERT_EQ(OB_ITER_END, result->next());
  }
}

} // namespace unittest
} // namespace oceanbase
