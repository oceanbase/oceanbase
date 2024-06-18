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
#include <stdlib.h>
#define USING_LOG_PREFIX STORAGE
#define protected public
#define private public

#include "env/ob_simple_cluster_test_base.h"
#include "env/ob_simple_server_restart_helper.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "logservice/rcservice/ob_role_change_service.h"
#include "logservice/ob_ls_adapter.h"
#include "storage/access/ob_rows_info.h"
#include "storage/checkpoint/ob_data_checkpoint.h"
#include "storage/compaction/ob_schedule_dag_func.h"
#include "storage/compaction/ob_tablet_merge_task.h"
#include "storage/ls/ob_freezer.h"
#include "storage/ls/ob_ls.h"
#include "storage/ls/ob_ls_meta.h"
#include "storage/ls/ob_ls_tablet_service.h"
#include "storage/ls/ob_ls_tx_service.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/ob_relative_table.h"
#include "storage/ob_storage_table_guard.h"
#include "storage/tx_storage/ob_ls_map.h"
#include "storage/tx_storage/ob_ls_service.h"

#undef private
#undef protected

static const char *TEST_FILE_NAME = "test_replay_from_middle";
static const char *BORN_CASE_NAME = "ObReplayFromMiddleTest";
static const char *RESTART_CASE_NAME = "ObReplayRestartTest";
static const char *COMMUNICATION_FILE_NAME = "/tmp/test_replay_from_middle_communication.txt";

ObSimpleServerRestartHelper *helper_ptr = nullptr;
bool long_time_tx_done = false;

namespace oceanbase
{

using namespace transaction;
using namespace storage;
using namespace share;

class TestRunCtx
{
public:
  uint64_t tenant_id_ = 0;
};

SCN     SSTABLE_END_SCN =  SCN::min_scn();
SCN     REPLAY_BARRIER = SCN::min_scn();
SCN     KEEP_ALIVE_SCN =  SCN::min_scn();
SCN     MAX_DECIDED_SCN_AFTER_COMMIT = SCN::min_scn();
ObLSID      LS_ID;
TestRunCtx  RunCtx;
ObTabletID  TABLET_ID;

namespace unittest
{

class ObReplayFromMidTestBase {
public:
  ObLS *get_ls(const int64_t tenant_id, const ObLSID ls_id)
  {
    int ret = OB_SUCCESS;
    ObLS *ls = nullptr;
    MTL_SWITCH(tenant_id)
    {
      ObLSIterator *ls_iter = nullptr;
      ObLSHandle ls_handle;
      ObLSService *ls_svr = MTL(ObLSService *);
      OB_ASSERT(OB_SUCCESS == ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
      OB_ASSERT(nullptr != (ls = ls_handle.get_ls()));
    }
    return ls;
  }
};

class ObReplayFromMiddleTest : public ObSimpleClusterTestBase, public ObReplayFromMidTestBase
{
public:
  ObReplayFromMiddleTest() : ObSimpleClusterTestBase(TEST_FILE_NAME) {}

  void basic_test();
  void create_test_table();
  void insert_long_time_tx();
  void minor_freeze_once();
  void insert_tiny_tx();
  void check_tx_data_minor_succeed();
  void write_max_decided_scn();
  void advance_checkpoint();
  void flush_tx_ctx_table();

private:
  bool stop = false;
  int freeze_duration_ = 30 * 1000 * 1000;
  int64_t first_rollback_ts_;
};

#define EXE_SQL(connection, sql_str)          \
  ASSERT_EQ(OB_SUCCESS, sql.assign(sql_str)); \
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

#define EXE_SQL_FMT(...)                              \
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(__VA_ARGS__)); \
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

#define WRITE_SQL_BY_CONN(conn, sql_str)      \
  ASSERT_EQ(OB_SUCCESS, sql.assign(sql_str)); \
  ASSERT_EQ(OB_SUCCESS, conn->execute_write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));

#define WRITE_SQL_FMT_BY_CONN(conn, ...)              \
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(__VA_ARGS__)); \
  ASSERT_EQ(OB_SUCCESS, conn->execute_write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));

void ObReplayFromMiddleTest::basic_test()
{
  int ret = OB_SUCCESS;
  ObTabletID tablet_id;
  ObLSID ls_id;
  std::vector<std::thread> work_threads;

  create_test_table();
  work_threads.push_back(std::thread([this]() { insert_long_time_tx(); }));
  work_threads.push_back(std::thread([this]() { insert_tiny_tx(); }));

  // waitting threads finish
  for (auto it = work_threads.begin(); it != work_threads.end(); it++) {
    it->join();
  }

  ::sleep(5);
  write_max_decided_scn();
}

#define DEF_VAL_FOR_SQL      \
  int ret = OB_SUCCESS;      \
  ObSqlString sql;           \
  int64_t affected_rows = 0; \
  sqlclient::ObISQLConnection *connection = nullptr;

void ObReplayFromMiddleTest::create_test_table()
{
  DEF_VAL_FOR_SQL
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().get_sql_proxy2().acquire(connection));
  WRITE_SQL_BY_CONN(connection, "create table if not exists test_replay_from_middle_t (c1 int, c2 int)");

  HEAP_VAR(ObMySQLProxy::MySQLResult, res)
  {
    int64_t int_tablet_id = 0;
    int64_t int_ls_id = 0;
    int64_t retry_times = 10;

    ASSERT_EQ(
        OB_SUCCESS,
        connection->execute_read(OB_SYS_TENANT_ID,
                                 "SELECT tablet_id, ls_id FROM oceanbase.DBA_OB_TABLE_LOCATIONS WHERE TABLE_NAME = "
                                 "\'test_replay_from_middle_t\'",
                                 res));
    common::sqlclient::ObMySQLResult *result = res.mysql_result();
    ASSERT_EQ(OB_SUCCESS, result->next());
    ASSERT_EQ(OB_SUCCESS, result->get_int("tablet_id", int_tablet_id));
    ASSERT_EQ(OB_SUCCESS, result->get_int("ls_id", int_ls_id));
    TABLET_ID = int_tablet_id;
    LS_ID = int_ls_id;
    fprintf(stdout, "get table info finish : tablet_id = %ld, ls_id = %ld\n", TABLET_ID.id(), LS_ID.id());
  }
}

void ObReplayFromMiddleTest::insert_long_time_tx()
{
  DEF_VAL_FOR_SQL
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().get_sql_proxy2().acquire(connection));
  WRITE_SQL_BY_CONN(connection, "begin");

  // do some insert operation
  fprintf(stdout, "insert long tx start.\n");
  for (int64_t i = 1; i <= 10; i++) {
    // insert a single row
    WRITE_SQL_FMT_BY_CONN(connection, "insert into test_replay_from_middle_t values(%ld, %ld)", i, i);

    // insert a single row and rollback
    WRITE_SQL_BY_CONN(connection, "savepoint x");
    WRITE_SQL_FMT_BY_CONN(connection, "insert into test_replay_from_middle_t values(%ld, %ld)", i, i);
    WRITE_SQL_BY_CONN(connection, "rollback to savepoint x");

    // do insert and rollback once a second
    ::sleep(1);
  }

  minor_freeze_once();
  advance_checkpoint();

  fprintf(stdout, "sleep before commit long tx\n");
  ::sleep(30);
  WRITE_SQL_BY_CONN(connection, "commit");
  fprintf(stdout, "insert long tx finish.\n");
  ::sleep(5); // wait tx ctx deleting
  flush_tx_ctx_table();
  long_time_tx_done = true;
}

void ObReplayFromMiddleTest::minor_freeze_once()
{
  DEF_VAL_FOR_SQL
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().get_sql_proxy2().acquire(connection));
  WRITE_SQL_BY_CONN(connection, "alter system minor freeze");
  ::sleep(2);

  {
    // check data memtable flushed
    HEAP_VAR(ObMySQLProxy::MySQLResult, res)
    {
      common::sqlclient::ObMySQLResult *result = nullptr;
      int64_t retry_times = 10;
      sql.assign_fmt("SELECT end_log_scn FROM oceanbase.__ALL_VIRTUAL_TABLE_MGR WHERE tablet_id = %ld and table_type = "
                     "12 and size > 0 and contain_uncommitted_row = \'YES\'",
                     TABLET_ID.id());

      do {
        res.reuse();
        ASSERT_EQ(OB_SUCCESS, connection->execute_read(OB_SYS_TENANT_ID, sql.ptr(), res));
        result = res.mysql_result();
        ret = result->next();
        fprintf(stdout, "select end_log_scn once, ret = %d\n", ret);
        ::sleep(1);
      } while (OB_SUCCESS != ret && (--retry_times > 0));

      uint64_t sstable_end_scn_val = 0;
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_EQ(OB_SUCCESS, result->get_uint("end_log_scn", sstable_end_scn_val));
      ASSERT_EQ(OB_SUCCESS, SSTABLE_END_SCN.convert_for_tx(sstable_end_scn_val));
      fprintf(stdout,
              "get end_log_scn from mini sstable : %lu ls_id : %ld tenant_id : %ld\n",
              sstable_end_scn_val,
              LS_ID.id(),
              RunCtx.tenant_id_);

    }
  }

  {
    // check tx data memtable flushed
    ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().get_sql_proxy().acquire(connection));
    HEAP_VAR(ObMySQLProxy::MySQLResult, res)
    {
      common::sqlclient::ObMySQLResult *result = nullptr;
      int64_t count = 0;
      int64_t retry_times = 10;
      sql.assign_fmt("SELECT count(*) as count FROM oceanbase.__ALL_VIRTUAL_TX_DATA_TABLE WHERE tenant_id = %ld and ls_id = %ld and state = \'MINI\'",
                     RunCtx.tenant_id_,
                     LS_ID.id());

      do {
        res.reuse();
        ASSERT_EQ(OB_SUCCESS, connection->execute_read(OB_SYS_TENANT_ID, sql.ptr(), res));
        result = res.mysql_result();
        ret = result->next();
        fprintf(stdout, "select __all_virtual_tx_data_table once, ret = %d\n", ret);
        ::sleep(1);
      } while (OB_SUCCESS != ret && (--retry_times > 0));
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_EQ(OB_SUCCESS, result->get_int("count", count));
      fprintf(stdout, "tx data sstable existed\n");
    }
  }
}

void ObReplayFromMiddleTest::advance_checkpoint()
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(RunCtx.tenant_id_)
  {
    ObLS *ls = get_ls(RunCtx.tenant_id_, LS_ID);
    int64_t retry_times = 20;
    SCN checkpoint = SCN::min_scn();
    int ret = ls->checkpoint_executor_.advance_checkpoint_by_flush(SCN::scn_inc(SSTABLE_END_SCN));
    ASSERT_EQ(true, OB_SUCCESS == ret || OB_NO_NEED_UPDATE == ret);
    while (--retry_times > 0) {
      checkpoint = ls->get_clog_checkpoint_scn();
      if (checkpoint > SSTABLE_END_SCN) {
        break;
      } else {
        ::sleep(1);
        fprintf(stdout,
                "waiting advance checkpoint, checkpoint = %lu SSTABLE_END_SCN = %lu\n",
                checkpoint.get_val_for_inner_table_field(),
                SSTABLE_END_SCN.get_val_for_inner_table_field());
      }
    }
    ASSERT_GT(ls->get_clog_checkpoint_scn(), SSTABLE_END_SCN);

    SCN min_start_scn = SCN::min_scn();
    SCN keep_alive_scn =  SCN::min_scn();
    MinStartScnStatus status = MinStartScnStatus::UNKOWN;
    retry_times = 40;
    while (--retry_times > 0) {
      ls->get_min_start_scn(min_start_scn, keep_alive_scn, status);
      if (MinStartScnStatus::HAS_CTX == status) {
        break;
      } else {
        ::sleep(1);
        fprintf(stdout,
                "waiting min_start_scn has ctx min_start_scn = %ld keep_alive_scn = %ld status = %d\n",
                min_start_scn.get_val_for_inner_table_field(),
                keep_alive_scn.get_val_for_inner_table_field(),
                status);
      }
    }
    ASSERT_NE(SCN::min_scn(), min_start_scn);
    ASSERT_NE(SCN::min_scn(), keep_alive_scn);
    ASSERT_EQ(MinStartScnStatus::HAS_CTX, status);
    KEEP_ALIVE_SCN = keep_alive_scn;
    fprintf(
        stdout,
        "advance checkpoint done, checkpoint = %lu SSTABLE_END_SCN = %lu min_start_scn = %lu keep_alive_scn = %lu\n",
        checkpoint.get_val_for_inner_table_field(),
        SSTABLE_END_SCN.get_val_for_inner_table_field(),
        min_start_scn.get_val_for_inner_table_field(),
        keep_alive_scn.get_val_for_inner_table_field());
  }
}

void ObReplayFromMiddleTest::flush_tx_ctx_table()
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(RunCtx.tenant_id_)
  {
    SCN tx_ctx_rec_scn_before_flush = SCN::min_scn();
    SCN tx_ctx_rec_scn_after_flush = SCN::min_scn();
    int64_t retry_times = 20;
    ObLS *ls = get_ls(RunCtx.tenant_id_, LS_ID);
    ASSERT_EQ(OB_SUCCESS, ls->ls_tx_svr_.mgr_->get_rec_scn(tx_ctx_rec_scn_before_flush));
    ASSERT_EQ(OB_SUCCESS, ls->ls_tx_svr_.flush_ls_inner_tablet(LS_TX_CTX_TABLET));
    while (--retry_times > 0) {
      ASSERT_EQ(OB_SUCCESS, ls->ls_tx_svr_.mgr_->get_rec_scn(tx_ctx_rec_scn_after_flush));
      if (tx_ctx_rec_scn_after_flush > tx_ctx_rec_scn_before_flush) {
        break;
      } else {
        fprintf(stdout,
                "wait flushing tx ctx table, tx_ctx_rec_scn_before_flusht = %lu tx_ctx_rec_scn_after_flush = %lu\n",
                tx_ctx_rec_scn_before_flush.get_val_for_inner_table_field(),
                tx_ctx_rec_scn_after_flush.get_val_for_inner_table_field());
      }
    }
  }
}

void ObReplayFromMiddleTest::insert_tiny_tx()
{
  DEF_VAL_FOR_SQL
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().get_sql_proxy2().acquire(connection));
  fprintf(stdout, "insert tiny tx start.\n");
  int64_t inserted_tiny_tx_cnt = 0;
  while (true) {
    // insert a single row
    WRITE_SQL_FMT_BY_CONN(connection, "insert into test_replay_from_middle_t values(%d, %d)", 888, 666);
    inserted_tiny_tx_cnt++;
    usleep(1000);

    if (long_time_tx_done) {
      break;
    }
  }
  fprintf(stdout, "insert tiny tx finish. inserted count : %ld \n", inserted_tiny_tx_cnt);
}

void ObReplayFromMiddleTest::write_max_decided_scn()
{
  ObLS *ls = get_ls(RunCtx.tenant_id_, LS_ID);
  ASSERT_EQ(OB_SUCCESS, ls->get_max_decided_scn(MAX_DECIDED_SCN_AFTER_COMMIT));

  // Using file sending sstable_end_scn to another process
  std::string file_name = std::string(COMMUNICATION_FILE_NAME);
  FILE *outfile = nullptr;
  ASSERT_NE(nullptr, outfile = fopen(file_name.c_str(), "w"));
  fprintf(
      outfile, "%ld\n%ld\n%lu\n%lu\n%lu\n", RunCtx.tenant_id_, LS_ID.id(), SSTABLE_END_SCN.get_val_for_inner_table_field(),
      MAX_DECIDED_SCN_AFTER_COMMIT.get_val_for_inner_table_field(),
      KEEP_ALIVE_SCN.get_val_for_inner_table_field());
  fclose(outfile);
  fprintf(stdout,
          "write communication file done, tenant_id = %ld ls_id = %ld sstable_end_scn = %lu max_decided_scn = %lu "
          "keep_alive_scn = %lu\n",
          RunCtx.tenant_id_,
          LS_ID.id(),
          SSTABLE_END_SCN.get_val_for_inner_table_field(),
          MAX_DECIDED_SCN_AFTER_COMMIT.get_val_for_inner_table_field(),
          KEEP_ALIVE_SCN.get_val_for_inner_table_field());
}

TEST_F(ObReplayFromMiddleTest, observer_start)
{
  SERVER_LOG(INFO, "observer_start succ");

  DEF_VAL_FOR_SQL
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().get_sql_proxy().acquire(connection));
  WRITE_SQL_BY_CONN(connection, "set ob_trx_timeout = 6000000000");
  WRITE_SQL_BY_CONN(connection, "set ob_trx_idle_timeout = 6000000000");
  WRITE_SQL_BY_CONN(connection, "alter system set _fast_commit_callback_count = 0");
  WRITE_SQL_BY_CONN(connection, "alter system set _private_buffer_size = '1B'");
}

TEST_F(ObReplayFromMiddleTest, add_tenant)
{
  // create tenant
  DEF_VAL_FOR_SQL
  ASSERT_EQ(OB_SUCCESS, create_tenant());
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(RunCtx.tenant_id_));
  ASSERT_NE(0, RunCtx.tenant_id_);
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().get_sql_proxy2().acquire(connection));
  WRITE_SQL_BY_CONN(connection, "alter system set minor_compact_trigger = 16");
}

TEST_F(ObReplayFromMiddleTest, basic_test)
{
  basic_test();
}

class ObReplayRestartTest : public ObSimpleClusterTestBase, public ObReplayFromMidTestBase
{
public:
  ObReplayRestartTest() : ObSimpleClusterTestBase(TEST_FILE_NAME) {}

  void restart_test();
  void wait_replay(ObLS *ls, const SCN expected_scn);
  void flush_tx_data(ObLS *ls);
};

void ObReplayRestartTest::restart_test()
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  ObTxTable *tx_table = nullptr;
  ObTxDataTable *tx_data_table = nullptr;
  share::SCN max_decided_scn = share::SCN::min_scn();
  share::SCN upper_trans_version = share::SCN::min_scn();

  MTL_SWITCH(RunCtx.tenant_id_)
  {
    ls = get_ls(RunCtx.tenant_id_, LS_ID);
    wait_replay(ls, SSTABLE_END_SCN);
    ASSERT_EQ(OB_SUCCESS, ls->get_max_decided_scn(max_decided_scn));
    ASSERT_EQ(false, max_decided_scn.is_min());

    tx_table = ls->get_tx_table();
    tx_data_table = tx_table->get_tx_data_table();

    {
      // 场景一： keep alive日志没有被回放，min_start_scn为初始值状态，跳过计算upper_trans_version
      ASSERT_EQ(SCN::min_scn(), tx_table->ctx_min_start_scn_info_.min_start_scn_in_ctx_);
      upper_trans_version.set_min();
      FLOG_INFO("get upper trans version, situation 1:", K(SSTABLE_END_SCN));
      ASSERT_EQ(OB_SUCCESS,
                tx_data_table->get_upper_trans_version_before_given_scn(SSTABLE_END_SCN, upper_trans_version));
    }

    {
      // 场景2： keep alive日志被回放，min_start_scn小于end_scn，跳过计算upper_trans_version
      REPLAY_BARRIER = SCN::plus(KEEP_ALIVE_SCN, 1);
      int64_t retry_times = 100;
      while (--retry_times > 0) {
        tx_table->update_min_start_scn_info(SCN::max_scn() /*max_decided_scn*/);
        if ( !tx_table->ctx_min_start_scn_info_.min_start_scn_in_ctx_.is_min()) {
          break;
        } else {
          ::sleep(1);
          fprintf(stdout, "waiting replay to keep alive log ...\n");
        }
      }
      upper_trans_version.set_min();
      FLOG_INFO("get upper trans version, situation 2:", K(SSTABLE_END_SCN));
      ASSERT_EQ(OB_SUCCESS,
                tx_data_table->get_upper_trans_version_before_given_scn(SSTABLE_END_SCN, upper_trans_version));
      ASSERT_GE(upper_trans_version, SCN::max_scn());
    }

    {
      // 场景3： 所有日志回放结束，事务数据也发生冻结转储，上下文及事务数据表都已没有事务
      REPLAY_BARRIER = SCN::max_scn();

      // wait replaying again
      wait_replay(ls, MAX_DECIDED_SCN_AFTER_COMMIT);
      flush_tx_data(ls);

      int64_t retry_times = 60;
      SCN min_start_scn = SCN::min_scn();
      SCN keep_alive_scn = SCN::min_scn();
      MinStartScnStatus status;
      while (--retry_times > 0) {
        ls->get_min_start_scn(min_start_scn, keep_alive_scn, status);
        tx_table->update_min_start_scn_info(SCN::max_scn() /*max_decided_scn*/);
        if (tx_table->ctx_min_start_scn_info_.min_start_scn_in_ctx_ > SSTABLE_END_SCN) {
          break;
        } else {
          ::sleep(1);
          fprintf(stdout,
                  "waiting all tx ctx clear, min_start_scn = %lu keep_alive_scn = %lu status = %d\n",
                  min_start_scn.get_val_for_inner_table_field(),
                  keep_alive_scn.get_val_for_inner_table_field(),
                  status);
        }
      }
      ASSERT_GT(tx_table->ctx_min_start_scn_info_.min_start_scn_in_ctx_, SSTABLE_END_SCN);

      retry_times = 60;
      while (--retry_times > 0) {
        ASSERT_EQ(OB_SUCCESS, ls->get_max_decided_scn(max_decided_scn));
        if (max_decided_scn > tx_table->ctx_min_start_scn_info_.keep_alive_scn_) {
          break;
        } else {
          ::sleep(1);
          fprintf(stdout, "waiting max decided scn, max_decided_scn = %lu keep_alive_scn = %lu\n",
                  max_decided_scn.get_val_for_inner_table_field(),
                  tx_table->ctx_min_start_scn_info_.keep_alive_scn_.get_val_for_inner_table_field());
        }
      }
      ASSERT_GT(max_decided_scn, tx_table->ctx_min_start_scn_info_.keep_alive_scn_);

      upper_trans_version.set_min();
      FLOG_INFO("get upper trans version, situation 3:", K(SSTABLE_END_SCN));
      ASSERT_EQ(OB_SUCCESS,
                tx_data_table->get_upper_trans_version_before_given_scn(SSTABLE_END_SCN, upper_trans_version));


      ::sleep(10);
      STORAGE_LOG(INFO, "finish restart test", K(upper_trans_version), K(SSTABLE_END_SCN), K(tx_table->ctx_min_start_scn_info_));
      ASSERT_LT(upper_trans_version, SCN::max_scn());
    }
  }
}

void ObReplayRestartTest::wait_replay(ObLS *ls, const SCN expected_scn)
{
  int64_t retry_times = 30;
  SCN max_decided_scn = SCN::min_scn();
  while (true) {
    max_decided_scn.set_min();
    ASSERT_EQ(OB_SUCCESS, ls->get_max_decided_scn(max_decided_scn));
    if (max_decided_scn >= expected_scn || (--retry_times < 0)) {
      break;
    } else {
      fprintf(stdout, "wait replaying...  max_decided_scn = %lu expected_scn = %lu\n", max_decided_scn.get_val_for_inner_table_field(),
              expected_scn.get_val_for_inner_table_field());
      ::sleep(1);
    }
  }
  ASSERT_GE(max_decided_scn, expected_scn);
}

void ObReplayRestartTest::flush_tx_data(ObLS *ls)
{
  SCN tx_ctx_rec_scn = SCN::min_scn();
  int64_t retry_times = 10;
  int64_t head_before_flush = 0;
  int64_t tail_before_flush = 0;
  int64_t head_after_flush = 0;
  int64_t tail_after_flush = 0;
  ObTxDataMemtableMgr &mgr = ls->ls_tablet_svr_.tx_data_memtable_mgr_;
  ASSERT_EQ(OB_SUCCESS, mgr.get_memtable_range(head_before_flush, tail_before_flush));
  int ret = ls->ls_tx_svr_.flush_ls_inner_tablet(LS_TX_DATA_TABLET);
  ASSERT_TRUE(OB_SUCCESS == ret || OB_EAGAIN == ret);

  while (--retry_times > 0) {
    ASSERT_EQ(OB_SUCCESS, mgr.get_memtable_range(head_after_flush, tail_after_flush));
    if (head_after_flush > head_before_flush && tail_after_flush > tail_before_flush &&
        head_after_flush + 1 == tail_after_flush) {
      break;
    } else {
      ::sleep(1);
      fprintf(stdout, "wait flushing tx data ...\n");
    }
  }
  ASSERT_GT(head_after_flush, head_before_flush);
  ASSERT_EQ(head_after_flush + 1, tail_after_flush);
  fprintf(stdout, "flush tx data done\n");
}

TEST_F(ObReplayRestartTest, observer_restart)
{
  SERVER_LOG(INFO, "observer_start succ");
}

TEST_F(ObReplayRestartTest, restart_test)
{
  restart_test();
}

}  // namespace unittest
}  // namespace oceanbase

int main(int argc, char **argv)
{
  int c = 0;
  int time_sec = 0;
  int concurrency = 1;
  char *log_level = (char *)"INFO";
  while (EOF != (c = getopt(argc, argv, "t:l:"))) {
    switch (c) {
      case 't':
        time_sec = atoi(optarg);
        break;
      case 'l':
        log_level = optarg;
        oceanbase::unittest::ObSimpleClusterTestBase::enable_env_warn_log_ = false;
        break;
      case 'c':
        concurrency = atoi(optarg);
        break;
      default:
        break;
    }
  }
  std::string gtest_file_name = std::string(TEST_FILE_NAME) + "_gtest.log";
  oceanbase::unittest::init_gtest_output(gtest_file_name);

  int ret = 0;
  ObSimpleServerRestartHelper restart_helper(argc, argv, TEST_FILE_NAME, BORN_CASE_NAME,
                                             RESTART_CASE_NAME);
  helper_ptr = &restart_helper;
  restart_helper.set_sleep_sec(time_sec);
  restart_helper.run();

  return ret;
}

namespace oceanbase {
namespace storage {

int ObLSService::online_ls()
{
  // do nothing
  int ret = OB_SUCCESS;
  /**************************** addtional code *****************************/
  if (helper_ptr->is_restart_) {
    int64_t int_ls_id = 0;
    std::string file_name = std::string(COMMUNICATION_FILE_NAME);
    FILE *infile = nullptr;
    OB_ASSERT(nullptr != (infile = fopen(file_name.c_str(), "r")));
    int64_t SSTABLE_END_SCN_VAL = 0;
    int64_t MAX_DECIDED_SCN_AFTER_COMMIT_VAL = 0;
    int64_t KEEP_ALIVE_SCN_VAL = 0;

    fscanf(infile, "%ld\n%ld\n%ld\n%ld\n%ld\n", &(RunCtx.tenant_id_), &int_ls_id, &SSTABLE_END_SCN_VAL, &MAX_DECIDED_SCN_AFTER_COMMIT_VAL, &KEEP_ALIVE_SCN_VAL);
    OB_ASSERT(RunCtx.tenant_id_ > 0);
    OB_ASSERT(SSTABLE_END_SCN_VAL > 0);
    OB_ASSERT(int_ls_id > 0);
    OB_ASSERT(MAX_DECIDED_SCN_AFTER_COMMIT_VAL > 0);
    uint64_t REPLAY_BARRIER_VAL = SSTABLE_END_SCN_VAL + 1;

    EXPECT_EQ(OB_SUCCESS, SSTABLE_END_SCN.convert_for_tx(SSTABLE_END_SCN_VAL));
    EXPECT_EQ(true, OB_SUCCESS == MAX_DECIDED_SCN_AFTER_COMMIT.convert_for_tx(MAX_DECIDED_SCN_AFTER_COMMIT_VAL));
    EXPECT_EQ(OB_SUCCESS, (KEEP_ALIVE_SCN.convert_for_tx(KEEP_ALIVE_SCN_VAL)));
    EXPECT_EQ(OB_SUCCESS, (REPLAY_BARRIER.convert_for_tx(REPLAY_BARRIER_VAL)));
    LS_ID = int_ls_id;
  }
  /**************************** addtional code *****************************/

  int tmp_ret = OB_SUCCESS;
  common::ObSharedGuard<ObLSIterator> ls_iter;
  ObLS *ls = nullptr;
  int64_t create_type = ObLSCreateType::NORMAL;
  if (OB_FAIL(get_ls_iter(ls_iter, ObLSGetMod::TXSTORAGE_MOD))) {
    LOG_WARN("failed to get ls iter", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(ls_iter->get_next(ls))) {
        if (OB_ITER_END != ret) {
          LOG_ERROR("fail to get next ls", K(ret));
        }
      } else if (nullptr == ls) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("ls is null", K(ret));
      } else {
        ObLSLockGuard lock_ls(ls);
        if (OB_FAIL(ls->get_create_type(create_type))) {
          LOG_WARN("get ls create type failed", K(ret));
        } else if (OB_FAIL(post_create_ls_(create_type, ls))) {
          LOG_WARN("post create ls failed", K(ret));
        }
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }

  return ret;
}

}  // namespace storage

namespace logservice
{

int ObLSAdapter::replay(ObLogReplayTask *replay_task)
{
  int ret = OB_SUCCESS;
  ObLS *ls = NULL;
  ObLSHandle ls_handle;
  int64_t start_ts = ObTimeUtility::fast_current_time();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ObLSAdapter not inited", K(ret));
  } else if (OB_FAIL(ls_service_->get_ls(replay_task->ls_id_, ls_handle, ObLSGetMod::ADAPTER_MOD))) {
    CLOG_LOG(ERROR, "get log stream failed", KPC(replay_task), K(ret));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, " log stream not exist", KPC(replay_task), K(ret));
  /**************************** addtional code *****************************/
  } else if (MTL_ID() == RunCtx.tenant_id_ && LS_ID == ls->get_ls_id() && replay_task->scn_ > REPLAY_BARRIER) {
    if (EXECUTE_COUNT_PER_SEC(1)) {
      fprintf(stdout,
              "stop replaying. tenant_id = %ld ls_id = %ld replay_scn = %lu replay_barrier = %lu\n",
              RunCtx.tenant_id_,
              LS_ID.id(),
              replay_task->scn_.get_val_for_inner_table_field(),
              REPLAY_BARRIER.get_val_for_inner_table_field());
    }
    ret = OB_EAGAIN;
  /**************************** addtional code *****************************/
  } else if (OB_FAIL(ls->replay(replay_task->log_type_,
                                replay_task->get_replay_payload(),
                                replay_task->get_replay_payload_size(),
                                replay_task->lsn_,
                                replay_task->scn_))) {
    CLOG_LOG(WARN, "log stream do replay failed", K(ret), KPC(replay_task));
  }
  if (OB_EAGAIN == ret) {
    if (common::OB_INVALID_TIMESTAMP == replay_task->first_handle_ts_) {
      replay_task->first_handle_ts_ = start_ts;
      replay_task->print_error_ts_ = start_ts;
    } else if ((start_ts - replay_task->print_error_ts_) > MAX_SINGLE_RETRY_WARNING_TIME_THRESOLD) {
      replay_task->retry_cost_ = start_ts - replay_task->first_handle_ts_;
      CLOG_LOG(WARN, "single replay task retry cost too much time. replay may be delayed",
                KPC(replay_task));
      replay_task->print_error_ts_ = start_ts;
    }
  }
  replay_task->replay_cost_ = ObTimeUtility::fast_current_time() - start_ts;
  if (replay_task->replay_cost_ > MAX_SINGLE_REPLAY_WARNING_TIME_THRESOLD) {
    if (replay_task->replay_cost_ > MAX_SINGLE_REPLAY_ERROR_TIME_THRESOLD && !get_replay_is_writing_throttling()) {
      CLOG_LOG(ERROR, "single replay task cost too much time. replay may be delayed", KPC(replay_task));
    } else {
      CLOG_LOG(WARN, "single replay task cost too much time", KPC(replay_task));
    }
  }
  return ret;
}

}


}  // namespace oceanbase
