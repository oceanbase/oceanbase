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

static const char *TEST_FILE_NAME = "test_tx_data_table_mit";
static const char *BORN_CASE_NAME = "ObTxDataTableTest";
static const char *RESTART_CASE_NAME = "ObTxDataTableRestartTest";

int INSERT_THREAD_NUM = 1;


namespace oceanbase
{
using namespace transaction;
using namespace storage;
using namespace palf;
using namespace share;

namespace storage
{

int ObTxDataMemtable::estimate_phy_size(const ObStoreRowkey *start_key,
                                        const ObStoreRowkey *end_key,
                                        int64_t &total_bytes,
                                        int64_t &total_rows)
{
  int ret = OB_SUCCESS;
  if (MTL_ID() == 1002 && freezer_->get_ls_id().id() == 1001) {
    total_bytes = 134217728LL * 2LL;
  } else {
    total_bytes = 1;
  }
  total_rows = 1;
  return ret;
}

}

namespace unittest
{


class TestRunCtx
{
public:
  uint64_t tenant_id_ = 0;
};

TestRunCtx RunCtx;

class ObTxDataTableTest : public ObSimpleClusterTestBase
{
public:
  ObTxDataTableTest() : ObSimpleClusterTestBase(TEST_FILE_NAME) {}
  void insert_tx_data();
  void insert_rollback_tx_data();
  void freeze_tx_data(ObTxDataTable *tx_data_table);
  void check_start_tx_scn(ObTxDataTable *tx_data_table);
  void basic_test();
  void parallel_dump_rollback();
  void check_minor_merge();
  void check_recycle_scn();

private:
  void check_tx_data_minor_succeed();

private:
  bool stop = false;
  int freeze_duration_ = 30 * 1000 * 1000;
  int64_t first_rollback_ts_;
};

#define EXE_SQL(sql_str)                      \
  ASSERT_EQ(OB_SUCCESS, sql.assign(sql_str)); \
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

#define EXE_SQL_FMT(...)                              \
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(__VA_ARGS__)); \
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

#define WRITE_SQL_BY_CONN(conn, sql_str)            \
  ASSERT_EQ(OB_SUCCESS, sql.assign(sql_str)); \
  ASSERT_EQ(OB_SUCCESS, conn->execute_write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));

#define WRITE_SQL_FMT_BY_CONN(conn, ...)                    \
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(__VA_ARGS__)); \
  ASSERT_EQ(OB_SUCCESS, conn->execute_write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));

#define DEF_VAL_FOR_SQL      \
  int ret = OB_SUCCESS;      \
  ObSqlString sql;           \
  int64_t affected_rows = 0;

static int64_t VAL = 0;

void ObTxDataTableTest::insert_tx_data()
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();

  OB_LOG(INFO, "insert tx data start");
  int64_t i = 0;
  int64_t affected_rows = 0;
  while (!ATOMIC_LOAD(&stop)) {
    i = ATOMIC_AAF(&VAL, 1);
    ObSqlString sql;
    EXE_SQL_FMT("insert into test_tx_data_t values(%ld, %ld)", i, i);
  }
}

void ObTxDataTableTest::insert_rollback_tx_data()
{
  DEF_VAL_FOR_SQL
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();

  OB_LOG(INFO, "insert rollback tx data start");
  int64_t i = 0;
  int rollback_cnt = 0;
  first_rollback_ts_ = 0;
  sqlclient::ObISQLConnection *connection = nullptr;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(connection));
  ASSERT_NE(nullptr, connection);

  WRITE_SQL_BY_CONN(connection, "set ob_trx_timeout = 3000000000");
  WRITE_SQL_BY_CONN(connection, "set ob_trx_idle_timeout = 3000000000");
  WRITE_SQL_BY_CONN(connection, "set ob_query_timeout = 3000000000");

  // start transaction
  WRITE_SQL_BY_CONN(connection, "begin");
  while (!ATOMIC_LOAD(&stop)) {
    i = ATOMIC_AAF(&VAL, 1);
    rollback_cnt++;

    WRITE_SQL_BY_CONN(connection, "savepoint x");
    WRITE_SQL_FMT_BY_CONN(connection, "insert into test_tx_data_t values(%ld, %ld)", i, i);
    WRITE_SQL_BY_CONN(connection, "rollback to savepoint x");
    if (0 == first_rollback_ts_) {
      first_rollback_ts_ = ObTimeUtil::current_time_ns();
      STORAGE_LOG(INFO, "", K(first_rollback_ts_));
    }

    WRITE_SQL_FMT_BY_CONN(connection, "insert into test_tx_data_t values(%ld, %ld)", i, i);
  }

  OB_LOG(INFO, "commit multiple rollback transaction.", K(rollback_cnt));
  WRITE_SQL_BY_CONN(connection, "commit");
}

void ObTxDataTableTest::freeze_tx_data(ObTxDataTable *tx_data_table)
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();

  const int64_t start_ts = ObTimeUtility::current_time();
  while (ObTimeUtility::current_time() - start_ts <= freeze_duration_) {
    ::sleep(1);
    int64_t affected_rows = 0;
    ObSqlString sql;
    EXE_SQL("alter system minor freeze tenant sys;");
    EXE_SQL("alter system minor freeze tenant all_user;");
    EXE_SQL("alter system minor freeze tenant all_meta;");
  }
  ATOMIC_STORE(&stop, true);
  STORAGE_LOG(INFO, "freeze done");
}

void ObTxDataTableTest::check_start_tx_scn(ObTxDataTable *tx_data_table)
{
  // wait for minor sstables generate
  // ::sleep(10);
  // MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
  // guard.switch_to(RunCtx.tenant_id_);

  SCN pre_start_tx_scn = SCN::min_scn();
  while (!ATOMIC_LOAD(&stop)) {
    SCN start_tx_scn = SCN::min_scn();
    SCN recycle_scn = SCN::min_scn();
    ASSERT_EQ(OB_SUCCESS, tx_data_table->get_start_tx_scn(start_tx_scn));
    ASSERT_LE(pre_start_tx_scn, start_tx_scn);
    ASSERT_EQ(OB_SUCCESS, tx_data_table->get_recycle_scn(recycle_scn));
    ASSERT_LE(start_tx_scn, recycle_scn);
    pre_start_tx_scn = start_tx_scn;
    OB_LOG(INFO, "check start tx scn : ", K(start_tx_scn), K(recycle_scn));

  //   ::sleep(1);
  }
}

void ObTxDataTableTest::check_tx_data_minor_succeed()
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();

  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  int64_t row_cnt = 0;
  ASSERT_EQ(OB_SUCCESS,
      sql.assign("select count(*) as row_cnt from oceanbase.__all_virtual_tablet_compaction_history where "
                 "tenant_id=1002 and ls_id=1001 and type = 'MINOR_MERGE';"));
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
    sqlclient::ObMySQLResult *result = res.get_result();
    ASSERT_NE(nullptr, result);
    ASSERT_EQ(OB_SUCCESS, result->next());
    ASSERT_EQ(OB_SUCCESS, result->get_int("row_cnt", row_cnt));
    SERVER_LOG(INFO, "row count from test_tx_data_t : ", K(row_cnt));
  }
  ASSERT_GT(row_cnt, 0);
}

void ObTxDataTableTest::basic_test()
{
  // switch tenant
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(RunCtx.tenant_id_));
  // get logstream pointer
  ObLS *ls = nullptr;
  ObLSIterator *ls_iter = nullptr;
  share::ObLSID ls_id(1001);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService *);
  ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD);

  ls = ls_handle.get_ls();
  int64_t clog_checkpoint_before_freeze = ls->get_ls_meta().get_clog_checkpoint_scn().get_val_for_tx();
  LOG_INFO("get clog checkpint before freeze:", K(clog_checkpoint_before_freeze));

  // get tx data table
  ObTxTable *tx_table = nullptr;
  ObTxDataTable *tx_data_table = nullptr;
  ObTxTableGuard tx_table_guard;
  ls->get_tx_table_guard(tx_table_guard);
  ASSERT_NE(nullptr, tx_table = tx_table_guard.get_tx_table());
  ASSERT_NE(nullptr, tx_data_table = tx_table->get_tx_data_table());
  ASSERT_TRUE(tx_data_table->is_inited_);

  std::vector<std::thread> insert_threads;
  for (int i = 0; i < INSERT_THREAD_NUM - 1; ++i) {
    insert_threads.push_back(std::thread([this]() { insert_tx_data(); }));
  }
  insert_threads.push_back(std::thread([this]() { insert_rollback_tx_data(); }));
  std::thread tenant_freeze_thread([this, tx_data_table]() { freeze_tx_data(tx_data_table); });
  // std::thread check_start_tx_scn_thread([this, tx_data_table]() { check_start_tx_scn(tx_data_table); });

  tenant_freeze_thread.join();
  for (int i = 0; i < INSERT_THREAD_NUM; ++i) {
    insert_threads[i].join();
  }
  // check_start_tx_scn_thread.join();

  {
    LOG_INFO("start advance checkpoint by flush", K(first_rollback_ts_));
    share::SCN tmp;
    tmp.convert_for_logservice(first_rollback_ts_);
    int ret = ls->get_checkpoint_executor()->advance_checkpoint_by_flush(tmp);
    int bool_ret = (OB_SUCCESS == ret || OB_NO_NEED_UPDATE == ret) ? true : false;
    ASSERT_EQ(true, bool_ret);
  }
  int64_t clog_checkpoint_after_freeze = 0;
  int wait_update_count = 10;
  while (--wait_update_count > 0) {
    sleep(1);
    ASSERT_EQ(OB_SUCCESS, ls->get_checkpoint_executor()->update_clog_checkpoint());
    clog_checkpoint_after_freeze = ls->get_ls_meta().get_clog_checkpoint_scn().get_val_for_tx();
    if (clog_checkpoint_after_freeze > first_rollback_ts_) {
      break;
    }
  }
  LOG_INFO("get clog checkpint after freeze: ", K(clog_checkpoint_after_freeze));
  ASSERT_NE(clog_checkpoint_before_freeze, clog_checkpoint_after_freeze);
  ASSERT_GT(clog_checkpoint_after_freeze, first_rollback_ts_);

  check_tx_data_minor_succeed();
}

void ObTxDataTableTest::parallel_dump_rollback()
{
  DEF_VAL_FOR_SQL
  common::ObMySQLProxy &sys_proxy = get_curr_simple_server().get_sql_proxy();
  sqlclient::ObISQLConnection *sys_conn = nullptr;
  ASSERT_EQ(OB_SUCCESS, sys_proxy.acquire(sys_conn));
  ASSERT_NE(nullptr, sys_conn);

  common::ObMySQLProxy &tt1_proxy = get_curr_simple_server().get_sql_proxy2();
  sqlclient::ObISQLConnection *tt1_conn = nullptr;
  ASSERT_EQ(OB_SUCCESS, tt1_proxy.acquire(tt1_conn));
  ASSERT_NE(nullptr, tt1_conn);

  WRITE_SQL_BY_CONN(tt1_conn, "create table if not exists test_parallel_dump_tx_data (c1 int, c2 int);");
  WRITE_SQL_BY_CONN(tt1_conn, "set ob_trx_timeout = 3000000000");
  WRITE_SQL_BY_CONN(tt1_conn, "set ob_trx_idle_timeout = 3000000000");
  WRITE_SQL_BY_CONN(tt1_conn, "set ob_query_timeout = 3000000000");

  fprintf(stdout, "start rollback to savepoint\n");
  WRITE_SQL_BY_CONN(tt1_conn, "begin");
  for (int64_t i = 0; i < 1000; i++) {
    WRITE_SQL_BY_CONN(tt1_conn, "savepoint x");
    WRITE_SQL_BY_CONN(tt1_conn, "insert into test_parallel_dump_tx_data values(1,1);");
    WRITE_SQL_BY_CONN(tt1_conn, "rollback to savepoint x");
    WRITE_SQL_BY_CONN(tt1_conn, "insert into test_parallel_dump_tx_data values(1,1);");

    if (0 == i % 100) {
      sys_conn->execute_write(OB_SYS_TENANT_ID, "alter system minor freeze tenant tt1 ls 1001 tablet_id = 49402;", affected_rows);
    }
  }
  WRITE_SQL_BY_CONN(tt1_conn, "commit");
  fprintf(stdout, "finish rollback to savepoint\n");
  sleep(5);

  sys_conn->execute_write(OB_SYS_TENANT_ID, "alter system minor freeze tenant tt1 ls 1001 tablet_id = 49402;", affected_rows);
  sleep(5);
}

void ObTxDataTableTest::check_minor_merge()
{
  DEF_VAL_FOR_SQL
  common::ObMySQLProxy &sys_proxy = get_curr_simple_server().get_sql_proxy();
  sqlclient::ObISQLConnection *sys_conn = nullptr;
  ASSERT_EQ(OB_SUCCESS, sys_proxy.acquire(sys_conn));
  ASSERT_NE(nullptr, sys_conn);

  HEAP_VAR(ObMySQLProxy::MySQLResult, res)
  {
    int64_t retry_times = 10;

    // 确认MINI MERGE 并行转储正确
    int64_t cnt = 0;
    while (--retry_times >= 0) {
      ASSERT_EQ(OB_SUCCESS,
                sys_conn->execute_read(
                    OB_SYS_TENANT_ID,
                    "select count(*) as cnt from oceanbase.__all_virtual_tablet_compaction_history where tenant_id "
                    "= 1002 and ls_id = 1001 and tablet_id = 49402 and parallel_degree > 1 and type = 'MINI_MERGE';",
                    res));
      common::sqlclient::ObMySQLResult *result = res.mysql_result();
      ASSERT_EQ(OB_SUCCESS, result->next());
      ASSERT_EQ(OB_SUCCESS, result->get_int("cnt", cnt));
      if (cnt > 0) {
        break;
      } else {
        sleep(1);
      }
    }
    // ASSERT_GT(cnt, 0);


    // 确认没有未能转储的memtable
    retry_times = 10;
    while (--retry_times >= 0) {
      ASSERT_EQ(OB_SUCCESS,
                sys_conn->execute_read(
                    OB_SYS_TENANT_ID,
                    "select count(*) as cnt from oceanbase.__all_virtual_tx_data_table where state = 'FROZEN' or state = 'FREEZING';",
                    res));
      common::sqlclient::ObMySQLResult *result = res.mysql_result();
      cnt = -1;
      ASSERT_EQ(OB_SUCCESS, result->next());
      ASSERT_EQ(OB_SUCCESS, result->get_int("cnt", cnt));
      if (0 == cnt) {
        break;
      } else {
        sleep(1);
      }
    }
    ASSERT_EQ(0, cnt);
  }

}

void ObTxDataTableTest::check_recycle_scn()
{
  DEF_VAL_FOR_SQL
  common::ObMySQLProxy &sys_proxy = get_curr_simple_server().get_sql_proxy();
  sqlclient::ObISQLConnection *sys_conn = nullptr;
  ASSERT_EQ(OB_SUCCESS, sys_proxy.acquire(sys_conn));
  ASSERT_NE(nullptr, sys_conn);

  HEAP_VAR(ObMySQLProxy::MySQLResult, res)
  {
    ASSERT_EQ(
        OB_SUCCESS,
        sys_conn->execute_read(
            OB_SYS_TENANT_ID,
            "select tenant_id, min_tx_log_scn from oceanbase.__all_virtual_tx_data_table where ls_id = 1 and state = 'MINOR';",
            res));
    common::sqlclient::ObMySQLResult *result = res.mysql_result();

    int tenant_cnt = 0;
    while (OB_SUCCESS == result->next()) {
      int64_t tenant_id = 0;
      int64_t min_tx_log_scn = 0;
      ASSERT_EQ(OB_SUCCESS, result->get_int("tenant_id", tenant_id));
      ASSERT_EQ(OB_SUCCESS, result->get_int("min_tx_log_scn", min_tx_log_scn));
      ASSERT_TRUE(1 == tenant_id || 1001 == tenant_id || 1002 == tenant_id);
      ASSERT_TRUE(min_tx_log_scn > 1);
      tenant_cnt++;
    }
    ASSERT_EQ(tenant_cnt, 3);
  }
}

TEST_F(ObTxDataTableTest, observer_start) { SERVER_LOG(INFO, "observer_start succ"); }

TEST_F(ObTxDataTableTest, add_tenant)
{
  // create tenant
  ASSERT_EQ(OB_SUCCESS, create_tenant());
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(RunCtx.tenant_id_));
  ASSERT_NE(0, RunCtx.tenant_id_);
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
}

TEST_F(ObTxDataTableTest, init_config)
{
  DEF_VAL_FOR_SQL
  common::ObMySQLProxy &sys_proxy = get_curr_simple_server().get_sql_proxy();
  sqlclient::ObISQLConnection *sys_conn = nullptr;
  ASSERT_EQ(OB_SUCCESS, sys_proxy.acquire(sys_conn));
  ASSERT_NE(nullptr, sys_conn);
  WRITE_SQL_BY_CONN(sys_conn, "alter system set _private_buffer_size = '1B'");
  WRITE_SQL_BY_CONN(sys_conn, "alter system set minor_compact_trigger = 0");
  WRITE_SQL_BY_CONN(sys_conn, "alter system set _minor_compaction_amplification_factor = 1");
  sleep(2);
}

TEST_F(ObTxDataTableTest, create_table)
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();

  OB_LOG(INFO, "create_table start");
  ObSqlString sql;
  int64_t affected_rows = 0;
  EXE_SQL("create table if not exists test_tx_data_t (c1 int, c2 int, primary key(c1))");
  OB_LOG(INFO, "create_table succ");
}

TEST_F(ObTxDataTableTest, basic_test)
{
  basic_test();
}

TEST_F(ObTxDataTableTest, parallel_dump_rollback)
{
  parallel_dump_rollback();
}

TEST_F(ObTxDataTableTest, check_minor_merge)
{
  check_minor_merge();
}

TEST_F(ObTxDataTableTest, check_recycle_scn)
{
  check_recycle_scn();
}

class ObTxDataTableRestartTest : public ObSimpleClusterTestBase
{
public:
  ObTxDataTableRestartTest() : ObSimpleClusterTestBase(TEST_FILE_NAME) {}

  void select_existed_data();

};

void ObTxDataTableRestartTest::select_existed_data()
{
  sleep(10);
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();

  int ret = OB_SUCCESS;
  const int MAX_TRY_SELECT_CNT = 20;
  int try_select_cnt = 0;
  bool select_succ = false;

  while (++try_select_cnt <= MAX_TRY_SELECT_CNT) {
    ObSqlString sql;
    int64_t affected_rows = 0;
    int64_t row_cnt = 0;
    ASSERT_EQ(OB_SUCCESS, sql.assign("select count(*) row_cnt from test_tx_data_t"));
    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      if (OB_FAIL(sql_proxy.read(res, sql.ptr()))) {
        // table or schema may not exist yet
        ret = OB_SUCCESS;
      } else {
        sqlclient::ObMySQLResult *result = res.get_result();
        ASSERT_NE(nullptr, result);
        ASSERT_EQ(OB_SUCCESS, result->next());
        ASSERT_EQ(OB_SUCCESS, result->get_int("row_cnt", row_cnt));
        SERVER_LOG(INFO, "row count from test_tx_data_t : ", K(row_cnt));
      }
    }

    if (row_cnt > 0) {
      select_succ = true;
      LOG_INFO("select done", K(try_select_cnt));
      break;
    } else {
      LOG_INFO("select once", K(try_select_cnt));
      ::sleep(1);
    }
  }

  ASSERT_EQ(select_succ, true);
}

TEST_F(ObTxDataTableRestartTest, observer_restart)
{
  // init sql proxy2 to use tenant tt1
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
  SERVER_LOG(INFO, "observer restart succ");
}

TEST_F(ObTxDataTableRestartTest, select_existed_data)
{
  select_existed_data();
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
  INSERT_THREAD_NUM = concurrency;
  ObSimpleServerRestartHelper restart_helper(argc, argv, TEST_FILE_NAME, BORN_CASE_NAME,
                                             RESTART_CASE_NAME);
  restart_helper.set_sleep_sec(time_sec);
  restart_helper.run();

  return ret;
}
