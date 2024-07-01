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
#define USING_LOG_PREFIX TABLELOCK
#define protected public
#define private public

#include "env/ob_simple_cluster_test_base.h"
#include "env/ob_simple_server_restart_helper.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tablelock/ob_lock_memtable.h"
#include "logservice/ob_log_base_type.h"
#include "mtlenv/tablelock/table_lock_tx_common_env.h"
#include "storage/tx_storage/ob_ls_handle.h" //ObLSHandle
#include "share/schema/ob_schema_getter_guard.h"
#include "storage/tablelock/ob_table_lock_service.h"
#include "share/ob_ls_id.h"

static const char *TEST_FILE_NAME = "test_lock_table_with_tx";
static const char *BORN_CASE_NAME = "ObLockTableBeforeRestartTest";
static const char *RESTART_CASE_NAME = "ObLockTableAfterRestartTest";

namespace oceanbase
{
namespace unittest
{

using namespace oceanbase::transaction;
using namespace oceanbase::storage;
using namespace oceanbase::share;
using namespace oceanbase::storage::checkpoint;

class TestRunCtx
{
public:
  uint64_t tenant_id_ = 0;
  int time_sec_ = 0;
};

TestRunCtx RunCtx;

class ObLockTableBeforeRestartTest : public ObSimpleClusterTestBase
{
public:
  ObLockTableBeforeRestartTest() : ObSimpleClusterTestBase(TEST_FILE_NAME) {}
  void get_table_id(const char* tname, uint64_t &table_id);
  void insert_data(SCN c1, SCN c2);
};

#define EXE_SQL(sql_str)                                            \
  ASSERT_EQ(OB_SUCCESS, sql.assign(sql_str));                       \
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

#define EXE_SQL_FMT(...)                                            \
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(__VA_ARGS__));               \
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

#define WRITE_SQL_BY_CONN(conn, sql_str)                                \
  ASSERT_EQ(OB_SUCCESS, sql.assign(sql_str));                           \
  ASSERT_EQ(OB_SUCCESS, conn->execute_write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));

#define WRITE_SQL_FMT_BY_CONN(conn, ...)                                \
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(__VA_ARGS__));                   \
  ASSERT_EQ(OB_SUCCESS, conn->execute_write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));

#define READ_SQL_BY_CONN(conn, sql_str)         \
  ASSERT_EQ(OB_SUCCESS, sql.assign(sql_str));                           \
  ASSERT_EQ(OB_SUCCESS, conn->execute_read(OB_SYS_TENANT_ID, sql.ptr(), read_res));

void ObLockTableBeforeRestartTest::get_table_id(const char* tname, uint64_t &table_id)
{
  ObSchemaGetterGuard guard;
  ASSERT_EQ(OB_SUCCESS, GCTX.schema_service_->get_tenant_schema_guard(RunCtx.tenant_id_, guard));
  ASSERT_EQ(OB_SUCCESS,
            guard.get_table_id(RunCtx.tenant_id_, "test", tname,
                               false, share::schema::ObSchemaGetterGuard::NON_TEMP_WITH_NON_HIDDEN_TABLE_TYPE, table_id));
}

void ObLockTableBeforeRestartTest::insert_data(SCN c1, SCN c2)
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  int64_t affected_rows = 0;
  ObSqlString sql;
  sqlclient::ObISQLConnection *connection = nullptr;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(connection));
  ASSERT_NE(nullptr, connection);

  WRITE_SQL_BY_CONN(connection, "begin;");
  WRITE_SQL_FMT_BY_CONN(connection, "insert into t_persist values(%lu, %lu)",
                        c1.get_val_for_inner_table_field(),
                        c2.get_val_for_inner_table_field());
  WRITE_SQL_BY_CONN(connection, "commit;");
}

TEST_F(ObLockTableBeforeRestartTest, observer_start)
{
  LOG_INFO("observer_start succ");
}

TEST_F(ObLockTableBeforeRestartTest, add_tenant)
{
  // create tenant
  ASSERT_EQ(OB_SUCCESS, create_tenant());
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(RunCtx.tenant_id_));
  ASSERT_NE(0, RunCtx.tenant_id_);
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
  // to make location exist?
  usleep(10 * 1000 * 1000);
}

TEST_F(ObLockTableBeforeRestartTest, create_table)
{
  LOG_INFO("ObTableLockServiceTest::create_table");
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  {
    LOG_INFO("create_table start");
    ObSqlString sql;
    int64_t affected_rows = 0;
    EXE_SQL("create table t_one_part (c1 bigint, c2 bigint, primary key(c1))");
    EXE_SQL("create table t_persist (c1 bigint, c2 bigint, primary key(c1))");
    LOG_INFO("create_table succ");
  }
}

TEST_F(ObLockTableBeforeRestartTest, minor_freeze)
{
  LOG_INFO("minor_freeze start");
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  int64_t affected_rows = 0;
  ObSqlString sql;
  EXE_SQL("alter system minor freeze tenant tt1;");
  LOG_INFO("minor_freeze finish");
}

TEST_F(ObLockTableBeforeRestartTest, test_commit_log)
{
  LOG_INFO("ObLockTableBeforeRestartTest::test_commit_log");
  // switch tenant
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(RunCtx.tenant_id_));

  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTableLockOwnerID OWNER_ONE;
  OWNER_ONE.convert_from_value(1);
  uint64_t table_id = 0;
  ObTableLockMode lock_mode = EXCLUSIVE;
  ObTableLockOwnerID lock_owner;
  lock_owner.convert_from_value(0);
  ObLS *ls = nullptr;
  ObLockMemtable *lock_memtable = nullptr;
  ObCheckpointExecutor *checkpoint_executor = nullptr;
  ObTxCtxMemtable *tx_ctx_memtable = nullptr;
  ObTxDataMemtableMgr *tx_data_mgr = nullptr;
  ObLSHandle handle;
  share::ObLSID ls_id = share::LOCK_SERVICE_LS;

  ObTableLockService *table_lock_svr = MTL(ObTableLockService*);
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_NE(nullptr, table_lock_svr);
  ASSERT_NE(nullptr, ls_svr);
  // 1. LOCK TABLE
  // 1.1 lock table
  LOG_INFO("ObLockTableBeforeRestartTest::test_commit_log 1.1");
  get_table_id("t_one_part", table_id);
  ret = table_lock_svr->lock_table(table_id,
                                   lock_mode,
                                   OWNER_ONE);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 1.2 check exist
  LOG_INFO("ObLockTableBeforeRestartTest::test_commit_log 1.2");
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, handle, ObLSGetMod::TABLELOCK_MOD));
  ASSERT_NE(nullptr, ls = handle.get_ls());
  checkpoint_executor = ls->get_checkpoint_executor();
  ASSERT_NE(nullptr, checkpoint_executor);

  lock_memtable = dynamic_cast<ObLockMemtable *>(dynamic_cast<ObLSTxService *>(checkpoint_executor
             ->handlers_[logservice::TRANS_SERVICE_LOG_BASE_TYPE])
                 ->common_checkpoints_[ObCommonCheckpointType::LOCK_MEMTABLE_TYPE]);

  lock_memtable->obj_lock_map_.print();
  LOG_INFO("ObLockTableBeforeRestartTest::test_commit_log 1.2 wait tablelock committed");
  // after the commit process return, the tablelock bottom may not committed yet.
  while (lock_memtable->get_rec_scn() == SCN::max_scn()) {
    usleep(100 * 1000);
  }
  SCN rec_scn = lock_memtable->get_rec_scn();
  LOG_INFO("ObLockTableBeforeRestartTest::test_commit_log 1.2 rec scn after tablelock committed", K(rec_scn));

  // 1.3 dump tx ctx table/ tx data table
  LOG_INFO("ObLockTableBeforeRestartTest::test_commit_log 1.3");
  tx_ctx_memtable
    = dynamic_cast<ObTxCtxMemtable *>(dynamic_cast<ObLSTxService *>(checkpoint_executor
        ->handlers_[logservice::TRANS_SERVICE_LOG_BASE_TYPE])
            ->common_checkpoints_[ObCommonCheckpointType::TX_CTX_MEMTABLE_TYPE]);
  tx_data_mgr
    = dynamic_cast<ObTxDataMemtableMgr *>(dynamic_cast<ObLSTxService *>(checkpoint_executor
        ->handlers_[logservice::TRANS_SERVICE_LOG_BASE_TYPE])
            ->common_checkpoints_[ObCommonCheckpointType::TX_DATA_MEMTABLE_TYPE]);

  while (OB_EAGAIN == tx_ctx_memtable->flush(SCN::max_scn(), true)) {
    usleep(100 * 1000);
  }
  LOG_INFO("ObLockTableBeforeRestartTest::test_commit_log 1.3 tx_ctx_memtable flush finished");
  while (OB_EAGAIN == tx_data_mgr->flush(SCN::max_scn(), true)) {
    usleep(100 * 1000);
  }
  LOG_INFO("ObLockTableBeforeRestartTest::test_commit_log 1.3 tx_data_mgr flush finished");

  // 1.4 update ls checkpoint(should be the lock op commit scn)
  // wait until ls checkpoint updated.
  LOG_INFO("ObLockTableBeforeRestartTest::test_commit_log 1.4");
  // freeze data, make sure other type flushed.
  ASSERT_EQ(OB_SUCCESS, ls->logstream_freeze(checkpoint::INVALID_TRACE_ID, false /*is_sync*/));
  while (ls->get_clog_checkpoint_scn() < rec_scn) {
    usleep(100 * 1000); // sleep 100 ms
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
      LOG_INFO("ls checkpoint smaller than expected", K(ls->get_clog_checkpoint_scn()), K(rec_scn));
    }
  }
  // 1.5 record lock table rec_scn and ls clog checkpoint scn
  LOG_INFO("ObLockTableBeforeRestartTest::test_commit_log 1.5", K(lock_memtable->get_rec_scn()),
           K(ls->get_clog_checkpoint_scn()));
  insert_data(lock_memtable->get_rec_scn(), ls->get_clog_checkpoint_scn());
  // 1.6 restart and recheck the lock

}

class ObLockTableAfterRestartTest : public ObSimpleClusterTestBase
{
public:
  ObLockTableAfterRestartTest() : ObSimpleClusterTestBase(TEST_FILE_NAME) {}
  void select_existed_data(SCN &lock_scn, SCN &ls_checkpoint_scn);
};

void ObLockTableAfterRestartTest::select_existed_data(SCN &lock_scn, SCN &ls_checkpoint_scn)
{
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();

  int ret = OB_SUCCESS;
  const int MAX_TRY_SELECT_CNT = 20;
  int try_select_cnt = 0;

  while (++try_select_cnt <= MAX_TRY_SELECT_CNT) {
    ObSqlString sql;
    int64_t affected_rows = 0;
    int64_t row_cnt = 0;
    ASSERT_EQ(OB_SUCCESS, sql.assign("select * from t_persist"));
    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      if (OB_FAIL(sql_proxy.read(res, sql.ptr()))) {
        // table or schema may not exist yet
        ret = OB_SUCCESS;
      } else {
        sqlclient::ObMySQLResult *result = res.get_result();
        ASSERT_NE(nullptr, result);
        int64_t lock_val = 0;
        int64_t ls_val = 0;
        ASSERT_EQ(OB_SUCCESS, result->next());
        ASSERT_EQ(OB_SUCCESS, result->get_int("c1", lock_val));
        ASSERT_EQ(OB_SUCCESS, result->get_int("c2", ls_val));
        ASSERT_EQ(OB_SUCCESS, lock_scn.convert_for_tx(lock_val));
        ASSERT_EQ(OB_SUCCESS, ls_checkpoint_scn.convert_for_tx(ls_val));
        break;
      }
    }

    ::sleep(1);
  }
}

TEST_F(ObLockTableAfterRestartTest, test_recover_lock_table)
{
  // ============================== restart successfully ==============================
  //switch tenant
  LOG_INFO("ObLockTableAfterRestartTest::test_recover_lock_table");
  uint64_t tenant_id = 0;
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id));
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));

  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLSService *ls_svr = MTL(ObLSService*);
  SCN lock_scn, ls_checkpoint_scn;
  ASSERT_NE(nullptr, ls_svr);
  ObLS *ls = nullptr;
  ObLSHandle handle;
  share::ObLSID ls_id = share::LOCK_SERVICE_LS;
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD));
  ASSERT_NE(nullptr, ls = handle.get_ls());
  ObCheckpointExecutor *checkpoint_executor = ls->get_checkpoint_executor();
  ASSERT_NE(nullptr, checkpoint_executor);

  // get lock_scn from table
  select_existed_data(lock_scn, ls_checkpoint_scn);

  // check lock_memtable scn
  ObLockMemtable *lock_memtable
    = dynamic_cast<ObLockMemtable *>(dynamic_cast<ObLSTxService *>(checkpoint_executor
            ->handlers_[logservice::TRANS_SERVICE_LOG_BASE_TYPE])
            ->common_checkpoints_[ObCommonCheckpointType::LOCK_MEMTABLE_TYPE]);
  lock_memtable->obj_lock_map_.print();
  ASSERT_EQ(lock_memtable->get_rec_scn(), lock_scn);
  ASSERT_EQ(lock_memtable->max_committed_scn_, lock_scn);
  LOG_INFO("ObLockTableAfterRestartTest::test_recover_lock_table finish");
}

} //unitest
} //oceanbase

int main(int argc, char **argv)
{
  int c = 0;
  int time_sec = 0;
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
      default:
        break;
    }
  }

  std::string gtest_file_name = std::string(TEST_FILE_NAME) + "_gtest.log";
  oceanbase::unittest::init_gtest_output(gtest_file_name);
  int ret = 0;
  ObSimpleServerRestartHelper restart_helper(argc, argv, TEST_FILE_NAME, BORN_CASE_NAME,
                                             RESTART_CASE_NAME);
  restart_helper.set_sleep_sec(time_sec + 10); // sleep 10s for schema restore
  restart_helper.run();

  return ret;
}
