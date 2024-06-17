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
#define USING_LOG_PREFIX STORAGE
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

static const char *TEST_FILE_NAME = "test_lock_table_persistence";
static const char *BORN_CASE_NAME = "ObLockTableBeforeRestartTest";
static const char *RESTART_CASE_NAME = "ObLockTableAfterRestartTest";

static oceanbase::share::SCN lock_scn;
static oceanbase::share::SCN unlock_scn;

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

//static int64_t lock_memtable_rec_scn = SCN::max_scn();

class ObLockTableBeforeRestartTest : public ObSimpleClusterTestBase
{
public:
  ObLockTableBeforeRestartTest() : ObSimpleClusterTestBase(TEST_FILE_NAME) {}
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

void ObLockTableBeforeRestartTest::insert_data(SCN c1, SCN c2)
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  int64_t affected_rows = 0;
  ObSqlString sql;
  sqlclient::ObISQLConnection *connection = nullptr;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(connection));
  ASSERT_NE(nullptr, connection);

  WRITE_SQL_BY_CONN(connection, "begin;");
  WRITE_SQL_FMT_BY_CONN(connection, "insert into test_lock_table_persistence_t values(%lu, %lu)",
                        c1.get_val_for_inner_table_field(),
                        c2.get_val_for_inner_table_field());
  WRITE_SQL_BY_CONN(connection, "commit;");
}

TEST_F(ObLockTableBeforeRestartTest, observer_start)
{
  SERVER_LOG(INFO, "observer_start succ");
}

TEST_F(ObLockTableBeforeRestartTest, add_tenant)
{
  // create tenant
  ASSERT_EQ(OB_SUCCESS, create_tenant());
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(RunCtx.tenant_id_));
  ASSERT_NE(0, RunCtx.tenant_id_);
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
}

TEST_F(ObLockTableBeforeRestartTest, create_table)
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  {
    OB_LOG(INFO, "create_table start");
    ObSqlString sql;
    int64_t affected_rows = 0;
    EXE_SQL("create table test_lock_table_persistence_t (c1 bigint, c2 bigint, primary key(c1))");
    OB_LOG(INFO, "create_table succ");
  }
}

TEST_F(ObLockTableBeforeRestartTest, test_lock_table_flush)
{
  // switch tenant
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(RunCtx.tenant_id_));

  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_NE(nullptr, ls_svr);
  ObLS *ls = nullptr;
  ObLSHandle handle;
  ObTableLockOwnerID owner_id;
  owner_id.convert_from_value(1);
  share::ObLSID ls_id = share::LOCK_SERVICE_LS;
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD));
  ASSERT_NE(nullptr, ls = handle.get_ls());
  ObCheckpointExecutor *checkpoint_executor = ls->get_checkpoint_executor();
  ASSERT_NE(nullptr, checkpoint_executor);

  ObSchemaGetterGuard guard;
  ASSERT_EQ(OB_SUCCESS, GCTX.schema_service_->get_tenant_schema_guard(RunCtx.tenant_id_, guard));

  //lock table
  uint64_t table_id = 0;
  ASSERT_EQ(OB_SUCCESS,
    guard.get_table_id(RunCtx.tenant_id_, "test", "test_lock_table_persistence_t",
    false, share::schema::ObSchemaGetterGuard::NON_TEMP_WITH_NON_HIDDEN_TABLE_TYPE, table_id));
  ObTableLockService *table_lock_ser = MTL(ObTableLockService*);
  ASSERT_EQ(OB_SUCCESS, table_lock_ser->lock_table(table_id, EXCLUSIVE, owner_id, 0));
  usleep(1000 * 1000);

  ObLockMemtable *lock_memtable
    = dynamic_cast<ObLockMemtable *>(dynamic_cast<ObLSTxService *>(checkpoint_executor
            ->handlers_[logservice::TRANS_SERVICE_LOG_BASE_TYPE])
            ->common_checkpoints_[ObCommonCheckpointType::LOCK_MEMTABLE_TYPE]);

  SCN rec_scn = lock_memtable->get_rec_scn();
  ASSERT_NE(rec_scn, SCN::max_scn());
  lock_scn = rec_scn;

  // flush
  ASSERT_EQ(OB_SUCCESS, lock_memtable->flush(SCN::max_scn(), true));
  ASSERT_EQ(lock_memtable->freeze_scn_, rec_scn);
  int retry_time = 0;
  while (lock_memtable->is_frozen_memtable()) {
    usleep(1000 * 1000);
    retry_time++;
    if (retry_time % 5 == 0) {
      OB_LOG(WARN, "wait lock memtable flush finish use too much time",
             K(retry_time), KPC(lock_memtable));
    }
  }
  ASSERT_EQ(lock_memtable->get_rec_scn(), SCN::max_scn());
  ASSERT_EQ(lock_memtable->flushed_scn_, lock_memtable->freeze_scn_);

  //unlock table
  ASSERT_EQ(OB_SUCCESS, table_lock_ser->unlock_table(table_id, EXCLUSIVE, owner_id, 0));
  usleep(1000 * 1000);
  unlock_scn = lock_memtable->get_rec_scn();
  ASSERT_NE(lock_memtable->get_rec_scn(), SCN::max_scn());

  //record scn to use when recover successfully
  insert_data(lock_scn, unlock_scn);
}

class ObLockTableAfterRestartTest : public ObSimpleClusterTestBase
{
public:
  ObLockTableAfterRestartTest() : ObSimpleClusterTestBase(TEST_FILE_NAME) {}
  void select_existed_data(SCN &lock_scn, SCN &unlock_scn);
};

void ObLockTableAfterRestartTest::select_existed_data(SCN &lock_scn, SCN &unlock_scn)
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
    ASSERT_EQ(OB_SUCCESS, sql.assign("select * from test_lock_table_persistence_t"));
    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      if (OB_FAIL(sql_proxy.read(res, sql.ptr()))) {
        // table or schema may not exist yet
        ret = OB_SUCCESS;
      } else {
        sqlclient::ObMySQLResult *result = res.get_result();
        ASSERT_NE(nullptr, result);
        int64_t lock_val = 0;
        int64_t unlock_val = 0;
        ASSERT_EQ(OB_SUCCESS, result->next());
        ASSERT_EQ(OB_SUCCESS, result->get_int("c1", lock_val));
        ASSERT_EQ(OB_SUCCESS, result->get_int("c2", unlock_val));
        ASSERT_EQ(OB_SUCCESS, lock_scn.convert_for_tx(lock_val));
        ASSERT_EQ(OB_SUCCESS, unlock_scn.convert_for_tx(unlock_val));
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
  uint64_t tenant_id = 0;
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id));
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));

  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_NE(nullptr, ls_svr);
  ObLS *ls = nullptr;
  ObLSHandle handle;
  share::ObLSID ls_id = share::LOCK_SERVICE_LS;
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD));
  ASSERT_NE(nullptr, ls = handle.get_ls());
  ObCheckpointExecutor *checkpoint_executor = ls->get_checkpoint_executor();
  ASSERT_NE(nullptr, checkpoint_executor);

  // get lock_scn from table
  select_existed_data(lock_scn, unlock_scn);

  // check lock_memtable scn
  ObLockMemtable *lock_memtable
    = dynamic_cast<ObLockMemtable *>(dynamic_cast<ObLSTxService *>(checkpoint_executor
            ->handlers_[logservice::TRANS_SERVICE_LOG_BASE_TYPE])
            ->common_checkpoints_[ObCommonCheckpointType::LOCK_MEMTABLE_TYPE]);
  ASSERT_EQ(lock_memtable->get_rec_scn(), unlock_scn);
  ASSERT_EQ(lock_memtable->flushed_scn_, lock_scn);
  ASSERT_EQ(lock_memtable->max_committed_scn_, unlock_scn);
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
