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
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/tx_storage/ob_ls_handle.h" //ObLSHandle

#undef private
#undef protected

static const char *TEST_FILE_NAME = "test_tx_recover";
static const char *BORN_CASE_NAME = "ObTxBeforeRestartTest";
static const char *RESTART_CASE_NAME = "ObTxAfterRestartTest";

namespace oceanbase
{
namespace unittest
{

using namespace oceanbase::transaction;
using namespace oceanbase::storage;
using namespace oceanbase::storage::checkpoint;

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

class ObTxBeforeRestartTest : public ObSimpleClusterTestBase
{
public:
  ObTxBeforeRestartTest() : ObSimpleClusterTestBase(TEST_FILE_NAME) {}
  void prepare_tenant_env()
  {
    common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
    int64_t affected_rows = 0;
    ObSqlString sql;
    sqlclient::ObISQLConnection *connection = nullptr;
    ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(connection));
    ASSERT_NE(nullptr, connection);
    WRITE_SQL_BY_CONN(connection, "set ob_trx_timeout = 3000000000");
    WRITE_SQL_BY_CONN(connection, "set ob_trx_idle_timeout = 3000000000");
    WRITE_SQL_BY_CONN(connection, "set ob_query_timeout = 3000000000");
    WRITE_SQL_BY_CONN(connection, "set autocommit=0");
  }

  void prepare_sys_env()
  {
    common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
    int64_t affected_rows = 0;
    ObSqlString sql;
    EXE_SQL("alter system set _private_buffer_size = '1B';");
  }

  void create_test_tenant(uint64_t &tenant_id)
  {
    TRANS_LOG(INFO, "create_tenant start");
    ASSERT_EQ(OB_SUCCESS, create_tenant());
    ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id));
    ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
    TRANS_LOG(INFO, "create_tenant end", K(tenant_id));
  }

  void get_ls(uint64_t tenant_id, ObLS *&ls)
  {
    ls = nullptr;
    share::ObTenantSwitchGuard tenant_guard;
    ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));

    ObLSService *ls_svr = MTL(ObLSService*);
    ASSERT_NE(nullptr, ls_svr);
    ObLSHandle handle;
    share::ObLSID ls_id(1001);
    ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD));
    ASSERT_NE(nullptr, ls = handle.get_ls());
  }

  void minor_freeze_data_memtable(ObLS *ls)
  {
    TRANS_LOG(INFO, "minor_freeze_data_memtable begin");
    ASSERT_EQ(OB_SUCCESS, ls->ls_freezer_.logstream_freeze(0));

    // TODO(handora.qc): use more graceful wait
    usleep(10 * 1000 * 1000);

    TRANS_LOG(INFO, "minor_freeze_data_memtable end");
  }

  void minor_freeze_tx_ctx_memtable(ObLS *ls)
    {
      TRANS_LOG(INFO, "minor_freeze_tx_ctx_memtable begin");

      ObCheckpointExecutor *checkpoint_executor = ls->get_checkpoint_executor();
      ASSERT_NE(nullptr, checkpoint_executor);
      ObTxCtxMemtable *tx_ctx_memtable
        = dynamic_cast<ObTxCtxMemtable *>(
          dynamic_cast<ObLSTxService *>(
            checkpoint_executor->handlers_[logservice::TRANS_SERVICE_LOG_BASE_TYPE])
          ->common_checkpoints_[ObCommonCheckpointType::TX_CTX_MEMTABLE_TYPE]);
      ASSERT_EQ(OB_SUCCESS, tx_ctx_memtable->flush(share::SCN::max_scn(), 0));

      // TODO(handora.qc): use more graceful wait
      usleep(10 * 1000 * 1000);

      TRANS_LOG(INFO, "minor_freeze_tx_ctx_memtable end");
    }
};

TEST_F(ObTxBeforeRestartTest, recover_tx_ctx_before_memtable)
{
  ObSqlString sql;
  int64_t affected_rows = 0;

  // ============================== create tenant and table ==============================
  uint64_t tenant_id = 0;
  create_test_tenant(tenant_id);

  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));

  TRANS_LOG(INFO, "create_table start");
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  EXE_SQL("create table test_tx_recover (a int)");
  TRANS_LOG(INFO, "create_table end");

  TRANS_LOG(INFO, "get checkpoint start");
  ObLS *ls = NULL;
  get_ls(tenant_id, ls);
  TRANS_LOG(INFO, "get ls end");

  ObCheckpointExecutor *checkpoint_executor = ls->get_checkpoint_executor();
  ASSERT_NE(nullptr, checkpoint_executor);

  ObTxCtxMemtable *tx_ctx_memtable
    = dynamic_cast<ObTxCtxMemtable *>(
      dynamic_cast<ObLSTxService *>(checkpoint_executor
                                    ->handlers_[logservice::TRANS_SERVICE_LOG_BASE_TYPE])
      ->common_checkpoints_[ObCommonCheckpointType::TX_CTX_MEMTABLE_TYPE]);
  TRANS_LOG(INFO, "get checkpoint end");

  minor_freeze_tx_ctx_memtable(ls);

  // ============================== create a txn and write into it ==============================
  sqlclient::ObISQLConnection *connection = nullptr;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(connection));
  ASSERT_NE(nullptr, connection);

  ASSERT_EQ(tx_ctx_memtable->get_rec_scn(), share::SCN::max_scn());

  TRANS_LOG(INFO, "insert data start");
  WRITE_SQL_BY_CONN(connection, "begin;");
  WRITE_SQL_FMT_BY_CONN(connection, "insert into test_tx_recover values(1);");
  TRANS_LOG(INFO, "insert data finish");

  ASSERT_NE(tx_ctx_memtable->get_rec_scn(), share::SCN::max_scn());

  // ============================== only freeze data memtable ==============================
  minor_freeze_data_memtable(ls);

  ASSERT_NE(tx_ctx_memtable->get_rec_scn(), share::SCN::max_scn());

  // ============================== commit the txn ==============================
  WRITE_SQL_BY_CONN(connection, "insert into test_tx_recover values(2);");
  WRITE_SQL_BY_CONN(connection, "commit;");

  ASSERT_EQ(tx_ctx_memtable->get_rec_scn(), share::SCN::max_scn());
}

class ObTxAfterRestartTest : public ObSimpleClusterTestBase
{
public:
  ObTxAfterRestartTest() : ObSimpleClusterTestBase(TEST_FILE_NAME) {}
};

TEST_F(ObTxAfterRestartTest, recover_tx_ctx_before_memtable_recover)
{
  ObSqlString sql;
  int64_t affected_rows = 0;
  ObISQLClient::ReadResult read_res;

  // ============================== restart successfully ==============================
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  sqlclient::ObISQLConnection *connection = nullptr;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(connection));
  ASSERT_NE(nullptr, connection);
  WRITE_SQL_FMT_BY_CONN(connection, "insert into test_tx_recover values(3);");
  READ_SQL_BY_CONN(connection, "select * from test_tx_recover");
  sqlclient::ObMySQLResult *result = read_res.get_result();
  int i = 0;
  int ret = 0;
  while (OB_SUCC(result->next())) {
    i++;
    int64_t a;
    ASSERT_EQ(OB_SUCCESS, result->get_int("a", a));
    ASSERT_EQ(a, i);
  }
  EXPECT_EQ(3, i);

  TRANS_LOG(INFO, "restart ok");
}

} // namespace unittest
} // namespace oceanbase


int main(int argc, char **argv)
{
  int ret = 0;
  std::string gtest_file_name = std::string(TEST_FILE_NAME) + "_gtest.log";
  oceanbase::unittest::init_gtest_output(gtest_file_name);
  ObSimpleServerRestartHelper restart_helper(argc,
                                             argv,
                                             TEST_FILE_NAME,
                                             BORN_CASE_NAME,
                                             RESTART_CASE_NAME);
  restart_helper.set_sleep_sec(10); // sleep 10s for schema restore
  restart_helper.run();

  return ret;
}
