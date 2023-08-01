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
#include "lib/mysqlclient/ob_mysql_result.h"

#include "storage/ls/ob_ls.h"
#include "storage/tx_storage/ob_ls_map.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx_storage/ob_tenant_freezer.h"

namespace oceanbase
{
using namespace transaction;
using namespace storage;
using namespace share;
using namespace storage::mds;

namespace unittest
{

class TestRunCtx
{
public:
  uint64_t tenant_id_ = 0;
  int time_sec_ = 0;
};

TestRunCtx RunCtx;
ObLSID LS_ID;

class ObMdsTableCheckpointTest : public ObSimpleClusterTestBase
{
public:
  // 指定case运行目录前缀 test_ob_simple_cluster_
  ObMdsTableCheckpointTest() : ObSimpleClusterTestBase("test_mds_table_checkpoint_") {}

  void do_basic_test();

  void create_mds_table();
  void get_ls_id();

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

#define DEF_VAL_FOR_SQL      \
  int ret = OB_SUCCESS;      \
  ObSqlString sql;           \
  int64_t affected_rows = 0; \
  sqlclient::ObISQLConnection *connection = nullptr;

void ObMdsTableCheckpointTest::do_basic_test()
{
  create_mds_table();
  get_ls_id();
  ObLS *ls = get_ls(RunCtx.tenant_id_, LS_ID);

  int ret = OB_SUCCESS;
  MTL_SWITCH(RunCtx.tenant_id_) {
    MdsTableMgrHandle mgr_handle;
    ObMdsTableMgr *mgr = nullptr;
    ASSERT_EQ(OB_SUCCESS, ls->get_tablet_svr()->get_mds_table_mgr(mgr_handle));
    ASSERT_NE(nullptr, mgr = mgr_handle.get_mds_table_mgr());
    SCN scn_before_flush = mgr->get_rec_scn();
    MDS_LOG(INFO, "start flush ls mds table", K(LS_ID));
    ASSERT_EQ(OB_SUCCESS, ls->flush_mds_table(INT64_MAX));
    MDS_LOG(INFO, "finish flush ls mds table", K(LS_ID));
    ::sleep(10);
    SCN scn_after_flush = mgr->get_rec_scn();
    ASSERT_LT(scn_before_flush, scn_after_flush);
    fprintf(stdout,
            "finish flush mds table, scn_before_flush = %ld scn_after_flush = "
            "%ld\n",
            scn_before_flush.get_val_for_tx(),
            scn_after_flush.get_val_for_tx());
  }
}

void ObMdsTableCheckpointTest::create_mds_table()
{
  DEF_VAL_FOR_SQL
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().get_sql_proxy2().acquire(connection));

  ObTenantFreezer::MDS_TABLE_FREEZE_TRIGGER_TENANT_PERCENTAGE = 0.01;

  for (int i = 0; i < 300; i++) {
    MDS_LOG(DEBUG, "create table :", K(i));
    WRITE_SQL_FMT_BY_CONN(connection, "create table if not exists test_mds_table_checkpoint_%d (c1 int, c2 int)", i);
    WRITE_SQL_FMT_BY_CONN(connection, "insert into test_mds_table_checkpoint_%d values(%d, %d)", i, 1, 1);
  }
}

void ObMdsTableCheckpointTest::get_ls_id()
{
  DEF_VAL_FOR_SQL

  HEAP_VAR(ObMySQLProxy::MySQLResult, res)
  {
    int64_t int_tablet_id = 0;
    int64_t int_ls_id = 0;
    int64_t retry_times = 10;

    ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().get_sql_proxy2().acquire(connection));
    ASSERT_EQ(
        OB_SUCCESS,
        connection->execute_read(OB_SYS_TENANT_ID,
                                 "SELECT tablet_id, ls_id FROM oceanbase.DBA_OB_TABLE_LOCATIONS WHERE TABLE_NAME = "
                                 "\'test_mds_table_checkpoint_0\'",
                                 res));
    common::sqlclient::ObMySQLResult *result = res.mysql_result();
    ASSERT_EQ(OB_SUCCESS, result->next());
    ASSERT_EQ(OB_SUCCESS, result->get_int("tablet_id", int_tablet_id));
    ASSERT_EQ(OB_SUCCESS, result->get_int("ls_id", int_ls_id));
    LS_ID = int_ls_id;
    fprintf(stdout, "get ls info finish : ls_id = %ld\n", LS_ID.id());
  }
}

TEST_F(ObMdsTableCheckpointTest, observer_start)
{
  SERVER_LOG(INFO, "observer_start succ");
}

TEST_F(ObMdsTableCheckpointTest, add_tenant)
{
  // create tenant
  ASSERT_EQ(OB_SUCCESS, create_tenant());
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(RunCtx.tenant_id_));
  ASSERT_NE(0, RunCtx.tenant_id_);
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
}

TEST_F(ObMdsTableCheckpointTest, basic_test)
{
  do_basic_test();
}

TEST_F(ObMdsTableCheckpointTest, end)
{
  if (RunCtx.time_sec_ > 0) {
    ::sleep(RunCtx.time_sec_);
  }
}

} // end unittest
} // end oceanbase


int main(int argc, char **argv)
{
  int c = 0;
  int time_sec = 0;
  char *log_level = (char*)"INFO";
  while(EOF != (c = getopt(argc,argv,"t:l:"))) {
    switch(c) {
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
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level(log_level);

  LOG_INFO("main>>>");
  oceanbase::unittest::RunCtx.time_sec_ = time_sec;
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
