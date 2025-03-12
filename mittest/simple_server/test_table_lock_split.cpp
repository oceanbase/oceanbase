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

static const char *TEST_FILE_NAME = "test_table_lock_split";
static const char *BORN_CASE_NAME = "ObLockTableSplitBeforeRestartTest";
static const char *RESTART_CASE_NAME = "ObLockTableSplitAfterRestartTest";

namespace oceanbase
{
namespace unittest
{

using namespace oceanbase::transaction;
using namespace oceanbase::storage;
using namespace oceanbase::storage::checkpoint;

class TestRunCtx
{
public:
  uint64_t tenant_id_ = 0;
  int time_sec_ = 0;
};

TestRunCtx RunCtx;

class ObLockTableSplitBeforeRestartTest : public ObSimpleClusterTestBase
{
public:
  ObLockTableSplitBeforeRestartTest() : ObSimpleClusterTestBase(TEST_FILE_NAME) {}
  void add_rx_in_trans_lock(sqlclient::ObISQLConnection *&connection);
  void commit_tx(sqlclient::ObISQLConnection *&connection);
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

void ObLockTableSplitBeforeRestartTest::add_rx_in_trans_lock(sqlclient::ObISQLConnection *&connection)
{
  LOG_INFO("insert data start");
  // common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  int64_t affected_rows = 0;
  ObSqlString sql;
  // sqlclient::ObISQLConnection *connection = nullptr;
  // ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(connection));
  ASSERT_NE(nullptr, connection);
  WRITE_SQL_BY_CONN(connection, "set ob_trx_timeout = 3000000000");
  WRITE_SQL_BY_CONN(connection, "set ob_trx_idle_timeout = 3000000000");
  WRITE_SQL_BY_CONN(connection, "set ob_query_timeout = 3000000000");
  WRITE_SQL_BY_CONN(connection, "set autocommit=0");

  // start and commit transaction
  // create data in tx data table.
  WRITE_SQL_BY_CONN(connection, "begin;");
  WRITE_SQL_FMT_BY_CONN(connection, "insert into test_table_lock_split_t values(1, 1);");
}

void ObLockTableSplitBeforeRestartTest::commit_tx(sqlclient::ObISQLConnection *&connection)
{
  int64_t affected_rows = 0;
  ObSqlString sql;
  ASSERT_NE(nullptr, connection);
  WRITE_SQL_BY_CONN(connection, "commit;");

  ObTransService *txs = MTL(ObTransService*);
  share::ObLSID ls_id(1001);
  ASSERT_EQ(OB_SUCCESS, txs->get_tx_ctx_mgr().clear_all_tx(ls_id));
}

TEST_F(ObLockTableSplitBeforeRestartTest, add_tenant)
{
  // create tenant
  LOG_INFO("step 1: 创建普通租户tt1");
  ASSERT_EQ(OB_SUCCESS, create_tenant());
  LOG_INFO("step 2: 获取租户tt1的tenant_id");
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(RunCtx.tenant_id_));
  LOG_INFO("step 3: 初始化普通租户tt1的sql proxy");
  ASSERT_NE(0, RunCtx.tenant_id_);
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
}

TEST_F(ObLockTableSplitBeforeRestartTest, create_table)
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  {
    OB_LOG(INFO, "create_table start");
    ObSqlString sql;
    int64_t affected_rows = 0;
    EXE_SQL("create table test_table_lock_split_t (c1 int, c2 int, primary key(c1))");
    OB_LOG(INFO, "create_table succ");
  }
}

TEST_F(ObLockTableSplitBeforeRestartTest, test_table_lock_split)
{
  // switch tenant
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(RunCtx.tenant_id_));

  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  sqlclient::ObISQLConnection *connection = nullptr;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(connection));
  add_rx_in_trans_lock(connection);

  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_NE(nullptr, ls_svr);
  ObLS *ls = nullptr;
  ObLSHandle handle;
  share::ObLSID ls_id(1001);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD));
  ASSERT_NE(nullptr, ls = handle.get_ls());
  ObTableHandleV2 table_handle;
  ObLockMemtable *lock_memtable = nullptr;
  ASSERT_EQ(OB_SUCCESS, ls->lock_table_.get_lock_memtable(table_handle));
  ASSERT_EQ(OB_SUCCESS, table_handle.get_lock_memtable(lock_memtable));

  // table_lock split
  ObTabletID src_tablet_id(200001);
  ObSArray<common::ObTabletID> dst_tablet_ids;
  ObTabletID dst_tablet_id1(300000);
  dst_tablet_ids.push_back(dst_tablet_id1);
  ObTabletID dst_tablet_id2(400000);
  dst_tablet_ids.push_back(dst_tablet_id2);

  ObTransID trans_id(1000);
  ObOBJLock *obj_lock = NULL;
  ObLockID lock_id;
  ASSERT_EQ(OB_SUCCESS, get_lock_id(dst_tablet_id1, lock_id));
  ASSERT_EQ(OB_SUCCESS, lock_memtable->table_lock_split(src_tablet_id, dst_tablet_ids, trans_id));
  int retry_time = 0;

  while (OB_FAIL(lock_memtable->obj_lock_map_.get_obj_lock_with_ref_(lock_id, obj_lock))) {
    usleep(1000 * 1000);
    retry_time++;
    if (retry_time % 5 == 0) {
      OB_LOG(WARN, "wait log callback use too much time", K(retry_time));
    }
  }
  ASSERT_NE(share::SCN::min_scn(), obj_lock->max_split_epoch_);
  // share::SCN split_scn = obj_lock->max_split_epoch_;
  // ASSERT_EQ(lock_memtable->get_rec_scn(), split_scn);
  // ASSERT_EQ(OB_SUCCESS, lock_memtable->flush(share::SCN::max_scn(), true));
  // ASSERT_EQ(lock_memtable->freeze_scn_, split_scn);
  // int retry_time = 0;
  // while (lock_memtable->is_frozen_memtable()) {
  //   usleep(1000 * 1000);
  //   retry_time++;
  //   if (retry_time % 5 == 0) {
  //     OB_LOG(WARN, "wait lock memtable flush finish use too much time",
  //            K(retry_time), KPC(lock_memtable));
  //   }
  // }
  // ASSERT_EQ(lock_memtable->get_rec_scn(), share::SCN::max_scn());
  // ASSERT_EQ(lock_memtable->flushed_scn_, lock_memtable->freeze_scn_);

  // // conflict with split lock
  // ObStoreCtx store_ctx;
  // store_ctx.ls_id_ = ls_id;
  // transaction::tablelock::ObTableLockOp out_trans_lock_op;
  // out_trans_lock_op.lock_id_ = lock_id;
  // out_trans_lock_op.lock_mode_ = SHARE;
  // out_trans_lock_op.op_type_ = OUT_TRANS_LOCK;
  // out_trans_lock_op.lock_op_status_ = LOCK_OP_DOING;
  // ASSERT_EQ(OB_TRY_LOCK_ROW_CONFLICT, obj_lock->eliminate_conflict_caused_by_split_if_need_(out_trans_lock_op, store_ctx));

  // commit_tx(connection);
  // ASSERT_EQ(OB_SUCCESS, obj_lock->eliminate_conflict_caused_by_split_if_need_(out_trans_lock_op, store_ctx));
  // ASSERT_EQ(share::SCN::min_scn(), obj_lock->max_split_epoch_);

  // ASSERT_EQ(OB_SUCCESS, ls->lock_table_.offline());
  // ASSERT_EQ(OB_SUCCESS, ls->lock_table_.load_lock());
  // ASSERT_EQ(OB_SUCCESS, get_lock_id(dst_tablet_id1, lock_id));
  // ASSERT_EQ(OB_SUCCESS, ls->lock_table_.get_lock_memtable(table_handle));
  // ASSERT_EQ(OB_SUCCESS, table_handle.get_lock_memtable(lock_memtable));
  // ASSERT_EQ(OB_SUCCESS, lock_memtable->obj_lock_map_.get_obj_lock_with_ref_(lock_id, obj_lock));
  // ASSERT_EQ(split_scn, obj_lock->max_split_epoch_);
}

class ObLockTableSplitAfterRestartTest : public ObSimpleClusterTestBase
{
public:
  ObLockTableSplitAfterRestartTest() : ObSimpleClusterTestBase(TEST_FILE_NAME) {}
};

TEST_F(ObLockTableSplitAfterRestartTest, test_recover)
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
  share::ObLSID ls_id(1001);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD));
  ASSERT_NE(nullptr, ls = handle.get_ls());

  // check lock_memtable scn
  ObTableHandleV2 table_handle;
  ObLockMemtable *lock_memtable = nullptr;
  ASSERT_EQ(OB_SUCCESS, ls->lock_table_.get_lock_memtable(table_handle));
  ASSERT_EQ(OB_SUCCESS, table_handle.get_lock_memtable(lock_memtable));
  ObOBJLock *obj_lock = NULL;
  ObLockID lock_id;
  ObTabletID dst_tablet_id1(300000);
  ASSERT_EQ(OB_SUCCESS, get_lock_id(dst_tablet_id1, lock_id));
  ASSERT_EQ(OB_SUCCESS, lock_memtable->obj_lock_map_.get_obj_lock_with_ref_(lock_id, obj_lock));
  ASSERT_NE(share::SCN::min_scn(), obj_lock->max_split_epoch_);
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
