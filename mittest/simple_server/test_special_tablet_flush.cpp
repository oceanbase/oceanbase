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
#define UNITTEST

#include "env/ob_simple_cluster_test_base.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tablelock/ob_lock_memtable.h"
#include "logservice/ob_log_base_type.h"
#include "mtlenv/tablelock/table_lock_tx_common_env.h"
#include "storage/tx_storage/ob_ls_handle.h" //ObLSHandle

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

class ObTabletFlushTest : public ObSimpleClusterTestBase
{
public:
  ObTabletFlushTest() : ObSimpleClusterTestBase("test_special_tablet_flush_") {}
  void set_private_buffer_size();
  void prepare_tx_table_data();
  void prepare_ddl_lock_table_data(ObLS *&ls);
  void minor_freeze();
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

void ObTabletFlushTest::set_private_buffer_size()
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  int64_t affected_rows = 0;
  ObSqlString sql;
  EXE_SQL("alter system set _private_buffer_size = '1B';");
}

void ObTabletFlushTest::prepare_tx_table_data()
{
  LOG_INFO("insert data start");
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  int64_t i = 0;
  int64_t affected_rows = 0;
  int rollback_cnt = 0;
  ObSqlString sql;
  sqlclient::ObISQLConnection *connection = nullptr;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(connection));
  ASSERT_NE(nullptr, connection);
  WRITE_SQL_BY_CONN(connection, "set ob_trx_timeout = 3000000000");
  WRITE_SQL_BY_CONN(connection, "set ob_trx_idle_timeout = 3000000000");
  WRITE_SQL_BY_CONN(connection, "set ob_query_timeout = 3000000000");
  WRITE_SQL_BY_CONN(connection, "set autocommit=0");

  // start and commit transaction
  // create data in tx data table.
  WRITE_SQL_BY_CONN(connection, "begin;");
  WRITE_SQL_FMT_BY_CONN(connection, "insert into test_special_tablet_flush_t values(1, 1);");
  WRITE_SQL_BY_CONN(connection, "commit;");
  // start transaction
  // create data in tx ctx table
  WRITE_SQL_BY_CONN(connection, "begin;");
  WRITE_SQL_FMT_BY_CONN(connection, "insert into test_special_tablet_flush_t values(2, 2);");
  LOG_INFO("insert data finish");
}

void ObTabletFlushTest::prepare_ddl_lock_table_data(ObLS *&ls)
{
  ObTableHandleV2 handle; // used for lock memtable
  MockTxEnv mock_env(MTL_ID(), ls->get_ls_id());
  share::SCN max_decided_scn;
  max_decided_scn.set_min();
  ASSERT_EQ(OB_SUCCESS, ls->ls_freezer_.get_max_consequent_callbacked_scn(max_decided_scn));
  transaction::tablelock::ObTableLockOp table_lock_op1;
  ObLockID lock_id1;
  lock_id1.obj_type_ = ObLockOBJType::OBJ_TYPE_TABLE;
  lock_id1.obj_id_ = 1000;
  table_lock_op1.lock_id_ = lock_id1;
  table_lock_op1.lock_mode_ = SHARE_ROW_EXCLUSIVE;
  table_lock_op1.create_trans_id_ = 2;
  table_lock_op1.op_type_ = OUT_TRANS_LOCK;
  table_lock_op1.lock_op_status_ = LOCK_OP_DOING;

  transaction::tablelock::ObTableLockOp table_lock_op2;
  ObLockID lock_id2;
  lock_id2.obj_type_ = ObLockOBJType::OBJ_TYPE_TABLE;
  lock_id2.obj_id_ = 1002;
  table_lock_op1.owner_id_ = 1;
  table_lock_op2.lock_id_ = lock_id2;
  table_lock_op2.lock_mode_ = ROW_SHARE;
  table_lock_op2.create_trans_id_ = 3;
  table_lock_op2.op_type_ = OUT_TRANS_UNLOCK;
  table_lock_op2.lock_op_status_ = LOCK_OP_DOING;

  ASSERT_EQ(OB_SUCCESS, ls->get_lock_memtable(handle));
  ObLockMemtable *lock_memtable = nullptr;
  ASSERT_EQ(OB_SUCCESS, handle.get_lock_memtable(lock_memtable));
  ASSERT_EQ(share::SCN::max_scn(), lock_memtable->get_rec_scn());
  ObTxIDSet unused;
  MockTxEnv::MyTxCtx default_ctx;
  ObStoreCtx store_ctx;
  ObTxTable fake_tx_table;
  ObLockParam param;

  param.is_try_lock_ = false;
  param.expired_time_ = ObClockGenerator::getClock() + 10000000000000;
  mock_env.start_tx(table_lock_op1.create_trans_id_, handle, default_ctx);
  mock_env.get_store_ctx(default_ctx, &fake_tx_table, store_ctx);
  default_ctx.tx_ctx_.change_to_leader();
  ASSERT_EQ(OB_SUCCESS, lock_memtable->obj_lock_map_.lock(param,
                                                          store_ctx,
                                                          table_lock_op1,
                                                          0x0,
                                                          unused));
  ASSERT_EQ(OB_SUCCESS, lock_memtable->update_lock_status(table_lock_op1,
                                                          share::SCN::minus(max_decided_scn, 2),
                                                          share::SCN::minus(max_decided_scn, 2),
                                                          LOCK_OP_COMPLETE));
  ASSERT_EQ(OB_SUCCESS, lock_memtable->lock(param, store_ctx, table_lock_op2));
  ASSERT_EQ(OB_SUCCESS, lock_memtable->update_lock_status(table_lock_op2,
                                                          share::SCN::minus(max_decided_scn, 1),
                                                          share::SCN::minus(max_decided_scn, 1),
                                                          LOCK_OP_COMPLETE));
  ASSERT_EQ(share::SCN::minus(max_decided_scn, 2), lock_memtable->get_rec_scn());
}

void ObTabletFlushTest::minor_freeze()
{
  LOG_INFO("minor_freeze start");
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  int64_t affected_rows = 0;
  ObSqlString sql;
  EXE_SQL("alter system minor freeze tenant tt1;");
  LOG_INFO("minor_freeze finish");
}

TEST_F(ObTabletFlushTest, observer_start)
{
  SERVER_LOG(INFO, "observer_start succ");
}

TEST_F(ObTabletFlushTest, add_tenant)
{
  // create tenant
  ASSERT_EQ(OB_SUCCESS, create_tenant());
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(RunCtx.tenant_id_));
  ASSERT_NE(0, RunCtx.tenant_id_);
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
}

TEST_F(ObTabletFlushTest, create_table)
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  {
    OB_LOG(INFO, "create_table start");
    ObSqlString sql;
    int64_t affected_rows = 0;
    EXE_SQL("create table test_special_tablet_flush_t (c1 int, c2 int, primary key(c1))");
    OB_LOG(INFO, "create_table succ");
  }
}

TEST_F(ObTabletFlushTest, test_special_tablet_flush)
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
  share::ObLSID ls_id(1001);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD));
  ASSERT_NE(nullptr, ls = handle.get_ls());
  ObCheckpointExecutor *checkpoint_executor = ls->get_checkpoint_executor();
  ASSERT_NE(nullptr, checkpoint_executor);

  set_private_buffer_size();

  prepare_tx_table_data();

  prepare_ddl_lock_table_data(ls);

  ObTxCtxMemtable *tx_ctx_memtable
    = dynamic_cast<ObTxCtxMemtable *>(dynamic_cast<ObLSTxService *>(checkpoint_executor
            ->handlers_[logservice::TRANS_SERVICE_LOG_BASE_TYPE])
            ->common_checkpoints_[ObCommonCheckpointType::TX_CTX_MEMTABLE_TYPE]);

  ObLockMemtable *lock_memtable
    = dynamic_cast<ObLockMemtable *>(dynamic_cast<ObLSTxService *>(checkpoint_executor
            ->handlers_[logservice::TRANS_SERVICE_LOG_BASE_TYPE])
            ->common_checkpoints_[ObCommonCheckpointType::LOCK_MEMTABLE_TYPE]);

  ObTxDataMemtableMgr *tx_data_mgr
    = dynamic_cast<ObTxDataMemtableMgr *>(dynamic_cast<ObLSTxService *>(checkpoint_executor
            ->handlers_[logservice::TRANS_SERVICE_LOG_BASE_TYPE])
            ->common_checkpoints_[ObCommonCheckpointType::TX_DATA_MEMTABLE_TYPE]);

  ASSERT_NE(lock_memtable->get_rec_scn(), share::SCN::max_scn());
  ASSERT_NE(tx_ctx_memtable->get_rec_scn(), share::SCN::max_scn());
  ASSERT_NE(tx_data_mgr->get_rec_scn(), share::SCN::max_scn());

  minor_freeze();

  int retry_time = 0;

  while ((lock_memtable->is_frozen_memtable() ||
         tx_ctx_memtable->is_frozen_memtable() ||
         tx_data_mgr->is_flushing())) {
    usleep(1000 * 1000);
    retry_time++;
    if (retry_time % 5 == 0) {
      OB_LOG(WARN, "wait lock memtable flush finish use too much time",
             K(retry_time), KPC(lock_memtable), KPC(tx_ctx_memtable), K(tx_data_mgr->is_flushing()));
    }
  }
  ASSERT_EQ(tx_ctx_memtable->get_rec_scn(), share::SCN::max_scn());
  ASSERT_EQ(tx_data_mgr->get_rec_scn(), share::SCN::max_scn());
}

TEST_F(ObTabletFlushTest, end)
{
  if (RunCtx.time_sec_ > 0) {
    ::sleep(RunCtx.time_sec_);
  }
}

} //unitest
} //oceanbase

int main(int argc, char **argv)
{
  int c = 0;
  int time_sec = 0;
  char *log_level = (char*)"INFO";
  while (EOF != (c = getopt(argc,argv,"t:l:"))) {
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
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_enable_async_log(false);
  GCONF._enable_defensive_check = false;

  LOG_INFO("main>>>");
  oceanbase::unittest::RunCtx.time_sec_ = time_sec;
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
