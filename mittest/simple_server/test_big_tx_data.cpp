/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include <chrono>
#include <gtest/gtest.h>
#include <thread>
#define protected public
#define private public
#include "share/ob_table_access_helper.h"
#include "lib/ob_define.h"
#include "observer/ob_server_struct.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/ls/ob_ls_meta.h"
#include "storage/ls/ob_ls.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "env/ob_simple_cluster_test_base.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include <iostream>

#include "rewrite_function_for_test_big_tx_data.cpp"

using namespace std;

namespace oceanbase
{
using namespace storage;
namespace unittest
{

using namespace oceanbase::transaction;
using namespace oceanbase::storage;

class TestRunCtx
{
public:
  uint64_t tenant_id_ = 0;
  int time_sec_ = 0;
};

TestRunCtx RunCtx;

class TestBigTxData : public ObSimpleClusterTestBase
{
public:
  // 指定case运行目录前缀 test_ob_simple_cluster_
  void minor_freeze_and_wait();
  TestBigTxData() : ObSimpleClusterTestBase("test_big_tx_data_") {}
};

#define DO(stmt) /*cout << "before do line:" << __LINE__ << endl;*/ASSERT_EQ((stmt), OB_SUCCESS);/*cout << "after do line:" << __LINE__ << endl;*/
#define EXEC_SQL(sql) connection->execute_write(OB_SYS_TENANT_ID, sql, affected_rows)

class DoNothingOP : public ObITxDataCheckFunctor
{
  virtual int operator()(const ObTxData &tx_data, ObTxCCCtx *tx_cc_ctx = nullptr) {
    UNUSED(tx_cc_ctx);
    cout << "read tx data:" << tx_data.tx_id_.get_id() << ", undo cnt:" << tx_data.undo_status_list_.undo_node_cnt_ << endl;
    STORAGE_LOG_RET(INFO, 0, "read tx data", K(tx_data.tx_id_), K(lbt()));
    return OB_SUCCESS;
  }
};

TEST_F(TestBigTxData, big_tx_data)
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(OB_SYS_TENANT_ID) {
    int64_t affected_rows = 0;
    sqlclient::ObISQLConnection *connection = nullptr;
    // 1，开启事务，生成一些savepoint
    ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().get_sql_proxy().acquire(connection));
    DO(EXEC_SQL("set ob_trx_timeout = 6000000000"));
    DO(EXEC_SQL("set ob_trx_idle_timeout = 6000000000"));
    DO(EXEC_SQL("alter system set _fast_commit_callback_count = 0"));// disallow fast commit
    DO(EXEC_SQL("create table test_big_tx_data (a int primary key, b int)"));
    DO(EXEC_SQL("alter system set _private_buffer_size = '1B'"));
    DO(EXEC_SQL("begin"));
    constexpr int64_t savepoint_size = 12;
    for (int64_t idx = 0; idx < savepoint_size; ++idx) {
      ObSqlString sql;
      DO(sql.append_fmt("insert into test_big_tx_data values(%ld, 1)", idx));
      DO(EXEC_SQL(sql.ptr()));
      sql.reset();
      DO(sql.append_fmt("savepoint s%ld", idx));
      DO(EXEC_SQL(sql.ptr()));
    }
    // 2，获取本事务ID，这个事务的tx data足够大
    HEAP_VAR(ObMySQLProxy::MySQLResult, res) {
      int64_t sess_id = 0;
      int64_t tx_id;
      DO(connection->execute_read(OB_SYS_TENANT_ID, "select connection_id()", res));
      common::sqlclient::ObMySQLResult *result = res.mysql_result();
      DO(result->next());
      result->get_int("connection_id()", sess_id);
      ObSqlString sql;
      DO(sql.append_fmt("select trans_id from oceanbase.__all_virtual_processlist where id=%ld", sess_id));
      res.reset();
      DO(connection->execute_read(OB_SYS_TENANT_ID, sql.ptr(), res));
      result = res.mysql_result();
      DO(result->next());
      result->get_int("trans_id", tx_id);
      ASSERT_EQ(OB_ITER_END, result->next());
      ATOMIC_STORE(&TEST_TX_ID, tx_id);
      std::cout << "tx_id:" << tx_id << std::endl;
    }
    // 3，写日志才可以生成undo status
    cout << "alter system minor freeze 1" << endl;
    minor_freeze_and_wait();
    // 4，回滚生成undo status
    for (int64_t idx = savepoint_size - 1; idx >= 0; --idx) {
      ObSqlString sql;
      DO(sql.append_fmt("rollback to s%ld", idx));
      DO(EXEC_SQL(sql.ptr()));
    }
    DO(EXEC_SQL("commit"));
    ::sleep(10);
    // 5，把tx data转下去
    cout << "alter system minor freeze 2" << endl;
    minor_freeze_and_wait();
    ASSERT_EQ(ATOMIC_LOAD(&DUMP_BIG_TX_DATA), true);// 确保tx data已经走过了序列化逻辑
    // 6, 读一次这个tx data
    DoNothingOP op;
    ObLSService *ls_service = MTL(ObLSService*);
    ObLSHandle handle;
    DO(ls_service->get_ls(ObLSID(1), handle, storage::ObLSGetMod::DEADLOCK_MOD));
    fprintf(stdout, "start read tx data from sstable, test_tx_id = %ld\n", TEST_TX_ID);
    ObTxDataMiniCache fake_cache;
    ObReadTxDataArg read_arg(ObTransID(ATOMIC_LOAD(&TEST_TX_ID)), 0, fake_cache);
    DO(handle.get_ls()->tx_table_.check_with_tx_data(read_arg, op));
    // 7，检查被测事务的tx data已经经过了deserialize
    ASSERT_EQ(ATOMIC_LOAD(&LOAD_BIG_TX_DATA), true);
  }
}

void TestBigTxData::minor_freeze_and_wait()
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(OB_SYS_TENANT_ID) {
    int64_t affected_rows = 0;
    int64_t retry_times = 40;
    ObLSService *ls_service = MTL(ObLSService*);
    ObLSHandle handle;
    DO(ls_service->get_ls(ObLSID(1), handle, storage::ObLSGetMod::DEADLOCK_MOD));
    ObTxDataMemtableMgr *mgr = handle.get_ls()->tx_table_.tx_data_table_.memtable_mgr_;
    ASSERT_NE(nullptr, mgr);

    int64_t head_before_freeze = -1;
    int64_t tail_before_freeze = -1;
    int64_t head_after_freeze = -1;
    int64_t tail_after_freeze = -1;
    while (--retry_times > 0) {
      DO(mgr->get_memtable_range(head_before_freeze, tail_before_freeze));
      ASSERT_GE(head_before_freeze, 0);
      ASSERT_GE(tail_before_freeze, 0);
      if (head_before_freeze + 1 != tail_before_freeze) {
        ::sleep(1);
        cout << "waiting last minor freeze done ... "
             << "head_before_freeze = " << head_before_freeze << " tail_before_freeze = " << tail_before_freeze << endl;
      } else {
        break;
      }
    }
    ASSERT_EQ(head_before_freeze + 1, tail_before_freeze);
    cout << "head_before_freeze : " << head_before_freeze << " tail_before_freeze" << tail_before_freeze << endl;

    // minor freeze once
    DO(get_curr_simple_server().get_sql_proxy().write("alter system minor freeze", affected_rows));

    retry_times = 60;
    while (--retry_times > 0)
    {
      DO(handle.get_ls()->tx_table_.tx_data_table_.memtable_mgr_->get_memtable_range(head_after_freeze, tail_after_freeze));
      ASSERT_GE(head_after_freeze, 0);
      ASSERT_GE(tail_after_freeze, 0);
      if (head_after_freeze > head_before_freeze && tail_after_freeze > tail_before_freeze && head_after_freeze + 1 == tail_after_freeze) {
        fprintf(stdout,
                "head_after_freeze : %ld, head_before_freeze : %ld, tail_after_freeze : %ld, tail_before_freeze : %ld\n",
                head_after_freeze,
                head_before_freeze,
                tail_after_freeze,
                tail_before_freeze);
        break;
      } else {
        ::sleep(1);
        cout << "waiting this minor freeze done, head_after_freeze: " << head_after_freeze << " tail_after_freeze " << tail_after_freeze << endl;
      }
    }
    ASSERT_GT(head_after_freeze, head_before_freeze);
    ASSERT_GT(tail_after_freeze, tail_before_freeze);
    ASSERT_EQ(head_after_freeze + 1, tail_after_freeze);
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

  oceanbase::unittest::RunCtx.time_sec_ = time_sec;
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
