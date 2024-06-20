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
#include <iostream>
#define protected public
#define private public

#include "env/ob_simple_cluster_test_base.h"
#include "rootserver/ob_tenant_balance_service.h"
#include "share/balance/ob_balance_job_table_operator.h"
#include "mittest/env/ob_simple_server_helper.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx/ob_tx_loop_worker.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/tx/ob_trans_define.h"

namespace oceanbase
{

namespace storage
{
int64_t ObTxTable::UPDATE_MIN_START_SCN_INTERVAL = 0;

int ObTransferHandler::wait_src_ls_advance_weak_read_ts_(
  const share::ObTransferTaskInfo &task_info,
  ObTimeoutCtx &timeout_ctx)
{
  UNUSED(task_info);
  UNUSED(timeout_ctx);
  return OB_SUCCESS;
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

#define WRITE_SQL_BY_CONN(conn, sql_str)                                \
  ASSERT_EQ(OB_SUCCESS, sql.assign(sql_str));                           \
  ASSERT_EQ(OB_SUCCESS, conn->execute_write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));

#define WRITE_SQL_FMT_BY_CONN(conn, ...)                                \
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(__VA_ARGS__));                   \
  ASSERT_EQ(OB_SUCCESS, conn->execute_write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));

#define READ_SQL_BY_CONN(conn, sql_str)         \
  ASSERT_EQ(OB_SUCCESS, sql.assign(sql_str));                           \
  ASSERT_EQ(OB_SUCCESS, conn->execute_read(OB_SYS_TENANT_ID, sql.ptr(), read_res));


class ObTransferWithCommitAction : public ObSimpleClusterTestBase
{
public:
  ObTransferWithCommitAction(): ObSimpleClusterTestBase("test_transfer_with_commit_action", "200G", "40G") {}

  void prepare_tenant_env()
  {
    common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
    int64_t affected_rows = 0;
    ObSqlString sql;
    sqlclient::ObISQLConnection *connection = nullptr;
    ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(connection));
    ASSERT_NE(nullptr, connection);
    WRITE_SQL_BY_CONN(connection, "set GLOBAL ob_trx_timeout = 10000000000");
    WRITE_SQL_BY_CONN(connection, "set GLOBAL ob_trx_idle_timeout = 10000000000");
    WRITE_SQL_BY_CONN(connection, "set GLOBAL ob_query_timeout = 10000000000");
    WRITE_SQL_BY_CONN(connection, "alter system set enable_early_lock_release = False;");
    WRITE_SQL_BY_CONN(connection, "alter system set undo_retention = 1800;");
    WRITE_SQL_BY_CONN(connection, "alter system set partition_balance_schedule_interval = '10s';");
    WRITE_SQL_BY_CONN(connection, "alter system set _enable_active_txn_transfer = True;");
    sleep(5);
  }

  void create_test_tenant(uint64_t &tenant_id)
  {
    TRANS_LOG(INFO, "create_tenant start");
    ASSERT_EQ(OB_SUCCESS, create_tenant("tt1", "20G", "100G"));
    fprintf(stdout, "finish create tenant\n");
    ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id));
    ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
    TRANS_LOG(INFO, "create_tenant end", K(tenant_id));
  }

  int wait_balance_clean(uint64_t tenant_id)
  {
    int ret = OB_SUCCESS;
    while (OB_SUCC(ret)) {
      bool is_clean = false;
      MTL_SWITCH(tenant_id) {
        ObBalanceJob job;
        int64_t start_time = OB_INVALID_TIMESTAMP, finish_time = OB_INVALID_TIMESTAMP;
        if (OB_FAIL(ObBalanceJobTableOperator::get_balance_job(tenant_id,
                                                               false,
                                                               *GCTX.sql_proxy_,
                                                               job,
                                                               start_time,
                                                               finish_time))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
            is_clean = true;
          }
        } else {
          ob_usleep(200 * 1000);
        }
      }
      if (is_clean) {
        int64_t transfer_task_count = 0;
        if (OB_FAIL(SSH::g_select_int64(tenant_id, "select count(*) as val from __all_transfer_task", transfer_task_count))) {
        } else if (transfer_task_count == 0) {
          break;
        } else {
          ob_usleep(200 * 1000);
        }
      }
    }
    return ret;
  }

  void get_tablet_info_with_table_name(const char *name,
                                       int64_t &table_id,
                                       int64_t &object_id,
                                       int64_t &tablet_id,
                                       int64_t &ls_id)
  {
    common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();

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

  int do_balance_inner_(uint64_t tenant_id)
  {
    int ret = OB_SUCCESS;
    static std::mutex mutex;
    mutex.lock();
    MTL_SWITCH(tenant_id) {
      TRANS_LOG(INFO, "worker to do partition_balance");
      auto b_svr = MTL(rootserver::ObTenantBalanceService*);
      b_svr->reset();
      int64_t job_cnt = 0;
      int64_t start_time = OB_INVALID_TIMESTAMP, finish_time = OB_INVALID_TIMESTAMP;
      ObBalanceJob job;
      if (OB_FAIL(b_svr->gather_stat_())) {
        TRANS_LOG(WARN, "failed to gather stat", KR(ret));
      } else if (OB_FAIL(ObBalanceJobTableOperator::get_balance_job(
                           tenant_id, false, *GCTX.sql_proxy_, job, start_time, finish_time))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          //NO JOB, need check current ls status
          ret = OB_SUCCESS;
          job_cnt = 0;
        } else {
          TRANS_LOG(WARN, "failed to get balance job", KR(ret), K(tenant_id));
        }
      } else if (OB_FAIL(b_svr->try_finish_current_job_(job, job_cnt))) {
        TRANS_LOG(WARN, "failed to finish current job", KR(ret), K(job));
      }
      if (OB_SUCC(ret) && job_cnt == 0 && OB_FAIL(b_svr->partition_balance_(true))) {
        TRANS_LOG(WARN, "failed to do partition balance", KR(ret));
      }
    }
    mutex.unlock();
    return ret;
  }

  int do_balance(uint64_t tenant_id)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(do_balance_inner_(tenant_id))) {
    } else if (OB_FAIL(do_balance_inner_(tenant_id))) {
    }
    return ret;
  }

  ObLS *get_ls(const int64_t tenant_id, const ObLSID ls_id)
  {
    int ret = OB_SUCCESS;
    ObLS *ls = nullptr;
    MTL_SWITCH(tenant_id)
    {
      ObLSHandle ls_handle;
      ObLSService *ls_svr = MTL(ObLSService *);
      OB_ASSERT(OB_SUCCESS == ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
      OB_ASSERT(nullptr != (ls = ls_handle.get_ls()));
    }
    return ls;
  }
};

TEST_F(ObTransferWithCommitAction, commit_action)
{
  ObSqlString sql;
  int64_t affected_rows = 0;

  // ============================== Phase1. create tenant ==============================
  TRANS_LOG(INFO, "create tenant start");
  uint64_t tenant_id = 0;
  create_test_tenant(tenant_id);
  TRANS_LOG(INFO, "create tenant end");

  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));

  prepare_tenant_env();

  // ============================== Phase2. create new ls ==============================
  ASSERT_EQ(0, SSH::create_ls(tenant_id, get_curr_observer().self_addr_));
  int64_t ls_count = 0;
  ASSERT_EQ(0, SSH::g_select_int64(tenant_id, "select count(ls_id) as val from __all_ls where ls_id!=1", ls_count));
  ASSERT_EQ(2, ls_count);

  // ============================== Phase3. create new tables ==============================
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  TRANS_LOG(INFO, "create table qcc2 start");
  EXE_SQL("create table qcc1 (a int)");
  TRANS_LOG(INFO, "create_table qcc2 end");

  TRANS_LOG(INFO, "create table qcc2 start");
  EXE_SQL("create table qcc2 (a int)");
  TRANS_LOG(INFO, "create_table qcc2 end");
  usleep(3 * 1000 * 1000);

  ObLSID loc1, loc2;
  ASSERT_EQ(0, SSH::select_table_loc(tenant_id, "qcc1", loc1));
  ASSERT_EQ(0, SSH::select_table_loc(tenant_id, "qcc2", loc2));
  ASSERT_NE(loc1, loc2);
  int64_t table1, table2;
  int64_t object1, object2;
  int64_t tablet1, tablet2;
  int64_t ls1, ls2;
  get_tablet_info_with_table_name("qcc1", table1, object1, tablet1, ls1);
  get_tablet_info_with_table_name("qcc2", table2, object2, tablet2, ls2);
  fprintf(stdout, "qcc is created successfully, loc1: %ld, loc2: %ld, table1: %ld, table2: %ld, tablet1: %ld, tablet2: %ld, ls1: %ld, ls2: %ld\n",
          loc1.id(), loc2.id(), table1, table2, tablet1, tablet2, ls1, ls2);

  EXE_SQL("create tablegroup tg1 sharding='NONE';");

  // ============================== Phase4. wait minor freeze to remove retain ctx ==============================
  sqlclient::ObISQLConnection *sys_conn = nullptr;
  common::ObMySQLProxy &sys_proxy = get_curr_simple_server().get_sql_proxy();
  ASSERT_EQ(OB_SUCCESS, sys_proxy.acquire(sys_conn));
  ASSERT_NE(nullptr, sys_conn);

  WRITE_SQL_BY_CONN(sys_conn, "alter system minor freeze tenant sys;");
  WRITE_SQL_BY_CONN(sys_conn, "alter system minor freeze tenant all_user;");
  WRITE_SQL_BY_CONN(sys_conn, "alter system minor freeze tenant all_meta;");
  sleep(5);

  int ret = OB_SUCCESS;
  HEAP_VAR(ObMySQLProxy::MySQLResult, res_0)
  {
    common::sqlclient::ObMySQLResult *result = nullptr;
    sql.assign(
      "SELECT count(*) as cnt FROM oceanbase.__all_virtual_trans_stat where tenant_id = 1002 and ls_id = 1001;");
    int retry_times = 100;
    int64_t cnt = 0;

    while (--retry_times >= 0) {
      res_0.reuse();
      ASSERT_EQ(OB_SUCCESS, sys_conn->execute_read(OB_SYS_TENANT_ID, sql.ptr(), res_0));
      result = res_0.mysql_result();
      ASSERT_EQ(OB_SUCCESS, result->next());

      ASSERT_EQ(OB_SUCCESS, result->get_int("cnt", cnt));
      if (0 == cnt) {
        break;
      } else {
        fprintf(stdout, "waitting for tx ctx table mini merge to clear retain ctx ... \n");
        sleep(1);
      }
    }
    ASSERT_EQ(0, cnt);
  }

  // ============================== Phase5. start the user txn ==============================
  sqlclient::ObISQLConnection *user_connection = nullptr;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(user_connection));
  ASSERT_NE(nullptr, user_connection);

  WRITE_SQL_BY_CONN(user_connection, "set SESSION ob_trx_timeout = 10000000000");
  WRITE_SQL_BY_CONN(user_connection, "set SESSION ob_trx_idle_timeout = 10000000000");
  WRITE_SQL_BY_CONN(user_connection, "set SESSION ob_query_timeout = 10000000000");

  TRANS_LOG(INFO, "start the txn");
  WRITE_SQL_BY_CONN(user_connection, "begin;");
  WRITE_SQL_FMT_BY_CONN(user_connection, "insert into qcc2 values(1);");

  ObTransID tx_id2;
  ObPartTransCtx *ctx2 = nullptr;
  ASSERT_EQ(0, SSH::find_tx(user_connection, tx_id2));
  ASSERT_EQ(0, SSH::get_tx_ctx(tenant_id, loc2, tx_id2, ctx2));

  fprintf(stdout, "start inject to tx %lu\n", tx_id2.get_id());

  InjectTxFaultHelper inject_tx_fault_helper;
  ASSERT_EQ(0, inject_tx_fault_helper.inject_tx_block(tenant_id, loc2, tx_id2, ObTxLogType::TX_COMMIT_INFO_LOG));

  std::thread th([user_connection] () {
                   user_connection->commit();
                 });

  usleep(1 * 1000 * 1000);

  ASSERT_EQ(3, ctx2->part_trans_action_);

  // ============================== Phase5. start the transfer ==============================
  EXE_SQL("alter tablegroup tg1 add qcc1,qcc2;");
  usleep(1 * 1000 * 1000);
  ASSERT_EQ(0, do_balance(tenant_id));
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

  ObPartTransCtx *ctx1 = nullptr;
  ASSERT_EQ(0, SSH::get_tx_ctx(tenant_id, loc1, tx_id2, ctx1));

  inject_tx_fault_helper.release();
  th.join();

  usleep(1 * 1000 * 1000);
  ASSERT_EQ(true, ctx1->is_exiting_);
  ASSERT_EQ(true, ctx2->is_exiting_);
  ASSERT_EQ(transaction::ObTxState::CLEAR, ctx1->exec_info_.state_);
  ASSERT_EQ(transaction::ObTxState::CLEAR, ctx2->exec_info_.state_);
  ASSERT_EQ(ObTxCommitData::COMMIT, ctx1->ctx_tx_data_.get_state());
  ASSERT_EQ(ObTxCommitData::COMMIT, ctx2->ctx_tx_data_.get_state());
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  using namespace oceanbase::unittest;

  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("info");
  ::testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
