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
#include "storage/tx/ob_trans_part_ctx.h"

#undef private
#undef protected

static const char *TEST_FILE_NAME = "test_tx_ctx_table_mit";
static const char *BORN_CASE_NAME = "ObTxCtxTableTest";
static const char *RESTART_CASE_NAME = "ObTxCtxTableRestartTest";

bool SLEEP_BEFORE_DUMP_TX_CTX = false;
bool HAS_GOT_TX_CTX = false;
int64_t TX_CTX_TABLE_LAST_CHECKPOINT = 0;


namespace oceanbase
{
using namespace transaction;
using namespace storage;
using namespace palf;
using namespace share;

namespace storage
{

int ObTxCtxMemtableScanIterator::serialize_next_tx_ctx_(ObTxLocalBuffer &buffer,
                                                        int64_t &serialize_size,
                                                        transaction::ObPartTransCtx *&tx_ctx)
{
  int ret = OB_SUCCESS;
  bool need_retry = true;

  while (OB_SUCC(ret) && need_retry) {
    if (OB_FAIL(ls_tx_ctx_iter_.get_next_tx_ctx(tx_ctx))) {
      if (OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "ls_tx_ctx_iter_.get_next_tx_ctx failed", K(ret));
      }
    } else if (OB_FAIL(tx_ctx->serialize_tx_ctx_to_buffer(buffer, serialize_size))) {
      if (OB_TRANS_CTX_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        STORAGE_LOG(WARN, "tx_ctx->get_tx_ctx_table_info failed", K(ret));
      }
      ls_tx_ctx_iter_.revert_tx_ctx(tx_ctx);
    } else {
      need_retry = false;
    }
  }

  if (OB_FAIL(ret)) {
    STORAGE_LOG(INFO, "get next tx ctx table info failed", KR(ret), KPC(tx_ctx));
  } else if (SLEEP_BEFORE_DUMP_TX_CTX) {
    fprintf(stdout, "ready to dump tx ctx, undo status node ptr : %p\n", tx_ctx->ctx_tx_data_.tx_data_guard_.tx_data()->undo_status_list_.head_);
    fprintf(stdout, "sleep 20 seconds before dump\n");
    HAS_GOT_TX_CTX = true;
    SLEEP_BEFORE_DUMP_TX_CTX = false;
    ::sleep(20);
  }

  return ret;
}

};

namespace unittest
{


class TestRunCtx
{
public:
  uint64_t tenant_id_ = 0;
};

TestRunCtx RunCtx;

class ObTxCtxTableTest : public ObSimpleClusterTestBase
{
public:
  ObTxCtxTableTest() : ObSimpleClusterTestBase(TEST_FILE_NAME) {}

  void dump_ctx_with_merged_undo_action();

private:
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
  int64_t affected_rows = 0; \

void ObTxCtxTableTest::dump_ctx_with_merged_undo_action()
{
  DEF_VAL_FOR_SQL;
  common::ObMySQLProxy &tt1_proxy = get_curr_simple_server().get_sql_proxy2();
  common::ObMySQLProxy &sys_proxy = get_curr_simple_server().get_sql_proxy();
  sqlclient::ObISQLConnection *tt1_conn = nullptr;
  sqlclient::ObISQLConnection *sys_conn = nullptr;


  ASSERT_EQ(OB_SUCCESS, tt1_proxy.acquire(tt1_conn));
  ASSERT_NE(nullptr, tt1_conn);
  ASSERT_EQ(OB_SUCCESS, sys_proxy.acquire(sys_conn));
  ASSERT_NE(nullptr, sys_conn);

  // 初始化测试用例所需参数
  WRITE_SQL_BY_CONN(sys_conn, "alter system set _private_buffer_size = '1B';");
  WRITE_SQL_BY_CONN(tt1_conn, "set ob_trx_timeout = 3000000000");
  WRITE_SQL_BY_CONN(tt1_conn, "set ob_trx_idle_timeout = 3000000000");
  WRITE_SQL_BY_CONN(tt1_conn, "set ob_query_timeout = 3000000000");
  WRITE_SQL_BY_CONN(tt1_conn, "alter system set undo_retention = 1800");
  sleep(5);

  // 建表
  WRITE_SQL_BY_CONN(tt1_conn, "create table if not exists test_tx_ctx_t (a int, b int)");

  // 开事务
  WRITE_SQL_BY_CONN(tt1_conn, "begin");
  WRITE_SQL_BY_CONN(tt1_conn, "insert into test_tx_ctx_t values(1, 1);");
  for (int i = 0; i < 4; i++) {
    WRITE_SQL_BY_CONN(tt1_conn, "insert into test_tx_ctx_t select a, b from test_tx_ctx_t;");
  }

  // 执行一次事务上下文表的转储，保证建表语句产生的多源事务能够被转储下去，retain ctx可以被释放
  SLEEP_BEFORE_DUMP_TX_CTX = false;
  WRITE_SQL_BY_CONN(sys_conn, "alter system minor freeze tenant sys;");
  WRITE_SQL_BY_CONN(sys_conn, "alter system minor freeze tenant all_user;");
  WRITE_SQL_BY_CONN(sys_conn, "alter system minor freeze tenant all_meta;");
  sleep(5);


  // 确认当前上下文中有且只有这一个事务
  HEAP_VAR(ObMySQLProxy::MySQLResult, res_0)
  {
    common::sqlclient::ObMySQLResult *result = nullptr;
    sql.assign(
        "SELECT count(*) as cnt FROM oceanbase.__all_virtual_trans_stat where tenant_id = 1002 and ls_id = 1001;");
    int retry_times = 20;
    int64_t cnt = 0;

    while (--retry_times >= 0) {
      res_0.reuse();
      ASSERT_EQ(OB_SUCCESS, sys_conn->execute_read(OB_SYS_TENANT_ID, sql.ptr(), res_0));
      result = res_0.mysql_result();
      ASSERT_EQ(OB_SUCCESS, result->next());

      ASSERT_EQ(OB_SUCCESS, result->get_int("cnt", cnt));
      if (1 == cnt) {
        break;
      } else {
        fprintf(stdout, "waitting for tx ctx table mini merge to clear retain ctx ... \n");
        sleep(1);
      }
    }
    ASSERT_EQ(1, cnt);
  }

  // 获取当前上下文表的转储位点，用于确认下一次转储是否成功
  HEAP_VAR(ObMySQLProxy::MySQLResult, res_1)
  {
    common::sqlclient::ObMySQLResult *result = nullptr;
    sql.assign(
        "SELECT end_log_scn FROM oceanbase.__all_virtual_table_mgr where tenant_id = 1002 and ls_id = 1001 and tablet_id = 49401 and table_type = 12;");

    res_1.reuse();
    ASSERT_EQ(OB_SUCCESS, sys_conn->execute_read(OB_SYS_TENANT_ID, sql.ptr(), res_1));
    result = res_1.mysql_result();

    ASSERT_EQ(OB_SUCCESS, result->next());
    ASSERT_EQ(OB_SUCCESS, result->get_int("end_log_scn", TX_CTX_TABLE_LAST_CHECKPOINT));
  }

  WRITE_SQL_BY_CONN(tt1_conn, "savepoint x1");
  for (int i = 0; i < 2; i++) {
    WRITE_SQL_BY_CONN(tt1_conn, "insert into test_tx_ctx_t select a, b from test_tx_ctx_t;");
  }


  // 产生一个undo action，同时插入一些新的数据
  WRITE_SQL_BY_CONN(tt1_conn, "savepoint x2");
  for (int i = 0; i < 2; i++) {
    WRITE_SQL_BY_CONN(tt1_conn, "insert into test_tx_ctx_t select a, b from test_tx_ctx_t;");
  }
  WRITE_SQL_BY_CONN(tt1_conn, "rollback to savepoint x2");
  WRITE_SQL_BY_CONN(tt1_conn, "insert into test_tx_ctx_t select a, b from test_tx_ctx_t;");


  // 转储ctx table，设置SLEEP_BEFORE_DUMP_TX_CTX标志
  SLEEP_BEFORE_DUMP_TX_CTX = true;
  HAS_GOT_TX_CTX = false;
  WRITE_SQL_BY_CONN(sys_conn, "alter system minor freeze tenant tt1 ls 1001 tablet_id = 49401;");

  // 等待一段时间保证上下文表的转储已经拿到了带有undo node的tx data
  while (!HAS_GOT_TX_CTX) {
    fprintf(stdout, "waitting for scheduling tx ctx table merge dag ...\n");
    sleep(2);
  }

  // 再次执行rollback，预期能merge掉第一次回滚产生的undo action
  WRITE_SQL_BY_CONN(tt1_conn, "rollback to savepoint x1");
  {
    sqlclient::ObISQLConnection *tt1_conn_2 = nullptr;
    ASSERT_EQ(OB_SUCCESS, tt1_proxy.acquire(tt1_conn_2));
    ASSERT_NE(nullptr, tt1_conn_2);

    int64_t insert_start_ts = ObTimeUtil::fast_current_time();
    fprintf(stdout, "doing insert while dump tx ctx table sleeping...\n");
    while (ObTimeUtil::fast_current_time() - insert_start_ts < 30 * 1000 * 1000/* 30 seconds */) {
      WRITE_SQL_BY_CONN(tt1_conn_2, "insert into test_tx_ctx_t values(3, 3)");
      if (REACH_TIME_INTERVAL(2 * 1000 * 1000)) {
      }
    }
  }

  // 确认上下文表转储成功
  HEAP_VAR(ObMySQLProxy::MySQLResult, res_2)
  {
    common::sqlclient::ObMySQLResult *result = nullptr;
    sql.assign("SELECT end_log_scn FROM oceanbase.__all_virtual_table_mgr where tenant_id = 1002 and ls_id = 1001 and "
               "tablet_id = 49401 and table_type = 12;");

    int retry_times = 40;
    int64_t end_log_scn = 0;
    while (--retry_times >= 0) {
      res_2.reuse();
      ASSERT_EQ(OB_SUCCESS, sys_conn->execute_read(OB_SYS_TENANT_ID, sql.ptr(), res_2));
      result = res_2.mysql_result();
      ASSERT_EQ(OB_SUCCESS, result->next());
      ASSERT_EQ(OB_SUCCESS, result->get_int("end_log_scn", end_log_scn));
      if (end_log_scn > TX_CTX_TABLE_LAST_CHECKPOINT) {
        break;
      } else {
        fprintf(stdout, "waitting for tx ctx table mini merge to dump rollback tx data\n");
      }
    }
    ASSERT_GT(end_log_scn, TX_CTX_TABLE_LAST_CHECKPOINT);
  }
}

TEST_F(ObTxCtxTableTest, observer_start) { SERVER_LOG(INFO, "observer_start succ"); }

TEST_F(ObTxCtxTableTest, add_tenant)
{
  // create tenant
  ASSERT_EQ(OB_SUCCESS, create_tenant());
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(RunCtx.tenant_id_));
  ASSERT_NE(0, RunCtx.tenant_id_);
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
  sleep(5);
}

TEST_F(ObTxCtxTableTest, dump_ctx_with_merged_undo_action)
{
  SLEEP_BEFORE_DUMP_TX_CTX = false;
  HAS_GOT_TX_CTX = false;
  dump_ctx_with_merged_undo_action();
  fprintf(stdout, "dump ctx with merge undo action done\n");
}

class ObTxCtxTableRestartTest : public ObSimpleClusterTestBase
{
public:
  ObTxCtxTableRestartTest() : ObSimpleClusterTestBase(TEST_FILE_NAME) {}

};

TEST_F(ObTxCtxTableRestartTest, observer_restart)
{
  // init sql proxy2 to use tenant tt1
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
  SERVER_LOG(INFO, "observer restart succ");
}

TEST_F(ObTxCtxTableRestartTest, test_to_do)
{
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
  restart_helper.set_sleep_sec(time_sec);
  restart_helper.run();

  return ret;
}
