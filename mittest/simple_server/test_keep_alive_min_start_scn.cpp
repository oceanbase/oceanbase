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
#include "storage/tx/ob_tx_loop_worker.h"
#include "storage/tx_storage/ob_ls_map.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{

namespace storage
{
int64_t ObTxTable::UPDATE_MIN_START_SCN_INTERVAL = 0;
}

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

class ObTestKeepAliveMinStartSCN : public ObSimpleClusterTestBase
{
public:
  ObTestKeepAliveMinStartSCN() : ObSimpleClusterTestBase("test_keep_alive_min_start_scn_") {}

  void test_min_start_scn();
  void loop_check_start_scn(SCN &prev_min_start_scn, SCN &prev_keep_alive_scn);

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
  sqlclient::ObISQLConnection *connection = nullptr;

void ObTestKeepAliveMinStartSCN::loop_check_start_scn(SCN &prev_min_start_scn, SCN &prev_keep_alive_scn)
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(RunCtx.tenant_id_)
  {
    ObLS *ls = get_ls(RunCtx.tenant_id_, ObLSID(1001));
    ObTxTable *tx_table = ls->get_tx_table();

    // 每100毫秒循环一次，对应tx loop worker的单次循环interval，循环200次，对应20秒
    // 因为tx loop worker会15秒遍历一次上下文，略大于遍历间隔
    int retry_times = 200;
    while (--retry_times >= 0) {
      // 每次循环都更新tx data table中的min_start_scn
      tx_table->update_min_start_scn_info(SCN::max_scn());

      // 判断min_start_scn的大小关系，若出错，打印到stdout
      if (prev_min_start_scn > tx_table->ctx_min_start_scn_info_.min_start_scn_in_ctx_) {
        fprintf(stdout,
                "Incorrect min_start_scn in tx data table, prev_min_start_scn = %s, current_min_start_scn = %s\n",
                to_cstring(prev_min_start_scn),
                to_cstring(tx_table->ctx_min_start_scn_info_.min_start_scn_in_ctx_));
      }
      ASSERT_LE(prev_min_start_scn, tx_table->ctx_min_start_scn_info_.min_start_scn_in_ctx_);
      prev_min_start_scn = tx_table->ctx_min_start_scn_info_.min_start_scn_in_ctx_;
      ASSERT_LE(prev_keep_alive_scn, tx_table->ctx_min_start_scn_info_.keep_alive_scn_);
      prev_keep_alive_scn = tx_table->ctx_min_start_scn_info_.keep_alive_scn_;

      ::usleep(ObTxLoopWorker::LOOP_INTERVAL);
    }
  }
}

void ObTestKeepAliveMinStartSCN::test_min_start_scn()
{
  DEF_VAL_FOR_SQL
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().get_sql_proxy2().acquire(connection));
  WRITE_SQL_BY_CONN(connection, "set ob_trx_timeout = 6000000000");
  WRITE_SQL_BY_CONN(connection, "set ob_trx_idle_timeout = 6000000000");

  int64_t current_ts = ObTimeUtil::current_time_ns();
  fprintf(stdout, "current ts : %ld\n", current_ts);

  SCN prev_min_start_scn = SCN::min_scn();
  SCN prev_keep_alive_scn = SCN::min_scn();

  // 执行循环检查前，先sleep一段时间，确保tx_loop_worker成功做一次遍历，keep_alive日志中会成功写下一次NO_CTX，
  // 在错误场景下，后续的UNKNOW日志会会修改keep_alive_handler中的keep_alive_scn，但是不会修改status
  ::sleep(15);

  // 循环检查并使用keep_alive_handler中的信息更新tx data table中的数据，在错误场景下，min_start_scn会被错误推大
  loop_check_start_scn(prev_min_start_scn, prev_keep_alive_scn);

  // create test table
  WRITE_SQL_BY_CONN(connection, "create table if not exists test.test_keep_alive_min_start_scn_t (c1 int, c2 int)");

  // insert data and trigger redo log write
  WRITE_SQL_BY_CONN(connection, "begin");
  WRITE_SQL_BY_CONN(connection, "insert into test.test_keep_alive_min_start_scn_t values(1,1)");

  // 由于系统租户设置过_private_buffer_size，所有写入都会立即产生CLOG，事务也拥有了start_scn
  // 在错误场景下，由于之前min_start_scn已经被错误推大，此时会出现min_start_scn回退
  loop_check_start_scn(prev_min_start_scn, prev_keep_alive_scn);

  WRITE_SQL_BY_CONN(connection, "commit");
  WRITE_SQL_BY_CONN(connection, "alter system minor freeze");

  // 在事务提交后再做一次检查，确保整个过程中不会有min_start_scn回退
  loop_check_start_scn(prev_min_start_scn, prev_keep_alive_scn);
}

TEST_F(ObTestKeepAliveMinStartSCN, observer_start)
{
  DEF_VAL_FOR_SQL
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().get_sql_proxy().acquire(connection));
  WRITE_SQL_BY_CONN(connection, "alter system set _private_buffer_size = '1B'");
  SERVER_LOG(INFO, "observer_start succ");
}

TEST_F(ObTestKeepAliveMinStartSCN, add_tenant)
{
  ASSERT_EQ(OB_SUCCESS, create_tenant());
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(RunCtx.tenant_id_));
  ASSERT_NE(0, RunCtx.tenant_id_);
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
}

TEST_F(ObTestKeepAliveMinStartSCN, min_start_scn_test)
{
  test_min_start_scn();
}

TEST_F(ObTestKeepAliveMinStartSCN, end)
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
