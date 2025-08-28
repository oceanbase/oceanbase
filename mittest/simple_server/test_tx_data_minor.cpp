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
#define USING_LOG_PREFIX SERVER
#define protected public
#define private public

#include "env/ob_simple_cluster_test_base.h"
#include "lib/mysqlclient/ob_mysql_result.h"

namespace oceanbase
{
namespace unittest
{

using namespace oceanbase::transaction;
using namespace oceanbase::storage;

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
  int64_t affected_rows = 0; 

class TestRunCtx
{
public:
  uint64_t tenant_id_ = 0;
  int64_t time_sec_ = 0;
};

TestRunCtx RunCtx;

class ObTxDataMinorTest : public ObSimpleClusterTestBase
{
public:
  // 指定case运行目录前缀 test_ob_simple_cluster_
  ObTxDataMinorTest() : ObSimpleClusterTestBase("test_tx_data_minor_") {}
  void insert_and_freeze();

public:
  void insert_tx_data();
  void freeze_tx_data();
  void check_minor_result();

public:
  bool stop = false;
  int freeze_duration_ = 60 * 1000 * 1000;
};

void ObTxDataMinorTest::insert_tx_data()
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();

  OB_LOG(INFO, "insert tx data start");
  int64_t i = 0;
  int64_t affected_rows = 0;
  while (!ATOMIC_LOAD(&stop)) {
    ObSqlString sql;
    EXE_SQL_FMT("insert into test_tx_data_minor_t values(%ld, %ld)", 1L, 1L);
  }
}

void ObTxDataMinorTest::freeze_tx_data()
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();

  const int64_t start_ts = ObClockGenerator::getClock();
  while (ObClockGenerator::getClock() - start_ts <= freeze_duration_) {
    ::sleep(5);
    int64_t affected_rows = 0;
    ObSqlString sql;
    EXE_SQL("alter system minor freeze tenant sys");
  }
  ATOMIC_STORE(&stop, true);
  STORAGE_LOG(INFO, "freeze done");
}

int INSERT_THREAD_NUM = 32;
void ObTxDataMinorTest::insert_and_freeze()
{
  std::vector<std::thread> insert_threads;
  for (int i = 0; i < INSERT_THREAD_NUM; ++i) {
    insert_threads.push_back(std::thread([this]() { this->insert_tx_data(); }));
  }

  std::thread tenant_freeze_thread([this]() { this->freeze_tx_data(); });
  tenant_freeze_thread.join();
  for (int i = 0; i < INSERT_THREAD_NUM; ++i) {
    insert_threads[i].join();
  }
}

void ObTxDataMinorTest::check_minor_result()
{
  sleep(5);
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();

  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  int64_t row_cnt = 0;
  ASSERT_EQ(
      OB_SUCCESS,
      sql.assign("select macro_block_count, multiplexed_macro_block_count from oceanbase.__all_virtual_tablet_compaction_history "
                 "where tenant_id = 1 and ls_id = 1 and tablet_id = 49402 and type = 'MINOR_MERGE'"));

  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
    sqlclient::ObMySQLResult *result = res.get_result();
    ASSERT_NE(nullptr, result);
    ASSERT_EQ(OB_SUCCESS, result->next());
    int64_t minor_cnt = 0;
    int64_t total_mb_cnt = 0;
    int64_t total_multiplexed_mb_cnt = 0;
    while (OB_SUCC(result->next())) {
      minor_cnt++;
      int64_t mb_cnt = 0;
      int64_t multiplexed_mb_cnt = 0;
      ASSERT_EQ(OB_SUCCESS, result->get_int("macro_block_count", mb_cnt));
      ASSERT_EQ(OB_SUCCESS, result->get_int("multiplexed_macro_block_count", multiplexed_mb_cnt));
      total_mb_cnt += mb_cnt;
      total_multiplexed_mb_cnt += multiplexed_mb_cnt;
    }

    ASSERT_GT(minor_cnt, 3);
    
    int64_t mb_reuse_percentage = total_multiplexed_mb_cnt * 100 / total_mb_cnt;
    ASSERT_GT(mb_reuse_percentage, 50);
  }
}

TEST_F(ObTxDataMinorTest, observer_start)
{
  RunCtx.tenant_id_ = 1;
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();

  int64_t affected_rows = 0;
  ObSqlString sql;
  EXE_SQL("alter system set ob_compaction_schedule_interval='5s' tenant sys");
  EXE_SQL("alter system set minor_compact_trigger=1 tenant sys");
  SERVER_LOG(INFO, "observer_start succ");
}

TEST_F(ObTxDataMinorTest, create_table)
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();

  int64_t affected_rows = 0;
  ObSqlString sql;
  OB_LOG(INFO, "create_table start");
  EXE_SQL("create table if not exists test_tx_data_minor_t (c1 int, c2 int)");
  OB_LOG(INFO, "create_table succ");
}

TEST_F(ObTxDataMinorTest, insert_and_freeze) 
{
  insert_and_freeze();
}

TEST_F(ObTxDataMinorTest, check_minor_result) 
{
  check_minor_result();
}


TEST_F(ObTxDataMinorTest, end)
{
  if (RunCtx.time_sec_ > 0) {
    ::sleep(RunCtx.time_sec_);
  }
}

} // end unittest
} // end oceanbase


int main(int argc, char **argv)
{
  int64_t c = 0;
  int64_t time_sec = 0;
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
