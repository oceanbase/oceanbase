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
#include "storage/tx_storage/ob_ls_service.h"

#undef private
#undef protected


using namespace oceanbase::transaction;
using namespace oceanbase::storage;
using namespace oceanbase::unittest;

class TestRunCtx
{
public:
  uint64_t tenant_id_ = 0;
  int64_t time_sec_ = 0;
};

TestRunCtx RunCtx;

namespace oceanbase
{

namespace storage
{

class TestFrequentlyFreeze : public ObSimpleClusterTestBase
{
public:
  TestFrequentlyFreeze() : ObSimpleClusterTestBase("test_frequently_freeze_") {}

  void fill_data(const int64_t idx);
  void async_tablet_freeze(const int64_t idx);
  void sync_tablet_freeze(const int64_t idx);
  void ls_freeze(const bool is_sync);
  void check_async_freeze_tablets_empty();


  void get_ls(const share::ObLSID &ls_id, ObLS *&ls);
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
  int64_t affected_rows = 0;

const int64_t SINGLE_TABLE_PARTITION_COUNT = 4000;
const int64_t TABLET_FREEZE_THREAD_COUNT = 10;
int64_t FIRST_TABLET_ID = 0;
int64_t LAST_TABLET_ID = 0;

void TestFrequentlyFreeze::get_ls(const share::ObLSID &ls_id, ObLS *&ls)
{
  ls = nullptr;
  ObLSIterator *ls_iter = nullptr;
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService *);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ASSERT_NE(nullptr, ls = ls_handle.get_ls());
}

void TestFrequentlyFreeze::fill_data(const int64_t idx)
{
  DEF_VAL_FOR_SQL
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  sqlclient::ObISQLConnection *connection = nullptr;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(connection));
  ASSERT_NE(nullptr, connection);

  WRITE_SQL_BY_CONN(connection, "set ob_trx_timeout = 3000000000");
  WRITE_SQL_BY_CONN(connection, "set ob_trx_idle_timeout = 3000000000");
  WRITE_SQL_BY_CONN(connection, "set ob_query_timeout = 3000000000");
  WRITE_SQL_FMT_BY_CONN(connection, "create sequence seq_%ld cache 1000000 noorder;", idx);
  STORAGE_LOG(INFO, "start fill data", K(idx));
  for (int i = 0; i < 5; i++) {
    fprintf(stdout, "start fill data. thread_idx = %ld, round = %d\n", idx, i);
    WRITE_SQL_FMT_BY_CONN(
        connection,
        "Insert /*+ parallel(4) enable_parallel_dml */ into gl_t_%ld select seq_%ld.nextval, 'ob' from "
        "table(generator(20000));",
        idx,
        idx);
    fprintf(stdout, "finish fill data. thread_idx = %ld, round = %d\n", idx, i);
  }
}

void TestFrequentlyFreeze::async_tablet_freeze(const int64_t idx)
{
  ObLS *ls = nullptr;
  (void)get_ls(share::ObLSID(1001), ls);

  int64_t int_tablet_id_to_freeze = FIRST_TABLET_ID + idx;
  fprintf(stdout, "async tablet freeze start. thread = %ld\n", idx);
  STORAGE_LOG(INFO, "start async tablet freeze", K(idx));
  while (int_tablet_id_to_freeze <= LAST_TABLET_ID) {
    const bool is_sync = false;
    ObTabletID tablet_to_freeze(int_tablet_id_to_freeze);
    STORAGE_LOG(INFO, "start tablet freeze", K(tablet_to_freeze), K(is_sync), K(idx));
    ASSERT_EQ(OB_SUCCESS, ls->tablet_freeze(tablet_to_freeze, is_sync));
    usleep(100 * 1000);
    STORAGE_LOG(INFO, "finish tablet freeze", K(tablet_to_freeze), K(is_sync), K(idx));

    int_tablet_id_to_freeze += TABLET_FREEZE_THREAD_COUNT;
  }
  fprintf(stdout, "async tablet freeze finish. thread = %ld\n", idx);
  STORAGE_LOG(INFO, "one async tablet freeze task finish", K(idx));
}

void TestFrequentlyFreeze::sync_tablet_freeze(const int64_t idx)
{
  ObLS *ls = nullptr;
  (void)get_ls(share::ObLSID(1001), ls);

  ObTabletID tablet_to_freeze(FIRST_TABLET_ID + idx);
  fprintf(stdout, "sync tablet freeze start. thread = %ld\n", idx);
  STORAGE_LOG(INFO, "start sync tablet freeze", K(idx));
  while (tablet_to_freeze.id() <= LAST_TABLET_ID) {
    const bool is_sync = true;
    const int64_t abs_timeout_ts = ObClockGenerator::getClock() + 600LL * 1000LL * 1000LL;
    STORAGE_LOG(INFO, "start tablet freeze", K(tablet_to_freeze), K(is_sync), K(idx));
    ASSERT_EQ(OB_SUCCESS, ls->tablet_freeze(tablet_to_freeze, is_sync, abs_timeout_ts));
    ::sleep(2);
    tablet_to_freeze = ObTabletID(tablet_to_freeze.id() + TABLET_FREEZE_THREAD_COUNT);
    STORAGE_LOG(INFO, "finish tablet freeze", K(tablet_to_freeze), K(is_sync), K(idx));
  }
  fprintf(stdout, "sync tablet freeze finish. thread = %ld\n", idx);

  STORAGE_LOG(INFO, "one async tablet freeze task finish", K(idx));
}

void TestFrequentlyFreeze::ls_freeze(const bool is_sync)
{
  ObLS *ls = nullptr;
  (void)get_ls(share::ObLSID(1001), ls);

  fprintf(stdout, "ls freeze start. is_sync = %d\n", is_sync);
  STORAGE_LOG(INFO, "start ls freeze", K(is_sync));
  const int64_t abs_timeout_ts = ObClockGenerator::getClock() + 600LL * 1000LL * 1000LL;
  for (int i = 0; i < 4; i++) {
    ASSERT_EQ(OB_SUCCESS, ls->logstream_freeze(-1, is_sync, abs_timeout_ts));
    sleep(200);
  }
  fprintf(stdout, "ls freeze finish. is_sync = %d\n", is_sync);
}

void TestFrequentlyFreeze::check_async_freeze_tablets_empty()
{
  ObLS *ls = nullptr;
  (void)get_ls(share::ObLSID(1001), ls);

  bool async_freeze_finished = false;
  int64_t max_retry_times = 100;
  while (max_retry_times-- > 0) {
    if (ls->get_freezer()->get_async_freeze_tablets().empty()) {
      async_freeze_finished = true;
      break;
    }
    sleep(1);
  }
  ASSERT_EQ(true, async_freeze_finished);
  fprintf(stdout, "check async freeze tablets finish. \n");
}

TEST_F(TestFrequentlyFreeze, observer_start)
{
  SERVER_LOG(INFO, "observer_start succ");
}

TEST_F(TestFrequentlyFreeze, add_tenant)
{
  ASSERT_EQ(OB_SUCCESS, create_tenant());
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(RunCtx.tenant_id_));
  ASSERT_NE(0, RunCtx.tenant_id_);
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
}

TEST_F(TestFrequentlyFreeze, create_table)
{
  DEF_VAL_FOR_SQL
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  sqlclient::ObISQLConnection *connection = nullptr;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(connection));
  ASSERT_NE(nullptr, connection);

  WRITE_SQL_BY_CONN(connection, "set ob_trx_timeout = 3000000000");
  WRITE_SQL_BY_CONN(connection, "set ob_trx_idle_timeout = 3000000000");
  WRITE_SQL_BY_CONN(connection, "set ob_query_timeout = 3000000000");

  FIRST_TABLET_ID = 200000 + 1;
  WRITE_SQL_FMT_BY_CONN(
      connection,
      "create table gl_t_%d (sid int primary key, sname varchar(10)) partition by hash(sid) partitions %ld",
      0,
      SINGLE_TABLE_PARTITION_COUNT);
  WRITE_SQL_FMT_BY_CONN(
      connection,
      "create table gl_t_%d (sid int primary key, sname varchar(10)) partition by hash(sid) partitions %ld",
      1,
      SINGLE_TABLE_PARTITION_COUNT);
  WRITE_SQL_FMT_BY_CONN(
      connection,
      "create table gl_t_%d (sid int primary key, sname varchar(10)) partition by hash(sid) partitions %ld",
      2,
      SINGLE_TABLE_PARTITION_COUNT);
  LAST_TABLET_ID = 200000 + SINGLE_TABLE_PARTITION_COUNT * 3;
}

/**
 * 本case主要用于验证在超多Tablet且都存在少量写入的场景下触发频繁的冻结是否会有问题。
 * 主要的流程为：
 * 1. 创建了三个具有4000个分区的表，分区方式是主键的hash
 * 2. 每个表都插入了少量数据，按主键递增的方式插入，插入行数为分区数的5倍，尽量保证每个分区都有数据
 * 3. 启动了20个线程在插入数据的过程中执行tablet冻结，其中10个异步冻结，10个同步冻结。异步冻结会重复做10次，同步冻结只做了1次
 *    总共预计是会执行13万次Tablet级冻结。
 * 4. 启动了1个线程执行同步日志流冻结，每隔50秒一次，总计执行20次
 * 5. 启动了1个线程执行异步日志流冻结，每隔50秒一次，总结执行20次
 * 6. 最后确认ObFreezer中没有残留的待异步的冻结的分区
 */
TEST_F(TestFrequentlyFreeze, frequently_freeze)
{
  int ret = OB_SUCCESS;
  std::vector<std::thread> worker_threads;

  for (int i = 0; i < 3; i++) {
    worker_threads.push_back(std::thread([i, this]() {
      int ret = OB_SUCCESS;
      MTL_SWITCH(RunCtx.tenant_id_) { this->fill_data(i); };
    }));
  }

  ::sleep(30);

  for (int i = 0; i < TABLET_FREEZE_THREAD_COUNT; i++) {
    worker_threads.push_back(std::thread([i, this]() {
      int ret = OB_SUCCESS;
      MTL_SWITCH(RunCtx.tenant_id_)
      {
        for (int k = 0; k < 10; k++) {
          this->async_tablet_freeze(i);
          sleep(10);
        }
      };
    }));
  }

  for (int i = 0; i < TABLET_FREEZE_THREAD_COUNT; i++) {
    worker_threads.push_back(std::thread([i, this]() {
      int ret = OB_SUCCESS;
      MTL_SWITCH(RunCtx.tenant_id_)
      {
        this->sync_tablet_freeze(i);
      };
    }));
  }

  worker_threads.push_back(std::thread([this]() {
    int ret = OB_SUCCESS;
    MTL_SWITCH(RunCtx.tenant_id_) { this->ls_freeze(true); };
  }));
  sleep(100);
  worker_threads.push_back(std::thread([this]() {
    int ret = OB_SUCCESS;
    MTL_SWITCH(RunCtx.tenant_id_) { this->ls_freeze(false); };
  }));


  for (auto &my_thread : worker_threads) {
    my_thread.join();
  }

  MTL_SWITCH(RunCtx.tenant_id_) { check_async_freeze_tablets_empty(); }
}

}  // namespace storage
}  // namespace oceanbase

int main(int argc, char **argv)
{
  int64_t c = 0;
  int64_t time_sec = 0;
  char *log_level = (char*)"WDIAG";
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
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
