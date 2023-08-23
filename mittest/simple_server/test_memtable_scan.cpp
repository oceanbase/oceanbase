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

#define WRITE_SQL_BY_CONN(conn, sql_str)      \
  ASSERT_EQ(OB_SUCCESS, sql.assign(sql_str)); \
  ASSERT_EQ(OB_SUCCESS, conn->execute_write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));

#define WRITE_SQL_FMT_BY_CONN(conn, ...)              \
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(__VA_ARGS__)); \
  ASSERT_EQ(OB_SUCCESS, conn->execute_write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));

#define READ_SQL_BY_CONN(conn, sql_str)                                 \
  ASSERT_EQ(OB_SUCCESS, sql.assign(sql_str));                           \
  ASSERT_EQ(OB_SUCCESS, conn->execute_read(OB_SYS_TENANT_ID, sql.ptr(), read_res));

#define READ_SQL_FMT_BY_CONN(conn, ...)                                 \
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(__VA_ARGS__));                   \
  ASSERT_EQ(OB_SUCCESS, conn->execute_read(OB_SYS_TENANT_ID, sql.ptr(), read_res));

#define DEF_VAL_FOR_SQL      \
  int ret = OB_SUCCESS;      \
  ObSqlString sql;           \
  int64_t affected_rows = 0; \
  sqlclient::ObISQLConnection *connection = nullptr;\
  ObISQLClient::ReadResult read_res;\
  sqlclient::ObMySQLResult *result;

class TestRunCtx
{
public:
  uint64_t tenant_id_ = 0;
  int time_sec_ = 0;
};

TestRunCtx RunCtx;

class ObTestMemtableScan : public ObSimpleClusterTestBase {
public:
  // 创建一个100G数据盘、15G内存的observer
  ObTestMemtableScan() : ObSimpleClusterTestBase("test_memtable_scan_", "200G", "100G") {}

  void minor_freeze_data_and_wait()
  {
    common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
    sqlclient::ObISQLConnection *connection = nullptr;
    ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(connection));

    int ret = OB_SUCCESS;
    ObSqlString sql;
    int64_t affected_rows = 0;

    WRITE_SQL_BY_CONN(connection, "alter system minor freeze tenant all;");
    fprintf(stdout, "start flush user data\n");
    sleep(10);

    int retry_times = 0;
    bool freeze_success = false;
    HEAP_VAR(ObMySQLProxy::MySQLResult, res)
    {
      int64_t cnt = -1;
      while (++retry_times <= 600) {
        ASSERT_EQ(OB_SUCCESS,
                  connection->execute_read(OB_SYS_TENANT_ID,
                                           "select count(*) as cnt from oceanbase.__all_virtual_table_mgr where "
                                           "table_type=0 and is_active like '%NO%';",
                                           res));
        common::sqlclient::ObMySQLResult *result = res.mysql_result();
        ASSERT_EQ(OB_SUCCESS, result->next());
        ASSERT_EQ(OB_SUCCESS, result->get_int("cnt", cnt));
        if (0 == cnt) {
          freeze_success = true;
          break;
        }
      }
      fprintf(stdout,  "waitting for data minor merge. retry times = %d\n", retry_times);
      sleep(1);
    }

    ASSERT_EQ(true, freeze_success);
  }
};

TEST_F(ObTestMemtableScan, observer_start)
{
  SERVER_LOG(INFO, "observer_start succ");
}

TEST_F(ObTestMemtableScan, add_tenant)
{
  // 创建普通租户tt1，使用大内存
  ASSERT_EQ(OB_SUCCESS, create_tenant("tt1", "10G", "50G", false));
  // 获取租户tt1的tenant_id
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(RunCtx.tenant_id_));
  ASSERT_NE(0, RunCtx.tenant_id_);
  // 初始化普通租户tt1的sql proxy
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());

  DEF_VAL_FOR_SQL;
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().get_sql_proxy2().acquire(connection));
  // 调大转储mini后触发minor merge的次数
  WRITE_SQL_BY_CONN(connection, "ALTER SYSTEM SET minor_compact_trigger = 10;");
  // 调大限速
  WRITE_SQL_BY_CONN(connection, "ALTER SYSTEM SET writing_throttling_trigger_percentage = 100;");
  sleep(2);
  // 调大冻结阈值
  WRITE_SQL_BY_CONN(connection, "ALTER SYSTEM SET freeze_trigger_percentage = 80;");
  // 开启优化
  WRITE_SQL_BY_CONN(connection, "alter system set memtable_scan_filter_pushdown = 1;");
}

// TEST_F(ObTestMemtableScan, multiple_scan_merge3)
// {
//   DEF_VAL_FOR_SQL;
//   common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
//   ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(connection));

//   // 调大超时时间
//   WRITE_SQL_BY_CONN(connection, "SET SESSION OB_QUERY_TIMEOUT = 1000 * 1000 * 1000 * 1000;");
//   WRITE_SQL_BY_CONN(connection, "SET SESSION OB_TRX_TIMEOUT = 1000 * 1000 * 1000 * 1000;");
//   WRITE_SQL_BY_CONN(connection, "SET SESSION OB_TRX_IDLE_TIMEOUT = 1000 * 1000 * 1000 * 1000;");
//   sleep(5);

//   // 建表
//   WRITE_SQL_BY_CONN(
//       connection,
//       "CREATE TABLE test.multiple_scan_merge3 (c1 BIGINT PRIMARY KEY, c2 BIGINT, c3 BIGINT, c4 BIGINT, c5 BIGINT, c6 BIGINT, c7 BIGINT);")
//   WRITE_SQL_BY_CONN(connection, "CREATE SEQUENCE m3s0 START WITH 0 MINVALUE 0 MAXVALUE 100000000 INCREMENT BY 300 CACHE 10000000 ORDER;");
//   WRITE_SQL_BY_CONN(connection, "CREATE SEQUENCE m3s1 START WITH 1 MINVALUE 0 MAXVALUE 100000000 INCREMENT BY 300 CACHE 10000000 ORDER;");
//   WRITE_SQL_BY_CONN(connection, "CREATE SEQUENCE m3s2 START WITH 2 MINVALUE 0 MAXVALUE 100000000 INCREMENT BY 300 CACHE 10000000 ORDER;");
//   WRITE_SQL_BY_CONN(connection, "CREATE SEQUENCE m3s3 START WITH 3 MINVALUE 0 MAXVALUE 100000000 INCREMENT BY 300 CACHE 10000000 ORDER;");
//   WRITE_SQL_BY_CONN(connection, "CREATE SEQUENCE m3s4 START WITH 0 MINVALUE 0 MAXVALUE 100000000 INCREMENT BY 1 CACHE 10000000 ORDER;");

//   // 循环 4 次，每次写 1000 行，产生 4 个 mini sstable
//   char sql_str[1024] = "";
//   for (int64_t i = 0; i < 4; i++) {
//     WRITE_SQL_FMT_BY_CONN(connection,
//                           "INSERT INTO                                "
//                           "       test.multiple_scan_merge3           "
//                           "SELECT                                     "
//                           "       m3s%ld.nextval c1,                  "
//                           "       uniform(1, 1000, random()) c2,      "
//                           "       uniform(1, 1000, random()) c3,      "
//                           "       uniform(1, 1000, random()) c4,      "
//                           "       uniform(1, 1000, random()) c5,      "
//                           "       uniform(1, 1000, random()) c6,      "
//                           "       uniform(1, 1000, random()) c7       "
//                           "FROM                                       "
//                           "       table(generator(1000));             ", i);
//     fprintf(stdout, "do minor freeze once. freeze count : %ld\n", i+1);
//     minor_freeze_data_and_wait();
//   }

//   // 写 100000 行数据到 memtable
//   WRITE_SQL_BY_CONN(connection,
//                     "INSERT IGNORE INTO                         "
//                     "       test.multiple_scan_merge3           "
//                     "SELECT                                     "
//                     "       m3s4.nextval c1,                    "
//                     "       uniform(1, 1000, random()) c2,      "
//                     "       uniform(1, 1000, random()) c3,      "
//                     "       uniform(1, 1000, random()) c4,      "
//                     "       uniform(1, 1000, random()) c5,      "
//                     "       uniform(1, 1000, random()) c6,      "
//                     "       uniform(1, 1000, random()) c7       "
//                     "FROM                                       "
//                     "       table(generator(100000));           ");

//   fprintf(stdout, "prepare data done, start time cost test ...\n");

//   int64_t len1 = 0,len2 = 0, c1;
//   clock_t start, end;

//   start = clock();
//   READ_SQL_BY_CONN(connection, "select c1, c2, c3 from test.multiple_scan_merge3 where (c1 < 100000) and (c2 < 700 and (c3 > 300 or (c4 * 2 < 1200 and (c5 + 100 > 200 or (c6 < 800 and c7 > 200))))); ");

//   result = read_res.get_result();
//   ASSERT_NE(nullptr, result);

//   while (OB_SUCC(result->next())) {
//     ASSERT_EQ(OB_SUCCESS, result->get_int("c1", c1));
//     ++len1;
//   }
//   ASSERT_EQ(OB_ITER_END, ret);
//   end = clock();

//   fprintf(stdout, "get %ld result rows, use timem %f\n", len1, (double)(end - start) / CLOCKS_PER_SEC);

//   WRITE_SQL_BY_CONN(connection, "alter system set memtable_scan_filter_pushdown = 0;");
//   sleep(10);

//   start = clock();
//   READ_SQL_BY_CONN(connection, "select c1, c2, c3 from test.multiple_scan_merge3 where (c1 < 100000) and (c2 < 700 and (c3 > 300 or (c4 * 2 < 1200 and (c5 + 100 > 200 or (c6 < 800 and c7 > 200))))); ");

//   result = read_res.get_result();
//   ASSERT_NE(nullptr, result);

//   while (OB_SUCC(result->next())) {
//     ASSERT_EQ(OB_SUCCESS, result->get_int("c1", c1));
//     ++len2;
//   }
//   ASSERT_EQ(OB_ITER_END, ret);
//   end = clock();

//   fprintf(stdout, "get %ld result rows, use timem %f\n", len2, (double)(end - start) / CLOCKS_PER_SEC);
// }

// 正确性验证，一种比较极端的情况，数据按 rowkey 先后分布在 4张 SSTable 与 MemTable 之上
// 这种情况会触发单边扫描，但是单边扫描立刻就会结束；理论上这种情况应该是修改后性能最差的情况；
// TEST_F(ObTestMemtableScan, multiple_scan_merge1)
// {
//   DEF_VAL_FOR_SQL;
//   common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
//   ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(connection));
//   // 调大超时时间
//   WRITE_SQL_BY_CONN(connection, "SET GLOBAL OB_QUERY_TIMEOUT = 10 * 1000 * 1000 * 1000;");
//   WRITE_SQL_BY_CONN(connection, "SET GLOBAL OB_TRX_TIMEOUT = 10 * 1000 * 1000 * 1000;");
//   WRITE_SQL_BY_CONN(connection, "SET GLOBAL OB_TRX_IDLE_TIMEOUT = 10 * 1000 * 1000 * 1000;");

//   // 建表
//   WRITE_SQL_BY_CONN(
//       connection,
//       "CREATE TABLE test.multiple_scan_merge1 (c1 BIGINT PRIMARY KEY, c2 BIGINT, c3 BIGINT);")
//   WRITE_SQL_BY_CONN(connection, "CREATE SEQUENCE m1s0 START WITH 0 MINVALUE 0 MAXVALUE 100000000 INCREMENT BY 5 CACHE 10000000 ORDER;");
//   WRITE_SQL_BY_CONN(connection, "CREATE SEQUENCE m1s1 START WITH 1 MINVALUE 0 MAXVALUE 100000000 INCREMENT BY 5 CACHE 10000000 ORDER;");
//   WRITE_SQL_BY_CONN(connection, "CREATE SEQUENCE m1s2 START WITH 2 MINVALUE 0 MAXVALUE 100000000 INCREMENT BY 5 CACHE 10000000 ORDER;");
//   WRITE_SQL_BY_CONN(connection, "CREATE SEQUENCE m1s3 START WITH 3 MINVALUE 0 MAXVALUE 100000000 INCREMENT BY 5 CACHE 10000000 ORDER;");
//   WRITE_SQL_BY_CONN(connection, "CREATE SEQUENCE m1s4 START WITH 4 MINVALUE 0 MAXVALUE 100000000 INCREMENT BY 5 CACHE 10000000 ORDER;");

//   // 循环 4 次，每次写 10000 行，产生 4 个 mini sstable
//   char sql_str[1024] = "";
//   for (int64_t i = 0; i < 4; i++) {
//     WRITE_SQL_FMT_BY_CONN(connection,
//                           "INSERT INTO                                "
//                           "       test.multiple_scan_merge1           "
//                           "SELECT                                     "
//                           "       m1s%ld.nextval c1,                  "
//                           "       random() %% 10000 c2,               "
//                           "       zipf(1, 100, random(3)) c3          "
//                           "FROM                                       "
//                           "       table(generator(1000));            ", i);
//     fprintf(stdout, "do minor freeze once. freeze count : %ld\n", i+1);
//     minor_freeze_data_and_wait();
//   }

//   // 写 10000 行数据到 memtable
//   WRITE_SQL_BY_CONN(connection,
//                     "INSERT INTO                                "
//                     "       test.multiple_scan_merge1           "
//                     "SELECT                                     "
//                     "       m1s4.nextval c1,                    "
//                     "       random() % 10000000 c2,             "
//                     "       zipf(1, 100, random(3)) c3          "
//                     "FROM                                       "
//                     "       table(generator(1000));            ");
//   fprintf(stdout, "prepare data done, start test ...\n");
//   // 从表中读取数据
//   READ_SQL_FMT_BY_CONN(connection, "select c1, c2, c3 from test.multiple_scan_merge1;");
//   result = read_res.get_result();
//   ASSERT_NE(nullptr, result);

//   int rowkey = 0;
//   while (OB_SUCC(result->next())) {
//     int64_t c1;
//     ASSERT_EQ(OB_SUCCESS, result->get_int("c1", c1));
//     ASSERT_EQ(c1, rowkey++);
//   }
//   ASSERT_EQ(OB_ITER_END, ret);
// }

// // 正确性测试，(0, 1, 2, 3) 分布在 4张 SSTable 之上；(4-9) 分布在 MemTable 之上；
// // 会触发单边扫描，每个 batch 包含 5 条数据；
// TEST_F(ObTestMemtableScan, multiple_scan_merge2)
// {
//   DEF_VAL_FOR_SQL;
//   common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
//   ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(connection));

//   // 建表
//   WRITE_SQL_BY_CONN(
//       connection,
//       "CREATE TABLE test.multiple_scan_merge2 (c1 BIGINT PRIMARY KEY, c2 BIGINT, c3 BIGINT);")
//   WRITE_SQL_BY_CONN(connection, "CREATE SEQUENCE m2s0 START WITH 0 MINVALUE 0 MAXVALUE 100000000 INCREMENT BY 10 CACHE 10000000 ORDER;");
//   WRITE_SQL_BY_CONN(connection, "CREATE SEQUENCE m2s1 START WITH 1 MINVALUE 0 MAXVALUE 100000000 INCREMENT BY 10 CACHE 10000000 ORDER;");
//   WRITE_SQL_BY_CONN(connection, "CREATE SEQUENCE m2s2 START WITH 2 MINVALUE 0 MAXVALUE 100000000 INCREMENT BY 10 CACHE 10000000 ORDER;");
//   WRITE_SQL_BY_CONN(connection, "CREATE SEQUENCE m2s3 START WITH 3 MINVALUE 0 MAXVALUE 100000000 INCREMENT BY 10 CACHE 10000000 ORDER;");
//   WRITE_SQL_BY_CONN(connection, "CREATE SEQUENCE m2s4 START WITH 0 MINVALUE 0 MAXVALUE 100000000 INCREMENT BY 1 CACHE 10000000 ORDER;");

//   // 循环 4 次，每次写 1000 行，产生 4 个 mini sstable
//   char sql_str[1024] = "";
//   for (int64_t i = 0; i < 4; i++) {
//     WRITE_SQL_FMT_BY_CONN(connection,
//                           "INSERT INTO                                "
//                           "       test.multiple_scan_merge2           "
//                           "SELECT                                     "
//                           "       m2s%ld.nextval c1,                  "
//                           "       random() %% 10000 c2,               "
//                           "       zipf(1, 100, random(3)) c3         "
//                           "FROM                                       "
//                           "       table(generator(1000));             ", i);
//     fprintf(stdout, "do minor freeze once. freeze count : %ld\n", i+1);
//     minor_freeze_data_and_wait();
//   }

//   // 写 10010 行数据到 memtable
//   WRITE_SQL_BY_CONN(connection,
//                     "INSERT IGNORE INTO                         "
//                     "       test.multiple_scan_merge2           "
//                     "SELECT                                     "
//                     "       m2s4.nextval c1,                    "
//                     "       random() % 10000000 c2,             "
//                     "       zipf(1, 100, random(3)) c3         "
//                     "FROM                                       "
//                     "       table(generator(10010));            ");
//   // 从表中读取数据
//   fprintf(stdout, "prepare data done, start test ...\n");
//   // 从表中读取数据
//   READ_SQL_FMT_BY_CONN(connection, "select c1, c2, c3 from test.multiple_scan_merge2 where c1 >= 100 order by c1");
//   result = read_res.get_result();
//   ASSERT_NE(nullptr, result);

//   int rowkey = 100;
//   while (OB_SUCC(result->next())) {
//     int64_t c1;
//     ASSERT_EQ(OB_SUCCESS, result->get_int("c1", c1));
//     ASSERT_EQ(c1, rowkey++);
//   }
//   ASSERT_EQ(OB_ITER_END, ret);
// }

// TEST_F(ObTestMemtableScan, multiple_scan_merge3)
// {
//   DEF_VAL_FOR_SQL;
//   common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
//   ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(connection));

//   // 调大超时时间
//   WRITE_SQL_BY_CONN(connection, "SET SESSION OB_QUERY_TIMEOUT = 1000 * 1000 * 1000 * 1000;");
//   WRITE_SQL_BY_CONN(connection, "SET SESSION OB_TRX_TIMEOUT = 1000 * 1000 * 1000 * 1000;");
//   WRITE_SQL_BY_CONN(connection, "SET SESSION OB_TRX_IDLE_TIMEOUT = 1000 * 1000 * 1000 * 1000;");
//   sleep(5);

//   // 建表
//   WRITE_SQL_BY_CONN(
//       connection,
//       "CREATE TABLE test.multiple_scan_merge3 (c1 BIGINT PRIMARY KEY, c2 BIGINT, c3 BIGINT);")
//   WRITE_SQL_BY_CONN(connection, "CREATE SEQUENCE m3s0 START WITH 0 MINVALUE 0 MAXVALUE 100000000 INCREMENT BY 300 CACHE 10000000 ORDER;");
//   WRITE_SQL_BY_CONN(connection, "CREATE SEQUENCE m3s1 START WITH 1 MINVALUE 0 MAXVALUE 100000000 INCREMENT BY 300 CACHE 10000000 ORDER;");
//   WRITE_SQL_BY_CONN(connection, "CREATE SEQUENCE m3s2 START WITH 2 MINVALUE 0 MAXVALUE 100000000 INCREMENT BY 300 CACHE 10000000 ORDER;");
//   WRITE_SQL_BY_CONN(connection, "CREATE SEQUENCE m3s3 START WITH 3 MINVALUE 0 MAXVALUE 100000000 INCREMENT BY 300 CACHE 10000000 ORDER;");
//   WRITE_SQL_BY_CONN(connection, "CREATE SEQUENCE m3s4 START WITH 0 MINVALUE 0 MAXVALUE 100000000 INCREMENT BY 1 CACHE 10000000 ORDER;");

//   // 循环 4 次，每次写 1000 行，产生 4 个 mini sstable
//   char sql_str[1024] = "";
//   for (int64_t i = 0; i < 4; i++) {
//     WRITE_SQL_FMT_BY_CONN(connection,
//                           "INSERT INTO                                "
//                           "       test.multiple_scan_merge3           "
//                           "SELECT                                     "
//                           "       m3s%ld.nextval c1,                  "
//                           "       uniform(1, 1000, random()) c2,      "
//                           "       uniform(1, 1000, random()) c3       "
//                           "FROM                                       "
//                           "       table(generator(1000));             ", i);
//     fprintf(stdout, "do minor freeze once. freeze count : %ld\n", i+1);
//     minor_freeze_data_and_wait();
//   }

//   // 写 100000 行数据到 memtable
//   WRITE_SQL_BY_CONN(connection,
//                     "INSERT IGNORE INTO                         "
//                     "       test.multiple_scan_merge3           "
//                     "SELECT                                     "
//                     "       m3s4.nextval c1,                    "
//                     "       uniform(1, 1000, random()) c2,      "
//                     "       uniform(1, 1000, random()) c3      "
//                     "FROM                                       "
//                     "       table(generator(10000));           ");

//   fprintf(stdout, "prepare data done, start time cost test ...\n");

//   READ_SQL_BY_CONN(connection, "select c1, c2, c3 from test.multiple_scan_merge3 where c1 < 10000;");

//   result = read_res.get_result();
//   ASSERT_NE(nullptr, result);

//   int64_t rowkey = 0, c1;
//   while (OB_SUCC(result->next())) {
//     ASSERT_EQ(OB_SUCCESS, result->get_int("c1", c1));
//     ASSERT_EQ(c1, rowkey++);
//   }
//   ASSERT_EQ(OB_ITER_END, ret);
// }

TEST_F(ObTestMemtableScan, correctness_with_filter)
{
  DEF_VAL_FOR_SQL;
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().get_sql_proxy2().acquire(connection));

  WRITE_SQL_BY_CONN(connection, "ALTER SYSTEM SET minor_compact_trigger = 10;");
  // 调大限速
  WRITE_SQL_BY_CONN(connection, "ALTER SYSTEM SET writing_throttling_trigger_percentage = 100;");
  // 调大冻结阈值
  WRITE_SQL_BY_CONN(connection, "ALTER SYSTEM SET freeze_trigger_percentage = 80;");

  // 调大超时时间
  WRITE_SQL_BY_CONN(connection, "SET SESSION OB_QUERY_TIMEOUT = 1000 * 1000 * 1000 * 1000;");
  WRITE_SQL_BY_CONN(connection, "SET SESSION OB_TRX_TIMEOUT = 1000 * 1000 * 1000 * 1000;");
  WRITE_SQL_BY_CONN(connection, "SET SESSION OB_TRX_IDLE_TIMEOUT = 1000 * 1000 * 1000 * 1000;");

  sleep(10);

  // 建表
  WRITE_SQL_BY_CONN(
      connection,
      "CREATE TABLE test.memtable_scan_with_filter (c1 BIGINT PRIMARY KEY, c2 INT, c3 MEDIUMINT);")

  // 循环五次，每次写一万行，产生5个mini sstable
  for (int64_t i = 0; i < 5; i++) {
    WRITE_SQL_BY_CONN(connection,
                      "INSERT IGNORE INTO                         "
                      "       test.memtable_scan_with_filter      "
                      "SELECT                                     "
                      "       uniform(1, 300000, random()) c1,    "
                      "       random() % 10000 c2,                "
                      "       zipf(1, 100, random(3)) c3          "
                      "FROM                                       "
                      "       table(generator(10000));            ");
    fprintf(stdout, "do minor freeze once. freeze count : %ld\n", i+1);
    minor_freeze_data_and_wait();
  }

  // 写10万行数据到memtable
  WRITE_SQL_BY_CONN(connection,
                    "INSERT IGNORE INTO                         "
                    "       test.memtable_scan_with_filter      "
                    "SELECT                                     "
                    "       uniform(1, 300000, random()) c1,    "
                    "       uniform(1, 1000, random()) c2,      "
                    "       uniform(1, 1000, random()) c3       "
                    "FROM                                       "
                    "       table(generator(100000));           ");
  fprintf(stdout, "prepare data done, start test ...\n");

  int64_t len1, len2, para1, para2, para3;
  int64_t result_y[2][10000];
  int64_t result_n[2][10000];
  for (int ntest = 0; ntest < 30; ++ntest) {
    len1 = len2 = 0;
    para1 = rand() % 300000;
    para2 = rand() % 1000;
    para3 = rand() % 1000;

    WRITE_SQL_BY_CONN(connection, "alter system set memtable_scan_filter_pushdown = 1;");
    sleep(10);
    READ_SQL_FMT_BY_CONN(connection, "select c1, c2 from memtable_scan_with_filter where c1 > %ld and c1 < %ld and "
                                     "c2 > %ld and c3 < %ld", para1, para1 + 10000, para2, para3);

    result = read_res.get_result();
    ASSERT_NE(nullptr, result);

    while (OB_SUCC(result->next())) {
      ASSERT_EQ(OB_SUCCESS, result->get_int("c1", result_y[0][len1]));
      ASSERT_EQ(OB_SUCCESS, result->get_int("c2", result_y[1][len1]));
      ++len1;
    }
    ASSERT_EQ(OB_ITER_END, ret);

    WRITE_SQL_BY_CONN(connection, "alter system set memtable_scan_filter_pushdown = 0;");
    sleep(10);
    READ_SQL_FMT_BY_CONN(connection, "select c1, c2 from memtable_scan_with_filter where c1 > %ld and c1 < %ld and "
                                     "c2 > %ld and c3 < %ld", para1, para1 + 10000, para2, para3);

    result = read_res.get_result();
    ASSERT_NE(nullptr, result);

    while (OB_SUCC(result->next())) {
      ASSERT_EQ(OB_SUCCESS, result->get_int("c1", result_n[0][len2]));
      ASSERT_EQ(OB_SUCCESS, result->get_int("c2", result_n[1][len2]));
      ++len2;
    }
    ASSERT_EQ(OB_ITER_END, ret);

    fprintf(stdout, "test %d with parameter %ld, %ld, %ld, get %ld, %ld result rows. %ld, %ld, %ld, %ld, %ld\n", ntest, para1, para2, para3, len1, len2,
                    result_y[0][len1 - 3], result_y[0][len1 - 2], result_y[0][len1 - 1], result_n[0][len2 - 2], result_n[0][len2 - 1]);
    
    ASSERT_EQ(len1, len2);

    for (int64_t i = 0; i < len1; ++i) {
      ASSERT_EQ(result_y[0][i], result_n[0][i]);
      ASSERT_EQ(result_y[1][i], result_n[1][i]);
    }
  }

  WRITE_SQL_BY_CONN(connection, "alter system set memtable_scan_filter_pushdown = 1;");
}

// memtable scan reach iter end without reset blockscan, which is shared among
// different iterators, later sstable start blockscan and get wrong info.
// TEST_F(ObTestMemtableScan, bugfix_test)
// {
//   DEF_VAL_FOR_SQL;
//   ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().get_sql_proxy2().acquire(connection));

//   // 调大超时时间
//   WRITE_SQL_BY_CONN(connection, "SET SESSION OB_QUERY_TIMEOUT = 1000 * 1000 * 1000 * 1000;");
//   WRITE_SQL_BY_CONN(connection, "SET SESSION OB_TRX_TIMEOUT = 1000 * 1000 * 1000 * 1000;");
//   WRITE_SQL_BY_CONN(connection, "SET SESSION OB_TRX_IDLE_TIMEOUT = 1000 * 1000 * 1000 * 1000;");

//   // 建表
//   WRITE_SQL_BY_CONN(
//       connection,
//       "CREATE TABLE test.bugfix_test (c1 BIGINT PRIMARY KEY, c2 BIGINT, c3 BIGINT);")
//   WRITE_SQL_BY_CONN(connection, "CREATE SEQUENCE s1 START WITH 0 MINVALUE 0 MAXVALUE 200000 INCREMENT BY 1 CACHE 200000 ORDER;");
//   WRITE_SQL_BY_CONN(connection, "CREATE SEQUENCE s2 START WITH 20000 MINVALUE 0 MAXVALUE 200000 INCREMENT BY 1 CACHE 200000 ORDER;");
//   WRITE_SQL_BY_CONN(connection, "CREATE SEQUENCE s3 START WITH 40000 MINVALUE 0 MAXVALUE 200000 INCREMENT BY 1 CACHE 200000 ORDER;");

//   WRITE_SQL_BY_CONN(connection,
//                 "INSERT INTO                                "
//                 "       test.bugfix_test                    "
//                 "SELECT                                     "
//                 "       s1.nextval c1,                      "
//                 "       random() % 10000 c2,                "
//                 "       uniform(1, 1000, random()) c3       "
//                 "FROM                                       "
//                 "       table(generator(10000));            ");
//   fprintf(stdout, "do minor freeze once. freeze count : 0\n");
//   minor_freeze_data_and_wait();

//   WRITE_SQL_BY_CONN(connection,
//                 "INSERT INTO                                "
//                 "       test.bugfix_test                    "
//                 "SELECT                                     "
//                 "       s3.nextval c1,                      "
//                 "       random() % 10000 c2,                "
//                 "       uniform(1, 1000, random()) c3       "
//                 "FROM                                       "
//                 "       table(generator(10000));            ");
//   fprintf(stdout, "do minor freeze once. freeze count : 1\n");
//   minor_freeze_data_and_wait();

//   WRITE_SQL_BY_CONN(connection,
//                   "INSERT INTO                                "
//                   "       test.bugfix_test                    "
//                   "SELECT                                     "
//                   "       s2.nextval c1,                      "
//                   "       random() % 10000 c2,                "
//                   "       zipf(1, 100, random(3)) c3          "
//                   "FROM                                       "
//                   "       table(generator(10000));            ");

//   WRITE_SQL_BY_CONN(connection, "alter system set memtable_scan_filter_pushdown = 1;");
//   fprintf(stdout, "prepare data done, start test ...\n");

//   READ_SQL_FMT_BY_CONN(connection, "select c1, c2 from test.bugfix_test where c1 < 50000 and c3 > 300;");

//   result = read_res.get_result();
//   ASSERT_NE(nullptr, result);

//   int64_t c1, len = 0;
//   while (OB_SUCC(result->next())) {
//     ASSERT_EQ(OB_SUCCESS, result->get_int("c1", c1));
//     ++len;
//   }
//   ASSERT_EQ(OB_ITER_END, ret);
// }

// The following tests are performance test. Better use observer cluster for performance.
// Simple server may be unstable, and the C++ code may introduce some extra unknown cost.

// TEST_F(ObTestMemtableScan, memtable_scan_time_cost_prepare_data)
// {
//   DEF_VAL_FOR_SQL;
//   common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
//   ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(connection));
//   // 调大超时时间
//   WRITE_SQL_BY_CONN(connection, "SET SESSION OB_QUERY_TIMEOUT = 1000 * 1000 * 1000 * 1000;");
//   WRITE_SQL_BY_CONN(connection, "SET SESSION OB_TRX_TIMEOUT = 1000 * 1000 * 1000 * 1000;");
//   WRITE_SQL_BY_CONN(connection, "SET SESSION OB_TRX_IDLE_TIMEOUT = 1000 * 1000 * 1000 * 1000;");
//   sleep(5);

//   // 建表
//   WRITE_SQL_BY_CONN(
//       connection,
//       "CREATE TABLE test.multiple_scan_merge5 (c1 BIGINT PRIMARY KEY, c2 BIGINT, c3 BIGINT, c4 BIGINT, c5 BIGINT, c6 BIGINT, c7 BIGINT);")
//   WRITE_SQL_BY_CONN(connection, "CREATE SEQUENCE m5s0 START WITH 0 MINVALUE 0 MAXVALUE 100000000 INCREMENT BY 300 CACHE 10000000 ORDER;");
//   WRITE_SQL_BY_CONN(connection, "CREATE SEQUENCE m5s1 START WITH 1 MINVALUE 0 MAXVALUE 100000000 INCREMENT BY 300 CACHE 10000000 ORDER;");
//   WRITE_SQL_BY_CONN(connection, "CREATE SEQUENCE m5s2 START WITH 2 MINVALUE 0 MAXVALUE 100000000 INCREMENT BY 300 CACHE 10000000 ORDER;");
//   WRITE_SQL_BY_CONN(connection, "CREATE SEQUENCE m5s3 START WITH 3 MINVALUE 0 MAXVALUE 100000000 INCREMENT BY 300 CACHE 10000000 ORDER;");
//   WRITE_SQL_BY_CONN(connection, "CREATE SEQUENCE m5s4 START WITH 0 MINVALUE 0 MAXVALUE 100000000 INCREMENT BY 1 CACHE 10000000 ORDER;");

//   // 循环 4 次，每次写 1000 行，产生 4 个 mini sstable
//   char sql_str[1024] = "";
//   for (int64_t i = 0; i < 4; i++) {
//     WRITE_SQL_FMT_BY_CONN(connection,
//                           "INSERT INTO                                "
//                           "       test.multiple_scan_merge5           "
//                           "SELECT                                     "
//                           "       m5s%ld.nextval c1,                  "
//                           "       uniform(1, 1000, random()) c2,      "
//                           "       uniform(1, 1000, random()) c3,      "
//                           "       uniform(1, 1000, random()) c4,      "
//                           "       uniform(1, 1000, random()) c5,      "
//                           "       uniform(1, 1000, random()) c6,      "
//                           "       uniform(1, 1000, random()) c7       "
//                           "FROM                                       "
//                           "       table(generator(1000));             ", i);
//     fprintf(stdout, "do minor freeze once. freeze count : %ld\n", i+1);
//     minor_freeze_data_and_wait();
//   }

//   // 写 100000 行数据到 memtable
//   WRITE_SQL_BY_CONN(connection,
//                     "INSERT IGNORE INTO                         "
//                     "       test.multiple_scan_merge5           "
//                     "SELECT                                     "
//                     "       m5s4.nextval c1,                    "
//                     "       uniform(1, 1000, random()) c2,      "
//                     "       uniform(1, 1000, random()) c3,      "
//                     "       uniform(1, 1000, random()) c4,      "
//                     "       uniform(1, 1000, random()) c5,      "
//                     "       uniform(1, 1000, random()) c6,      "
//                     "       uniform(1, 1000, random()) c7       "
//                     "FROM                                       "
//                     "       table(generator(100000));           ");

//   fprintf(stdout, "prepare data done, start time cost test ...\n");
//   WRITE_SQL_BY_CONN(connection, "alter system set memtable_scan_filter_pushdown = 1;");
// }

// TEST_F(ObTestMemtableScan, memtable_scan_time_cost_with_no_filter)
// {
//   DEF_VAL_FOR_SQL;
//   common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
//   ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(connection));

//   clock_t start, end;
//   int64_t res_len, c1;

//   // TODO(jianxian): outter loop for differetn parameters.
//   WRITE_SQL_BY_CONN(connection, "alter system set memtable_scan_filter_pushdown = 1;");
//   sleep(5);
//   for (int i = 0; i < 5; ++i) {
//     res_len = 0;
//     start = clock();
//     READ_SQL_FMT_BY_CONN(connection, "select c1, c2, c6 from test.multiple_scan_merge5 where "
//                                      "(c1 < 100000);");
//     result = read_res.get_result();
//     ASSERT_NE(nullptr, result);

//     while (OB_SUCC(result->next())) {
//       ASSERT_EQ(OB_SUCCESS, result->get_int("c1", c1));
//       ++res_len;
//     }

//     ASSERT_EQ(OB_ITER_END, ret);
//     end = clock();
//     fprintf(stdout, "get %ld result rows with blockscan, use time %f\n", res_len, (double)(end - start) / CLOCKS_PER_SEC);
//   }

//   WRITE_SQL_BY_CONN(connection, "alter system set memtable_scan_filter_pushdown = 0;");
//   sleep(5);
//   for (int i = 0; i < 5; ++i) {
//     res_len = 0;
//     start = clock();
//     READ_SQL_FMT_BY_CONN(connection, "select c1, c2, c6 from test.multiple_scan_merge5 where"
//                                      "(c1 < 100000);");
//     result = read_res.get_result();
//     ASSERT_NE(nullptr, result);

//     while (OB_SUCC(result->next())) {
//       ASSERT_EQ(OB_SUCCESS, result->get_int("c1", c1));
//       ++res_len;
//     }

//     ASSERT_EQ(OB_ITER_END, ret);
//     end = clock();
//     fprintf(stdout, "get %ld result rows without blockscan, use time %f\n", res_len, (double)(end - start) / CLOCKS_PER_SEC);
//   }

//   WRITE_SQL_BY_CONN(connection, "alter system set memtable_scan_filter_pushdown = 1;");
// }

// TEST_F(ObTestMemtableScan, memtable_scan_time_cost_with_simple_filter)
// {
//   DEF_VAL_FOR_SQL;
//   common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
//   ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(connection));

//   clock_t start, end;
//   int64_t res_len, c1;

//   // TODO(jianxian): outter loop for differetn parameters.
//   WRITE_SQL_BY_CONN(connection, "alter system set memtable_scan_filter_pushdown = 1;");
//   sleep(5);
//   for (int i = 0; i < 5; ++i) {
//     res_len = 0;
//     start = clock();
//     READ_SQL_FMT_BY_CONN(connection, "select c1, c2, c6 from test.multiple_scan_merge5 where "
//                                      "(c1 < 100000) and (c2 < 700);");
//     result = read_res.get_result();
//     ASSERT_NE(nullptr, result);

//     while (OB_SUCC(result->next())) {
//       ASSERT_EQ(OB_SUCCESS, result->get_int("c1", c1));
//       ++res_len;
//     }

//     ASSERT_EQ(OB_ITER_END, ret);
//     end = clock();
//     fprintf(stdout, "get %ld result rows with blockscan, use time %f\n", res_len, (double)(end - start) / CLOCKS_PER_SEC);
//   }

//   WRITE_SQL_BY_CONN(connection, "alter system set memtable_scan_filter_pushdown = 0;");
//   sleep(5);
//   for (int i = 0; i < 5; ++i) {
//     res_len = 0;
//     start = clock();
//     READ_SQL_FMT_BY_CONN(connection, "select c1, c2, c6 from test.multiple_scan_merge5 where "
//                                      "(c1 < 100000) and (c2 < 700);");
//     result = read_res.get_result();
//     ASSERT_NE(nullptr, result);

//     while (OB_SUCC(result->next())) {
//       ASSERT_EQ(OB_SUCCESS, result->get_int("c1", c1));
//       ++res_len;
//     }

//     ASSERT_EQ(OB_ITER_END, ret);
//     end = clock();
//     fprintf(stdout, "get %ld result rows without blockscan, use time %f\n", res_len, (double)(end - start) / CLOCKS_PER_SEC);
//   }

//   WRITE_SQL_BY_CONN(connection, "alter system set memtable_scan_filter_pushdown = 1;");
// }

// TEST_F(ObTestMemtableScan, memtable_scan_time_cost_with_complex_filter)
// {
//   DEF_VAL_FOR_SQL;
//   common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
//   ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(connection));

//   clock_t start, end;
//   int64_t res_len, c1;

//   // TODO(jianxian): outter loop for differetn parameters.
//   WRITE_SQL_BY_CONN(connection, "alter system set memtable_scan_filter_pushdown = 1;");
//   sleep(5);
//   for (int i = 0; i < 5; ++i) {
//     res_len = 0;
//     start = clock();
//     READ_SQL_FMT_BY_CONN(connection, "select c1, c2, c6 from test.multiple_scan_merge5 where "
//                                      "(c1 < 100000) and (c2 < 700) and (c3 > 300) and (c4 * 2 < 1200) and (c5 + 100 > 200);");
//     result = read_res.get_result();
//     ASSERT_NE(nullptr, result);

//     while (OB_SUCC(result->next())) {
//       ASSERT_EQ(OB_SUCCESS, result->get_int("c1", c1));
//       ++res_len;
//     }

//     ASSERT_EQ(OB_ITER_END, ret);
//     end = clock();
//     fprintf(stdout, "get %ld result rows with blockscan, use time %f\n", res_len, (double)(end - start) / CLOCKS_PER_SEC);
//   }

//   WRITE_SQL_BY_CONN(connection, "alter system set memtable_scan_filter_pushdown = 0;");
//   sleep(5);
//   for (int i = 0; i < 5; ++i) {
//     res_len = 0;
//     start = clock();
//     READ_SQL_FMT_BY_CONN(connection, "select c1, c2, c6 from test.multiple_scan_merge5 where "
//                                      "(c1 < 100000) and (c2 < 700) and (c3 > 300) and (c4 * 2 < 1200) and (c5 + 100 > 200);");
//     result = read_res.get_result();
//     ASSERT_NE(nullptr, result);

//     while (OB_SUCC(result->next())) {
//       ASSERT_EQ(OB_SUCCESS, result->get_int("c1", c1));
//       ++res_len;
//     }

//     ASSERT_EQ(OB_ITER_END, ret);
//     end = clock();
//     fprintf(stdout, "get %ld result rows without blockscan, use time %f\n", res_len, (double)(end - start) / CLOCKS_PER_SEC);
//   }

//   WRITE_SQL_BY_CONN(connection, "alter system set memtable_scan_filter_pushdown = 1;");
// }

// TEST_F(ObTestMemtableScan, memtable_scan_time_cost_worst_case)
// {
//   DEF_VAL_FOR_SQL;
//   common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
//   ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(connection));
//   // 调大超时时间
//   WRITE_SQL_BY_CONN(connection, "SET SESSION OB_QUERY_TIMEOUT = 1000 * 1000 * 1000 * 1000;");
//   WRITE_SQL_BY_CONN(connection, "SET SESSION OB_TRX_TIMEOUT = 1000 * 1000 * 1000 * 1000;");
//   WRITE_SQL_BY_CONN(connection, "SET SESSION OB_TRX_IDLE_TIMEOUT = 1000 * 1000 * 1000 * 1000;");
//   sleep(5);

//   // 建表
//   WRITE_SQL_BY_CONN(
//       connection,
//       "CREATE TABLE test.multiple_scan_merge3 (c1 BIGINT PRIMARY KEY, c2 BIGINT, c3 BIGINT, c4 BIGINT, c5 BIGINT, c6 VARCHAR(10));")
//   WRITE_SQL_BY_CONN(connection, "CREATE SEQUENCE wcs0 START WITH 0 MINVALUE 0 MAXVALUE 100000000 INCREMENT BY 5 CACHE 10000000 ORDER;");
//   WRITE_SQL_BY_CONN(connection, "CREATE SEQUENCE wcs1 START WITH 1 MINVALUE 0 MAXVALUE 100000000 INCREMENT BY 5 CACHE 10000000 ORDER;");
//   WRITE_SQL_BY_CONN(connection, "CREATE SEQUENCE wcs2 START WITH 2 MINVALUE 0 MAXVALUE 100000000 INCREMENT BY 5 CACHE 10000000 ORDER;");
//   WRITE_SQL_BY_CONN(connection, "CREATE SEQUENCE wcs3 START WITH 3 MINVALUE 0 MAXVALUE 100000000 INCREMENT BY 5 CACHE 10000000 ORDER;");
//   WRITE_SQL_BY_CONN(connection, "CREATE SEQUENCE wcs4 START WITH 4 MINVALUE 0 MAXVALUE 100000000 INCREMENT BY 5 CACHE 10000000 ORDER;");

//   // 循环 4 次，每次写 10000 行，产生 4 个 mini sstable
//   char sql_str[1024] = "";
//   for (int64_t i = 0; i < 4; i++) {
//     WRITE_SQL_FMT_BY_CONN(connection,
//                           "INSERT INTO                                "
//                           "       test.multiple_scan_merge3           "
//                           "SELECT                                     "
//                           "       wcs%ld.nextval c1,                  "
//                           "       random() %% 10000 c2,               "
//                           "       zipf(1, 100, random(3)) c3,         "
//                           "       uniform(1, 100, random()) c4,       "
//                           "       uniform(1, 100, random()) c5,       "
//                           "       randstr(10, random()) c6            "
//                           "FROM                                       "
//                           "       table(generator(10000));            ", i);
//     fprintf(stdout, "do minor freeze once. freeze count : %ld\n", i+1);
//     minor_freeze_data_and_wait();
//   }

//   // 写 10000 行数据到 memtable
//   WRITE_SQL_BY_CONN(connection,
//                     "INSERT INTO                         "
//                     "       test.multiple_scan_merge3           "
//                     "SELECT                                     "
//                     "       wcs4.nextval c1,                    "
//                     "       uniform(1, 1000, random()) c2,      "
//                     "       uniform(1, 1000, random()) c3,      "
//                     "       uniform(1, 1000, random()) c4,      "
//                     "       uniform(1, 1000, random()) c5,      "
//                     "       randstr(10, random()) c6            "
//                     "FROM                                       "
//                     "       table(generator(10000));           ");

//   fprintf(stdout, "prepare data done, start time cost test ...\n");

//   clock_t start, end;
//   int64_t res_len, c1;

//   WRITE_SQL_BY_CONN(connection, "alter system set memtable_scan_filter_pushdown = 1;");
//   sleep(5);
//   for (int i = 0; i < 5; ++i) {
//     res_len = 0;
//     start = clock();
//     READ_SQL_FMT_BY_CONN(connection, "select c1, c2, c6 from test.multiple_scan_merge3 where "
//                                      "(c1 < 100000) and (c2 < 700);");
//     result = read_res.get_result();
//     ASSERT_NE(nullptr, result);

//     while (OB_SUCC(result->next())) {
//       ASSERT_EQ(OB_SUCCESS, result->get_int("c1", c1));
//       ++res_len;
//     }

//     ASSERT_EQ(OB_ITER_END, ret);
//     end = clock();
//     fprintf(stdout, "get %ld result rows with blockscan, use time %f\n", res_len, (double)(end - start) / CLOCKS_PER_SEC);
//   }

//   WRITE_SQL_BY_CONN(connection, "alter system set memtable_scan_filter_pushdown = 0;");
//   sleep(5);
//   for (int i = 0; i < 5; ++i) {
//     res_len = 0;
//     start = clock();
//     READ_SQL_FMT_BY_CONN(connection, "select c1, c2, c6 from test.multiple_scan_merge3 where "
//                                      "(c1 < 100000) and (c2 < 700);");
//     result = read_res.get_result();
//     ASSERT_NE(nullptr, result);

//     while (OB_SUCC(result->next())) {
//       ASSERT_EQ(OB_SUCCESS, result->get_int("c1", c1));
//       ++res_len;
//     }

//     ASSERT_EQ(OB_ITER_END, ret);
//     end = clock();
//     fprintf(stdout, "get %ld result rows without blockscan, use time %f\n", res_len, (double)(end - start) / CLOCKS_PER_SEC);
//   }
// }

TEST_F(ObTestMemtableScan, end)
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
  srand((unsigned)time(NULL));
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