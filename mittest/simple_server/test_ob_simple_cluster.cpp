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


class TestRunCtx
{
public:
  uint64_t tenant_id_ = 0;
  int time_sec_ = 0;
};

TestRunCtx RunCtx;

class ObSimpleClusterExampleTest : public ObSimpleClusterTestBase
{
public:
  // 指定case运行目录前缀 test_ob_simple_cluster_
  ObSimpleClusterExampleTest() : ObSimpleClusterTestBase("test_ob_simple_cluster_") {}
};

TEST_F(ObSimpleClusterExampleTest, observer_start)
{
  SERVER_LOG(INFO, "observer_start succ");
}

// 创建租户并不轻量，看场景必要性使用
TEST_F(ObSimpleClusterExampleTest, add_tenant)
{
  // 创建普通租户tt1
  ASSERT_EQ(OB_SUCCESS, create_tenant());
  // 获取租户tt1的tenant_id
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(RunCtx.tenant_id_));
  ASSERT_NE(0, RunCtx.tenant_id_);
  // 初始化普通租户tt1的sql proxy
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
}

/*
TEST_F(ObSimpleClusterExampleTest, create_table)
{
  int ret = OB_SUCCESS;
  // 使用普通租户tt1
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  // 创建表
  {
    OB_LOG(INFO, "create_table start");
    ObSqlString sql;
    sql.assign_fmt(
      "create table school (sid int,sname varchar(100), primary key(sid)) "
      "partition by range(sid) (partition p0 values less than (100), partition p1 values less than (200), partition p2 values less than MAXVALUE)");
    int64_t affected_rows = 0;
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
    OB_LOG(INFO, "create_table succ");
  }

  {
    OB_LOG(INFO, "insert data start");
    for (int i = 1;i <= 1000; i++) {
      ObSqlString sql;
      ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("insert into school values(%d, '%s')", i, "ob"));
      int64_t affected_rows = 0;
      ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
    }
    //check row count
    OB_LOG(INFO, "check row count");
    {
      int64_t row_cnt = 0;
      ObSqlString sql;
      ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select count(*) row_cnt from school"));
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
        sqlclient::ObMySQLResult *result = res.get_result();
        ASSERT_NE(nullptr, result);
        ASSERT_EQ(OB_SUCCESS, result->next());
        ASSERT_EQ(OB_SUCCESS, result->get_int("row_cnt", row_cnt));
      }
      ASSERT_EQ(row_cnt, 1000);
    }

  }
}
*/
/*
TEST_F(ObSimpleClusterExampleTest, delete_tenant)
{
  ASSERT_EQ(OB_SUCCESS, delete_tenant());
}
*/

TEST_F(ObSimpleClusterExampleTest, end)
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
