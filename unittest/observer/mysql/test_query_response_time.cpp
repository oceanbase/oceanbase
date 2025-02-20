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
#include "observer/mysql/ob_query_response_time.h"
#include "sql/resolver/ob_stmt_type.h"

using namespace oceanbase::common;
using namespace oceanbase::observer;

class TestQueryRsponseTime: public ::testing::Test
{
public:
  TestQueryRsponseTime();
  virtual ~TestQueryRsponseTime();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestQueryRsponseTime);
protected:
  // function members
};

TestQueryRsponseTime::TestQueryRsponseTime()
{
}

TestQueryRsponseTime::~TestQueryRsponseTime()
{
}

void TestQueryRsponseTime::SetUp()
{
}

void TestQueryRsponseTime::TearDown()
{
}

TEST_F(TestQueryRsponseTime, basic_test){
    ObRespTimeInfoCollector time_collector;
    ASSERT_EQ(OB_SUCCESS, time_collector.setup(10));
    ASSERT_EQ(100,time_collector.utility().bound(2));
    ASSERT_EQ(13,time_collector.utility().bound_count());
    ASSERT_EQ(OB_SUCCESS, time_collector.collect(oceanbase::sql::stmt::StmtType::T_SELECT, false/*is_inner_sql*/, 5));
    ASSERT_EQ(OB_SUCCESS, time_collector.collect(oceanbase::sql::stmt::StmtType::T_SELECT, false/*is_inner_sql*/, 50));

    int64_t val = -1;
    ASSERT_EQ(OB_SUCCESS, time_collector.get_count_val(RespTimeSqlType::select_sql, 1, val));
    ASSERT_EQ(1, val);

    ASSERT_EQ(OB_SUCCESS, time_collector.get_total_time_val(RespTimeSqlType::select_sql, 1, val));
    ASSERT_EQ(5, val);

    ASSERT_EQ(OB_SUCCESS, time_collector.get_count_val(RespTimeSqlType::select_sql, 2, val));
    ASSERT_EQ(1, val);

    ASSERT_EQ(OB_SUCCESS, time_collector.get_total_time_val(RespTimeSqlType::select_sql, 2, val));
    ASSERT_EQ(50, val);
    ASSERT_EQ(OB_SUCCESS, time_collector.flush());

    ASSERT_EQ(OB_SUCCESS, time_collector.get_count_val(RespTimeSqlType::select_sql, 1, val));
    ASSERT_EQ(0, val);

    ASSERT_EQ(OB_SUCCESS, time_collector.get_total_time_val(RespTimeSqlType::select_sql, 1, val));
    ASSERT_EQ(0, val);

    ASSERT_EQ(OB_SUCCESS, time_collector.get_count_val(RespTimeSqlType::select_sql, 2, val));
    ASSERT_EQ(0, val);
    ASSERT_EQ(0,time_collector.get_total_time_val(RespTimeSqlType::select_sql, 2, val));

    ASSERT_EQ(OB_SUCCESS, time_collector.setup(100));
    ASSERT_EQ(100,time_collector.utility().bound(1));
    ASSERT_EQ(7,time_collector.utility().bound_count());
    ASSERT_EQ(OB_SUCCESS, time_collector.collect(oceanbase::sql::stmt::StmtType::T_SELECT, false/*is_inner_sql*/, 5));
    ASSERT_EQ(OB_SUCCESS, time_collector.collect(oceanbase::sql::stmt::StmtType::T_SELECT, false/*is_inner_sql*/, 50));

    ASSERT_EQ(OB_SUCCESS, time_collector.get_count_val(RespTimeSqlType::select_sql, 1, val));
    ASSERT_EQ(2, val);

    ASSERT_EQ(OB_SUCCESS, time_collector.get_total_time_val(RespTimeSqlType::select_sql, 1, val));
    ASSERT_EQ(55, val);
    ASSERT_EQ(OB_SUCCESS, time_collector.get_count_val(RespTimeSqlType::select_sql, 2, val));
    ASSERT_EQ(0, val);
    ASSERT_EQ(OB_SUCCESS, time_collector.get_total_time_val(RespTimeSqlType::select_sql, 2, val));
    ASSERT_EQ(0, val);
}

int main(int argc, char** argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_ob_query_response_time.log", true);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
