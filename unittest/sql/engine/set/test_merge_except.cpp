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
#define private public
#define protected public
#include "sql/engine/set/ob_set_test_util.h"
#include "sql/ob_sql_init.h"
#include "sql/engine/set/ob_merge_except.h"
#include "sql/engine/table/ob_fake_table.h"
#include "observer/ob_server.h"
#include "observer/ob_server_struct.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/ob_tenant_mgr.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::observer;

class TestMergeExcept : public ObMergeExcept
{
public:
  TestMergeExcept() :ObMergeExcept(alloc_) {}
  ~TestMergeExcept() {}
};

class TestMergeExceptTest: public ::testing::Test
{
public:
  TestMergeExceptTest();
  virtual ~TestMergeExceptTest();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  TestMergeExceptTest(const TestMergeExceptTest &other);
  TestMergeExceptTest& operator=(const TestMergeExceptTest &other);
private:
  // data members
};
TestMergeExceptTest::TestMergeExceptTest()
{
}

TestMergeExceptTest::~TestMergeExceptTest()
{
}

void TestMergeExceptTest::SetUp()
{
}

void TestMergeExceptTest::TearDown()
{
}

TEST_F(TestMergeExceptTest, test_all_get_next_row1)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ObSQLSessionInfo my_session;
  ASSERT_EQ(OB_SUCCESS, my_session.test_init(0,0,0,NULL));
  ctx.set_my_session(&my_session);
  ObFakeTable &fake_table1 = TestSetOperatorFactory::get_fake_table1();
  ObFakeTable &fake_table2 = TestSetOperatorFactory::get_fake_table2();
  ObFakeTable &result_table = TestSetOperatorFactory::get_result_table();
  TestMergeExcept merge_except;
  ObCollationType agg_cs_type = CS_TYPE_UTF8MB4_BIN;

  bool set_direction = false;
  TestSetOperatorFactory::init(ctx, &merge_except, 5, false, set_direction);
  //prepare rows, equal in the first row
  ADD_ROW(fake_table1, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table1, COL(2), COL(3), COL(null), COL("yuming"), COL("oceanbase"));
  ADD_ROW(fake_table2, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table2, COL(2), COL(2), COL(4.0), COL("oceanbase"), COL(null));
  ADD_ROW(result_table, COL(2), COL(3), COL(null), COL("yuming"), COL("oceanbase"));

  ASSERT_EQ(OB_SUCCESS, merge_except.open(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.open(ctx));
  EXCEPT_RESULT_SET(ctx, result_table, merge_except, 0, 5, agg_cs_type);
  ASSERT_EQ(OB_SUCCESS, merge_except.close(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.close(ctx));
}

TEST_F(TestMergeExceptTest, test_all_get_next_row2)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ObSQLSessionInfo my_session;
  ASSERT_EQ(OB_SUCCESS, my_session.test_init(0,0,0,NULL));
  ctx.set_my_session(&my_session);
  ObFakeTable &fake_table1 = TestSetOperatorFactory::get_fake_table1();
  ObFakeTable &fake_table2 = TestSetOperatorFactory::get_fake_table2();
  ObFakeTable &result_table = TestSetOperatorFactory::get_result_table();
  TestMergeExcept merge_except;
  ObCollationType agg_cs_type = CS_TYPE_UTF8MB4_BIN;


  bool set_direction = false;
  TestSetOperatorFactory::init(ctx, &merge_except, 5, false, set_direction);
  //prepare rows, equal in the first row
  ADD_ROW(fake_table1, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table1, COL(2), COL(3), COL(null), COL("yuming"), COL("oceanbase"));
  ADD_ROW(fake_table1, COL(3), COL(4), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table1, COL(4), COL(2), COL(4.0), COL("oceanbase"), COL(null));
  ADD_ROW(fake_table2, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table2, COL(2), COL(2), COL(4.0), COL("oceanbase"), COL(null));
  ADD_ROW(fake_table2, COL(3), COL(4), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(result_table, COL(2), COL(3), COL(null), COL("yuming"), COL("oceanbase"));
  ADD_ROW(result_table, COL(4), COL(2), COL(4.0), COL("oceanbase"), COL(null));

  ASSERT_EQ(OB_SUCCESS, merge_except.open(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.open(ctx));
  EXCEPT_RESULT_SET(ctx, result_table, merge_except, 0, 5, agg_cs_type);
  ASSERT_EQ(OB_SUCCESS, merge_except.close(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.close(ctx));
}

TEST_F(TestMergeExceptTest, test_all_get_next_row3)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ObSQLSessionInfo my_session;
  ASSERT_EQ(OB_SUCCESS, my_session.test_init(0,0,0,NULL));
  ctx.set_my_session(&my_session);
  ObFakeTable &fake_table1 = TestSetOperatorFactory::get_fake_table1();
  ObFakeTable &result_table = TestSetOperatorFactory::get_result_table();
  TestMergeExcept merge_except;
  ObCollationType agg_cs_type = CS_TYPE_UTF8MB4_BIN;


  bool set_direction = false;
  TestSetOperatorFactory::init(ctx, &merge_except, 5, false, set_direction);
  //prepare rows, equal in the first row
  ADD_ROW(fake_table1, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table1, COL(2), COL(3), COL(null), COL("yuming"), COL("oceanbase"));
  ADD_ROW(result_table, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(result_table, COL(2), COL(3), COL(null), COL("yuming"), COL("oceanbase"));

  ASSERT_EQ(OB_SUCCESS, merge_except.open(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.open(ctx));
  EXCEPT_RESULT_SET(ctx, result_table, merge_except, 0, 5, agg_cs_type);
  ASSERT_EQ(OB_SUCCESS, merge_except.close(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.close(ctx));
}

TEST_F(TestMergeExceptTest, test_all_get_next_row4)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ObSQLSessionInfo my_session;
  ASSERT_EQ(OB_SUCCESS, my_session.test_init(0,0,0,NULL));
  ctx.set_my_session(&my_session);
  ObFakeTable &fake_table1 = TestSetOperatorFactory::get_fake_table1();
  ObFakeTable &fake_table2 = TestSetOperatorFactory::get_fake_table2();
  ObFakeTable &result_table = TestSetOperatorFactory::get_result_table();
  TestMergeExcept merge_except;
  ObCollationType agg_cs_type = CS_TYPE_UTF8MB4_BIN;

  bool set_direction = false;
  TestSetOperatorFactory::init(ctx, &merge_except, 5, false, set_direction);
  //prepare rows, equal in the first row
  ADD_ROW(fake_table1, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table1, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table1, COL(2), COL(3), COL(null), COL("yuming"), COL("oceanbase"));
  ADD_ROW(fake_table1, COL(3), COL(4), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table1, COL(4), COL(2), COL(4.0), COL("oceanbase"), COL(null));
  ADD_ROW(fake_table2, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table2, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table2, COL(2), COL(2), COL(4.0), COL("oceanbase"), COL(null));
  ADD_ROW(fake_table2, COL(3), COL(4), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(result_table, COL(2), COL(3), COL(null), COL("yuming"), COL("oceanbase"));
  ADD_ROW(result_table, COL(4), COL(2), COL(4.0), COL("oceanbase"), COL(null));

  ASSERT_EQ(OB_SUCCESS, merge_except.open(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.open(ctx));
  EXCEPT_RESULT_SET(ctx, result_table, merge_except, 0, 5, agg_cs_type);
  ASSERT_EQ(OB_SUCCESS, merge_except.close(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.close(ctx));
}

TEST_F(TestMergeExceptTest, test_distinct_get_next_row1)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ObSQLSessionInfo my_session;
  ASSERT_EQ(OB_SUCCESS, my_session.test_init(0,0,0,NULL));
  ctx.set_my_session(&my_session);
  ObFakeTable &fake_table1 = TestSetOperatorFactory::get_fake_table1();
  ObFakeTable &fake_table2 = TestSetOperatorFactory::get_fake_table2();
  ObFakeTable &result_table = TestSetOperatorFactory::get_result_table();
  TestMergeExcept merge_except;
  ObCollationType agg_cs_type = CS_TYPE_UTF8MB4_BIN;


  bool set_direction = false;
  TestSetOperatorFactory::init(ctx, &merge_except, 5, true, set_direction);
  //prepare rows, equal in the first row
  ADD_ROW(fake_table1, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table1, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table1, COL(2), COL(3), COL(null), COL("yuming"), COL("oceanbase"));
  ADD_ROW(fake_table2, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table2, COL(2), COL(2), COL(4.0), COL("oceanbase"), COL(null));
  ADD_ROW(fake_table2, COL(2), COL(2), COL(4.0), COL("oceanbase"), COL(null));
  ADD_ROW(result_table, COL(2), COL(3), COL(null), COL("yuming"), COL("oceanbase"));

  ASSERT_EQ(OB_SUCCESS, merge_except.open(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.open(ctx));
  EXCEPT_RESULT_SET(ctx, result_table, merge_except, 0, 5, agg_cs_type);
  ASSERT_EQ(OB_SUCCESS, merge_except.close(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.close(ctx));
}

TEST_F(TestMergeExceptTest, test_distinct_get_next_row2)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ObSQLSessionInfo my_session;
  ASSERT_EQ(OB_SUCCESS, my_session.test_init(0,0,0,NULL));
  ctx.set_my_session(&my_session);
  ObFakeTable &fake_table1 = TestSetOperatorFactory::get_fake_table1();
  ObFakeTable &fake_table2 = TestSetOperatorFactory::get_fake_table2();
  ObFakeTable &result_table = TestSetOperatorFactory::get_result_table();
  TestMergeExcept merge_except;
  ObCollationType agg_cs_type = CS_TYPE_UTF8MB4_BIN;

  bool set_direction = false;
  TestSetOperatorFactory::init(ctx, &merge_except, 5, true, set_direction);
  //prepare rows, equal in the first row
  ADD_ROW(fake_table1, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table1, COL(2), COL(3), COL(null), COL("yuming"), COL("oceanbase"));
  ADD_ROW(fake_table1, COL(2), COL(3), COL(null), COL("yuming"), COL("oceanbase"));
  ADD_ROW(fake_table1, COL(3), COL(4), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table1, COL(4), COL(2), COL(4.0), COL("oceanbase"), COL(null));
  ADD_ROW(fake_table1, COL(4), COL(2), COL(4.0), COL("oceanbase"), COL(null));
  ADD_ROW(fake_table2, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table2, COL(2), COL(2), COL(4.0), COL("oceanbase"), COL(null));
  ADD_ROW(fake_table2, COL(3), COL(4), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(result_table, COL(2), COL(3), COL(null), COL("yuming"), COL("oceanbase"));
  ADD_ROW(result_table, COL(4), COL(2), COL(4.0), COL("oceanbase"), COL(null));

  ASSERT_EQ(OB_SUCCESS, merge_except.open(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.open(ctx));
  EXCEPT_RESULT_SET(ctx, result_table, merge_except, 0, 5, agg_cs_type);
  ASSERT_EQ(OB_SUCCESS, merge_except.close(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.close(ctx));
}

TEST_F(TestMergeExceptTest, test_distinct_get_next_row3)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ObSQLSessionInfo my_session;
  ASSERT_EQ(OB_SUCCESS, my_session.test_init(0,0,0,NULL));
  ctx.set_my_session(&my_session);
  ObFakeTable &fake_table1 = TestSetOperatorFactory::get_fake_table1();
  ObFakeTable &result_table = TestSetOperatorFactory::get_result_table();
  TestMergeExcept merge_except;
  ObCollationType agg_cs_type = CS_TYPE_UTF8MB4_BIN;


  bool set_direction = false;
  TestSetOperatorFactory::init(ctx, &merge_except, 5, true, set_direction);
  //prepare rows, equal in the first row
  ADD_ROW(fake_table1, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table1, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(result_table, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));

  ASSERT_EQ(OB_SUCCESS, merge_except.open(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.open(ctx));
  EXCEPT_RESULT_SET(ctx, result_table, merge_except, 0, 5, agg_cs_type);
  ASSERT_EQ(OB_SUCCESS, merge_except.close(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.close(ctx));
}

TEST_F(TestMergeExceptTest, test_except_distinct_bug)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ObSQLSessionInfo my_session;
  ASSERT_EQ(OB_SUCCESS, my_session.test_init(0,0,0,NULL));
  ctx.set_my_session(&my_session);
  ObFakeTable &fake_table1 = TestSetOperatorFactory::get_fake_table1();
  ObFakeTable &fake_table2 = TestSetOperatorFactory::get_fake_table2();
  ObFakeTable &result_table = TestSetOperatorFactory::get_result_table();
  TestMergeExcept merge_except;
  ObCollationType agg_cs_type = CS_TYPE_UTF8MB4_BIN;

  bool set_direction = false;
  TestSetOperatorFactory::init(ctx, &merge_except, 1, true, set_direction);
  //prepare rows, equal in the first row
  ADD_ROW(fake_table1, COL(1));
  ADD_ROW(fake_table1, COL(2));
  ADD_ROW(fake_table1, COL(2));
  ADD_ROW(fake_table1, COL(3));
  ADD_ROW(fake_table1, COL(3));
  ADD_ROW(fake_table2, COL(1));
  ADD_ROW(fake_table2, COL(3));
  ADD_ROW(fake_table2, COL(3));
  ADD_ROW(result_table, COL(2));

  ASSERT_EQ(OB_SUCCESS, merge_except.open(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.open(ctx));
  EXCEPT_RESULT_SET(ctx, result_table, merge_except, 0, 1, agg_cs_type);
  ASSERT_EQ(OB_SUCCESS, merge_except.close(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.close(ctx));
}

TEST_F(TestMergeExceptTest, test_distinct_with_direction_get_next_row3)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ObSQLSessionInfo my_session;
  ASSERT_EQ(OB_SUCCESS, my_session.test_init(0,0,0,NULL));
  ctx.set_my_session(&my_session);
  ObFakeTable &fake_table1 = TestSetOperatorFactory::get_fake_table1();
  ObFakeTable &fake_table2 = TestSetOperatorFactory::get_fake_table2();
  ObFakeTable &result_table = TestSetOperatorFactory::get_result_table();
  TestMergeExcept merge_except;
  ObCollationType agg_cs_type = CS_TYPE_UTF8MB4_BIN;


  bool set_direction = true;
  TestSetOperatorFactory::init(ctx, &merge_except, 5, true, set_direction);
  //prepare rows, equal in the first row
  ADD_ROW(fake_table1, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table1, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table1, COL(1), COL(2), COL(1.0), COL("xuchen.wyc"), COL(null));
  ADD_ROW(fake_table1, COL(1), COL(2), COL(1.0), COL("xuchen.wyc"), COL(null));
  ADD_ROW(fake_table1, COL(2), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table1, COL(2), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table1, COL(2), COL(2), COL(1.0), COL("xuchen.wyc"), COL(null));
  ADD_ROW(fake_table1, COL(2), COL(2), COL(1.0), COL("xuchen.wyc"), COL(null));

  ADD_ROW(fake_table2, COL(2), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));

  ADD_ROW(result_table, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(result_table, COL(1), COL(2), COL(1.0), COL("xuchen.wyc"), COL(null));
  ADD_ROW(result_table, COL(2), COL(2), COL(1.0), COL("xuchen.wyc"), COL(null));

  ASSERT_EQ(OB_SUCCESS, merge_except.open(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.open(ctx));
  EXCEPT_RESULT_SET(ctx, result_table, merge_except, 0, 5, agg_cs_type);
  ASSERT_EQ(OB_SUCCESS, merge_except.close(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.close(ctx));
}

TEST_F(TestMergeExceptTest, test_all_with_direction_get_next_row3)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ObSQLSessionInfo my_session;
  ASSERT_EQ(OB_SUCCESS, my_session.test_init(0,0,0,NULL));
  ctx.set_my_session(&my_session);
  ObFakeTable &fake_table1 = TestSetOperatorFactory::get_fake_table1();
  ObFakeTable &fake_table2 = TestSetOperatorFactory::get_fake_table2();
  ObFakeTable &result_table = TestSetOperatorFactory::get_result_table();
  TestMergeExcept merge_except;
  ObCollationType agg_cs_type = CS_TYPE_UTF8MB4_BIN;


  bool set_direction = true;
  TestSetOperatorFactory::init(ctx, &merge_except, 5, false, set_direction);
  //prepare rows, equal in the first row
  ADD_ROW(fake_table1, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table1, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table1, COL(1), COL(2), COL(1.0), COL("xuchen.wyc"), COL(null));
  ADD_ROW(fake_table1, COL(1), COL(2), COL(1.0), COL("xuchen.wyc"), COL(null));
  ADD_ROW(fake_table1, COL(2), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table1, COL(2), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table1, COL(2), COL(2), COL(1.0), COL("xuchen.wyc"), COL(null));
  ADD_ROW(fake_table1, COL(2), COL(2), COL(1.0), COL("xuchen.wyc"), COL(null));

  ADD_ROW(fake_table2, COL(2), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));

  ADD_ROW(result_table, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(result_table, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(result_table, COL(1), COL(2), COL(1.0), COL("xuchen.wyc"), COL(null));
  ADD_ROW(result_table, COL(1), COL(2), COL(1.0), COL("xuchen.wyc"), COL(null));
  ADD_ROW(result_table, COL(2), COL(2), COL(1.0), COL("xuchen.wyc"), COL(null));
  ADD_ROW(result_table, COL(2), COL(2), COL(1.0), COL("xuchen.wyc"), COL(null));

  ASSERT_EQ(OB_SUCCESS, merge_except.open(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.open(ctx));
  EXCEPT_RESULT_SET(ctx, result_table, merge_except, 0, 5, agg_cs_type);
  ASSERT_EQ(OB_SUCCESS, merge_except.close(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.close(ctx));
}

int main(int argc, char **argv)
{
  init_global_memory_pool();
  init_sql_factories();
  ::testing::InitGoogleTest(&argc,argv);
  int ret = RUN_ALL_TESTS();
  OB_LOGGER.disable();
  return ret;
}
