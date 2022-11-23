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
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/set/ob_set_test_util.h"
#include "sql/ob_sql_init.h"
#include "sql/engine/set/ob_merge_union.h"
#include "sql/engine/table/ob_fake_table.h"
#include "observer/ob_server.h"
#include "observer/ob_server_struct.h"
#include "share/ob_tenant_mgr.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::observer;

class TestMergeUnion : public ObMergeUnion
{
public:
  TestMergeUnion() :ObMergeUnion(alloc_) {}
  virtual ~TestMergeUnion() {}
};

class TestMergeUnionTest: public ::testing::Test
{
public:
  TestMergeUnionTest();
  virtual ~TestMergeUnionTest();
  virtual void SetUp();
  virtual void TearDown();
protected:
private:
  // disallow copy
  TestMergeUnionTest(const TestMergeUnionTest &other);
  TestMergeUnionTest& operator=(const TestMergeUnionTest &other);
private:
  // data members
};
TestMergeUnionTest::TestMergeUnionTest()
{
}

TestMergeUnionTest::~TestMergeUnionTest()
{
}

void TestMergeUnionTest::SetUp()
{
}

void TestMergeUnionTest::TearDown()
{
}

TEST_F(TestMergeUnionTest, test_all_get_next_row1)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ObSQLSessionInfo my_session;
  ASSERT_EQ(OB_SUCCESS, my_session.test_init(0,0,0,NULL));
  ctx.set_my_session(&my_session);
  ObFakeTable &fake_table1 = TestSetOperatorFactory::get_fake_table1();
  ObFakeTable &fake_table2 = TestSetOperatorFactory::get_fake_table2();
  ObFakeTable &result_table = TestSetOperatorFactory::get_result_table();
  TestMergeUnion merge_union;
  ObCollationType agg_cs_type = CS_TYPE_UTF8MB4_BIN;

  bool set_direction = false;
  TestSetOperatorFactory::init(ctx, &merge_union, 5, false, set_direction);
  //prepare rows, equal in the first row
  ADD_ROW(fake_table1, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table1, COL(2), COL(3), COL(null), COL("yuming"), COL("oceanbase"));
  ADD_ROW(fake_table2, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table2, COL(2), COL(2), COL(4.0), COL("oceanbase"), COL(null));

  ADD_ROW(result_table, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(result_table, COL(2), COL(3), COL(null), COL("yuming"), COL("oceanbase"));
  ADD_ROW(result_table, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(result_table, COL(2), COL(2), COL(4.0), COL("oceanbase"), COL(null));

  ASSERT_EQ(OB_SUCCESS, merge_union.open(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.open(ctx));
  EXCEPT_RESULT(ctx, result_table, merge_union, agg_cs_type);
  ASSERT_EQ(OB_SUCCESS, merge_union.close(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.close(ctx));
}

TEST_F(TestMergeUnionTest, test_all_get_next_row2)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ObSQLSessionInfo my_session;
  ASSERT_EQ(OB_SUCCESS, my_session.test_init(0,0,0,NULL));
  ctx.set_my_session(&my_session);
  ObFakeTable &fake_table1 = TestSetOperatorFactory::get_fake_table1();
  ObFakeTable &result_table = TestSetOperatorFactory::get_result_table();
  TestMergeUnion merge_union;
  ObCollationType agg_cs_type = CS_TYPE_UTF8MB4_BIN;


  bool set_direction = false;
  TestSetOperatorFactory::init(ctx, &merge_union, 5, false, set_direction);
  //prepare rows, equal in the first row
  ADD_ROW(fake_table1, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(result_table, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));

  ASSERT_EQ(OB_SUCCESS, merge_union.open(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.open(ctx));
  EXCEPT_RESULT(ctx, result_table, merge_union, agg_cs_type);
  ASSERT_EQ(OB_SUCCESS, merge_union.close(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.close(ctx));
}

TEST_F(TestMergeUnionTest, test_all_get_next_row3)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ObSQLSessionInfo my_session;
  ASSERT_EQ(OB_SUCCESS, my_session.test_init(0,0,0,NULL));
  ctx.set_my_session(&my_session);
  const ObNewRow *row = NULL;
  TestMergeUnion merge_union;


  bool set_direction = false;
  TestSetOperatorFactory::init(ctx, &merge_union, 5, false, set_direction);

  ASSERT_EQ(OB_SUCCESS, merge_union.open(ctx));
  ret = merge_union.get_next_row(ctx, row);
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(OB_SUCCESS, merge_union.close(ctx));
}

TEST_F(TestMergeUnionTest, test_all_get_next_row4)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ObSQLSessionInfo my_session;
  ASSERT_EQ(OB_SUCCESS, my_session.test_init(0,0,0,NULL));
  ctx.set_my_session(&my_session);
  ObFakeTable &fake_table2 = TestSetOperatorFactory::get_fake_table2();
  ObFakeTable &result_table = TestSetOperatorFactory::get_result_table();
  TestMergeUnion merge_union;
  ObCollationType agg_cs_type = CS_TYPE_UTF8MB4_GENERAL_CI;


  bool set_direction = false;
  TestSetOperatorFactory::init(ctx, &merge_union, 5, false, set_direction);
  //prepare rows, equal in the first row
  ADD_ROW(fake_table2, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(result_table, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));

  ASSERT_EQ(OB_SUCCESS, merge_union.open(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.open(ctx));
  EXCEPT_RESULT(ctx, result_table, merge_union, agg_cs_type);
  ASSERT_EQ(OB_SUCCESS, merge_union.close(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.close(ctx));
}

TEST_F(TestMergeUnionTest, test_distinct_get_next_row1)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ObSQLSessionInfo my_session;
  ASSERT_EQ(OB_SUCCESS, my_session.test_init(0,0,0,NULL));
  ctx.set_my_session(&my_session);
  ObFakeTable &fake_table1 = TestSetOperatorFactory::get_fake_table1();
  ObFakeTable &fake_table2 = TestSetOperatorFactory::get_fake_table2();
  ObFakeTable &result_table = TestSetOperatorFactory::get_result_table();
  TestMergeUnion merge_union;
  ObCollationType agg_cs_type = CS_TYPE_UTF8MB4_BIN;


  bool set_direction = false;
  TestSetOperatorFactory::init(ctx, &merge_union, 5, true, set_direction);
  //prepare rows, equal in the first row
  ADD_ROW(fake_table1, COL(1), COL(2), COL(1), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table1, COL(1), COL(2), COL(1), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table1, COL(3), COL(2), COL(null), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table1, COL(5), COL(2), COL(4), COL("oceanbase"), COL(null));
  ADD_ROW(fake_table1, COL(6), COL(2), COL(4), COL("oceanbase"), COL(null));
  ADD_ROW(fake_table1, COL(6), COL(2), COL(4), COL("oceanbase"), COL(null));
  ADD_ROW(fake_table1, COL(7), COL(2), COL(4), COL("oceanbase"), COL(null));
  ADD_ROW(fake_table2, COL(2), COL(3), COL(15), COL("yuming"), COL("oceanbase"));
  ADD_ROW(fake_table2, COL(2), COL(3), COL(15), COL("yuming"), COL("oceanbase"));
  ADD_ROW(fake_table2, COL(3), COL(2), COL(null), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table2, COL(4), COL(2), COL(16), COL("oceanbase"), COL(null));
  ADD_ROW(fake_table2, COL(8), COL(2), COL(16), COL("oceanbase"), COL(null));

  ADD_ROW(result_table, COL(1), COL(2), COL("1"), COL("yuchen.wyc"), COL(null));
  ADD_ROW(result_table, COL(2), COL(3), COL("15"), COL("yuming"), COL("oceanbase"));
  ADD_ROW(result_table, COL(3), COL(2), COL(null), COL("yuchen.wyc"), COL(null));
  ADD_ROW(result_table, COL(4), COL(2), COL("16"), COL("oceanbase"), COL(null));
  ADD_ROW(result_table, COL(5), COL(2), COL("4"), COL("oceanbase"), COL(null));
  ADD_ROW(result_table, COL(6), COL(2), COL("4"), COL("oceanbase"), COL(null));
  ADD_ROW(result_table, COL(7), COL(2), COL("4"), COL("oceanbase"), COL(null));
  ADD_ROW(result_table, COL(8), COL(2), COL("16"), COL("oceanbase"), COL(null));

  ASSERT_EQ(OB_SUCCESS, merge_union.open(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.open(ctx));
  EXCEPT_RESULT(ctx, result_table, merge_union, agg_cs_type);
  ASSERT_EQ(OB_SUCCESS, merge_union.close(ctx));
  ASSERT_EQ(OB_SUCCESS, merge_union.close(ctx));
}

TEST_F(TestMergeUnionTest, test_distinct_get_next_row2)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ObSQLSessionInfo my_session;
  ASSERT_EQ(OB_SUCCESS, my_session.test_init(0,0,0,NULL));
  ctx.set_my_session(&my_session);
  ObFakeTable &fake_table1 = TestSetOperatorFactory::get_fake_table1();
  ObFakeTable &fake_table2 = TestSetOperatorFactory::get_fake_table2();
  ObFakeTable &result_table = TestSetOperatorFactory::get_result_table();
  TestMergeUnion merge_union;
  ObCollationType agg_cs_type = CS_TYPE_UTF8MB4_BIN;

  bool set_direction = false;
  TestSetOperatorFactory::init(ctx, &merge_union, 5, true, set_direction);
  //prepare rows, equal in the first row
  ADD_ROW(fake_table1, COL(1), COL(2), COL(1.1), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table1, COL(2), COL(2), COL(1.1), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table1, COL(3), COL(2), COL(1.1), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table2, COL(1), COL(2), COL(1.1), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table2, COL(1), COL(2), COL(1.1), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table2, COL(2), COL(2), COL(1.1), COL("yuchen.wyc"), COL(null));

  ADD_ROW(result_table, COL(1), COL(2), COL(1.1), COL("yuchen.wyc"), COL(null));
  ADD_ROW(result_table, COL(2), COL(2), COL(1.1), COL("yuchen.wyc"), COL(null));
  ADD_ROW(result_table, COL(3), COL(2), COL(1.1), COL("yuchen.wyc"), COL(null));

  ASSERT_EQ(OB_SUCCESS, merge_union.open(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.open(ctx));
  EXCEPT_RESULT(ctx, result_table, merge_union, agg_cs_type);
  ASSERT_EQ(OB_SUCCESS, merge_union.close(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.close(ctx));
}

TEST_F(TestMergeUnionTest, test_distinct_get_next_row3)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ObSQLSessionInfo my_session;
  ASSERT_EQ(OB_SUCCESS, my_session.test_init(0,0,0,NULL));
  ctx.set_my_session(&my_session);
  ObFakeTable &fake_table1 = TestSetOperatorFactory::get_fake_table1();
  ObFakeTable &result_table = TestSetOperatorFactory::get_result_table();
  TestMergeUnion merge_union;
  ObCollationType agg_cs_type = CS_TYPE_UTF8MB4_BIN;


  bool set_direction = false;
  TestSetOperatorFactory::init(ctx, &merge_union, 5, true, set_direction);
  //prepare rows, equal in the first row
  ADD_ROW(fake_table1, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(result_table, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));

  ASSERT_EQ(OB_SUCCESS, merge_union.open(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.open(ctx));
  EXCEPT_RESULT(ctx, result_table, merge_union, agg_cs_type);
  ASSERT_EQ(OB_SUCCESS, merge_union.close(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.close(ctx));
}

TEST_F(TestMergeUnionTest, test_distinct_get_next_row4)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ObSQLSessionInfo my_session;
  ASSERT_EQ(OB_SUCCESS, my_session.test_init(0,0,0,NULL));
  ctx.set_my_session(&my_session);
  const ObNewRow *row = NULL;
  TestMergeUnion merge_union;


  bool set_direction = false;
  TestSetOperatorFactory::init(ctx, &merge_union, 5, true, set_direction);

  ASSERT_EQ(OB_SUCCESS, merge_union.open(ctx));
  ret = merge_union.get_next_row(ctx, row);
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(OB_SUCCESS, merge_union.close(ctx));
}

TEST_F(TestMergeUnionTest, test_distinct_get_next_row5)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ObSQLSessionInfo my_session;
  ASSERT_EQ(OB_SUCCESS, my_session.test_init(0,0,0,NULL));
  ctx.set_my_session(&my_session);
  ObFakeTable &fake_table2 = TestSetOperatorFactory::get_fake_table2();
  ObFakeTable &result_table = TestSetOperatorFactory::get_result_table();
  TestMergeUnion merge_union;
  ObCollationType agg_cs_type = CS_TYPE_UTF8MB4_BIN;


  bool set_direction = false;
  TestSetOperatorFactory::init(ctx, &merge_union, 5, true, set_direction);
  //prepare rows, equal in the first row
  ADD_ROW(fake_table2, COL(1), COL(2), COL(1), COL("yuchen.wyc"), COL(null));
  ADD_ROW(result_table, COL(1), COL(2), COL(1), COL("yuchen.wyc"), COL(null));

  ASSERT_EQ(OB_SUCCESS, merge_union.open(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.open(ctx));
  EXCEPT_RESULT(ctx, result_table, merge_union, agg_cs_type);
  ASSERT_EQ(OB_SUCCESS, merge_union.close(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.close(ctx));
}

TEST_F(TestMergeUnionTest, test_distinct_get_next_row6)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ObSQLSessionInfo my_session;
  ASSERT_EQ(OB_SUCCESS, my_session.test_init(0,0,0,NULL));
  ctx.set_my_session(&my_session);
  ObFakeTable &fake_table1 = TestSetOperatorFactory::get_fake_table1();
  ObFakeTable &fake_table2 = TestSetOperatorFactory::get_fake_table2();
  ObFakeTable &result_table = TestSetOperatorFactory::get_result_table();
  TestMergeUnion merge_union;
  ObCollationType agg_cs_type = CS_TYPE_UTF8MB4_BIN;

  bool set_direction = false;
  TestSetOperatorFactory::init(ctx, &merge_union, 2, true, set_direction);
  //prepare rows, equal in the first row
  ADD_ROW(fake_table1, COL("1"), COL("a"));
  ADD_ROW(fake_table1, COL("2"), COL("b"));
  ADD_ROW(fake_table2, COL("a"), COL("1"));
  ADD_ROW(fake_table2, COL("b"), COL("2"));

  ADD_ROW(result_table, COL("1"), COL("a"));
  ADD_ROW(result_table, COL("2"), COL("b"));
  ADD_ROW(result_table, COL("a"), COL("1"));
  ADD_ROW(result_table, COL("b"), COL("2"));

  ASSERT_EQ(OB_SUCCESS, merge_union.open(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.open(ctx));
  EXCEPT_RESULT(ctx, result_table, merge_union, agg_cs_type);
  ASSERT_EQ(OB_SUCCESS, merge_union.close(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.close(ctx));
}

TEST_F(TestMergeUnionTest, test_distinct_with_direction_get_next_row1)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ObSQLSessionInfo my_session;
  ASSERT_EQ(OB_SUCCESS, my_session.test_init(0,0,0,NULL));
  ctx.set_my_session(&my_session);
  ObFakeTable &fake_table1 = TestSetOperatorFactory::get_fake_table1();
  ObFakeTable &fake_table2 = TestSetOperatorFactory::get_fake_table2();
  ObFakeTable &result_table = TestSetOperatorFactory::get_result_table();
  TestMergeUnion merge_union;
  ObCollationType agg_cs_type = CS_TYPE_UTF8MB4_BIN;

  bool set_direction = true;
  TestSetOperatorFactory::init(ctx, &merge_union, 2, true, set_direction);
  //prepare rows, equal in the first row
  ADD_ROW(fake_table1, COL("1"), COL("a"));
  ADD_ROW(fake_table1, COL("2"), COL("b"));
  ADD_ROW(fake_table2, COL("1"), COL("c"));
  ADD_ROW(fake_table2, COL("2"), COL("c"));

  ADD_ROW(result_table, COL("1"), COL("c"));
  ADD_ROW(result_table, COL("1"), COL("b"));
  ADD_ROW(result_table, COL("2"), COL("c"));
  ADD_ROW(result_table, COL("2"), COL("b"));

  ASSERT_EQ(OB_SUCCESS, merge_union.open(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.open(ctx));
  EXCEPT_RESULT(ctx, result_table, merge_union, agg_cs_type);
  ASSERT_EQ(OB_SUCCESS, merge_union.close(ctx));
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
