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
#include "sql/engine/set/ob_merge_intersect.h"
#include "sql/engine/table/ob_fake_table.h"
#include "sql/session/ob_sql_session_info.h"
#include "observer/ob_server.h"
#include "observer/ob_server_struct.h"
#include "share/ob_tenant_mgr.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::observer;
class TestMergeIntersect : public ObMergeIntersect
{
public:
  TestMergeIntersect() : ObMergeIntersect(alloc_) {}
  virtual ~TestMergeIntersect(){}
};

class TestMergeIntersectTest: public ::testing::Test
{
public:
  TestMergeIntersectTest();
  virtual ~TestMergeIntersectTest();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  TestMergeIntersectTest(const TestMergeIntersectTest &other);
  TestMergeIntersectTest& operator=(const TestMergeIntersectTest &other);
private:
  // data members
};
TestMergeIntersectTest::TestMergeIntersectTest()
{
}

TestMergeIntersectTest::~TestMergeIntersectTest()
{
}

void TestMergeIntersectTest::SetUp()
{
}

void TestMergeIntersectTest::TearDown()
{
}

TEST_F(TestMergeIntersectTest, test_fake_table)
{
  int ret = OB_SUCCESS;
  ObFakeTable fake_table;
  ObExecContext ctx;
  ObSQLSessionInfo my_session;
  ASSERT_EQ(OB_SUCCESS, my_session.test_init(0,0,0,NULL));
  ctx.set_my_session(&my_session);
  const ObNewRow *row = NULL;
  ObPhysicalPlan physical_plan;
  fake_table.set_column_count(5);
  fake_table.set_id(0);
  fake_table.set_phy_plan(&physical_plan);
  ASSERT_EQ(OB_SUCCESS, ctx.init_phy_op(1));
  ASSERT_EQ(OB_SUCCESS, ctx.create_physical_plan_ctx());

  ADD_ROW(fake_table, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table, COL(2), COL(3), COL(null), COL(null), COL(null));
  ADD_ROW(fake_table, COL(3), COL(3), COL(2.0), COL("yuming"), COL("oceanbase"));
  printf("fake_table: \n%s\n", to_cstring(fake_table));
  ASSERT_EQ(OB_SUCCESS, fake_table.open(ctx));
  while (OB_SUCCESS == (ret = fake_table.get_next_row(ctx, row))) {
    printf("row=%s\n", to_cstring(*row));
  }
  ASSERT_EQ(OB_ITER_END, ret);
}

TEST_F(TestMergeIntersectTest, test_all_get_next_row1)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ObSQLSessionInfo my_session;
  ASSERT_EQ(OB_SUCCESS, my_session.test_init(0,0,0,NULL));
  ctx.set_my_session(&my_session);
  ObFakeTable &fake_table1 = TestSetOperatorFactory::get_fake_table1();
  ObFakeTable &fake_table2 = TestSetOperatorFactory::get_fake_table2();
  ObFakeTable &result_table = TestSetOperatorFactory::get_result_table();
  TestMergeIntersect merge_intersect;
  ObCollationType agg_cs_type = CS_TYPE_UTF8MB4_BIN;

  bool set_direction = false;
  TestSetOperatorFactory::init(ctx, &merge_intersect, 5, false, set_direction);
  //prepare rows, equal in the first row
  ADD_ROW(fake_table1, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table1, COL(2), COL(3), COL(null), COL("yuming"), COL("oceanbase"));
  ADD_ROW(fake_table2, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table2, COL(2), COL(2), COL(4.0), COL("oceanbase"), COL(null));
  ADD_ROW(result_table, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));

  ASSERT_EQ(OB_SUCCESS, merge_intersect.open(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.open(ctx));
  EXCEPT_RESULT_SET(ctx, result_table, merge_intersect, 0, 5, agg_cs_type);
  ASSERT_EQ(OB_SUCCESS, merge_intersect.close(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.close(ctx));
}

TEST_F(TestMergeIntersectTest, test_all_get_next_row2)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ObSQLSessionInfo my_session;
  ASSERT_EQ(OB_SUCCESS, my_session.test_init(0,0,0,NULL));
  ctx.set_my_session(&my_session);
  ObFakeTable &fake_table1 = TestSetOperatorFactory::get_fake_table1();
  ObFakeTable &fake_table2 = TestSetOperatorFactory::get_fake_table2();
  ObFakeTable &result_table = TestSetOperatorFactory::get_result_table();
  TestMergeIntersect merge_intersect;
  ObCollationType agg_cs_type = CS_TYPE_UTF8MB4_BIN;

  bool set_direction = false;
  TestSetOperatorFactory::init(ctx, &merge_intersect, 5, false, set_direction);
  //prepare rows, equal in the first row
  ADD_ROW(fake_table1, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table1, COL(2), COL(3), COL(null), COL("yuming"), COL("oceanbase"));
  ADD_ROW(fake_table1, COL(3), COL(4), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table1, COL(4), COL(2), COL(4.0), COL("oceanbase"), COL(null));
  ADD_ROW(fake_table2, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table2, COL(2), COL(2), COL(4.0), COL("oceanbase"), COL(null));
  ADD_ROW(fake_table2, COL(3), COL(4), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(result_table, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(result_table, COL(3), COL(4), COL(1.0), COL("yuchen.wyc"), COL(null));

  ASSERT_EQ(OB_SUCCESS, merge_intersect.open(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.open(ctx));
  EXCEPT_RESULT_SET(ctx, result_table, merge_intersect, 0, 5, agg_cs_type);
  ASSERT_EQ(OB_SUCCESS, merge_intersect.close(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.close(ctx));
}

TEST_F(TestMergeIntersectTest, test_all_get_next_row3)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ObSQLSessionInfo my_session;
  ASSERT_EQ(OB_SUCCESS, my_session.test_init(0,0,0,NULL));
  ctx.set_my_session(&my_session);
  ObFakeTable &fake_table1 = TestSetOperatorFactory::get_fake_table1();
  ObFakeTable &fake_table2 = TestSetOperatorFactory::get_fake_table2();
  ObFakeTable &result_table = TestSetOperatorFactory::get_result_table();
  TestMergeIntersect merge_intersect;
  ObCollationType agg_cs_type = CS_TYPE_UTF8MB4_BIN;


  bool set_direction = false;
  TestSetOperatorFactory::init(ctx, &merge_intersect, 5, false, set_direction);
  //prepare rows, equal in the first row
  ADD_ROW(fake_table1, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table1, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table1, COL(3), COL(4), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table1, COL(4), COL(2), COL(4.0), COL("oceanbase"), COL(null));
  ADD_ROW(fake_table2, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table2, COL(2), COL(2), COL(4.0), COL("oceanbase"), COL(null));
  ADD_ROW(fake_table2, COL(3), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(result_table, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(result_table, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));

  ASSERT_EQ(OB_SUCCESS, merge_intersect.open(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.open(ctx));
  EXCEPT_RESULT_SET(ctx, result_table, merge_intersect, 0, 5, agg_cs_type);
  ASSERT_EQ(OB_SUCCESS, merge_intersect.close(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.close(ctx));
}

TEST_F(TestMergeIntersectTest, test_all_get_next_row4)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ObSQLSessionInfo my_session;
  ASSERT_EQ(OB_SUCCESS, my_session.test_init(0,0,0,NULL));
  ctx.set_my_session(&my_session);
  ObFakeTable &fake_table1 = TestSetOperatorFactory::get_fake_table1();
  const ObNewRow *row = NULL;
  TestMergeIntersect merge_intersect;

  bool set_direction = false;
  TestSetOperatorFactory::init(ctx, &merge_intersect, 5, false, set_direction);
  //prepare rows, equal in the first row
  ADD_ROW(fake_table1, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table1, COL(2), COL(3), COL(null), COL("yuming"), COL("oceanbase"));

  ASSERT_EQ(OB_SUCCESS, merge_intersect.open(ctx));
  ASSERT_EQ(OB_ITER_END, merge_intersect.get_next_row(ctx, row));
  ASSERT_EQ(OB_SUCCESS, merge_intersect.close(ctx));
}

TEST_F(TestMergeIntersectTest, test_distinct_get_next_row1)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ObSQLSessionInfo my_session;
  ASSERT_EQ(OB_SUCCESS, my_session.test_init(0,0,0,NULL));
  ctx.set_my_session(&my_session);
  ObFakeTable &fake_table1 = TestSetOperatorFactory::get_fake_table1();
  ObFakeTable &fake_table2 = TestSetOperatorFactory::get_fake_table2();
  ObFakeTable &result_table = TestSetOperatorFactory::get_result_table();
  TestMergeIntersect merge_intersect;
  ObCollationType agg_cs_type = CS_TYPE_UTF8MB4_BIN;

  bool set_direction = false;
  TestSetOperatorFactory::init(ctx, &merge_intersect, 5, true, set_direction);
  //prepare rows, equal in the first row
  ADD_ROW(fake_table1, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table1, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table1, COL(2), COL(3), COL(null), COL("yuming"), COL("oceanbase"));
  ADD_ROW(fake_table2, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table2, COL(2), COL(2), COL(4.0), COL("oceanbase"), COL(null));
  ADD_ROW(fake_table2, COL(2), COL(2), COL(4.0), COL("oceanbase"), COL(null));
  ADD_ROW(result_table, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));

  ASSERT_EQ(OB_SUCCESS, merge_intersect.open(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.open(ctx));
  EXCEPT_RESULT_SET(ctx, result_table, merge_intersect, 0, 5, agg_cs_type);
  ASSERT_EQ(OB_SUCCESS, merge_intersect.close(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.close(ctx));
}

TEST_F(TestMergeIntersectTest, test_distinct_get_next_row2)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ObSQLSessionInfo my_session;
  ASSERT_EQ(OB_SUCCESS, my_session.test_init(0,0,0,NULL));
  ctx.set_my_session(&my_session);
  ObFakeTable &fake_table1 = TestSetOperatorFactory::get_fake_table1();
  ObFakeTable &fake_table2 = TestSetOperatorFactory::get_fake_table2();
  ObFakeTable &result_table = TestSetOperatorFactory::get_result_table();
  TestMergeIntersect merge_intersect;

 ObCollationType agg_cs_type = CS_TYPE_UTF8MB4_BIN;

  bool set_direction = false;
  TestSetOperatorFactory::init(ctx, &merge_intersect, 5, true, set_direction);
  //prepare rows, equal in the first row
  ADD_ROW(fake_table1, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table1, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table1, COL(2), COL(3), COL(null), COL("yuming"), COL("oceanbase"));
  ADD_ROW(fake_table1, COL(3), COL(4), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table1, COL(3), COL(4), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table1, COL(4), COL(2), COL(4.0), COL("oceanbase"), COL(null));
  ADD_ROW(fake_table2, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table2, COL(2), COL(2), COL(4.0), COL("oceanbase"), COL(null));
  ADD_ROW(fake_table2, COL(3), COL(4), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(result_table, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(result_table, COL(3), COL(4), COL(1.0), COL("yuchen.wyc"), COL(null));

  ASSERT_EQ(OB_SUCCESS, merge_intersect.open(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.open(ctx));
  EXCEPT_RESULT_SET(ctx, result_table, merge_intersect, 0, 5, agg_cs_type);
  ASSERT_EQ(OB_SUCCESS, merge_intersect.close(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.close(ctx));
}

TEST_F(TestMergeIntersectTest, test_distinct_get_next_row3)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ObSQLSessionInfo my_session;
  ASSERT_EQ(OB_SUCCESS, my_session.test_init(0,0,0,NULL));
  ctx.set_my_session(&my_session);
  ObFakeTable &fake_table1 = TestSetOperatorFactory::get_fake_table1();
  const ObNewRow *row = NULL;
  TestMergeIntersect merge_intersect;


  bool set_direction = false;
  TestSetOperatorFactory::init(ctx, &merge_intersect, 5, true, set_direction);
  //prepare rows, equal in the first row
  ADD_ROW(fake_table1, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table1, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));

  ASSERT_EQ(OB_SUCCESS, merge_intersect.open(ctx));
  ASSERT_EQ(OB_ITER_END, merge_intersect.get_next_row(ctx, row));
  ASSERT_EQ(OB_SUCCESS, merge_intersect.close(ctx));
}

TEST_F(TestMergeIntersectTest, invalid_argument)
{
  ObExecContext ctx;
  ObSQLSessionInfo my_session;
  ASSERT_EQ(OB_SUCCESS, my_session.test_init(0,0,0,NULL));
  ctx.set_my_session(&my_session);
  TestMergeIntersect merge_intersect;


  ASSERT_EQ(OB_NOT_INIT, merge_intersect.open(ctx));
  merge_intersect.reset();
  merge_intersect.reuse();
}

TEST_F(TestMergeIntersectTest, test_distinct_with_direction_get_next_row2)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ObSQLSessionInfo my_session;
  ASSERT_EQ(OB_SUCCESS, my_session.test_init(0,0,0,NULL));
  ctx.set_my_session(&my_session);
  ObFakeTable &fake_table1 = TestSetOperatorFactory::get_fake_table1();
  ObFakeTable &fake_table2 = TestSetOperatorFactory::get_fake_table2();
  ObFakeTable &result_table = TestSetOperatorFactory::get_result_table();
  TestMergeIntersect merge_intersect;

  ObCollationType agg_cs_type = CS_TYPE_UTF8MB4_BIN;
  bool set_direction = true;
  TestSetOperatorFactory::init(ctx, &merge_intersect, 5, true, set_direction);
  //prepare rows, equal in the first row
  ADD_ROW(fake_table1, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table1, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table1, COL(1), COL(2), COL(1.0), COL("xuchen.wyc"), COL(null));
  ADD_ROW(fake_table1, COL(2), COL(3), COL(null), COL("yuming"), COL("oceanbase"));
  ADD_ROW(fake_table1, COL(3), COL(4), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table1, COL(3), COL(4), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table1, COL(4), COL(2), COL(4.0), COL("oceanbase"), COL(null));

  ADD_ROW(fake_table2, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table2, COL(1), COL(2), COL(1.0), COL("xuchen.wyc"), COL(null));
  ADD_ROW(fake_table2, COL(2), COL(2), COL(4.0), COL("oceanbase"), COL(null));
  ADD_ROW(fake_table2, COL(3), COL(4), COL(1.0), COL("yuchen.wyc"), COL(null));

  ADD_ROW(result_table, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(result_table, COL(1), COL(2), COL(1.0), COL("xuchen.wyc"), COL(null));
  ADD_ROW(result_table, COL(3), COL(4), COL(1.0), COL("yuchen.wyc"), COL(null));

  ASSERT_EQ(OB_SUCCESS, merge_intersect.open(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.open(ctx));
  EXCEPT_RESULT_SET(ctx, result_table, merge_intersect, 0, 5, agg_cs_type);
  ASSERT_EQ(OB_SUCCESS, merge_intersect.close(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.close(ctx));
}


TEST_F(TestMergeIntersectTest, test_all_with_direction_get_next_row2)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  ObSQLSessionInfo my_session;
  ASSERT_EQ(OB_SUCCESS, my_session.test_init(0,0,0,NULL));
  ctx.set_my_session(&my_session);
  ObFakeTable &fake_table1 = TestSetOperatorFactory::get_fake_table1();
  ObFakeTable &fake_table2 = TestSetOperatorFactory::get_fake_table2();
  ObFakeTable &result_table = TestSetOperatorFactory::get_result_table();
  TestMergeIntersect merge_intersect;

 ObCollationType agg_cs_type = CS_TYPE_UTF8MB4_BIN;

  bool set_direction = true;
  TestSetOperatorFactory::init(ctx, &merge_intersect, 5, false, set_direction);
  //prepare rows, equal in the first row
  ADD_ROW(fake_table1, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table1, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table1, COL(1), COL(2), COL(1.0), COL("xuchen.wyc"), COL(null));
  ADD_ROW(fake_table1, COL(2), COL(3), COL(null), COL("yuming"), COL("oceanbase"));
  ADD_ROW(fake_table1, COL(3), COL(4), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table1, COL(3), COL(4), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table1, COL(4), COL(2), COL(4.0), COL("oceanbase"), COL(null));

  ADD_ROW(fake_table2, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table2, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(fake_table2, COL(1), COL(2), COL(1.0), COL("xuchen.wyc"), COL(null));
  ADD_ROW(fake_table2, COL(2), COL(2), COL(4.0), COL("oceanbase"), COL(null));
  ADD_ROW(fake_table2, COL(3), COL(4), COL(1.0), COL("yuchen.wyc"), COL(null));

  ADD_ROW(result_table, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(result_table, COL(1), COL(2), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(result_table, COL(1), COL(2), COL(1.0), COL("xuchen.wyc"), COL(null));
  ADD_ROW(result_table, COL(3), COL(4), COL(1.0), COL("yuchen.wyc"), COL(null));
  ADD_ROW(result_table, COL(3), COL(4), COL(1.0), COL("yuchen.wyc"), COL(null));

  ASSERT_EQ(OB_SUCCESS, merge_intersect.open(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.open(ctx));
  EXCEPT_RESULT_SET(ctx, result_table, merge_intersect, 0, 5, agg_cs_type);
  ASSERT_EQ(OB_SUCCESS, merge_intersect.close(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.close(ctx));
}
int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  init_global_memory_pool();
  init_sql_factories();
  ::testing::InitGoogleTest(&argc,argv);
  int ret = RUN_ALL_TESTS();
  OB_LOGGER.disable();
  return ret;
}
