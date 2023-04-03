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

#include "sql/engine/join/ob_nested_loop_join.h"
#include <gtest/gtest.h>
#include "lib/utility/ob_test_util.h"
#include "sql/ob_sql_init.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;
class TestNLJTest2: public ::testing::Test
{
public:
  TestNLJTest2();
  virtual ~TestNLJTest2();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestNLJTest2);
protected:
  // function members
protected:
  // data members
};

TestNLJTest2::TestNLJTest2()
{
}

TestNLJTest2::~TestNLJTest2()
{
}

void TestNLJTest2::SetUp()
{
}

void TestNLJTest2::TearDown()
{
}

TEST_F(TestNLJTest2, serialize_rescan_params)
{
  ObPhysicalPlan phy_plan;
  ObNestedLoopJoin join(phy_plan.get_allocator());
  join.set_phy_plan(&phy_plan);
  join.set_id(100);
  join.set_column_count(111);
  join.init_param_count(200);
  ObSqlExpressionFactory factory(phy_plan.get_allocator());
  ObSqlExpression *expr = NULL;
  ASSERT_EQ(OB_SUCCESS, factory.alloc(expr));
  ASSERT_TRUE(NULL != expr);
  ObPostExprItem item;
  expr->set_item_count(1);
  ASSERT_EQ(OB_SUCCESS, item.set_column(10));
  ASSERT_EQ(OB_SUCCESS, expr->add_expr_item(item));
  ASSERT_EQ(OB_SUCCESS, join.add_nlj_param(expr, 3));
  COMMON_LOG(INFO, "join", K(join));
  // serialize
  char buf[1024];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, join.serialize(buf, 1024, pos));
  ASSERT_EQ(pos, join.get_serialize_size());
  const char* join_str = S(join);
  // deserialize
  int64_t data_len = pos;
  pos = 0;
  ObPhysicalPlan phy_plan2;
  ObNestedLoopJoin join2(phy_plan2.get_allocator());
  join2.set_phy_plan(&phy_plan2);
  ASSERT_EQ(OB_SUCCESS, join2.deserialize(buf, data_len, pos));
  ASSERT_EQ(data_len, pos);
  COMMON_LOG(INFO, "join2", K(join2));
  const char* join2_str = S(join2);
  ASSERT_STREQ(join_str, join2_str);
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  init_sql_factories();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
