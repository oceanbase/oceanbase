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

#include "sql/optimizer/ob_log_plan.h"
#include <gtest/gtest.h>


#define BUF_LEN 102400 // 100K

using namespace oceanbase::sql;
using namespace oceanbase::common;

class ObLogPlanTest: public ::testing::Test
{
  public:
	  ObLogPlanTest();
    virtual ~ObLogPlanTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObLogPlanTest(const ObLogPlanTest &other);
    ObLogPlanTest& operator=(const ObLogPlanTest &other);
  private:
    // data members
};

ObLogPlanTest::ObLogPlanTest()
{
}

ObLogPlanTest::~ObLogPlanTest()
{
}

void ObLogPlanTest::SetUp()
{
}

void ObLogPlanTest::TearDown()
{
}

TEST_F(ObLogPlanTest, ob_explain_test)
{

  // create a plan
  ObSQLSessionInfo session_info
  ASSERT_EQ(OB_SUCCESS, session_info.test_init(0, 0, 0, NULL));
  ObOptimizerContext ctx(NULL, NULL);
  ObSelectStmt *select_stmt = NULL;
	ObSelectLogPlan plan(ctx, select_stmt);

  // create the operator tree
  ObLogTableScan *left =
    reinterpret_cast<ObLogTableScan *>(log_op_factory_.allocate(LOG_TABLE_SCAN));
  ObLogTableScan *right =
    reinterpret_cast<ObLogTableScan *>(log_op_factory_.allocate(LOG_TABLE_SCAN));
  ObLogJoin *join =
    reinterpret_cast<ObLogJoin *>(log_op_factory_.allocate(LOG_JOIN));
  ObLogGroupBy *group_by =
    reinterpret_cast<ObLogGroupBy *>(log_op_factory_.allocate(LOG_GROUP_BY));
  ObLogOrderBy *order_by =
    reinterpret_cast<ObLogOrderBy *>(log_op_factory_.allocate(LOG_ORDER_BY));
  ObLogLimit *limit =
    reinterpret_cast<ObLogLimit *>(log_op_factory_.allocate(LOG_LIMIT));

  ASSERT_EQ(OB_SUCCESS, plan.set_plan_root(limit));
  ASSERT_EQ(OB_SUCCESS, limit.set_child(first_child, order_by));
  ASSERT_EQ(OB_SUCCESS, order_by.set_parent(limit));
  ASSERT_EQ(OB_SUCCESS, order_by->set_child(first_child, group_by));
  ASSERT_EQ(OB_SUCCESS, group_by->set_parent(order_by));
  ASSERT_EQ(OB_SUCCESS, group_by->set_child(first_child, join));
  ASSERT_EQ(OB_SUCCESS, join->set_parent(group_by));
  ASSERT_EQ(OB_SUCCESS, join->set_child(first_child, left));
  ASSERT_EQ(OB_SUCCESS, left->set_parent(join));
  ASSERT_EQ(OB_SUCCESS, join->set_child(second_child, right));
  ASSERT_EQ(OB_SUCCESS, right->set_parent(join));

  // print a plan
  char buf[BUF_LEN];
	ASSERT_EQ(OB_SUCCESS,	plan.to_string(buf, BUF_LEN));
	printf("%s\n", buf);
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
