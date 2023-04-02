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

#define USING_LOG_PREFIX SQL_OPT
#include <gtest/gtest.h>
#include "lib/json/ob_json.h"
#include "sql/optimizer/ob_log_plan_factory.h"
#include "sql/optimizer/ob_log_plan.h"
#include "sql/optimizer/ob_select_log_plan.h"
#include "sql/optimizer/ob_log_join.h"
#include "sql/optimizer/ob_log_table_scan.h"
#include "sql/optimizer/ob_log_sort.h"
#include "sql/optimizer/ob_log_group_by.h"
#include "sql/optimizer/ob_log_exchange.h"
#include "sql/optimizer/ob_join_order.h"
#include "sql/optimizer/ob_log_limit.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/parser/ob_parser.h"
#include "sql/optimizer/ob_optimizer.h"
#include "sql/ob_sql_context.h"
#include "sql/ob_sql.h"
#include "sql/resolver/ob_resolver.h"

#define BUF_LEN 102400 // 100K

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::json;

namespace test
{
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
  ObSQLSessionInfo session_info;
  ASSERT_EQ(OB_SUCCESS, session_info.test_init(0, 0, 0, NULL));
  ObArenaAllocator mempool(ObModIds::OB_SQL_COMPILE, OB_MALLOC_NORMAL_BLOCK_SIZE);
  ObExecContext exec_ctx(mempool);
  ObAddr addr;
  ObGlobalHint global_hint;
  ObRawExprFactory expr_factory(mempool);
  ObLogPlanFactory log_plan_factory_(mempool);
  ObOptimizerContext ctx(&session_info, &exec_ctx, NULL, NULL,
                         static_cast<ObIAllocator &>(mempool),
                         NULL, addr, NULL, global_hint, 
                         expr_factory, NULL, false);
  ObSelectStmt *select_stmt = NULL;
	ObSelectLogPlan plan(ctx, select_stmt);

  // create the operator tree
  ObLogTableScan *left =
    reinterpret_cast<ObLogTableScan *>(plan.log_op_factory_.allocate(plan, log_op_def::LOG_TABLE_SCAN));
  ObLogTableScan *right =
    reinterpret_cast<ObLogTableScan *>(plan.log_op_factory_.allocate(plan, log_op_def::LOG_TABLE_SCAN));
  ObLogJoin *join =
    reinterpret_cast<ObLogJoin *>(plan.log_op_factory_.allocate(plan, log_op_def::LOG_JOIN));
  ObLogGroupBy *group_by =
    reinterpret_cast<ObLogGroupBy *>(plan.log_op_factory_.allocate(plan, log_op_def::LOG_GROUP_BY));
  ObLogSort *order_by =
    reinterpret_cast<ObLogSort *>(plan.log_op_factory_.allocate(plan, log_op_def::LOG_SORT));
  ObLogLimit *limit =
    reinterpret_cast<ObLogLimit *>(plan.log_op_factory_.allocate(plan, log_op_def::LOG_LIMIT));


  plan.set_plan_root(reinterpret_cast<ObLogicalOperator*>(limit));
  limit->set_child(ObLogicalOperator::first_child, order_by);
  order_by->set_parent(limit);
  order_by->set_child(ObLogicalOperator::first_child, group_by);
  group_by->set_parent(order_by);
  group_by->set_child(ObLogicalOperator::first_child, join);
  join->set_parent(group_by);
  join->set_child(ObLogicalOperator::first_child, left);
  left->set_parent(join);
  join->set_child(ObLogicalOperator::second_child, right);
  right->set_parent(join);

  // print a plan
  char buf[BUF_LEN];
	plan.to_string(buf, BUF_LEN);
	printf("%s\n", buf);

  // print plan as json
  char output_buf[OB_MAX_LOG_BUFFER_SIZE];
  int64_t pos = 0;
  // Value *val = NULL;
  // plan.get_plan_root()->to_json(output_buf, OB_MAX_LOG_BUFFER_SIZE, pos, val);
  // Tidy tidy(val);
  // LOG_DEBUG("succ to generate json object", K(pos));

  // output_buf[0] = '\n';
  // pos = tidy.to_string(output_buf + pos, OB_MAX_LOG_BUFFER_SIZE - pos);
  if (pos < OB_MAX_LOG_BUFFER_SIZE -2) {
    output_buf[pos + 1] = '\n';
    _OB_LOG(INFO, "%.*s", static_cast<int32_t>(pos + 2), output_buf);
  }
}

}
int main(int argc, char **argv)
{
  // OB_LOGGER.set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
