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
#include "sql/ob_sql_init.h"
#include "sql/engine/table/ob_fake_table.h"
#include "sql/engine/dml/ob_table_insert.h"
#include "sql/engine/basic/ob_expr_values.h"
#include "observer/ob_server.h"
#include "observer/ob_server_struct.h"
#include "sql/engine/test_engine_util.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::observer;

class ObTableInsertTest: public ::testing::Test
{
public:
  ObTableInsertTest();
  virtual ~ObTableInsertTest();
  virtual void SetUp();
  virtual void TearDown();

  int create_const_expr(ObObj &obj, ObSqlExpression *&expr);
  int create_expr_values(ObExprValues *&expr_values);
private:
  // disallow copy
  ObTableInsertTest(const ObTableInsertTest &other);
  ObTableInsertTest& operator=(const ObTableInsertTest &other);
protected:
  // data members
  ObPhysicalPlan physical_plan_;
};

ObTableInsertTest::ObTableInsertTest()
{
}

ObTableInsertTest::~ObTableInsertTest()
{
}

void ObTableInsertTest::SetUp()
{
}

void ObTableInsertTest::TearDown()
{
}

int ObTableInsertTest::create_const_expr(ObObj &obj, ObSqlExpression *&expr)
{
  int ret = OB_SUCCESS;
  ObPostExprItem expr_item;

  if (OB_SUCCESS != (ret = ObSqlExpressionUtil::make_sql_expr(&physical_plan_, expr))) {
    SQL_ENG_LOG(WARN, "make expression failed", K(ret));
  } else if (OB_SUCCESS != (ret = expr_item.assign(obj))) {
    SQL_ENG_LOG(WARN, "assign expr item failed", K(ret));
  } else if (OB_FAIL(expr->set_item_count(1))) {
    SQL_ENG_LOG(WARN, "fail to set item count", K(ret));
  } else if (OB_SUCCESS != (ret = expr->add_expr_item(expr_item))) {
    SQL_ENG_LOG(WARN, "add expression item failed", K(ret));
  }
  return ret;
}

int ObTableInsertTest::create_expr_values(ObExprValues *&expr_values)
{
  return physical_plan_.alloc_operator_by_type(PHY_EXPR_VALUES, expr_values);
}

TEST_F(ObTableInsertTest, test_operator_serialize)
{
  ObExprValues *expr_values = NULL;
  ASSERT_EQ(OB_SUCCESS, create_expr_values(expr_values));
  expr_values->set_column_count(4);
  expr_values->set_id(0);
  ASSERT_EQ(OB_SUCCESS, expr_values->init_value_count(4));
  for (int64_t i = 0; i < 4; ++i) {
    ObSqlExpression *expr = NULL;
    ObObj obj;
    obj.set_int(i);
    ObAccuracy accuracy;
    ASSERT_EQ(OB_SUCCESS, create_const_expr(obj, expr));
    ASSERT_EQ(OB_SUCCESS, expr_values->add_value(expr));
  }
  ObExprValues *dec_values = NULL;
  char buf[1024] = {'\0'};
  int64_t pos = 0;
  ObPhyOpSeriCtx seri_ctx;
  ASSERT_EQ(OB_SUCCESS, expr_values->serialize(buf, sizeof(buf), pos, seri_ctx));
  int64_t data_len = pos;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, create_expr_values(dec_values));
  ASSERT_EQ(OB_SUCCESS, dec_values->deserialize(buf, data_len, pos));
  _OB_LOG(INFO, "expr_values: %s", to_cstring(*expr_values));
  _OB_LOG(INFO, "dec_values: %s", to_cstring(*dec_values));
  ASSERT_EQ(0, strcmp(to_cstring(*expr_values), to_cstring(*dec_values)));

  int ret = OB_SUCCESS;
  uint64_t app_column_id = 16;
  ObTableInsert *table_insert = NULL;
  ASSERT_EQ(OB_SUCCESS, physical_plan_.alloc_operator_by_type(PHY_INSERT, table_insert));
  table_insert->set_column_count(4);
  table_insert->set_id(1);
  table_insert->set_table_id(3003);
  table_insert->set_index_tid(3003);
  ASSERT_EQ(OB_SUCCESS, table_insert->set_child(0, *expr_values));
  expr_values->set_parent(table_insert);
  table_insert->init_column_ids_count(4);
  for (int64_t i = 0; OB_SUCC(ret) && i < 4; ++i) {
    ASSERT_EQ(OB_SUCCESS, table_insert->add_column_id(app_column_id + i));
  }
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, table_insert->serialize(buf, sizeof(buf), pos));
  data_len = pos;
  ObTableInsert *dec_insert = NULL;
  ASSERT_EQ(OB_SUCCESS, physical_plan_.alloc_operator_by_type(PHY_INSERT, dec_insert));
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, dec_insert->deserialize(buf, data_len, pos));
  _OB_LOG(INFO, "table_insert: %s", to_cstring(*table_insert));
  _OB_LOG(INFO, "dec_insert: %s", to_cstring(*dec_insert));
  ASSERT_EQ(0, strcmp(to_cstring(*table_insert), to_cstring(*dec_insert)));
}

/*
TEST_F(ObTableInsertTest, test_insert_basic)
{
  ObPhysicalPlan *phy_plan = ObPhysicalPlan::alloc();;
  ObTableInsert *table_insert = NULL;
  ObExprValues *expr_values = NULL;
  CREATE_PHY_OPERATOR(table_insert, ObTableInsert, PHY_INSERT, phy_plan);
  CREATE_PHY_OPERATOR(expr_values, ObExprValues, PHY_EXPR_VALUES, phy_plan);
  OB_ASSERT(phy_plan);
  OB_ASSERT(expr_values);
  OB_ASSERT(table_insert);

  expr_values->set_id(0);
  expr_values->set_column_count(4);
  for (int64_t i = 0; i < 4; ++i) {
    ObSqlExpression *expr = NULL;
    ObObj obj;
    obj.set_int(i);
    ASSERT_EQ(OB_SUCCESS, create_const_expr(obj, expr));
    ASSERT_EQ(OB_SUCCESS, expr_values->add_value(expr));
  }
  table_insert->set_id(1);
  table_insert->set_column_count(4);
  table_insert->set_table_id(3003);
  ASSERT_EQ(OB_SUCCESS, table_insert->set_child(0, *expr_values));

  ObExecContext ctx;
  ObPhysicalPlanCtx *plan_ctx = NULL;
  ObSQLSessionInfo *my_session = NULL;
  ASSERT_EQ(OB_SUCCESS, ctx.init(4));
  ASSERT_EQ(OB_SUCCESS, ctx.create_physical_plan_ctx(plan_ctx));
  ASSERT_EQ(OB_SUCCESS, ctx.create_my_session(my_session));
  ASSERT_FALSE(NULL == plan_ctx);
  ASSERT_FALSE(NULL == my_session);
  plan_ctx->set_phy_plan(phy_plan);
  ASSERT_EQ(OB_SUCCESS, table_insert->open(ctx));
  ASSERT_EQ(OB_SUCCESS, table_insert->close(ctx));
}
*/

int main(int argc, char **argv)
{
  init_global_memory_pool();
  init_sql_factories();
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc,argv);
  int ret = RUN_ALL_TESTS();
  return ret;
}
