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
#include "common/object/ob_object.h"
#include "common/row/ob_row.h"
#include "sql/ob_sql_init.h"
#include "sql/engine/expr/ob_sql_expression.h"
#include "sql/engine/ob_phy_operator.h"
#include "sql/engine/ob_single_child_phy_operator.h"
#include "sql/engine/ob_no_children_phy_operator.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/aggregate/ob_merge_groupby.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/session/ob_sql_session_info.h"

using namespace oceanbase;
using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace ::testing;
class MockSqlExpression : public common::ObDLinkBase<ObSqlExpression>
{
public:
  static ObSqlExpression *alloc();
  static void free(ObSqlExpression *ptr);
protected:
  friend class ::ObAggregateFunctionTest;
  // data members
  ObPostfixExpression post_expr_;
};
typedef common::ObGlobalFactory<MockSqlExpression, 1, common::ObModIds::OB_SQL_EXPR>
MockSqlExpressionFactory;
typedef common::ObTCFactory < MockSqlExpression, 1, common::ObModIds::OB_SQL_EXPR, 1 << 24,
                                                                                    8192 > MockSqlExpressionTCFactory;
inline MockSqlExpression *MockSqlExpression::alloc()
{
  return MockSqlExpressionTCFactory::get_instance()->get(MockSqlExpression::EXPR_TYPE_SQL);
}

inline void MockSqlExpression::free(MockSqlExpression *expr)
{
  if (OB_LIKELY(expr)) {
    expr->prev_ = expr->next_ = NULL;
  }
  MockSqlExpressionTCFactory::get_instance()->put(expr);
}

class mockPhysicalPlan
{
public:
  mockPhysicalPlan() {}
  ~mockPhysicalPlan() {}
  int destory()
  {
    int ret = OB_SUCCESS;
    DLIST_FOREACH(node, sql_expression_) {
      MockSqlExpression::free(node);
    }
    return ret;
  }
  int store_sql_expresion(MockSqlExpression *sql)
  {
    return sql_expression_.add_last(sql);
  }
private:
  ObDList<MockSqlExpression> sql_expression_;
};
class ObPhyOperatorTest: public ::testing::Test
{
public:
  ObPhyOperatorTest();
  virtual ~ObPhyOperatorTest();
  virtual void SetUp();
  virtual void TearDown();

private:
  // disallow copy
  ObPhyOperatorTest(const ObPhyOperatorTest &other);
  ObPhyOperatorTest& operator=(const ObPhyOperatorTest &other);
private:
  // data members
};
ObPhyOperatorTest::ObPhyOperatorTest()
{
}

ObPhyOperatorTest::~ObPhyOperatorTest()
{
}

void ObPhyOperatorTest::SetUp()
{
}

void ObPhyOperatorTest::TearDown()
{
}

TEST_F(ObPhyOperatorTest, physical_plan_size)
{
  typedef oceanbase::common::ObFixedArray<ObPhysicalPlan::ObTableVersion, common::ObIAllocator> BaseTableStore;
  typedef oceanbase::common::ObFixedArray<ObPhysicalPlan::ObTableVersion, common::ObIAllocator> ViewTableStore;
  typedef oceanbase::common::ObFixedArray<common::ObField, common::ObIAllocator> ParamsFieldArray;
  typedef oceanbase::common::ObFixedArray<common::ObField, common::ObIAllocator> ColumnsFieldArray;
  struct MockPhysicalPlan
  {
    common::ObArenaAllocator inner_alloc_;
    common::ObArenaAllocator &allocator_;
    int64_t prepare_count_;
    volatile int64_t ref_count_;
    int64_t schema_version_;
    uint64_t plan_id_;
    int64_t hint_timeout_us_;
    ObPhyOperator *main_query_;
    BaseTableStore table_store_;
    ViewTableStore view_table_store_;
    int64_t param_count_;
    int64_t merged_version_;
    common::ObDList<ObSqlExpression> pre_calc_exprs_;
    common::ObFixedArray<ObParamInfo, common::ObIAllocator> params_info_;
    uint64_t signature_;
    ColumnsFieldArray field_columns_;
    ParamsFieldArray param_columns_;
    common::ObFixedArray<share::AutoincParam, common::ObIAllocator> autoinc_params_; //auto-incrment param
    share::schema::ObStmtNeedPrivs stmt_need_privs_;
    common::ObFixedArray<ObVarInfo, common::ObIAllocator> vars_;
    ObPhyOperatorFactory op_factory_;
    ObSqlExpressionFactory sql_expression_factory_;
    ObExprOperatorFactory expr_op_factory_;
    stmt::StmtType stmt_type_;
    stmt::StmtType literal_stmt_type_; // 含义参考ObBasicStmt中对应定义
    ObPhyPlanType plan_type_;
    common::ObConsistencyLevel hint_consistency_;
    uint32_t next_phy_operator_id_; //share val
    // for regexp expression's compilation
    int16_t regexp_op_count_;
    bool is_sfu_;
    bool fetch_cur_time_;
    bool is_contain_virtual_table_;//为虚拟表服务，如果判断出语句中涉及虚拟表
    bool is_require_sys_tenant_priv_;
    //if the stmt  contains user variable assignment
    //such as @a:=123
    //we may need to serialize the map to remote server
    bool is_contains_assignment_;
    bool affected_last_insert_id_; //不需要序列化远端，只在本地生成执行计划和open resultset的时候需要
    bool is_affect_found_row_; //not need serialize
  };

  printf("output mock physical plan = %ld, ModulePageAllocator=%ld\n", sizeof(MockPhysicalPlan), sizeof(ModulePageAllocator));
  ObPhysicalPlan plan;
  printf("output plan size=%ld\n", sizeof(plan));
  common::ObConsistencyLevel hint_consistency;
  printf("output enum size = %ld\n", sizeof(hint_consistency));
  common::ObDList<ObSqlExpression> sql_expression;
  printf("output dlist size=%ld\n", sizeof(sql_expression));
  //printf("output fixarray = %ld, IArray=%ld\n", sizeof(common::ObFixedArray), sizeof(common::ObIArray));
  printf("output IArray=%ld\n", sizeof(common::ObIArray<int>));
  printf("output fixarray = %ld\n", sizeof(common::ObFixedArray<int, common::ObArenaAllocator>));
  printf("output physical plan size=%ld\n", sizeof(ObPhysicalPlan));
  printf("output factory=%ld, sql_expression=%ld\n", sizeof(ObPhyOperatorFactory), sizeof(ObSqlExpression));
  printf("output sql_factory=%ld, expr_op=%ld\n", sizeof(ObSqlExpressionFactory), sizeof(ObExprOperatorFactory));
  common::ObArenaAllocator alloc;
  common::ObArenaAllocator &a = alloc;
  common::ObArenaAllocator *p = &alloc;
  printf("output referrence =%ld, Obiallocator=%ld\n", sizeof(common::ObArenaAllocator &), sizeof(common::ObIAllocator &));
  printf("output alloc =%ld, refference=%ld, point=%ld\n", sizeof(alloc), sizeof(a), sizeof(p));
  printf("output pramInfo before=%ld\n", sizeof(ObParamInfo));
  struct MockParamInfo
  {
    common::ObObjType type_;
    common::ObScale scale_;
    common::ObObjParam::ParamFlag flag_;
  };
  printf("output MockParamInfo after=%ld\n", sizeof(MockParamInfo));
  printf("output ObPostItem size before=%ld, obj=%ld\n", sizeof(ObPostExprItem),sizeof(common::ObObj));
  class MockPostExprItem
  {
    common::ObObj v1_;  // const
    union
    {
      int64_t query_id_;
      int64_t cell_index_;  // column reference, aka cell index in ObRow
      ObExprOperator *op_;  // expression operator
      ObSetVar::SetScopeType sys_var_scope_;
    } v2_;
    common::ObAccuracy accuracy_;  // for const, column, questionmark
    ObItemType  item_type_;
  };
  printf("output MockPostItem size=%ld\n", sizeof(MockPostExprItem));
  printf("output ObSqlExpression size=%ld, ObPostfixExpression=%ld\n", sizeof(ObSqlExpression), sizeof(ObPostfixExpression));
  printf("output ObDList size=%ld, ObNode size=%ld\n", sizeof(ObDList<int>), sizeof(ObDLinkBase<int>));
  printf("output phy_operator =%ld", sizeof(ObPhyOperator));
  printf("start to test tc_factory");
  mockPhysicalPlan mock_plan;
  MockSqlExpression *sql1 = MockSqlExpression::alloc();
  MockSqlExpression *sql2 = MockSqlExpression::alloc();
  EXPECT_NE(NULL, sql1);
  EXPECT_NE(NULL, sql2);
  EXPECT_EQ(OB_SUCCESS, mock_plan.store_sql_expresion(sql1));
  EXPECT_EQ(OB_SUCCESS, mock_plan.store_sql_expresion(sql2));
  mock_plan.destory();
}
int main(int argc, char **argv)
{
  init_sql_factories();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
