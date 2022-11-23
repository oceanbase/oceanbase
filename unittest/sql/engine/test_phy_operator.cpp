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
#include "sql/engine/set/ob_merge_union.h"
using namespace oceanbase;
using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace ::testing;
using namespace oceanbase::share;

int64_t row_size = 3;
static ObArenaAllocator alloc_;
class ObSingleChildOperatorFake : public ObSingleChildPhyOperator
{
  friend class ObPhyOperatorTest;
protected:
  class ObSingleOpCtx : public ObPhyOperator::ObPhyOperatorCtx
  {
  public:
    ObSingleOpCtx(ObExecContext &ctx)
      : ObPhyOperatorCtx(ctx)
  {
  }
    virtual void destroy() { return ObPhyOperatorCtx::destroy_base(); }
  };
public:
  ObSingleChildOperatorFake() : ObSingleChildPhyOperator(alloc_) {}
  ObPhyOperatorType get_type() const
  {
    return static_cast<ObPhyOperatorType>(0);
  }

  int inner_open(ObExecContext &ctx) const
  {
    int ret = OB_SUCCESS;
    if (OB_SUCCESS != (ret = init_op_ctx(ctx))) {
      _OB_LOG(WARN, "init operator context failed, ret=%d", ret);
    }/* else {
      ret = ObSingleChildPhyOperator::open(ctx);
    }*/
    return ret;
  }

  int inner_close(ObExecContext &ctx) const
  {
    UNUSED(ctx);
    int ret = OB_SUCCESS;
    //ret = ObSingleChildPhyOperator::close(ctx);
    return ret;
  }

  /*
   * int close(ObExecContext &ctx) const
  {
    int ret = OB_SUCCESS;
    ret = ObSingleChildPhyOperator::close(ctx);
    return ret;
  }
   */

protected:
  virtual int inner_get_next_row(ObExecContext &ctx, const ObNewRow *&row) const
  {
    int ret = OB_SUCCESS;
    const ObNewRow *input_row = NULL;
    ObPhyOperator *child_op = NULL;
    ObSingleOpCtx *phy_op_ctx = NULL;

    if (NULL == (child_op = get_child(0))) {
      ret = OB_ERR_UNEXPECTED;
      _OB_LOG(WARN, "get child operator failed");
    } else if (OB_SUCCESS != (ret = child_op->get_next_row(ctx, input_row))) {
      if (OB_ITER_END != ret) {
        _OB_LOG(WARN, "get_next_row failed, ret=%d", ret);
      }
    } else if (NULL == (phy_op_ctx = GET_PHY_OPERATOR_CTX(ObSingleOpCtx, ctx, get_id()))) {
      ret = OB_ERR_UNEXPECTED;
      _OB_LOG(WARN, "get physical operator context failed, ret=%d", ret);
    } else {
      _OB_LOG(DEBUG, "inner_get_next_row, row=%s", to_cstring(*input_row));
      ObNewRow &cur_row = phy_op_ctx->get_cur_row();
      OB_ASSERT(input_row->count_ <= cur_row.count_);
      for (int64_t i = 0; i < input_row->count_; ++i) {
        cur_row.cells_[i] = input_row->cells_[i];
      }
      row = &cur_row;
    }
    return ret;
  }

  virtual int init_op_ctx(ObExecContext &ctx) const
  {
    int ret = OB_SUCCESS;
    ObPhyOperatorCtx *op_ctx = NULL;

    if (OB_SUCCESS != (ret = CREATE_PHY_OPERATOR_CTX(ObSingleOpCtx, ctx, get_id(), get_type(), op_ctx))) {
      _OB_LOG(WARN, "create physical operator context failed, ret=%d", ret);
    } else if (OB_SUCCESS != (ret = op_ctx->create_cur_row(get_column_count(), projector_, projector_size_))) {
      _OB_LOG(WARN, "create current row failed, ret=%d", ret);
    }
    return ret;
  }
};
class ObTableScanFake : public ObNoChildrenPhyOperator
{
protected:
  class ObTableScanFakeCtx : public ObPhyOperatorCtx
  {
  public:
    ObTableScanFakeCtx(ObExecContext &ctx)
        : ObPhyOperatorCtx(ctx)
    {
    }
    virtual void destroy() { return ObPhyOperatorCtx::destroy_base(); }
  };

public:
  ObTableScanFake() : ObNoChildrenPhyOperator(alloc_),
      row_store_(NULL), store_size_(0), cur_index_(0)
  {
  }
  ObPhyOperatorType get_type() const
  {
    return static_cast<ObPhyOperatorType>(0);
  }

  int inner_open(ObExecContext &ctx) const
  {
    int ret = OB_SUCCESS;
    if (OB_SUCCESS != (ret = init_op_ctx(ctx))) {
      _OB_LOG(WARN, "init operator context failed, ret=%d", ret);
    }
    return ret;
  }

  int inner_close(ObExecContext &ctx) const
  {
    UNUSED(ctx);
    return OB_SUCCESS;
  }

  void load_data_row(ObExecContext &ctx, int64_t store_size)
  {
    int64_t cell_val = -1;
    void *ptr = NULL;

    store_size_ = store_size;
    ASSERT_GT(store_size_, 0);
    ptr = ctx.get_allocator().alloc(store_size * sizeof(ObNewRow));
    ASSERT_FALSE(NULL == ptr);
    row_store_ = static_cast<ObNewRow *>(ptr);
    for (int64_t i = 0; i < store_size; ++i) {
      void *cells = ctx.get_allocator().alloc(row_size * sizeof(ObObj));
      ASSERT_FALSE(NULL == cells);
      row_store_[i].cells_ = static_cast<ObObj*>(cells);
      row_store_[i].count_ = row_size;
      for (int64_t j = 0; j < row_size; ++j) {
        row_store_[i].cells_[j].set_int(++cell_val);
      }
    }
  }
protected:
  virtual int inner_get_next_row(ObExecContext &ctx, const ObNewRow *&row) const
  {
    UNUSED(ctx);
    int ret = OB_SUCCESS;
    if (cur_index_ < store_size_) {
      row = &row_store_[cur_index_++];
    } else {
      ret = OB_ITER_END;
    }
    return ret;
  }

  virtual int init_op_ctx(ObExecContext &ctx) const
  {
    int ret = OB_SUCCESS;
    ObPhyOperatorCtx *op_ctx = NULL;
    ret = CREATE_PHY_OPERATOR_CTX(ObTableScanFakeCtx, ctx, get_id(), get_type(), op_ctx);
    UNUSED(op_ctx);
    return ret;
  }
private:
  ObNewRow *row_store_;
  int64_t store_size_;
  mutable int64_t cur_index_;
};

class ObPhyOperatorTest: public ::testing::Test
{
public:
  ObPhyOperatorTest();
  virtual ~ObPhyOperatorTest();
  virtual void SetUp();
  virtual void TearDown();

  int test_calculate_row(ObExecContext &ctx, ObSingleChildOperatorFake &phy_op)
  {
    int ret = OB_SUCCESS;
    ObExprCtx expr_ctx;
    ObSingleChildOperatorFake::ObSingleOpCtx single_op_ctx(ctx);

    if (OB_SUCCESS != (ret = single_op_ctx.create_cur_row(2, NULL, 0))) {
      _OB_LOG(WARN, "create current row failed, ret=%d", ret);
    } else {
      ret = phy_op.calculate_row(expr_ctx, single_op_ctx.get_cur_row());
    }
    return ret;
  }

  int test_wrap_expr_ctx(ObExecContext &exec_ctx,
                         ObExprCtx &expr_ctx,
                         ObSingleChildOperatorFake &phy_op)
  {
    return phy_op.wrap_expr_ctx(exec_ctx, expr_ctx);
  }
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

/**
 * @brief test operator only get row
 */
TEST_F(ObPhyOperatorTest, test_get_row)
{
  const ObNewRow *row = NULL;
  ObExecContext ctx;
  ObSQLSessionInfo my_session;
  ASSERT_EQ(OB_SUCCESS, my_session.test_init(0,0,0,NULL));
  ctx.set_my_session(&my_session);
  ObTableScanFake table_scan;
  ObSingleChildOperatorFake root;
  ObPhysicalPlan physical_plan;
  int64_t op_size = 2;

  ASSERT_EQ(OB_SUCCESS, ctx.init_phy_op(op_size));
  ASSERT_EQ(OB_SUCCESS, ctx.create_physical_plan_ctx());

  table_scan.load_data_row(ctx, 1);
  table_scan.set_id(0);
  table_scan.set_column_count(row_size);
  table_scan.set_phy_plan(&physical_plan);

  root.set_id(1);
  root.set_column_count(row_size);
  root.set_phy_plan(&physical_plan);
  ASSERT_EQ(OB_SUCCESS, root.set_child(0, table_scan));
  ASSERT_EQ(OB_SUCCESS, root.open(ctx));
  ASSERT_EQ(OB_SUCCESS, root.get_next_row(ctx, row));
  for (int64_t i = 0; i < row_size; ++i) {
    ObObj res;
    res.set_int(i);
    ASSERT_TRUE(res == row->cells_[i]);
  }
  printf("row=%s\n", to_cstring(*row));
  ASSERT_EQ(OB_SUCCESS, root.close(ctx));
  root.reset();
  root.reuse();
  table_scan.reset();
  table_scan.reuse();
}

TEST_F(ObPhyOperatorTest, test_filter_and_calc_row)
{
  const ObNewRow *row = NULL;
  ObExecContext ctx;
  ObTableScanFake table_scan;
  ObSingleChildOperatorFake root;
  ObPhysicalPlan physical_plan;
  int64_t op_size = 2;
  ObSQLSessionInfo my_session;
  my_session.test_init(0,0,0,NULL);
  ctx.set_my_session(&my_session);

  physical_plan.set_main_query(&root);
  ASSERT_EQ(OB_SUCCESS, ctx.init_phy_op(op_size));
  ASSERT_EQ(OB_SUCCESS, ctx.create_physical_plan_ctx());



  table_scan.load_data_row(ctx, 1);
  table_scan.set_id(0);
  table_scan.set_column_count(row_size);
  table_scan.set_phy_plan(&physical_plan);

  root.set_id(1);
  root.set_column_count(row_size);
  root.set_phy_plan(&physical_plan);
  ASSERT_EQ(OB_SUCCESS, root.set_child(0, table_scan));
  ASSERT_EQ(OB_SUCCESS, root.open(ctx));
  ASSERT_EQ(OB_SUCCESS, root.get_next_row(ctx, row));
  for (int64_t i = 0; i < row_size; ++i) {
    ObObj res;
    res.set_int(i);
    ASSERT_TRUE(res == row->cells_[i]);
  }
  printf("row=%s\n", to_cstring(*row));
  ASSERT_EQ(OB_SUCCESS, root.close(ctx));
  root.reset();
  root.reuse();
  table_scan.reset();
  table_scan.reuse();
}

TEST_F(ObPhyOperatorTest, test_filter_and_calc_row_1)
{
  const ObNewRow *row = NULL;
  ObExecContext ctx;
  ObTableScanFake table_scan;
  ObSingleChildOperatorFake root;
  ObPhysicalPlan physical_plan;
  ObSQLSessionInfo my_session;
  int64_t op_size = 2;

  my_session.test_init(0,0,0,NULL);
  ctx.set_my_session(&my_session);

  physical_plan.set_main_query(&root);
  ASSERT_EQ(OB_SUCCESS, ctx.init_phy_op(op_size));
  ASSERT_EQ(OB_SUCCESS, ctx.create_physical_plan_ctx());

  table_scan.set_id(0);
  table_scan.load_data_row(ctx, 2);
  table_scan.set_column_count(row_size);
  table_scan.set_phy_plan(&physical_plan);

  ObExprOperator *expr_op = NULL;
  ObExprResType res_type;
  res_type.set_calc_type(ObIntType);
  res_type.set_type(ObIntType);
  /*
   * filter c3 - 1 = 1
   */
  ObSqlExpression *filer_expr = NULL;
  ASSERT_EQ(OB_SUCCESS, ObSqlExpressionUtil::make_sql_expr(&physical_plan, filer_expr));
  ASSERT_FALSE(NULL == filer_expr);
  filer_expr->set_item_count(5);
  ObPostExprItem expr_item;

  expr_item.set_int(1);
  expr_item.set_item_type(T_INT);
  ASSERT_EQ(OB_SUCCESS, filer_expr->add_expr_item(expr_item));
  expr_item.set_column(2);
  ASSERT_EQ(OB_SUCCESS, filer_expr->add_expr_item(expr_item));
  expr_item.set_int(1);
  expr_item.set_item_type(T_INT);
  ASSERT_EQ(OB_SUCCESS, filer_expr->add_expr_item(expr_item));
  expr_item.set_op(physical_plan.get_allocator(), "-", 2);
  expr_op = expr_item.get_expr_operator();
  expr_op->set_result_type(res_type);
  ASSERT_EQ(OB_SUCCESS, filer_expr->add_expr_item(expr_item));
  expr_item.set_op(physical_plan.get_allocator(), "=", 2);
  ASSERT_EQ(OB_SUCCESS, filer_expr->add_expr_item(expr_item));
  expr_op = expr_item.get_expr_operator();
  expr_op->set_result_type(res_type);
  /*
   * calculate c1 + 1
   */
  ObColumnExpression *calc_expr = NULL;
  ASSERT_EQ(OB_SUCCESS, ObSqlExpressionUtil::make_sql_expr(&physical_plan, calc_expr));
  ASSERT_FALSE(NULL == calc_expr);
  calc_expr->set_item_count(3);
  calc_expr->set_result_index(row_size);
  ObPostExprItem calc_item;
  calc_item.set_int(1);
  calc_item.set_item_type(T_INT);
  ASSERT_EQ(OB_SUCCESS, calc_expr->add_expr_item(calc_item));
  //column c1
  calc_item.set_column(0);
  ASSERT_EQ(OB_SUCCESS, calc_expr->add_expr_item(calc_item));
  calc_item.set_op(physical_plan.get_allocator(), "+", 2);
  expr_op = calc_item.get_expr_operator();
  expr_op->set_result_type(res_type);
  ASSERT_EQ(OB_SUCCESS, calc_expr->add_expr_item(calc_item));

  root.set_id(1);
  root.set_column_count(row_size + 1);
  root.set_phy_plan(&physical_plan);
  ASSERT_EQ(OB_SUCCESS, root.add_filter(filer_expr));
  ASSERT_EQ(OB_SUCCESS, root.add_compute(calc_expr));
  ASSERT_EQ(OB_SUCCESS, root.set_child(0, table_scan));
  ASSERT_EQ(OB_SUCCESS, root.open(ctx));
  ASSERT_EQ(OB_SUCCESS, root.get_next_row(ctx, row));

  ObObj cmp;
  cmp.set_int(1);
  ASSERT_TRUE(cmp == row->cells_[row_size]);
  for (int64_t i = 1; i < row_size; ++i) {
    ObObj res;
    res.set_int(i);
    ASSERT_TRUE(res == row->cells_[i]);
  }
  printf("row=%s\n", to_cstring(*row));
  ASSERT_EQ(OB_ITER_END, root.get_next_row(ctx, row));
  ASSERT_EQ(OB_SUCCESS, root.close(ctx));
  ASSERT_FALSE(NULL == root.get_phy_plan());
  root.reset();
  root.reuse();
  table_scan.reset();
  table_scan.reuse();
}

TEST_F(ObPhyOperatorTest, test_get_op_name)
{
  ObArenaAllocator alloc;
  ObMergeUnion op(alloc);
  op.set_type(PHY_HASH_UNION);
  const char* name = "PHY_HASH_UNION";
  ASSERT_EQ(0, strcmp(name, op.get_name()));
  int64_t size = strlen(name);
  ASSERT_EQ(size, strlen(op.get_name()));
}

TEST_F(ObPhyOperatorTest, test_invalid_argument)
{
  ObExecContext ctx;
  ObExprCtx expr_ctx;
  ObSingleChildOperatorFake root;
  ObPhysicalPlan physical_plan;
  ObMergeGroupBy *groupby = NULL;
  ObSQLSessionInfo my_session;
  ASSERT_EQ(OB_SUCCESS, my_session.test_init(0,0,0,NULL));
  ctx.set_my_session(&my_session);

  ASSERT_EQ(OB_SUCCESS, physical_plan.alloc_operator_by_type(PHY_MERGE_GROUP_BY, groupby));

  //invalid argument calculate_row() function
  ObColumnExpression *calc_expr = NULL;
  ASSERT_EQ(OB_SUCCESS, ObSqlExpressionUtil::make_sql_expr(&physical_plan, calc_expr));
  ASSERT_FALSE(NULL == calc_expr);
  calc_expr->set_item_count(3);
  calc_expr->set_result_index(3);
  ObPostExprItem calc_item;
  calc_item.set_int(1);
  calc_item.set_item_type(T_INT);
  ASSERT_EQ(OB_SUCCESS, calc_expr->add_expr_item(calc_item));
  //column c1
  calc_item.set_column(0);
  ASSERT_EQ(OB_SUCCESS, calc_expr->add_expr_item(calc_item));
  calc_item.set_op(physical_plan.get_allocator(), "+", 2);
  ASSERT_EQ(OB_SUCCESS, calc_expr->add_expr_item(calc_item));
  root.set_phy_plan(&physical_plan);
  ASSERT_EQ(OB_SUCCESS, root.add_compute(calc_expr));
  ASSERT_EQ(OB_SUCCESS, ctx.init_phy_op(1));
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObPhyOperatorTest::test_calculate_row(ctx, root));

  //invalid argument wrap_expr_ctx()
  ObExecContext invalid_ctx;
  invalid_ctx.set_my_session(&my_session);
  root.reset();
  ASSERT_EQ(OB_ERR_UNEXPECTED, ObPhyOperatorTest::test_wrap_expr_ctx(invalid_ctx, expr_ctx, root));
  ASSERT_EQ(OB_SUCCESS, invalid_ctx.init_phy_op(1));
  ASSERT_EQ(OB_SUCCESS, invalid_ctx.create_physical_plan_ctx());
  ASSERT_EQ(OB_ERR_UNEXPECTED, ObPhyOperatorTest::test_wrap_expr_ctx(invalid_ctx, expr_ctx, root));

  //invalid argument for add filter
  root.reset();
  ASSERT_EQ(OB_INVALID_ARGUMENT, root.add_filter(NULL));
  ASSERT_EQ(OB_INVALID_ARGUMENT, root.add_compute(NULL));
}

TEST_F(ObPhyOperatorTest, test_serialize_and_deserialize)
{
  /*
  int64_t pos = 0;
  const int64_t MAX_SERIALIZE_BUF_LEN = 1024;
  char buf[MAX_SERIALIZE_BUF_LEN] = {'\0'};
  ObTableScanFake table_scan1;
  ObTableScanFake table_scan2;
  ObPhysicalPlan physical_plan;
  ObSqlExpression *filter_expr = NULL;
  ASSERT_EQ(OB_SUCCESS, ObSqlExpressionUtil::make_sql_expr(&physical_plan, filter_expr));
  ASSERT_FALSE(NULL == filter_expr);
  filter_expr->set_item_count(5);
  ObPostExprItem expr_item;
  expr_item.set_int(1);
  expr_item.set_item_type(T_INT);
  ASSERT_EQ(OB_SUCCESS, filter_expr->add_expr_item(expr_item));
  expr_item.set_column(2);
  ASSERT_EQ(OB_SUCCESS, filter_expr->add_expr_item(expr_item));
  expr_item.set_int(1);
  expr_item.set_item_type(T_INT);
  ASSERT_EQ(OB_SUCCESS, filter_expr->add_expr_item(expr_item));
  expr_item.set_op(physical_plan.get_allocator(), "-", 2);
  ASSERT_EQ(OB_SUCCESS, filter_expr->add_expr_item(expr_item));
  expr_item.set_op(physical_plan.get_allocator(), "=", 2);
  ASSERT_EQ(OB_SUCCESS, filter_expr->add_expr_item(expr_item));
  ASSERT_EQ(OB_SUCCESS, filter_expr->serialize(buf, MAX_SERIALIZE_BUF_LEN, pos));
  ASSERT_EQ(pos, filter_expr->get_serialize_size());
  ObColumnExpression *calc_expr = NULL;
  ASSERT_EQ(OB_SUCCESS, ObSqlExpressionUtil::make_sql_expr(&physical_plan, calc_expr));
  ASSERT_FALSE(NULL == calc_expr);
  calc_expr->set_item_count(3);
  calc_expr->set_result_index(row_size);
  ObPostExprItem calc_item;
  calc_item.set_int(1);
  calc_item.set_item_type(T_INT);
  ASSERT_EQ(OB_SUCCESS, calc_expr->add_expr_item(calc_item));
  //column c1
  calc_item.set_column(0);
  ASSERT_EQ(OB_SUCCESS, calc_expr->add_expr_item(calc_item));
  calc_item.set_op(physical_plan.get_allocator(), "+", 2);
  ASSERT_EQ(OB_SUCCESS, calc_expr->add_expr_item(calc_item));
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, calc_expr->serialize(buf, MAX_SERIALIZE_BUF_LEN, pos));
  ASSERT_EQ(pos, calc_expr->get_serialize_size());

  table_scan1.set_id(1);
  table_scan1.set_column_count(row_size + 1);
  table_scan1.set_phy_plan(&physical_plan);
  ASSERT_EQ(OB_SUCCESS, table_scan1.add_filter(filter_expr));
  ASSERT_EQ(OB_SUCCESS, table_scan1.add_compute(calc_expr));

  pos = 0;
  ASSERT_EQ(OB_SUCCESS, table_scan1.serialize(buf, MAX_SERIALIZE_BUF_LEN, pos));
  ASSERT_EQ(pos, table_scan1.get_serialize_size());
  int64_t data_len = pos;
  pos = 0;
  table_scan2.set_phy_plan(&physical_plan);
  ASSERT_EQ(OB_SUCCESS, table_scan2.deserialize(buf, data_len, pos));
  ASSERT_EQ(pos, data_len);
  ASSERT_EQ(0, strcmp(to_cstring(table_scan1), to_cstring(table_scan2)));
  printf("table_scan serialize: %s\ntable_scan deserialize: %s\n",
         to_cstring(table_scan1), to_cstring(table_scan2));
         */
}

TEST_F(ObPhyOperatorTest, physical_plan_size)
{
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
    common::ParamFlag flag_;
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
    } v2_ __maybe_unused;
    common::ObAccuracy accuracy_;  // for const, column, questionmark
    ObItemType  item_type_ __maybe_unused;
  };
  printf("output MockPostItem size=%ld\n", sizeof(MockPostExprItem));
  printf("output ObSqlExpression size=%ld, ObPostfixExpression=%ld\n", sizeof(ObSqlExpression), sizeof(ObPostfixExpression));
}

int main(int argc, char **argv)
{
  init_sql_factories();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
