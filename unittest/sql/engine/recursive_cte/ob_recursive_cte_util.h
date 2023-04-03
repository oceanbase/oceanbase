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

#define private  public
#define protected  public
#ifndef OCEANBASE_UNITTEST_SQL_ENGINE_SET_OB_RECURSIVE_CTE_TEST_UTIL_H_
#define OCEANBASE_UNITTEST_SQL_ENGINE_SET_OB_RECURSIVE_CTE_TEST_UTIL_H_
#include <gtest/gtest.h>
#include "common/row/ob_row.h"
#include "sql/engine/table/ob_fake_table.h"
#include "sql/engine/recursive_cte/ob_recursive_union_all.h"
#include "sql/engine/test_engine_util.h"

#define EXPECT_SAME_ROW(my_row, table_op, start_idx, end_idx, collation, args...) \
  if (OB_SUCC(ret)) { \
    ObObj _cells[OB_MAX_COLUMN_NUMBER]; \
    ObNewRow _row; \
    _row.cells_ = _cells; \
    _row.count_ = table_op.get_column_count(); \
    if (OB_SUCCESS != (ret = fill_row(_row, ##args))) { \
      _OB_LOG(WARN, "fail to fill row, ret=%d", ret); \
    } else {\
    	printf("FAKE row=%s\n", to_cstring(my_row));\
    	printf("FAKE except_row=%s\n", to_cstring(_row)); \
      ASSERT_TRUE(_row.count_ == my_row.count_); \
      for (int64_t i = start_idx; i < end_idx; ++i) { \
        printf("FAKE index=%ld, cell=%s, respect_cell=%s\n", i, to_cstring(my_row.cells_[i]), to_cstring(_row.cells_[i])); \
        ASSERT_TRUE(0 == _row.cells_[i].compare(my_row.cells_[i], collation)); \
      } \
    } \
  }

class TestRecursiveCTEFactory
{
public:
  TestRecursiveCTEFactory() {}
  ~TestRecursiveCTEFactory() {}

  static void init(ObExecContext &ctx, ObRecursiveUnionAll *cte_operator, ObFakeCTETable *fake_cte, int64_t col_count)
  {
    ASSERT_FALSE(NULL == cte_operator);
    ASSERT_FALSE(NULL == fake_cte);
    cte_operator->set_fake_cte_table(fake_cte);
    cte_operator->reset();
    cte_operator->reuse();
    fake_table1_planA_.reset();
    fake_table1_planA_.reuse();
    fake_table2_PlanB_.reset();
    fake_table2_PlanB_.reuse();
    result_table_.reset();
    result_table_.reuse();

    cte_operator->init(ctx);
    int32_t projector[3] = {1,2,3};
    fake_table1_planA_.set_column_count(col_count);
    fake_table2_PlanB_.set_column_count(col_count);
    result_table_.set_column_count(col_count);
    cte_operator->set_column_count(col_count);
    fake_cte->set_column_count(col_count);
    fake_table1_planA_.set_projector(projector, col_count);
    fake_table2_PlanB_.set_projector(projector, col_count);
    result_table_.set_projector(projector, col_count);
    cte_operator->set_projector(projector, col_count);
    fake_cte->set_projector(projector, col_count);
    for (int64_t i = 0; i < col_count; ++i) {
    	oceanbase::sql::ObSortColumn col_info;
    	col_info.index_ = i;
    	col_info.cs_type_ = ObCharset::get_default_collation(ObCharset::get_default_charset());
    	cte_operator->add_search_by_col(col_info);
    }

    fake_table1_planA_.set_id(0);
    fake_table2_PlanB_.set_id(1);
    result_table_.set_id(2);
    cte_operator->set_id(3);
    fake_cte->set_id(4);

    fake_table1_planA_.set_phy_plan(&physical_plan_);
    fake_table2_PlanB_.set_phy_plan(&physical_plan_);
    result_table_.set_phy_plan(&physical_plan_);
    cte_operator->set_phy_plan(&physical_plan_);
    fake_cte->set_phy_plan(&physical_plan_);

    cte_operator->set_child(0, fake_table1_planA_);
    cte_operator->set_child(1, fake_table2_PlanB_);

    ASSERT_EQ(OB_SUCCESS, ctx.init_phy_op(5));
    ASSERT_EQ(OB_SUCCESS, ctx.create_physical_plan_ctx());
    ASSERT_EQ(OB_SUCCESS, create_test_session(ctx));
    ASSERT_FALSE(NULL == ctx.get_physical_plan_ctx());
    ASSERT_FALSE(NULL == ctx.get_my_session());
  }

  static void init_depth(ObExecContext &ctx, ObRecursiveUnionAll *cte_operator, ObFakeCTETable *fake_cte, int64_t col_count)
  {
    ASSERT_FALSE(NULL == cte_operator);
    ASSERT_FALSE(NULL == fake_cte);
    cte_operator->set_fake_cte_table(fake_cte);
    cte_operator->reset();
    cte_operator->reuse();
    fake_table1_planA_.reset();
    fake_table1_planA_.reuse();
    fake_table2_PlanB_.reset();
    fake_table2_PlanB_.reuse();
    result_table_.reset();
    result_table_.reuse();

    cte_operator->init(ctx);
    cte_operator->set_search_strategy(ObRecursiveInnerData::SearchStrategyType::DEPTH_FRIST);
    int32_t projector[3] = {1,2,3};
    fake_table1_planA_.set_column_count(col_count);
    fake_table2_PlanB_.set_column_count(col_count);
    result_table_.set_column_count(col_count);
    cte_operator->set_column_count(col_count);
    fake_cte->set_column_count(col_count);
    fake_table1_planA_.set_projector(projector, col_count);
    fake_table2_PlanB_.set_projector(projector, col_count);
    result_table_.set_projector(projector, col_count);
    cte_operator->set_projector(projector, col_count);
    fake_cte->set_projector(projector, col_count);

		oceanbase::sql::ObSortColumn search_col_info;
		search_col_info.index_ = 3;
		search_col_info.cs_type_ = ObCharset::get_default_collation(ObCharset::get_default_charset());
		cte_operator->add_search_by_col(search_col_info);

    fake_table1_planA_.set_id(0);
    fake_table2_PlanB_.set_id(1);
    result_table_.set_id(2);
    cte_operator->set_id(3);
    fake_cte->set_id(4);

    fake_table1_planA_.set_phy_plan(&physical_plan_);
    fake_table2_PlanB_.set_phy_plan(&physical_plan_);
    result_table_.set_phy_plan(&physical_plan_);
    cte_operator->set_phy_plan(&physical_plan_);
    fake_cte->set_phy_plan(&physical_plan_);

    cte_operator->set_child(0, fake_table1_planA_);
    cte_operator->set_child(1, fake_table2_PlanB_);

    ASSERT_EQ(OB_SUCCESS, ctx.init_phy_op(5));
    ASSERT_EQ(OB_SUCCESS, ctx.create_physical_plan_ctx());
    ASSERT_EQ(OB_SUCCESS, create_test_session(ctx));
    ASSERT_FALSE(NULL == ctx.get_physical_plan_ctx());
    ASSERT_FALSE(NULL == ctx.get_my_session());
  }

  static void init_depth_search_by_col3(ObExecContext &ctx, ObRecursiveUnionAll *cte_operator, ObFakeCTETable *fake_cte, int64_t col_count)
  {
    ASSERT_FALSE(NULL == cte_operator);
    ASSERT_FALSE(NULL == fake_cte);
    cte_operator->set_fake_cte_table(fake_cte);
    cte_operator->reset();
    cte_operator->reuse();
    fake_table1_planA_.reset();
    fake_table1_planA_.reuse();
    fake_table2_PlanB_.reset();
    fake_table2_PlanB_.reuse();
    result_table_.reset();
    result_table_.reuse();

    cte_operator->init(ctx);
    cte_operator->set_search_strategy(ObRecursiveInnerData::SearchStrategyType::DEPTH_FRIST);
    int32_t projector[3] = {1,2,3};
    fake_table1_planA_.set_column_count(col_count);
    fake_table2_PlanB_.set_column_count(col_count);
    result_table_.set_column_count(col_count);
    cte_operator->set_column_count(col_count);
    fake_cte->set_column_count(col_count);
    fake_table1_planA_.set_projector(projector, col_count);
    fake_table2_PlanB_.set_projector(projector, col_count);
    result_table_.set_projector(projector, col_count);
    cte_operator->set_projector(projector, col_count);
    fake_cte->set_projector(projector, col_count);

		oceanbase::sql::ObSortColumn search_col_info;
		search_col_info.index_ = 3;
		search_col_info.cs_type_ = ObCharset::get_default_collation(ObCharset::get_default_charset());
		cte_operator->add_search_by_col(search_col_info);

    fake_table1_planA_.set_id(0);
    fake_table2_PlanB_.set_id(1);
    result_table_.set_id(2);
    cte_operator->set_id(3);
    fake_cte->set_id(4);

    fake_table1_planA_.set_phy_plan(&physical_plan_);
    fake_table2_PlanB_.set_phy_plan(&physical_plan_);
    result_table_.set_phy_plan(&physical_plan_);
    cte_operator->set_phy_plan(&physical_plan_);
    fake_cte->set_phy_plan(&physical_plan_);

    cte_operator->set_child(0, fake_table1_planA_);
    cte_operator->set_child(1, fake_table2_PlanB_);

    ASSERT_EQ(OB_SUCCESS, ctx.init_phy_op(5));
    ASSERT_EQ(OB_SUCCESS, ctx.create_physical_plan_ctx());
    ASSERT_EQ(OB_SUCCESS, create_test_session(ctx));
    ASSERT_FALSE(NULL == ctx.get_physical_plan_ctx());
    ASSERT_FALSE(NULL == ctx.get_my_session());
  }

  static void init_breadth_search_by_col3_cyc_by_col2(ObExecContext &ctx, ObRecursiveUnionAll *cte_operator, ObFakeCTETable *fake_cte, int64_t col_count)
  {
    ASSERT_FALSE(NULL == cte_operator);
    ASSERT_FALSE(NULL == fake_cte);
    cte_operator->set_fake_cte_table(fake_cte);
    cte_operator->reset();
    cte_operator->reuse();
    fake_table1_planA_.reset();
    fake_table1_planA_.reuse();
    fake_table2_PlanB_.reset();
    fake_table2_PlanB_.reuse();
    result_table_.reset();
    result_table_.reuse();

    cte_operator->init(ctx);
    cte_operator->set_search_strategy(ObRecursiveInnerData::SearchStrategyType::BREADTH_FRIST);
    int32_t projector[3] = {1,2,3};
    fake_table1_planA_.set_column_count(col_count);
    fake_table2_PlanB_.set_column_count(col_count);
    result_table_.set_column_count(col_count);
    cte_operator->set_column_count(col_count);
    fake_cte->set_column_count(col_count);
    fake_table1_planA_.set_projector(projector, col_count);
    fake_table2_PlanB_.set_projector(projector, col_count);
    result_table_.set_projector(projector, col_count);
    cte_operator->set_projector(projector, col_count);
    fake_cte->set_projector(projector, col_count);

		oceanbase::sql::ObSortColumn search_col_info;
		search_col_info.index_ = 3;
		search_col_info.cs_type_ = ObCharset::get_default_collation(ObCharset::get_default_charset());
		cte_operator->add_search_by_col(search_col_info);

		oceanbase::common::ObColumnInfo cycle_col_info;
		cycle_col_info.index_ = 2;
		cycle_col_info.cs_type_ = ObCharset::get_default_collation(ObCharset::get_default_charset());
		cte_operator->add_cycle_by_col(cycle_col_info);

    fake_table1_planA_.set_id(0);
    fake_table2_PlanB_.set_id(1);
    result_table_.set_id(2);
    cte_operator->set_id(3);
    fake_cte->set_id(4);

    fake_table1_planA_.set_phy_plan(&physical_plan_);
    fake_table2_PlanB_.set_phy_plan(&physical_plan_);
    result_table_.set_phy_plan(&physical_plan_);
    cte_operator->set_phy_plan(&physical_plan_);
    fake_cte->set_phy_plan(&physical_plan_);

    cte_operator->set_child(0, fake_table1_planA_);
    cte_operator->set_child(1, fake_table2_PlanB_);

    ASSERT_EQ(OB_SUCCESS, ctx.init_phy_op(5));
    ASSERT_EQ(OB_SUCCESS, ctx.create_physical_plan_ctx());
    ASSERT_EQ(OB_SUCCESS, create_test_session(ctx));
    ASSERT_FALSE(NULL == ctx.get_physical_plan_ctx());
    ASSERT_FALSE(NULL == ctx.get_my_session());
  }

  static void init_depth_search_by_col3_cyc_by_col2(ObExecContext &ctx, ObRecursiveUnionAll *cte_operator, ObFakeCTETable *fake_cte, int64_t col_count)
  {
    ASSERT_FALSE(NULL == cte_operator);
    ASSERT_FALSE(NULL == fake_cte);
    cte_operator->set_fake_cte_table(fake_cte);
    cte_operator->reset();
    cte_operator->reuse();
    fake_table1_planA_.reset();
    fake_table1_planA_.reuse();
    fake_table2_PlanB_.reset();
    fake_table2_PlanB_.reuse();
    result_table_.reset();
    result_table_.reuse();

    cte_operator->init(ctx);
    cte_operator->set_search_strategy(ObRecursiveInnerData::SearchStrategyType::DEPTH_FRIST);
    int32_t projector[3] = {1,2,3};
    fake_table1_planA_.set_column_count(col_count);
    fake_table2_PlanB_.set_column_count(col_count);
    result_table_.set_column_count(col_count);
    cte_operator->set_column_count(col_count);
    fake_cte->set_column_count(col_count);
    fake_table1_planA_.set_projector(projector, col_count);
    fake_table2_PlanB_.set_projector(projector, col_count);
    result_table_.set_projector(projector, col_count);
    cte_operator->set_projector(projector, col_count);
    fake_cte->set_projector(projector, col_count);

    oceanbase::sql::ObSortColumn search_col_info;
		search_col_info.index_ = 3;
		search_col_info.cs_type_ = ObCharset::get_default_collation(ObCharset::get_default_charset());
		cte_operator->add_search_by_col(search_col_info);

		oceanbase::common::ObColumnInfo cycle_col_info;
		cycle_col_info.index_ = 2;
		cycle_col_info.cs_type_ = ObCharset::get_default_collation(ObCharset::get_default_charset());
		cte_operator->add_cycle_by_col(cycle_col_info);

    fake_table1_planA_.set_id(0);
    fake_table2_PlanB_.set_id(1);
    result_table_.set_id(2);
    cte_operator->set_id(3);
    fake_cte->set_id(4);

    fake_table1_planA_.set_phy_plan(&physical_plan_);
    fake_table2_PlanB_.set_phy_plan(&physical_plan_);
    result_table_.set_phy_plan(&physical_plan_);
    cte_operator->set_phy_plan(&physical_plan_);
    fake_cte->set_phy_plan(&physical_plan_);

    cte_operator->set_child(0, fake_table1_planA_);
    cte_operator->set_child(1, fake_table2_PlanB_);

    ASSERT_EQ(OB_SUCCESS, ctx.init_phy_op(5));
    ASSERT_EQ(OB_SUCCESS, ctx.create_physical_plan_ctx());
    ASSERT_EQ(OB_SUCCESS, create_test_session(ctx));
    ASSERT_FALSE(NULL == ctx.get_physical_plan_ctx());
    ASSERT_FALSE(NULL == ctx.get_my_session());
  }


  static ObFakeTable &get_planA_op_() { return fake_table1_planA_; }
  static ObFakeTable &get_planB_op_() { return fake_table2_PlanB_; }
  static ObFakeTable &get_result_table() { return result_table_; }
  static ObPhysicalPlan &get_physical_plan() { return physical_plan_; }
private:
  static ObPhysicalPlan physical_plan_;
  static ObFakeTable fake_table1_planA_;
  static ObFakeTable fake_table2_PlanB_;
  static ObFakeTable result_table_;
};

ObPhysicalPlan TestRecursiveCTEFactory::physical_plan_;
ObFakeTable TestRecursiveCTEFactory::fake_table1_planA_(TestRecursiveCTEFactory::get_physical_plan().get_allocator());
ObFakeTable TestRecursiveCTEFactory::fake_table2_PlanB_(TestRecursiveCTEFactory::get_physical_plan().get_allocator());
ObFakeTable TestRecursiveCTEFactory::result_table_(TestRecursiveCTEFactory::get_physical_plan().get_allocator());

#endif /* OCEANBASE_UNITTEST_SQL_ENGINE_SET_OB_RECURSIVE_CTE_TEST_UTIL_H_ */
