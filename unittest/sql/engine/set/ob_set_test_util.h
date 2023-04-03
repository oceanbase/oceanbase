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
#ifndef OCEANBASE_UNITTEST_SQL_ENGINE_SET_OB_SET_TEST_UTIL_H_
#define OCEANBASE_UNITTEST_SQL_ENGINE_SET_OB_SET_TEST_UTIL_H_
#include <gtest/gtest.h>
#include "sql/engine/table/ob_fake_table.h"
#include "sql/engine/set/ob_merge_set_operator.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/ob_sql_define.h"
#include "sql/engine/test_engine_util.h"

class TestSetOperatorFactory
{
public:
  TestSetOperatorFactory() {}
  ~TestSetOperatorFactory() {}

  static void init(ObExecContext &ctx, ObMergeSetOperator *set_operator, int64_t col_count, bool distinct,
                   bool set_direction)
  {
    ASSERT_FALSE(NULL == set_operator);
    set_operator->reset();
    set_operator->reuse();
    fake_table1_.reset();
    fake_table1_.reuse();
    fake_table2_.reset();
    fake_table2_.reuse();
    result_table_.reset();
    result_table_.reuse();

    set_operator->init(col_count);
    int32_t projector[3] = {1,2,3};
    fake_table1_.set_column_count(col_count);
    fake_table2_.set_column_count(col_count);
    result_table_.set_column_count(col_count);
    set_operator->set_column_count(col_count);
    fake_table1_.set_projector(projector, col_count);
    fake_table2_.set_projector(projector, col_count);
    result_table_.set_projector(projector, col_count);
    set_operator->set_projector(projector, col_count);
    for (int64_t i = 0; i < col_count; ++i) {
      ASSERT_EQ(OB_SUCCESS, set_operator->add_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset())));
      if (set_direction && (i % 2)) {
        ASSERT_EQ(OB_SUCCESS, set_operator->add_set_direction(oceanbase::sql::NULLS_LAST_DESC));
      } else {
        ASSERT_EQ(OB_SUCCESS, set_operator->add_set_direction(oceanbase::sql::NULLS_FIRST_ASC));
      }
    }

    fake_table1_.set_id(0);
    fake_table2_.set_id(1);
    result_table_.set_id(2);
    set_operator->set_id(3);

    fake_table1_.set_phy_plan(&physical_plan_);
    fake_table2_.set_phy_plan(&physical_plan_);
    result_table_.set_phy_plan(&physical_plan_);
    set_operator->set_phy_plan(&physical_plan_);

    set_operator->create_child_array(2);
    set_operator->set_child(0, fake_table1_);
    set_operator->set_child(1, fake_table2_);
    set_operator->set_distinct(distinct);

    ASSERT_EQ(OB_SUCCESS, ctx.init_phy_op(4));
    ASSERT_EQ(OB_SUCCESS, ctx.create_physical_plan_ctx());
    ASSERT_EQ(OB_SUCCESS, create_test_session(ctx));
    ASSERT_FALSE(NULL == ctx.get_physical_plan_ctx());
    ASSERT_FALSE(NULL == ctx.get_my_session());
    ASSERT_EQ(OB_SUCCESS, ctx.get_my_session()->set_time_zone(ObString("+8:00"), true, true));
  }

  static ObFakeTable &get_fake_table1() { return fake_table1_; }
  static ObFakeTable &get_fake_table2() { return fake_table2_; }
  static ObFakeTable &get_result_table() { return result_table_; }
  static ObPhysicalPlan &get_physical_plan() { return physical_plan_; }
private:
  static ObPhysicalPlan physical_plan_;
  static ObFakeTable fake_table1_;
  static ObFakeTable fake_table2_;
  static ObFakeTable result_table_;
};

ObPhysicalPlan TestSetOperatorFactory::physical_plan_;
ObFakeTable TestSetOperatorFactory::fake_table1_(TestSetOperatorFactory::get_physical_plan().get_allocator());
ObFakeTable TestSetOperatorFactory::fake_table2_(TestSetOperatorFactory::get_physical_plan().get_allocator());
ObFakeTable TestSetOperatorFactory::result_table_(TestSetOperatorFactory::get_physical_plan().get_allocator());

#endif /* OCEANBASE_UNITTEST_SQL_ENGINE_SET_OB_SET_TEST_UTIL_H_ */
