/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_KIT_H_
#define OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_KIT_H_

#include <gtest/gtest.h>
#include "unittest/sql/engine/op_tests/ob_op_test_engine.h"
#include "unittest/sql/engine/op_tests/ob_op_test_material.h"
#include "unittest/sql/engine/op_tests/ob_op_test_limit.h"
#include "unittest/sql/engine/op_tests/ob_op_test_scalar_aggregate.h"
#include "unittest/sql/engine/op_tests/ob_op_test_window_function.h"
#include "unittest/sql/engine/op_tests/ob_op_test_merge_groupby.h"
#include "unittest/sql/engine/op_tests/ob_op_test_hash_groupby.h"
#include "unittest/sql/engine/op_tests/ob_op_test_expand.h"
#include "unittest/sql/engine/op_tests/ob_op_test_base.h"

namespace oceanbase
{
namespace sql
{

/**
 * @brief OpTestKit - Base test fixture for operator unit tests.
 *
 * Provides:
 * - OpTestEngine instance with lifecycle management
 * - Factory methods for creating test specifications
 *
 * Usage:
 *   class MyTest : public OpTestKit {};
 *   TEST_F(MyTest, BasicTest) {
 *     material_test().table("t", "a int").select("a").with_data({{1}}).run(engine_);
 *   }
 */
class OpTestKit : public ::testing::Test
{
protected:
  OpTestKit() {}
  virtual ~OpTestKit() {}

  virtual void SetUp() override
  {
    engine_.init();
  }

  virtual void TearDown() override
  {
    engine_.destroy();
  }

  // ===== Factory Methods =====

  MaterialTestSpec material_test() { return MaterialTestSpec(); }
  ExprTestSpec expr_unit_test() { return ExprTestSpec(); }
  LimitTestSpec limit_test() { return LimitTestSpec(); }
  ScalarAggTestSpec scalar_agg_test() { return ScalarAggTestSpec(); }
  WindowFunctionTestSpec window_function_test() { return WindowFunctionTestSpec(); }
  MergeGroupByTestSpec merge_groupby_test() { return MergeGroupByTestSpec(); }
  HashGroupByTestSpec hash_groupby_test() { return HashGroupByTestSpec(); }
  ExpandTestSpec expand_test() { return ExpandTestSpec(); }

protected:
  OpTestEngine engine_;
};

}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_KIT_H_