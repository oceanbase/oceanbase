/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_KIT_H_
#define OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_KIT_H_

#include <gtest/gtest.h>
#include "unittest/sql/engine/op_test/ob_op_test_engine.h"
#include "unittest/sql/engine/op_test/ob_op_test_material.h"
#include "unittest/sql/engine/op_test/ob_op_test_limit.h"
#include "unittest/sql/engine/op_test/ob_op_test_scalar_aggregate.h"
#include "unittest/sql/engine/op_test/ob_op_test_base.h"

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

  /**
   * @brief Create a Material operator test specification.
   */
  MaterialTestSpec material_test()
  {
    return MaterialTestSpec();
  }

  /**
   * @brief Create an expression unit test specification.
   * Convenience wrapper for testing expressions without operator-specific logic.
   */
  ExprTestSpec expr_unit_test()
  {
    return ExprTestSpec();
  }

  /**
   * @brief Create a Limit operator test specification.
   */
  LimitTestSpec limit_test()
  {
    return LimitTestSpec();
  }

  /**
   * @brief Create a Scalar Aggregate operator test specification.
   */
  ScalarAggTestSpec scalar_agg_test()
  {
    return ScalarAggTestSpec();
  }

protected:
  OpTestEngine engine_;
};

}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_KIT_H_