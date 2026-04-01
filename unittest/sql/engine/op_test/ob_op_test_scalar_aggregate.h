/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_SCALAR_AGGREGATE_H_
#define OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_SCALAR_AGGREGATE_H_

#include "unittest/sql/engine/op_test/ob_op_test_base.h"
#include "sql/engine/aggregate/ob_scalar_aggregate_vec_op.h"
#include "sql/engine/aggregate/ob_groupby_vec_op.h"
#include "sql/engine/ob_operator.h"
#include "sql/code_generator/ob_static_engine_expr_cg.h"

namespace oceanbase
{
namespace sql
{

/**
 * @brief ScalarAggTestSpec - Test specification for Scalar Aggregate operator.
 *
 * Scalar aggregate aggregates all rows into a single result row (no GROUP BY).
 *
 * Usage:
 *   ScalarAggTestSpec spec;
 *   spec.table("t", "a int, b int")
 *       .select("COUNT(*), SUM(a), AVG(b)")
 *       .with_data({{1, 2}, {3, 4}, {5, 6}})
 *       .run(engine);
 *
 * Advanced usage:
 *   // Empty table behavior
 *   spec.select("COUNT(*), SUM(a)")
 *       .with_data({})
 *       .run(engine);  // Returns 1 row: (0, NULL)
 *
 *   // COUNT DISTINCT
 *   spec.select("COUNT(DISTINCT a)")
 *       .enable_hash_base_distinct(true)
 *       .run(engine);
 */
class ScalarAggTestSpec : public OpSpecBuilder<ScalarAggTestSpec>
{
public:
  ScalarAggTestSpec()
    : can_return_empty_set_(false),
      enable_hash_base_distinct_(false)
  {}
  ~ScalarAggTestSpec() = default;

  // ===== Configuration Interface =====

  /**
   * @brief Set whether empty input can return empty result.
   * Default is false: empty input returns 1 row with NULLs (except COUNT = 0).
   * @param enable true to allow empty result set
   */
  ScalarAggTestSpec& can_return_empty_set(bool enable = true)
  {
    can_return_empty_set_ = enable;
    return *this;
  }

  /**
   * @brief Enable hash-based distinct aggregation.
   * Used for COUNT(DISTINCT), SUM(DISTINCT), etc.
   * @param enable true to enable
   */
  ScalarAggTestSpec& enable_hash_base_distinct(bool enable = true)
  {
    enable_hash_base_distinct_ = enable;
    return *this;
  }

  // ===== Create Spec =====

  /**
   * @brief Create ObScalarAggregateVecSpec with aggr_infos_ populated.
   * @param alloc Allocator
   * @param child_spec MockDataSourceSpec (child)
   * @param output_exprs The real output expressions (aggregate result expressions)
   * @param limit_expr LIMIT expression (unused for scalar agg)
   * @param offset_expr OFFSET expression (unused for scalar agg)
   * @param use_rich_format Whether to use rich format (from session setting)
   * @return ObScalarAggregateVecSpec instance
   */
  ObOpSpec *create_spec(common::ObIAllocator &alloc, MockDataSourceSpec *child_spec,
                         const ExprFixedArray &output_exprs,
                         ObExpr *limit_expr, ObExpr *offset_expr, bool use_rich_format)
  {
    int ret = OB_SUCCESS;
    // Allocate ObScalarAggregateVecSpec
    void *mem = alloc.alloc(sizeof(ObScalarAggregateVecSpec));
    if (OB_ISNULL(mem)) {
      LOG_WARN("alloc ObScalarAggregateVecSpec failed", K(ret));
      return nullptr;
    }

    ObScalarAggregateVecSpec *agg_spec = new (mem) ObScalarAggregateVecSpec(alloc, PHY_SCALAR_AGGREGATE);

    // Set plan pointer for open() to work
    agg_spec->plan_ = child_spec->plan_;
    agg_spec->max_batch_size_ = child_spec->max_batch_size_;
    agg_spec->use_rich_format_ = use_rich_format;

    // IMPORTANT: Set output_ to the real aggregate result expressions
    agg_spec->output_ = output_exprs;

    void *child_spec_mem = alloc.alloc(sizeof(ObOpSpec *));
    if (OB_ISNULL(child_spec_mem)) {
      LOG_WARN("alloc child spec array failed", K(ret));
      return nullptr;
    }
    ObOpSpec **children = reinterpret_cast<ObOpSpec **>(child_spec_mem);
    children[0] = child_spec;
    if (OB_FAIL(agg_spec->set_children_pointer(children, 1))) {
      LOG_WARN("set children pointer failed", K(ret));
      return nullptr;
    }

    return agg_spec;
  }

  // ===== Create Operator =====

  /**
   * @brief Create ObScalarAggregateVecOp with MockDataSourceOp as child.
   * @param ctx Execution context
   * @param spec The spec (ObScalarAggregateVecSpec)
   * @param child_op The child operator (MockDataSourceOp)
   * @return ObScalarAggregateVecOp instance
   */
  ObOperator *create_op(ObExecContext &ctx, ObOpSpec &spec, ObOperator *child_op)
  {
    int ret = OB_SUCCESS;

    // Allocate ObScalarAggregateVecOp
    void *mem = ctx.get_allocator().alloc(sizeof(ObScalarAggregateVecOp));
    if (OB_ISNULL(mem)) {
      LOG_WARN("alloc ObScalarAggregateVecOp failed", K(ret));
      return nullptr;
    }

    ObScalarAggregateVecOp *agg_op = new (mem) ObScalarAggregateVecOp(ctx, spec, nullptr);

    // Allocate children array and set child operator
    void *children_mem = ctx.get_allocator().alloc(sizeof(ObOperator *));
    if (OB_ISNULL(children_mem)) {
      LOG_WARN("alloc children array failed", K(ret));
      return nullptr;
    }
    ObOperator **children = reinterpret_cast<ObOperator **>(children_mem);
    children[0] = child_op;

    if (OB_FAIL(agg_op->set_children_pointer(children, 1))) {
      LOG_WARN("set children pointer failed", K(ret));
      return nullptr;
    }
    // Also set child_ member via set_child
    if (OB_FAIL(agg_op->set_child(0, child_op))) {
      LOG_WARN("set child failed", K(ret));
      return nullptr;
    }

    return agg_op;
  }

  /**
   * @brief Fallback create_op when no parent spec exists.
   * Returns child_op directly (pass-through mode).
   */
  ObOperator *create_op(ObExecContext &ctx, ObOperator *child_op)
  {
    return child_op;
  }

private:
  bool can_return_empty_set_;
  bool enable_hash_base_distinct_;
};

}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_SCALAR_AGGREGATE_H_