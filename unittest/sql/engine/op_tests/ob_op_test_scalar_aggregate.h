/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_SCALAR_AGGREGATE_H_
#define OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_SCALAR_AGGREGATE_H_

#include "unittest/sql/engine/op_tests/ob_op_test_base.h"
#include "sql/engine/aggregate/ob_scalar_aggregate_vec_op.h"  // includes ob_scalar_aggregate_op.h
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
 * Supports both 1.0 (ObScalarAggregateOp/PHY_SCALAR_AGGREGATE) and
 * 2.0 (ObScalarAggregateVecOp/PHY_VEC_SCALAR_AGGREGATE) operators.
 * The operator version is selected at create_spec() time via the use_rich_format parameter.
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
 *
 *   // Dual-format comparison (runs both 1.0 and 2.0, verifies identical results)
 *   spec.select("COUNT(*), SUM(a)")
 *       .enable_dual_format_check()
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
   * NOTE: This option only applies to the 2.0 path (ObScalarAggregateVecSpec).
   *       The 1.0 spec (ObScalarAggregateSpec) does not support this flag.
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
   * @brief Create ObScalarAggregateVecSpec (2.0) or ObScalarAggregateSpec (1.0).
   *
   * For 2.0: uses PHY_VEC_SCALAR_AGGREGATE, supports can_return_empty_set_.
   * For 1.0: uses PHY_SCALAR_AGGREGATE, can_return_empty_set_ is ignored.
   *
   * NOTE: This method fills aggr_infos_ using the context set by run():
   *   - resolved_stmt_ (for get_aggr_items())
   *   - phy_plan_ (for OpTestAggrCG)
   *   - child_output_exprs_ (for all_expr_)
   */
  ObOpSpec *create_spec(common::ObIAllocator &alloc, MockDataSourceSpec *child_spec,
                         const ExprFixedArray &output_exprs,
                         ObExpr *limit_expr, ObExpr *offset_expr, bool use_rich_format)
  {
    int ret = OB_SUCCESS;
    if (use_rich_format) {
      // 2.0: ObScalarAggregateVecSpec
      void *mem = alloc.alloc(sizeof(ObScalarAggregateVecSpec));
      if (OB_ISNULL(mem)) {
        LOG_WARN("alloc ObScalarAggregateVecSpec failed", K(ret));
        return nullptr;
      }
      ObScalarAggregateVecSpec *agg_spec = new (mem) ObScalarAggregateVecSpec(
          alloc, PHY_VEC_SCALAR_AGGREGATE);
      agg_spec->plan_ = child_spec->plan_;
      agg_spec->max_batch_size_ = child_spec->max_batch_size_;
      agg_spec->use_rich_format_ = use_rich_format;
      agg_spec->output_ = output_exprs;
      agg_spec->enable_hash_base_distinct_ = enable_hash_base_distinct_;
      if (can_return_empty_set_) {
        agg_spec->set_cant_return_empty_set();
      }

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

      ret = fill_aggr_infos_for_groupby(alloc, agg_spec, resolved_stmt_, phy_plan_, use_rich_format);
      if (OB_FAIL(ret)) {
        LOG_WARN("fill_aggr_infos_for_groupby failed", K(ret));
      }
      return agg_spec;
    } else {
      // 1.0: ObScalarAggregateSpec (no can_return_empty_set_ support)
      void *mem = alloc.alloc(sizeof(ObScalarAggregateSpec));
      if (OB_ISNULL(mem)) {
        LOG_WARN("alloc ObScalarAggregateSpec failed", K(ret));
        return nullptr;
      }
      ObScalarAggregateSpec *agg_spec = new (mem) ObScalarAggregateSpec(
          alloc, PHY_SCALAR_AGGREGATE);
      agg_spec->plan_ = child_spec->plan_;
      agg_spec->max_batch_size_ = child_spec->max_batch_size_;
      agg_spec->use_rich_format_ = use_rich_format;
      agg_spec->output_ = output_exprs;
      agg_spec->enable_hash_base_distinct_ = enable_hash_base_distinct_;

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

      ret = fill_aggr_infos_for_groupby(alloc, agg_spec, resolved_stmt_, phy_plan_, use_rich_format);
      if (OB_FAIL(ret)) {
        LOG_WARN("fill_aggr_infos_for_groupby failed", K(ret));
      }
      return agg_spec;
    }
  }

  // ===== Create Operator =====

  /**
   * @brief Create ObScalarAggregateVecOp (2.0) or ObScalarAggregateOp (1.0) based on spec type.
   */
  ObOperator *create_op(ObExecContext &ctx, ObOpSpec &spec, ObOperator *child_op)
  {
    if (spec.type_ == PHY_VEC_SCALAR_AGGREGATE) {
      return default_create_op<ObScalarAggregateVecOp>(ctx, spec, child_op);
    } else {
      return default_create_op<ObScalarAggregateOp>(ctx, spec, child_op);
    }
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
