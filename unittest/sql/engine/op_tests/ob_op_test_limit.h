/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_LIMIT_H_
#define OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_LIMIT_H_

#include "unittest/sql/engine/op_tests/ob_op_test_base.h"
#include "sql/engine/basic/ob_limit_op.h"
#include "sql/engine/basic/ob_limit_vec_op.h"

namespace oceanbase
{
namespace sql
{

/**
 * @brief LimitTestSpec - Test specification for Limit operator.
 *
 * Supports both 1.0 (ObLimitOp/PHY_LIMIT) and 2.0 (ObLimitVecOp/PHY_VEC_LIMIT) operators.
 * The operator version is selected at create_spec() time via the use_rich_format parameter.
 *
 * Usage:
 *   LimitTestSpec spec;
 *   spec.table("t", "a int, b int")
 *       .select("a, b")
 *       .with_data({{1, 2}, {3, 4}, {5, 6}})
 *       .limit(2)
 *       .run(engine);
 *
 * Advanced usage:
 *   // LIMIT with OFFSET
 *   spec.limit(2).offset(1).run(engine);
 *
 *   // Dual-format comparison (runs both 1.0 and 2.0, verifies identical results)
 *   spec.limit(2).enable_dual_format_check().run(engine);
 */
class LimitTestSpec : public OpSpecBuilder<LimitTestSpec>
{
public:
  LimitTestSpec() : is_top_limit_(true) {}
  ~LimitTestSpec() = default;

  /**
   * @brief Set whether this is a top-level limit.
   * @param is_top true for top-level limit (default)
   */
  LimitTestSpec& top_limit(bool is_top = true)
  {
    is_top_limit_ = is_top;
    return *this;
  }

  /**
   * @brief Create ObLimitVecSpec (2.0) or ObLimitSpec (1.0) based on use_rich_format.
   * Both spec types share the same field names, so a template helper avoids duplication.
   */
  ObOpSpec *create_spec(common::ObIAllocator &alloc, MockDataSourceSpec *child_spec,
                         const ExprFixedArray &output_exprs,
                         ObExpr *limit_expr, ObExpr *offset_expr, bool use_rich_format)
  {
    if (use_rich_format) {
      return create_spec_impl<ObLimitVecSpec>(alloc, PHY_VEC_LIMIT, child_spec, output_exprs,
                                              limit_expr, offset_expr, use_rich_format);
    } else {
      return create_spec_impl<ObLimitSpec>(alloc, PHY_LIMIT, child_spec, output_exprs,
                                           limit_expr, offset_expr, use_rich_format);
    }
  }

  /**
   * @brief Create ObLimitVecOp (2.0) or ObLimitOp (1.0) based on spec type.
   */
  ObOperator *create_op(ObExecContext &ctx, ObOpSpec &spec, ObOperator *child_op)
  {
    if (spec.type_ == PHY_VEC_LIMIT) {
      return default_create_op<ObLimitVecOp>(ctx, spec, child_op);
    } else {
      return default_create_op<ObLimitOp>(ctx, spec, child_op);
    }
  }

  /**
   * @brief Fallback create_op when no parent spec exists.
   */
  ObOperator *create_op(ObExecContext &ctx, ObOperator *child_op)
  {
    return child_op;
  }

private:
  /**
   * @brief Templated spec creation for both 1.0 (ObLimitSpec) and 2.0 (ObLimitVecSpec).
   * Both spec types share the same public field names.
   */
  template <typename SpecType>
  SpecType *create_spec_impl(common::ObIAllocator &alloc, ObPhyOperatorType phy_type,
                              MockDataSourceSpec *child_spec,
                              const ExprFixedArray &output_exprs,
                              ObExpr *limit_expr, ObExpr *offset_expr, bool use_rich_format)
  {
    int ret = OB_SUCCESS;
    void *mem = alloc.alloc(sizeof(SpecType));
    if (OB_ISNULL(mem)) {
      LOG_WARN("alloc spec failed", K(ret));
      return nullptr;
    }
    SpecType *limit_spec = new (mem) SpecType(alloc, phy_type);
    limit_spec->limit_expr_ = limit_expr;
    limit_spec->offset_expr_ = offset_expr;
    limit_spec->percent_expr_ = nullptr;
    limit_spec->calc_found_rows_ = false;
    limit_spec->is_top_limit_ = is_top_limit_;
    limit_spec->is_fetch_with_ties_ = false;
    limit_spec->plan_ = child_spec->plan_;
    limit_spec->max_batch_size_ = child_spec->max_batch_size_;
    limit_spec->use_rich_format_ = use_rich_format;
    limit_spec->output_ = output_exprs;

    void *child_spec_mem = alloc.alloc(sizeof(ObOpSpec *));
    if (OB_ISNULL(child_spec_mem)) {
      LOG_WARN("alloc child spec array failed", K(ret));
      return nullptr;
    }
    ObOpSpec **children = reinterpret_cast<ObOpSpec **>(child_spec_mem);
    children[0] = child_spec;
    if (OB_FAIL(limit_spec->set_children_pointer(children, 1))) {
      LOG_WARN("set children pointer failed", K(ret));
      return nullptr;
    }
    return limit_spec;
  }

  bool is_top_limit_;
};

}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_LIMIT_H_
