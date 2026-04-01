/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_LIMIT_H_
#define OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_LIMIT_H_

#include "unittest/sql/engine/op_test/ob_op_test_base.h"
#include "sql/engine/basic/ob_limit_vec_op.h"

namespace oceanbase
{
namespace sql
{

/**
 * @brief LimitTestSpec - Test specification for Limit operator.
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
   * @brief Create ObLimitVecSpec with MockDataSourceSpec as child.
   * @param output_exprs The real output expressions (SELECT expressions)
   */
  ObOpSpec *create_spec(common::ObIAllocator &alloc, MockDataSourceSpec *child_spec,
                         const ExprFixedArray &output_exprs,
                         ObExpr *limit_expr, ObExpr *offset_expr, bool use_rich_format)
  {
    int ret = OB_SUCCESS;
    void *mem = alloc.alloc(sizeof(ObLimitVecSpec));
    if (OB_ISNULL(mem)) {
      LOG_WARN("alloc ObLimitVecSpec failed", K(ret));
      return nullptr;
    }

    ObLimitVecSpec *limit_spec = new (mem) ObLimitVecSpec(alloc, PHY_VEC_LIMIT);
    limit_spec->limit_expr_ = limit_expr;
    limit_spec->offset_expr_ = offset_expr;
    limit_spec->percent_expr_ = nullptr;
    limit_spec->calc_found_rows_ = false;
    limit_spec->is_top_limit_ = is_top_limit_;
    limit_spec->is_fetch_with_ties_ = false;

    limit_spec->plan_ = child_spec->plan_;
    limit_spec->max_batch_size_ = child_spec->max_batch_size_;
    limit_spec->use_rich_format_ = use_rich_format;
    // IMPORTANT: Set output_ to the real SELECT expressions, not child's columns
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

  /**
   * @brief Create ObLimitVecOp with MockDataSourceOp as child.
   */
  ObOperator *create_op(ObExecContext &ctx, ObOpSpec &spec, ObOperator *child_op)
  {
    int ret = OB_SUCCESS;
    void *mem = ctx.get_allocator().alloc(sizeof(ObLimitVecOp));
    if (OB_ISNULL(mem)) {
      LOG_WARN("alloc ObLimitVecOp failed", K(ret));
      return nullptr;
    }

    ObLimitVecOp *limit_op = new (mem) ObLimitVecOp(ctx, spec, nullptr);

    void *children_mem = ctx.get_allocator().alloc(sizeof(ObOperator *));
    if (OB_ISNULL(children_mem)) {
      LOG_WARN("alloc children array failed", K(ret));
      return nullptr;
    }
    ObOperator **children = reinterpret_cast<ObOperator **>(children_mem);
    children[0] = child_op;

    if (OB_FAIL(limit_op->set_children_pointer(children, 1))) {
      LOG_WARN("set children pointer failed", K(ret));
      return nullptr;
    }
    return limit_op;
  }

  /**
   * @brief Fallback create_op when no parent spec exists.
   */
  ObOperator *create_op(ObExecContext &ctx, ObOperator *child_op)
  {
    return child_op;
  }

private:
  bool is_top_limit_;
};

}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_LIMIT_H_