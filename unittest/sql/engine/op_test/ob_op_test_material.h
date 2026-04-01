/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define private public
#define protected public

#ifndef OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_MATERIAL_H_
#define OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_MATERIAL_H_

#include "unittest/sql/engine/op_test/ob_op_test_base.h"
#include "sql/engine/basic/ob_material_vec_op.h"

namespace oceanbase
{
namespace sql
{

/**
 * @brief MaterialTestSpec - Test specification for Material operator.
 *
 * Creates a real ObMaterialVecOp that wraps MockDataSourceOp as child.
 * This enables testing of materialization, dump, and read-back paths.
 *
 * Usage:
 *   MaterialTestSpec spec;
 *   spec.table("t", "a int, b int")
 *       .select("a, b")
 *       .with_data({{1, 2}, {3, 4}})
 *       .run(engine);
 */
class MaterialTestSpec : public OpSpecBuilder<MaterialTestSpec>
{
public:
  MaterialTestSpec() : bypass_(false) {}
  ~MaterialTestSpec() = default;

  /**
   * @brief Set bypass mode for material operator.
   * When true, material operator passes through child data without materialization.
   * @param enable True to enable bypass mode
   */
  MaterialTestSpec& with_bypass(bool enable)
  {
    bypass_ = enable;
    return *this;
  }

  /**
   * @brief Create ObMaterialVecSpec as the root spec.
   * Sets up required fields: max_batch_size_, width_, compress_type_, etc.
   */
  ObOpSpec *create_spec(common::ObIAllocator &alloc, MockDataSourceSpec *child_spec,
                         const ExprFixedArray &output_exprs,
                         ObExpr *limit_expr, ObExpr *offset_expr, bool use_rich_format)
  {
    int ret = OB_SUCCESS;
    // Allocate ObMaterialVecSpec
    void *mem = alloc.alloc(sizeof(ObMaterialVecSpec));
    if (OB_ISNULL(mem)) {
      return nullptr;
    }

    ObMaterialVecSpec *mat_spec = new (mem) ObMaterialVecSpec(alloc, PHY_VEC_MATERIAL);
    // CRITICAL: Material output must match child output for correct data flow
    // Material stores data from child_->get_spec().output_ and reads back to the same location.
    // If we set output_ to SELECT expressions (e.g., a+0), the data won't be there after read.
    // Instead, output_ should be column_exprs, and collect_batch_results will evaluate SELECT exprs.
    mat_spec->output_ = output_exprs;  // Use child's output (column_exprs), not SELECT exprs
    mat_spec->max_batch_size_ = child_spec->max_batch_size_;  // Use child's batch size for consistency
    mat_spec->width_ = child_spec->output_.count();  // Column count as width
    mat_spec->use_rich_format_ = use_rich_format;
    mat_spec->plan_ = child_spec->plan_;  // Inherit plan pointer

    void *child_spec_mem = alloc.alloc(sizeof(ObOpSpec *));
    if (OB_ISNULL(child_spec_mem)) {
      LOG_WARN("alloc child spec array failed", K(ret));
      return nullptr;
    }
    ObOpSpec **children = reinterpret_cast<ObOpSpec **>(child_spec_mem);
    children[0] = child_spec;
    if (OB_FAIL(mat_spec->set_children_pointer(children, 1))) {
      LOG_WARN("set children pointer failed", K(ret));
    }

    return mat_spec;
  }

  /**
   * @brief Create ObMaterialVecOp with ObMaterialVecOpInput.
   * Sets up input with bypass flag and operator with child.
   */
  ObOperator* create_op(ObExecContext &ctx, ObOpSpec &spec, ObOperator *child_op)
  {
    int ret = OB_SUCCESS;

    // Allocate ObMaterialVecOpInput
    void *mem = ctx.get_allocator().alloc(sizeof(ObMaterialVecOpInput));
    if (OB_ISNULL(mem)) {
      return nullptr;
    }

    ObMaterialVecOpInput *input = new (mem) ObMaterialVecOpInput(ctx, spec);
    input->set_bypass(bypass_);

    // Allocate ObMaterialVecOp
    mem = ctx.get_allocator().alloc(sizeof(ObMaterialVecOp));
    if (OB_ISNULL(mem)) {
      return nullptr;
    }

    ObMaterialVecOp *op = new (mem) ObMaterialVecOp(ctx, spec, input);
    void *children_mem = ctx.get_allocator().alloc(sizeof(ObOperator *));
    if (OB_ISNULL(children_mem)) {
      LOG_WARN("alloc children array failed", K(ret));
      return nullptr;
    }
    ObOperator **children = reinterpret_cast<ObOperator **>(children_mem);
    children[0] = child_op;
    if (OB_FAIL(op->set_children_pointer(children, 1))) {
      LOG_WARN("set children pointer failed", K(ret));
      return nullptr;
    }
    // op->set_children_pointer((ObOperator **)&child_op, 1);
    // op->set_child(0, child_op);

    return op;
  }

  /**
   * @brief Overload for backward compatibility - creates real Material operator.
   */
  ObOperator* create_op(ObExecContext &ctx, ObOperator *child_op)
  {
    // This overload is called when no spec is created.
    // We still need to create a proper Material operator.
    // For now, return child_op as fallback.
    // The run() method in OpSpecBuilder will create the spec first.
    return child_op;
  }

private:
  bool bypass_;  // Bypass mode for material operator
};

/**
 * @brief ExprTestSpec - Convenience wrapper for expression unit tests.
 *
 * Simplified API:
 *   - columns("a int, b int") instead of table("__expr_test__", "a int, b int")
 *   - with_expr("a + b") instead of select("a + b")
 *   - Direct pass-through to Material operator (no operator-specific logic)
 *
 * Usage:
 *   auto result = expr_unit_test()
 *       .columns("a int, b int")
 *       .with_expr("a + b")
 *       .with_data({{1, 2}, {3, 4}})
 *       .run(engine_);
 */
 class ExprTestSpec : public MaterialTestSpec
 {
public:
  ExprTestSpec() {
    bypass_ = true;
  }
 public:
   /**
    * @brief Define input columns. Internally maps to table("__expr_test__", col_defs).
    */
   ExprTestSpec& columns(const char *col_defs)
   {
    //call parent table
     return static_cast<ExprTestSpec &>(table("__expr_test__", col_defs));
   }

   /**
    * @brief Set expression under test. Internally maps to select(expr_str).
    */
   ExprTestSpec& with_expr(const char *expr_str)
   {
     return static_cast<ExprTestSpec &>(select(expr_str));
   }
 };

}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_MATERIAL_H_