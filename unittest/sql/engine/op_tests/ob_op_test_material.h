/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define private public
#define protected public

#ifndef OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_MATERIAL_H_
#define OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_MATERIAL_H_

#include "unittest/sql/engine/op_tests/ob_op_test_base.h"
#include "sql/engine/basic/ob_material_op.h"
#include "sql/engine/basic/ob_material_vec_op.h"

namespace oceanbase
{
namespace sql
{

/**
 * @brief MaterialTestSpec - Test specification for Material operator.
 *
 * Supports both 1.0 (ObMaterialOp/PHY_MATERIAL) and 2.0 (ObMaterialVecOp/PHY_VEC_MATERIAL).
 * The operator version is selected at create_spec() time via the use_rich_format parameter.
 *
 * Usage:
 *   MaterialTestSpec spec;
 *   spec.table("t", "a int, b int")
 *       .select("a, b")
 *       .with_data({{1, 2}, {3, 4}})
 *       .run(engine);
 *
 *   // Dual-format comparison (runs both 1.0 and 2.0, verifies identical results)
 *   spec.with_data(...).enable_dual_format_check().run(engine);
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
   * @brief Create ObMaterialVecSpec (2.0) or ObMaterialSpec (1.0) based on use_rich_format.
   * ObMaterialVecSpec has an additional width_ field; ObMaterialSpec is minimal.
   */
  ObOpSpec *create_spec(common::ObIAllocator &alloc, MockDataSourceSpec *child_spec,
                         const ExprFixedArray &output_exprs,
                         ObExpr *limit_expr, ObExpr *offset_expr, bool use_rich_format)
  {
    int ret = OB_SUCCESS;
    FatalErrorChecker error_checker(ret);
    if (use_rich_format) {
      // 2.0: ObMaterialVecSpec (includes width_ field)
      void *mem = alloc.alloc(sizeof(ObMaterialVecSpec));
      if (OB_ISNULL(mem)) {
        return nullptr;
      }
      ObMaterialVecSpec *mat_spec = new (mem) ObMaterialVecSpec(alloc, PHY_VEC_MATERIAL);
      // CRITICAL: Material output must match child output for correct data flow
      mat_spec->output_ = output_exprs;
      mat_spec->max_batch_size_ = child_spec->max_batch_size_;
      mat_spec->width_ = child_spec->output_.count();
      mat_spec->use_rich_format_ = use_rich_format;
      mat_spec->plan_ = child_spec->plan_;

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
    } else {
      // 1.0: ObMaterialSpec (minimal, no width_ field)
      void *mem = alloc.alloc(sizeof(ObMaterialSpec));
      if (OB_ISNULL(mem)) {
        return nullptr;
      }
      ObMaterialSpec *mat_spec = new (mem) ObMaterialSpec(alloc, PHY_MATERIAL);
      mat_spec->output_ = output_exprs;
      mat_spec->max_batch_size_ = child_spec->max_batch_size_;
      mat_spec->use_rich_format_ = use_rich_format;
      mat_spec->plan_ = child_spec->plan_;

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
  }

  /**
   * @brief Create ObMaterialVecOpInput (2.0) or ObMaterialOpInput (1.0) based on spec type.
   * Both input types support the same set_bypass() interface.
   */
  ObOpInput *create_input(ObExecContext &ctx, ObOpSpec &spec)
  {
    if (spec.type_ == PHY_VEC_MATERIAL) {
      void *mem = ctx.get_allocator().alloc(sizeof(ObMaterialVecOpInput));
      if (OB_ISNULL(mem)) {
        return nullptr;
      }
      ObMaterialVecOpInput *input = new (mem) ObMaterialVecOpInput(ctx, spec);
      input->set_bypass(bypass_);
      return input;
    } else {
      void *mem = ctx.get_allocator().alloc(sizeof(ObMaterialOpInput));
      if (OB_ISNULL(mem)) {
        return nullptr;
      }
      ObMaterialOpInput *input = new (mem) ObMaterialOpInput(ctx, spec);
      input->set_bypass(bypass_);
      return input;
    }
  }

  /**
   * @brief Create ObMaterialVecOp (2.0) or ObMaterialOp (1.0) based on spec type.
   */
  ObOperator* create_op(ObExecContext &ctx, ObOpSpec &spec, ObOperator *child_op)
  {
    if (spec.type_ == PHY_VEC_MATERIAL) {
      return default_create_op<ObMaterialVecOp>(ctx, spec, child_op);
    } else {
      return default_create_op<ObMaterialOp>(ctx, spec, child_op);
    }
  }

  /**
   * @brief Fallback create_op when no parent spec exists.
   */
  ObOperator* create_op(ObExecContext &ctx, ObOperator *child_op)
  {
    return child_op;
  }

private:
  bool bypass_;  // Bypass mode for material operator
};

#undef private

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
    with_bypass(true);
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

   /**
    * @brief Code-generate (and execute) the expression as true vectorization 1.0.
    * The expression frame is built without a vector header (vector_header_off_ == UINT32_MAX),
    * MockDataSource fills datums directly, and evaluation hits eval_batch_func_/eval_func_
    * (1.0 path) rather than eval_vector_func_ (2.0 path). Both CG and execution run in non-rich
    * format for a coherent single 1.0 run.
    */
   ExprTestSpec& cg_as_vector_1_0()
   {
     expr_cg_rich_format_ = false;  // CG frame: true 1.0 datum frame
     rich_format_ = false;          // execution format: 1.0
     return *this;
   }

   /**
    * @brief Code-generate (and execute) the expression as vectorization 2.0 (rich format).
    * This is the default; provided so tests can state the intent explicitly.
    */
   ExprTestSpec& cg_as_vector_2_0()
   {
     expr_cg_rich_format_ = true;
     rich_format_ = true;
     return *this;
   }

   /**
    * @brief Enable expression-level dual-format check.
    * run() resolves the SQL once, then CG + executes the SAME expression once as true 1.0 and
    * once as 2.0, and compares row count + per-row results. This is a real per-expression check
    * (each format is independently code-generated), unlike the operator-level
    * enable_dual_format_check() which code-generates only once.
    * @param enable True to enable (default: true)
    */
   ExprTestSpec& enable_dual_format_expr_check(bool enable = true)
   {
     dual_format_expr_check_ = enable;
     return *this;
   }
 };

}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_MATERIAL_H_
