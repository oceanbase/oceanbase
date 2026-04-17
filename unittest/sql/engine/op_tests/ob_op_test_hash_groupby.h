/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_HASH_GROUPBY_H_
#define OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_HASH_GROUPBY_H_

#include "unittest/sql/engine/op_tests/ob_op_test_base.h"
#include "sql/engine/aggregate/ob_hash_groupby_op.h"  // ObHashGroupBySpec, ObHashGroupByOp
#include "sql/engine/aggregate/ob_hash_groupby_vec_op.h"  // ObHashGroupByVecSpec, ObHashGroupByVecOp
#include "sql/engine/aggregate/ob_groupby_op.h"
#include "sql/engine/ob_operator.h"
#include "sql/code_generator/ob_static_engine_expr_cg.h"
#include "share/datum/ob_datum_funcs.h"

namespace oceanbase
{
namespace sql
{

/**
 * @brief HashGroupByTestSpec - Test specification for Hash GroupBy operator.
 *
 * Supports both 1.0 (ObHashGroupByOp/PHY_HASH_GROUP_BY) and
 * 2.0 (ObHashGroupByVecOp/PHY_VEC_HASH_GROUP_BY) operators.
 * The operator version is selected at create_spec() time via the use_rich_format parameter.
 *
 * Hash GroupBy groups rows by GROUP BY keys using a hash table and applies aggregate functions.
 *
 * Usage:
 *   HashGroupByTestSpec spec;
 *   spec.table("t", "a int, b int")
 *       .select("a, COUNT(*), SUM(b)")
 *       .group_by("a")
 *       .with_data({{1, 10}, {1, 20}, {2, 30}})
 *       .run(engine);
 *
 * Advanced usage:
 *   // Custom estimated group count
 *   spec.select("a, SUM(b)")
 *       .group_by("a")
 *       .est_group_cnt(500)
 *       .run(engine);
 *
 *   // Enable adaptive bypass
 *   spec.select("a, COUNT(*)")
 *       .group_by("a")
 *       .enable_bypass(true)
 *       .run(engine);
 *
 *   // Push LIMIT into hash gby (2.0 only)
 *   spec.select("a, COUNT(*)")
 *       .group_by("a")
 *       .with_limit(10)
 *       .run(engine);
 *
 *   // Dual-format comparison (runs both 1.0 and 2.0, verifies identical results)
 *   spec.select("a, SUM(b)")
 *       .group_by("a")
 *       .enable_dual_format_check()
 *       .run(engine);
 */
class HashGroupByTestSpec : public OpSpecBuilder<HashGroupByTestSpec>
{
public:
  HashGroupByTestSpec()
    : est_group_cnt_(100),
      enable_bypass_(false),
      push_limit_to_gby_(false)
  {}
  ~HashGroupByTestSpec() = default;

  // ===== Configuration Interface =====

  /**
   * @brief Set estimated group count for hash table pre-allocation.
   * Default is 100.
   * @param cnt Estimated number of distinct groups
   */
  HashGroupByTestSpec& est_group_cnt(int64_t cnt)
  {
    est_group_cnt_ = cnt;
    return *this;
  }

  /**
   * @brief Enable or disable adaptive bypass mode.
   * When enabled, the operator may bypass hash aggregation for low-NDV scenarios.
   * Sets by_pass_enabled_ on the spec (via ObGroupBySpec base class).
   * @param enable true to enable bypass
   */
  HashGroupByTestSpec& enable_bypass(bool enable = true)
  {
    enable_bypass_ = enable;
    return *this;
  }

  /**
   * @brief Push LIMIT into hash groupby (2.0 only).
   * Calls the base class limit(n) to add LIMIT n to the SQL, and sets a flag
   * so that create_spec() passes prepared_limit_expr_ to ObHashGroupByVecSpec::limit_expr_.
   * For the 1.0 path, the flag is ignored (no limit_expr_ on ObHashGroupBySpec).
   * @param cnt Number of groups to return
   */
  HashGroupByTestSpec& with_limit(int64_t cnt)
  {
    // Delegate to base class limit() which adds LIMIT n to SQL and sets has_limit_/limit_
    limit(cnt);
    // Also mark that we want to push limit into the gby spec (2.0 only)
    push_limit_to_gby_ = true;
    return *this;
  }

  // ===== Create Spec =====

  /**
   * @brief Create ObHashGroupByVecSpec (2.0) or ObHashGroupBySpec (1.0).
   *
   * For 2.0: uses PHY_VEC_HASH_GROUP_BY, supports limit_expr_ push-down.
   * For 1.0: uses PHY_HASH_GROUP_BY.
   *
   * Both paths:
   *   - Fill aggr_infos_ via fill_aggr_infos_for_groupby()
   *   - Fill group_exprs_ from resolved_stmt_->get_group_exprs()
   *   - Fill cmp_funcs_ using ObDatumFuncs::get_nullsafe_cmp_func()
   *   - Set est_group_cnt_ via set_est_group_cnt()
   *   - Set by_pass_enabled_ if enable_bypass() was called
   */
  ObOpSpec *create_spec(common::ObIAllocator &alloc, MockDataSourceSpec *child_spec,
                         const ExprFixedArray &output_exprs,
                         ObExpr *limit_expr, ObExpr *offset_expr, bool use_rich_format)
  {
    int ret = OB_SUCCESS;
    FatalErrorChecker error_checker(ret);
    if (use_rich_format) {
      // 2.0: ObHashGroupByVecSpec
      void *mem = alloc.alloc(sizeof(ObHashGroupByVecSpec));
      if (OB_ISNULL(mem)) {
        LOG_WARN("alloc ObHashGroupByVecSpec failed", K(ret));
        return nullptr;
      }
      ObHashGroupByVecSpec *spec = new (mem) ObHashGroupByVecSpec(alloc, PHY_VEC_HASH_GROUP_BY);
      spec->plan_ = child_spec->plan_;
      spec->max_batch_size_ = child_spec->max_batch_size_;
      spec->use_rich_format_ = use_rich_format;
      spec->output_ = output_exprs;

      // Set child pointer
      void *child_spec_mem = alloc.alloc(sizeof(ObOpSpec *));
      if (OB_ISNULL(child_spec_mem)) {
        LOG_WARN("alloc child spec array failed", K(ret));
        return nullptr;
      }
      ObOpSpec **children = reinterpret_cast<ObOpSpec **>(child_spec_mem);
      children[0] = child_spec;
      if (OB_FAIL(spec->set_children_pointer(children, 1))) {
        LOG_WARN("set children pointer failed", K(ret));
        return nullptr;
      }

      // Fill aggr_infos_
      ret = fill_aggr_infos_for_groupby(alloc, spec, resolved_stmt_, phy_plan_, use_rich_format);
      if (OB_FAIL(ret)) {
        LOG_WARN("fill_aggr_infos_for_groupby failed", K(ret));
        return nullptr;
      }

      // Fill group_exprs_ and cmp_funcs_
      if (OB_FAIL(fill_group_exprs_and_cmp_funcs(alloc, spec))) {
        LOG_WARN("fill_group_exprs_and_cmp_funcs failed", K(ret));
        return nullptr;
      }

      // Set est_group_cnt_
      spec->set_est_group_cnt(est_group_cnt_);

      // Set by_pass_enabled_ (on ObGroupBySpec base)
      spec->by_pass_enabled_ = enable_bypass_;

      // Push limit expr into spec (2.0 only)
      if (push_limit_to_gby_ && prepared_limit_expr_ != nullptr) {
        spec->limit_expr_ = prepared_limit_expr_;
      }

      return spec;
    } else {
      // 1.0: ObHashGroupBySpec
      void *mem = alloc.alloc(sizeof(ObHashGroupBySpec));
      if (OB_ISNULL(mem)) {
        LOG_WARN("alloc ObHashGroupBySpec failed", K(ret));
        return nullptr;
      }
      ObHashGroupBySpec *spec = new (mem) ObHashGroupBySpec(alloc, PHY_HASH_GROUP_BY);
      spec->plan_ = child_spec->plan_;
      spec->max_batch_size_ = child_spec->max_batch_size_;
      spec->use_rich_format_ = use_rich_format;
      spec->output_ = output_exprs;

      // Set child pointer
      void *child_spec_mem = alloc.alloc(sizeof(ObOpSpec *));
      if (OB_ISNULL(child_spec_mem)) {
        LOG_WARN("alloc child spec array failed", K(ret));
        return nullptr;
      }
      ObOpSpec **children = reinterpret_cast<ObOpSpec **>(child_spec_mem);
      children[0] = child_spec;
      if (OB_FAIL(spec->set_children_pointer(children, 1))) {
        LOG_WARN("set children pointer failed", K(ret));
        return nullptr;
      }

      // Fill aggr_infos_
      ret = fill_aggr_infos_for_groupby(alloc, spec, resolved_stmt_, phy_plan_, use_rich_format);
      if (OB_FAIL(ret)) {
        LOG_WARN("fill_aggr_infos_for_groupby failed", K(ret));
        return nullptr;
      }

      // Fill group_exprs_ and cmp_funcs_
      if (OB_FAIL(fill_group_exprs_and_cmp_funcs(alloc, spec))) {
        LOG_WARN("fill_group_exprs_and_cmp_funcs failed", K(ret));
        return nullptr;
      }

      // Set est_group_cnt_
      spec->set_est_group_cnt(est_group_cnt_);

      // Set by_pass_enabled_ (on ObGroupBySpec base)
      spec->by_pass_enabled_ = enable_bypass_;

      // NOTE: ObHashGroupBySpec has no limit_expr_; push_limit_to_gby_ is ignored for 1.0.

      return spec;
    }
  }

  // ===== Create Operator =====

  /**
   * @brief Create ObHashGroupByVecOp (2.0) or ObHashGroupByOp (1.0) based on spec type.
   */
  ObOperator *create_op(ObExecContext &ctx, ObOpSpec &spec, ObOperator *child_op)
  {
    if (spec.type_ == PHY_VEC_HASH_GROUP_BY) {
      return default_create_op<ObHashGroupByVecOp>(ctx, spec, child_op);
    } else {
      return default_create_op<ObHashGroupByOp>(ctx, spec, child_op);
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
  /**
   * @brief Fill group_exprs_ and cmp_funcs_ on the given spec.
   * Works for both ObHashGroupBySpec and ObHashGroupByVecSpec since both have
   * identical group_exprs_ / cmp_funcs_ layouts (both derive from ObGroupBySpec
   * and declare the same members).
   *
   * @tparam SpecType ObHashGroupBySpec or ObHashGroupByVecSpec
   */
  template <typename SpecType>
  int fill_group_exprs_and_cmp_funcs(common::ObIAllocator &/*alloc*/, SpecType *spec)
  {
    int ret = OB_SUCCESS;
    FatalErrorChecker error_checker(ret);
    if (OB_ISNULL(spec) || OB_ISNULL(resolved_stmt_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("spec or resolved_stmt_ is null", K(ret), KP(spec), KP(resolved_stmt_));
      return ret;
    }

    const common::ObIArray<ObRawExpr *> &group_raw_exprs = resolved_stmt_->get_group_exprs();
    const int64_t group_cnt = group_raw_exprs.count();

    if (group_cnt > 0) {
      // Initialize group_exprs_ fixed array
      if (OB_FAIL(spec->group_exprs_.init(group_cnt))) {
        LOG_WARN("init group_exprs_ failed", K(ret), K(group_cnt));
        return ret;
      }
      if (OB_FAIL(spec->cmp_funcs_.init(group_cnt))) {
        LOG_WARN("init cmp_funcs_ failed", K(ret), K(group_cnt));
        return ret;
      }

      for (int64_t i = 0; OB_SUCC(ret) && i < group_cnt; ++i) {
        ObRawExpr *raw_expr = group_raw_exprs.at(i);
        if (OB_ISNULL(raw_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("group raw expr is null", K(ret), K(i));
          break;
        }
        ObExpr *expr = ObStaticEngineExprCG::get_rt_expr(*raw_expr);
        if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get_rt_expr returned null for group expr", K(ret), K(i));
          break;
        }

        // Push into group_exprs_
        if (OB_FAIL(spec->group_exprs_.push_back(expr))) {
          LOG_WARN("push_back group expr failed", K(ret), K(i));
          break;
        }

        // Build cmp_func for this group expr
        ObCmpFunc cmp_func;
        cmp_func.cmp_func_ = ObDatumFuncs::get_nullsafe_cmp_func(
            expr->datum_meta_.type_,
            expr->datum_meta_.type_,
            NULL_LAST,
            expr->datum_meta_.cs_type_,
            expr->datum_meta_.scale_,
            lib::is_oracle_mode(),
            expr->obj_meta_.has_lob_header(),
            expr->datum_meta_.precision_,
            expr->datum_meta_.precision_);
        if (OB_ISNULL(cmp_func.cmp_func_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get_nullsafe_cmp_func returned null, check datatype",
                   K(ret), K(i),
                   K(expr->datum_meta_.type_),
                   K(expr->datum_meta_.cs_type_));
          break;
        }
        if (OB_FAIL(spec->cmp_funcs_.push_back(cmp_func))) {
          LOG_WARN("push_back cmp_func failed", K(ret), K(i));
          break;
        }
      }
    }
    return ret;
  }

private:
  int64_t est_group_cnt_;    // Estimated distinct group count (default 100)
  bool enable_bypass_;       // Enable adaptive bypass (by_pass_enabled_)
  bool push_limit_to_gby_;   // Push limit_expr_ into 2.0 spec
};

}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_HASH_GROUPBY_H_
