/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_WINDOW_FUNCTION_H_
#define OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_WINDOW_FUNCTION_H_

#include "unittest/sql/engine/op_tests/ob_op_test_base.h"
#include "sql/engine/window_function/ob_window_function_op.h"
#include "sql/engine/window_function/ob_window_function_vec_op.h"
#include "sql/code_generator/ob_static_engine_expr_cg.h"
#include "share/vector/expr_cmp_func.h"

namespace oceanbase
{
namespace sql
{

// Forward declaration for OpTestWFCG
class OpTestWFCG;

/**
 * @brief OpTestWFCG - Helper class to fill WinFuncInfo using ObStaticEngineCG::fill_wf_info.
 *
 * Similar to OpTestAggrCG, this class creates a minimal CG context to call
 * ObStaticEngineCG::fill_wf_info() for window function testing.
 */
class OpTestWFCG : public ObStaticEngineCG
{
public:
  OpTestWFCG(ObPhysicalPlan &phy_plan, common::ObIAllocator &allocator,
             const uint64_t cur_cluster_version)
    : ObStaticEngineCG(cur_cluster_version),
      cur_op_exprs_(),
      fake_allocator_("OpTestWFCG"),
      fake_expr_factory_(fake_allocator_),
      fake_global_hint_(fake_allocator_)
  {
    phy_plan_ = &phy_plan;
    // Create a minimal fake optimizer context to bypass null check
    fake_opt_ctx_ = static_cast<ObOptimizerContext*>(
        fake_allocator_.alloc(sizeof(ObOptimizerContext)));
    if (OB_NOT_NULL(fake_opt_ctx_)) {
      new (fake_opt_ctx_) ObOptimizerContext(
          nullptr, nullptr, nullptr, nullptr,
          fake_allocator_, nullptr, common::ObAddr(),
          nullptr, fake_global_hint_, fake_expr_factory_,
          nullptr, false);
      opt_ctx_ = fake_opt_ctx_;
    }
  }

  // Expose generate_rt_expr for direct use
  using ObStaticEngineCG::generate_rt_expr;

  /**
   * @brief Wrapper for fill_wf_info with unittest-friendly defaults.
   * @param all_expr Accumulated expressions (child output + window function exprs)
   * @param win_expr The window function raw expression
   * @param wf_info Output WinFuncInfo to fill
   * @param can_push_down Whether the window function can be pushed down
   * @param enable_rich_format Whether to use rich format
   * @return OB_SUCCESS on success
   */
  int fill_wf_info_for_test(ObIArray<ObExpr *> &all_expr,
                             ObWinFunRawExpr &win_expr,
                             WinFuncInfo &wf_info,
                             const bool can_push_down,
                             const bool enable_rich_format)
  {
    cur_op_exprs_.reset();
    UNUSED(enable_rich_format);
    int ret = fill_wf_info(all_expr, win_expr, wf_info,
                           can_push_down);
    if (OB_FAIL(ret)) {
      LOG_WARN("fill_wf_info failed", K(ret),
               "func_type", win_expr.get_func_type());
    }
    return ret;
  }

private:
  ObSEArray<ObRawExpr*, 64> cur_op_exprs_;
  common::ObArenaAllocator fake_allocator_;
  ObRawExprFactory fake_expr_factory_;
  ObGlobalHint fake_global_hint_;
  ObOptimizerContext *fake_opt_ctx_;
};

/**
 * @brief WindowFunctionTestSpec - Test specification for Window Function operator.
 *
 * Supports both 1.0 (ObWindowFunctionOp/PHY_WINDOW_FUNCTION) and
 * 2.0 (ObWindowFunctionVecOp/PHY_VEC_WINDOW_FUNCTION) operators.
 *
 * For 1.0 mode:
 *   - Uses ObWindowFunctionSpec + ObWindowFunctionOp
 *   - Fills wf_infos_ and all_expr_ using the same CG
 *   - Leaves sort_cmp_funcs_.cmp_func_ (datum-based) as set by CG
 *   - range_dist_parallel rd fields are NOT filled for 1.0 (use 2.0 for rd tests)
 *
 * For 2.0 mode:
 *   - Uses ObWindowFunctionVecSpec + ObWindowFunctionVecOp
 *   - Fills wf_infos_ and all_expr_ using the same CG
 *   - Overwrites sort_cmp_funcs_.row_cmp_func_ via fill_vec_sort_cmp_funcs
 *   - Fills rd fields via fill_rd_fields for range_dist_parallel
 *
 * Usage:
 *   // Normal window function
 *   WindowFunctionTestSpec()
 *       .table("t", "a int, b int")
 *       .select("sum(b) over (partition by a order by b)")
 *       .with_sorted_data({{1, 10}, {1, 20}, {2, 5}}, "a ASC, b ASC")
 *       .run(engine);
 *
 *   // Streaming mode (2.0 only)
 *   WindowFunctionTestSpec()
 *       .table("t", "a int, b int")
 *       .select("row_number() over (partition by a order by b)")
 *       .with_sorted_data({...}, "a ASC, b ASC")
 *       .enable_streaming()
 *       .with_batch_size(3)  // Force multiple batches
 *       .run(engine);
 */
class WindowFunctionTestSpec : public OpSpecBuilder<WindowFunctionTestSpec>
{
public:
  WindowFunctionTestSpec()
    : single_part_parallel_(false),
      range_dist_parallel_(false),
      role_type_(0),
      use_streaming_(false),
      local_task_count_(1),
      total_task_count_(1),
      input_rows_mem_bound_ratio_(0.5),
      estimated_part_cnt_(1)
  {}
  ~WindowFunctionTestSpec() = default;

  // ===== Configuration Interface =====

  /**
   * @brief Enable single partition parallel mode.
   * Sets single_part_parallel_ = true, which triggers datahub usage.
   */
  WindowFunctionTestSpec& enable_single_part_parallel(bool enable = true)
  {
    single_part_parallel_ = enable;
    return *this;
  }

  /**
   * @brief Enable Range Distribution parallel mode.
   * NOTE: rd fields are only filled for 2.0 path; use with_rich_format(true) or 2.0 mode.
   */
  WindowFunctionTestSpec& enable_range_dist_parallel(bool enable = true)
  {
    range_dist_parallel_ = enable;
    return *this;
  }

  /**
   * @brief Set role type (NORMAL=0, PARTICIPATOR, CONSOLIDATOR).
   */
  WindowFunctionTestSpec& set_role_type(int64_t role_type)
  {
    role_type_ = role_type;
    return *this;
  }

  /**
   * @brief Enable streaming mode (StreamingWindowProcessor).
   * Only applies to 2.0 VecSpec (ObWindowFunctionVecSpec).
   */
  WindowFunctionTestSpec& enable_streaming(bool enable = true)
  {
    use_streaming_ = enable;
    return *this;
  }

  /**
   * @brief Set task count for parallel execution.
   * Affects OpInput creation.
   */
  WindowFunctionTestSpec& with_task_count(int64_t local, int64_t total)
  {
    local_task_count_ = local;
    total_task_count_ = total;
    return *this;
  }

  /**
   * @brief Set memory bound ratio for input rows.
   */
  WindowFunctionTestSpec& with_mem_bound_ratio(double ratio)
  {
    input_rows_mem_bound_ratio_ = ratio;
    return *this;
  }

  /**
   * @brief Set estimated partition count.
   */
  WindowFunctionTestSpec& with_estimated_part_cnt(int64_t cnt)
  {
    estimated_part_cnt_ = cnt;
    return *this;
  }

  // ===== Create Spec =====

  /**
   * @brief Create ObWindowFunctionVecSpec (2.0) or ObWindowFunctionSpec (1.0).
   *
   * For 2.0:
   *   1. Allocates ObWindowFunctionVecSpec (PHY_VEC_WINDOW_FUNCTION)
   *   2. Sets basic fields including use_streaming_
   *   3. Fills wf_infos_ using OpTestWFCG::fill_wf_info_for_test
   *   4. Fills sort_cmp_funcs_.row_cmp_func_ via fill_vec_sort_cmp_funcs
   *   5. Fills rd_* fields for range_dist_parallel
   *
   * For 1.0:
   *   1. Allocates ObWindowFunctionSpec (PHY_WINDOW_FUNCTION)
   *   2. Sets basic fields (no use_streaming_ field in 1.0)
   *   3. Fills wf_infos_ using OpTestWFCG::fill_wf_info_for_test
   *   4. Leaves sort_cmp_funcs_.cmp_func_ as set by CG (datum-based comparison)
   *   5. Skips rd fields (not compatible with 1.0 datum comparison)
   */
  ObOpSpec *create_spec(common::ObIAllocator &alloc, MockDataSourceSpec *child_spec,
                         const ExprFixedArray &output_exprs,
                         ObExpr *limit_expr, ObExpr *offset_expr, bool use_rich_format)
  {
    int ret = OB_SUCCESS;

    if (use_rich_format) {
      return create_vec_spec(alloc, child_spec, output_exprs, use_rich_format);
    } else {
      return create_datum_spec(alloc, child_spec, output_exprs, use_rich_format);
    }
  }

  // ===== Create Operator Input =====

  /**
   * @brief Create ObWindowFunctionVecOpInput (2.0) or ObWindowFunctionOpInput (1.0).
   * Both input types support set_local_task_count/set_total_task_count.
   */
  ObOpInput *create_input(ObExecContext &ctx, ObOpSpec &spec)
  {
    if (spec.type_ == PHY_VEC_WINDOW_FUNCTION) {
      void *mem = ctx.get_allocator().alloc(sizeof(ObWindowFunctionVecOpInput));
      if (OB_ISNULL(mem)) {
        return nullptr;
      }
      ObWindowFunctionVecOpInput *input = new (mem) ObWindowFunctionVecOpInput(ctx, spec);
      input->set_local_task_count(local_task_count_);
      input->set_total_task_count(total_task_count_);
      return input;
    } else {
      void *mem = ctx.get_allocator().alloc(sizeof(ObWindowFunctionOpInput));
      if (OB_ISNULL(mem)) {
        return nullptr;
      }
      ObWindowFunctionOpInput *input = new (mem) ObWindowFunctionOpInput(ctx, spec);
      input->set_local_task_count(local_task_count_);
      input->set_total_task_count(total_task_count_);
      return input;
    }
  }

  // ===== Create Operator =====

  /**
   * @brief Create ObWindowFunctionVecOp (2.0) or ObWindowFunctionOp (1.0) based on spec type.
   */
  ObOperator *create_op(ObExecContext &ctx, ObOpSpec &spec, ObOperator *child_op)
  {
    if (spec.type_ == PHY_VEC_WINDOW_FUNCTION) {
      return default_create_op<ObWindowFunctionVecOp>(ctx, spec, child_op);
    } else {
      return default_create_op<ObWindowFunctionOp>(ctx, spec, child_op);
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
   * @brief Create 2.0 ObWindowFunctionVecSpec with all vec-specific setup.
   */
  ObOpSpec *create_vec_spec(common::ObIAllocator &alloc, MockDataSourceSpec *child_spec,
                             const ExprFixedArray &output_exprs, bool use_rich_format)
  {
    int ret = OB_SUCCESS;
    FatalErrorChecker error_checker(ret);

    void *mem = alloc.alloc(sizeof(ObWindowFunctionVecSpec));
    if (OB_ISNULL(mem)) {
      LOG_WARN("alloc ObWindowFunctionVecSpec failed");
      return nullptr;
    }

    ObWindowFunctionVecSpec *wf_spec = new (mem) ObWindowFunctionVecSpec(alloc, PHY_VEC_WINDOW_FUNCTION);

    // Set basic properties
    wf_spec->plan_ = child_spec->plan_;
    wf_spec->max_batch_size_ = child_spec->max_batch_size_;
    wf_spec->use_rich_format_ = use_rich_format;
    wf_spec->output_ = output_exprs;
    wf_spec->single_part_parallel_ = single_part_parallel_;
    wf_spec->range_dist_parallel_ = range_dist_parallel_;
    wf_spec->role_type_ = role_type_;
    // wf_spec->use_streaming_ = use_streaming_;  // not available on this branch
    wf_spec->enable_hash_base_distinct_ = true;
    wf_spec->input_rows_mem_bound_ratio_ = input_rows_mem_bound_ratio_;
    wf_spec->estimated_part_cnt_ = estimated_part_cnt_;
    wf_spec->wf_aggr_status_expr_ = nullptr;

    // Set child
    void *child_mem = alloc.alloc(sizeof(ObOpSpec *));
    if (OB_ISNULL(child_mem)) {
      LOG_WARN("alloc child spec array failed");
      return nullptr;
    }
    ObOpSpec **children = reinterpret_cast<ObOpSpec **>(child_mem);
    children[0] = child_spec;
    if (OB_FAIL(wf_spec->set_children_pointer(children, 1))) {
      LOG_WARN("set children pointer failed", K(ret));
      return nullptr;
    }

    // Fill wf_infos_ and all_expr_ using context from run()
    if (OB_NOT_NULL(resolved_stmt_) && OB_NOT_NULL(child_output_exprs_) && OB_NOT_NULL(phy_plan_)) {
      const common::ObIArray<ObWinFunRawExpr *> &wf_raw_exprs =
          resolved_stmt_->get_window_func_exprs();

      if (wf_raw_exprs.count() > 0) {
        ObSEArray<ObExpr *, 16> all_expr;
        OZ(append_array_no_dup(all_expr, *child_output_exprs_));

        OZ(wf_spec->wf_infos_.prepare_allocate(wf_raw_exprs.count()));

        if (OB_SUCC(ret)) {
          OpTestWFCG wf_cg(*phy_plan_, alloc, GET_MIN_CLUSTER_VERSION());
          for (int64_t i = 0; OB_SUCC(ret) && i < wf_raw_exprs.count(); ++i) {
            ObWinFunRawExpr *wf_raw = wf_raw_exprs.at(i);
            if (OB_ISNULL(wf_raw)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("wf_raw is null", K(ret), K(i));
            } else {
              WinFuncInfo &wf_info = wf_spec->wf_infos_.at(i);
              ret = wf_cg.fill_wf_info_for_test(
                  all_expr, *wf_raw, wf_info,
                  false /*can_push_down*/, use_rich_format);
              if (OB_FAIL(ret)) {
                LOG_WARN("fill_wf_info_for_test failed", K(ret), K(i),
                         "func_type", wf_raw->get_func_type());
              }
            }
          }
        }

        OZ(wf_spec->all_expr_.assign(all_expr));

        // Fill VEC sort_cmp_funcs_.row_cmp_func_
        if (OB_SUCC(ret)) {
          fill_vec_sort_cmp_funcs(wf_spec);
        }

        // Fill rd_* fields for range_dist_parallel
        if (OB_SUCC(ret) && range_dist_parallel_) {
          fill_rd_fields(wf_spec);
        }
      }
    }

    return wf_spec;
  }

  /**
   * @brief Create 1.0 ObWindowFunctionSpec with datum-based comparison setup.
   * Skips fill_vec_sort_cmp_funcs (leaves cmp_func_ from CG) and fill_rd_fields.
   */
  ObOpSpec *create_datum_spec(common::ObIAllocator &alloc, MockDataSourceSpec *child_spec,
                               const ExprFixedArray &output_exprs, bool use_rich_format)
  {
    int ret = OB_SUCCESS;
    FatalErrorChecker error_checker(ret);

    void *mem = alloc.alloc(sizeof(ObWindowFunctionSpec));
    if (OB_ISNULL(mem)) {
      LOG_WARN("alloc ObWindowFunctionSpec failed");
      return nullptr;
    }

    ObWindowFunctionSpec *wf_spec = new (mem) ObWindowFunctionSpec(alloc, PHY_WINDOW_FUNCTION);

    // Set basic properties (no use_streaming_ in 1.0)
    wf_spec->plan_ = child_spec->plan_;
    wf_spec->max_batch_size_ = child_spec->max_batch_size_;
    wf_spec->use_rich_format_ = use_rich_format;
    wf_spec->output_ = output_exprs;
    wf_spec->single_part_parallel_ = single_part_parallel_;
    wf_spec->range_dist_parallel_ = range_dist_parallel_;
    wf_spec->role_type_ = role_type_;
    wf_spec->enable_hash_base_distinct_ = true;
    wf_spec->input_rows_mem_bound_ratio_ = input_rows_mem_bound_ratio_;
    wf_spec->estimated_part_cnt_ = estimated_part_cnt_;
    wf_spec->wf_aggr_status_expr_ = nullptr;

    // Set child
    void *child_mem = alloc.alloc(sizeof(ObOpSpec *));
    if (OB_ISNULL(child_mem)) {
      LOG_WARN("alloc child spec array failed");
      return nullptr;
    }
    ObOpSpec **children = reinterpret_cast<ObOpSpec **>(child_mem);
    children[0] = child_spec;
    if (OB_FAIL(wf_spec->set_children_pointer(children, 1))) {
      LOG_WARN("set children pointer failed", K(ret));
      return nullptr;
    }

    // Fill wf_infos_ and all_expr_ using context from run()
    if (OB_NOT_NULL(resolved_stmt_) && OB_NOT_NULL(child_output_exprs_) && OB_NOT_NULL(phy_plan_)) {
      const common::ObIArray<ObWinFunRawExpr *> &wf_raw_exprs =
          resolved_stmt_->get_window_func_exprs();

      if (wf_raw_exprs.count() > 0) {
        ObSEArray<ObExpr *, 16> all_expr;
        OZ(append_array_no_dup(all_expr, *child_output_exprs_));

        OZ(wf_spec->wf_infos_.prepare_allocate(wf_raw_exprs.count()));

        if (OB_SUCC(ret)) {
          OpTestWFCG wf_cg(*phy_plan_, alloc, GET_MIN_CLUSTER_VERSION());
          for (int64_t i = 0; OB_SUCC(ret) && i < wf_raw_exprs.count(); ++i) {
            ObWinFunRawExpr *wf_raw = wf_raw_exprs.at(i);
            if (OB_ISNULL(wf_raw)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("wf_raw is null", K(ret), K(i));
            } else {
              WinFuncInfo &wf_info = wf_spec->wf_infos_.at(i);
              // use_rich_format=false: CG fills datum cmp_func_, not row_cmp_func_
              ret = wf_cg.fill_wf_info_for_test(
                  all_expr, *wf_raw, wf_info,
                  false /*can_push_down*/, use_rich_format);
              if (OB_FAIL(ret)) {
                LOG_WARN("fill_wf_info_for_test failed", K(ret), K(i),
                         "func_type", wf_raw->get_func_type());
              }
            }
          }
        }

        OZ(wf_spec->all_expr_.assign(all_expr));
        // NOTE: Intentionally skip fill_vec_sort_cmp_funcs for 1.0 path.
        //       The datum cmp_func_ set by CG is used by ObWindowFunctionOp.
        // NOTE: Intentionally skip fill_rd_fields for 1.0 path.
        //       rd fields use row_cmp_func_ which is not compatible with 1.0 datum comparison.
      }
    }

    return wf_spec;
  }

  /**
   * @brief Fill VEC row_cmp_func_ for each sort_cmp_funcs_.
   *
   * VEC execution requires row_cmp_func_ to be set for row-by-row comparison
   * in partition boundary detection. This function uses VectorCmpExprFuncsHelper
   * to get the appropriate comparison function based on sort expression type.
   *
   * NOTE: Only called for 2.0 path. Sets row_cmp_func_ union member (overwrites cmp_func_).
   */
  void fill_vec_sort_cmp_funcs(ObWindowFunctionVecSpec *wf_spec)
  {
    int ret = OB_SUCCESS;
    FatalErrorChecker error_checker(ret);
    for (int64_t i = 0; OB_SUCC(ret) && i < wf_spec->wf_infos_.count(); ++i) {
      WinFuncInfo &wf_info = wf_spec->wf_infos_.at(i);
      for (int64_t j = 0; OB_SUCC(ret) && j < wf_info.sort_exprs_.count(); ++j) {
        ObExpr *sort_expr = wf_info.sort_exprs_.at(j);
        if (OB_ISNULL(sort_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("sort_expr is null", K(ret), K(i), K(j));
          continue;
        }

        // Get VEC comparison functions
        sql::NullSafeRowCmpFunc null_first_cmp = nullptr;
        sql::NullSafeRowCmpFunc null_last_cmp = nullptr;
        VectorCmpExprFuncsHelper::get_cmp_set(
            sort_expr->datum_meta_,
            sort_expr->datum_meta_,
            null_first_cmp,
            null_last_cmp);

        if (OB_ISNULL(null_first_cmp) || OB_ISNULL(null_last_cmp)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get_cmp_set returned null", K(ret), K(i), K(j),
                   K(sort_expr->datum_meta_.type_));
        } else {
          // Set row_cmp_func_ based on null position in sort collation
          if (wf_info.sort_collations_.at(j).null_pos_ == NULL_FIRST) {
            wf_info.sort_cmp_funcs_.at(j).row_cmp_func_ = null_first_cmp;
          } else {
            wf_info.sort_cmp_funcs_.at(j).row_cmp_func_ = null_last_cmp;
          }
        }
      }
    }
  }

  /**
   * @brief Fill rd_* fields for range distribution parallel mode.
   *
   * Range distribution requires:
   * - rd_sort_collations_ / rd_sort_cmp_funcs_: from first wf's sort info
   * - rd_coord_exprs_: all wf exprs + first wf's sort exprs
   * - rd_wfs_: indices of all window functions
   * - rd_pby_sort_cnt_: partition by count from first wf
   *
   * NOTE: Only called for 2.0 path. Sets row_cmp_func_ union member.
   */
  void fill_rd_fields(ObWindowFunctionVecSpec *wf_spec)
  {
    int ret = OB_SUCCESS;
    FatalErrorChecker error_checker(ret);
    ObSEArray<ObExpr *, 16> rd_expr;

    if (wf_spec->wf_infos_.count() > 0) {
      WinFuncInfo &first_wf = wf_spec->wf_infos_.at(0);

      // Copy sort collations and cmp funcs from first wf
      OZ(wf_spec->rd_sort_collations_.assign(first_wf.sort_collations_));
      OZ(wf_spec->rd_sort_cmp_funcs_.assign(first_wf.sort_cmp_funcs_));
      OZ(append(rd_expr, first_wf.sort_exprs_));

      // Set partition by sort count
      wf_spec->rd_pby_sort_cnt_ = first_wf.partition_exprs_.count();
    }

    // Initialize rd_wfs_ with all window function indices
    OZ(wf_spec->rd_wfs_.init(wf_spec->wf_infos_.count()));
    for (int64_t i = 0; OB_SUCC(ret) && i < wf_spec->wf_infos_.count(); ++i) {
      OZ(wf_spec->rd_wfs_.push_back(i));
      OZ(rd_expr.push_back(wf_spec->wf_infos_.at(i).expr_));
    }

    // Set rd_coord_exprs_
    OZ(wf_spec->rd_coord_exprs_.assign(rd_expr));

    // Fill row_cmp_func_ for rd_sort_cmp_funcs_
    for (int64_t i = 0; OB_SUCC(ret) && i < wf_spec->rd_sort_collations_.count(); ++i) {
      ObExpr *expr = wf_spec->rd_coord_exprs_.at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rd_coord_expr is null", K(ret), K(i));
        continue;
      }

      sql::NullSafeRowCmpFunc null_first_cmp = nullptr;
      sql::NullSafeRowCmpFunc null_last_cmp = nullptr;
      VectorCmpExprFuncsHelper::get_cmp_set(
          expr->datum_meta_,
          expr->datum_meta_,
          null_first_cmp,
          null_last_cmp);

      if (OB_ISNULL(null_first_cmp) || OB_ISNULL(null_last_cmp)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get_cmp_set returned null for rd", K(ret), K(i),
                 K(expr->datum_meta_.type_));
      } else {
        if (wf_spec->rd_sort_collations_.at(i).null_pos_ == NULL_FIRST) {
          wf_spec->rd_sort_cmp_funcs_.at(i).row_cmp_func_ = null_first_cmp;
        } else {
          wf_spec->rd_sort_cmp_funcs_.at(i).row_cmp_func_ = null_last_cmp;
        }
      }
    }
  }

private:
  // Configuration flags
  bool single_part_parallel_;
  bool range_dist_parallel_;
  int64_t role_type_;
  bool use_streaming_;  // 2.0 only

  // OpInput configuration
  int64_t local_task_count_;
  int64_t total_task_count_;

  // Spec configuration
  double input_rows_mem_bound_ratio_;
  int64_t estimated_part_cnt_;
};

}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_WINDOW_FUNCTION_H_
