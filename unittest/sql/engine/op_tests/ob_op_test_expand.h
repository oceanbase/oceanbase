/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_EXPAND_H_
#define OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_EXPAND_H_

#include "unittest/sql/engine/op_tests/ob_op_test_base.h"
#include "sql/engine/expand/ob_expand_vec_op.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/code_generator/ob_static_engine_expr_cg.h"
#include <algorithm>  // for std::transform

namespace oceanbase
{
namespace sql
{

/**
 * @brief ExpandTestSpec - Test spec for ObExpandVecOp.
 *
 * SQL column name resolution is done by the SQL resolver (not manual string parsing).
 * Use group_by_rollup() to specify ROLLUP columns; the generated SQL will contain
 * "GROUP BY <cols> WITH ROLLUP", and create_spec() reads resolved_stmt_->get_rollup_exprs()
 * to obtain the CG'd ObExpr* pointers.
 *
 * Usage:
 *   ExpandTestSpec()
 *     .table("t", "a int, b int")
 *     .select("a, b")
 *     .group_by_rollup("a, b")   // generates GROUP BY a, b WITH ROLLUP
 *     .with_data({{1, 2}, {3, 4}})
 *     .run(engine_);
 */
class ExpandTestSpec : public OpSpecBuilder<ExpandTestSpec>
{
public:
  ExpandTestSpec()
    : is_ordered_output_(false),
      use_rollup_(false),
      use_grouping_sets_(false),
      grouping_id_raw_(nullptr)
  {}
  ~ExpandTestSpec() = default;

  // ===== Builder Interface =====

  /**
   * @brief Specify ROLLUP columns using Oracle/standard syntax.
   * Generates: GROUP BY ROLLUP(cols)
   * SQL resolver parses and stores in resolved_stmt_->get_rollup_items().
   * @param rollup_cols Comma-separated column names, e.g., "a, b, c"
   */
  ExpandTestSpec& group_by_rollup(const char *rollup_cols)
  {
    group_by_exprs_ = "ROLLUP(" + std::string(rollup_cols) + ")";
    use_rollup_ = true;
    return *this;
  }

  /**
   * @brief Specify GROUPING SETS columns.
   * Sets GROUP BY GROUPING SETS(...) in the generated SQL.
   * SQL resolver performs column name resolution; create_spec() reads
   * resolved_stmt_->get_grouping_sets_items() for CG'd ObExpr* pointers.
   * @param grouping_sets_desc GROUPING SETS description, e.g., "(a, b), (a), ()"
   */
  ExpandTestSpec& group_by_grouping_sets(const char *grouping_sets_desc)
  {
    group_by_exprs_ = "GROUPING SETS(" + std::string(grouping_sets_desc) + ")";
    use_grouping_sets_ = true;
    return *this;
  }

  ExpandTestSpec& with_ordered_output(bool enable = true)
  {
    is_ordered_output_ = enable;
    return *this;
  }

  // ===== CRTP Hooks =====

  /**
   * @brief Create grouping_id pseudo column before CG.
   * Called after SQL resolution but before expression code generation.
   * Registers the pseudo column via engine.add_pending_raw_expr() so it gets CG'd.
   */
  int post_resolve_hook(OpTestEngine &engine, ObDMLStmt &stmt)
  {
    int ret = OB_SUCCESS;
    FatalErrorChecker error_checker(ret);
    ObOpPseudoColumnRawExpr *grouping_id = nullptr;
    if (OB_FAIL(ObRawExprUtils::build_grouping_id(
            engine.get_expr_factory(), engine.get_session_info(), grouping_id))) {
      LOG_WARN("build_grouping_id failed", K(ret));
    } else if (OB_ISNULL(grouping_id)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("grouping_id is null", K(ret));
    } else {
      grouping_id_raw_ = grouping_id;
      engine.add_pending_raw_expr(grouping_id);
    }
    return ret;
  }

  /**
   * @brief Adjust output expressions after prepare().
   * If user specified output_expr_strs_, only add grouping_id if "grouping_id" is in the list.
   * Otherwise, always append grouping_id to output.
   */
  void adjust_output_exprs()
  {
    if (OB_ISNULL(grouping_id_raw_)) {
      return;
    }
    ObExpr *expr = ObStaticEngineExprCG::get_rt_expr(*grouping_id_raw_);
    if (OB_ISNULL(expr)) {
      return;
    }

    // Check if user explicitly specified output expressions
    if (!output_expr_strs_.empty()) {
      // Only add grouping_id if user requested it
      for (const std::string &target : output_expr_strs_) {
        std::string trimmed = target;
        trimmed.erase(0, trimmed.find_first_not_of(" \t"));
        trimmed.erase(trimmed.find_last_not_of(" \t") + 1);
        // Case-insensitive comparison
        std::string upper_target = trimmed;
        std::transform(upper_target.begin(), upper_target.end(), upper_target.begin(), ::toupper);
        if (upper_target == "GROUPING_ID") {
          prepared_out_exprs_vec_.push_back(expr);
          return;
        }
      }
      // User specified output_exprs but didn't include grouping_id, skip
    } else {
      // No explicit output_exprs, always append grouping_id
      prepared_out_exprs_vec_.push_back(expr);
    }
  }

  // ===== Spec / Op Creation =====

  ObOpSpec *create_spec(common::ObIAllocator &alloc, MockDataSourceSpec *child_spec,
                        const ExprFixedArray &output_exprs,
                        ObExpr *limit_expr, ObExpr *offset_expr, bool use_rich_format)
  {
    int ret = OB_SUCCESS;
    FatalErrorChecker erro_checker(ret);
    // Expand is vec-only (PHY_EXPAND)
    void *mem = alloc.alloc(sizeof(ObExpandVecSpec));
    if (OB_ISNULL(mem)) {
      LOG_WARN("alloc spec failed");
      return nullptr;
    }
    ObExpandVecSpec *spec = new (mem) ObExpandVecSpec(alloc, PHY_EXPAND);
    spec->plan_ = child_spec->plan_;
    spec->max_batch_size_ = child_spec->max_batch_size_;
    spec->use_rich_format_ = use_rich_format;
    spec->is_ordered_output_ = is_ordered_output_;

    // Set child pointer
    void *child_spec_mem = alloc.alloc(sizeof(ObOpSpec *));
    if (OB_ISNULL(child_spec_mem)) { return nullptr; }
    ObOpSpec **children = reinterpret_cast<ObOpSpec **>(child_spec_mem);
    children[0] = child_spec;
    if (OB_FAIL(spec->set_children_pointer(children, 1))) { return nullptr; }

    // Set grouping_id_expr_ from CG'd pseudo column
    if (OB_ISNULL(grouping_id_raw_)) {
      LOG_WARN("grouping_id_raw_ is null, post_resolve_hook not called?");
      return nullptr;
    }
    spec->grouping_id_expr_ = ObStaticEngineExprCG::get_rt_expr(*grouping_id_raw_);
    if (OB_ISNULL(spec->grouping_id_expr_)) {
      LOG_WARN("grouping_id rt_expr is null");
      return nullptr;
    }

    // Set output = all output_exprs (including grouping_id added by adjust_output_exprs)
    spec->output_ = output_exprs;

    // Build group_set_exprs_ from SQL-resolved rollup expressions.
    // SQL resolver has already parsed the column names; we just read CG'd ObExpr* pointers.
    if (use_rollup_) {
      // ROLLUP(a, b, c) syntax: resolved_stmt_->get_rollup_items()
      // Each ObRollupItem.rollup_list_exprs_ contains ObGroupbyExpr for each column
      // Generates n+1 grouping sets: set k has exprs [col0..col_{n-1-k}]
      if (OB_ISNULL(resolved_stmt_)) {
        LOG_WARN("resolved_stmt_ is null");
        return nullptr;
      }
      const common::ObIArray<ObRollupItem> &rollup_items =
          resolved_stmt_->get_rollup_items();
      if (rollup_items.count() != 1) {
        LOG_WARN("unexpected rollup_items count", K(rollup_items.count()));
        return nullptr;
      }
      const ObRollupItem &rollup_item = rollup_items.at(0);
      int64_t n = rollup_item.rollup_list_exprs_.count();  // number of columns in ROLLUP
      int64_t set_count = n + 1;

      if (OB_FAIL(spec->group_set_exprs_.prepare_allocate(set_count))) {
        LOG_WARN("prepare_allocate group_set_exprs failed", K(ret));
        return nullptr;
      }
      // Build each grouping set: set k contains columns 0..n-1-k
      for (int64_t k = 0; k <= n && OB_SUCC(ret); ++k) {
        ObSEArray<ObExpr *, 4> temp_arr;
        for (int64_t j = 0; j < n - k && OB_SUCC(ret); ++j) {
          const ObGroupbyExpr &gby_expr = rollup_item.rollup_list_exprs_.at(j);
          if (gby_expr.groupby_exprs_.count() != 1) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("expected single expr in rollup groupby_exprs", K(ret), K(j));
          } else {
            ObRawExpr *raw_expr = gby_expr.groupby_exprs_.at(0);
            if (OB_ISNULL(raw_expr)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("rollup raw_expr is null", K(ret), K(j));
            } else {
              ObExpr *rt_expr = ObStaticEngineExprCG::get_rt_expr(*raw_expr);
              if (OB_ISNULL(rt_expr)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("rollup rt_expr is null", K(ret), K(j));
              } else if (OB_FAIL(temp_arr.push_back(rt_expr))) {
                LOG_WARN("push_back failed", K(ret));
              }
            }
          }
        }
        if (OB_SUCC(ret) && OB_FAIL(spec->group_set_exprs_.at(k).assign(temp_arr))) {
          LOG_WARN("assign group_set_exprs[k] failed", K(ret), K(k));
          return nullptr;
        }
      }
    } else if (use_grouping_sets_) {
      // GROUPING SETS: resolved_stmt_->get_grouping_sets_items()
      // Each ObGroupingSetsItem contains grouping_sets_exprs_ (ObSqlArray<ObGroupbyExpr>)
      // Each ObGroupbyExpr contains groupby_exprs_ (ObIArray<ObRawExpr*>)
      if (OB_ISNULL(resolved_stmt_)) {
        LOG_WARN("resolved_stmt_ is null");
        return nullptr;
      }
      const common::ObIArray<ObGroupingSetsItem> &grouping_sets_items =
          resolved_stmt_->get_grouping_sets_items();
      // Typically there's one ObGroupingSetsItem containing all the sets
      if (grouping_sets_items.count() != 1) {
        LOG_WARN("unexpected grouping_sets_items count", K(grouping_sets_items.count()));
        return nullptr;
      }
      const ObGroupingSetsItem &gs_item = grouping_sets_items.at(0);
      int64_t set_count = gs_item.grouping_sets_exprs_.count();

      if (OB_FAIL(spec->group_set_exprs_.prepare_allocate(set_count))) {
        LOG_WARN("prepare_allocate group_set_exprs failed", K(ret));
        return nullptr;
      }
      for (int64_t k = 0; k < set_count && OB_SUCC(ret); ++k) {
        const ObGroupbyExpr &gby_expr = gs_item.grouping_sets_exprs_.at(k);
        ObSEArray<ObExpr *, 4> temp_arr;
        for (int64_t j = 0; j < gby_expr.groupby_exprs_.count() && OB_SUCC(ret); ++j) {
          ObRawExpr *raw_expr = gby_expr.groupby_exprs_.at(j);
          if (OB_ISNULL(raw_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("groupby raw_expr is null", K(ret), K(k), K(j));
          } else {
            ObExpr *rt_expr = ObStaticEngineExprCG::get_rt_expr(*raw_expr);
            if (OB_ISNULL(rt_expr)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("groupby rt_expr is null", K(ret), K(k), K(j));
            } else if (OB_FAIL(temp_arr.push_back(rt_expr))) {
              LOG_WARN("push_back failed", K(ret));
            }
          }
        }
        if (OB_SUCC(ret) && OB_FAIL(spec->group_set_exprs_.at(k).assign(temp_arr))) {
          LOG_WARN("assign group_set_exprs[k] failed", K(ret), K(k));
          return nullptr;
        }
      }
    }

    // expand_exprs_, gby_exprs_, dup_expr_pairs_ stay empty (grouping sets path)
    // hash_val_expr_ stays nullptr

    return spec;
  }

  ObOperator *create_op(ObExecContext &ctx, ObOpSpec &spec, ObOperator *child_op)
  {
    return default_create_op<ObExpandVecOp>(ctx, spec, child_op);
  }

  ObOperator *create_op(ObExecContext &ctx, ObOperator *child_op)
  {
    return child_op;
  }

private:
  bool is_ordered_output_;
  bool use_rollup_;
  bool use_grouping_sets_;
  ObOpPseudoColumnRawExpr *grouping_id_raw_;
};

}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_EXPAND_H_
