/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_UNITTEST_SQL_ENGINE_OP_TESTS_OB_OP_TEST_MERGE_JOIN_H_
#define OCEANBASE_UNITTEST_SQL_ENGINE_OP_TESTS_OB_OP_TEST_MERGE_JOIN_H_

#include "lib/ob_errno.h"
#define USING_LOG_PREFIX SQL
#define private public
#include "sql/engine/ob_operator.h"
#include "sql/engine/join/ob_merge_join_op.h"
#include "sql/engine/join/ob_merge_join_vec_op.h"
#undef private
#include "sql/ob_sql_define.h"
#include "share/datum/ob_datum_funcs.h"
#include "unittest/sql/engine/op_tests/ob_op_test_join_base.h"

namespace oceanbase
{
namespace sql
{

/**
 * @brief MergeJoinTestSpec - Test specification for Merge Join operator.
 *
 * Supports both 1.0 (ObMergeJoinOp/PHY_MERGE_JOIN) and 2.0 (ObMergeJoinVecOp/PHY_VEC_MERGE_JOIN)
 * operators. Merge Join requires both input tables to be sorted by join key.
 *
 * Usage:
 *   MergeJoinTestSpec()
 *     .left_table("t1", "a int, b int")
 *     .right_table("t2", "c int, d int")
 *     .select("t1.a, t1.b, t2.c, t2.d")
 *     .with_left_sorted_data({{1, 10}, {3, 30}}, "a ASC")
 *     .with_right_sorted_data({{1, 100}, {3, 300}}, "c ASC")
 *     .join_type(INNER_JOIN)
 *     .on("t1.a = t2.c")
 *     .run(engine_);
 *
 * Features:
 * - Automatic equal condition extraction from ON clause
 * - Automatic ns_cmp_func_ initialization via ObDatumFuncs
 * - Smart is_opposite_ detection based on table_id
 * - Supports multiple join types (INNER, LEFT, RIGHT, FULL)
 */
class MergeJoinTestSpec : public JoinSpecBuilder<MergeJoinTestSpec>
{
public:
  MergeJoinTestSpec() : is_left_unique_(false) {}
  ~MergeJoinTestSpec() = default;

  /**
   * @brief Set whether left table is unique (for optimization).
   */
  MergeJoinTestSpec& with_left_unique(bool is_unique)
  {
    is_left_unique_ = is_unique;
    return *this;
  }

  /**
   * @brief Create ObMergeJoinVecSpec (2.0) or ObMergeJoinSpec (1.0) based on use_rich_format.
   */
  ObOpSpec *create_spec(common::ObIAllocator &alloc,
                        MockDataSourceSpec *left_mock_spec,
                        MockDataSourceSpec *right_mock_spec,
                        const ExprFixedArray &output_exprs,
                        bool use_rich_format)
  {
    int ret = OB_SUCCESS;
    if (use_rich_format) {
      return create_spec_impl<ObMergeJoinVecSpec, ObMergeJoinVecOp>(
          alloc, left_mock_spec, right_mock_spec, output_exprs, use_rich_format, PHY_VEC_MERGE_JOIN);
    } else {
      return create_spec_impl<ObMergeJoinSpec, ObMergeJoinOp>(
          alloc, left_mock_spec, right_mock_spec, output_exprs, use_rich_format, PHY_MERGE_JOIN);
    }
  }

  /**
   * @brief Create ObMergeJoinVecOp (2.0) or ObMergeJoinOp (1.0) based on spec type.
   */
  ObOperator *create_op(ObExecContext &ctx, ObOpSpec &spec,
                        ObOperator *left_op, ObOperator *right_op)
  {
    if (spec.type_ == PHY_VEC_MERGE_JOIN) {
      return default_create_join_op<ObMergeJoinVecOp>(ctx, spec, left_op, right_op);
    } else {
      return default_create_join_op<ObMergeJoinOp>(ctx, spec, left_op, right_op);
    }
  }

  /**
   * @brief No OpInput needed for Merge Join.
   */
  ObOpInput *create_input(ObExecContext &ctx, ObOpSpec &spec)
  {
    return nullptr;
  }

private:
  bool is_left_unique_;

  // For 2.0 (Vec) spec, get comparison function from basic_funcs_
  // SFINAE: only compiles when T's ns_cmp_func_ is compatible with NullSafeRowCmpFunc
  template <typename T>
  auto fill_ns_cmp_func_2_0(T &equal_cond_info, ObExpr *rt_expr, bool, int)
    -> decltype(equal_cond_info.ns_cmp_func_ = rt_expr->basic_funcs_->row_null_last_cmp_, void())
  {
    if (OB_ISNULL(rt_expr) || OB_ISNULL(rt_expr->basic_funcs_)) {
      equal_cond_info.ns_cmp_func_ = nullptr;
      return;
    }
    equal_cond_info.ns_cmp_func_ = rt_expr->basic_funcs_->row_null_last_cmp_;
  }
  template <typename T>
  void fill_ns_cmp_func_2_0(T &, ObExpr *, bool, long)
  {
    // 1.0: ns_cmp_func_ is ObDatumCmpFuncType, handled by fill_ns_cmp_func_1_0
  }

  // SFINAE helpers for setting is_left_unique_ (only on 1.0 spec)
  template <typename T>
  auto set_is_left_unique(T *spec, bool val, int)
    -> decltype(spec->is_left_unique_ = val, void())
  {
    spec->is_left_unique_ = val;
  }
  template <typename T>
  void set_is_left_unique(T *, bool, long) {}

  // SFINAE helpers for initializing equal_keys arrays (only on 2.0 Vec spec)
  template <typename T>
  auto init_equal_keys(T *spec, int64_t cnt, int)
    -> decltype(spec->init_equal_keys(cnt), void())
  {
    spec->init_equal_keys(cnt);
  }
  template <typename T>
  void init_equal_keys(T *, int64_t, long) {}

  // SFINAE helpers for pushing equal key exprs (only on 2.0 Vec spec)
  template <typename T>
  auto push_equal_keys(T *spec, ObExpr *left_key, ObExpr *right_key,
                        int64_t left_key_idx, int64_t right_key_idx, int)
    -> decltype(spec->left_child_fetcher_equal_keys_.push_back(left_key), void())
  {
    spec->left_child_fetcher_equal_keys_.push_back(left_key);
    spec->right_child_fetcher_equal_keys_.push_back(right_key);
    spec->left_child_fetcher_equal_keys_idx_.push_back(left_key_idx);
    spec->right_child_fetcher_equal_keys_idx_.push_back(right_key_idx);
  }
  template <typename T>
  void push_equal_keys(T *, ObExpr *, ObExpr *, int64_t, int64_t, long) {}

  // SFINAE helpers for ns_cmp_func_ type detection
  template <typename T>
  auto fill_ns_cmp_func_1_0(T &equal_cond_info,
                             const ObDatumMeta &l_meta,
                             const ObDatumMeta &r_meta,
                             const bool has_lob_header,
                             const ObScale scale,
                             bool is_opposite, int)
    -> decltype(equal_cond_info.ns_cmp_func_ = ObDatumFuncs::get_nullsafe_cmp_func(
        l_meta.type_, r_meta.type_, common::ObCmpNullPos::NULL_LAST, l_meta.cs_type_, scale,
        lib::is_oracle_mode(), has_lob_header, l_meta.precision_, r_meta.precision_), void())
  {
    const common::ObCmpNullPos null_pos = common::ObCmpNullPos::NULL_LAST;
    if (is_opposite) {
      equal_cond_info.ns_cmp_func_ = ObDatumFuncs::get_nullsafe_cmp_func(
          r_meta.type_, l_meta.type_, null_pos, r_meta.cs_type_, scale,
          lib::is_oracle_mode(), has_lob_header, r_meta.precision_, l_meta.precision_);
    } else {
      equal_cond_info.ns_cmp_func_ = ObDatumFuncs::get_nullsafe_cmp_func(
          l_meta.type_, r_meta.type_, null_pos, l_meta.cs_type_, scale,
          lib::is_oracle_mode(), has_lob_header, l_meta.precision_, r_meta.precision_);
    }
  }
  template <typename T>
  void fill_ns_cmp_func_1_0(T &, const ObDatumMeta &, const ObDatumMeta &,
                             const bool, const ObScale, bool, long)
  {
    // 2.0: handled by fill_ns_cmp_func_2_0 below
  }

  /**
   * @brief Templated spec creation for both 1.0 and 2.0 Merge Join specs.
   *
   * Key steps:
   * 1. Initialize equal_cond_infos_ from ON clause equal conditions
   * 2. Compute ns_cmp_func_ via ObDatumFuncs (1.0) or basic_funcs_ (2.0)
   * 3. Determine is_opposite_ based on table_id
   * 4. Populate left/right_child_fetcher_all_exprs_
   * 5. Set merge_directions_ (default all ASC)
   */
  template <typename SpecType, typename OpType>
  SpecType *create_spec_impl(common::ObIAllocator &alloc,
                               MockDataSourceSpec *left_mock_spec,
                               MockDataSourceSpec *right_mock_spec,
                               const ExprFixedArray &output_exprs,
                               bool use_rich_format,
                               ObPhyOperatorType op_type)
  {
    int ret = OB_SUCCESS;
    FatalErrorChecker error_checker(ret);

    // Step 1: Allocate spec
    void *mem = alloc.alloc(sizeof(SpecType));
    if (OB_ISNULL(mem)) {
      LOG_WARN("alloc merge join spec failed", K(ret));
      return nullptr;
    }
    SpecType *mj_spec = new (mem) SpecType(alloc, op_type);

    // Step 2: Set basic fields
    mj_spec->plan_ = left_mock_spec->plan_;
    mj_spec->max_batch_size_ = left_mock_spec->max_batch_size_;
    mj_spec->use_rich_format_ = use_rich_format;
    mj_spec->output_ = output_exprs;
    mj_spec->join_type_ = join_type_;
    set_is_left_unique(mj_spec, is_left_unique_, 0);

    // Step 3: Set children pointer
    void *child_spec_mem = alloc.alloc(sizeof(ObOpSpec *) * 2);
    if (OB_ISNULL(child_spec_mem)) {
      LOG_WARN("alloc children array failed", K(ret));
      return nullptr;
    }
    ObOpSpec **children = reinterpret_cast<ObOpSpec **>(child_spec_mem);
    children[0] = left_mock_spec;
    children[1] = right_mock_spec;
    if (OB_FAIL(mj_spec->set_children_pointer(children, 2))) {
      LOG_WARN("set children pointer failed", K(ret));
      return nullptr;
    }

    // Step 4: Extract equal conditions from ON clause
    // Join ON conditions are stored in JoinedTable::join_conditions_, not in get_condition_exprs()
    // get_condition_exprs() returns WHERE conditions only
    common::ObSEArray<ObRawExpr *, 4> equal_join_conds;

    // Collect from JoinedTable::join_conditions_ (ON clause)
    const common::ObIArray<JoinedTable *> &joined_tables = resolved_stmt_->get_joined_tables();
    for (int64_t i = 0; i < joined_tables.count(); ++i) {
      JoinedTable *jt = joined_tables.at(i);
      if (OB_NOT_NULL(jt)) {
        const common::ObIArray<ObRawExpr *> &join_conds = jt->get_join_conditions();
        for (int64_t j = 0; j < join_conds.count(); ++j) {
          ObRawExpr *raw_expr = join_conds.at(j);
          if (OB_NOT_NULL(raw_expr)) {
            ObItemType expr_type = raw_expr->get_expr_type();
            if (T_OP_EQ == expr_type || T_OP_NSEQ == expr_type) {
              if (OB_FAIL(equal_join_conds.push_back(raw_expr))) {
                LOG_WARN("push equal join cond failed", K(ret));
                return nullptr;
              }
            }
          }
        }
      }
    }

    // Also check condition_exprs (WHERE clause) for additional equal conditions
    const common::ObIArray<ObRawExpr *> &cond_exprs = resolved_stmt_->get_condition_exprs();
    for (int64_t i = 0; i < cond_exprs.count(); ++i) {
      ObRawExpr *raw_expr = cond_exprs.at(i);
      if (OB_NOT_NULL(raw_expr)) {
        ObItemType expr_type = raw_expr->get_expr_type();
        if (T_OP_EQ == expr_type || T_OP_NSEQ == expr_type) {
          // Avoid duplicates
          bool exists = false;
          for (int64_t j = 0; j < equal_join_conds.count(); ++j) {
            if (equal_join_conds.at(j) == raw_expr) {
              exists = true;
              break;
            }
          }
          if (!exists) {
            if (OB_FAIL(equal_join_conds.push_back(raw_expr))) {
              LOG_WARN("push equal join cond failed", K(ret));
              return nullptr;
            }
          }
        }
      }
    }

    // Step 5: Initialize equal_cond_infos_
    if (OB_FAIL(mj_spec->equal_cond_infos_.init(equal_join_conds.count()))) {
      LOG_WARN("init equal_cond_infos failed", K(ret));
      return nullptr;
    }
    // For 2.0 spec, also initialize equal_keys arrays
    init_equal_keys(mj_spec, equal_join_conds.count(), 0);

    // Step 6: Initialize left/right_child_fetcher_all_exprs_
    // left_child_fetcher_all_exprs_ = left output + left join keys
    // right_child_fetcher_all_exprs_ = right output + right join keys
    const int64_t left_output_cnt = left_mock_spec->output_.count();
    const int64_t right_output_cnt = right_mock_spec->output_.count();
    const int64_t equal_cond_cnt = equal_join_conds.count();

    if (OB_FAIL(mj_spec->left_child_fetcher_all_exprs_.init(left_output_cnt + equal_cond_cnt))) {
      LOG_WARN("init left_child_fetcher_all_exprs failed", K(ret));
      return nullptr;
    }
    if (OB_FAIL(mj_spec->right_child_fetcher_all_exprs_.init(right_output_cnt + equal_cond_cnt))) {
      LOG_WARN("init right_child_fetcher_all_exprs failed", K(ret));
      return nullptr;
    }

    // Step 7: Append left/right child output to fetcher_all_exprs
    for (int64_t i = 0; OB_SUCC(ret) && i < left_output_cnt; ++i) {
      if (OB_FAIL(mj_spec->left_child_fetcher_all_exprs_.push_back(left_mock_spec->output_.at(i)))) {
        LOG_WARN("push left output expr failed", K(ret), K(i));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < right_output_cnt; ++i) {
      if (OB_FAIL(mj_spec->right_child_fetcher_all_exprs_.push_back(right_mock_spec->output_.at(i)))) {
        LOG_WARN("push right output expr failed", K(ret), K(i));
      }
    }

    // Step 8: Process each equal condition
    // Need to find table_id for each side of the equal condition
    const common::ObIArray<ColumnItem> &column_items = resolved_stmt_->get_column_items();
    const common::ObIArray<TableItem *> &tables = resolved_stmt_->get_table_items();

    // Find left and right table_id from resolved tables
    uint64_t left_resolved_table_id = 0;
    uint64_t right_resolved_table_id = 0;
    for (int64_t i = 0; i < tables.count(); ++i) {
      TableItem *table = tables.at(i);
      if (OB_NOT_NULL(table)) {
        std::string table_name(table->table_name_.ptr(), table->table_name_.length());
        if (table_name == left_table_name_) {
          left_resolved_table_id = table->table_id_;
        } else if (table_name == right_table_name_) {
          right_resolved_table_id = table->table_id_;
        }
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < equal_join_conds.count(); ++i) {
      ObRawExpr *raw_expr = equal_join_conds.at(i);
      if (OB_ISNULL(raw_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("raw_expr is null", K(ret), K(i));
        continue;
      }

      // Get runtime expression
      ObExpr *rt_expr = ObStaticEngineExprCG::get_rt_expr(*raw_expr);
      if (OB_ISNULL(rt_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get_rt_expr returned null", K(ret), K(i));
        continue;
      }

      // Check it's a binary operator (arg_cnt_ == 2)
      if (rt_expr->arg_cnt_ != 2 || OB_ISNULL(rt_expr->args_) ||
          OB_ISNULL(rt_expr->args_[0]) || OB_ISNULL(rt_expr->args_[1])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid equal condition expr", K(ret), K(i));
        continue;
      }

      // Create EqualConditionInfo
      typename SpecType::EqualConditionInfo equal_cond_info;
      equal_cond_info.expr_ = rt_expr;

      // Determine is_opposite_ by checking which side of the equal condition
      // comes from which table
      bool is_left_arg_from_left = is_expr_from_table(rt_expr->args_[0], left_resolved_table_id,
                                                       column_items);
      bool is_right_arg_from_right = is_expr_from_table(rt_expr->args_[1], right_resolved_table_id,
                                                         column_items);

      if (is_left_arg_from_left && is_right_arg_from_right) {
        equal_cond_info.is_opposite_ = false;
      } else {
        // Check if reversed
        bool is_left_arg_from_right = is_expr_from_table(rt_expr->args_[0], right_resolved_table_id,
                                                          column_items);
        bool is_right_arg_from_left = is_expr_from_table(rt_expr->args_[1], left_resolved_table_id,
                                                          column_items);
        if (is_left_arg_from_right && is_right_arg_from_left) {
          equal_cond_info.is_opposite_ = true;
        } else {
          // Default: try to infer from datum_meta or just set to false
          equal_cond_info.is_opposite_ = false;
        }
      }

      // Get datum meta for comparison function
      ObDatumMeta &l_meta = rt_expr->args_[0]->datum_meta_;
      ObDatumMeta &r_meta = rt_expr->args_[1]->datum_meta_;
      const bool has_lob_header = rt_expr->args_[0]->obj_meta_.has_lob_header() ||
                                   rt_expr->args_[1]->obj_meta_.has_lob_header();
      const ObScale scale = ObDatumFuncs::max_scale(l_meta.scale_, r_meta.scale_);

      // Get nullsafe comparison function
      // 1.0 uses ObDatumCmpFuncType via ObDatumFuncs::get_nullsafe_cmp_func
      // 2.0 uses NullSafeRowCmpFunc via basic_funcs_->row_null_last_cmp_
      fill_ns_cmp_func_1_0(equal_cond_info, l_meta, r_meta, has_lob_header, scale,
                           equal_cond_info.is_opposite_, 0);
      fill_ns_cmp_func_2_0(equal_cond_info, rt_expr, equal_cond_info.is_opposite_, 0);

      if (OB_ISNULL(equal_cond_info.ns_cmp_func_)) {
        LOG_WARN("get_nullsafe_cmp_func returned null", K(ret), K(i),
                 K(l_meta.type_), K(r_meta.type_));
        // Continue anyway, use null comparison function
      }

      // Push to equal_cond_infos_
      if (OB_FAIL(mj_spec->equal_cond_infos_.push_back(equal_cond_info))) {
        LOG_WARN("push equal_cond_info failed", K(ret), K(i));
        continue;
      }

      // Add join keys to fetcher_all_exprs (without duplicates)
      // For left_child_fetcher_all_exprs_: add the arg from left table
      // For right_child_fetcher_all_exprs_: add the arg from right table
      ObExpr *left_key = equal_cond_info.is_opposite_ ? rt_expr->args_[1] : rt_expr->args_[0];
      ObExpr *right_key = equal_cond_info.is_opposite_ ? rt_expr->args_[0] : rt_expr->args_[1];

      // Check if already in fetcher_all_exprs to avoid duplicates
      bool left_key_exists = false;
      bool right_key_exists = false;
      for (int64_t j = 0; j < mj_spec->left_child_fetcher_all_exprs_.count(); ++j) {
        if (mj_spec->left_child_fetcher_all_exprs_.at(j) == left_key) {
          left_key_exists = true;
          break;
        }
      }
      for (int64_t j = 0; j < mj_spec->right_child_fetcher_all_exprs_.count(); ++j) {
        if (mj_spec->right_child_fetcher_all_exprs_.at(j) == right_key) {
          right_key_exists = true;
          break;
        }
      }

      if (!left_key_exists && OB_FAIL(mj_spec->left_child_fetcher_all_exprs_.push_back(left_key))) {
        LOG_WARN("push left join key failed", K(ret), K(i));
        continue;
      }
      if (!right_key_exists && OB_FAIL(mj_spec->right_child_fetcher_all_exprs_.push_back(right_key))) {
        LOG_WARN("push right join key failed", K(ret), K(i));
        continue;
      }

      // For 2.0 (Vec) spec, also populate equal_keys and their idx arrays
      // These are required by ObMergeJoinVecOp for vectorized comparison
      int64_t left_key_idx = -1;
      int64_t right_key_idx = -1;
      for (int64_t j = 0; j < mj_spec->left_child_fetcher_all_exprs_.count() && left_key_idx < 0; ++j) {
        if (mj_spec->left_child_fetcher_all_exprs_.at(j) == left_key) {
          left_key_idx = j;
        }
      }
      for (int64_t j = 0; j < mj_spec->right_child_fetcher_all_exprs_.count() && right_key_idx < 0; ++j) {
        if (mj_spec->right_child_fetcher_all_exprs_.at(j) == right_key) {
          right_key_idx = j;
        }
      }
      if (left_key_idx >= 0 && right_key_idx >= 0) {
        push_equal_keys(mj_spec, left_key, right_key, left_key_idx, right_key_idx, 0);
      }
    }  // end for each equal condition

    // Step 9: Set merge_directions_ matching the sort order of input data
    if (OB_SUCC(ret)) {
      common::ObSEArray<ObOrderDirection, 4> merge_directions;
      const common::ObIArray<ColumnItem> &col_items = resolved_stmt_->get_column_items();
      for (int64_t i = 0; OB_SUCC(ret) && i < mj_spec->equal_cond_infos_.count(); ++i) {
        const auto &eq_cond = mj_spec->equal_cond_infos_.at(i);
        ObExpr *left_key = eq_cond.is_opposite_ ? eq_cond.expr_->args_[1] : eq_cond.expr_->args_[0];
        ObExpr *right_key = eq_cond.is_opposite_ ? eq_cond.expr_->args_[0] : eq_cond.expr_->args_[1];
        ObOrderDirection dir = get_merge_direction_for_expr(left_key, left_sorted_desc_, col_items);
        if (OB_FAIL(merge_directions.push_back(dir))) {
          LOG_WARN("push merge direction failed", K(ret), K(i));
        }
      }
      if (OB_SUCC(ret) && merge_directions.count() > 0) {
        if (OB_FAIL(mj_spec->set_merge_directions(merge_directions))) {
          LOG_WARN("set merge directions failed", K(ret));
        }
      }
    }

    return mj_spec;
  }

  /**
   * @brief Check if an expression comes from a specific table.
   *
   * This is used to determine is_opposite_ for equal conditions.
   * We check if any ColumnRef in the expression has the given table_id.
   */
  bool is_expr_from_table(ObExpr *expr, uint64_t table_id,
                           const common::ObIArray<ColumnItem> &column_items)
  {
    if (OB_ISNULL(expr)) {
      return false;
    }

    // For column reference (T_REF_COLUMN), check table_id directly
    if (T_REF_COLUMN == expr->type_) {
      // Find the column item that matches this expression
      for (int64_t i = 0; i < column_items.count(); ++i) {
        const ColumnItem &col = column_items.at(i);
        if (OB_NOT_NULL(col.expr_)) {
          ObExpr *col_expr = ObStaticEngineExprCG::get_rt_expr(*col.expr_);
          if (col_expr == expr) {
            return col.table_id_ == table_id;
          }
        }
      }
    }

    // For complex expressions, check all children recursively
    for (uint32_t i = 0; i < expr->arg_cnt_ && OB_NOT_NULL(expr->args_); ++i) {
      if (is_expr_from_table(expr->args_[i], table_id, column_items)) {
        return true;
      }
    }

    return false;
  }

  /**
   * @brief Determine merge direction for a given join key expression
   * based on the sort order description string.
   *
   * Parses "col ASC" or "col DESC" patterns to match the sort direction.
   * Returns NULLS_LAST_ASC by default if no match is found.
   */
  ObOrderDirection get_merge_direction_for_expr(ObExpr *key_expr,
                                                 const std::string &sort_desc,
                                                 const common::ObIArray<ColumnItem> &column_items)
  {
    if (sort_desc.empty() || OB_ISNULL(key_expr)) {
      return ObOrderDirection::NULLS_LAST_ASC;
    }

    // Try to find the column name for this expression
    std::string key_col_name;
    if (T_REF_COLUMN == key_expr->type_) {
      for (int64_t i = 0; i < column_items.count(); ++i) {
        const ColumnItem &col = column_items.at(i);
        if (OB_NOT_NULL(col.expr_)) {
          ObExpr *col_expr = ObStaticEngineExprCG::get_rt_expr(*col.expr_);
          if (col_expr == key_expr) {
            key_col_name.assign(col.column_name_.ptr(), col.column_name_.length());
            break;
          }
        }
      }
    }

    if (key_col_name.empty()) {
      return ObOrderDirection::NULLS_LAST_ASC;
    }

    // Search for "col_name ASC" or "col_name DESC" in sort_desc
    std::string upper_col = key_col_name;
    for (auto &c : upper_col) c = ::toupper(c);
    std::string upper_desc = sort_desc;
    for (auto &c : upper_desc) c = ::toupper(c);

    size_t pos = upper_desc.find(upper_col);
    if (pos == std::string::npos) {
      return ObOrderDirection::NULLS_LAST_ASC;
    }

    // Check what follows the column name
    std::string after = upper_desc.substr(pos + upper_col.length());
    // Trim leading whitespace
    size_t start = after.find_first_not_of(" \t");
    if (start == std::string::npos) {
      return ObOrderDirection::NULLS_LAST_ASC;
    }
    after = after.substr(start);

    // Check for DESC or ASC keyword
    if (after.substr(0, 4) == "DESC") {
      return ObOrderDirection::NULLS_FIRST_DESC;
    } else if (after.substr(0, 3) == "ASC") {
      return ObOrderDirection::NULLS_FIRST_ASC;
    }

    return ObOrderDirection::NULLS_FIRST_ASC;
  }
};

}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_MERGE_JOIN_H_