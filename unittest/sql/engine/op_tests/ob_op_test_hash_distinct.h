/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_HASH_DISTINCT_H_
#define OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_HASH_DISTINCT_H_

#include "unittest/sql/engine/op_tests/ob_op_test_base.h"
#include "sql/engine/aggregate/ob_hash_distinct_op.h"  // ObHashDistinctSpec, ObHashDistinctOp
#include "sql/engine/aggregate/ob_hash_distinct_vec_op.h"  // ObHashDistinctVecSpec, ObHashDistinctVecOp
#include "sql/engine/aggregate/ob_distinct_op.h"
#include "sql/engine/ob_operator.h"
#include "sql/code_generator/ob_static_engine_expr_cg.h"
#include "share/datum/ob_datum_funcs.h"
#include <vector>
#include <string>

namespace oceanbase
{
namespace sql
{

/**
 * @brief HashDistinctTestSpec - Test specification for Hash Distinct operator.
 *
 * Supports both 1.0 (ObHashDistinctOp/PHY_HASH_DISTINCT) and
 * 2.0 (ObHashDistinctVecOp/PHY_VEC_HASH_DISTINCT) operators.
 * The operator version is selected at create_spec() time via the use_rich_format parameter.
 *
 * Usage:
 *   HashDistinctTestSpec spec;
 *   spec.table("t", "a int, b int")
 *       .select("a, b")
 *       .with_data({{1, 10}, {1, 10}, {2, 20}})
 *       .run(engine);
 *
 * Advanced usage:
 *   // Distinct on specific columns (others as additional exprs)
 *   spec.select("a, b, c")
 *       .distinct_by("a")
 *       .run(engine);
 *
 *   // Block mode (default) or non-block mode
 *   spec.select("a, b")
 *       .block_mode(false)
 *       .run(engine);
 *
 *   // Enable adaptive bypass (automatically sets non-block + push_down)
 *   spec.select("a")
 *       .enable_bypass(true)
 *       .run(engine);
 *
 *   // Group distinct (Vec-only, for multi-group distinct aggregation)
 *   spec.select("gid, a, b")
 *       .with_rich_format(true)
 *       .with_group_distinct({{"a"}, {"b"}}, "gid")
 *       .run(engine);
 *
 *   // Dual-format comparison (runs both 1.0 and 2.0, verifies identical results)
 *   spec.select("a, b")
 *       .enable_dual_format_check()
 *       .run(engine);
 */
class HashDistinctTestSpec : public OpSpecBuilder<HashDistinctTestSpec>
{
public:
  HashDistinctTestSpec()
    : block_mode_(true),
      push_down_(false),
      enable_bypass_(false),
      group_distinct_groups_(),
      grouping_id_col_()
  {}
  ~HashDistinctTestSpec() = default;

  // ===== Configuration Interface =====

  /**
   * @brief Specify which columns are distinct keys.
   * Columns not in distinct_by become additional exprs.
   * If not called, all select columns are distinct keys.
   * @param exprs Comma-separated column names (e.g., "a, b")
   */
  HashDistinctTestSpec& distinct_by(const std::string &exprs)
  {
    distinct_by_exprs_ = exprs;
    return *this;
  }

  /**
   * @brief Set block mode (default true).
   * Block mode: build hash table first, then output.
   * Non-block mode: output row-by-row while building.
   * @param enable true for block mode, false for non-block
   */
  HashDistinctTestSpec& block_mode(bool enable = true)
  {
    // Enforce bypass invariant: bypass_enabled => !block_mode
    if (enable && enable_bypass_) {
      EXPECT_TRUE(false) << "block_mode(true) conflicts with enable_bypass=true: bypass requires non-block mode";
      return *this;
    }
    block_mode_ = enable;
    return *this;
  }

  /**
   * @brief Enable push_down mode (forced pushdown without bypass).
   * Conflicts with enable_bypass (bypass already implies push_down).
   * @param enable true to enable forced push_down
   */
  HashDistinctTestSpec& push_down(bool enable = true)
  {
    if (enable && enable_bypass_) {
      EXPECT_TRUE(false) << "push_down(true) conflicts with enable_bypass=true: bypass already implies push_down";
      return *this;
    }
    push_down_ = enable;
    return *this;
  }

  /**
   * @brief Enable adaptive bypass mode.
   * When enabled, automatically sets:
   *   - is_push_down_ = true
   *   - is_block_mode_ = false
   * Invariant: bypass_enabled => !block_mode && push_down
   * @param enable true to enable bypass
   */
  HashDistinctTestSpec& enable_bypass(bool enable = true)
  {
    enable_bypass_ = enable;
    if (enable) {
      // Bypass requires: push_down=true, block_mode=false
      push_down_ = true;
      block_mode_ = false;
    }
    return *this;
  }

  /**
   * @brief Configure group distinct for Vec-only multi-group distinct.
   * Each group has its own set of distinct columns.
   * grouping_id_col identifies which group each row belongs to.
   * @param groups Vector of groups, each group is a vector of column names
   * @param grouping_id_col Column name for grouping ID
   */
  HashDistinctTestSpec& with_group_distinct(
      const std::vector<std::vector<std::string>> &groups,
      const std::string &grouping_id_col)
  {
    group_distinct_groups_ = groups;
    grouping_id_col_ = grouping_id_col;
    return *this;
  }

  // ===== Create Spec =====

  /**
   * @brief Create ObHashDistinctVecSpec (2.0) or ObHashDistinctSpec (1.0).
   *
   * For 2.0: uses PHY_VEC_HASH_DISTINCT, supports group_distinct_exprs_.
   * For 1.0: uses PHY_HASH_DISTINCT, supports cmp_funcs_/hash_funcs_.
   *
   * Both paths:
   *   - Fill distinct_exprs_ from select items (split by distinct_by_exprs_)
   *   - Fill sort_collations_ for distinct keys
   *   - Set is_block_mode_ / by_pass_enabled_ / is_push_down_
   *
   * Row-only (1.0):
   *   - Fill cmp_funcs_ using ObDatumFuncs::get_nullsafe_cmp_func()
   *   - Fill hash_funcs_ using expr->basic_funcs_->murmur_hash_
   *
   * Vec-only (2.0) with group_distinct:
   *   - Fill group_distinct_exprs_ / group_sort_collations_ / grouping_id_
   */
  ObOpSpec *create_spec(common::ObIAllocator &alloc, MockDataSourceSpec *child_spec,
                         const ExprFixedArray &output_exprs,
                         ObExpr *limit_expr, ObExpr *offset_expr, bool use_rich_format)
  {
    int ret = OB_SUCCESS;
    FatalErrorChecker error_checker(ret);

    if (use_rich_format) {
      // 2.0: ObHashDistinctVecSpec
      void *mem = alloc.alloc(sizeof(ObHashDistinctVecSpec));
      if (OB_ISNULL(mem)) {
        LOG_WARN("alloc ObHashDistinctVecSpec failed", K(ret));
        return nullptr;
      }
      ObHashDistinctVecSpec *spec = new (mem) ObHashDistinctVecSpec(alloc, PHY_VEC_HASH_DISTINCT);
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

      // Fill distinct_exprs_ and sort_collations_
      if (OB_FAIL(fill_distinct_exprs_and_collations(alloc, spec, use_rich_format))) {
        LOG_WARN("fill_distinct_exprs_and_collations failed", K(ret));
        return nullptr;
      }

      // Set mode flags
      spec->is_block_mode_ = block_mode_;
      spec->by_pass_enabled_ = enable_bypass_;
      spec->is_push_down_ = push_down_;

      // Fill group_distinct if configured (Vec-only)
      if (!group_distinct_groups_.empty()) {
        if (OB_FAIL(fill_group_distinct(alloc, spec))) {
          LOG_WARN("fill_group_distinct failed", K(ret));
          return nullptr;
        }
      }

      return spec;
    } else {
      // 1.0: ObHashDistinctSpec
      void *mem = alloc.alloc(sizeof(ObHashDistinctSpec));
      if (OB_ISNULL(mem)) {
        LOG_WARN("alloc ObHashDistinctSpec failed", K(ret));
        return nullptr;
      }
      ObHashDistinctSpec *spec = new (mem) ObHashDistinctSpec(alloc, PHY_HASH_DISTINCT);
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

      // Fill distinct_exprs_ and sort_collations_
      if (OB_FAIL(fill_distinct_exprs_and_collations(alloc, spec, use_rich_format))) {
        LOG_WARN("fill_distinct_exprs_and_collations failed", K(ret));
        return nullptr;
      }

      // Fill cmp_funcs_ and hash_funcs_ (Row-only)
      if (OB_FAIL(fill_cmp_and_hash_funcs(alloc, spec))) {
        LOG_WARN("fill_cmp_and_hash_funcs failed", K(ret));
        return nullptr;
      }

      // Set mode flags
      spec->is_block_mode_ = block_mode_;
      spec->by_pass_enabled_ = enable_bypass_;
      spec->is_push_down_ = push_down_;

      return spec;
    }
  }

  // ===== Create Operator =====

  /**
   * @brief Create ObHashDistinctVecOp (2.0) or ObHashDistinctOp (1.0) based on spec type.
   */
  ObOperator *create_op(ObExecContext &ctx, ObOpSpec &spec, ObOperator *child_op)
  {
    if (spec.type_ == PHY_VEC_HASH_DISTINCT) {
      return default_create_op<ObHashDistinctVecOp>(ctx, spec, child_op);
    } else {
      return default_create_op<ObHashDistinctOp>(ctx, spec, child_op);
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
   * @brief Parse distinct_by_exprs_ string to get distinct key column names.
   * Returns vector of column names (uppercase, trimmed).
   */
  std::vector<std::string> parse_distinct_keys() const
  {
    std::vector<std::string> keys;
    if (distinct_by_exprs_.empty()) {
      return keys;
    }
    std::string exprs = distinct_by_exprs_;
    // Split by comma
    size_t pos = 0;
    while (pos < exprs.size()) {
      // Skip whitespace
      while (pos < exprs.size() && std::isspace(exprs[pos])) pos++;
      if (pos >= exprs.size()) break;
      // Find end of name
      size_t start = pos;
      while (pos < exprs.size() && exprs[pos] != ',' && !std::isspace(exprs[pos])) pos++;
      std::string name = exprs.substr(start, pos - start);
      // Convert to uppercase
      std::transform(name.begin(), name.end(), name.begin(), ::toupper);
      if (!name.empty()) {
        keys.push_back(name);
      }
      // Skip comma
      while (pos < exprs.size() && (exprs[pos] == ',' || std::isspace(exprs[pos]))) pos++;
    }
    return keys;
  }

  /**
   * @brief Find ObExpr* by column name in select_items or column_items.
   * Uses same matching logic as build_sort_key_specs (case-insensitive, strip table prefix).
   */
  ObExpr *find_expr_by_name(const std::string &name_upper,
                              const common::ObIArray<SelectItem> &select_items,
                              const common::ObIArray<ColumnItem> &column_items) const
  {
    auto trim_ws = [](const std::string &s) -> std::string {
      size_t a = s.find_first_not_of(" \t");
      size_t b = s.find_last_not_of(" \t");
      return (a == std::string::npos) ? "" : s.substr(a, b - a + 1);
    };
    auto strip_table_prefix = [](const std::string &s) -> std::string {
      std::string result = s;
      size_t pos = 0;
      while ((pos = result.find('.', pos)) != std::string::npos && pos > 0) {
        size_t start = pos;
        while (start > 0 && (isalnum(result[start-1]) || result[start-1] == '_')) {
          start--;
        }
        if (start < pos) {
          result.erase(start, pos - start + 1);
          pos = start;
        } else {
          pos++;
        }
      }
      return result;
    };

    char name_buf[512];
    ObExpr *found_expr = nullptr;

    // 1. Search in select_items
    for (int64_t i = 0; i < select_items.count() && found_expr == nullptr; ++i) {
      ObRawExpr *raw = select_items.at(i).expr_;
      if (OB_ISNULL(raw)) continue;
      int64_t buf_pos = 0;
      if (OB_SUCCESS == raw->get_name(name_buf, static_cast<int64_t>(sizeof(name_buf)) - 1, buf_pos)) {
        std::string select_name(name_buf, buf_pos);
        std::string select_upper = trim_ws(select_name);
        std::transform(select_upper.begin(), select_upper.end(), select_upper.begin(), ::toupper);
        std::string select_stripped = strip_table_prefix(select_upper);
        if (select_upper == name_upper || select_stripped == name_upper) {
          found_expr = ObStaticEngineExprCG::get_rt_expr(*raw);
        }
      }
    }

    // 2. Search in column_items
    if (found_expr == nullptr) {
      for (int64_t i = 0; i < column_items.count() && found_expr == nullptr; ++i) {
        const ColumnItem &col = column_items.at(i);
        if (OB_ISNULL(col.expr_)) continue;
        ObString cn = col.column_name_;
        std::string col_name(cn.ptr(), cn.length());
        std::transform(col_name.begin(), col_name.end(), col_name.begin(), ::toupper);
        if (col_name == name_upper) {
          found_expr = ObStaticEngineExprCG::get_rt_expr(*col.expr_);
        }
      }
    }

    return found_expr;
  }

  /**
   * @brief Fill distinct_exprs_ and sort_collations_ for both Row and Vec specs.
   * distinct_exprs_ = distinct_keys ++ additional_exprs (order preserved)
   */
  template <typename SpecType>
  int fill_distinct_exprs_and_collations(common::ObIAllocator &alloc, SpecType *spec, bool use_rich_format)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(spec) || OB_ISNULL(resolved_stmt_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("spec or resolved_stmt_ is null", K(ret));
      return ret;
    }

    const common::ObIArray<SelectItem> &select_items = resolved_stmt_->get_select_items();
    const common::ObIArray<ColumnItem> &column_items = resolved_stmt_->get_column_items();

    // Parse distinct keys
    std::vector<std::string> distinct_key_names = parse_distinct_keys();
    std::vector<ObExpr*> distinct_keys;
    std::vector<ObExpr*> additional_exprs;

    // If no distinct_by, all select columns are distinct keys
    if (distinct_key_names.empty()) {
      for (int64_t i = 0; i < select_items.count(); ++i) {
        ObRawExpr *raw = select_items.at(i).expr_;
        if (OB_ISNULL(raw)) continue;
        ObExpr *expr = ObStaticEngineExprCG::get_rt_expr(*raw);
        if (OB_ISNULL(expr)) continue;
        distinct_keys.push_back(expr);
      }
    } else {
      // Match distinct keys by name, others go to additional
      for (int64_t i = 0; i < select_items.count(); ++i) {
        ObRawExpr *raw = select_items.at(i).expr_;
        if (OB_ISNULL(raw)) continue;
        ObExpr *expr = ObStaticEngineExprCG::get_rt_expr(*raw);
        if (OB_ISNULL(expr)) continue;

        // Check if this expr matches any distinct key name
        char name_buf[512];
        int64_t buf_pos = 0;
        bool is_distinct_key = false;
        if (OB_SUCCESS == raw->get_name(name_buf, sizeof(name_buf) - 1, buf_pos)) {
          std::string expr_name(name_buf, buf_pos);
          // Strip whitespace and convert to uppercase for comparison
          std::transform(expr_name.begin(), expr_name.end(), expr_name.begin(), ::toupper);
          for (const auto &key_name : distinct_key_names) {
            // Strip table prefix for comparison
            std::string stripped = expr_name;
            size_t dot_pos = stripped.find('.');
            if (dot_pos != std::string::npos && dot_pos > 0) {
              size_t start = dot_pos;
              while (start > 0 && (isalnum(stripped[start-1]) || stripped[start-1] == '_')) start--;
              if (start < dot_pos) stripped.erase(start, dot_pos - start + 1);
            }
            if (stripped == key_name) {
              is_distinct_key = true;
              break;
            }
          }
        }

        if (is_distinct_key) {
          distinct_keys.push_back(expr);
        } else {
          additional_exprs.push_back(expr);
        }
      }
    }

    // Initialize distinct_exprs_: distinct_keys ++ additional_exprs
    const int64_t total_cnt = distinct_keys.size() + additional_exprs.size();
    if (total_cnt > 0) {
      if (OB_FAIL(spec->distinct_exprs_.init(total_cnt))) {
        LOG_WARN("init distinct_exprs_ failed", K(ret), K(total_cnt));
        return ret;
      }
      for (ObExpr *expr : distinct_keys) {
        if (OB_FAIL(spec->distinct_exprs_.push_back(expr))) {
          LOG_WARN("push_back distinct key failed", K(ret));
          return ret;
        }
      }
      for (ObExpr *expr : additional_exprs) {
        if (OB_FAIL(spec->distinct_exprs_.push_back(expr))) {
          LOG_WARN("push_back additional expr failed", K(ret));
          return ret;
        }
      }
    }

    // Initialize sort_collations_ for distinct keys only
    const int64_t key_cnt = distinct_keys.size();
    if (key_cnt > 0) {
      if (OB_FAIL(spec->sort_collations_.init(key_cnt))) {
        LOG_WARN("init sort_collations_ failed", K(ret), K(key_cnt));
        return ret;
      }
      for (int64_t i = 0; i < key_cnt; ++i) {
        ObExpr *expr = distinct_keys[i];
        ObSortFieldCollation collation;
        collation.field_idx_ = i;  // Index in distinct_keys
        collation.cs_type_ = expr->datum_meta_.cs_type_;
        collation.null_pos_ = NULL_FIRST;  // Default null position
        collation.is_ascending_ = true;  // Ascending (distinct doesn't care about order, but need a value)
        collation.is_not_null_ = false;
        if (OB_FAIL(spec->sort_collations_.push_back(collation))) {
          LOG_WARN("push_back sort_collation failed", K(ret), K(i));
          return ret;
        }
      }
    }

    return ret;
  }

  /**
   * @brief Fill cmp_funcs_ and hash_funcs_ for Row-only spec (ObHashDistinctSpec).
   */
  int fill_cmp_and_hash_funcs(common::ObIAllocator &alloc, ObHashDistinctSpec *spec)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(spec)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("spec is null", K(ret));
      return ret;
    }

    // Find distinct keys (indices 0..key_cnt-1 in distinct_exprs_)
    std::vector<std::string> distinct_key_names = parse_distinct_keys();
    const int64_t key_cnt = distinct_key_names.empty()
        ? spec->distinct_exprs_.count()
        : distinct_key_names.size();

    if (key_cnt > 0) {
      if (OB_FAIL(spec->cmp_funcs_.init(key_cnt))) {
        LOG_WARN("init cmp_funcs_ failed", K(ret), K(key_cnt));
        return ret;
      }
      if (OB_FAIL(spec->hash_funcs_.init(key_cnt))) {
        LOG_WARN("init hash_funcs_ failed", K(ret), K(key_cnt));
        return ret;
      }

      for (int64_t i = 0; i < key_cnt && OB_SUCC(ret); ++i) {
        ObExpr *expr = spec->distinct_exprs_.at(i);
        if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("distinct expr is null", K(ret), K(i));
          break;
        }

        // Build cmp_func
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
          LOG_WARN("get_nullsafe_cmp_func returned null", K(ret), K(i),
                   K(expr->datum_meta_.type_), K(expr->datum_meta_.cs_type_));
          break;
        }
        if (OB_FAIL(spec->cmp_funcs_.push_back(cmp_func))) {
          LOG_WARN("push_back cmp_func failed", K(ret), K(i));
          break;
        }

        // Build hash_func
        ObHashFunc hash_func;
        hash_func.hash_func_ = expr->basic_funcs_->murmur_hash_;
        hash_func.batch_hash_func_ = expr->basic_funcs_->murmur_hash_batch_;
        if (OB_ISNULL(hash_func.hash_func_) || OB_ISNULL(hash_func.batch_hash_func_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("hash_func or batch_hash_func is null", K(ret), K(i));
          break;
        }
        if (OB_FAIL(spec->hash_funcs_.push_back(hash_func))) {
          LOG_WARN("push_back hash_func failed", K(ret), K(i));
          break;
        }
      }
    }

    return ret;
  }

  /**
   * @brief Fill group_distinct_exprs_ / group_sort_collations_ / grouping_id_ for Vec-only spec.
   */
  int fill_group_distinct(common::ObIAllocator &alloc, ObHashDistinctVecSpec *spec)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(spec) || OB_ISNULL(resolved_stmt_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("spec or resolved_stmt_ is null", K(ret));
      return ret;
    }

    const common::ObIArray<SelectItem> &select_items = resolved_stmt_->get_select_items();
    const common::ObIArray<ColumnItem> &column_items = resolved_stmt_->get_column_items();

    const int64_t group_cnt = group_distinct_groups_.size();
    if (group_cnt == 0) {
      return ret;  // No group distinct configured
    }

    // Initialize group_distinct_exprs_ array
    if (OB_FAIL(spec->group_distinct_exprs_.init(group_cnt))) {
      LOG_WARN("init group_distinct_exprs_ failed", K(ret), K(group_cnt));
      return ret;
    }

    // Initialize group_sort_collations_ array
    if (OB_FAIL(spec->group_sort_collations_.init(group_cnt))) {
      LOG_WARN("init group_sort_collations_ failed", K(ret), K(group_cnt));
      return ret;
    }

    // For each group, fill exprs and collations
    for (int64_t g = 0; g < group_cnt && OB_SUCC(ret); ++g) {
      const std::vector<std::string> &group_cols = group_distinct_groups_[g];

      // Initialize ExprFixedArray for this group
      ExprFixedArray group_exprs(alloc);
      if (OB_FAIL(group_exprs.init(group_cols.size()))) {
        LOG_WARN("init group_exprs failed", K(ret), K(g));
        break;
      }

      // Initialize sort collations for this group
      ObSortCollations group_collations(alloc);
      if (OB_FAIL(group_collations.init(group_cols.size()))) {
        LOG_WARN("init group_collations failed", K(ret), K(g));
        break;
      }

      for (int64_t c = 0; c < static_cast<int64_t>(group_cols.size()) && OB_SUCC(ret); ++c) {
        std::string col_name_upper = group_cols[c];
        std::transform(col_name_upper.begin(), col_name_upper.end(),
                       col_name_upper.begin(), ::toupper);

        ObExpr *expr = find_expr_by_name(col_name_upper, select_items, column_items);
        if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("group distinct column not found", K(ret), K(g), K(c), "col_name", col_name_upper.c_str());
          break;
        }

        if (OB_FAIL(group_exprs.push_back(expr))) {
          LOG_WARN("push_back group expr failed", K(ret), K(g), K(c));
          break;
        }

        ObSortFieldCollation collation;
        collation.field_idx_ = c;  // Index within this group
        collation.cs_type_ = expr->datum_meta_.cs_type_;
        collation.null_pos_ = NULL_FIRST;
        collation.is_ascending_ = true;
        collation.is_not_null_ = false;
        if (OB_FAIL(group_collations.push_back(collation))) {
          LOG_WARN("push_back group collation failed", K(ret), K(g), K(c));
          break;
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(spec->group_distinct_exprs_.push_back(group_exprs))) {
          LOG_WARN("push_back group_exprs failed", K(ret), K(g));
          break;
        }
        if (OB_FAIL(spec->group_sort_collations_.push_back(group_collations))) {
          LOG_WARN("push_back group_collations failed", K(ret), K(g));
          break;
        }
      }
    }

    // Find grouping_id expr
    if (OB_SUCC(ret) && !grouping_id_col_.empty()) {
      std::string gid_upper = grouping_id_col_;
      std::transform(gid_upper.begin(), gid_upper.end(), gid_upper.begin(), ::toupper);
      spec->grouping_id_ = find_expr_by_name(gid_upper, select_items, column_items);
      if (OB_ISNULL(spec->grouping_id_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("grouping_id column not found", K(ret), "grouping_id_col", grouping_id_col_.c_str());
      }
    }

    if (OB_SUCC(ret)) {
      spec->has_non_distinct_aggr_params_ = false;
    }

    return ret;
  }

private:
  std::string distinct_by_exprs_;       // Columns to distinct on (comma-separated)
  bool block_mode_;                     // Block vs non-block mode (default true)
  bool push_down_;                      // Forced push_down without bypass
  bool enable_bypass_;                  // Adaptive bypass (implies !block_mode && push_down)
  std::vector<std::vector<std::string>> group_distinct_groups_;  // Vec-only: groups of distinct cols
  std::string grouping_id_col_;         // Vec-only: grouping ID column name
};

}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_HASH_DISTINCT_H_