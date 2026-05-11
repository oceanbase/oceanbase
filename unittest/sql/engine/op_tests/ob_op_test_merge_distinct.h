/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_MERGE_DISTINCT_H_
#define OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_MERGE_DISTINCT_H_

#include "unittest/sql/engine/op_tests/ob_op_test_base.h"
#include "sql/engine/aggregate/ob_merge_distinct_op.h"
#include "sql/engine/aggregate/ob_merge_distinct_vec_op.h"
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
 * @brief MergeDistinctTestSpec - Test specification for Merge Distinct operator.
 *
 * Supports both 1.0 (ObMergeDistinctOp/PHY_MERGE_DISTINCT) and
 * 2.0 (ObMergeDistinctVecOp/PHY_VEC_MERGE_DISTINCT) operators.
 * The operator version is selected at create_spec() time via the use_rich_format parameter.
 *
 * Merge Distinct requires the input to be pre-sorted on the distinct keys.
 * Use OpSpecBuilder::with_sorted_data(rows, order_desc) to supply sorted input.
 * Merge Distinct does NOT support block mode (is_block_mode_ must be false) and
 * does NOT implement bypass / group distinct (those are Hash-only concepts).
 *
 * Usage:
 *   MergeDistinctTestSpec()
 *       .table("t", "a int, b int")
 *       .select("a, b")
 *       .with_sorted_data({{1, 10}, {1, 10}, {2, 20}, {3, 30}}, "a, b")
 *       .enable_dual_format_check().run(engine);
 *
 * Advanced:
 *   // Distinct on specific columns, others become additional exprs
 *   MergeDistinctTestSpec()
 *       .select("a, b, c")
 *       .distinct_by("a")
 *       .with_sorted_data(rows, "a")
 *       .run(engine);
 */
class MergeDistinctTestSpec : public OpSpecBuilder<MergeDistinctTestSpec>
{
public:
  MergeDistinctTestSpec() = default;
  ~MergeDistinctTestSpec() = default;

  // ===== Configuration Interface =====

  /**
   * @brief Specify which columns are distinct keys.
   * Columns not listed in distinct_by become additional exprs appended after the keys.
   * If not called, all select columns are treated as distinct keys.
   * @param exprs Comma-separated column names (e.g., "a, b")
   */
  MergeDistinctTestSpec& distinct_by(const std::string &exprs)
  {
    distinct_by_exprs_ = exprs;
    return *this;
  }

  // ===== Create Spec =====

  ObOpSpec *create_spec(common::ObIAllocator &alloc, MockDataSourceSpec *child_spec,
                         const ExprFixedArray &output_exprs,
                         ObExpr *limit_expr, ObExpr *offset_expr, bool use_rich_format)
  {
    UNUSED(limit_expr);
    UNUSED(offset_expr);
    if (use_rich_format) {
      return create_spec_impl<ObMergeDistinctVecSpec>(
          alloc, child_spec, output_exprs, use_rich_format, PHY_VEC_MERGE_DISTINCT);
    } else {
      return create_spec_impl<ObMergeDistinctSpec>(
          alloc, child_spec, output_exprs, use_rich_format, PHY_MERGE_DISTINCT);
    }
  }

  // ===== Create Operator =====

  ObOperator *create_op(ObExecContext &ctx, ObOpSpec &spec, ObOperator *child_op)
  {
    if (spec.type_ == PHY_VEC_MERGE_DISTINCT) {
      return default_create_op<ObMergeDistinctVecOp>(ctx, spec, child_op);
    } else {
      return default_create_op<ObMergeDistinctOp>(ctx, spec, child_op);
    }
  }

  ObOperator *create_op(ObExecContext &ctx, ObOperator *child_op)
  {
    return child_op;
  }

private:
  /**
   * @brief Parse distinct_by_exprs_ string into uppercase column names.
   */
  std::vector<std::string> parse_distinct_keys() const
  {
    std::vector<std::string> keys;
    if (distinct_by_exprs_.empty()) {
      return keys;
    }
    const std::string &exprs = distinct_by_exprs_;
    size_t pos = 0;
    while (pos < exprs.size()) {
      while (pos < exprs.size() && std::isspace(exprs[pos])) pos++;
      if (pos >= exprs.size()) break;
      size_t start = pos;
      while (pos < exprs.size() && exprs[pos] != ',' && !std::isspace(exprs[pos])) pos++;
      std::string name = exprs.substr(start, pos - start);
      std::transform(name.begin(), name.end(), name.begin(), ::toupper);
      if (!name.empty()) {
        keys.push_back(name);
      }
      while (pos < exprs.size() && (exprs[pos] == ',' || std::isspace(exprs[pos]))) pos++;
    }
    return keys;
  }

  /**
   * @brief Check whether raw_expr's name matches any distinct key name (case-insensitive,
   * table prefix stripped).
   */
  static bool is_distinct_key(ObRawExpr *raw,
                              const std::vector<std::string> &distinct_key_names)
  {
    if (OB_ISNULL(raw) || distinct_key_names.empty()) {
      return false;
    }
    char name_buf[512];
    int64_t buf_pos = 0;
    if (OB_SUCCESS != raw->get_name(name_buf, static_cast<int64_t>(sizeof(name_buf)) - 1, buf_pos)) {
      return false;
    }
    std::string expr_name(name_buf, buf_pos);
    std::transform(expr_name.begin(), expr_name.end(), expr_name.begin(), ::toupper);
    std::string stripped = expr_name;
    size_t dot_pos = stripped.find('.');
    if (dot_pos != std::string::npos && dot_pos > 0) {
      size_t start = dot_pos;
      while (start > 0 && (isalnum(stripped[start - 1]) || stripped[start - 1] == '_')) start--;
      if (start < dot_pos) stripped.erase(start, dot_pos - start + 1);
    }
    for (const auto &key_name : distinct_key_names) {
      if (expr_name == key_name || stripped == key_name) {
        return true;
      }
    }
    return false;
  }

  /**
   * @brief Create and populate a MergeDistinct spec (1.0 or 2.0).
   * Both variants inherit ObDistinctSpec, so the population logic is identical.
   */
  template <typename SpecType>
  ObOpSpec *create_spec_impl(common::ObIAllocator &alloc, MockDataSourceSpec *child_spec,
                              const ExprFixedArray &output_exprs,
                              bool use_rich_format, ObPhyOperatorType op_type)
  {
    int ret = OB_SUCCESS;
    FatalErrorChecker error_checker(ret);
    if (OB_ISNULL(child_spec) || OB_ISNULL(resolved_stmt_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child_spec or resolved_stmt_ is null", K(ret));
      return nullptr;
    }

    void *mem = alloc.alloc(sizeof(SpecType));
    if (OB_ISNULL(mem)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc merge distinct spec failed", K(ret));
      return nullptr;
    }
    SpecType *spec = new (mem) SpecType(alloc, op_type);
    spec->plan_ = child_spec->plan_;
    spec->max_batch_size_ = child_spec->max_batch_size_;
    spec->use_rich_format_ = use_rich_format;
    spec->output_ = output_exprs;

    // Set child pointer
    void *child_spec_mem = alloc.alloc(sizeof(ObOpSpec *));
    if (OB_ISNULL(child_spec_mem)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc child spec array failed", K(ret));
      return nullptr;
    }
    ObOpSpec **children = reinterpret_cast<ObOpSpec **>(child_spec_mem);
    children[0] = child_spec;
    if (OB_FAIL(spec->set_children_pointer(children, 1))) {
      LOG_WARN("set children pointer failed", K(ret));
      return nullptr;
    }

    // Split select_items into distinct_keys and additional_exprs
    const common::ObIArray<SelectItem> &select_items = resolved_stmt_->get_select_items();
    std::vector<std::string> distinct_key_names = parse_distinct_keys();
    std::vector<ObExpr*> distinct_keys;
    std::vector<ObExpr*> additional_exprs;

    if (distinct_key_names.empty()) {
      // All select columns are distinct keys
      for (int64_t i = 0; i < select_items.count(); ++i) {
        ObRawExpr *raw = select_items.at(i).expr_;
        if (OB_ISNULL(raw)) continue;
        ObExpr *expr = ObStaticEngineExprCG::get_rt_expr(*raw);
        if (OB_ISNULL(expr)) continue;
        distinct_keys.push_back(expr);
      }
    } else {
      for (int64_t i = 0; i < select_items.count(); ++i) {
        ObRawExpr *raw = select_items.at(i).expr_;
        if (OB_ISNULL(raw)) continue;
        ObExpr *expr = ObStaticEngineExprCG::get_rt_expr(*raw);
        if (OB_ISNULL(expr)) continue;
        if (is_distinct_key(raw, distinct_key_names)) {
          distinct_keys.push_back(expr);
        } else {
          additional_exprs.push_back(expr);
        }
      }
    }

    // Fill distinct_exprs_ = distinct_keys ++ additional_exprs
    const int64_t total_cnt = static_cast<int64_t>(distinct_keys.size() + additional_exprs.size());
    if (total_cnt > 0) {
      if (OB_FAIL(spec->distinct_exprs_.init(total_cnt))) {
        LOG_WARN("init distinct_exprs_ failed", K(ret), K(total_cnt));
        return nullptr;
      }
      for (ObExpr *expr : distinct_keys) {
        if (OB_FAIL(spec->distinct_exprs_.push_back(expr))) {
          LOG_WARN("push_back distinct key failed", K(ret));
          return nullptr;
        }
      }
      for (ObExpr *expr : additional_exprs) {
        if (OB_FAIL(spec->distinct_exprs_.push_back(expr))) {
          LOG_WARN("push_back additional expr failed", K(ret));
          return nullptr;
        }
      }
    }

    // Fill cmp_funcs_ for distinct keys only (matches generate_merge_distinct_spec in CG)
    const int64_t key_cnt = static_cast<int64_t>(distinct_keys.size());
    if (key_cnt > 0) {
      if (OB_FAIL(spec->cmp_funcs_.init(key_cnt))) {
        LOG_WARN("init cmp_funcs_ failed", K(ret), K(key_cnt));
        return nullptr;
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < key_cnt; ++i) {
        ObExpr *expr = distinct_keys[i];
        if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("distinct key expr is null", K(ret), K(i));
          return nullptr;
        }
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
          return nullptr;
        }
        if (OB_FAIL(spec->cmp_funcs_.push_back(cmp_func))) {
          LOG_WARN("push_back cmp_func failed", K(ret), K(i));
          return nullptr;
        }
      }
    }

    // Merge distinct does NOT support block mode and does NOT implement bypass.
    spec->is_block_mode_ = false;
    spec->by_pass_enabled_ = false;

    return spec;
  }

private:
  std::string distinct_by_exprs_;  // Columns to distinct on (comma-separated)
};

}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_MERGE_DISTINCT_H_
