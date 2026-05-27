/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_SORT_H_
#define OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_SORT_H_

#include "unittest/sql/engine/op_tests/ob_op_test_base.h"
#include "sql/engine/sort/ob_sort_op.h"
#include "sql/engine/sort/ob_sort_vec_op.h"

namespace oceanbase
{
namespace sql
{

class SortTestSpec : public OpSpecBuilder<SortTestSpec>
{
public:
  SortTestSpec() : prefix_pos_(0) {}
  ~SortTestSpec() = default;

  SortTestSpec &prefix_pos(int64_t prefix_pos)
  {
    prefix_pos_ = prefix_pos;
    return *this;
  }

  ObOpSpec *create_spec(common::ObIAllocator &alloc, MockDataSourceSpec *child_spec,
                        const ExprFixedArray &output_exprs,
                        ObExpr *limit_expr, ObExpr *offset_expr, bool use_rich_format)
  {
    UNUSED(limit_expr);
    UNUSED(offset_expr);
    ObOpSpec *spec = nullptr;
    if (use_rich_format) {
      spec = create_sort_spec<ObSortVecSpec>(alloc, PHY_VEC_SORT, child_spec, output_exprs,
                                             use_rich_format);
    } else {
      spec = create_sort_spec<ObSortSpec>(alloc, PHY_SORT, child_spec, output_exprs,
                                          use_rich_format);
    }
    return spec;
  }

  ObOperator *create_op(ObExecContext &ctx, ObOpSpec &spec, ObOperator *child_op)
  {
    if (spec.type_ == PHY_VEC_SORT) {
      return default_create_op<ObSortVecOp>(ctx, spec, child_op);
    } else {
      return default_create_op<ObSortOp>(ctx, spec, child_op);
    }
  }

  ObOperator *create_op(ObExecContext &ctx, ObOperator *child_op)
  {
    UNUSED(ctx);
    return child_op;
  }

private:
  static ObOrderDirection get_order_direction(const bool ascending, const bool nulls_first)
  {
    return ascending
           ? (nulls_first ? NULLS_FIRST_ASC : NULLS_LAST_ASC)
           : (nulls_first ? NULLS_FIRST_DESC : NULLS_LAST_DESC);
  }

  int get_sort_key_specs(std::vector<SortKeySpec> &sort_specs)
  {
    int ret = OB_SUCCESS;
    if (order_by_exprs_.empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("sort test requires order by", K(ret));
    } else if (OB_ISNULL(resolved_stmt_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("resolved stmt is null", K(ret));
    } else {
      sort_specs = build_sort_key_specs(order_by_exprs_,
                                        resolved_stmt_->get_select_items(),
                                        resolved_stmt_->get_column_items());
      if (sort_specs.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to build sort key specs", K(ret), K(order_by_exprs_.c_str()));
      }
    }
    return ret;
  }

  int fill_sort_collations(common::ObIAllocator &alloc,
                           const std::vector<SortKeySpec> &sort_specs,
                           ObSortCollations &sort_collations,
                           ObSortFuncs *sort_cmp_funcs)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(sort_collations.init(static_cast<int64_t>(sort_specs.size())))) {
      LOG_WARN("init sort collations failed", K(ret));
    } else if (OB_NOT_NULL(sort_cmp_funcs)
               && OB_FAIL(sort_cmp_funcs->init(static_cast<int64_t>(sort_specs.size())))) {
      LOG_WARN("init sort cmp funcs failed", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < static_cast<int64_t>(sort_specs.size()); ++i) {
      const SortKeySpec &sort_spec = sort_specs[static_cast<size_t>(i)];
      const ObOrderDirection direction =
          get_order_direction(sort_spec.ascending, sort_spec.nulls_first);
      if (OB_NOT_NULL(sort_cmp_funcs)) {
        if (OB_FAIL(fill_sort_info_for_expr(sort_spec.key_expr, direction,
                                            sort_collations, *sort_cmp_funcs, alloc))) {
          LOG_WARN("fill sort info failed", K(ret), K(i));
        }
      } else {
        ObSortFuncs dummy_cmp_funcs(alloc);
        if (OB_FAIL(dummy_cmp_funcs.init(1))) {
          LOG_WARN("init dummy cmp funcs failed", K(ret));
        } else if (OB_FAIL(fill_sort_info_for_expr(sort_spec.key_expr, direction,
                                                   sort_collations, dummy_cmp_funcs, alloc))) {
          LOG_WARN("fill sort collation failed", K(ret), K(i));
        }
      }
      if (OB_SUCC(ret)) {
        sort_collations.at(i).field_idx_ = i;
      }
    }
    return ret;
  }

  int fill_exprs(const std::vector<ObExpr *> &exprs, ExprFixedArray &target)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(target.init(static_cast<int64_t>(exprs.size())))) {
      LOG_WARN("init expr array failed", K(ret));
    } else if (OB_FAIL(target.prepare_allocate(static_cast<int64_t>(exprs.size())))) {
      LOG_WARN("prepare expr array failed", K(ret));
    } else {
      for (int64_t i = 0; i < static_cast<int64_t>(exprs.size()); ++i) {
        target.at(i) = exprs[static_cast<size_t>(i)];
      }
    }
    return ret;
  }

  template <typename SpecType>
  SpecType *create_sort_spec(common::ObIAllocator &alloc, ObPhyOperatorType phy_type,
                             MockDataSourceSpec *child_spec,
                             const ExprFixedArray &output_exprs,
                             bool use_rich_format)
  {
    int ret = OB_SUCCESS;
    FatalErrorChecker error_checker(ret);
    std::vector<SortKeySpec> sort_specs;
    SpecType *sort_spec = nullptr;
    void *mem = nullptr;
    if (OB_FAIL(get_sort_key_specs(sort_specs))) {
      LOG_WARN("get sort key specs failed", K(ret));
    } else if (prefix_pos_ < 0 || prefix_pos_ > static_cast<int64_t>(sort_specs.size())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid prefix pos", K(ret), K(prefix_pos_), K(sort_specs.size()));
    } else if (OB_ISNULL(mem = alloc.alloc(sizeof(SpecType)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc sort spec failed", K(ret));
    } else {
      sort_spec = new (mem) SpecType(alloc, phy_type);
      sort_spec->topn_expr_ = nullptr;
      sort_spec->topk_limit_expr_ = nullptr;
      sort_spec->topk_offset_expr_ = nullptr;
      sort_spec->minimum_row_count_ = 0;
      sort_spec->topk_precision_ = 0;
      sort_spec->prefix_pos_ = prefix_pos_;
      sort_spec->is_local_merge_sort_ = false;
      sort_spec->is_fetch_with_ties_ = false;
      sort_spec->prescan_enabled_ = false;
      sort_spec->enable_encode_sortkey_opt_ = false;
      sort_spec->part_cnt_ = 0;
      sort_spec->plan_ = child_spec->plan_;
      sort_spec->max_batch_size_ = child_spec->max_batch_size_;
      sort_spec->use_rich_format_ = use_rich_format;
      sort_spec->output_ = output_exprs;
      sort_spec->rows_ = child_spec->rows_;
      sort_spec->width_ = child_spec->width_;

      void *child_spec_mem = alloc.alloc(sizeof(ObOpSpec *));
      if (OB_ISNULL(child_spec_mem)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc child spec array failed", K(ret));
      } else {
        ObOpSpec **children = reinterpret_cast<ObOpSpec **>(child_spec_mem);
        children[0] = child_spec;
        if (OB_FAIL(sort_spec->set_children_pointer(children, 1))) {
          LOG_WARN("set children pointer failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      ret = fill_sort_spec(alloc, sort_specs, output_exprs, *sort_spec);
    }
    return OB_SUCC(ret) ? sort_spec : nullptr;
  }

  int fill_sort_spec(common::ObIAllocator &alloc,
                     const std::vector<SortKeySpec> &sort_specs,
                     const ExprFixedArray &output_exprs,
                     ObSortSpec &sort_spec)
  {
    int ret = OB_SUCCESS;
    std::vector<ObExpr *> all_exprs;
    all_exprs.reserve(sort_specs.size() + output_exprs.count());
    for (const SortKeySpec &sort_key : sort_specs) {
      all_exprs.push_back(sort_key.key_expr);
    }
    for (int64_t i = 0; i < output_exprs.count(); ++i) {
      ObExpr *expr = output_exprs.at(i);
      bool exists = false;
      for (ObExpr *existing_expr : all_exprs) {
        if (existing_expr == expr) {
          exists = true;
          break;
        }
      }
      if (!exists) {
        all_exprs.push_back(expr);
      }
    }
    if (OB_FAIL(fill_exprs(all_exprs, sort_spec.all_exprs_))) {
      LOG_WARN("fill all exprs failed", K(ret));
    } else if (OB_FAIL(fill_sort_collations(alloc, sort_specs,
                                            sort_spec.sort_collations_,
                                            &sort_spec.sort_cmp_funs_))) {
      LOG_WARN("fill sort collations failed", K(ret));
    }
    return ret;
  }

  int fill_sort_spec(common::ObIAllocator &alloc,
                     const std::vector<SortKeySpec> &sort_specs,
                     const ExprFixedArray &output_exprs,
                     ObSortVecSpec &sort_spec)
  {
    int ret = OB_SUCCESS;
    std::vector<ObExpr *> sk_exprs;
    std::vector<ObExpr *> addon_exprs;
    sk_exprs.reserve(sort_specs.size());
    addon_exprs.reserve(output_exprs.count());
    for (const SortKeySpec &sort_key : sort_specs) {
      sk_exprs.push_back(sort_key.key_expr);
    }
    for (int64_t i = 0; i < output_exprs.count(); ++i) {
      addon_exprs.push_back(output_exprs.at(i));
    }
    sort_spec.has_addon_ = true;
    sort_spec.compress_type_ = NONE_COMPRESSOR;
    sort_spec.enable_single_col_compare_opt_ = false;
    if (OB_FAIL(fill_exprs(sk_exprs, sort_spec.sk_exprs_))) {
      LOG_WARN("fill sort key exprs failed", K(ret));
    } else if (OB_FAIL(fill_exprs(addon_exprs, sort_spec.addon_exprs_))) {
      LOG_WARN("fill addon exprs failed", K(ret));
    } else if (OB_FAIL(fill_sort_collations(alloc, sort_specs,
                                            sort_spec.sk_collations_,
                                            nullptr))) {
      LOG_WARN("fill sort collations failed", K(ret));
    }
    return ret;
  }

private:
  int64_t prefix_pos_;
};

}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_SORT_H_
