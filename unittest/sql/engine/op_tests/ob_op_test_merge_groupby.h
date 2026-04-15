/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_MERGE_GROUPBY_H_
#define OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_MERGE_GROUPBY_H_

#include "unittest/sql/engine/op_tests/ob_op_test_base.h"
#include "sql/engine/aggregate/ob_merge_groupby_op.h"
#include "sql/engine/aggregate/ob_merge_groupby_vec_op.h"
#include "sql/ob_sql_define.h"

namespace oceanbase
{
namespace sql
{

class MergeGroupByTestSpec : public OpSpecBuilder<MergeGroupByTestSpec>
{
public:
  MergeGroupByTestSpec()
    : has_rollup_(false),
      enable_hash_base_distinct_(true),
      est_rows_per_group_(0)
  {}
  ~MergeGroupByTestSpec() = default;

  MergeGroupByTestSpec& with_rollup(bool enable = true)
  {
    has_rollup_ = enable;
    if (enable && !group_by_exprs_.empty()
        && group_by_exprs_.find("WITH ROLLUP") == std::string::npos) {
      group_by_exprs_ += " WITH ROLLUP";
    }
    return *this;
  }

  MergeGroupByTestSpec& with_hash_base_distinct(bool enable = true)
  {
    enable_hash_base_distinct_ = enable;
    return *this;
  }

  MergeGroupByTestSpec& with_est_rows_per_group(int64_t n)
  {
    est_rows_per_group_ = n;
    return *this;
  }

  ObOpSpec *create_spec(common::ObIAllocator &alloc, MockDataSourceSpec *child_spec,
                        const ExprFixedArray &output_exprs,
                        ObExpr *limit_expr, ObExpr *offset_expr, bool use_rich_format)
  {
    int ret = OB_SUCCESS;
    if (use_rich_format) {
      return create_spec_impl<ObMergeGroupByVecSpec, ObMergeGroupByVecOp>(
          alloc, child_spec, output_exprs, use_rich_format, PHY_VEC_MERGE_GROUP_BY);
    } else {
      return create_spec_impl<ObMergeGroupBySpec, ObMergeGroupByOp>(
          alloc, child_spec, output_exprs, use_rich_format, PHY_MERGE_GROUP_BY);
    }
  }

  ObOperator *create_op(ObExecContext &ctx, ObOpSpec &spec, ObOperator *child_op)
  {
    if (spec.type_ == PHY_VEC_MERGE_GROUP_BY) {
      return default_create_op<ObMergeGroupByVecOp>(ctx, spec, child_op);
    } else {
      return default_create_op<ObMergeGroupByOp>(ctx, spec, child_op);
    }
  }

  ObOperator *create_op(ObExecContext &ctx, ObOperator *child_op)
  {
    return child_op;
  }

private:
  template <typename SpecType, typename OpType>
  ObOpSpec *create_spec_impl(common::ObIAllocator &alloc, MockDataSourceSpec *child_spec,
                              const ExprFixedArray &output_exprs,
                              bool use_rich_format, ObPhyOperatorType op_type)
  {
    int ret = OB_SUCCESS;
    void *mem = alloc.alloc(sizeof(SpecType));
    if (OB_ISNULL(mem)) {
      LOG_WARN("alloc spec failed");
      return nullptr;
    }
    SpecType *spec = new (mem) SpecType(alloc, op_type);
    spec->plan_ = child_spec->plan_;
    spec->max_batch_size_ = child_spec->max_batch_size_;
    spec->use_rich_format_ = use_rich_format;
    spec->output_ = output_exprs;
    spec->enable_hash_base_distinct_ = enable_hash_base_distinct_;
    spec->est_rows_per_group_ = est_rows_per_group_;

    // Set child pointer
    void *child_spec_mem = alloc.alloc(sizeof(ObOpSpec *));
    if (OB_ISNULL(child_spec_mem)) { return nullptr; }
    ObOpSpec **children = reinterpret_cast<ObOpSpec **>(child_spec_mem);
    children[0] = child_spec;
    if (OB_FAIL(spec->set_children_pointer(children, 1))) { return nullptr; }

    // Fill aggr_infos_
    ret = fill_aggr_infos_for_groupby(alloc, spec, resolved_stmt_, phy_plan_, use_rich_format);
    if (OB_FAIL(ret)) {
      LOG_WARN("fill_aggr_infos_for_groupby failed", K(ret));
      return nullptr;
    }

    // Fill group_exprs_
    const common::ObIArray<ObRawExpr *> &group_raw_exprs = resolved_stmt_->get_group_exprs();
    if (OB_FAIL(spec->init_group_exprs(group_raw_exprs.count()))) {
      LOG_WARN("init group exprs failed", K(ret));
      return nullptr;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < group_raw_exprs.count(); ++i) {
      ObExpr *rt_expr = ObStaticEngineExprCG::get_rt_expr(*group_raw_exprs.at(i));
      if (OB_ISNULL(rt_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rt_expr is null", K(ret), K(i));
        return nullptr;
      }
      if (OB_FAIL(spec->add_group_expr(rt_expr))) {
        LOG_WARN("add group expr failed", K(ret));
        return nullptr;
      }
    }

    // Fill rollup_exprs_ (if ROLLUP)
    if (has_rollup_) {
      const common::ObIArray<ObRawExpr *> &rollup_raw_exprs = resolved_stmt_->get_rollup_exprs();
      if (OB_FAIL(spec->init_rollup_exprs(rollup_raw_exprs.count()))) {
        LOG_WARN("init rollup exprs failed", K(ret));
        return nullptr;
      }
      if (OB_FAIL(spec->init_duplicate_rollup_expr(rollup_raw_exprs.count()))) {
        LOG_WARN("init duplicate rollup expr failed", K(ret));
        return nullptr;
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < rollup_raw_exprs.count(); ++i) {
        ObExpr *rt_expr = ObStaticEngineExprCG::get_rt_expr(*rollup_raw_exprs.at(i));
        if (OB_ISNULL(rt_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("rollup rt_expr is null", K(ret), K(i));
          return nullptr;
        }
        if (OB_FAIL(spec->add_rollup_expr(rt_expr))) {
          LOG_WARN("add rollup expr failed", K(ret));
          return nullptr;
        }
        // Check if this rollup expr is duplicate (already in group_exprs_)
        bool is_dup = false;
        for (int64_t j = 0; j < spec->group_exprs_.count() && !is_dup; ++j) {
          if (spec->group_exprs_.at(j) == rt_expr) {
            is_dup = true;
          }
        }
        if (OB_FAIL(spec->is_duplicate_rollup_expr_.push_back(is_dup))) {
          LOG_WARN("push back is_duplicate_rollup_expr failed", K(ret));
          return nullptr;
        }
      }
      spec->set_rollup(true);
      spec->rollup_status_ = ObRollupStatus::ROLLUP_NORMAL;
      spec->is_parallel_ = false;
      spec->rollup_id_expr_ = nullptr;
      spec->enable_encode_sort_ = false;
    }

    // Fill sort_exprs_ = group_exprs_ + rollup_exprs_
    int64_t sort_cnt = spec->group_exprs_.count() + spec->rollup_exprs_.count();
    if (OB_FAIL(spec->sort_exprs_.init(sort_cnt))) {
      LOG_WARN("init sort_exprs failed", K(ret));
      return nullptr;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < spec->group_exprs_.count(); ++i) {
      if (OB_FAIL(spec->sort_exprs_.push_back(spec->group_exprs_.at(i)))) {
        LOG_WARN("push sort expr failed", K(ret));
        return nullptr;
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < spec->rollup_exprs_.count(); ++i) {
      if (OB_FAIL(spec->sort_exprs_.push_back(spec->rollup_exprs_.at(i)))) {
        LOG_WARN("push rollup sort expr failed", K(ret));
        return nullptr;
      }
    }

    // Fill sort_collations_ and sort_cmp_funcs_
    if (OB_FAIL(spec->sort_collations_.init(sort_cnt))) {
      LOG_WARN("init sort_collations failed", K(ret));
      return nullptr;
    }
    if (OB_FAIL(spec->sort_cmp_funcs_.init(sort_cnt))) {
      LOG_WARN("init sort_cmp_funcs failed", K(ret));
      return nullptr;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < spec->sort_exprs_.count(); ++i) {
      ObExpr *sort_expr = spec->sort_exprs_.at(i);
      if (OB_FAIL(fill_sort_info_for_expr(sort_expr, NULLS_LAST_ASC,
                                           spec->sort_collations_,
                                           spec->sort_cmp_funcs_, alloc))) {
        LOG_WARN("fill_sort_info_for_expr failed", K(ret), K(i));
        return nullptr;
      }
    }
    // Fix sort collation index
    for (int64_t i = 0; i < spec->sort_collations_.count(); ++i) {
      spec->sort_collations_.at(i).field_idx_ = i;
    }

    return spec;
  }

  bool has_rollup_;
  bool enable_hash_base_distinct_;
  int64_t est_rows_per_group_;
};

}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_MERGE_GROUPBY_H_
