/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/aggregate/ob_groupby.h"
#include "lib/utility/utility.h"
#include "sql/ob_sql_utils.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/expr/ob_sql_expression.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase {
using namespace common;
using namespace serialization;

namespace sql {
ObGroupBy::ObGroupBy(common::ObIAllocator& alloc)
    : ObSingleChildPhyOperator(alloc),
      mem_size_limit_(0),
      prepare_row_num_(1),
      distinct_set_bucket_num_(100),
      group_col_idxs_(alloc),
      has_rollup_(false),
      est_group_cnt_(0),
      rollup_col_idxs_(alloc)
{
  UNUSED(alloc);
}

ObGroupBy::~ObGroupBy()
{}

void ObGroupBy::reset()
{
  aggr_columns_.reset();
  mem_size_limit_ = 0;
  prepare_row_num_ = 1;
  distinct_set_bucket_num_ = 100;
  group_col_idxs_.reset();
  est_group_cnt_ = 0;
  rollup_col_idxs_.reset();
  ObSingleChildPhyOperator::reset();
}

void ObGroupBy::reuse()
{
  aggr_columns_.reset();
  mem_size_limit_ = 0;
  prepare_row_num_ = 1;
  distinct_set_bucket_num_ = 100;
  group_col_idxs_.reuse();
  est_group_cnt_ = 0;
  rollup_col_idxs_.reuse();
  ObSingleChildPhyOperator::reuse();
}

void ObGroupBy::set_mem_size_limit(const int64_t limit)
{
  mem_size_limit_ = limit;
}

void ObGroupBy::set_rollup(const bool has_rollup)
{
  has_rollup_ = has_rollup;
}

int ObGroupBy::add_aggr_column(ObAggregateExpression* expr)
{
  int ret = ObSqlExpressionUtil::add_expr_to_list(aggr_columns_, expr);
  return ret;
}

int ObGroupBy::init_group_by(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  int64_t child_column_count = 0;
  int64_t est_bucket_size = 0;
  int64_t optimizer_est_input_rows = 0;
  ObExprCtx expr_ctx;
  ObGroupByCtx* groupby_ctx = NULL;

  if (OB_FAIL(init_op_ctx(ctx))) {
    LOG_WARN("init operator context failed", K(ret));
  } else if (OB_FAIL(wrap_expr_ctx(ctx, expr_ctx))) {
    LOG_WARN("wrap expr context failed", K(ret));
  } else if (OB_ISNULL(groupby_ctx = GET_PHY_OPERATOR_CTX(ObGroupByCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator context failed");
  } else if (OB_ISNULL(my_phy_plan_) || OB_ISNULL(child_op_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K_(my_phy_plan), K_(child_op));
  } else if (OB_FAIL(ObSQLUtils::get_default_cast_mode(my_phy_plan_, expr_ctx.my_session_, expr_ctx.cast_mode_))) {
    LOG_WARN("set cast mode failed", K(ret));
  } else if (OB_ISNULL(get_child(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the child of group by can not be null", K(ret));
  } else {
    child_column_count =
        (child_op_->get_projector_size() > 0 ? child_op_->get_projector_size() : child_op_->get_column_count());
    optimizer_est_input_rows = get_child(0)->get_rows();
    est_bucket_size = optimizer_est_input_rows;
    est_bucket_size = est_bucket_size < MIN_BUCKET_COUNT ? MIN_BUCKET_COUNT : est_bucket_size;
    est_bucket_size = est_bucket_size > MAX_BUCKET_COUNT ? MAX_BUCKET_COUNT : est_bucket_size;
    // LOG_DEBUG("Init hash distinct bucket size", K(optimizer_est_input_rows), K(est_distinct_input_rows));
    if (OB_FAIL(groupby_ctx->get_aggr_func().init_first_rollup_cols(
            &ctx.get_allocator(), group_col_idxs_, rollup_col_idxs_))) {
      LOG_WARN("failed to init group cols.", K(ret));
    } else if (OB_FAIL(groupby_ctx->get_aggr_func().init(
                   child_column_count, &aggr_columns_, expr_ctx, prepare_row_num_, est_bucket_size))) {
      LOG_WARN("failed to construct row desc", K(ret));
    } else if (OB_FAIL(groupby_ctx->get_aggr_func().init_agg_udf(agg_udf_meta_))) {
      LOG_WARN("failed to init udf meta", K(ret));
    } else {
      groupby_ctx->get_aggr_func().set_tenant_id(ctx.get_my_session()->get_effective_tenant_id());
    }
  }
  return ret;
}

int ObGroupBy::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx* op_ctx = NULL;

  if (OB_FAIL(inner_create_operator_ctx(ctx, op_ctx))) {
    LOG_WARN("create operator context failed", K(ret), K_(id));
  } else if (OB_ISNULL(op_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op_ctx is null");
  } else if (OB_FAIL(init_cur_row(*op_ctx, true /* need create row cells */))) {
    LOG_WARN("create current row failed", K(ret));
  }
  return ret;
}

int ObGroupBy::is_same_group(
    const ObRowStore::StoredRow& row1, const ObNewRow& row2, bool& result, int64_t& first_diff_pos) const
{
  int ret = OB_SUCCESS;
  const ObObj* lcell = NULL;
  const ObObj* rcell = NULL;

  result = true;
  for (int64_t i = 0; OB_SUCC(ret) && result && i < group_col_idxs_.count(); ++i) {
    int64_t group_idx = group_col_idxs_[i].index_;
    if (OB_UNLIKELY(group_idx >= row1.reserved_cells_count_) || OB_UNLIKELY(row2.is_invalid()) ||
        OB_UNLIKELY(group_idx >= row2.get_count())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(row1), K(row2), K(group_idx));
    } else {
      lcell = &row1.reserved_cells_[group_idx];
      rcell = &row2.get_cell(group_idx);  // read through projector
      if (lcell->compare(*rcell, group_col_idxs_[i].cs_type_) != 0) {
        first_diff_pos = i;
        result = false;
      }
    }
  }

  for (int64_t j = 0; OB_SUCC(ret) && result && j < rollup_col_idxs_.count(); ++j) {
    int64_t rollup_idx = rollup_col_idxs_[j].index_;
    if (OB_INVALID_INDEX == rollup_idx)
      continue;
    if (OB_UNLIKELY(rollup_idx >= row1.reserved_cells_count_) || OB_UNLIKELY(row2.is_invalid()) ||
        OB_UNLIKELY(rollup_idx >= row2.get_count())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(row1), K(row2), K(rollup_idx));
    } else {
      lcell = &row1.reserved_cells_[rollup_idx];
      rcell = &row2.get_cell(rollup_idx);  // read through projector
      if (lcell->compare(*rcell, rollup_col_idxs_[j].cs_type_) != 0) {
        first_diff_pos = j + group_col_idxs_.count();
        result = false;
      }
    }
  }

  return ret;
}

int64_t ObGroupBy::to_string_kv(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_KV(N_AGGR_COLUMN,
      aggr_columns_,
      N_MEM_LIMIT,
      mem_size_limit_,
      N_GROUP_BY_IDX,
      group_col_idxs_,
      "rollup",
      has_rollup_,
      N_ROLLUP_IDX,
      rollup_col_idxs_);
  return pos;
}

OB_DEF_SERIALIZE(ObGroupBy)
{
  int ret = OB_SUCCESS;
  int32_t aggr_col_count = aggr_columns_.get_size();

  OB_UNIS_ENCODE(aggr_col_count);
  DLIST_FOREACH(node, aggr_columns_)
  {
    if (OB_ISNULL(node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("node or expr is null");
    } else if (OB_FAIL(node->serialize(buf, buf_len, pos))) {
      LOG_WARN("serialize aggregate column expression failed", K(ret));
    }
  }
  OB_UNIS_ENCODE(group_col_idxs_);
  OB_UNIS_ENCODE(mem_size_limit_);
  OB_UNIS_ENCODE(prepare_row_num_);
  OB_UNIS_ENCODE(distinct_set_bucket_num_);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObSingleChildPhyOperator::serialize(buf, buf_len, pos))) {
      LOG_WARN("serialize single child operator failed", K(ret));
    }
  }
  OB_UNIS_ENCODE(has_rollup_);
  OB_UNIS_ENCODE(agg_udf_meta_);
  for (int64_t i = 0; i < agg_udf_meta_.count() && OB_SUCC(ret); ++i) {
    const ObAggUdfMeta& udf_meta = agg_udf_meta_.at(i);
    for (int64_t j = 0; j < udf_meta.calculable_results_.count() && OB_SUCC(ret); ++j) {
      const ObUdfConstArgs& args = udf_meta.calculable_results_.at(i);
      const ObSqlExpression* expr = args.sql_calc_;
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null");
      } else if (OB_FAIL(expr->serialize(buf, buf_len, pos))) {
        LOG_WARN("serialize aggregate column expression failed", K(ret));
      }
    }
  }
  OB_UNIS_ENCODE(est_group_cnt_);
  OB_UNIS_ENCODE(rollup_col_idxs_);
  return ret;
}

OB_DEF_DESERIALIZE(ObGroupBy)
{
  int ret = OB_SUCCESS;
  int32_t aggr_col_count = 0;

  if (OB_ISNULL(my_phy_plan_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("phy_plan_ is null");
  }
  OB_UNIS_DECODE(aggr_col_count);
  for (int32_t i = 0; OB_SUCC(ret) && i < aggr_col_count; ++i) {
    ObAggregateExpression* expr = NULL;
    if (OB_FAIL(ObSqlExpressionUtil::make_sql_expr(my_phy_plan_, expr))) {
      LOG_WARN("failed to make expr", K(ret));
    } else if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null");
    } else if (OB_FAIL(expr->deserialize(buf, data_len, pos))) {
      LOG_WARN("fail to deserialize expression", K(ret));
    } else if (OB_FAIL(add_aggr_column(expr))) {
      LOG_WARN("fail to add aggregate function", K(ret), K(buf), K(data_len), K(pos));
    }
  }
  OB_UNIS_DECODE(group_col_idxs_);
  OB_UNIS_DECODE(mem_size_limit_);
  OB_UNIS_DECODE(prepare_row_num_);
  OB_UNIS_DECODE(distinct_set_bucket_num_);
  if (OB_SUCC(ret) && OB_FAIL(ObSingleChildPhyOperator::deserialize(buf, data_len, pos))) {
    LOG_WARN("deserialize group by child operator failed", K(ret));
  }
  OB_UNIS_DECODE(has_rollup_);
  OB_UNIS_DECODE(agg_udf_meta_);
  for (int64_t i = 0; i < agg_udf_meta_.count() && OB_SUCC(ret); ++i) {
    ObAggUdfMeta& udf_meta = agg_udf_meta_.at(i);
    for (int64_t j = 0; j < udf_meta.calculable_results_.count() && OB_SUCC(ret); ++j) {
      ObUdfConstArgs& args = udf_meta.calculable_results_.at(i);
      ObSqlExpression* expr = nullptr;
      if (OB_FAIL(ObSqlExpressionUtil::make_sql_expr(my_phy_plan_, expr))) {
        LOG_WARN("failed to make expr", K(ret));
      } else if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null");
      } else if (OB_FAIL(expr->deserialize(buf, data_len, pos))) {
        LOG_WARN("fail to deserialize expression", K(ret));
      } else {
        args.sql_calc_ = expr;
      }
    }
  }
  if (pos < data_len) {
    OB_UNIS_DECODE(est_group_cnt_);
  }
  OB_UNIS_DECODE(rollup_col_idxs_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObGroupBy)
{
  int64_t len = 0;
  int32_t aggr_col_count = aggr_columns_.get_size();
  OB_UNIS_ADD_LEN(aggr_col_count);
  DLIST_FOREACH_NORET(node, aggr_columns_)
  {
    if (node != NULL) {
      const ObAggregateExpression* p = static_cast<const ObAggregateExpression*>(node);
      len += p->get_serialize_size();
    }
  }
  OB_UNIS_ADD_LEN(group_col_idxs_);
  OB_UNIS_ADD_LEN(mem_size_limit_);
  OB_UNIS_ADD_LEN(prepare_row_num_);
  OB_UNIS_ADD_LEN(distinct_set_bucket_num_);
  len += ObSingleChildPhyOperator::get_serialize_size();
  OB_UNIS_ADD_LEN(has_rollup_);
  OB_UNIS_ADD_LEN(agg_udf_meta_);
  for (int64_t i = 0; i < agg_udf_meta_.count(); ++i) {
    const ObAggUdfMeta& udf_meta = agg_udf_meta_.at(i);
    for (int64_t j = 0; j < udf_meta.calculable_results_.count(); ++j) {
      const ObUdfConstArgs& args = udf_meta.calculable_results_.at(i);
      const ObSqlExpression* expr = args.sql_calc_;
      if (OB_ISNULL(expr)) {
        LOG_ERROR("udf agg expr is null");
      } else {
        len += expr->get_serialize_size();
      }
    }
  }
  OB_UNIS_ADD_LEN(est_group_cnt_);
  OB_UNIS_ADD_LEN(rollup_col_idxs_);
  return len;
}
}  // namespace sql
}  // namespace oceanbase
