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
#include "sql/engine/recursive_cte/ob_recursive_union_all.h"
#include "common/object/ob_object.h"
#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase {
using namespace common;
namespace sql {

int ObRecursiveUnionAll::ObRecursiveUnionAllOperatorCtx::ctx_close()
{
  int ret = OB_SUCCESS;
  inner_data_.search_by_col_lists_.reset();
  inner_data_.cycle_by_col_lists_.reset();
  inner_data_.result_output_.reset();
  inner_data_.pump_operator_ = nullptr;
  inner_data_.left_op_ = nullptr;
  inner_data_.right_op_ = nullptr;
  return ret;
}

ObRecursiveUnionAll::ObRecursiveUnionAll(common::ObIAllocator& alloc)
    : ObMergeSetOperator(alloc),
      search_by_col_lists_(alloc),
      cycle_by_col_lists_(alloc),
      pump_operator_(nullptr),
      strategy_(ObRecursiveInnerData::SearchStrategyType::BREADTH_FRIST),
      cycle_value_(alloc),
      cycle_default_value_(alloc)
{}

ObRecursiveUnionAll::~ObRecursiveUnionAll()
{}

void ObRecursiveUnionAll::reset()
{
  cycle_by_col_lists_.reset();
  search_by_col_lists_.reset();
  ObMergeSetOperator::reset();
}

void ObRecursiveUnionAll::reuse()
{
  search_by_col_lists_.reuse();
  cycle_by_col_lists_.reuse();
  ObMergeSetOperator::reuse();
}

int ObRecursiveUnionAll::rescan(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("Do recursive union rescan");
  ObRecursiveUnionAllOperatorCtx* cte_ctx = nullptr;
  if (OB_ISNULL(cte_ctx = GET_PHY_OPERATOR_CTX(ObRecursiveUnionAllOperatorCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Failed to get physical operator", K(ret), K(ctx), K_(id));
  } else if (OB_FAIL(cte_ctx->inner_data_.rescan(ctx))) {
    LOG_WARN("Failed to rescan inner data", K(ret));
  } else if (OB_FAIL(ObPhyOperator::rescan(ctx))) {
    LOG_WARN("Operator rescan failed", K(ret));
  }
  return ret;
}

int ObRecursiveUnionAll::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObRecursiveUnionAllOperatorCtx* cte_ctx = nullptr;
  ObObj cycle_value;
  ObObj non_cycle_value;
  ObNewRow empty_row;
  if (OB_ISNULL(get_child(FIRST_CHILD))) {
    ret = OB_NOT_INIT;
    LOG_WARN("Left op is null", K(ret));
  } else if (OB_FAIL(init_op_ctx(ctx))) {
    LOG_WARN("Failed to init operator context", K(ret), K(ret));
  } else if (OB_ISNULL(cte_ctx = GET_PHY_OPERATOR_CTX(ObRecursiveUnionAllOperatorCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Failed to get physical operator", K(ret), K(ctx), K_(id));
  } else if (OB_FAIL(wrap_expr_ctx(ctx, cte_ctx->expr_ctx_))) {
    LOG_WARN("Failed to wrap expr ctx", K(ret));
  } else if (OB_FAIL(cte_ctx->inner_data_.init(cte_pseudo_column_row_desc_))) {
    LOG_WARN("Failed to create hash filter", K(ret));
  } else if (!cycle_value_.is_empty() && OB_FAIL(cycle_value_.calc(cte_ctx->expr_ctx_, empty_row, cycle_value))) {
    LOG_WARN("Failed to calculate cycle value", K(ret));
  } else if (!cycle_default_value_.is_empty() &&
             OB_FAIL(cycle_default_value_.calc(cte_ctx->expr_ctx_, empty_row, non_cycle_value))) {
    LOG_WARN("Failed to calculate non-cycle value", K(ret));
  } else {
    cte_ctx->inner_data_.set_left_child(get_child(FIRST_CHILD));
    cte_ctx->inner_data_.set_right_child(get_child(SECOND_CHILD));
    cte_ctx->inner_data_.set_fake_cte_table(pump_operator_);
    cte_ctx->inner_data_.set_search_strategy(strategy_);
    cte_ctx->inner_data_.cycle_value_ = cycle_value;
    cte_ctx->inner_data_.non_cycle_value_ = non_cycle_value;
    cte_ctx->inner_data_.set_op_schema_objs(get_op_schema_objs());
    cte_ctx->inner_data_.set_calc_buf(&cte_ctx->get_calc_buf());
    ARRAY_FOREACH(search_by_col_lists_, i)
    {
      if (OB_FAIL(cte_ctx->inner_data_.add_search_column(search_by_col_lists_.at(i)))) {
        LOG_WARN("Add search col to inner data failed", K(ret));
      }
    }
    ARRAY_FOREACH(cycle_by_col_lists_, i)
    {
      if (OB_FAIL(cte_ctx->inner_data_.add_cycle_column(cycle_by_col_lists_.at(i)))) {
        LOG_WARN("Add cycle col to inner data failed", K(ret));
      }
    }
  }
  return ret;
}

int ObRecursiveUnionAll::inner_close(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObRecursiveUnionAllOperatorCtx* cte_ctx = nullptr;
  if (OB_ISNULL(cte_ctx = GET_PHY_OPERATOR_CTX(ObRecursiveUnionAllOperatorCtx, ctx, get_id()))) {
    LOG_DEBUG("The operator has not been opened.", K(ret), K_(id), "op_type", ob_phy_operator_type_str(get_type()));
  } else if (OB_FAIL(cte_ctx->ctx_close())) {
    LOG_WARN("ctx close failed", K(ret));
  }
  return ret;
}

int ObRecursiveUnionAll::inner_get_next_row(ObExecContext& exec_ctx, const common::ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObRecursiveUnionAllOperatorCtx* cte_ctx = nullptr;
  if (OB_FAIL(try_check_status(exec_ctx))) {
    LOG_WARN("Failed to check physical plan status", K(ret));
  } else if ((OB_ISNULL(cte_ctx = GET_PHY_OPERATOR_CTX(ObRecursiveUnionAllOperatorCtx, exec_ctx, get_id())))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Failed to get recursive union ctx", K(ret));
  } else if (OB_FAIL(cte_ctx->inner_data_.get_next_row(exec_ctx, row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("Failed to get next sort row from recursive inner data", K(ret));
    }
  }
  return ret;
}

int ObRecursiveUnionAll::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx* op_ctx = nullptr;
  if (OB_FAIL(inner_create_operator_ctx(ctx, op_ctx))) {
    LOG_WARN("Inner create operator context failed", K(ret));
  } else if (OB_ISNULL(op_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op_ctx is null", K(ret));
  } else if (OB_FAIL(init_cur_row(*op_ctx, true /* need create cells */))) {
    LOG_WARN("Init current row failed", K(ret));
  } else {
    ObRecursiveUnionAllOperatorCtx* union_ctx = static_cast<ObRecursiveUnionAllOperatorCtx*>(op_ctx);
    union_ctx->set_cur_row();
  }
  return ret;
}

int ObRecursiveUnionAll::inner_create_operator_ctx(ObExecContext& ctx, ObPhyOperatorCtx*& op_ctx) const
{
  return CREATE_PHY_OPERATOR_CTX(ObRecursiveUnionAllOperatorCtx, ctx, get_id(), get_type(), op_ctx);
}

int ObRecursiveUnionAll::set_cycle_pseudo_values(ObSqlExpression& v, ObSqlExpression& d_v)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cycle_value_.assign(v))) {
    LOG_WARN("Failed to add cycle value", K(ret));
  } else if (OB_FAIL(cycle_default_value_.assign(d_v))) {
    LOG_WARN("Failed to add non-cycle value", K(ret));
  }
  return ret;
}

int64_t ObRecursiveUnionAll::to_string_kv(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_KV("strategy ", strategy_, "search_by_col_lists", search_by_col_lists_, "cycle_by_col_lists_", cycle_by_col_lists_);
  return pos;
}

OB_DEF_SERIALIZE(ObRecursiveUnionAll)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObMergeSetOperator::serialize(buf, buf_len, pos))) {
    LOG_WARN("failed to serialize merge set operator", K(ret));
  } else if (OB_FAIL(search_by_col_lists_.serialize(buf, buf_len, pos))) {
    LOG_WARN("failed to serialize search_by_col_list", K(ret));
  } else if (OB_FAIL(cycle_by_col_lists_.serialize(buf, buf_len, pos))) {
    LOG_WARN("failed to serialize cycle_by_col_list", K(ret));
  } else {
    OB_UNIS_ENCODE(strategy_);
    OB_UNIS_ENCODE(cycle_value_);
    OB_UNIS_ENCODE(cycle_default_value_);
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObRecursiveUnionAll)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObMergeSetOperator::deserialize(buf, data_len, pos))) {
    LOG_WARN("failed to deserialize merge set operator", K(ret));
  } else if (OB_FAIL(search_by_col_lists_.deserialize(buf, data_len, pos))) {
    LOG_WARN("failed to deserialize search_by_col_list", K(ret));
  } else if (OB_FAIL(cycle_by_col_lists_.deserialize(buf, data_len, pos))) {
    LOG_WARN("failed to deserialize cycle_by_col_list", K(ret));
  } else {
    OB_UNIS_DECODE(strategy_);
    OB_UNIS_DECODE(cycle_value_);
    OB_UNIS_DECODE(cycle_default_value_);
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObRecursiveUnionAll)
{
  int64_t len = 0;
  len += ObMergeSetOperator::get_serialize_size();
  OB_UNIS_ADD_LEN(search_by_col_lists_);
  OB_UNIS_ADD_LEN(cycle_by_col_lists_);
  OB_UNIS_ADD_LEN(strategy_);
  OB_UNIS_ADD_LEN(cycle_value_);
  OB_UNIS_ADD_LEN(cycle_default_value_);
  return len;
}
}  // namespace sql
}  // namespace oceanbase
