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
#include "sql/engine/basic/ob_count.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase {
using namespace common;
namespace sql {
ObCount::ObCount(ObIAllocator& alloc)
    : ObSingleChildPhyOperator(alloc), rownum_limit_expr_(NULL), anti_monotone_filter_exprs_()
{}

ObCount::~ObCount()
{
  reset();
}

void ObCount::reset()
{
  rownum_limit_expr_ = NULL;
  anti_monotone_filter_exprs_.reset();
  ObSingleChildPhyOperator::reset();
}

void ObCount::reuse()
{
  reset();
}

int ObCount::rescan(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObCountCtx* count_ctx = NULL;
  if (OB_FAIL(ObSingleChildPhyOperator::rescan(ctx))) {
    LOG_WARN("rescan single child phy operator failed", K(ret));
  } else if (OB_ISNULL(count_ctx = GET_PHY_OPERATOR_CTX(ObCountCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("count ctx is null", K(ret));
  } else {
    count_ctx->cur_rownum_ = 1;
  }
  return ret;
}

int ObCount::add_anti_monotone_filter_exprs(ObSqlExpression* expr)
{
  return ObSqlExpressionUtil::add_expr_to_list(anti_monotone_filter_exprs_, expr);
}

int ObCount::get_rownum_limit_value(ObExecContext& ctx, int64_t& rownum_limit_value) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_int_value(ctx, rownum_limit_expr_, rownum_limit_value))) {
    LOG_WARN("failed to get rownum value from rownum expr", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObCount::get_int_value(ObExecContext& ctx, const ObSqlExpression* in_val, int64_t& out_val) const
{
  int ret = OB_SUCCESS;
  ObNewRow input_row;
  ObObj result;
  ObExprCtx expr_ctx;
  if (in_val != NULL && !in_val->is_empty()) {
    if (OB_FAIL(wrap_expr_ctx(ctx, expr_ctx))) {
      LOG_WARN("wrap expr context failed", K(ret));
    } else if (OB_FAIL(in_val->calc(expr_ctx, input_row, result))) {
      LOG_WARN("Failed to calculate expression", K(ret));
    } else if (OB_LIKELY(result.is_int())) {
      if (OB_FAIL(result.get_int(out_val))) {
        LOG_WARN("get_int error", K(ret), K(result));
      }
    } else if (result.is_null()) {
      out_val = 0;
    } else {
      EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
      EXPR_GET_INT64_V2(result, out_val);
      if (OB_FAIL(ret)) {
        LOG_WARN("get_int error", K(ret), K(result));
      }
    }
  }
  return ret;
}

bool ObCount::is_valid() const
{
  return (get_column_count() > 0 && child_op_ != NULL && child_op_->get_column_count() > 0);
}

int ObCount::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx* op_ctx = NULL;
  if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObCountCtx, ctx, get_id(), get_type(), op_ctx))) {
    LOG_WARN("failed to create LimitCtx", K(ret));
  } else if (OB_ISNULL(op_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op_ctx is null");
  } else if (OB_FAIL(init_cur_row(*op_ctx, true))) {
    LOG_WARN("init current row failed", K(ret));
  }
  return ret;
}

int ObCount::get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObCountCtx* count_ctx = NULL;
  ObSQLSessionInfo* my_session = NULL;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("count operator is invalid", K(ret));
  } else if (OB_ISNULL(count_ctx = GET_PHY_OPERATOR_CTX(ObCountCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator ctx failed");
  } else if (OB_ISNULL(my_session = GET_MY_SESSION(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else if (count_ctx->has_rownum_limit_ &&
             (count_ctx->rownum_limit_value_ <= 0 || count_ctx->cur_rownum_ - 1 == count_ctx->rownum_limit_value_)) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(ObSingleChildPhyOperator::get_next_row(ctx, row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get next row from child phy operator", K(ret));
    }
  } else {
    ++count_ctx->cur_rownum_;
  }
  return ret;
}

int ObCount::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObCountCtx* count_ctx = NULL;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("count operator is invalid", K(ret));
  } else if (OB_FAIL(init_op_ctx(ctx))) {
    LOG_WARN("initialize operator context failed", K(ret));
  } else if (OB_ISNULL(count_ctx = GET_PHY_OPERATOR_CTX(ObCountCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator context failed", K_(id));
  } else {
    if (NULL != rownum_limit_expr_ && !rownum_limit_expr_->is_empty()) {
      count_ctx->has_rownum_limit_ = true;
      if (OB_FAIL(get_rownum_limit_value(ctx, count_ctx->rownum_limit_value_))) {
        LOG_WARN("fail to instantiate rownum limit", K(ret));
      }
    }
  }
  return ret;
}

int ObCount::switch_iterator(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObCountCtx* count_ctx = NULL;
  if (OB_FAIL(ObSingleChildPhyOperator::switch_iterator(ctx))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("switch parent iterator failed", K(ret));
    }
  } else if (OB_ISNULL(count_ctx = GET_PHY_OPERATOR_CTX(ObCountCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator context failed", K_(id));
  } else {
    if (NULL != rownum_limit_expr_ && !rownum_limit_expr_->is_empty()) {
      count_ctx->has_rownum_limit_ = true;
      if (OB_FAIL(get_rownum_limit_value(ctx, count_ctx->rownum_limit_value_))) {
        LOG_WARN("fail to instantiate rownum limit", K(ret));
      }
    }
  }
  return ret;
}

int ObCount::inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObCountCtx* count_ctx = NULL;
  ObSQLSessionInfo* my_session = NULL;
  const ObNewRow* input_row = NULL;
  bool is_filtered = false;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("count operator is invalid", K(ret));
  } else if (OB_ISNULL(count_ctx = GET_PHY_OPERATOR_CTX(ObCountCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator ctx failed");
  } else if (OB_ISNULL(my_session = GET_MY_SESSION(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else if (OB_FAIL(child_op_->get_next_row(ctx, input_row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("child_op failed to get next row", K(ret));
    }
  } else if (OB_ISNULL(input_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(input_row), K(ret));
  } else if (OB_FAIL(ObPhyOperator::filter_row(
                 count_ctx->expr_ctx_, *input_row, anti_monotone_filter_exprs_, is_filtered))) {
    LOG_WARN("failed to evaluate anti-monotone filter exprs", K(ret));
  } else if (is_filtered) {
    row = NULL;
    ret = OB_ITER_END;
    LOG_DEBUG("row is filtered by anti-monotone filters, running is stopped");
  } else {
    row = input_row;
    if (OB_FAIL(copy_cur_row_by_projector(*count_ctx, row))) {
      LOG_WARN("copy current row failed", K(ret));
    } else {
      LOG_DEBUG("succeed to copy cur row by projector", K(*row));
    }
  }
  return ret;
}

int ObCount::inner_close(ObExecContext& ctx) const
{
  UNUSED(ctx);
  return OB_SUCCESS;
}

int64_t ObCount::to_string_kv(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (rownum_limit_expr_) {
    J_KV(N_LIMIT, rownum_limit_expr_);
  }
  return pos;
}

OB_DEF_SERIALIZE(ObCount)
{
  int ret = OB_SUCCESS;
  bool has_rownum_limit = (rownum_limit_expr_ != NULL && !rownum_limit_expr_->is_empty());
  OB_UNIS_ENCODE(has_rownum_limit);
  if (OB_SUCC(ret) && has_rownum_limit) {
    OB_UNIS_ENCODE(*rownum_limit_expr_);
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObSingleChildPhyOperator::serialize(buf, buf_len, pos))) {
      LOG_WARN("serialize child operator failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(serialize_dlist(anti_monotone_filter_exprs_, buf, buf_len, pos))) {
      LOG_WARN("failed to serialize filter_exprs_", K(ret));
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObCount)
{
  bool has_rownum_limit = (rownum_limit_expr_ != NULL && !rownum_limit_expr_->is_empty());
  int64_t len = 0;
  OB_UNIS_ADD_LEN(has_rownum_limit);
  if (has_rownum_limit) {
    OB_UNIS_ADD_LEN(*rownum_limit_expr_);
  }
  len += ObSingleChildPhyOperator::get_serialize_size();
  len += get_dlist_serialize_size(anti_monotone_filter_exprs_);
  return len;
}

OB_DEF_DESERIALIZE(ObCount)
{
  int ret = OB_SUCCESS;
  bool has_rownum_limit = false;
  OB_UNIS_DECODE(has_rownum_limit);
  if (OB_SUCC(ret) && has_rownum_limit) {
    if (OB_FAIL(ObSqlExpressionUtil::make_sql_expr(my_phy_plan_, rownum_limit_expr_))) {
      LOG_WARN("make sql expression failed", K(ret));
    } else if (OB_LIKELY(rownum_limit_expr_ != NULL)) {
      OB_UNIS_DECODE(*rownum_limit_expr_);
    }
  }
  if (OB_SUCC(ret)) {
    ret = ObSingleChildPhyOperator::deserialize(buf, data_len, pos);
  }
  if (OB_SUCC(ret)) {
    OB_UNIS_DECODE_EXPR_DLIST(ObSqlExpression, anti_monotone_filter_exprs_, my_phy_plan_);
  }
  return ret;
}

} /* namespace sql */
} /* namespace oceanbase */