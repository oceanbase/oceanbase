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

#include "sql/engine/join/ob_join_op.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

OB_SERIALIZE_MEMBER((ObJoinSpec, ObOpSpec),
                    join_type_, other_join_conds_);

int ObJoinOp::inner_rescan()
{
  output_row_produced_ = false;
  left_row_joined_ = false;
  return ObOperator::inner_rescan();
}

int ObJoinOp::blank_row(const ExprFixedArray &exprs)
{
  int ret = OB_SUCCESS;
  // clear evaluated flag first to make sure expression evaluated with the null values.
  clear_evaluated_flag();
  for (int64_t i = 0; i < exprs.count(); i++) {
    ObExpr *expr = exprs.at(i);
    expr->locate_expr_datum(eval_ctx_).set_null();
    expr->set_evaluated_projected(eval_ctx_);
  }
  return ret;
}

void ObJoinOp::blank_row_batch(const ExprFixedArray &exprs, int64_t batch_size)
{
  for (int64_t col_idx = 0; col_idx < exprs.count(); col_idx++) {
    ObExpr *e = exprs.at(col_idx);
    ObDatum *datums = e->locate_batch_datums(eval_ctx_);
    if (!e->is_batch_result()) {
      datums[0].set_null();
    } else {
      for (int64_t i = 0; i < batch_size; i++) {
        datums[i].set_null();
      }
    }
    e->set_evaluated_projected(eval_ctx_);
    e->get_eval_info(eval_ctx_).notnull_ = false;
  }
}

void ObJoinOp::blank_row_batch_one(const ExprFixedArray &exprs)
{
  clear_datum_eval_flag();
  for (int64_t i = 0; i < exprs.count(); i++) {
    ObExpr *expr = exprs.at(i);
    expr->locate_expr_datum(eval_ctx_).set_null();
    expr->set_evaluated_flag(eval_ctx_);
    if (expr->is_batch_result()) {
      expr->get_eval_info(eval_ctx_).notnull_ = false;
    }
  }
}

int ObJoinOp::calc_other_conds(bool &is_match)
{
  int ret = OB_SUCCESS;
  is_match = true;
  const ObIArray<ObExpr *> &conds = get_spec().other_join_conds_;
  ObDatum *cmp_res = NULL;
  ARRAY_FOREACH(conds, i) {
    if (OB_FAIL(conds.at(i)->eval(eval_ctx_, cmp_res))) {
      LOG_WARN("fail to calc other join condition", K(ret), K(*conds.at(i)));
    } else if (cmp_res->is_null() || 0 == cmp_res->get_int()) {
      is_match = false;
      break;
    }
  }

  return ret;
}

int ObJoinOp::get_next_left_row()
{
  int ret = common::OB_SUCCESS;
  left_row_joined_ = false;
  if (OB_FAIL(left_->get_next_row()) && OB_ITER_END != ret) {
    LOG_WARN("fail to get next left row", K(ret));
  }
  return ret;
}

int ObJoinOp::get_next_right_row()
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(right_->get_next_row()) && OB_ITER_END != ret) {
    LOG_WARN("fail to get next right row", K(ret));
  }

  return ret;
}

} // namespace sql
} // namespace oceanbase
