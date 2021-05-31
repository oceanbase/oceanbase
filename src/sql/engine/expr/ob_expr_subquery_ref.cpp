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
#include "sql/engine/expr/ob_expr_subquery_ref.h"
#include "sql/engine/subquery/ob_subplan_filter_op.h"
#include "common/row/ob_row_iterator.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase {
using namespace common;
namespace sql {
OB_SERIALIZE_MEMBER(
    (ObExprSubQueryRef, ObExprOperator), result_is_scalar_, scalar_result_type_, subquery_idx_, row_desc_);

ObExprSubQueryRef::ObExprSubQueryRef(ObIAllocator& alloc)
    : ObExprOperator(alloc, T_REF_QUERY, N_REF_QUERY, 0, NOT_ROW_DIMENSION),
      result_is_scalar_(false),
      scalar_result_type_(alloc),
      subquery_idx_(OB_INVALID_INDEX),
      row_desc_(alloc)
{}

ObExprSubQueryRef::~ObExprSubQueryRef()
{}

int ObExprSubQueryRef::assign(const ObExprOperator& other)
{
  int ret = OB_SUCCESS;
  const ObExprSubQueryRef* tmp_other = dynamic_cast<const ObExprSubQueryRef*>(&other);
  if (OB_UNLIKELY(NULL == tmp_other)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument. wrong type for other", K(ret), K(other));
  } else if (this != tmp_other) {
    if (OB_FAIL(ObExprOperator::assign(other))) {
      LOG_WARN("copy in Base class ObExprOperator failed", K(ret));
    } else if (OB_FAIL(scalar_result_type_.assign(tmp_other->scalar_result_type_))) {
      LOG_WARN("copy scalar_result_type_ failed", K(ret));
    } else if (OB_FAIL(row_desc_.assign(tmp_other->row_desc_))) {
      LOG_WARN("copy accuracy desc failed", K(ret));
    } else {
      this->result_is_scalar_ = tmp_other->result_is_scalar_;
      this->subquery_idx_ = tmp_other->subquery_idx_;
    }
  }
  return ret;
}

void ObExprSubQueryRef::reset()
{
  result_is_scalar_ = false;
  subquery_idx_ = OB_INVALID_INDEX;
  result_type_.set_null();
  row_desc_.reset();
}

int ObExprSubQueryRef::calc_result_type0(ObExprResType& type, ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  if (result_is_scalar_) {
    type = scalar_result_type_;
  } else {
    // for static typing engine, we return ObExpr::extra_
    type.set_int();
    type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].scale_);
    type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].precision_);
  }
  return OB_SUCCESS;
}

int ObExprSubQueryRef::calc_result0(ObObj& result, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  ObNewRow* row = NULL;
  ObNewRowIterator* row_iter = NULL;
  ObIArray<ObNewRowIterator*>* row_iters = expr_ctx.subplan_iters_;
  if (OB_ISNULL(row_iters) || OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_UNLIKELY(subquery_idx_ < 0 || subquery_idx_ >= row_iters->count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("subquery row iterator does not exist", "row_iters_count", row_iters->count(), K_(subquery_idx));
  } else if (OB_ISNULL(row_iter = row_iters->at(subquery_idx_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    row_iter->reset();
  }
  if (OB_FAIL(ret)) {
    /*do nothing*/
  } else if (result_is_scalar_) {
    // subquery result is a scalar obj
    bool iter_end = false;
    if (OB_FAIL(row_iter->get_next_row(row))) {
      if (OB_LIKELY(OB_ITER_END == ret)) {
        ret = OB_SUCCESS;
        iter_end = true;
        result.set_null();
      } else {
        LOG_WARN("get next row from row iterator failed", K(ret));
      }
    } else if (OB_ISNULL(row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("row is null", K(ret));
    } else if (OB_UNLIKELY(1 != row->get_count())) {
      // not a scalar obj
      ret = OB_ERR_INVALID_COLUMN_NUM;
      LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_NUM, (int64_t)1);
    } else if (OB_FAIL(ob_write_obj(*expr_ctx.calc_buf_, row->get_cell(0), result))) {
      LOG_WARN("deep copy obj failed", K(ret), K(row->get_cell(0)));
    }
    if (OB_SUCC(ret)) {
      if (iter_end) {
        // it's iter end, don't get next row
        // #bug29851474
      } else if (OB_UNLIKELY(OB_SUCCESS == (ret = row_iter->get_next_row(row)))) {
        LOG_WARN("subquery too many rows", K(ret));
        ret = OB_SUBQUERY_TOO_MANY_ROW;
      } else if (OB_LIKELY(OB_ITER_END == ret)) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("get next row from row iterator failed", K(ret));
      }
    }
  } else {
    // subquery result is a row or a set, so return the index of subquery row iterator
    result.set_int(subquery_idx_);
  }
  return ret;
}

void ObExprSubQueryRef::set_scalar_result_type(const ObExprResType& result_type)
{
  scalar_result_type_ = result_type;
}

int ObExprSubQueryRef::cg_expr(ObExprCGCtx&, const ObRawExpr&, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  ExtraInfo::get_info(rt_expr).is_scalar_ = result_is_scalar_;
  CK(OB_INVALID_INDEX != subquery_idx_);
  ExtraInfo::get_info(rt_expr).iter_idx_ = subquery_idx_;
  rt_expr.eval_func_ = &expr_eval;
  return ret;
}

int ObExprSubQueryRef::expr_eval(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  const ExtraInfo& info = ExtraInfo::get_info(expr);
  if (info.is_scalar_) {
    ObDatum* datum = NULL;
    ObSubQueryIterator* iter = NULL;
    if (OB_FAIL(get_subquery_iter(ctx, info, iter))) {
      LOG_WARN("get sub query iterator failed", K(ret), K(info));
    } else if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null iter returned", K(ret));
    } else if (1 != iter->get_output().count()) {
      // not a scalar obj
      ret = OB_ERR_INVALID_COLUMN_NUM;
      LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_NUM, 1L);
    } else {
      iter->reset();
      if (OB_FAIL(iter->get_next_row())) {
        if (OB_LIKELY(OB_ITER_END == ret)) {
          ret = OB_SUCCESS;
          expr_datum.set_null();
        } else {
          LOG_WARN("get next row from subquery failed", K(ret));
        }
      } else if (OB_FAIL(iter->get_output().at(0)->eval(ctx, datum))) {
        LOG_WARN("expr evaluate failed", K(ret));
        // deep copy datum since the following iter->get_next_row() make datum invalid.
      } else if (OB_FAIL(expr.deep_copy_datum(ctx, *datum))) {
        LOG_WARN("deep copy datum failed", K(ret), K(datum));
      } else {
        if (OB_UNLIKELY(OB_SUCCESS == (ret = iter->get_next_row()))) {
          ret = OB_SUBQUERY_TOO_MANY_ROW;
          LOG_WARN("subquery too many rows", K(ret));
        } else if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("get next row from subquery failed", K(ret));
        }
      }
    }
  } else {
    // result is row or set, return extra info directly
    expr_datum.set_int(static_cast<int64_t>(expr.extra_));
  }
  return ret;
}

int ObExprSubQueryRef::get_subquery_iter(ObEvalCtx& ctx, const ExtraInfo& extra_info, ObSubQueryIterator*& iter)
{
  int ret = OB_SUCCESS;
  if (!extra_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid extra info", K(ret), K(extra_info));
  } else {
    ObOperatorKit* kit = ctx.exec_ctx_.get_operator_kit(extra_info.op_id_);
    if (OB_ISNULL(kit) || OB_ISNULL(kit->op_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("operator is NULL", K(ret), K(extra_info), KP(kit));
    } else if (PHY_SUBPLAN_FILTER != kit->op_->get_spec().type_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("is not subplan filter operator", K(ret), K(extra_info), "spec", kit->op_->get_spec());
    } else {
      ObSubPlanFilterOp* op = static_cast<ObSubPlanFilterOp*>(kit->op_);
      if (extra_info.iter_idx_ >= op->get_subplan_iters().count()) {
        ret = OB_ARRAY_OUT_OF_RANGE;
        LOG_WARN("out of range", K(ret), K(extra_info), "iter_cnt", op->get_subplan_iters().count());
      } else {
        iter = op->get_subplan_iters().at(extra_info.iter_idx_);
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
