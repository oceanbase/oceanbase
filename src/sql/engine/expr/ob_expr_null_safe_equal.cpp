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
#include "sql/engine/expr/ob_expr_null_safe_equal.h"
#include "common/object/ob_obj_compare.h"
#include "sql/engine/expr/ob_expr_equal.h"
#include "sql/session/ob_sql_session_info.h"
//#include "sql/engine/expr/ob_expr_promotion_util.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

ObExprNullSafeEqual::ObExprNullSafeEqual(ObIAllocator &alloc)
    : ObRelationalExprOperator(alloc, T_OP_NSEQ, N_NS_EQUAL, 2)
{
}

int ObExprNullSafeEqual::calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObRelationalExprOperator::calc_result_type2(type, type1, type2, type_ctx))) {
    LOG_WARN("failed to calc_result_type2", K(ret));
  }
  // always allow NULL value
  type.set_result_flag(NOT_NULL_FLAG);
  return ret;
}

int ObExprNullSafeEqual::calc_result_typeN(ObExprResType &type,
                                ObExprResType *types,
                                int64_t param_num,
                                ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObRelationalExprOperator::calc_result_typeN(type, types, param_num, type_ctx))) {
    LOG_WARN("failed to calc_result_typeN", K(ret));
  }
  // always allow NULL value
  type.set_result_flag(NOT_NULL_FLAG);
  return ret;
}

int ObExprNullSafeEqual::compare_row2(common::ObObj &result, const common::ObNewRow *left_row,
                      const common::ObNewRow *right_row, common::ObExprCtx &expr_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(left_row)
      || OB_ISNULL(right_row)
      || OB_UNLIKELY(left_row->is_invalid())
      || OB_UNLIKELY(right_row->is_invalid())
      || OB_UNLIKELY(left_row->count_ != right_row->count_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameter", KPC(left_row), KPC(right_row), K(ret));
  } else {
    EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
    ObObj cmp_result;
    bool is_null_safe = true;
    ObCompareCtx cmp_ctx(ObMaxType,
                         CS_TYPE_INVALID,
                         is_null_safe,
                         expr_ctx.tz_offset_,
                         default_null_pos());
    result.set_int32(static_cast<int32_t>(true));
    for (int64_t i = 0; OB_SUCC(ret) && i < left_row->count_; ++i) {
      cmp_ctx.cmp_type_ = left_row->cells_[i].get_type();
      cmp_ctx.cmp_cs_type_ = left_row->cells_[i].get_collation_type();
      if (OB_FAIL(compare(cmp_result, left_row->cells_[i], right_row->cells_[i], cmp_ctx, cast_ctx, CO_EQ))) {
        LOG_WARN("fail to compare", KPC(left_row), KPC(right_row), K(ret));
      } else if (cmp_result.is_false()
                 || cmp_result.is_null()) {
        result.set_int32(static_cast<int32_t>(false));
      }
    }
  }
  return ret;
}
int ObExprNullSafeEqual::cg_expr(
    ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  int row_dim = 0;
  CK(NULL != expr_cg_ctx.allocator_);
  CK(2 == rt_expr.arg_cnt_);
  OZ(is_row_cmp(raw_expr, row_dim));
  if (OB_SUCC(ret)) {
    if (row_dim <= 0) { // non row comparison
      void **funcs = static_cast<void **>(expr_cg_ctx.allocator_->alloc(sizeof(void *)));
      if (OB_ISNULL(funcs)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else {
        auto &l = rt_expr.args_[0]->datum_meta_;
        auto &r = rt_expr.args_[1]->datum_meta_;
        bool has_lob_header = rt_expr.args_[0]->obj_meta_.has_lob_header() ||
                              rt_expr.args_[1]->obj_meta_.has_lob_header();
        if (ObDatumFuncs::is_string_type(l.type_) && ObDatumFuncs::is_string_type(r.type_)) {
          CK(l.cs_type_ == r.cs_type_);
        }

        if (OB_SUCC(ret)) {
          funcs[0] = (void *)ObExprCmpFuncsHelper::get_datum_expr_cmp_func(
                  l.type_, r.type_, l.scale_, r.scale_, lib::is_oracle_mode(), l.cs_type_, has_lob_header);
          CK(NULL != funcs[0]);
          rt_expr.inner_functions_ = funcs;
          rt_expr.inner_func_cnt_ = 1;
          rt_expr.eval_func_ = &ns_equal_eval;
        }
      }
    } else {
      // for row comparison , inner functions is the same with ObRelationalExprOperator::row_cmp
      OZ(cg_row_cmp_expr(row_dim, *expr_cg_ctx.allocator_, raw_expr, input_types_, rt_expr));
      if (OB_SUCC(ret)) {
        rt_expr.eval_func_ = &row_ns_equal_eval;
      }
    }
  }
  return ret;
}

int ObExprNullSafeEqual::ns_equal(const ObExpr &expr, ObDatum &res,
                                  ObExpr **left, ObEvalCtx &lctx, ObExpr **right, ObEvalCtx &rctx)
{
  int ret = OB_SUCCESS;
  ObDatum *l = NULL;
  ObDatum *r = NULL;
  bool equal = true;
  int cmp_ret = 0;
  for (int64_t i = 0; OB_SUCC(ret) && equal && i < expr.inner_func_cnt_; i++) {
    if (NULL == expr.inner_functions_[i]) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("NULL inner function", K(ret), K(i), K(expr));
    } else if (OB_FAIL(left[i]->eval(lctx, l))
        || OB_FAIL(right[i]->eval(rctx, r))) {
      LOG_WARN("expr evaluate failed", K(ret));
    } else {
      if (l->is_null() && r->is_null()) {
        equal = true;
      } else if (!l->is_null() && !r->is_null()) {
        if (OB_FAIL(reinterpret_cast<DatumCmpFunc>(expr.inner_functions_[i])(*l, *r, cmp_ret))) {
          LOG_WARN("cmp failed", K(ret));
        } else {
          equal = (0 == cmp_ret);
        }
      } else {
        equal = false;
      }
    }
  }
  if (OB_SUCC(ret)) {
    res.set_int(equal);
  }
  return ret;
}

int ObExprNullSafeEqual::ns_equal_eval(
    const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  CK(NULL != expr.inner_functions_ && 1 == expr.inner_func_cnt_);
  if (OB_SUCC(ret)) {
    ret = ns_equal(expr, expr_datum, expr.args_, ctx, expr.args_ + 1, ctx);
  }
  return ret;
}

int ObExprNullSafeEqual::row_ns_equal_eval(
    const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  CK(NULL != expr.inner_functions_
     && 2 == expr.arg_cnt_
     && expr.args_[0]->arg_cnt_ == expr.inner_func_cnt_
     && expr.args_[1]->arg_cnt_ == expr.inner_func_cnt_);
  if (OB_SUCC(ret)) {
    ret = ns_equal(expr, expr_datum, expr.args_[0]->args_, ctx, expr.args_[1]->args_, ctx);
  }
  return ret;
}

}//end of ns sql
}//end of ns oceanbase
