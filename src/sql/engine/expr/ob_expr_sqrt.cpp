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
#include "ob_expr_sqrt.h"
#include <cmath>
#include <type_traits>
#include "share/object/ob_obj_cast.h"
#include "objit/common/ob_item_type.h"
//#include "sql/engine/expr/ob_expr_promotion_util.h"
#include "sql/session/ob_sql_session_info.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

ObExprSqrt::ObExprSqrt(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_SQRT, N_SQRT, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprSqrt::~ObExprSqrt()
{
}

int ObExprSqrt::calc_result_type1(ObExprResType &type,
                                  ObExprResType &type1,
                                  common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (NOT_ROW_DIMENSION != row_dimension_ || ObMaxType == type1.get_type()) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
  } else if (!lib::is_oracle_mode()) {
    type.set_double();
  } else {
    if (ob_is_real_type(type1.get_type())) {
      type.set_type(type1.get_type());
    } else {
      type.set_number();
    }
    type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY2[ORACLE_MODE][type.get_type()].get_scale());
    type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY2[ORACLE_MODE][type.get_type()].get_precision());
  }
  type1.set_calc_type(type.get_type());
  ObExprOperator::calc_result_flag1(type, type1);
  return ret;
}

int calc_sqrt_expr_mysql(const ObExpr &expr, ObEvalCtx &ctx,
                                ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *arg = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, arg))) {
    LOG_WARN("eval arg failed", K(ret), K(expr));
  } else if (arg->is_null()) {
    res_datum.set_null();
  } else {
    double val = arg->get_double();
    if (val < 0) {
      res_datum.set_null();
    } else {
      res_datum.set_double(std::sqrt(val));
    }
  }
  return ret;
}

int calc_sqrt_expr_mysql_in_batch(const ObExpr &expr,
                                  ObEvalCtx &ctx,
                                  const ObBitVector &skip,
                                  const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("vectorized evaluate failed", K(ret), K(expr));
  } else {
    ObBitVector &eval_flag = expr.get_evaluated_flags(ctx);
    ObDatumVector arg_datums = expr.args_[0]->locate_expr_datumvector(ctx);
    ObDatum *res_datums = expr.locate_batch_datums(ctx);
    for (int64_t i = 0; i < batch_size; ++i) {
      if (!eval_flag.contain(i) && !skip.contain(i)) {
        if (arg_datums.at(i)->is_null() || arg_datums.at(i)->get_double() < 0) {
          res_datums[i].set_null();
        } else {
          res_datums[i].set_double(std::sqrt(arg_datums.at(i)->get_double()));
        }
        eval_flag.set(i);
      }
    }
  }
  return ret;
}

int calc_sqrt_expr_oracle_double(const ObExpr &expr, ObEvalCtx &ctx,
                                ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *arg = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, arg))) {
    LOG_WARN("eval arg failed", K(ret), K(expr));
  } else if (arg->is_null()) {
    res_datum.set_null();
  } else {
    ObObjType arg_type = expr.args_[0]->datum_meta_.type_;
    if (ObDoubleType == arg_type) {
      double val = arg->get_double();
      if (val < 0) {
        res_datum.set_double(NAN);
      } else {
        res_datum.set_double(std::sqrt(val));
      }
    } else if (ObFloatType == arg_type) {
      float val = arg->get_float();
      if (val < 0) {
        res_datum.set_float(NAN);
      } else {
        res_datum.set_float(sqrtf(val));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected arg type", K(ret), K(arg_type));
    }
  }
  return ret;
}

template <typename T>
int calc_sqrt_expr_oracle_double_in_batch_impl(const ObExpr &expr,
                                               ObEvalCtx &ctx,
                                               const ObBitVector &skip,
                                               const int64_t batch_size)
{
  ObBitVector &eval_flag = expr.get_evaluated_flags(ctx);
  ObDatumVector arg_datums = expr.args_[0]->locate_expr_datumvector(ctx);
  ObDatum *res_datums = expr.locate_batch_datums(ctx);
  int ret = OB_SUCCESS;
  if (std::is_same<T, float>::value) {
    for (int64_t i = 0; i < batch_size && OB_SUCC(ret); ++i) {
      if (!eval_flag.contain(i) && !skip.contain(i)) {
        ObDatum *arg = arg_datums.at(i);
        if (arg->is_null()) {
          res_datums[i].set_null();
        } else if (arg->get_float() < 0) {
          res_datums[i].set_float(NAN);
        } else {
          res_datums[i].set_float(std::sqrt(arg->get_float()));
        }
        eval_flag.set(i);
      }
    }
  } else if (std::is_same<T, double>::value) {
    for (int64_t i = 0; i < batch_size && OB_SUCC(ret); ++i) {
      if (!eval_flag.contain(i) && !skip.contain(i)) {
        ObDatum *arg = arg_datums.at(i);
        if (arg->is_null()) {
          res_datums[i].set_null();
        } else if (arg->get_double() < 0) {
          res_datums[i].set_double(NAN);
        } else {
          res_datums[i].set_double(std::sqrt(arg->get_double()));
        }
        eval_flag.set(i);
      }
    }
  }
  return ret;
}

int calc_sqrt_expr_oracle_double_in_batch(const ObExpr &expr,
                                          ObEvalCtx &ctx,
                                          const ObBitVector &skip,
                                          const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("vectorized evaluate failed", K(ret), K(expr));
  } else {
    ObObjType arg_type = expr.args_[0]->datum_meta_.type_;
    switch (arg_type) {
    case ObDoubleType:
      ret = calc_sqrt_expr_oracle_double_in_batch_impl<double>(expr, ctx, skip, batch_size);
      break;
    case ObFloatType:
      ret = calc_sqrt_expr_oracle_double_in_batch_impl<float>(expr, ctx, skip, batch_size);
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected arg type", K(ret), K(arg_type));
      break;
    }
  }
  return ret;
}

int calc_sqrt_expr_oracle_number(const ObExpr &expr, ObEvalCtx &ctx,
                                ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *arg = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, arg))) {
    LOG_WARN("eval arg failed", K(ret), K(expr));
  } else if (arg->is_null()) {
    res_datum.set_null();
  } else {
    const number::ObNumber arg_nmb(arg->get_number());
    number::ObNumber res_nmb;
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    if (OB_FAIL(arg_nmb.sqrt(res_nmb, alloc_guard.get_allocator()))) {
      LOG_WARN("calc sqrt failed", K(ret), K(arg_nmb), K(res_nmb));
    } else {
      res_datum.set_number(res_nmb);
    }
  }
  return ret;
}

int calc_sqrt_expr_oracle_number_in_batch(const ObExpr &expr,
                                          ObEvalCtx &ctx,
                                          const ObBitVector &skip,
                                          const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("vectorized evaluate failed", K(ret), K(expr));
  } else {
    ObBitVector &eval_flag = expr.get_evaluated_flags(ctx);
    ObDatumVector arg_datums = expr.args_[0]->locate_expr_datumvector(ctx);
    ObDatum *res_datums = expr.locate_batch_datums(ctx);
    number::ObNumber res_nmb;
    number::ObNumber arg_nmb;
    char temp_buf_alloc[number::ObNumber::MAX_CALC_BYTE_LEN];
    ObDataBuffer temp_allocator(temp_buf_alloc, number::ObNumber::MAX_CALC_BYTE_LEN);
    for (int64_t i = 0; i < batch_size && OB_SUCC(ret); ++i) {
      if (!eval_flag.contain(i) && !skip.contain(i)) {
        if (arg_datums.at(i)->is_null()) {
          res_datums[i].set_null();
        } else {
          arg_nmb = arg_datums.at(i)->get_number();
          if (OB_FAIL(arg_nmb.sqrt(res_nmb, temp_allocator))) {
            LOG_WARN("calc sqrt failed", K(ret), K(arg_nmb), K(res_nmb));
          } else {
            res_datums[i].set_number(res_nmb);
          }
          temp_allocator.free();
        }
        eval_flag.set(i);
      }
    }
  }
  return ret;
}

int ObExprSqrt::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  if (OB_UNLIKELY(1 != rt_expr.arg_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg_cnt_ of expr", K(ret), K(rt_expr));
  } else {
    ObObjType arg_res_type = rt_expr.args_[0]->datum_meta_.type_;
    if (lib::is_oracle_mode()) {
      if (ObDoubleType == arg_res_type || ObFloatType == arg_res_type) {
        rt_expr.eval_func_ = calc_sqrt_expr_oracle_double;
        rt_expr.eval_batch_func_ = calc_sqrt_expr_oracle_double_in_batch;
      } else if (ObNumberType == arg_res_type) {
        rt_expr.eval_func_ = calc_sqrt_expr_oracle_number;
        rt_expr.eval_batch_func_ = calc_sqrt_expr_oracle_number_in_batch;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("arg type must be double or number in oracle mode", K(ret),
            K(arg_res_type), K(rt_expr));
      }
    } else {
      if (ObDoubleType == arg_res_type) {
        rt_expr.eval_func_ = calc_sqrt_expr_mysql;
        rt_expr.eval_batch_func_ = calc_sqrt_expr_mysql_in_batch;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("arg_res_type must be double in mysql mode", K(ret), K(arg_res_type));
      }
    }
  }
  return ret;
}
} //namespace sql
} //namespace oceanbase
