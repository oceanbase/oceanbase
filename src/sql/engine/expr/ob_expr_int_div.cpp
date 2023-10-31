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
#include <math.h>
#include "lib/oblog/ob_log.h"
#include "share/object/ob_obj_cast.h"
#include "sql/engine/expr/ob_expr_int_div.h"
#include "sql/engine/expr/ob_expr_div.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/session/ob_sql_session_info.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;
ObArithFunc ObExprIntDiv::int_div_funcs_[ObMaxTC] =
{
  NULL,
  ObExprIntDiv::intdiv_int,//int
  ObExprIntDiv::intdiv_uint,//uint
  ObExprIntDiv::intdiv_number,//float
  ObExprIntDiv::intdiv_number,//double
  ObExprIntDiv::intdiv_number,//number
  NULL,//datetime
  NULL,//date
  NULL,//time
  NULL,//year
  NULL,//string
  NULL,//extend
  NULL,//unknown
  NULL,//text
  NULL,//bit
  NULL,//enumset
  NULL,//enumsetinner
};

#define REPORT_OUT_OF_RANGE_ERROR(format, meta, left, right) \
  do \
  {\
    ret = OB_OPERATE_OVERFLOW;\
    pos = 0;\
    int tmp_ret = OB_SUCCESS;\
    if (OB_UNLIKELY((tmp_ret = databuff_printf(expr_str, \
                                              OB_MAX_TWO_OPERATOR_EXPR_LENGTH, \
                                              pos, \
                                              format, \
                                              left, \
                                              right)) != OB_SUCCESS)) {\
      ret = tmp_ret;\
      LOG_WARN("fail to print databuff", K(ret), K(OB_MAX_TWO_OPERATOR_EXPR_LENGTH), K(pos));\
    } else {\
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, meta, expr_str);\
    }\
  }while(0)

ObExprIntDiv::ObExprIntDiv(ObIAllocator &alloc)
  : ObArithExprOperator(alloc,
                        T_OP_INT_DIV,
                        N_INT_DIV,
                        2,
                        NOT_ROW_DIMENSION,
                        ObExprResultTypeUtil::get_int_div_result_type,
                        ObExprResultTypeUtil::get_int_div_calc_type,
                        int_div_funcs_)
{}

int ObExprIntDiv::calc_result_type2(ObExprResType &type, ObExprResType &type1, ObExprResType &type2, ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObArithExprOperator::calc_result_type2(type, type1, type2, type_ctx))) {
    LOG_WARN("fail to calc result type", K(ret), K(type), K(type1), K(type2));
  } else {
    type.set_precision(static_cast<ObPrecision>(MAX(type1.get_precision(), 0)));
    type.set_scale(0);
  }
  return ret;
}

int ObExprIntDiv::calc(ObObj &res, const ObObj &left, const ObObj &right, ObIAllocator *allocator, ObScale scale)
{
  int ret = OB_SUCCESS;
  ObObjType res_type;
  common::ObObjType result_ob1_type;
  common::ObObjType result_ob2_type;
  ObCalcTypeFunc calc_type_func = ObExprResultTypeUtil::get_int_div_calc_type;
  if (OB_FAIL(ObExprResultTypeUtil::get_int_div_result_type(res_type, result_ob1_type, result_ob2_type, left.get_type(), right.get_type()))) {
    LOG_WARN("fail to get int div result type", K(ret), K(left), K(right));
  } else if (OB_FAIL(ObArithExprOperator::calc(res, left, right, allocator, scale, calc_type_func, int_div_funcs_))) {
    LOG_WARN("fail to calc int div", K(ret), K(left), K(right));
  } else if (res_type != res.get_type() && ObNullType != res.get_type()) {
    if (ObNumberTC == res.get_type_class()) {
      number::ObNumber num;
      if (OB_FAIL(num.from(res.get_number(), *allocator))) {
        LOG_WARN("deep copy failed", K(ret), K(res.get_number()));
      } else if (OB_FAIL(num.trunc(0))) {
        LOG_WARN("fail to trunc number", K(ret), K(num));
      } else {
        res.set_number(num);
      }
    }
    if (OB_SUCC(ret)) {
      const ObObj *res_obj = NULL;
      ObCastCtx cast_ctx(allocator, NULL, CM_WARN_ON_FAIL, CS_TYPE_INVALID);
      EXPR_CAST_OBJ_V2(res_type, res, res_obj);
      if (OB_SUCC(ret)) {
        if (OB_UNLIKELY(NULL == res_obj)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("cast obj result is NULL", K(ret));
        } else {
          res = *res_obj;
        }
      } else {
        LOG_WARN("fail to cast obj", K(ret), K(res_type), K(res));
      }
    }
  } else { /* do nothing */ }
  return ret;
}

int ObExprIntDiv::intdiv_int(ObObj &res, const ObObj &left, const ObObj &right, ObIAllocator *allocator, ObScale scale)
{
  UNUSED(allocator);
  UNUSED(scale);
  int ret = OB_SUCCESS;
  int64_t left_i = left.get_int();
  int64_t right_i = 0;
  char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
  int64_t pos = 0;
  if (OB_UNLIKELY(left.get_type_class() != ObIntTC)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected operator type", "left type", left.get_type_class(), "right type", right.get_type_class());
  } else {
    if (OB_LIKELY(ObIntTC == right.get_type_class())) {
      right_i = right.get_int();
      if (OB_UNLIKELY(0 == right_i)) {
        res.set_null();
      } else if (OB_UNLIKELY(INT64_MIN == left_i && (-1) == right_i)) {
        //INT64_MIN / -1 is undefined behavior in C language.
        //report an error no matter in dml or ddl stmt to be compatible with mysql
        REPORT_OUT_OF_RANGE_ERROR("'(%ld div %ld)'", "BIGINT", left_i, right_i);
      } else {
        int64_t res_i = left_i / right_i;
        if (OB_UNLIKELY(is_int_int_out_of_range(left_i, right_i, res_i))) {
          REPORT_OUT_OF_RANGE_ERROR("'(%ld div %ld)'", "BIGINT", left_i, right_i);
        } else {
          res.set_int(res_i);
        }
      }
    } else if (ObUIntTC == right.get_type_class()) {
      uint64_t left_ui = left_i < 0 ? -left_i : left_i;
      uint64_t right_ui = right.get_uint64();
      if (OB_UNLIKELY(0 == right_ui)) {
        res.set_null();
      } else {
        uint64_t res_ui = left_ui / right_ui;
        if (OB_UNLIKELY(left_i < 0 && res_ui > 0)){
          REPORT_OUT_OF_RANGE_ERROR("'(%ld div %lu)'", "BIGINT UNSIGNED", left_i, right_ui);
        } else  {
          res.set_uint64(res_ui);
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected operator type", "left type", left.get_type_class(), "right type", right.get_type_class());
    }
  }
  return ret;
}

int ObExprIntDiv::intdiv_uint(ObObj &res, const ObObj &left, const ObObj &right, ObIAllocator *allocator, ObScale scale)
{
  UNUSED(allocator);
  UNUSED(scale);
  int ret = OB_SUCCESS;
  uint64_t left_ui = left.get_uint64();
  uint64_t right_ui = 0;
  char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
  int64_t pos = 0;
  if (OB_UNLIKELY(left.get_type_class() != ObUIntTC)){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected operator type", "left type", left.get_type_class(), "right type", right.get_type_class());
  } else {
    if (OB_LIKELY(ObUIntTC == right.get_type_class())) {
      right_ui = right.get_uint64();
      if (OB_UNLIKELY(0 == right_ui)) {
        res.set_null();
      } else {
        uint64_t res_ui = left_ui / right_ui;
        res.set_uint64(res_ui);
      }
    } else if (ObIntTC == right.get_type_class()){
      int64_t right_i = right.get_int();
      right_ui = right_i < 0 ? -right_i : right_i;
      if (OB_UNLIKELY(0 == right_ui)) {
        res.set_null();
      } else {
        uint64_t res_ui = left_ui / right_ui;
        if (OB_UNLIKELY(right_i < 0 && res_ui > 0)) { // eg: 7 div -6
          REPORT_OUT_OF_RANGE_ERROR("'(%lu div %ld)'", "BIGINT UNSIGNED", left_ui, right_i);
        } else  {
          res.set_uint64(res_ui);
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected operator type", "left type", left.get_type_class(), "right type", right.get_type_class());
    }
  }
  return ret;
}

int ObExprIntDiv::intdiv_number(ObObj &res, const ObObj &left, const ObObj &right, ObIAllocator *allocator, ObScale scale)
{
  int ret=OB_SUCCESS;
  if (OB_UNLIKELY(left.get_type_class() != right.get_type_class())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected operator type", "left type", left.get_type_class(), "right type", right.get_type_class());
  } else if (OB_FAIL(ObExprDiv::calc(res, left, right, allocator, scale))) {
    LOG_WARN("failed to div numbers", K(ret), K(left), K(right));
  } else {/* do nothing */ }
  return ret;
}


int ObExprIntDiv::div_int_int(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &datum)
{
  int ret = OB_SUCCESS;
  ObDatum *left = NULL;
  ObDatum *right = NULL;
  bool is_finish = false;
  if (OB_FAIL(ObArithExprOperator::get_arith_operand(expr, ctx, left, right, datum, is_finish))) {
    LOG_WARN("get_arith_operand failed", K(ret));
  } else if (is_finish) {
    //do nothing
  } else if (right->get_int() == 0) {
    if (expr.is_error_div_by_zero_) {
      ret = OB_DIVISION_BY_ZERO;
    } else {
      datum.set_null();
    }
  } else {
    char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
    int64_t pos = 0;
    int64_t left_i = left->get_int();
    int64_t right_i = right->get_int();
    if (OB_UNLIKELY(INT64_MIN == left_i && (-1) == right_i)) {
      //INT64_MIN / -1 is undefined behavior in C language.
      //report an error no matter in dml or ddl stmt to be compatible with mysql
      REPORT_OUT_OF_RANGE_ERROR("'(%ld div %ld)'", "BIGINT", left_i, right_i);
    } else {
      int64_t res_i = left_i / right_i;
      if (OB_UNLIKELY(is_int_int_out_of_range(left_i, right_i, res_i))) {
        REPORT_OUT_OF_RANGE_ERROR("'(%ld div %ld)'", "BIGINT", left_i, right_i);
      } else {
        datum.set_int(res_i);
      }
    }
  }
  return ret;
}


int ObExprIntDiv::div_int_uint(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &datum)
{
  int ret = OB_SUCCESS;
  ObDatum *left = NULL;
  ObDatum *right = NULL;
  bool is_finish = false;
  if (OB_FAIL(ObArithExprOperator::get_arith_operand(expr, ctx, left, right, datum, is_finish))) {
    LOG_WARN("get_arith_operand failed", K(ret));
  } else if (is_finish) {
    //do nothing
  } else if (right->get_uint() == 0) {
    if (expr.is_error_div_by_zero_) {
      ret = OB_DIVISION_BY_ZERO;
    } else {
      datum.set_null();
    }
  } else {
    int64_t left_i = left->get_int();
    uint64_t left_ui = left_i < 0 ? -left_i : left_i;
    uint64_t right_ui = right->get_uint();
    uint64_t res_ui = left_ui / right_ui;
    if (OB_UNLIKELY(left_i < 0 && res_ui > 0)){
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      int64_t pos = 0;
      REPORT_OUT_OF_RANGE_ERROR("'(%ld div %lu)'", "BIGINT UNSIGNED", left_i, right_ui);
    } else  {
      datum.set_uint(res_ui);
    }
  }
  return ret;
}

int ObExprIntDiv::div_uint_uint(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &datum)
{
  int ret = OB_SUCCESS;
  ObDatum *left = NULL;
  ObDatum *right = NULL;
  bool is_finish = false;
  if (OB_FAIL(ObArithExprOperator::get_arith_operand(expr, ctx, left, right, datum, is_finish))) {
    LOG_WARN("get_arith_operand failed", K(ret));
  } else if (is_finish) {
    //do nothing
  } else if (right->get_uint() == 0) {
    if (expr.is_error_div_by_zero_) {
      ret = OB_DIVISION_BY_ZERO;
    } else {
      datum.set_null();
    }
  } else {
    uint64_t left_ui = left->get_uint();
    uint64_t right_ui = right->get_uint();
    uint64_t res_ui = left_ui / right_ui;
    datum.set_uint(res_ui);

  }
  return ret;
}

int ObExprIntDiv::div_uint_int(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &datum)
{
  int ret = OB_SUCCESS;
  ObDatum *left = NULL;
  ObDatum *right = NULL;
  bool is_finish = false;
  if (OB_FAIL(ObArithExprOperator::get_arith_operand(expr, ctx, left, right, datum, is_finish))) {
    LOG_WARN("get_arith_operand failed", K(ret));
  } else if (is_finish) {
    //do nothing
  } else if (right->get_int() == 0) {
    if (expr.is_error_div_by_zero_) {
      ret = OB_DIVISION_BY_ZERO;
    } else {
      datum.set_null();
    }
  } else {
    uint64_t left_ui = left->get_uint();
    int64_t right_i = right->get_int();
    uint64_t right_ui = right_i < 0 ? -right_i : right_i;
    uint64_t res_ui = left_ui / right_ui;
    if (OB_UNLIKELY(right_i < 0 && res_ui > 0)) { // eg: 7 div -6
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      int64_t pos = 0;
      REPORT_OUT_OF_RANGE_ERROR("'(%lu div %ld)'", "BIGINT UNSIGNED", left_ui, right_i);
    } else  {
      datum.set_uint(res_ui);
    }
  }
  return ret;
}

int ObExprIntDiv::div_number(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &datum)
{
  int ret = OB_SUCCESS;
  ObDatum *left = NULL;
  ObDatum *right = NULL;
  bool is_finish = false;
  if (OB_FAIL(get_arith_operand(expr, ctx, left, right, datum, is_finish))) {
    LOG_WARN("get_arith_operand failed", K(ret));
  } else if (is_finish) {
    //do nothing
  } else if (right->get_number().is_zero()) {
    if (expr.is_error_div_by_zero_) {
      ret = OB_DIVISION_BY_ZERO;
    } else {
      datum.set_null();
    }
  } else {
    number::ObNumber lnum(left->get_number());
    number::ObNumber rnum(right->get_number());
    char local_buff[number::ObNumber::MAX_BYTE_LEN];
    ObDataBuffer local_alloc(local_buff, number::ObNumber::MAX_BYTE_LEN);
    number::ObNumber result;
    int64_t int64_value = 0;
    uint64_t uint64_value = 0;

    if (OB_FAIL(lnum.div(rnum, result, local_alloc))) {
      LOG_WARN("add number failed", K(ret));
    } else {
      if (ObIntType == expr.datum_meta_.type_) {
        ret = result.extract_valid_int64_with_trunc(int64_value);
      } else {
        ret = result.extract_valid_uint64_with_trunc(uint64_value);
      }

      if (OB_FAIL(ret)) {
        LOG_WARN("fail to trunc number", K(ret), K(result));
        if (OB_DATA_OUT_OF_RANGE == ret) {
          ret = OB_OPERATE_OVERFLOW;
          LOG_WARN("operate overflow", K(ret), K(result), K(int64_value), K(uint64_value));
        }
      } else {
        if (ObIntType == expr.datum_meta_.type_) {
          datum.set_int(int64_value);
        } else {
          datum.set_uint(uint64_value);
        }
      }
    }
  }
  return ret;
}

int ObExprIntDiv::cg_expr(ObExprCGCtx &op_cg_ctx,
                          const ObRawExpr &raw_expr,
                          ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  OB_ASSERT(2 == rt_expr.arg_cnt_);
  OB_ASSERT(NULL != rt_expr.args_);
  OB_ASSERT(NULL != rt_expr.args_[0]);
  OB_ASSERT(NULL != rt_expr.args_[1]);
  const common::ObObjType left = rt_expr.args_[0]->datum_meta_.type_;
  const common::ObObjType right = rt_expr.args_[1]->datum_meta_.type_;
  const ObObjTypeClass left_tc = ob_obj_type_class(left);
  const ObObjTypeClass right_tc = ob_obj_type_class(right);
  OB_ASSERT(left == input_types_[0].get_calc_type());
  OB_ASSERT(right == input_types_[1].get_calc_type());

  rt_expr.inner_functions_ = NULL;
  LOG_DEBUG("arrive here cg_expr", K(ret), K(rt_expr), K(left), K(right));
  if (ObIntTC == left_tc) {
    if (ObIntTC == right_tc) {
      rt_expr.eval_func_ = ObExprIntDiv::div_int_int;
    } else if (ObUIntTC == right_tc) {
      rt_expr.eval_func_ = ObExprIntDiv::div_int_uint;
    } else {
      rt_expr.eval_func_ = ObExprIntDiv::div_number;
    }
  } else if (ObUIntTC == left_tc) {
    if (ObIntTC == right_tc) {
      rt_expr.eval_func_ = ObExprIntDiv::div_uint_int;
    } else if (ObUIntTC == right_tc) {
      rt_expr.eval_func_ = ObExprIntDiv::div_uint_uint;
    } else {
      rt_expr.eval_func_ = ObExprIntDiv::div_number;
    }
  } else {
    rt_expr.eval_func_ = ObExprIntDiv::div_number;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(op_cg_ctx.session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected session is null", K(ret));
  } else {
    stmt::StmtType stmt_type = op_cg_ctx.session_->get_stmt_type();
    if (lib::is_mysql_mode()
        && is_error_for_division_by_zero(op_cg_ctx.session_->get_sql_mode())
        && is_strict_mode(op_cg_ctx.session_->get_sql_mode())
        && !op_cg_ctx.session_->is_ignore_stmt()
        && (stmt::T_INSERT == stmt_type
            || stmt::T_REPLACE == stmt_type
            || stmt::T_UPDATE == stmt_type)) {
      rt_expr.is_error_div_by_zero_ = true;
    } else {
      rt_expr.is_error_div_by_zero_ = false;
    }
  }
  return ret;
}
