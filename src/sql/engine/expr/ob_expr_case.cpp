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
#include "sql/engine/expr/ob_expr_case.h"
#include "sql/engine/expr/ob_expr_operator.h"
//#include "sql/engine/expr/ob_expr_promotion_util.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase {
using namespace common;
namespace sql {

typedef int (*CheckIsMatchFunc)(const ObDatum* when_datum, bool& match_when);

ObExprCase::ObExprCase(ObIAllocator& alloc) : ObExprOperator(alloc, T_OP_CASE, N_CASE, MORE_THAN_ONE, NOT_ROW_DIMENSION)
{
  disable_operand_auto_cast();
  param_lazy_eval_ = true;
}

ObExprCase::~ObExprCase()
{}

// param_num include then/else expr, exclude when expr.
int ObExprCase::calc_result_typeN(
    ObExprResType& type, ObExprResType* types_stack, int64_t param_num, ObExprTypeCtx& type_ctx) const
{
  // case
  //  when 10 then expr1
  //  when 11 then expr2
  //  [else expr3]
  int ret = OB_SUCCESS;
  if (OB_ISNULL(types_stack)) {
    LOG_WARN("null types");
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(param_num < 3 || param_num % 2 == 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param num is not correct", K(param_num));
  } else {  // param_num >=3 and param_num is odd

    /* in order to be compatible with mysql
     * both in ob_expr_cae.cpp and ob_expr_arg_case.cpp
     * types_stack includes the condition exprs.
     * In expr_case, there is no arg param expr compared with expr_arg_case
     */
    const int64_t cond_type_count = param_num / 2;
    const int64_t val_type_count = param_num - cond_type_count;
    const ObLengthSemantics default_length_semantics =
        (OB_NOT_NULL(type_ctx.get_session()) ? type_ctx.get_session()->get_actual_nls_length_semantics() : LS_BYTE);

    if (OB_FAIL(aggregate_result_type_for_case(type,
            types_stack + cond_type_count,
            val_type_count,
            type_ctx.get_coll_type(),
            lib::is_oracle_mode(),
            default_length_semantics,
            type_ctx.get_session()))) {
      LOG_WARN("failed to aggregate result type");
    } else {
      ObExprOperator::calc_result_flagN(type, types_stack + cond_type_count, val_type_count);
    }

    const ObSQLSessionInfo* session = dynamic_cast<const ObSQLSessionInfo*>(type_ctx.get_session());
    if (OB_ISNULL(session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cast basic session to sql session info failed", K(ret));
    } else if (session->use_static_typing_engine()) {
      for (int64_t i = 0; i < cond_type_count; ++i) {
        const ObObjType cond_type = types_stack[i].get_type();
        const ObObjTypeClass cond_tc = ob_obj_type_class(cond_type);
        if (ObIntTC == cond_tc || ObUIntTC == cond_tc || ObNumberTC == cond_tc || ObNullTC == cond_tc) {
          types_stack[i].set_calc_type(cond_type);
          types_stack[i].set_calc_collation(types_stack[i]);
        } else {
          types_stack[i].set_calc_type(ObDoubleType);
          types_stack[i].set_calc_collation_type(CS_TYPE_BINARY);
          types_stack[i].set_calc_collation_level(CS_LEVEL_NUMERIC);
        }
      }

      bool is_expr_integer_type = (ob_is_int_tc(type.get_type()) || ob_is_uint_tc(type.get_type()));
      for (int64_t i = cond_type_count; OB_SUCC(ret) && i < param_num; ++i) {
        bool is_arg_integer_type =
            (ob_is_int_tc(types_stack[i].get_type()) || ob_is_uint_tc(types_stack[i].get_type()));
        if ((is_arg_integer_type && is_expr_integer_type) || ObNullType == types_stack[i].get_type()) {
          // see ObExprCoalesce::calc_result_typeN
          types_stack[i].set_calc_meta(types_stack[i].get_obj_meta());
        } else {
          types_stack[i].set_calc_meta(type.get_obj_meta());
        }
      }
    }
  }
  return ret;
}

int ObExprCase::calc_resultN(ObObj& result, const ObObj* objs_stack, int64_t param_num, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr_ctx.calc_buf is null", K(expr_ctx.calc_buf_));
  } else {
    ret = calc(result, objs_stack, param_num, result_type_, expr_ctx);
  }
  return ret;
}

// Case expr support parameter lazy evaluation, param_eval() must called
// before access object in %objs_stack,
int ObExprCase::calc(ObObj& result, const ObObj* objs_stack, int64_t param_num, const ObExprResType& expected_type,
    ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  bool match_when = false;
  ObObj tmp_result;
  int64_t i = 0;
  if (OB_ISNULL(objs_stack)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null stack", K(objs_stack));
  } else if (OB_UNLIKELY(param_num < 2)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("no enough param", K(param_num));
  } else {
    const bool has_else = (param_num % 2 != 0);
    int64_t loop = (has_else) ? param_num - 1 : param_num;
    double double_v = 0;
    bool bool_v = false;
    EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
    for (i = 0; OB_SUCC(ret) && !match_when && i < loop; i += 2) {
      if (OB_FAIL(param_eval(expr_ctx, objs_stack[i], i))) {
        LOG_WARN("parameter evaluate failed", K(ret), K(i));
      } else {
        ObObjType type = objs_stack[i].get_type();
        if (ObNullType != type) {
          if (ob_is_int_tc(type)) {
            bool_v = (objs_stack[i].get_int() != 0);
          } else if (ob_is_uint_tc(type) || ob_is_enumset_tc(type)) {
            bool_v = (objs_stack[i].get_uint64() != 0);
          } else if (ob_is_number_tc(type)) {
            bool_v = !(objs_stack[i].get_number().is_zero());
          } else {
            EXPR_GET_DOUBLE_V2(objs_stack[i], double_v);
            if (OB_FAIL(ret)) {
              LOG_WARN("fail cast obj to tinyint", K(objs_stack[i]), K(ret));
            } else {
              bool_v = (double_v >= DOUBLE_TRUE_VALUE_THRESHOLD || double_v <= -DOUBLE_TRUE_VALUE_THRESHOLD);
            }
          }
          if (bool_v) {  // no need to test ret here
            if (OB_FAIL(param_eval(expr_ctx, objs_stack[i + 1], i + 1))) {
              LOG_WARN("parameter evaluate failed", K(ret), K(i + 1));
            } else {
              tmp_result = objs_stack[i + 1];
              match_when = true;
              LOG_DEBUG("match", K(tmp_result));
            }
          }
        }
      }
    }  // end for
    if (OB_SUCC(ret)) {
      if (!match_when) {
        if (has_else) {
          if (OB_FAIL(param_eval(expr_ctx, objs_stack[param_num - 1], param_num - 1))) {
            LOG_WARN("parameter evaluate failed", K(ret), K(param_num - 1));
          } else {
            tmp_result = objs_stack[param_num - 1];  // match else
          }
        } else {
          tmp_result.set_null();
        }
      }
      if (OB_SUCC(ret)) {
        if (tmp_result.is_null()) {
          result.set_null();
        } else {
          if (ob_is_string_or_lob_type(result_type_.get_type())) {
            cast_ctx.dest_collation_ = result_type_.get_collation_type();
          }
          if (OB_FAIL(ObObjCaster::to_type(expected_type.get_type(), cast_ctx, tmp_result, result))) {
            LOG_WARN("fail cast obj", K(expected_type.get_type()), K(tmp_result), K(ret));
          }
        }
      }
    }  // end set result
  }
  return ret;
}

int ObExprCase::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(op_cg_ctx);

  const ObCaseOpRawExpr& case_expr = dynamic_cast<const ObCaseOpRawExpr&>(raw_expr);
  // when expr must return int/null because it must be bool expr.
  for (int64_t i = 0; OB_SUCC(ret) && i < case_expr.get_when_expr_size(); ++i) {
    const ObRawExpr* when_expr = case_expr.get_when_param_expr(i);
    const ObObjType& when_expr_res_type = when_expr->get_result_type().get_type();
    if (OB_UNLIKELY(ObNullType != when_expr_res_type && !ob_is_integer_type(when_expr_res_type))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("when expr must return integer", K(ret), K(when_expr_res_type));
    }
  }

  if (OB_SUCC(ret)) {
    rt_expr.eval_func_ = calc_case_expr;
  }
  return ret;
}

static int check_is_match(const ObDatum& when_datum, bool& match_when)
{
  int ret = OB_SUCCESS;
  if (when_datum.is_null()) {
    match_when = false;
  } else {
    int64_t v = when_datum.get_int();
    match_when = (v != 0) ? true : false;
  }
  return ret;
}

int ObExprCase::calc_case_expr(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  const bool has_else = (expr.arg_cnt_ % 2 != 0);
  int64_t loop = (has_else) ? expr.arg_cnt_ - 1 : expr.arg_cnt_;
  bool match_when = false;
  ObDatum* when_datum = NULL;
  ObDatum* then_datum = NULL;
  bool has_result = false;
  int64_t expr_idx = 0;
  for (; OB_SUCC(ret) && !match_when && expr_idx < loop; expr_idx += 2) {
    if (OB_FAIL(expr.args_[expr_idx]->eval(ctx, when_datum))) {
      LOG_WARN("eval when expr failed", K(ret), K(expr_idx));
    } else if (OB_FAIL(check_is_match(*when_datum, match_when))) {
      LOG_WARN("check is when expr match failed", K(ret), K(expr_idx));
    } else if (match_when) {
      if (OB_FAIL(expr.args_[expr_idx + 1]->eval(ctx, then_datum))) {
        LOG_WARN("eval then expr failed", K(ret), K(expr_idx + 1));
      } else {
        has_result = true;
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (!match_when) {
      if (has_else) {
        if (OB_FAIL(expr.args_[expr.arg_cnt_ - 1]->eval(ctx, then_datum))) {
          LOG_WARN("eval else expr failed for case when", K(ret));
        } else {
          has_result = true;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (!has_result) {
      res_datum.set_null();
    } else {
      if (OB_ISNULL(then_datum)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("then_datum is NULL", K(ret));
      } else {
        res_datum.set_datum(*then_datum);
      }
    }
  }
  return ret;
}

int ObExprCase::is_same_kind_type_for_case(const ObIArray<ObExprResType>& type_arr)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret)) {
    bool match = false;
    int64_t first_not_null_idx = OB_INVALID_ID;
    for (int64_t i = 0; OB_SUCC(ret) && OB_INVALID_ID == first_not_null_idx && i < type_arr.count(); ++i) {
      if (!ob_is_null(type_arr.at(i).get_type())) {
        first_not_null_idx = i;
      }
    }
    first_not_null_idx = OB_INVALID_ID == first_not_null_idx ? 0 : first_not_null_idx;
    const ObExprResType& res_type = type_arr.at(first_not_null_idx);
    for (int64_t i = first_not_null_idx + 1; OB_SUCC(ret) && i < type_arr.count(); ++i) {
      if (OB_FAIL(ObExprOperator::is_same_kind_type_for_case(res_type, type_arr.at(i), match))) {
        LOG_WARN("fail to judge same type", K(i), K(res_type), K(type_arr.at(i)), K(ret));
      } else if (!match) {
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
        LOG_WARN("fail to judge same type", K(i), K(res_type), K(type_arr.at(i)), K(ret));
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
