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

#include "sql/engine/expr/ob_expr_space.h"

#include "lib/utility/ob_macro_utils.h"

#include "sql/engine/expr/ob_expr_repeat.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "ob_expr_util.h"

namespace oceanbase
{
namespace sql
{
using namespace common;
ObExprSpace::ObExprSpace(ObIAllocator &alloc)
  : ObStringExprOperator(alloc, T_FUN_SYS_SPACE, N_SPACE, 1, VALID_FOR_GENERATED_COL) {}

inline int ObExprSpace::calc_result_type1(
    ObExprResType &type,
    ObExprResType &type1,
    ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  ObRawExpr * raw_expr = nullptr;
  ObRawExpr * child_raw_expr = nullptr;
  // space is mysql only expr
  CK(lib::is_mysql_mode());
  ObObjType res_type = ObMaxType;
  if (type1.is_null() || (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_2_0_0)) {
    res_type = ObVarcharType;
  } else if (type1.is_literal()) {
    if (OB_FAIL(calc_result_type(type_ctx, type1.get_param(), res_type))) {
      LOG_WARN("calc_result_type fail", K(ret), K(type1), K(type_ctx));
    }
  } else if (OB_ISNULL(raw_expr = type_ctx.get_raw_expr())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("raw_expr is null", K(ret), K(type_ctx));
  } else if (OB_ISNULL(child_raw_expr = raw_expr->get_param_expr(0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get child expr fail", K(ret), KPC(raw_expr), K(type_ctx));
  } else if (T_FUN_SYS_CAST == child_raw_expr->get_expr_type()
        && child_raw_expr->has_flag(IS_OP_OPERAND_IMPLICIT_CAST)
        && child_raw_expr->get_param_expr(0)->get_result_type().is_literal()) {
    // some scenarios may have multiple type inferences
    // if the input is a string literal, the first inference will add an implicit cast to convert a string to an integer
    // this makes the input type not literal in the second type inference, resulting in an inference to longtext
    // this in turn causes multiple implicit cast to be added to the same expr
    // so need to make a special judgment here
    // eg :
    //      create table space_t1(c1 char(181) default (space( '1')));
    //      select c1 from space_t1;
    if (child_raw_expr->get_param_expr(0)->get_result_type().is_null()) {
      res_type = ObVarcharType;
    } else if (OB_FAIL(calc_result_type(type_ctx, child_raw_expr->get_param_expr(0)->get_result_type().get_param(), res_type))) {
      LOG_WARN("calc_result_type fail", K(ret), K(type1), K(type_ctx));
    }
  } else {
    res_type = ObLongTextType;
  }
  type.set_type(res_type);
  type.set_collation_level(type1.get_collation_level());
  type.set_collation_type(get_default_collation_type(type.get_type(), type_ctx));
  if (ObVarcharType == type.get_type()) {
    if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_2_0_0) {
      type.set_length(OB_MAX_VARCHAR_LENGTH);
    } else {
      type.set_length(MAX_CHAR_LENGTH_FOR_VARCAHR_RESULT);
    }
  } else if (ob_is_text_tc(type.get_type())) {
    const int32_t mbmaxlen = 4;
    const int32_t default_text_length =
      ObAccuracy::DDL_DEFAULT_ACCURACY[type.get_type()].get_length() / mbmaxlen;
    // need to set a correct length for text tc in mysql mode
    type.set_length(default_text_length);
  }
  type1.set_calc_type(ObIntType);
  // Set cast mode for parameter casting, truncate string to integer.
  type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_STRING_INTEGER_TRUNC);
  return ret;
}

int ObExprSpace::calc_result_type(ObExprTypeCtx &type_ctx, const ObObj &obj, ObObjType &res_type) const
{
  int ret = OB_SUCCESS;
  ObArenaAllocator alloc(ObModIds::OB_SQL_RES_TYPE);
  const ObDataTypeCastParams dtc_params = type_ctx.get_dtc_params();
  int64_t cur_time = 0;
  ObCastMode cast_mode = CM_NONE;
  // void return function
  ObSQLUtils::get_default_cast_mode(type_ctx.get_sql_mode(), cast_mode);
  cast_mode |= CM_WARN_ON_FAIL;
  ObCastCtx cast_ctx(
      &alloc, &dtc_params, cur_time, cast_mode, CS_TYPE_INVALID);
  int64_t count_val = 0;
  EXPR_GET_INT64_V2(obj, count_val);
  if (OB_SUCC(ret)) {
    res_type = get_result_type_mysql(count_val);
  }
  return ret;
}

int ObExprSpace::cg_expr(ObExprCGCtx &, const ObRawExpr &, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  CK(1 == rt_expr.arg_cnt_);
  rt_expr.eval_func_ = eval_space;
  return ret;
}

int ObExprSpace::eval_space(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *count = NULL;
  int64_t max_size = 0;
  bool is_null = false;
  ObString output;
  ObString space(1, " ");
  ObExprStrResAlloc expr_res_alloc(expr, ctx);
  bool has_lob_header = expr.obj_meta_.has_lob_header();
  ObSolidifiedVarsGetter helper(expr, ctx, ctx.exec_ctx_.get_my_session());
  if (OB_ISNULL(ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null session", K(ret));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, count))) {
    LOG_WARN("evaluate parameters failed", K(ret));
  } else if (count->is_null()) {
    expr_datum.set_null();
  } else if (OB_FAIL(helper.get_max_allowed_packet(max_size))) {
    LOG_WARN("get max packet length failed", K(ret));
  } else if (count->get_int() > max_size) {
    LOG_WARN("Result of space was larger than max_allow_packet_size",
             K(count->get_int()), K(max_size));
    LOG_USER_WARN(OB_ERR_FUNC_RESULT_TOO_LARGE, "space", static_cast<int>(max_size));
    expr_datum.set_null();
  } else {
    if (!ob_is_text_tc(expr.datum_meta_.type_)) {
      // TODO:@huangweixiang.hwx(from @xiaofeng.lby)
      // 1. If we do not need to consider the character set, changing repeat to memset may be better
      // 2. Actually, we need to deal with the character set.
      ret = ObExprRepeat::repeat(output, is_null, space, count->get_int(), expr_res_alloc, max_size);
    } else { // text tc
      ret = ObExprRepeat::repeat_text(expr.datum_meta_.type_, has_lob_header, output, is_null,
                                      space, count->get_int(), expr_res_alloc, max_size);
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("do repeat failed", K(ret));
    } else {
      if (is_null) {
        expr_datum.set_null();
      } else {
        expr_datum.set_string(output);
      }
    }
  }
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprSpace, raw_expr) {
  int ret = OB_SUCCESS;
  if (lib::is_mysql_mode()) {
    SET_LOCAL_SYSVAR_CAPACITY(4);
    EXPR_ADD_LOCAL_SYSVAR(SYS_VAR_COLLATION_CONNECTION);
    EXPR_ADD_LOCAL_SYSVAR(SYS_VAR_SQL_MODE);
    EXPR_ADD_LOCAL_SYSVAR(SYS_VAR_TIME_ZONE);
    EXPR_ADD_LOCAL_SYSVAR(SYS_VAR_MAX_ALLOWED_PACKET);
  }
  return ret;
}

} // sql
} // oceanbase
