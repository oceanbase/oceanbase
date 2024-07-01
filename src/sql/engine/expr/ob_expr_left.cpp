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
#include "sql/engine/expr/ob_expr_left.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "lib/charset/ob_charset.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

ObExprLeft::ObExprLeft(ObIAllocator &alloc)
: ObStringExprOperator(alloc, T_FUN_SYS_LEFT, "left", 2, VALID_FOR_GENERATED_COL)
{
}

ObExprLeft::~ObExprLeft()
{
}

// to make "select left('abcd', '1.9')" compatible with mysql
int ObExprLeft::cast_param_type(const ObObj& in,
                                ObExprCtx& expr_ctx,
                                ObObj& out) const
{
  int ret = OB_SUCCESS;
  ObCastMode cast_mode = CM_NONE;
  EXPR_DEFINE_CAST_CTX(expr_ctx, cast_mode);
  // select left('abcd', '1.9'); -- MySQL will trunc '1.9' to 2
  // select left('abcd', 1.9); -- MySQL will round 1.9 to 1
  if (ObVarcharType == in.get_type()) {
    int64_t tmp = 0;
    if (OB_FAIL(ObExprUtil::get_trunc_int64(in, expr_ctx, tmp))) {
      LOG_WARN("ObExprLeft get_trunc_int64 failed", K(in.get_type()));
    } else if (INT_MAX < tmp) {
      out.set_int(INT_MAX);
    } else if (INT_MIN > tmp) {
      out.set_int(INT_MIN);
    } else {
      out.set_int(static_cast<int>(tmp));
    }
  } else if (OB_FAIL(ObObjCaster::to_type(ObIntType, cast_ctx, in, out))) {
    LOG_WARN("ObExprLeft to_type failed", K(in.get_type()));
  }
  return ret;
}

int ObExprLeft::calc_result_type2(ObExprResType &type,
																	ObExprResType &type1,
																	ObExprResType &type2,
																	ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = const_cast<ObSQLSessionInfo *>(type_ctx.get_session());
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else if (session->is_varparams_sql_prepare()) {
    // the ps prepare stage does not do type deduction, and directly gives a default type.
    type.set_char();
    type.set_default_collation_type();
    type.set_collation_level(CS_LEVEL_IMPLICIT);
    type.set_length(0);
  } else {
    type2.set_calc_type(ObIntType);
    type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_STRING_INTEGER_TRUNC);
    type.set_varchar();
    int64_t result_len = type1.get_length();
    const ObObj &result_len_obj = type2.get_param();
    ObObj int_obj;
    ObExprCtx tmp_expr_ctx;
    ObArenaAllocator allocator(common::ObModIds::OB_SQL_EXPR_CALC);
    tmp_expr_ctx.calc_buf_ = &allocator;
    tmp_expr_ctx.cast_mode_ = type_ctx.get_cast_mode();
    if (OB_FAIL(cast_param_type(result_len_obj, tmp_expr_ctx, int_obj))) {
      LOG_WARN("cast_param_type failed", K(result_len_obj.get_type()));
    } else if (int_obj.get_int() <= 0) {
      type.set_char();
      type.set_length(0);
    } else {
      type.set_length(int_obj.get_int());
    }

    OZ(aggregate_charsets_for_string_result(type, &type1, 1, type_ctx.get_coll_type()));
    OX(type1.set_calc_type(ObVarcharType));
    OX(type1.set_calc_collation_type(type.get_collation_type()));
  }
  return ret;
}

int calc_left(ObString &res_str, const ObString &text, const ObCollationType type,
							const int64_t required_num_char)
{
	int ret = OB_SUCCESS;
	const char* str_ptr = text.ptr();
	int64_t str_length = text.length();
	if(OB_ISNULL(str_ptr) && 0 != str_length) {
		ret = OB_INVALID_ARGUMENT;
		LOG_WARN("invalid argument", K(ret));
	} else {
		int64_t input_num_char = ObCharset::strlen_char(type, text.ptr(), text.length());
		int64_t expected_num_char = min(required_num_char, input_num_char);
		int64_t start_pos = 0;
		int64_t char_length = 0;
		while((start_pos < str_length) && OB_SUCC(ret) && (0 < expected_num_char)) {
			if(OB_FAIL(ObCharset::first_valid_char(type, str_ptr + start_pos,
                                             str_length - start_pos, char_length))) {
				LOG_WARN("get char failed", K(ret));
			} else {
				start_pos += char_length;
				--expected_num_char;
			}
		}
		if(OB_SUCC(ret)) {
      res_str.assign_ptr(str_ptr, static_cast<int32_t> (start_pos));
		}
	}
	return ret;
}

int calc_left_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  // left(s, n)
  ObDatum *s_datum = NULL;
  ObDatum *n_datum = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, s_datum)) ||
      OB_FAIL(expr.args_[1]->eval(ctx, n_datum))) {
    LOG_WARN("eval arg failed", K(ret), KP(s_datum), KP(n_datum));
  } else if (s_datum->is_null() || n_datum->is_null()) {
    res_datum.set_null();
  } else {
    // res_str会指向s_datum的内存空间，所以下面不能改变res_str指向的字符串
    ObString res_str;
    const ObCollationType arg_cs_type = expr.args_[0]->datum_meta_.cs_type_;
    if (OB_FAIL(calc_left(res_str, s_datum->get_string(), arg_cs_type, n_datum->get_int()))) {
      LOG_WARN("failed to calculate left expression", K(ret));
    } else {
      if (res_str.empty() && is_oracle_mode()) {
        res_datum.set_null();
      } else {
        res_datum.set_string(res_str);
      }
    }
  }
  return ret;
}


int ObExprLeft::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                            ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_left_expr;
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprLeft, raw_expr) {
  int ret = OB_SUCCESS;
  SET_LOCAL_SYSVAR_CAPACITY(1);
  EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_COLLATION_CONNECTION);
  return ret;
}

} /* namespace sql */
} /* namespace oceanbase */
