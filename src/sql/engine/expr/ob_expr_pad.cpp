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

#include "sql/engine/expr/ob_expr_pad.h"
#include "sql/engine/expr/ob_expr_lrpad.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

ObExprPad::ObExprPad(ObIAllocator &alloc)
    : ObStringExprOperator(alloc, T_FUN_PAD, N_PAD, 3, VALID_FOR_GENERATED_COL)
{
  need_charset_convert_ = false;
}

ObExprPad::~ObExprPad()
{
}

int ObExprPad::calc_result_type3(ObExprResType &type,
                                 ObExprResType &source,
                                 ObExprResType &padding_str,
                                 ObExprResType &length,
                                 common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  ObObjType text_type = ObNullType;
  ObObjType len_type = ObNullType;
  int64_t max_len = -1;

  len_type = ObIntType;
  text_type = source.is_nstring() ? ObNVarchar2Type : ObVarcharType;
  max_len = OB_MAX_VARCHAR_LENGTH;

  CK (OB_NOT_NULL(type_ctx.get_session()));

  if (OB_SUCC(ret)) {
    const common::ObLengthSemantics default_length_semantics = (OB_NOT_NULL(type_ctx.get_session())
            ? type_ctx.get_session()->get_actual_nls_length_semantics()
            : common::LS_BYTE);
    type.set_type(text_type);
    type.set_length(static_cast<ObLength>(max_len));
    if (source.is_nstring()) {
      type.set_length_semantics(LS_CHAR);
    } else {
      type.set_length_semantics(padding_str.is_varchar_or_char() ?  padding_str.get_length_semantics() : default_length_semantics);
    }
    source.set_calc_type(text_type);
    length.set_calc_type(len_type);
    padding_str.set_calc_type(text_type);

    if (is_oracle_mode()) {
      //for oracle mode, this funcion is only used for padding char/nchar types
      const ObSQLSessionInfo *session = type_ctx.get_session();
      if (OB_ISNULL(session)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("session is NULL", K(ret));
      } else {
        ObSEArray<ObExprResType*, 2, ObNullAllocator> types;
        OZ(types.push_back(&padding_str));
        OZ(aggregate_string_type_and_charset_oracle(*session, types, type));
        OZ(types.push_back(&source));
        OZ(deduce_string_param_calc_type_and_charset(*session, type, types));
      }
    } else {
      ObSEArray<ObExprResType, 2> types;
      if (OB_FAIL(types.push_back(source))) {
        LOG_WARN("failed to push back source type", K(ret));
      } else if (OB_FAIL(types.push_back(padding_str))) {
        LOG_WARN("failed to push back padding source type", K(ret));
      } else if (OB_FAIL(aggregate_charsets_for_string_result(
              type, &types.at(0), 2, type_ctx.get_coll_type()))) {
        LOG_WARN("failed to set collation", K(ret));
      } else {
        source.set_calc_collation_type(type.get_collation_type());
        padding_str.set_calc_collation_type(type.get_collation_type());
      }
    }
  }
  LOG_DEBUG("varify calc meta", K(type), K(source.get_calc_meta()), K(padding_str.get_calc_meta()));
  return ret;
}

int ObExprPad::calc_pad_expr(const ObExpr &expr, ObEvalCtx &ctx,
                                          ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *src = NULL;
  ObDatum *pad = NULL;
  ObDatum *len = NULL;
  if (OB_UNLIKELY(3 != expr.arg_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg cnt, must be 3", K(ret), K(expr.arg_cnt_));
  } else if (OB_FAIL(expr.eval_param_value(ctx, src, pad, len))) {
    LOG_WARN("eval param value failed", K(ret));
  } else if (src->is_null() || pad->is_null() || len->is_null()) {
    res.set_null();
  } else {
    int64_t len_int = len->get_int();
    const ObString src_str = src->get_string();
    int64_t byte_delta = 0;
    int64_t src_char_len = ObCharset::strlen_char(expr.datum_meta_.cs_type_,
                                                  src_str.ptr(), src_str.length());
    if (is_oracle_byte_length(lib::is_oracle_mode(), expr.datum_meta_.length_semantics_)) {
      byte_delta = src_str.length() - src_char_len;
      len_int -= byte_delta;
    }

    if (lib::is_oracle_mode() && 1 == expr.extra_) {
      //for column convert char/nchar padding, the padded length should be less than max length of target types
      const int64_t max_len = expr.is_called_in_sql_ ? OB_MAX_ORACLE_CHAR_LENGTH_BYTE : OB_MAX_ORACLE_PL_CHAR_LENGTH_BYTE;
      int64_t min_mb_len = 0;
      if (OB_FAIL(ObCharset::get_mbminlen_by_coll(expr.datum_meta_.cs_type_, min_mb_len))) {
        LOG_WARN("fail to get min mb len", K(ret));
      } else {
        int64_t max_pad_chars = (max_len - src_str.length())/min_mb_len;
        len_int = std::min(len_int, max_pad_chars +  src_char_len);
      }
    }

    if (OB_SUCC(ret) && src_char_len < len_int) {
      ObDatum len_char;
      len_char.ptr_ = reinterpret_cast<const char*>(&len_int);
      len_char.pack_ = sizeof(len_int);
      const ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
      ObExprStrResAlloc res_alloc(expr, ctx); // make sure alloc() is called only once
      if (OB_ISNULL(session)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("session is NULL", K(ret));
      } else if (OB_FAIL(ObExprBaseLRpad::calc_mysql(ObExprBaseLRpad::RPAD_TYPE, expr, ctx,
                            *src, len_char, *pad, *session, res_alloc, res))) {
        LOG_WARN("calc_mysql failed", K(ret));
      }
    } else {
      res.set_datum(*src);
    }
  }
  return ret;
}

int ObExprPad::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_pad_expr;
  rt_expr.extra_ = raw_expr.get_extra();
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprPad, raw_expr) {
  int ret = OB_SUCCESS;
  SET_LOCAL_SYSVAR_CAPACITY(2);
  EXPR_ADD_LOCAL_SYSVAR(SYS_VAR_MAX_ALLOWED_PACKET);
  EXPR_ADD_LOCAL_SYSVAR(SYS_VAR_COLLATION_CONNECTION);
  return ret;
}

}
}
