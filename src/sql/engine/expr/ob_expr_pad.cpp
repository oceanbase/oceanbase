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

namespace oceanbase {
namespace sql {

ObExprPad::ObExprPad(ObIAllocator& alloc) : ObStringExprOperator(alloc, T_FUN_PAD, N_PAD, 3)
{
  need_charset_convert_ = false;
}

ObExprPad::~ObExprPad()
{}

int ObExprPad::calc_result_type3(ObExprResType& type, ObExprResType& source, ObExprResType& padding_str,
    ObExprResType& length, common::ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  ObObjType text_type = ObNullType;
  ObObjType len_type = ObNullType;
  int64_t max_len = -1;

  len_type = ObIntType;
  text_type = source.is_nstring() ? ObNVarchar2Type : ObVarcharType;
  max_len = OB_MAX_VARCHAR_LENGTH;

  CK(OB_NOT_NULL(type_ctx.get_session()));

  if (OB_SUCC(ret)) {
    const common::ObLengthSemantics default_length_semantics =
        (OB_NOT_NULL(type_ctx.get_session()) ? type_ctx.get_session()->get_actual_nls_length_semantics()
                                             : common::LS_BYTE);
    type.set_type(text_type);
    type.set_length(static_cast<ObLength>(max_len));
    if (source.is_nstring()) {
      type.set_length_semantics(LS_CHAR);
    } else {
      type.set_length_semantics(
          padding_str.is_varchar_or_char() ? padding_str.get_length_semantics() : default_length_semantics);
    }
    source.set_calc_type(text_type);
    length.set_calc_type(len_type);
    padding_str.set_calc_type(text_type);

    if (is_oracle_mode()) {
      const ObSQLSessionInfo* session = type_ctx.get_session();
      if (OB_ISNULL(session)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("session is NULL", K(ret));
      } else {
        ObSEArray<ObExprResType*, 2, ObNullAllocator> types;
        OZ(types.push_back(&source));
        OZ(types.push_back(&padding_str));
        OZ(aggregate_string_type_and_charset_oracle(*session, types, type));
        OZ(deduce_string_param_calc_type_and_charset(*session, type, types));
      }
    } else {
      ObSEArray<ObExprResType, 2> types;
      if (OB_FAIL(types.push_back(source))) {
        LOG_WARN("failed to push back source type", K(ret));
      } else if (OB_FAIL(types.push_back(padding_str))) {
        LOG_WARN("failed to push back padding source type", K(ret));
      } else if (OB_FAIL(aggregate_charsets_for_string_result(type, &types.at(0), 2, type_ctx.get_coll_type()))) {
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

int ObExprPad::calc_result3(
    ObObj& result, const ObObj& source, const ObObj& padding_str, const ObObj& length, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (source.is_null() || padding_str.is_null() || length.is_null()) {
    result.set_null();
  } else {
    TYPE_CHECK(length, ObIntType);

    int64_t local_len = length.get_int();
    ObString local_str_text = source.get_string();
    int64_t byte_delta = 0;
    int64_t source_length = ObCharset::strlen_char(
        result_type_.get_collation_type(), const_cast<const char*>(local_str_text.ptr()), local_str_text.length());

    if (is_oracle_byte_length(share::is_oracle_mode(), result_type_.get_length_semantics())) {
      byte_delta = local_str_text.length() - source_length;
      local_len -= byte_delta;
    }
    if (source_length < local_len) {
      ObObj target_length;
      target_length.set_int(local_len);
      if (OB_FAIL(ObExprBaseLRpad::calc_mysql(ObExprBaseLRpad::RPAD_TYPE,
              result_type_,
              source,
              target_length,
              padding_str,
              expr_ctx.my_session_,
              expr_ctx.calc_buf_,
              result))) {
        SQL_ENG_LOG(WARN,
            "fail to calc result",
            K(ret),
            K(result_type_),
            K(source),
            K(length),
            K(padding_str),
            K(expr_ctx.my_session_),
            K(expr_ctx.calc_buf_));
      } else {
        result.set_collation_level(result_type_.get_collation_level());
        result.set_collation_type(result_type_.get_collation_type());
        SQL_ENG_LOG(DEBUG,
            "calc padding",
            K(result),
            K(local_str_text),
            K(local_str_text.length()),
            K(local_len),
            K(source_length),
            K(byte_delta),
            K(result_type_));
      }
    } else {
      result = source;
      result.set_collation_type(result_type_.get_collation_type());
    }
  }
  return ret;
}

int ObExprPad::calc_pad_expr(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res)
{
  int ret = OB_SUCCESS;
  ObDatum* src = NULL;
  ObDatum* pad = NULL;
  ObDatum* len = NULL;
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
    int64_t src_char_len = ObCharset::strlen_char(expr.datum_meta_.cs_type_, src_str.ptr(), src_str.length());
    if (is_oracle_byte_length(share::is_oracle_mode(), expr.datum_meta_.length_semantics_)) {
      byte_delta = src_str.length() - src_char_len;
      len_int -= byte_delta;
    }
    if (src_char_len < len_int) {
      ObDatum len_char;
      len_char.ptr_ = reinterpret_cast<const char*>(&len_int);
      len_char.pack_ = sizeof(len_int);
      const ObSQLSessionInfo* session = ctx.exec_ctx_.get_my_session();
      ObExprStrResAlloc res_alloc(expr, ctx);  // make sure alloc() is called only once
      if (OB_ISNULL(session)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("session is NULL", K(ret));
      } else if (OB_FAIL(ObExprBaseLRpad::calc_mysql(
                     ObExprBaseLRpad::RPAD_TYPE, expr, *src, len_char, *pad, *session, res_alloc, res))) {
        LOG_WARN("calc_mysql failed", K(ret));
      }
    } else {
      res.set_datum(*src);
    }
  }
  return ret;
}

int ObExprPad::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_pad_expr;
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
