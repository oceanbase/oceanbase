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

#include "lib/oblog/ob_log.h"
#include "share/object/ob_obj_cast.h"
#include "sql/engine/expr/ob_expr_substr.h"
#include "sql/parser/ob_item_type.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase {
using namespace common;
namespace sql {

ObExprSubstr::ObExprSubstr(ObIAllocator& alloc) : ObStringExprOperator(alloc, T_FUN_SYS_SUBSTR, N_SUBSTR, TWO_OR_THREE)
{}

ObExprSubstr::~ObExprSubstr()
{}

int ObExprSubstr::calc_result_length(
    ObExprResType* types_array, int64_t param_num, ObCollationType cs_type, int64_t& res_len) const
{
  int ret = OB_SUCCESS;
  ObString str_text;
  int64_t start_pos = 1;
  int64_t result_len = types_array[0].get_length();
  int64_t substr_len = result_len;
  const bool is_oracle_mode = share::is_oracle_mode();
  ObExprCtx expr_ctx;
  ObArenaAllocator allocator(common::ObModIds::OB_SQL_EXPR_CALC);
  expr_ctx.calc_buf_ = &allocator;
  res_len = result_len;
  if (OB_UNLIKELY(2 != param_num && 3 != param_num)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("substr should have two or three arguments", K(param_num), K(ret));
  } else {
    const ObObj start_obj = types_array[1].get_param();
    if (!start_obj.is_null()) {
      if (is_oracle_mode) {
        if (OB_FAIL(ObExprUtil::get_trunc_int64(start_obj, expr_ctx, start_pos))) {
          ret = OB_SUCCESS;
          LOG_WARN("ignore failure when calc result type length oracle mode", K(ret));
        }
        if (0 == start_pos) {
          start_pos = 1;
        }
      } else if (start_obj.is_int()) {
        start_pos = start_obj.get_int();
      }
    }
    if (OB_SUCC(ret) && param_num == 3 && !types_array[2].get_param().is_null()) {
      const ObObj len_obj = types_array[2].get_param();
      if (is_oracle_mode && OB_FAIL(ObExprUtil::get_trunc_int64(len_obj, expr_ctx, substr_len))) {
        ret = OB_SUCCESS;
        LOG_WARN("ignore failure when calc result type length oracle mode", K(ret));
      } else if (!is_oracle_mode && len_obj.is_int()) {
        substr_len = len_obj.get_int();
      }
    }
  }
  if (OB_SUCC(ret)) {
    LOG_DEBUG("substr calc len", K(result_len), K(substr_len), K(start_pos), K(types_array[0].get_param()));
    if (0 >= result_len || 0 >= substr_len || start_pos > result_len) {
      res_len = 0;
    } else {
      const ObObj text_obj = types_array[0].get_param();
      if (ob_is_string_type(text_obj.get_type()) && OB_SUCC(text_obj.get_string(str_text))) {
        int64_t mb_len = ObCharset::strlen_char(cs_type, str_text.ptr(), str_text.length());
        start_pos = (start_pos >= 0) ? start_pos - 1 : start_pos + mb_len;
        if (OB_UNLIKELY(start_pos < 0 || start_pos >= mb_len)) {
          res_len = 0;
        } else {
          res_len = min(substr_len, mb_len - start_pos);
          int64_t offset = ObCharset::charpos(cs_type, str_text.ptr(), str_text.length(), start_pos);
          res_len = ObCharset::charpos(cs_type,
              str_text.ptr() + offset,
              (offset == 0) ? str_text.length() : str_text.length() - offset + 1,
              res_len);
        }
      } else {
        int64_t mbmaxlen = 0;
        if (OB_FAIL(ObCharset::get_mbmaxlen_by_coll(cs_type, mbmaxlen))) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "fail to get mbmaxlen", K(ret), K(cs_type));
        } else if (0 == mbmaxlen) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(ERROR, "mbmaxlen can not be 0", K(ret));
        } else {
          if (LS_CHAR == types_array[0].get_length_semantics()) {
            mbmaxlen = 1;
          }
          if (start_pos > 0 && substr_len > 0) {
            if (start_pos + substr_len <= result_len + 1) {
              if (is_oracle_mode) {
                res_len = substr_len * mbmaxlen;
              } else {
                res_len = substr_len;
              }
            } else {
              if (is_oracle_mode) {
                res_len = (result_len - start_pos + 1) * mbmaxlen;
              } else {
                res_len = result_len - start_pos + 1;
              }
            }
          } else if (is_oracle_mode) {
            res_len *= mbmaxlen;
          }
          if (types_array[0].is_lob() && res_len > OB_MAX_LONGTEXT_LENGTH / mbmaxlen) {
            res_len = OB_MAX_LONGTEXT_LENGTH / mbmaxlen;
          }
        }
      }
    }
  }
  return ret;
}

int ObExprSubstr::calc_result_typeN(
    ObExprResType& type, ObExprResType* types_array, int64_t param_num, ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  CK(NULL != type_ctx.get_session());
  CK(2 == param_num || 3 == param_num);

  if (lib::is_oracle_mode()) {
    bool prefer_varchar = true;
    auto str_params = make_const_carray(&types_array[0]);
    OZ(aggregate_string_type_and_charset_oracle(*type_ctx.get_session(), str_params, type, prefer_varchar));
    OZ(deduce_string_param_calc_type_and_charset(*type_ctx.get_session(), type, str_params));

    for (int i = 1; i < param_num; i++) {
      types_array[i].set_calc_type(ObNumberType);
      types_array[i].set_calc_scale(NUMBER_SCALE_UNKNOWN_YET);
    }
  } else {
    type.set_varchar();
    OZ(aggregate_charsets_for_string_result(type, types_array, 1, type_ctx.get_coll_type()));
    types_array[0].set_calc_type(type.get_type());
    types_array[0].set_calc_collation_level(type.get_calc_collation_level());
    types_array[0].set_calc_collation_type(type.get_collation_type());

    if (OB_SUCC(ret)) {
      if (type_ctx.get_session()->use_static_typing_engine()) {
        for (int i = 1; i < param_num; i++) {
          types_array[i].set_calc_type(ObIntType);
        }
      } else {
        for (int i = 1; i < param_num; i++) {
          types_array[i].set_calc_type(types_array[i].get_type());
        }
      }
    }

    // Set cast mode for integer parameters, truncate string to integer.
    // see: ObExprSubstr::cast_param_type_for_mysql
    type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_STRING_INTEGER_TRUNC);
  }

  // deduce max length.
  int64_t len = types_array[0].get_length();
  OZ(calc_result_length(types_array, param_num, type.get_collation_type(), len));
  CK(len <= INT32_MAX);
  if (OB_SUCC(ret)) {
    type.set_length(static_cast<ObLength>(len));
  }
  return ret;
}

// to make "select substr('abcd', '1.9')" compatible with mysql
int ObExprSubstr::cast_param_type_for_mysql(const ObObj& in, ObExprCtx& expr_ctx, ObObj& out) const
{
  int ret = OB_SUCCESS;
  ObCastMode cast_mode = CM_NONE;
  EXPR_DEFINE_CAST_CTX(expr_ctx, cast_mode);
  LOG_DEBUG("ObExprSubstr cast_param_type_for_mysql in.get_type(): ", K(in.get_type()));
  // select substr('abcd', '1.9'); MySQL truncate'1.9' to 1
  // select substr('abcd', 1.9), MySQL round 1.9 to 2
  if (ObVarcharType == in.get_type()) {
    int64_t tmp = 0;
    if (OB_FAIL(ObExprUtil::get_trunc_int64(in, expr_ctx, tmp))) {
      LOG_WARN("ObExprSubstr get_trunc_int64 failed", K(in.get_type()));
    } else if (INT_MAX < tmp) {
      out.set_int(INT_MAX);
    } else if (INT_MIN > tmp) {
      out.set_int(INT_MIN);
    } else {
      out.set_int(static_cast<int>(tmp));
    }
  } else if (OB_FAIL(ObObjCaster::to_type(ObIntType, cast_ctx, in, out))) {
    LOG_WARN("ObExprSubstr to_type failed", K(in.get_type()));
  }
  return ret;
}

int ObExprSubstr::calc_result2_for_mysql(
    ObObj& result, const ObObj& text, const ObObj& start_pos, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  ObObj trunced_start_pos;
  if (OB_FAIL(cast_param_type_for_mysql(start_pos, expr_ctx, trunced_start_pos))) {
    LOG_WARN("ObExprSubstr cast_param_type_for_mysql failed", K(start_pos.get_type()));
  } else {
    ObObj length;
    length.set_int(text.get_string().length() - trunced_start_pos.get_int() + 1);
    ret = calc_result3_for_mysql(result, text, trunced_start_pos, length, expr_ctx);
  }
  return ret;
}

int ObExprSubstr::calc_result2_for_oracle(
    ObObj& result, const ObObj& text, const ObObj& start_pos, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  bool is_clob = text.is_clob() ? true : false;
  ObCollationType cs_type = result_type_.get_collation_type();
  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    LOG_WARN("varchar buffer not init");
    ret = OB_NOT_INIT;
  } else if (text.is_null() || start_pos.is_null()) {
    result.set_null();
  } else {
    TYPE_CHECK(start_pos, ObNumberType);
    const ObString& str_val = text.get_varchar();
    int64_t start_pos_val = 0;
    LOG_DEBUG("ObExprSubstr", K(ret), K(str_val), K(start_pos));
    if (OB_FAIL(ObExprUtil::get_trunc_int64(start_pos, expr_ctx, start_pos_val))) {
      LOG_WARN("get int value failed", K(ret));
    } else if (OB_FAIL(calc(
                   result, str_val, start_pos_val, text.get_string().length() - start_pos_val + 1, cs_type, is_clob))) {
      LOG_WARN("failed to calc for substr", K(str_val), K(start_pos), K(ret));
    } else {
      if (!result.is_null()) {
        result.set_meta_type(result_type_);
      }
    }
  }
  return ret;
}

int ObExprSubstr::calc_result2(ObObj& result, const ObObj& text, const ObObj& start_pos, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (share::is_oracle_mode()) {
    ret = calc_result2_for_oracle(result, text, start_pos, expr_ctx);
  } else {
    ret = calc_result2_for_mysql(result, text, start_pos, expr_ctx);
  }
  return ret;
}

int ObExprSubstr::calc_result3_for_mysql(
    ObObj& result, const ObObj& text, const ObObj& start_pos, const ObObj& length, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  ObObj trunced_start_pos;
  ObObj trunced_length;
  ObCollationType cs_type = result_type_.get_collation_type();
  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    LOG_WARN("varchar buffer not init");
    ret = OB_NOT_INIT;
  } else if (text.is_null() || start_pos.is_null() || length.is_null()) {
    result.set_null();
  } else {
    if (OB_FAIL(cast_param_type_for_mysql(start_pos, expr_ctx, trunced_start_pos))) {
      LOG_WARN("ObExprSubstr cast_param_type_for_mysql failed", K(start_pos.get_type()));
    } else if (OB_FAIL(cast_param_type_for_mysql(length, expr_ctx, trunced_length))) {
      LOG_WARN("ObExprSubstr cast_param_type_for_mysql failed", K(length.get_type()));
    } else {
      TYPE_CHECK(text, ObVarcharType);
      TYPE_CHECK(trunced_start_pos, ObIntType);
      TYPE_CHECK(trunced_length, ObIntType);
      const ObString& str_val = text.get_varchar();
      int64_t start_pos_val = trunced_start_pos.get_int();
      int64_t length_val = trunced_length.get_int();
      if (OB_FAIL(calc(result, str_val, start_pos_val, length_val, cs_type, false))) {
        LOG_WARN("failed to calc for substr", K(text), K(trunced_start_pos), K(trunced_length), K(ret));
      } else {
        if (!result.is_null()) {
          result.set_collation(result_type_);
        }
      }
    }
  }
  return ret;
}

int ObExprSubstr::calc_result3_for_oracle(
    ObObj& result, const ObObj& text, const ObObj& start_pos, const ObObj& length, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  bool is_clob = text.is_clob() ? true : false;
  ObCollationType cs_type = result_type_.get_collation_type();

  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    LOG_WARN("varchar buffer not init");
    ret = OB_NOT_INIT;
  } else if (text.is_null() || start_pos.is_null() || length.is_null()) {
    result.set_null();
  } else {
    TYPE_CHECK(start_pos, ObNumberType);
    TYPE_CHECK(length, ObNumberType);
    const ObString& str_val = text.get_varchar();
    int64_t start_pos_val = 0;
    int64_t length_val = 0;
    LOG_DEBUG("ObExprSubstr", K(share::is_oracle_mode()), K(ret), K(str_val), K(start_pos), K(length));
    if (OB_FAIL(ObExprUtil::get_trunc_int64(start_pos, expr_ctx, start_pos_val)) ||
        ObExprUtil::get_trunc_int64(length, expr_ctx, length_val)) {
      LOG_WARN("get int value failed", K(ret), K(start_pos), K(length));
    } else if (OB_FAIL(calc(result, str_val, start_pos_val, length_val, cs_type, is_clob))) {
      LOG_WARN("failed to calc for substr", K(text), K(start_pos), K(length), K(ret));
    } else {
      if (!result.is_null()) {
        result.set_collation(result_type_);
      }
    }
  }
  return ret;
}

int ObExprSubstr::calc_result3(
    ObObj& result, const ObObj& text, const ObObj& start_pos, const ObObj& length, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (share::is_oracle_mode()) {
    ret = calc_result3_for_oracle(result, text, start_pos, length, expr_ctx);
  } else {
    ret = calc_result3_for_mysql(result, text, start_pos, length, expr_ctx);
  }
  return ret;
}

int ObExprSubstr::substr(common::ObString& varchar, const common::ObString& text, const int64_t start_pos,
    const int64_t length, common::ObCollationType cs_type)
{
  int ret = OB_SUCCESS;
  varchar = text;
  if (OB_UNLIKELY(0 >= varchar.length() || 0 >= length)) {
    // empty result string
    varchar.assign(NULL, 0);
  } else {
    int64_t start = start_pos;
    int64_t res_len = 0;
    int64_t mb_len = ObCharset::strlen_char(cs_type, varchar.ptr(), varchar.length());
    if (share::is_oracle_mode() && 0 == start) {
      start = 1;
    }
    start = (start >= 0) ? start - 1 : start + mb_len;
    if (OB_UNLIKELY(start < 0 || start >= mb_len)) {
      varchar.assign(NULL, 0);
    } else {
      // It holds that 0<=start<mb_len && length > 0
      res_len = min(length, mb_len - start);
      int64_t offset = ObCharset::charpos(cs_type, varchar.ptr(), varchar.length(), start);
      res_len = ObCharset::charpos(
          cs_type, varchar.ptr() + offset, (offset == 0) ? varchar.length() : varchar.length() - offset + 1, res_len);
      varchar.assign_ptr(varchar.ptr() + offset, static_cast<int32_t>(res_len));
    }
  }
  return ret;
}

int ObExprSubstr::calc(ObObj& result, const ObString& text, const int64_t start_pos, const int64_t length,
    ObCollationType cs_type, const bool is_clob)
{
  int ret = OB_SUCCESS;
  ObString varchar;
  if (OB_FAIL(substr(varchar, text, start_pos, length, cs_type))) {
    LOG_WARN("get substr failed", K(ret));
  } else {
    if (varchar.length() <= 0 && lib::is_oracle_mode() && !is_clob) {
      result.set_null();
    } else {
      if (is_clob) {
        result.set_lob_value(ObLongTextType, varchar.ptr(), varchar.length());
      } else {
        result.set_varchar(varchar);
      }
    }
  }
  return ret;
}

int ObExprSubstr::calc_resultN(ObObj& result, const ObObj* objs_array, int64_t param_num, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (2 == param_num) {
    ret = calc_result2(result, objs_array[0], objs_array[1], expr_ctx);
  } else if (3 == param_num) {
    ret = calc_result3(result, objs_array[0], objs_array[1], objs_array[2], expr_ctx);
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument number for substr()", K(param_num), K(ret));
  }
  return ret;
}

int ObExprSubstr::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  CK(2 == rt_expr.arg_cnt_ || 3 == rt_expr.arg_cnt_);
  if (OB_SUCC(ret)) {
    rt_expr.eval_func_ = eval_substr;
  }
  return ret;
}

int ObExprSubstr::eval_substr(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.eval_param_value(ctx))) {
    LOG_WARN("evaluate parameters failed", K(ret));
  } else {
    ObDatum* str_datum = &expr.locate_param_datum(ctx, 0);
    ObDatum* pos_datum = &expr.locate_param_datum(ctx, 1);
    ObDatum* len_datum = NULL;
    if (expr.arg_cnt_ > 2) {
      len_datum = &expr.locate_param_datum(ctx, 2);
    }
    if (str_datum->is_null() || pos_datum->is_null() || (NULL != len_datum && len_datum->is_null())) {
      expr_datum.set_null();
    } else {
      ObString input = str_datum->get_string();
      int64_t pos = 0;
      int64_t len = input.length();
      if (lib::is_oracle_mode()) {
        if (OB_FAIL(ObExprUtil::trunc_num2int64(*pos_datum, pos)) ||
            (NULL != len_datum && OB_FAIL(ObExprUtil::trunc_num2int64(*len_datum, len)))) {
          LOG_WARN("get integer value failed", K(ret));
        }
      } else {
        pos = pos_datum->get_int();
        len = NULL == len_datum ? len : len_datum->get_int();
      }
      ObString output;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(substr(output, input, pos, len, expr.datum_meta_.cs_type_))) {
        LOG_WARN("get substr failed", K(ret));
      } else {
        if (OB_UNLIKELY(output.length() <= 0) && lib::is_oracle_mode() && !expr.args_[0]->datum_meta_.is_clob()) {
          expr_datum.set_null();
        } else {
          expr_datum.set_string(output);
        }
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
