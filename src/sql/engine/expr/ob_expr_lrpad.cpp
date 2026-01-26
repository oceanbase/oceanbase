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

#include "sql/engine/expr/ob_expr_lrpad.h"
#include "sql/engine/ob_exec_context.h"

#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "lib/charset/ob_charset_string_helper.h"

namespace oceanbase
{
using namespace oceanbase::common;
using namespace oceanbase::sql;
namespace sql
{
/* ObExprBaseLRpad {{{1 */
/* common util {{{2 */
ObExprBaseLRpad::ObExprBaseLRpad(ObIAllocator &alloc,
                                 ObExprOperatorType type,
                                 const char *name,
                                 int32_t param_num)
    : ObStringExprOperator(alloc, type, name, param_num, VALID_FOR_GENERATED_COL)
{
}

ObExprBaseLRpad::~ObExprBaseLRpad()
{
}

//参考ObExprBaseLRpad::calc_mysql
int ObExprBaseLRpad::calc_type_length_mysql(const ObExprResType result_type,
                                            const ObObj &text,
                                            const ObObj &pad_text,
                                            const ObObj &len,
                                            const ObExprTypeCtx &type_ctx,
                                            int64_t &result_size)
{
  int ret = OB_SUCCESS;
  int64_t max_result_size = OB_MAX_VARCHAR_LENGTH;
  int64_t int_len = 0;
  int64_t repeat_count = 0;
  int64_t prefix_size = 0;
  int64_t text_len = 0;

  ObString str_text;
  ObString str_pad;
  result_size = max_result_size;
  ObExprCtx expr_ctx;
  ObArenaAllocator allocator(common::ObModIds::OB_SQL_EXPR_CALC);
  expr_ctx.calc_buf_ = &allocator;
  max_result_size = type_ctx.get_max_allowed_packet();
  if (OB_FAIL(ret)) {
    //do nothing
  } else if (OB_FAIL(ObExprUtil::get_round_int64(len, expr_ctx, int_len))) {
    LOG_WARN("get_round_int64 failed and ignored", K(ret));
    ret = OB_SUCCESS;
  } else if (int_len < 0) {
    // 当长度参数为负数时，结果长度应该为0，因为RPAD/LPAD会返回NULL
    result_size = 0;
  } else {
    if (!ob_is_string_type(text.get_type())) {
      result_size = int_len;
    } else if (OB_FAIL(text.get_string(str_text))) {
      LOG_WARN("Failed to get str_text", K(ret), K(text));
    } else if (FALSE_IT(text_len = ObCharset::strlen_char(
                result_type.get_collation_type(),
                const_cast<const char *>(str_text.ptr()),
                str_text.length()))) {
      LOG_WARN("Failed to get displayed length", K(ret), K(str_text));
    } else if (text_len >= int_len) {
      // only substr needed
      result_size = ObCharset::charpos(result_type.get_collation_type(), str_text.ptr(), str_text.length(), int_len);
    } else {
      if (!ob_is_string_type(pad_text.get_type())) {
        result_size = int_len;
      } else if (OB_FAIL(pad_text.get_string(str_pad))) {
        LOG_WARN("Failed to get str_text", K(ret), K(pad_text));
      } else if (str_pad.length() == 0) {
        result_size = int_len;
      } else if (OB_FAIL(get_padding_info_mysql(
                result_type.get_collation_type(), str_text, text_len, int_len, str_pad,
                max_result_size, repeat_count, prefix_size, result_size))) {
        LOG_WARN("Failed to get padding info", K(ret), K(str_text), K(int_len), K(str_pad), K(max_result_size));
      } else {
        result_size = str_text.length() + str_pad.length() * repeat_count + prefix_size;
      }
    }
  }
  if (result_size > max_result_size) {
    result_size = max_result_size;
  }
  return ret;
}

//参考ObExprBaseLRpad::calc_oracle
int ObExprBaseLRpad::calc_type_length_oracle(const ObExprResType &result_type,
                                             const ObObj &text,
                                             const ObObj &pad_text,
                                             const ObObj &len,
                                             int64_t &result_size)
{
  int ret = OB_SUCCESS;
  const int64_t max_result_size = result_type.is_lob() ?
                                  OB_MAX_LONGTEXT_LENGTH : OB_MAX_ORACLE_VARCHAR_LENGTH;
  int64_t width = 0;
  int64_t repeat_count = 0;
  int64_t prefix_size = 0;
  bool pad_space = false;
  int64_t text_width = 0;
  int64_t mbmaxlen = 0;
  ObString str_text;
  ObString str_pad;
  ObString space_str = ObCharsetUtils::get_const_str(result_type.get_collation_type(), ' ');

  ObExprCtx expr_ctx;
  ObArenaAllocator allocator(common::ObModIds::OB_SQL_EXPR_CALC);
  expr_ctx.calc_buf_ = &allocator;
  result_size = max_result_size;
  /* get length */
  if (OB_FAIL(ObExprUtil::get_trunc_int64(len, expr_ctx, width))) {
    LOG_WARN("get_trunc_int64 failed and ignored", K(ret));
    ret = OB_SUCCESS;
  } else if (width < 0) {
    // 当长度参数为负数时，结果长度应该为0，因为RPAD/LPAD会返回NULL
    result_size = 0;
  } else {
    // both_const_str 为 true 表示 rpad(a, count, b) 中的 a 和 b 均为常量
    bool both_const_str = false;
    // 当text 或 pad_text 为常量时，text 和 pad_text 才不为空
    // 据此可以判断输入是否为常量
    OX (both_const_str = ob_is_string_type(text.get_type())
                         && ob_is_string_type(pad_text.get_type()));
    OZ (ObCharset::get_mbmaxlen_by_coll(result_type.get_collation_type(), mbmaxlen));
    if (OB_FAIL(ret)) {
    } else if (false == both_const_str) {
      // 当两个参数中任意一个不是常量时，Oracle 会简单地将字符数乘以mbmaxlen作为字节数
      result_size = LS_CHAR == result_type.get_length_semantics() ? width : width * mbmaxlen;
    } else {
      if (OB_FAIL(text.get_string(str_text))) {
        LOG_WARN("Failed to get str_text", K(ret), K(text));
      } else if (OB_FAIL(ObCharset::display_len(result_type.get_collation_type(),
                                                str_text, text_width))) {
        LOG_WARN("Failed to get displayed length", K(ret), K(str_text));
      } else if (text_width == width) {
        result_size = width;
      } else if (text_width > width) {
        // substr
        int64_t total_width = 0;
        pad_space = true;
        if (OB_FAIL(ObCharset::max_display_width_charpos(
          result_type.get_collation_type(), str_text.ptr(), str_text.length(),
          width, prefix_size, &total_width))) {
        } else {
          pad_space = (total_width != width);
          result_size = prefix_size + (pad_space ? space_str.length() : 0);
        }
      } else if (OB_FAIL(pad_text.get_string(str_pad))) {
        LOG_WARN("Failed to get str_text", K(ret), K(pad_text));
      } else if (OB_FAIL(get_padding_info_oracle(
        result_type.get_collation_type(), str_text, width, str_pad,
        max_result_size, repeat_count, prefix_size, pad_space))) {
        LOG_WARN("Failed to get padding info", K(ret), K(str_text),
                 K(width), K(str_pad), K(max_result_size));
      } else {
        ObCollationType cs_type = result_type.get_collation_type();
        if (LS_CHAR != result_type.get_length_semantics()) {
          result_size = str_text.length()
            + str_pad.length() * repeat_count
            + prefix_size + (pad_space ? space_str.length() : 0);
        } else {
          result_size = ObCharset::strlen_char(cs_type, str_text.ptr(), str_text.length())
            + ObCharset::strlen_char(cs_type, str_pad.ptr(), str_pad.length()) * repeat_count
            + prefix_size + (pad_space ? space_str.length() : 0);
        }
      }
    }
  }
  if (result_size > max_result_size) {
    result_size = max_result_size;
  }
  return ret;
}

int ObExprBaseLRpad::get_origin_len_obj(ObObj &len_obj) const
{
  int ret = OB_SUCCESS;
  ObRawExpr *expr = NULL;
  if (OB_ISNULL(expr = get_raw_expr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get_raw_expr", K(ret));
  } else if (expr->get_param_count() >= 2 && OB_NOT_NULL(expr = expr->get_param_expr(1))
             && expr->get_expr_type() == T_FUN_SYS_CAST && CM_IS_IMPLICIT_CAST(expr->get_cast_mode())) {
    do {
      if (expr->get_param_count() >= 1
          && OB_ISNULL(expr = expr->get_param_expr(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get_param_expr", K(ret));
      }
    } while (OB_SUCC(ret) && T_FUN_SYS_CAST == expr->get_expr_type()
             && CM_IS_IMPLICIT_CAST(expr->get_cast_mode()));
    if (OB_FAIL(ret)) {
    } else if (!expr->is_const_raw_expr()) {
      len_obj.set_null();
    } else {
      len_obj = static_cast<ObConstRawExpr *>(expr)->get_param();
    }
  }
  return ret;
}

int ObExprBaseLRpad::calc_type(ObExprResType &type,
                               ObExprResType &text,
                               ObExprResType &len,
                               ObExprResType *pad_text,
                               ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  ObObjType text_type = ObNullType;
  ObObjType len_type = ObNullType;
  const bool is_oracle_mode = lib::is_oracle_mode();
  int64_t max_len = OB_MAX_VARCHAR_LENGTH;
  int64_t text_len = text.get_length();
  if (is_oracle_mode) {
    len_type = ObNumberType;

    if (text.is_nstring()) {
      text_type = ObNVarchar2Type;
      max_len = OB_MAX_ORACLE_VARCHAR_LENGTH;
    } else if (!text.is_lob()) {
      text_type = ObVarcharType;
      max_len = OB_MAX_ORACLE_VARCHAR_LENGTH;
    } else if (text.is_blob()) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "Blob type in LRpad");
      LOG_WARN("Blob type in LRpad not supported", K(ret),  K(text.get_type()));
    } else {
      text_type = ObLongTextType;
      max_len = OB_MAX_LONGTEXT_LENGTH;
    }
  } else if (lib::is_mysql_mode()) {
    len_type = ObIntType;
    text_type = ObVarcharType;
    max_len = OB_MAX_VARCHAR_LENGTH;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error compat mode", K(ret));
  }

  const ObSQLSessionInfo *session = type_ctx.get_session();
  CK(OB_NOT_NULL(session));

  if (OB_SUCC(ret)) {
    ObObj length_obj = len.get_param();
    ObObj text_obj = text.get_param();
    ObObj pad_obj;
    ObString default_pad_str = ObString(" "); //默认为' '
    type.set_length(static_cast<ObLength>(max_len));
    len.set_calc_type(len_type);
    if (is_mysql_mode()) {
      pad_obj = pad_text->get_param();
      ObSEArray<ObExprResType, 2> types;
      OZ(types.push_back(text));
      OZ(types.push_back(*pad_text));
      OZ(aggregate_charsets_for_string_result(type, &types.at(0), 2, type_ctx));
      OX(text.set_calc_collation_type(type.get_collation_type()));
      OX(pad_text->set_calc_collation_type(type.get_collation_type()));
      if (OB_SUCC(ret)) {
        // len expr may add cast, search real len obj
        if (OB_FAIL(get_origin_len_obj(length_obj))) {
          LOG_WARN("fail to get ori len obj", K(ret));
        } else if (!length_obj.is_null()) {
          if (OB_FAIL(calc_type_length_mysql(type, text_obj, pad_obj, length_obj, type_ctx, text_len))) {
            LOG_WARN("failed to calc result type length mysql mode", K(ret));
          }
        } else {
          text_len = max_len;
        }
        if (OB_SUCC(ret)) {
          text_type = get_result_type_mysql(text_len);
          type.set_type(text_type);
          if (!ob_is_text_tc(text.get_type())) {
            text.set_calc_type(text_type);
          }
          if (!ob_is_text_tc(pad_text->get_type())) {
            pad_text->set_calc_type(text_type);
          }
        }
      }
    } else {
      ObSEArray<ObExprResType*, 2> types;
      OZ(types.push_back(&text));
      OZ(aggregate_string_type_and_charset_oracle(*session, types, type,
          PREFER_VAR_LEN_CHAR | PREFER_NLS_LENGTH_SEMANTICS));
      if (NULL != pad_text) {
        OZ(types.push_back(pad_text));
        OX(pad_obj = pad_text->get_param());
      } else {
        OX(pad_obj.set_string(ObVarcharType, default_pad_str));
        OX(pad_obj.set_collation_type(ObCharset::get_system_collation()));
      }
      OZ(deduce_string_param_calc_type_and_charset(*session, type, types));
    }

    const int64_t buf_len = pad_obj.is_character_type() ? pad_obj.get_string_len() * 4 : 0;
    char buf[buf_len];
    if (OB_SUCC(ret) && lib::is_oracle_mode()
        && !pad_obj.is_null_oracle() && pad_obj.is_character_type()) {
      ObDataBuffer data_buf(buf, buf_len);
      if (OB_FAIL(convert_result_collation(type, pad_obj, &data_buf))) {
        LOG_WARN("fail to convert result_collation", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (!length_obj.is_null()) {
        if (is_oracle_mode && OB_FAIL(calc_type_length_oracle(type, text_obj, pad_obj, length_obj, text_len))) {
          LOG_WARN("failed to calc result type length oracle mode", K(ret));
        }
      } else {
        text_len = max_len;
      }
      text_len = (text_len > max_len)? max_len: text_len;
      type.set_length(static_cast<ObLength>(text_len));

      LOG_DEBUG("ObExprBaseLRpad::calc_type()", K(ret), K(text), K(text_obj), K(pad_obj), K(len), K(length_obj), KP(pad_text),
                                K(type), K(text_len), K(max_len), K(type.get_length_semantics()));
    }
  }
  return ret;
}

int ObExprBaseLRpad::padding_inner(LRpadType type,
                                   const char *text,
                                   const int64_t &text_size,
                                   const char *pad,
                                   const int64_t &pad_size,
                                   const int64_t &prefix_size,
                                   const int64_t &repeat_count,
                                   const bool &pad_space,
                                   ObString &space_str,
                                   char* &result)
{
  int ret = OB_SUCCESS;
  char *text_start_pos = NULL;
  char *pad_start_pos = NULL;
  char *sp_start_pos = NULL;
  if (OB_ISNULL(result)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to alloc", K(ret));
  } else if (type == LPAD_TYPE) {
    // lpad: [sp] + padtext * t + padprefix + text
    if (pad_space) {
      sp_start_pos = result;
      pad_start_pos = result + space_str.length();
    } else {
      pad_start_pos = result;
    }
    text_start_pos = pad_start_pos + (pad_size * repeat_count + prefix_size);
  } else if (type == RPAD_TYPE) {
    // rpad: text + padtext * t + padprefix + [sp]
    text_start_pos = result;
    pad_start_pos = text_start_pos + text_size;
    if (pad_space) {
      sp_start_pos = pad_start_pos + (pad_size * repeat_count + prefix_size);
    }
  }

  if (OB_SUCC(ret)) {
    // place pad string
    for (int64_t i = 0; i < repeat_count; i++) {
      MEMCPY(pad_start_pos + i * pad_size, pad, pad_size);
    }

    // place pad string prefix
    MEMCPY(pad_start_pos + repeat_count * pad_size, pad, prefix_size);

    // place text string
    MEMCPY(text_start_pos, text, text_size);
    if (pad_space) {
      MEMCPY(sp_start_pos, space_str.ptr(), space_str.length());
    }
  }
  return ret;
}

int ObExprBaseLRpad::padding(LRpadType type,
                             const ObCollationType coll_type,
                             const char *text,
                             const int64_t &text_size,
                             const char *pad,
                             const int64_t &pad_size,
                             const int64_t &prefix_size,
                             const int64_t &repeat_count,
                             const bool &pad_space, // for oracle
                             ObIAllocator *allocator,
                             char* &result,
                             int64_t &size,
                             ObObjType res_type,
                             bool has_lob_header)
{
  int ret = OB_SUCCESS;
  ObString space_str = ObCharsetUtils::get_const_str(coll_type, ' ');
  // start pos
  char *text_start_pos = NULL;
  char *pad_start_pos = NULL;
  char *sp_start_pos = NULL;

  size = text_size + pad_size * repeat_count + prefix_size + (pad_space? space_str.length(): 0);
  if (OB_UNLIKELY(size <= 0)
      || OB_UNLIKELY(repeat_count < 0)
      || OB_UNLIKELY(pad_size <= 0)
      || OB_UNLIKELY(prefix_size >= pad_size)
      || (OB_ISNULL(text) && text_size != 0)
      || OB_ISNULL(pad)
      || OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Wrong param", K(ret), K(size), K(repeat_count), K(pad_size), K(prefix_size),
             K(text), K(text_size), K(pad), K(allocator));
  } else {
    if (!ob_is_text_tc(res_type)) {
      result = static_cast<char *>(allocator->alloc(size));
      // result validation inside padding_inner
      ret = padding_inner(type, text, text_size, pad, pad_size, prefix_size,
                          repeat_count, pad_space, space_str, result);
    } else { // res is text tc
      ObTextStringResult result_buffer(res_type, has_lob_header, allocator);
      int64_t buffer_len = 0;
      if (OB_FAIL(result_buffer.init(size))) {
        LOG_WARN("init stringtextbuffer failed", K(ret), K(size));
      } else if (OB_FAIL(result_buffer.get_reserved_buffer(result, buffer_len))) {
        LOG_WARN("get empty buffer len failed", K(ret), K(buffer_len));
      } else if (OB_FAIL(padding_inner(type, text, text_size, pad, pad_size, prefix_size,
                                       repeat_count, pad_space, space_str, result))) {
      } else if (OB_FAIL(result_buffer.lseek(buffer_len, 0))) {
        LOG_WARN("temp lob lseek failed", K(ret));
      } else {
        ObString output;
        result_buffer.get_result_buffer(output);
        result = output.ptr();
        size = output.length();
      }
    }
  }
  return ret;
}

int ObExprBaseLRpad::get_padding_info_mysql(const ObCollationType &cs,
                                            const ObString &str_text,
                                            const int64_t text_len,
                                            const int64_t &len,
                                            const ObString &str_padtext,
                                            const int64_t max_result_size,
                                            int64_t &repeat_count,
                                            int64_t &prefix_size,
                                            int64_t &size)
{
  // lpad: [sp] + padtext * t + padprefix + text
  // rpad: text + padtext * t + padprefix + [sp]
  int ret = OB_SUCCESS;
  int64_t text_size = str_text.length();
  int64_t pad_size = str_padtext.length();

  // GOAL: get repeat_count, prefix_size and pad space.
  int64_t pad_len = ObCharset::strlen_char(cs, const_cast<const char *>(str_padtext.ptr()), str_padtext.length());

  if (OB_UNLIKELY(len <= text_len)
             || OB_UNLIKELY(len <= 0)
             || OB_UNLIKELY(pad_len <= 0)
             || OB_UNLIKELY(pad_size <= 0)) {
    // this should been resolve outside
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wrong len", K(ret), K(len), K(text_len), K(pad_len), K(pad_size));
  } else {
    repeat_count = std::min((len - text_len) / pad_len, (max_result_size - text_size) / pad_size);
    int64_t remain_len = len - (text_len + pad_len * repeat_count);
    prefix_size = ObCharset::charpos(cs, const_cast<const char *>(str_padtext.ptr()),
                                     str_padtext.length(), remain_len);

    size = text_size + pad_size * repeat_count + prefix_size;
  }
  return ret;
}

// for engine 3.0
int ObExprBaseLRpad::calc_mysql_pad_expr(const ObExpr &expr, ObEvalCtx &ctx,
                                           LRpadType pad_type, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *text = NULL;
  ObDatum *len = NULL;
  ObDatum *pad_text = NULL;
  if (OB_UNLIKELY(3 != expr.arg_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("arg cnt must be 3", K(ret), K(expr.arg_cnt_));
  } else if (OB_FAIL(expr.eval_param_value(ctx, text, len, pad_text))) {
    LOG_WARN("eval param value failed", K(ret));
  } else if (OB_ISNULL(text) || OB_ISNULL(len) || OB_ISNULL(pad_text)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected datum", K(ret), KP(text), KP(len), KP(pad_text));
  } else {
    const ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
    ObExprStrResAlloc res_alloc(expr, ctx); // make sure alloc() is called only once
    if (OB_ISNULL(session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session is NULL", K(ret));
    } else if (OB_FAIL(calc_mysql(pad_type, expr, ctx, *text, *len, *pad_text, *session,
                                  res_alloc, res))) {
      LOG_WARN("calc_mysql failed", K(ret));
    }
  }
  return ret;
}

int ObExprBaseLRpad::calc_mysql_inner(const LRpadType pad_type,
                                      const ObExpr &expr,
                                      const ObDatum &len,
                                      int64_t &max_result_size,
                                      const ObString &str_text,
                                      const ObString &str_pad,
                                      ObIAllocator &res_alloc,
                                      ObDatum &res)
{
  int ret = OB_SUCCESS;
  int64_t repeat_count = 0;
  int64_t prefix_size = 0;
  int64_t text_len = 0;

  char *result_ptr = NULL;
  int64_t result_size = 0;
  const ObCollationType cs_type = expr.datum_meta_.cs_type_;
  const ObObjType type = expr.datum_meta_.type_;
  bool has_lob_header = expr.obj_meta_.has_lob_header();
  bool has_set_to_lob_locator = false;
  int64_t int_len = len.get_int();
  if (int_len < 0) {
    res.set_null();
  } else if (int_len == 0) {
    res.set_string(ObString::make_empty_string());
  } else if (FALSE_IT(text_len = ObCharset::strlen_char(cs_type,
             const_cast<const char *>(str_text.ptr()), str_text.length()))) {
    LOG_WARN("Failed to get displayed length", K(ret), K(str_text));
  } else if (text_len >= int_len ) {
    // only substr needed
    result_size = ObCharset::charpos(cs_type, str_text.ptr(), str_text.length(), int_len);
    res.set_string(ObString(result_size, str_text.ptr()));
  } else if (str_pad.length() == 0) {
    res.set_null(); // mysql 5.7 return null while mysql 8.0 return empty string
  } else {
    has_set_to_lob_locator = true;
    if (OB_FAIL(get_padding_info_mysql(cs_type, str_text, text_len, int_len, str_pad,
                max_result_size, repeat_count, prefix_size, result_size))) {
      LOG_WARN("Failed to get padding info", K(ret), K(str_text), K(int_len),
                                              K(str_pad), K(max_result_size));
    } else if (result_size > max_result_size) {
      res.set_null();
      if (pad_type == RPAD_TYPE) {
        LOG_USER_WARN(OB_ERR_FUNC_RESULT_TOO_LARGE, "rpad", static_cast<int>(max_result_size));
      } else {
        LOG_USER_WARN(OB_ERR_FUNC_RESULT_TOO_LARGE, "lpad", static_cast<int>(max_result_size));
      }
    } else if (OB_FAIL(padding(pad_type, cs_type, str_text.ptr(), str_text.length(), str_pad.ptr(),
                                str_pad.length(), prefix_size, repeat_count, false, &res_alloc,
                                result_ptr, result_size, type, has_lob_header))) {
      LOG_WARN("Failed to pad", K(ret), K(str_text), K(str_pad), K(prefix_size), K(repeat_count));
    } else {
      if (NULL == result_ptr || 0 == result_size) {
        res.set_null();
      } else {
        res.set_string(result_ptr, result_size);
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (ob_is_text_tc(type) && !res.is_null() &&
      has_lob_header && !has_set_to_lob_locator) {
    ObString data = res.get_string();
    ObTextStringResult result_buffer(type, has_lob_header, &res_alloc);
    int64_t buffer_len = 0;
    if (OB_FAIL(result_buffer.init(data.length()))) {
      LOG_WARN("init stringtextbuffer failed", K(ret), K(data));
    } else if (OB_FAIL(result_buffer.append(data))) {
      LOG_WARN("temp lob lseek failed", K(ret));
    } else {
      ObString output;
      result_buffer.get_result_buffer(output);
      res.set_string(output);
    }
  } else if (ob_is_text_tc(expr.args_[0]->datum_meta_.type_) && ob_is_string_tc(type)
      && ! has_set_to_lob_locator && ! res.is_null()) {
    // in mysql mode, input is lob, but output is varchar
    // if lob is outrow, may cause return data that's allocated from tmp allocator
    ObString data = res.get_string();
    ObString output;
    if (data.empty()) { // skip empty string
    } else if (OB_FAIL(ob_write_string(res_alloc, data, output))) {
      LOG_WARN("ob_write_string fail", K(ret));
    } else {
      res.set_string(output);
    }
  }
  return ret;
}

int ObExprBaseLRpad::calc_mysql(const LRpadType pad_type, const ObExpr &expr, ObEvalCtx &ctx,
                                const ObDatum &text, const ObDatum &len, const ObDatum &pad_text,
                                const ObSQLSessionInfo &session, ObIAllocator &res_alloc,
                                ObDatum &res)
{
  int ret = OB_SUCCESS;
  int64_t max_result_size = -1;
  ObSolidifiedVarsGetter helper(expr, ctx, &session);
  if (OB_FAIL(helper.get_max_allowed_packet(max_result_size))) {
    LOG_WARN("Failed to get max allow packet size", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (text.is_null() || len.is_null() || pad_text.is_null()) {
    res.set_null();
  } else {
    if (!ob_is_text_tc(expr.args_[0]->datum_meta_.type_)
        && !ob_is_text_tc(expr.args_[2]->datum_meta_.type_)) {
      const ObString &str_text = text.get_string();
      const ObString &str_pad = pad_text.get_string();
      if (OB_FAIL(calc_mysql_inner(pad_type, expr, len, max_result_size,
                                   str_text, str_pad, res_alloc, res))) {
        LOG_WARN("Failed to eval base lrpad", K(ret));
      }
    } else { // text tc
      ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
      common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
      ObString str_text;
      ObString str_pad;
      if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator,
                                                            text,
                                                            expr.args_[0]->datum_meta_,
                                                            expr.args_[0]->obj_meta_.has_lob_header(),
                                                            str_text))) {
        LOG_WARN("failed to read lob data text", K(ret), K(text));
      } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator,
                                                                   pad_text,
                                                                   expr.args_[2]->datum_meta_,
                                                                   expr.args_[2]->obj_meta_.has_lob_header(),
                                                                   str_pad))) {
        LOG_WARN("failed to read lob data pattern", K(ret), K(pad_text));
      } else if (OB_FAIL(calc_mysql_inner(pad_type, expr, len, max_result_size,
                                          str_text, str_pad, res_alloc, res))) {
        LOG_WARN("Failed to eval base lrpad", K(ret));
      }
    }
  }
  return ret;
}
/* mysql util END }}} */

// O(logN) repeat algorithm
// repeat('x', 10) is computed as follows:
//
// repeat_times = 10 in binary format is 0b1010. use
// is_odd to record the last bit of the repeat_times, and
// update repeat_times by sra (shift-right-logical) 1.
//
// initialization: copy 1-repitition to dst
// k = 0, is_odd = 0, repeat_times = 0b101;
// dst = 'x'.
//
// round 1: k = 0, is_odd = 0, repeat_times = 0b101;
// copy 2^k = 1 repetition once, because is_odd == 0;
// dst = 'xx'
//
// round 2: k = 1, is_odd = 1, repeat_times = 0b10;
// copy 2^k = 2 repetitions twice, because is_odd == 1;
// dst = 'xxxxxx'.
//
// round 3: k = 2, is_odd = 0, repeat_times = 0b1;
// copy 2^k = 4 repetitions once, because is_odd == 0;
// dst= 'xxxxxxxxxx'.
//
// round 4: k = 3, is_odd = 1, repeat_times = 0;
// when repeat_times becomes 0, the algorithm finishes
//
// k never exceeds position(from right) of the most signifcant bit of the
// repeat_times, so k = log2(repeat_times), for each round, memcpy is
// called for at most twice.  so total memcpy call count is 2log2(repeat_times),
// time complexity is O(logN), drastically decreased call count implies
// more data will be copy and give memcpy more change to speedup via SIMD optimization.
inline void fast_repeat(char* dst, const char* src, size_t src_size, int32_t repeat_times) {
  if (OB_UNLIKELY(repeat_times <= 0)) {
    return;
  }
  char* dst_begin = dst;
  char* dst_curr = dst;
  int32_t k = 0;
  int32_t is_odd = repeat_times & 1;
  repeat_times >>= 1;

  memcpy(dst_curr, src, src_size);
  dst_curr += src_size;
  for (; repeat_times > 0; k += 1, is_odd = repeat_times & 1, repeat_times >>= 1) {
    int32_t len = src_size * (1 << k);
    memcpy(dst_curr, dst_begin, len);
    dst_curr += len;
    if (is_odd) {
      memcpy(dst_curr, dst_begin, len);
      dst_curr += len;
    }
  }
}

int ObExprBaseLRpad::padding_vector(LRpadType type,
                                    const ObCollationType coll_type,
                                    const char *text,
                                    const int64_t &text_size,
                                    const char *pad,
                                    const int64_t &pad_size,
                                    const int64_t &prefix_size,
                                    const int64_t &repeat_count,
                                    const bool &pad_space, // for oracle
                                    char* result,
                                    int64_t &size)
{
  int ret = OB_SUCCESS;
  ObString space_str = ObCharsetUtils::get_const_str(coll_type, ' ');
  // start pos
  char *text_start_pos = NULL;
  char *pad_start_pos = NULL;
  char *sp_start_pos = NULL;

  size = text_size + pad_size * repeat_count + prefix_size + (pad_space? space_str.length(): 0);

  if (OB_ISNULL(result)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to alloc", K(ret));
  } else if (type == LPAD_TYPE) {
    // lpad: [sp] + padtext * t + padprefix + text
    if (pad_space) {
      sp_start_pos = result;
      pad_start_pos = result + space_str.length();
    } else {
      pad_start_pos = result;
    }
    text_start_pos = pad_start_pos + (pad_size * repeat_count + prefix_size);
  } else if (type == RPAD_TYPE) {
    // rpad: text + padtext * t + padprefix + [sp]
    text_start_pos = result;
    pad_start_pos = text_start_pos + text_size;
    if (pad_space) {
      sp_start_pos = pad_start_pos + (pad_size * repeat_count + prefix_size);
    }
  }

  if (OB_SUCC(ret)) {
    // place pad string
    fast_repeat(pad_start_pos, pad, pad_size, repeat_count);
    // place pad string prefix
    memcpy(pad_start_pos + repeat_count * pad_size, pad, prefix_size);
    // place text string
    memcpy(text_start_pos, text, text_size);
    if (pad_space) {
      MEMCPY(sp_start_pos, space_str.ptr(), space_str.length());
    }
  }

  return ret;
}

// {{{ mysql util vector

int ObExprBaseLRpad::get_padding_info_mysql_vector(const ObCollationType &cs,
                                                   const ObString &str_text,
                                                   const int64_t text_len,
                                                   const int64_t &len,
                                                   const ObString &str_padtext,
                                                   int64_t pad_len,
                                                   const int64_t max_result_size,
                                                   int64_t &repeat_count,
                                                   int64_t &prefix_size,
                                                   int64_t &size)
{
  // lpad: [sp] + padtext * t + padprefix + text
  // rpad: text + padtext * t + padprefix + [sp]
  int ret = OB_SUCCESS;
  int64_t text_size = str_text.length();
  int64_t pad_size = str_padtext.length();
  // GOAL: get repeat_count, prefix_size and pad space.
  if (pad_len < 0) {
    pad_len = ObCharsetStringHelper::fast_strlen_char<false, false>(cs, str_padtext);
  }
  repeat_count = std::min((len - text_len) / pad_len, (max_result_size - text_size) / pad_size);
  int64_t remain_len = len - (text_len + pad_len * repeat_count);
  prefix_size = ObCharsetStringHelper::fast_charpos<false, false>(cs, str_padtext, remain_len);
  size = text_size + pad_size * repeat_count + prefix_size;
  return ret;
}

int ObExprBaseLRpad::calc_mysql_pad_expr_vector(const ObExpr &expr,
                                                ObEvalCtx &ctx,
                                                const ObBitVector &skip,
                                                const EvalBound &bound,
                                                LRpadType pad_type) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.eval_vector_param_value(ctx, skip, bound))) {
    LOG_WARN("eval param value failed", K(ret));
  } else {
    const ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
    if (OB_ISNULL(session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session is NULL", K(ret));
    } else {
      if (!ob_is_text_tc(expr.datum_meta_.type_) &&
          !ob_is_text_tc(expr.args_[0]->datum_meta_.type_) &&
          !ob_is_text_tc(expr.args_[2]->datum_meta_.type_) &&
          expr.args_[1]->is_static_const_ &&
          expr.args_[2]->is_static_const_ &&
          !expr.args_[1]->get_vector(ctx)->has_null() &&
          !expr.args_[2]->get_vector(ctx)->has_null()) {
        VectorFormat res_format = expr.get_format(ctx);
        VectorFormat text_format = expr.args_[0]->get_format(ctx);
        if (VEC_DISCRETE == res_format) {
          if (VEC_DISCRETE == expr.args_[0]->get_format(ctx)) {
            ret = calc_mysql_vector_optimized<StrDiscVec, StrDiscVec>(expr, ctx, skip, bound, pad_type, *session);
          } else if (VEC_UNIFORM == expr.args_[0]->get_format(ctx)) {
            ret = calc_mysql_vector_optimized<StrDiscVec, StrUniVec>(expr, ctx, skip, bound, pad_type, *session);
          } else {
            ret = calc_mysql_vector_optimized<StrDiscVec, ObVectorBase>(expr, ctx, skip, bound, pad_type, *session);
          }
        } else if (VEC_UNIFORM == res_format) {
          if (VEC_DISCRETE == expr.args_[0]->get_format(ctx)) {
            ret = calc_mysql_vector_optimized<StrUniVec, StrDiscVec>(expr, ctx, skip, bound, pad_type, *session);
          } else if (VEC_UNIFORM == expr.args_[0]->get_format(ctx)) {
            ret = calc_mysql_vector_optimized<StrUniVec, StrUniVec>(expr, ctx, skip, bound, pad_type, *session);
          } else {
            ret = calc_mysql_vector_optimized<StrUniVec, ObVectorBase>(expr, ctx, skip, bound, pad_type, *session);
          }
        } else {
          if (VEC_DISCRETE == expr.args_[0]->get_format(ctx)) {
            ret = calc_mysql_vector_optimized<ObVectorBase, StrDiscVec>(expr, ctx, skip, bound, pad_type, *session);
          } else if (VEC_UNIFORM == expr.args_[0]->get_format(ctx)) {
            ret = calc_mysql_vector_optimized<ObVectorBase, StrUniVec>(expr, ctx, skip, bound, pad_type, *session);
          } else {
            ret = calc_mysql_vector_optimized<ObVectorBase, ObVectorBase>(expr, ctx, skip, bound, pad_type, *session);
          }
        }
      } else {
        VectorFormat res_format = expr.get_format(ctx);
        VectorFormat text_format = expr.args_[0]->get_format(ctx);
        if (VEC_DISCRETE == res_format) {
          if (VEC_DISCRETE == expr.args_[0]->get_format(ctx)) {
            ret = calc_mysql_vector<StrDiscVec, StrDiscVec, ObVectorBase, ObVectorBase>(expr, ctx, skip, bound, pad_type, *session);
          } else if (VEC_UNIFORM == expr.args_[0]->get_format(ctx)) {
            ret = calc_mysql_vector<StrDiscVec, StrUniVec, ObVectorBase, ObVectorBase>(expr, ctx, skip, bound, pad_type, *session);
          } else {
            ret = calc_mysql_vector<StrDiscVec, ObVectorBase, ObVectorBase, ObVectorBase>(expr, ctx, skip, bound, pad_type, *session);
          }
        } else if (VEC_UNIFORM == res_format) {
          if (VEC_DISCRETE == expr.args_[0]->get_format(ctx)) {
            ret = calc_mysql_vector<StrUniVec, StrDiscVec, ObVectorBase, ObVectorBase>(expr, ctx, skip, bound, pad_type, *session);
          } else if (VEC_UNIFORM == expr.args_[0]->get_format(ctx)) {
            ret = calc_mysql_vector<StrUniVec, StrUniVec, ObVectorBase, ObVectorBase>(expr, ctx, skip, bound, pad_type, *session);
          } else {
            ret = calc_mysql_vector<StrUniVec, ObVectorBase, ObVectorBase, ObVectorBase>(expr, ctx, skip, bound, pad_type, *session);
          }
        } else {
          if (VEC_DISCRETE == expr.args_[0]->get_format(ctx)) {
            ret = calc_mysql_vector<ObVectorBase, StrDiscVec, ObVectorBase, ObVectorBase>(expr, ctx, skip, bound, pad_type, *session);
          } else if (VEC_UNIFORM == expr.args_[0]->get_format(ctx)) {
            ret = calc_mysql_vector<ObVectorBase, StrUniVec, ObVectorBase, ObVectorBase>(expr, ctx, skip, bound, pad_type, *session);
          } else {
            ret = calc_mysql_vector<ObVectorBase, ObVectorBase, ObVectorBase, ObVectorBase>(expr, ctx, skip, bound, pad_type, *session);
          }
        }
      }
    }
  }
  return ret;
}

template<typename ResVec, typename TextVec, typename LenVec, typename PadVec>
int ObExprBaseLRpad::calc_mysql_vector(const ObExpr &expr,
                                       ObEvalCtx &ctx,
                                       const ObBitVector &skip,
                                       const EvalBound &bound,
                                       const LRpadType pad_type,
                                       const ObSQLSessionInfo &session) {
  int ret = OB_SUCCESS;
  int64_t max_result_size = -1;
  int64_t pad_len = -1;
  bool is_ascii = false;
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  TextVec *text_vec = static_cast<TextVec *>(expr.args_[0]->get_vector(ctx));
  LenVec *len_vec = static_cast<LenVec *>(expr.args_[1]->get_vector(ctx));
  PadVec *pad_text_vec = static_cast<PadVec *>(expr.args_[2]->get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);

  ObSolidifiedVarsGetter helper(expr, ctx, &session);
  if (OB_FAIL(helper.get_max_allowed_packet(max_result_size))) {
    LOG_WARN("Failed to get max allow packet size", K(ret));
  } else {
    is_ascii = expr.args_[0]->get_vector(ctx)->is_batch_ascii();
    if (expr.args_[2]->is_static_const_ &&
        !pad_text_vec->is_null(bound.start()) &&
        !ob_is_text_tc(expr.args_[2]->datum_meta_.type_)) {
      ObString str_pad = pad_text_vec->get_string(bound.start());
      pad_len = ObCharset::strlen_char(expr.datum_meta_.cs_type_, str_pad.ptr(), str_pad.length());
    }
  }

  for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
    if (skip.at(idx) || eval_flags.at(idx)) {
      continue;
    } else if (text_vec->is_null(idx) || len_vec->is_null(idx) || pad_text_vec->is_null(idx)) {
      res_vec->set_null(idx);
    } else {
      if (!ob_is_text_tc(expr.args_[0]->datum_meta_.type_) &&
          !ob_is_text_tc(expr.args_[2]->datum_meta_.type_)) {
        const ObString &str_text = text_vec->get_string(idx);
        const ObString &str_pad = pad_text_vec->get_string(idx);
        ret = calc_mysql_inner_vector<ResVec>(
            pad_type, expr, ctx, len_vec->get_int(idx), max_result_size, str_text,
            str_pad, pad_len, res_vec, idx, is_ascii);
      } else { // text tc
        ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
        common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
        ObString str_text;
        ObString str_pad;
        if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator,
                                                              text_vec,
                                                              expr.args_[0]->datum_meta_,
                                                              expr.args_[0]->obj_meta_.has_lob_header(),
                                                              str_text,
                                                              idx))) {
          LOG_WARN("failed to read lob data text", K(ret), K(text_vec->get_string(idx)));
        } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator,
                                                                     pad_text_vec,
                                                                     expr.args_[2]->datum_meta_,
                                                                     expr.args_[2]->obj_meta_.has_lob_header(),
                                                                     str_pad,
                                                                     idx))) {
          LOG_WARN("failed to read lob data pattern", K(ret), K(pad_text_vec->get_string(idx)));
        } else if (OB_FAIL(calc_mysql_inner_vector<ResVec>(pad_type, expr, ctx, len_vec->get_int(idx),
                max_result_size, str_text, str_pad, pad_len, res_vec, idx, is_ascii))) {
          LOG_WARN("Failed to eval base lrpad", K(ret));
        }
      }
    }
  }
  return ret;
}

template <typename ResVec>
int ObExprBaseLRpad::calc_mysql_inner_vector(const LRpadType pad_type,
                                             const ObExpr &expr,
                                             ObEvalCtx &ctx,
                                             const int64_t int_len,
                                             int64_t &max_result_size,
                                             const ObString &str_text,
                                             const ObString &str_pad,
                                             int64_t pad_len,
                                             ResVec *res_vec,
                                             const int64_t idx,
                                             bool is_ascii)
{
  int ret = OB_SUCCESS;
  int64_t repeat_count = 0;
  int64_t prefix_size = 0;
  int64_t text_len = 0;

  int64_t result_size = 0;
  const ObCollationType cs_type = expr.datum_meta_.cs_type_;
  const ObObjType type = expr.datum_meta_.type_;
  bool has_set_to_lob_locator = false;
  if (OB_UNLIKELY(int_len <= 0)) {
    if (int_len < 0) {
      res_vec->set_null(idx);
    } else if (int_len == 0) {
      res_vec->set_string(idx, ObString::make_empty_string());
    }
  } else if (FALSE_IT(text_len = ObCharsetStringHelper::fast_strlen_char(cs_type, str_text, is_ascii))) {
  } else if (text_len >= int_len ) {
    // only substr needed
    result_size = ObCharsetStringHelper::fast_charpos(cs_type, str_text, int_len, is_ascii);
    res_vec->set_string(idx, ObString(result_size, str_text.ptr()));
  } else if (OB_UNLIKELY(str_pad.length() == 0)) {
    res_vec->set_null(idx); // mysql 5.7 return null while mysql 8.0 return empty string
  } else {
    has_set_to_lob_locator = true;
    if (OB_FAIL(get_padding_info_mysql_vector(cs_type, str_text, text_len, int_len, str_pad, pad_len,
                    max_result_size, repeat_count, prefix_size, result_size))) {
      LOG_WARN("Failed to get padding info", K(ret), K(str_text), K(int_len),
                                              K(str_pad), K(max_result_size));
    } else if (OB_UNLIKELY(result_size > max_result_size)) {
      res_vec->set_null(idx);
      if (pad_type == RPAD_TYPE) {
        LOG_USER_WARN(OB_ERR_FUNC_RESULT_TOO_LARGE, "rpad", static_cast<int>(max_result_size));
      } else {
        LOG_USER_WARN(OB_ERR_FUNC_RESULT_TOO_LARGE, "lpad", static_cast<int>(max_result_size));
      }
    } else if (!ob_is_text_tc(type)) {
      char *result_buf = expr.get_str_res_mem(ctx, result_size, idx);
      if (OB_FAIL(padding_vector(pad_type, cs_type, str_text.ptr(), str_text.length(), str_pad.ptr(),
                                  str_pad.length(), prefix_size, repeat_count, false, result_buf,
                                  result_size))) {
        LOG_WARN("Failed to pad", K(ret), K(str_text), K(str_pad), K(prefix_size), K(repeat_count));
      } else {
        if (OB_UNLIKELY(0 == result_size)) {
          res_vec->set_null(idx);
        } else {
          res_vec->set_string(idx, result_buf, result_size);
        }
      }
    } else {
      char *result_buf = NULL;
      int64_t buf_len = 0;
      ObTextStringVectorResult<ResVec> output_result(type, &expr, &ctx, res_vec, idx);
      if (OB_FAIL(output_result.init_with_batch_idx(result_size, idx))) {
        LOG_WARN("init lob result failed", K(ret));
      } else if (OB_FAIL(output_result.get_reserved_buffer(result_buf, buf_len))) {
        LOG_WARN("failed to get reserved buffer", K(ret));
      } else if (OB_FAIL(padding_vector(pad_type, cs_type, str_text.ptr(), str_text.length(), str_pad.ptr(),
                                  str_pad.length(), prefix_size, repeat_count, false, result_buf,
                                  result_size))) {
        LOG_WARN("Failed to pad", K(ret), K(str_text), K(str_pad), K(prefix_size), K(repeat_count));
      } else if (OB_FAIL(output_result.lseek(buf_len, 0))) {
        LOG_WARN("temp lob lseek failed", K(ret));
      } else {
        if (OB_UNLIKELY(0 == result_size)) {
          res_vec->set_null(idx);
        } else {
          ObString output;
          output_result.get_result_buffer(output);
          res_vec->set_string(idx, output);
        }
      }
    }
  }

  if (OB_FAIL(ret) || has_set_to_lob_locator) {
  } else if (ob_is_text_tc(type) && !res_vec->is_null(idx) && expr.obj_meta_.has_lob_header()) {
    ObString data = res_vec->get_string(idx);
    ObTextStringVectorResult<ResVec> output_result(type, &expr, &ctx, res_vec, idx);
    int64_t buffer_len = 0;
    if (OB_FAIL(output_result.init_with_batch_idx(data.length(), idx))) {
      LOG_WARN("init stringtextbuffer failed", K(ret), K(data));
    } else if (OB_FAIL(output_result.append(data))) {
      LOG_WARN("temp lob lseek failed", K(ret));
    } else {
      ObString output;
      output_result.get_result_buffer(output);
      res_vec->set_string(idx, output);
    }
  } else if (ob_is_text_tc(expr.args_[0]->datum_meta_.type_) &&
             ob_is_string_tc(type) && !res_vec->is_null(idx)) {
    // in mysql mode, input is lob, but output is varchar
    // if lob is outrow, may cause return data that's allocated from tmp allocator
    ObString data = res_vec->get_string(idx);
    char *result_buf = expr.get_str_res_mem(ctx, data.length(), idx);
    if (data.empty()) { // skip empty string
    } else {
      MEMCPY(result_buf, data.ptr(), data.length());
      res_vec->set_string(idx, result_buf, data.length());
    }
  }
  return ret;
}

template <bool IsLeftPad>
inline static int padding_vector_mysql_optimized(const char *text,
                                                 const int64_t &text_size,
                                                 const char *pad_buf,
                                                 const int64_t &total_pad_size,
                                                 char *result)
{
  int ret = OB_SUCCESS;
  // start pos
  if (IsLeftPad) {
    // lpad: [sp] + padtext * t + padprefix + text
    memcpy(result, pad_buf, total_pad_size);
    memcpy(result + total_pad_size, text, text_size);
  } else if (!IsLeftPad) {
    // rpad: text + padtext * t + padprefix + [sp]
    memcpy(result, text, text_size);
    memcpy(result + text_size, pad_buf, total_pad_size);
  }
  return ret;
}

template <bool IsLeftPad>
inline static int padding_vector_oracle_optimized(const char *text,
                                                  const int64_t &text_size,
                                                  const char *pad_buf,
                                                  const int64_t &total_pad_size,
                                                  const bool &pad_space,  // for oracle
                                                  const ObString &space_str,
                                                  char *result)
{
  int ret = OB_SUCCESS;
  if (IsLeftPad) {
    // lpad: [sp] + [padtext] + [text]
    if (pad_space) {
      // [sp]
      memcpy(result, space_str.ptr(), space_str.length());
      // [padtext]
      memcpy(result + space_str.length(), pad_buf, total_pad_size);
      // [text]
      memcpy(result + space_str.length() + total_pad_size, text, text_size);
    } else {
      // [padtext]
      memcpy(result, pad_buf, total_pad_size);
      // [text]
      memcpy(result + total_pad_size, text, text_size);
    }
  } else if (!IsLeftPad) {
    // rpad: [text] + [padtext] + [sp]
    if (pad_space) {
      // [sp]
      memcpy(result + text_size + total_pad_size, space_str.ptr(), space_str.length());
    }
    // [text]
    memcpy(result, text, text_size);
    // [padtext]
    memcpy(result + text_size, pad_buf, total_pad_size);
  }
  return ret;
}

int ObExprBaseLRpad::init_pad_ctx(const ObExpr &expr,
                                 ObEvalCtx &ctx,
                                 ObExecContext &exec_ctx,
                                 uint64_t pad_id,
                                 const ObString &str_pad,
                                 int64_t length,
                                 int64_t buffer_size,
                                 bool is_mysql_mode,
                                 ObExprLRPadContext *&pad_ctx)
{
  int ret = OB_SUCCESS;
  int64_t pad_len = 0;

  if (OB_FAIL(exec_ctx.create_expr_op_ctx(pad_id, pad_ctx))) {
    LOG_WARN("failed to create operator ctx", K(ret), K(pad_id));
  } else {
    // 根据模式选择不同的 pad_len 计算方法
    if (is_mysql_mode) {
      pad_len = ObCharset::strlen_char(expr.datum_meta_.cs_type_, str_pad.ptr(), str_pad.length());
    } else {
      if (OB_FAIL(ObCharset::display_len(expr.datum_meta_.cs_type_, str_pad, pad_len))) {
        LOG_WARN("failed to get display len", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else {
      int64_t repeat_count = (length + pad_len - 1) / pad_len;
      char *pad_buf = static_cast<char *>(exec_ctx.get_allocator().alloc(str_pad.length() * repeat_count));
      if (OB_ISNULL(pad_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Failed to alloc", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < repeat_count; ++i) {
          MEMCPY(pad_buf + i * str_pad.length(), str_pad.ptr(), str_pad.length());
        }
        pad_ctx->set_pad_buf(pad_buf);
        pad_ctx->set_pad_len(pad_len);
      }
    }
  }

  // 初始化 result_buf 和 char_pos（MySQL 和 Oracle 模式都需要）
  if (OB_SUCC(ret)) {
    int64_t max_batch_size = ctx.max_batch_size_;
    char **result_buf = static_cast<char **>(exec_ctx.get_allocator().alloc(max_batch_size * sizeof(char *)));
    if (OB_ISNULL(result_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to alloc", K(ret));
    } else {
      pad_ctx->set_result_buf(result_buf);
      for (int64_t i = 0; OB_SUCC(ret) && i < max_batch_size; ++i) {
        result_buf[i] = expr.get_str_res_mem(ctx, buffer_size, i);
      }
    }
  }

  if (OB_SUCC(ret)) {
    int64_t *char_pos = NULL;
    if (OB_ISNULL(char_pos = static_cast<int64_t *>(exec_ctx.get_allocator().alloc(pad_len * sizeof(int64_t))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to alloc", K(ret));
    } else {
      pad_ctx->set_char_pos(char_pos);
      char_pos[0] = 0;
      if (is_mysql_mode) {
        // MySQL 模式：使用 charpos 计算字符位置
        for (int64_t i = 1; OB_SUCC(ret) && i < pad_len; ++i) {
          char_pos[i] = char_pos[i - 1] + ObCharset::charpos(expr.datum_meta_.cs_type_, str_pad.ptr() + char_pos[i - 1], str_pad.length() - char_pos[i - 1], 1);
        }
      } else {
        // Oracle 模式：使用 display_len 计算字符位置
        for (int64_t i = 1; OB_SUCC(ret) && i < pad_len; ++i) {
          int64_t char_pos_tmp = 0;
          int64_t total_width = 0;
          ObString remaining_str(str_pad.length() - char_pos[i - 1], str_pad.ptr() + char_pos[i - 1]);
          if (OB_FAIL(ObCharset::max_display_width_charpos(
                  expr.datum_meta_.cs_type_, remaining_str.ptr(), remaining_str.length(), 1, char_pos_tmp, &total_width))) {
            LOG_WARN("failed to get display charpos", K(ret));
          } else {
            char_pos[i] = char_pos[i - 1] + char_pos_tmp;
          }
        }
      }
    }
  }

  return ret;
}

template<typename ResVec, typename TextVec>
int ObExprBaseLRpad::calc_mysql_vector_optimized(const ObExpr &expr,
                                                 ObEvalCtx &ctx,
                                                 const ObBitVector &skip,
                                                 const EvalBound &bound,
                                                 const LRpadType pad_type,
                                                 const ObSQLSessionInfo &session) {
  int ret = OB_SUCCESS;
  int64_t max_result_size = -1;
  int64_t mbmaxlen = 0;
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  TextVec *text_vec = static_cast<TextVec *>(expr.args_[0]->get_vector(ctx));
  IntegerUniCVec *len_vec = static_cast<IntegerUniCVec *>(expr.args_[1]->get_vector(ctx));
  StrUniCVec *pad_text_vec = static_cast<StrUniCVec *>(expr.args_[2]->get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ObExprLRPadContext *pad_ctx = NULL;
  ObSolidifiedVarsGetter helper(expr, ctx, &session);

  if (OB_UNLIKELY(len_vec->has_null() || pad_text_vec->has_null() || len_vec->get_int(0) < 0 || pad_text_vec->get_string(0).empty())) {
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      if (skip.at(idx) || eval_flags.at(idx)) {
        continue;
      } else {
        res_vec->set_null(idx);
      }
    }
  } else if (OB_UNLIKELY(len_vec->get_int(0) == 0)) {
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      if (skip.at(idx) || eval_flags.at(idx)) {
        continue;
      } else {
        res_vec->set_string(idx, ObString::make_empty_string());
      }
    }
  } else if (OB_FAIL(helper.get_max_allowed_packet(max_result_size))) {
    LOG_WARN("Failed to get max allow packet size", K(ret));
  } else if (OB_FAIL(ObCharset::get_mbmaxlen_by_coll(expr.datum_meta_.cs_type_, mbmaxlen))) {
    LOG_WARN("failed to get mbmaxlen by coll", K(ret));
  } else {
    // get context
    int64_t length = len_vec->get_int(0);
    int64_t buffer_size = length * mbmaxlen;
    ObExecContext &exec_ctx = ctx.exec_ctx_;
    uint64_t pad_id = static_cast<uint64_t>(expr.expr_ctx_id_);
    if (NULL == (pad_ctx = static_cast<ObExprLRPadContext *>(exec_ctx.get_expr_op_ctx(pad_id)))) {
      ObString str_pad = pad_text_vec->get_string(bound.start());
      if (OB_FAIL(init_pad_ctx(expr, ctx, exec_ctx, pad_id, str_pad, length, buffer_size, true, pad_ctx))) {
        LOG_WARN("failed to init pad ctx", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      bool is_left_pad = pad_type == LPAD_TYPE;
      bool is_ascii = text_vec->is_batch_ascii();
      bool can_do_ascii_optimize = storage::can_do_ascii_optimize(expr.datum_meta_.cs_type_);
      if (is_left_pad) {
        if (is_ascii) {
          if (can_do_ascii_optimize) {
            ret = calc_mysql_inner_vector_optimized<ResVec, TextVec, true, true, true>(expr, ctx, skip, bound, mbmaxlen, max_result_size, pad_ctx);
          } else {
            ret = calc_mysql_inner_vector_optimized<ResVec, TextVec, true, true, false>(expr, ctx, skip, bound, mbmaxlen, max_result_size, pad_ctx);
          }
        } else {
          if (can_do_ascii_optimize) {
            ret = calc_mysql_inner_vector_optimized<ResVec, TextVec, true, false, true>(expr, ctx, skip, bound, mbmaxlen, max_result_size, pad_ctx);
          } else {
            ret = calc_mysql_inner_vector_optimized<ResVec, TextVec, true, false, false>(expr, ctx, skip, bound, mbmaxlen, max_result_size, pad_ctx);
          }
        }
      } else {
        if (is_ascii) {
          if (can_do_ascii_optimize) {
            ret = calc_mysql_inner_vector_optimized<ResVec, TextVec, false, true, true>(expr, ctx, skip, bound, mbmaxlen, max_result_size, pad_ctx);
          } else {
            ret = calc_mysql_inner_vector_optimized<ResVec, TextVec, false, true, false>(expr, ctx, skip, bound, mbmaxlen, max_result_size, pad_ctx);
          }
        } else {
          if (can_do_ascii_optimize) {
            ret = calc_mysql_inner_vector_optimized<ResVec, TextVec, false, false, true>(expr, ctx, skip, bound, mbmaxlen, max_result_size, pad_ctx);
          } else {
            ret = calc_mysql_inner_vector_optimized<ResVec, TextVec, false, false, false>(expr, ctx, skip, bound, mbmaxlen, max_result_size, pad_ctx);
          }
        }
      }
    }
  }
  return ret;
}

template <typename ResVec, typename TextVec, bool IsLeftPad, bool IsAscii, bool CanDoAsciiOptimize>
int ObExprBaseLRpad::calc_mysql_inner_vector_optimized(const ObExpr &expr,
                                                       ObEvalCtx &ctx,
                                                       const ObBitVector &skip,
                                                       const EvalBound &bound,
                                                       const int64_t mbmaxlen,
                                                       const int64_t max_result_size,
                                                       ObExprLRPadContext *pad_ctx)
{
  int ret = OB_SUCCESS;
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  TextVec *text_vec = static_cast<TextVec *>(expr.args_[0]->get_vector(ctx));
  IntegerUniCVec *len_vec = static_cast<IntegerUniCVec *>(expr.args_[1]->get_vector(ctx));
  StrUniCVec *pad_text_vec = static_cast<StrUniCVec *>(expr.args_[2]->get_vector(ctx));
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  int64_t length = len_vec->get_int(0);
  ObString str_pad = pad_text_vec->get_string(0);
  int64_t pad_len = pad_ctx->get_pad_len();
  char *pad_buf = pad_ctx->get_pad_buf();
  char **result_bufs = pad_ctx->get_result_buf();
  int64_t *char_pos = pad_ctx->get_char_pos();
  const ObCollationType cs_type = expr.datum_meta_.cs_type_;
  for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
    if (skip.at(idx) || eval_flags.at(idx)) {
      continue;
    } else if (text_vec->is_null(idx)) {
      res_vec->set_null(idx);
    } else {
      ObString str_text = text_vec->get_string(idx);
      int64_t text_len = ObCharsetStringHelper::fast_strlen_char<IsAscii, CanDoAsciiOptimize>(cs_type, str_text);
      if (text_len >= length) {
        // only substr needed
        int64_t result_size = ObCharsetStringHelper::fast_charpos<IsAscii, CanDoAsciiOptimize>(cs_type, str_text, length);
        res_vec->set_string(idx, str_text.ptr(), result_size);
      } else {
        char *result_buf = result_bufs[idx];
        int64_t repeat_count = (length - text_len) / pad_len;
        int64_t remain_len = (length - text_len) % pad_len;
        int64_t prefix_size = char_pos[remain_len];
        int64_t result_size =  str_text.length() + str_pad.length() * repeat_count + prefix_size;
        if (OB_UNLIKELY(result_size > max_result_size)) {
          res_vec->set_null(idx);
          if (!IsLeftPad) {
            LOG_USER_WARN(OB_ERR_FUNC_RESULT_TOO_LARGE, "rpad", static_cast<int>(max_result_size));
          } else {
            LOG_USER_WARN(OB_ERR_FUNC_RESULT_TOO_LARGE, "lpad", static_cast<int>(max_result_size));
          }
        } else if (OB_FAIL(padding_vector_mysql_optimized<IsLeftPad>(str_text.ptr(),
                                                                       str_text.length(),
                                                                       pad_buf,
                                                                       result_size - str_text.length(),
                                                                       result_buf))) {
          LOG_WARN("Failed to pad", K(ret), K(str_text), K(str_pad));
        } else {
          res_vec->set_string(idx, result_buf, result_size);
        }
      }
    }
  }
  return ret;
}

/* mysql util vector END }}} */

int ObExprBaseLRpad::get_padding_info_oracle(const ObCollationType cs,
                                             const ObString &str_text,
                                             const int64_t &width,
                                             const ObString &str_padtext,
                                             const int64_t max_result_size,
                                             int64_t &repeat_count,
                                             int64_t &prefix_size,
                                             bool &pad_space)
{
  // lpad: [sp] + padtext * t + padprefix + text
  // rpad: text + padtext * t + padprefix + [sp]
  int ret = OB_SUCCESS;
  int64_t text_size = str_text.length();
  int64_t pad_size = str_padtext.length();

  int64_t text_width = 0;
  int64_t pad_width = 0;
  pad_space = false;

  // GOAL: get repeat_count, prefix_size and pad space.
  if (OB_FAIL(ObCharset::display_len(cs, str_text, text_width))) {
    LOG_WARN("Failed to get displayed length", K(ret), K(str_text));
  } else if (OB_FAIL(ObCharset::display_len(cs, str_padtext, pad_width))) {
    LOG_WARN("Failed to get displayed length", K(ret), K(str_padtext));
  } else if (OB_UNLIKELY(width <= text_width)
             || OB_UNLIKELY(width <= 0)
             || OB_UNLIKELY(pad_size <= 0)
             || OB_UNLIKELY(pad_width <= 0)) {
    // this should been resolve outside
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wrong width", K(ret), K(width), K(text_width), K(pad_size), K(pad_width));
  } else {
    repeat_count = std::min((width - text_width) / pad_width, (max_result_size - text_size) / pad_size);
    int64_t remain_width = width - (text_width + repeat_count * pad_width);
    int64_t remain_size = max_result_size - (text_size + repeat_count * pad_size);
    int64_t total_width = 0;
    LOG_DEBUG("calc pad", K(remain_width), K(width), K(text_width), K(pad_width),
              K(max_result_size), K(text_size), K(pad_size), K(ret), K(remain_size));

    if (remain_width > 0 && remain_size > 0) {
      // 有 pad prefix 或者 pad space
      if (OB_FAIL(ObCharset::max_display_width_charpos(
                  cs, str_padtext.ptr(), std::min(remain_size, pad_size),
                  remain_width, prefix_size, &total_width))) {
        LOG_WARN("Failed to get max display width", K(ret), K(str_text), K(remain_width));
      } else if (remain_width != total_width && remain_size != prefix_size) {
        // 没到达指定宽度, 补一个空格
        pad_space = true;
      }
    }

  }
  return ret;
}

// for engine 3.0
int ObExprBaseLRpad::calc_oracle_pad_expr(const ObExpr &expr, ObEvalCtx &ctx,
                                          LRpadType pad_type, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *text = NULL;
  ObDatum *len = NULL;
  ObDatum *pad_text = NULL;
  if (OB_FAIL(expr.eval_param_value(ctx, text, len, pad_text))) {
    LOG_WARN("eval param failed", K(ret));
  } else {
    ObExprStrResAlloc res_alloc(expr, ctx);
    bool is_unchanged_clob = false;
    if (3 == expr.arg_cnt_) {
      if (OB_ISNULL(text) || OB_ISNULL(len) || OB_ISNULL(pad_text)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected datum", K(ret), KP(text), KP(len), KP(pad_text));
      } else {
        if (!ob_is_text_tc(expr.args_[0]->datum_meta_.type_)
            && !ob_is_text_tc(expr.args_[2]->datum_meta_.type_)) {
          if (OB_FAIL(calc_oracle(pad_type, expr, *text, *len, *pad_text, res_alloc, res,
                                  is_unchanged_clob))) {
            LOG_WARN("calc pad failed", K(ret));
          }
        } else { // text tc
          ObDatum text_inrow;
          text_inrow.set_datum(*text); // copy meta
          ObDatum pad_text_inrow;
          pad_text_inrow.set_datum(*pad_text);
          ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
          common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();

          if (!text->is_null()) {
            ObString text_data = text->get_string();
            if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *text,
                                                                  expr.args_[0]->datum_meta_,
                                                                  expr.args_[0]->obj_meta_.has_lob_header(),
                                                                  text_data))) {
              LOG_WARN("failed to read real text", K(ret), K(text_data));
            } else {
              text_inrow.set_string(text_data); // Notice: lob header flag is removed!
            }
          }
          if (OB_SUCC(ret) && !pad_text->is_null()) {
            ObString pad_data = pad_text->get_string();
            if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *pad_text,
                                                                  expr.args_[2]->datum_meta_,
                                                                  expr.args_[2]->obj_meta_.has_lob_header(),
                                                                  pad_data))) {
              LOG_WARN("failed to read real pattern", K(ret), K(pad_data));
            } else {
              pad_text_inrow.set_string(pad_data);
            }
          }
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(calc_oracle(pad_type, expr, text_inrow, *len, pad_text_inrow,
                                         res_alloc, res, is_unchanged_clob))) {
            LOG_WARN("calc pad for text failed", K(ret));
          } else if (is_unchanged_clob) {
            res.set_string(text->get_string());
          }
        }
      }
    } else if (2 == expr.arg_cnt_) {
      ObCollationType in_coll = ObCharset::get_system_collation();
      ObCollationType out_coll = expr.datum_meta_.cs_type_;
      ObEvalCtx::TempAllocGuard alloc_guard(ctx);
      ObIAllocator &calc_alloc = alloc_guard.get_allocator();
      ObString pad_str_utf8(1, " ");
      ObString pad_str;
      ObDatum tmp_pad_text;
      tmp_pad_text.set_string(pad_str);
      if (OB_ISNULL(text) || OB_ISNULL(len)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected datum", K(ret), KP(text), KP(len));
      } else if (OB_FAIL(ObExprUtil::convert_string_collation(pad_str_utf8, in_coll,
                                        pad_str, out_coll, calc_alloc))) {
        LOG_WARN("convert collation failed", K(ret), K(in_coll), K(pad_str), K(out_coll));
      } else if (OB_UNLIKELY(pad_str.empty())) {
        LOG_WARN("unexpected pad_str after convert collation", K(ret), K(pad_str));
      } else {
        tmp_pad_text.set_string(pad_str);
        if (!ob_is_text_tc(expr.args_[0]->datum_meta_.type_)) {
          if (OB_FAIL(calc_oracle(pad_type, expr, *text, *len, tmp_pad_text, res_alloc, res,
                                  is_unchanged_clob))) {
            LOG_WARN("calc pad failed", K(ret));
          }
        } else { // text tc
          ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
          common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
          ObDatum text_inrow;
          text_inrow.set_datum(*text);
          if (!text->is_null()) {
            ObString text_data = text->get_string();
            if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *text,
                                                                  expr.args_[0]->datum_meta_,
                                                                  expr.args_[0]->obj_meta_.has_lob_header(),
                                                                  text_data))) {
              LOG_WARN("failed to read real text", K(ret), K(text_data));
            } else {
              text_inrow.set_string(text_data);
            }
          }
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(calc_oracle(pad_type, expr, text_inrow, *len, tmp_pad_text,
                                         res_alloc, res, is_unchanged_clob))) {
            LOG_WARN("calc pad failed", K(ret));
          } else if (is_unchanged_clob) {
            res.set_string(text->get_string());
          }
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid arg cnt", K(ret), K(expr.arg_cnt_));
    }
  }

  return ret;
}

int ObExprBaseLRpad::calc_oracle(LRpadType pad_type, const ObExpr &expr,
                                 const ObDatum &text, const ObDatum &len,
                                 const ObDatum &pad_text, ObIAllocator &res_alloc,
                                 ObDatum &res, bool &is_unchanged_clob)
{
  int ret = OB_SUCCESS;
  ObObjType res_type = expr.datum_meta_.type_;
  if (text.is_null() || len.is_null() || pad_text.is_null()) {
    res.set_null();
  } else {
    int64_t width = 0;
    int64_t repeat_count = 0;
    int64_t prefix_size = 0;
    bool pad_space = false;
    int64_t text_width = 0;
    const ObString &str_text = text.get_string();
    const ObString &str_pad = pad_text.get_string();
    const ObCollationType cs_type = expr.datum_meta_.cs_type_;

    // Max VARCHAR2 size is 32767 in Oracle PL/SQL.
    // However, Oracle SQL allow user to determine max VARCHAR2 size through `MAX_STRING_SIZE` parameter.
    // The max VARCHAR2 size is 4000 when `MAX_STRING_SIZE` is set to `STANDARD`, and 32767 when set to `EXTENDED`.
    // OB does not support `MAX_STRING_SIZE` parameter for now, but behaves compatibly with `EXTENDED` mode.
    const ObExprOracleLRpadInfo *info = nullptr;
    int64_t max_varchar2_size = OB_MAX_ORACLE_VARCHAR_LENGTH;
    if (OB_NOT_NULL(info = static_cast<ObExprOracleLRpadInfo *>(expr.extra_info_)) &&
        info->is_called_in_sql_) {
      const int64_t ORACLE_EXTENDED_MAX_VARCHAR_LENGTH = 32767;
      max_varchar2_size = ORACLE_EXTENDED_MAX_VARCHAR_LENGTH;
    }
    int64_t max_result_size = ob_is_text_tc(expr.datum_meta_.type_)
        ? OB_MAX_LONGTEXT_LENGTH
        : max_varchar2_size;

    number::ObNumber len_num(len.get_number());
    int64_t decimal_parts = -1;
    if (len_num.is_negative()) {
      width = -1;
    } else if (!len_num.is_int_parts_valid_int64(width, decimal_parts)) {
      // LOB 最大也就 4G, 这里用 UINT32_MAX.
      // 负数已经被过滤掉了.
      width = UINT32_MAX;
    }
    if (width <= 0) {
      res.set_null();
    } else if (OB_FAIL(ObCharset::display_len(cs_type, str_text, text_width))) {
      LOG_WARN("Failed to get displayed length", K(ret), K(str_text));
    } else if ((3 == expr.arg_cnt_)
               && expr.args_[0]->datum_meta_.is_clob()
               && (0 == str_pad.length())
               && (text_width <= width)) {
      // pad_text 是 empty_clob，text 是 clob，如果不走截断逻辑的话，结果直接置为原 clob
      res.set_datum(text);
      is_unchanged_clob = ob_is_text_tc(res_type);
    } else if (text_width == width) {
      res.set_datum(text);
      is_unchanged_clob = ob_is_text_tc(res_type);
    } else {
      char *result_ptr = NULL;
      int64_t result_size = 0;
      bool has_lob_header = expr.obj_meta_.has_lob_header();
      if (text_width > width) {
        // substr
        int64_t total_width = 0;
        if (OB_FAIL(ObCharset::max_display_width_charpos(cs_type, str_text.ptr(),
                str_text.length(), width, prefix_size, &total_width))) {
          LOG_WARN("Failed to get max display width", K(ret));
        } else if (OB_FAIL(padding(pad_type, cs_type, "", 0, str_text.ptr(), str_text.length(),
                                    prefix_size, 0, (total_width != width), &res_alloc,
                                    result_ptr, result_size, res_type, has_lob_header))) {
          LOG_WARN("Failed to pad", K(ret), K(str_text), K(str_pad), K(prefix_size),
                                    K(repeat_count), K(pad_space));
        }
      } else if (OB_FAIL(get_padding_info_oracle(cs_type, str_text, width, str_pad,
                  max_result_size, repeat_count, prefix_size, pad_space))) {
        LOG_WARN("Failed to get padding info", K(ret), K(str_text), K(width),
                                              K(str_pad), K(max_result_size));
      } else if (OB_FAIL(padding(pad_type, cs_type, str_text.ptr(), str_text.length(), str_pad.ptr(),
                                str_pad.length(), prefix_size, repeat_count, pad_space,
                                &res_alloc, result_ptr, result_size, res_type, has_lob_header))) {
        LOG_WARN("Failed to pad", K(ret), K(str_text), K(str_pad), K(prefix_size),
                                  K(repeat_count), K(pad_space));
      }

      if (OB_SUCC(ret)) {
        if (NULL == result_ptr || 0 == result_size) {
          res.set_null();
        } else {
          res.set_string(result_ptr, result_size);
        }
      }
    }
  }
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprBaseLRpad, raw_expr) {
  int ret = OB_SUCCESS;
  SET_LOCAL_SYSVAR_CAPACITY(2);
  EXPR_ADD_LOCAL_SYSVAR(SYS_VAR_MAX_ALLOWED_PACKET);
  EXPR_ADD_LOCAL_SYSVAR(SYS_VAR_COLLATION_CONNECTION);
  return ret;
}

/* oracle util END }}} */
/* ObExprBaseLRpad END }}} */

/* ObExprLpad {{{1 */
ObExprLpad::ObExprLpad(ObIAllocator &alloc)
  : ObExprBaseLRpad(alloc, T_FUN_SYS_LPAD, N_LPAD, 3)
{
}

ObExprLpad::~ObExprLpad()
{
}

int ObExprLpad::calc_result_type3(ObExprResType &type,
                                  ObExprResType &text,
                                  ObExprResType &len,
                                  ObExprResType &pad_text,
                                  ObExprTypeCtx &type_ctx) const
{
  return ObExprBaseLRpad::calc_type(type, text, len, &pad_text, type_ctx);
}

int ObExprLpad::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_mysql_lpad_expr;
  rt_expr.eval_vector_func_ = calc_mysql_lpad_expr_vector;
  return ret;
}

int ObExprLpad::calc_mysql_lpad_expr(const ObExpr &expr, ObEvalCtx &ctx,
                                          ObDatum &res)
{
  return calc_mysql_pad_expr(expr, ctx, LPAD_TYPE, res);
}

int ObExprLpad::calc_mysql_lpad_expr_vector(VECTOR_EVAL_FUNC_ARG_DECL)
{
  return calc_mysql_pad_expr_vector(VECTOR_EVAL_FUNC_ARG_LIST, LPAD_TYPE);
}
/* ObExprLpad END }}} */

/* ObExprRpad {{{1 */
ObExprRpad::ObExprRpad(ObIAllocator &alloc)
  : ObExprBaseLRpad(alloc, T_FUN_SYS_RPAD, N_RPAD, 3)
{
}

ObExprRpad::ObExprRpad(ObIAllocator &alloc,
                       ObExprOperatorType type,
                       const char *name)
    : ObExprBaseLRpad(alloc, type, name, 3)
{
}

ObExprRpad::~ObExprRpad()
{
}

int ObExprRpad::calc_result_type3(ObExprResType &type,
                                  ObExprResType &text,
                                  ObExprResType &len,
                                  ObExprResType &pad_text,
                                  ObExprTypeCtx &type_ctx) const
{
  return ObExprBaseLRpad::calc_type(type, text, len, &pad_text, type_ctx);
}

int ObExprRpad::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_mysql_rpad_expr;
  rt_expr.eval_vector_func_ = calc_mysql_rpad_expr_vector;
  return ret;
}

int ObExprRpad::calc_mysql_rpad_expr(const ObExpr &expr, ObEvalCtx &ctx,
                                           ObDatum &res)
{
  return calc_mysql_pad_expr(expr, ctx, RPAD_TYPE, res);
}
int ObExprRpad::calc_mysql_rpad_expr_vector(VECTOR_EVAL_FUNC_ARG_DECL)
{
  return calc_mysql_pad_expr_vector(VECTOR_EVAL_FUNC_ARG_LIST, RPAD_TYPE);
}
/* ObExprRpad END }}} */

/* ObExprLpadOracle {{{1 */
ObExprOracleLpad::ObExprOracleLpad(ObIAllocator &alloc)
  : ObExprBaseLRpad(alloc, T_FUN_SYS_LPAD, N_LPAD, TWO_OR_THREE)
{
}

ObExprOracleLpad::~ObExprOracleLpad()
{
}

int ObExprOracleLpad::calc_result_typeN(ObExprResType &type,
                                        ObExprResType *types_array,
                                        int64_t param_num,
                                        ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  if (param_num == 3) {
    if (OB_ISNULL(types_array)
        || OB_ISNULL(types_array + 1)
        || OB_ISNULL(types_array + 2)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL param", K(ret), K(types_array[0]), K(types_array[1]), K(types_array[2]));
    } else if (OB_FAIL(ObExprBaseLRpad::calc_type(
                type, types_array[0], types_array[1], types_array + 2, type_ctx))) {
      LOG_WARN("Failed to calc_type", K(ret));
    }
  } else if (param_num == 2) {
    if (OB_ISNULL(types_array)
        || OB_ISNULL(types_array + 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL param", K(ret), K(types_array[0]), K(types_array[1]));
    } else if (OB_FAIL(ObExprBaseLRpad::calc_type(
                type, types_array[0], types_array[1], NULL, type_ctx))) {
      LOG_WARN("Failed to calc_type", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Wrong param num", K(ret), K(param_num));
  }
  return ret;
}

int ObExprOracleLpad::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(raw_expr);
  if (OB_ISNULL(expr_cg_ctx.allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Allocator is NULL", K(ret));
  } else if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_2_1_0) {
    // lpad expr do not have a extra_info before 4_2_0_release, need to maintain compatibility
    rt_expr.extra_info_ = nullptr;
    rt_expr.eval_func_ = calc_oracle_lpad_expr;
    rt_expr.eval_vector_func_ = calc_oracle_lpad_expr_vector;
  } else {
    ObIAllocator &alloc = *expr_cg_ctx.allocator_;
    ObIExprExtraInfo *extra_info = nullptr;
    if (OB_FAIL(ObExprExtraInfoFactory::alloc(alloc, rt_expr.type_, extra_info))) {
      LOG_WARN("Failed to allocate memory for ObExprOracleLRpadInfo", K(ret));
    } else if (OB_ISNULL(extra_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("extra_info should not be nullptr", K(ret));
    } else {
      ObExprOracleLRpadInfo *info = static_cast<ObExprOracleLRpadInfo *>(extra_info);
      info->is_called_in_sql_ = is_called_in_sql();
      rt_expr.extra_info_ = extra_info;
      rt_expr.eval_func_ = calc_oracle_lpad_expr;
      rt_expr.eval_vector_func_ = calc_oracle_lpad_expr_vector;
    }
  }
  return ret;
}

int ObExprOracleLpad::calc_oracle_lpad_expr(const ObExpr &expr, ObEvalCtx &ctx,
                                           ObDatum &res)
{
  return calc_oracle_pad_expr(expr, ctx, LPAD_TYPE, res);
}

int ObExprOracleLpad::calc_oracle_lpad_expr_vector(VECTOR_EVAL_FUNC_ARG_DECL)
{
  return calc_oracle_pad_expr_vector(VECTOR_EVAL_FUNC_ARG_LIST, LPAD_TYPE);
}
/* ObExprLpadOracle END }}} */

/* ObExprRpadOracle {{{1 */
ObExprOracleRpad::ObExprOracleRpad(ObIAllocator &alloc)
  : ObExprBaseLRpad(alloc, T_FUN_SYS_RPAD, N_RPAD, TWO_OR_THREE)
{
}

ObExprOracleRpad::~ObExprOracleRpad()
{
}

int ObExprOracleRpad::calc_result_typeN(ObExprResType &type,
                                        ObExprResType *types_array,
                                        int64_t param_num,
                                        ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  if (param_num == 3) {
    if (OB_ISNULL(types_array)
        || OB_ISNULL(types_array + 1)
        || OB_ISNULL(types_array + 2)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL param", K(ret), K(types_array[0]), K(types_array[1]), K(types_array[2]));
    } else if (OB_FAIL(ObExprBaseLRpad::calc_type(
                type, types_array[0], types_array[1], &(types_array[2]), type_ctx))) {
      LOG_WARN("Failed to calc_type", K(ret));
    }
  } else if (param_num == 2) {
    if (OB_ISNULL(types_array)
        || OB_ISNULL(types_array + 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL param", K(ret), K(types_array[0]), K(types_array[1]));
    } else if (OB_FAIL(ObExprBaseLRpad::calc_type(
                type, types_array[0], types_array[1], NULL, type_ctx))) {
      LOG_WARN("Failed to calc_type", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Wrong param num", K(ret), K(param_num));
  }
  return ret;
}

int ObExprOracleRpad::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(raw_expr);
  if (OB_ISNULL(expr_cg_ctx.allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Allocator is NULL", K(ret));
  } else if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_2_1_0) {
    // rpad expr do not have a extra_info before 4_2_0_release, need to maintain compatibility
    rt_expr.extra_info_ = nullptr;
    rt_expr.eval_func_ = calc_oracle_rpad_expr;
  } else {
    ObIAllocator &alloc = *expr_cg_ctx.allocator_;
    ObIExprExtraInfo *extra_info = nullptr;
    if (OB_FAIL(ObExprExtraInfoFactory::alloc(alloc, rt_expr.type_, extra_info))) {
      LOG_WARN("Failed to allocate memory for ObExprOracleLRpadInfo", K(ret));
    } else if (OB_ISNULL(extra_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("extra_info should not be nullptr", K(ret));
    } else {
      ObExprOracleLRpadInfo *info = static_cast<ObExprOracleLRpadInfo *>(extra_info);
      info->is_called_in_sql_ = is_called_in_sql();
      rt_expr.extra_info_ = extra_info;
      rt_expr.eval_func_ = calc_oracle_rpad_expr;
      rt_expr.eval_vector_func_ = calc_oracle_rpad_expr_vector;
    }
  }
  return ret;
}

int ObExprOracleRpad::calc_oracle_rpad_expr(const ObExpr &expr, ObEvalCtx &ctx,
                                           ObDatum &res)
{
  return calc_oracle_pad_expr(expr, ctx, RPAD_TYPE, res);
}
int ObExprOracleRpad::calc_oracle_rpad_expr_vector(VECTOR_EVAL_FUNC_ARG_DECL)
{
  return calc_oracle_pad_expr_vector(VECTOR_EVAL_FUNC_ARG_LIST, RPAD_TYPE);
}
/* ObExprRpadOracle END }}} */

/* ObExprLRpadOracle Vector Begin*/

int ObExprBaseLRpad::get_padding_info_oracle_vector(const ObCollationType &cs,
                                            const ObString &str_text,
                                            const int64_t text_width,
                                            const int64_t &width,
                                            const ObString &str_pad,
                                            int64_t pad_width,
                                            const int64_t max_result_size,
                                            int64_t &repeat_count,
                                            int64_t &prefix_size,
                                            bool &pad_space)
{
  // lpad: [sp] + padtext * t + padprefix + text
  // rpad: text + padtext * t + padprefix + [sp]
  int ret = OB_SUCCESS;
  int64_t text_size = str_text.length();
  int64_t pad_size = str_pad.length();
  // GOAL: get repeat_count, prefix_size and pad space.
  if (pad_width < 0 && OB_FAIL(ObCharset::display_len(cs, str_pad, pad_width))) {
    LOG_WARN("failed to get display len", K(ret), K(str_pad));
  } else {
    repeat_count = std::min((width - text_width) / pad_width, (max_result_size - text_size) / pad_size);
    int64_t remain_width = width - (text_width + pad_width * repeat_count);
    int64_t remain_size = max_result_size - (text_size + repeat_count * pad_size);
    int64_t total_width = 0;
    if (remain_width > 0 && remain_size > 0) {
      if (OB_FAIL(ObCharsetStringHelper::fast_display_charpos(cs,
                                                                     str_pad.ptr(),
                                                                     std::min<int64_t>(str_pad.length(), remain_size),
                                                                     remain_width,
                                                                     false,
                                                                     prefix_size,
                                                                     total_width))) {
        LOG_WARN("Failed to get max display width", K(ret), K(str_text), K(remain_width));
      } else if (remain_width != total_width && remain_size != prefix_size) {
        pad_space = true;
      }
    }
  }
  return ret;
}

int ObExprBaseLRpad::calc_oracle_pad_expr_vector(const ObExpr &expr,
                                                 ObEvalCtx &ctx,
                                                 const ObBitVector &skip,
                                                 const EvalBound &bound,
                                                 LRpadType pad_type)
{
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else if (OB_UNLIKELY(3 != expr.arg_cnt_ && 2 != expr.arg_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("arg cnt must be 3", K(ret), K(expr.arg_cnt_));
  } else if (OB_FAIL(expr.eval_vector_param_value(ctx, skip, bound))) {
    LOG_WARN("eval param value failed", K(ret));
  } else {
    if (!ob_is_text_tc(expr.datum_meta_.type_) &&
        !ob_is_text_tc(expr.args_[0]->datum_meta_.type_) &&
        (expr.arg_cnt_ == 2 || !ob_is_text_tc(expr.args_[2]->datum_meta_.type_)) &&
        expr.args_[1]->is_static_const_ && !expr.args_[1]->get_vector(ctx)->has_null() &&
        (expr.arg_cnt_ == 2 || (expr.args_[2]->is_static_const_ && !expr.args_[2]->get_vector(ctx)->has_null()))) {
      VectorFormat res_format = expr.get_format(ctx);
      VectorFormat text_format = expr.args_[0]->get_format(ctx);
      if (VEC_DISCRETE == res_format) {
        if (VEC_DISCRETE == expr.args_[0]->get_format(ctx)) {
          ret = calc_oracle_vector_optimized<StrDiscVec, StrDiscVec>(expr, ctx, skip, bound, pad_type, *session);
        } else if (VEC_UNIFORM == expr.args_[0]->get_format(ctx)) {
          ret = calc_oracle_vector_optimized<StrDiscVec, StrUniVec>(expr, ctx, skip, bound, pad_type, *session);
        } else {
          ret = calc_oracle_vector_optimized<StrDiscVec, ObVectorBase>(expr, ctx, skip, bound, pad_type, *session);
        }
      } else if (VEC_UNIFORM == res_format) {
        if (VEC_DISCRETE == expr.args_[0]->get_format(ctx)) {
          ret = calc_oracle_vector_optimized<StrUniVec, StrDiscVec>(expr, ctx, skip, bound, pad_type, *session);
        } else if (VEC_UNIFORM == expr.args_[0]->get_format(ctx)) {
          ret = calc_oracle_vector_optimized<StrUniVec, StrUniVec>(expr, ctx, skip, bound, pad_type, *session);
        } else {
          ret = calc_oracle_vector_optimized<StrUniVec, ObVectorBase>(expr, ctx, skip, bound, pad_type, *session);
        }
      } else {
        if (VEC_DISCRETE == expr.args_[0]->get_format(ctx)) {
          ret = calc_oracle_vector_optimized<ObVectorBase, StrDiscVec>(expr, ctx, skip, bound, pad_type, *session);
        } else if (VEC_UNIFORM == expr.args_[0]->get_format(ctx)) {
          ret = calc_oracle_vector_optimized<ObVectorBase, StrUniVec>(expr, ctx, skip, bound, pad_type, *session);
        } else {
          ret = calc_oracle_vector_optimized<ObVectorBase, ObVectorBase>(expr, ctx, skip, bound, pad_type, *session);
        }
      }
    } else {
      VectorFormat res_format = expr.get_format(ctx);
      VectorFormat text_format = expr.args_[0]->get_format(ctx);
      if (VEC_DISCRETE == res_format) {
        if (VEC_DISCRETE == expr.args_[0]->get_format(ctx)) {
          ret = calc_oracle_vector<StrDiscVec, StrDiscVec, ObVectorBase, ObVectorBase>(expr, ctx, skip, bound, pad_type, *session);
        } else if (VEC_UNIFORM == expr.args_[0]->get_format(ctx)) {
          ret = calc_oracle_vector<StrDiscVec, StrUniVec, ObVectorBase, ObVectorBase>(expr, ctx, skip, bound, pad_type, *session);
        } else {
          ret = calc_oracle_vector<StrDiscVec, ObVectorBase, ObVectorBase, ObVectorBase>(expr, ctx, skip, bound, pad_type, *session);
        }
      } else if (VEC_UNIFORM == res_format) {
        if (VEC_DISCRETE == expr.args_[0]->get_format(ctx)) {
          ret = calc_oracle_vector<StrUniVec, StrDiscVec, ObVectorBase, ObVectorBase>(expr, ctx, skip, bound, pad_type, *session);
        } else if (VEC_UNIFORM == expr.args_[0]->get_format(ctx)) {
          ret = calc_oracle_vector<StrUniVec, StrUniVec, ObVectorBase, ObVectorBase>(expr, ctx, skip, bound, pad_type, *session);
        } else {
          ret = calc_oracle_vector<StrUniVec, ObVectorBase, ObVectorBase, ObVectorBase>(expr, ctx, skip, bound, pad_type, *session);
        }
      } else {
        if (VEC_DISCRETE == expr.args_[0]->get_format(ctx)) {
          ret = calc_oracle_vector<ObVectorBase, StrDiscVec, ObVectorBase, ObVectorBase>(expr, ctx, skip, bound, pad_type, *session);
        } else if (VEC_UNIFORM == expr.args_[0]->get_format(ctx)) {
          ret = calc_oracle_vector<ObVectorBase, StrUniVec, ObVectorBase, ObVectorBase>(expr, ctx, skip, bound, pad_type, *session);
        } else {
          ret = calc_oracle_vector<ObVectorBase, ObVectorBase, ObVectorBase, ObVectorBase>(expr, ctx, skip, bound, pad_type, *session);
        }
      }
    }
  }
  return ret;
}

template <typename ResVec, typename TextVec, typename LenVec, typename PadVec>
int ObExprBaseLRpad::calc_oracle_vector(const ObExpr &expr,
                                        ObEvalCtx &ctx,
                                        const ObBitVector &skip,
                                        const EvalBound &bound,
                                        const LRpadType pad_type,
                                        const ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  bool is_ascii = expr.args_[0]->get_vector(ctx)->is_batch_ascii();
  int64_t max_result_size = -1;
  int64_t len_int = -1;
  int64_t pad_len = -1;
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  TextVec *text_vec = static_cast<TextVec *>(expr.args_[0]->get_vector(ctx));
  LenVec *len_vec = static_cast<LenVec *>(expr.args_[1]->get_vector(ctx));
  PadVec *pad_text_vec = expr.arg_cnt_ == 2 ? NULL :
                             static_cast<PadVec *>(expr.args_[2]->get_vector(ctx));
  ObString str_pad;
  ObString str_text;
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);

  // Max VARCHAR2 size is 32767 in Oracle PL/SQL.
  // However, Oracle SQL allow user to determine max VARCHAR2 size through `MAX_STRING_SIZE` parameter.
  // The max VARCHAR2 size is 4000 when `MAX_STRING_SIZE` is set to `STANDARD`, and 32767 when set to `EXTENDED`.
  // OB does not support `MAX_STRING_SIZE` parameter for now, but behaves compatibly with `EXTENDED` mode.
  const ObExprOracleLRpadInfo *info = nullptr;
  int64_t max_varchar2_size = OB_MAX_ORACLE_VARCHAR_LENGTH;
  if (OB_NOT_NULL(info = static_cast<ObExprOracleLRpadInfo *>(expr.extra_info_)) &&
      info->is_called_in_sql_) {
    const int64_t ORACLE_EXTENDED_MAX_VARCHAR_LENGTH = 32767;
    max_varchar2_size = ORACLE_EXTENDED_MAX_VARCHAR_LENGTH;
  }
  max_result_size = ob_is_text_tc(expr.datum_meta_.type_)
                        ? OB_MAX_LONGTEXT_LENGTH
                        : max_varchar2_size;

  if (expr.arg_cnt_ == 3) {
    if (expr.args_[2]->is_static_const_ &&
        !expr.args_[2]->get_vector(ctx)->is_null(bound.start()) &&
        !ob_is_text_tc(expr.args_[2]->datum_meta_.type_)) {
      str_pad = expr.args_[2]->get_vector(ctx)->get_string(bound.start());
      if (OB_FAIL(ObCharset::display_len(expr.datum_meta_.cs_type_, str_pad, pad_len)))  {
        LOG_WARN("faild to get display len", K(ret), K(str_pad));
      }
    }
  } else if (expr.arg_cnt_ == 2) {
    ObCollationType in_coll = ObCharset::get_system_collation();
    ObCollationType out_coll = expr.datum_meta_.cs_type_;
    ObString pad_str_utf8(1, " ");
    if (OB_FAIL(ObExprUtil::convert_string_collation(pad_str_utf8, in_coll,
                                      str_pad, out_coll, ctx.exec_ctx_.get_allocator()))) {
      LOG_WARN("convert collation failed", K(ret), K(in_coll), K(str_pad), K(out_coll));
    } else if (OB_UNLIKELY(str_pad.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected pad_str after convert collation", K(ret), K(str_pad));
    } else if (OB_FAIL(ObCharset::display_len(expr.datum_meta_.cs_type_, str_pad, pad_len)))  {
      LOG_WARN("faild to get display len", K(ret), K(str_pad));
    }
  }

  for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
    if (skip.at(idx) || eval_flags.at(idx)) {
      continue;
    } else if (text_vec->is_null(idx) || len_vec->is_null(idx) ||
               (expr.arg_cnt_ == 3 && pad_text_vec->is_null(idx))) {
      res_vec->set_null(idx);
    } else {
      number::ObNumber len_num(len_vec->get_number(idx));
      int64_t decimal_parts = -1;
      if (len_num.is_negative()) {
        len_int = -1;
      } else if (!len_num.is_int_parts_valid_int64(len_int, decimal_parts)) {
        // LOB 最大也就 4G, 这里用 UINT32_MAX.
        // 负数已经被过滤掉了.
        len_int = UINT32_MAX;
      }
      if (len_int <= 0) {
        res_vec->set_null(idx);
      } else {
        ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
        common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
        bool is_unchanged_clob = false;
        // get text str
        if (!ob_is_text_tc(expr.args_[0]->datum_meta_.type_)) {
          str_text = text_vec->get_string(idx);
        } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                       temp_allocator, text_vec, expr.args_[0]->datum_meta_,
                       expr.args_[0]->obj_meta_.has_lob_header(), str_text, idx))) {
          LOG_WARN("failed to read lob data text", K(ret), K(text_vec->get_string(idx)));
        }
        // get pad str
        if (OB_FAIL(ret)) {
        } else if (expr.arg_cnt_ == 2) {
          // already got, do nothing
        } else if (!ob_is_text_tc(expr.args_[2]->datum_meta_.type_)) {
          str_pad = pad_text_vec->get_string(idx);
        } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                       temp_allocator, pad_text_vec, expr.args_[2]->datum_meta_,
                       expr.args_[2]->obj_meta_.has_lob_header(), str_pad, idx))) {
          LOG_WARN("failed to read lob data pattern", K(ret), K(pad_text_vec->get_string(idx)));
        }
        // check unchanged clob
        if ((3 == expr.arg_cnt_) && expr.args_[0]->datum_meta_.is_clob() && (0 == str_pad.length())) {
          int64_t text_width = 0;
          if (OB_FAIL(ObCharset::display_len(expr.datum_meta_.cs_type_, str_text, text_width))) {
            LOG_WARN("Failed to get displayed length", K(ret), K(str_text));
          } else if (text_width <= len_int) {
            char *result_buf = NULL;
            int64_t buf_len = 0;
            ObTextStringVectorResult<ResVec> output_result(expr.datum_meta_.type_, &expr, &ctx, res_vec, idx);
            if (OB_FAIL(output_result.init_with_batch_idx(str_text.length(), idx))) {
              LOG_WARN("init lob result failed", K(ret));
            } else if (OB_FAIL(output_result.get_reserved_buffer(result_buf, buf_len))) {
              LOG_WARN("failed to get reserved buffer", K(ret));
            } else if (FALSE_IT((MEMCPY(result_buf, str_text.ptr(), str_text.length())))) {
            } else if (OB_FAIL(output_result.lseek(buf_len, 0))) {
              LOG_WARN("temp lob lseek failed", K(ret));
            } else {
              ObString output;
              output_result.get_result_buffer(output);
              res_vec->set_string(idx, output);
              is_unchanged_clob = true;
            }
          }
        }
        if (OB_FAIL(ret) || is_unchanged_clob) {
          // do nothing
        } else if (OB_FAIL(calc_oracle_inner_vector<ResVec>(pad_type, expr, ctx, len_int,
                max_result_size, str_text, str_pad, pad_len, res_vec, idx, is_ascii))) {
          LOG_WARN("Failed to eval base lrpad", K(ret));
        }
      }
    }
  }
  return ret;
}

template <typename ResVec>
int ObExprBaseLRpad::calc_oracle_inner_vector(const LRpadType pad_type,
                                              const ObExpr &expr,
                                              ObEvalCtx &ctx,
                                              const int64_t width,
                                              int64_t &max_result_size,
                                              const ObString &str_text,
                                              const ObString &str_pad,
                                              int64_t pad_width,
                                              ResVec *res_vec,
                                              const int64_t idx,
                                              bool is_ascii)
{
  int ret = OB_SUCCESS;
  int64_t repeat_count = 0;
  int64_t prefix_size = 0;
  int64_t text_width = 0;
  int64_t result_size = 0;
  const ObCollationType cs_type = expr.datum_meta_.cs_type_;
  const ObObjType type = expr.datum_meta_.type_;
  if (OB_FAIL(ObCharsetStringHelper::fast_display_len(cs_type, str_text, is_ascii, text_width))) {
    LOG_WARN("Failed to get displayed length", K(ret), K(str_text));
  } else if (text_width >= width) {
    // only substr needed
    int64_t total_width = 0;
    // fast width charpos
    if (OB_FAIL(ObCharset::max_display_width_charpos(cs_type, str_text.ptr(),
                str_text.length(), width, prefix_size, &total_width))) {
      LOG_WARN("Failed to get max display width", K(ret));
    } else if (!ob_is_text_tc(type)) {
      char *result_buf = expr.get_str_res_mem(ctx, prefix_size, idx);
      if (OB_FAIL(padding_vector(pad_type, cs_type, "", 0, str_text.ptr(),
                                 str_text.length(), prefix_size, 0,
                                 (total_width != width), result_buf,
                                 result_size))) {
        LOG_WARN("Failed to pad", K(ret), K(str_text), K(str_pad));
      } else {
        if (OB_UNLIKELY(0 == result_size)) {
          res_vec->set_null(idx);
        } else {
          res_vec->set_string(idx, result_buf, result_size);
        }
      }
    } else {
      char *result_buf = NULL;
      int64_t buf_len = 0;
      ObTextStringVectorResult<ResVec> output_result(type, &expr, &ctx, res_vec, idx);
      if (OB_FAIL(output_result.init_with_batch_idx(prefix_size, idx))) {
        LOG_WARN("init lob result failed", K(ret));
      } else if (OB_FAIL(output_result.get_reserved_buffer(result_buf, buf_len))) {
        LOG_WARN("failed to get reserved buffer", K(ret));
      } else if (OB_FAIL(padding_vector(pad_type, cs_type, "", 0,
                                        str_text.ptr(), str_text.length(),
                                        prefix_size, 0, (total_width != width),
                                        result_buf, result_size))) {
        LOG_WARN("Failed to pad", K(ret), K(str_text), K(str_pad));
      } else if (OB_FAIL(output_result.lseek(buf_len, 0))) {
        LOG_WARN("temp lob lseek failed", K(ret));
      } else {
        if (OB_UNLIKELY(0 == result_size)) {
          res_vec->set_null(idx);
        } else {
          ObString output;
          output_result.get_result_buffer(output);
          res_vec->set_string(idx, output);
        }
      }
    }

  } else {
    bool pad_space = false;
    if (OB_FAIL(get_padding_info_oracle_vector(
            cs_type, str_text, text_width, width, str_pad, pad_width,
            max_result_size, repeat_count, prefix_size, pad_space))) {
      LOG_WARN("Failed to get padding info", K(ret), K(str_text), K(width),
                                              K(str_pad), K(max_result_size));
    } else if (!ob_is_text_tc(type)) {
      result_size = str_text.length() + str_pad.length() * repeat_count + prefix_size +
                    (pad_space ? ObCharsetUtils::get_const_str(cs_type, ' ').length() : 0);
      char *result_buf = expr.get_str_res_mem(ctx, result_size, idx);
      if (OB_FAIL(padding_vector(pad_type, cs_type, str_text.ptr(),
                                 str_text.length(), str_pad.ptr(),
                                 str_pad.length(), prefix_size, repeat_count,
                                 pad_space, result_buf, result_size))) {
        LOG_WARN("Failed to pad", K(ret), K(str_text), K(str_pad));
      } else {
        if (OB_UNLIKELY(0 == result_size)) {
          res_vec->set_null(idx);
        } else {
          res_vec->set_string(idx, result_buf, result_size);
        }
      }
    } else {
      char *result_buf = NULL;
      int64_t buf_len = 0;
      result_size = str_text.length() + str_pad.length() * repeat_count + prefix_size +
                    (pad_space ? ObCharsetUtils::get_const_str(cs_type, ' ').length() : 0);
      ObTextStringVectorResult<ResVec> output_result(type, &expr, &ctx, res_vec, idx);
      if (OB_FAIL(output_result.init_with_batch_idx(result_size, idx))) {
        LOG_WARN("init lob result failed", K(ret));
      } else if (OB_FAIL(output_result.get_reserved_buffer(result_buf, buf_len))) {
        LOG_WARN("failed to get reserved buffer", K(ret));
      } else if (OB_FAIL(padding_vector(pad_type, cs_type, str_text.ptr(),
                                        str_text.length(), str_pad.ptr(),
                                        str_pad.length(), prefix_size, repeat_count,
                                        pad_space, result_buf, result_size))) {
        LOG_WARN("Failed to pad", K(ret), K(str_text), K(str_pad));
      } else if (OB_FAIL(output_result.lseek(buf_len, 0))) {
        LOG_WARN("temp lob lseek failed", K(ret));
      } else {
        if (OB_UNLIKELY(0 == result_size)) {
          res_vec->set_null(idx);
        } else {
          ObString output;
          output_result.get_result_buffer(output);
          res_vec->set_string(idx, output);
        }
      }
    }
  }
  return ret;
}

template <typename ResVec, typename TextVec>
int ObExprBaseLRpad::calc_oracle_vector_optimized(const ObExpr &expr,
                                                  ObEvalCtx &ctx,
                                                  const ObBitVector &skip,
                                                  const EvalBound &bound,
                                                  const LRpadType pad_type,
                                                  const ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  bool is_ascii = expr.args_[0]->get_vector(ctx)->is_batch_ascii();
  int64_t mbmaxlen = 0;
  int64_t max_result_size = -1;
  int64_t len_int = -1;
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  TextVec *text_vec = static_cast<TextVec *>(expr.args_[0]->get_vector(ctx));
  IntegerUniCVec *len_vec = static_cast<IntegerUniCVec *>(expr.args_[1]->get_vector(ctx));
  StrUniCVec *pad_text_vec = expr.arg_cnt_ == 2 ? NULL :
                             static_cast<StrUniCVec *>(expr.args_[2]->get_vector(ctx));
  ObString str_pad;
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ObExprLRPadContext *pad_ctx = NULL;

  // Max VARCHAR2 size is 32767 in Oracle PL/SQL.
  // However, Oracle SQL allow user to determine max VARCHAR2 size through `MAX_STRING_SIZE` parameter.
  // The max VARCHAR2 size is 4000 when `MAX_STRING_SIZE` is set to `STANDARD`, and 32767 when set to `EXTENDED`.
  // OB does not support `MAX_STRING_SIZE` parameter for now, but behaves compatibly with `EXTENDED` mode.
  const ObExprOracleLRpadInfo *info = nullptr;
  int64_t max_varchar2_size = OB_MAX_ORACLE_VARCHAR_LENGTH;
  if (OB_NOT_NULL(info = static_cast<ObExprOracleLRpadInfo *>(expr.extra_info_)) &&
      info->is_called_in_sql_) {
    const int64_t ORACLE_EXTENDED_MAX_VARCHAR_LENGTH = 32767;
    max_varchar2_size = ORACLE_EXTENDED_MAX_VARCHAR_LENGTH;
  }
  max_result_size = max_varchar2_size;

  // get len_int (the second argument of lrpad expr)
  // set len_int to -1 if the result of expr should be set to null
  if (len_vec->is_null(bound.start()) || (expr.arg_cnt_ == 3 && pad_text_vec->is_null(bound.start()))) {
    len_int = -1;
  } else {
    number::ObNumber len_num(len_vec->get_number(bound.start()));
    int64_t decimal_parts = -1;
    if (len_num.is_negative()) {
      len_int = -1;
    } else if (!len_num.is_int_parts_valid_int64(len_int, decimal_parts)) {
      // LOB 最大也就 4G, 这里用 UINT32_MAX.
      // 负数已经被过滤掉了.
      len_int = UINT32_MAX;
    }
  }

  if (len_int <= 0) {
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      if (skip.at(idx) || eval_flags.at(idx)) {
        continue;
      } else {
        res_vec->set_null(idx);
      }
    }
  } else if (OB_FAIL(ObCharset::get_mbmaxlen_by_coll(expr.datum_meta_.cs_type_, mbmaxlen))) {
    LOG_WARN("failed to get mbmaxlen by coll", K(ret));
  }

  // get str_pad
  if (OB_SUCC(ret) && len_int > 0) {
    if (expr.arg_cnt_ == 3) {
      str_pad = pad_text_vec->get_string(bound.start());
    } else if (expr.arg_cnt_ == 2) {
      ObCollationType in_coll = ObCharset::get_system_collation();
      ObCollationType out_coll = expr.datum_meta_.cs_type_;
      ObString pad_str_utf8(1, " ");
      if (OB_FAIL(ObExprUtil::convert_string_collation(pad_str_utf8, in_coll,
                                        str_pad, out_coll, ctx.exec_ctx_.get_allocator()))) {
        LOG_WARN("convert collation failed", K(ret), K(in_coll), K(str_pad), K(out_coll));
      } else if (OB_UNLIKELY(str_pad.empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected pad_str after convert collation", K(ret), K(str_pad));
      }
    }
  }

  // get context
  if (OB_SUCC(ret) && len_int > 0) {
    ObExecContext &exec_ctx = ctx.exec_ctx_;
    uint64_t pad_id = static_cast<uint64_t>(expr.expr_ctx_id_);
    if (NULL == (pad_ctx = static_cast<ObExprLRPadContext *>(exec_ctx.get_expr_op_ctx(pad_id)))) {
      int64_t buffer_size = len_int * mbmaxlen;
      if (OB_FAIL(init_pad_ctx(expr, ctx, exec_ctx, pad_id, str_pad, len_int, buffer_size, false, pad_ctx))) {
        LOG_WARN("failed to init pad ctx", K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && len_int > 0) {
    bool is_left_pad = pad_type == LPAD_TYPE;
    bool can_do_ascii_optimize = storage::can_do_ascii_optimize(expr.datum_meta_.cs_type_);
    if (is_left_pad) {
      if (is_ascii) {
        if (can_do_ascii_optimize) {
          ret = calc_oracle_inner_vector_optimized<ResVec, TextVec, true, true, true>(expr, ctx, skip, bound, mbmaxlen, max_result_size, len_int, str_pad, pad_ctx);
        } else {
          ret = calc_oracle_inner_vector_optimized<ResVec, TextVec, true, true, false>(expr, ctx, skip, bound, mbmaxlen, max_result_size, len_int, str_pad, pad_ctx);
        }
      } else {
        if (can_do_ascii_optimize) {
          ret = calc_oracle_inner_vector_optimized<ResVec, TextVec, true, false, true>(expr, ctx, skip, bound, mbmaxlen, max_result_size, len_int, str_pad, pad_ctx);
        } else {
          ret = calc_oracle_inner_vector_optimized<ResVec, TextVec, true, false, false>(expr, ctx, skip, bound, mbmaxlen, max_result_size, len_int, str_pad, pad_ctx);
        }
      }
    } else {
      if (is_ascii) {
        if (can_do_ascii_optimize) {
          ret = calc_oracle_inner_vector_optimized<ResVec, TextVec, false, true, true>(expr, ctx, skip, bound, mbmaxlen, max_result_size, len_int, str_pad, pad_ctx);
        } else {
          ret = calc_oracle_inner_vector_optimized<ResVec, TextVec, false, true, false>(expr, ctx, skip, bound, mbmaxlen, max_result_size, len_int, str_pad, pad_ctx);
        }
      } else {
        if (can_do_ascii_optimize) {
          ret = calc_oracle_inner_vector_optimized<ResVec, TextVec, false, false, true>(expr, ctx, skip, bound, mbmaxlen, max_result_size, len_int, str_pad, pad_ctx);
        } else {
          ret = calc_oracle_inner_vector_optimized<ResVec, TextVec, false, false, false>(expr, ctx, skip, bound, mbmaxlen, max_result_size, len_int, str_pad, pad_ctx);
        }
      }
    }
  }
  return ret;
}

template <typename ResVec, typename TextVec, bool IsLeftPad, bool IsAscii, bool CanDoAsciiOptimize>
int ObExprBaseLRpad::calc_oracle_inner_vector_optimized(const ObExpr &expr,
                                                        ObEvalCtx &ctx,
                                                        const ObBitVector &skip,
                                                        const EvalBound &bound,
                                                        const int64_t mbmaxlen,
                                                        const int64_t max_result_size,
                                                        const int64_t len_int,
                                                        const ObString &str_pad,
                                                        ObExprLRPadContext *pad_ctx)
{
  int ret = OB_SUCCESS;
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  TextVec *text_vec = static_cast<TextVec *>(expr.args_[0]->get_vector(ctx));
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  const ObCollationType cs_type = expr.datum_meta_.cs_type_;
  int64_t buffer_size = len_int * mbmaxlen;
  int64_t pad_len = pad_ctx->get_pad_len();
  char *pad_buf = pad_ctx->get_pad_buf();
  char **result_bufs = pad_ctx->get_result_buf();
  int64_t *char_pos = pad_ctx->get_char_pos();
  ObString space_str = ObCharsetUtils::get_const_str(cs_type, ' ');
  LRpadType pad_type = IsLeftPad ? LPAD_TYPE : RPAD_TYPE;

  for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
    if (skip.at(idx) || eval_flags.at(idx)) {
      continue;
    } else if (text_vec->is_null(idx)) {
      res_vec->set_null(idx);
    } else {
      ObString str_text = text_vec->get_string(idx);
      char *result_buf = result_bufs[idx];
      int64_t text_width = 0;
      int64_t prefix_size = 0;
      int64_t result_size = 0;
      if (OB_FAIL((ObCharsetStringHelper::fast_display_len<IsAscii, CanDoAsciiOptimize>(cs_type, str_text, text_width)))) {
        LOG_WARN("Failed to get displayed length", K(ret), K(str_text));
      } else if (text_width >= len_int) {
        // substr and pad space
        int64_t total_width = 0;
        if (OB_FAIL((ObCharsetStringHelper::fast_display_charpos<IsAscii, CanDoAsciiOptimize>( cs_type, str_text, len_int, prefix_size, total_width)))) {
          LOG_WARN("Failed to get max display width", K(ret));
        } else if (OB_FAIL(padding_vector(pad_type, cs_type, "", 0, str_text.ptr(),
                                          str_text.length(), prefix_size, 0,
                                          (total_width != len_int), result_buf,
                                          result_size))) {
          LOG_WARN("Failed to pad", K(ret), K(str_text), K(str_pad));
        } else {
          if (OB_UNLIKELY(0 == result_size)) {
            res_vec->set_null(idx);
          } else {
            res_vec->set_string(idx, result_buf, result_size);
          }
        }
      } else {
        bool pad_space = false;
        int64_t text_size = str_text.length();
        int64_t pad_size = str_pad.length();
        int64_t pad_width = pad_len;
        // GOAL: get repeat_count, prefix_size and pad space.
        int64_t repeat_count = (len_int - text_width) / pad_width;
        int64_t remain_width = (len_int - text_width) % pad_width;
        int64_t total_width = 0;
        if (OB_FAIL((ObCharsetStringHelper::fast_display_charpos<false, CanDoAsciiOptimize>(cs_type,
                                                                                                      str_pad,
                                                                                                      remain_width,
                                                                                                      prefix_size,
                                                                                                      total_width)))) {
          LOG_WARN("Failed to get max display width", K(ret), K(str_text), K(remain_width));
        } else {
          if (remain_width != total_width) {
            pad_space = true;
          }
          int64_t total_pad_size = repeat_count * pad_size + prefix_size;
          int64_t result_size = text_size + total_pad_size + (pad_space ? space_str.length(): 0);
          if (OB_UNLIKELY(result_size > max_result_size)) {
            repeat_count = (max_result_size - text_size) / pad_size;
            int64_t remain_width = len_int - (text_width + pad_width * repeat_count);
            int64_t remain_size = max_result_size - (text_size + repeat_count * pad_size);
            int64_t total_width = 0;
            if (remain_width > 0 && remain_size > 0) {
              if (OB_FAIL(ObCharsetStringHelper::fast_display_charpos(cs_type,
                                                                             str_pad.ptr(),
                                                                             std::min<int64_t>(str_pad.length(), remain_size),
                                                                             remain_width,
                                                                             false,
                                                                             prefix_size,
                                                                             total_width))) {
                LOG_WARN("Failed to get max display width", K(ret), K(str_text), K(remain_width));
              } else if (remain_width != total_width && remain_size != prefix_size) {
                pad_space = true;
              }
            }
          } else if (OB_FAIL((padding_vector_oracle_optimized<IsLeftPad>(str_text.ptr(), text_size, pad_buf, total_pad_size, pad_space, space_str, result_buf)))) {
            LOG_WARN("Failed to pad", K(ret), K(str_text), K(str_pad));
          } else {
            res_vec->set_string(idx, result_buf, result_size);
          }
        }
      }
    }
  }
  return ret;
}

/* ObExprLRpadOracle Vector End */

OB_DEF_SERIALIZE(ObExprOracleLRpadInfo)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, is_called_in_sql_);
  return ret;
}

OB_DEF_DESERIALIZE(ObExprOracleLRpadInfo)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, is_called_in_sql_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObExprOracleLRpadInfo)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, is_called_in_sql_);
  return len;
}

int ObExprOracleLRpadInfo::deep_copy(common::ObIAllocator &allocator,
                                     const ObExprOperatorType type,
                                     ObIExprExtraInfo *&copied_info) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObExprExtraInfoFactory::alloc(allocator, type, copied_info))) {
    LOG_WARN("Failed to allocate memory for ObExprOracleLRpadInfo", K(ret));
  } else if (OB_ISNULL(copied_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("extra_info should not be nullptr", K(ret));
  } else {
    ObExprOracleLRpadInfo *other = static_cast<ObExprOracleLRpadInfo *>(copied_info);
    other->is_called_in_sql_ = is_called_in_sql_;
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
