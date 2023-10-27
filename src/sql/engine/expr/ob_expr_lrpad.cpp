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
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/engine/ob_exec_context.h"
#include <limits.h>
#include <string.h>

#include "sql/session/ob_sql_session_info.h"
#include "lib/worker.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

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
                                            const ObBasicSessionInfo *session,
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
  if (OB_NOT_NULL(session) && OB_FAIL(session->get_max_allowed_packet(max_result_size))) {
    if (OB_ENTRY_NOT_EXIST == ret) { // for compatibility with server before 1470
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("Failed to get max allow packet size", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    //do nothing
  } else if (OB_FAIL(ObExprUtil::get_round_int64(len, expr_ctx, int_len))) {
    LOG_WARN("get_round_int64 failed and ignored", K(ret));
    ret = OB_SUCCESS;
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
                result_type.get_collation_type(), str_text, int_len, str_pad,
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
    //do nothing
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
  } else if (expr->get_children_count() >= 2 && OB_NOT_NULL(expr = expr->get_param_expr(1))
             && expr->get_expr_type() == T_FUN_SYS_CAST && CM_IS_IMPLICIT_CAST(expr->get_extra())) {
    do {
      if (expr->get_children_count() >= 1
          && OB_ISNULL(expr = expr->get_param_expr(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get_param_expr", K(ret));
      }
    } while (OB_SUCC(ret) && T_FUN_SYS_CAST == expr->get_expr_type()
             && CM_IS_IMPLICIT_CAST(expr->get_extra()));
    if (OB_SUCC(ret)) {
      len_obj = expr->get_result_type().get_param();
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
      OZ(aggregate_charsets_for_string_result(type, &types.at(0), 2,
                                              type_ctx.get_coll_type()));
      OX(text.set_calc_collation_type(type.get_collation_type()));
      OX(pad_text->set_calc_collation_type(type.get_collation_type()));
      if (OB_SUCC(ret)) {
        // len expr may add cast, search real len obj
        if (OB_FAIL(get_origin_len_obj(length_obj))) {
          LOG_WARN("fail to get ori len obj", K(ret));
        } else if (!length_obj.is_null()) {
          if (OB_FAIL(calc_type_length_mysql(type, text_obj, pad_obj, length_obj, type_ctx.get_session(), text_len))) {
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

int ObExprBaseLRpad::calc(const LRpadType type,
                          const ObObj &text,
                          const ObObj &len,
                          const ObObj &pad_text,
                          ObExprCtx &expr_ctx,
                          ObObj &result) const
{
  int ret = OB_SUCCESS;
  if (lib::is_mysql_mode()) {
    if (OB_FAIL(calc_mysql(type, result_type_, text, len, pad_text, expr_ctx.my_session_, expr_ctx.calc_buf_, result))) {
      LOG_WARN("Failed to calc mysql", K(type), K(text), K(len), K(pad_text),
               K(expr_ctx.my_session_), K(expr_ctx.calc_buf_));
    }
  } else if (lib::is_oracle_mode()) {
    if (OB_FAIL(calc_oracle(type, result_type_, text, len, pad_text, expr_ctx, result))) {
      LOG_WARN("Failed to calc oracle", K(type), K(text), K(len), K(pad_text), K(expr_ctx.calc_buf_));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error compat mode", K(ret));
  }
  return ret;
}
/* common util END }}} */

/* mysql util {{{2 */
int ObExprBaseLRpad::calc_mysql(const LRpadType type,
                                const ObExprResType result_type,
                                const ObObj &text,
                                const ObObj &len,
                                const ObObj &pad_text,
                                const ObSQLSessionInfo *session,
                                ObIAllocator *allocator,
                                ObObj &result)
{
  int ret = OB_SUCCESS;
  int64_t max_result_size = -1;
  int64_t int_len = 0;
  int64_t repeat_count = 0;
  int64_t prefix_size = 0;

  int64_t text_len = 0;

  ObString str_text;
  ObString str_pad;

  char *result_ptr = NULL;
  int64_t result_size = 0;

  if (OB_ISNULL(session)|| OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null session or allocator in Lpad function", K(ret), K(session), K(allocator));
  } else if (OB_FAIL(session->get_max_allowed_packet(max_result_size))) {
    if (OB_ENTRY_NOT_EXIST == ret) { // for compatibility with server before 1470
      ret = OB_SUCCESS;
      max_result_size = OB_MAX_VARCHAR_LENGTH;
    } else {
      LOG_WARN("Failed to get max allow packet size", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (text.is_null() || len.is_null() || pad_text.is_null()) {
    result.set_null();
  } else {
    TYPE_CHECK(len, ObIntType);
    bool has_lob_header = result_type.has_lob_header();
    if (OB_FAIL(len.get_int(int_len))) {
      LOG_WARN("Failed to get len", K(ret), K(len));
    } else if (int_len < 0) {
      result.set_null();
    } else if (int_len == 0) {
      result.set_varchar(ObString::make_empty_string());
      result.set_collation(result_type);
    } else if (!ob_is_text_tc(text.get_type()) && !ob_is_text_tc(pad_text.get_type())) {
      if (OB_FAIL(text.get_string(str_text))) {
        LOG_WARN("Failed to get str_text", K(ret), K(text));
      } else if (OB_FAIL(pad_text.get_string(str_pad))) {
        LOG_WARN("Failed to get str_text", K(ret), K(pad_text));
      }
    } else { // text tc
      str_text = text.get_string();
      str_pad = pad_text.get_string();
      if (OB_FAIL(ObTextStringHelper::read_real_string_data(allocator, text, str_text))) {
        LOG_WARN("failed to read real text", K(ret), K(str_text));
      } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(allocator, pad_text, str_pad))) {
        LOG_WARN("failed to read real pattern", K(ret), K(str_pad));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (FALSE_IT(text_len = ObCharset::strlen_char(
                result_type.get_collation_type(),
                const_cast<const char *>(str_text.ptr()),
                str_text.length()))) {
      LOG_WARN("Failed to get displayed length", K(ret), K(str_text));
    } else if (text_len >= int_len ) {
      // only substr needed
      result_size = ObCharset::charpos(result_type.get_collation_type(), str_text.ptr(), str_text.length(), int_len);
      result_ptr = static_cast<char*>(allocator->alloc(result_size));
      if (OB_ISNULL(result_ptr)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Failed to alloc", K(ret));
      } else {
        MEMCPY(result_ptr, str_text.ptr(), result_size);
        result.set_varchar(result_ptr, static_cast<ObString::obstr_size_t>(result_size));
        result.set_collation(result_type);
      }
    } else if (str_pad.length() == 0) {
      result.set_null();
    } else if (OB_FAIL(get_padding_info_mysql(
                result_type.get_collation_type(), str_text, int_len, str_pad,
                max_result_size, repeat_count, prefix_size, result_size))) {
      LOG_WARN("Failed to get padding info", K(ret), K(str_text), K(int_len), K(str_pad), K(max_result_size));
    } else if (result_size > max_result_size) {
      result.set_null();
      if (type == RPAD_TYPE) {
        LOG_USER_WARN(OB_ERR_FUNC_RESULT_TOO_LARGE, "rpad", static_cast<int>(max_result_size));
      } else {
        LOG_USER_WARN(OB_ERR_FUNC_RESULT_TOO_LARGE, "lpad", static_cast<int>(max_result_size));
      }
    } else if (OB_FAIL(padding(type, result_type.get_collation_type(),
                               str_text.ptr(), str_text.length(), str_pad.ptr(), str_pad.length(),
                               prefix_size, repeat_count, false, allocator, result_ptr, result_size,
                               result_type.get_type(), has_lob_header))) {
      LOG_WARN("Failed to pad", K(ret), K(str_text), K(str_pad), K(prefix_size), K(repeat_count));
    } else {
      result.set_string(result_type.get_type(), result_ptr, static_cast<ObString::obstr_size_t>(result_size));
      result.set_collation(result_type);
      if (has_lob_header) {
        result.set_has_lob_header();
      }
    }
  }
  return ret;
}

int ObExprBaseLRpad::get_padding_info_mysql(const ObCollationType &cs,
                                            const ObString &str_text,
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
  int64_t text_len = ObCharset::strlen_char(cs, const_cast<const char *>(str_text.ptr()), str_text.length());
  int64_t pad_len = ObCharset::strlen_char(cs, const_cast<const char *>(str_padtext.ptr()), str_padtext.length());

  if (OB_UNLIKELY(len <= text_len)
             || OB_UNLIKELY(len <= 0)
             || OB_UNLIKELY(pad_len <= 0)
             || OB_UNLIKELY(pad_size <= 0)) {
    // this should been resolve outside
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wrong len", K(ret), K(len), K(text_len));
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
  } else {
    has_set_to_lob_locator = true;
    if (str_pad.length() == 0) {
      res.set_string(ObString::make_empty_string());
    } else if (OB_FAIL(get_padding_info_mysql(cs_type, str_text, int_len, str_pad,
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
  if (OB_SUCC(ret) && ob_is_text_tc(type) && !res.is_null() &&
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
  if (OB_FAIL(session.get_max_allowed_packet(max_result_size))) {
    if (OB_ENTRY_NOT_EXIST == ret) { // for compatibility with server before 1470
      ret = OB_SUCCESS;
      max_result_size = OB_MAX_VARCHAR_LENGTH;
    } else {
      LOG_WARN("Failed to get max allow packet size", K(ret));
    }
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

/* oracle util {{{2 */
int ObExprBaseLRpad::calc_oracle(const LRpadType type,
                                 const ObExprResType result_type,
                                 const ObObj &text,
                                 const ObObj &len,
                                 const ObObj &pad_text,
                                 common::ObExprCtx &expr_ctx,
                                 ObObj &result)
{
  int ret = OB_SUCCESS;
  int64_t max_result_size = result_type.is_lob()? OB_MAX_LONGTEXT_LENGTH: OB_MAX_ORACLE_VARCHAR_LENGTH;
  int64_t width = 0;
  int64_t repeat_count = 0;
  int64_t prefix_size = 0;
  bool pad_space = false;

  int64_t text_width = 0;

  ObString str_text;
  ObString str_pad;

  char *result_ptr = NULL;
  int64_t result_size = 0;

  ObIAllocator *allocator = expr_ctx.calc_buf_;
  int64_t tmp_pad_len = 0;

  if (text.is_null_oracle() || len.is_null_oracle() || pad_text.is_null_oracle()) {
    result.set_null();
  } else if (OB_UNLIKELY(!text.is_string_type()) // clob or varchar
             || OB_UNLIKELY(!len.is_number()) // number
             || OB_UNLIKELY(!pad_text.is_string_type())
             || OB_ISNULL(allocator)) { // clob or varchar
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Wrong param", K(ret), K(text.get_type()), K(len.get_type()), K(pad_text.get_type()), K(allocator));
  } else if (text.is_blob()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Blob type in LRpad");
    LOG_WARN("Blob type in LRpad not supported", K(ret), K(text.get_type()));
  } else if (OB_FAIL(ObExprUtil::get_trunc_int64(len, expr_ctx, tmp_pad_len))) {
    LOG_WARN("fail to get pad_len", K(ret), K(len));
  } else if (text.is_clob() && pad_text.is_clob() && (0 == pad_text.val_len_)
             && (text.val_len_ <= tmp_pad_len)) {
    // pad_text 是 empty_clob，text 是 clob，如果不走截断逻辑的话，结果直接置为原 clob
    result.set_string(result_type.get_type(), text.get_string());
    result.set_collation(result_type);
  } else {
    /* get length */
    number::ObNumber len_num;
    int64_t decimal_parts = -1;
    if (OB_FAIL(len.get_number(len_num))) {
      LOG_WARN("failed to get length", K(ret));
    } else if (len_num.is_negative()) {
      width = -1;
    } else if (!len_num.is_int_parts_valid_int64(width, decimal_parts)) {
      // LOB 最大也就 4G, 这里用 UINT32_MAX.
      // 负数已经被过滤掉了.
      width = UINT32_MAX;
    }

    if (OB_FAIL(ret)) {
    } else if (width <= 0) {
      result.set_null();
    } else if (!ob_is_text_tc(text.get_type()) && !ob_is_text_tc(pad_text.get_type())) {
      if (OB_FAIL(text.get_string(str_text))) {
        LOG_WARN("Failed to get str_text", K(ret), K(text));
      } else if (OB_FAIL(pad_text.get_string(str_pad))) {
        LOG_WARN("Failed to get str_text", K(ret), K(pad_text));
      }
    } else { // text tc
      str_text = text.get_string();
      str_pad = pad_text.get_string();
      if (OB_FAIL(ObTextStringHelper::read_real_string_data(allocator, text, str_text))) {
        LOG_WARN("failed to read real text", K(ret), K(str_text));
      } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(allocator, pad_text, str_pad))) {
        LOG_WARN("failed to read real pattern", K(ret), K(str_pad));
      }
    }
    bool has_lob_header = result_type.has_lob_header();
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObCharset::display_len(result_type.get_collation_type(), str_text, text_width))) {
      LOG_WARN("Failed to get displayed length", K(ret), K(str_text));
    } else {
      if (text_width == width) {
        result = text;
        result_ptr = const_cast<char*>(text.get_string_ptr());
        result_size = text.get_string_len();
      } else if (text_width > width) {
        // substr
        int64_t total_width = 0;
        if (OB_FAIL(ObCharset::max_display_width_charpos(
                    result_type.get_collation_type(), str_text.ptr(), str_text.length(),
                    width, prefix_size, &total_width))) {
          LOG_WARN("Failed to get max display width", K(ret));
        } else if (OB_FAIL(padding(type, result_type.get_collation_type(),
                                   "", 0, str_text.ptr(), str_text.length(),
                                   prefix_size, 0, (total_width != width), allocator, result_ptr,
                                   result_size, result_type.get_type(), has_lob_header))) {
          LOG_WARN("Failed to pad", K(ret), K(str_text), K(str_pad), K(prefix_size), K(repeat_count), K(pad_space));
        }
      } else if (OB_FAIL(get_padding_info_oracle(
                  result_type.get_collation_type(), str_text, width, str_pad,
                  max_result_size, repeat_count, prefix_size, pad_space))) {
        LOG_WARN("Failed to get padding info", K(ret), K(str_text), K(width), K(str_pad), K(max_result_size));
      } else if (OB_FAIL(padding(type, result_type.get_collation_type(),
                                 str_text.ptr(), str_text.length(), str_pad.ptr(), str_pad.length(),
                                 prefix_size, repeat_count, pad_space, allocator, result_ptr,
                                 result_size, result_type.get_type(), has_lob_header))) {
        LOG_WARN("Failed to pad", K(ret), K(str_text), K(str_pad), K(prefix_size), K(repeat_count), K(pad_space));
      }

      if (OB_SUCC(ret)) {
        result.set_string(result_type.get_type(), result_ptr, static_cast<ObString::obstr_size_t>(result_size));
        result.set_collation(result_type);
        if (has_lob_header) {
          result.set_has_lob_header();
        }
      }
    }
  }
  return ret;
}

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
  return ret;
}

int ObExprLpad::calc_mysql_lpad_expr(const ObExpr &expr, ObEvalCtx &ctx,
                                          ObDatum &res)
{
  return calc_mysql_pad_expr(expr, ctx, LPAD_TYPE, res);
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
  return ret;
}

int ObExprRpad::calc_mysql_rpad_expr(const ObExpr &expr, ObEvalCtx &ctx,
                                           ObDatum &res)
{
  return calc_mysql_pad_expr(expr, ctx, RPAD_TYPE, res);
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
      LOG_WARN("NULL param", K(ret), K(types_array[0]), K(types_array[1]), K(types_array[2]));
    } else if (OB_FAIL(ObExprBaseLRpad::calc_type(
                type, types_array[0], types_array[1], types_array + 2, type_ctx))) {
      LOG_WARN("Failed to calc_type", K(ret));
    }
  } else if (param_num == 2) {
    if (OB_ISNULL(types_array)
        || OB_ISNULL(types_array + 1)) {
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
    }
  }
  return ret;
}

int ObExprOracleLpad::calc_oracle_lpad_expr(const ObExpr &expr, ObEvalCtx &ctx,
                                           ObDatum &res)
{
  return calc_oracle_pad_expr(expr, ctx, LPAD_TYPE, res);
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
      LOG_WARN("NULL param", K(ret), K(types_array[0]), K(types_array[1]), K(types_array[2]));
    } else if (OB_FAIL(ObExprBaseLRpad::calc_type(
                type, types_array[0], types_array[1], &(types_array[2]), type_ctx))) {
      LOG_WARN("Failed to calc_type", K(ret));
    }
  } else if (param_num == 2) {
    if (OB_ISNULL(types_array)
        || OB_ISNULL(types_array + 1)) {
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
    }
  }
  return ret;
}

int ObExprOracleRpad::calc_oracle_rpad_expr(const ObExpr &expr, ObEvalCtx &ctx,
                                           ObDatum &res)
{
  return calc_oracle_pad_expr(expr, ctx, RPAD_TYPE, res);
}
/* ObExprRpadOracle END }}} */

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
