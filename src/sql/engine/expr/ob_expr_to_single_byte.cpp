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

#include "lib/ob_name_def.h"
#include "sql/engine/expr/ob_expr_to_single_byte.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace sql
{

ObExprToSingleByte::ObExprToSingleByte(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_TO_SINGLE_BYTE, N_TO_SINGLE_BYTE, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}
ObExprToSingleByte::~ObExprToSingleByte()
{
}

int ObExprToSingleByte::calc_result_type1(ObExprResType &type,
                                          ObExprResType &type1,
                                          ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  ObArray<ObExprResType *> params;
  ObLength length = 0;
  CK(OB_NOT_NULL(type_ctx.get_session()));
  OZ (params.push_back(&type1));
  // 推导结果的字符集和长度语义
  OZ (aggregate_string_type_and_charset_oracle(*type_ctx.get_session(),
                                               params,
                                               type,
                                               PREFER_VAR_LEN_CHAR));
  // clob 是一种比较特殊的类型，当参数为clob时，结果为varchar2, byte 语义
  if (OB_SUCC(ret) && type1.is_clob()) {
    type.set_varchar();
    type.set_length_semantics(LS_BYTE);
  }
  // 设置参数的 calc_type
  OZ (deduce_string_param_calc_type_and_charset(*type_ctx.get_session(),
                                                type,
                                                params));
  // 推导结果的长度
  OX (type.set_length(type1.get_calc_length()));
  return ret;
}

int calc_to_single_byte_expr(const ObString &input, const ObCollationType cs_type,
                             char *buf, const int64_t buf_len, int32_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t min_char_width = 0;

  if (OB_FAIL(ObCharset::get_mbminlen_by_coll(cs_type, min_char_width))) {
    LOG_WARN("fail to get mbminlen", K(ret));
  } else if (min_char_width > 1) {
    ObDataBuffer allocator(buf, buf_len);
    ObString output;

    if (OB_FAIL(ob_write_string(allocator, input, output))) {
      LOG_WARN("invalid input value", K(ret), K(buf_len), K(input));
    } else {
      pos = output.length();
    }
  } else {
    ObStringScanner scanner(input, cs_type);
    ObString encoding;
    int32_t wc = 0;
    char *ptr = buf;

    while (OB_SUCC(ret)
           && scanner.next_character(encoding, wc, ret)) {
      int32_t length = 0;

      if (wc == 0x3000) { //处理空格
        wc = 0x20;
      //smart quote not support https://gerry.lamost.org/blog/?p=295757
      //} else if (wc == 0x201D) { // ” --> " smart double quote
      //  wc = 0x22;
      //} else if (wc == 0x2019) { // ’ --> ' smart single quote
      //  wc = 0x27;
      } else if (wc >= 0xFF01 && wc <= 0xFF5E) {
        wc -= 65248;
      } else {
        //do nothing
      }
      OZ (ObCharset::wc_mb(cs_type, wc, ptr, buf + buf_len - ptr, length));
      ptr += length;
      LOG_DEBUG("process char", K(ret), K(wc));
    }
    pos = ptr - buf;
  }

  return ret;
}

int ObExprToSingleByte::cg_expr(ObExprCGCtx &op_cg_ctx,
                                const ObRawExpr &raw_expr,
                                ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_to_single_byte;
  return ret;
}

int ObExprToSingleByte::calc_to_single_byte(const ObExpr &expr,
                                            ObEvalCtx &ctx,
                                            ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *src_param = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, src_param))) {
    LOG_WARN("eval arg failed", K(ret));
  } else {
    if (src_param->is_null()) {
      res_datum.set_null();
    } else {
      ObString src = src_param->get_string();
      char *buf = NULL;
      int64_t buf_len = src.length();
      int32_t length = 0;

      if (OB_ISNULL(buf = static_cast<char*>(expr.get_str_res_mem(ctx, buf_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", K(ret), K(src));
      } else if (OB_FAIL(calc_to_single_byte_expr(src, expr.args_[0]->datum_meta_.cs_type_,
                                                  buf, buf_len, length))) {
         LOG_WARN("fail to calc unistr", K(ret));
      } else {
        res_datum.set_string(buf, length);
      }
    }
  }
  return ret;
}

}
}
