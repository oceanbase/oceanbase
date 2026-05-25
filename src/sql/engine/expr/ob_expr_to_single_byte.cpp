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

#include "sql/engine/expr/ob_expr_to_single_byte.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "lib/charset/ob_charset_string_helper.h"

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
  const ObSQLSessionInfo *session = type_ctx.get_session();
  CK(OB_NOT_NULL(session));
  if (OB_SUCC(ret)) {
    if (lib::is_oracle_mode()) {
      ObArray<ObExprResType *> params;
      OZ (params.push_back(&type1));
      // 推导结果的字符集和长度语义
      OZ (aggregate_string_type_and_charset_oracle(*session,
                                                   params,
                                                   type,
                                                   PREFER_VAR_LEN_CHAR));
      // clob 是一种比较特殊的类型，当参数为clob时，结果为varchar2, byte 语义
      if (OB_SUCC(ret) && type1.is_clob()) {
        type.set_varchar();
        type.set_length_semantics(LS_BYTE);
      }
      // 设置参数的 calc_type
      OZ (deduce_string_param_calc_type_and_charset(*session,
                                                    type,
                                                    params));
      // 推导结果的长度
      OX (type.set_length(type1.get_calc_length()));
    } else {
      if (ObTinyTextType == type1.get_type()) {
        type.set_type(ObVarcharType);
      } else if (type1.is_lob()) {
        type.set_type(ObLongTextType);
      } else {
        type.set_varchar();
      }
      type1.set_calc_type(type.get_type());
      OZ (aggregate_charsets_for_string_result(type, &type1, 1, type_ctx));
      OX (type1.set_calc_collation_type(type.get_collation_type()));
      OX (type1.set_calc_collation_level(type.get_collation_level()));
      OX (type.set_length(type1.get_length()));
    }
  }
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
    char *ptr = buf;
    struct Functor {
      Functor(const ObCollationType cs_type, char *&ptr, char *&buf, const int64_t buf_len)
          : cs_type(cs_type), ptr(ptr), buf(buf), buf_len(buf_len) {}
      const ObCollationType cs_type;
      char *&ptr;
      char *&buf;
      const int64_t buf_len;
      int operator() (const ObString &encoding, ob_wc_t wc) {
        int ret = OB_SUCCESS;
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

        return ret;
      };
    };

    Functor temp_handler(cs_type, ptr, buf, buf_len);
    ObCharsetType charset_type = ObCharset::charset_type_by_coll(cs_type);
    OZ(ObFastStringScanner::foreach_char(input, charset_type, temp_handler));
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
  ObString src;
  ObString result;
  const bool is_text_result = ob_is_text_tc(expr.datum_meta_.type_);
  if (OB_FAIL(expr.args_[0]->eval(ctx, src_param))) {
    LOG_WARN("eval arg failed", K(ret));
  } else if (src_param->is_null()) {
    res_datum.set_null();
  } else {
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    src = src_param->get_string();
    if (ob_is_text_tc(expr.args_[0]->datum_meta_.type_)
        && OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator,
                                                             *src_param,
                                                             expr.args_[0]->datum_meta_,
                                                             expr.args_[0]->obj_meta_.has_lob_header(),
                                                             src))) {
      LOG_WARN("failed to get real string data", K(ret), K(expr.args_[0]->datum_meta_));
    } else if (src.empty()) {
      if (is_text_result) {
        ObTextStringDatumResult empty_result(expr.datum_meta_.type_, &expr, &ctx, &res_datum);
        if (OB_FAIL(empty_result.init(0))) {
          LOG_WARN("init empty lob result failed", K(ret));
        } else {
          empty_result.set_result();
        }
      } else {
        res_datum.set_string(ObString());
      }
    } else {
      char *buf = NULL;
      int64_t buf_len = src.length();
      int32_t length = 0;
      if (is_text_result
          && OB_ISNULL(buf = static_cast<char *>(temp_allocator.alloc(buf_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate temp memory", K(ret), K(src), K(buf_len));
      } else if (!is_text_result
                 && OB_ISNULL(buf = static_cast<char *>(expr.get_str_res_mem(ctx, buf_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", K(ret), K(src));
      } else if (OB_FAIL(calc_to_single_byte_expr(src,
                                                  expr.args_[0]->datum_meta_.cs_type_,
                                                  buf,
                                                  buf_len,
                                                  length))) {
        LOG_WARN("fail to calc unistr", K(ret));
      } else if (FALSE_IT(result.assign_ptr(buf, length))) {
      } else if (is_text_result) {
        if (OB_FAIL(ObTextStringHelper::string_to_templob_result(expr, ctx, res_datum, result))) {
          LOG_WARN("set lob result failed", K(ret), K(expr.datum_meta_.type_));
        }
      } else {
        res_datum.set_string(result);
      }
    }
  }
  return ret;
}

}
}
