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
#include "ob_expr_reverse.h"
#include <string.h>
#include "lib/charset/ob_charset.h"
#include "share/object/ob_obj_cast.h"
#include "objit/common/ob_item_type.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprReverse::ObExprReverse(ObIAllocator &alloc) :
    ObStringExprOperator(alloc, T_FUN_SYS_REVERSE, "reverse", 1, VALID_FOR_GENERATED_COL)
{
}

ObExprReverse::~ObExprReverse()
{
}

int ObExprReverse::do_reverse(const ObString &input_str,
                              const ObCollationType &cs_type,
                              ObIAllocator *allocator, // make sure alloc() is called once
                              ObString &res_str)
{
  int ret = OB_SUCCESS;
  const char * input_start = input_str.ptr();
  int64_t input_length = input_str.length();
  char *buf = NULL;
  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("nullptr allocator.", K(allocator));
  } else if (OB_UNLIKELY(input_length == 0)) {
    res_str.reset();
  } else if (OB_ISNULL(input_start)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid string, buf is null", K(ret));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator->alloc(input_length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc memory failed. ", "size", input_length);
  } else {
    int64_t converted_length = 0;
    int64_t char_begin = 0;
    int64_t char_length = 0;
    char *buf_tail = buf + input_length;
    while (OB_SUCC(ret) && (converted_length < input_length)) {
      if (lib::is_mysql_mode() && OB_FAIL(ObCharset::first_valid_char(cs_type,
          input_start + char_begin,
          input_length - converted_length,
          char_length))) {
        LOG_WARN("Get first valid char failed ", K(ret));
      } else if (lib::is_oracle_mode() && FALSE_IT(char_length = 1)) {
        // Oracle reverse string by single byte
      } else {
        MEMCPY(buf_tail - char_length, input_start + char_begin, char_length);
        buf_tail -= char_length;
        converted_length += char_length;
        char_begin += char_length;
      }
    }
    if (OB_SUCC(ret)) {
      res_str.assign_ptr(buf, static_cast<int32_t>(input_length));
    }
  }
  return ret;
}

int calc_reverse_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *arg = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, arg))) {
    LOG_WARN("eval arg failed", K(ret));
  } else if (arg->is_null()) {
    res_datum.set_null();
  } else {
    const ObString &arg_str = arg->get_string();
    const ObCollationType &arg_cs_type = expr.args_[0]->datum_meta_.cs_type_;
    ObString res_str;
    if (!ob_is_text_tc(expr.args_[0]->datum_meta_.type_)) {
      ObExprStrResAlloc res_alloc(expr, ctx);
      if (OB_FAIL(ObExprReverse::do_reverse(arg_str, arg_cs_type, &res_alloc, res_str))) {
        LOG_WARN("do_reverse failed", K(ret), K(arg_str), K(arg_cs_type));
      } else {
        // expr reverse is in mysql mode. no need to check res_str.empty()
        res_datum.set_string(res_str);
      }
    } else { // text tc
      ObEvalCtx::TempAllocGuard alloc_guard(ctx);
      ObIAllocator &calc_alloc = alloc_guard.get_allocator();
      char *buf;
      int64_t buf_size = 0;
      int64_t total_byte_len = 0;
      const bool has_lob_header = expr.args_[0]->obj_meta_.has_lob_header();
      ObTextStringIter input_iter(expr.args_[0]->datum_meta_.type_, arg_cs_type, arg->get_string(), has_lob_header);
      ObTextStringDatumResult output_result(expr.datum_meta_.type_, &expr, &ctx, &res_datum);
      if (OB_FAIL(input_iter.init(0, NULL, &calc_alloc))) {
        LOG_WARN("init input_iter failed ", K(ret), K(input_iter));
      } else if (OB_FAIL(input_iter.get_byte_len(total_byte_len))) {
        LOG_WARN("get input byte len failed", K(ret));
      } else if (OB_FAIL(output_result.init(total_byte_len))) {
        LOG_WARN("init stringtext result failed", K(ret));
      } else if (total_byte_len == 0) {
        output_result.set_result();
      } else if (OB_FAIL(output_result.get_reserved_buffer(buf, buf_size))) {
        LOG_WARN("stringtext result reserve buffer failed", K(ret));
      } else {
        ObTextStringIterState state;
        ObString src_block_data;
        input_iter.set_backward();
        while (OB_SUCC(ret)
               && buf_size > 0
               && (state = input_iter.get_next_block(src_block_data)) == TEXTSTRING_ITER_NEXT) {
          ObDataBuffer data_buf(buf, buf_size);
          if (OB_FAIL(ObExprReverse::do_reverse(src_block_data, arg_cs_type, &data_buf, res_str))) {
            LOG_WARN("do_reverse failed", K(ret), K(arg_str), K(arg_cs_type));
          } else if (OB_FAIL(output_result.lseek(res_str.length(), 0))) {
            LOG_WARN("result lseek failed", K(ret));
          } else {
            buf += res_str.length();
            buf_size -= res_str.length();
          }
        }
        if (OB_FAIL(ret)) {
        } else if (state != TEXTSTRING_ITER_NEXT && state != TEXTSTRING_ITER_END) {
          ret = (input_iter.get_inner_ret() != OB_SUCCESS) ?
                input_iter.get_inner_ret() : OB_INVALID_DATA;
          LOG_WARN("iter state invalid", K(ret), K(state), K(input_iter));
        } else {
          output_result.set_result();
        }
      }
    }
  }
  return ret;
}

int ObExprReverse::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                               ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_reverse_expr;
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprReverse, raw_expr) {
  int ret = OB_SUCCESS;
  SET_LOCAL_SYSVAR_CAPACITY(1);
  EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_COLLATION_CONNECTION);
  return ret;
}

}
}
