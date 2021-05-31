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
#include "sql/parser/ob_item_type.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase {
using namespace common;
namespace sql {

ObExprReverse::ObExprReverse(ObIAllocator& alloc) : ObStringExprOperator(alloc, T_FUN_SYS_REVERSE, "reverse", 1)
{}

ObExprReverse::~ObExprReverse()
{}

int ObExprReverse::calc_result1(common::ObObj& result, const common::ObObj& obj1, common::ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (obj1.is_null()) {
    result.set_null();
  } else {
    TYPE_CHECK(obj1, ObVarcharType);
    ObString res_str;
    if (OB_FAIL(do_reverse(obj1.get_string(), obj1.get_collation_type(), expr_ctx.calc_buf_, res_str))) {
      LOG_WARN("Failed to calc", K(ret));
    } else {
      result.set_varchar(res_str);
      result.set_collation(result_type_);
    }
  }
  return ret;
}

int ObExprReverse::do_reverse(const ObString& input_str, const ObCollationType& cs_type,
    ObIAllocator* allocator,  // make sure alloc() is called once
    ObString& res_str)
{
  int ret = OB_SUCCESS;
  const char* input_start = input_str.ptr();
  int64_t input_length = input_str.length();
  char* buf = NULL;
  if (OB_ISNULL(allocator)) {
    LOG_WARN("nullptr allocator.", K(allocator));
  } else if (OB_UNLIKELY(input_length == 0)) {
    res_str.reset();
  } else if (OB_ISNULL(input_start)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid string, buf is null", K(ret));
  } else if (OB_ISNULL(buf = static_cast<char*>(allocator->alloc(input_length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc memory failed. ", "size", input_length);
  } else {
    int64_t converted_length = 0;
    int64_t char_begin = 0;
    int64_t char_length = 0;
    char* buf_tail = buf + input_length;
    while (OB_SUCC(ret) && (converted_length < input_length)) {
      if (OB_FAIL(ObCharset::first_valid_char(
              cs_type, input_start + char_begin, input_length - converted_length, char_length))) {
        LOG_WARN("Get first valid char failed ", K(ret));
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

int calc_reverse_expr(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* arg = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, arg))) {
    LOG_WARN("eval arg failed", K(ret));
  } else if (arg->is_null()) {
    res_datum.set_null();
  } else {
    const ObString& arg_str = arg->get_string();
    const ObCollationType& arg_cs_type = expr.args_[0]->datum_meta_.cs_type_;
    ObString res_str;
    ObExprStrResAlloc res_alloc(expr, ctx);
    if (OB_FAIL(ObExprReverse::do_reverse(arg_str, arg_cs_type, &res_alloc, res_str))) {
      LOG_WARN("do_reverse failed", K(ret), K(arg_str), K(arg_cs_type));
    } else {
      // expr reverse is in mysql mode. no need to check res_str.empty()
      res_datum.set_string(res_str);
    }
  }
  return ret;
}

int ObExprReverse::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_reverse_expr;
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
