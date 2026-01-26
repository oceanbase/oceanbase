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
#include "ob_expr_quote.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprQuote::ObExprQuote(ObIAllocator &alloc)
    : ObStringExprOperator(alloc, T_OP_QUOTE, N_QUOTE, 1, VALID_FOR_GENERATED_COL)
{
}

ObExprQuote::~ObExprQuote()
{
}

int ObExprQuote::calc_result_type1(ObExprResType &type, ObExprResType &type1,
                                   ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  if (is_oracle_mode()) {
    const ObSQLSessionInfo *session = type_ctx.get_session();
    if (OB_ISNULL(session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session is NULL", K(ret));
    } else {
      ObSEArray<ObExprResType*, 1, ObNullAllocator> params;
      type.set_length(2 * type1.get_length() + 2);
      OZ(params.push_back(&type1));
      OZ(aggregate_string_type_and_charset_oracle(*session, params, type));
      OZ(deduce_string_param_calc_type_and_charset(*session, type, params));
    }
  } else {
    type.set_varchar();
    type.set_length(2 * type1.get_length() + 2);
    if OB_FAIL(aggregate_charsets_for_string_result(type, &type1, 1, type_ctx)) {
      LOG_WARN("aggregate charset for res failed", K(ret));
    } else {
      type1.set_calc_type(ObVarcharType);
      type1.set_calc_collation_type(type.get_collation_type());
      type1.set_calc_collation_level(type.get_collation_level());
    }
  }
  return ret;
}

int ObExprQuote::string_write_buf(const ObString &str, char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(!str.empty())) {
    if (OB_UNLIKELY(pos + str.length() > buf_len)) {
      ret = OB_SIZE_OVERFLOW;
    } else {
      MEMCPY(buf + pos, str.ptr(), str.length());
      pos += str.length();
    }
  }
  return ret;
}

// Common helper for escaping special characters
int ObExprQuote::escape_char_in_string(int wchar, const ObString &escape_char,
                                        const ObString &quote_char, ObCollationType coll_type,
                                        const ObString &code_point, char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  switch (wchar) {
  case '\0': {
    OZ(string_write_buf(escape_char, buf, buf_len, pos));
    OZ(string_write_buf(ObCharsetUtils::get_const_str(coll_type, '0'), buf, buf_len, pos));
    break;
  }
  case '\'': {
    OZ(string_write_buf(escape_char, buf, buf_len, pos));
    OZ(string_write_buf(quote_char, buf, buf_len, pos));
    break;
  }
  case '\\': {
    OZ(string_write_buf(escape_char, buf, buf_len, pos));
    OZ(string_write_buf(escape_char, buf, buf_len, pos));
    break;
  }
  case '\032': {
    OZ(string_write_buf(escape_char, buf, buf_len, pos));
    OZ(string_write_buf(ObCharsetUtils::get_const_str(coll_type, 'Z'), buf, buf_len, pos));
    break;
  }
  default: {
    OZ(string_write_buf(code_point, buf, buf_len, pos));
    break;
  }
  }
  return ret;
}

int ObExprQuote::calc(ObString &res_str, ObString str, ObCollationType coll_type,
                      ObIAllocator *allocator) // make sure alloc() is called once
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator)) {
    ret = OB_NOT_INIT;
    LOG_WARN("calc buf is NULL", K(ret));
  } else {
    ObString escape_char = ObCharsetUtils::get_const_str(coll_type, '\\');
    ObString quote_char = ObCharsetUtils::get_const_str(coll_type, '\'');
    int64_t buf_len = str.length() * 2 + 2 * escape_char.length();
    int64_t pos = 0;
    char *buf = reinterpret_cast<char *>(allocator->alloc(buf_len));
    if (NULL == buf) { //alloc memory failed
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed.", K(ret));
    } else {
      HandleCharEscape handle_char_func(escape_char, quote_char, coll_type, buf, buf_len, pos);
      OZ(string_write_buf(quote_char, buf, buf_len, pos));
      OZ(ObCharsetUtils::foreach_char(str, coll_type, handle_char_func));
      OZ(string_write_buf(quote_char, buf, buf_len, pos));
      res_str.assign_ptr(buf, pos);
    }
  }
  return ret;
}

int ObExprQuote::calc_quote_expr(const ObExpr &expr, ObEvalCtx &ctx,
                                 ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *arg = NULL;
  if (OB_FAIL(expr.eval_param_value(ctx, arg))) {
    LOG_WARN("eval param failed", K(ret));
  } else if (arg->is_null()) {
    // in mysql, quote(null) does not return nulltype but 'null'
    // (without enclosing single quotation marks)
    ObExprStrResAlloc res_alloc(expr, ctx);
    char *buf = reinterpret_cast<char *>(res_alloc.alloc(LEN_OF_NULL));
    if (NULL == buf) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc memory failed.", K(ret));
    } else {
      MEMCPY(buf, "NULL", LEN_OF_NULL);
      res_datum.set_string(buf, LEN_OF_NULL);
    }
  } else {
    const ObString &str = arg->get_string();
    ObString res_str;
    ObExprStrResAlloc res_alloc(expr, ctx);
    if (OB_FAIL(calc(res_str, str, expr.datum_meta_.cs_type_, &res_alloc))) {
      res_datum.set_null();
      // ret = OB_ERR_INCORRECT_STRING_VALUE means input string is not invalid, then output NULL.
      if (OB_ERR_INCORRECT_STRING_VALUE == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("calc quote expr failed", K(ret), K(str));
      }
    } else {
      res_datum.set_string(res_str);
    }
  }
  return ret;
}

// Check if a string contains special characters that need escaping
bool ObExprQuote::has_special_chars(const ObString &str, ObCollationType coll_type)
{
  bool has_special = false;
  if (storage::is_ascii_str(str.ptr(), str.length())) {
    // For ASCII strings, use byte-by-byte iteration to avoid mb_wc overhead
    CheckByteForSpecialChar check_byte_func(has_special);
    ObCharsetUtils::foreach_byte(str, check_byte_func);
  } else {
    // For multi-byte strings, use character-by-character iteration
    CheckCharForSpecialChar check_char_func(has_special);
    ObCharsetUtils::foreach_char(str, coll_type, check_char_func);
  }
  return has_special;
}

// Process a string with special characters, write quoted result to buf
int ObExprQuote::process_string_with_special_chars(const ObString &str, ObCollationType coll_type,
                                                    char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  ObString escape_char = ObCharsetUtils::get_const_str(coll_type, '\\');
  ObString quote_char = ObCharsetUtils::get_const_str(coll_type, '\'');

  // Write opening quote
  OZ(string_write_buf(quote_char, buf, buf_len, pos));

  HandleCharEscape handle_char_func(escape_char, quote_char, coll_type, buf, buf_len, pos);

  OZ(ObCharsetUtils::foreach_char(str, coll_type, handle_char_func));

  // Write closing quote
  if (OB_SUCC(ret)) {
    OZ(string_write_buf(quote_char, buf, buf_len, pos));
  }

  return ret;
}

// Process a string without special characters, just add quotes
int ObExprQuote::process_string_simple(const ObString &str, const ObString &quote_char, ObCollationType coll_type,
                                        char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  // Write opening quote
  OZ(string_write_buf(quote_char, buf, buf_len, pos));
  // Write string content
  OZ(string_write_buf(str, buf, buf_len, pos));
  // Write closing quote
  OZ(string_write_buf(quote_char, buf, buf_len, pos));

  return ret;
}

template <typename IN_VEC, typename OUT_VEC>
int ObExprQuote::inner_eval_quote_vector(VECTOR_EVAL_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  IN_VEC *in_vec = static_cast<IN_VEC *>(expr.args_[0]->get_vector(ctx));
  OUT_VEC *res_vec = static_cast<OUT_VEC *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ObCollationType coll_type = expr.datum_meta_.cs_type_;
  ObString escape_char = ObCharsetUtils::get_const_str(coll_type, '\\');
  ObString quote_char = ObCharsetUtils::get_const_str(coll_type, '\'');

  const int64_t batch_size = bound.batch_size();
  const int64_t start = bound.start();
  const int64_t end = bound.end();

  // Allocate memory for bitmaps using TempAllocGuard
  ObEvalCtx::TempAllocGuard alloc_guard(ctx);
  int64_t bitmap_mem_size = ObBitVector::memory_size(batch_size);
  void *need_special_handle_buf = alloc_guard.get_allocator().alloc(bitmap_mem_size);
  void *no_special_handle_buf = alloc_guard.get_allocator().alloc(bitmap_mem_size);

  if (OB_ISNULL(need_special_handle_buf) || OB_ISNULL(no_special_handle_buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc bitmap memory failed", K(ret), K(bitmap_mem_size));
  } else {
    ObBitVector *need_special_handle = to_bit_vector(need_special_handle_buf);
    ObBitVector *no_special_handle = to_bit_vector(no_special_handle_buf);
    need_special_handle->reset(batch_size);
    no_special_handle->reset(batch_size);

    // First pass: identify strings needing special handling
    for (int64_t i = start; i < end; ++i) {
      if (skip.at(i) || eval_flags.at(i)) {
        continue;
      }

      if (!in_vec->has_null() || !in_vec->is_null(i)) {
        ObString input_str = in_vec->get_string(i);
        if (has_special_chars(input_str, coll_type)) {
          need_special_handle->set(i);
        } else {
          no_special_handle->set(i);
        }
      }
    }

    // Second pass: process NULL values first
    for (int64_t i = start; OB_SUCC(ret) && i < end; ++i) {
      if (skip.at(i) || eval_flags.at(i)) {
        continue;
      }

      if (in_vec->has_null() && in_vec->is_null(i)) {
        // Handle NULL: allocate memory and write "NULL"
        char *buf = expr.get_str_res_mem(ctx, LEN_OF_NULL, i);
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc memory failed", K(ret));
        } else {
          MEMCPY(buf, "NULL", LEN_OF_NULL);
          res_vec->set_string(i, buf, LEN_OF_NULL);
        }
      }
    }
    // Third pass: process strings without special characters (fast path)
    for (int64_t i = start; OB_SUCC(ret) && i < end; ++i) {
      if (skip.at(i) || eval_flags.at(i) || !no_special_handle->at(i)) {
        continue;
      }

      ObString input_str = in_vec->get_string(i);
      // Calculate result length: input length + 2 quotes
      int64_t result_len = input_str.length() + 2 * quote_char.length();
      char *buf = expr.get_str_res_mem(ctx, result_len, i);
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc memory failed", K(ret));
      } else {
        int64_t pos = 0;
        if (OB_FAIL(process_string_simple(input_str, quote_char, coll_type, buf, result_len, pos))) {
          LOG_WARN("process simple string failed", K(ret));
        } else {
          res_vec->set_string(i, buf, pos);
        }
      }
    }

    // Fourth pass: process strings with special characters (slow path)
    for (int64_t i = start; OB_SUCC(ret) && i < end; ++i) {
      if (skip.at(i) || eval_flags.at(i) || !need_special_handle->at(i)) {
        continue;
      }

        ObString input_str = in_vec->get_string(i);
        // Calculate worst case result length: every char needs escaping (2x) + 2 quotes + escape chars
        int64_t max_result_len = input_str.length() * 2 + 2 * escape_char.length() + 2 * quote_char.length();
        char *buf = expr.get_str_res_mem(ctx, max_result_len, i);
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc memory failed", K(ret));
        } else {
          int64_t pos = 0;
          if (OB_FAIL(process_string_with_special_chars(input_str, coll_type, buf, max_result_len, pos))) {
            LOG_WARN("process string with special chars failed", K(ret));
          } else {
            res_vec->set_string(i, buf, pos);
          }
        }
    }
  }

  return ret;
}

using Discrete = ObDiscreteFormat;
using Continuous = ObContinuousFormat;
using Uniform = ObUniformFormat<false>;
using UniformConst = ObUniformFormat<true>;

#define CALC_FORMAT(IN_FORMAT, OUT_FORMAT) (IN_FORMAT << 4 | OUT_FORMAT)

#define DISPATCH_QUOTE_FORMAT(IN_FMT, OUT_FMT, IN_TYPE, OUT_TYPE)                                    \
  case CALC_FORMAT(IN_FMT, OUT_FMT): {                                                               \
    ret = inner_eval_quote_vector<IN_TYPE, OUT_TYPE>(VECTOR_EVAL_FUNC_ARG_LIST);                     \
    break;                                                                                           \
  }

int ObExprQuote::eval_quote_vector(VECTOR_EVAL_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;

  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    SQL_LOG(WARN, "failed to eval batch result input", K(ret));
  } else {
    VectorFormat res_format = expr.get_format(ctx);
    VectorFormat in_format = expr.args_[0]->get_format(ctx);

    const int64_t cond = CALC_FORMAT(in_format, res_format);
    switch (cond) {
      DISPATCH_QUOTE_FORMAT(VEC_DISCRETE, VEC_DISCRETE, Discrete, Discrete);
      DISPATCH_QUOTE_FORMAT(VEC_DISCRETE, VEC_CONTINUOUS, Discrete, Continuous);
      DISPATCH_QUOTE_FORMAT(VEC_DISCRETE, VEC_UNIFORM, Discrete, Uniform);
      DISPATCH_QUOTE_FORMAT(VEC_DISCRETE, VEC_UNIFORM_CONST, Discrete, UniformConst);

      DISPATCH_QUOTE_FORMAT(VEC_CONTINUOUS, VEC_DISCRETE, Continuous, Discrete);
      DISPATCH_QUOTE_FORMAT(VEC_CONTINUOUS, VEC_CONTINUOUS, Continuous, Continuous);
      DISPATCH_QUOTE_FORMAT(VEC_CONTINUOUS, VEC_UNIFORM, Continuous, Uniform);
      DISPATCH_QUOTE_FORMAT(VEC_CONTINUOUS, VEC_UNIFORM_CONST, Continuous, UniformConst);

      DISPATCH_QUOTE_FORMAT(VEC_UNIFORM, VEC_DISCRETE, Uniform, Discrete);
      DISPATCH_QUOTE_FORMAT(VEC_UNIFORM, VEC_CONTINUOUS, Uniform, Continuous);
      DISPATCH_QUOTE_FORMAT(VEC_UNIFORM, VEC_UNIFORM, Uniform, Uniform);
      DISPATCH_QUOTE_FORMAT(VEC_UNIFORM, VEC_UNIFORM_CONST, Uniform, UniformConst);

      DISPATCH_QUOTE_FORMAT(VEC_UNIFORM_CONST, VEC_DISCRETE, UniformConst, Discrete);
      DISPATCH_QUOTE_FORMAT(VEC_UNIFORM_CONST, VEC_CONTINUOUS, UniformConst, Continuous);
      DISPATCH_QUOTE_FORMAT(VEC_UNIFORM_CONST, VEC_UNIFORM, UniformConst, Uniform);
      DISPATCH_QUOTE_FORMAT(VEC_UNIFORM_CONST, VEC_UNIFORM_CONST, UniformConst, UniformConst);

    default: {
      ret = inner_eval_quote_vector<ObVectorBase, ObVectorBase>(VECTOR_EVAL_FUNC_ARG_LIST);
      break;
    }
    }
  }

  return ret;
}

int ObExprQuote::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_quote_expr;
  rt_expr.eval_vector_func_ = eval_quote_vector;
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprQuote, raw_expr) {
  int ret = OB_SUCCESS;
  SET_LOCAL_SYSVAR_CAPACITY(1);
  EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_COLLATION_CONNECTION);
  return ret;
}

} //namespace sql
} //namespace oceanbase
