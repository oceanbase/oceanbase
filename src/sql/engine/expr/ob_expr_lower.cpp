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

#if defined(__x86_64__)
#include <immintrin.h>
#endif

#include "sql/engine/expr/ob_expr_lower.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

namespace oceanbase {
using namespace common;
namespace sql {

ObExprLowerUpper::ObExprLowerUpper(ObIAllocator &alloc, ObExprOperatorType type, const char *name, int32_t param_num)
    : ObStringExprOperator(alloc, type, name, param_num, VALID_FOR_GENERATED_COL)
{
}

ObExprLower::ObExprLower(ObIAllocator &alloc)
    : ObExprLowerUpper(alloc, T_FUN_SYS_LOWER, N_LOWER, 1)
{}

ObExprUpper::ObExprUpper(ObIAllocator &alloc)
    : ObExprLowerUpper(alloc, T_FUN_SYS_UPPER, N_UPPER, 1)
{}

ObExprNlsLower::ObExprNlsLower(ObIAllocator &alloc)
    : ObExprLowerUpper(alloc, T_FUN_SYS_NLS_LOWER, N_NLS_LOWER, PARAM_NUM_UNKNOWN)
{}

ObExprNlsUpper::ObExprNlsUpper(ObIAllocator &alloc)
    : ObExprLowerUpper(alloc, T_FUN_SYS_NLS_UPPER, N_NLS_UPPER, PARAM_NUM_UNKNOWN)
{}

int ObExprLowerUpper::calc_result_type1(ObExprResType &type, ObExprResType &text,
    common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo *session = type_ctx.get_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else {
    if (is_oracle_mode()) {
      ObSEArray<ObExprResType*, 1, ObNullAllocator> param;
      OZ(param.push_back(&text));
      OZ(aggregate_string_type_and_charset_oracle(*session, param, type));
      OZ(deduce_string_param_calc_type_and_charset(*session, type, param));
      OX(type.set_length(text.get_calc_length() * get_case_mutiply(type.get_collation_type())));
    } else {
      if (ObTinyTextType == text.get_type()) {
        type.set_type(ObVarcharType);
      } else if (text.is_lob()) {
        type.set_type(ObLongTextType);
      } else {
        type.set_varchar();
      }
      text.set_calc_type(type.get_type());
      const common::ObLengthSemantics default_length_semantics = (OB_NOT_NULL(type_ctx.get_session())
          ? type_ctx.get_session()->get_actual_nls_length_semantics()
          : common::LS_BYTE);
      ret = aggregate_charsets_for_string_result(type, &text, 1, type_ctx);
      OX(text.set_calc_collation_type(type.get_collation_type()));
      OX(text.set_calc_collation_level(type.get_collation_level()));
      OX(type.set_length(text.get_length()));
    }
  }

  return ret;
}

// For oracle only functions nls_lower/nls_upper
int ObExprLowerUpper::calc_result_typeN(ObExprResType &type,
                                        ObExprResType *texts,
                                        int64_t param_num,
                                        ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo *session = type_ctx.get_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else if (param_num <= 0) {
    ret = OB_ERR_NOT_ENOUGH_ARGS_FOR_FUN;
    LOG_WARN("nls_lower/nls_upper require at least one parameter", K(ret), K(param_num));
  } else if (param_num > 2) {
    ret = OB_ERR_TOO_MANY_ARGS_FOR_FUN;
    LOG_WARN("nls_lower/nls_upper require at most two parameters", K(ret), K(param_num));
  } else if (OB_ISNULL(texts)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(param_num), K(texts));
  } else {
    // 根据第一个参数计算即可
    ObSEArray<ObExprResType*, 1, ObNullAllocator> param;
    OZ(param.push_back(&texts[0]));
    OZ(aggregate_string_type_and_charset_oracle(*session, param, type));
    OZ(deduce_string_param_calc_type_and_charset(*session, type, param));
    OX(type.set_length(texts[0].get_calc_length() * ObCharset::MAX_CASE_MULTIPLY));
  }

  return ret;
}

int ObExprLowerUpper::calc(ObObj &result, const ObString &text,
                           ObCollationType cs_type, ObIAllocator &string_buf) const
{
  int ret = OB_SUCCESS;
  ObString str_result;
  bool has_lob_header = get_result_type().get_calc_meta().has_lob_header();
  if (text.empty()) {
    str_result.reset();
  } else if (!ob_is_text_tc(result.get_type())) {
    int64_t buf_len = text.length() * get_case_mutiply(cs_type);
    char *buf = reinterpret_cast<char *>(string_buf.alloc(buf_len));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc memory failed", "size", buf_len);
    } else  {
      // binary collation的casedn什么都没做，所以需要copy
      MEMCPY(buf, text.ptr(), text.length());
      int32_t out_len = 0;
      //gb18030 可能会膨胀，src和dst不同，其他字符集相同
      char *src_str = (get_case_mutiply(cs_type) > 1) ? const_cast<char*>(text.ptr()) : buf;
      if (OB_FAIL(calc(cs_type, src_str, text.length(), buf, buf_len, out_len))) {
        LOG_WARN("failed to calc", K(ret));
      }
      str_result.assign(buf, static_cast<int32_t>(out_len));
    }
  } else {
    ObString real_str;
    int32_t multiply = get_case_mutiply(cs_type);
    ObObjType text_type = get_result_type().get_calc_meta().get_type();
    // Need calc buff, user AreanaAllocator instead, for cannot find the caller of this function
    common::ObArenaAllocator temp_allocator(ObModIds::OB_LOB_READER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    // not found caller of this func, need text src obj to judge whether has lob header
    ObTextStringIter src_iter(text_type, cs_type, text, has_lob_header);
    char *buf = NULL; // res buffer
    int64_t src_byte_len = 0;
    int64_t buf_size = 0;
    int32_t buf_len = 0;

    if (OB_FAIL(src_iter.init(0, NULL, &temp_allocator))) {
      LOG_WARN("init lob str iter failed ", K(ret), K(src_iter));
    } else if (OB_FAIL(src_iter.get_byte_len(src_byte_len))) {
      LOG_WARN("get input byte len failed");
    } else {
      int64_t buf_len = src_byte_len * multiply;
      ObTextStringResult output_result(text_type, has_lob_header, &string_buf);
      if (OB_FAIL(output_result.init(buf_len))) {
        LOG_WARN("init stringtext result failed", K(ret), K(output_result), K(buf_len));
      } else if (buf_len == 0) {
        str_result.reset();
      } else if (OB_FAIL(output_result.get_reserved_buffer(buf, buf_size))) {
        LOG_WARN("get empty buffer failed", K(ret), K(buf_len));
      } else if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("alloc memory failed", "size", buf_len);
      } else  {
        OB_ASSERT(buf_size == buf_len);
        ObTextStringIterState state;
        ObString src_block_data;

        while (OB_SUCC(ret)
               && buf_size > 0
               && (state = src_iter.get_next_block(src_block_data)) == TEXTSTRING_ITER_NEXT) {
          // binary collation的casedn什么都没做，所以需要copy
          MEMCPY(buf, src_block_data.ptr(), src_block_data.length());
          int32_t out_len = 0;
          //gb18030 可能会膨胀，src和dst不同，其他字符集相同
          char *src_str = (multiply > 1) ? const_cast<char*>(src_block_data.ptr()) : buf;
          if (OB_FAIL(calc(cs_type, src_str, src_block_data.length(), buf, buf_size, out_len))) {
            LOG_WARN("failed to calc", K(ret));
          } else if (OB_FAIL(output_result.lseek(out_len, 0))) {
            LOG_WARN("output_result lseek failed", K(ret));
          } else {
            buf += out_len;
            buf_size -= out_len;
          }
        }
        output_result.get_result_buffer(str_result);
      }
    }
  }

  if (OB_SUCC(ret)) {
    result.set_common_value(str_result);
    result.set_meta_type(get_result_type()); // should set has lob header here
  }

  return ret;
}

int ObExprLower::calc(const ObCollationType cs_type, char *src, int32_t src_len,
                      char *dst, int32_t dst_len, int32_t &out_len) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src) || OB_ISNULL(dst)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("src or dst is null", K(ret));
  } else {
    out_len = static_cast<int32_t>(ObCharset::casedn(cs_type, src, src_len, dst, dst_len));
  }
  return ret;
}


int32_t ObExprLower::get_case_mutiply(const ObCollationType cs_type) const
{
  int32_t mutiply_num = 0;
  if (OB_UNLIKELY(!ObCharset::is_valid_collation(cs_type))) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid charset", K(cs_type));
  } else {
    mutiply_num = ObCharset::get_charset(cs_type)->casedn_multiply;
  }
  return mutiply_num;
}

int ObExprUpper::calc(const ObCollationType cs_type, char *src, int32_t src_len,
                      char *dst, int32_t dst_len, int32_t &out_len) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src) || OB_ISNULL(dst)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("src or dst is null", K(ret));
  } else {
    out_len = static_cast<int32_t>(ObCharset::caseup(cs_type, src, src_len, dst, dst_len));
  }
  return ret;
}


int32_t ObExprUpper::get_case_mutiply(const ObCollationType cs_type) const
{
  int32_t mutiply_num = 0;
  if (OB_UNLIKELY(!ObCharset::is_valid_collation(cs_type))) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid charset", K(cs_type));
  } else {
    mutiply_num = ObCharset::get_charset(cs_type)->caseup_multiply;
  }
  return mutiply_num;
}

int ObExprLowerUpper::cg_expr_common(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  ObObjType text_type = ObMaxType;
  if (rt_expr.arg_cnt_ != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("lower/upper expr should have one param", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of lower/upper expr is null", K(ret), K(rt_expr.args_));
  } else if (FALSE_IT(text_type = rt_expr.args_[0]->datum_meta_.type_)) {
  } else if ((is_oracle_mode() && ObVarcharType != text_type
          && ObCharType != text_type && !ob_is_nstring_type(text_type)
          && !ob_is_text_tc(text_type))
  || (!is_oracle_mode() && ObVarcharType != text_type && ObLongTextType != text_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(text_type), K(ret));
  }
  return ret;
}

int ObExprLowerUpper::cg_expr_nls_common(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (!is_oracle_mode()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "this function in MySql mode");
    LOG_WARN("this function is only supported in Oracle mode", K(ret));
  } else if (rt_expr.arg_cnt_ <= 0) {
    ret = OB_ERR_NOT_ENOUGH_ARGS_FOR_FUN;
    LOG_WARN("nls_lower/nls_upper require at lease one parameter", K(ret), K(rt_expr.arg_cnt_));
  } else if (rt_expr.arg_cnt_ > 2) {
    ret = OB_ERR_TOO_MANY_ARGS_FOR_FUN;
    LOG_WARN("nls_lower/nls_upper require at most two parameters", K(ret), K(rt_expr.arg_cnt_));
  } 
  else if (OB_ISNULL(rt_expr.args_)
           || OB_ISNULL(rt_expr.args_[0])
           || (rt_expr.arg_cnt_ == 2 && OB_ISNULL(rt_expr.args_[1]))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of lower/upper expr is null", K(ret), K(rt_expr.args_));
  }
  for (int i = 0; OB_SUCC(ret) && i < rt_expr.arg_cnt_; ++i) {
    ObObjType text_type = rt_expr.args_[i]->datum_meta_.type_;
    if (!ob_is_string_type(text_type)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(text_type), K(ret));
    }
  }
  return ret;
}

int ObExprLower::cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cg_expr_common(op_cg_ctx, raw_expr, rt_expr))) {
    LOG_WARN("lower expr cg expr failed", K(ret));
  } else {
    rt_expr.eval_func_ = ObExprLower::calc_lower;
    rt_expr.eval_vector_func_ = eval_lower_vector;
  }
  return ret;
}

int ObExprUpper::cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cg_expr_common(op_cg_ctx, raw_expr, rt_expr))) {
    LOG_WARN("upper expr cg expr failed", K(ret));
  } else {
    rt_expr.eval_func_ = ObExprUpper::calc_upper;
    rt_expr.eval_vector_func_ = eval_upper_vector;
  }
  return ret;
}

static inline int32_t calc_common_inner(char *buf,
                                        const int32_t &buf_len,
                                        const ObString &m_text,
                                        const ObCollationType &cs_type,
                                        const bool &is_lower)
{
  MEMCPY(buf, m_text.ptr(), m_text.length());
  //gb18030 may expand in size, src_str and dst_str should has different buf, other cs_type can use the same buf
  char *src_str = (buf_len != m_text.length()) ? const_cast<char*>(m_text.ptr()) : buf;
  return (is_lower
          ? static_cast<int32_t>(ObCharset::casedn(cs_type, src_str, m_text.length(), buf, buf_len))
          : static_cast<int32_t>(ObCharset::caseup(cs_type, src_str, m_text.length(), buf, buf_len))
          );
}

int ObExprLowerUpper::calc_common(const ObExpr &expr, ObEvalCtx &ctx,
                                  ObDatum &expr_datum, bool lower, ObCollationType cs_type)
{
  int ret = OB_SUCCESS;
  ObDatum *text_datum = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, text_datum))) {
    LOG_WARN("eval param value failed", K(ret));
  } else if (text_datum->is_null()) {
    expr_datum.set_null();
  } else {
    ObString m_text = text_datum->get_string();
    if (cs_type == CS_TYPE_INVALID) {
      cs_type = expr.datum_meta_.cs_type_;
    }
    ObString str_result;
    bool has_lob_header = expr.args_[0]->obj_meta_.has_lob_header();
    ObDatumMeta text_meta = expr.args_[0]->datum_meta_;
    uchar multiply = 0;
    if (m_text.empty() && !ob_is_text_tc(text_meta.type_)) {
      str_result.reset();
    } else if (OB_UNLIKELY(!ObCharset::is_valid_collation(cs_type))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("charset is null", K(ret), K(cs_type));
    } else if (FALSE_IT(multiply = (lower ? ObCharset::get_charset(cs_type)->casedn_multiply
                                          : ObCharset::get_charset(cs_type)->caseup_multiply))) {
    } else if (!ob_is_text_tc(text_meta.type_)) {
      int32_t buf_len = m_text.length() * multiply;
      char *buf = expr.get_str_res_mem(ctx, buf_len);
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("alloc memory failed", "size", buf_len);
      } else {
        int32_t out_len = calc_common_inner(buf, buf_len, m_text, cs_type, lower);
        str_result.assign(buf, static_cast<int32_t>(out_len));
      }
    } else { // text tc only
      ObEvalCtx::TempAllocGuard alloc_guard(ctx);
      ObIAllocator &calc_alloc = alloc_guard.get_allocator();
      ObTextStringIter src_iter(text_meta.type_, text_meta.cs_type_, text_datum->get_string(), has_lob_header);
      ObTextStringDatumResult output_result(expr.datum_meta_.type_, &expr, &ctx, &expr_datum);

      ObString dst;
      char *buf = NULL; // res buffer
      int64_t src_byte_len = 0;
      int64_t buf_size = 0;
      int32_t buf_len = 0;
      if (OB_UNLIKELY(!ObCharset::is_valid_collation(cs_type))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("charset is null", K(ret), K(cs_type));
      } else if (OB_FAIL(src_iter.init(0, NULL, &calc_alloc))) {
        LOG_WARN("init src_iter failed ", K(ret), K(src_iter));
      } else if (OB_FAIL(src_iter.get_byte_len(src_byte_len))) {
        LOG_WARN("get input byte len failed", K(ret));
      } else if (FALSE_IT(buf_len = multiply * src_byte_len)) {
      } else if (OB_FAIL(output_result.init(buf_len))) {
        LOG_WARN("init stringtext result failed", K(ret));
      } else if (buf_len == 0) {
        output_result.set_result();
        output_result.get_result_buffer(str_result);
      } else if (OB_FAIL(output_result.get_reserved_buffer(buf, buf_size))) {
        LOG_WARN("stringtext result reserve buffer failed", K(ret));
      } else if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("alloc memory failed", "size", buf_len);
      } else {
        OB_ASSERT(buf_size == buf_len);
        ObTextStringIterState state;
        ObString src_block_data;

        while (OB_SUCC(ret)
               && buf_size > 0
               && (state = src_iter.get_next_block(src_block_data)) == TEXTSTRING_ITER_NEXT) {
          int32_t out_len = calc_common_inner(buf,
                                              buf_size,
                                              src_block_data,
                                              cs_type,
                                              lower);
          buf += out_len;
          buf_size -= out_len;
          if (OB_FAIL(output_result.lseek(out_len, 0))) {
            LOG_WARN("result lseek failed", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (state != TEXTSTRING_ITER_NEXT && state != TEXTSTRING_ITER_END) {
          ret = (src_iter.get_inner_ret() != OB_SUCCESS) ?
                src_iter.get_inner_ret() : OB_INVALID_DATA;
          LOG_WARN("iter state invalid", K(ret), K(state), K(src_iter));
        } else {
          output_result.get_result_buffer(str_result);
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(is_oracle_mode() && str_result.length() == 0
                      && ob_is_string_tc(expr.datum_meta_.type_))) {
        expr_datum.set_null();
      } else {
        expr_datum.set_string(str_result);
      }
    }
  }
  return ret;
}

template <char CA, char CZ>
void ObExprLowerUpper::calc_common_inner_optimized(
    char *buf, const int32_t &buf_len, const ObString &m_text)
{
  MEMCPY(buf, m_text.ptr(), m_text.length());
  const char *src_ptr = m_text.ptr();
  const size_t size = m_text.length();
  char *dst_ptr = buf;
  const char *end = m_text.ptr() + size;
#if defined(__x86_64__)
  const char *begin = src_ptr;
  static constexpr int SSE2_BYTES = sizeof(__m128i);
  const char *simd_end = begin + (size & ~(SSE2_BYTES - 1));
  const auto a_minus1 = _mm_set1_epi8(CA - 1);
  const auto z_plus1 = _mm_set1_epi8(CZ + 1);
  const auto flips = _mm_set1_epi8(32);
  for (; src_ptr > simd_end; src_ptr += SSE2_BYTES, dst_ptr += SSE2_BYTES) {
    auto bytes = _mm_loadu_si128((const __m128i*)src_ptr);
    // the i-th byte of masks is set to 0xff if the corresponding byte is
    // between a..z when computing upper function (A..Z when computing lower function),
    // otherwise set to 0;
    auto masks = _mm_and_si128(_mm_cmpgt_epi8(bytes, a_minus1), _mm_cmpgt_epi8(z_plus1, bytes));
    // only flip 5th bit of lowcase(uppercase) byte, other bytes keep verbatim.
    _mm_storeu_si128((__m128i*)dst_ptr, _mm_xor_si128(bytes, _mm_and_si128(masks, flips)));
  }
#endif
  // only flip 5th bit of lowcase(uppercase) byte, other bytes keep verbatim.
  // i.e.  'a' and 'A' are 0b0110'0001 and 0b'0100'0001 respectively in binary form,
  // whether 'a' to 'A' or 'A' to 'a' conversion, just flip 5th bit(xor 32).
  for (; src_ptr < end; src_ptr += 1, dst_ptr += 1) {
    *dst_ptr = *src_ptr ^ (((CA <= *src_ptr) & (*src_ptr <= CZ)) << 5);
  }
}

template <typename ArgVec, typename ResVec, bool IsLower>
int ObExprLowerUpper::vector_lower_upper(VECTOR_EVAL_FUNC_ARG_DECL, common::ObCollationType cs_type)
{
  int ret = OB_SUCCESS;
  const ArgVec *arg0_vec = static_cast<const ArgVec *>(expr.args_[0]->get_vector(ctx));
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);

  // 1. check if result all null according to text param
  bool is_params_all_null = true;
  for (int64_t idx = bound.start(); idx < bound.end(); ++idx) {
    if (skip.at(idx) || eval_flags.at(idx)) {
      continue;
    } else if (arg0_vec->is_null(idx)) {
      res_vec->set_null(idx);
      eval_flags.set(idx);
    } else {
      is_params_all_null = false;
    }
  }
  // 2. calc lower_upper while result is not all null
  if (!is_params_all_null) {
    if (cs_type == CS_TYPE_INVALID) {
      cs_type = expr.datum_meta_.cs_type_;
    }
    ObDatumMeta text_meta = expr.args_[0]->datum_meta_;
    uchar multiply = 0;
    ObString str_result;
    bool is_arg_batch_ascii = arg0_vec->is_batch_ascii();
    bool is_result_batch_ascii = true;
    bool do_ascii_optimize_check = storage::can_do_ascii_optimize(expr.datum_meta_.cs_type_);
    if (OB_UNLIKELY(!ObCharset::is_valid_collation(cs_type))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("charset is null", K(ret), K(cs_type));
    } else if (FALSE_IT(multiply = (IsLower ? ObCharset::get_charset(cs_type)->casedn_multiply
                                          : ObCharset::get_charset(cs_type)->caseup_multiply))) {
    } else if (!ob_is_text_tc(text_meta.type_)) { // 2.1 deal with string tc
      for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
        if (skip.at(idx) || eval_flags.at(idx)) {
          continue;
        } else {
          ObString m_text = arg0_vec->get_string(idx);
          char *buf = expr.get_str_res_mem(ctx, m_text.length() * multiply, idx);
          if (m_text.empty()) {
            str_result.reset();
          } else if (OB_ISNULL(buf)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("alloc memory failed", "size", m_text.length() * multiply);
          } else if (is_arg_batch_ascii || (do_ascii_optimize_check
                      && storage::is_ascii_str(m_text.ptr(), m_text.length()))) {
            if (IsLower) {
              calc_common_inner_optimized<'A', 'Z'>(buf, m_text.length(), m_text);
            } else {
              calc_common_inner_optimized<'a', 'z'>(buf, m_text.length(), m_text);
            }
            str_result.assign(buf, static_cast<int32_t>(m_text.length()));
          } else {
            is_result_batch_ascii = false;
            int32_t out_len = calc_common_inner(
                              buf, m_text.length() * multiply, m_text, cs_type, IsLower);
            str_result.assign(buf, static_cast<int32_t>(out_len));
          }
          if (OB_SUCC(ret)) {
            if (OB_UNLIKELY(is_oracle_mode() && str_result.empty())) {
              res_vec->set_null(idx);
            } else {
              res_vec->set_string(idx, str_result);
            }
            eval_flags.set(idx);
          }
        }
      }
    } else { // 2.2 deal with text tc
      const ObDatumMeta &input_meta = expr.args_[0]->datum_meta_;
      const bool has_lob_header = expr.args_[0]->obj_meta_.has_lob_header();
      for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
        if (skip.at(idx) || eval_flags.at(idx)) {
          continue;
        } else {
          ObEvalCtx::TempAllocGuard alloc_guard(ctx);
          ObIAllocator &calc_alloc = alloc_guard.get_allocator();
          ObTextStringIter src_iter(
              input_meta.type_, input_meta.cs_type_, arg0_vec->get_string(idx), has_lob_header);
          ObTextStringVectorResult<ResVec> output_result(expr.datum_meta_.type_, &expr, &ctx, res_vec, idx);
          ObString dst;
          char *buf = NULL; // res buffer
          int64_t src_byte_len = 0;
          int64_t buf_size = 0;
          int32_t buf_len = 0;
          bool is_ascii = (is_arg_batch_ascii
                           || (do_ascii_optimize_check
                               && storage::is_ascii_str(arg0_vec->get_string(idx).ptr(),
                                                        arg0_vec->get_string(idx).length())));
          if (!is_ascii) {
            is_result_batch_ascii = false;
          }
          if (OB_UNLIKELY(!ObCharset::is_valid_collation(cs_type))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("charset is null", K(ret), K(cs_type));
          } else if (OB_FAIL(src_iter.init(0, NULL, &calc_alloc))) {
            LOG_WARN("init src_iter failed ", K(ret), K(src_iter));
          } else if (OB_FAIL(src_iter.get_byte_len(src_byte_len))) {
            LOG_WARN("get input byte len failed", K(ret));
          } else if (FALSE_IT(buf_len = multiply * src_byte_len)) {
          } else if (OB_FAIL(output_result.init_with_batch_idx(buf_len, idx))) {
            LOG_WARN("init stringtext result failed", K(ret));
          } else if (buf_len == 0) {
            output_result.set_result();
            output_result.get_result_buffer(str_result);
          } else if (OB_FAIL(output_result.get_reserved_buffer(buf, buf_size))) {
            LOG_WARN("stringtext result reserve buffer failed", K(ret));
          } else if (OB_ISNULL(buf)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_ERROR("alloc memory failed", "size", buf_len);
          } else {
            OB_ASSERT(buf_size == buf_len);
            ObTextStringIterState state;
            ObString src_block_data;
            while (OB_SUCC(ret)
                   && buf_size > 0
                   && (state = src_iter.get_next_block(src_block_data)) == TEXTSTRING_ITER_NEXT) {
              int32_t out_len = 0;
              if (is_ascii) {
                if (IsLower) {
                  calc_common_inner_optimized<'A', 'Z'>(buf, buf_size, src_block_data);
                } else {
                  calc_common_inner_optimized<'a', 'z'>(buf, buf_size, src_block_data);
                }
                out_len = src_block_data.length();
              } else {
                out_len = calc_common_inner(buf,
                                            buf_size,
                                            src_block_data,
                                            cs_type,
                                            IsLower);
              }
              buf += out_len;
              buf_size -= out_len;
              if (OB_FAIL(output_result.lseek(out_len, 0))) {
                LOG_WARN("result lseek failed", K(ret));
              }
            }
            if (OB_FAIL(ret)) {
            } else if (state != TEXTSTRING_ITER_NEXT && state != TEXTSTRING_ITER_END) {
              ret = (src_iter.get_inner_ret() != OB_SUCCESS) ?
                    src_iter.get_inner_ret() : OB_INVALID_DATA;
              LOG_WARN("iter state invalid", K(ret), K(state), K(src_iter));
            } else {
              output_result.get_result_buffer(str_result);
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_UNLIKELY(is_oracle_mode() && str_result.length() == 0
                      && ob_is_string_tc(expr.datum_meta_.type_))) {
              res_vec->set_null(idx);
            } else {
              res_vec->set_string(idx, str_result);
            }
            eval_flags.set(idx);
          }
        }
      }
    }
    // TODO Set set_is_batch_ascii = true only if bound is a whole batch and there is no skip.
    /*
    if (OB_SUCC(ret)) {
      if (is_result_batch_ascii) {
        res_vec->set_is_batch_ascii();
      }
    } */
  }
  return ret;
}

template <bool IsLower>
int ObExprLowerUpper::calc_common_vector(
    VECTOR_EVAL_FUNC_ARG_DECL, common::ObCollationType cs_type)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("failed to eval vector result args0", K(ret));
  } else {
    VectorFormat arg_format = expr.args_[0]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    if (VEC_DISCRETE == arg_format && VEC_DISCRETE == res_format) {
      ret = vector_lower_upper<StrDiscVec, StrDiscVec, IsLower>(VECTOR_EVAL_FUNC_ARG_LIST, cs_type);
    } else if (VEC_UNIFORM == arg_format && VEC_DISCRETE == res_format) {
      ret = vector_lower_upper<StrUniVec, StrDiscVec, IsLower>(VECTOR_EVAL_FUNC_ARG_LIST, cs_type);
    } else if (VEC_CONTINUOUS == arg_format && VEC_DISCRETE == res_format) {
      ret = vector_lower_upper<StrContVec, StrDiscVec, IsLower>(VECTOR_EVAL_FUNC_ARG_LIST, cs_type);
    } else if (VEC_DISCRETE == arg_format && VEC_UNIFORM == res_format) {
      ret = vector_lower_upper<StrDiscVec, StrUniVec, IsLower>(VECTOR_EVAL_FUNC_ARG_LIST, cs_type);
    } else if (VEC_UNIFORM == arg_format && VEC_UNIFORM == res_format) {
      ret = vector_lower_upper<StrUniVec, StrUniVec, IsLower>(VECTOR_EVAL_FUNC_ARG_LIST, cs_type);
    } else if (VEC_CONTINUOUS == arg_format && VEC_UNIFORM == res_format) {
      ret = vector_lower_upper<StrContVec, StrUniVec, IsLower>(VECTOR_EVAL_FUNC_ARG_LIST, cs_type);
    } else {
      ret = vector_lower_upper<ObVectorBase, ObVectorBase, IsLower>(VECTOR_EVAL_FUNC_ARG_LIST, cs_type);
    }
  }
  return ret;
}

int ObExprLower::eval_lower_vector(VECTOR_EVAL_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(calc_common_vector<true>(VECTOR_EVAL_FUNC_ARG_LIST, CS_TYPE_INVALID))) {
    LOG_WARN("failed to calc_common_vector", K(ret));
  }
  return ret;
}

int ObExprUpper::eval_upper_vector(VECTOR_EVAL_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(calc_common_vector<false>(VECTOR_EVAL_FUNC_ARG_LIST, CS_TYPE_INVALID))) {
    LOG_WARN("failed to calc_common_vector", K(ret));
  }
  return ret;
}

int ObExprLowerUpper::calc_nls_common(const ObExpr &expr, ObEvalCtx &ctx,
                                      ObDatum &expr_datum, bool lower)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(expr.arg_cnt_ <= 0)) {
    ret = OB_ERR_NOT_ENOUGH_ARGS_FOR_FUN;
    LOG_WARN("nls_lower/nls_upper requires at least one parameter", K(ret), K(expr.arg_cnt_));
  } else if (OB_UNLIKELY(expr.arg_cnt_ > 2)) {
    ret = OB_ERR_TOO_MANY_ARGS_FOR_FUN;
    LOG_WARN("nls_lower/nls_upper requires at most 2 parameters", K(ret), K(expr.arg_cnt_));
  } else if (expr.arg_cnt_ == 1) {
    if (OB_FAIL(calc_common(expr, ctx, expr_datum, lower, CS_TYPE_INVALID))) {
      LOG_WARN("failed to call calc_common", K(ret));
    }
  } else { // expr.arg_cnt_ == 2
    ObCollationType cs_type = expr.datum_meta_.cs_type_;
    ObDatum *param_datum = NULL;
    if (OB_ISNULL(expr.args_[1])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get nls parameter", K(ret));
    } else if (OB_FAIL(expr.args_[1]->eval(ctx, param_datum))) {
      LOG_WARN("eval nls parameter failed", K(ret));
    } else if (param_datum->is_null()) {
      // Second param_datum is null, set result null as well.
      expr_datum.set_null();
    } else {
      if (!ob_is_text_tc(expr.args_[1]->datum_meta_.type_)) {
        const ObString &m_param = param_datum->get_string();
        if (OB_UNLIKELY(!ObExprOperator::is_valid_nls_param(m_param))) {
          ret = OB_ERR_INVALID_NLS_PARAMETER_STRING;
          LOG_WARN("invalid nls parameter", K(ret), K(m_param));
        }
      } else { // text tc only
        ObString m_param = param_datum->get_string();
        ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
        common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
        if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *param_datum,
                    expr.args_[1]->datum_meta_, expr.args_[1]->obj_meta_.has_lob_header(), m_param))) {
          LOG_WARN("failed to read real pattern", K(ret), K(m_param));
        } else if (OB_UNLIKELY(!ObExprOperator::is_valid_nls_param(m_param))) {
          ret = OB_ERR_INVALID_NLS_PARAMETER_STRING;
          LOG_WARN("invalid nls parameter", K(ret), K(m_param));
        }
      }
      if (OB_SUCC(ret)) {
        // Should set cs_type here, but for now, we do nothing 
        // since nls parameter only support BINARY
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(calc_common(expr, ctx, expr_datum, lower, cs_type))) {
          LOG_WARN("failed to call calc_common", K(ret), K(cs_type));
        }
      }
    }
  }

  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprLowerUpper, raw_expr) {
  int ret = OB_SUCCESS;
  SET_LOCAL_SYSVAR_CAPACITY(1);
  EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_COLLATION_CONNECTION);
  return ret;
}

int ObExprLower::calc_lower(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  return calc_common(expr, ctx, expr_datum, true, CS_TYPE_INVALID);
}

int ObExprUpper::calc_upper(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  return calc_common(expr, ctx, expr_datum, false, CS_TYPE_INVALID);
}

int ObExprNlsLower::calc(const ObCollationType cs_type, char *src, int32_t src_len,
                         char *dst, int32_t dst_len, int32_t &out_len) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src) || OB_ISNULL(dst)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("src or dst is null", K(ret));
  } else {
    out_len = static_cast<int32_t>(ObCharset::casedn(cs_type, src, src_len, dst, dst_len));
  }
  return ret;
}

int32_t ObExprNlsLower::get_case_mutiply(const ObCollationType cs_type) const
{
  int32_t mutiply_num = 0;
  if (OB_UNLIKELY(!ObCharset::is_valid_collation(cs_type))) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid charset", K(cs_type));
  } else {
    mutiply_num = ObCharset::get_charset(cs_type)->casedn_multiply;
  }
  return mutiply_num;
}

int ObExprNlsLower::cg_expr(ObExprCGCtx &op_cg_ctx,
                            const ObRawExpr &raw_expr,
                            ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cg_expr_nls_common(op_cg_ctx, raw_expr, rt_expr))) {
    LOG_WARN("lower expr cg expr failed", K(ret));
  } else {
    rt_expr.eval_func_ = ObExprNlsLower::calc_lower;
  }
  return ret;
}

int ObExprNlsLower::calc_lower(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  return calc_nls_common(expr, ctx, expr_datum, true);
}

int ObExprNlsUpper::calc(const ObCollationType cs_type, char *src, int32_t src_len,
                         char *dst, int32_t dst_len, int32_t &out_len) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src) || OB_ISNULL(dst)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("src or dst is null", K(ret));
  } else {
    out_len = static_cast<int32_t>(ObCharset::caseup(cs_type, src, src_len, dst, dst_len));
  }
  return ret;
}

int32_t ObExprNlsUpper::get_case_mutiply(const ObCollationType cs_type) const
{
  int32_t mutiply_num = 0;
  if (OB_UNLIKELY(!ObCharset::is_valid_collation(cs_type))) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid charset", K(cs_type));
  } else {
    mutiply_num = ObCharset::get_charset(cs_type)->casedn_multiply;
  }
  return mutiply_num;
}

int ObExprNlsUpper::cg_expr(ObExprCGCtx &op_cg_ctx,
                            const ObRawExpr &raw_expr,
                            ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cg_expr_nls_common(op_cg_ctx, raw_expr, rt_expr))) {
    LOG_WARN("lower expr cg expr failed", K(ret));
  } else {
    rt_expr.eval_func_ = ObExprNlsUpper::calc_upper;
  }
  return ret;
}

int ObExprNlsUpper::calc_upper(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  return calc_nls_common(expr, ctx, expr_datum, false);
}

}
}
