
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

#include <unicode/ucnv.h>
#include <unicode/unistr.h>

#include "lib/oblog/ob_log.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/engine/expr/ob_expr_url_codec.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/session/ob_sql_session_info.h"

#define USING_LOG_PREFIX COMMON

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprURLEncode::ObExprURLEncode(ObIAllocator &alloc) :
  ObExprURLCODEC(alloc, T_FUN_URL_ENCODE, N_URL_ENCODE){};

ObExprURLDecode::ObExprURLDecode(ObIAllocator &alloc) :
  ObExprURLCODEC(alloc, T_FUN_URL_DECODE, N_URL_DECODE){};

OB_INLINE int hex_char_to_decimal(char ch)
{
  // HEX CHAR only, otherwise -1
  int res = -1;
  if (ch >= '0' && ch <= '9') {
    res = ch - '0';
  } else if (ch >= 'A' && ch <= 'F') {
    res = ch - 'A' + 10;
  } else if (ch >= 'a' && ch <= 'f') {
    res = ch - 'a' + 10;
  }
  return res;
}

int url_decode_process(char *input, int64_t len, char *&output, ObIAllocator &alloc,
                       int64_t &out_len)
{
  int ret = OB_SUCCESS;
  out_len = len;
  for (int64_t i = 0; i < len; ++i) {
    if (input[i] == '%') {
      // Check if there are at least two characters after '%'
      if (i + 2 >= len || !isxdigit(input[i + 1]) || !isxdigit(input[i + 2])) {
        ret = INCORRECT_ARGUMENTS_TO_URL_DECODE;
        if (i == len - 1) {
          LOG_USER_ERROR(INCORRECT_ARGUMENTS_TO_URL_DECODE, "illegal input", 0, 0);
        } else if (i == len - 2) {
          LOG_USER_ERROR(INCORRECT_ARGUMENTS_TO_URL_DECODE, "illegal input", input[i + 1], 0);
        } else {
          LOG_USER_ERROR(INCORRECT_ARGUMENTS_TO_URL_DECODE, "illegal input", input[i + 1],
                         input[i + 2]);
        }
        break;
      }
      // Move index forward by two positions to skip the hex digits
      i += 2;
      out_len -= 2;
    }
  }
  if (OB_SUCC(ret) && out_len > 0) {
    output = (char *)alloc.alloc(out_len);
    if (OB_ISNULL(output)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Alloc memory failed", K(ret), K(out_len));
    } else {
      int off = 0;
      for (int64_t i = 0; i + off < len; ++i) {
        int cur = i + off;
        if (input[cur] == '%') {
          int high = hex_char_to_decimal(input[cur + 1]);
          int low = hex_char_to_decimal(input[cur + 2]);
          if (OB_UNLIKELY(high == -1 || low == -1)) {
            ret = INCORRECT_ARGUMENTS_TO_URL_DECODE;
            LOG_USER_ERROR(INCORRECT_ARGUMENTS_TO_URL_DECODE, "invalid hex character", input[i + 1], input[i + 2]);
            break;
          }
          output[i] = static_cast<char>((high << 4) | low);
          off += 2;
        } else {
          output[i] = input[cur];
        }
      }
    }
  }

  return ret;
}

inline bool is_not_encode(char ch)
{
  bool ret = false;
  if (isalnum(ch) || ((ch) == '-') || ((ch) == '.') || ((ch) == '_') || ((ch) == '~')) {
    ret = true;
  }
  return ret;
}

int url_encode_process(char *input, int64_t len, char *&output, ObIAllocator &alloc,
                       int64_t &out_len)
{
  int ret = OB_SUCCESS;
  out_len = len;
  for (int64_t i = 0; i < len; ++i) {
    unsigned char in = (unsigned char)input[i];
    if (!is_not_encode(in)) {
      out_len += 2;
    }
  }
  if (out_len > 0) {
    output = (char *)alloc.alloc(out_len);
    if (OB_ISNULL(output)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Alloc memory failed", K(ret), K(out_len));
    } else {
      int off = 0;
      const static char HEX[] = "0123456789ABCDEF";
      for (int64_t i = 0; i < len; ++i) {
        unsigned char in = (unsigned char)input[i];
        if (is_not_encode(in)) {
          output[i + off] = input[i];
        } else {
          output[i + off] = '%';
          output[i + off + 1] = HEX[in >> 4];
          output[i + off + 2] = HEX[in & 0xf];
          off += 2;
        }
      }
    }
  }

  return ret;
}

int convert_string(const ObExpr &expr, ObEvalCtx &ctx, ObString &input_str, ObString &output_str,
                   bool is_encode)
{
  int ret = OB_SUCCESS;

  // Oceanbase url_encode and url_decode are using RFC3986 standard.

  char *res = nullptr;
  int64_t res_len = 0;

  ObEvalCtx::TempAllocGuard alloc_guard(ctx);

  if (input_str.length() == 0) {
    // Empty string '' will not be converted
  } else if (is_encode
             && OB_FAIL(url_encode_process(input_str.ptr(), input_str.length(), res,
                                           alloc_guard.get_allocator(), res_len))) {
    LOG_WARN("URL_ENCODE invalid argument.", K(ret), K(input_str), K(input_str.length()));
  } else if (!is_encode
             && OB_FAIL(url_decode_process(input_str.ptr(), input_str.length(), res,
                                           alloc_guard.get_allocator(), res_len))) {
    LOG_WARN("URL_DECODE invalid argument.", K(ret), K(input_str), K(input_str.length()));
  }

  if (OB_FAIL(ret)) {
  } else if (input_str.length() == 0) {
    output_str.reset();
  } else if (OB_ISNULL(res)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("URL_ENCODE/DECODE UNEXPECTED NULL", K(input_str), K(input_str.length()), K(is_encode),
             K(ret));
  } else {
    ObString converted_result;
    if (OB_FAIL(ObExprUtil::convert_string_collation(
          ObString(res_len, res), expr.args_[0]->datum_meta_.cs_type_, converted_result,
          expr.datum_meta_.cs_type_, alloc_guard.get_allocator()))) {
      LOG_WARN("fail to convert string", K(ret), K(input_str), K(input_str.length()), K(is_encode));
    } else {
      if (OB_FAIL(ob_write_string(ctx.get_expr_res_alloc(), converted_result, output_str, true))) {
        LOG_WARN("Copy string failed", K(ret), K(res_len));
      }
    }
  }
  return ret;
}

template <typename IN_VEC, typename OUT_VEC>
int inner_encode_string(VECTOR_EVAL_FUNC_ARG_DECL, bool is_encode)
{
  int ret = OB_SUCCESS;
  IN_VEC *in_vec = static_cast<IN_VEC *>(expr.args_[0]->get_vector(ctx));
  OUT_VEC *res_vec = static_cast<OUT_VEC *>(expr.get_vector(ctx));

  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);

  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
  batch_info_guard.set_batch_size(bound.batch_size());

  for (int64_t i = bound.start(); OB_SUCC(ret) && i < bound.end(); ++i) {
    batch_info_guard.set_batch_idx(i);
    if (skip.at(i) || eval_flags.at(i)) {
      continue;
    } else if (in_vec->is_null(i)) {
      res_vec->set_null(i);
    } else {
      ObString input_str = in_vec->get_string(i);
      ObString output_str = nullptr;
      if (OB_FAIL(convert_string(expr, ctx, input_str, output_str, is_encode))) {
        LOG_WARN("fail to convert string", K(ret), K(input_str), K(input_str.length()),
                 K(is_encode));
      } else {
        res_vec->set_string(i, output_str);
      }
    }
    if (OB_SUCC(ret)) {
      eval_flags.set(i);
    }
  }
  return ret;
}

using Discrete = ObDiscreteFormat;
using Uniform = ObUniformFormat<false>;
using UniformConst = ObUniformFormat<true>;

int ObExprURLEncode::eval_url_encode(EVAL_FUNC_ARG_DECL)
{
  return eval_url_codec(EVAL_FUNC_ARG_LIST, true);
}

int ObExprURLDecode::eval_url_decode(EVAL_FUNC_ARG_DECL)
{
  return eval_url_codec(EVAL_FUNC_ARG_LIST, false);
}

int ObExprURLCODEC::eval_url_codec(EVAL_FUNC_ARG_DECL, bool is_encode)
{
  int ret = OB_SUCCESS;
  ObDatum *input = NULL;

  if (OB_FAIL(expr.args_[0]->eval(ctx, input))) {
    LOG_WARN("fail to eval", K(ret), KPC(expr.args_[0]));
  } else if (input->is_null()) {
    expr_datum.set_null();
  } else {
    ObString input_str = input->get_string();
    ObString output_str = nullptr;
    if (OB_FAIL(convert_string(expr, ctx, input_str, output_str, is_encode))) {
      LOG_WARN("fail to convert string", K(ret), K(input_str), K(input_str.length()), K(is_encode));
    } else {
      expr_datum.set_string(output_str);
    }
  }

  return ret;
}

int ObExprURLEncode::eval_url_encode_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  return eval_url_codec_batch(BATCH_EVAL_FUNC_ARG_LIST, true);
}

int ObExprURLDecode::eval_url_decode_batch(BATCH_EVAL_FUNC_ARG_DECL)
{
  return eval_url_codec_batch(BATCH_EVAL_FUNC_ARG_LIST, false);
}

int ObExprURLCODEC::eval_url_codec_batch(BATCH_EVAL_FUNC_ARG_DECL, bool is_encode)
{
  int ret = OB_SUCCESS;
  ObDatum *results = expr.locate_batch_datums(ctx);

  if (OB_ISNULL(results)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr results frame is not init", K(ret));
  } else {
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    if (OB_FAIL(expr.args_[0]->eval_batch(ctx, skip, size))) {
      LOG_WARN("failed to eval batch result input", K(ret));
    } else {
      ObDatumVector datum_array = expr.args_[0]->locate_expr_datumvector(ctx);

      ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
      batch_info_guard.set_batch_size(size);
      for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
        batch_info_guard.set_batch_idx(i);
        if (skip.at(i) || eval_flags.at(i)) {
          continue;
        } else if (datum_array.at(i)->is_null()) {
          results[i].set_null();
        } else {
          ObString input_str = datum_array.at(i)->get_string();
          ObString output_str = nullptr;
          if (OB_FAIL(convert_string(expr, ctx, input_str, output_str, is_encode))) {
            LOG_WARN("fail to convert string", K(ret), K(input_str), K(input_str.length()),
                     K(is_encode));
          } else {
            results[i].set_string(output_str);
          }
        }
        if (OB_SUCC(ret)) {
          eval_flags.set(i);
        }
      }
    }
  }
  return ret;
}

int ObExprURLEncode::eval_url_encode_vector(VECTOR_EVAL_FUNC_ARG_DECL)
{
  return eval_url_codec_vector(VECTOR_EVAL_FUNC_ARG_LIST, true);
}

int ObExprURLDecode::eval_url_decode_vector(VECTOR_EVAL_FUNC_ARG_DECL)
{
  return eval_url_codec_vector(VECTOR_EVAL_FUNC_ARG_LIST, false);
}

#define CALC_FORMAT(IN_FORMAT, OUT_FORMAT) (IN_FORMAT << 4 | OUT_FORMAT)

#define DISPATCH_RES_FORMAT(IN, OUT)                                                               \
  case CALC_FORMAT(IN::FORMAT, OUT::FORMAT): {                                                     \
    ret = inner_encode_string<IN, OUT>(VECTOR_EVAL_FUNC_ARG_LIST, is_encode);                      \
    break;                                                                                         \
  }

int ObExprURLCODEC::eval_url_codec_vector(VECTOR_EVAL_FUNC_ARG_DECL, bool is_encode)
{
  int ret = OB_SUCCESS;

  int batch_size = bound.batch_size();
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    SQL_LOG(WARN, "failed to eval batch result input", K(ret));
  } else {
    VectorFormat res_format = expr.get_format(ctx);
    VectorFormat in_format = expr.args_[0]->get_format(ctx);

    const int64_t cond = CALC_FORMAT(in_format, res_format);
    switch (cond) {
      DISPATCH_RES_FORMAT(Discrete, Discrete);
      DISPATCH_RES_FORMAT(Discrete, Uniform);
      DISPATCH_RES_FORMAT(Discrete, UniformConst);

      DISPATCH_RES_FORMAT(Uniform, Discrete);
      DISPATCH_RES_FORMAT(Uniform, Uniform);
      DISPATCH_RES_FORMAT(Uniform, UniformConst);

      DISPATCH_RES_FORMAT(UniformConst, Discrete);
      DISPATCH_RES_FORMAT(UniformConst, Uniform);
      DISPATCH_RES_FORMAT(UniformConst, UniformConst);

    default: {
      ret = inner_encode_string<ObVectorBase, ObVectorBase>(VECTOR_EVAL_FUNC_ARG_LIST, is_encode);
    }
    }
  }
  return ret;
}

int ObExprURLEncode::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                             ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_3_5_1) {
    if (OB_UNLIKELY((1 != rt_expr.arg_cnt_ && 2 != rt_expr.arg_cnt_) || OB_ISNULL(rt_expr.args_))) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "args_ is NULL or arg_cnt_ is invalid", K(ret), K(rt_expr));
    } else {
      rt_expr.eval_func_ = eval_url_encode;
      rt_expr.eval_batch_func_ = eval_url_encode_batch;
      rt_expr.eval_vector_func_ = eval_url_encode_vector;
    }
  }else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Url_decode not supported", K(GET_MIN_CLUSTER_VERSION()),
             K(CLUSTER_VERSION_4_3_5_1), K(ret));
  }

  return ret;
}

int ObExprURLDecode::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                             ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_3_5_1) {
    if (OB_UNLIKELY((1 != rt_expr.arg_cnt_ && 2 != rt_expr.arg_cnt_) || OB_ISNULL(rt_expr.args_))) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "args_ is NULL or arg_cnt_ is invalid", K(ret), K(rt_expr));
    } else {
      rt_expr.eval_func_ = eval_url_decode;
      rt_expr.eval_batch_func_ = eval_url_decode_batch;
      rt_expr.eval_vector_func_ = eval_url_decode_vector;
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Url_decode not supported", K(GET_MIN_CLUSTER_VERSION()),
             K(CLUSTER_VERSION_4_3_5_1), K(ret));
  }

  return ret;
}

int ObExprURLCODEC::calc_result_type1(ObExprResType &type, ObExprResType &type_1,
                                      common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;

  type_1.set_calc_type(ObVarcharType);
  type_1.set_calc_collation_type(CS_TYPE_UTF8MB4_ZH_0900_AS_CS);
  type.set_varchar();
  // If the input is a string, output the same collation as the input. Otherwise, output current
  // session collation
  if (ob_is_string_tc(type_1.get_type())) {
    type.set_collation_type(type_1.get_collation_type());
  } else {
    type.set_collation_type(type_ctx.get_coll_type());
  }
  type.set_collation_level(common::CS_LEVEL_COERCIBLE);

  ObLength input_length = type_1.get_length();
  if (input_length <= 0) {
    type.set_length(OB_MAX_VARCHAR_LENGTH);
  } else {
    int64_t output_length = input_length * 3;
    if (output_length > OB_MAX_VARCHAR_LENGTH) {
      output_length = OB_MAX_VARCHAR_LENGTH;
    }
    type.set_length(output_length);
  }

  return ret;
}
} // namespace sql
} // namespace oceanbase