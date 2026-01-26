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

#include "ob_expr_from_base64.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace sql
{

ObExprFromBase64::ObExprFromBase64(ObIAllocator &alloc)
:ObStringExprOperator(alloc, T_FUN_SYS_FROM_BASE64, N_FROM_BASE64, 1, VALID_FOR_GENERATED_COL)
{
}

ObExprFromBase64::~ObExprFromBase64()
{
}

int ObExprFromBase64::calc(ObObj &result,
                           const ObObj &obj,
                           ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null allocator", K(ret), K(allocator));
  } else {
    const ObString & in_raw = obj.get_string();
    ObLength in_raw_len = in_raw.length();
    if (OB_UNLIKELY(in_raw_len == 0)) {
      result.set_string(obj.get_type(), nullptr, 0);
    } else {
      const char *buf = in_raw.ptr();
      int64_t buf_len = base64_needed_decoded_length(in_raw_len);
      int64_t pos = 0;
      char *output_buf = static_cast<char*>(allocator->alloc(buf_len));
      if (OB_ISNULL(output_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("output_buf is null", K(ret), K(buf_len), K(in_raw_len));
        result.set_null();
      } else if (OB_FAIL(ObBase64Encoder::decode(buf, in_raw_len,
                                                 reinterpret_cast<uint8_t*>(output_buf),
                                                 buf_len, pos, true))) {
        if (OB_UNLIKELY(ret == OB_INVALID_ARGUMENT)) {
          ret = OB_SUCCESS;
          result.set_null();
        } else {
          LOG_WARN("failed to decode base64", K(ret));
        }
      } else {
        result.set_string(obj.get_type(), output_buf, pos);
        result.set_collation_level(obj.get_collation_level());
        result.set_collation_type(obj.get_collation_type());
      }
    }
  }
  return ret;
}

int ObExprFromBase64::calc_result_type1(ObExprResType &type,
                                        ObExprResType &str,
                                        ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;

  if (ob_is_string_type(str.get_type())) {
    str.set_calc_type(str.get_type());
    str.set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
  } else {
    str.set_calc_type(ObVarcharType);
    str.set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
  }

  int64_t mbmaxlen = 0;
  int64_t max_result_length = 0;
  if (OB_FAIL(common::ObCharset::get_mbmaxlen_by_coll(str.get_collation_type(), mbmaxlen))) {
    LOG_WARN("fail to get mbmaxlen", K(type.get_collation_type()), K(ret));
  } else {
    max_result_length = base64_needed_decoded_length(str.get_length()) * mbmaxlen;
    if (max_result_length > OB_MAX_BLOB_WIDTH) {
      max_result_length = OB_MAX_BLOB_WIDTH;
    }
    int64_t max_l = max_result_length / mbmaxlen;
    int64_t max_deduce_length = max_l * mbmaxlen;
    if (max_deduce_length < OB_MAX_MYSQL_VARCHAR_LENGTH) {
      type.set_varbinary();
      type.set_length(max_deduce_length);
      type.set_collation_level(CS_LEVEL_IMPLICIT);
    } else {
      type.set_blob();
      // TODO : Fixme the blob type do not need to set_length.
      // Maybe need wait ObDDLResolver::check_text_length fix the judge of length.
      type.set_length(max_deduce_length);
    }
  }

  return ret;
}

int ObExprFromBase64::eval_from_base64(const ObExpr &expr,
                                       ObEvalCtx &ctx,
                                       ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *arg = nullptr;
  ObString in_raw;
  int64_t in_raw_len;
  const char *buf;
  ObEvalCtx::TempAllocGuard alloc_guard(ctx);
  if (OB_FAIL(expr.args_[0]->eval(ctx, arg))) {
    LOG_WARN("eval arg failed", K(ret));
  } else if (OB_UNLIKELY(arg->is_null())) {
    res.set_null();
  } else {
    if(ob_is_text_tc(expr.args_[0]->datum_meta_.get_type())) {
      ObLobLocatorV2 locator(arg->get_string(), expr.args_[0]->obj_meta_.has_lob_header());
      if (OB_FAIL(locator.get_lob_data_byte_len(in_raw_len))) {
        LOG_WARN("get lob data byte length failed", K(ret), K(locator));
      } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(alloc_guard.get_allocator(), *arg,
                        expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), in_raw))) {
        LOG_WARN("failed to get string data", K(ret), K(expr.args_[0]->datum_meta_));
      } else {
        buf = in_raw.ptr();
      }
    } else {
      in_raw = arg->get_string();
      in_raw_len = in_raw.length();
      buf = in_raw.ptr();
    }
    if (OB_SUCC(ret)) {
      if (NULL == buf) {
        res.set_string(nullptr, 0);
      } else {
        char *output_buf = NULL;
        int64_t buf_len = base64_needed_decoded_length(in_raw_len);
        int64_t pos = 0;
        ObEvalCtx::TempAllocGuard alloc_guard(ctx);
        output_buf = static_cast<char*>(alloc_guard.get_allocator().alloc(buf_len));
        if (OB_FAIL(ObBase64Encoder::decode(buf, in_raw_len,
                                            reinterpret_cast<uint8_t*>(output_buf),
                                            buf_len, pos, true))) {
          if (OB_UNLIKELY(ret == OB_INVALID_ARGUMENT)) {
            ret = OB_SUCCESS;
            res.set_null();
          } else {
            LOG_WARN("failed to decode base64", K(ret));
          }
        } else {
          res.set_string(output_buf, pos);
          if (OB_FAIL(ObExprUtil::set_expr_ascii_result(
            expr, ctx, res, ObString(pos, output_buf)))) {
            LOG_WARN("set ASCII result failed", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObExprFromBase64::eval_from_base64_batch(const ObExpr &expr, ObEvalCtx &ctx,
                           const ObBitVector &skip,
                           const int64_t batch_size) {
  int ret = OB_SUCCESS;
  ObDatum *res = expr.locate_batch_datums(ctx);
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ObDatum *arg = nullptr;
  ObString in_raw;
  int64_t in_raw_len;
  const char *buf;

  if (OB_FAIL(expr.args_[0]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("eval arg failed", K(ret));
  } else {
    ObDatumVector args = expr.args_[0]->locate_expr_datumvector(ctx);
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
    batch_info_guard.set_batch_size(batch_size);
    for (int64_t j = 0; OB_SUCC(ret) && j < batch_size; ++j) {
      if (skip.at(j) || eval_flags.at(j)) {
        continue;
      }
      batch_info_guard.set_batch_idx(j);
      arg = args.at(j);
      if (ob_is_text_tc(expr.args_[0]->datum_meta_.get_type())) {
        ObLobLocatorV2 locator(arg->get_string(), expr.args_[0]->obj_meta_.has_lob_header());
        if (OB_FAIL(locator.get_lob_data_byte_len(in_raw_len))) {
          LOG_WARN("get lob data byte length failed", K(ret), K(locator));
        } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(alloc_guard.get_allocator(), *arg,
                          expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), in_raw))) {
          LOG_WARN("failed to get string data", K(ret), K(expr.args_[0]->datum_meta_));
        } else {
          buf = in_raw.ptr();
        }
      } else {
        in_raw = arg->get_string();
        in_raw_len = in_raw.length();
        buf = in_raw.ptr();
      }

      if (OB_SUCC(ret)) {
          if (arg->is_null()) {
            res[j].set_null();
          } else if (NULL == buf) {
            res[j].set_string(nullptr, 0);
          } else {
            char *output_buf = nullptr;
            int64_t buf_len = base64_needed_decoded_length(in_raw_len);
            int64_t pos = 0;
            output_buf = static_cast<char*>(alloc_guard.get_allocator().alloc(buf_len));
            if (OB_FAIL(ObBase64Encoder::decode(buf, in_raw_len,
                                                reinterpret_cast<uint8_t*>(output_buf),
                                                buf_len, pos, true))) {
              if (OB_UNLIKELY(ret == OB_INVALID_ARGUMENT)) {
                ret = OB_SUCCESS;
                res[j].set_null();
              } else {
                LOG_WARN("failed to decode base64", K(ret));
              }
            } else {
              res[j].set_string(output_buf, pos);
              if (OB_FAIL(ObExprUtil::set_expr_ascii_result(
                expr, ctx, res[j], ObString(pos, output_buf)))) {
                LOG_WARN("set ASCII result failed", K(ret));
              }
            }
          }
        } // end for batch
      }

  }
  return ret;
}

int ObExprFromBase64::eval_from_base64_vector(VECTOR_EVAL_FUNC_ARG_DECL) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval frombase64 param", K(ret));
  } else {
    VectorFormat arg_format = expr.args_[0]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    bool is_text = ob_is_text_tc(expr.args_[0]->datum_meta_.get_type());
    if (is_text) {
      if (VEC_DISCRETE == arg_format && VEC_DISCRETE == res_format) {
        ret = vector_from_base64<StrDiscVec, StrDiscVec, true>(VECTOR_EVAL_FUNC_ARG_LIST);
      } else if (VEC_CONTINUOUS == arg_format && VEC_DISCRETE == res_format) {
        ret = vector_from_base64<StrContVec, StrDiscVec, true>(VECTOR_EVAL_FUNC_ARG_LIST);
      } else if (VEC_UNIFORM == arg_format && VEC_DISCRETE == res_format) {
        ret = vector_from_base64<StrUniVec, StrDiscVec, true>(VECTOR_EVAL_FUNC_ARG_LIST);
      } else if (VEC_DISCRETE == arg_format && VEC_CONTINUOUS == res_format) {
        ret = vector_from_base64<StrDiscVec, StrContVec, true>(VECTOR_EVAL_FUNC_ARG_LIST);
      } else if (VEC_CONTINUOUS == arg_format && VEC_CONTINUOUS == res_format) {
        ret = vector_from_base64<StrContVec, StrContVec, true>(VECTOR_EVAL_FUNC_ARG_LIST);
      } else if (VEC_UNIFORM == arg_format && VEC_CONTINUOUS == res_format) {
        ret = vector_from_base64<StrUniVec, StrContVec, true>(VECTOR_EVAL_FUNC_ARG_LIST);
      } else if (VEC_DISCRETE == arg_format && VEC_UNIFORM == res_format) {
        ret = vector_from_base64<StrDiscVec, StrUniVec, true>(VECTOR_EVAL_FUNC_ARG_LIST);
      } else if (VEC_CONTINUOUS == arg_format && VEC_UNIFORM == res_format) {
        ret = vector_from_base64<StrContVec, StrUniVec, true>(VECTOR_EVAL_FUNC_ARG_LIST);
      } else if (VEC_UNIFORM == arg_format && VEC_UNIFORM == res_format) {
        ret = vector_from_base64<StrUniVec, StrUniVec, true>(VECTOR_EVAL_FUNC_ARG_LIST);
      } else {
        ret = vector_from_base64<ObVectorBase, ObVectorBase, true>(VECTOR_EVAL_FUNC_ARG_LIST);
      }
    } else {
      if (VEC_DISCRETE == arg_format && VEC_DISCRETE == res_format) {
        ret = vector_from_base64<StrDiscVec, StrDiscVec, false>(VECTOR_EVAL_FUNC_ARG_LIST);
      } else if (VEC_CONTINUOUS == arg_format && VEC_DISCRETE == res_format) {
        ret = vector_from_base64<StrContVec, StrDiscVec, false>(VECTOR_EVAL_FUNC_ARG_LIST);
      } else if (VEC_UNIFORM == arg_format && VEC_DISCRETE == res_format) {
        ret = vector_from_base64<StrUniVec, StrDiscVec, false>(VECTOR_EVAL_FUNC_ARG_LIST);
      } else if (VEC_DISCRETE == arg_format && VEC_CONTINUOUS == res_format) {
        ret = vector_from_base64<StrDiscVec, StrContVec, false>(VECTOR_EVAL_FUNC_ARG_LIST);
      } else if (VEC_CONTINUOUS == arg_format && VEC_CONTINUOUS == res_format) {
        ret = vector_from_base64<StrContVec, StrContVec, false>(VECTOR_EVAL_FUNC_ARG_LIST);
      } else if (VEC_UNIFORM == arg_format && VEC_CONTINUOUS == res_format) {
        ret = vector_from_base64<StrUniVec, StrContVec, false>(VECTOR_EVAL_FUNC_ARG_LIST);
      } else if (VEC_DISCRETE == arg_format && VEC_UNIFORM == res_format) {
        ret = vector_from_base64<StrDiscVec, StrUniVec, false>(VECTOR_EVAL_FUNC_ARG_LIST);
      } else if (VEC_CONTINUOUS == arg_format && VEC_UNIFORM == res_format) {
        ret = vector_from_base64<StrContVec, StrUniVec, false>(VECTOR_EVAL_FUNC_ARG_LIST);
      } else if (VEC_UNIFORM == arg_format && VEC_UNIFORM == res_format) {
        ret = vector_from_base64<StrUniVec, StrUniVec, false>(VECTOR_EVAL_FUNC_ARG_LIST);
      } else {
        ret = vector_from_base64<ObVectorBase, ObVectorBase, false>(VECTOR_EVAL_FUNC_ARG_LIST);
      }
    }

  }
  return ret;
}

template<typename ArgVec, typename ResVec, bool isText>
int ObExprFromBase64::vector_from_base64(VECTOR_EVAL_FUNC_ARG_DECL) {
  int ret = OB_SUCCESS;
  ArgVec *arg_vec = static_cast<ArgVec *>(expr.args_[0]->get_vector(ctx));
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
  ObEvalCtx::TempAllocGuard alloc_guard(ctx);
  char *output_buf = nullptr;
  ObString in_raw;
  int64_t in_raw_len;
  const char *buf;
  batch_info_guard.set_batch_size(bound.batch_size());
  for (int i = bound.start(); i < bound.end() && OB_SUCC(ret); ++i) {
    if (OB_LIKELY(!(skip.at(i) || eval_flags.at(i)))) {
      batch_info_guard.set_batch_idx(i);
      if (arg_vec->is_null(i)) {
        res_vec->set_null(i);
      } else {
        if constexpr (isText) {
          ObLobLocatorV2 locator(arg_vec->get_string(i), expr.args_[0]->obj_meta_.has_lob_header());
          if (OB_FAIL(locator.get_lob_data_byte_len(in_raw_len))) {
            LOG_WARN("get lob data byte length failed", K(ret), K(locator));
          } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(alloc_guard.get_allocator(), arg_vec,
                            expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), in_raw, i))) {
            LOG_WARN("failed to get string data", K(ret), K(expr.args_[0]->datum_meta_));
          } else {
            buf = in_raw.ptr();
          }
        } else {
          in_raw = arg_vec->get_string(i);
          in_raw_len = in_raw.length();
          buf = in_raw.ptr();
        }
        if (OB_SUCC(ret)) {
          if (in_raw.empty()) {
            res_vec->set_string(i, nullptr, 0);
          } else {
            int64_t buf_len = base64_needed_decoded_length(in_raw_len);
            int64_t pos = 0;
            output_buf = static_cast<char *>(alloc_guard.get_allocator().alloc(buf_len));
            if (OB_FAIL(ObBase64Encoder::decode_with_simd(buf, in_raw_len, reinterpret_cast<uint8_t *>(output_buf), buf_len, pos, /* skip_spaces= */true))) {
              if (OB_UNLIKELY(ret == OB_INVALID_ARGUMENT)) {
                ret = OB_SUCCESS;
                res_vec->set_null(i);
              } else {
                LOG_WARN("failed to decode base64", K(ret));
              }
            } else {
              ObTextStringDatumResult res_text(expr.datum_meta_.type_, &expr, &ctx, res_vec, i);
              res_vec->set_string(i, output_buf, pos);
              if (OB_FAIL(ObExprUtil::set_expr_ascii_result(expr, ctx, res_text, ObString(pos, output_buf), i, /* is_ascii= */true))) {
                LOG_WARN("set expr ascii result failed", K(ret));
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObExprFromBase64::cg_expr(ObExprCGCtx &expr_cg_ctx,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  rt_expr.eval_func_ = ObExprFromBase64::eval_from_base64;
  rt_expr.eval_batch_func_ = ObExprFromBase64::eval_from_base64_batch;
  rt_expr.eval_vector_func_ = ObExprFromBase64::eval_from_base64_vector;
  return ret;
}

}//namespace sql
}//namespace oceanbase
