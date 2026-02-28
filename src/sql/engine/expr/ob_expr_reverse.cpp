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
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/engine/expr/ob_array_expr_utils.h"
#include "lib/charset/ob_charset_string_helper.h"

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
  } else if (ob_is_collection_sql_type(expr.args_[0]->datum_meta_.get_type())) {
    // array type
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
    const uint16_t subschema_id = expr.args_[0]->obj_meta_.get_subschema_id();
    ObIArrayType *src_arr = NULL;
    ObIArrayType *res_arr = NULL;
    if (subschema_id != expr.obj_meta_.get_subschema_id()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("subschema id not match", K(ret), K(subschema_id), K(expr.obj_meta_.get_subschema_id()));
    } else if (OB_FAIL(ObArrayExprUtils::get_array_obj(tmp_allocator, ctx, subschema_id, arg->get_string(), src_arr))) {
      LOG_WARN("construct array obj failed", K(ret));
    } else if (OB_FAIL(ObArrayExprUtils::construct_array_obj(tmp_allocator,ctx, subschema_id, res_arr, false))) {
      LOG_WARN("construct child array obj failed", K(ret));
    }
    for (int64_t i = 0; i < src_arr->size() && OB_SUCC(ret); i++) {
      int64_t idx = src_arr->size() - i - 1;
      if (OB_FAIL(res_arr->insert_from(*src_arr, idx, 1))) {
        LOG_WARN("failed to insert elem", K(ret), K(i), K(idx));
      }
    } //end for
    if (OB_FAIL(ret)) {
    } else {
      ObString res_str;
      if (OB_FAIL(ObArrayExprUtils::set_array_res(
              res_arr, res_arr->get_raw_binary_len(), expr, ctx, res_str))) {
        LOG_WARN("get array binary string failed", K(ret));
      } else {
        res_datum.set_string(res_str);
      }
    }
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

template <typename ArgVec, typename ResVec>
int ObExprReverse::vector_reverse_str(const ObExpr &expr,
                                      ObEvalCtx &ctx,
                                      const ObBitVector &skip,
                                      const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  VectorFormat res_format = expr.get_format(ctx);
  const ArgVec *arg0_vec = static_cast<const ArgVec *>(expr.args_[0]->get_vector(ctx));
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  bool is_oracle_mode = lib::is_oracle_mode();
  bool has_null = arg0_vec->has_null();
  bool can_do_ascii_optimize = ObCharsetStringHelper::can_do_ascii_optimize(expr.datum_meta_.cs_type_);
  int64_t char_length = 0;
  const ObCollationType &cs_type = expr.args_[0]->datum_meta_.cs_type_;
  if (!ob_is_text_tc(expr.args_[0]->datum_meta_.type_)) {
    // string type
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      if (skip.at(idx) || eval_flags.at(idx)) {
        continue;
      } else if (has_null && arg0_vec->is_null(idx)) {
        res_vec->set_null(idx);
        continue;
      }
      ObString input_str = arg0_vec->get_string(idx);
      char *buf = expr.get_str_res_mem(ctx, input_str.length(), idx);
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret), K(input_str.length()));
      } else if (is_oracle_mode ||
                 (can_do_ascii_optimize && ObCharsetStringHelper::is_ascii_str(arg0_vec->get_string(idx).ptr(), arg0_vec->get_string(idx).length()))) {
        ObExprReverse::do_reverse_ascii(input_str.ptr(), input_str.ptr() + input_str.length(), buf + input_str.length());
      } else {
        ret = ObExprReverse::do_reverse_vector(input_str.ptr(), input_str.length(), cs_type, buf);
      }
      if (OB_SUCC(ret)) {
        res_vec->set_string(idx, buf, input_str.length());
      } else {
        LOG_WARN("do_reverse failed", K(ret), K(input_str), K(cs_type));
      }
    }
  } else {
    // text tc
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      if (skip.at(idx) || eval_flags.at(idx)) {
        continue;
      } else if (has_null && arg0_vec->is_null(idx)) {
        res_vec->set_null(idx);
        continue;
      }
      ObString input_str = arg0_vec->get_string(idx);
      ObEvalCtx::TempAllocGuard alloc_guard(ctx);
      ObIAllocator &calc_alloc = alloc_guard.get_allocator();
      char *buf = NULL;
      int64_t buf_size = 0;
      int64_t total_byte_len = 0;
      const bool has_lob_header = expr.args_[0]->obj_meta_.has_lob_header();
      ObTextStringIter input_iter(expr.args_[0]->datum_meta_.type_, cs_type, input_str, has_lob_header);
      ObTextStringVectorResult<ResVec> output_result(expr.datum_meta_.type_, &expr, &ctx, res_vec, idx);
      if (OB_FAIL(input_iter.init(0, NULL, &calc_alloc))) {
        LOG_WARN("init input_iter failed ", K(ret), K(input_iter));
      } else if (OB_FAIL(input_iter.get_byte_len(total_byte_len))) {
        LOG_WARN("get input byte len failed", K(ret));
      } else if (OB_FAIL(output_result.init_with_batch_idx(total_byte_len, idx))) {
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
          ObString block_res_str;
          if (is_oracle_mode ||
              (can_do_ascii_optimize && ObCharsetStringHelper::is_ascii_str(src_block_data.ptr(), src_block_data.length()))) {
            ObExprReverse::do_reverse_ascii(src_block_data.ptr(), src_block_data.ptr() + src_block_data.length(), buf + src_block_data.length());
          } else {
            ret = ObExprReverse::do_reverse_vector(src_block_data.ptr(), src_block_data.length(), cs_type, buf);
          }
          if (OB_FAIL(output_result.lseek(src_block_data.length(), 0))) {
            LOG_WARN("result lseek failed", K(ret));
          } else {
            buf += src_block_data.length();
            buf_size -= src_block_data.length();
          }
        }
        if (OB_FAIL(ret)) {
          LOG_WARN("do_reverse failed", K(ret), K(input_str), K(cs_type));
        } else if (state != TEXTSTRING_ITER_NEXT && state != TEXTSTRING_ITER_END) {
          ret = (input_iter.get_inner_ret() != OB_SUCCESS) ?
                input_iter.get_inner_ret() : OB_INVALID_DATA;
          LOG_WARN("iter state invalid", K(ret), K(state), K(input_iter));
        } else {
          ObString res_str;
          output_result.set_result();
          output_result.get_result_buffer(res_str);
          res_vec->set_string(idx, res_str);
        }
      }
    }
  }
  return ret;
}

template <typename ArgVec, typename ResVec>
int ObExprReverse::vector_reverse_array(const ObExpr &expr,
                                        ObEvalCtx &ctx,
                                        const ObBitVector &skip,
                                        const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  VectorFormat res_format = expr.get_format(ctx);
  const ArgVec *arg0_vec = static_cast<const ArgVec *>(expr.args_[0]->get_vector(ctx));
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  bool is_oracle_mode = lib::is_oracle_mode();
  bool has_null = arg0_vec->has_null();
  int64_t char_length = 0;
  const ObCollationType &cs_type = expr.args_[0]->datum_meta_.cs_type_;
  for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
    if (skip.at(idx) || eval_flags.at(idx)) {
      continue;
    } else if (has_null && arg0_vec->is_null(idx)) {
      res_vec->set_null(idx);
      continue;
    }
    ObString input_str = arg0_vec->get_string(idx);
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
    const uint16_t subschema_id = expr.args_[0]->obj_meta_.get_subschema_id();
    ObIArrayType *src_arr = NULL;
    ObIArrayType *res_arr = NULL;
    if (subschema_id != expr.obj_meta_.get_subschema_id()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("subschema id not match", K(ret), K(subschema_id), K(expr.obj_meta_.get_subschema_id()));
    } else if (OB_FAIL(ObArrayExprUtils::get_array_obj(tmp_allocator, ctx, subschema_id, input_str, src_arr))) {
      LOG_WARN("construct array obj failed", K(ret));
    } else if (OB_FAIL(ObArrayExprUtils::construct_array_obj(tmp_allocator, ctx, subschema_id, res_arr, false))) {
      LOG_WARN("construct child array obj failed", K(ret));
    }
    for (int64_t i = 0; i < src_arr->size() && OB_SUCC(ret); i++) {
      int64_t rev_idx = src_arr->size() - i - 1;
      if (OB_FAIL(res_arr->insert_from(*src_arr, rev_idx, 1))) {
        LOG_WARN("failed to insert elem", K(ret), K(i), K(rev_idx));
      }
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(ObArrayExprUtils::set_array_res<ResVec>(res_arr, expr, ctx, res_vec, idx))) {
      LOG_WARN("set array res failed", K(ret));
    }
  }
  return ret;
}

int ObExprReverse::calc_reverse_expr_vector(VECTOR_EVAL_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("failed to eval vector result args0", K(ret));
  } else {
    VectorFormat arg_format = expr.args_[0]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    if (ob_is_collection_sql_type(expr.args_[0]->datum_meta_.get_type())) {
      if (VEC_DISCRETE == arg_format && VEC_DISCRETE == res_format) {
        ret = vector_reverse_array<StrDiscVec, StrDiscVec>(expr, ctx, skip, bound);
      } else if (VEC_UNIFORM == arg_format && VEC_DISCRETE == res_format) {
        ret = vector_reverse_array<StrUniVec, StrDiscVec>(expr, ctx, skip, bound);
      } else if (VEC_CONTINUOUS == arg_format && VEC_DISCRETE == res_format) {
        ret = vector_reverse_array<StrContVec, StrDiscVec>(expr, ctx, skip, bound);
      } else if (VEC_DISCRETE == arg_format && VEC_UNIFORM == res_format) {
        ret = vector_reverse_array<StrDiscVec, StrUniVec>(expr, ctx, skip, bound);
      } else if (VEC_UNIFORM == arg_format && VEC_UNIFORM == res_format) {
        ret = vector_reverse_array<StrUniVec, StrUniVec>(expr, ctx, skip, bound);
      } else if (VEC_CONTINUOUS == arg_format && VEC_UNIFORM == res_format) {
        ret = vector_reverse_array<StrContVec, StrUniVec>(expr, ctx, skip, bound);
      } else {
        ret = vector_reverse_array<ObVectorBase, ObVectorBase>(expr, ctx, skip, bound);
      }
    } else {
      if (VEC_DISCRETE == arg_format && VEC_DISCRETE == res_format) {
        ret = vector_reverse_str<StrDiscVec, StrDiscVec>(expr, ctx, skip, bound);
      } else if (VEC_UNIFORM == arg_format && VEC_DISCRETE == res_format) {
        ret = vector_reverse_str<StrUniVec, StrDiscVec>(expr, ctx, skip, bound);
      } else if (VEC_CONTINUOUS == arg_format && VEC_DISCRETE == res_format) {
        ret = vector_reverse_str<StrContVec, StrDiscVec>(expr, ctx, skip, bound);
      } else if (VEC_DISCRETE == arg_format && VEC_UNIFORM == res_format) {
        ret = vector_reverse_str<StrDiscVec, StrUniVec>(expr, ctx, skip, bound);
      } else if (VEC_UNIFORM == arg_format && VEC_UNIFORM == res_format) {
        ret = vector_reverse_str<StrUniVec, StrUniVec>(expr, ctx, skip, bound);
      } else if (VEC_CONTINUOUS == arg_format && VEC_UNIFORM == res_format) {
        ret = vector_reverse_str<StrContVec, StrUniVec>(expr, ctx, skip, bound);
      } else {
        ret = vector_reverse_str<ObVectorBase, ObVectorBase>(expr, ctx, skip, bound);
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
  rt_expr.eval_vector_func_ = calc_reverse_expr_vector;
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
