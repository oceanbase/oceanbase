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
#include "sql/engine/expr/ob_expr_md5_concat_ws.h"
#include "sql/engine/expr/ob_expr_concat_ws.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/expr/ob_expr_md5.h"
#include <openssl/md5.h>

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

ObExprMd5ConcatWs::ObExprMd5ConcatWs(ObIAllocator &alloc)
    :ObStringExprOperator(alloc, T_FUN_MD5_CNN_WS, N_MD5_CONCAT_WS, PARAM_NUM_UNKNOWN, VALID_FOR_GENERATED_COL)
{
  need_charset_convert_ = false;
}

ObExprMd5ConcatWs::~ObExprMd5ConcatWs() {}

int ObExprMd5ConcatWs::calc_result_typeN(
                       ObExprResType &type,
                       ObExprResType *types,
                       int64_t param_num,
                       ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (!is_mysql_mode()) {
    ret = OB_ERR_FUNCTION_UNKNOWN;
    LOG_WARN("md5_concat_ws only support on mysql mode", K(ret));
  } else if (param_num <= 2) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument number, param should not less than 2", K(ret), K(param_num));
  } else {
    bool has_text = false;
    for (int64_t i = 0; !has_text && i < param_num; ++i) {
      if (ObTinyTextType != types[i].get_type() && types[i].is_lob()) {
        has_text = true;
      }
    }
    if (has_text) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("md5_concat_ws func not support lob", K(ret));
    } else {
      type.set_varchar();
      static const int64_t MD5_RES_BIT_LENGTH = 32;
      type.set_length(MD5_RES_BIT_LENGTH);
      type.set_collation_type(get_default_collation_type(type.get_type(), type_ctx));
      type.set_collation_level(CS_LEVEL_COERCIBLE);
    }
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; i < param_num; ++i) {
        types[i].set_calc_type(type.get_type());
      }
      if (OB_FAIL(aggregate_charsets_for_string_result(type, types, param_num, type_ctx))) {
        LOG_WARN("aggregate_charsets_for_string_result failed", K(ret));
      } else {
        for (int64_t i = 0; i < param_num; i++) {
          types[i].set_calc_collation_type(type.get_collation_type());
          types[i].set_calc_collation_level(type.get_collation_level());
        }
      }
    }
  }
  return ret;
}

int ObExprMd5ConcatWs::calc_md5_concat_ws_expr(
                       const ObExpr &expr,
                       ObEvalCtx &ctx,
                       ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *sep = NULL;
  ObDatum *replace_null_dat = NULL;
  ObExprStrResAlloc res_alloc(expr, ctx);
  if (OB_UNLIKELY(2 >= expr.arg_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg cnt", K(ret), K(expr.arg_cnt_));
  } else if (OB_FAIL(expr.eval_param_value(ctx, sep, replace_null_dat))) {
    LOG_WARN("eval param failed", K(ret));
  } else if (sep->is_null() || replace_null_dat->is_null()) {
    res.set_null();
  } else {
    const ObString &sep_str = expr.locate_param_datum(ctx, 0).get_string();
    const ObString &replace_null_string = expr.locate_param_datum(ctx, 1).get_string();
    ObSEArray<ObString, 32> words;

    for (int64_t i = 2; OB_SUCC(ret) && i < expr.arg_cnt_; ++i) {
      const ObDatum &dat = expr.locate_param_datum(ctx, i);
      if (!dat.is_null() && OB_FAIL(words.push_back(dat.get_string()))) {
        LOG_WARN("push back string failed", K(ret), K(i));
      } else if (dat.is_null() && OB_FAIL(words.push_back(replace_null_string))) {
        LOG_WARN("fail to push back null string", K(ret));
      }
    }
    ObString res_str;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(concat_and_calc_md5(ctx, sep_str, words, res_alloc, res_str))) {
      LOG_WARN("fail to calc md5 result", K(ret), K(sep_str), K(res_str));
    } else {
      if (OB_UNLIKELY(res_str.length() <= 0)) {
        res.set_null();
      } else {
        res.set_string(res_str);
      }
    }
  }
  return ret;
}

int ObExprMd5ConcatWs::calc_md5_concat_ws_vector(
                       VECTOR_EVAL_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(skip.accumulate_bit_cnt(bound) == bound.range_size())) {
    // do nothing
  } else if (OB_FAIL(expr.eval_vector_param_value(ctx, skip, bound))) {
    // this logic copy from regex replace
    if (lib::is_mysql_mode() && ret == OB_ERR_INCORRECT_STRING_VALUE) {//compatible mysql
      ret = OB_SUCCESS;
      ObVectorBase* res_vec = static_cast<ObVectorBase *>(expr.get_vector(ctx));
      for (int64_t i = bound.start(); i < bound.end(); i++) {
        res_vec->set_null(i);
      }
      const char *charset_name = ObCharset::charset_name(expr.args_[0]->datum_meta_.cs_type_);
      int64_t charset_name_len = strlen(charset_name);
      const char *tmp_char = NULL;
      LOG_USER_WARN(OB_ERR_INVALID_CHARACTER_STRING, static_cast<int>(charset_name_len),
                    charset_name, 0, tmp_char);
    } else {
      LOG_WARN("evaluate parameters failed", K(ret));
    }
  } else {
    // cause of the input vector params length would change, use the vector base class container
    ret = vector_md5_concat_ws<ObVectorBase, ObVectorBase>(VECTOR_EVAL_FUNC_ARG_LIST);
  }
  return ret;
}

template <typename StrVec, typename ResVec>
int ObExprMd5ConcatWs::vector_md5_concat_ws(
                       VECTOR_EVAL_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  #define GET_VECTOR(arg_idx) expr.arg_cnt_ > arg_idx ? \
    static_cast<StrVec *>(expr.args_[arg_idx]->get_vector(ctx)) : NULL
  const StrVec *sep_vec = GET_VECTOR(0);
  const StrVec *replace_null_value_vec = GET_VECTOR(1);
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  bool null_result = (sep_vec != NULL && sep_vec->is_null(0)) ||
                     (replace_null_value_vec != NULL && replace_null_value_vec->is_null(0));
  bool is_result_all_null = false;
  ObString sep, replace_null_string;
  ObExprStrResAlloc res_alloc(expr, ctx);
  if (OB_ISNULL(sep_vec) || OB_ISNULL(replace_null_value_vec)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sep_vec or replace_null_string is nullptr", K(ret), KP(sep_vec),
             KP(replace_null_value_vec));
  } else if (sep_vec->is_null(0) || replace_null_value_vec->is_null(0)) {
    is_result_all_null = true;
  } else {
    sep = sep_vec->get_string(0);
    replace_null_string = replace_null_value_vec->get_string(0);
  }
  for (int bound_idx = bound.start(); bound_idx < bound.end() && OB_SUCC(ret); bound_idx++) {
    if (skip.at(bound_idx) || eval_flags.at(bound_idx)) {
      continue;
    } else if (is_result_all_null) {
      res_vec->set_null(bound_idx);
      eval_flags.set(bound_idx);
    } else {
      ObSEArray<ObString, 32> words;
      // get all strs from vector
      for (int64_t arg_idx = 2; OB_SUCC(ret) && arg_idx < expr.arg_cnt_; ++arg_idx) {
        const StrVec *tmp_vec = GET_VECTOR(arg_idx);
        if (OB_ISNULL(tmp_vec)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("vec is null", K(ret), KP(tmp_vec));
        } else {
          bool is_null = tmp_vec->is_null(bound_idx);
          if (!is_null && OB_FAIL(words.push_back(tmp_vec->get_string(bound_idx)))) {
            LOG_WARN("push back string failed", K(ret), K(bound_idx), K(arg_idx));
          } else if (is_null && OB_FAIL(words.push_back(replace_null_string))) {
            LOG_WARN("fail to push back null string", K(ret), K(bound_idx), K(arg_idx));
          }
        }
      }
      ObString res_str;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(concat_and_calc_md5(ctx, sep, words, res_alloc, res_str))) {
        LOG_WARN("fail to calc md5 result", K(ret), K(sep), K(res_str));
      } else {
        if (OB_UNLIKELY(res_str.length() <= 0)) {
          res_vec->set_null(bound_idx);
        } else {
          res_vec->set_string(bound_idx, res_str);
        }
        LOG_DEBUG("gen vectro result", K(bound_idx), K(res_str), KP(res_str.ptr()), K(res_vec->get_string(bound_idx)));
      }
      eval_flags.set(bound_idx);
    }
  }
  return ret;
}

int ObExprMd5ConcatWs::concat_and_calc_md5(
                       ObEvalCtx &ctx,
                       const ObString sep_str,
                       const ObIArray<ObString> &words,
                       ObExprStrResAlloc &res_alloc,
                       ObString &res)
{
  int ret = OB_SUCCESS;
  res.reset();
  ObString tmp_str;
  if (0 == words.count()) {
    res.reset();
  } else {
    // use tmp allocator to generate tmp result
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    if (OB_FAIL(ObExprConcatWs::calc(sep_str, words, alloc_guard.get_allocator(), tmp_str))) {
      LOG_WARN("calc concat ws failed", K(ret));
    } else {
    // clac md5 result
      ObString::obstr_size_t md5_raw_res_len = MD5_LENGTH;
      // convert md5 sum to hexadecimal string, we need double bytes.
      // an extra byte for '\0' at the end of md5 str.
      ObString::obstr_size_t md5_hex_res_len = MD5_LENGTH * 2 + 1;
      char *md5_raw_res_buf = static_cast<char*>(alloc_guard.get_allocator().alloc(md5_raw_res_len));
      char *md5_hex_res_buf = static_cast<char*>(res_alloc.alloc(md5_hex_res_len));
      if (OB_ISNULL(md5_raw_res_buf) || OB_ISNULL(md5_hex_res_buf)) {
        res.assign(NULL, 0);
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("alloc memory failed", K(ret), K(md5_raw_res_buf),
                  K(md5_hex_res_buf), K(md5_raw_res_len), K(md5_hex_res_len));
      } else {
        unsigned char *md5_res = MD5(reinterpret_cast<const unsigned char *>(tmp_str.ptr()),
                                     tmp_str.length(),
                                     reinterpret_cast<unsigned char *>(md5_raw_res_buf));
        if (OB_ISNULL(md5_res)) {
          // MD5() in openssl always return an pointer not NULL, so we need not check return value.
          // Even so, we HAVE TO check it here. You know it.
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("md5 res is null", K(ret), K(res));
        } else if (OB_FAIL(to_hex_cstr(md5_raw_res_buf, md5_raw_res_len,
                                        md5_hex_res_buf, md5_hex_res_len))) {
          res.assign(NULL, 0);
          LOG_WARN("to hex cstr error", K(ret));
        } else {
          size_t tmp_len = ObCharset::casedn(CS_TYPE_UTF8MB4_BIN,
                                              md5_hex_res_buf,
                                              md5_hex_res_len,
                                              md5_hex_res_buf,
                                              md5_hex_res_len);
          ObString::obstr_size_t len = static_cast<ObString::obstr_size_t>(tmp_len);
          res.assign_buffer(md5_hex_res_buf, len);
          res.set_length(len-1); /* do not contain \0 in the result */
        }
      }
      LOG_DEBUG("compute md5 result", K(ret), K(tmp_str));
    }
  }
  return ret;
}
int ObExprMd5ConcatWs::cg_expr(
                       ObExprCGCtx &expr_cg_ctx,
                       const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
    LOG_WARN("fail to get min data version", KR(ret));
  } else if ((data_version < MOCK_DATA_VERSION_4_3_5_4) || (data_version >= DATA_VERSION_4_4_0_0 && data_version < DATA_VERSION_4_4_2_0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("data version below 4.3.5.4 or between 4.4.0.0 and 4.4.2.0, not support this function", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "data version below 4.3.5.4 or between 4.4.0.0 and 4.4.2.0, mds_concat_ws func is not supported");
  } else {
    rt_expr.eval_func_ = calc_md5_concat_ws_expr;
    if (rt_expr.arg_cnt_ >= 2) {
      bool vector_flag = true;
      for (int i = 2; i < rt_expr.arg_cnt_; i++) {
        if (!rt_expr.args_[i]->is_batch_result()) {
          vector_flag = false;
          break;
        }
      }
      if (vector_flag) {
        rt_expr.eval_vector_func_ = calc_md5_concat_ws_vector;
      }
    }
  }
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprMd5ConcatWs, raw_expr) {
  int ret = OB_SUCCESS;
  SET_LOCAL_SYSVAR_CAPACITY(1);
  EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_COLLATION_CONNECTION);
  return ret;
}

} //namespace sql
} //namespace oceanbase
