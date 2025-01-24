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
#include "sql/engine/expr/ob_expr_concat_ws.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

ObExprConcatWs::ObExprConcatWs(ObIAllocator &alloc)
    :ObStringExprOperator(alloc, T_FUN_CNN_WS, N_CONCAT_WS, PARAM_NUM_UNKNOWN, VALID_FOR_GENERATED_COL)
{
  need_charset_convert_ = false;
}

ObExprConcatWs::~ObExprConcatWs() {}

static bool enable_return_longtext()
{
  const uint64_t min_cluster_version = GET_MIN_CLUSTER_VERSION();
  // [4.2.5, 4.3.0) || [4.3.3, )
  return (min_cluster_version >= CLUSTER_VERSION_4_3_3_0)
    || (MOCK_CLUSTER_VERSION_4_2_5_0 <= min_cluster_version && min_cluster_version < CLUSTER_VERSION_4_3_0_0);
}

int ObExprConcatWs::calc_result_typeN(ObExprResType &type,
                                      ObExprResType *types,
                                      int64_t param_num,
                                      ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (!is_mysql_mode()) {
    ret = OB_ERR_FUNCTION_UNKNOWN;
    LOG_WARN("concat_ws only support on mysql mode", K(ret));
  } else if (param_num <= 1) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument number, param should not less than 1", K(ret), K(param_num));
  } else {
    bool has_text = false;
    for (int64_t i = 0; !has_text && i < param_num; ++i) {
      if (ObTinyTextType != types[i].get_type() && types[i].is_lob()) {
        has_text = true;
      }
    }
    if (has_text && enable_return_longtext()) {
      type.set_type(ObLongTextType);
    } else {
      type.set_varchar();
    }

    ObLength len = 0;
    for (int64_t i = 1; i < param_num; ++i) {
      len += types[i].get_length();
      types[i].set_calc_type(type.get_type());
    }
    len += static_cast<ObLength>(types[0].get_length() * (param_num - 1));
    types[0].set_calc_type(type.get_type());
    type.set_length(len);
    if (OB_FAIL(aggregate_charsets_for_string_result(type, types, param_num, type_ctx))) {
      LOG_WARN("aggregate_charsets_for_string_result failed", K(ret));
    } else {
      for (int64_t i = 0; i < param_num; i++) {
        types[i].set_calc_collation_type(type.get_collation_type());
        types[i].set_calc_collation_level(type.get_collation_level());
      }
    }
  }
  return ret;
}


int ObExprConcatWs::concat_ws(const ObString obj1,//separator
                              const ObString obj2,//next string to connect
                              const int64_t buf_len,
                              char **buf,
                              int64_t &buf_pos)// writing position of buf now
{
  int ret = OB_SUCCESS;
  int32_t len1 = obj1.length();
  int32_t len2 = obj2.length();
  if (buf_pos + len1 > buf_len) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("extend length limit.", K(ret), K(buf_pos), K(len1));
  } else {
    MEMCPY(*buf + buf_pos, obj1.ptr(), len1);//separator
    buf_pos += len1;
  }

  if (OB_SUCC(ret)) {
    if (buf_pos + len2 > buf_len) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("extend length limit.", K(ret), K(buf_pos), K(len2));
    } else {
      MEMCPY(*buf + buf_pos, obj2.ptr(), len2);//next string to connect
      buf_pos +=  len2;
    }
  }
  return ret;
}

// for engine 3.0
// make sure alloc.alloc() is called only once
int ObExprConcatWs::calc(const ObString &sep_str, const ObIArray<ObString> &words,
                         ObIAllocator &alloc, ObString &res_str)
{
  int ret = OB_SUCCESS;
  res_str.reset();

  int64_t tmp_alloc_len = 0;
  int64_t alloc_len = 0;
  if (OB_UNLIKELY(0 >= words.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid num of words", K(ret), K(words.count()));
  }
  // calc total len of all words
  for (int64_t i = 0; OB_SUCC(ret) && i < words.count(); ++i) {
    const ObString &word = words.at(i);
    tmp_alloc_len = alloc_len + word.length();
    if (ObExprAdd::is_int_int_out_of_range(alloc_len, word.length(), tmp_alloc_len)) {
      ret = OB_OPERATE_OVERFLOW;
      LOG_WARN("add is overflow.", K(ret), K(word.length()), K(alloc_len));
    } else {
      alloc_len = tmp_alloc_len;
    }
  }

  if (OB_SUCC(ret)) {
    // calc total len with sep_str
    if (is_multi_overflow64(words.count()-1, sep_str.length())) {
      ret = OB_OPERATE_OVERFLOW;
      LOG_WARN("string is too long for concat ws", K(ret), K(sep_str.length()), K(words.count()-1));
    } else {
      int64_t all_sep_str_len = (words.count()-1) * sep_str.length();
      tmp_alloc_len += all_sep_str_len;
      if (ObExprAdd::is_int_int_out_of_range(alloc_len, all_sep_str_len, tmp_alloc_len)) {
      ret = OB_OPERATE_OVERFLOW;
      LOG_WARN("string is too long for concat ws", K(ret), K(alloc_len), K(all_sep_str_len));
      } else {
        alloc_len = tmp_alloc_len;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (alloc_len > OB_MAX_VARCHAR_LENGTH) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("extend len limit", K(ret), K(alloc_len));
    } else if (0 == alloc_len) {
      res_str.reset();
    } else if (alloc_len < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("alloc_len is less than zero", K(ret), K(alloc_len));
    } else if (1 == words.count()) {
      res_str = words.at(0);
    } else if (1 < words.count()) {
      char *res_buf = NULL;
      const ObString &word = words.at(0);
      if (OB_ISNULL(res_buf = reinterpret_cast<char*>(alloc.alloc(alloc_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc mem failed", K(ret), K(alloc_len));
      } else if (word.length() > alloc_len) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected alloc_len", K(ret), K(word), K(alloc_len));
      } else {
        MEMCPY(res_buf, word.ptr(), word.length());
        int64_t buf_pos = word.length();
        for (int64_t i = 1; OB_SUCC(ret) && i < words.count(); ++i) {
          const ObString &word = words.at(i);
          if (OB_FAIL(concat_ws(sep_str, word, alloc_len, &res_buf, buf_pos))) {
            LOG_WARN("concat ws failed", K(ret), K(sep_str), K(word), K(i));
          }
        }
        if (OB_SUCC(ret)) {
          if (buf_pos > OB_MAX_VARCHAR_LENGTH) {
            ret = OB_SIZE_OVERFLOW;
            LOG_WARN("extend len limit", K(ret), K(alloc_len));
          } else {
            res_str.assign_ptr(res_buf, buf_pos);
          }
        }
      }
    }
  }
  return ret;
}  

int ObExprConcatWs::calc_text(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  const ObExpr *sep_expr = expr.args_[0];
  const ObDatum &sep_datum = expr.locate_param_datum(ctx, 0);
  ObString sep_str;
  ObSEArray<ObExpr*, 32> words;

  if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, sep_datum, sep_expr->datum_meta_, sep_expr->obj_meta_.has_lob_header(), sep_str))) {
    LOG_WARN("fail to get real data.", K(ret), K(sep_datum));
  }

  for (int64_t i = 1; OB_SUCC(ret) && i < expr.arg_cnt_; ++i) {
    const ObDatum &dat = expr.locate_param_datum(ctx, i);
    if (!dat.is_null() && OB_FAIL(words.push_back(expr.args_[i]))) {
      LOG_WARN("push back string failed", K(ret), K(i));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(calc_text(expr, ctx, sep_str, words, temp_allocator, res))) {
    LOG_WARN("calc_text fail", K(ret), K(expr), K(words), K(sep_str));
  }
  return ret;
}

int ObExprConcatWs::calc_text(
    const ObExpr &expr,
    ObEvalCtx &ctx,
    const ObString &sep_str,
    const ObIArray<ObExpr *> &words,
    ObIAllocator &temp_allocator,
    ObDatum &res)
{
  int ret = OB_SUCCESS;
  int64_t res_len = 0;
  int64_t word_cnt = 0;
  ObTextStringDatumResult output_result(expr.datum_meta_.type_, &expr, &ctx, &res);
  // calc total len of all words
  for (int64_t i = 0; OB_SUCC(ret) && i < words.count(); i++) {
    const ObExpr *word_expr = words.at(i);;
    ObDatum &v = word_expr->locate_expr_datum(ctx);
    if (! ob_is_text_tc(word_expr->datum_meta_.type_)) {
      res_len += v.len_;
    } else {
      ObLobLocatorV2 locator(v.get_string(), word_expr->obj_meta_.has_lob_header());
      int64_t lob_data_byte_len = 0;
      if (OB_FAIL(locator.get_lob_data_byte_len(lob_data_byte_len))) {
        LOG_WARN("get lob data byte length failed", K(ret), K(locator));
      } else {
        res_len += lob_data_byte_len;
      }
    }
  }

  // calc total len with sep_str
  if (OB_SUCC(ret) && words.count() > 0) {
    res_len += (words.count() - 1) * sep_str.length();
  }

  if (OB_FAIL(ret)) {
  } else if (res_len < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("res_len is less than zero", K(ret), K(res_len));
  } else if (OB_FAIL(output_result.init(res_len))) {
    LOG_WARN("output_result init failed", K(ret), K(res_len));
  } else {
    int64_t append_data_len = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < words.count(); i++) {
      const ObExpr *word_expr = words.at(i);;
      ObDatum &word_datum = word_expr->locate_expr_datum(ctx);
      ObDatumMeta word_meta = word_expr->datum_meta_;
      bool has_lob_header = word_expr->obj_meta_.has_lob_header();

      // append word
      ObTextStringIter input_iter(word_meta.type_, word_meta.cs_type_, word_datum.get_string(), has_lob_header);
      ObTextStringIterState state;
      ObString src_block_data;
      if (OB_FAIL(input_iter.init(0, NULL, &temp_allocator))) {
        LOG_WARN("init input_iter fail", K(ret), K(input_iter));
      }
      while (OB_SUCC(ret)
              && (state = input_iter.get_next_block(src_block_data)) == TEXTSTRING_ITER_NEXT) {
        if (OB_FAIL(output_result.append(src_block_data))) {
          LOG_WARN("output_result append fail", K(ret), K(src_block_data));
        } else {
          append_data_len += src_block_data.length();
        }
      }
      if (OB_FAIL(ret)) {
      } else if (state != TEXTSTRING_ITER_NEXT && state != TEXTSTRING_ITER_END) {
        ret = (input_iter.get_inner_ret() != OB_SUCCESS) ?
              input_iter.get_inner_ret() : OB_INVALID_DATA;
        LOG_WARN("iter state invalid", K(ret), K(state), K(input_iter));
      }

      // append sep word if need
      if (OB_FAIL(ret)) {
      } else if (i == words.count() - 1) {
        // last word is not need sep_str
      } else if (OB_FAIL(output_result.append(sep_str))) {
        LOG_WARN("output_result append sep fail", K(ret), K(sep_str));
      } else {
        append_data_len += sep_str.length();
      }
    }

    if (OB_FAIL(ret)) {
    } else if (append_data_len != res_len) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("append data length is not equal res_len", K(ret), K(append_data_len), K(res_len));
    } else {
      output_result.set_result();
    }
  }
  return ret;
}

int ObExprConcatWs::calc_concat_ws_expr(const ObExpr &expr, ObEvalCtx &ctx,
                                        ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *sep = NULL;
  ObObjType res_type = expr.datum_meta_.type_;
  if (OB_UNLIKELY(1 >= expr.arg_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg cnt", K(ret), K(expr.arg_cnt_));
  } else if (OB_FAIL(expr.eval_param_value(ctx, sep))) {
    LOG_WARN("eval param failed", K(ret));
  } else if (sep->is_null()) {
    res.set_null();
  } else if (ob_is_text_tc(res_type)) {
    if (OB_FAIL(calc_text(expr, ctx, res))) {
      LOG_WARN("calc concat text ws failed", K(ret));
    }
  } else {
    ObSEArray<ObString, 32> words;
    for (int64_t i = 1; OB_SUCC(ret) && i < expr.arg_cnt_; ++i) {
      const ObDatum &dat = expr.locate_param_datum(ctx, i);
      if (!dat.is_null() && OB_FAIL(words.push_back(dat.get_string()))) {
        LOG_WARN("push back string failed", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret)) {
      ObString res_str;
      if (0 == words.count()) {
        res.set_string(res_str);
      } else {
        const ObString &sep_str = expr.locate_param_datum(ctx, 0).get_string();
        ObExprStrResAlloc res_alloc(expr, ctx);
        if (OB_FAIL(calc(sep_str, words, res_alloc, res_str))) {
          LOG_WARN("calc concat ws failed", K(ret));
        } else {
          res.set_string(res_str);
        }
      }
    }
  }
  return ret;
}

int ObExprConcatWs::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_concat_ws_expr;
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprConcatWs, raw_expr) {
  int ret = OB_SUCCESS;
  SET_LOCAL_SYSVAR_CAPACITY(1);
  EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_COLLATION_CONNECTION);
  return ret;
}

} //namespace sql
} //namespace oceanbase
