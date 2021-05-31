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
#include <string.h>
#include "sql/engine/expr/ob_expr_add.h"
#include "lib/oblog/ob_log.h"
#include "sql/parser/ob_item_type.h"
//#include "sql/engine/expr/ob_expr_promotion_util.h"
#include "sql/session/ob_sql_session_info.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase {
namespace sql {

ObExprConcatWs::ObExprConcatWs(ObIAllocator& alloc)
    : ObStringExprOperator(alloc, T_FUN_CNN_WS, N_CONCAT_WS, PARAM_NUM_UNKNOWN)
{
  need_charset_convert_ = false;
}

ObExprConcatWs::~ObExprConcatWs()
{}

int ObExprConcatWs::calc_result_typeN(
    ObExprResType& type, ObExprResType* types, int64_t param_num, ObExprTypeCtx& type_ctx) const
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
    ObLength len = 0;
    for (int64_t i = 1; i < param_num; ++i) {
      len += types[i].get_length();
      types[i].set_calc_type(ObVarcharType);
    }
    len += static_cast<ObLength>(types[0].get_length() * (param_num - 1));
    types[0].set_calc_type(ObVarcharType);
    type.set_length(len);
    type.set_varchar();
    if (OB_FAIL(aggregate_charsets_for_string_result(type, types, param_num, type_ctx.get_coll_type()))) {
      LOG_WARN("aggregate_charsets_for_string_result failed", K(ret));
    } else {
      for (int64_t i = 0; i < param_num; i++) {
        types[i].set_calc_collation_type(type.get_collation_type());
      }
    }
  }
  return ret;
}

int ObExprConcatWs::calc(common::ObObj& result, const ObString& sep_str,
    const common::ObObj* objs,  // separator is varchar
    const int64_t param_num, common::ObExprCtx& expr_ctx)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(expr_ctx.calc_buf_) || OB_ISNULL(objs)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("calc_buf_ is NULL or objs is NULL", K(ret), KP(expr_ctx.calc_buf_), KP(objs));
  } else {
    ObSEArray<ObString, 32> words;
    for (int64_t i = 1; OB_SUCC(ret) && i < param_num; ++i) {
      const ObObj& obj = objs[i];
      if (!obj.is_null() && OB_FAIL(words.push_back(obj.get_string()))) {
        LOG_WARN("push back string failed", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret)) {
      ObString res_str;
      if (0 == words.count()) {
        result.set_varchar(res_str);
      } else {
        if (OB_FAIL(calc(sep_str, words, *expr_ctx.calc_buf_, res_str))) {
          LOG_WARN("calc concat ws failed", K(ret));
        } else {
          result.set_varchar(res_str);
        }
      }
    }
  }
  return ret;
}

int ObExprConcatWs::calc_resultN(
    common::ObObj& result, const common::ObObj* objs, int64_t param_num, common::ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("varchar buffer not init", K(ret));
  } else if (param_num <= 1) {  // testing the number of parameter whether is valid
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Incorrect parameter count in the call to native function 'concat_ws'", K(ret), K(param_num));
  } else if (objs[0].is_null()) {  // testing whether the separator is NULL
    result.set_null();
  } else {
    TYPE_CHECK(objs[0], ObVarcharType);
    ObString sep_str = objs[0].get_string();
    ret = calc(result, sep_str, objs, param_num, expr_ctx);
    if (OB_LIKELY(OB_SUCCESS == ret && !result.is_null())) {
      result.set_collation(result_type_);
    }
  }
  return ret;
}

int ObExprConcatWs::concat_ws(const ObString obj1,  // separator
    const ObString obj2,                            // next string to connect
    const int64_t buf_len, char** buf,
    int64_t& buf_pos)  // writing position of buf now
{
  int ret = OB_SUCCESS;
  int32_t len1 = obj1.length();
  int32_t len2 = obj2.length();
  if (buf_pos + len1 > buf_len) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("extend length limit.", K(ret), K(buf_pos), K(len1));
  } else {
    MEMCPY(*buf + buf_pos, obj1.ptr(), len1);  // separator
    buf_pos += len1;
  }

  if (OB_SUCC(ret)) {
    if (buf_pos + len2 > buf_len) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("extend length limit.", K(ret), K(buf_pos), K(len2));
    } else {
      MEMCPY(*buf + buf_pos, obj2.ptr(), len2);  // next string to connect
      buf_pos += len2;
    }
  }
  return ret;
}

// for engine 3.0
// make sure alloc.alloc() is called only once
int ObExprConcatWs::calc(
    const ObString& sep_str, const ObIArray<ObString>& words, ObIAllocator& alloc, ObString& res_str)
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
    const ObString& word = words.at(i);
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
    if (is_multi_overflow64(words.count() - 1, sep_str.length())) {
      ret = OB_OPERATE_OVERFLOW;
      LOG_WARN("string is too long for concat ws", K(ret), K(sep_str.length()), K(words.count() - 1));
    } else {
      int64_t all_sep_str_len = (words.count() - 1) * sep_str.length();
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
      char* res_buf = NULL;
      const ObString& word = words.at(0);
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
          const ObString& word = words.at(i);
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

int ObExprConcatWs::calc_concat_ws_expr(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res)
{
  int ret = OB_SUCCESS;
  ObDatum* sep = NULL;
  if (OB_UNLIKELY(1 >= expr.arg_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg cnt", K(ret), K(expr.arg_cnt_));
  } else if (OB_FAIL(expr.eval_param_value(ctx, sep))) {
    LOG_WARN("eval param failed", K(ret));
  } else if (sep->is_null()) {
    res.set_null();
  } else {
    ObIAllocator& calc_alloc = ctx.get_reset_tmp_alloc();
    ObSEArray<ObString, 32, ObIAllocator> words(OB_MALLOC_MIDDLE_BLOCK_SIZE, calc_alloc);
    for (int64_t i = 1; OB_SUCC(ret) && i < expr.arg_cnt_; ++i) {
      const ObDatum& dat = expr.locate_param_datum(ctx, i);
      if (!dat.is_null() && OB_FAIL(words.push_back(dat.get_string()))) {
        LOG_WARN("push back string failed", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret)) {
      ObString res_str;
      if (0 == words.count()) {
        res.set_string(res_str);
      } else {
        const ObString& sep_str = expr.locate_param_datum(ctx, 0).get_string();
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

int ObExprConcatWs::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_concat_ws_expr;
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
