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
#include "sql/engine/expr/ob_expr_to_pinyin.h"
#include "objit/common/ob_item_type.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/charset/ob_charset_string_helper.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

ObExprToPinyin::ObExprToPinyin(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_TO_PINYIN, N_TO_PINYIN, ONE_OR_TWO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprToPinyin::~ObExprToPinyin()
{
}

int ObExprToPinyin::calc_convert_mode(const ObString &convert_option, 
                                      ModeOption &convert_mode)
{
  int ret = OB_SUCCESS;
  int i = 0;
  for (; i < OptionCnt; ++i) {
    if (0 == convert_option.case_compare(OptionStr[i])) {
      convert_mode = static_cast<ModeOption>(i);
      break;
    }
  }
  if (i == OptionCnt) {
    ret = OB_INVALID_ARGUMENT;
  }
  return ret;
}

int ObExprToPinyin::calc_result_length(const ObExprResType &type,
                                       int64_t &res_len)
{
  int ret = OB_SUCCESS;
  res_len = type.get_length();
  return ret;
}

int ObExprToPinyin::calc_result_type1(ObExprResType &type,
                                      ObExprResType &type1,
                                      common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  type1.set_calc_type(ObVarcharType);
  type1.set_calc_collation_type(CS_TYPE_UTF8MB4_ZH_0900_AS_CS);
  type.set_varchar();
  type.set_collation_level(common::CS_LEVEL_COERCIBLE);
  const sql::ObSQLSessionInfo *session = type_ctx.get_session();
  int64_t res_len = 0;
  if (OB_UNLIKELY(OB_ISNULL(session))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null",K(ret));
  } else if (OB_FAIL(calc_result_length(type1, res_len))) {
    LOG_WARN("calc result length failed", K(ret));
  } else {
    type.set_collation_type(lib::is_oracle_mode() ?
      session->get_nls_collation() :
      session->get_local_collation_connection());
    type.set_length(res_len);
    type.set_length_semantics(type1.get_length_semantics());
  }
  return ret;
}

int ObExprToPinyin::calc_result_type2(ObExprResType &type,
                                      ObExprResType &type1,
                                      ObExprResType &type2,
                                      common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  type1.set_calc_type(ObVarcharType);
  type1.set_calc_collation_type(CS_TYPE_UTF8MB4_ZH_0900_AS_CS);
  type2.set_calc_type(ObVarcharType);
  type2.set_calc_collation_type(CS_TYPE_UTF8MB4_ZH_0900_AS_CS);
  type.set_varchar();
  type.set_collation_level(common::CS_LEVEL_COERCIBLE);
  const sql::ObSQLSessionInfo *session = type_ctx.get_session();
  int64_t res_len = 0;
  if (OB_UNLIKELY(OB_ISNULL(session))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null",K(ret));
  } else if (OB_FAIL(calc_result_length(type1, res_len))) {
    LOG_WARN("calc result length failed", K(ret));
  } else {
    type.set_collation_type(lib::is_oracle_mode() ?
      session->get_nls_collation() :
      session->get_local_collation_connection());
    type.set_length(res_len);
    type.set_length_semantics(type1.get_length_semantics());
  }
  return ret;
}

int ObExprToPinyin::calc_result_typeN(ObExprResType &type,
                                      ObExprResType *types_array,
                                      int64_t param_num,
                                      common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  if (1 == param_num) {
    ret = calc_result_type1(type, types_array[0], type_ctx);
  } else {
    ret = calc_result_type2(type, types_array[0], types_array[1], type_ctx);
  }
  return ret;
}

uint64_t convert_to_sortkey(ObIAllocator &alloc, ObString input) {
  const ObCharsetInfo *cs = ObCharset::get_charset(CS_TYPE_UTF8MB4_ZH_0900_AS_CS);
  char *buf = NULL;
  size_t buf_len = cs->coll->strnxfrmlen(cs, cs->mbmaxlen*input.length());
  bool is_valid_unicode_tmp = 1;
  size_t result_len = 0;
  
  uint64_t sortkey = -1;
  if (OB_ISNULL(buf = static_cast<char*>(alloc.alloc(buf_len)))) {
    int ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc buf", K(ret), K(buf_len), K(input));
  } else {
    result_len = cs->coll->strnxfrm(cs,
                                    reinterpret_cast<uchar *>(buf),
                                    buf_len,
                                    buf_len,
                                    reinterpret_cast<const uchar *>(input.ptr()),
                                    input.length(),
                                    0,
                                    &is_valid_unicode_tmp);
    uint64_t res = *reinterpret_cast<uint64_t *>(buf);
    alloc.free(buf);
    sortkey = (res % 256) * 256 + (res / 256 % 256);
  }
  return sortkey;
}

bool compare_end(const PinyinPair& a, const PinyinPair& b) {
  return a.end < b.end;
}

ObString convert_word_to_pinyin(ObIAllocator &alloc, ObString input, bool firstWord = false, ModeOption mode = ModeOption::Full) {
  int ret = OB_SUCCESS;
  uint64_t input_sortkey = convert_to_sortkey(alloc, input);
  ObString result;
  // 根据sortkey转换为拼音
  // 二分查找
  PinyinPair target = {0, input_sortkey, ""};
  PinyinPair *it = std::lower_bound(PINYIN_TABLE, PINYIN_TABLE + PINYIN_COUNT, target, compare_end);
  if(it != PINYIN_TABLE + PINYIN_COUNT &&
      input_sortkey >= it->begin && input_sortkey <= it->end) {
    result = it->pinyin;
    if (mode >= ModeOption::Initial && mode <= ModeOption::All_Cap_Initial) {
      ObString encoding;
      int32_t wchar;
      ObStringScanner scanner(result, CS_TYPE_UTF8MB4_ZH_0900_AS_CS);
      if (OB_ITER_END == (ret = scanner.next_character(encoding, wchar))) {
        ret = OB_SUCCESS;
      }
      if (OB_SUCC(ret) && OB_FALSE_IT(result.assign(encoding.ptr(), encoding.length()))) {
        // do nothing
      } else if (mode == ModeOption::All_Cap_Initial || (firstWord && mode == ModeOption::Cap_Initial)) {
        if (OB_FAIL(ObCharset::toupper(CS_TYPE_UTF8MB4_ZH_0900_AS_CS, result, result, alloc))) {
          LOG_WARN("toupper failed", K(ret));
        }
      }
    } else if (mode == ModeOption::All_Cap) {
      if (OB_FAIL(ObCharset::toupper(CS_TYPE_UTF8MB4_ZH_0900_AS_CS, result, result, alloc))) {
        LOG_WARN("toupper failed", K(ret));
      }
    } else if (mode == ModeOption::Cap || (mode == ModeOption::First_Cap && firstWord)) {
      ObString firstStr;
      int32_t wchar;
      ObStringScanner scanner(result, CS_TYPE_UTF8MB4_ZH_0900_AS_CS);
      if (OB_ITER_END == (ret = scanner.next_character(firstStr, wchar))) {
        ret = OB_SUCCESS;
      }
      ObString remainStr = scanner.get_remain_str();
      char *buf = NULL;
      if (OB_SUCC(ret) && OB_FAIL(ObCharset::toupper(CS_TYPE_UTF8MB4_ZH_0900_AS_CS, firstStr, firstStr, alloc))) {
        LOG_WARN("toupper failed", K(ret));
      } else if (OB_ISNULL(buf = static_cast<char *>(alloc.alloc(firstStr.length() + remainStr.length())))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc buf", K(ret));
      } else if (OB_FALSE_IT(MEMCPY(buf, firstStr.ptr(), firstStr.length()))) {

      } else if (OB_FALSE_IT(MEMCPY(buf+firstStr.length(), remainStr.ptr(), remainStr.length()))) {

      } else {
        result.assign(buf, firstStr.length() + remainStr.length());
      }
    }
  } else {
    result = input;
  }
  return result;
}

struct Functor {
  Functor(char *buf, int64_t &off, ObIAllocator &alloc, ModeOption &mode) 
          : buf(buf), off(off), calc_alloc(alloc), firstWord(true), convert_mode(mode) {}
  char *buf;
  int64_t &off;
  ObIAllocator &calc_alloc;
  bool firstWord;
  ModeOption &convert_mode;
  int operator() (const ObString &str, ob_wc_t wchar) {
    int ret = OB_SUCCESS;
    ObString pinyin = convert_word_to_pinyin(calc_alloc, str, firstWord, convert_mode);
    if(!pinyin.empty()) {
      MEMCPY(buf + off, pinyin.ptr(), pinyin.length());
      off += pinyin.length();
    } else {
      ret = OB_ERR_UNEXPECTED;
    }
    if (firstWord) {
      firstWord = false;
    }
    return ret;
  }
};

int ObExprToPinyin::eval_to_pinyin(const ObExpr &expr, ObEvalCtx &ctx,
    ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;

  ObDatum *input = NULL;
  ObString input_str;
  ObEvalCtx::TempAllocGuard alloc_guard(ctx);
  ObIAllocator &calc_alloc = alloc_guard.get_allocator();
  ObExprStrResAlloc expr_res_alloc(expr, ctx);
  const sql::ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();

  // text to convert
  if (OB_FAIL(expr.args_[0]->eval(ctx, input))) {
    LOG_WARN("fail to eval", K(ret), KPC(expr.args_[0]));
  } else if (input->is_null()) {
    expr_datum.set_null();
    return ret;
  } else {
    input_str = input->get_string();
  }

  // optional param
  ModeOption convert_mode = ModeOption::Full;
  if (expr.arg_cnt_ < 2) {
    // do nothing
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, input))) {
    LOG_WARN("fail to eval", K(ret), KPC(expr.args_[1]));
  } else if (input->is_null()) {
    // do nothing
  } else if(OB_FAIL(calc_convert_mode(input->get_string(), convert_mode))){
    LOG_WARN("calc convert mode failed", K(ret), K(input->get_string()));
  }

  const ObCharsetInfo *cs = ObCharset::get_charset(CS_TYPE_UTF8MB4_ZH_0900_AS_CS);
  size_t buf_len = cs->mbmaxlen*input_str.length();
  char *buf = NULL;
  if (OB_FAIL(ret) || 0 == buf_len) {
    expr_datum.set_null();
  } else if (OB_ISNULL(buf = static_cast<char*>(calc_alloc.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc buf", K(ret), K(buf_len), K(input_str));
  } else {
    int64_t off = 0;
    Functor temp_handler(buf, off, calc_alloc, convert_mode);
    ObCharsetType charset_type = ObCharset::charset_type_by_coll(CS_TYPE_UTF8MB4_ZH_0900_AS_CS);
    ObFastStringScanner::foreach_char(input_str, charset_type, temp_handler);
    ObString converted_result;
    OZ(ObExprUtil::convert_string_collation(ObString(off, buf),
                                        CS_TYPE_UTF8MB4_ZH_0900_AS_CS,
                                        converted_result,
                                        lib::is_oracle_mode() ? session->get_nls_collation() : session->get_local_collation_connection(),
                                        calc_alloc));
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(ObExprUtil::deep_copy_str(converted_result, converted_result, expr_res_alloc))) {
      LOG_WARN("deep copy str failed", K(ret), K(converted_result));
    } else {
      expr_datum.set_string(converted_result);
    }
  }
  return ret;
}

int ObExprToPinyin::eval_to_pinyin_batch(
  const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const int64_t batch_size)
{
  LOG_DEBUG("eval to_pinyin in batch mode", K(batch_size));
  int ret = OB_SUCCESS;
  ObDatum *results = expr.locate_batch_datums(ctx);
  const sql::ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  const bool has_option_param = (expr.arg_cnt_ == 2);
  if (OB_ISNULL(results)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr results frame is not init", K(ret));
  } else {
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    if (OB_FAIL(expr.args_[0]->eval_batch(ctx, skip, batch_size))) {
      LOG_WARN("failed to eval batch result input", K(ret));
    } else {
      ObDatum *datum_array = expr.args_[0]->locate_batch_datums(ctx);
      ObEvalCtx::TempAllocGuard alloc_guard(ctx);
      ObIAllocator &calc_alloc = alloc_guard.get_allocator();
      ObExprStrResAlloc expr_res_alloc(expr, ctx);
      const ObCharsetInfo *cs = ObCharset::get_charset(CS_TYPE_UTF8MB4_ZH_0900_AS_CS);
      ModeOption convert_mode = ModeOption::Full;
      if (has_option_param) {
        ObDatum *mode_datum = NULL;
        if (OB_FAIL(expr.args_[1]->eval(ctx, mode_datum))) {
          LOG_WARN("eval mode_datum failed", K(ret));
        } else if (mode_datum->is_null()) {
          // do nothing
        } else if (OB_FAIL(calc_convert_mode(mode_datum->get_string(), convert_mode))) {
          LOG_WARN("calc convert mode failed", K(ret), K(mode_datum->get_string()));
        }
      }
      // 使用BatchInfoScopeGuard来设置当前处理的datum索引
      ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
      for(int64_t j = 0; OB_SUCC(ret) && j < batch_size; ++j) {
        if (skip.at(j) || eval_flags.at(j)) {
          continue;
        } else if (datum_array[j].is_null()) {
          results[j].set_null();
          eval_flags.set(j);
        } else {
          ObString input_str = datum_array[j].get_string();
          int64_t off = 0;
          char *buf = NULL;
          size_t buf_len = cs->mbmaxlen*input_str.length();
          if (0 == buf_len) {
            results[j].set_null();
            eval_flags.set(j);
          } else if (OB_ISNULL(buf = static_cast<char *>(calc_alloc.alloc(buf_len)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc buf", K(ret), K(buf_len), K(input_str));
          } else {
            int64_t off = 0;
            Functor temp_handler(buf, off, calc_alloc, convert_mode);
            ObCharsetType charset_type = ObCharset::charset_type_by_coll(CS_TYPE_UTF8MB4_ZH_0900_AS_CS);
            ObFastStringScanner::foreach_char(input_str, charset_type, temp_handler);
            ObString converted_result;
            batch_info_guard.set_batch_idx(j);
            OZ(ObExprUtil::convert_string_collation(ObString(off, buf),
                                                CS_TYPE_UTF8MB4_ZH_0900_AS_CS,
                                                converted_result,
                                                lib::is_oracle_mode() ? session->get_nls_collation() : session->get_local_collation_connection(),
                                                calc_alloc));
            if (OB_FAIL(ret)) {
              // do nothing
            } else if (OB_FAIL(ObExprUtil::deep_copy_str(converted_result, converted_result, expr_res_alloc))) {
              LOG_WARN("deep copy str failed", K(ret), K(converted_result));
            } else {
              results[j].set_string(converted_result);
              eval_flags.set(j);
            }
          }
        }
      }
    }
  }
  return ret;
}

template <typename ArgVec, typename ResVec>
int ObExprToPinyin::to_pinyin_vector(VECTOR_EVAL_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  ObIAllocator &calc_alloc = tmp_alloc_g.get_allocator();
  ObExprStrResAlloc expr_res_alloc(expr, ctx);
  const bool has_option_param = (expr.arg_cnt_ == 2);
  const ArgVec *arg0_vec = static_cast<const ArgVec *>(expr.args_[0]->get_vector(ctx));
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  ModeOption convert_mode = ModeOption::Full;
  const ObCharsetInfo *cs = ObCharset::get_charset(CS_TYPE_UTF8MB4_ZH_0900_AS_CS);
  const sql::ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  if (has_option_param) {
    ConstUniformFormat *mode_vec = NULL;
    if (OB_FAIL(expr.args_[1]->eval_vector(ctx, skip, bound))) {
      LOG_WARN("failed to eval vector result args1", K(ret));
    } else {
      mode_vec = static_cast<ConstUniformFormat *>(expr.args_[1]->get_vector(ctx));
      if (mode_vec->is_null(0)) {
        // do nothing
      } else if (OB_FAIL(calc_convert_mode(mode_vec->get_string(0), convert_mode))) {
        LOG_WARN("calc convert mode failed", K(ret), K(mode_vec->get_string(0)));
      }
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else {
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      if (skip.at(idx) || eval_flags.at(idx)) {
        continue;
      } else if (arg0_vec->is_null(idx)) {
        res_vec->set_null(idx);
        eval_flags.set(idx);
      } else {
        ObString input_str = arg0_vec->get_string(idx);
        int64_t off = 0;
        char *buf = NULL;
        size_t buf_len = cs->mbmaxlen * input_str.length();
        if (0 == buf_len) {
          res_vec->set_null(idx);
          eval_flags.set(idx);
        } else if (OB_ISNULL(buf = static_cast<char *>(calc_alloc.alloc(buf_len)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc buf", K(ret), K(buf_len), K(input_str));
        } else {
          int64_t off = 0;
          Functor temp_handler(buf, off, calc_alloc, convert_mode);
          ObCharsetType charset_type = ObCharset::charset_type_by_coll(CS_TYPE_UTF8MB4_ZH_0900_AS_CS);
          ObFastStringScanner::foreach_char(input_str, charset_type, temp_handler);
          ObString converted_result;
          batch_info_guard.set_batch_idx(idx);
          OZ(ObExprUtil::convert_string_collation(ObString(off, buf),
                                              CS_TYPE_UTF8MB4_ZH_0900_AS_CS,
                                              converted_result,
                                              lib::is_oracle_mode() ? session->get_nls_collation() : session->get_local_collation_connection(),
                                              calc_alloc));
          if (OB_FAIL(ret)) {
            // do nothing
          } else if (OB_FAIL(ObExprUtil::deep_copy_str(converted_result, converted_result, expr_res_alloc))) {
            LOG_WARN("deep copy str failed", K(ret), K(converted_result));
          } else {
            res_vec->set_string(idx, converted_result);
            eval_flags.set(idx);
          }
        }
      }
    }
  }
  return ret;
}

int ObExprToPinyin::eval_to_pinyin_vector(VECTOR_EVAL_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("failed to eval vector result args0", K(ret));
  } else {
    VectorFormat arg_format = expr.args_[0]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    if (VEC_DISCRETE == arg_format && VEC_DISCRETE == res_format) {
      ret = to_pinyin_vector<StrDiscVec, StrDiscVec>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_UNIFORM == arg_format && VEC_DISCRETE == res_format) {
      ret = to_pinyin_vector<StrUniVec, StrDiscVec>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_CONTINUOUS == arg_format && VEC_DISCRETE == res_format) {
      ret = to_pinyin_vector<StrContVec, StrDiscVec>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_DISCRETE == arg_format && VEC_UNIFORM == res_format) {
      ret = to_pinyin_vector<StrDiscVec, StrUniVec>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_UNIFORM == arg_format && VEC_UNIFORM == res_format) {
      ret = to_pinyin_vector<StrUniVec, StrUniVec>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_CONTINUOUS == arg_format && VEC_UNIFORM == res_format) {
      ret = to_pinyin_vector<StrContVec, StrUniVec>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else {
      ret = to_pinyin_vector<ObVectorBase, ObVectorBase>(VECTOR_EVAL_FUNC_ARG_LIST);
    }
  }
  return ret;
}

int ObExprToPinyin::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr,
    ObExpr &rt_expr) const
{
  UNUSED(op_cg_ctx);
  rt_expr.eval_func_ = ObExprToPinyin::eval_to_pinyin;
  if (rt_expr.args_[0]->is_batch_result()) {
    if ((rt_expr.arg_cnt_ == 1) || (rt_expr.arg_cnt_ == 2 && !rt_expr.args_[1]->is_batch_result())) {
      if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_3_2_0) {
        rt_expr.eval_vector_func_ = ObExprToPinyin::eval_to_pinyin_vector;
      } else {
        rt_expr.eval_batch_func_ = ObExprToPinyin::eval_to_pinyin_batch;
      }
    }
  }
  return OB_SUCCESS;
}

} //namespace sql
} //namespace oceanbase
