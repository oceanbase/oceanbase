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

#include "sql/engine/expr/ob_expr_like.h"
//#include "sql/engine/expr/ob_expr_promotion_util.h"
#include "sql/engine/ob_exec_context.h"
#include "share/object/ob_obj_cast.h"
#include "lib/oblog/ob_log.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase {
using namespace common;
namespace sql {

ObExprLike::ObExprLike(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_OP_LIKE, N_LIKE, 3, NOT_ROW_DIMENSION),
      is_pattern_literal_(false),
      is_text_literal_(true),
      is_escape_literal_(false),
      like_id_(-1)
{
  need_charset_convert_ = false;
}

ObExprLike::~ObExprLike()
{}

int ObExprLike::assign(const ObExprOperator& other)
{
  int ret = OB_SUCCESS;
  const ObExprLike* tmp_other = dynamic_cast<const ObExprLike*>(&other);
  if (OB_UNLIKELY(NULL == tmp_other)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument. wrong type for other", K(ret), K(other));
  } else if (OB_LIKELY(this != tmp_other)) {
    if (OB_FAIL(ObFuncExprOperator::assign(other))) {
      LOG_WARN("copy in Base class ObFuncExprOperator failed", K(ret));
    } else {
      this->is_pattern_literal_ = tmp_other->is_pattern_literal_;
      this->is_text_literal_ = tmp_other->is_text_literal_;
      this->is_escape_literal_ = tmp_other->is_escape_literal_;
      this->like_id_ = tmp_other->like_id_;
    }
  }
  return ret;
}

// Oracle mode, the following character of escape character only support _ and % and self, or report error
// check valid must be process first, even parttern or text is null, it will report error
// eg: select 1 from dual where null like 'a' escape '';
// like: select 1 from t1 where '_%a' like 'a_a%aa' escape 'a';  --ok
//       select 1 from t1 where '_%' like 'aba%' escape 'a';  --error, ab is invalid
// ORA-01424: missing or illegal character following the escape character
template <typename T>
int ObExprLike::check_pattern_valid(const T& pattern, const T& escape, const ObCollationType escape_coll,
    ObCollationType coll_type, ObExecContext* exec_ctx, const uint64_t like_id, const bool check_optimization,
    bool is_static_engine)
{
  int ret = OB_SUCCESS;
  int32_t escape_wc = 0;
  const ObCharsetInfo* cs = NULL;
  ObString escape_val = escape.get_string();
  ObString pattern_val = pattern.get_string();
  ObExprLikeContext* like_ctx = NULL;
  if (is_static_engine) {
    // check_optimizaiton is true, only if pattern and escape are const.
    if (check_optimization &&
        NULL == (like_ctx = static_cast<ObExprLikeContext*>(exec_ctx->get_expr_op_ctx(like_id)))) {
      if (OB_FAIL(exec_ctx->create_expr_op_ctx(like_id, like_ctx))) {
        LOG_WARN("failed to create operator ctx", K(ret), K(like_id));
      }
    }
  } else if (NULL != exec_ctx) {
    // When text, pattern and escape are all const, report error when create op ctx
    // If it's error, then don't optimize check
    int tmp_ret = OB_SUCCESS;
    if (NULL == (like_ctx = static_cast<ObExprLikeContext*>(exec_ctx->get_expr_op_ctx(like_id)))) {
      if (OB_SUCCESS != (tmp_ret = exec_ctx->create_expr_op_ctx(like_id, like_ctx))) {
        LOG_DEBUG("failed to create operator ctx", K(ret), K(like_id));
      }
    }
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("failed to check pattern", K(ret), K(like_id));
  } else if (!lib::is_oracle_mode()) {
    // don't check in mysql mode
  } else if (NULL != like_ctx &&
             checked_already(
                 *like_ctx, pattern.is_null(), pattern_val, escape.is_null(), escape_val, is_static_engine)) {
    // skip check if pattern and escape are same as checked last time.
    // select * from t1 where exist (select * from t2 where 'abc' like t1.c1 escape t1.c2);
    // pattern t1.c1 and escape t1.c2 are const in subquery, but they may change.
    like_ctx->same_as_last = true;
  } else if (escape.is_null() || 1 != escape_val.length()) {
    ret = OB_ERR_INVALID_ESCAPE_CHAR_LENGTH;
    LOG_WARN("escape character must be character string of length 1", K(escape_val), K(ret));
  } else if (OB_FAIL(calc_escape_wc(escape_coll, escape_val, escape_wc))) {
    LOG_WARN("fail to calc escape wc", K(escape_val), K(escape_coll));
  } else if (OB_UNLIKELY(OB_ISNULL(cs = ObCharset::get_charset(coll_type)) || OB_ISNULL(cs->cset))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected error. invalid argument(s)", K(coll_type));
  } else if (!pattern.is_null()) {
    const char* buf_start = pattern_val.ptr();
    const char* buf_end = pattern_val.ptr() + pattern_val.length();
    int error = 0;
    int32_t char_len = 0;
    bool is_valid = true;
    bool is_char_escape = false;
    bool pre_char_is_escape = false;
    while (OB_SUCC(ret) && buf_start < buf_end && is_valid) {
      char_len = static_cast<int32_t>(cs->cset->well_formed_len(buf_start, buf_end - buf_start, 1, &error));
      if (OB_UNLIKELY(0 != error)) {
        ret = OB_ERR_INVALID_CHARACTER_STRING;
        LOG_WARN("well_formed_len failed. invalid char", K(buf_start), K(pattern_val), K(char_len));
      } else if (OB_FAIL(is_escape(escape_coll, buf_start, char_len, escape_wc, is_char_escape))) {
        LOG_WARN("fail to judge escape", K(escape_val), K(escape_coll));
      } else if (is_char_escape) {
        // connect the two escape char, like: select 1 from t1 where 'a' like 'aa' escape 'a'; -- it's ok
        if (pre_char_is_escape) {
          pre_char_is_escape = false;
          is_char_escape = false;
        } else {
          pre_char_is_escape = true;
          is_char_escape = false;
        }
      } else if (pre_char_is_escape) {
        // If pre char is escape char, then the following char must be '_' or '%'
        // Eg: select 1 from t1 where 'a' like 'a_a%' escape 'a'; -- it's ok
        if (1 != char_len) {
          ret = OB_ERR_INVALID_CHAR_FOLLOWING_ESCAPE_CHAR;
          LOG_WARN(
              "missing or illegal character following the escape character", K(escape_val), K(pattern_val), K(ret));
        } else if ('%' == *buf_start || '_' == *buf_start) {
          // it's ok
        } else {
          ret = OB_ERR_INVALID_CHAR_FOLLOWING_ESCAPE_CHAR;
          LOG_WARN(
              "missing or illegal character following the escape character", K(escape_val), K(pattern_val), K(ret));
        }
        pre_char_is_escape = false;
      }
      buf_start += char_len;
    }  // end while
    if (pre_char_is_escape) {
      // Last character is escape character
      // // Eg: select 1 from t1 where 'a' like 'a_a' escape 'a'; -- it's error
      ret = OB_ERR_INVALID_CHAR_FOLLOWING_ESCAPE_CHAR;
      LOG_WARN("missing or illegal character following the escape character", K(escape_val), K(pattern_val), K(ret));
    }
    if (NULL != like_ctx) {
      record_last_check(*like_ctx, pattern_val, escape_val, &exec_ctx->get_allocator(), is_static_engine);
    }
  }
  return ret;
}

int ObExprLike::calc(ObObj& result, ObCollationType coll_type, const ObObj& text, const ObObj& pattern,
    const ObObj& escape, ObExprCtx& expr_ctx, const bool is_going_optimization, const uint64_t like_id,
    const bool check_optimization)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("expr_ctx is not inited successfully", K(ret), K(expr_ctx.calc_buf_));
  } else if (OB_FAIL(check_pattern_valid(pattern,
                 escape,
                 escape.get_collation_type(),
                 coll_type,
                 expr_ctx.exec_ctx_,
                 like_id,
                 check_optimization,
                 false))) {
    LOG_WARN("fail to check pattern string", K(pattern), K(escape), K(coll_type));
  } else if (text.is_null() || pattern.is_null()) {
    result.set_null();
  } else {
    if (!text.is_clob()) {
      TYPE_CHECK(text, ObVarcharType);
    }
    if (!pattern.is_clob()) {
      TYPE_CHECK(pattern, ObVarcharType);
    }
    ObString text_val = text.get_string();
    ObString pattern_val = pattern.get_string();
    ObString escape_val;
    if (escape.is_null()) {
      escape_val.assign_ptr("\\", 1);
    } else {
      TYPE_CHECK(escape, ObVarcharType);
      escape_val = escape.get_string();
      if (escape_val.empty()) {
        escape_val.assign_ptr("\\", 1);
      }
    }
    if (OB_UNLIKELY(NULL == expr_ctx.exec_ctx_ && false == expr_ctx.is_pre_calculation_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error. exec ctx should not be null in pre calculation",
          K(expr_ctx.exec_ctx_),
          K(expr_ctx.is_pre_calculation_));
    }
    if (OB_SUCC(ret)) {
      if (is_going_optimization && like_id != OB_INVALID_ID && expr_ctx.exec_ctx_ != NULL && (!text_val.empty()) &&
          (!pattern_val.empty())) {
        ObExprLikeContext* like_ctx = NULL;
        if (NULL == (like_ctx = static_cast<ObExprLikeContext*>(expr_ctx.exec_ctx_->get_expr_op_ctx(like_id)))) {
          if (OB_FAIL(expr_ctx.exec_ctx_->create_expr_op_ctx(like_id, like_ctx))) {
            LOG_WARN("failed to create operator ctx", K(ret), K(like_id));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_UNLIKELY(!(like_ctx->is_analyzed()))) {
            ObIAllocator* exec_allocator = GET_EXEC_ALLOCATOR(expr_ctx);
            if (OB_FAIL(set_instr_info(exec_allocator,
                    coll_type,
                    text_val,
                    pattern_val,
                    escape_val,
                    escape.get_collation_type(),
                    *like_ctx))) {
              LOG_WARN("failed to set instr info", K(ret), K(pattern_val), K(text_val));
            } else if (like_ctx->is_instr_mode()) {  // instr mode
              ret = calc_with_instr_mode(result, coll_type, text_val, *like_ctx);
            } else {  // not instr mode
              ret = calc_with_non_instr_mode(
                  result, coll_type, escape.get_collation_type(), text_val, pattern_val, escape_val);
            }
            // no matter what happened
            // we will not analyze pattern any more.
            like_ctx->set_analyzed();
          } else if (like_ctx->is_instr_mode()) {  // instr mode
            ret = calc_with_instr_mode(result, coll_type, text_val, *like_ctx);
          } else {  // not instr mode
            ret = calc_with_non_instr_mode(
                result, coll_type, escape.get_collation_type(), text_val, pattern_val, escape_val);
          }
        }
      } else {  // normal path. no optimization here.
        ret =
            calc_with_non_instr_mode(result, coll_type, escape.get_collation_type(), text_val, pattern_val, escape_val);
      }
    }
  }
  return ret;
}

int ObExprLike::calc_result3(
    ObObj& result, const ObObj& text, const ObObj& pattern, const ObObj& escape, ObExprCtx& expr_ctx) const
{
  bool is_going_optimization = is_pattern_literal() && (!(is_text_literal())) && is_escape_literal();
  bool check_optimization = is_pattern_literal() && is_escape_literal();
  return calc(result,
      result_type_.get_calc_collation_type(),
      text,
      pattern,
      escape,
      expr_ctx,
      is_going_optimization,
      get_id(),
      check_optimization);
}

int ObExprLike::calc_result_type3(ObExprResType& type, ObExprResType& type1, ObExprResType& type2, ObExprResType& type3,
    ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (!type1.is_null() && !type2.is_null() && !type3.is_null() &&
      (!is_type_valid(type1.get_type()) || !is_type_valid(type2.get_type()) || !is_type_valid(type3.get_type()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the param is not castable", K(type1), K(type2), K(type3), K(ret));
  } else {
    type.set_int();
    ObObjMeta types[2] = {type1, type2};
    type.set_calc_type(ObVarcharType);
    type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
    type.set_precision(DEFAULT_PRECISION_FOR_BOOL);
    if (!type1.is_clob()) {
      type1.set_calc_type(ObVarcharType);
    }
    if (!type2.is_clob()) {
      type2.set_calc_type(ObVarcharType);
    }
    type3.set_calc_type(ObVarcharType);
    type3.set_calc_collation_type(type3.get_collation_type());
    if (share::is_oracle_mode()) {
      if (OB_ISNULL(type_ctx.get_session())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("session is null", K(ret));
      } else {
        type.set_calc_collation_type(type_ctx.get_session()->get_nls_collation());
      }
    } else {
      ret = aggregate_charsets_for_comparison(type.get_calc_meta(), types, 2, type_ctx.get_coll_type());
    }
    type1.set_calc_collation_type(type.get_calc_collation_type());
    type2.set_calc_collation_type(type.get_calc_collation_type());
    ObExprOperator::calc_result_flag2(type, type1, type2);  // ESCAPE is ignored
  }
  return ret;
}

int ObExprLike::set_instr_info(ObIAllocator* exec_allocator, const ObCollationType cs_type, const ObString& text,
    const ObString& pattern, const ObString& escape, const ObCollationType escape_coll, ObExprLikeContext& like_ctx)
{
  // If you feel tough to understand this func,
  // please feel free to refer here for more details :
  // https://gw.alicdn.com/tfscom/TB1XAvqMpXXXXaVXpXXXXXXXXXX.jpg
  int ret = OB_SUCCESS;
  like_ctx.instr_mode_ = ObExprLikeContext::INVALID_INSTR_MODE;
  const char* instr_start_tmp = NULL;
  int32_t instr_len_tmp = 0;
  const ObCharsetInfo* cs = NULL;
  char* pattern_buf = nullptr;
  ObIAllocator* exec_cal_buf = exec_allocator;
  if (cs_type != CS_TYPE_UTF8MB4_BIN) {
    // we optimize the case in which cs_type == CS_TYPE_UTF8MB4_BIN only
    // just let it go
  } else if (OB_UNLIKELY(OB_ISNULL(cs = ObCharset::get_charset(cs_type)) || OB_ISNULL(cs->cset))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected error. invalid argument(s)", K(cs_type), K(pattern), K(escape));
  } else if (OB_UNLIKELY(pattern.empty()) || OB_UNLIKELY(text.empty())) {
    // do nothing.just let it go.
  } else if (OB_ISNULL(exec_cal_buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Failed to get exec cal buf", K(ret));
  } else {
    int32_t escape_wc = 0;
    if (pattern.length() <= like_ctx.instr_buf_length_) {
      pattern_buf = like_ctx.instr_buf_;
    } else if (OB_ISNULL(pattern_buf = (char*)(exec_cal_buf->alloc(sizeof(char) * pattern.length() * 2)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("No more memories", K(ret));
    } else {
      like_ctx.instr_buf_ = pattern_buf;
      like_ctx.instr_buf_length_ = pattern.length() * 2;
    }
    if (OB_SUCC(ret)) {
      MEMCPY(pattern_buf, pattern.ptr(), pattern.length());
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(calc_escape_wc(escape_coll, escape, escape_wc))) {
    } else {
      // iterate pattern now
      const char* buf_start = pattern_buf;
      const char* buf_end = pattern_buf + pattern.length();
      int error = 0;
      int32_t char_len = 0;
      STATE current_state = INIT;
      bool is_char_escape = false;
      while (OB_SUCC(ret) && buf_start < buf_end && current_state != END) {
        char_len = static_cast<int32_t>(cs->cset->well_formed_len(buf_start, buf_end - buf_start, 1, &error));
        is_char_escape = false;
        if (OB_UNLIKELY(0 != error)) {
          ret = OB_ERR_INVALID_CHARACTER_STRING;
          LOG_WARN("well_formed_len failed. invalid char", K(cs_type), K(buf_start), K(pattern), K(char_len));
        } else if (OB_FAIL(is_escape(escape_coll, buf_start, char_len, escape_wc, is_char_escape))) {
          break;
        } else if (is_char_escape || (1 == char_len && '_' == *buf_start)) {
          // when there are "_" or escape in pattern
          // the case can not be optimized.
          current_state = END;
        } else if ((1 == char_len && '%' == *buf_start)) {  // percent sign
          state_trans_with_percent_sign(current_state);
          buf_start += char_len;
        } else {  // non-percent char
          if (NULL == instr_start_tmp) {
            instr_start_tmp = buf_start;
          }
          state_trans_with_nonpercent_char(current_state);
          buf_start += char_len;
          instr_len_tmp += char_len;
        }
      }  // end while
      // now, we deduce the instr mode according to the current_state
      if (OB_SUCC(ret) && current_state != END) {
        like_ctx.instr_start_ = instr_start_tmp;
        like_ctx.instr_length_ = instr_len_tmp;
        switch (current_state) {
          case PERCENT_NONPERCENT_PENCENT:
            // fall through
          case PERCENT: {
            like_ctx.instr_mode_ = ObExprLikeContext::START_END_WITH_PERCENT_SIGN;
            break;
          }
          case PERCENT_NONPERCENT: {
            like_ctx.instr_mode_ = ObExprLikeContext::START_WITH_PERCENT_SIGN;
            break;
          }
          case NONPERCENT_PERCENT: {
            like_ctx.instr_mode_ = ObExprLikeContext::END_WITH_PERCENT_SIGN;
            break;
          }
          default: {
            like_ctx.instr_mode_ = ObExprLikeContext::INVALID_INSTR_MODE;
            break;
          }
        }
      }  // end deduce instrmode
    }    // end else
  }
  return ret;
}

template <typename T>
int ObExprLike::calc_with_instr_mode(
    T& result, const ObCollationType cs_type, const ObString& text, const ObExprLikeContext& like_ctx)
{
  int ret = OB_SUCCESS;
  // position of first non-percent char
  const char* pattern_ptr = like_ctx.instr_start_;
  const int32_t pattern_len = like_ctx.instr_length_;
  const char* text_ptr = text.ptr();
  const int32_t text_len = text.length();
  if (OB_UNLIKELY(cs_type != CS_TYPE_UTF8MB4_BIN)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument(s)", K(ret), K(cs_type), K(text));
  } else if (OB_UNLIKELY(0 == pattern_len || NULL == pattern_ptr)) {
    result.set_int(1);
  } else if (OB_UNLIKELY(text_len < pattern_len)) {
    result.set_int(0);
  } else {
    switch (like_ctx.instr_mode_) {
      case ObExprLikeContext::START_WITH_PERCENT_SIGN: {
        int cmp = MEMCMP(text_ptr + text_len - pattern_len, pattern_ptr, pattern_len);
        int64_t res = (0 == cmp) ? 1 : 0;
        result.set_int(res);
        break;
      }
      case ObExprLikeContext::START_END_WITH_PERCENT_SIGN: {
        void* instr = MEMMEM(text_ptr, text_len, pattern_ptr, pattern_len);
        int64_t res = (NULL == instr) ? 0 : 1;
        result.set_int(res);
        break;
      }
      case ObExprLikeContext::END_WITH_PERCENT_SIGN: {
        int cmp = MEMCMP(text_ptr, pattern_ptr, pattern_len);
        int64_t res = (0 == cmp) ? 1 : 0;
        result.set_int(res);
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected instr mode", K(ret), K(like_ctx.instr_mode_), K(text));
        break;
      }
    }
  }
  return ret;
}

int ObExprLike::calc_escape_wc(const ObCollationType escape_coll, const ObString& escape, int32_t& escape_wc)
{
  int ret = OB_SUCCESS;
  size_t length = ObCharset::strlen_char(escape_coll, escape.ptr(), escape.length());
  if (1 != length) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument to ESCAPE", K(escape), K(length), K(ret));
  } else if (OB_FAIL(ObCharset::mb_wc(escape_coll, escape, escape_wc))) {
    LOG_WARN("failed to convert escape to wc", K(ret), K(escape), K(escape_coll), K(escape_wc));
    ret = OB_INVALID_ARGUMENT;
  }
  return ret;
}

void ObExprLike::state_trans_with_percent_sign(STATE& current_state)
{
  switch (current_state) {
    case INIT: {
      //"" ==> "%"
      current_state = PERCENT;
      break;
    }
    case PERCENT: {
      //"%" ==> "%%"
      current_state = PERCENT;
      break;
    }
    case NONPERCENT: {
      //"a" ==> "a%"
      current_state = NONPERCENT_PERCENT;
      break;
    }
    case PERCENT_NONPERCENT: {
      //"%a" ==> "%a%"
      current_state = PERCENT_NONPERCENT_PENCENT;
      break;
    }
    case NONPERCENT_PERCENT: {
      //"a%" ==> "a%%"
      current_state = NONPERCENT_PERCENT;
      break;
    }
    case PERCENT_NONPERCENT_PENCENT: {
      current_state = PERCENT_NONPERCENT_PENCENT;
      break;
    }
    default: {
      current_state = END;
      break;
    }
  }
}
void ObExprLike::state_trans_with_nonpercent_char(STATE& current_state)
{
  switch (current_state) {
    case INIT: {
      //"" ==> "a"
      current_state = NONPERCENT;
      break;
    }
    case PERCENT: {
      //"%" ==> "%a"
      current_state = PERCENT_NONPERCENT;
      break;
    }
    case NONPERCENT: {
      //"a" ==> "aa"
      current_state = NONPERCENT;
      break;
    }
    case PERCENT_NONPERCENT: {
      //"%a" ==> "%aa"
      current_state = PERCENT_NONPERCENT;
      break;
    }
    case NONPERCENT_PERCENT: {
      //"a%" ==> "a%a". invalid
      current_state = END;
      break;
    }
    case PERCENT_NONPERCENT_PENCENT: {
      //"%a%" ==> "%a%a". invalid
      current_state = END;
      break;
    }
    default: {
      current_state = END;
      break;
    }
  }
}

int ObExprLike::is_escape(
    const ObCollationType cs_type, const char* buf_start, int32_t char_len, int32_t escape_wc, bool& res)
{
  int ret = OB_SUCCESS;
  res = false;
  // once is_escape is called
  // we have to construct and destruct the string.
  // while, note that is_escape will not be called too frequently
  // so, never mind it
  ObString string(char_len, buf_start);
  int32_t wc = 0;
  if (OB_FAIL(ObCharset::mb_wc(cs_type, string, wc))) {
    LOG_WARN("failed to get wc", K(ret), K(string), K(cs_type));
    ret = OB_INVALID_ARGUMENT;
  } else {
    res = (wc == escape_wc);
  }
  return ret;
}

template <typename T>
int ObExprLike::calc_with_non_instr_mode(T& result, const ObCollationType coll_type, const ObCollationType escape_coll,
    const ObString& text_val, const ObString& pattern_val, const ObString& escape_val)
{
  // convert escape char
  // escape use its own collation,
  // try this query in MySQL:
  // mysql>  select 'a%' like 'A2%' ESCAPE X'32', X'32';
  // +------------------------------+-------+
  // | 'a%' like 'A2%' ESCAPE X'32' | X'32' |
  // +------------------------------+-------+
  // |                            1 | 2     |
  // +------------------------------+-------+
  int ret = OB_SUCCESS;
  int32_t escape_wc = 0;
  if (OB_FAIL(calc_escape_wc(escape_coll, escape_val, escape_wc))) {
    LOG_WARN("failed to get the wc of escape", K(ret), K(escape_val), K(escape_coll), K(escape_wc));
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ESCAPE");
  } else if (text_val.length() <= 0 && pattern_val.length() <= 0) {
    // empty string
    result.set_int(1);
  } else {
    bool b = ObCharset::wildcmp(
        coll_type, text_val, pattern_val, escape_wc, static_cast<int32_t>('_'), static_cast<int32_t>('%'));
    result.set_int(static_cast<int64_t>(b));
  }
  return ret;
}

OB_DEF_SERIALIZE(ObExprLike)
{
  int ret = OB_SUCCESS;
  BASE_SER((ObExprLike, ObFuncExprOperator));
  OB_UNIS_ENCODE(is_pattern_literal_);
  OB_UNIS_ENCODE(is_text_literal_);
  OB_UNIS_ENCODE(is_escape_literal_);
  OB_UNIS_ENCODE(like_id_);
  return ret;
}

OB_DEF_DESERIALIZE(ObExprLike)
{
  int ret = OB_SUCCESS;
  BASE_DESER((ObExprLike, ObFuncExprOperator));
  is_pattern_literal_ = false;
  is_text_literal_ = true;
  is_escape_literal_ = false;
  like_id_ = -1;
  OB_UNIS_DECODE(is_pattern_literal_);
  OB_UNIS_DECODE(is_text_literal_);
  OB_UNIS_DECODE(is_escape_literal_);
  OB_UNIS_DECODE(like_id_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObExprLike)
{
  int64_t len = 0;
  BASE_ADD_LEN((ObExprLike, ObFuncExprOperator));
  OB_UNIS_ADD_LEN(is_pattern_literal_);
  OB_UNIS_ADD_LEN(is_text_literal_);
  OB_UNIS_ADD_LEN(is_escape_literal_);
  OB_UNIS_ADD_LEN(like_id_);
  return len;
}

int ObExprLike::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  UNUSED(op_cg_ctx);
  int ret = OB_SUCCESS;
  const ObRawExpr* text_expr = NULL;
  const ObRawExpr* pattern_expr = NULL;
  const ObRawExpr* escape_expr = NULL;
  if (OB_UNLIKELY(3 != raw_expr.get_param_count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("like op should have 3 arguments", K(raw_expr.get_param_count()));
  } else if (OB_ISNULL(text_expr = raw_expr.get_param_expr(0)) ||
             OB_ISNULL(pattern_expr = raw_expr.get_param_expr(1)) ||
             OB_ISNULL(escape_expr = raw_expr.get_param_expr(2))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("null pointer", K(text_expr), K(pattern_expr), K(escape_expr));
  } else if (rt_expr.arg_cnt_ != 3 || OB_ISNULL(rt_expr.args_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("like expr should have 3 arguments", K(ret), K(rt_expr.arg_cnt_), K(rt_expr.args_));
  } else if (OB_ISNULL(rt_expr.args_[0]) || OB_ISNULL(rt_expr.args_[1]) || OB_ISNULL(rt_expr.args_[2])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child is null", K(ret), K(rt_expr.args_[0]), K(rt_expr.args_[1]), K(rt_expr.args_[2]));
  } else {
    OB_ASSERT(ObVarcharType == rt_expr.args_[0]->datum_meta_.type_ ||
              ObLongTextType == rt_expr.args_[0]->datum_meta_.type_ ||
              ObNullType == rt_expr.args_[0]->datum_meta_.type_);
    OB_ASSERT(ObVarcharType == rt_expr.args_[1]->datum_meta_.type_ ||
              ObLongTextType == rt_expr.args_[1]->datum_meta_.type_ ||
              ObNullType == rt_expr.args_[1]->datum_meta_.type_);
    OB_ASSERT(
        ObVarcharType == rt_expr.args_[2]->datum_meta_.type_ || ObNullType == rt_expr.args_[2]->datum_meta_.type_);
    bool pattern_literal = pattern_expr->has_flag(IS_CONST) || pattern_expr->has_flag(IS_CONST_EXPR);
    bool escape_literal = escape_expr->has_flag(IS_CONST) || escape_expr->has_flag(IS_CONST_EXPR);
    // do check and match optimization only if extra_ is 1.
    if (pattern_literal && escape_literal) {
      rt_expr.extra_ = 1;
    } else {
      rt_expr.extra_ = 0;
    }
    rt_expr.eval_func_ = ObExprLike::like_varchar;
  }

  return ret;
}

void ObExprLike::record_last_check(ObExprLikeContext& like_ctx, const ObString pattern_val, const ObString escape_val,
    ObIAllocator* buf_alloc, bool is_static_engine)
{
  if (is_static_engine) {
    const uint32_t init_len = 16;
    like_ctx.same_as_last = false;
    like_ctx.last_pattern_len_ = pattern_val.length();
    if (pattern_val.length() > like_ctx.pattern_buf_len_) {
      if (0 == like_ctx.pattern_buf_len_) {
        like_ctx.pattern_buf_len_ = init_len;
      }
      while (pattern_val.length() > like_ctx.pattern_buf_len_) {
        like_ctx.pattern_buf_len_ *= 2;
      }
      like_ctx.last_pattern_ = (char*)(buf_alloc->alloc(sizeof(char) * like_ctx.pattern_buf_len_));
    }
    MEMCPY(like_ctx.last_pattern_, pattern_val.ptr(), pattern_val.length());
    like_ctx.last_escape_len_ = escape_val.length();
    if (escape_val.length() > like_ctx.escape_buf_len_) {
      if (0 == like_ctx.escape_buf_len_) {
        like_ctx.escape_buf_len_ = init_len;
      }
      while (escape_val.length() > like_ctx.escape_buf_len_) {
        like_ctx.escape_buf_len_ *= 2;
      }
      like_ctx.last_escape_ = (char*)(buf_alloc->alloc(sizeof(char) * like_ctx.escape_buf_len_));
    }
    MEMCPY(like_ctx.last_escape_, escape_val.ptr(), escape_val.length());
  } else {
    like_ctx.set_checked();
  }
}

bool ObExprLike::checked_already(const ObExprLikeContext& like_ctx, bool null_pattern, const ObString pattern_val,
    bool null_escape, const ObString escape_val, bool is_static_engine)
{
  bool res = false;
  if (is_static_engine) {
    res = !null_pattern && !null_escape && escape_val.length() == like_ctx.last_escape_len_ &&
          pattern_val.length() == like_ctx.last_pattern_len_ &&
          0 == MEMCMP(escape_val.ptr(), like_ctx.last_escape_, escape_val.length()) &&
          0 == MEMCMP(pattern_val.ptr(), like_ctx.last_pattern_, pattern_val.length());
  } else {
    res = like_ctx.is_checked();
  }
  return res;
}

int ObExprLike::like_varchar(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  const bool do_optimization = expr.extra_;
  if (OB_FAIL(expr.eval_param_value(ctx))) {
    LOG_WARN("eval param value failed", K(ret));
  }
  ObDatum& text = expr.locate_param_datum(ctx, 0);
  ObDatum& pattern = expr.locate_param_datum(ctx, 1);
  ObDatum& escape = expr.locate_param_datum(ctx, 2);
  uint64_t like_id = static_cast<uint64_t>(expr.expr_ctx_id_);
  const ObCollationType escape_coll = expr.args_[2]->datum_meta_.cs_type_;
  const ObCollationType coll_type = expr.args_[0]->datum_meta_.cs_type_;
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(check_pattern_valid(
                 pattern, escape, escape_coll, coll_type, &ctx.exec_ctx_, like_id, do_optimization, true))) {
    LOG_WARN("fail to check pattern string", K(pattern), K(escape), K(coll_type));
  } else if (text.is_null() || pattern.is_null()) {
    expr_datum.set_null();
  } else {
    ObString text_val = text.get_string();
    ObString pattern_val = pattern.get_string();
    ObString escape_val;
    if (escape.is_null()) {
      escape_val.assign_ptr("\\", 1);
    } else {
      escape_val = escape.get_string();
    }
    if (OB_SUCC(ret)) {
      if (do_optimization && like_id != OB_INVALID_ID && (!text_val.empty()) && (!pattern_val.empty())) {
        ObExprLikeContext* like_ctx = NULL;
        if (NULL == (like_ctx = static_cast<ObExprLikeContext*>(ctx.exec_ctx_.get_expr_op_ctx(like_id)))) {
          ret = OB_ERR_UNEXPECTED;
          // like context should be created while checking validation.
          LOG_WARN("like context is null", K(ret), K(like_id));
        }
        if (OB_SUCC(ret)) {
          if (OB_UNLIKELY(
                  (!is_oracle_mode() && !checked_already(*like_ctx, false, pattern_val, false, escape_val, true)) ||
                  (is_oracle_mode() && !like_ctx->same_as_last))) {
            if (OB_FAIL(set_instr_info(&ctx.exec_ctx_.get_allocator(),
                    coll_type,
                    text_val,
                    pattern_val,
                    escape_val,
                    escape_coll,
                    *like_ctx))) {
              LOG_WARN("failed to set instr info", K(ret), K(pattern_val), K(text_val));
            } else if (like_ctx->is_instr_mode()) {  // instr mode
              ret = calc_with_instr_mode(expr_datum, coll_type, text_val, *like_ctx);
            } else {  // not instr mode
              ret = calc_with_non_instr_mode(expr_datum, coll_type, escape_coll, text_val, pattern_val, escape_val);
            }
            if (!is_oracle_mode()) {
              record_last_check(*like_ctx, pattern_val, escape_val, &ctx.exec_ctx_.get_allocator(), true);
            }
          } else if (like_ctx->is_instr_mode()) {  // instr mode
            ret = calc_with_instr_mode(expr_datum, coll_type, text_val, *like_ctx);
          } else {  // not instr mode
            ret = calc_with_non_instr_mode(expr_datum, coll_type, escape_coll, text_val, pattern_val, escape_val);
          }
        }
      } else {  // normal path. no optimization here.
        ret = calc_with_non_instr_mode(expr_datum, coll_type, escape_coll, text_val, pattern_val, escape_val);
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
