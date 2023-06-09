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
#include "sql/engine/expr/ob_expr_lob_utils.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

#define PERCENT_SIGN_START(mode) (START_WITH_PERCENT_SIGN == mode || START_END_WITH_PERCENT_SIGN == mode)
#define PERCENT_SIGN_END(mode) (END_WITH_PERCENT_SIGN == mode || START_END_WITH_PERCENT_SIGN == mode)

int ObExprLike::InstrInfo::record_pattern(char *&pattern_buf, const ObString &pattern)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(pattern.length() <= instr_buf_length_)) {
    pattern_buf = instr_buf_;
  } else if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", K(ret));
  } else if (OB_ISNULL(pattern_buf = (char*)(allocator_->alloc(sizeof(char)
                                              * pattern.length() * 2)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("No more memories", K(ret));
  } else {
    instr_buf_ = pattern_buf;
    instr_buf_length_ = pattern.length() * 2;
  }

  if (OB_SUCC(ret)) {
    MEMCPY(pattern_buf, pattern.ptr(), pattern.length());
  }
  return ret;
}

int ObExprLike::InstrInfo::add_instr_info(const char *start, const uint32_t length)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(instr_cnt_ < instr_info_buf_size_)) {
    instr_starts_[instr_cnt_] = start;
    instr_lengths_[instr_cnt_] = length;
    instr_cnt_++;
  } else {
    const uint32_t init_buf_size = 8;
    const uint32_t new_buf_size = MAX(init_buf_size, instr_info_buf_size_ * 2);
    const char **new_instr_starts = NULL;
    uint32_t *new_instr_lengths = NULL;
    if (OB_ISNULL(new_instr_starts =
                  static_cast<const char **>(allocator_->alloc(sizeof(char *) * new_buf_size)))
        || OB_ISNULL(new_instr_lengths =
                    static_cast<uint32_t *>(allocator_->alloc(sizeof(uint32_t) * new_buf_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocator memory failed", K(ret), K(new_instr_starts), K(new_buf_size));
    } else {
      MEMCPY(new_instr_starts, instr_starts_, sizeof(char *) * instr_cnt_);
      MEMCPY(new_instr_lengths, instr_lengths_, sizeof(uint32_t) * instr_cnt_);
      instr_info_buf_size_ = new_buf_size;
      instr_starts_ = new_instr_starts;
      instr_lengths_ = new_instr_lengths;
      instr_starts_[instr_cnt_] = start;
      instr_lengths_[instr_cnt_] = length;
      instr_cnt_++;
    }
  }
  return ret;
}

ObExprLike::ObExprLike(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_OP_LIKE, N_LIKE, 3, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION),
      is_pattern_literal_(false), is_text_literal_(true), is_escape_literal_(false),
      like_id_(-1)
{
  need_charset_convert_ = false;
}

ObExprLike::~ObExprLike()
{
}

int ObExprLike::assign(const ObExprOperator &other)
{
  int ret = OB_SUCCESS;
  const ObExprLike *tmp_other = dynamic_cast<const ObExprLike *>(&other);
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
template <bool is_static_engine, typename T>
int ObExprLike::check_pattern_valid(const T &pattern,
                              const T &escape,
                              const ObCollationType escape_coll,
                              ObCollationType coll_type,
                              ObExecContext *exec_ctx,
                              const uint64_t like_id,
                              const bool check_optimization)
{
  int ret = OB_SUCCESS;
  int32_t escape_wc = 0;
  const ObCharsetInfo *cs = NULL;
  ObString escape_val = escape.get_string();
  ObString pattern_val = pattern.get_string();
  ObExprLikeContext *like_ctx = NULL;
  if (is_static_engine) {
    // check_optimizaiton is true, only if pattern and escape are const.
    if (check_optimization && NULL == (like_ctx = static_cast<ObExprLikeContext *>
                                                    (exec_ctx->get_expr_op_ctx(like_id)))) {
      if (OB_FAIL(exec_ctx->create_expr_op_ctx(like_id, like_ctx))) {
        LOG_WARN("failed to create operator ctx", K(ret), K(like_id));
      } else {
        like_ctx->instr_info_.set_allocator(exec_ctx->get_allocator());
      }
    }
  } else if (NULL != exec_ctx) {
    // When text, pattern and escape are all const, report error when create op ctx
    // If it's error, then don't optimize check
    int tmp_ret = OB_SUCCESS;
    if (NULL == (like_ctx = static_cast<ObExprLikeContext *>
                                      (exec_ctx->get_expr_op_ctx(like_id)))) {
      if (OB_SUCCESS != (tmp_ret = exec_ctx->create_expr_op_ctx(like_id, like_ctx))) {
        LOG_DEBUG("failed to create operator ctx", K(ret), K(like_id));
      } else {
        like_ctx->instr_info_.set_allocator(exec_ctx->get_allocator());
      }
    }
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("failed to check pattern", K(ret), K(like_id));
  } else if (!lib::is_oracle_mode()) {
    //don't check in mysql mode
  } else if (NULL != like_ctx && checked_already<is_static_engine>(*like_ctx, pattern.is_null(),
                                                        pattern_val, escape.is_null(), escape_val)) {
    // skip check if pattern and escape are same as checked last time.
    //select * from t1 where exist (select * from t2 where 'abc' like t1.c1 escape t1.c2);
    //pattern t1.c1 and escape t1.c2 are const in subquery, but they may change.
    like_ctx->same_as_last = true;
  } else if (escape.is_null() || 1 != escape_val.length()) {
    ret = OB_ERR_INVALID_ESCAPE_CHAR_LENGTH;
    LOG_WARN("escape character must be character string of length 1", K(escape_val), K(ret));
  } else if (OB_FAIL(calc_escape_wc(escape_coll, escape_val, escape_wc))) {
    LOG_WARN("fail to calc escape wc", K(escape_val), K(escape_coll));
  } else if (OB_UNLIKELY(OB_ISNULL(cs = ObCharset::get_charset(coll_type)) ||
                  OB_ISNULL(cs->cset))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected error. invalid argument(s)",K(coll_type));
  } else if (!pattern.is_null()) {
    const char *buf_start = pattern_val.ptr();
    const char *buf_end = pattern_val.ptr() + pattern_val.length();
    int error = 0;
    int32_t char_len = 0;
    bool is_valid = true;
    bool is_char_escape = false;
    bool pre_char_is_escape = false;
    while (OB_SUCC(ret) && buf_start < buf_end && is_valid) {
      char_len = static_cast<int32_t>(cs->cset->well_formed_len(cs, buf_start, buf_end, 1, &error));
      if (OB_UNLIKELY(0 != error)) {
        ret = OB_ERR_INVALID_CHARACTER_STRING;
        LOG_WARN("well_formed_len failed. invalid char",
                  K(buf_start), K(pattern_val), K(char_len));
      } else if (OB_FAIL(is_escape(coll_type, buf_start, char_len, escape_wc, is_char_escape))) {
        LOG_WARN("fail to judge escape", K(escape_val), K(escape_coll));
      } else if (is_char_escape) {
        // 连续两个escape char, like: select 1 from t1 where 'a' like 'aa' escape 'a'; -- it's ok
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
        ObString percent_str = ObCharsetUtils::get_const_str(coll_type, '%');
        ObString underline_str = ObCharsetUtils::get_const_str(coll_type, '_');
        const ObString pattern_char = ObString(char_len, buf_start);
        if (0 == pattern_char.compare(percent_str) || 0 == pattern_char.compare(underline_str)) {
          // it's ok
        } else {
          ret = OB_ERR_INVALID_CHAR_FOLLOWING_ESCAPE_CHAR;
          LOG_WARN("missing or illegal character following the escape character",
                    K(escape_val), K(pattern_val), K(pattern_char), K(ret));
        }
        pre_char_is_escape = false;
      }
      buf_start += char_len;
    }//end while
    if (pre_char_is_escape) {
      // Last character is escape character
      // // Eg: select 1 from t1 where 'a' like 'a_a' escape 'a'; -- it's error
      ret = OB_ERR_INVALID_CHAR_FOLLOWING_ESCAPE_CHAR;
      LOG_WARN("missing or illegal character following the escape character", K(escape_val),
                  K(pattern_val), K(ret));
    }
    if (OB_SUCC(ret) && NULL != like_ctx) {
      record_last_check<is_static_engine>(*like_ctx, pattern_val,
                                          escape_val, &exec_ctx->get_allocator());
    }
  }
  return ret;
}

int ObExprLike::calc_result_type3(ObExprResType &type,
                                  ObExprResType &type1,
                                  ObExprResType &type2,
                                  ObExprResType &type3,
                                  ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (!type1.is_null()
      && !type2.is_null()
      && !type3.is_null()
      && (!is_type_valid(type1.get_type())
          || !is_type_valid(type2.get_type())
          || !is_type_valid(type3.get_type()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the param is not castable", K(type1), K(type2), K(type3), K(ret));
  } else if (OB_NOT_NULL(type_ctx.get_session())
             && lib::is_oracle_mode()) {
    ObSEArray<ObExprResType*, 2> str_params;
    ObExprResType tmp_result_type;

    OZ(str_params.push_back(&type1));
    OZ(aggregate_string_type_and_charset_oracle(*type_ctx.get_session(), str_params, tmp_result_type));
    OZ(str_params.push_back(&type2));
    OZ(deduce_string_param_calc_type_and_charset(*type_ctx.get_session(), tmp_result_type, str_params));

    type3.set_calc_type(ObVarcharType);
    type3.set_calc_collation_type(type_ctx.get_session()->get_nls_collation());
    
    type.set_int();
    type.set_calc_type(type1.get_calc_type());
    type.set_calc_collation_type(type1.get_calc_collation_type());
    type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
    type.set_precision(DEFAULT_PRECISION_FOR_BOOL);
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
    if (lib::is_oracle_mode()) {
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
    ObExprOperator::calc_result_flag2(type, type1, type2); // ESCAPE is ignored
  }
  return ret;
}

int ObExprLike::set_instr_info(ObIAllocator *exec_allocator,
                               const ObCollationType cs_type,
                               const ObString &pattern,
                               const ObString &escape,
                               const ObCollationType escape_coll,
                               ObExprLikeContext &like_ctx)
{
  //If you feel tough to understand this func,
  //please feel free to refer here for more details :
  //
  int ret = OB_SUCCESS;
  like_ctx.instr_info_.reuse();
  const ObCharsetInfo *cs = NULL;
  char *pattern_buf = nullptr;
  ObIAllocator *exec_cal_buf = exec_allocator;
  InstrInfo &instr_info = like_ctx.instr_info_;
  if (cs_type != CS_TYPE_UTF8MB4_BIN) {
    //we optimize the case in which cs_type == CS_TYPE_UTF8MB4_BIN only
    //just let it go
  } else if (OB_UNLIKELY(OB_ISNULL(cs = ObCharset::get_charset(cs_type)) ||
                  OB_ISNULL(cs->cset))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected error. invalid argument(s)",K(cs_type), K(pattern), K(escape));
  } else if (OB_UNLIKELY(pattern.empty())) {
    //do nothing.just let it go.
  } else if (OB_ISNULL(exec_cal_buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Failed to get exec cal buf", K(ret));
  } else {
    int32_t escape_wc = 0;
    if (OB_FAIL(instr_info.record_pattern(pattern_buf, pattern))) {
      LOG_WARN("record pattern failed", K(ret));
    } else if (OB_FAIL(calc_escape_wc(escape_coll, escape, escape_wc))) {
      LOG_WARN("calc escape wc failed", K(ret), K(escape_coll), K(escape));
    } else {
      //iterate pattern now
      const char *buf_start = pattern_buf;
      const char *buf_end = pattern_buf + pattern.length();
      int error = 0;
      int32_t char_len = 0;
      bool is_char_escape = false;
      bool use_instr_mode = true;
      const char *instr_start = NULL;
      uint32_t  instr_len = 0;
      bool percent_sign_exist = false;
      while (OB_SUCC(ret) && buf_start < buf_end && use_instr_mode) {
        char_len = static_cast<int32_t>(cs->cset->well_formed_len(cs, buf_start, buf_end, 1, &error));
        is_char_escape = false;
        if (OB_UNLIKELY(0 != error)) {
          ret = OB_ERR_INVALID_CHARACTER_STRING;
          LOG_WARN("well_formed_len failed. invalid char",
                    K(cs_type), K(buf_start), K(pattern), K(char_len));
        } else if (OB_FAIL(is_escape(cs_type, buf_start, char_len, escape_wc, is_char_escape))) {
          LOG_WARN("check is escape failed", K(ret), K(escape_coll));
        } else if (is_char_escape || (1 == char_len && '_' == *buf_start)) {
          //when there are "_" or escape in pattern
          //the case can not be optimized.
          use_instr_mode = false;
        // since cs_type is CS_TYPE_UTF8MB4_BIN, length of '%' must be 1.
        } else if ((1 == char_len && '%' == *buf_start)) { //percent sign
          percent_sign_exist = true;
          if (OB_LIKELY(instr_len > 0)) {
            if (OB_FAIL(instr_info.add_instr_info(instr_start, instr_len))) {
              LOG_WARN("add instr info failed", K(ret));
            }
            instr_info.instr_total_length_ += instr_len;
            instr_len = 0;
          }
          buf_start += char_len;
        } else {  //non-percent char
          if (0 == instr_len) {
            instr_start = buf_start;
          }
          buf_start += char_len;
          instr_len += char_len;
        }
      }//end while

      if (OB_SUCC(ret) && use_instr_mode && percent_sign_exist) {
        bool end_with_percent_sign = true;
        if (instr_len > 0) {  // record last instr
          end_with_percent_sign = false;
          instr_info.instr_total_length_ += instr_len;
          if (OB_FAIL(instr_info.add_instr_info(instr_start, instr_len))) {
            LOG_WARN("add instr info failed", K(ret));
          }
        }
        if (OB_UNLIKELY(instr_info.empty())) {
          instr_info.instr_mode_ = ALL_PERCENT_SIGN;
        } else {
          bool start_with_percent_sign = instr_info.instr_starts_[0] != pattern_buf;
          instr_info.instr_mode_ = start_with_percent_sign ?
                  (end_with_percent_sign ? START_END_WITH_PERCENT_SIGN : START_WITH_PERCENT_SIGN) :
                  (end_with_percent_sign ? END_WITH_PERCENT_SIGN : MIDDLE_PERCENT_SIGN);
        }
      }//end deduce instrmode
    }//end else
  }
  LOG_DEBUG("end set instr info", K(cs_type), K(pattern), K(escape),
            K(escape_coll), K(instr_info));
  return ret;
}

template <typename T>
int ObExprLike::calc_with_instr_mode(T &result,
    const ObCollationType cs_type,
    const ObString &text,
    const ObExprLikeContext &like_ctx)
{
  int ret = OB_SUCCESS;
  const InstrInfo instr_info = like_ctx.instr_info_;
  const int32_t text_len = text.length();
  if (OB_UNLIKELY(cs_type != CS_TYPE_UTF8MB4_BIN)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument(s)", K(ret), K(cs_type), K(text));
  } else if (OB_UNLIKELY(instr_info.empty())) {
    result.set_int(1);
  } else if (OB_UNLIKELY(text_len < instr_info.instr_total_length_)) {
    result.set_int(0);
  } else {
    int64_t res = 0;
    switch(instr_info.instr_mode_) {
      case START_WITH_PERCENT_SIGN: {
        res = match_with_instr_mode<true, false>(text, instr_info);
        break;
      }
      case START_END_WITH_PERCENT_SIGN: {
        res = match_with_instr_mode<true, true>(text, instr_info);
        break;
      }
      case END_WITH_PERCENT_SIGN: {
        res = match_with_instr_mode<false, true>(text, instr_info);
        break;
      }
      case MIDDLE_PERCENT_SIGN: {
        res = match_with_instr_mode<false ,false>(text, instr_info);
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected instr mode",
                  K(ret), K(instr_info.instr_mode_), K(text));
        break;
      }
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("match with instr mode failed", K(ret), K(instr_info.instr_mode_), K(text));
    } else {
      result.set_int(res);
    }
  }
  return ret;
}

int ObExprLike::calc_escape_wc(const ObCollationType escape_coll,
                               const ObString &escape,
                               int32_t &escape_wc)
{
  int ret = OB_SUCCESS;
  size_t length = ObCharset::strlen_char(escape_coll, escape.ptr(),
                                         escape.length());
  if (1 != length) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument to ESCAPE", K(escape), K(length), K(ret));
  } else if (OB_FAIL(ObCharset::mb_wc(escape_coll, escape, escape_wc))) {
    LOG_WARN("failed to convert escape to wc", K(ret), K(escape),
             K(escape_coll), K(escape_wc));
    ret = OB_INVALID_ARGUMENT;
  }
  return ret;
}

int ObExprLike::is_escape(const ObCollationType cs_type,
                       const char *buf_start,
                       int32_t char_len,
                       int32_t escape_wc,
                       bool &res)
{
  int ret = OB_SUCCESS;
  res = false;
  //once is_escape is called
  //we have to construct and destruct the string.
  //while, note that is_escape will not be called too frequently
  //so, never mind it
  ObString string(char_len, buf_start);
  int32_t wc = 0;
  if (OB_FAIL(ObCharset::mb_wc(cs_type, string, wc))) {
    LOG_WARN("failed to get wc", K(ret), K(string),
               K(cs_type));
    ret = OB_INVALID_ARGUMENT;
  } else {
    res = (wc == escape_wc);
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

int ObExprLike::cg_expr(ObExprCGCtx &op_cg_ctx,
                        const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSED(op_cg_ctx);
  int ret = OB_SUCCESS;
  const ObRawExpr *text_expr = NULL;
  const ObRawExpr *pattern_expr = NULL;
  const ObRawExpr *escape_expr = NULL;
  if (OB_UNLIKELY(3 != raw_expr.get_param_count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("like op should have 3 arguments", K(raw_expr.get_param_count()));
  } else if (OB_ISNULL(text_expr = raw_expr.get_param_expr(0))
            || OB_ISNULL(pattern_expr = raw_expr.get_param_expr(1))
            || OB_ISNULL(escape_expr = raw_expr.get_param_expr(2))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("null pointer", K(text_expr), K(pattern_expr), K(escape_expr));
  } else if (rt_expr.arg_cnt_ != 3 || OB_ISNULL(rt_expr.args_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("like expr should have 3 arguments", K(ret), K(rt_expr.arg_cnt_), K(rt_expr.args_));
  } else if (OB_ISNULL(rt_expr.args_[0]) || OB_ISNULL(rt_expr.args_[1])
            || OB_ISNULL(rt_expr.args_[2])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child is null", K(ret), K(rt_expr.args_[0]), K(rt_expr.args_[1]),
                              K(rt_expr.args_[2]));
  } else if (OB_UNLIKELY(!((ob_is_string_tc(rt_expr.args_[0]->datum_meta_.type_)
                            || ObLongTextType == rt_expr.args_[0]->datum_meta_.type_
                            || ObNullType == rt_expr.args_[0]->datum_meta_.type_)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected param type", K(ret), K(rt_expr.args_[0]->datum_meta_));
  } else if (OB_UNLIKELY(!(ob_is_string_tc(rt_expr.args_[1]->datum_meta_.type_)
                           || ObLongTextType == rt_expr.args_[1]->datum_meta_.type_
                           || ObNullType == rt_expr.args_[1]->datum_meta_.type_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected param type", K(ret), K(rt_expr.args_[1]->datum_meta_));
  } else if (OB_UNLIKELY(!(ObVarcharType == rt_expr.args_[2]->datum_meta_.type_
              || ObNullType == rt_expr.args_[2]->datum_meta_.type_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected param type", K(ret), K(rt_expr.args_[2]->datum_meta_));
  } else {
    //Do optimization even if pattern_expr/escape is pushdown parameter, pattern and escape are 
    //checked whether the same as last time which is recorded in like_ctx for each row in execution.
    bool pattern_literal = pattern_expr->is_const_expr();
    bool escape_literal = escape_expr->is_const_expr();
    //do check and match optimization only if extra_ is 1.
    if (pattern_literal && escape_literal) {
      rt_expr.extra_ = 1;
    } else {
      rt_expr.extra_ = 0;
    }
    rt_expr.eval_func_ = ObExprLike::like_varchar;
    // Since pattern and escape are both literal in TPCH, only support vectorized eval with literal
    // pattern and escape now.
    // In the full vectorized implement of like expr, like_ctx will be useless.
    if (text_expr->is_vectorize_result() &&
        !rt_expr.args_[1]->is_batch_result() &&
        !rt_expr.args_[2]->is_batch_result()) {
      rt_expr.eval_batch_func_ = ObExprLike::eval_like_expr_batch_only_text_vectorized;
    }
  }
  return ret;
}

template <bool is_static_engine>
void ObExprLike::record_last_check(ObExprLikeContext &like_ctx,
                                  const ObString pattern_val,
                                  const ObString escape_val,
                                  ObIAllocator *buf_alloc)
{
  if (is_static_engine) {
    const uint32_t init_len = 16;
    like_ctx.same_as_last = false;
    like_ctx.last_pattern_len_ = pattern_val.length();
    if (pattern_val.length() > like_ctx.pattern_buf_len_) {
      if(0 == like_ctx.pattern_buf_len_) {
        like_ctx.pattern_buf_len_ = init_len;
      }
      while (pattern_val.length() > like_ctx.pattern_buf_len_) {
        like_ctx.pattern_buf_len_ *= 2;
      }
      like_ctx.last_pattern_ = (char*) (buf_alloc->alloc(sizeof(char) *
                                        like_ctx.pattern_buf_len_));
    }
    MEMCPY(like_ctx.last_pattern_, pattern_val.ptr(), pattern_val.length());
    like_ctx.last_escape_len_ = escape_val.length();
    if (escape_val.length() > like_ctx.escape_buf_len_) {
      if(0 == like_ctx.escape_buf_len_) {
        like_ctx.escape_buf_len_ = init_len;
      }
      while (escape_val.length() > like_ctx.escape_buf_len_) {
        like_ctx.escape_buf_len_ *= 2;
      }
      like_ctx.last_escape_ = (char*) (buf_alloc->alloc(sizeof(char) *
                                        like_ctx.escape_buf_len_));
    }
    MEMCPY(like_ctx.last_escape_, escape_val.ptr(), escape_val.length());
  } else {
    like_ctx.set_checked();
  }
}

template <bool is_static_engine>
bool ObExprLike::checked_already(const ObExprLikeContext &like_ctx, bool null_pattern,
                                const ObString pattern_val, bool null_escape,
                                const ObString escape_val)
{
  bool res = false;
  if (is_static_engine) {
    res = !null_pattern && !null_escape && escape_val.length() == like_ctx.last_escape_len_
            && pattern_val.length() == like_ctx.last_pattern_len_
            && 0 == MEMCMP(escape_val.ptr(), like_ctx.last_escape_, escape_val.length())
            && 0 == MEMCMP(pattern_val.ptr(), like_ctx.last_pattern_, pattern_val.length());
  } else {
    res = like_ctx.is_checked();
  }
  LOG_DEBUG("like check already end", K(null_pattern), K(pattern_val), K(escape_val), K(res));
  return res;
}

int ObExprLike::like_varchar_inner(const ObExpr &expr, ObEvalCtx &ctx,  ObDatum &expr_datum,
                                    ObDatum &text, ObDatum &pattern, ObDatum &escape)
{
  int ret = OB_SUCCESS;
  const bool do_optimization = expr.extra_;
  uint64_t like_id = static_cast<uint64_t>(expr.expr_ctx_id_);
  const ObCollationType escape_coll = expr.args_[2]->datum_meta_.cs_type_;
  const ObCollationType coll_type = expr.args_[1]->datum_meta_.cs_type_;
  if (OB_FAIL(check_pattern_valid<true>(pattern, escape, escape_coll, coll_type,
                                        &ctx.exec_ctx_, like_id, do_optimization))) {
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
      if (escape_val.empty()) {
        escape_val.assign_ptr("\\", 1);
      }
    }
    if (do_optimization
        && like_id != OB_INVALID_ID
        && (!text_val.empty())
        && (!pattern_val.empty())) {
      ObExprLikeContext *like_ctx = NULL;
      if (NULL == (like_ctx = static_cast<ObExprLikeContext *>
                      (ctx.exec_ctx_.get_expr_op_ctx(like_id)))) {
        ret = OB_ERR_UNEXPECTED;
        //like context should be created while checking validation.
        LOG_WARN("like context is null", K(ret), K(like_id));
      }
      if (OB_SUCC(ret)) {
        if (OB_UNLIKELY((!is_oracle_mode() && !checked_already<true>(*like_ctx, false, pattern_val,
                                                              false, escape_val))
                        || (is_oracle_mode() && !like_ctx->same_as_last))) {
          if (OB_FAIL(set_instr_info(&ctx.exec_ctx_.get_allocator(), coll_type, pattern_val,
              escape_val, escape_coll, *like_ctx))) {
            LOG_WARN("failed to set instr info", K(ret), K(pattern_val), K(text_val));
          } else if (like_ctx->is_instr_mode()) {//instr mode
            ret = calc_with_instr_mode(expr_datum, coll_type, text_val, *like_ctx);
          } else {//not instr mode
            ret = calc_with_non_instr_mode(expr_datum, coll_type, escape_coll,
                                            text_val, pattern_val, escape_val);
          }
          if (OB_SUCC(ret) && !is_oracle_mode()) {
            record_last_check<true>(*like_ctx, pattern_val, escape_val,
                              &ctx.exec_ctx_.get_allocator());
          }
        } else if (like_ctx->is_instr_mode()) {//instr mode
          ret = calc_with_instr_mode(expr_datum, coll_type, text_val, *like_ctx);
        } else { //not instr mode
          ret = calc_with_non_instr_mode(expr_datum, coll_type, escape_coll, text_val,
                                          pattern_val, escape_val);
        }
      }
    } else { //normal path. no optimization here.
      ret = calc_with_non_instr_mode(expr_datum, coll_type, escape_coll, text_val,
                                      pattern_val, escape_val);
    }
  }
  return ret;
}

int ObExprLike::like_varchar(const ObExpr &expr, ObEvalCtx &ctx,  ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.eval_param_value(ctx))) {
    LOG_WARN("eval param value failed", K(ret));
  }
  ObDatum &text = expr.locate_param_datum(ctx, 0);
  ObDatum &pattern = expr.locate_param_datum(ctx, 1);
  ObDatum &escape = expr.locate_param_datum(ctx, 2);
  // the third arg escape must be varchar
  if (OB_FAIL(ret)) {
  } else if (!ob_is_text_tc(expr.args_[0]->datum_meta_.type_)
             && !ob_is_text_tc(expr.args_[1]->datum_meta_.type_)) {
    ret = like_varchar_inner(expr, ctx, expr_datum, text, pattern, escape);
  } else { // text tc
    ObString text_val = text.get_string();
    ObString pattern_val = pattern.get_string();
    ObString escape_str = escape.get_string();
    // Notice: should not change original datums
    // ToDo: @gehao Streaming like interfaces
    ObDatum text_inrow = text; // copy datum flags;
    ObDatum pattern_inrow = pattern;
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, text,
                expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), text_val))) {
      LOG_WARN("failed to read text", K(ret), K(text_val));
    } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, pattern,
                       expr.args_[1]->datum_meta_, expr.args_[1]->obj_meta_.has_lob_header(), pattern_val))) {
      LOG_WARN("failed to read pattern", K(ret), K(pattern_val));
    } else {
      if (!text_inrow.is_null() && !text_inrow.is_nop()) {
        text_inrow.set_string(text_val);
      }
      if (!pattern_inrow.is_null() && !pattern_inrow.is_nop()) {
        pattern_inrow.set_string(pattern_val);
      }
      ret = like_varchar_inner(expr, ctx, expr_datum, text_inrow, pattern_inrow, escape);
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to eval like varchar", K(ret));
  }
  return ret;
}


template <bool percent_sign_start, bool percent_sign_end>
int64_t ObExprLike::match_with_instr_mode(const ObString &text, const InstrInfo instr_info)
{
  int64_t res = 0;
  const char *text_ptr = text.ptr();
  uint32_t text_len = text.length();
  const char **instr_pos = instr_info.instr_starts_;
  const uint32_t *instr_len = instr_info.instr_lengths_;
  bool match = true;
  int64_t idx = 0;
  int64_t idx_end = percent_sign_end ? instr_info.instr_cnt_ : instr_info.instr_cnt_ - 1;
  // if not start with %, memcmp for first instr.
  if (!percent_sign_start) {
    if (text_len < instr_len[0]) {
      match = false;
    } else {
      int cmp = MEMCMP(text_ptr, instr_pos[0], instr_len[0]);
      match = 0 == cmp;
      text_ptr += instr_len[0];
      text_len -= instr_len[0];
      idx++;
    }
  }
  // memmem for str surrounded by %
  for (; idx < idx_end && match; idx++) {
    char *new_text = static_cast<char*>(MEMMEM(text_ptr, text_len, instr_pos[idx], instr_len[idx]));
    text_len -= new_text != NULL ? new_text - text_ptr + instr_len[idx] : 0;
    if (OB_UNLIKELY(text_len < 0)) {
      match = false;
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "unexpected result of memmem", K(text),
                K(ObString(instr_len[idx], instr_pos[idx])));
    } else {
      match = new_text != NULL;
      text_ptr = new_text + instr_len[idx];
    }
  }
  // if not end with %, memcmp for last instr
  if (match && !percent_sign_end) {
    if (text_len < instr_len[idx]) {
      match = false;
    } else {
      match = 0 == MEMCMP(text.ptr() + text.length() - instr_len[idx],
                          instr_pos[idx], instr_len[idx]);
    }
  }
  res = match ? 1 : 0;
  return res;
}

struct ObNonInstrModeMatcher
{
  inline int64_t operator() (const ObCollationType coll_type,
                        const ObString &text_val,
                        const ObString &pattern_val,
                        int32_t escape_wc,
                        int &ret)
  {
    int64_t res = 0;
    if (OB_UNLIKELY(text_val.length() <= 0 && pattern_val.length() <= 0)) {
      // empty string
      res = 1;
    } else {
      bool b = ObCharset::wildcmp(coll_type, text_val, pattern_val, escape_wc,
                                  static_cast<int32_t>('_'), static_cast<int32_t>('%'));
      res = static_cast<int>(b);
    }
    return res;
  }
};

template <bool NullCheck, bool UseInstrMode, INSTR_MODE InstrMode>
int ObExprLike::match_text_batch(BATCH_EVAL_FUNC_ARG_DECL,
                                const ObCollationType coll_type,
                                const int32_t escape_wc,
                                const ObString &pattern_val,
                                const InstrInfo instr_info)
{
  int ret = OB_SUCCESS;
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ObDatum *res_datums = expr.locate_batch_datums(ctx);
  ObDatum *text_datums = expr.args_[0]->locate_batch_datums(ctx);
  const int64_t step_size = sizeof(uint16_t) * CHAR_BIT;
  const ObObjType text_type = expr.args_[0]->datum_meta_.type_;
  // calc match result for each text
  for (int64_t i = 0; i < size && OB_SUCC(ret);) {
    const int64_t bit_vec_off = i / (CHAR_BIT * sizeof(uint16_t));
    const uint16_t skip_v = skip.reinterpret_data<uint16_t>()[bit_vec_off];
    uint16_t &eval_v = eval_flags.reinterpret_data<uint16_t>()[bit_vec_off];
    if (i + step_size < size && (0 == (skip_v | eval_v))) {
      for (int64_t j = 0; OB_SUCC(ret) && j < step_size; i++, j++) {
        if (NullCheck && text_datums[i].is_null()) {
          res_datums[i].set_null();
        } else if (!ob_is_text_tc(text_type)) {
          if (UseInstrMode) {
            int64_t res = ALL_PERCENT_SIGN == InstrMode ? 1
                    : match_with_instr_mode<PERCENT_SIGN_START(InstrMode), PERCENT_SIGN_END(InstrMode)>
                    (text_datums[i].get_string(), instr_info);
            res_datums[i].set_int(res);
          } else {
            res_datums[i].set_int(ObNonInstrModeMatcher()(coll_type, text_datums[i].get_string(),
                                                          pattern_val, escape_wc, ret));
          }
        } else { // text tc
          ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
          common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
          ObString text_val = text_datums[i].get_string();
          if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator,
                                                                text_datums[i],
                                                                expr.args_[0]->datum_meta_,
                                                                expr.args_[0]->obj_meta_.has_lob_header(),
                                                                text_val))) {
            LOG_WARN("failed to read text", K(ret), K(text_val));
          } else if (UseInstrMode) {
            int64_t res = ALL_PERCENT_SIGN == InstrMode ? 1
                  : match_with_instr_mode<PERCENT_SIGN_START(InstrMode), PERCENT_SIGN_END(InstrMode)>
                  (text_val, instr_info);
            res_datums[i].set_int(res);
          } else {
            res_datums[i].set_int(ObNonInstrModeMatcher()(coll_type, text_val,
                                                          pattern_val, escape_wc, ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        eval_v = 0xFFFF;
      }
    } else if (i + step_size < size && (0xFFFF == (skip_v | eval_v))) {
      i += step_size;
    } else {
      const int64_t new_size = std::min(size, i + step_size);
      for (; i < new_size && OB_SUCC(ret); i++) {
        if (!(skip.at(i) || eval_flags.at(i))) {
          if (NullCheck && text_datums[i].is_null()) {
            res_datums[i].set_null();
          } else if (!ob_is_text_tc(text_type)) {
            if (UseInstrMode) {
            int64_t res = ALL_PERCENT_SIGN == InstrMode ? 1
                : match_with_instr_mode<PERCENT_SIGN_START(InstrMode), PERCENT_SIGN_END(InstrMode)>
                (text_datums[i].get_string(), instr_info);
              res_datums[i].set_int(res);
            } else {
              res_datums[i].set_int(ObNonInstrModeMatcher()(coll_type, text_datums[i].get_string(),
                                                            pattern_val, escape_wc, ret));
            }
          } else { // text tc
            ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
            common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
            ObString text_val = text_datums[i].get_string();
            if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator,
                                                                  text_datums[i],
                                                                  expr.args_[0]->datum_meta_,
                                                                  expr.args_[0]->obj_meta_.has_lob_header(),
                                                                  text_val))) {
              LOG_WARN("failed to read text", K(ret), K(text_val));
            } else {
              if (UseInstrMode) {
                int64_t res = ALL_PERCENT_SIGN == InstrMode ? 1
                    : match_with_instr_mode<PERCENT_SIGN_START(InstrMode), PERCENT_SIGN_END(InstrMode)>
                    (text_val, instr_info);
                res_datums[i].set_int(res);
              } else {
                res_datums[i].set_int(ObNonInstrModeMatcher()(coll_type, text_val,
                                                              pattern_val, escape_wc, ret));
              }
            }
          }
          eval_flags.set(i);
        }
      }
    }
  }
  return ret;
}

int ObExprLike::like_text_vectorized_inner(const ObExpr &expr, ObEvalCtx &ctx,
                                           const ObBitVector &skip, const int64_t size,
                                           ObExpr &text, ObDatum *pattern_datum, ObDatum *escape_datum)
{
  int ret = OB_SUCCESS;
  const bool do_optimization = true;
  uint64_t like_id = static_cast<uint64_t>(expr.expr_ctx_id_);
  const ObCollationType coll_type = expr.args_[0]->datum_meta_.cs_type_;
  const ObCollationType escape_coll = expr.args_[2]->datum_meta_.cs_type_;
  if (OB_FAIL(check_pattern_valid<true>(*pattern_datum, *escape_datum,
                                        escape_coll, coll_type,
                                        &ctx.exec_ctx_, like_id, do_optimization))) {
    LOG_WARN("check pattern valid failed", K(ret));
  } else if (OB_FAIL(text.eval_batch(ctx, skip, size))) {
    LOG_WARN("eval text batch failed", K(ret));
  } else if (OB_UNLIKELY(pattern_datum->is_null())) {
    ObDatum *res_datums = expr.locate_batch_datums(ctx);
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    for (int64_t i = 0; i < size; i++) {
      if (!skip.contain(i)) {
        res_datums[i].set_null();
        eval_flags.set(i);
      }
    }
    expr.get_eval_info(ctx).notnull_ = false;
  } else {
    ObString pattern_val = pattern_datum->get_string();
    ObString escape_val;
    // check pattern is not null already, so result is null if and only if text is null.
    bool null_check = !expr.args_[0]->get_eval_info(ctx).notnull_;
    if (escape_datum->is_null() || escape_datum->get_string().empty()) {
      escape_val.assign_ptr("\\", 1);
    } else {
      escape_val = escape_datum->get_string();
    }
    ObExprLikeContext *like_ctx = NULL;
    if (OB_ISNULL(like_ctx = static_cast<ObExprLikeContext *>
                    (ctx.exec_ctx_.get_expr_op_ctx(like_id)))) {
      ret = OB_ERR_UNEXPECTED;
      //like context should be created while checking validation.
      LOG_WARN("like context is null", K(ret), K(like_id));
    } else if (OB_UNLIKELY((!is_oracle_mode() && !checked_already<true>(*like_ctx, false, pattern_val,
                                                                        false, escape_val))
                        || (is_oracle_mode() && !like_ctx->same_as_last))) {
      if (OB_FAIL(set_instr_info(&ctx.exec_ctx_.get_allocator(), coll_type, pattern_val,
          escape_val, escape_coll, *like_ctx))) {
        LOG_WARN("failed to set instr info", K(ret), K(pattern_val));
      } else if (!is_oracle_mode()) {
        record_last_check<true>(*like_ctx, pattern_val, escape_val,
                          &ctx.exec_ctx_.get_allocator());
      }
    }
    INSTR_MODE instr_mode = like_ctx->get_instr_mode();
    const InstrInfo instr_info = like_ctx->instr_info_;
    int32_t escape_wc = 0;
    LOG_DEBUG("set instr info inner end", K(coll_type), K(pattern_val), K(instr_mode),
              K(like_ctx->same_as_last));
    if (OB_FAIL(ret)) {
    } else if (INVALID_INSTR_MODE == instr_mode
               && OB_FAIL(calc_escape_wc(escape_coll, escape_val, escape_wc))) {
      LOG_WARN("calc escape wc failed", K(ret));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ESCAPE");
    } else {
      #define MATCH_TEXT_BATCH_ARG_LIST expr, ctx, skip, size, coll_type, escape_wc, pattern_val, \
                instr_info
      // it seems to take a lot of work to make eval_info.notnull_ correct and it may be removed.
      // so null_check variable is not used now, match_text_batch is called always with null check.
      #define CALL_MATCH_TEXT_BATCH(use_instr_mode, instr_mode) \
          ret = match_text_batch<true, use_instr_mode, instr_mode>(MATCH_TEXT_BATCH_ARG_LIST);

      switch (instr_mode) {
        case INVALID_INSTR_MODE: {
          CALL_MATCH_TEXT_BATCH(false, INVALID_INSTR_MODE)
          break;
        }
        case START_WITH_PERCENT_SIGN: {
          CALL_MATCH_TEXT_BATCH(true, START_WITH_PERCENT_SIGN)
          break;
        }
        case START_END_WITH_PERCENT_SIGN: {
          CALL_MATCH_TEXT_BATCH(true, START_END_WITH_PERCENT_SIGN)
          break;
        }
        case MIDDLE_PERCENT_SIGN: {
          CALL_MATCH_TEXT_BATCH(true, MIDDLE_PERCENT_SIGN);
          break;
        }
        case END_WITH_PERCENT_SIGN: {
          CALL_MATCH_TEXT_BATCH(true, END_WITH_PERCENT_SIGN)
          break;
        }
        case ALL_PERCENT_SIGN: {
          CALL_MATCH_TEXT_BATCH(true, ALL_PERCENT_SIGN)
          break;
        }
        default : {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("unexpected instr mode", K(ret), K(instr_mode), K(pattern_val));
          break;
        }
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("match text batch failed", K(ret), K(instr_mode), K(null_check));
      } else {
        expr.get_eval_info(ctx).notnull_ = !null_check;
      }
      #undef MATCH_TEXT_BATCH_ARG_LIST
      #undef CALL_MATCH_TEXT_BATCH
    }
  }
  return ret;
}

// only text is vectorized, check pattern validation and mode first, then try to match each text.
int ObExprLike::eval_like_expr_batch_only_text_vectorized(BATCH_EVAL_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  ObExpr &text = *expr.args_[0];
  ObExpr &pattern = *expr.args_[1];
  ObExpr &escape = *expr.args_[2];
  ObDatum *pattern_datum = NULL;
  ObDatum *escape_datum = NULL;
  if (OB_FAIL(pattern.eval(ctx, pattern_datum))) {
    LOG_WARN("eval pattern failed", K(ret));
  } else if (OB_FAIL(escape.eval(ctx, escape_datum))) {
    LOG_WARN("eval escape failed", K(ret));
  // the third arg escape must be varchar
  } else if ((!ob_is_text_tc(text.datum_meta_.type_) && !ob_is_text_tc(pattern.datum_meta_.type_))) {
    ret = like_text_vectorized_inner(expr, ctx, skip, size, text, pattern_datum, escape_datum);
  } else {
    ObDatum pattern_inrow;
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    ObString pattern_val = pattern_datum->get_string();
    if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *pattern_datum,
                expr.args_[1]->datum_meta_, expr.args_[1]->obj_meta_.has_lob_header(), pattern_val))) {
      LOG_WARN("failed to read pattern", K(ret), K(pattern_val));
    } else {
      pattern_inrow = *pattern_datum;
      if (!pattern_inrow.is_null() && !pattern_inrow.is_nop()) {
        pattern_inrow.set_string(pattern_val);
      }
      ret = like_text_vectorized_inner(expr, ctx, skip, size, text, &pattern_inrow, escape_datum);
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to eval_like_expr_batch_only_text_vectorized", K(ret));
  }

  return ret;
}

}
}
