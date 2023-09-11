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
#include "sql/engine/expr/ob_expr_regexp.h"
#include "lib/oblog/ob_log.h"
#include "share/object/ob_obj_cast.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_physical_plan.h"
#include "lib/timezone/ob_timezone_info.h"
#include "sql/engine/expr/ob_expr_util.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

ObExprRegexp::ObExprRegexp(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_OP_REGEXP, N_REGEXP, 2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION),
      regexp_idx_(OB_COMPACT_INVALID_INDEX),
      pattern_is_const_(false),
      value_is_const_(false)
{
  need_charset_convert_ = false;
}

ObExprRegexp::~ObExprRegexp()
{
}

int ObExprRegexp::assign(const ObExprOperator &other)
{
  int ret = OB_SUCCESS;
  const ObExprRegexp *tmp_other = dynamic_cast<const ObExprRegexp *>(&other);
  if (OB_UNLIKELY(NULL == tmp_other)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument. wrong type for other", K(ret), K(other));
  } else if (OB_LIKELY(this != tmp_other)) {
    if (OB_FAIL(ObFuncExprOperator::assign(other))) {
      LOG_WARN("copy in Base class ObFuncExprOperator failed", K(ret));
    } else {
      this->regexp_idx_ = tmp_other->regexp_idx_;
      this->pattern_is_const_ = tmp_other->pattern_is_const_;
      this->value_is_const_ = tmp_other->value_is_const_;
    }
  }
  return ret;
}

int ObExprRegexp::calc_result_type2(ObExprResType &type,
                                    ObExprResType &type1,
                                    ObExprResType &type2,
                                    ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  ObRawExpr * raw_expr = type_ctx.get_raw_expr();
  ObCollationType res_cs_type = CS_TYPE_INVALID;
  ObCollationLevel res_cs_level = CS_LEVEL_INVALID;
  CK(NULL != type_ctx.get_raw_expr());
  if (type1.is_null() || type2.is_null()) {
    type.set_int32();
    type.set_precision(DEFAULT_PRECISION_FOR_BOOL);
    type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
  } else if (OB_UNLIKELY(!is_type_valid(type1.get_type()) || !is_type_valid(type2.get_type()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the param is not castable", K(ret), K(type1), K(type2));
  } else if ((ObExprRegexContext::is_binary_string(type1) || ObExprRegexContext::is_binary_string(type2))
              && (!ObExprRegexContext::is_binary_compatible(type1) || !ObExprRegexContext::is_binary_compatible(type2))) {
    const char *coll_name1 = ObCharset::collation_name(type1.get_collation_type());
    const char *coll_name2 = ObCharset::collation_name(type2.get_collation_type());
    ObString collation1 = ObString::make_string(coll_name1);
    ObString collation2 = ObString::make_string(coll_name2);
    ret = OB_ERR_MYSQL_CHARACTER_SET_MISMATCH;
    LOG_USER_ERROR(OB_ERR_MYSQL_CHARACTER_SET_MISMATCH, collation1.length(), collation1.ptr(), collation2.length(), collation2.ptr());
    LOG_WARN("If one of the params is binary string, all of the params should be implicitly castable to binary charset.", K(ret), K(type1), K(type2));
  } else if (OB_FAIL(ObCharset::aggregate_collation(type1.get_collation_level(),
                                              type1.get_collation_type(),
                                              type2.get_collation_level(),
                                              type2.get_collation_type(),
                                              res_cs_level,
                                              res_cs_type))) {
      LOG_WARN("fail to aggregate collation", K(ret), K(type1), K(type2));
  } else {
    type.set_int32();
    type.set_precision(DEFAULT_PRECISION_FOR_BOOL);
    type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
    //why we set the calc collation type is utf16, because the ICU regexp engine is used uft16,
    //we need convert it the need collation in advance, and no need to think about in regexp.
    bool is_case_sensitive = ObCharset::is_bin_sort(res_cs_type);
    bool need_utf8 = false;
    type1.set_calc_type(ObVarcharType);
    type1.set_calc_collation_level(type.get_collation_level());
    type2.set_calc_type(ObVarcharType);
    type2.set_calc_collation_level(type.get_collation_level());
    if (OB_FAIL(ObExprRegexContext::check_need_utf8(raw_expr->get_param_expr(1), need_utf8))) {
      LOG_WARN("fail to check need utf8", K(ret));
    } else if (need_utf8) {
      type2.set_calc_collation_type(is_case_sensitive ? CS_TYPE_UTF8MB4_BIN : CS_TYPE_UTF8MB4_GENERAL_CI);
    } else {
      type2.set_calc_collation_type(is_case_sensitive ? CS_TYPE_UTF16_BIN : CS_TYPE_UTF16_GENERAL_CI);
    }

    need_utf8 = false;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObExprRegexContext::check_need_utf8(raw_expr->get_param_expr(0), need_utf8))) {
      LOG_WARN("fail to check need utf8", K(ret));
    } else if (need_utf8) {
      type1.set_calc_collation_type(is_case_sensitive ? CS_TYPE_UTF8MB4_BIN : CS_TYPE_UTF8MB4_GENERAL_CI);
    } else {
      type1.set_calc_collation_type(is_case_sensitive ? CS_TYPE_UTF16_BIN : CS_TYPE_UTF16_GENERAL_CI);
    }
  }
  return ret;
}

int ObExprRegexp::need_fast_calc(ObExprCtx &expr_ctx, bool &result) const
{
  int ret = OB_SUCCESS;
  result = false;
  if (OB_ISNULL(expr_ctx.exec_ctx_)) {
    if (OB_UNLIKELY(false == expr_ctx.is_pre_calculation_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error. exec ctx should not be null in pre calculation", K(expr_ctx.exec_ctx_), K(expr_ctx.is_pre_calculation_));
    }
  } else {
    result = (true == pattern_is_const_ && false == value_is_const_ && get_id() != OB_INVALID_ID);
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObExprRegexp, ObFuncExprOperator),
                    regexp_idx_, pattern_is_const_, value_is_const_);


int ObExprRegexp::cg_expr(ObExprCGCtx &, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  const ObRawExpr *text = raw_expr.get_param_expr(0);
  const ObRawExpr *pattern = raw_expr.get_param_expr(1);
  CK(lib::is_mysql_mode()); // regexp is mysql only expr
  CK(2 == rt_expr.arg_cnt_);
  CK(NULL != text);
  CK(NULL != pattern);

  if (OB_SUCC(ret)) {
     const bool const_text = text->is_const_expr();
     const bool const_pattern = pattern->is_const_expr();
     rt_expr.extra_ = (!const_text && const_pattern) ? 1 : 0;
     rt_expr.eval_func_ = eval_regexp;
     LOG_DEBUG("regexp expr cg", K(const_text), K(const_pattern), K(rt_expr.extra_));
  }
  return ret;
}

int ObExprRegexp::eval_regexp(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *text = NULL;
  ObDatum *pattern = NULL;
  if (OB_FAIL(expr.eval_param_value(ctx, text, pattern))) {
    if (ret == OB_ERR_INCORRECT_STRING_VALUE) {//compatible mysql
      ret = OB_SUCCESS;
      expr_datum.set_null();
      const char *charset_name = ObCharset::charset_name(expr.args_[0]->datum_meta_.cs_type_);
      int64_t charset_name_len = strlen(charset_name);
      const char *tmp_char = NULL;
      LOG_USER_WARN(OB_ERR_INVALID_CHARACTER_STRING, static_cast<int>(charset_name_len), charset_name, 0, tmp_char);
    } else {
      LOG_WARN("evaluate parameters failed", K(ret));
    }
  } else if (text->is_null() || pattern->is_null()) {
    expr_datum.set_null();
  } else if (OB_UNLIKELY(expr.arg_cnt_ != 2 ||
                         (expr.args_[0]->datum_meta_.cs_type_ != CS_TYPE_UTF8MB4_GENERAL_CI &&
                           expr.args_[0]->datum_meta_.cs_type_ != CS_TYPE_UTF8MB4_BIN &&
                           expr.args_[0]->datum_meta_.cs_type_ != CS_TYPE_UTF16_GENERAL_CI &&
                           expr.args_[0]->datum_meta_.cs_type_ != CS_TYPE_UTF16_BIN) ||
                         (expr.args_[1]->datum_meta_.cs_type_ != CS_TYPE_UTF8MB4_GENERAL_CI &&
                          expr.args_[1]->datum_meta_.cs_type_ != CS_TYPE_UTF8MB4_BIN &&
                          expr.args_[1]->datum_meta_.cs_type_ != CS_TYPE_UTF16_GENERAL_CI &&
                          expr.args_[1]->datum_meta_.cs_type_ != CS_TYPE_UTF16_BIN))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(expr));
  } else if (0 == pattern->len_) {
    ret = OB_ERR_REGEXP_ERROR;
    LOG_WARN("empty regex expression", K(ret));
    expr_datum.set_null();
  } else {
    const bool reusable = (0 != expr.extra_)
        && ObExpr::INVALID_EXP_CTX_ID != expr.expr_ctx_id_;
    ObExprRegexContext local_regex_ctx;
    ObExprRegexContext *regex_ctx = &local_regex_ctx;
    if (reusable) {
      if (NULL == (regex_ctx = static_cast<ObExprRegexContext *>(
                  ctx.exec_ctx_.get_expr_op_ctx(expr.expr_ctx_id_)))) {
        if (OB_FAIL(ctx.exec_ctx_.create_expr_op_ctx(expr.expr_ctx_id_, regex_ctx))) {
          LOG_WARN("create expr regex context failed", K(ret), K(expr));
        } else if (OB_ISNULL(regex_ctx)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL context returned", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObEvalCtx::TempAllocGuard alloc_guard(ctx);
      ObIAllocator &tmp_alloc = alloc_guard.get_allocator();
      bool match = false;
      uint32_t flags = 0;
      ObString match_string;
      int64_t start_pos = 1;
      bool is_case_sensitive = ObCharset::is_bin_sort(expr.args_[0]->datum_meta_.cs_type_);
      ObString text_utf16;
      if (OB_FAIL(ObExprRegexContext::get_regexp_flags(match_string, is_case_sensitive, flags))) {
        LOG_WARN("failed to get regexp flags", K(ret));
      } else if (OB_FAIL(regex_ctx->init(reusable ? ctx.exec_ctx_.get_allocator() : tmp_alloc,
                                         ctx.exec_ctx_.get_my_session(),
                                         pattern->get_string(), flags, reusable, expr.args_[1]->datum_meta_.cs_type_))) {
        LOG_WARN("init regex context failed", K(ret), K(pattern->get_string()));
      } else if (expr.args_[0]->datum_meta_.cs_type_ == CS_TYPE_UTF8MB4_BIN ||
                 expr.args_[0]->datum_meta_.cs_type_ == CS_TYPE_UTF8MB4_GENERAL_CI) {
        if (OB_FAIL(ObExprUtil::convert_string_collation(text->get_string(),
                                                         expr.args_[0]->datum_meta_.cs_type_,
                                                         text_utf16,
                                                         is_case_sensitive ? CS_TYPE_UTF16_BIN : CS_TYPE_UTF16_GENERAL_CI,
                                                         tmp_alloc))) {
          LOG_WARN("convert charset failed", K(ret));
        }
      } else {
        text_utf16 = text->get_string();
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(regex_ctx->match(tmp_alloc, text_utf16, start_pos - 1, match))) {
        LOG_WARN("regex match failed", K(ret));
      } else {
        expr_datum.set_int32(match);
      }
    }
  }
  return ret;
}


}
}
