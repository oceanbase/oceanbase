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

using namespace oceanbase::common;

namespace oceanbase {
namespace sql {

ObExprRegexp::ObExprRegexp(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_OP_REGEXP, N_REGEXP, 2, NOT_ROW_DIMENSION),
      regexp_idx_(OB_COMPACT_INVALID_INDEX),
      pattern_is_const_(false),
      value_is_const_(false)
{
  need_charset_convert_ = false;
}

ObExprRegexp::~ObExprRegexp()
{}

int ObExprRegexp::assign(const ObExprOperator& other)
{
  int ret = OB_SUCCESS;
  const ObExprRegexp* tmp_other = dynamic_cast<const ObExprRegexp*>(&other);
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

int ObExprRegexp::calc_result_type2(
    ObExprResType& type, ObExprResType& type1, ObExprResType& type2, ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (type1.is_null() || type2.is_null()) {
    type.set_int32();
    type.set_precision(DEFAULT_PRECISION_FOR_BOOL);
    type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
  } else {
    if (OB_UNLIKELY(!is_type_valid(type1.get_type()) || !is_type_valid(type2.get_type()))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("the param is not castable", K(ret), K(type1), K(type2));
    } else {
      type.set_int32();
      type.set_precision(DEFAULT_PRECISION_FOR_BOOL);
      type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
      // set calc type
      type1.set_calc_type(ObVarcharType);
      type1.set_calc_collation_type(ObCharset::get_default_collation_oracle(CHARSET_UTF8MB4));
      type2.set_calc_type(ObVarcharType);
      type2.set_calc_collation_type(ObCharset::get_default_collation_oracle(CHARSET_UTF8MB4));
      ObObjMeta types[2] = {type1, type2};
      if (OB_FAIL(aggregate_charsets_for_comparison(type.get_calc_meta(), types, 2, type_ctx.get_coll_type()))) {
        LOG_WARN("fail to aggregate charsets for comparison", K(ret), K(type1), K(type2));
      } else {
        ObExprOperator::calc_result_flag2(type, type1, type2);
        type1.set_calc_collation(type);
        type2.set_calc_collation(type);
      }
    }
  }
  return ret;
}

int ObExprRegexp::calc_result2(ObObj& result, const ObObj& text, const ObObj& pattern, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("varchar buffer not init or exec ctx or physical plan ctx is NULL", K(expr_ctx.calc_buf_));
  } else if (text.is_null() || pattern.is_null()) {
    result.set_null();
  } else {
    if (!is_type_valid(text.get_type()) || !is_type_valid(pattern.get_type())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(text), K(pattern));
    } else {
      ObExprRegexContext posix_regexp_obj;
      ObExprRegexContext* posix_regexp = NULL;
      uint64_t expr_id = get_id();
      TYPE_CHECK(text, ObVarcharType);
      TYPE_CHECK(pattern, ObVarcharType);
      ObString text_val = text.get_string();
      ObString pattern_val = pattern.get_string();
      bool need_fast_calculation = false;
      if (OB_UNLIKELY(pattern_val.length() <= 0)) {
        ret = OB_ERR_REGEXP_ERROR;
        LOG_WARN("empty regex expression", K(ret));
      } else if (text_val.length() == 0) {
        result.set_bool(false);
      } else if (OB_FAIL(need_fast_calc(expr_ctx, need_fast_calculation))) {
        LOG_WARN("failed to need fast calc", K(ret));
      } else {
        if (need_fast_calculation) {
          // no need to test expr_ctx.exec_ctx_ is null or not again. PLEASE !!! PLEASE !!!
          if (NULL == (posix_regexp = static_cast<ObExprRegexContext*>(expr_ctx.exec_ctx_->get_expr_op_ctx(expr_id)))) {
            if (OB_FAIL(expr_ctx.exec_ctx_->create_expr_op_ctx(expr_id, posix_regexp))) {
              LOG_WARN("failed to create operator ctx", K(ret), K(expr_id));
            }
          }
        } else {
          posix_regexp = &posix_regexp_obj;
        }
        if (OB_FAIL(ret)) {
        } else {
          if (OB_ISNULL(posix_regexp)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("posix regexp ptr is NULL", K(ret));
          } else {
            const bool reusable = need_fast_calculation;
            int regex_lib_flags = (ObCharset::is_bin_sort(result_type_.get_calc_collation_type()))
                                      ? OB_REG_EXTENDED | OB_REG_NOSUB
                                      : OB_REG_EXTENDED | OB_REG_NOSUB | OB_REG_ICASE;
            if (OB_FAIL(posix_regexp->init(pattern_val,
                    regex_lib_flags,
                    reusable ? expr_ctx.exec_ctx_->get_allocator() : *expr_ctx.calc_buf_,
                    reusable))) {
              LOG_WARN("failed to initialize", K(pattern_val), K(ret));
            }
          }
          if (OB_FAIL(ret)) {
          } else {
            bool is_match = false;
            if (OB_FAIL(posix_regexp->match(text_val, 0, is_match, *expr_ctx.calc_buf_))) {
              LOG_WARN("match error", K(ret));
            } else {
              result.set_bool(is_match);
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObExprRegexp::need_fast_calc(ObExprCtx& expr_ctx, bool& result) const
{
  int ret = OB_SUCCESS;
  result = false;
  if (OB_ISNULL(expr_ctx.exec_ctx_)) {
    if (OB_UNLIKELY(false == expr_ctx.is_pre_calculation_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error. exec ctx should not be null in pre calculation",
          K(expr_ctx.exec_ctx_),
          K(expr_ctx.is_pre_calculation_));
    }
  } else {
    result = (true == pattern_is_const_ && false == value_is_const_ && get_id() != OB_INVALID_ID);
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObExprRegexp, ObFuncExprOperator), regexp_idx_, pattern_is_const_, value_is_const_);

int ObExprRegexp::cg_expr(ObExprCGCtx&, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  const ObRawExpr* text = raw_expr.get_param_expr(0);
  const ObRawExpr* pattern = raw_expr.get_param_expr(1);
  CK(lib::is_mysql_mode());  // regexp is mysql only expr
  CK(2 == rt_expr.arg_cnt_);
  CK(NULL != text);
  CK(NULL != pattern);

  if (OB_SUCC(ret)) {
    const bool const_text = text->has_flag(IS_CONST) || text->has_flag(IS_CONST_EXPR);
    const bool const_pattern = pattern->has_flag(IS_CONST) || pattern->has_flag(IS_CONST_EXPR);
    rt_expr.extra_ = (!const_text && const_pattern) ? 1 : 0;
    rt_expr.eval_func_ = eval_regexp;
    LOG_DEBUG("regexp expr cg", K(const_text), K(const_pattern), K(rt_expr.extra_));
  }
  return ret;
}

int ObExprRegexp::eval_regexp(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* text = NULL;
  ObDatum* pattern = NULL;
  if (OB_FAIL(expr.eval_param_value(ctx, text, pattern))) {
    LOG_WARN("evaluate parameters failed", K(ret));
  } else if (text->is_null() || pattern->is_null()) {
    expr_datum.set_null();
  } else if (0 == pattern->len_) {
    ret = OB_ERR_REGEXP_ERROR;
    LOG_WARN("empty regex expression", K(ret));
    expr_datum.set_null();
  } else {
    const bool reusable = (0 != expr.extra_) && ObExpr::INVALID_EXP_CTX_ID != expr.expr_ctx_id_;
    ObExprRegexContext local_regex_ctx;
    ObExprRegexContext* regex_ctx = &local_regex_ctx;
    if (reusable) {
      if (NULL == (regex_ctx = static_cast<ObExprRegexContext*>(ctx.exec_ctx_.get_expr_op_ctx(expr.expr_ctx_id_)))) {
        if (OB_FAIL(ctx.exec_ctx_.create_expr_op_ctx(expr.expr_ctx_id_, regex_ctx))) {
          LOG_WARN("create expr regex context failed", K(ret), K(expr));
        } else if (OB_ISNULL(regex_ctx)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL context returned", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObIAllocator& tmp_alloc = ctx.get_reset_tmp_alloc();
      const int64_t start_off = 0;
      bool match = false;
      int flags = ObCharset::is_bin_sort(expr.args_[0]->datum_meta_.cs_type_)
                      ? OB_REG_EXTENDED | OB_REG_NOSUB
                      : OB_REG_EXTENDED | OB_REG_NOSUB | OB_REG_ICASE;
      if (OB_FAIL(regex_ctx->init(
              pattern->get_string(), flags, reusable ? ctx.exec_ctx_.get_allocator() : tmp_alloc, reusable))) {
        LOG_WARN("init regex context failed", K(ret), K(pattern->get_string()));
      } else if (OB_FAIL(regex_ctx->match(text->get_string(), start_off, match, tmp_alloc))) {
        LOG_WARN("regex match failed", K(ret));
      } else {
        expr_datum.set_int32(match);
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
