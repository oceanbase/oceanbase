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
#include "sql/engine/expr/ob_expr_regexp_count.h"
#include "lib/oblog/ob_log.h"
#include "lib/number/ob_number_v2.h"
#include "share/object/ob_obj_cast.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

ObExprRegexpCount::ObExprRegexpCount(ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_SYS_REGEXP_COUNT, N_REGEXP_COUNT, MORE_THAN_ONE, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprRegexpCount::~ObExprRegexpCount()
{
}

int ObExprRegexpCount::calc_result_typeN(ObExprResType &type,
                                         ObExprResType *types,
                                         int64_t param_num,
                                         common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  ObRawExpr * raw_expr = type_ctx.get_raw_expr();
  CK(NULL != type_ctx.get_raw_expr());
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(param_num < 2 || param_num > 4)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("param number of regexp_count at least 2 and at most 4", K(ret), K(param_num));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < param_num; i++) {
      if (!types[i].is_null() && !is_type_valid(types[i].get_type())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("the parameter is not castable", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret)) {
      bool is_case_sensitive = ObCharset::is_bin_sort(types[0].get_calc_collation_type());
      bool need_utf8 = false;
      switch (param_num) {
        case 4/*match type*/:
          types[3].set_calc_type(ObVarcharType);
          types[3].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
          types[3].set_calc_collation_level(CS_LEVEL_IMPLICIT);
        case 3/*postition*/:
          types[2].set_calc_type(ObNumberType);
          types[2].set_scale(NUMBER_SCALE_UNKNOWN_YET);
          types[2].set_precision(PRECISION_UNKNOWN_YET);
        case 2/*pattern and text*/:
          //why we set the calc collation type is utf16, because the ICU regexp engine is used uft16,
          //we need convert it the need collation in advance, and no need to think about in regexp.
          //lob TODO,jiangxiu.wt
          types[1].set_calc_type(ObVarcharType);
          types[1].set_calc_collation_level(CS_LEVEL_IMPLICIT);
          if (!types[0].is_clob()) {
            types[0].set_calc_type(ObVarcharType);
          }
          types[0].set_calc_collation_level(CS_LEVEL_IMPLICIT);
          need_utf8 = false;
          if (OB_FAIL(ObExprRegexContext::check_need_utf8(raw_expr->get_param_expr(1), need_utf8))) {
            LOG_WARN("fail to check need utf8", K(ret));
          } else if (need_utf8) {
            types[1].set_calc_collation_type(is_case_sensitive ? CS_TYPE_UTF8MB4_BIN : CS_TYPE_UTF8MB4_GENERAL_CI);
          } else {
            types[1].set_calc_collation_type(is_case_sensitive ? CS_TYPE_UTF16_BIN : CS_TYPE_UTF16_GENERAL_CI);
          }
          need_utf8 = false;
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(ObExprRegexContext::check_need_utf8(raw_expr->get_param_expr(0), need_utf8))) {
            LOG_WARN("fail to check need utf8", K(ret));
          } else if (need_utf8) {
            types[0].set_calc_collation_type(is_case_sensitive ? CS_TYPE_UTF8MB4_BIN : CS_TYPE_UTF8MB4_GENERAL_CI);
          } else {
            types[0].set_calc_collation_type(is_case_sensitive ? CS_TYPE_UTF16_BIN : CS_TYPE_UTF16_GENERAL_CI);
          }
        default:
          // already check before
          break;
      }
      type.set_number();
      type.set_scale(NUMBER_SCALE_UNKNOWN_YET);
      type.set_precision(PRECISION_UNKNOWN_YET);
    }
  }
  return ret;
}

int ObExprRegexpCount::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  // regex_count is oracle only expr
  UNUSED(op_cg_ctx);
  CK(lib::is_oracle_mode());
  CK(2 <= rt_expr.arg_cnt_ && rt_expr.arg_cnt_ <= 4);
  CK(raw_expr.get_param_count() >= 2);
  if (OB_SUCC(ret)) {
    const ObRawExpr *text = raw_expr.get_param_expr(0);
    const ObRawExpr *pattern = raw_expr.get_param_expr(1);
    if (OB_ISNULL(text) || OB_ISNULL(pattern)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(text), K(pattern), K(ret));
    } else {
      const bool const_text = text->is_const_expr();
      const bool const_pattern = pattern->is_const_expr();
      rt_expr.extra_ = (!const_text && const_pattern) ? 1 : 0;
      rt_expr.eval_func_ = &eval_regexp_count;
      LOG_TRACE("regexp count expr cg", K(const_text), K(const_pattern), K(rt_expr.extra_));
    }
  }
  return ret;
}

int ObExprRegexpCount::eval_regexp_count(
    const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *text = NULL;
  ObDatum *pattern = NULL;
  ObDatum *position = NULL;
  ObDatum *match_type = NULL;
  if (OB_FAIL(expr.eval_param_value(ctx, text, pattern, position, match_type))) {
    LOG_WARN("evaluate parameters failed", K(ret));
  } else if (OB_UNLIKELY(expr.arg_cnt_ < 2 ||
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
  } else {
    int64_t pos = 1;
    if (position != NULL && position->is_null()) {
      expr_datum.set_null();
    } else if (OB_FAIL(ObExprUtil::get_int_param_val(
                 position, expr.arg_cnt_ > 2 && expr.args_[2]->obj_meta_.is_decimal_int(), pos))) {
      LOG_WARN("truncate number to int64 failed", K(ret));
    } else if (pos <= 0) {
      ret = OB_ERR_ARGUMENT_OUT_OF_RANGE;
      LOG_WARN("position out of range", K(ret), K(position), K(pos));
    } else {
      ObString match_param = (NULL != match_type && !match_type->is_null()) ? match_type->get_string() : ObString();
      ObEvalCtx::TempAllocGuard alloc_guard(ctx);
      ObIAllocator &tmp_alloc = alloc_guard.get_allocator();
      int64_t res_count = 0;
      ObExprRegexContext local_regex_ctx;
      ObExprRegexContext *regexp_ctx = &local_regex_ctx;
      ObExprRegexpSessionVariables regexp_vars;
      const bool reusable = (0 != expr.extra_) && ObExpr::INVALID_EXP_CTX_ID != expr.expr_ctx_id_;
      bool is_case_sensitive = ObCharset::is_bin_sort(expr.args_[0]->datum_meta_.cs_type_);
      uint32_t flags = 0;
      ObString text_utf16;
      ObString text_str;
      bool is_null = true;
      if (reusable) {
        if (NULL == (regexp_ctx = static_cast<ObExprRegexContext *>(
                    ctx.exec_ctx_.get_expr_op_ctx(expr.expr_ctx_id_)))) {
          if (OB_FAIL(ctx.exec_ctx_.create_expr_op_ctx(expr.expr_ctx_id_, regexp_ctx))) {
            LOG_WARN("create expr regex context failed", K(ret), K(expr));
          } else if (OB_ISNULL(regexp_ctx)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("NULL context returned", K(ret));
          }
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObExprRegexContext::get_regexp_flags(match_param, is_case_sensitive, flags))) {
        LOG_WARN("fail to get regexp flags", K(ret), K(match_param));
      } else if (OB_FAIL(ctx.exec_ctx_.get_my_session()->get_regexp_session_vars(regexp_vars))) {
        LOG_WARN("fail to get regexp");
      } else if (!pattern->is_null() &&
                 OB_FAIL(regexp_ctx->init(reusable ? ctx.exec_ctx_.get_allocator() : tmp_alloc,
                                          regexp_vars,
                                          pattern->get_string(), flags, reusable, expr.args_[1]->datum_meta_.cs_type_))) {
        LOG_WARN("fail to init regexp", K(pattern), K(flags), K(ret));
      } else if (ob_is_text_tc(expr.args_[0]->datum_meta_.type_)) {
        if (OB_FAIL(ObTextStringHelper::get_string(expr, tmp_alloc, 0, text, text_str))) {
          LOG_WARN("get text string failed", K(ret));
        }
      } else {
        text_str = text->get_string();
      }

      if (OB_FAIL(ret)) {
      } else if (text->is_null() || pattern->is_null() ||
                 (position != NULL && position->is_null())) {
        expr_datum.set_null();
      } else {
        is_null = false;
        if (expr.args_[0]->datum_meta_.cs_type_ == CS_TYPE_UTF8MB4_BIN ||
            expr.args_[0]->datum_meta_.cs_type_ == CS_TYPE_UTF8MB4_GENERAL_CI) {
          if (OB_FAIL(ObExprUtil::convert_string_collation(text_str, expr.args_[0]->datum_meta_.cs_type_, text_utf16,
                                ObCharset::is_bin_sort(expr.args_[0]->datum_meta_.cs_type_) ? CS_TYPE_UTF16_BIN : CS_TYPE_UTF16_GENERAL_CI,
                                tmp_alloc))) {
            LOG_WARN("convert charset failed", K(ret));
          }
        } else {
          text_utf16 = text_str;
        }
      }
      if (OB_FAIL(ret) || is_null) {
      } else if (OB_FAIL(regexp_ctx->count(tmp_alloc, text_utf16, pos - 1, res_count))) {
        LOG_WARN("failed to regexp count", K(ret));
      } else {
        number::ObNumber nmb;
        ObNumStackOnceAlloc nmb_alloc;
        if (OB_FAIL(nmb.from(res_count, nmb_alloc))) {
          LOG_WARN("set integer to number failed", K(ret));
        } else {
          expr_datum.set_number(nmb);
        }
      }
    }
  }
  return ret;
}

}
}
