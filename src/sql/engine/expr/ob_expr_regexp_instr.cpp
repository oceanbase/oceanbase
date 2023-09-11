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
#include "sql/engine/expr/ob_expr_regexp_instr.h"
#include "lib/oblog/ob_log.h"
#include "share/object/ob_obj_cast.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/expr/ob_expr_regexp_count.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

ObExprRegexpInstr::ObExprRegexpInstr(ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_SYS_REGEXP_INSTR, N_REGEXP_INSTR, MORE_THAN_ONE, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprRegexpInstr::~ObExprRegexpInstr()
{
}

int ObExprRegexpInstr::calc_result_typeN(ObExprResType &type,
                                         ObExprResType *types,
                                         int64_t param_num,
                                         common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  ObRawExpr * raw_expr = type_ctx.get_raw_expr();
  CK(NULL != type_ctx.get_raw_expr());
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(param_num < 2 || param_num > 7)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("param number of regexp_instr at least 2 and at most 7", K(ret), K(param_num));
  } else {
    bool is_case_sensitive = types[0].get_calc_collation_type();
    for (int i = 0; OB_SUCC(ret) && i < param_num; i++) {
      if (!types[i].is_null() && !is_type_valid(types[i].get_type())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("the parameter is not castable", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret) && is_mysql_mode()) {
      ObExprResType cmp_type;
      if (OB_FAIL(ObExprRegexContext::check_binary_compatible(types, 2))) {
        LOG_WARN("types are not compatible with binary.", K(ret));
      } else if (OB_FAIL(aggregate_charsets_for_comparison(cmp_type, types, 2, type_ctx.get_coll_type()))) {
        LOG_WARN("fail to aggregate charsets for comparison");
      } else {
        is_case_sensitive = ObCharset::is_bin_sort(cmp_type.get_calc_collation_type());
      }
    }
    if (OB_SUCC(ret)) {
      bool need_utf8 = false;
      switch (param_num) {
        case 7/*subexpr*/:
          if (lib::is_oracle_mode()) {
            types[6].set_calc_type(ObNumberType);
          } else {//mysql8.0 only has 6 arguments
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid match param", K(ret));
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, "too many arguments in regexp_instr");
          }
        case 6/*match type*/:
          types[5].set_calc_type(ObVarcharType);
          types[5].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
          types[5].set_calc_collation_level(CS_LEVEL_IMPLICIT);
	      case 5/*return opt*/:
          types[4].set_calc_type(lib::is_oracle_mode() ? ObNumberType : ObIntType);
          types[4].set_scale(lib::is_oracle_mode() ? NUMBER_SCALE_UNKNOWN_YET : DEFAULT_SCALE_FOR_INTEGER);
          types[4].set_precision(lib::is_oracle_mode() ? PRECISION_UNKNOWN_YET :
                                            ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].precision_);
        case 4/*occurrence*/:
          types[3].set_calc_type(lib::is_oracle_mode() ? ObNumberType : ObIntType);
          types[3].set_scale(lib::is_oracle_mode() ? NUMBER_SCALE_UNKNOWN_YET : DEFAULT_SCALE_FOR_INTEGER);
          types[3].set_precision(lib::is_oracle_mode() ? PRECISION_UNKNOWN_YET :
                                            ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].precision_);
        case 3/*position*/:
          types[2].set_calc_type(lib::is_oracle_mode() ? ObNumberType : ObIntType);
          types[2].set_scale(lib::is_oracle_mode() ? NUMBER_SCALE_UNKNOWN_YET : DEFAULT_SCALE_FOR_INTEGER);
          types[2].set_precision(lib::is_oracle_mode() ? PRECISION_UNKNOWN_YET :
                                            ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].precision_);
          type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_STRING_INTEGER_TRUNC);
        case 2/*text and pattern*/:{
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
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(ObExprRegexContext::check_need_utf8(raw_expr->get_param_expr(1), need_utf8))) {
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
        }
        default:
          // already check before
          break;
      }
    }
    if (OB_SUCC(ret)) {
      if (lib::is_oracle_mode()) {//set default value
        type.set_number();
        type.set_scale(NUMBER_SCALE_UNKNOWN_YET);
        type.set_precision(PRECISION_UNKNOWN_YET);
      } else {
        type.set_int();
        type.set_scale(common::DEFAULT_SCALE_FOR_INTEGER);
        type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].precision_);
      }
    }
  }
  return ret;
}

int ObExprRegexpInstr::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(op_cg_ctx);
  CK(2 <= rt_expr.arg_cnt_ && rt_expr.arg_cnt_ <= 7);
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
      rt_expr.eval_func_ = &eval_regexp_instr;
      LOG_DEBUG("regexp instr expr cg", K(const_text), K(const_pattern), K(rt_expr.extra_));
    }
  }
  return ret;
}

int ObExprRegexpInstr::eval_regexp_instr(
    const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *text = NULL;
  ObDatum *pattern = NULL;
  ObDatum *position = NULL;
  ObDatum *occurrence = NULL;
  ObDatum *return_opt = NULL;
  ObDatum *match_type = NULL;
  ObDatum *subexpr= NULL;
  if (OB_FAIL(expr.eval_param_value(
              ctx, text, pattern, position, occurrence, return_opt, match_type, subexpr))) {
    if (lib::is_mysql_mode() && ret == OB_ERR_INCORRECT_STRING_VALUE) {//compatible mysql
      ret = OB_SUCCESS;
      expr_datum.set_null();
      const char *charset_name = ObCharset::charset_name(expr.args_[0]->datum_meta_.cs_type_);
      int64_t charset_name_len = strlen(charset_name);
      const char *tmp_char = NULL;
      LOG_USER_WARN(OB_ERR_INVALID_CHARACTER_STRING, static_cast<int>(charset_name_len), charset_name, 0, tmp_char);
    } else {
      LOG_WARN("evaluate parameters failed", K(ret));
    }
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
  } else if (lib::is_mysql_mode() && !pattern->is_null() && pattern->get_string().empty()) {
    if (NULL == match_type || !match_type->is_null()) {
      ret = OB_ERR_REGEXP_ERROR;
      LOG_WARN("empty regex expression", K(ret));
    } else {
      expr_datum.set_null();
    }
  } else {
    int64_t pos = 1;
    int64_t occur = 1;
    int64_t return_opt_val = 0;
    int64_t subexpr_val = 0;
    bool null_result = lib::is_mysql_mode() && ((position != NULL && position->is_null()) ||
                                                (occurrence != NULL && occurrence->is_null()) ||
                                                (return_opt != NULL && return_opt->is_null()) ||
                                                (match_type != NULL && match_type->is_null()));
    if (OB_FAIL(ObExprUtil::get_int_param_val(position, pos))
        || OB_FAIL(ObExprUtil::get_int_param_val(occurrence, occur))
        || OB_FAIL(ObExprUtil::get_int_param_val(return_opt, return_opt_val))
        || OB_FAIL(ObExprUtil::get_int_param_val(subexpr, subexpr_val))) {
      LOG_WARN("get integer parameter value failed", K(ret));
    } else if (!null_result &&
               (pos <= 0 || occur < 0 || subexpr_val < 0
                || (lib::is_mysql_mode() && (return_opt_val < 0 || return_opt_val > 1))
                || (lib::is_oracle_mode() && occurrence != NULL && occur ==0))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("regexp_instr position or occurrence or return_option or subexpr is invalid",
               K(ret), K(pos), K(occur), K(subexpr_val));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "use position or occurrence or return_option or subexpr in regexp_instr");
    } else if (lib::is_oracle_mode() && return_opt_val < 0) {
      ret = OB_ERR_ARGUMENT_OUT_OF_RANGE;
      LOG_WARN("regexp_instr return_option is invalid", K(ret), K(return_opt_val));
    } else {
      ObString match_param = (NULL != match_type && !match_type->is_null()) ? match_type->get_string() : ObString();
      int64_t res_pos = 0;
      ObEvalCtx::TempAllocGuard alloc_guard(ctx);
      ObIAllocator &tmp_alloc = alloc_guard.get_allocator();
      ObExprRegexContext local_regex_ctx;
      ObExprRegexContext *regexp_ctx = &local_regex_ctx;
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
      } else if (!pattern->is_null() && !null_result &&
                 OB_FAIL(regexp_ctx->init(reusable ? ctx.exec_ctx_.get_allocator() : tmp_alloc,
                                          ctx.exec_ctx_.get_my_session(),
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
      } else if (text->is_null() || pattern->is_null() || null_result ||
                 (NULL != position && position->is_null()) ||
                 (NULL != occurrence && occurrence->is_null()) ||
                 (NULL != subexpr && subexpr->is_null()) ||
                 (NULL != return_opt && return_opt->is_null()) ||
                 (lib::is_mysql_mode() && NULL != match_type && match_type->is_null())) {
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
      } else if (OB_FAIL(regexp_ctx->find(tmp_alloc, text_utf16, pos - 1, occur,
                                          return_opt_val, subexpr_val, res_pos))) {
        LOG_WARN("failed to regexp find loc", K(ret));
      } else if (lib::is_mysql_mode()) {
        expr_datum.set_int(res_pos);
      } else {
        number::ObNumber nmb;
        ObNumStackOnceAlloc nmb_alloc;
        if (OB_FAIL(nmb.from(res_pos, nmb_alloc))) {
          LOG_WARN("set integer to number failed", K(ret));
        } else {
          expr_datum.set_number(nmb);
        }
      }
    }
  }
  return ret;
}

int ObExprRegexpInstr::is_valid_for_generated_column(const ObRawExpr*expr,
                                                     const common::ObIArray<ObRawExpr *> &exprs,
                                                     bool &is_valid) const {
  int ret = OB_SUCCESS;
  is_valid = lib::is_mysql_mode();
  return ret;
}

}
}
