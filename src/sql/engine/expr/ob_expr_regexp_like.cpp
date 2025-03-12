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
#include "sql/engine/expr/ob_expr_regexp_like.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

ObExprRegexpLike::ObExprRegexpLike(ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_SYS_REGEXP_LIKE, N_REGEXP_LIKE, MORE_THAN_ONE, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprRegexpLike::~ObExprRegexpLike()
{
}

int ObExprRegexpLike::calc_result_typeN(ObExprResType &type,
                                        ObExprResType *types,
                                        int64_t param_num,
                                        common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  ObRawExpr * raw_expr = type_ctx.get_raw_expr();
  CK(NULL != type_ctx.get_raw_expr());
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(param_num < 2 || param_num > 3)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("param number of regexp_replace at least 2 and at most 3", K(ret), K(param_num));
  } else {
    bool is_case_sensitive = ObCharset::is_bin_sort(types[0].get_collation_type());
    for (int i = 0; OB_SUCC(ret) && i < param_num; i++) {
      if (!types[i].is_null() && !is_type_valid_regexp(types[i].get_type())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("the parameter is not castable", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret) && is_mysql_mode()) {
      ObExprResType cmp_type;
      if (OB_FAIL(ObExprRegexContext::check_binary_compatible(types, 2))) {
        LOG_WARN("types are not compatible with binary.", K(ret));
      } else if (OB_FAIL(aggregate_charsets_for_comparison(cmp_type, types, 2, type_ctx))) {
        LOG_WARN("fail to aggregate charsets for comparison");
      } else {
        is_case_sensitive = ObCharset::is_bin_sort(cmp_type.get_calc_collation_type());
      }
    }
    if (OB_SUCC(ret)) {
      if (param_num == 3) {/*match type*/
        types[2].set_calc_type(ObVarcharType);
        types[2].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
        types[2].set_calc_collation_level(CS_LEVEL_IMPLICIT);
      }
      //we set the calc collation type to utf8 and convert it to utf16 in excution stage, because the ICU regexp engine is used uft16,
      //we need convert it the need collation in advance, and no need to think about in regexp.
      //lob TODO,jiangxiu.wt
      bool need_utf8 = false;
      bool is_use_hs = type_ctx.get_session()->get_enable_hyperscan_regexp_engine();
      types[1].set_calc_type(ObVarcharType);
      types[1].set_calc_collation_level(CS_LEVEL_IMPLICIT);
      if (!types[0].is_clob()) {
        types[0].set_calc_type(ObVarcharType);
      }
      types[0].set_calc_collation_level(CS_LEVEL_IMPLICIT);
      type.set_tinyint();
      type.set_precision(DEFAULT_PRECISION_FOR_BOOL);
      type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
      ObExprOperator::calc_result_flag2(type, types[0], types[1]);
      need_utf8 = false;
      if (OB_FAIL(ObExprRegexContext::check_need_utf8(raw_expr->get_param_expr(1), need_utf8))) {
        LOG_WARN("fail to check need utf8", K(ret));
      } else if (need_utf8 || is_use_hs) {
        types[1].set_calc_collation_type(is_case_sensitive ? CS_TYPE_UTF8MB4_BIN : CS_TYPE_UTF8MB4_GENERAL_CI);
      } else {
        types[1].set_calc_collation_type(is_case_sensitive ? CS_TYPE_UTF16_BIN : CS_TYPE_UTF16_GENERAL_CI);
      }
      need_utf8 = false;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObExprRegexContext::check_need_utf8(raw_expr->get_param_expr(0), need_utf8))) {
        LOG_WARN("fail to check need utf8", K(ret));
      } else if (need_utf8 || is_use_hs) {
        types[0].set_calc_collation_type(is_case_sensitive ? CS_TYPE_UTF8MB4_BIN : CS_TYPE_UTF8MB4_GENERAL_CI);
      } else {
        types[0].set_calc_collation_type(is_case_sensitive ? CS_TYPE_UTF16_BIN : CS_TYPE_UTF16_GENERAL_CI);
      }
    }
  }
  return ret;
}

int ObExprRegexpLike::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(op_cg_ctx);
  CK(2 == rt_expr.arg_cnt_ || 3 == rt_expr.arg_cnt_);
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
      const bool is_use_hs = op_cg_ctx.session_->get_enable_hyperscan_regexp_engine();
      rt_expr.eval_func_ = is_use_hs ? eval_hs_regexp_like : eval_regexp_like;
      LOG_DEBUG("regexp like expr cg", K(const_text), K(const_pattern), K(rt_expr.extra_));
    }
  }
  return ret;
}

int ObExprRegexpLike::is_valid_for_generated_column(const ObRawExpr*expr,
                                                    const common::ObIArray<ObRawExpr *> &exprs,
                                                    bool &is_valid) const {
  int ret = OB_SUCCESS;
  is_valid = true;
  return ret;
}

template<typename RegExpCtx>
int ObExprRegexpLike::regexp_like(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *text = NULL;
  ObDatum *pattern = NULL;
  ObDatum *match_type = NULL;
  if (OB_FAIL(expr.eval_param_value(ctx, text, pattern, match_type))) {
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
    ObString match_param = (NULL != match_type && !match_type->is_null()) ? match_type->get_string() : ObString();
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    ObIAllocator &tmp_alloc = alloc_guard.get_allocator();
    RegExpCtx local_regex_ctx;
    RegExpCtx *regexp_ctx = &local_regex_ctx;
    ObExprRegexpSessionVariables regexp_vars;
    uint32_t flags = 0;
    int64_t start_pos = 1;
    bool match = false;
    bool is_case_sensitive = ObCharset::is_bin_sort(expr.args_[0]->datum_meta_.cs_type_);
    const bool reusable = (0 != expr.extra_) && ObExpr::INVALID_EXP_CTX_ID != expr.expr_ctx_id_;
    ObString text_utf;
    ObString text_str;
    if (reusable) {
      if (NULL == (regexp_ctx = static_cast<RegExpCtx *>(
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
    } else if (OB_FAIL(RegExpCtx::get_regexp_flags(match_param, is_case_sensitive, false, true, flags))) {
      LOG_WARN("fail to get regexp flags", K(ret), K(match_param));
    } else if (OB_FAIL(ctx.exec_ctx_.get_my_session()->get_regexp_session_vars(regexp_vars))) {
      LOG_WARN("fail to get regexp");
    } else if (!pattern->is_null() &&
               OB_FAIL(regexp_ctx->init(reusable ? ctx.exec_ctx_.get_allocator() : tmp_alloc,
                                        regexp_vars,
                                        pattern->get_string(), flags, reusable,
                                        expr.args_[1]->datum_meta_.cs_type_))) {
      LOG_WARN("fail to init regexp", K(pattern), K(flags), K(ret));
    //need pre check the pattern valid, and then set result.
    } else if (ob_is_text_tc(expr.args_[0]->datum_meta_.type_)) {
      if (OB_FAIL(ObTextStringHelper::get_string(expr, tmp_alloc, 0, text, text_str))) {
        LOG_WARN("get text string failed", K(ret));
      }
    } else {
      text_str = text->get_string();
    }
    if (OB_FAIL(ret)) {
    } else if (text->is_null() || pattern->is_null() ||
               (lib::is_mysql_mode() && NULL != match_type && match_type->is_null())) {
      expr_datum.set_null();
    } else {
      const ObCollationType constexpr expected_bin_coll =
        std::is_same<RegExpCtx, ObExprRegexContext>::value ? CS_TYPE_UTF16_BIN :
                                                             CS_TYPE_UTF8MB4_BIN;
      const ObCollationType constexpr expected_ci_coll =
        std::is_same<RegExpCtx, ObExprRegexContext>::value ? CS_TYPE_UTF16_GENERAL_CI :
                                                             CS_TYPE_UTF8MB4_GENERAL_CI;
      ObCollationType res_coll_type = ObCharset::is_bin_sort(expr.args_[0]->datum_meta_.cs_type_) ?
                                        expected_bin_coll :
                                        expected_ci_coll;
      if (expr.args_[0]->datum_meta_.cs_type_ != expected_bin_coll
          && expr.args_[0]->datum_meta_.cs_type_ != expected_ci_coll) {
        if (OB_FAIL(ObExprUtil::convert_string_collation(
              text_str, expr.args_[0]->datum_meta_.cs_type_, text_utf, res_coll_type, tmp_alloc))) {
          LOG_WARN("convert charset failed", K(ret));
        }
      } else {
        text_utf = text_str;
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(regexp_ctx->match(tmp_alloc, text_utf, res_coll_type, start_pos - 1, match))) {
        LOG_WARN("fail to match", K(ret), K(text));
      } else {
        expr_datum.set_int32(match);
      }
    }
  }
  return ret;
}

int ObExprRegexpLike::eval_regexp_like(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  return regexp_like<ObExprRegexContext>(expr, ctx, expr_datum);
}

int ObExprRegexpLike::eval_hs_regexp_like(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
#if defined(__x86_64__)
  return regexp_like<ObExprHsRegexCtx>(expr, ctx, expr_datum);
#else
  return OB_NOT_IMPLEMENT;
#endif
}

}
}
