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
#include "sql/engine/expr/ob_expr_regexp_substr.h"
#include "lib/oblog/ob_log.h"
#include "share/object/ob_obj_cast.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/engine/expr/ob_expr_regexp_count.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{
//修改了之前定义的T_OP_REGEXP_SUBSTR为T_FUN_SYS_REGEXP_SUBSTR，因为之前T_OP_REGEXP_SUBSTR的注册值仅在mysql当中
ObExprRegexpSubstr::ObExprRegexpSubstr(ObIAllocator &alloc)
  : ObStringExprOperator(alloc, T_FUN_SYS_REGEXP_SUBSTR, N_REGEXP_SUBSTR, MORE_THAN_ONE, NOT_VALID_FOR_GENERATED_COL)
{
}

ObExprRegexpSubstr::~ObExprRegexpSubstr()
{
}

int ObExprRegexpSubstr::calc_result_typeN(ObExprResType &type,
                                          ObExprResType *types,
                                          int64_t param_num,
                                          common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  ObRawExpr * raw_expr = type_ctx.get_raw_expr();
  const ObRawExpr * real_text = NULL;
  const ObRawExpr * real_pattern = NULL;
  CK(NULL != type_ctx.get_raw_expr());
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(param_num < 2 || param_num > 6)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("param number of regexp_substr at least 2 and at most 6", K(ret), K(param_num));
  } else if (OB_FAIL(ObRawExprUtils::get_real_expr_without_cast(raw_expr->get_param_expr(0), real_text))) {
    LOG_WARN("fail to get real expr without cast", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::get_real_expr_without_cast(raw_expr->get_param_expr(1), real_pattern))) {
    LOG_WARN("fail to get real expr without cast", K(ret));
  } else if (OB_ISNULL(real_text) || OB_ISNULL(real_pattern)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("real expr is invalid", K(ret), K(real_text), K(real_pattern));
  } else {
    const ObExprResType &text = real_text->get_result_type();
    const ObExprResType &pattern = real_pattern->get_result_type();
    bool is_case_sensitive = false;
    for (int i = 0; OB_SUCC(ret) && i < param_num; i++) {
      if (!types[i].is_null() && !is_type_valid(types[i].get_type())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("the parameter is not castable", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret)) {
      //need duduce result type, then reset calc type.
      if (lib::is_oracle_mode()) {
        // set max length.
        type.set_length(static_cast<common::ObLength>(text.get_length()));
        auto str_params = make_const_carray(const_cast<ObExprResType*>(&text));
        is_case_sensitive = ObCharset::is_bin_sort(types[0].get_collation_type());
        OZ(aggregate_string_type_and_charset_oracle(*type_ctx.get_session(),
                                                    str_params,
                                                    type,
                                                    PREFER_VAR_LEN_CHAR));
        OZ(deduce_string_param_calc_type_and_charset(*type_ctx.get_session(), type, str_params));
        OX(type.set_length_semantics(type_ctx.get_session()->get_actual_nls_length_semantics()));
      } else {
        const common::ObLengthSemantics default_length_semantics = (OB_NOT_NULL(type_ctx.get_session())
                ? type_ctx.get_session()->get_actual_nls_length_semantics()
                : common::LS_BYTE);
        ObObjMeta real_types[2] = {text, pattern};
        if (text.is_blob()) {
          type.set_blob();
        } else {
          type.set_varchar();
          type.set_length_semantics(text.is_varchar_or_char() ? text.get_length_semantics() : default_length_semantics);
        }
        type.set_length(text.get_length());
        if (OB_FAIL(ObExprRegexContext::check_binary_compatible(types, 2))) {
          LOG_WARN("types are not compatible with binary.", K(ret));
        } else {
          ret = aggregate_charsets_for_string_result(type, real_types, 2, type_ctx.get_coll_type());
          is_case_sensitive = ObCharset::is_bin_sort(type.get_collation_type());
        }
      }
      if (OB_SUCC(ret)) {
        bool need_utf8 = false;
        switch (param_num) {
          case 6/*subexpr*/:
            types[5].set_calc_type(lib::is_oracle_mode() ? ObNumberType : ObIntType);
            types[5].set_scale(lib::is_oracle_mode() ? NUMBER_SCALE_UNKNOWN_YET : DEFAULT_SCALE_FOR_INTEGER);
            types[5].set_precision(lib::is_oracle_mode() ? PRECISION_UNKNOWN_YET :
                                            ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].precision_);
          case 5/*match type*/:
            types[4].set_calc_type(ObVarcharType);
            types[4].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
            types[4].set_calc_collation_level(CS_LEVEL_IMPLICIT);
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
          case 2/*text and pattern*/:
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
          default:
            // already check before
            break;
        }
      }
    }
  }
  return ret;
}

int ObExprRegexpSubstr::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(op_cg_ctx);
  CK(2 <= rt_expr.arg_cnt_ && rt_expr.arg_cnt_ <= 6);
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
      rt_expr.eval_func_ = &eval_regexp_substr;
      LOG_DEBUG("regexp expr cg", K(const_text), K(const_pattern), K(rt_expr.extra_));
    }
  }
  return ret;
}

int ObExprRegexpSubstr::eval_regexp_substr(
    const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *text = NULL;
  ObDatum *pattern = NULL;
  ObDatum *position = NULL;
  ObDatum *occurrence = NULL;
  ObDatum *match_type = NULL;
  ObDatum *subexpr= NULL;
  int64_t pos = 1;
  int64_t occur = 1;
  int64_t subexpr_val = 0;
  if (OB_FAIL(expr.eval_param_value(ctx, text, pattern, position, occurrence, match_type, subexpr))) {
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
    bool null_result = (position != NULL && position->is_null()) ||
                       (occurrence != NULL && occurrence->is_null()) ||
                       (subexpr != NULL && subexpr->is_null()) ||
                       (lib::is_mysql_mode() && match_type != NULL && match_type->is_null());
    if (OB_FAIL(ObExprUtil::get_int_param_val(
          position, (expr.arg_cnt_ > 2 && expr.args_[2]->obj_meta_.is_decimal_int()), pos))
        || OB_FAIL(ObExprUtil::get_int_param_val(
          occurrence, (expr.arg_cnt_ > 3 && expr.args_[3]->obj_meta_.is_decimal_int()), occur))
        || OB_FAIL(ObExprUtil::get_int_param_val(
          subexpr, expr.arg_cnt_ > 5 && expr.args_[5]->obj_meta_.is_decimal_int(), subexpr_val))) {
      LOG_WARN("get integer parameter value failed", K(ret));
    } else if (!null_result && (pos <= 0 || occur <= 0 || subexpr_val < 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("regexp_substr position or occurrence or subexpr is invalid",
                K(ret), K(pos), K(occur), K(subexpr_val));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "use position or occurrence or subexpr in regexp_substr");
    } else {
      ObString match_param = (NULL != match_type && !match_type->is_null()) ? match_type->get_string() : ObString();
      ObEvalCtx::TempAllocGuard alloc_guard(ctx);
      ObIAllocator &tmp_alloc = alloc_guard.get_allocator();
      ObExprRegexContext local_regex_ctx;
      ObExprRegexContext *regexp_ctx = &local_regex_ctx;
      ObExprRegexpSessionVariables regexp_vars;
      const bool reusable = (0 != expr.extra_) && ObExpr::INVALID_EXP_CTX_ID != expr.expr_ctx_id_;
      bool is_case_sensitive = ObCharset::is_bin_sort(expr.args_[0]->datum_meta_.cs_type_);
      ObString res_substr;
      ObString text_utf16;
      ObString text_str;
      ObCollationType res_coll_type = CS_TYPE_INVALID;
      bool is_null = true;
      bool is_final = false;
      uint32_t flags = 0;
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
      } else if (!pattern->is_null() && !null_result &&
                OB_FAIL(regexp_ctx->init(reusable ? ctx.exec_ctx_.get_allocator() : tmp_alloc,
                                          regexp_vars,
                                          pattern->get_string(), flags, reusable,
                                          expr.args_[1]->datum_meta_.cs_type_))) {
        LOG_WARN("fail to init regexp", K(pattern), K(flags), K(ret));
      } else if (text->is_null() || pattern->is_null() || null_result ||
                (NULL != position && position->is_null()) ||
                (NULL != occurrence && occurrence->is_null()) ||
                (NULL != subexpr && subexpr->is_null()) ||
                (lib::is_mysql_mode() && NULL != match_type && match_type->is_null())) {
        expr_datum.set_null();
        is_final = true;
      } else if (ob_is_text_tc(expr.args_[0]->datum_meta_.type_)) {
        if (OB_FAIL(ObTextStringHelper::get_string(expr, tmp_alloc, 0, text, text_str))) {
          LOG_WARN("get text string failed", K(ret));
        }
      } else {
        text_str = text->get_string();
      }
      if (OB_FAIL(ret) || is_final) {
      } else {
        is_null = false;
        if (expr.args_[0]->datum_meta_.cs_type_ == CS_TYPE_UTF8MB4_BIN ||
            expr.args_[0]->datum_meta_.cs_type_ == CS_TYPE_UTF8MB4_GENERAL_CI) {
          res_coll_type = ObCharset::is_bin_sort(expr.args_[0]->datum_meta_.cs_type_) ? CS_TYPE_UTF16_BIN : CS_TYPE_UTF16_GENERAL_CI;
          if (OB_FAIL(ObExprUtil::convert_string_collation(text_str, expr.args_[0]->datum_meta_.cs_type_, text_utf16,
                                                      res_coll_type, tmp_alloc))) {
            LOG_WARN("convert charset failed", K(ret));
          }
        } else {
          res_coll_type = expr.args_[0]->datum_meta_.cs_type_;
          text_utf16 = text_str;
        }
      }
      if (OB_FAIL(ret) || is_null || is_final) {
      } else if (OB_FAIL(regexp_ctx->substr(tmp_alloc, text_utf16, pos - 1,
                                            occur, subexpr_val, res_substr))) {
        LOG_WARN("failed to regexp substr", K(ret));
      } else if (res_substr.empty() && lib::is_oracle_mode() && expr.args_[0]->datum_meta_.is_clob()) {
        // emptyp clob
        ObTextStringDatumResult empty_lob_result(expr.datum_meta_.type_, &expr, &ctx, &expr_datum);
        if (OB_FAIL(empty_lob_result.init(0))) {
          LOG_WARN("init empty lob result failed");
        } else {
          empty_lob_result.set_result();
        }
      } else if (res_substr.empty()) {
        expr_datum.set_null();
      } else {
        ObExprStrResAlloc out_alloc(expr, ctx);
        ObString out;
        if (!ob_is_text_tc(expr.datum_meta_.type_)) {
          if (OB_FAIL(ObExprUtil::convert_string_collation(res_substr, res_coll_type,
                                                           out, expr.datum_meta_.cs_type_, out_alloc))) {
            LOG_WARN("convert charset failed", K(ret));
          } else if (out.ptr() == res_substr.ptr()) {
            // res_substr is allocated in temporary allocator, deep copy here.
            char *mem = expr.get_str_res_mem(ctx, res_substr.length());
            if (OB_ISNULL(mem)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("allocate memory failed", K(ret));
            } else {
              MEMCPY(mem, res_substr.ptr(), res_substr.length());
              expr_datum.set_string(mem, res_substr.length());
            }
          } else {
            expr_datum.set_string(out.ptr(), out.length());
          }
        } else { // output is text type
          ObTextStringDatumResult text_res(expr.datum_meta_.type_, &expr, &ctx, &expr_datum);
          if (OB_FAIL(ObExprUtil::convert_string_collation(res_substr, res_coll_type,
                                                           out, expr.datum_meta_.cs_type_, tmp_alloc))) {
            LOG_WARN("convert charset failed", K(ret));
          } else if (OB_FAIL(text_res.init(out.length()))) {
            LOG_WARN("init lob result failed", K(ret), K(out.length()));
          } else if (OB_FAIL(text_res.append(out.ptr(), out.length()))) {
            LOG_WARN("failed to append realdata", K(ret), K(out), K(text_res));
          } else {
            text_res.set_result();
          }
        }
      }
    }
  }
  return ret;
}

int ObExprRegexpSubstr::is_valid_for_generated_column(const ObRawExpr*expr,
                                                      const common::ObIArray<ObRawExpr *> &exprs,
                                                      bool &is_valid) const {
  int ret = OB_SUCCESS;
  is_valid = lib::is_mysql_mode();
  return ret;
}

}
}
