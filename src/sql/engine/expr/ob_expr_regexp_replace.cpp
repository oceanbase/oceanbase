
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
#include "sql/engine/expr/ob_expr_regexp_replace.h"
#include "lib/oblog/ob_log.h"
#include "share/object/ob_obj_cast.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/engine/expr/ob_expr_regexp_count.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

ObExprRegexpReplace::ObExprRegexpReplace(ObIAllocator &alloc)
  : ObStringExprOperator(alloc, T_FUN_SYS_REGEXP_REPLACE, N_REGEXP_REPLACE, MORE_THAN_ONE, NOT_VALID_FOR_GENERATED_COL)
{
}

ObExprRegexpReplace::~ObExprRegexpReplace()
{
}

int ObExprRegexpReplace::calc_result_typeN(ObExprResType &type,
                                           ObExprResType *types,
                                           int64_t param_num,
                                           common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  ObRawExpr * raw_expr = type_ctx.get_raw_expr();
  CK(NULL != type_ctx.get_raw_expr());
  int64_t max_allowed_packet = 0;
  const ObRawExpr *real_text = NULL;
  const ObRawExpr *real_pattern = NULL;
  bool is_case_sensitive = false;
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(param_num < 2 || param_num > 6)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("param number of regexp_replace at least 2 and at most 6", K(ret), K(param_num));
  } else if (OB_ISNULL(type_ctx.get_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(type_ctx.get_session()));
  } else if (OB_FAIL(type_ctx.get_session()->get_max_allowed_packet(max_allowed_packet))) {
    LOG_WARN("failed to get max allowed packet", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::get_real_expr_without_cast(raw_expr->get_param_expr(0), real_text))) {
    LOG_WARN("fail to get real expr without cast", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::get_real_expr_without_cast(raw_expr->get_param_expr(1), real_pattern))) {
    LOG_WARN("fail to get real expr without cast", K(ret));
  } else if (OB_ISNULL(real_text) || OB_ISNULL(real_pattern)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("real expr is invalid", K(ret), K(real_text), K(real_pattern));
  } else {
    const ObExprResType &text = real_text->get_result_type();
    for (int i = 0; OB_SUCC(ret) && i < param_num; i++) {
      if (!types[i].is_null() && !is_type_valid(types[i].get_type())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("the parameter is not castable", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret)) {
      if (lib::is_oracle_mode()) {
        //deduce length
        int64_t to_len = types[2].get_length();
        common::ObLength len = text.get_length();
        int64_t offset = len * to_len;
        len = static_cast<common::ObLength>(len + offset);
        CK(len <= INT32_MAX);
        type.set_length(static_cast<common::ObLength>(len));
        auto input_params = make_const_carray(const_cast<ObExprResType*>(&text));
        OZ(aggregate_string_type_and_charset_oracle(
                *type_ctx.get_session(), input_params, type, PREFER_VAR_LEN_CHAR));
        OZ(deduce_string_param_calc_type_and_charset(*type_ctx.get_session(), type, input_params));
        OX(type.set_length_semantics(type_ctx.get_session()->get_actual_nls_length_semantics()));
        is_case_sensitive = ObCharset::is_bin_sort(types[0].get_collation_type());
      } else {
        const ObExprResType &pattern = real_pattern->get_result_type();
        const common::ObLengthSemantics default_length_semantics = (OB_NOT_NULL(type_ctx.get_session())
                ? type_ctx.get_session()->get_actual_nls_length_semantics()
                : common::LS_BYTE);
        ObObjMeta real_types[2] = {text, pattern};
        if (text.is_blob()) {
          type.set_blob();
        } else if (pattern.is_blob()) {
          type.set_blob();
        } else {
          type.set_clob();
          type.set_length_semantics(text.is_varchar_or_char() ? text.get_length_semantics() : default_length_semantics);
        }
        //建表列的最大长度
        type.set_length(max_allowed_packet);
        if (OB_FAIL(ObExprRegexContext::check_binary_compatible(types, 3))) {
          LOG_WARN("types are not compatible with binary.", K(ret));
        } else {
          ret = aggregate_charsets_for_string_result(type, real_types, 2, type_ctx.get_coll_type());
          is_case_sensitive = ObCharset::is_bin_sort(type.get_collation_type());
        }
      }
    }
    if (OB_SUCC(ret)) {
      bool need_utf8 = false;
      switch (param_num) {
        case 6/*match type*/:
          types[5].set_calc_type(ObVarcharType);
          types[5].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
          types[5].set_calc_collation_level(CS_LEVEL_IMPLICIT);
        case 5/*occurence*/:
          types[4].set_calc_type(lib::is_oracle_mode() ? ObNumberType : ObIntType);
          types[4].set_scale(lib::is_oracle_mode() ? NUMBER_SCALE_UNKNOWN_YET : DEFAULT_SCALE_FOR_INTEGER);
          types[4].set_precision(lib::is_oracle_mode() ? PRECISION_UNKNOWN_YET :
                                            ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].precision_);
        case 4/*position*/:
          types[3].set_calc_type(lib::is_oracle_mode() ? ObNumberType : ObIntType);
          types[3].set_scale(lib::is_oracle_mode() ? NUMBER_SCALE_UNKNOWN_YET : DEFAULT_SCALE_FOR_INTEGER);
          types[3].set_precision(lib::is_oracle_mode() ? PRECISION_UNKNOWN_YET :
                                          ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].precision_);
          type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_STRING_INTEGER_TRUNC);
        case 3/*replace string*/:
          types[2].set_calc_type(ObVarcharType);
          types[2].set_calc_collation_type(is_case_sensitive ? CS_TYPE_UTF8MB4_BIN : CS_TYPE_UTF8MB4_GENERAL_CI);
          types[2].set_calc_collation_level(CS_LEVEL_IMPLICIT);
          need_utf8 = false;
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(ObExprRegexContext::check_need_utf8(raw_expr->get_param_expr(2), need_utf8))) {
            LOG_WARN("fail to check need utf8", K(ret));
          } else if (need_utf8) {
            types[2].set_calc_collation_type(is_case_sensitive ? CS_TYPE_UTF8MB4_BIN : CS_TYPE_UTF8MB4_GENERAL_CI);
          } else {
            types[2].set_calc_collation_type(is_case_sensitive ? CS_TYPE_UTF16_BIN : CS_TYPE_UTF16_GENERAL_CI);
          }
        case 2/*pattern and text*/:
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
  return ret;
}

int ObExprRegexpReplace::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
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
      rt_expr.eval_func_ = &eval_regexp_replace;
      LOG_DEBUG("regexp reeplace expr cg", K(const_text), K(const_pattern), K(rt_expr.extra_));
    }
  }
  return ret;
}

int ObExprRegexpReplace::eval_regexp_replace(
    const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *text = NULL;
  ObDatum *pattern = NULL;
  ObDatum *to = NULL;
  ObDatum *position = NULL;
  ObDatum *occurrence = NULL;
  ObDatum *match_type = NULL;
  ObString res_replace;
  bool is_no_pattern_to_replace = false;
  bool need_convert = false;
  ObCollationType res_coll_type = CS_TYPE_INVALID;
  ObEvalCtx::TempAllocGuard alloc_guard(ctx);
  ObIAllocator &tmp_alloc = alloc_guard.get_allocator();
  if (OB_FAIL(expr.eval_param_value(ctx, text, pattern, to, position, occurrence, match_type))) {
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
  } else if (lib::is_oracle_mode() && pattern->is_null()) {
    if (text->is_null()) {
      expr_datum.set_null();
    } else {
      is_no_pattern_to_replace = true;
      res_replace = text->get_string(); // if text is lob type, res_replace only get locator
      res_coll_type = expr.args_[0]->datum_meta_.cs_type_;
      need_convert = true;
    }
  } else if (expr.args_[0]->datum_meta_.is_clob()
             && ob_is_empty_lob(expr.args_[0]->datum_meta_.type_, *text, expr.args_[0]->obj_meta_.has_lob_header())) {
    expr_datum.set_datum(*text);
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
  } else if (OB_UNLIKELY(expr.arg_cnt_ > 2 &&
                         expr.args_[2]->datum_meta_.cs_type_ != CS_TYPE_UTF8MB4_GENERAL_CI &&
                         expr.args_[2]->datum_meta_.cs_type_ != CS_TYPE_UTF8MB4_BIN &&
                         expr.args_[2]->datum_meta_.cs_type_ != CS_TYPE_UTF16_GENERAL_CI &&
                         expr.args_[2]->datum_meta_.cs_type_ != CS_TYPE_UTF16_BIN)) {
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
    int64_t occur = 0;
    bool null_result = (position != NULL && position->is_null()) ||
                       (occurrence != NULL && occurrence->is_null()) ||
                       (lib::is_mysql_mode() && match_type != NULL && match_type->is_null());
    if (OB_FAIL(ObExprUtil::get_int_param_val(position, pos))
        || OB_FAIL(ObExprUtil::get_int_param_val(occurrence, occur))) {
      LOG_WARN("get integer parameter value failed", K(ret));
    } else if (!null_result && (pos <= 0 || occur < 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("regexp_replace position or occurrence is invalid", K(ret), K(pos), K(occur));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "use position or occurrence in regexp_replace");
    } else {
      ObString to_str = (NULL != to && !to->is_null()) ? to->get_string() : ObString();
      ObString match_param = (NULL != match_type && !match_type->is_null()) ? match_type->get_string() : ObString();
      ObExprRegexContext local_regex_ctx;
      ObExprRegexContext *regexp_ctx = &local_regex_ctx;
      const bool reusable = (0 != expr.extra_) && ObExpr::INVALID_EXP_CTX_ID != expr.expr_ctx_id_;
      uint32_t flags = 0;
      bool is_case_sensitive = ObCharset::is_bin_sort(expr.args_[0]->datum_meta_.cs_type_);
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
                                           pattern->get_string(), flags, reusable,
                                           expr.args_[1]->datum_meta_.cs_type_))) {
        LOG_WARN("fail to init regexp", K(pattern), K(flags), K(ret));
      } else if (text->is_null() ||
                 pattern->is_null() ||
                 null_result ||
                 (NULL != position && position->is_null()) ||
                 (NULL != occurrence && occurrence->is_null()) ||
                 (lib::is_mysql_mode() && NULL != pattern && pattern->is_null()) ||
                 (lib::is_mysql_mode() && NULL != to && to->is_null()) ||
                 (lib::is_mysql_mode() && NULL != match_type && match_type->is_null())) {
        if (lib::is_oracle_mode() && !text->is_null() && !text->get_string().empty()) {
          if ((NULL != position && position->is_null()) ||
              (NULL != occurrence && occurrence->is_null())) {
            expr_datum.set_null();
          } else {
            is_no_pattern_to_replace = true;
            res_replace = text->get_string(); // if text is lob type, res_replace only get locator;
            res_coll_type = expr.args_[0]->datum_meta_.cs_type_;
            need_convert = true;
          }
        } else {
          expr_datum.set_null();
        }
      } else {
        ObString text_utf16;
        ObString to_utf16;
        ObString text_str;
        if (ob_is_text_tc(expr.args_[0]->datum_meta_.type_)) {
          if (OB_FAIL(ObTextStringHelper::get_string(expr, tmp_alloc, 0, text, text_str))) {
            LOG_WARN("get text string failed", K(ret));
          }
        } else {
          text_str = text->get_string();
        }
        if (OB_FAIL(ret)) {
        } else if (expr.args_[0]->datum_meta_.cs_type_ == CS_TYPE_UTF8MB4_BIN ||
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
        if (OB_FAIL(ret)) {
        } else if (expr.arg_cnt_ > 2 && (expr.args_[2]->datum_meta_.cs_type_ == CS_TYPE_UTF8MB4_BIN ||
            expr.args_[2]->datum_meta_.cs_type_ == CS_TYPE_UTF8MB4_GENERAL_CI)) {
          if (OB_FAIL(ObExprUtil::convert_string_collation(to_str, expr.args_[2]->datum_meta_.cs_type_, to_utf16,
                                    ObCharset::is_bin_sort(expr.args_[2]->datum_meta_.cs_type_) ? CS_TYPE_UTF16_BIN : CS_TYPE_UTF16_GENERAL_CI,
                                    tmp_alloc))) {
            LOG_WARN("convert charset failed", K(ret));
          }
        } else {
          to_utf16 = to_str;
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(regexp_ctx->replace(tmp_alloc, text_utf16, to_utf16, pos - 1,
                                              occur, res_replace))) {
          LOG_WARN("failed to regexp replace str", K(ret));
        } else if (res_replace.empty() && lib::is_oracle_mode()) {
          expr_datum.set_null();
        } else {
          need_convert = true;
        }
      }
    }
  }

  if (OB_SUCC(ret) && need_convert) {
    ObExprStrResAlloc out_alloc(expr, ctx);
    ObString out;
    if (is_no_pattern_to_replace && ob_is_text_tc(expr.args_[0]->datum_meta_.type_)) {
      if (OB_FAIL(ObTextStringHelper::get_string(expr, tmp_alloc, 0, text, res_replace))) {
        LOG_WARN("get text string failed", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (!ob_is_text_tc(expr.datum_meta_.type_)) {
      if (OB_FAIL(ObExprUtil::convert_string_collation(res_replace, res_coll_type, out,
                                                       expr.datum_meta_.cs_type_, out_alloc))) {
        LOG_WARN("convert charset failed", K(ret));
      } else if (out.ptr() == res_replace.ptr()) {
        // res_replace is allocated in temporary allocator, deep copy here.
        char *mem = expr.get_str_res_mem(ctx, res_replace.length());
        if (OB_ISNULL(mem)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret));
        } else {
          MEMCPY(mem, res_replace.ptr(), res_replace.length());
          expr_datum.set_string(mem, res_replace.length());
        }
      } else {
        expr_datum.set_string(out.ptr(), out.length());
      }
    } else { // output is text type
      ObTextStringDatumResult text_res(expr.datum_meta_.type_, &expr, &ctx, &expr_datum);
      if (OB_FAIL(ObExprUtil::convert_string_collation(res_replace, res_coll_type, out,
                                                       expr.datum_meta_.cs_type_, tmp_alloc))) {
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
  return ret;
}

int ObExprRegexpReplace::is_valid_for_generated_column(const ObRawExpr*expr,
                                                       const common::ObIArray<ObRawExpr *> &exprs,
                                                       bool &is_valid) const {
  int ret = OB_SUCCESS;
  is_valid = lib::is_mysql_mode();
  return ret;
}

}
}
