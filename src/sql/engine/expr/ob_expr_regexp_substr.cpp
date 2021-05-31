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

using namespace oceanbase::common;

namespace oceanbase {
namespace sql {
// Modify the previously defined T_OP_REGEXP_SUBSTR to T_FUN_SYS_REGEXP_SUBSTR, because the previous
// registered value of T_OP_REGEXP_SUBSTR is only in mysql
ObExprRegexpSubstr::ObExprRegexpSubstr(ObIAllocator& alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_REGEXP_SUBSTR, N_REGEXP_SUBSTR, MORE_THAN_ONE)
{}

ObExprRegexpSubstr::~ObExprRegexpSubstr()
{}

int ObExprRegexpSubstr::calc_result_typeN(
    ObExprResType& type, ObExprResType* types, int64_t param_num, common::ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  CK(NULL != type_ctx.get_raw_expr());
  if (OB_FAIL(ret)) {
    // Because the introduced regular expression library engine has utf8 restrictions, it is necessary
    // to reset the relevant parameter types and implicitly convert
  } else if (OB_UNLIKELY(param_num < 2 || param_num > 6)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("param number of regexp_substr at least 2 and at most 6", K(ret), K(param_num));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < param_num; i++) {
      if (!types[i].is_null() && !is_type_valid(types[i].get_type())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("the parameter is not castable", K(ret), K(i));
      }
    }
    if (lib::is_oracle_mode()) {
      // set max length.
      type.set_length(static_cast<common::ObLength>(types[0].get_length()));
      bool prefer_varchar = true;
      auto str_params = make_const_carray(&types[0]);
      OZ(aggregate_string_type_and_charset_oracle(*type_ctx.get_session(), str_params, type, prefer_varchar));
      OZ(deduce_string_param_calc_type_and_charset(*type_ctx.get_session(), type, str_params));
      OX(type.set_length_semantics(type_ctx.get_session()->get_actual_nls_length_semantics()));
    } else {
      const common::ObLengthSemantics default_length_semantics =
          (OB_NOT_NULL(type_ctx.get_session()) ? type_ctx.get_session()->get_actual_nls_length_semantics()
                                               : common::LS_BYTE);
      type.set_varchar();
      type.set_length(types[0].get_length());
      type.set_length_semantics(
          types[0].is_varchar_or_char() ? types[0].get_length_semantics() : default_length_semantics);
      ret = aggregate_charsets_for_string_result(type, types, 1, type_ctx.get_coll_type());
    }
    if (OB_SUCC(ret)) {  // because of regexp engine need utf8 code, here need reset calc collation type and can
                         // implicit cast.
      switch (param_num) {
        case 6:
          if (lib::is_oracle_mode()) {
            types[5].set_calc_type(ObNumberType);
          } else {
            type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_STRING_INTEGER_TRUNC);
            types[5].set_calc_type(ObIntType);
          }
        case 5:
          types[4].set_calc_type(ObVarcharType);
        case 4:
          if (lib::is_oracle_mode()) {
            types[3].set_calc_type(ObNumberType);
          } else {
            type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_STRING_INTEGER_TRUNC);
            types[3].set_calc_type(ObIntType);
          }
        case 3:
          if (lib::is_oracle_mode()) {
            types[2].set_calc_type(ObNumberType);
          } else {
            type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_STRING_INTEGER_TRUNC);
            types[2].set_calc_type(ObIntType);
          }
        case 2:
          types[1].set_calc_type(ObVarcharType);
          if (ObCharset::is_bin_sort(types[1].get_calc_collation_type())) {
            types[1].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
          } else {
            types[1].set_calc_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
          }
          types[1].set_calc_collation_level(type.get_collation_level());
          if (!types[0].is_clob()) {
            types[0].set_calc_type(ObVarcharType);
            if (ObCharset::is_bin_sort(types[0].get_calc_collation_type())) {
              types[0].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
            } else {
              types[0].set_calc_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
            }
            types[0].set_calc_collation_level(type.get_collation_level());
          }
        default:
          // already check before
          break;
      }
    }
  }
  return ret;
}

int ObExprRegexpSubstr::calc_resultN(
    common::ObObj& result, const common::ObObj* objs, int64_t param_num, common::ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  bool has_null = false;
  ObString text, pattern, match_param;
  int64_t position = 1, occurrence = 1, subexpr = 0;
  bool is_flag_null = false;
  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("varchar buffer not init or exec ctx or physical plan ctx is NULL", K(expr_ctx.calc_buf_));
  } else {
    for (int i = 0; !has_null && i < param_num; i++) {
      // match param is null, use default flags
      if (objs[i].is_null()) {
        if (share::is_oracle_mode() && i == 4) {  // compatible oracle
        } else {
          has_null = true;
          is_flag_null = (i == 4);
        }
      }
    }
    for (int i = 0; OB_SUCC(ret) && i < param_num; i++) {
      if (!objs[i].is_null() && !is_type_valid(objs[i].get_type())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret), K(i), K(objs[i]));
      }
    }
    if (OB_SUCC(ret)) {
      switch (param_num) {
        case 6:
          if (!objs[5].is_null()) {
            if (OB_FAIL(ObExprUtil::get_trunc_int64(objs[5], expr_ctx, subexpr))) {
              LOG_WARN("fail get position", K(subexpr), K(ret));
              break;
            } else if (OB_UNLIKELY(subexpr < 0)) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("regexp_instr subexpr is invalid", K(ret), K(subexpr));
              break;
            }
          }
        case 5:
          if (!objs[4].is_null()) {
            TYPE_CHECK(objs[4], ObVarcharType);
            match_param = objs[4].get_string();
          }
        case 4:
          if (!objs[3].is_null()) {
            if (OB_FAIL(ObExprUtil::get_trunc_int64(objs[3], expr_ctx, occurrence))) {
              LOG_WARN("fail get position", K(occurrence), K(ret));
              break;
            } else if (OB_UNLIKELY(occurrence <= 0)) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("regexp_instr occurrence is invalid", K(ret), K(occurrence));
              break;
            }
          }
        case 3:
          if (!objs[2].is_null()) {
            if (OB_FAIL(ObExprUtil::get_trunc_int64(objs[2], expr_ctx, position))) {
              LOG_WARN("fail get position", K(position), K(ret));
              break;
            } else if (OB_UNLIKELY(position < 0)) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("regexp_instr position is invalid", K(ret), K(position));
              break;
            }
          }
        case 2:
          if (!objs[1].is_null()) {
            TYPE_CHECK(objs[1], ObVarcharType);
            pattern = objs[1].get_string();
            if (share::is_mysql_mode() && pattern.empty() && !is_flag_null) {  // compatible mysql
              ret = OB_ERR_REGEXP_ERROR;
              LOG_WARN("empty regex expression", K(ret));
            }
          }
          if (OB_FAIL(ret)) {
          } else if (!objs[0].is_null() && !objs[0].is_clob()) {
            TYPE_CHECK(objs[0], ObVarcharType);
            text = objs[0].get_string();
          } else if (objs[0].is_clob()) {
            text = objs[0].get_string();
          }
        default:
          break;
      }
      ObExprRegexContext regexp_obj;
      if (OB_SUCC(ret)) {
        bool is_null = false;
        ObString res;
        if (OB_FAIL(calc(res,
                is_null,
                text,
                pattern,
                position,
                occurrence,
                result_type_.get_collation_type(),
                match_param,
                subexpr,
                has_null,
                &regexp_obj,
                *expr_ctx.calc_buf_))) {
          LOG_WARN(
              "fail to cacl regexp_substr", K(ret), K(text), K(pattern), K(occurrence), K(match_param), K(subexpr));
        } else if (is_null) {
          result.set_null();
        } else if (objs[0].is_clob()) {
          result.set_lob_value(ObLongTextType, res.ptr(), res.length());
        } else {
          result.set_string(ObVarcharType, res);
        }
      }
    }
  }
  if (OB_LIKELY(OB_SUCCESS == ret && !result.is_null())) {
    result.set_collation_type(ObCharset::get_default_collation_oracle(CHARSET_UTF8MB4));
    if (OB_FAIL(convert_result_collation(result_type_, result, expr_ctx.calc_buf_))) {
      LOG_WARN("failed to convert result collation", K(ret));
    } else {
      result.set_collation(result_type_);
    }
  }
  return ret;
}

int ObExprRegexpSubstr::calc(ObString& result, bool& is_null, const ObString& text, const ObString& pattern,
    int64_t position, int64_t occurrence, const ObCollationType cs_type, const ObString& match_param, int64_t subexpr,
    bool has_null_argument, ObExprRegexContext* regexp_ptr, ObExprStringBuf& string_buf)
{
  int ret = OB_SUCCESS;
  // begin with '^'
  bool from_begin = false;
  // end with '$'
  bool from_end = false;
  ObExprRegexContext* regexp_substr_ctx = regexp_ptr;
  ObSEArray<uint32_t, 4> begin_locations(common::ObModIds::OB_SQL_EXPR_REPLACE, common::OB_MALLOC_NORMAL_BLOCK_SIZE);
  int flags = 0, multi_flag = 0;
  const bool reusable = false;
  ObString sub;
  if (OB_ISNULL(regexp_substr_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("regexp ptr is null", K(ret));
  } else if (!regexp_substr_ctx->is_inited()) {
    if (OB_FAIL(ObExprRegexpCount::get_regexp_flags(cs_type, match_param, flags, multi_flag))) {
      LOG_WARN("fail to get regexp flags", K(ret), K(match_param));
    } else if (OB_FAIL(regexp_substr_ctx->init(pattern, flags, string_buf, reusable))) {
      LOG_WARN("fail to init regexp", K(pattern), K(flags));
    }
  }
  if (OB_SUCC(ret)) {
    if (has_null_argument) {
      is_null = true;
    } else if (position <= 0 || occurrence <= 0) {
      if (share::is_oracle_mode()) {
        ret = OB_ERR_ARGUMENT_OUT_OF_RANGE;
      } else {
        ret = OB_INVALID_ARGUMENT;
      }
      LOG_WARN("position or occurrence is invalid", K(ret), K(position), K(occurrence));
    } else {
      if (!pattern.empty()) {
        const char* pat = pattern.ptr();
        if ('^' == pat[0]) {
          from_begin = true;
        } else if ('$' == pat[pattern.length() - 1]) {
          from_end = true;
        }
      }
      if (OB_FAIL(begin_locations.push_back(position - 1))) {
        LOG_WARN("begin_locations push_back error", K(ret));
      } else if (from_begin && 1 != position && multi_flag) {
        is_null = true;
        result = ObString();
      } else {
        ObSEArray<size_t, 4> byte_num;
        ObSEArray<size_t, 4> byte_offsets;
        if (OB_FAIL(ObExprUtil::get_mb_str_info(
                text, ObCharset::get_default_collation_oracle(CHARSET_UTF8MB4), byte_num, byte_offsets))) {
          LOG_WARN("failed to get mb str info", K(ret));
        } else if (multi_flag) {
          if (from_begin && 1 != position) {
            begin_locations.pop_back();
          }
          const char* text_ptr = text.ptr();
          for (int64_t idx = position; OB_SUCC(ret) && idx < byte_offsets.count() - 1; ++idx) {
            if (OB_UNLIKELY(byte_offsets.at(idx) >= text.length())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get unexpected error", K(byte_offsets.at(idx)), K(text.length()), K(ret));
            } else if (text_ptr[byte_offsets.at(idx)] == '\n') {
              if (OB_FAIL(begin_locations.push_back(idx + 1))) {
                LOG_WARN("begin_locations push_back error", K(ret));
              }
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(regexp_substr_ctx->substr(
                  text, occurrence, subexpr, sub, string_buf, from_begin, from_end, begin_locations))) {
            LOG_WARN(
                "fail to get substr", K(ret), K(text), K(pattern), K(occurrence), K(match_param), K(flags), K(subexpr));
          } else if (lib::is_mysql_mode() && OB_ISNULL(sub.ptr())) {
            is_null = true;
          } else if (lib::is_oracle_mode() && OB_ISNULL(sub.ptr()) && OB_NOT_NULL(text.ptr())) {
            is_null = true;
          } else {
            result = sub;
          }
        }
      }
    }
  }
  return ret;
}

int ObExprRegexpSubstr::cg_expr(ObExprCGCtx&, const ObRawExpr&, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  CK(2 <= rt_expr.arg_cnt_ && rt_expr.arg_cnt_ <= 6);
  rt_expr.eval_func_ = &eval_regexp_substr;
  return ret;
}

int ObExprRegexpSubstr::eval_regexp_substr(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* text = NULL;
  ObDatum* pattern = NULL;
  ObDatum* position = NULL;
  ObDatum* occurrence = NULL;
  ObDatum* flags = NULL;
  ObDatum* subexpr = NULL;
  int64_t pos = 1;
  int64_t occur = 1;
  int64_t subexpr_val = 0;
  if (OB_FAIL(expr.eval_param_value(ctx, text, pattern, position, occurrence, flags, subexpr))) {
    LOG_WARN("evaluate parameters failed", K(ret));
  } else if (OB_FAIL(ObExprUtil::get_int_param_val(position, pos)) ||
             OB_FAIL(ObExprUtil::get_int_param_val(occurrence, occur)) ||
             OB_FAIL(ObExprUtil::get_int_param_val(subexpr, subexpr_val))) {
    LOG_WARN("get integer parameter value failed", K(ret));
  } else if (pos < 0 || occur <= 0 || subexpr_val < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("regexp_substr position or occurrence or subexpr is invalid", K(ret), K(pos), K(occur), K(subexpr_val));
  } else {
    // %flags may be null
    bool is_flag_null = (NULL != flags && flags->is_null());
    const bool null_result = (text->is_null() || pattern->is_null() || (NULL != position && position->is_null()) ||
                              (NULL != occurrence && occurrence->is_null()) ||
                              (NULL != subexpr && subexpr->is_null()) || (share::is_mysql_mode() && is_flag_null));
    ObString match_param = (NULL != flags && !flags->is_null()) ? flags->get_string() : ObString();

    ObString res;
    ObExprRegexContext regexp_ctx;
    ObIAllocator& alloc = ctx.get_reset_tmp_alloc();
    bool is_null = false;
    if (OB_FAIL(ret)) {
    } else if (share::is_mysql_mode() && !pattern->is_null() && pattern->get_string().empty() &&
               !is_flag_null) {  // compatible mysql
      ret = OB_ERR_REGEXP_ERROR;
      LOG_WARN("empty regex expression", K(ret));
    } else if (OB_FAIL(calc(res,
                   is_null,
                   text->get_string(),
                   pattern->get_string(),
                   pos,
                   occur,
                   expr.datum_meta_.cs_type_,
                   match_param,
                   subexpr_val,
                   null_result,
                   &regexp_ctx,
                   alloc))) {
      LOG_WARN("calc failed", K(ret));
    } else if (null_result) {
      expr_datum.set_null();
    } else {
      if (is_null || (res.empty() && lib::is_oracle_mode() && !(expr.args_[0]->datum_meta_.is_clob()))) {
        expr_datum.set_null();
      } else if (res.empty() && lib::is_oracle_mode() && expr.args_[0]->datum_meta_.is_clob()) {
        expr_datum.set_string(ObString().ptr(), ObString().length());
      } else {
        ObExprStrResAlloc out_alloc(expr, ctx);
        ObString out;
        if (OB_FAIL(ObExprUtil::convert_string_collation(
                res, expr.args_[0]->datum_meta_.cs_type_, out, expr.datum_meta_.cs_type_, out_alloc))) {
          LOG_WARN("convert charset failed", K(ret));
        } else {
          if (out.ptr() == res.ptr()) {
            // res is allocated in temporary allocator, deep copy here.
            char* mem = expr.get_str_res_mem(ctx, res.length());
            if (OB_ISNULL(mem)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("allocate memory failed", K(ret));
            } else {
              MEMCPY(mem, res.ptr(), res.length());
              expr_datum.set_string(mem, res.length());
            }
          } else {
            expr_datum.set_string(out.ptr(), out.length());
          }
        }
      }
    }
  }

  return ret;
}

}  // namespace sql
}  // namespace oceanbase
