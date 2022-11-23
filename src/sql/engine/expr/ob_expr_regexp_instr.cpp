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

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

ObExprRegexpInstr::ObExprRegexpInstr(ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_SYS_REGEXP_INSTR, N_REGEXP_INSTR, MORE_THAN_ONE, NOT_ROW_DIMENSION)
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
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  CK(NULL != type_ctx.get_raw_expr());
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(param_num < 2 || param_num > 7)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("param number of regexp_instr at least 2 and at most 7", K(ret), K(param_num));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < param_num; i++) {
      if (!types[i].is_null() && !is_type_valid(types[i].get_type())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("the parameter is not castable", K(ret), K(i));
      }
    }
     if (lib::is_oracle_mode()) {//set default value
      type.set_calc_type(ObVarcharType);
      type.set_calc_collation_type(ObCharset::get_default_collation_oracle(CHARSET_UTF8MB4));
      type.set_calc_collation_level(CS_LEVEL_IMPLICIT);
      type.set_number();
      type.set_precision(PRECISION_UNKNOWN_YET);
      type.set_scale(NUMBER_SCALE_UNKNOWN_YET);
    } else {
      ObObjMeta type_metas[2] = {types[0], types[1]};
      if (OB_FAIL(aggregate_charsets_for_comparison(
                  type.get_calc_meta(), type_metas, 2, type_ctx.get_coll_type()))) {
        LOG_WARN("fail to aggregate charsets for comparison", K(ret), K(types[0]), K(types[1]));
      } else {
        type.set_int();
        type.set_scale(common::DEFAULT_SCALE_FOR_INTEGER);
        type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].precision_);
      }
    }
    if (OB_SUCC(ret)) {//because of regexp engine need utf8 code, here need reset calc collation type and can implicit cast.
      switch (param_num) {
        case 7:
          if (lib::is_oracle_mode()) {
            types[6].set_calc_type(ObNumberType);
          } else {//mysql8.0 only has 6 arguments
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("too many arguments for regexp_instr in mysql mode", K(ret));
          }
        case 6:
          types[5].set_calc_type(ObVarcharType);
	      case 5:
          if (lib::is_oracle_mode()) {
            types[4].set_calc_type(ObNumberType);
          } else {
            type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_STRING_INTEGER_TRUNC);
            types[4].set_calc_type(ObIntType);
          }
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
          if (!types[0].is_clob()) {
            types[0].set_calc_type(ObVarcharType);
            if (ObCharset::is_bin_sort(types[0].get_calc_collation_type())) {
              types[0].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
            } else {
              types[0].set_calc_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
            }
          }
        default:
          // already check before
          break;
      }
    }
  }
  return ret;
}

int ObExprRegexpInstr::calc(int64_t &ret_pos,
                            const ObString &text,
                            const ObString &pattern,
                            int64_t position,
                            int64_t occurrence,
							              int64_t return_option,
                            const ObCollationType cs_type,
                            const ObString &match_param,
                            int64_t subexpr,
                            bool has_null_argument,
                            bool reusable,
                            ObExprRegexContext *regexp_ptr,
                            ObExprStringBuf &string_buf,
                            ObIAllocator &exec_ctx_alloc)
{
  int ret = OB_SUCCESS;
  //begin with '^'
  bool from_begin = false;
  //end with '$'
  bool from_end = false;
  ret_pos = 0;
  ObExprRegexContext *regexp_instr_ctx = regexp_ptr;
  ObSEArray<uint32_t, 4> begin_locations(common::ObModIds::OB_SQL_EXPR_REPLACE, 
                                           common::OB_MALLOC_NORMAL_BLOCK_SIZE);
  int flags = 0, multi_flag = 0;
  int64_t sub = 0;
  if (OB_ISNULL(regexp_instr_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("regexp ptr is null", K(ret));
  } else if (OB_FAIL(ObExprRegexpCount::get_regexp_flags(cs_type, match_param,
                                                         flags, multi_flag))) {
    LOG_WARN("fail to get regexp flags", K(ret), K(match_param));
  } else if (OB_FAIL(regexp_instr_ctx->init(pattern, flags,
                                            reusable ? exec_ctx_alloc : string_buf,
                                            reusable))) {
    LOG_WARN("fail to init regexp", K(pattern), K(flags));
  }
  if (OB_SUCC(ret)) {
    //为了更好的和oracle兼容
    if (has_null_argument) {
      // ret_pos = 0;
    } else if (position <= 0 || occurrence <= 0) {
      if (lib::is_oracle_mode()) {
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
      } else if (from_begin && 1 != position && !multi_flag) {
        ret_pos = sub;
      } else {
        ObSEArray<size_t, 4> byte_num;
        ObSEArray<size_t, 4> byte_offsets;
        if (OB_FAIL(ObExprUtil::get_mb_str_info(text,
                                                ObCharset::get_default_collation_oracle(CHARSET_UTF8MB4),
                                                byte_num,
                                                byte_offsets))) {
          LOG_WARN("failed to get mb str info", K(ret));
        } else if (multi_flag) {
          if (from_begin && 1 != position) {
            begin_locations.pop_back();
          }
          const char *text_ptr = text.ptr();
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
          if (OB_FAIL(regexp_instr_ctx->instr(text, occurrence, return_option, subexpr, sub,
                                              string_buf, from_begin, from_end, begin_locations))) {
            LOG_WARN("fail to get location", K(ret), K(text), K(pattern),
                      K(occurrence), K(match_param), K(position), K(subexpr));
          } else if (sub >= 0 ){
            ret_pos = sub + 1;
          } else {
            ret_pos = 0;
          }
        }
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
  ObDatum *flags = NULL;
  ObDatum *subexpr= NULL;
  if (OB_FAIL(expr.eval_param_value(
              ctx, text, pattern, position, occurrence, return_opt, flags, subexpr))) {
    LOG_WARN("evaluate parameters failed", K(ret));
  } else {
    bool is_flag_null = (NULL != flags && flags->is_null());
    const bool null_result = (text->is_null()
                              || pattern->is_null()
                              || (NULL != position && position->is_null())
                              || (NULL != occurrence && occurrence->is_null())
                              || (NULL != return_opt && return_opt->is_null())
                              || (NULL != subexpr && subexpr->is_null())
                              || (lib::is_mysql_mode() && is_flag_null));
    int64_t pos = 1;
    int64_t occur = 1;
    int64_t return_opt_val = 0;
    int64_t subexpr_val = 0;
    if (OB_FAIL(ObExprUtil::get_int_param_val(position, pos))
        || OB_FAIL(ObExprUtil::get_int_param_val(occurrence, occur))
        || OB_FAIL(ObExprUtil::get_int_param_val(return_opt, return_opt_val))
        || OB_FAIL(ObExprUtil::get_int_param_val(subexpr, subexpr_val))) {
      LOG_WARN("get integer parameter value failed", K(ret));
    } else if (pos <= 0 || occur < 0 || subexpr_val < 0
               || (lib::is_mysql_mode() && (return_opt_val < 0 || return_opt_val > 1))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("regexp_instr position or occurrence or return_option or subexpr is invalid",
               K(ret), K(pos), K(occur), K(subexpr_val));
    } else if (lib::is_oracle_mode() && return_opt_val < 0) {
      ret = OB_ERR_ARGUMENT_OUT_OF_RANGE;
      LOG_WARN("regexp_instr return_option is invalid", K(ret), K(return_opt_val));
    }
    ObString match_param = (NULL != flags && !flags->is_null())
        ? flags->get_string()
        : ObString();

    int64_t res_pos = 0;
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    ObIAllocator &alloc = alloc_guard.get_allocator();
    ObExprRegexContext local_regex_ctx;
    ObExprRegexContext *regexp_ctx = &local_regex_ctx;
    const bool reusable = (0 != expr.extra_) && ObExpr::INVALID_EXP_CTX_ID != expr.expr_ctx_id_;
    if (OB_SUCC(ret) && reusable) {
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
    } else if (lib::is_mysql_mode() && !pattern->is_null() &&
               pattern->get_string().empty() && !is_flag_null) {//compatible mysql
      ret = OB_ERR_REGEXP_ERROR;
      LOG_WARN("empty regex expression", K(ret));
    } else if (OB_FAIL(calc(res_pos, text->get_string(), pattern->get_string(),
                            pos, occur, return_opt_val,
                            expr.args_[0]->datum_meta_.cs_type_, match_param, subexpr_val,
                            null_result, reusable, regexp_ctx, alloc,
                            ctx.exec_ctx_.get_allocator()))) {
    } else if (null_result) {
      expr_datum.set_null();
    } else {
      if (lib::is_mysql_mode()) {
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

}
}
