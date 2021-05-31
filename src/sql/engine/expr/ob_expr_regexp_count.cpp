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

using namespace oceanbase::common;

namespace oceanbase {
namespace sql {

ObExprRegexpCount::ObExprRegexpCount(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_REGEXP_COUNT, N_REGEXP_COUNT, MORE_THAN_ONE, NOT_ROW_DIMENSION)
{}

ObExprRegexpCount::~ObExprRegexpCount()
{}

int ObExprRegexpCount::calc_result_typeN(
    ObExprResType& type, ObExprResType* types, int64_t param_num, common::ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
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
      switch (param_num) {  // because of regexp engine need utf8 code, here need reset calc collation type and can
                            // implicit cast.
        case 4:
          types[3].set_calc_type(ObVarcharType);
        case 3:
          types[2].set_calc_type(ObNumberType);
        case 2:
          types[1].set_calc_type(ObVarcharType);
          types[1].set_calc_collation_type(ObCharset::get_default_collation_oracle(CHARSET_UTF8MB4));
          if (!types[0].is_clob()) {
            types[0].set_calc_type(ObVarcharType);
            types[0].set_calc_collation_type(ObCharset::get_default_collation_oracle(CHARSET_UTF8MB4));
          }
        default:
          // already check before
          break;
      }
      type.set_calc_type(ObVarcharType);
      type.set_calc_collation_type(ObCharset::get_default_collation_oracle(CHARSET_UTF8MB4));
      type.set_calc_collation_level(CS_LEVEL_IMPLICIT);
      type.set_number();
      type.set_precision(PRECISION_UNKNOWN_YET);
      type.set_scale(NUMBER_SCALE_UNKNOWN_YET);
    }
  }
  return ret;
}

int ObExprRegexpCount::calc_resultN(
    common::ObObj& result, const common::ObObj* objs, int64_t param_num, common::ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  bool has_null = false;
  ObString text, pattern, match_param;
  int64_t position = 1, subexpr = 0;
  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("varchar buffer not init or exec ctx or physical plan ctx is NULL", K(expr_ctx.calc_buf_));
  } else {
    for (int i = 0; !has_null && i < param_num; i++) {
      // match param is null, use default flags
      if (objs[i].is_null()) {
        if (share::is_oracle_mode() && i == 3) {  // compatible oracle
        } else {
          has_null = true;
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
        case 4:
          if (!objs[3].is_null()) {
            TYPE_CHECK(objs[3], ObVarcharType);
            match_param = objs[3].get_string();
          }
        case 3:
          if (!objs[2].is_null()) {
            TYPE_CHECK(objs[2], ObNumberType);
            if (OB_FAIL(ObExprUtil::get_trunc_int64(objs[2], expr_ctx, position))) {
              LOG_WARN("fail get position", K(position), K(ret));
              break;
            } else if (OB_UNLIKELY(position <= 0)) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("regexp_count position is invalid", K(ret), K(position));
              break;
            }
          }
        case 2:
          if (!objs[1].is_null()) {
            TYPE_CHECK(objs[1], ObVarcharType);
            pattern = objs[1].get_string();
          }
          if (!objs[0].is_null() && !objs[0].is_clob()) {
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
        int64_t ret_count = 0;
        number::ObNumber num;
        if (OB_FAIL(calc(ret_count,
                text,
                pattern,
                position,
                result_type_.get_calc_collation_type(),
                match_param,
                subexpr,
                has_null,
                &regexp_obj,
                *expr_ctx.calc_buf_))) {
          LOG_WARN("fail to cacl regexp_count", K(ret), K(text), K(pattern), K(match_param), K(subexpr));
        } else if (has_null) {
          result.set_null();
        } else if (OB_FAIL(num.from(ret_count, *expr_ctx.calc_buf_))) {
          LOG_WARN("fail to get ObNumber from ret_count", K(ret), K(ret_count));
        } else {
          result.set_number(num);
        }
      }
    }
  }
  return ret;
}

int ObExprRegexpCount::calc(int64_t& ret_count, const ObString& text, const ObString& pattern, int64_t position,
    ObCollationType calc_cs_type, const ObString& match_param, int64_t subexpr, bool has_null_argument,
    ObExprRegexContext* regexp_ptr, ObExprStringBuf& string_buf)
{
  int ret = OB_SUCCESS;
  // begin with '^'
  bool from_begin = false;
  // end with '$'
  bool from_end = false;
  int64_t sub = 0;
  ret_count = 0;
  ObExprRegexContext* regexp_count_ctx = regexp_ptr;
  ObSEArray<uint32_t, 4> begin_locations(common::ObModIds::OB_SQL_EXPR_REPLACE, common::OB_MALLOC_NORMAL_BLOCK_SIZE);
  int flags = 0, multi_flag = 0;
  const bool reusable = false;
  if (OB_ISNULL(regexp_count_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("regexp_count_ctx is null", K(ret));
  } else if (!regexp_count_ctx->is_inited()) {
    if (OB_FAIL(get_regexp_flags(calc_cs_type, match_param, flags, multi_flag))) {
      LOG_WARN("fail to get regexp flags", K(ret), K(match_param));
    } else if (OB_FAIL(regexp_count_ctx->init(pattern, flags, string_buf, reusable))) {
      LOG_WARN("fail to init regexp", K(pattern), K(flags));
    }
  }
  if (OB_SUCC(ret)) {
    if (has_null_argument) {
      /*do nothing*/
    } else if (share::is_mysql_mode() && pattern.empty()) {
      ret = OB_ERR_REGEXP_ERROR;
      LOG_WARN("empty regex expression", K(ret));
    } else {
      if (OB_NOT_NULL(pattern)) {
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
        ret_count = sub;
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
          if (OB_FAIL(regexp_count_ctx->count_match_str(
                  text, subexpr, sub, string_buf, from_begin, from_end, begin_locations))) {
            LOG_WARN("fail to get count_match_str", K(ret), K(text), K(pattern), K(match_param), K(flags), K(subexpr));
          } else {
            ret_count = sub;
          }
        }
      }
    }
  }
  return ret;
}

int ObExprRegexpCount::get_regexp_flags(
    const common::ObCollationType calc_cs_type, const ObString& match_param, int& flags, int& multi_flag)
{
  int ret = OB_SUCCESS;
  int n_flag = 1;
  int m_flag = 0;
  const char* ptr = match_param.ptr();
  int length = match_param.length();
  flags = OB_REG_EXTENDED | OB_REG_NLSTOP | OB_REG_NEWLINE;
  if (share::is_oracle_mode()) {
    flags |= OB_REG_ORACLE_MODE;
  } else {
    flags &= ~OB_REG_ORACLE_MODE;
  }
  if (!ObCharset::is_bin_sort(calc_cs_type)) {
    flags |= OB_REG_ICASE;
  }
  for (int i = 0; i < length; i++) {
    char c = ptr[i];
    switch (c) {
      case 'i':
        flags |= OB_REG_ICASE;
        break;
      case 'c':
        flags &= ~OB_REG_ICASE;
        break;
      case 'n':
        flags &= ~OB_REG_NLSTOP;
        n_flag = 0;
        break;
      case 'm':
        flags &= ~OB_REG_NLSTOP;
        flags |= OB_REG_NEWLINE;
        flags |= OB_REG_NLANCH;
        m_flag = 1;
        break;
      case 'x':
        if (share::is_oracle_mode()) {  // compatible oracle
          flags |= OB_REG_EXPANDED;
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid match_param", K(match_param));
        }
        break;
      case 'u':
        if (share::is_mysql_mode()) {  // compatible oracle
          flags |= OB_REG_NEWLINE;
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid match_param", K(match_param));
        }
        break;
      default:
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid match_param", K(match_param));
    }
  }
  multi_flag = m_flag & n_flag;
  if (m_flag == 1 && n_flag == 0) {
    flags &= ~OB_REG_NEWLINE;
    flags &= ~OB_REG_NLANCH;
  }
  return ret;
}

int ObExprRegexpCount::cg_expr(ObExprCGCtx&, const ObRawExpr&, ObExpr& rt_expr) const
{
  // FIXME : reuse pattern compile
  int ret = OB_SUCCESS;
  // regex_count is oracle only expr
  CK(lib::is_oracle_mode());
  CK(2 <= rt_expr.arg_cnt_ && rt_expr.arg_cnt_ <= 4);
  rt_expr.eval_func_ = eval_regexp_count;
  return ret;
}

int ObExprRegexpCount::eval_regexp_count(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* text = NULL;
  ObDatum* pattern = NULL;
  ObDatum* position = NULL;
  ObDatum* flags = NULL;
  if (OB_FAIL(expr.eval_param_value(ctx, text, pattern, position, flags))) {
    LOG_WARN("evaluate parameters failed", K(ret));
  } else {
    // use default flags if NULL == flags and flags->is_null()
    const bool null_result = (text->is_null() || pattern->is_null() || (NULL != position && position->is_null()) ||
                              (share::is_mysql_mode() && NULL != flags && flags->is_null()));
    int64_t pos = 1;
    ObString match_param = (NULL != flags && !flags->is_null()) ? flags->get_string() : ObString();
    if (OB_FAIL(ObExprUtil::get_int_param_val(position, pos))) {
      LOG_WARN("truncate number to int64 failed", K(ret));
    } else if (pos <= 0) {
      ret = OB_ERR_ARGUMENT_OUT_OF_RANGE;
      LOG_WARN("position out of range", K(ret), K(position), K(pos));
    }

    ObExprRegexContext regexp_ctx;
    ObIAllocator& alloc = ctx.get_reset_tmp_alloc();
    ObNumStackOnceAlloc num_alloc;
    number::ObNumber num;
    int64_t subexpr = 0;
    int64_t cnt = 0;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(calc(cnt,
                   text->get_string(),
                   pattern->get_string(),
                   pos,
                   expr.args_[0]->datum_meta_.cs_type_,
                   match_param,
                   subexpr,
                   null_result,
                   &regexp_ctx,
                   alloc))) {
      LOG_WARN("calc regex count failed", K(ret));
    } else if (null_result) {
      expr_datum.set_null();
    } else if (OB_FAIL(num.from(cnt, num_alloc))) {
      LOG_WARN("convert int to number failed", K(ret));
    } else {
      expr_datum.set_number(num);
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
