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
 * This file contains implementation for _st_asewkb expr.
 */

#define USING_LOG_PREFIX  SQL_ENG
#include "ob_expr_inner_decode_like.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_datum_cast.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
ObExprInnerDecodeLike::ObExprInnerDecodeLike(ObIAllocator &alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_INNER_DECODE_LIKE, N_INNER_DECODE_LIKE, 6, NOT_VALID_FOR_GENERATED_COL,
                           INTERNAL_IN_MYSQL_MODE, INTERNAL_IN_ORACLE_MODE)
{
}

int ObExprInnerDecodeLike::cg_expr(ObExprCGCtx &op_cg_ctx,
                                const ObRawExpr &raw_expr,
                                ObExpr &rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(6 != rt_expr.arg_cnt_)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("invalid arg cnt of expr", K(ret), K(rt_expr));
  } else {
    rt_expr.eval_func_ = ObExprInnerDecodeLike::eval_inner_decode_like;
  }
  return ret;
}

int ObExprInnerDecodeLike::eval_inner_decode_like(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum) {
  int ret = OB_SUCCESS;
  ObDatum *pattern = NULL;
  ObDatum *escape = NULL;
  ObDatum *is_start = NULL;
  ObDatum *col_type = NULL;
  ObDatum *col_collation = NULL;
  ObDatum *col_length = NULL;
  ObDatum *pattern_val = NULL;
  const ObDataTypeCastParams &dtc_params = ctx.exec_ctx_.get_my_session()->get_dtc_params();
  if (OB_ISNULL(expr.args_[0]) ||OB_ISNULL(expr.args_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(expr.eval_param_value(ctx, pattern, escape, is_start, col_type, col_collation, col_length))) {
    LOG_WARN("eval args failed", K(ret));
  } else if (OB_ISNULL(pattern) || OB_ISNULL(escape)
            || OB_ISNULL(is_start) || OB_ISNULL(col_collation) || OB_ISNULL(col_length)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params are unexpected null", K(ret), K(pattern), K(escape),
                                           K(is_start), K(col_collation), K(col_length));
  } else if (pattern->is_null()) {
    //a like null return empty range
    expr_datum.set_null();
  } else if (col_length->get_int() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, unexpected length", K(ret));
  } else if (OB_FAIL(cast_like_obj_if_needed(ctx, *expr.args_[0], pattern, expr, pattern_val))) {
    LOG_WARN("failed to cast like obj if needed", K(ret));
  } else {
    int64_t mbmaxlen = 1;
    ObCollationType cs_type = static_cast<ObCollationType>(col_collation->get_int());
    int32_t col_len = static_cast<int32_t>(col_length->get_int());
    ObString escape_str;
    ObString pattern_str = pattern_val->get_string();
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    if (escape->is_null()) {  //如果escape是null,则给默认的'\\'
      escape_str.assign_ptr("\\", 1);
    } else {
      ObString escape_val = escape->get_string();
      ObCollationType escape_collation = expr.args_[1]->datum_meta_.cs_type_;
      if (ObCharset::is_cs_nonascii(escape_collation)) {
        if (OB_FAIL(ObCharset::charset_convert(tmp_alloc_g.get_allocator(),
                                              escape_val,
                                              escape_collation,
                                              CS_TYPE_UTF8MB4_GENERAL_CI,
                                              escape_str, true))) {
          LOG_WARN("failed to do charset convert", K(ret), K(escape_val));
        }
      } else {
        escape_str = escape->get_string();
        if (escape_str.empty()) {
          escape_str.assign_ptr("\\", 1);
        } else { /* do nothing */ }
      }
    }
    if (OB_FAIL(ret)) {
      // do nothing;
    } else if (OB_FAIL(ObCharset::get_mbmaxlen_by_coll(cs_type, mbmaxlen))) {
      LOG_WARN("fail to get mbmaxlen", K(ret), K(cs_type), K(pattern), K(escape));
    } else if (OB_ISNULL(escape_str.ptr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Escape str should not be NULL", K(ret));
    } else if (OB_UNLIKELY(1 > escape_str.length())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("failed to check escape length", K(escape_str), K(escape_str.length()));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ESCAPE");
    } else {
      // For a pattern like 'aaa%' that ends with `%`, we will extract a precise range with some special handling:
      // We need to fill the end key of the like range with the maximum character 
      // up to the target column's length to match the semantics of `%`.
      // However, when the target column length is less than the effective prefix length of the pattern, 
      // the pattern gets truncated, resulting in an imprecise range and incorrect results.
      // So, we need to ensure that the effective prefix of the pattern is not truncated 
      // to guarantee that the range is always precise.
      int32_t range_str_len = col_len;
      //convert character counts to len in bytes
      range_str_len = static_cast<int32_t>(range_str_len * mbmaxlen);
      size_t min_str_len = range_str_len;
      size_t max_str_len = range_str_len;
      size_t res_len = 0;
      size_t prefix_len = 0;
      int32_t start_flag = is_start->get_int();
      common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
      char *min_str_buf = NULL;
      char *max_str_buf = NULL;
      char *res_buf = NULL;
      if (OB_ISNULL(min_str_buf = (char*)temp_allocator.alloc(min_str_len))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("alloc memory failed", K(min_str_len));
      } else if (OB_ISNULL(max_str_buf = (char*)temp_allocator.alloc(max_str_len))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("alloc memory failed", K(max_str_len));
      } else if (OB_FAIL(ObCharset::like_range(cs_type,
                                               pattern_str,
                                               *(escape_str.ptr()),
                                               static_cast<char*>(min_str_buf),
                                               &min_str_len,
                                               static_cast<char*>(max_str_buf),
                                               &max_str_len,
                                               &prefix_len))) {
        LOG_WARN("calc like range failed", K(ret), K(pattern_str), K(escape_str), K(cs_type));
        if (OB_EMPTY_RANGE == ret) {
          expr_datum.set_null();
          ret = OB_SUCCESS;
        }
      } else {
        if (prefix_len >= col_len && ObCharset::strlen_char(cs_type, min_str_buf, prefix_len) >= col_len) {
          int32_t pattern_prefix_len = 0; // strlen_char of prefix
          if (OB_FAIL(get_pattern_prefix_len(cs_type, 
                                            escape_str, 
                                            pattern_str,
                                            pattern_prefix_len))) {
            LOG_WARN("failed to get pattern prefix len", K(ret), K(pattern_str), K(escape_str));
          } else {
            range_str_len = max(col_len, pattern_prefix_len);
            range_str_len = static_cast<int32_t>(range_str_len * mbmaxlen);
            min_str_len = range_str_len;
            max_str_len = range_str_len;
            if (OB_ISNULL(min_str_buf = (char*)temp_allocator.alloc(min_str_len))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("alloc memory failed", K(min_str_len));
            } else if (OB_ISNULL(max_str_buf = (char*)temp_allocator.alloc(max_str_len))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("alloc memory failed", K(max_str_len));
            } else if (OB_FAIL(ObCharset::like_range(cs_type,
                                                     pattern_str,
                                                     *(escape_str.ptr()),
                                                     static_cast<char*>(min_str_buf),
                                                     &min_str_len,
                                                     static_cast<char*>(max_str_buf),
                                                     &max_str_len))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("calc like range failed", K(ret), K(pattern_str), K(escape_str), K(cs_type));
            }
          }
        }
        if (OB_SUCC(ret)) {
          ObExprStrResAlloc res_alloc(expr, ctx);
          char *buf = NULL;
          if (is_start->get_int() == 1) {
            res_buf = min_str_buf;
            res_len = min_str_len;
          } else {
            res_buf = max_str_buf;
            res_len = max_str_len;
          }
          buf =  (char*)res_alloc.alloc(res_len);
          if (OB_ISNULL(buf)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("alloc memory failed", K(ret), K(min_str_len));
          } else {
            MEMCPY(buf, res_buf, res_len);
            expr_datum.set_string(buf, res_len);
          }
        }
      }
    }
  }
  return ret;
}

int ObExprInnerDecodeLike::calc_result_typeN(ObExprResType &type,
                                          ObExprResType *types_stack,
                                          int64_t param_num,
                                          common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  if (param_num != 6) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param num", K(ret), K(param_num));
  } else {
    const ObObj &dest_type = types_stack[3].get_param();
    const ObObj &dest_collation = types_stack[4].get_param();
    const ObObj &dest_length = types_stack[5].get_param();
    if (!types_stack[2].is_int() || !types_stack[3].is_int() || !types_stack[4].is_int() || !types_stack[5].is_int()
        || !types_stack[1].is_string_type() || !types_stack[0].is_string_type()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument, unexpected obj type", K(ret), K(types_stack[0]), K(types_stack[1]), K(types_stack[2]),
                                                      K(types_stack[3]), K(types_stack[4]), K(types_stack[5]));
    } else {
      ObObjType expected_type = static_cast<ObObjType>(dest_type.get_int());
      ObCollationType cs_type = static_cast<ObCollationType>(dest_collation.get_int());
      int64_t length = dest_length.get_int();
      type.set_type(expected_type);
      type.set_length(length);
      type.set_collation_level(CS_LEVEL_COERCIBLE);
      type.set_collation_type(cs_type);
      //pattern
      types_stack[0].set_calc_meta(types_stack[0].get_obj_meta());
      types_stack[0].set_calc_collation_type(types_stack[0].get_collation_type());
      types_stack[0].set_calc_collation_level(CS_LEVEL_COERCIBLE);
      //escape
      types_stack[1].set_calc_meta(types_stack[1].get_obj_meta());
      types_stack[1].set_calc_collation_type(types_stack[1].get_collation_type());
      types_stack[1].set_calc_collation_level(CS_LEVEL_COERCIBLE);
      //is_start
      types_stack[2].set_calc_type(ObIntType);
      //column type
      types_stack[3].set_calc_type(ObIntType);
      //column collation
      types_stack[4].set_calc_type(ObIntType);
      //column length
      types_stack[5].set_calc_type(ObIntType);
    }
  }
  return ret;
}

int ObExprInnerDecodeLike::cast_like_obj_if_needed(ObEvalCtx &ctx, const ObExpr &pattern_expr, ObDatum *pattern_datum,
                                                   const ObExpr &dst_expr, ObDatum * &cast_datum) {
  int ret = OB_SUCCESS;
  ObCastMode cm = CM_NONE;
  if (OB_FAIL(ObSQLUtils::get_default_cast_mode(false /*is_explicit_cast*/,
                                                0 /*result_flag*/,
                                                ctx.exec_ctx_.get_my_session(),
                                                cm))) {
    LOG_WARN("get default cast mode failed", K(ret));
  } else if (pattern_expr.datum_meta_.type_ == dst_expr.datum_meta_.type_
      && pattern_expr.datum_meta_.cs_type_ == dst_expr.datum_meta_.cs_type_) {
    cast_datum = pattern_datum;
  } else if (OB_ISNULL(ctx.datum_caster_) && OB_FAIL(ctx.init_datum_caster())) {
    LOG_WARN("init datum caster failed", K(ret));
  } else if (OB_FAIL(ctx.datum_caster_->to_type(dst_expr.datum_meta_, pattern_expr, cm, cast_datum, ctx.get_batch_idx()))) {
    LOG_WARN("fail to dynamic cast", K(ret));
  }
  return ret;
}

int ObExprInnerDecodeLike::get_pattern_prefix_len(const ObCollationType &cs_type, 
                                                  const ObString &escape_str, 
                                                  const ObString &pattern_str,
                                                  int32_t &pattern_prefix_len)
{
  int ret = OB_SUCCESS;
  int64_t mbmaxlen = 1;
  pattern_prefix_len = 0;
  if (OB_NOT_NULL(pattern_str.ptr()) && OB_NOT_NULL(escape_str.ptr()) && escape_str.length() == 1 &&
      cs_type != CS_TYPE_INVALID && cs_type < CS_TYPE_MAX) {
    if (OB_FAIL(ObCharset::get_mbmaxlen_by_coll(cs_type, mbmaxlen))) {
      LOG_WARN("fail to get mbmaxlen", K(ret), K(cs_type));
    } else {
      ObArenaAllocator allocator;
      size_t pattern_len = pattern_str.length();
      pattern_len = static_cast<int32_t>(pattern_len * mbmaxlen);
      size_t min_str_len = pattern_len;
      size_t max_str_len = pattern_len;
      size_t prefix_len = pattern_len;
      char *min_str_buf = NULL;
      char *max_str_buf = NULL;
      if (OB_ISNULL(min_str_buf = (char *)allocator.alloc(min_str_len))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("no enough memory", K(ret), K(pattern_len));
      } else if (OB_ISNULL(max_str_buf = (char *)allocator.alloc(max_str_len))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("no enough memory", K(ret), K(pattern_len));
      } else if (OB_FAIL(ObCharset::like_range(cs_type, pattern_str, *(escape_str.ptr()),
                                       min_str_buf, &min_str_len,
                                       max_str_buf, &max_str_len,
                                       &prefix_len))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to retrive like range", K(ret));
      } else {
        pattern_prefix_len = ObCharset::strlen_char(cs_type, min_str_buf, prefix_len);
      }
    }
  }
  return ret;
}
}
}