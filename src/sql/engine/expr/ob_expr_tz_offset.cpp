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
#include "sql/engine/expr/ob_expr_tz_offset.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "lib/time/ob_time_utility.h"
#include "lib/ob_name_def.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/timezone/ob_time_convert.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

const int64_t ORACLE_TZ_OFFSET_MAX_LENGTH = 7;//[+|-]TZH:TZM

ObExprTzOffset::ObExprTzOffset(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_TZ_OFFSET, N_TZ_OFFSET, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

int ObExprTzOffset::calc_result_type1(ObExprResType &type,
                                      ObExprResType &input,
                                      common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!input.is_varchar_or_char()
      && !input.is_null())) {
    ret = common::OB_ERR_INVALID_TYPE_FOR_ARGUMENT;

    LOG_WARN("invalid type", K(input.get_type()));
  } else {
    CK(OB_NOT_NULL(type_ctx.get_session()));
    if (OB_SUCC(ret)) {
      type.set_varchar();
      type.set_collation_type(type_ctx.get_session()->get_nls_collation());
      type.set_collation_level(CS_LEVEL_IMPLICIT);
      type.set_length(ORACLE_TZ_OFFSET_MAX_LENGTH);
      type.set_length_semantics(type_ctx.get_session()->get_actual_nls_length_semantics());

      input.set_calc_type(ObVarcharType);
      input.set_calc_collation_type(ObCharset::get_system_collation());
      input.set_calc_collation_level(CS_LEVEL_IMPLICIT);
    }
  }
  return ret;
}

int ObExprTzOffset::eval_tz_offset_val(const ObString &in_tz_str,
                                       ObSQLSessionInfo &my_session,
                                       ObIAllocator &alloc,
                                       ObString &res_str)
{
  int ret = OB_SUCCESS;
  ObString trimed_tz_str = in_tz_str;
  trimed_tz_str = trimed_tz_str.trim();
  int32_t offset_sec = 0;
  int32_t offset_min = 0;
  int ret_more = OB_SUCCESS;

  if (OB_FAIL(ObTimeConverter::str_to_offset(trimed_tz_str, offset_sec, ret_more,
                                            is_oracle_mode(), true))) {
    if (ret != OB_ERR_UNKNOWN_TIME_ZONE) {
      LOG_WARN("fail to convert str_to_offset", K(trimed_tz_str), K(ret));
    } else if (ret_more != OB_SUCCESS) {
      ret = ret_more;
      LOG_WARN("invalid time zone hour or minute", K(trimed_tz_str), K(ret));
    }
  } else {
    offset_min = offset_sec / SECS_PER_MIN;
  }

  if (OB_ERR_UNKNOWN_TIME_ZONE == ret) {
    ObTime ob_time(DT_TYPE_ORACLE_TTZ);
    MEMCPY(ob_time.tz_name_, trimed_tz_str.ptr(), trimed_tz_str.length());
    ob_time.tz_name_[trimed_tz_str.length()] = '\0';
    ob_time.is_tz_name_valid_ = true;
    ObTimeConvertCtx cvrt_ctx(TZ_INFO(&my_session), true);
    ObOTimestampData tmp_ot_data;
    tmp_ot_data.time_ctx_.tz_desc_ = 0;
    tmp_ot_data.time_ctx_.store_tz_id_ = 0;
    tmp_ot_data.time_us_ = ObTimeUtility::current_time();

    if (OB_FAIL(ObTimeConverter::otimestamp_to_ob_time(ObTimestampLTZType, tmp_ot_data,
                                                       NULL, ob_time))) {
      LOG_WARN("failed to convert otimestamp_to_ob_time", K(ret));
    } else if (OB_FAIL(ObTimeConverter::str_to_tz_offset(cvrt_ctx, ob_time))) {
      LOG_WARN("failed to convert string to tz_offset", K(ret));
    } else {
      offset_min = ob_time.parts_[DT_OFFSET_MIN];
      LOG_DEBUG("finish str_to_tz_offset", K(ob_time), K(tmp_ot_data));
    }
  }


  if (OB_SUCC(ret)) {
    char *res_ptr = NULL;
    int64_t pos = 0;
    const char *fmt_str = (offset_min < 0 ? "-%02d:%02d" : "+%02d:%02d");
    // 即使alloc分配的空间比实际需要的多应该也没关系
    // ORACLE_TZ_OFFSET_MAX_LENGTH为7, 很小的值
    if (OB_ISNULL(res_ptr = static_cast<char*>(alloc.alloc(ORACLE_TZ_OFFSET_MAX_LENGTH)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to alloc", "size", ORACLE_TZ_OFFSET_MAX_LENGTH , K(ret));
    } else if (OB_FAIL(databuff_printf(res_ptr, ORACLE_TZ_OFFSET_MAX_LENGTH, pos, fmt_str,
                        abs(offset_min) / 60, abs(offset_min) % 60))) {
      LOG_WARN("Failed to databuff_printf", K(offset_min), K(ret));
    } else {
      res_str.assign_ptr(res_ptr, pos);
    }
  }
  return ret;
}

int ObExprTzOffset::eval_tz_offset(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *tz = NULL;
  if (OB_FAIL(expr.eval_param_value(ctx, tz))) {
    LOG_WARN("eval param_value failed", K(ret));
  } else if (tz->is_null()) {
    res.set_null();
  } else {
    const ObString &tz_str = tz->get_string();
    ObString res_str_utf8;
    ObString res_str;
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    ObIAllocator &calc_alloc = alloc_guard.get_allocator();
    ObExprStrResAlloc res_alloc(expr, ctx);
    const ObCollationType out_cs_type = expr.datum_meta_.cs_type_;
    CK(OB_NOT_NULL(ctx.exec_ctx_.get_my_session()));
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(eval_tz_offset_val(tz_str, *ctx.exec_ctx_.get_my_session(),
                                          calc_alloc, res_str_utf8))) {
      LOG_WARN("eval_tz_offset_val failed", K(ret), K(tz_str));
    } else if (OB_FAIL(ObExprUtil::convert_string_collation(
                          res_str_utf8, ObCharset::get_system_collation(),
                          res_str, out_cs_type, calc_alloc))) {
      LOG_WARN("convert string coll failed", K(ret), K(res_str_utf8), K(out_cs_type));
    } else if (OB_FAIL(ObExprUtil::deep_copy_str(res_str, res_str, res_alloc))) {
      LOG_WARN("copy str failed", K(ret));
    } else {
      res.set_string(res_str);
    }
  }
  return ret;
}

int ObExprTzOffset::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                            ObExpr &expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  CK(1 == expr.arg_cnt_);
  CK(OB_NOT_NULL(expr.args_) && OB_NOT_NULL(expr.args_[0]));
  CK(ob_is_varchar_or_char(expr.args_[0]->datum_meta_.type_,
                           expr.args_[0]->datum_meta_.cs_type_));
  CK(ob_is_varchar_or_char(expr.datum_meta_.type_,
                           expr.datum_meta_.cs_type_));
  OX(expr.eval_func_ = eval_tz_offset);
  return ret;
}
}
}
