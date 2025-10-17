/**
 * Copyright (c) 2025 OceanBase
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
#include "sql/engine/expr/ob_expr_format_profile.h"
#include "share/diagnosis/ob_runtime_profile.h"
#include "sql/engine/ob_exec_context.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
typedef ObOpProfile<ObMetric> ObProfile;
ObExprFormatProfile::ObExprFormatProfile(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_FORMAT_PROFILE, N_FORMAT_PROFILE, PARAM_NUM_UNKNOWN,
                         NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{}

ObExprFormatProfile::~ObExprFormatProfile()
{}

int ObExprFormatProfile::calc_result_typeN(ObExprResType &result_type, ObExprResType *types_stack,
                                           int64_t param_num, ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(types_stack)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null types", K(ret));
  } else if (param_num != 1 && param_num != 2) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("should has 1 or 2 parameters", K(param_num));
  } else if (!types_stack[0].is_varbinary()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("first param must be varbinary");
  } else if (param_num == 2 && !types_stack[1].is_int()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("second param must be int");
  } else {
    result_type.set_varchar();
    result_type.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
    result_type.set_length(OB_MAX_MYSQL_VARCHAR_LENGTH);
  }
  return ret;
}

int ObExprFormatProfile::format_profile(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObDatum *raw_profile = nullptr;
  ObDatum *metric_level = nullptr;
  const ObBasicSessionInfo *session_info = nullptr;
  if (OB_ISNULL(session_info = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is null", K(ret));
  } else if (OB_FAIL(expr.eval_param_value(ctx, raw_profile))) {
    LOG_WARN("evaluate parameter value failed", K(ret));
  } else if (raw_profile->is_null()) {
    expr_datum.set_null();
  } else if (expr.arg_cnt_ == 2 && OB_FAIL(expr.args_[1]->eval(ctx, metric_level))) {
    LOG_WARN("eval arg failed");
  } else {
    ObProfile *real_time_profile = nullptr;
    const char *json = nullptr;
    char *persist_profile = const_cast<char *>(raw_profile->get_string().ptr());
    const int64_t persist_len = raw_profile->get_string().length();
    metric::Level display_level = metric::Level::STANDARD;
    if (nullptr != metric_level && !metric_level->is_null()) {
      int64_t int_val = metric_level->get_int();
      if (int_val >= 0 && int_val <= 2) {
        display_level = static_cast<metric::Level>(metric_level->get_int());
      }
    }
    OZ(convert_persist_profile_to_realtime(
        persist_profile, persist_len, real_time_profile, &ctx.exec_ctx_.get_allocator()));
    OZ(real_time_profile->to_format_json(&ctx.exec_ctx_.get_allocator(), json, false, display_level));
    if (OB_SUCC(ret)) {
      expr_datum.set_string(json, strlen(json));
    }
  }
  return ret;
}

int ObExprFormatProfile::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr,
                                 ObExpr &rt_expr) const
{
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  rt_expr.eval_func_ = ObExprFormatProfile::format_profile;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase