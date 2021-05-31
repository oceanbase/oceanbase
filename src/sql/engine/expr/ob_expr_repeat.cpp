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
#include "sql/engine/expr/ob_expr_repeat.h"

#include <limits.h>
#include <string.h>

#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase {
namespace sql {}
}  // namespace oceanbase

ObExprRepeat::ObExprRepeat(ObIAllocator& alloc) : ObStringExprOperator(alloc, T_FUN_SYS_REPEAT, N_REPEAT, 2)
{}

ObExprRepeat::~ObExprRepeat()
{}

int ObExprRepeat::calc_result_type2(
    ObExprResType& type, ObExprResType& text, ObExprResType& count, ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  text.set_calc_type(common::ObVarcharType);
  count.set_calc_type(common::ObIntType);
  // Set cast mode for %count parameter, truncate string to integer.
  type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_STRING_INTEGER_TRUNC);
  // repeat is mysql only epxr.
  CK(lib::is_mysql_mode());
  if (OB_SUCC(ret)) {
    ObObjType res_type = ObMaxType;
    if (text.is_null() || count.is_null()) {
      res_type = ObVarcharType;
    } else if (count.is_literal()) {
      const ObObj& obj = count.get_param();
      ObArenaAllocator alloc(ObModIds::OB_SQL_RES_TYPE);
      const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(type_ctx.get_session());
      int64_t cur_time = 0;
      ObCastCtx cast_ctx(&alloc, &dtc_params, cur_time, CM_WARN_ON_FAIL, CS_TYPE_INVALID);
      int64_t count_val = 0;
      EXPR_GET_INT64_V2(obj, count_val);
      res_type = get_result_type_mysql(text.get_length() * count_val);
    } else {
      res_type = ObLongTextType;
    }
    type.set_type(res_type);
    type.set_collation_level(text.get_collation_level());
    type.set_collation_type(text.get_collation_type());
    if (ObVarcharType == type.get_type()) {
      /*
       * In terms of repeat(A, B), we can know the actual length when B is literal
       * constant indeed.
       * But we do not want to do this since if we did, the plan cache will not be
       * hit. So, no matter B comes from literal constant or column we just set
       * length to be  OB_MAX_VARCHAR_LENGTH here.
       */
      type.set_length(MAX_CHAR_LENGTH_FOR_VARCAHR_RESULT);
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(aggregate_charsets_for_string_result(type, &text, 1, type_ctx.get_coll_type()))) {
      LOG_WARN("failed to aggregate charsets for string result", K(ret));
    } else {
      text.set_calc_collation_level(type.get_collation_level());
      text.set_calc_collation_type(type.get_collation_type());
    }
  }
  return ret;
}

int ObExprRepeat::calc(ObObj& result, const ObObj& text, const ObObj& count, ObIAllocator* allocator,
    ObObjType res_type, const int64_t max_result_size)
{
  int ret = OB_SUCCESS;
  if (text.is_null() || count.is_null()) {
    result.set_null();
  } else {
    TYPE_CHECK(text, ObVarcharType);
    TYPE_CHECK(count, ObIntType);
    ObString local_text = text.get_string();
    int64_t local_count = count.get_int();
    if (OB_FAIL(calc(result, res_type, local_text, local_count, allocator, max_result_size))) {
      LOG_WARN("Failed to cacl result");
    }
  }
  return ret;
}

int ObExprRepeat::calc_result2(ObObj& result, const ObObj& text, const ObObj& count, ObExprCtx& expr_ctx) const
{
  EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
  int ret = OB_SUCCESS;
  int64_t max_allow_packet_size = 0;
  if (OB_ISNULL(expr_ctx.my_session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null session in repeat function", K(ret));
  } else if (OB_FAIL(expr_ctx.my_session_->get_max_allowed_packet(max_allow_packet_size))) {
    if (OB_ENTRY_NOT_EXIST == ret) {  // for compatibility
      ret = OB_SUCCESS;
      max_allow_packet_size = OB_MAX_VARCHAR_LENGTH;
    } else {
      LOG_WARN("Failed to get max allow packet size", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(calc(result, text, count, expr_ctx.calc_buf_, get_result_type().get_type(), max_allow_packet_size))) {
      LOG_WARN("Calc result error", K(ret));
    } else if (OB_LIKELY(!result.is_null())) {
      result.set_collation(result_type_);
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObExprRepeat::repeat(ObString& output, bool& is_null, const ObString& text, const int64_t count,
    common::ObIAllocator& allocator, const int64_t max_result_size)
{
  is_null = false;
  int ret = OB_SUCCESS;
  if (count <= 0 || text.length() <= 0 || max_result_size <= 0) {
    output.assign_ptr(NULL, 0);
  } else {
    int64_t length = static_cast<int64_t>(text.length());

    // Safe length check
    if ((length > max_result_size / count) || (length > INT_MAX / count)) {
      LOG_WARN(
          "Result of repeat was larger than max_allow_packet_size", K(ret), K(length), K(count), K(max_result_size));
      LOG_USER_WARN(OB_ERR_FUNC_RESULT_TOO_LARGE, "repeat", static_cast<int>(max_result_size));
      is_null = true;
    } else {
      // avoid realloc
      if (1 == count) {
        output = text;
      } else {
        int64_t tot_length = length * count;
        char* buf = static_cast<char*>(allocator.alloc(tot_length));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("alloc memory failed", K(ret), K(tot_length));
        } else {
          int64_t tmp_count = count;
          char* tmp_buf = buf;
          while (tmp_count--) {
            MEMCPY(tmp_buf, text.ptr(), length);
            tmp_buf += length;
          }
          output.assign_ptr(buf, static_cast<int32_t>(tot_length));
        }
      }
    }
  }
  return ret;
}

int ObExprRepeat::calc(ObObj& result, const ObObjType res_type, const ObString& text, const int64_t count,
    ObIAllocator* allocator, const int64_t max_result_size)
{
  int ret = OB_SUCCESS;
  ObString output;
  bool is_null = false;
  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null allocator", K(ret), K(allocator));
  } else if (false == ob_is_string_type(res_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("make sure res_type is string type", K(ret), K(res_type));
  } else if (OB_FAIL(repeat(output, is_null, text, count, *allocator, max_result_size))) {
    LOG_WARN("do repeat failed", K(ret));
  } else {
    if (is_null) {
      result.set_null();
    } else {
      result.set_string(res_type, output);
    }
  }
  return ret;
}

int ObExprRepeat::cg_expr(ObExprCGCtx&, const ObRawExpr&, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  CK(2 == rt_expr.arg_cnt_);
  rt_expr.eval_func_ = eval_repeat;
  return ret;
}

int ObExprRepeat::eval_repeat(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* text = NULL;
  ObDatum* count = NULL;
  int64_t max_size = 0;
  if (OB_FAIL(expr.args_[0]->eval(ctx, text)) || OB_FAIL(expr.args_[1]->eval(ctx, count))) {
    LOG_WARN("evaluate parameters failed", K(ret));
  } else if (text->is_null() || count->is_null()) {
    expr_datum.set_null();
  } else if (OB_FAIL(ctx.exec_ctx_.get_my_session()->get_max_allowed_packet(max_size))) {
    LOG_WARN("get max length failed", K(ret));
  } else {
    ObExprStrResAlloc expr_res_alloc(expr, ctx);
    bool is_null = false;
    ObString output;
    if (OB_FAIL(repeat(output, is_null, text->get_string(), count->get_int(), expr_res_alloc, max_size))) {
      LOG_WARN("do repeat failed", K(ret));
    } else {
      if (is_null) {
        expr_datum.set_null();
      } else {
        expr_datum.set_string(output);
      }
    }
  }
  return ret;
}
