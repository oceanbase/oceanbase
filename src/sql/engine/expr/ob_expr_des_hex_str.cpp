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

#include "sql/engine/expr/ob_expr_des_hex_str.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase {
using namespace oceanbase::common;
namespace sql {
ObExprDesHexStr::ObExprDesHexStr(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_DES_HEX_STR, N_DES_HEX_STR, 1, NOT_ROW_DIMENSION)
{}

int ObExprDesHexStr::calc_result_type1(ObExprResType& type, ObExprResType& type1, common::ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = common::OB_SUCCESS;

  if (!type1.is_varchar()) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid type1 argument.", K(ret), K(type1));
  } else {
    type.set_varchar();
    type.set_collation_level(type1.get_collation_level());
    type.set_collation_type(type1.get_collation_type());
    type.set_scale(type1.get_scale());
  }

  return ret;
}

int ObExprDesHexStr::calc_result1(common::ObObj& result, const common::ObObj& obj, common::ObExprCtx& expr_ctx) const
{
  int ret = common::OB_SUCCESS;
  ObString obj_string;

  if (!obj.is_valid_type() || OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument.", K(ret), K(obj.is_valid_type()), KP(expr_ctx.calc_buf_));
  } else {
    obj_string = obj.get_varchar();
    if (OB_FAIL(deserialize_hex_cstr(obj_string.ptr(), obj_string.length(), *expr_ctx.calc_buf_, result))) {
      LOG_WARN("deserialize_hex_cstr obj_string failed.", K(ret));
    }
  }

  return ret;
}

int ObExprDesHexStr::deserialize_hex_cstr(
    const char* buf, int64_t buf_len, common::ObExprStringBuf& string_buf, common::ObObj& obj)
{
  int ret = common::OB_SUCCESS;

  int64_t pos = 0;
  int64_t ret_len = 0;
  char* res_buf = NULL;
  if (OB_ISNULL(buf) || buf_len < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments.", K(ret), K(buf_len));
  } else if (NULL == (res_buf = static_cast<char*>(string_buf.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to allocate memory for res_buf.", K(ret), K(buf_len));
  } else if (buf_len != (ret_len = common::str_to_hex(
                             buf, static_cast<int32_t>(buf_len), res_buf, static_cast<int32_t>(buf_len)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("transfer str to hex failed.", K(ret), K(buf), K(buf_len), K(ret_len));
  } else if (OB_FAIL(obj.deserialize(res_buf, ret_len / 2, pos))) {
    LOG_WARN("deserialize obj failed.", K(ret), K(buf), K(buf_len), K(pos));
  }

  return ret;
}

int ObExprDesHexStr::cg_expr(ObExprCGCtx&, const ObRawExpr&, ObExpr& expr) const
{
  int ret = OB_SUCCESS;
  CK(1 == expr.arg_cnt_);
  expr.eval_func_ = eval_des_hex_str;
  return ret;
}

int ObExprDesHexStr::eval_des_hex_str(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* in = NULL;
  ObIAllocator& alloc = ctx.get_reset_tmp_alloc();
  ObObj obj;
  if (OB_FAIL(expr.eval_param_value(ctx, in))) {
    LOG_WARN("evaluate parameter failed", K(ret));
  } else if (in->is_null()) {
    expr_datum.set_null();
  } else if (OB_FAIL(deserialize_hex_cstr(in->ptr_, in->len_, alloc, obj))) {
    LOG_WARN("unhex or deserialize failed", K(ret));
  } else {
    const int64_t len = std::max(128, in->len_ * 2);
    char* buf = expr.get_str_res_mem(ctx, len);
    int64_t pos = 0;
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else if (OB_FAIL(obj.print_plain_str_literal(buf, len, pos))) {
      LOG_WARN("print sql literal failed", K(ret), K(obj));
    } else {
      expr_datum.set_string(buf, pos);
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
