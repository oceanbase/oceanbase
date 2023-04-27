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
#include <string.h>
#include "objit/common/ob_item_type.h"
#include "sql/engine/expr/ob_expr_int2ip.h"
//#include "sql/engine/expr/ob_expr_promotion_util.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

ObExprInt2ip::ObExprInt2ip(ObIAllocator &alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_INT2IP, N_INT2IP, 1, VALID_FOR_GENERATED_COL)
{
}

ObExprInt2ip::~ObExprInt2ip()
{
}

int ObExprInt2ip::calc(ObObj &result, const ObObj &text, common::ObExprStringBuf &string_buf)
{
  int ret = OB_SUCCESS;
  if (text.is_null()) {
    result.set_null();
  } else {
    TYPE_CHECK(text, ObIntType);
    int64_t int_val = text.get_int();
    if (OB_FAIL(int2ip(result, int_val, string_buf))) {
      LOG_WARN("fail to convert int 2 ip", K(ret), K(int_val));
    }
  }
  return ret;
}

int ObExprInt2ip::int2ip(ObObj &result, const int64_t int_val, ObExprStringBuf &string_buf)
{
  int ret = OB_SUCCESS;
  ObString str_result;
  char *buf = static_cast<char *>(string_buf.alloc(16));
  if (OB_ISNULL(buf)) {
    result.set_null();
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc memory failed", K(ret));
  } else {
    if ((int_val >> 32) > 0 || int_val < 0) {
      result.set_null();
    } else {
      in_addr addr;
      addr.s_addr = htonl(static_cast<uint32_t>(int_val));
      const char *iret = inet_ntop(AF_INET, &addr, buf, 16);
      // Fixme : The problem with inet_ntop() is that it is available starting from Windows Vista,
      // but the minimum supported version is Windows 2000.
      if (OB_ISNULL(iret)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("fail to inet_ntop", K(ret), K(int_val), K(addr.s_addr));
      } else {
        str_result.assign_ptr(buf, static_cast<ObString::obstr_size_t>(strlen(buf)));
        result.set_varchar(str_result);
      }
    }
  }
  return ret;
}

int ObExprInt2ip::int2ip(ObDatum &result, const int64_t int_val)
{
  int ret = OB_SUCCESS;
  ObString str_result = result.get_string();
  char *buf = str_result.ptr();
  if ((int_val >> 32) > 0 || int_val < 0) {
    result.set_null();
  } else {
    in_addr addr;
    addr.s_addr = htonl(static_cast<uint32_t>(int_val));
    const char *iret = inet_ntop(AF_INET, &addr, buf,  16);
    if (OB_ISNULL(iret)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("fail to inet_ntop", K(ret), K(int_val), K(addr.s_addr));
    } else {
      str_result.assign_ptr(buf, static_cast<ObString::obstr_size_t>(strlen(buf)));
      result.set_string(str_result);
    }
  }

  return ret;
}

int ObExprInt2ip::cg_expr(ObExprCGCtx &op_cg_ctx,
                          const ObRawExpr &raw_expr,
                          ObExpr &rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (rt_expr.arg_cnt_ != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("int2ip expr should have one param", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of int2ip expr is null", K(ret), K(rt_expr.args_));
  } else {
    CK(ObIntType == rt_expr.args_[0]->datum_meta_.type_);
    rt_expr.eval_func_ = ObExprInt2ip::int2ip_varchar;
  }
  return ret;
}

int ObExprInt2ip::int2ip_varchar(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.eval_param_value(ctx))) {
    LOG_WARN("int2ip expr eval param value failed", K(ret));
  } else {
    ObDatum &text = expr.locate_param_datum(ctx, 0);
    if (text.is_null()) {
      expr_datum.set_null();
    } else {
      CK(expr.res_buf_len_ >= 16);
      int64_t int_val = text.get_int();
      if (OB_FAIL(int2ip(expr_datum, int_val))) {
        LOG_WARN("fail to convert int 2 ip", K(ret), K(int_val));
      }
    }
  }
  return ret;
}

}
}

