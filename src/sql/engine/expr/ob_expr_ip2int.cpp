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
#include "sql/parser/ob_item_type.h"
#include "sql/engine/expr/ob_expr_ip2int.h"
//#include "sql/engine/expr/ob_expr_promotion_util.h"
#include "share/object/ob_obj_cast.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase {
namespace sql {

ObExprIp2int::ObExprIp2int(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_IP2INT, N_IP2INT, 1, NOT_ROW_DIMENSION)
{}

ObExprIp2int::~ObExprIp2int()
{}

int ObExprIp2int::calc(ObObj& result, const ObObj& text, ObExprCtx& expr_ctx)
{
  int ret = OB_SUCCESS;
  if (text.is_null()) {
    result.set_null();
  } else {
    TYPE_CHECK(text, ObVarcharType);
    ObString m_text = text.get_string();
    if (OB_FAIL(ip2int(result, m_text))) {
      LOG_WARN("fail to convert ip to int", K(ret), K(m_text));
    }
  }
  UNUSED(expr_ctx);
  return ret;
}

int ObExprIp2int::calc_result1(ObObj& result, const ObObj& text, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("varchar buffer not init", K(ret));
  } else if (OB_FAIL(calc(result, text, expr_ctx))) {
    LOG_WARN("fail to calc", K(ret), K(text));
  } else {
  }
  return ret;
}

template <typename T>
int ObExprIp2int::ip2int(T& result, const ObString& text)
{
  int ret = OB_SUCCESS;
  char buf[16];
  if (text.length() > 15) {
    LOG_WARN("ip format invalid", K(text));
    result.set_null();
  } else {
    MEMCPY(buf, text.ptr(), text.length());
    int len = text.length();
    buf[len] = '\0';
    int cnt = 0;
    for (int i = 0; OB_SUCC(ret) && i < len; ++i) {
      if (text.ptr()[i] == '.') {
        cnt++;
      } else {
      }
    }
    if (OB_FAIL(ret)) {
    } else if (cnt != 3) {
      LOG_WARN("ip format invalid", K(cnt));
      result.set_null();
    } else {
      struct in_addr addr;
      int err = inet_aton(buf, &addr);
      if (0 != err) {
        result.set_int(ntohl(addr.s_addr));
      } else {
        LOG_WARN("ip format invalid", K(err), K(buf));
        result.set_null();
      }
    }
  }
  return ret;
}

int ObExprIp2int::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (rt_expr.arg_cnt_ != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ip2int expr should have one param", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of ip2int expr is null", K(ret), K(rt_expr.args_));
  } else {
    CK(ObVarcharType == rt_expr.args_[0]->datum_meta_.type_);
    rt_expr.eval_func_ = ObExprIp2int::ip2int_varchar;
  }
  return ret;
}

int ObExprIp2int::ip2int_varchar(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.eval_param_value(ctx))) {
    LOG_WARN("ip2int expr eval param value failed", K(ret));
  } else {
    ObDatum& text = expr.locate_param_datum(ctx, 0);
    if (text.is_null()) {
      expr_datum.set_null();
    } else {
      ObString m_text = text.get_string();
      if (OB_FAIL(ip2int(expr_datum, m_text))) {
        LOG_WARN("fail to convert ip to int", K(ret), K(m_text));
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
