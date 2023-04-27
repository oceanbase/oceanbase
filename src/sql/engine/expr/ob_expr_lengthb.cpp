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

#include "sql/engine/expr/ob_expr_lengthb.h"
#include "sql/engine/expr/ob_expr_util.h"
#include <string.h>
#include "lib/oblog/ob_log.h"
#include "share/object/ob_obj_cast.h"
#include "objit/common/ob_item_type.h"
//#include "sql/engine/expr/ob_expr_promotion_util.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprLengthb::ObExprLengthb(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_LENGTHB, N_LENGTHB, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprLengthb::~ObExprLengthb()
{
}

int ObExprLengthb::calc_result_type1(ObExprResType &type, ObExprResType &text,
                                     ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo *session = type_ctx.get_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else {
    const ObAccuracy &acc =
      ObAccuracy::DDL_DEFAULT_ACCURACY2[common::ORACLE_MODE][common::ObNumberType];
    type.set_number();
    type.set_scale(acc.get_scale());
    type.set_precision(acc.get_precision());
    if (!text.is_string_type()) {
      text.set_calc_type(ObVarcharType);
      text.set_calc_collation_type(session->get_nls_collation());
      text.set_calc_collation_level(CS_LEVEL_IMPLICIT);
    }
    if (text.get_calc_meta().is_text()) {
      ret = OB_CLOB_ONLY_SUPPORT_WITH_MULTIBYTE_FUN;
      LOG_WARN("CLOB or NCLOB in multibyte character set not supported", K(text), K(ret));
      LOG_USER_ERROR(OB_CLOB_ONLY_SUPPORT_WITH_MULTIBYTE_FUN);
    }
    OX(ObExprOperator::calc_result_flag1(type, text));
  }

  return ret;
}

int ObExprLengthb::calc(ObObj &result, const ObObj &text, ObExprCtx &expr_ctx)
{
  int ret = OB_SUCCESS;
  ObObjTypeClass type_class = ob_obj_type_class(text.get_type());
  if (text.is_text()) {
    ret = OB_CLOB_ONLY_SUPPORT_WITH_MULTIBYTE_FUN;
    LOG_WARN("CLOB or NCLOB in multibyte character set not supported", K(text), K(ret));
    LOG_USER_ERROR(OB_CLOB_ONLY_SUPPORT_WITH_MULTIBYTE_FUN);
    result.set_null();
  } else if (!ob_is_castable_type_class(type_class)) {
    result.set_null();
  } else {
    ObString m_text = text.get_string();
    number::ObNumber num;
    if (OB_FAIL(num.from(static_cast<int64_t>(m_text.length()), *(expr_ctx.calc_buf_)))) {
      LOG_WARN("copy number fail", K(ret));
    } else {
      result.set_number(num);
    }
  }
  UNUSED(expr_ctx);
  return ret;
}

int ObExprLengthb::calc_lengthb_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(1 != expr.arg_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected arg cnt", K(ret), K(expr.arg_cnt_));
  } else {
    ObDatum *arg0 = NULL;
    ObObjTypeClass type_class = ob_obj_type_class(expr.args_[0]->datum_meta_.type_);
    if (OB_FAIL(expr.eval_param_value(ctx, arg0))) {
      LOG_WARN("eval_param_value failed", K(ret));
    } else if (arg0->is_null() || !ob_is_castable_type_class(type_class)) {
      res_datum.set_null();
    } else {
      int64_t byte_len = static_cast<int64_t>(arg0->len_);
      if (ob_is_text_tc(expr.args_[0]->datum_meta_.type_)) {
        ObLobLocatorV2 locator(arg0->get_string(), expr.args_[0]->obj_meta_.has_lob_header());
        if (OB_FAIL(locator.get_lob_data_byte_len(byte_len))) {
          LOG_WARN("get lob data byte length failed", K(ret));
        }
      }
      number::ObNumber num;
      ObNumStackOnceAlloc tmp_alloc;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(num.from(byte_len, tmp_alloc))) {
        LOG_WARN("copy number fail", K(ret));
      } else {
        res_datum.set_number(num);
      }
    }
  }
  return ret;
}

int ObExprLengthb::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                          ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_lengthb_expr;
  return ret;
}

}
}
