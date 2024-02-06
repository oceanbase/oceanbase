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

#include "sql/engine/expr/ob_expr_length.h"

#include <string.h>
#include "lib/oblog/ob_log.h"
#include "share/object/ob_obj_cast.h"
#include "objit/common/ob_item_type.h"
//#include "sql/engine/expr/ob_expr_promotion_util.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprLength::ObExprLength(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_LENGTH, N_LENGTH, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprLength::~ObExprLength()
{
}

int ObExprLength::calc_result_type1(ObExprResType &type, ObExprResType &text,
                                    ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo *session = type_ctx.get_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else {
    if (lib::is_oracle_mode()) {
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
    } else {
      type.set_int();
      type.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].scale_);
      type.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].precision_);
      if (ob_is_text_tc(text.get_type()) && (session->get_exec_min_cluster_version() >= CLUSTER_VERSION_4_1_0_0)) {
        // no need to do cast, save memory
      } else {
        text.set_calc_type(common::ObVarcharType);
      }
    }
    OX(ObExprOperator::calc_result_flag1(type, text));
  }
  return ret;
}

int ObExprLength::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (rt_expr.arg_cnt_ != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("length expr should have one param", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of length expr is null", K(ret), K(rt_expr.args_));
  } else {
    ObObjType text_type = rt_expr.args_[0]->datum_meta_.type_;
    ObObjTypeClass type_class = ob_obj_type_class(text_type);

    if (!ob_is_castable_type_class(type_class)) {
      rt_expr.eval_func_ = ObExprLength::calc_null;
    } else if (lib::is_oracle_mode()) {
      rt_expr.eval_func_ = ObExprLength::calc_oracle_mode;
    } else {
      if (op_cg_ctx.session_->get_exec_min_cluster_version() < CLUSTER_VERSION_4_1_0_0) {
        CK(ObVarcharType == text_type);
      }
      rt_expr.eval_func_ = ObExprLength::calc_mysql_mode;
    }
  }
  return ret;
}

int ObExprLength::calc_null(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  UNUSED(expr);
  UNUSED(ctx);
  expr_datum.set_null();
  return OB_SUCCESS;
}
int ObExprLength::calc_oracle_mode(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *text_datum = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, text_datum))) {
    LOG_WARN("eval param value failed", K(ret));
  } else if (text_datum->is_null()) {
    expr_datum.set_null();
  } else {
    ObString m_text = text_datum->get_string();
    int64_t c_len = 0;
    if (!is_lob_storage(expr.args_[0]->datum_meta_.type_)) {
      c_len = ObCharset::strlen_char(expr.args_[0]->datum_meta_.cs_type_, m_text.ptr(),
                                    static_cast<int64_t>(m_text.length()));
    } else if (OB_FAIL(ObTextStringHelper::get_char_len(ctx, *text_datum, expr.args_[0]->datum_meta_,
                       expr.args_[0]->obj_meta_.has_lob_header(), c_len))) {
      LOG_WARN("failed to get char len for lob type", K(ret), K(expr.args_[0]->datum_meta_.type_));
    }
    if (OB_SUCC(ret)) {
      ObNumStackOnceAlloc tmp_alloc;
      number::ObNumber num;
      if (OB_FAIL(num.from(static_cast<int64_t>(c_len), tmp_alloc))) {
        LOG_WARN("copy number fail", K(ret));
      } else {
        expr_datum.set_number(num);
      }
    }
  }
  return ret;
}
int ObExprLength::calc_mysql_mode(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *text_datum = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, text_datum))) {
    LOG_WARN("eval param value failed", K(ret));
  } else if (text_datum->is_null()) {
    expr_datum.set_null();
  } else if (!is_lob_storage(expr.args_[0]->datum_meta_.type_)) {
    expr_datum.set_int(static_cast<int64_t>(text_datum->len_));
  } else { // text tc only
    ObLobLocatorV2 locator(text_datum->get_string(), expr.args_[0]->obj_meta_.has_lob_header());
    int64_t lob_data_byte_len = 0;
    if (OB_FAIL(locator.get_lob_data_byte_len(lob_data_byte_len))) {
      LOG_WARN("get lob data byte length failed", K(ret), K(locator));
    } else {
      expr_datum.set_int(static_cast<int64_t>(lob_data_byte_len));
    }
  }
  return ret;
}

}
}
