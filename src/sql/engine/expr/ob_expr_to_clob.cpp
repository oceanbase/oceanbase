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
#include "sql/engine/expr/ob_expr_to_clob.h"
#include "sql/session/ob_sql_session_info.h"
#include "objit/common/ob_item_type.h"
#include "lib/oblog/ob_log.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprToClob::ObExprToClob(ObIAllocator &alloc)
    : ObExprToCharCommon(alloc, T_FUN_SYS_TO_CLOB, N_TO_CLOB, 1, VALID_FOR_GENERATED_COL)
{
}

ObExprToClob::~ObExprToClob()
{
}

int ObExprToClob::calc_result_type1(ObExprResType &type,
                                    ObExprResType &text,
                                    common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  ObSessionNLSParams nls_param = type_ctx.get_session()->get_session_nls_params();

  if (OB_ISNULL(type_ctx.get_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else if (ob_is_null(text.get_type())
             || ob_is_string_tc(text.get_type())
             || ob_is_clob(text.get_type(), text.get_collation_type())
             || ob_is_raw(text.get_type())
             || ob_is_numeric_type(text.get_type())
             || ob_is_oracle_datetime_tc(text.get_type())
             || ob_is_rowid_tc(text.get_type())
             || ob_is_interval_tc(text.get_type())
             || text.is_xml_sql_type()) {
    type.set_clob();
    type.set_collation_level(CS_LEVEL_IMPLICIT);
    type.set_collation_type(nls_param.nls_collation_);
    text.set_calc_collation_type(nls_param.nls_collation_);
    if (!text.is_clob()) {
      text.set_calc_type(ObVarcharType);
      ObLength res_len = -1;
      ObExprResType tmp_calc_type; // for deducing res len
      tmp_calc_type.set_varchar(); // set calc type of param to tmp_calc_type
      tmp_calc_type.set_length_semantics(LS_BYTE); // length_semantics of res type clob is byte
      tmp_calc_type.set_collation_type(nls_param.nls_collation_);
      if (OB_FAIL(ObExprResultTypeUtil::deduce_max_string_length_oracle(
                  type_ctx.get_session()->get_dtc_params(), text, tmp_calc_type, res_len))) {
        LOG_WARN("fail to deduce result length", K(ret), K(text), K(type));
      } else {
        text.set_calc_length_semantics(text.is_character_type() ? text.get_length_semantics() : LS_BYTE);
        text.set_calc_length(text.is_character_type() ? text.get_length() : res_len);
        type.set_length(res_len);
      }
    } else {
      type.set_length(text.get_length());
    }
  } else {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("wrong type of argument in function to_clob", K(ret), K(text));
  }

  return ret;
}

int ObExprToClob::calc_to_clob_expr(const ObExpr &expr, ObEvalCtx &ctx,
                                    ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *arg = NULL;
  ObObjType input_type = expr.args_[0]->datum_meta_.type_;
  ObCollationType cs_type = expr.args_[0]->datum_meta_.cs_type_;
  if (OB_UNLIKELY(1 != expr.arg_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg cnt or arg res type", K(ret), K(expr.arg_cnt_));
  } else if (OB_FAIL(expr.eval_param_value(ctx, arg))) {
    LOG_WARN("eval param failed", K(ret));
  } else if (arg->is_null()) {
    res.set_null();
  } else if (ob_is_clob(input_type, cs_type) || expr.args_[0]->obj_meta_.is_xml_sql_type()) {
    // todo convert to clob for xml type
    res.set_datum(*arg);
  } else {
    ObString raw_string = arg->get_string();
    ObTextStringDatumResult str_result(expr.datum_meta_.type_, &expr, &ctx, &res);
    uint32_t result_len = 0;
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    char *buf = NULL;
    int64_t reserve_len = raw_string.length() * 4;
    if (OB_ISNULL(buf = (char*)temp_allocator.alloc(reserve_len))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc mem failed", K(ret));
    } else if (OB_FAIL(ObCharset::charset_convert(cs_type, raw_string.ptr(), raw_string.length(),
                                                  cs_type, buf, reserve_len, result_len, false, false,
                                                  ObCharset::is_cs_unicode(cs_type) ? 0xFFFD : '?'))) {
      LOG_WARN("charset convert failed", K(ret));
    } else {
      raw_string.assign_ptr(buf, result_len);
    }
    LOG_DEBUG("try convert param value", K(raw_string), K(ObHexStringWrap(raw_string)), K(cs_type));
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(str_result.init(raw_string.length()))) {
      LOG_WARN("init lob result failed");
    } else if (OB_FAIL(str_result.append(raw_string.ptr(), raw_string.length()))) {
      LOG_WARN("append lob result failed");
    } else {
      str_result.set_result();
    }
  }
  if (OB_SUCC(ret) && !res.is_null() && res.len_ > OB_MAX_LONGTEXT_LENGTH) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wrong length of result in function to_clob", K(ret), K(res.len_));
  }
  return ret;
}

int ObExprToClob::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_to_clob_expr;
  return ret;
}
} // end of sql
} // end of oceanbase
