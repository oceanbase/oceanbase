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
#include "sql/engine/expr/ob_expr_sys_connect_by_path.h"
#include "sql/engine/expr/ob_expr_concat.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/engine/ob_exec_context.h"
#include "share/ob_i_sql_expression.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
ObExprSysConnectByPath::ObExprSysConnectByPath(common::ObIAllocator &alloc)
  : ObExprOperator(alloc, T_FUN_SYS_CONNECT_BY_PATH, N_SYS_CONNECT_BY_PATH, 2, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION) {};

int ObExprSysConnectByPath::calc_result_type2(ObExprResType &type,
                                              ObExprResType &type1,
                                              ObExprResType &type2,
                                              common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  if (is_oracle_mode()) {
    ObSEArray<ObExprResType*, 2, ObNullAllocator> params;
    OZ (params.push_back(&type1));
    OZ (params.push_back(&type2));
    OZ (aggregate_string_type_and_charset_oracle(*type_ctx.get_session(), params, type,
                                                 PREFER_VAR_LEN_CHAR));
    OZ (deduce_string_param_calc_type_and_charset(*type_ctx.get_session(), type, params));
    type.set_calc_meta(type); // old engine need to set the calc_meta of type
    type.set_length(OB_MAX_ORACLE_VARCHAR_LENGTH);
  } else {
    type.set_varchar();
    type.set_length(OB_MAX_VARCHAR_LENGTH);//length值result的row count相关，分析阶段无法确定
    ObExprResType types[2];
    type1.set_calc_type(ObVarcharType);
    type2.set_calc_type(ObVarcharType);
    types[0] = type1;
    types[1] = type2;
    if (OB_FAIL(aggregate_charsets_for_string_result(type, types, 2, type_ctx.get_coll_type()))) {
      LOG_WARN("fail to aggregate charset", K(type1), K(type2), K(ret));
    } else if (OB_FAIL(aggregate_charsets_for_comparison(type, types, 2, type_ctx.get_coll_type()))) {
      LOG_WARN("fail to aggregate charset", K(type1), K(type2), K(ret));
    }
  }
  return ret;
}

int ObExprSysConnectByPath::StringContain(const ObString &src_str,
                                          const ObString &sub_str, bool &is_contain,
                                          ObCollationType cs_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(CS_TYPE_INVALID == cs_type)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected collation type", K(cs_type), K(ret));
  } else {
    int64_t start_pos = 1;
    uint32_t idx = ObCharset::locate(cs_type,
                                     src_str.ptr(), src_str.length(),
                                     sub_str.ptr(), sub_str.length(),
                                     start_pos);
    is_contain = (idx != 0);
  }
  return ret;
}

int ObExprSysConnectByPath::calc_sys_path(const ObExpr &expr, ObEvalCtx &ctx,
                                          ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  UNUSED(ctx);
  UNUSED(expr_datum);
  return ret;
}

int ObExprSysConnectByPath::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                                ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = ObExprSysConnectByPath::calc_sys_path;
  return ret;
}

int ObExprSysConnectByPath::eval_sys_connect_by_path(const ObExpr &expr,
  ObEvalCtx &ctx, ObDatum &expr_datum, ObNLConnectByOpBase *connect_by_op)
{
  int ret = OB_SUCCESS;
  ObDatum *left = NULL;
  ObDatum *right = NULL;
  if (OB_FAIL(expr.eval_param_value(ctx, left, right))) {
    LOG_WARN("eval param failed", K(ret));
  } else {
    ObObj node_res;
    ObObj result;
    ObString parent_path;
    ObString val_str = left->is_null() ? ObString("") : left->get_string();
    ObString separator_str = right->get_string();
    bool is_contain = false;
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    ObIAllocator &tmp_alloc = alloc_guard.get_allocator();
    char *res_buf = NULL;
    int64_t res_length = 0;
    if (OB_FAIL(StringContain(val_str, separator_str, is_contain, expr.datum_meta_.cs_type_))) {
      LOG_WARN("fail to judge contain string", K(val_str), K(separator_str), K(ret));
    } else if (OB_UNLIKELY(is_contain)) {
      ret = OB_ERR_INVALID_SEPARATOR;
      LOG_WARN("separator can't be part of column value", K(ret), K(val_str), K(separator_str));
    } else if (OB_FAIL(ObExprConcat::calc(
                       node_res, separator_str, val_str, &tmp_alloc, false, 0))) {
      LOG_WARN("fail to concat string", K(right), K(left), K(ret));
    } else if (OB_FAIL(connect_by_op->get_sys_parent_path(parent_path))) {
      LOG_WARN("fail to get parent path", K(ret));
    } else if (FALSE_IT(res_length = parent_path.length() + node_res.get_string().length())) {
    } else if (res_length > OB_MAX_ORACLE_VARCHAR_LENGTH) {
      ret = OB_ERR_TOO_LONG_STRING_IN_CONCAT;
      LOG_WARN("sys cnnt by path result too long", K(ret), K(res_length));
    } else if (OB_ISNULL(res_buf = expr.get_str_res_mem(ctx, res_length))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate failed", K(ret));
    } else {
      memcpy(res_buf, parent_path.ptr(), parent_path.length());
      memcpy(res_buf + parent_path.length(), node_res.get_string().ptr(),
             node_res.get_string().length());
      expr_datum.set_string(res_buf, res_length);
      if (OB_FAIL(connect_by_op->set_sys_current_path(node_res.get_string(),
                                                      expr_datum.get_string()))) {
        LOG_WARN("fail to set cur node path", K(result), K(ret));
      }
    }
  }
  return ret;
}
