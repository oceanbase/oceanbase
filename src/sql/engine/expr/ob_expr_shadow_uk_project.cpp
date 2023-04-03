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

#include "sql/engine/expr/ob_expr_shadow_uk_project.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
int ObExprShadowUKProject::cg_expr(ObExprCGCtx &,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(raw_expr);
  CK(rt_expr.arg_cnt_ >= 2);
  rt_expr.eval_func_ = ObExprShadowUKProject::shadow_uk_project;

  return ret;
}

int ObExprShadowUKProject::shadow_uk_project(const ObExpr &expr,
                                             ObEvalCtx &ctx,
                                             ObDatum &datum)
{
  int ret = OB_SUCCESS;
  bool need_shadow_columns = false;
  if (OB_FAIL(expr.eval_param_value(ctx))) {
    LOG_WARN("evaluate parameters values failed", K(ret));
  } else if (lib::is_mysql_mode()){
    // mysql兼容：只要unique index key中有null列，则需要填充shadow列
    bool rowkey_has_null = false;
    for (int64_t i = 0; !rowkey_has_null && i < expr.arg_cnt_ - 1; i++) {
      ObDatum &v = expr.locate_param_datum(ctx, i);
      rowkey_has_null = v.is_null();
    }
    need_shadow_columns = rowkey_has_null;
  } else {
    // oracle兼容：只有unique index key全为null列时，才需要填充shadow列
    bool is_rowkey_all_null = true;
    for (int64_t i = 0; is_rowkey_all_null && i < expr.arg_cnt_ - 1; i++) {
      ObDatum &v = expr.locate_param_datum(ctx, i);
      is_rowkey_all_null = v.is_null();
    }
    need_shadow_columns = is_rowkey_all_null;
  }
  if (!need_shadow_columns) {
    datum.set_null();
  } else {
    ObDatum &v = expr.locate_param_datum(ctx, expr.arg_cnt_ - 1);
    datum.set_datum(v);
  }

  return ret;
}

}  // namespace sql
}  // namespace oceanbase
