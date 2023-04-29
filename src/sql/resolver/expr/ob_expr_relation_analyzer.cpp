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

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/expr/ob_expr_relation_analyzer.h"
#include "lib/oblog/ob_log_module.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "common/ob_smart_call.h"
namespace oceanbase
{
using namespace common;
namespace sql
{
ObExprRelationAnalyzer::ObExprRelationAnalyzer()
{
}

/**
 * @brief ObExprRelationAnalyzer::pull_expr_relation_id_and_levels
 * ObColumnRefRawExpr:
 *   relation ids: the bit index of the table item that the column belongs to
 * Other Exprs:
 *   relation ids: the union of all param columns exprs' relation ids
 * @return
 */
int ObExprRelationAnalyzer::pull_expr_relation_id(ObRawExpr *expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret), K(expr));
  } else if (OB_FAIL(visit_expr(*expr))) {
    LOG_WARN("failed to pull expr relation id and levels", K(ret));
  }
  return ret;
}

int ObExprRelationAnalyzer::visit_expr(ObRawExpr &expr)
{
  int ret = OB_SUCCESS;
  int64_t param_count = expr.has_flag(IS_ONETIME) ? 1 : expr.get_param_count();
  if (!expr.is_column_ref_expr() &&
      T_PSEUDO_EXTERNAL_FILE_COL != expr.get_expr_type() &&
      T_ORA_ROWSCN != expr.get_expr_type()) {
    expr.get_relation_ids().reuse();
  }
  // not sure whether we should visit onetime exec param
  for (int64_t i = 0; OB_SUCC(ret) && i < param_count; ++i) {
    ObRawExpr *param = expr.has_flag(IS_ONETIME) ?
          static_cast<ObExecParamRawExpr &>(expr).get_ref_expr() :
          expr.get_param_expr(i);
    if (OB_ISNULL(param)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param expr is null", K(ret), K(param), K(i), K(expr));
    } else if (OB_FAIL(SMART_CALL(visit_expr(*param)))) {
      LOG_WARN("failed to visit param", K(ret));
    } else if (OB_FAIL(expr.get_relation_ids().add_members(param->get_relation_ids()))) {
      LOG_WARN("failed to add relation ids", K(ret));
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
