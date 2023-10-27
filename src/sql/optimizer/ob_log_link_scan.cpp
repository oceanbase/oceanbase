/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_OPT
#include "sql/optimizer/ob_log_link_scan.h"
#include "sql/resolver/expr/ob_expr_info_flag.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::sql::log_op_def;

namespace oceanbase
{
namespace sql
{

ObLogLinkScan::ObLogLinkScan(ObLogPlan &plan)
  : ObLogLink(plan)
{}

int ObLogLinkScan::get_op_exprs(ObIArray<ObRawExpr*> &all_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(append(all_exprs, select_exprs_))) {
    LOG_WARN("failed to push back select exprs", K(ret));
  } else if (OB_FAIL(ObLogicalOperator::get_op_exprs(all_exprs))) {
    LOG_WARN("failed to get op exprs", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObLogLinkScan::allocate_expr_post(ObAllocExprContext &ctx)
{
  int ret = OB_SUCCESS;
  const ObSelectStmt *stmt = NULL;
  if (OB_ISNULL(get_plan()) ||
      OB_ISNULL(stmt = static_cast<const ObSelectStmt *>(get_plan()->get_stmt()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_plan()), K(stmt), K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_select_item_size(); ++i) {
    ObRawExpr *expr = stmt->get_select_item(i).expr_;
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null expr", K(ret));
    } else if (OB_FAIL(mark_expr_produced(expr,
                                          branch_id_,
                                          id_,
                                          ctx))) {
      LOG_WARN("failed to mark expr as produced", K(branch_id_), K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObLogicalOperator::allocate_expr_post(ctx))) {
      LOG_WARN("failed to allocate_expr_post", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
