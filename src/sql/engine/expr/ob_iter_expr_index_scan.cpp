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
#include "sql/engine/expr/ob_iter_expr_index_scan.h"
#include "common/row/ob_row_iterator.h"
#include "share/ob_i_sql_expression.h"
namespace oceanbase {
using namespace common;
namespace sql {
int ObIndexScanIterExpr::get_next_row(ObIterExprCtx& expr_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObNewRow* cur_row = NULL;
  if (OB_ISNULL(expr_ctx.get_index_scan_iters())) {
    ret = OB_NOT_INIT;
    LOG_WARN("subplan iters is null");
  } else if (OB_UNLIKELY(iter_idx_ < 0) || OB_UNLIKELY(iter_idx_ >= expr_ctx.get_index_scan_iters()->count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iterator index is unexpected", K_(iter_idx), K(expr_ctx.get_index_scan_iters()->count()));
  } else if (OB_FAIL(expr_ctx.get_index_scan_iters()->at(iter_idx_)->get_next_row(cur_row))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("get current row failed", K(ret));
    }
  } else {
    row = cur_row;
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
