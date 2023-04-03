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
#include "sql/engine/expr/ob_iter_expr_range_param.h"
#include "sql/engine/ob_exec_context.h"
namespace oceanbase
{
using namespace common;
namespace sql
{
OB_SERIALIZE_MEMBER((ObIterExprRangeParam, ObIterExprOperator), start_index_, end_index_);

int ObIterExprRangeParam::get_next_row(ObIterExprCtx &expr_ctx, const common::ObNewRow *&result) const
{
  int ret = OB_SUCCESS;
  int64_t cur_cell_idx = 0;
  ObNewRow *cur_row = expr_ctx.get_cur_row();
  ObPhysicalPlanCtx *plan_ctx = expr_ctx.get_exec_context().get_physical_plan_ctx();
  if (OB_ISNULL(cur_row) || OB_ISNULL(plan_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current row is null", K(ret), K(cur_row), K(plan_ctx));
  }
  for (int64_t i = start_index_; OB_SUCC(ret) && i <= end_index_; ++i) {
    if (OB_UNLIKELY(i < 0) || OB_UNLIKELY(i >= plan_ctx->get_param_store().count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid param index", K(ret), K(i), K_(start_index), K_(end_index), K(plan_ctx->get_param_store().count()));
    } else if (OB_UNLIKELY(cur_cell_idx >= cur_row->get_count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("current cell index is invalid", K(ret), K(cur_cell_idx), K(cur_row->get_count()));
    } else {
      cur_row->get_cell(cur_cell_idx++) = plan_ctx->get_param_store().at(i);
    }
  }
  if (OB_SUCC(ret)) {
    result = cur_row;
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
