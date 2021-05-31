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

#include "sql/engine/expr/ob_expr_part_id_pseudo_column.h"

namespace oceanbase {
namespace sql {
using namespace oceanbase::common;

ObExprPartIdPseudoColumn::ObExprPartIdPseudoColumn(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_PDML_PARTITION_ID, N_PDML_PARTITION_ID, 0, NOT_ROW_DIMENSION)
{
  // do nothing
}

ObExprPartIdPseudoColumn::~ObExprPartIdPseudoColumn()
{
  // do nothing
}

int ObExprPartIdPseudoColumn::calc_result_type0(ObExprResType& type, ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  type.set_int();
  return ret;
}

int ObExprPartIdPseudoColumn::calc_result0(ObObj& result, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  int64_t partition_id = expr_ctx.pdml_partition_id_;
  if (partition_id == OB_INVALID_INDEX_INT64) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get pdml partition from ObExprCtx", K(ret));
  } else {
    result.set_int(partition_id);
  }
  return ret;
}

int ObExprPartIdPseudoColumn::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = ObExprPartIdPseudoColumn::eval_part_id;
  return ret;
}

int ObExprPartIdPseudoColumn::eval_part_id(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  UNUSED(expr);
  UNUSED(ctx);
  UNUSED(expr_datum);
  return OB_SUCCESS;
}

}  // namespace sql
}  // namespace oceanbase
