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

namespace oceanbase
{
namespace sql
{
using namespace oceanbase::common;

ObExprPartIdPseudoColumn::ObExprPartIdPseudoColumn(ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_PDML_PARTITION_ID, N_PDML_PARTITION_ID, 0, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION,
                       INTERNAL_IN_MYSQL_MODE, INTERNAL_IN_ORACLE_MODE)
{
  // do nothing
}

ObExprPartIdPseudoColumn::~ObExprPartIdPseudoColumn()
{
  // do nothing
}

int ObExprPartIdPseudoColumn::calc_result_type0(ObExprResType &type,
                                                ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  type.set_int(); // 默认partition id的数据类型是int64_t
  return ret;
}

int ObExprPartIdPseudoColumn::cg_expr(ObExprCGCtx &op_cg_ctx,
                                      const ObRawExpr &raw_expr,
                                      ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = ObExprPartIdPseudoColumn::eval_part_id;
  return ret;
}

int ObExprPartIdPseudoColumn::eval_part_id(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  // partition id 伪列表达式比较特殊，其对应的值是提前被设置到expr对应的datum中
  // 每次都是直接通过local_expr_dutam获得，直接访问
  UNUSED(expr);
  UNUSED(ctx);
  UNUSED(expr_datum);
  return OB_SUCCESS;
}

} // end oceanbase
} // end sql
