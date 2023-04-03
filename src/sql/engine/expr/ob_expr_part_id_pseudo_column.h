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

#ifndef _OCEANBASE_SQL_ENGINE_EXPR_PART_ID_PSEUDO_COLUMN_FOR_PDML_H_
#define _OCEANBASE_SQL_ENGINE_EXPR_PART_ID_PSEUDO_COLUMN_FOR_PDML_H_

#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_expr_res_type.h"
#include "share/ob_i_sql_expression.h"

// 为pdml功能中提供计算partition id的功能，具体的计算方法为：
// 1. child算子（例如Table scan）在计算一行时，将对应的partition id填充到ObExprCtx中的ObExecContext中
// 2. ObExprPartIdPseudoColumn表达式直接从ObExprCtx中获得对应的partition id
namespace oceanbase
{
namespace sql
{
class ObExprPartIdPseudoColumn: public ObFuncExprOperator
{
public:
  explicit ObExprPartIdPseudoColumn(common::ObIAllocator &alloc);
  virtual ~ObExprPartIdPseudoColumn();

  virtual int calc_result_type0(ObExprResType &type,
                                common::ObExprTypeCtx &type_ctx) const;

  // 3.0 engine
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_part_id(const ObExpr &expr, ObEvalCtx &ctx, common::ObDatum &expr_datum);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprPartIdPseudoColumn);
};
} // namespace sql
} // namespace oceanbase

#endif
