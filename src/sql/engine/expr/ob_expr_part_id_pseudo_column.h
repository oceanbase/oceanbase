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

// Provide the function of calculating partition id in the pdml function,
// the specific calculation method is:
// 1. When the child operator (such as Table scan) calculates a row,
//    fills the corresponding partition id into the ObExecContext in ObExprCtx
// 2. ObExprPartIdPseudoColumn expression directly obtains the corresponding partition id from ObExprCtx
namespace oceanbase {
namespace sql {
using namespace oceanbase::common;

class ObExprPartIdPseudoColumn : public ObFuncExprOperator {
public:
  explicit ObExprPartIdPseudoColumn(ObIAllocator& alloc);
  virtual ~ObExprPartIdPseudoColumn();

  virtual int calc_result_type0(ObExprResType& type, ObExprTypeCtx& type_ctx) const;

  virtual int calc_result0(ObObj& result, ObExprCtx& expr_ctx) const;
  // 3.0 engine
  virtual int cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const override;
  static int eval_part_id(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprPartIdPseudoColumn);
};
}  // namespace sql
}  // namespace oceanbase

#endif