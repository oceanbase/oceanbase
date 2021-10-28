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

#ifndef _OB_EXPR_FUNC_SEQ_SETVAL_H
#define _OB_EXPR_FUNC_SEQ_SETVAL_H
#include "sql/engine/expr/ob_expr_operator.h"
#include "share/ob_i_sql_expression.h"

namespace oceanbase {
namespace sql {
class ObPhysicalPlanCtx;
class ObExprFuncSeqSetval : public ObFuncExprOperator {
public:
  explicit ObExprFuncSeqSetval(common::ObIAllocator& alloc);
  virtual ~ObExprFuncSeqSetval();
  int calc_result_typeN(ObExprResType& type, ObExprResType* types, int64_t param_num, ObExprTypeCtx& type_ctx) const;
  int calc_resultN(ObObj& res, const ObObj* objs, int64_t param_num, ObExprCtx& ctx) const;
  int cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const;
  static int calc_sequence_setval(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res);

private:
  // disallow copy
  static int acquire_sequence_schema(const uint64_t tenant_id, ObExecContext& exec_ctx, int64_t seq_id, const share::schema::ObSequenceSchema*& seq_schema);
  static int number_from_obj(const ObObj& obj, common::number::ObNumber& number, common::ObIAllocator& allocator);
  DISALLOW_COPY_AND_ASSIGN(ObExprFuncSeqSetval);
};
}  // end namespace sql
}  // end namespace oceanbase
#endif
