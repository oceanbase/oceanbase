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

#ifndef _OB_EXPR_SEQ_NEXT_VALUE_H
#define _OB_EXPR_SEQ_NEXT_VALUE_H
#include "sql/engine/expr/ob_expr_operator.h"
#include "share/ob_i_sql_expression.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObSchemaGetterGuard;
}
}

namespace sql
{
class ObPhysicalPlanCtx;
class ObExprPLSeqNextval : public ObFuncExprOperator
  {
  public:
    explicit  ObExprPLSeqNextval(common::ObIAllocator &alloc);
    virtual ~ObExprPLSeqNextval();
    int calc_result_type1(ObExprResType &type,
                          ObExprResType &type1,
                          common::ObExprTypeCtx &type_ctx) const;
    virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                        const ObRawExpr &raw_expr,
                        ObExpr &rt_expr) const override;
    static int eval_pl_seq_next_val(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  private:
    int get_schema_guard(ObExecContext *exec_ctx,
                         share::schema::ObSchemaGetterGuard *&schema_guard) const;
    // disallow copy
    DISALLOW_COPY_AND_ASSIGN(ObExprPLSeqNextval);

  };
}//end namespace sql
}//end namespace oceanbase
#endif
