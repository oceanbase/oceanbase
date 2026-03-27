/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef SRC_SQL_ENGINE_EXPR_OB_EXPR_INSTRB_H_
#define SRC_SQL_ENGINE_EXPR_OB_EXPR_INSTRB_H_
#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{

class ObExprInstrb: public ObLocationExprOperator
{
public:
  ObExprInstrb();
  explicit  ObExprInstrb(common::ObIAllocator &alloc);
  virtual ~ObExprInstrb();
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *type_array,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                               ObExpr &rt_expr) const;
  virtual bool need_rt_ctx() const override { return true; }
  static int calc_instrb_expr_batch(const ObExpr &expr,
                                    ObEvalCtx &ctx,
                                    const ObBitVector &skip,
                                    const int64_t batch_size);
private:
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const;
  DISALLOW_COPY_AND_ASSIGN(ObExprInstrb);
};

}//end of namespace sql
}//end of namespace oceanbase

#endif /* SRC_SQL_ENGINE_EXPR_OB_EXPR_INSTRB_H_ */
