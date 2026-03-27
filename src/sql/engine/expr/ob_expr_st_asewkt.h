/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 * This file contains implementation for st_asewkt.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_ST_ASEWKT_
#define OCEANBASE_SQL_OB_EXPR_ST_ASEWKT_

#include "sql/engine/expr/ob_expr_operator.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

class ObExprPrivSTAsEwkt : public ObFuncExprOperator
{
public:
  static const int DEFAULT_DIGITS_IN_DOUBLE = 15;
  explicit ObExprPrivSTAsEwkt(common::ObIAllocator &alloc);
  virtual ~ObExprPrivSTAsEwkt();
  virtual int calc_result_typeN(ObExprResType& type,
                                ObExprResType* types,
                                int64_t param_num,
                                common::ObExprTypeCtx& type_ctx)
                                const override;
  virtual int calc_resultN(common::ObObj &result,
                           const common::ObObj *objs,
                           int64_t param_num,
                           common::ObExprCtx &expr_ctx) const override;
  static int eval_priv_st_asewkt(const ObExpr &expr,
                            ObEvalCtx &ctx,
                            ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprPrivSTAsEwkt);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_ST_ASEWKT_