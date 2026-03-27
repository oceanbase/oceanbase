/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXTRACT_CERT_EXPIRED_TIME_H
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXTRACT_CERT_EXPIRED_TIME_H
#include "sql/engine/expr/ob_expr_operator.h"
namespace oceanbase {
namespace sql {

class ObExprExtractExpiredTime : public ObFuncExprOperator
{
public:
  ObExprExtractExpiredTime();
  explicit ObExprExtractExpiredTime(common::ObIAllocator& alloc);
  virtual ~ObExprExtractExpiredTime();
  virtual int calc_result_type1(ObExprResType &type,
              ObExprResType &text,
              common::ObExprTypeCtx &type_ctx) const;
  static int eval_extract_cert_expired_time(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprExtractExpiredTime);
};

}
}
#endif