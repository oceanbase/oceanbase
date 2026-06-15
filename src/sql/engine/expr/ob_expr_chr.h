/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_CHR_
#define OCEANBASE_SQL_ENGINE_EXPR_CHR_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprChr : public ObStringExprOperator
{
public:
  explicit ObExprChr(common::ObIAllocator &alloc);
  virtual ~ObExprChr();
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *texts,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                          ObExpr &rt_expr) const;
  static int calc_chr_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprChr);
};

class ObExprNchr : public ObStringExprOperator
{
public:
  explicit ObExprNchr(common::ObIAllocator &alloc);
  virtual ~ObExprNchr();
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *texts,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                          ObExpr &rt_expr) const;
  static int calc_nchr_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprNchr);
};

} //end namespace sql
} //end namespace oceanbase

#endif /* OCEANBASE_SQL_ENGINE_EXPR_CHR_ */
