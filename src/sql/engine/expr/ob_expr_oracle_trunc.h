/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OCEANBASE_SQL_OB_EXPR_ORACLE_TRUNC_H_
#define _OCEANBASE_SQL_OB_EXPR_ORACLE_TRUNC_H_
#include "lib/ob_name_def.h"
#include "share/object/ob_obj_cast.h"
#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprOracleTrunc : public ObFuncExprOperator
{
public:
  explicit ObExprOracleTrunc(common::ObIAllocator &alloc);
  explicit ObExprOracleTrunc(common::ObIAllocator &alloc, const char *name);
  virtual ~ObExprOracleTrunc() {}
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *params,
                                int64_t params_count,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                            ObExpr &rt_expr) const;
  virtual bool need_rt_ctx() const override { return true; }
protected:
  int calc_with_date(common::ObObj &result,
                     const common::ObObj &source,
                     const common::ObObj &format,
                     common::ObExprCtx &expr_ctx) const;
  int calc_with_decimal(common::ObObj &result,
                        const common::ObObj &source,
                        const common::ObObj &format,
                        common::ObExprCtx &expr_ctx) const;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprOracleTrunc);
};

class ObExprTrunc : public ObExprOracleTrunc
{
public:
  explicit ObExprTrunc(common::ObIAllocator &alloc);
  virtual ~ObExprTrunc() {}

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprTrunc);
};


} //sql
} //oceanbase
#endif //_OCEANBASE_SQL_OB_EXPR_ORACLE_TRUNC_H_
