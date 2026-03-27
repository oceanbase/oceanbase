/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OCEANBASE_SQL_OB_EXPR_TO_BLOB_H_
#define _OCEANBASE_SQL_OB_EXPR_TO_BLOB_H_
#include "sql/engine/expr/ob_expr_operator.h"
#include "share/object/ob_obj_cast.h"

namespace oceanbase
{
namespace sql
{

// https://docs.oracle.com/cd/B28359_01/server.111/b28286/functions186.htm#SQLRF30029
class ObExprToBlob : public ObStringExprOperator
{
public:
  explicit ObExprToBlob(common::ObIAllocator &alloc);
  virtual ~ObExprToBlob();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &text,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const;
  static int eval_to_blob(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprToBlob);
};

}
}

#endif // OB_EXPR_TO_BLOB_H_
