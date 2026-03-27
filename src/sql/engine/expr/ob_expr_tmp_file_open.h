/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_TMP_FILE_OPEN_H_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_TMP_FILE_OPEN_H_

#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/container/ob_iarray.h"

namespace oceanbase
{
namespace sql
{

class ObExprTmpFileOpen : public ObFuncExprOperator
{
public:
  explicit ObExprTmpFileOpen(common::ObIAllocator &alloc);
  virtual ~ObExprTmpFileOpen();

  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const override;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr, ObExpr &rt_expr) const override;

  static int eval_tmp_file_open(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);

private:
  static int create_temp_file(int64_t &fd);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprTmpFileOpen);
};

}
}

#endif // OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_TMP_FILE_OPEN_H_