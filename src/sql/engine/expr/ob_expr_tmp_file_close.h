/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_TMP_FILE_CLOSE_H_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_TMP_FILE_CLOSE_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{

class ObExprTmpFileClose : public ObFuncExprOperator
{
public:
  explicit ObExprTmpFileClose(common::ObIAllocator &alloc);
  virtual ~ObExprTmpFileClose();

  virtual int calc_result_type1(ObExprResType &type, ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const override;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr, ObExpr &rt_expr) const override;

  static int eval_tmp_file_close(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);

private:
  static int close_temp_file(int64_t fd);
  static int validate_input_param(const ObDatum &param_datum, int64_t &fd);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprTmpFileClose);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_TMP_FILE_CLOSE_H_