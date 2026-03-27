/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_TMP_FILE_WRITE_H_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_TMP_FILE_WRITE_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{

class ObExprTmpFileWrite : public ObFuncExprOperator
{
public:
  explicit ObExprTmpFileWrite(common::ObIAllocator &alloc);
  virtual ~ObExprTmpFileWrite();

  virtual int calc_result_type2(ObExprResType &type, ObExprResType &type1, ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const override;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr, ObExpr &rt_expr) const override;

  static int eval_tmp_file_write(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);

private:
  static int write_temp_file(int64_t fd, const ObString &data_str, int64_t &file_length);
  static int validate_input_params(const ObDatum &fd_datum, const ObDatum &data_datum,
                                  int64_t &fd, ObString &data_str);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprTmpFileWrite);
};

}
}

#endif // OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_TMP_FILE_WRITE_H_