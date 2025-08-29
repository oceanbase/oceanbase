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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_TMP_FILE_READ_H_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_TMP_FILE_READ_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{

class ObExprTmpFileRead : public ObFuncExprOperator
{
public:
  explicit ObExprTmpFileRead(common::ObIAllocator &alloc);
  virtual ~ObExprTmpFileRead();

  virtual int calc_result_type3(ObExprResType &type, ObExprResType &type1, ObExprResType &type2, ObExprResType &type3,
                                common::ObExprTypeCtx &type_ctx) const override;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr, ObExpr &rt_expr) const override;

  static int eval_tmp_file_read(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);

private:
  static int read_temp_file(int64_t fd, int64_t offset, int64_t size, char *buffer);
  static int validate_input_params(const ObDatum &fd_datum, const ObDatum &offset_datum, const ObDatum &size_datum,
                                  int64_t &fd, int64_t &offset, int64_t &size);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprTmpFileRead);
};

}
}

#endif // OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_TMP_FILE_READ_H_