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

#ifndef SRC_SQL_ENGINE_EXPR_OB_EXPR_PL_SQLCODE_SQL_ERRM_H_
#define SRC_SQL_ENGINE_EXPR_OB_EXPR_PL_SQLCODE_SQL_ERRM_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprPLSQLCodeSQLErrm : public ObFuncExprOperator
{
  OB_UNIS_VERSION(1);
public:
  explicit ObExprPLSQLCodeSQLErrm(common::ObIAllocator &alloc);
  virtual ~ObExprPLSQLCodeSQLErrm();
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;

  virtual int assign(const ObExprOperator &other);

  void set_is_sqlcode(bool is_sqlcode) { is_sqlcode_ = is_sqlcode; }
  int cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const;
  static int eval_pl_sql_code_errm(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);

private:
  bool is_sqlcode_; // TRUE代表获取SQLCODE, FALSE代表获取SQLERRM
  DISALLOW_COPY_AND_ASSIGN(ObExprPLSQLCodeSQLErrm);
};

}
}

#endif /* SRC_SQL_ENGINE_EXPR_OB_EXPR_PL_SQLCODE_SQL_ERRM_H_ */
