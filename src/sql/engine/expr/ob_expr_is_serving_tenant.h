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

#ifndef SQL_ENGINE_EXPR_OB_EXPR_IS_SERVING_TENANT_
#define SQL_ENGINE_EXPR_OB_EXPR_IS_SERVING_TENANT_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprIsServingTenant : public ObFuncExprOperator
{
public:
  explicit  ObExprIsServingTenant(common::ObIAllocator &alloc);
  virtual ~ObExprIsServingTenant();

  virtual int calc_result_type3(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                ObExprResType &type3,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  static int eval_is_serving_tenant(const ObExpr &expr,
                                    ObEvalCtx &ctx,
                                    ObDatum &expr_datum);
private:
  static int check_serving_tenant(bool &serving,
                                  ObExecContext &exec_ctx,
                                  const common::ObString &ip,
                                  const int64_t port,
                                  const uint64_t tenant_id);
  /* functions */
  /* variables */
  DISALLOW_COPY_AND_ASSIGN(ObExprIsServingTenant);
};
}
}
#endif /* SQL_ENGINE_EXPR_OB_EXPR_IS_SERVING_TENANT_ */

