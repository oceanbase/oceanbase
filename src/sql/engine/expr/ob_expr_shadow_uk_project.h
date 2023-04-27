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

#ifndef SQL_ENGINE_EXPR_OB_EXPR_SHADOW_UK_PROJECT_H_
#define SQL_ENGINE_EXPR_OB_EXPR_SHADOW_UK_PROJECT_H_
#include "sql/engine/expr/ob_expr_operator.h"
namespace oceanbase
{
namespace sql
{
class ObExprShadowUKProject : public ObExprOperator
{
public:
  ObExprShadowUKProject(common::ObIAllocator &alloc)
      : ObExprOperator(alloc, T_OP_SHADOW_UK_PROJECT, N_SHADOW_UK_PROJECTOR, MORE_THAN_ONE, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION, INTERNAL_IN_MYSQL_MODE)
  {
  }
  virtual ~ObExprShadowUKProject()
  {
  }
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const
  {
    UNUSED(type_ctx);
    int ret = common::OB_SUCCESS;
    if (OB_ISNULL(types) || OB_UNLIKELY(param_num <= 1)) {
      ret = common::OB_INVALID_ARGUMENT;
      SQL_ENG_LOG(WARN, "invalid argument", K(types), K(param_num));
    } else {
      type = types[param_num - 1];
    }
    return ret;
  }

  virtual int cg_expr(ObExprCGCtx &ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  static int shadow_uk_project(const ObExpr &expr,
                               ObEvalCtx &ctx,
                               ObDatum &datum);
};
}  // namespace sql
}  // namespace oceanbase
#endif /* SQL_ENGINE_EXPR_OB_EXPR_SHADOW_UK_PROJECT_H_ */
