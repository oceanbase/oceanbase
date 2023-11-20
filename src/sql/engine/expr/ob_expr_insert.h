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

#ifndef OCEANBASE_SQL_OB_EXPR_INSERT_H_
#define OCEANBASE_SQL_OB_EXPR_INSERT_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprInsert : public ObStringExprOperator
{
public:
  explicit  ObExprInsert(common::ObIAllocator &alloc);
  virtual ~ObExprInsert();
  virtual int calc_result(common::ObObj &result,
                          const common::ObObj &text,
                          const common::ObObj &start_pos,
                          const common::ObObj &length,
                          const common::ObObj &replace_text,
                          common::ObExprCtx &expr_ctx) const;
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types_stack,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;
  static int calc(common::ObObj &result,
                  const common::ObObj &text,
                  const common::ObObj &start_pos,
                  const common::ObObj &length,
                  const common::ObObj &replace_text,
                  common::ObExprCtx &expr_ctx,
                  common::ObCollationType cs_type);
  static int calc(common::ObString &result,
                  const common::ObString &text,
                  const int64_t start_pos,
                  const int64_t expect_length_of_str,
                  const common::ObString &replace_text,
                  common::ObIAllocator &allocator,
                  common::ObCollationType cs_type);
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int calc_expr_insert(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  DECLARE_SET_LOCAL_SESSION_VARS;

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprInsert);
};
}
}

#endif /* OCEANBASE_SQL_OB_EXPR_INSERT_H_ */
