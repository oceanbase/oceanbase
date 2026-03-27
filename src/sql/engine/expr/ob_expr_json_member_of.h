/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_EXPR_JSON_MEMBER_OF_H_
#define OCEANBASE_SQL_OB_EXPR_JSON_MEMBER_OF_H_

#include "sql/engine/expr/ob_expr_operator.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

class ObExprJsonMemberOf : public ObFuncExprOperator
{
public:
  explicit ObExprJsonMemberOf(common::ObIAllocator &alloc);
  virtual ~ObExprJsonMemberOf();
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                ObExprTypeCtx &type_ctx) const override;
  static int check_json_member_of_array(const ObIJsonBase *json_a,
                                      const ObIJsonBase *json_b,
                                      bool &is_member_of);
  static int eval_json_member_of(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprJsonMemberOf);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_JSON_MEMBER_OF_H_