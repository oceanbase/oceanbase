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
 * This file contains implementation for _st_point.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_PRIV_ST_POINT_
#define OCEANBASE_SQL_OB_EXPR_PRIV_ST_POINT_

#include "sql/engine/expr/ob_expr_operator.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{
class ObExprPrivSTPoint : public ObFuncExprOperator
{
public:
  explicit ObExprPrivSTPoint(common::ObIAllocator &alloc);
  virtual ~ObExprPrivSTPoint();
  virtual int calc_result_typeN(ObExprResType& type,
                                ObExprResType* types,
                                int64_t param_num,
                                common::ObExprTypeCtx& type_ctx)
                                const override;
  static int eval_priv_st_point(const ObExpr &expr,
                               ObEvalCtx &ctx,
                               ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  static int string_to_double(const common::ObString &in_str, ObCollationType cs_type, double &res);
  DISALLOW_COPY_AND_ASSIGN(ObExprPrivSTPoint);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_PRIV_ST_POINT_