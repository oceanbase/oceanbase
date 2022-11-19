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

#ifndef OB_SQL_ENGINE_EXPR_MAKEDATE_
#define OB_SQL_ENGINE_EXPR_MAKEDATE_

#include "lib/allocator/ob_allocator.h"
#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprMakedate : public ObFuncExprOperator
{
public:
  explicit  ObExprMakedate(common::ObIAllocator &alloc);
  virtual ~ObExprMakedate();
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &year,
                                ObExprResType &day,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual common::ObCastMode get_cast_mode() const { return CM_STRING_INTEGER_TRUNC;}
  static int calc_makedate(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &result);
  template <typename T>
  static int calc(T &res, int64_t year, int64_t day);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprMakedate);
};
}
}

#endif /* OB_SQL_ENGINE_EXPR_MAKEDATE_ */
