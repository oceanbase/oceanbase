/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_EXPR_SYS_MAKEXML_H_
#define OCEANBASE_SQL_OB_EXPR_SYS_MAKEXML_H_

#include "sql/engine/expr/ob_expr_operator.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{
// Internal expression used for generate xmltype data from blob or udt with schema
class ObExprSysMakeXML : public ObFuncExprOperator
{
public:
  explicit ObExprSysMakeXML(common::ObIAllocator &alloc);
  virtual ~ObExprSysMakeXML();

  virtual int calc_result_typeN(ObExprResType& type,
                                ObExprResType* types,
                                int64_t param_num,
                                common::ObExprTypeCtx& type_ctx) const override;
  static int eval_sys_makexml(const ObExpr &expr, ObEvalCtx &ctx,  ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  // If xml schema implemented, use rt ctx to store parsed xml schema. Not used currently.
  virtual bool need_rt_ctx() const override { return true; }
  private:
    DISALLOW_COPY_AND_ASSIGN(ObExprSysMakeXML);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_SYS_MAKEXML_H_