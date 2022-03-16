/*
 * Copyright 2014-2021 Alibaba Inc. All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * ob_expr_uuid_short.h is for uuid_short function
 *
 * Date: 2021/8/20
 *
 * Authors:
 *     ailing.lcq<ailing.lcq@alibaba-inc.com>
 *
 */
#ifndef _OB_EXPR_UUID_SHORT_H_
#define _OB_EXPR_UUID_SHORT_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase {
namespace sql {
class ObExprUuidShort : public ObFuncExprOperator {
public:
  explicit ObExprUuidShort(common::ObIAllocator &alloc);
  virtual ~ObExprUuidShort();

  virtual int calc_result_type0(ObExprResType &type, common::ObExprTypeCtx &type_ctx) const;
  virtual int calc_result0(common::ObObj &result, common::ObExprCtx &expr_ctx) const;

  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const override;

  static int eval_uuid_short(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);

private:
  static uint64_t generate_uuid_short();
  DISALLOW_COPY_AND_ASSIGN(ObExprUuidShort) const;
};

inline int ObExprUuidShort::calc_result_type0(ObExprResType &type, common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  type.set_uint64();
  return common::OB_SUCCESS;
}

}  // namespace sql
}  // namespace oceanbase
#endif /* _OB_EXPR_UUID_SHORT_H_ */

// select uuid_short();