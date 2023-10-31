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

#ifndef _OB_EXPR_UUID_SHORT_H_
#define _OB_EXPR_UUID_SHORT_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprUuidShort : public ObFuncExprOperator
{
public:
  explicit ObExprUuidShort(common::ObIAllocator &alloc);
  virtual ~ObExprUuidShort();

  virtual int calc_result_type0(ObExprResType &type,
                              common::ObExprTypeCtx &type_ctx) const;

  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  static int eval_uuid_short(const ObExpr &expr,
                      ObEvalCtx &ctx,
                      ObDatum &expr_datum);
private:
  static uint64_t generate_uuid_short();
  DISALLOW_COPY_AND_ASSIGN(ObExprUuidShort) const;
};

inline int ObExprUuidShort::calc_result_type0(ObExprResType &type,
                                         common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  type.set_uint64();
  type.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObUInt64Type].scale_);
  type.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObUInt64Type].precision_);
  return common::OB_SUCCESS;
}

}
}
#endif  /* _OB_EXPR_UUID_SHORT_H_ */

// select uuid_short();
