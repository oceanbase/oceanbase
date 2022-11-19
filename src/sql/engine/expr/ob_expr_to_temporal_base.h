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

#ifndef OCEANBASE_EXPR_TO_TEMPORAL_BASE_H
#define OCEANBASE_EXPR_TO_TEMPORAL_BASE_H

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{

class ObExprToTemporalBase: public ObFuncExprOperator
{
public:
  explicit ObExprToTemporalBase(common::ObIAllocator &alloc,
                                 ObExprOperatorType type,
                                 const char *name);
  virtual ~ObExprToTemporalBase() {}
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types_array,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int set_my_result_from_ob_time(common::ObExprCtx &expr_ctx,
                                         common::ObTime &ob_time,
                                         common::ObObj &result) const = 0;
  virtual common::ObObjType get_my_target_obj_type() const = 0;

  //engine3.0
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  virtual bool need_rt_ctx() const override { return true; }
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprToTemporalBase);
};


}
}

#endif // OCEANBASE_EXPR_TO_TEMPORAL_BASE_H
