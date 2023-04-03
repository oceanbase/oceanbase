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

#ifndef _OB_EXPR_TO_TYPE_H_
#define _OB_EXPR_TO_TYPE_H_

#include "share/object/ob_obj_cast.h"
#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{

class ObExprToType: public ObFuncExprOperator
{
  OB_UNIS_VERSION_V(1);
public:
  ObExprToType();
  explicit ObExprToType(common::ObIAllocator &alloc);
  virtual ~ObExprToType() {};

  virtual int calc_result_type1(ObExprResType &type, ObExprResType &type1, common::ObExprTypeCtx &type_ctx) const;

  virtual int assign(const ObExprOperator &other);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const;
public:
  OB_INLINE void set_expect_type(common::ObObjType expect_type) { expect_type_ = expect_type; }
  OB_INLINE void set_cast_mode(common::ObCastMode cast_mode) { cast_mode_ = cast_mode; }
private:
  int calc_result_type_for_literal(ObExprResType &type, ObExprResType &type1, common::ObExprTypeCtx &type_ctx) const;
  int calc_result_type_for_column(ObExprResType &type, ObExprResType &type1, common::ObExprTypeCtx &type_ctx) const;
private:
  // data members
  common::ObObjType expect_type_;
  common::ObCastMode cast_mode_;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprToType);
};

}
}
#endif  /* _OB_EXPR_TO_TYPE_H_ */
