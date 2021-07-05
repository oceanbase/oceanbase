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

#ifndef _OB_EXPR_TO_SINGLE_TYPE_H
#define _OB_EXPR_TO_SINGLE_TYPE_H

#include "sql/engine/expr/ob_expr_operator.h"
namespace oceanbase {
namespace sql {

class ObExprToSingleByte : public ObFuncExprOperator {
public:
  explicit ObExprToSingleByte(common::ObIAllocator& alloc);
  virtual ~ObExprToSingleByte();
  int calc_result_type1(ObExprResType& type, ObExprResType& type1, common::ObExprTypeCtx& type_ctx) const;
  int calc_result1(common::ObObj& result, const common::ObObj& obj, common::ObExprCtx& expr_ctx) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprToSingleByte);
};

}  // namespace sql
}  // namespace oceanbase

#endif  // _OB_EXPR_TO_SINGLE_TYPE_H
