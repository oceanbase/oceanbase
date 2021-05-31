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

#ifndef _OB_EXPR_NOT_IN_H_
#define _OB_EXPR_NOT_IN_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase {
namespace sql {
// class ObExprNotIn: public ObVectorExprOperator
//{
// public:
//  explicit  ObExprNotIn(common::ObIAllocator &alloc);
//  virtual ~ObExprNotIn() {};
//
//  virtual int calc_resultN(common::ObObj &result,
//                           const common::ObObj *objs,
//                           int64_t param_num,
//                           common::ObExprCtx &expr_ctx) const;
//
//  virtual int calc_result_typeN(ObExprResType &type,
//                                ObExprResType *types,
//                                int64_t param_num,
//                                common::ObExprTypeCtx &type_ctx) const;
//
// private:
//  // disallow copy
//  ObExprNotIn(const ObExprNotIn &other);
//  ObExprNotIn &operator=(const ObExprNotIn &other);
// protected:
//  // data members
//};

}
}  // namespace oceanbase
#endif /* _OB_EXPR_NOT_IN_H_ */
