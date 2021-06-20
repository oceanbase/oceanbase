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

#ifndef _OB_EXPR_SPM_H
#define _OB_EXPR_SPM_H 1

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase {
namespace sql {

class ObExprSpmLoadPlans : public ObFuncExprOperator {
public:
  explicit ObExprSpmLoadPlans(common::ObIAllocator& alloc);
  virtual ~ObExprSpmLoadPlans();
  virtual int calc_result_typeN(
      ObExprResType& type, ObExprResType* types, int64_t param_num, common::ObExprTypeCtx& type_ctx) const;
  virtual int calc_resultN(
      common::ObObj& result, const common::ObObj* objs, int64_t param_num, common::ObExprCtx& expr_ctx) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprSpmLoadPlans);
};

class ObExprSpmAlterBaseline : public ObFuncExprOperator {
public:
  explicit ObExprSpmAlterBaseline(common::ObIAllocator& alloc);
  virtual ~ObExprSpmAlterBaseline();
  virtual int calc_resultN(
      common::ObObj& result, const common::ObObj* objs, int64_t param_num, common::ObExprCtx& expr_ctx) const;
  virtual int calc_result_typeN(
      ObExprResType& type, ObExprResType* types, int64_t param_num, common::ObExprTypeCtx& type_ctx) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprSpmAlterBaseline);
};

class ObExprSpmDropBaseline : public ObFuncExprOperator {
public:
  explicit ObExprSpmDropBaseline(common::ObIAllocator& alloc);
  virtual ~ObExprSpmDropBaseline();
  int calc_result2(
      common::ObObj& result, const common::ObObj& obj1, const common::ObObj& obj2, common::ObExprCtx& expr_ctx) const;
  int calc_result_type2(
      ObExprResType& type, ObExprResType& type1, ObExprResType& type2, common::ObExprTypeCtx& type_ctx) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprSpmDropBaseline);
};

}  // namespace sql
}  // namespace oceanbase

#endif /* _OB_EXPR_SPM_H */
