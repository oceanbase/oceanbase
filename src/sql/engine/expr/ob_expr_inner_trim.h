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

#ifndef _OB_SQL_EXPR_INNER_TRIM_H_
#define _OB_SQL_EXPR_INNER_TRIM_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase {
namespace sql {
class ObExprInnerTrim : public ObStringExprOperator {
public:
  explicit ObExprInnerTrim(common::ObIAllocator& alloc);
  virtual ~ObExprInnerTrim();
  virtual int calc_result_type3(ObExprResType& type, ObExprResType& trim_type, ObExprResType& trim_pattern,
      ObExprResType& text, common::ObExprTypeCtx& type_ctx) const;
  virtual int calc_result3(common::ObObj& result, const common::ObObj& trim_type, const common::ObObj& trim_pattern,
      const common::ObObj& text, common::ObExprCtx& expr_ctx) const;

  virtual int cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const override;

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprInnerTrim);
};

}  // namespace sql
}  // namespace oceanbase
#endif /* _OB_SQL_EXPR_TRIM_H_ */
