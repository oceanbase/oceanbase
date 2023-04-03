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

#ifndef OCEANBASE_SQL_OB_EXPR_NOT_BETWEEN_H_
#define OCEANBASE_SQL_OB_EXPR_NOT_BETWEEN_H_

#include "lib/ob_name_def.h"
#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprNotBetween: public ObRelationalExprOperator
{
public:
  ObExprNotBetween();
  explicit  ObExprNotBetween(common::ObIAllocator &alloc);
  virtual ~ObExprNotBetween() {};
  static int calc(common::ObObj &result,
                  const common::ObObj &obj1,
                  const common::ObObj &beg,
                  const common::ObObj &end,
                  common::ObObjType cmp_type,
                  common::ObExprCtx &expr_ctx,
                  common::ObCollationType cs_type);

  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                            const ObRawExpr &raw_expr,
                            ObExpr &rt_expr) const override;
private:
  // types and constants
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprNotBetween);
  // function members
private:
  // data members

};

} // end namespace sql
} // end namespace oceanbase



#endif // OCEANBASE_SQL_OB_EXPR_NOT_BETWEEN_H_
