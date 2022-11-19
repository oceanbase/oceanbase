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

#ifndef _OB_SQL_EXPR_GREATEST_H_
#define _OB_SQL_EXPR_GREATEST_H_

#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_expr_least.h"

namespace oceanbase
{
namespace sql
{
class ObExprGreatest : public ObExprLeastGreatest
{
public:
  explicit  ObExprGreatest(common::ObIAllocator &alloc);
  virtual ~ObExprGreatest() {}
  static int calc_greatest(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprGreatest);
};


}
}
#endif /* _OB_SQL_EXPR_GREATEST_H_ */
