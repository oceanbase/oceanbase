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

#ifndef _OB_EXPR_GENERATOR_H
#define _OB_EXPR_GENERATOR_H 1
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/engine/expr/ob_sql_expression.h"

namespace oceanbase
{
namespace sql
{

class ObExprGenerator
{
public:
  ObExprGenerator(){}
  virtual ~ObExprGenerator(){}

  virtual int generate(ObRawExpr &raw_expr, ObSqlExpression &out_expr) = 0;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprGenerator);
};
} // end namespace sql
} // end namespace oceanbase

#endif /* _OB_EXPR_GENERATOR_H */
