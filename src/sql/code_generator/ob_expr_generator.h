/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
