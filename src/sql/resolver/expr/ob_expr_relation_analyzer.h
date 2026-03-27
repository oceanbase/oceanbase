/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SRC_SQL_RESOLVER_EXPR_OB_EXPR_RELATION_ANALYZER_H_
#define OCEANBASE_SRC_SQL_RESOLVER_EXPR_OB_EXPR_RELATION_ANALYZER_H_

#include "lib/container/ob_se_array.h"
namespace oceanbase
{
namespace sql
{
class ObRawExpr;
class ObExprRelationAnalyzer
{
public:
  explicit ObExprRelationAnalyzer();
  int pull_expr_relation_id(ObRawExpr *expr);
private:
  int visit_expr(ObRawExpr &expr);
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SRC_SQL_RESOLVER_EXPR_OB_EXPR_RELATION_ANALYZER_H_ */
