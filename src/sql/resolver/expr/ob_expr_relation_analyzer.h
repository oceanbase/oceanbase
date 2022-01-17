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

#ifndef OCEANBASE_SRC_SQL_RESOLVER_EXPR_OB_EXPR_RELATION_ANALYZER_H_
#define OCEANBASE_SRC_SQL_RESOLVER_EXPR_OB_EXPR_RELATION_ANALYZER_H_

#include "lib/container/ob_se_array.h"
namespace oceanbase {
namespace sql {
class ObRawExpr;
class ObDMLStmt;
class ObQueryRefRawExpr;
class ObExprRelationAnalyzer {
public:
  explicit ObExprRelationAnalyzer();
  int pull_expr_relation_id_and_levels(ObRawExpr* expr, int32_t cur_stmt_level);

private:
  int init_expr_info(ObRawExpr& expr);
  int visit_expr(ObRawExpr& expr, int32_t stmt_level);
  int visit_stmt(ObDMLStmt* stmt);

private:
  // auto_free = false, only used in function stack
  common::ObSEArray<ObQueryRefRawExpr*, common::OB_MAX_SUBQUERY_LAYER_NUM> query_exprs_;
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SRC_SQL_RESOLVER_EXPR_OB_EXPR_RELATION_ANALYZER_H_ */
