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

#ifndef OCEANBASE_SRC_SQL_RESOLVER_DML_OB_AGGR_EXPR_PUSH_UP_ANALYZER_H_
#define OCEANBASE_SRC_SQL_RESOLVER_DML_OB_AGGR_EXPR_PUSH_UP_ANALYZER_H_
#include "lib/container/ob_array.h"
#include "sql/ob_sql_utils.h"
#include "sql/resolver/expr/ob_raw_expr.h"
namespace oceanbase {
namespace sql {
struct JoinedTable;
class ObDMLResolver;
class ObAggFunRawExpr;
class ObSelectResolver;
class ObSelectStmt;
class ObAggrExprPushUpAnalyzer {
public:
  ObAggrExprPushUpAnalyzer(ObSelectResolver& cur_resolver)
      : aggr_columns_checker_(aggr_columns_), level_exprs_checker_(level_exprs_), cur_resolver_(cur_resolver)
  {}
  ~ObAggrExprPushUpAnalyzer()
  {}

  int analyze_and_push_up_aggr_expr(ObAggFunRawExpr* aggr_expr, ObAggFunRawExpr*& final_aggr);

private:
  int analyze_aggr_param_exprs(common::ObIArray<ObRawExpr*>& param_exprs, ObExprLevels& aggr_expr_levels);
  int analyze_aggr_param_expr(ObRawExpr*& param_expr, ObExprLevels& aggr_expr_levels);
  int analyze_child_stmt(ObSelectStmt* child_stmt, ObExprLevels& aggr_expr_levels);
  int analyze_joined_table_exprs(JoinedTable& joined_table, ObExprLevels& aggr_expr_levels);
  int32_t get_final_aggr_level() const;
  ObSelectResolver* fetch_final_aggr_resolver(ObDMLResolver* cur_resolver, int32_t final_aggr_level);
  int push_up_aggr_column(int32_t final_aggr_level);
  int adjust_query_level(int32_t final_aggr_level);
  int push_up_subquery_in_aggr(ObSelectResolver& final_resolver);

private:
  static const int64_t AGGR_HASH_BUCKET_SIZE = 64;

private:
  // table column or alias column in aggregate function
  common::ObArray<ObRawExpr*, common::ModulePageAllocator, true> aggr_columns_;
  RelExprChecker aggr_columns_checker_;
  // all exprs that have expr level in aggregate function
  common::ObArray<ObRawExpr*, common::ModulePageAllocator, true> level_exprs_;
  RelExprChecker level_exprs_checker_;
  // all child stmts in aggregate function
  common::ObArray<ObSelectStmt*, common::ModulePageAllocator, true> child_stmts_;
  // aggregate function in this resolver
  ObSelectResolver& cur_resolver_;
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SRC_SQL_RESOLVER_DML_OB_AGGR_EXPR_PUSH_UP_ANALYZER_H_ */
