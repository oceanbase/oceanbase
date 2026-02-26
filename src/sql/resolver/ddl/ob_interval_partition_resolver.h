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

#ifndef OCEANBASE_SQL_RESOLVER_DDL_OB_INTERVAL_PARTITION_RESOLVER_H
#define OCEANBASE_SQL_RESOLVER_DDL_OB_INTERVAL_PARTITION_RESOLVER_H

#include "sql/resolver/ob_stmt_resolver.h"

namespace oceanbase
{
namespace sql
{
class ObPartitionedStmt;
class ObAlterTableStmt;

class ObIntervalPartitionResolver : public ObStmtResolver
{
public:
  explicit ObIntervalPartitionResolver(ObResolverParams &params) : ObStmtResolver(params) {}
  virtual ~ObIntervalPartitionResolver() {}
  virtual int resolve(const ParseNode &parse_tree) override;
  int resolve_interval_clause(ObPartitionedStmt *stmt,
                              ParseNode *node,
                              share::schema::ObTableSchema &table_schema,
                              common::ObSEArray<ObRawExpr*, 8> &range_exprs);
  int resolve_set_interval(ObAlterTableStmt &stmt,
                           const ParseNode &node,
                           const share::schema::ObTableSchema &table_schema);
  int resolve_interval_and_transition(const ParseNode &node,
                                      ObAlterTableStmt &alter_stmt,
                                      const share::schema::ObTableSchema &table_schema);

private:
  int resolve_interval_node_(ObResolverParams &params,
                             ParseNode *interval_node,
                             common::ColumnType &col_dt,
                             int64_t precision,
                             int64_t scale,
                             ObRawExpr *&interval_value_expr_out);
  int resolve_interval_expr_low_(ObResolverParams &params,
                                 ParseNode *interval_node,
                                 const share::schema::ObTableSchema &table_schema,
                                 ObRawExpr *transition_expr,
                                 ObRawExpr *&interval_value);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObIntervalPartitionResolver);
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_SQL_RESOLVER_DDL_OB_INTERVAL_PARTITION_RESOLVER_H
