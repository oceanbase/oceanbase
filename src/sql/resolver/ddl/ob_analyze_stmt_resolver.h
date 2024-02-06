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

#ifndef SRC_SQL_RESOLVER_DDL_OB_ANALYZE_STMT_RESOLVER_H_
#define SRC_SQL_RESOLVER_DDL_OB_ANALYZE_STMT_RESOLVER_H_

#include "ob_ddl_resolver.h"

namespace oceanbase
{
namespace sql
{
class ObAnalyzeStmt;

class ObAnalyzeStmtResolver: public ObDDLResolver
{
  static const int64_t DEFAULT_SAMPLE_ROWCOUNT_PER_BUCKET = 512;
public:
  ObAnalyzeStmtResolver(ObResolverParams &params);
  virtual ~ObAnalyzeStmtResolver();
  virtual int resolve(const ParseNode &parse_tree);
private:
  int resolve_oracle_analyze(const ParseNode &parse_node,
                             ObAnalyzeStmt &analyze_stmt);
  int resolve_mysql_update_histogram(const ParseNode &parse_node,
                                     ObAnalyzeStmt &analyze_stmt);
  int resolve_mysql_delete_histogram(const ParseNode &parse_node,
                                     ObAnalyzeStmt &analyze_stmt);
  int resolve_mysql_column_bucket_info(const ParseNode *column_node,
                                       const int64_t bucket_number,
                                       ObAnalyzeStmt &analyze_stmt);
  int resolve_table_info(const ParseNode *table_node,
                         ObAnalyzeStmt &analyze_stmt);
  int resolve_partition_info(const ParseNode *part_node,
                             ObAnalyzeStmt &analyze_stmt,
                             bool &is_hist_subpart);
  int resolve_statistic_info(const ParseNode *statistic_node,
                             const bool is_hist_subpart,
                             ObAnalyzeStmt &analyze_stmt);
  int resolve_for_clause_info(const ParseNode *for_clause_node,
                              const bool is_hist_subpart,
                              ObAnalyzeStmt &analyze_stmt);
  int resolve_for_clause_element(const ParseNode *for_clause_node,
                                 const bool is_hist_subpart,
                                 ObAnalyzeStmt &analyze_stmt);
  int resolve_sample_clause_info(const ParseNode *sample_clause_node,
                                 ObAnalyzeStmt &analyze_stmt);

  int get_bucket_size(const ParseNode *node, int64_t &bucket_num);

  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObAnalyzeStmtResolver);
};

} /* namespace sql */
} /* namespace oceanbase */

#endif /* SRC_SQL_RESOLVER_DDL_OB_ANALYZE_STMT_RESOLVER_H_ */
