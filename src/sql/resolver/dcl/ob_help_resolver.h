/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.src/sql/resolver/dcl/ob_help_resolver.h
 */

#ifndef OCEANBASE_SQL_RESOLVER_DCL_OB_HELP_RESOLVER_
#define OCEANBASE_SQL_RESOLVER_DCL_OB_HELP_RESOLVER_
#include "sql/resolver/dml/ob_select_resolver.h"
namespace oceanbase {
namespace sql {
class ObHelpResolver : public ObSelectResolver {
public:
  class ObHelpResolverContext;
  class ShowColumnInfo;
  explicit ObHelpResolver(ObResolverParams& params);
  virtual ~ObHelpResolver();
  virtual int resolve(const ParseNode& parse_tree);
  int parse_and_resolve_select_sql(const ObString& select_sql);
private:
  ObSqlString sql;
  int search_topic(const ParseNode& parse_tree, int64_t &topic_count, ObString &select_sql);
  int search_keyword(const ParseNode& parse_tree, int64_t &topic_count, ObString &select_sql);
  int search_category(const ParseNode& parse_tree, int64_t &category_count, ObString &select_sql);
};  // ObHelpresolver
}   // namespace sql
}  	// namespace oceanbase
#endif /* OCEANBASE_SQL_RESOLVER_DCL_OB_HELP_RESOLVER_ */
