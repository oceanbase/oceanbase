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

#ifndef OCEANBASE_SQL_RESOLVER_CMD_OB_HELP_RESOLVER_H
#define OCEANBASE_SQL_RESOLVER_CMD_OB_HELP_RESOLVER_H
#include "sql/resolver/dml/ob_select_resolver.h"
#include "sql/resolver/cmd/ob_help_stmt.h"
namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
}
namespace sql
{
class ObHelpResolver : public ObDMLResolver
{
public:
  explicit ObHelpResolver(ObResolverParams &params)
      : ObDMLResolver(params),
      sql_proxy_(params.sql_proxy_)
      {}
  virtual ~ObHelpResolver() {}
  virtual int resolve(const ParseNode &parse_tree);
private:
  int search_topic(ObHelpStmt *help_stmt,const common::ObSqlString &sql, int &topic_count,
                   const common::ObString &source_category = common::ObString::make_string("None"));
  int search_category(ObHelpStmt *help_stmt, const common::ObSqlString &sql,
                      int &category_count, common::ObString &category_name,
                      const common::ObString &source_category = common::ObString::make_string("None"));
  int create_topic_sql(const common::ObString &like_pattern, common::ObSqlString &sql);
  int create_keyword_sql(const common::ObString &like_pattern, common::ObSqlString &sql);
  int create_category_sql(const common::ObString &like_pattern, common::ObSqlString &sql);
  int create_category_topic_sql(const common::ObString &like_pattern, common::ObSqlString &sql);
  int create_category_child_sql(const common::ObString &like_pattern, common::ObSqlString &sql);
private:
  common::ObMySQLProxy *sql_proxy_;
  DISALLOW_COPY_AND_ASSIGN(ObHelpResolver);
};
}//sql
}//oceanbase
#endif
