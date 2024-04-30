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

#ifndef _OB_SET_COMMENT_RESOLVER_H
#define _OB_SET_COMMENT_RESOLVER_H 1

#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "sql/resolver/ddl/ob_alter_table_stmt.h"

namespace oceanbase
{
namespace sql
{
class ObSetCommentResolver : public ObDDLResolver
{
public:
  explicit ObSetCommentResolver(ObResolverParams &params);
  virtual ~ObSetCommentResolver();
  virtual int resolve(const ParseNode &parse_tree);
  ObAlterTableStmt *get_alter_table_stmt() { return static_cast<ObAlterTableStmt*>(stmt_); };
  int get_table_schema(const ParseNode *db_node,
                       const uint64_t tenant_id,
                       ObString &database_name,
                       ObString &table_name,
                       const ObTableSchema *&table_schema);
private:
  share::schema::ObTableSchema table_schema_;
  common::ObCollationType collation_type_;
  common::ObCharsetType charset_type_;
  DISALLOW_COPY_AND_ASSIGN(ObSetCommentResolver);
};
}//namespace sql
}//namespace oceanbase
#endif
