/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
