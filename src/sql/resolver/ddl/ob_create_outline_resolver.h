/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_CREATE_OUTLINE_RESOLVER_H_
#define OCEANBASE_SQL_OB_CREATE_OUTLINE_RESOLVER_H_

#include "sql/resolver/ddl/ob_outline_resolver.h"
namespace oceanbase
{
namespace sql
{
class ObCreateOutlineStmt;
class ObCreateOutlineResolver : public ObOutlineResolver
{
public:
  explicit ObCreateOutlineResolver(ObResolverParams &params) : ObOutlineResolver(params) {}
  virtual ~ObCreateOutlineResolver() {}
  virtual int resolve(const ParseNode &parse_tree);
private:
  int resolve_sql_id(const ParseNode *node, ObCreateOutlineStmt &create_outline_stmt, bool is_format_sql);
  int resolve_hint(const ParseNode *node, ObCreateOutlineStmt &create_outline_stmt);
  static const int64_t OUTLINE_CHILD_COUNT = 6;
  DISALLOW_COPY_AND_ASSIGN(ObCreateOutlineResolver);
};
}//namespace sql
}//namespace oceanbase
#endif //OCEANBASE_SQL_OB_CREATE_OUTLINE_RESOLVER_H_
