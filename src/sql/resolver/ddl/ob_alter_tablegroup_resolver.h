/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_ALTER_TABLEGROUP_RESOLVER_
#define OCEANBASE_SQL_OB_ALTER_TABLEGROUP_RESOLVER_

#include "sql/resolver/ddl/ob_alter_tablegroup_stmt.h"
#include "sql/resolver/ddl/ob_tablegroup_resolver.h"

namespace oceanbase
{
namespace sql
{
class ObAlterTablegroupResolver : public ObTableGroupResolver
{
  static const int TG_NAME = 0;
  static const int TABLE_LIST = 1;
public:
  explicit ObAlterTablegroupResolver(ObResolverParams &params);
  virtual ~ObAlterTablegroupResolver();
  virtual int resolve(const ParseNode &parse_tree);
  ObAlterTablegroupStmt *get_alter_tablegroup_stmt() { return static_cast<ObAlterTablegroupStmt*>(stmt_); };

private:
  DISALLOW_COPY_AND_ASSIGN(ObAlterTablegroupResolver);
};


} //end namespace sql
} //end namespace oceanbase

#endif // OCEANBASE_SQL_OB_ALTER_TABLEGROUP_RESOLVER_
