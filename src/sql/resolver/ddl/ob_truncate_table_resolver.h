/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_TRUNCATE_TABLE_RESOLVER_
#define OCEANBASE_SQL_OB_TRUNCATE_TABLE_RESOLVER_

#include "sql/resolver/ddl/ob_truncate_table_stmt.h"
#include "sql/resolver/ddl/ob_ddl_resolver.h"

namespace oceanbase
{
namespace sql
{

class ObTruncateTableResolver : public ObDDLResolver
{
public:
  explicit ObTruncateTableResolver(ObResolverParams &params);
  virtual ~ObTruncateTableResolver();
  virtual int resolve(const ParseNode &parse_tree);
  ObTruncateTableStmt *get_truncate_table_stmt()
  {
    return static_cast<ObTruncateTableStmt*>(stmt_);
  };

private:
  static const int64_t TABLE_NODE = 0;         /* 0. table_node  */
  DISALLOW_COPY_AND_ASSIGN(ObTruncateTableResolver);
};


} //end namespace sql
} //end namespace oceanbase

#endif // OCEANBASE_SQL_OB_TRUNCATE_TABLE_RESOLVER_

