/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_RESOLVER_DDL_OB_ALTER_VIEW_RESOLVER_H_
#define OCEANBASE_SQL_RESOLVER_DDL_OB_ALTER_VIEW_RESOLVER_H_

#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "sql/resolver/ddl/ob_alter_view_stmt.h"

namespace oceanbase
{
namespace sql
{
class ObAlterViewResolver : public ObDDLResolver
{
  static const int64_t IF_EXISTS = 0;      // if_exists node (can be NULL)
  static const int64_t VIEW_NAME = 1;      // view_name node
  static const int64_t ALTER_ACTION = 2;   // alter action node (T_ALTER_VIEW_COMPILE, etc.)
public:
  explicit ObAlterViewResolver(ObResolverParams &params);
  virtual ~ObAlterViewResolver();

  virtual int resolve(const ParseNode &parse_tree);

private:
  int resolve_alter_view(const ParseNode &parse_tree);

  DISALLOW_COPY_AND_ASSIGN(ObAlterViewResolver);
};

} // namespace sql
} // namespace oceanbase
#endif // OCEANBASE_SQL_RESOLVER_DDL_OB_ALTER_VIEW_RESOLVER_H_
