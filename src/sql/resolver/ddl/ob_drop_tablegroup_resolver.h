/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_DROP_TABLEGROUP_RESOLVER_
#define OCEANBASE_SQL_OB_DROP_TABLEGROUP_RESOLVER_
#include "sql/resolver/ddl/ob_drop_database_stmt.h"
#include "sql/resolver/ddl/ob_ddl_resolver.h"

namespace oceanbase
{
namespace sql
{
class ObDropTablegroupResolver: public ObDDLResolver
{
  static const int64_t IF_NOT_EXIST = 0;
  static const int64_t TG_NAME = 1;
  static const int64_t TG_NODE_COUNT = 2;
public:
  explicit ObDropTablegroupResolver(ObResolverParams &params);
  virtual ~ObDropTablegroupResolver();

  virtual int resolve(const ParseNode &parse_tree);
private:
  // types and constants
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObDropTablegroupResolver);

private:
  // data members
};

} // end namespace sql
} // end namespace oceanbase

#endif  /* OCEANBASE_SQL_OB_DROP_TABLEGROUP_RESOLVER_ */
