/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_DROP_DBLINK_RESOLVER_H_
#define OCEANBASE_SQL_OB_DROP_DBLINK_RESOLVER_H_
#include "sql/resolver/ddl/ob_ddl_resolver.h"

namespace oceanbase
{
namespace sql
{
class ObDropDbLinkResolver: public ObDDLResolver
{
  static const int64_t IF_EXIST = 0;
  static const int64_t DBLINK_NAME = 1;
  static const int64_t DBLINK_NODE_COUNT = 2;
public:
  explicit ObDropDbLinkResolver(ObResolverParams &params);
  virtual ~ObDropDbLinkResolver();
  virtual int resolve(const ParseNode &parse_tree);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObDropDbLinkResolver);
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_OB_DROP_DBLINK_RESOLVER_H_*/
