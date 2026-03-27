/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_DROP_PACKAGE_RESOLVER_H_
#define OCEANBASE_SQL_OB_DROP_PACKAGE_RESOLVER_H_

#include "sql/resolver/ddl/ob_ddl_resolver.h"
namespace oceanbase
{
namespace sql
{
class ObDropPackageResolver: public ObDDLResolver
{
public:
  static const int64_t DROP_PACKAGE_NODE_CHILD_COUNT = 1;
  explicit ObDropPackageResolver(ObResolverParams &params) : ObDDLResolver(params) {}
  virtual ~ObDropPackageResolver() {}

  virtual int resolve(const ParseNode &parse_tree);
private:
  DISALLOW_COPY_AND_ASSIGN(ObDropPackageResolver);
};
} //namespace sql
} //namespace oceanbase

#endif /* OCEANBASE_SQL_OB_DROP_PACKAGE_RESOLVER_H_ */
