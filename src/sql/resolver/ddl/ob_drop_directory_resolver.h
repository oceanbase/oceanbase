/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_DROP_DIRECTORY_RESOLVER_H_
#define OCEANBASE_SQL_OB_DROP_DIRECTORY_RESOLVER_H_

#include "sql/resolver/ddl/ob_ddl_resolver.h"

namespace oceanbase
{
namespace sql
{
class ObDropDirectoryResolver: public ObDDLResolver
{
  static const int64_t DIRECTORY_NAME = 0;
  static const int64_t DIRECTORY_NODE_COUNT = 1;
public:
  explicit ObDropDirectoryResolver(ObResolverParams &params);
  virtual ~ObDropDirectoryResolver();
  virtual int resolve(const ParseNode &parse_tree);
private:
  DISALLOW_COPY_AND_ASSIGN(ObDropDirectoryResolver);
};
} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_SQL_OB_DROP_DIRECTORY_RESOLVER_H_