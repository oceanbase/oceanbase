/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_CREATE_DIRECTORY_RESOLVER_H_
#define OCEANBASE_SQL_OB_CREATE_DIRECTORY_RESOLVER_H_

#include "sql/resolver/ddl/ob_ddl_resolver.h"

namespace oceanbase
{
namespace sql
{
class ObCreateDirectoryResolver: public ObDDLResolver
{
  static const int64_t DIRECTORY_REPLACE = 0;
  static const int64_t DIRECTORY_NAME = 1;
  static const int64_t DIRECTORY_PATH = 2;
  static const int64_t DIRECTORY_NODE_COUNT = 3;
public:
  explicit ObCreateDirectoryResolver(ObResolverParams &params);
  virtual ~ObCreateDirectoryResolver();
  virtual int resolve(const ParseNode &parse_tree);
private:
  DISALLOW_COPY_AND_ASSIGN(ObCreateDirectoryResolver);
};
} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_SQL_OB_CREATE_DIRECTORY_RESOLVER_H_