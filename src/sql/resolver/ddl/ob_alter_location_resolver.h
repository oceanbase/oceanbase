/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_ALTER_LOCATION_RESOLVER_H_
#define OCEANBASE_SQL_OB_ALTER_LOCATION_RESOLVER_H_

#include "sql/resolver/ddl/ob_ddl_resolver.h"

namespace oceanbase
{
namespace sql
{
class ObAlterLocationResolver: public ObDDLResolver
{
  static const int64_t LOCATION_NAME = 0;
  static const int64_t LOCATION_MODIFY = 1;
  static const int64_t LOCATION_NODE_COUNT = 2;
public:
  explicit ObAlterLocationResolver(ObResolverParams &params);
  virtual ~ObAlterLocationResolver();
  virtual int resolve(const ParseNode &parse_tree);
private:
  DISALLOW_COPY_AND_ASSIGN(ObAlterLocationResolver);
};
} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_SQL_OB_ALTER_LOCATION_RESOLVER_H_