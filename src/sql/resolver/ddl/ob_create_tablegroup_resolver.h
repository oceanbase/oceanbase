/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_CREATE_TABLEGROUP_RESOLVER_
#define OCEANBASE_SQL_OB_CREATE_TABLEGROUP_RESOLVER_
#include "sql/resolver/ddl/ob_create_tablegroup_stmt.h"
#include "sql/resolver/ddl/ob_tablegroup_resolver.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace sql
{
class ObCreateTablegroupResolver: public ObTableGroupResolver
{
  static const int64_t IF_NOT_EXIST = 0;
  static const int64_t TG_NAME = 1;
  static const int64_t TABLEGROUP_OPTION = 2;
  static const int64_t PARTITION_OPTION = 3;
public:
  explicit ObCreateTablegroupResolver(ObResolverParams &params);
  virtual ~ObCreateTablegroupResolver();
  virtual int resolve(const ParseNode &parse_tree);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObCreateTablegroupResolver);
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_OB_CREATE_TABLEGROUP_RESOLVER_ */
