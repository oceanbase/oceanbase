/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_RESOLVER_DROP_INDEX_RESOLVER_
#define OCEANBASE_SQL_RESOLVER_DROP_INDEX_RESOLVER_

#include "lib/hash/ob_placement_hashset.h"
#include "share/ob_rpc_struct.h"
#include "sql/resolver/ddl/ob_ddl_resolver.h"

namespace oceanbase
{
namespace sql
{
class ObDropIndexResolver : public ObDDLResolver
{
public:
  explicit ObDropIndexResolver(ObResolverParams &params);
  virtual ~ObDropIndexResolver();

  virtual int resolve(const ParseNode &parse_tree);
private:
  DISALLOW_COPY_AND_ASSIGN(ObDropIndexResolver);
};
}  // namespace sql
}  // namespace oceanbase
#endif
