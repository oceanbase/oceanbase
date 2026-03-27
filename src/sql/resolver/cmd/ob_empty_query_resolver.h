/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_RESOLVER_CMD_EMPTY_QUERY_RESOLVER_H_
#define OCEANBASE_SQL_RESOLVER_CMD_EMPTY_QUERY_RESOLVER_H_
#include "sql/resolver/cmd/ob_cmd_resolver.h"
namespace oceanbase
{
namespace sql
{
class ObEmptyQueryResolver : public ObCMDResolver
{
public:
  explicit ObEmptyQueryResolver(ObResolverParams &params) : ObCMDResolver(params)
  {
  }
  virtual ~ObEmptyQueryResolver()
  {
  }
  virtual int resolve(const ParseNode &parse_tree);
private:
  DISALLOW_COPY_AND_ASSIGN(ObEmptyQueryResolver);
};
}
}
#endif /*OCEANBASE_RESOLVER_CMD_EMPTY_QUERY_RESOLVER_H_*/
