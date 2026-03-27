/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_RESOLVER_CMD_OB_KILL_RESOLVER_H
#define OCEANBASE_SQL_RESOLVER_CMD_OB_KILL_RESOLVER_H
#include "sql/resolver/cmd/ob_cmd_resolver.h"
namespace oceanbase
{
namespace sql
{
class ObKillResolver : public ObCMDResolver
{
public:
  explicit ObKillResolver(ObResolverParams &params):
        ObCMDResolver(params)
  {
  }
  virtual ~ObKillResolver()
  {
  }
  virtual int resolve(const ParseNode &parse_tree);
private:
  DISALLOW_COPY_AND_ASSIGN(ObKillResolver);
};
}//sql
}//oceanbase
#endif
