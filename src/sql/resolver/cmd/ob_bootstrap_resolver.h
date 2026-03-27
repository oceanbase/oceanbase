/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_RESOLVER_CMD_BOOTSTRAP_RESOLVER_
#define OCEANBASE_SQL_RESOLVER_CMD_BOOTSTRAP_RESOLVER_

#include "sql/resolver/cmd/ob_system_cmd_resolver.h"

namespace oceanbase
{
namespace sql
{
class ObBootstrapStmt;

class ObBootstrapResolver : public ObSystemCmdResolver
{
public:
  explicit ObBootstrapResolver(ObResolverParams &params);
  virtual ~ObBootstrapResolver();

  virtual int resolve(const ParseNode &parse_tree);
private:
  int do_resolve_bootstrap_info_(const ParseNode &parse_tree, ObBootstrapStmt* bootstrap_stmt);
  int do_resolve_logservice_access_point_(
    const ParseNode &parse_tree,
    ObBootstrapStmt* bootstrap_stmt);
  int do_resolve_shared_storage_info_(
    const ParseNode &parse_tree,
    ObBootstrapStmt* bootstrap_stmt);

  DISALLOW_COPY_AND_ASSIGN(ObBootstrapResolver);
};
}// namespace sql
}// namespace oceanbase
#endif /* OCEANBASE_SQL_RESOLVER_CMD_BOOTSTRAP_RESOLVER_ */
