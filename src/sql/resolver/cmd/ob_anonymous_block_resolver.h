/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_ANONYMOUS_BLOCK_RESOLVER_H_
#define OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_ANONYMOUS_BLOCK_RESOLVER_H_

#include "sql/resolver/cmd/ob_cmd_resolver.h"
#include "lib/container/ob_se_array.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace sql
{
class ObAnonymousBlockStmt;
class ObAnonymousBlockResolver: public ObCMDResolver
{
public:
  explicit ObAnonymousBlockResolver(ObResolverParams &params) : ObCMDResolver(params) {}
  virtual ~ObAnonymousBlockResolver() {}

  virtual int resolve(const ParseNode &parse_tree);
private:
  int resolve_anonymous_block(
    ParseNode &block_node, ObAnonymousBlockStmt &stmt, bool resolve_inout_param = false);
  int add_param();
  // types and constants
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObAnonymousBlockResolver);
  // function members

private:
  // data members

};

} // end namespace sql
} // end namespace oceanbase




#endif /* OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_ANONYMOUS_BLOCK_RESOLVER_H_ */
