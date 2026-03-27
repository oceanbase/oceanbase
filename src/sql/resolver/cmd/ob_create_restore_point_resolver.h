/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_CREATE_RESTORE_POINT_RESOLVER_
#define OCEANBASE_CREATE_RESTORE_POINT_RESOLVER_ 1

#include "sql/resolver/cmd/ob_system_cmd_resolver.h"

namespace oceanbase
{
namespace sql
{
class ObCreateRestorePointResolver: public ObSystemCmdResolver
{
public:
  explicit ObCreateRestorePointResolver(ObResolverParams &params);
  virtual ~ObCreateRestorePointResolver();

  virtual int resolve(const ParseNode &parse_tree);
private:
  // types and constants
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObCreateRestorePointResolver);
  // function members

private:
  // data members
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_CREATE_RESTORE_POINT_RESOLVER_ */
