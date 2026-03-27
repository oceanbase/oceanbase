/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_RESOVLER_DCL_OB_ALTER_ROLE_RESOLVER_
#define OCEANBASE_SQL_RESOVLER_DCL_OB_ALTER_ROLE_RESOLVER_
#include "sql/resolver/dcl/ob_dcl_resolver.h"
namespace oceanbase
{
namespace sql
{
class ObAlterRoleResolver: public ObDCLResolver
{
public:
  explicit ObAlterRoleResolver(ObResolverParams &params);
  virtual ~ObAlterRoleResolver();

  virtual int resolve(const ParseNode &parse_tree);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObAlterRoleResolver);
};

} // end namespace sql
} // end namespace oceanbase
#endif
