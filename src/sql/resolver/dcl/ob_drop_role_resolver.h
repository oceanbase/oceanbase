/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_DROP_ROLE_RESOLVER_H
#define _OB_DROP_ROLE_RESOLVER_H 1

#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "sql/resolver/dcl/ob_dcl_resolver.h"
namespace oceanbase
{
namespace sql
{
class ObDropRoleResolver: public ObDCLResolver
{
public:
  explicit ObDropRoleResolver(ObResolverParams &params);
  virtual ~ObDropRoleResolver();

  virtual int resolve(const ParseNode &parse_tree);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObDropRoleResolver);
};
}//namespace sql
}//namespace oceanbase
#endif
