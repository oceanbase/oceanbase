/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_CREATE_ROLE_RESOLVER_H
#define _OB_CREATE_ROLE_RESOLVER_H 1

#include "sql/resolver/dcl/ob_dcl_resolver.h"
namespace oceanbase
{
namespace sql
{
class ObCreateRoleResolver: public ObDCLResolver
{
public:
  explicit ObCreateRoleResolver(ObResolverParams &params);
  virtual ~ObCreateRoleResolver();

  virtual int resolve(const ParseNode &parse_tree);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObCreateRoleResolver);
};
}//namespace sql
}//namespace oceanbase
#endif
