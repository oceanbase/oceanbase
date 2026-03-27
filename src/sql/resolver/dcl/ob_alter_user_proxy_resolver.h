/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_ALTER_USER_PROXY_RESOLVER_H
#define OB_ALTER_USER_PROXY_RESOLVER_H

#include "sql/resolver/dcl/ob_alter_user_proxy_stmt.h"
#include "sql/resolver/dcl/ob_dcl_resolver.h"
#include "share/ob_rpc_struct.h"

namespace oceanbase
{
namespace sql
{
class ObAlterUserProxyResolver: public ObDCLResolver
{
public:
  explicit ObAlterUserProxyResolver(ObResolverParams &params);
  virtual ~ObAlterUserProxyResolver();
  virtual int resolve(const ParseNode &parse_tree);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObAlterUserProxyResolver);
};

} // end namespace sql
} // end namespace oceanbase



#endif // OB_ALTER_USER_PROFILE_RESOLVER_H
