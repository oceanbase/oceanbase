/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_ALTER_USER_PRIMARY_ZONE_RESOLVER_H
#define OB_ALTER_USER_PRIMARY_ZONE_RESOLVER_H

#include "sql/resolver/dcl/ob_alter_user_primary_zone_stmt.h"
#include "sql/resolver/dcl/ob_dcl_resolver.h"
#include "share/ob_rpc_struct.h"

namespace oceanbase
{
namespace sql
{
class ObAlterUserPrimaryZoneResolver: public ObDCLResolver
{
public:
  explicit ObAlterUserPrimaryZoneResolver(ObResolverParams &params);
  virtual ~ObAlterUserPrimaryZoneResolver();
  virtual int resolve(const ParseNode &parse_tree);
  DISALLOW_COPY_AND_ASSIGN(ObAlterUserPrimaryZoneResolver);
};

} // end namespace sql
} // end namespace oceanbase

#endif
