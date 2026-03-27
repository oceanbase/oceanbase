/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_RESOLVER_DCL_OB_LOCK_USER_RESOLVER_H_
#define OCEANBASE_SQL_RESOLVER_DCL_OB_LOCK_USER_RESOLVER_H_
#include "sql/resolver/dcl/ob_lock_user_stmt.h"
#include "sql/resolver/dcl/ob_dcl_resolver.h"
namespace oceanbase
{
namespace sql
{
class ObLockUserResolver: public ObDCLResolver
{
public:
  explicit ObLockUserResolver(ObResolverParams &params);
  virtual ~ObLockUserResolver();
  virtual int resolve(const ParseNode &parse_tree);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObLockUserResolver);
};

} // end namespace sql
} // end namespace oceanbase
#endif
