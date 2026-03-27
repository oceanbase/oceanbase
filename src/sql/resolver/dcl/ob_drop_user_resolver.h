/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_RESOLVER_DCL_OB_DROP_USER_RESOLVER_
#define OCEANBASE_SQL_RESOLVER_DCL_OB_DROP_USER_RESOLVER_
#include "sql/resolver/dcl/ob_dcl_resolver.h"
#include "sql/resolver/dcl/ob_drop_user_stmt.h"
namespace oceanbase
{
namespace sql
{
class ObDropUserResolver: public ObDCLResolver
{
public:
  explicit ObDropUserResolver(ObResolverParams &params);
  virtual ~ObDropUserResolver();

  virtual int resolve(const ParseNode &parse_tree);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObDropUserResolver);
};

} // end namespace sql
} // end namespace oceanbase
#endif
