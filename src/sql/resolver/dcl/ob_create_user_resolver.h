/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_RESOVLER_DCL_OB_CREATE_USER_RESOLVER_
#define OCEANBASE_SQL_RESOVLER_DCL_OB_CREATE_USER_RESOLVER_
#include "sql/resolver/dcl/ob_dcl_resolver.h"
#include "sql/resolver/dcl/ob_create_user_stmt.h"
namespace oceanbase
{
namespace sql
{
class ObCreateUserResolver: public ObDCLResolver
{
public:
  explicit ObCreateUserResolver(ObResolverParams &params);
  virtual ~ObCreateUserResolver();

  virtual int resolve(const ParseNode &parse_tree);
private:
  const static uint64_t MAX_CONNECTIONS = 4294967295;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObCreateUserResolver);
};

} // end namespace sql
} // end namespace oceanbase
#endif
