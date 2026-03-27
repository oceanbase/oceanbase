/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_RESOVLER_DCL_OB_RENAME_USER_RESOLVER_
#define OCEANBASE_SQL_RESOVLER_DCL_OB_RENAME_USER_RESOLVER_
#include "sql/resolver/dcl/ob_dcl_resolver.h"
#include "sql/resolver/dcl/ob_rename_user_stmt.h"
namespace oceanbase
{
namespace sql
{
class ObRenameUserResolver: public ObDCLResolver
{
public:
  explicit ObRenameUserResolver(ObResolverParams &params);
  virtual ~ObRenameUserResolver();

  virtual int resolve(const ParseNode &parse_tree);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObRenameUserResolver);
};

} // end namespace sql
} // end namespace oceanbase
#endif
