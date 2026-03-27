/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_DROP_FUNC_RESOLVER_H
#define _OB_DROP_FUNC_RESOLVER_H 1

#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "sql/resolver/ddl/ob_drop_func_stmt.h"

namespace oceanbase
{
namespace sql
{

class ObDropFuncResolver : public ObDDLResolver
{
public:
  explicit ObDropFuncResolver(ObResolverParams &params);
  virtual ~ObDropFuncResolver();

  virtual int resolve(const ParseNode &parse_tree);
private:
  DISALLOW_COPY_AND_ASSIGN(ObDropFuncResolver);
};

}
}

#endif /* _OB_DROP_FUNC_RESOLVER_H */


