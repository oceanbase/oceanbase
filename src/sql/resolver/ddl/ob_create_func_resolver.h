/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_CREATE_FUNC_RESOLVER_H
#define _OB_CREATE_FUNC_RESOLVER_H 1

#include "sql/resolver/ddl/ob_create_func_stmt.h"
#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "lib/container/ob_se_array.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace sql
{

class ObCreateFuncResolver : public ObDDLResolver
{
public:
  explicit ObCreateFuncResolver(ObResolverParams &params);
  virtual ~ObCreateFuncResolver();

  virtual int resolve(const ParseNode &parse_tree);
};

}
}

#endif /* _OB_CREATE_FUNC_RESOLVER_H */


