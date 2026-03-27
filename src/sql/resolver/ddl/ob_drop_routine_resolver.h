/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_DROP_PROCEDURE_RESOLVER_H_
#define OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_DROP_PROCEDURE_RESOLVER_H_

#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "lib/container/ob_se_array.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace sql
{
class ObDropProcedureResolver: public ObDDLResolver
{
public:
  explicit ObDropProcedureResolver(ObResolverParams &params) : ObDDLResolver(params) {}
  virtual ~ObDropProcedureResolver() {}

  virtual int resolve(const ParseNode &parse_tree);
private:
  // types and constants
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObDropProcedureResolver);
  // function members

private:
  // data members
};

class ObDropFunctionResolver: public ObDDLResolver
{
public:
  explicit ObDropFunctionResolver(ObResolverParams &params) : ObDDLResolver(params) {}
  virtual ~ObDropFunctionResolver() {}

  virtual int resolve(const ParseNode &parse_tree);
private:
  DISALLOW_COPY_AND_ASSIGN(ObDropFunctionResolver);
};
} // end namespace sql
} // end namespace oceanbase
#endif /* OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_DROP_PROCEDURE_RESOLVER_H_ */
