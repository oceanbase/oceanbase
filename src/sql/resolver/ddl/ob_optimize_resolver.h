/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_RESOLVER_DDL_OPTIMIZE_RESOLVER_H_
#define OCEANBASE_SQL_RESOLVER_DDL_OPTIMIZE_RESOLVER_H_

#include "sql/resolver/ddl/ob_optimize_stmt.h"
#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
namespace sql
{

class ObOptimizeTableResolver : public ObDDLResolver
{
public:
  explicit ObOptimizeTableResolver(ObResolverParams &params)
    : ObDDLResolver(params)
  {}
  virtual ~ObOptimizeTableResolver() = default;
  virtual int resolve(const ParseNode &parse_tree);
private:
  static const int64_t TABLE_LIST_NODE = 0;
  DISALLOW_COPY_AND_ASSIGN(ObOptimizeTableResolver);
};

class ObOptimizeTenantResolver : public ObDDLResolver
{
public:
  explicit ObOptimizeTenantResolver(ObResolverParams &params)
    : ObDDLResolver(params)
  {}
  virtual ~ObOptimizeTenantResolver() = default;
  virtual int resolve(const ParseNode &parser_tree);
private:
  static const int64_t TABLE_LIST_NODE = 0;
  DISALLOW_COPY_AND_ASSIGN(ObOptimizeTenantResolver);
};

class ObOptimizeAllResolver : public ObDDLResolver
{
public:
  explicit ObOptimizeAllResolver(ObResolverParams &params)
    : ObDDLResolver(params)
  {}
  virtual ~ObOptimizeAllResolver() = default;
  virtual int resolve(const ParseNode &parser_tree);
private:
  DISALLOW_COPY_AND_ASSIGN(ObOptimizeAllResolver);
};

}  // end namespace sql
}  // end namespace oceanbase

#endif  // OCEANBASE_SQL_RESOLVER_DDL_OPTIMIZE_RESOLVER_H_
