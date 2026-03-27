/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_TCL_RESOLVER_
#define OCEANBASE_SQL_TCL_RESOLVER_

#include "sql/resolver/ob_stmt_resolver.h"

namespace oceanbase
{
namespace sql
{

class ObTCLResolver : public ObStmtResolver
{
public:
  explicit ObTCLResolver(ObResolverParams &params);
  virtual ~ObTCLResolver();

  virtual int resolve_special_expr(ObRawExpr *&expr, ObStmtScope scope);
  virtual int resolve_sub_query_info(const common::ObIArray<ObSubQueryInfo> &subquery_info,
                                     const ObStmtScope upper_scope);
  virtual int resolve_columns(ObRawExpr *&expr, common::ObArray<ObQualifiedName> &columns);
private:
  /* functions */
  /* variables */
  DISALLOW_COPY_AND_ASSIGN(ObTCLResolver);
};

}
}
#endif /* OCEANBASE_SQL_TCL_RESOLVER_ */
//// end of header file
