/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "sql/resolver/tcl/ob_tcl_resolver.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace sql
{


ObTCLResolver::ObTCLResolver(ObResolverParams &params)
  : ObStmtResolver(params)
{
}

ObTCLResolver::~ObTCLResolver()
{
}

int ObTCLResolver::resolve_special_expr(ObRawExpr *&expr, ObStmtScope scope)
{
  UNUSED(expr);
  UNUSED(scope);
  return OB_SUCCESS;
}
int ObTCLResolver::resolve_sub_query_info(const ObIArray<ObSubQueryInfo> &subquery_info,
                                          const ObStmtScope upper_scope)
{
  UNUSED(subquery_info);
  UNUSED(upper_scope);
  return OB_SUCCESS;
}
int ObTCLResolver::resolve_columns(ObRawExpr *&expr, ObArray<ObQualifiedName> &columns)
{
  UNUSED(expr);
  UNUSED(columns);
  return OB_SUCCESS;
}


}  // namespace sql
}  // namespace oceanbase
