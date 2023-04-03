/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
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
