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
