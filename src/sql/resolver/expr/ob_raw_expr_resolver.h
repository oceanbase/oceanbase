/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_RAW_EXPR_RESOLVER_H
#define _OB_RAW_EXPR_RESOLVER_H
#include "lib/oblog/ob_log.h"
#include "sql/parser/parse_node.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/ob_stmt_resolver.h"

namespace oceanbase
{
namespace sql
{
class ObRawExprResolver
{
public:
  ObRawExprResolver() {}

  virtual ~ObRawExprResolver() {}

  virtual int resolve(const ParseNode *node,
                      ObRawExpr *&expr,
                      common::ObIArray<ObQualifiedName> &columns,
                      common::ObIArray<ObVarInfo> &sys_vars,
                      common::ObIArray<ObSubQueryInfo> &sub_query_info,
                      common::ObIArray<ObAggFunRawExpr*> &aggr_exprs,
                      common::ObIArray<ObWinFunRawExpr*> &win_exprs,
                      common::ObIArray<ObUDFInfo> &udf_exprs,
                      common::ObIArray<ObOpRawExpr*> &op_exprs,
                      common::ObIArray<ObUserVarIdentRawExpr*> &user_var_exprs,
                      common::ObIArray<ObInListInfo> &inlist_infos,
                      common::ObIArray<ObMatchFunRawExpr*> &match_exprs) = 0;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObRawExprResolver);
  // function members
private:
  // data members
};

} // end namespace sql
} // end namespace oceanbase

#endif /* _OB_RAW_EXPR_RESOLVER_H */
