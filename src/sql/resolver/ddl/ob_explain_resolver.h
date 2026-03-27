/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_EXPLAIN_RESOLVER_H
#define _OB_EXPLAIN_RESOLVER_H
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/resolver/dml/ob_dml_resolver.h"
#include "sql/resolver/ddl/ob_explain_stmt.h"

namespace oceanbase
{
namespace sql
{
  class ObExplainResolver : public ObDMLResolver
  {
  public:
    ObExplainResolver(ObResolverParams &params)
      : ObDMLResolver(params)
    {}
    virtual ~ObExplainResolver() {}
    virtual int resolve(const ParseNode &parse_tree);
    ObExplainStmt *get_explain_stmt();
    int resolve_columns(ObRawExpr *&expr, common::ObArray<ObQualifiedName> &columns)
    {
      UNUSED(expr);
      UNUSED(columns);
      return common::OB_SUCCESS;
    }
    virtual int resolve_order_item(const ParseNode &sort_node, OrderItem &order_item)
    {
      UNUSED(sort_node);
      UNUSED(order_item);
      return common::OB_SUCCESS;
    }
  };

}
}
#endif
