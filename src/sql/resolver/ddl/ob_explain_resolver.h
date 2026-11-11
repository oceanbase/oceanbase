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
    // T_EXPLAIN parse tree child indices, @see sql_parser_mysql_mode.y
    static const int64_t EXPLAIN_FORMAT;
    static const int64_t EXPLAIN_DISPLAY_OPTION;
    static const int64_t EXPLAIN_INTO_TABLE;
    static const int64_t EXPLAIN_STMT_ID;
    static const int64_t EXPLAIN_CHILD_STMT;
    static const int64_t EXPLAIN_CHILD_COUNT;

    static const ParseNode *get_child_stmt_node(const ParseNode *explain_node);
    static bool is_ddl_explain(const ParseNode *explain_node);
    static bool is_supported_ddl_item_type(const ObItemType item_type);

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
