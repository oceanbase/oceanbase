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

#ifndef OCEANBASE_SQL_OB_CREATE_OUTLINE_RESOLVER_H_
#define OCEANBASE_SQL_OB_CREATE_OUTLINE_RESOLVER_H_

#include "sql/resolver/ddl/ob_outline_resolver.h"
#include "sql/resolver/ddl/ob_outline_binding_rule.h"
#include "sql/outline/ob_outline_template_matcher.h"
namespace oceanbase
{
namespace sql
{
class ObCreateOutlineStmt;
class ObDMLStmt;
class ObPatternParser;
class ObCreateOutlineResolver : public ObOutlineResolver
{
public:
  explicit ObCreateOutlineResolver(ObResolverParams &params) : ObOutlineResolver(params) {}
  virtual ~ObCreateOutlineResolver() {}
  virtual int resolve(const ParseNode &parse_tree);
private:
  int resolve_sql_id(const ParseNode *node, ObCreateOutlineStmt &create_outline_stmt, bool is_format_sql);
  int resolve_hint(const ParseNode *node, ObCreateOutlineStmt &create_outline_stmt);
  int resolve_binding_rule(const ParseNode *node, ObCreateOutlineStmt &create_outline_stmt);
  int resolve_binding_rule_scope(const ParseNode *node, ObOutlineBindingRule &binding_rule);
  int resolve_binding_rule_map(const ParseNode *node, ObOutlineBindingRule &binding_rule);
  int resolve_binding_rule_map_item(const ParseNode *node, ObOutlineRuleMapping &mapping);
  int parse_and_set_db_pattern(ObPatternParser &pp, const common::ObString &pat,
                               ObOutlineRuleMapping &mapping);
  int parse_and_set_table_pattern(ObPatternParser &pp, const common::ObString &pat,
                                  ObOutlineRuleMapping &mapping);
  int match_ast_position(ObDMLStmt *outline_stmt, ObOutlineBindingRule &binding_rule,
                         const common::ObIArray<ObTableDbName> &table_db_names);
  int fill_obj_ids_from_resolved_stmt(ObDMLStmt *outline_stmt, ObOutlineRuleMapping &mapping,
                                      const common::ObString &tbl_name, const common::ObString &db_name,
                                      uint64_t tenant_id);
  int expand_auto_map_items(const ParseNode *stmt_node, ObDMLStmt *outline_stmt,
                            ObOutlineBindingRule &binding_rule);
  int expand_fixed_mappings(ObDMLStmt *outline_stmt, ObOutlineBindingRule &binding_rule,
                            const common::ObIArray<ObTableDbName> &table_db_names);
  // Generate template signature from the pristine outline AST and attach it to stmt.
  // Must be called after binding_rule/map_items are resolved and BEFORE the executor
  // hands outline_stmt to transform_stmt() — otherwise transform's JOIN flatten
  // would diverge the signature from the runtime light_resolve path.
  int generate_template_signature(ObCreateOutlineStmt &stmt);
  static const int64_t OUTLINE_CHILD_COUNT = 7;
  DISALLOW_COPY_AND_ASSIGN(ObCreateOutlineResolver);
};
}//namespace sql
}//namespace oceanbase
#endif //OCEANBASE_SQL_OB_CREATE_OUTLINE_RESOLVER_H_
