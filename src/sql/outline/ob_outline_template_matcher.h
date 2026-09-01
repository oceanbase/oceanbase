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

#ifndef OCEANBASE_SQL_OUTLINE_OB_OUTLINE_TEMPLATE_MATCHER_H_
#define OCEANBASE_SQL_OUTLINE_OB_OUTLINE_TEMPLATE_MATCHER_H_

#include <utility>
#include "lib/string/ob_string.h"
#include "lib/container/ob_se_array.h"
#include "lib/allocator/ob_allocator.h"
#include "sql/parser/parse_node.h"

namespace oceanbase
{
namespace share { namespace schema { class ObSchemaGetterGuard; class ObOutlineInfo; } }
namespace sql
{
class ObPlanCacheCtx;
class ObSQLSessionInfo;
class ObOutlineBindingRule;
class ObDMLStmt;
struct TableItem;

/**
 * @brief ObOutlineTemplateMatcher - Template outline matching and content reconstruction
 *
 * Extracted from ObSql to reduce ob_sql.cpp size and improve modularity.
 * Handles BINDING_RULE-based template outlines: signature generation,
 * pattern matching, and placeholder reconstruction.
 */
// pair<table_name, db_name>, indexed together to prevent misalignment
typedef std::pair<common::ObString, common::ObString> ObTableDbName;

class ObOutlineTemplateMatcher
{
public:
  /**
   * Facade: Try to match a template outline for the given query.
   * Generates template signatures, queries candidate outlines from schema,
   * and performs pattern matching. Sets outline_info if a match is found.
   */
  static int try_match_template_outline(
      ObPlanCacheCtx &pc_ctx,
      share::schema::ObSchemaGetterGuard *schema_guard,
      ObSQLSessionInfo *session,
      uint64_t database_id,
      const common::ObString &signature_sql,
      const share::schema::ObOutlineInfo *&outline_info);

  /**
   * Facade: Reconstruct outline_content with actual table names if needed.
   * Replaces placeholders (DB_xxx$N, TB_xxx$N) with actual table names
   * from the current query's parse tree.
   */
  static int reconstruct_content_if_needed(
      ObPlanCacheCtx &pc_ctx,
      const share::schema::ObOutlineInfo *outline_info,
      common::ObIAllocator &allocator,
      common::ObString &outline_content);


  /**
   * Match a single template outline candidate against the current query.
   * Deserializes pattern rules, performs ICU pattern matching.
   */
  static int match_template_outline(
      ObPlanCacheCtx &pc_ctx,
      const share::schema::ObOutlineInfo &outline_info,
      bool &matched);

  /**
   * Reconstruct outline content by replacing placeholders with actual table names.
   */
  static int reconstruct_outline_content(
      const common::ObString &template_content,
      const ObOutlineBindingRule &binding_rule,
      const common::ObIArray<common::ObString> &actual_table_names,
      common::ObIAllocator &allocator,
      common::ObString &result);

  /**
   * Check whether a TableItem had an explicit database prefix in the original SQL.
   * Uses ParseNode (table_item->node_) to distinguish user-written "db.table"
   * from resolver-filled database_name_.
   */
  static bool has_explicit_db_prefix(const TableItem *table_item);

  /**
   * Generate template signature via parse-tree text surgery (no resolve).
   * Locates db/table identifier tokens in sql_text using each ParseNode's
   * sql_str_off_, overwrites them with wildcards (every base table normalizes
   * to "*.*"; column-ref table qualifiers that name a physical FROM table
   * become wildcards, aliases are preserved), then fast-parameterizes via
   * get_outline_key. Shared by creation and matching phases so signatures are
   * byte-symmetric. stmt_node's identifier offsets MUST be relative to sql_text.
   */
  static int generate_template_signature_from_parse_tree(
      const common::ObString &sql_text,
      const ParseNode *stmt_node,
      ObSQLSessionInfo *session,
      common::ObIAllocator &allocator,
      bool need_format,
      common::ObString &template_signature);

  /**
   * Generic AST DFS: traverse the statement parse tree, collect real
   * T_RELATION_FACTOR table references in DFS order, and skip hint-only
   * subtrees so hint table names do not affect ast_position assignment.
   * Works for any statement type (SELECT/UPDATE/DELETE/INSERT/MERGE).
   * Both creation and matching phases call this function to ensure
   * symmetric table ordering.
   */
  static int collect_table_names_dfs(
      const ParseNode *node,
      common::ObIArray<ObTableDbName> &table_db_names);

  /**
   * Generate outline_match_template_signature_ from the query's parse tree for
   * the has_template_outline == false case where try_match_template_outline()
   * left it empty. Reuses sql_ctx_.outline_match_parse_result_ (no resolve) and
   * keeps the signature byte-identical to the DDL side. Best-effort: never
   * reports failure to the hard-parse path.
   */
  static int try_gen_template_signature(
      ObPlanCacheCtx &pc_ctx);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_OUTLINE_OB_OUTLINE_TEMPLATE_MATCHER_H_
