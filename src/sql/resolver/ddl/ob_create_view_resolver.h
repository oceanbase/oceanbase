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

#ifndef OCEANBASE_SQL_RESOLVER_DDL_CREATE_VIEW_RESOLVER_H_
#define OCEANBASE_SQL_RESOLVER_DDL_CREATE_VIEW_RESOLVER_H_

#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "sql/resolver/dml/ob_view_table_resolver.h" // resolve select clause
#include "sql/resolver/ddl/ob_create_table_resolver_base.h"
#include "share/schema/ob_table_schema.h"
#include "lib/hash/ob_hashset.h"
#include "sql/resolver/ddl/ob_create_table_stmt.h" // share CREATE TABLE stmt

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObColumnSchemaV2;
}
}
namespace sql
{
class ObCreateViewResolver : public ObCreateTableResolverBase
{
  static const int64_t MATERIALIZED_NODE = 0;
  static const int64_t VIEW_NODE = 1;
  static const int64_t VIEW_COLUMNS_NODE = 2;
  static const int64_t TABLE_ID_NODE = 3;
  static const int64_t SELECT_STMT_NODE = 4;
  static const int64_t IF_NOT_EXISTS_NODE = 5;
  static const int64_t WITH_OPT_NODE = 6;
  static const int64_t FORCE_VIEW_NODE = 7;
  static const int64_t MVIEW_NODE = 8;
  static const int64_t PARTITION_NODE = 9;
  static const int64_t TABLE_OPTION_NODE = 10;
  static const int64_t HINT_NODE = 11;
  static const int64_t ROOT_NUM_CHILD = 12;

public:
  explicit ObCreateViewResolver(ObResolverParams &params);
  virtual ~ObCreateViewResolver();

  virtual int resolve(const ParseNode &parse_tree);
  static int resolve_select_node_for_force_view(bool &add_undefined_columns,
                                                ParseNode *select_stmt_node,
                                                ObIArray<SelectItem> &select_items);
  static int add_undefined_column_infos(const uint64_t tenant_id,
                                        ObIArray<SelectItem> &select_items,
                                        ObTableSchema &table_schema,
                                        const common::ObIArray<ObString> &column_list);
  static int try_add_undefined_column_infos(const uint64_t tenant_id,
                                            bool has_resolved_field_list,
                                            ParseNode *select_stmt_node,
                                            ObSelectStmt &select_stmt,
                                            ObTableSchema &table_schema,
                                            const common::ObIArray<ObString> &column_list);
  static int add_column_infos(const uint64_t tenant_id,
                              ObSelectStmt &select_stmt,
                              ObTableSchema &table_schema,
                              common::ObIAllocator &alloc,
                              sql::ObSQLSessionInfo &session_info,
                              const common::ObIArray<ObString> &column_list,
                              const common::ObIArray<ObString> &comment_list,
                              bool is_from_create_mview = false);
  static int fill_column_meta_infos(const ObRawExpr &expr,
                                    const ObCharsetType charset_type,
                                    const uint64_t table_id,
                                    ObColumnSchemaV2 &column,
                                    bool is_from_create_mview = false);
  static int resolve_column_default_value(const sql::ObSelectStmt *select_stmt,
                                        const sql::SelectItem &select_item,
                                        schema::ObColumnSchemaV2 &column_schema,
                                        common::ObIAllocator &alloc,
                                        sql::ObSQLSessionInfo &session_info);
  static int resolve_columns_nullable_value(const sql::ObSelectStmt *select_stmt,
                                            const ObTableSchema &table_schema,
                                            const sql::SelectItem &select_item,
                                            ObColumnSchemaV2 &column_schema,
                                            ObIAllocator &alloc,
                                            ObSQLSessionInfo &session_info,
                                            share::schema::ObSchemaGetterGuard *schema_guard);
private:
  int check_privilege(ObCreateTableStmt *stmt,
                      ObSelectStmt *select_stmt);
  int resolve_column_list(ParseNode *view_columns_node,
                          common::ObIArray<common::ObString> &column_list,
                          ParseNode *&mv_primary_key_node);
  int resolve_mv_options(const ObSelectStmt *stmt,
                         ParseNode *options_node,
                         ObMVRefreshInfo &refresh_info,
                         ObTableSchema &table_schema);
  int resolve_mv_refresh_info(ParseNode *refresh_info_node,
                              ObMVRefreshInfo &refresh_info);

  int check_view_stmt_col_name(ObSelectStmt &select_stmt,
                               ObArray<int64_t> &index_array,
                               common::hash::ObHashSet<ObString> &view_col_names);
  int check_view_columns(ObSelectStmt &select_stmt,
                         ParseNode *view_columns_node,
                         share::schema::ObErrorInfo &error_info,
                         const bool is_force_view,
                         bool &can_expand_star,
                         bool &add_undefined_columns);
  int check_privilege_needed(ObCreateTableStmt &stmt,
                             ObSelectStmt &select_stmt,
                             const bool is_force_view);
  int try_add_error_info(const uint64_t error_number,
                         share::schema::ObErrorInfo &error_info);
  int create_alias_names_auto(
      ObArray<int64_t> &index_array,
      ObSelectStmt *select_stmt,
      common::hash::ObHashSet<ObString> &view_col_names);
  /**
   * use stmt_print instead of ObSelectStmtPrinter. When do_print return OB_SIZE_OVERFLOW
   * and the buf_len is less than OB_MAX_PACKET_LENGTH, stmt_print will expand buf and try again.
   */
  int print_rebuilt_view_stmt(const ObSelectStmt *stmt,
                              common::ObIArray<common::ObString> *column_list,
                              common::ObString &expanded_view);
  int print_star_expanded_view_stmt(common::ObString &expanded_view,
                                    const int64_t view_definition_start_pos,
                                    const int64_t view_definition_end_pos);
  int collect_dependency_infos(ObQueryCtx *query_ctx,
                               obrpc::ObCreateTableArg &create_arg);
  int get_sel_priv_tables_in_subquery(const ObSelectStmt *child_stmt,
                                      hash::ObHashMap<int64_t, const TableItem *> &select_tables);
  int get_need_priv_tables(ObSelectStmt &select_stmt,
                           hash::ObHashMap<int64_t, const TableItem *> &select_tables,
                           hash::ObHashMap<int64_t, const TableItem *> &any_tables);
  int add_hidden_tablet_seq_col(ObTableSchema &table_schema);
  int resolve_materialized_view_container_table(ParseNode *partition_node,
                                                ParseNode *mv_primary_key_node,
                                                ObTableSchema &container_table_schema,
                                                ObSEArray<ObConstraint,4>& csts);
  int resolve_primary_key_node(ParseNode &pk_node, ObTableSchema &table_schema);
  int check_on_query_computation_supported(const ObSelectStmt *stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObCreateViewResolver);
};
}  // namespace sql
}  // namespace oceanbase
#endif // OCEANBASE_SQL_RESOLVER_DDL_OB_CREATE_VIEW_RESOLVER_H_
