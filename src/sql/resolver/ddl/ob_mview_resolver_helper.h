/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_RESOLVER_MV_RESOLVER_HELPER_H_
#define OCEANBASE_SQL_RESOLVER_MV_RESOLVER_HELPER_H_

#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "sql/resolver/dml/ob_view_table_resolver.h" // resolve select clause
#include "sql/resolver/ddl/ob_create_table_resolver_base.h"
#include "share/schema/ob_table_schema.h"
#include "lib/hash/ob_hashset.h"
#include "sql/resolver/ddl/ob_create_table_stmt.h" // share CREATE TABLE stmt
#include "sql/resolver/ddl/ob_create_view_resolver.h"

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
class ObMViewResolverHelper
{
public:
  static int resolve_materialized_view(const ParseNode &parse_tree,
                                       ObCreateTableStmt *stmt,
                                       ParseNode *mv_primary_key_node,
                                       ObTableSchema &table_schema,
                                       ObSelectStmt *select_stmt,
                                       ObCreateTableArg &create_arg,
                                       ObCreateViewResolver &resolver);
  static int resolve_refresh_method(const int64_t refresh_method_value,
                                    ObMVRefreshMethod &refresh_method);
  static int resolve_refresh_parallel_node(const ParseNode *refresh_parallel_node,
                                           int64_t &refresh_dop);
  static int resolve_nested_refresh_node(const ParseNode *nested_refresh_node,
                                         const uint64_t tenant_id,
                                         ObMVNestedRefreshMode &nested_refresh_mode);
  static int resolve_refresh_on_node(const ParseNode *refresh_on_node,
                                     ObMVRefreshMode &refresh_mode);
  static int resolve_refresh_interval_node(const ParseNode *start_date,
                                            const ParseNode *next_date,
                                            ObResolverParams &resolver_params,
                                            int64_t &start_time,
                                            ObString &next_time_expr,
                                            int64_t &period_sec);
  static int resolve_compat_version(const ParseNode *compat_version_node,
                                    const uint64_t tenant_id,
                                    uint64_t &compat_version);
  static int resolve_mv_refresh_info(const ParseNode *refresh_info_node,
                                     ObResolverParams &resolver_params,
                                     obrpc::ObMVRefreshInfo &refresh_info);
  static int check_duplicate_mv_options(const ParseNode *options_node);

private:
  static int resolve_mview_table_organization(const ParseNode *table_option_node,
                                               const ParseNode *mv_primary_key_node,
                                               ObTableOrganizationMode &organization_mode,
                                               ObCreateViewResolver &resolver);
  static int resolve_materialized_view_container_table(ParseNode *partition_node,
                                                       ParseNode *mv_primary_key_node,
                                                       const ObTableOrganizationMode organization_mode,
                                                       const uint64_t tenant_data_version,
                                                       ObTableSchema &container_table_schema,
                                                       ObSEArray<ObConstraint,4>& csts,
                                                       ObCreateViewResolver &resolver);
  static int resolve_primary_key_node(ParseNode &pk_node,
                                      ObTableSchema &table_schema,
                                      ObCreateViewResolver &resolver);
  static int resolve_mv_options(const ObSelectStmt *stmt,
                                ParseNode *options_node,
                                obrpc::ObMVRefreshInfo &refresh_info,
                                ObTableSchema &table_schema,
                                ObTableSchema &container_table_schema,
                                ObIArray<obrpc::ObMVRequiredColumnsInfo> &required_columns_infos,
                                ObCreateViewResolver &resolver);
  static int resolve_mv_fast_refresh(const ObSelectStmt *stmt,
                                      obrpc::ObMVRefreshInfo &refresh_info,
                                      ObTableSchema &table_schema,
                                      ObTableSchema &container_table_schema,
                                      ObIArray<obrpc::ObMVRequiredColumnsInfo> &required_columns_infos,
                                      ObCreateViewResolver &resolver);
  static int resolve_create_mview_options(const ParseNode *options_node,
                                          const ObSelectStmt *stmt,
                                          ObCreateViewResolver &resolver,
                                          obrpc::ObMVRefreshInfo &refresh_info,
                                          int64_t &mv_flags);
  static int init_create_mview_options(const uint64_t tenant_id,
                                       obrpc::ObMVRefreshInfo &refresh_info);
  static int resolve_mv_sched_suspended(const uint64_t tenant_id,
                                        bool &sched_suspended);
  static int fill_mv_refresh_interval(const ParseNode *start_node,
                                      const ParseNode *next_node,
                                      const uint64_t tenant_id,
                                      ObResolverParams &resolver_params,
                                      obrpc::ObMVRefreshInfo &refresh_info);
  static int check_mv_refresh_interval(const uint64_t tenant_id,
                                       const obrpc::ObMVRefreshInfo &refresh_info,
                                       const int64_t period_sec);
  static int resolve_mv_build_mode(const ParseNode *build_mode_node,
                                   ObMViewBuildMode &build_mode);
  static int check_on_query_computation_supported(const ObSelectStmt *stmt);
  static int load_mview_dep_session_vars(ObSQLSessionInfo &session_info,
                                         ObSelectStmt *stmt,
                                         ObLocalSessionVar &dep_vars);
  static int get_dep_session_vars_from_stmt(ObSQLSessionInfo &session_info,
                                            ObSelectStmt *stmt,
                                            ObLocalSessionVar &dep_vars);
  static int generate_hidden_column_comment(const std::pair<ObRawExpr *, int64_t> &dep_col,
                                            const bool is_set_stmt,
                                            ObSchemaGetterGuard *schema_guard,
                                            ObSQLSessionInfo *session_info,
                                            char *buf,
                                            ObString &comment_string);
  static int add_hidden_cols_for_mv(ObTableSchema &table_schema,
                                    const uint64_t column_id,
                                    const SelectItem &select_item,
                                    const ObString &comment_string,
                                    ObCreateViewResolver &resolver);
};
}  // namespace sql
}  // namespace oceanbase
#endif // OCEANBASE_SQL_RESOLVER_MV_RESOLVER_HELPER_H_
