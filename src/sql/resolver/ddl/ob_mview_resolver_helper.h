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

private:
  static int resolve_materialized_view_container_table(ParseNode *partition_node,
                                                       ParseNode *mv_primary_key_node,
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
  static int resolve_mv_refresh_info(ParseNode *refresh_info_node,
                                     obrpc::ObMVRefreshInfo &refresh_info,
                                     ObCreateViewResolver &resolver);
  static int check_on_query_computation_supported(const ObSelectStmt *stmt);
  static int load_mview_dep_session_vars(ObSQLSessionInfo &session_info,
                                         ObSelectStmt *stmt,
                                         ObLocalSessionVar &dep_vars);
  static int get_dep_session_vars_from_stmt(ObSQLSessionInfo &session_info,
                                            ObSelectStmt *stmt,
                                            ObLocalSessionVar &dep_vars);
};
}  // namespace sql
}  // namespace oceanbase
#endif // OCEANBASE_SQL_RESOLVER_MV_RESOLVER_HELPER_H_
