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

#ifndef OCEANBASE_SQL_RESOLVER_ALTER_MVIEW_UTILS_H_
#define OCEANBASE_SQL_RESOLVER_ALTER_MVIEW_UTILS_H_

#include "lib/ob_define.h"
#include "sql/resolver/ddl/ob_ddl_resolver.h"

namespace oceanbase
{
namespace sql
{

class ObAlterMviewUtils
{
public:
  static int resolve_mv_options(const ParseNode &node,
                                ObSQLSessionInfo *session_info,
                                ObAlterTableStmt *alter_table_stmt,
                                const share::schema::ObTableSchema *table_schema,
                                ObSchemaGetterGuard *schema_guard,
                                common::ObIAllocator *allocator,
                                ObResolverParams &resolver_params);

  static int resolve_mlog_options(const ParseNode &node,
                                  ObSQLSessionInfo *session_info,
                                  ObAlterTableStmt *alter_table_stmt,
                                  common::ObIAllocator *allocator,
                                  ObResolverParams &resolver_params);
  static int check_column_option_for_mv_base_table(const ObTableSchema &table_schema,
                                                   const ObItemType type);
  static int check_action_node_for_mv_base_table(const ObTableSchema &table_schema,
                                                 const ObItemType type);
  static int check_partition_option_for_mv_base_table(const ObTableSchema &table_schema,
                                                      const ObItemType type);

  static int check_database_referenced_by_mv_from_other_database(
      common::ObISQLClient &sql_client,
      const uint64_t tenant_id,
      const uint64_t database_id);
  static int check_complete_refresh_min_interval(const uint64_t tenant_id,
                                                 const uint64_t mview_id,
                                                 const int64_t period_sec);
};

} // namespace sql
} // namespace oceanbase
#endif
