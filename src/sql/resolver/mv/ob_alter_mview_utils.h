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

private:
  template<typename T>
  static int resolve_interval_node(const ParseNode &node,
                                   ObSQLSessionInfo *session_info,
                                   common::ObIAllocator *allocator,
                                   ObResolverParams &resolver_params,
                                   T &arg);
};

} // namespace sql
} // namespace oceanbase
#endif
