/**
 * Copyright (c) 2025 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_OBSERVER_OB_TABLE_SQL_UTILS_H_
#define OCEANBASE_OBSERVER_OB_TABLE_SQL_UTILS_H_

#include "lib/mysqlclient/ob_isql_client.h"
#include "sql/parser/parse_node.h"
#include "sql/resolver/ob_stmt.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/resolver/ob_resolver.h"

namespace oceanbase
{
namespace table
{

class ObTableSqlUtils
{
public:
  static constexpr const char *DROP_TABLEGROUP_SQL = "DROP TABLEGROUP `%.*s`";
public:
  static int write(const uint64_t tenant_id, const char *sql, int64_t &affected_rows);
  static int read(const uint64_t tenant_id, const char *sql, common::ObISQLClient::ReadResult &res);
  // htable ddl
  static int create_table(common::ObIAllocator &allocator,
                          share::schema::ObSchemaGetterGuard &schema_guard,
                          sql::ObSQLSessionInfo &session,
                          int64_t timeout,
                          const common::ObString &database,
                          const common::ObString &create_tablegroup_sql,
                          const common::ObIArray<common::ObString> &create_table_sqls);
  static int drop_table(common::ObIAllocator &allocator,
                        share::schema::ObSchemaGetterGuard &schema_guard,
                        sql::ObSQLSessionInfo &session,
                        int64_t timeout,
                        const common::ObString &database,
                        const common::ObString &tablegroup);
  static int disable_table(common::ObIAllocator &allocator,
                           share::schema::ObSchemaGetterGuard &schema_guard,
                           sql::ObSQLSessionInfo &session,
                           int64_t timeout,
                           const common::ObString &database,
                           const common::ObString &tablegroup);
  static int enable_table(common::ObIAllocator &allocator,
                          share::schema::ObSchemaGetterGuard &schema_guard,
                          sql::ObSQLSessionInfo &session,
                          int64_t timeout,
                          const common::ObString &database,
                          const common::ObString &tablegroup);
private:
  static int parse(common::ObIAllocator &allocator,
                   sql::ObSQLSessionInfo &session,
                   const common::ObString &stmt,
                   ParseResult &result);
  static int parse(common::ObIAllocator &allocator,
                   sql::ObSQLSessionInfo &session,
                   const common::ObIArray<common::ObString> &stmts,
                   ParseResult *results,
                   const int64_t results_count);
  static int resolve(sql::ObResolver &resolver,
                     const ParseResult &parse_result,
                     sql::ObStmt *&stmt);
  static int resolve(sql::ObResolver &resolver,
                     const ParseResult *parse_results,
                     const int64_t parse_results_count,
                     common::ObIArray<sql::ObStmt*> &stmts);
  static int execute_create_table(sql::ObExecContext &ctx,
                                  const common::ObString &create_tablegroup_sql,
                                  const common::ObIArray<common::ObString> &create_table_sqls,
                                  sql::ObStmt &tablegroup_stmt,
                                  const common::ObIArray<sql::ObStmt*> &table_stmts,
                                  int64_t timeout);
  static int execute_drop_table(sql::ObExecContext &ctx,
                                const common::ObString &create_tablegroup_sql,
                                sql::ObStmt &tablegroup_stmt,
                                int64_t timeout);
  static int execute_set_kv_attribute(ObIAllocator &allocator,
                                      sql::ObExecContext &ctx,
                                      int64_t timeout,
                                      const common::ObString &database,
                                      const common::ObString &tablegroup,
                                      bool is_disable);
  static int check_parallel_ddl_schema_in_sync(sql::ObSQLSessionInfo &session,
                                               const int64_t schema_version,
                                               const int64_t timeout);
};

class ObTableSqlParserUtils
{
public:
  static int parse(common::ObIAllocator &allocator,
                   sql::ObSQLSessionInfo &session,
                   const common::ObString &stmt,
                   ParseResult &parse_result);
};

class ObTableRsExecutor
{
public:
  static int execute(sql::ObExecContext &ctx, obrpc::ObHTableDDLArg &arg, obrpc::ObHTableDDLRes &res);
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableRsExecutor);
};

class ObTableMockUtils
{
public:
  static int execute(common::ObIAllocator &allocator, sql::ObSQLSessionInfo &session, share::schema::ObSchemaGetterGuard &schema_guard);
  static int execute_set_kv_attribute(bool is_disable);
};

} // end namespace table
} // end namespace oceanbase

#endif /* OCEANBASE_OBSERVER_OB_TABLE_SQL_UTILS_H_ */
