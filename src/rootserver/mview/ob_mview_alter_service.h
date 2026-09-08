/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_ROOTSERVER_MVIEW_OB_MVIEW_ALTER_SERVICE_H_
#define OCEANBASE_ROOTSERVER_MVIEW_OB_MVIEW_ALTER_SERVICE_H_

#include "lib/ob_define.h"
#include "share/schema/ob_table_schema.h"
#include "src/share/ob_rpc_struct.h"
#include "rootserver/ob_ddl_service.h"

namespace oceanbase
{
namespace sql
{
class ObSchemaChecker;
class ObSelectStmt;
class ObSQLSessionInfo;
}
namespace rootserver
{
class ObMviewAlterService
{
public:
  static int alter_mview_or_mlog_in_trans(obrpc::ObAlterTableArg &alter_table_arg,
                                          obrpc::ObAlterTableRes &res,
                                          ObSchemaGetterGuard &schema_guard,
                                          share::schema::ObMultiVersionSchemaService *schema_service,
                                          common::ObMySQLProxy *sql_proxy,
                                          const uint64_t tenant_data_version);
  static int update_mlog_and_mview_in_alter_column(const share::schema::ObTableSchema &new_table_schema,
                                                   const share::schema::ObColumnSchemaV2 &new_column_schema,
                                                   const share::schema::ObSchemaOperationType column_operation_type,
                                                   share::schema::ObSchemaGetterGuard &schema_guard,
                                                   ObDDLOperator &ddl_operator,
                                                   common::ObMySQLTransaction &trans);

private:
  class ObMviewStmtHandler;
  class ObCheckColumnReferenceHandler;
  class ObUpdateMvContainerSchemaHandler;

  static int alter_mview_attributes(const uint64_t tenant_id,
                                    const ObTableSchema *orig_table_schema,
                                    obrpc::ObAlterTableArg &alter_table_arg,
                                    ObDDLOperator &ddl_operator, ObSchemaGetterGuard &schema_guard,
                                    ObDDLSQLTransaction &trans);
  static int alter_mlog_attributes(const uint64_t tenant_id, const ObTableSchema *orig_table_schema,
                                   obrpc::ObAlterTableArg &alter_table_arg,
                                   ObDDLOperator &ddl_operator, ObSchemaGetterGuard &schema_guard,
                                   ObDDLSQLTransaction &trans);
  static int update_mlog_in_modify_column(const share::schema::ObTableSchema &new_table_schema,
                                          share::schema::ObSchemaGetterGuard &schema_guard,
                                          ObDDLOperator &ddl_operator,
                                          common::ObMySQLTransaction &trans);
  static int update_mview_in_modify_column(const share::schema::ObTableSchema &new_table_schema,
                                           share::schema::ObSchemaGetterGuard &schema_guard,
                                           ObDDLOperator &ddl_operator,
                                           common::ObMySQLTransaction &trans);
  static int update_mview_with_new_table(const uint64_t mv_id,
                                         const share::schema::ObTableSchema &new_table_schema,
                                         share::schema::ObSchemaGetterGuard &schema_guard,
                                         ObDDLOperator &ddl_operator,
                                         common::ObMySQLTransaction &trans);
  static int check_column_referenced_by_mlog_or_mview(const share::schema::ObTableSchema &base_table_schema,
                                                      const uint64_t base_column_id,
                                                      share::schema::ObSchemaGetterGuard &schema_guard,
                                                      common::ObMySQLTransaction &trans,
                                                      bool &is_referenced);
  static int check_column_referenced_by_mview(const uint64_t tenant_id,
                                              const uint64_t mview_id,
                                              const uint64_t base_table_id,
                                              const uint64_t base_column_id,
                                              share::schema::ObSchemaGetterGuard &schema_guard,
                                              bool &is_referenced);
  static int check_column_referenced_by_stmt(const sql::ObSelectStmt *stmt,
                                             const uint64_t base_table_id,
                                             const uint64_t base_column_id,
                                             bool &is_referenced);
  static int rebuild_container_schema_with_new_table(share::schema::ObSchemaGetterGuard &schema_guard,
                                                     const share::schema::ObTableSchema &orig_mv_schema,
                                                     const share::schema::ObTableSchema &orig_container_schema,
                                                     const share::schema::ObTableSchema &new_table_schema,
                                                     share::schema::ObTableSchema &container_schema);
  static int rebuild_mv_container_schema(sql::ObSchemaChecker &schema_checker,
                                         const share::schema::ObTableSchema &orig_mv_schema,
                                         const share::schema::ObTableSchema &orig_container_schema,
                                         share::schema::ObTableSchema &new_container_schema);
  static int update_mv_container_schema_with_stmt(const sql::ObSelectStmt *stmt,
                                                  sql::ObSQLSessionInfo &session_info,
                                                  share::schema::ObTableSchema &mv_container_schema);
  static int resolve_mv_definition(sql::ObSchemaChecker &schema_checker,
                                   const share::schema::ObTableSchema &mv_schema,
                                   ObMviewStmtHandler &handler);
};

} // namespace rootserver
} // namespace oceanbase
#endif
