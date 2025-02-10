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

#ifndef OCEANBASE_ROOTSERVER_OB_DDL_PL_OPERATOR_H_
#define OCEANBASE_ROOTSERVER_OB_DDL_PL_OPERATOR_H_

#include "rootserver/ob_ddl_operator.h"

namespace oceanbase
{

namespace rootserver
{


class ObPLDDLOperator : public rootserver::ObDDLOperator
{
public:
  ObPLDDLOperator(share::schema::ObMultiVersionSchemaService &schema_service,
                  common::ObMySQLProxy &sql_proxy);
  virtual ~ObPLDDLOperator();

  //----Functions for managing routine----
  int create_routine(share::schema::ObRoutineInfo &routine_info,
                      common::ObMySQLTransaction &trans,
                      share::schema::ObErrorInfo &error_info,
                      common::ObIArray<share::schema::ObDependencyInfo> &dep_infos,
                      const common::ObString *ddl_stmt_str/*=NULL*/);
  int replace_routine(share::schema::ObRoutineInfo &routine_info,
                      const share::schema::ObRoutineInfo *old_routine_info,
                      common::ObMySQLTransaction &trans,
                      share::schema::ObErrorInfo &error_info,
                      common::ObIArray<share::schema::ObDependencyInfo> &dep_infos,
                      const common::ObString *ddl_stmt_str/*=NULL*/);
  int alter_routine(const share::schema::ObRoutineInfo &routine_info,
                    common::ObMySQLTransaction &trans,
                    share::schema::ObErrorInfo &error_info,
                    const common::ObString *ddl_stmt_str/*=NULL*/);
  int drop_routine(const share::schema::ObRoutineInfo &routine_info,
                    common::ObMySQLTransaction &trans,
                     share::schema::ObErrorInfo &error_info,
                     const common::ObString *ddl_stmt_str/*=NULL*/);
  //----End of functions for managing routine----

  //----Functions for managing udt----
  int create_udt(share::schema::ObUDTTypeInfo &udt_info,
                 common::ObMySQLTransaction &trans,
                 share::schema::ObErrorInfo &error_info,
                 common::ObIArray<share::schema::ObRoutineInfo> &public_routine_infos,
                 share::schema::ObSchemaGetterGuard &schema_guard,
                 common::ObIArray<share::schema::ObDependencyInfo> &dep_infos,
                 const common::ObString *ddl_stmt_str/*=NULL*/);
  int replace_udt(share::schema::ObUDTTypeInfo &udt_info,
                  const share::schema::ObUDTTypeInfo *old_udt_info,
                  common::ObMySQLTransaction &trans,
                  share::schema::ObErrorInfo &error_info,
                  common::ObIArray<share::schema::ObRoutineInfo> &public_routine_infos,
                  share::schema::ObSchemaGetterGuard &schema_guard,
                  common::ObIArray<share::schema::ObDependencyInfo> &dep_infos,
                  const common::ObString *ddl_stmt_str/*=NULL*/);
  int drop_udt(const share::schema::ObUDTTypeInfo &udt_info,
               common::ObMySQLTransaction &trans,
               share::schema::ObSchemaGetterGuard &schema_guard,
               const common::ObString *ddl_stmt_str/*=NULL*/);
  //----End of functions for managing udt----

  //----Functions for managing package----
  int create_package(const share::schema::ObPackageInfo *old_package_info,
                     share::schema::ObPackageInfo &new_package_info,
                     common::ObMySQLTransaction &trans,
                     share::schema::ObSchemaGetterGuard &schema_guard,
                     common::ObIArray<share::schema::ObRoutineInfo> &public_routine_infos,
                     share::schema::ObErrorInfo &error_info,
                     common::ObIArray<share::schema::ObDependencyInfo> &dep_infos,
                     const common::ObString *ddl_stmt_str/*=NULL*/);
  int alter_package(share::schema::ObPackageInfo &package_info,
                    ObSchemaGetterGuard &schema_guard,
                    common::ObMySQLTransaction &trans,
                    ObIArray<ObRoutineInfo> &public_routine_infos,
                    share::schema::ObErrorInfo &error_info,
                    const common::ObString *ddl_stmt_str);
  int drop_package(const share::schema::ObPackageInfo &package_info,
                   common::ObMySQLTransaction &trans,
                   share::schema::ObSchemaGetterGuard &schema_guard,
                   share::schema::ObErrorInfo &error_info,
                   const common::ObString *ddl_stmt_str/*=NULL*/);
  //----End of functions for managing package----
  int create_trigger(share::schema::ObTriggerInfo &trigger_info,
                     common::ObMySQLTransaction &trans,
                     share::schema::ObErrorInfo &error_info,
                     ObIArray<ObDependencyInfo> &dep_infos,
                     int64_t &table_schema_version,
                     const common::ObString *ddl_stmt_str/*=NULL*/,
                     bool is_update_table_schema_version = true,
                     bool is_for_truncate_table = false);
  // set ddl_stmt_str to NULL if the statement is not 'drop trigger xxx'.
  int drop_trigger(const share::schema::ObTriggerInfo &trigger_info,
                   common::ObMySQLTransaction &trans,
                   const common::ObString *ddl_stmt_str/*=NULL*/,
                   bool is_update_table_schema_version = true,
                   const bool in_offline_ddl_white_list = false);
  int drop_trigger_to_recyclebin(const share::schema::ObTriggerInfo &trigger_info,
                                 share::schema::ObSchemaGetterGuard &schema_guard,
                                 common::ObMySQLTransaction &trans);
  int alter_trigger(share::schema::ObTriggerInfo &trigger_info,
                    common::ObMySQLTransaction &trans,
                    const common::ObString *ddl_stmt_str/*=NULL*/,
                    bool is_update_table_schema_version = true);
  int flashback_trigger(const share::schema::ObTriggerInfo &trigger_info,
                        uint64_t new_database_id,
                        const common::ObString &new_table_name,
                        share::schema::ObSchemaGetterGuard &schema_guard,
                        common::ObMySQLTransaction &trans);
  static int purge_table_trigger(const share::schema::ObTableSchema &table_schema,
                                 share::schema::ObSchemaGetterGuard &schema_guard,
                                 common::ObMySQLTransaction &trans,
                                 ObDDLOperator &ddl_operator);
  int purge_trigger(const share::schema::ObTriggerInfo &trigger_info,
                    common::ObMySQLTransaction &trans);
  int rebuild_trigger_on_rename(const share::schema::ObTriggerInfo &trigger_info,
                                const common::ObString &database_name,
                                const common::ObString &table_name,
                                common::ObMySQLTransaction &trans);
  static int drop_trigger_in_drop_database(uint64_t tenant_id,
                                           const ObDatabaseSchema &db_schema,
                                           ObDDLOperator &ddl_operator,
                                           ObMySQLTransaction &trans);
  static int drop_trigger_cascade(const share::schema::ObTableSchema &table_schema,
                                  common::ObMySQLTransaction &trans,
                                  ObDDLOperator &ddl_operator);
  //----Functions for managing trigger----

  //----End of functions for managing trigger----
private:
  static int insert_dependency_infos(common::ObMySQLTransaction &trans,
                                     common::ObIArray<share::schema::ObDependencyInfo> &dep_infos,
                                     uint64_t tenant_id,
                                     uint64_t dep_obj_id,
                                     uint64_t schema_version,
                                     uint64_t owner_id);
  int del_routines_in_package(const share::schema::ObPackageInfo &package_info,
                              common::ObMySQLTransaction &trans,
                              share::schema::ObSchemaGetterGuard &schema_guard);
  int del_routines_in_udt(const share::schema::ObUDTTypeInfo &udt_info,
                          common::ObMySQLTransaction &trans,
                          share::schema::ObSchemaGetterGuard &schema_guard);
  int fill_trigger_id(share::schema::ObSchemaService &schema_service,
                      share::schema::ObTriggerInfo &trigger_info);
  int update_routine_info(share::schema::ObRoutineInfo &routine_info,
                          int64_t tenant_id,
                          int64_t parent_id,
                          int64_t owner_id,
                          int64_t database_id,
                          int64_t routine_id = OB_INVALID_ID);
  template <typename SchemaType>
  int build_flashback_object_name(const SchemaType &object_schema,
                                  const char *data_table_prifix,
                                  const char *object_type_prefix,
                                  share::schema::ObSchemaGetterGuard &schema_guard,
                                  common::ObIAllocator &allocator,
                                  common::ObString &object_name);
};

} // namespace rootserver
} // namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_OB_DDL_PL_OPERATOR_H_
