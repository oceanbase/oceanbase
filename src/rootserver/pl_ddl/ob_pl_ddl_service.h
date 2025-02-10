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

#ifndef _OCEANBASE_ROOTSERVER_OB_PL_DDL_SERVICE_H_
#define _OCEANBASE_ROOTSERVER_OB_PL_DDL_SERVICE_H_

#include "rootserver/ob_ddl_service.h"
#include "ob_pl_ddl_operator.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_dependency_info.h"

namespace oceanbase
{
using namespace obrpc;
using namespace share;

namespace rootserver
{
class ObDDLSQLTransaction;

class ObPLDDLService
{
public:
  //----Functions for managing routine----
  static int create_routine(const obrpc::ObCreateRoutineArg &arg,
                            obrpc::ObRoutineDDLRes* res,
                            rootserver::ObDDLService &ddl_service);
  static int alter_routine(const obrpc::ObCreateRoutineArg &arg,
                           obrpc::ObRoutineDDLRes* res,
                           rootserver::ObDDLService &ddl_service);
  static int drop_routine(const ObDropRoutineArg &arg,
                          rootserver::ObDDLService &ddl_service);
  //----End of functions for managing routine----

  //----Functions for managing udt----
  static int create_udt(const obrpc::ObCreateUDTArg &arg,
                        obrpc::ObRoutineDDLRes *res,
                        rootserver::ObDDLService &ddl_service);
  static int drop_udt(const ObDropUDTArg &arg,
                      rootserver::ObDDLService &ddl_service);
  //----End of functions for managing udt----

  //----Functions for managing package----
  static int create_package(const obrpc::ObCreatePackageArg &arg,
                            obrpc::ObRoutineDDLRes *res,
                            rootserver::ObDDLService &ddl_service);
  static int alter_package(const obrpc::ObAlterPackageArg &arg,
                           obrpc::ObRoutineDDLRes *res,
                           rootserver::ObDDLService &ddl_service);
  static int drop_package(const obrpc::ObDropPackageArg &arg,
                          rootserver::ObDDLService &ddl_service);
  //----End of functions for managing package----

  //----Functions for managing trigger----
  static int create_trigger(const obrpc::ObCreateTriggerArg &arg,
                            obrpc::ObCreateTriggerRes *res,
                            rootserver::ObDDLService &ddl_service);
  static int alter_trigger(const obrpc::ObAlterTriggerArg &arg,
                           obrpc::ObRoutineDDLRes *res,
                           rootserver::ObDDLService &ddl_service);
  static int drop_trigger(const obrpc::ObDropTriggerArg &arg,
                          rootserver::ObDDLService &ddl_service);
  static int drop_trigger_in_drop_table(ObMySQLTransaction &trans,
                                        rootserver::ObDDLOperator &ddl_operator,
                                        share::schema::ObSchemaGetterGuard &schema_guard,
                                        const share::schema::ObTableSchema &table_schema,
                                        const bool to_recyclebin);
  static int drop_trigger_in_drop_user(ObMySQLTransaction &trans,
                                      rootserver::ObDDLOperator &ddl_operator,
                                      ObSchemaGetterGuard &schema_guard,
                                      const uint64_t tenant_id,
                                      const uint64_t user_id);
  static int rebuild_triggers_on_hidden_table(const ObTableSchema &orig_table_schema,
                                              const ObTableSchema &hidden_table_schema,
                                              ObSchemaGetterGuard &src_tenant_schema_guard,
                                              ObSchemaGetterGuard &dst_tenant_schema_guard,
                                              rootserver::ObDDLOperator &ddl_operator,
                                              ObMySQLTransaction &trans);
  static int rebuild_trigger_on_rename(share::schema::ObSchemaGetterGuard &schema_guard,
                                       const share::schema::ObTableSchema &table_schema,
                                       rootserver::ObDDLOperator &ddl_operator,
                                       ObMySQLTransaction &trans);
  static int rebuild_trigger_on_rename(share::schema::ObSchemaGetterGuard &schema_guard,
                                       const uint64_t tenant_id,
                                       const common::ObIArray<uint64_t> &trigger_list,
                                       const common::ObString &database_name,
                                       const common::ObString &table_name,
                                       rootserver::ObDDLOperator &ddl_operator,
                                       ObMySQLTransaction &trans);
  static int create_trigger_for_truncate_table(share::schema::ObSchemaGetterGuard &schema_guard,
                                               const common::ObIArray<uint64_t> &origin_trigger_list,
                                               share::schema::ObTableSchema &new_table_schema,
                                               rootserver::ObDDLOperator &ddl_operator,
                                               ObMySQLTransaction &trans);
  static int flashback_trigger(const share::schema::ObTableSchema &table_schema,
                               const uint64_t new_database_id,
                               const common::ObString &new_table_name,
                               share::schema::ObSchemaGetterGuard &schema_guard,
                               ObMySQLTransaction &trans,
                               rootserver::ObDDLOperator &ddl_operator);
  //----End of functions for managing trigger----
private:
  template <typename ArgType>
  static int check_env_before_ddl(share::schema::ObSchemaGetterGuard &schema_guard,
                                  const ArgType &arg,
                                  rootserver::ObDDLService &ddl_service);
  //----Functions for managing routine----
  static int create_routine(ObRoutineInfo &routine_info,
                            const ObRoutineInfo* old_routine_info,
                            bool replace,
                            ObErrorInfo &error_info,
                            ObIArray<ObDependencyInfo> &dep_infos,
                            const ObString *ddl_stmt_str,
                            share::schema::ObSchemaGetterGuard &schema_guard,
                            rootserver::ObDDLService &ddl_service);
  static int alter_routine(const ObRoutineInfo &routine_info,
                           ObErrorInfo &error_info,
                           const ObString *ddl_stmt_str,
                           share::schema::ObSchemaGetterGuard &schema_guard,
                           rootserver::ObDDLService &ddl_service);
  static int drop_routine(const ObRoutineInfo &routine_info,
                          ObErrorInfo &error_info,
                          const ObString *ddl_stmt_str,
                          share::schema::ObSchemaGetterGuard &schema_guard,
                          rootserver::ObDDLService &ddl_service);
  //----End of functions for managing routine----

  //----Functions for managing udt----
  static int create_udt(ObUDTTypeInfo &udt_info,
                        const ObUDTTypeInfo* old_udt_info,
                        ObIArray<ObRoutineInfo> &public_routine_infos,
                        ObErrorInfo &error_info,
                        ObSchemaGetterGuard &schema_guard,
                        ObIArray<ObDependencyInfo> &dep_infos,
                        const ObString *ddl_stmt_str,
                        bool need_replace,
                        bool exist_valid_udt,
                        bool specify_force,
                        rootserver::ObDDLService &ddl_service);
  static int drop_udt(const ObUDTTypeInfo &udt_info,
                       share::schema::ObSchemaGetterGuard &schema_guard,
                       const common::ObString *ddl_stmt_str,
                       bool specify_force,
                       bool exist_valid_udt,
                       rootserver::ObDDLService &ddl_service);
  //----End of functions for managing udt----

  //----Functions for managing package----
  static int create_package(ObSchemaGetterGuard &schema_guard,
                            const ObPackageInfo *old_package_info,
                            ObPackageInfo &new_package_info,
                            ObIArray<ObRoutineInfo> &public_routine_infos,
                            ObErrorInfo &error_info,
                            ObIArray<ObDependencyInfo> &dep_infos,
                            const ObString *ddl_stmt_str,
                            rootserver::ObDDLService &ddl_service);
  static int alter_package(ObSchemaGetterGuard &schema_guard,
                           ObPackageInfo &package_info,
                           ObIArray<ObRoutineInfo> &public_routine_infos,
                           share::schema::ObErrorInfo &error_info,
                           const ObString *ddl_stmt_str,
                           rootserver::ObDDLService &ddl_service);
  static int drop_package(ObSchemaGetterGuard &schema_guard,
                          const ObPackageInfo &package_info,
                          ObErrorInfo &error_info,
                          const ObString *ddl_stmt_str,
                          rootserver::ObDDLService &ddl_service);
  //----Functions for managing trigger----
  static int create_trigger(const obrpc::ObCreateTriggerArg &arg,
                            ObSchemaGetterGuard &schema_guard,
                            obrpc::ObCreateTriggerRes *res,
                            rootserver::ObDDLService &ddl_service);
  static int create_trigger_in_trans(share::schema::ObTriggerInfo &trigger_info,
                                      share::schema::ObErrorInfo &error_info,
                                      ObIArray<ObDependencyInfo> &dep_infos,
                                      const common::ObString *ddl_stmt_str,
                                      bool in_second_stage,
                                      share::schema::ObSchemaGetterGuard &schema_guard,
                                      int64_t &table_schema_version,
                                      rootserver::ObDDLService &ddl_service);
  static int drop_trigger_in_trans(const share::schema::ObTriggerInfo &trigger_info,
                                    const common::ObString *ddl_stmt_str,
                                    share::schema::ObSchemaGetterGuard &schema_guard,
                                    rootserver::ObDDLService &ddl_service);
  static int try_get_exist_trigger(share::schema::ObSchemaGetterGuard &schema_guard,
                                    const share::schema::ObTriggerInfo &new_trigger_info,
                                    const share::schema::ObTriggerInfo *&old_trigger_info,
                                    bool with_replace);
  static int adjust_trigger_action_order(share::schema::ObSchemaGetterGuard &schema_guard,
                                          rootserver::ObDDLSQLTransaction &trans,
                                          ObPLDDLOperator &pl_operator,
                                          ObTriggerInfo &trigger_info,
                                          bool is_create_trigger);
  static int recursive_alter_ref_trigger(share::schema::ObSchemaGetterGuard &schema_guard,
                                          rootserver::ObDDLSQLTransaction &trans,
                                          ObPLDDLOperator &pl_operator,
                                          const ObTriggerInfo &ref_trigger_info,
                                          const common::ObIArray<uint64_t> &trigger_list,
                                          const ObString &trigger_name,
                                          int64_t action_order);
  static int recursive_check_trigger_ref_cyclic(share::schema::ObSchemaGetterGuard &schema_guard,
                                                const ObTriggerInfo &ref_trigger_info,
                                                const common::ObIArray<uint64_t> &trigger_list,
                                                const ObString &create_trigger_name,
                                                const ObString &generate_cyclic_name);
  static int get_object_info(ObSchemaGetterGuard &schema_guard,
                             const uint64_t tenant_id,
                             const ObString &object_database,
                             const ObString &object_name,
                             ObSchemaType &object_type,
                             uint64_t &object_id,
                             rootserver::ObDDLService &ddl_service);
  //----End of functions for managing trigger----
};

} // namespace rootserver
} // namespace oceanbase

#endif // _OCEANBASE_ROOTSERVER_OB_PL_DDL_SERVICE_H_
