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

#ifndef OCEANBASE_ROOTSERVER_OB_DDL_OPERATOR_H_
#define OCEANBASE_ROOTSERVER_OB_DDL_OPERATOR_H_

#include "lib/container/ob_iarray.h"
#include "lib/list/ob_dlist.h"
#include "lib/list/ob_dlink_node.h"
#include "lib/string/ob_string.h"
#include "lib/string/ob_sql_string.h"
#include "share/schema/ob_ddl_sql_service.h"
#include "share/config/ob_server_config.h"
#include "share/ob_get_compat_mode.h"
#include "share/ob_partition_modify.h"
#include "share/ob_rpc_struct.h"

namespace oceanbase
{
namespace obrpc
{
class ObAccountArg;
class ObSplitPartitionArg;
class ObAlterTablegroupArg;
class ObSetPasswdArg;
class ObAlterIndexParallelArg;
class ObSequenceDDLArg;
class ObAlterIndexTablespaceArg;
class ObSchemaReviseArg;
}

namespace common
{
class ObIAllocator;
class ObMySQLTransaction;
class ObMySQLProxy;
class ObISQLClient;
}
namespace share
{
class SCN;
}
namespace sql
{
class ObSchemaChecker;
}

namespace share
{
class ObSplitInfo;
namespace schema
{
class ObMultiVersionSchemaService;
class ObTenantSchema;
class ObDatabaseSchema;
class ObTablegroupSchema;
class ObUser;
class ObTableSchema;
class AlterTableSchema;
class ObColumnSchemaV2;
class AlterColumnSchema;
struct ObSchemaOperation;
class ObSchemaService;
class ObTriggerInfo;
class ObConstraint;
class ObUDF;
class ObDependencyInfo;
}
}

namespace obrpc
{
class ObSrvRpcProxy;
class ObDropIndexArg;
class ObAlterIndexArg;
class ObRenameIndexArg;
class ObCreateTenantArg;
class ObDropForeignKeyArg;
} // end of namespace rpc

namespace rootserver
{

class ObDDLOperator
{
public:
  ObDDLOperator(share::schema::ObMultiVersionSchemaService &schema_service,
                common::ObMySQLProxy &sql_proxy);
  virtual ~ObDDLOperator();

  virtual int create_tenant(share::schema::ObTenantSchema &tenant_schema,
                            const share::schema::ObSchemaOperationType op,
                            common::ObMySQLTransaction &trans,
                            const common::ObString *ddl_stmt_str = NULL);

  virtual int drop_tenant(const uint64_t tenant_id, common::ObMySQLTransaction &trans,
                          const common::ObString *ddl_stmt_str = NULL);
  virtual int alter_tenant(share::schema::ObTenantSchema &tenant_schema,
                           common::ObMySQLTransaction &trans,
                           const common::ObString *ddl_stmt_str = NULL);
  virtual int rename_tenant(share::schema::ObTenantSchema &tenant_schema,
                           common::ObMySQLTransaction &trans,
                           const common::ObString *ddl_stmt_str = NULL);
  virtual int delay_to_drop_tenant(share::schema::ObTenantSchema &tenant_schema,
                                   common::ObMySQLTransaction &trans,
                                   const common::ObString *ddl_stmt_str = NULL);
  virtual int drop_tenant_to_recyclebin(common::ObSqlString &new_tenant_name,
                                        share::schema::ObTenantSchema &tenant_schema,
                                        common::ObMySQLTransaction &trans,
                                        const common::ObString *ddl_stmt_str = NULL);
  virtual int flashback_tenant_from_recyclebin(const share::schema::ObTenantSchema &tenant_schema,
                                               common::ObMySQLTransaction &trans,
                                               const common::ObString &new_tenant_name,
                                               share::schema::ObSchemaGetterGuard &schema_guard,
                                               const common::ObString &ddl_stmt_str);
  virtual int purge_tenant_in_recyclebin(const share::schema::ObTenantSchema &tenant_schema,
                                         common::ObMySQLTransaction &trans,
                                         const common::ObString *ddl_stmt_str);
  virtual int replace_sys_variable(share::schema::ObSysVariableSchema &sys_variable_schema,
                                   const int64_t schema_version,
                                   common::ObMySQLTransaction &trans,
                                   const share::schema::ObSchemaOperationType &operation_type,
                                   const common::ObString *ddl_stmt_str = NULL);
  virtual int create_database(share::schema::ObDatabaseSchema &database_schema,
                              common::ObMySQLTransaction &trans,
                              const common::ObString *ddl_stmt_str = NULL);
  virtual int alter_database(share::schema::ObDatabaseSchema &new_database_schema,
                             common::ObMySQLTransaction &trans,
                             const share::schema::ObSchemaOperationType op_type,
                             const common::ObString *ddl_stmt_str = NULL,
                             const bool need_update_schema_version = true);
  virtual int drop_database(const share::schema::ObDatabaseSchema &db_schema,
                            common::ObMySQLTransaction &trans,
                            const common::ObString *ddl_stmt_str = NULL);
  virtual int create_tablegroup(share::schema::ObTablegroupSchema &tablegroup_schema,
                                common::ObMySQLTransaction &trans,
                                const common::ObString *ddl_stmt_str = NULL);
  virtual int drop_tablegroup(const share::schema::ObTablegroupSchema &tablegroup_schema,
                              common::ObMySQLTransaction &trans,
                              const common::ObString *ddl_stmt_str = NULL);
  virtual int alter_tablegroup(share::schema::ObTablegroupSchema &new_schema,
                               common::ObMySQLTransaction &trans,
                               const common::ObString *ddl_stmt_str = NULL);
  int alter_tablegroup(share::schema::ObSchemaGetterGuard &schema_guard,
                       share::schema::ObTableSchema &new_table_schema,
                       common::ObMySQLTransaction &trans,
                       const common::ObString *ddl_stmt_str = NULL);
  static int check_part_equal(
      const share::schema::ObPartitionFuncType part_type,
      const share::schema::ObPartition *r_part,
      const share::schema::ObPartition *l_part,
      bool &is_equal);
  static bool is_list_values_equal(
       const common::ObRowkey &fir_values,
       const common::ObIArray<common::ObNewRow> &sed_values);

  int update_default_partition_part_idx_for_external_table(const share::schema::ObTableSchema &orig_table_schema,
                                        const share::schema::ObTableSchema &new_table_schema,
                                        ObMySQLTransaction &trans);
  int add_table_partitions(const share::schema::ObTableSchema &orig_table_schema,
                           share::schema::ObTableSchema &inc_table_schema,
                           const int64_t schema_version,
                           common::ObMySQLTransaction &trans);
  int drop_table_partitions(const share::schema::ObTableSchema &orig_table_schema,
                            share::schema::ObTableSchema &inc_table_schema,
                            const int64_t schema_version,
                            common::ObMySQLTransaction &trans);
  int modify_check_constraints_state(
      const share::schema::ObTableSchema &orig_table_schema,
      const share::schema::ObTableSchema &inc_table_schema,
      share::schema::ObTableSchema &new_table_schema,
      common::ObMySQLTransaction &trans);
  int add_table_constraints(const share::schema::ObTableSchema &inc_table_schema,
                            share::schema::ObTableSchema &new_table_schema,
                            common::ObMySQLTransaction &trans,
                            common::ObSArray<uint64_t> *cst_ids = NULL);
  int add_table_foreign_keys(const share::schema::ObTableSchema &orig_table_schema,
                             share::schema::ObTableSchema &inc_table_schema,
                             common::ObMySQLTransaction &trans);
  int update_table_foreign_keys(share::schema::ObTableSchema &new_table_schema,
                                common::ObMySQLTransaction &trans,
                                bool in_offline_ddl_white_list);
  int drop_table_constraints(const share::schema::ObTableSchema &orig_table_schema,
                            const share::schema::ObTableSchema &inc_table_schema,
                            share::schema::ObTableSchema &new_table_schema,
                            common::ObMySQLTransaction &trans);
  virtual int create_user(share::schema::ObUserInfo &user,
                          const common::ObString *ddl_stmt_str,
                          common::ObMySQLTransaction &trans);
  int alter_user_default_role(const common::ObString &ddl_str,
                              const share::schema::ObUserInfo &schema,
                              common::ObIArray<uint64_t> &role_id_array,
                              common::ObIArray<uint64_t> &disable_flag_array,
                              common::ObMySQLTransaction &trans);
  int alter_user_profile(const common::ObString &ddl_str,
                         share::schema::ObUserInfo &schema,
                         common::ObMySQLTransaction &trans);
  virtual int create_table(share::schema::ObTableSchema &table_schema,
                           common::ObMySQLTransaction &trans,
                           const common::ObString *ddl_stmt_str = NULL,
                           const bool need_sync_schema_version = true,
                           const bool is_truncate_table = false);
  int truncate_table(const ObString *ddl_stmt_str,
                     const share::schema::ObTableSchema &orig_table_schema,
                     const share::schema::ObTableSchema &new_table_schema,
                     common::ObMySQLTransaction &trans);
  int update_boundary_schema_version(const uint64_t &tenant_id,
                                     const uint64_t &boundary_schema_version,
                                     common::ObMySQLTransaction &trans);
  int inc_table_schema_version(ObMySQLTransaction &trans,
                               const uint64_t tenant_id,
                               const uint64_t table_id);
  int truncate_table_partitions(const share::schema::ObTableSchema &orig_table_schema,
                                share::schema::ObTableSchema &inc_table_schema,
                                share::schema::ObTableSchema &del_table_schema,
                                common::ObMySQLTransaction &trans);
  int truncate_table_subpartitions(const share::schema::ObTableSchema &orig_table_schema,
                                share::schema::ObTableSchema &inc_table_schema,
                                share::schema::ObTableSchema &del_table_schema,
                                common::ObMySQLTransaction &trans);
  int add_table_partitions(const share::schema::ObTableSchema &orig_table_schema,
                           share::schema::ObTableSchema &inc_table_schema,
                           share::schema::ObTableSchema &new_table_schema,
                           common::ObMySQLTransaction &trans);
  int add_table_subpartitions(const share::schema::ObTableSchema &orig_table_schema,
                              share::schema::ObTableSchema &inc_table_schema,
                              share::schema::ObTableSchema &new_table_schema,
                              common::ObMySQLTransaction &trans);
  int split_table_partitions(const share::schema::ObTableSchema &orig_table_schema,
                             share::schema::ObTableSchema &inc_table_schema,
                             share::schema::ObTableSchema &new_table_schema,
                             share::schema::ObTableSchema &upd_table_schema,
                             common::ObMySQLTransaction &trans);
  int drop_table_splitted_partitions(const share::schema::ObTableSchema &orig_table_schema,
                                     share::schema::ObTableSchema &inc_table_schema,
                                     common::ObMySQLTransaction &trans);
  int drop_table_partitions(const share::schema::ObTableSchema &orig_table_schema,
                            share::schema::ObTableSchema &inc_table_schema,
                            share::schema::ObTableSchema &new_table_schema,
                            common::ObMySQLTransaction &trans);
  int drop_table_subpartitions(const share::schema::ObTableSchema &orig_table_schema,
                            share::schema::ObTableSchema &inc_table_schema,
                            share::schema::ObTableSchema &new_table_schema,
                            common::ObMySQLTransaction &trans);
  int rename_table_partitions(const share::schema::ObTableSchema &orig_table_schema,
                            share::schema::ObTableSchema &inc_table_schema,
                            share::schema::ObTableSchema &new_table_schema,
                            common::ObMySQLTransaction &trans);
  int rename_table_subpartitions(const share::schema::ObTableSchema &orig_table_schema,
                              share::schema::ObTableSchema &inc_table_schema,
                              share::schema::ObTableSchema &new_table_schema,
                              common::ObMySQLTransaction &trans);
  int get_part_array_from_table(const share::schema::ObTableSchema &orig_table_schema,
                                const share::schema::ObTableSchema &inc_table_schema,
                                common::ObIArray<share::schema::ObPartition*> &part_array);
  int insert_column_groups(ObMySQLTransaction &trans, const share::schema::ObTableSchema &new_table_schema);
  int insert_column_ids_into_column_group(ObMySQLTransaction &trans,
                                          const share::schema::ObTableSchema &new_table_schema,
                                          const ObIArray<uint64_t> &column_ids,
                                          const share::schema::ObColumnGroupSchema &column_group);
  int update_origin_column_group_with_new_schema(ObMySQLTransaction &trans,
                                                const ObTableSchema &origin_table_schema,
                                                const ObTableSchema &new_table_schema);
  int insert_single_column(common::ObMySQLTransaction &trans,
                           const share::schema::ObTableSchema &new_table_schema,
                           share::schema::ObColumnSchemaV2 &new_column);
  int delete_single_column(common::ObMySQLTransaction &trans,
                           const int64_t new_schema_version,
                           share::schema::ObTableSchema &new_table_schema,
                           const common::ObString &column_name);
  int batch_update_system_table_columns(
      common::ObMySQLTransaction &trans,
      const share::schema::ObTableSchema &orig_table_schema,
      share::schema::ObTableSchema &new_table_schema,
      const common::ObIArray<uint64_t> &add_column_ids,
      const common::ObIArray<uint64_t> &alter_column_ids,
      const common::ObString *ddl_stmt_str = NULL);
  int reinit_autoinc_row(const share::schema::ObTableSchema &table_schema,
                         common::ObMySQLTransaction &trans);
  // for alter table autoinc to check __all_auto_increment
  int try_reinit_autoinc_row(const share::schema::ObTableSchema &table_schema,
                             common::ObMySQLTransaction &trans);
  int create_sequence_in_create_table(share::schema::ObTableSchema &table_schema,
                                      common::ObMySQLTransaction &trans,
                                      share::schema::ObSchemaGetterGuard &schema_guard,
                                      const obrpc::ObSequenceDDLArg *sequence_ddl_arg);
  int drop_sequence_in_drop_table(const share::schema::ObTableSchema &table_schema,
                                  common::ObMySQLTransaction &trans,
                                  share::schema::ObSchemaGetterGuard &schema_guard);
  int create_sequence_in_add_column(const share::schema::ObTableSchema &table_schema,
                                    share::schema::ObColumnSchemaV2 &column_schema,
                                    common::ObMySQLTransaction &trans,
                                    share::schema::ObSchemaGetterGuard &schema_guard,
                                    obrpc::ObSequenceDDLArg &sequence_ddl_arg);

  int drop_sequence_in_drop_column(const share::schema::ObColumnSchemaV2 &column_schema,
                                   common::ObMySQLTransaction &trans,
                                   share::schema::ObSchemaGetterGuard &schema_guard);
  virtual int alter_table_create_index(const share::schema::ObTableSchema &new_table_schema,
                                       common::ObIArray<share::schema::ObColumnSchemaV2*> &gen_columns,
                                       share::schema::ObTableSchema &index_schema,
                                       common::ObMySQLTransaction &trans);

  virtual int alter_table_drop_index(const share::schema::ObTableSchema *index_table_schema,
                                     share::schema::ObTableSchema &new_data_table_schema,
                                     common::ObMySQLTransaction &trans);
  virtual int alter_table_alter_index(const uint64_t tenant_id,
                                      const uint64_t data_table_id,
                                      const uint64_t database_id,
                                      const obrpc::ObAlterIndexArg &alter_index_arg,
                                      common::ObMySQLTransaction &trans);
  virtual int alter_table_drop_foreign_key(const share::schema::ObTableSchema &table_schema,
                                           const obrpc::ObDropForeignKeyArg &drop_foreign_key_arg,
                                           common::ObMySQLTransaction &trans,
                                           const share::schema::ObForeignKeyInfo *&parent_table_mock_foreign_key_info,
                                           const bool parent_table_in_offline_ddl_white_list);
  virtual int alter_index_drop_options(const share::schema::ObTableSchema &index_table_schema,
                                       const common::ObString &table_name,
                                       share::schema::ObTableSchema &new_index_table_schema,
                                       common::ObMySQLTransaction &trans);
  virtual int alter_table_rename_index(const uint64_t tenant_id,
                                       const uint64_t data_table_id,
                                       const uint64_t database_id,
                                       const obrpc::ObRenameIndexArg &rename_index_arg,
                                       const share::schema::ObIndexStatus *new_index_status,
                                       const bool is_in_deleting,
                                       common::ObMySQLTransaction &trans,
                                       share::schema::ObTableSchema &new_index_table_schema);
  int alter_table_rename_index_with_origin_index_name(
      const uint64_t tenant_id,
      const uint64_t index_table_id,
      const ObString &new_index_name, // Attention!!! origin index name, don't use table name. For example, __idx_500005_{index_name}, please using index_name!!!
      const share::schema::ObIndexStatus &new_index_status,
      const bool is_in_deleting,
      common::ObMySQLTransaction &trans,
      share::schema::ObTableSchema &new_index_table_schema);

  virtual int alter_index_table_parallel(const uint64_t tenant_id,
                                         const uint64_t data_table_id,
                                         const uint64_t database_id,
                                         const obrpc::ObAlterIndexParallelArg &alter_parallel_arg,
                                         common::ObMySQLTransaction &trans);
  virtual int alter_index_table_tablespace(const uint64_t data_table_id,
                                           const uint64_t database_id,
                                           const obrpc::ObAlterIndexTablespaceArg &alter_tablespace_arg,
                                           share::schema::ObSchemaGetterGuard &schema_guard,
                                           common::ObMySQLTransaction &trans);

  virtual int alter_table_options(
      share::schema::ObSchemaGetterGuard &schema_guard,
      share::schema::ObTableSchema &new_table_schema,
      const share::schema::ObTableSchema &table_schema,
      const bool need_update_aux_table,
      common::ObMySQLTransaction &trans,
      const common::ObIArray<share::schema::ObTableSchema> *global_idx_schema_array = NULL,
      common::ObIArray<std::pair<uint64_t, int64_t>> *idx_schema_versions = NULL);

  virtual int update_aux_table(
      const share::schema::ObTableSchema &table_schema,
      const share::schema::ObTableSchema &new_table_schema,
      share::schema::ObSchemaGetterGuard &schema_guard,
      common::ObMySQLTransaction &trans,
      const share::schema::ObTableType table_type,
      bool &has_aux_table_updated,
      const common::ObIArray<share::schema::ObTableSchema> *global_idx_schema_array = NULL,
      common::ObIArray<std::pair<uint64_t, int64_t>> *idx_schema_versions = NULL);

  virtual int update_table_attribute(share::schema::ObTableSchema &new_table_schema,
                                     common::ObMySQLTransaction &trans,
                                     const share::schema::ObSchemaOperationType operation_type,
                                     const common::ObString *ddl_stmt_str = NULL);
  virtual int insert_ori_schema_version(
      common::ObMySQLTransaction &trans,
      const uint64_t tenant_id,
      const uint64_t table_id,
      const int64_t &ori_schema_version);
  virtual int update_prev_id_for_delete_column(const share::schema::ObTableSchema &origin_table_schema,
      share::schema::ObTableSchema &new_table_schema,
      const share::schema::ObColumnSchemaV2 &ori_column_schema,
      common::ObMySQLTransaction &trans);
  virtual int drop_inner_generated_index_column(common::ObMySQLTransaction &trans,
      share::schema::ObSchemaGetterGuard &schema_guard,
      const share::schema::ObTableSchema &index_schema,
      share::schema::ObTableSchema &new_data_table_schema);
  virtual int drop_inner_generated_domain_extra_column(common::ObMySQLTransaction &trans,
      const share::schema::ObTableSchema *ori_data_schema,
      const share::schema::ObColumnSchemaV2 &ori_column_schema,
      share::schema::ObTableSchema &new_data_table_schema);

//  virtual int log_ddl_operation(share::schema::ObSchemaOperation &ddl_operation,
//                                       common::ObMySQLTransaction &trans);

  int drop_tablet_of_table(
      const share::schema::ObTableSchema &table_schema,
      ObMySQLTransaction &trans);

  // mock_fk_parent_table begin
  virtual int create_mock_fk_parent_table(
        ObMySQLTransaction &trans,
        const share::schema::ObMockFKParentTableSchema &mock_fk_parent_table_schema,
        const bool need_update_foreign_key);
  virtual int alter_mock_fk_parent_table(
        ObMySQLTransaction &trans,
        share::schema::ObMockFKParentTableSchema &mock_fk_parent_table_schema);
  virtual int drop_mock_fk_parent_table(
        ObMySQLTransaction &trans,
        const share::schema::ObMockFKParentTableSchema &mock_fk_parent_table_schema);
  virtual int replace_mock_fk_parent_table(
        ObMySQLTransaction &trans,
        share::schema::ObSchemaGetterGuard &schema_guard,
        const share::schema::ObMockFKParentTableSchema &mock_fk_parent_table_schema);
  virtual int sync_version_for_cascade_mock_fk_parent_table(
      const uint64_t tenant_id,
      const common::ObIArray<uint64_t> &mock_fk_parent_table_ids,
      common::ObMySQLTransaction &trans);
  virtual int deal_with_mock_fk_parent_table(
        ObMySQLTransaction &trans,
        share::schema::ObSchemaGetterGuard &schema_guard,
        share::schema::ObMockFKParentTableSchema &mock_fk_parent_table_schema);
  virtual int deal_with_mock_fk_parent_tables(
        ObMySQLTransaction &trans,
        share::schema::ObSchemaGetterGuard &schema_guard,
        ObIArray<share::schema::ObMockFKParentTableSchema> &mock_fk_parent_table_schema_array);
  // mock_fk_parent_table end

  virtual int drop_table(const share::schema::ObTableSchema &table_schema,
                         common::ObMySQLTransaction &trans,
                         const common::ObString *ddl_stmt_str = NULL,
                         const bool is_truncate_table = false,
                         share::schema::DropTableIdHashSet *drop_table_set = NULL,
                         const bool is_drop_db = false,
                         const bool delete_priv = true);
  virtual int drop_table_for_not_dropped_schema(
      const share::schema::ObTableSchema &table_schema,
      common::ObMySQLTransaction &trans,
      const common::ObString *ddl_stmt_str = NULL,
      const bool is_truncate_table = false,
      share::schema::DropTableIdHashSet *drop_table_set = NULL,
      const bool is_drop_db = false,
      const bool delete_priv = true);
  virtual int drop_table_to_recyclebin(const share::schema::ObTableSchema &table_schema,
                                       share::schema::ObSchemaGetterGuard &schema_guard,
                                       common::ObMySQLTransaction &trans,
                                       const common::ObString *ddl_stmt_str,/*= NULL*/
                                       const bool is_truncate_table = false);
  virtual int flashback_table_from_recyclebin(const share::schema::ObTableSchema &table_schema,
                                              share::schema::ObTableSchema &new_table_schema,
                                              common::ObMySQLTransaction &trans,
                                              const uint64_t new_db_id,
                                              const common::ObString &new_table_name,
                                              const common::ObString *ddl_stmt_str,
                                              share::schema::ObSchemaGetterGuard &guard);
  virtual int purge_table_in_recyclebin(
      const share::schema::ObTableSchema &table_schema,
      common::ObMySQLTransaction &trans,
      const common::ObString *ddl_stmt_str/*=NULL*/);
  // create_table_in_recylebin only applies to truncate table.
  virtual int create_index_in_recyclebin(share::schema::ObTableSchema &table_schema,
                                         share::schema::ObSchemaGetterGuard &schema_guard,
                                         common::ObMySQLTransaction &trans,
                                         const common::ObString *ddl_stmt_str);

  virtual int drop_database_to_recyclebin(const share::schema::ObDatabaseSchema &database_schema,
                                          common::ObMySQLTransaction &trans,
                                          const common::ObString *ddl_stmt_str);
  virtual int flashback_database_from_recyclebin(const share::schema::ObDatabaseSchema &database_schema,
                                                 common::ObMySQLTransaction &trans,
                                                 const common::ObString &new_db_name,
                                                 share::schema::ObSchemaGetterGuard &schema_guard,
                                                 const common::ObString &ddl_stmt_str);
  virtual int purge_table_of_database(const share::schema::ObDatabaseSchema &db_schema,
                                      common::ObMySQLTransaction &trans);
  virtual int purge_database_in_recyclebin(const share::schema::ObDatabaseSchema &database_schema,
                                           common::ObMySQLTransaction &trans,
                                           const common::ObString *ddl_stmt_str);
  virtual int purge_table_with_aux_table(
      const share::schema::ObTableSchema &table_schema,
      share::schema::ObSchemaGetterGuard &schema_guard,
      common::ObMySQLTransaction &trans,
      const common::ObString *ddl_stmt_str);
  virtual int purge_aux_table(
      const share::schema::ObTableSchema &table_schema,
      share::schema::ObSchemaGetterGuard &schema_guard,
      common::ObMySQLTransaction &trans,
      const share::schema::ObTableType table_type);

  virtual int fetch_expire_recycle_objects(
      const uint64_t tenant_id,
      const int64_t expire_time,
      common::ObIArray<share::schema::ObRecycleObject> &recycle_objs);

  virtual int init_tenant_schemas(
      const share::schema::ObTenantSchema &tenant_schema,
      const share::schema::ObSysVariableSchema &sys_variable,
      common::ObMySQLTransaction &trans);
  virtual int rename_table(const share::schema::ObTableSchema &table_schema,
                           const common::ObString &new_table_name,
                           const uint64_t new_db_id,
                           const bool need_reset_object_status,
                           common::ObMySQLTransaction &trans,
                           const common::ObString *ddl_stmt_str,
                           int64_t &new_data_table_schema_version /*OUTPUT*/,
                           ObIArray<std::pair<uint64_t, int64_t>> &idx_schema_versions /*OUTPUT*/);
  virtual int rename_aux_table(const share::schema::ObTableSchema &new_table_schema,
                               const uint64_t table_id,
                               share::schema::ObSchemaGetterGuard &schema_guard,
                               ObMySQLTransaction &trans,
                               share::schema::ObTableSchema &new_aux_table_schema,
                               bool &has_aux_table_updated);
  virtual int update_index_status(
              const uint64_t tenant_id,
              const uint64_t data_table_id,
              const uint64_t index_table_id,
              const share::schema::ObIndexStatus status,
              const bool in_offline_ddl_white_list,
              common::ObMySQLTransaction &trans,
              const common::ObString *ddl_stmt_str);
  virtual int update_index_type(const share::schema::ObTableSchema &data_table_schema,
                                const uint64_t index_table_id,
                                const share::schema::ObIndexType index_type,
                                const common::ObString *ddl_stmt_str,
                                common::ObMySQLTransaction &trans);
  virtual int update_indexes_type(const share::schema::ObTableSchema &data_table_schema,
                                 const ObIArray<uint64_t> &index_table_ids,
                                 const ObIArray<share::schema::ObIndexType> &index_types,
                                 const common::ObString *ddl_stmt_str,
                                 common::ObMySQLTransaction &trans);

  // tablespace
  virtual int create_tablespace(share::schema::ObTablespaceSchema &tablespace_schema,
                                common::ObMySQLTransaction &trans,
                                share::schema::ObSchemaGetterGuard &schema_guard,
                                const common::ObString *ddl_stmt_str);
  virtual int alter_tablespace(share::schema::ObTablespaceSchema &tablespace_schema,
                                common::ObMySQLTransaction &trans,
                                share::schema::ObSchemaGetterGuard &schema_guard,
                                const common::ObString *ddl_stmt_str);
  virtual int drop_tablespace(share::schema::ObTablespaceSchema &tablespace_schema,
                                common::ObMySQLTransaction &trans,
                                share::schema::ObSchemaGetterGuard &schema_guard,
                                const common::ObString *ddl_stmt_str);
  int update_tablespace_table(const share::schema::ObTableSchema *table_schema,
                              common::ObMySQLTransaction &trans,
                              share::schema::ObTablespaceSchema &tablespace_schema);

  //----Functions for managing privileges----
  virtual int create_user(const share::schema::ObUserInfo &user_info,
                          const common::ObString *ddl_stmt_str,
                          common::ObMySQLTransaction &trans);

  virtual int drop_user(const uint64_t tenant_id,
                        const uint64_t user_id,
                        const common::ObString *ddl_stmt_str,
                        common::ObMySQLTransaction &trans);
  virtual int drop_db_table_privs(const uint64_t tenant_id,
                                  const uint64_t user_id,
                                  common::ObMySQLTransaction &trans);
  int get_drop_db_table_privs_count(const int64_t tenant_id, const int64_t user_id, int64_t &ddl_count);
  virtual int rename_user(const uint64_t tenant_id,
                          const uint64_t user_id,
                          const obrpc::ObAccountArg &new_account,
                          const common::ObString *ddl_stmt_str,
                          common::ObMySQLTransaction &trans);
  virtual int set_passwd(const uint64_t tenant_id,
                         const uint64_t user_id,
                         const common::ObString &passwd,
                         const common::ObString *ddl_stmt_str,
                         common::ObMySQLTransaction &trans);
  virtual int set_max_connections(const uint64_t tenant_id,
                                  const uint64_t user_id,
                                  const uint64_t max_connections_per_hour,
                                  const uint64_t max_user_connections,
                                  const common::ObString *ddl_stmt_str,
                                  common::ObMySQLTransaction &trans);
  virtual int alter_role(const uint64_t tenant_id,
                         const uint64_t user_id,
                         const common::ObString &passwd,
                         const common::ObString *ddl_stmt_str,
                         common::ObMySQLTransaction &trans);
  virtual int alter_user_require(const uint64_t tenant_id,
                                 const uint64_t user_id,
                                 const obrpc::ObSetPasswdArg &arg,
                                 const common::ObString *ddl_stmt_str,
                                 common::ObMySQLTransaction &trans);
  virtual int grant_revoke_user(const uint64_t tenant_id,
                                const uint64_t user_id,
                                const ObPrivSet priv_set,
                                const bool grant,
                                const bool is_from_inner_sql,
                                const common::ObString *ddl_stmt_str,
                                common::ObMySQLTransaction &trans);
  virtual int lock_user(const uint64_t tenant_id,
                        const uint64_t user_id,
                        const bool locked,
                        const common::ObString *ddl_stmt_str,
                        common::ObMySQLTransaction &trans);
  virtual int grant_database(const share::schema::ObOriginalDBKey &db_priv_key,
                             const ObPrivSet priv_set,
                             const common::ObString *ddl_stmt_str,
                             common::ObMySQLTransaction &trans);
  virtual int revoke_database(const share::schema::ObOriginalDBKey &db_priv_key,
                              const ObPrivSet priv_set,
                              common::ObMySQLTransaction &trans);
  virtual int grant_table(const share::schema::ObTablePrivSortKey &table_priv_key,
                          const ObPrivSet priv_set,
                          const common::ObString *ddl_stmt_str,
                          common::ObMySQLTransaction &trans,
                          const share::ObRawObjPrivArray &obj_priv_array,
                          const uint64_t option,
                          const share::schema::ObObjPrivSortKey &obj_priv_key,
                          const common::ObString &grantor = "",
                          const common::ObString &grantor_host = "");
  virtual int revoke_table(const share::schema::ObTablePrivSortKey &table_priv_key,
                           const ObPrivSet priv_set,
                           common::ObMySQLTransaction &trans,
                           const share::schema::ObObjPrivSortKey &obj_priv_key,
                           const share::ObRawObjPrivArray &obj_priv_array,
                           const bool revoke_all_ora,
                           const common::ObString &grantor = "",
                           const common::ObString &grantor_host = "");

  virtual int grant_routine(const share::schema::ObRoutinePrivSortKey &routine_priv_key,
                            const ObPrivSet priv_set,
                            common::ObMySQLTransaction &trans,
                            const uint64_t option,
                            const bool gen_ddl_stmt = true,
                            const common::ObString &grantor = "",
                            const common::ObString &grantor_host = "");
  virtual int revoke_routine(const share::schema::ObRoutinePrivSortKey &routine_priv_key,
                            const ObPrivSet priv_set,
                            common::ObMySQLTransaction &trans,
                            const bool report_error = true,
                            const bool gen_ddl_stmt = true,
                            const common::ObString &grantor = "",
                            const common::ObString &grantor_host = "");
  virtual int grant_column(share::schema::ObSchemaGetterGuard &schema_guard,
                           const share::schema::ObColumnPrivSortKey &column_priv_key,
                           const ObPrivSet priv_set,
                           const ObString *ddl_stmt_str,
                           common::ObMySQLTransaction &trans,
                           const bool is_grant);
  virtual int grant_revoke_role(const uint64_t tenant_id,
                                const share::schema::ObUserInfo &user_info,
                                const common::ObIArray<uint64_t> &role_ids,
                                const share::schema::ObUserInfo *specified_role_info,
                                common::ObMySQLTransaction &trans,
                                const bool log_operation,
                                const bool is_grant,
                                const uint64_t option);
  virtual int get_flush_role_array(const uint64_t option,
                                   const common::ObIArray<uint64_t> &org_role_ids,
                                   bool &need_flush,
                                   bool is_grant,
                                   const share::schema::ObUserInfo &user_info,
                                   common::ObIArray<uint64_t> &role_ids);
  virtual int get_flush_priv_array(const uint64_t option,
                                   const share::ObRawPrivArray &priv_array,
                                   const share::schema::ObSysPriv *sys_priv,
                                   share::ObRawPrivArray &new_priv_array,
                                   bool &need_flush,
                                   const bool is_grant,
                                   const share::schema::ObUserInfo &user_info);
  virtual int grant_sys_priv_to_ur(const uint64_t tenant_id,
                                   const uint64_t grantee_id,
                                   const share::schema::ObSysPriv* sys_priv,
                                   const uint64_t option,
                                   const share::ObRawPrivArray priv_array,
                                   common::ObMySQLTransaction &trans,
                                   const bool is_grant,
                                   const common::ObString *ddl_stmt_str,
                                   share::schema::ObSchemaGetterGuard &schema_guard);
  static int drop_obj_privs(const uint64_t tenant_id,
                            const uint64_t obj_id,
                            const uint64_t obj_ypte,
                            common::ObMySQLTransaction &trans,
                            share::schema::ObMultiVersionSchemaService &schema_service,
                            share::schema::ObSchemaGetterGuard &schema_guard);
  //----End of functions for managing privileges----
  //----Functions for managing outlines----
  int create_outline(share::schema::ObOutlineInfo &outline_info,
                     common::ObMySQLTransaction &trans,
                     const common::ObString *ddl_stmt_str/*=NULL*/);
  int replace_outline(share::schema::ObOutlineInfo &outline_info,
                      common::ObMySQLTransaction &trans,
                      const common::ObString *ddl_stmt_str/*=NULL*/);
  int alter_outline(share::schema::ObOutlineInfo &outline_info,
                    common::ObMySQLTransaction &trans,
                    const common::ObString *ddl_stmt_str/*=NULL*/);
  int drop_outline(const uint64_t tenant_id,
                   const uint64_t database_id,
                   const uint64_t outline_id,
                   common::ObMySQLTransaction &trans,
                   const common::ObString *ddl_stmt_str/*=NULL*/);
  //----End of functions for managing outlines----
  //

  //----Functions for managing dblinks----
  int create_dblink(share::schema::ObDbLinkBaseInfo &dblink_info,
                    common::ObMySQLTransaction &trans,
                    const common::ObString *ddl_stmt_str/*=NULL*/);
  int drop_dblink(share::schema::ObDbLinkBaseInfo &dblink_info,
                  common::ObMySQLTransaction &trans,
                  const common::ObString *ddl_stmt_str/*=NULL*/);
  //----End of functions for managing dblinks----

  //----Functions for managing schema revise
  int revise_constraint_column_info(
      obrpc::ObSchemaReviseArg arg, common::ObMySQLTransaction &trans);

  int revise_not_null_constraint_info(
      obrpc::ObSchemaReviseArg arg,
      share::schema::ObSchemaGetterGuard &schema_guard,
      common::ObMySQLTransaction &trans);
  //----End of functions for managing schema revise

  int create_synonym(share::schema::ObSynonymInfo &synonym_info,
                     common::ObMySQLTransaction &trans,
                     const common::ObString *ddl_stmt_str/*=NULL*/);
  int replace_synonym(share::schema::ObSynonymInfo &synonym_info,
                      common::ObMySQLTransaction &trans,
                      const common::ObString *ddl_stmt_str/*=NULL*/);
  int drop_synonym(const uint64_t tenant_id,
                   const uint64_t database_id,
                   const uint64_t synonym_info_id,
                   common::ObMySQLTransaction &trans,
                   const common::ObString *ddl_stmt_str/*=NULL*/);

  // -------------manage keystore----------------//
  virtual int create_keystore(share::schema::ObKeystoreSchema &keystore_schema,
                              common::ObMySQLTransaction &trans,
                              share::schema::ObSchemaGetterGuard &schema_guard,
                              const common::ObString *ddl_stmt_str);
  virtual int alter_keystore(share::schema::ObKeystoreSchema &keystore_schema,
                             common::ObMySQLTransaction &trans,
                             share::schema::ObSchemaGetterGuard &schema_guard,
                             const common::ObString *ddl_stmt_str,
                             bool &set_key,
                             bool is_kms);

  // --------------end  keystore ---------------//

  int handle_audit_metainfo(const share::schema::ObSAuditSchema &audit_schema,
                            const share::schema::ObSAuditModifyType modify_type,
                            const bool need_update,
                            const ObString *ddl_stmt_str,
                            common::ObMySQLTransaction &trans,
                            common::ObSqlString &public_sql_string);
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
                    share::schema::ObSchemaGetterGuard &schema_guard,
                    common::ObMySQLTransaction &trans,
                    ObIArray<share::schema::ObRoutineInfo> &public_routine_infos,
                    share::schema::ObErrorInfo &error_info,
                    const common::ObString *ddl_stmt_str);
  int drop_package(const share::schema::ObPackageInfo &package_info,
                   common::ObMySQLTransaction &trans,
                   share::schema::ObSchemaGetterGuard &schema_guard,
                   share::schema::ObErrorInfo &error_info,
                   const common::ObString *ddl_stmt_str/*=NULL*/);
  int del_routines_in_package(const share::schema::ObPackageInfo &package_info,
                              common::ObMySQLTransaction &trans,
                              share::schema::ObSchemaGetterGuard &schema_guard);
  //----End of functions for managing package----

  //----Functions for managing trigger----
  int create_trigger(share::schema::ObTriggerInfo &new_trigger_info,
                     common::ObMySQLTransaction &trans,
                     share::schema::ObErrorInfo &error_info,
                     ObIArray<share::schema::ObDependencyInfo> &dep_infos,
                     int64_t &table_schema_version,
                     const common::ObString *ddl_stmt_str/*=NULL*/,
                     bool is_update_table_schema_version = true,
                     bool is_for_truncate_table = false);
  // set ddl_stmt_str to NULL if the statement is not 'drop trigger xxx'.
  int drop_trigger_to_recyclebin(const share::schema::ObTriggerInfo &trigger_info,
                                 share::schema::ObSchemaGetterGuard &schema_guard,
                                 common::ObMySQLTransaction &trans);
  int alter_trigger(share::schema::ObTriggerInfo &new_trigger_info,
                    common::ObMySQLTransaction &tran,
                    const common::ObString *ddl_stmt_str/*=NULL*/,
                    bool is_update_table_schema_version = true);
  int flashback_trigger(const share::schema::ObTriggerInfo &trigger_info,
                        uint64_t new_database_id,
                        const common::ObString &new_table_name,
                        share::schema::ObSchemaGetterGuard &schema_guard,
                        common::ObMySQLTransaction &trans);
  int purge_table_trigger(const share::schema::ObTableSchema &table_schema,
                          share::schema::ObSchemaGetterGuard &schema_guard,
                          common::ObMySQLTransaction &trans);
  int purge_trigger(const share::schema::ObTriggerInfo &trigger_info,
                    common::ObMySQLTransaction &trans);
  int rebuild_trigger_package(const share::schema::ObTriggerInfo &trigger_info,
                              const common::ObString &database_name,
                              const common::ObString &table_name,
                              common::ObMySQLTransaction &trans);
  //----End of functions for managing trigger----

  //----Functions for managing UDF----
  int create_user_defined_function(share::schema::ObUDF &udf_info,
                                   common::ObMySQLTransaction &trans,
                                   const common::ObString *ddl_stmt_str/*=NULL*/);
  int drop_user_defined_function(const uint64_t tenant_id,
                                 const common::ObString &name,
                                 common::ObMySQLTransaction &trans,
                                 const common::ObString *ddl_stmt_str/*=NULL*/);
  //----End of functions for managing UDF----

  //----Functions for label security----
  int handle_label_se_policy_function(share::schema::ObSchemaOperationType ddl_type,
                                      const common::ObString &ddl_stmt_str,
                                      share::schema::ObSchemaGetterGuard &schema_guard,
                                      share::schema::ObLabelSePolicySchema &schema,
                                      common::ObMySQLTransaction &trans);
  int handle_label_se_component_function(share::schema::ObSchemaOperationType ddl_type,
                                         const common::ObString &ddl_stmt_str,
                                         const common::ObString &policy_name,
                                         share::schema::ObSchemaGetterGuard &schema_guard,
                                         share::schema::ObLabelSeComponentSchema &schema,
                                         common::ObMySQLTransaction &trans);
  int handle_label_se_label_function(share::schema::ObSchemaOperationType ddl_type,
                                     const common::ObString &ddl_stmt_str,
                                     const common::ObString &policy_name,
                                     share::schema::ObSchemaGetterGuard &schema_guard,
                                     share::schema::ObLabelSeLabelSchema &schema,
                                     common::ObMySQLTransaction &trans);
  int handle_label_se_user_level_function(share::schema::ObSchemaOperationType ddl_type,
                                          const common::ObString &ddl_stmt_str,
                                          const common::ObString &policy_name,
                                          share::schema::ObSchemaGetterGuard &schema_guard,
                                          share::schema::ObLabelSeUserLevelSchema &schema,
                                          common::ObMySQLTransaction &trans);
  int drop_all_label_se_user_components(uint64_t tenant_id,
                                        uint64_t user_id,
                                        uint64_t policy_id,
                                        common::ObMySQLTransaction &trans,
                                        const common::ObString &ddl_stmt_str,
                                        share::schema::ObSchemaGetterGuard &schema_guard);
  int drop_all_label_se_components_in_policy(uint64_t tenant_id,
                                             uint64_t policy_id,
                                             common::ObMySQLTransaction &trans,
                                             const common::ObString &ddl_stmt_str,
                                             share::schema::ObSchemaGetterGuard &schema_guard);
  int drop_all_label_se_labels_in_policy(uint64_t tenant_id,
                                         uint64_t policy_id,
                                         common::ObMySQLTransaction &trans,
                                         const common::ObString &ddl_stmt_str,
                                         share::schema::ObSchemaGetterGuard &schema_guard);
  int drop_all_label_se_table_column(uint64_t tenant_id,
                                     uint64_t policy_id,
                                     common::ObMySQLTransaction &trans,
                                     share::schema::ObSchemaGetterGuard &schema_guard);

  //----End of functions for label security----
  //----Functions for managing profile----
  int handle_profile_function(share::schema::ObProfileSchema &schema,
                              common::ObMySQLTransaction &trans,
                              share::schema::ObSchemaOperationType ddl_type,
                              const common::ObString &ddl_stmt_str,
                              share::schema::ObSchemaGetterGuard &schema_guard);
  //----End of functions for managing profile----

  //----Functions for directory object----
  int create_directory(const ObString &ddl_str,
                       const uint64_t user_id,
                       share::schema::ObDirectorySchema &schema,
                       common::ObMySQLTransaction &trans);
  int alter_directory(const ObString &ddl_str,
                      share::schema::ObDirectorySchema &schema,
                      common::ObMySQLTransaction &trans);
  int drop_directory(const ObString &ddl_str,
                     share::schema::ObDirectorySchema &schema,
                     common::ObMySQLTransaction &trans);
  //----End of functions for directory object----

  //----Functions for row level security----
  int create_rls_policy(share::schema::ObRlsPolicySchema &schema,
                        common::ObMySQLTransaction &trans,
                        const common::ObString &ddl_stmt_str,
                        bool is_update_table_schema,
                        const share::schema::ObTableSchema *table_schema);
  int drop_rls_policy(const share::schema::ObRlsPolicySchema &schema,
                      common::ObMySQLTransaction &trans,
                      const common::ObString &ddl_stmt_str,
                      bool is_update_table_schem,
                      const share::schema::ObTableSchema *table_schemaa);
  int alter_rls_policy(const share::schema::ObRlsPolicySchema &schema,
                       common::ObMySQLTransaction &trans,
                       const common::ObString &ddl_stmt_str);
  int create_rls_group(share::schema::ObRlsGroupSchema &schema,
                       common::ObMySQLTransaction &trans,
                       const common::ObString &ddl_stmt_str,
                       bool is_update_table_schema,
                       const share::schema::ObTableSchema *table_schema);
  int drop_rls_group(const share::schema::ObRlsGroupSchema &schema,
                     common::ObMySQLTransaction &trans,
                     const common::ObString &ddl_stmt_str,
                     bool is_update_table_schema,
                     const share::schema::ObTableSchema *table_schema);
  int create_rls_context(share::schema::ObRlsContextSchema &schema,
                         common::ObMySQLTransaction &trans,
                         const common::ObString &ddl_stmt_str,
                         bool is_update_table_schema,
                         const share::schema::ObTableSchema *table_schema);
  int drop_rls_context(const share::schema::ObRlsContextSchema &schema,
                       common::ObMySQLTransaction &trans,
                       const common::ObString &ddl_stmt_str,
                       bool is_update_table_schema,
                       const share::schema::ObTableSchema *table_schema);
  int drop_rls_sec_column(const share::schema::ObRlsPolicySchema &schema,
                          const share::schema::ObRlsSecColumnSchema &column_schema,
                          common::ObMySQLTransaction &trans,
                          const common::ObString &ddl_stmt_str);
  int update_rls_table_schema(const share::schema::ObTableSchema &table_schema,
                              const share::schema::ObSchemaOperationType ddl_type,
                              common::ObMySQLTransaction &trans);
  int drop_rls_object_in_drop_table(const share::schema::ObTableSchema &table_schema,
                                    common::ObMySQLTransaction &trans,
                                    share::schema::ObSchemaGetterGuard &schema_guard);
  //----End of functions for row level security----


  virtual int insert_temp_table_info(common::ObMySQLTransaction &trans,
                                     const share::schema::ObTableSchema &table_schema);
  virtual int delete_temp_table_info(common::ObMySQLTransaction &trans,
                                     const share::schema::ObTableSchema &table_schema);
  virtual int alter_table_drop_aux_column(
      share::schema::ObTableSchema &new_table_schema,
      const share::schema::ObColumnSchemaV2 &new_column_schema,
      common::ObMySQLTransaction &trans,
      const share::schema::ObTableType table_type);
  int update_single_column(common::ObMySQLTransaction &trans,
                           const share::schema::ObTableSchema &origin_table_schema,
                           const share::schema::ObTableSchema &new_table_schema,
                           share::schema::ObColumnSchemaV2 &column_schema,
                           const bool need_del_stats /*for online drop column, need delete column stat*/);
  int update_single_column_group(common::ObMySQLTransaction &trans,
                                 const share::schema::ObTableSchema &origin_table_schema,
                                 const share::schema::ObColumnSchemaV2 &new_column_schema);
  int update_column_and_column_group(common::ObMySQLTransaction &trans,
                                     const share::schema::ObTableSchema &origin_table_schema,
                                     const share::schema::ObTableSchema &new_table_schema,
                                     share::schema::ObColumnSchemaV2 &column_schema,
                                    const bool need_del_stats /*for online drop column, need delete column stat*/);
  int update_partition_option(common::ObMySQLTransaction &trans,
                              share::schema::ObTableSchema &table_schema);
  int update_partition_option(common::ObMySQLTransaction &trans,
                              share::schema::ObTableSchema &table_schema,
                              const common::ObString &ddl_stmt_str);
  int update_check_constraint_state(common::ObMySQLTransaction &trans,
                                    const share::schema::ObTableSchema &table,
                                    share::schema::ObConstraint &cst);
  int sync_aux_schema_version_for_history(common::ObMySQLTransaction &trans,
                                          const share::schema::ObTableSchema &index_schema);
  template <typename T>
  int construct_new_name_for_recyclebin(const T &schema,
                                        common::ObSqlString &new_table_name);

  int update_table_status(const share::schema::ObTableSchema &orig_table_schema,
                          const int64_t schema_version,
                          const share::schema::ObObjectStatus new_status,
                          const bool update_object_status_ignore_version,
                          common::ObMySQLTransaction &trans);
  int update_view_columns(const share::schema::ObTableSchema &view_schema,
                          common::ObMySQLTransaction &trans);
  int reset_view_status(common::ObMySQLTransaction &trans,
                        const uint64_t tenant_id,
                        const share::schema::ObTableSchema *table);
  int try_add_dep_info_for_synonym(const share::schema::ObSimpleSynonymSchema *synonym_info,
                                   common::ObMySQLTransaction &trans);
  int exchange_table_partitions(const share::schema::ObTableSchema &orig_table_schema,
                                share::schema::ObTableSchema &inc_table_schema,
                                share::schema::ObTableSchema &del_table_schema,
                                common::ObMySQLTransaction &trans);
  int exchange_table_subpartitions(const share::schema::ObTableSchema &orig_table_schema,
                                   share::schema::ObTableSchema &inc_table_schema,
                                   share::schema::ObTableSchema &del_table_schema,
                                   common::ObMySQLTransaction &trans,
                                   const bool is_subpart_idx_specified);
  int get_target_auto_inc_sequence_value(const uint64_t tenant_id,
                                         const uint64_t table_id,
                                         const uint64_t column_id,
                                         uint64_t &sequence_value,
                                         common::ObMySQLTransaction &trans);
  int set_target_auto_inc_sync_value(const uint64_t tenant_id,
                                     const uint64_t table_id,
                                     const uint64_t column_id,
                                     const uint64_t new_sequence_value,
                                     const uint64_t new_sync_value,
                                     common::ObMySQLTransaction &trans);
  int get_target_sequence_sync_value(const uint64_t tenant_id,
                                     const uint64_t sequence_id,
                                     common::ObMySQLTransaction &trans,
                                     ObIAllocator &allocator,
                                     common::number::ObNumber &next_value);
  int alter_target_sequence_start_with(const share::schema::ObSequenceSchema &sequence_schema,
                                       common::ObMySQLTransaction &trans);
  int alter_user_proxy(const share::schema::ObUserInfo* client_user_info,
                        const share::schema::ObUserInfo* proxy_user_info,
                        const uint64_t flags,
                        const bool is_grant,
                        const ObIArray<uint64_t> &role_ids,
                        ObIArray<share::schema::ObUserInfo> &users_to_update,
                        ObMySQLTransaction &trans);
  inline share::schema::ObMultiVersionSchemaService &get_multi_schema_service() { return schema_service_; }
  inline common::ObMySQLProxy &get_sql_proxy() { return sql_proxy_; }
private:
  virtual int set_need_flush_ora(
      share::schema::ObSchemaGetterGuard &schema_guard,
      const share::schema::ObObjPrivSortKey &obj_priv_key,          /* in: obj priv key*/
      const uint64_t option,                                        /* in: new option */
      const share::ObRawObjPrivArray &obj_priv_array,               /* in: new privs used want to add */
      share::ObRawObjPrivArray &new_obj_priv_array);
  virtual int init_tenant_tablegroup(const uint64_t tenant_id,
                                     common::ObMySQLTransaction &trans);
  virtual int init_tenant_database(const share::schema::ObTenantSchema &tenant_schema,
                                   const common::ObString &db_name,
                                   const uint64_t pure_db_id,
                                   const common::ObString &db_comment,
                                   common::ObMySQLTransaction &trans,
                                   const bool is_oracle_mode = false);
  virtual int init_tenant_databases(const share::schema::ObTenantSchema &tenant_schema,
                                    const share::schema::ObSysVariableSchema &sys_variable,
                                    common::ObMySQLTransaction &trans);
  virtual int init_tenant_user(const uint64_t tenant_id,
                               const common::ObString &user_name,
                               const common::ObString &pwd_raw,
                               const uint64_t pure_user_id,
                               const common::ObString &user_comment,
                               common::ObMySQLTransaction &trans,
                               const bool set_locked = false,
                               const bool is_user = true,
                               const bool is_oracle_mode = false);
  virtual int init_tenant_users(const share::schema::ObTenantSchema &tenant_schema,
                                const share::schema::ObSysVariableSchema &sys_variable,
                                common::ObMySQLTransaction &trans);
  virtual int init_freeze_info(const uint64_t tenant_id,
                               common::ObMySQLTransaction &trans);
  virtual int init_tenant_srs(const uint64_t tenant_id,
                              common::ObMySQLTransaction &trans);
  virtual int init_sys_tenant_charset(common::ObMySQLTransaction &trans);

  virtual int init_sys_tenant_collation(common::ObMySQLTransaction &trans);
  virtual int init_sys_tenant_privilege(common::ObMySQLTransaction &trans);
  int check_tenant_exist(share::schema::ObSchemaGetterGuard &schema_guard,
                         const common::ObString &tenant_name,
                         bool &is_exist);

  int sync_version_for_cascade_table(
      const uint64_t tenant_id,
      const common::ObIArray<uint64_t> &table_ids,
      common::ObMySQLTransaction &trans);

  int cleanup_autoinc_cache(const share::schema::ObTableSchema &table_schema);

  bool is_aux_object(const share::schema::ObDatabaseSchema &schema);
  bool is_aux_object(const share::schema::ObTableSchema &schema);
  bool is_aux_object(const share::schema::ObTriggerInfo &schema);
  bool is_aux_object(const share::schema::ObTenantSchema &schema);
  bool is_global_index_object(const share::schema::ObDatabaseSchema &schema);
  bool is_global_index_object(const share::schema::ObTableSchema &schema);
  bool is_global_index_object(const share::schema::ObTriggerInfo &schema);
  bool is_global_index_object(const share::schema::ObTenantSchema &schema);
  int update_tablegroup_id_of_tables(const share::schema::ObDatabaseSchema &database_schema,
                                     common::ObMySQLTransaction &trans,
                                     share::schema::ObSchemaGetterGuard &schema_guard);
  int update_table_version_of_db(const share::schema::ObDatabaseSchema &database_schema,
                                 common::ObMySQLTransaction &trans);

  int get_user_id_for_inner_ur(
      share::schema::ObUserInfo &user,
      bool &is_inner_ur,
      uint64_t &new_user_id);


private:
  int alter_table_rename_built_in_index_(
      const uint64_t tenant_id,
      const uint64_t data_table_id,
      const uint64_t database_id,
      const share::schema::ObIndexType index_type,
      const ObString &index_name,
      const ObString &new_index_name,
      const share::schema::ObIndexStatus *new_index_status,
      const bool is_in_deleting,
      share::schema::ObSchemaGetterGuard &schema_guard,
      common::ObMySQLTransaction &trans,
      ObArenaAllocator &allocator);

  int inner_alter_table_rename_index_(
      const uint64_t tenant_id,
      const share::schema::ObTableSchema *index_table_schema,
      const ObString &new_index_table_name,
      const share::schema::ObIndexStatus *new_index_status,
      const bool is_in_deleting,
      common::ObMySQLTransaction &trans,
      share::schema::ObTableSchema &new_index_table_schema);

  int drop_fk_cascade(
      uint64_t tenant_id,
      share::schema::ObSchemaGetterGuard &schema_guard,
      bool has_ref_priv,
      bool has_no_cascade,
      const common::ObString &grantee_name,
      const common::ObString &parent_db_name,
      const common::ObString &parent_tab_name,
      common::ObMySQLTransaction &trans);

  int build_fk_array_by_parent_table(
      uint64_t tenant_id,
      share::schema::ObSchemaGetterGuard &schema_guard,
      const common::ObString &grantee_name,
      const common::ObString &db_name,
      const common::ObString &tab_name,
      common::ObIArray<obrpc::ObDropForeignKeyArg> &drop_fk_array,
      common::ObIArray<uint64_t> &ref_tab_id_array);

  int build_next_level_revoke_obj(
      share::schema::ObSchemaGetterGuard &schema_guard,
      const share::schema::ObObjPrivSortKey &old_key,
      share::schema::ObObjPrivSortKey &new_key,
      common::ObIArray<const share::schema::ObObjPriv *> &obj_privs);

  int revoke_obj_cascade(
      share::schema::ObSchemaGetterGuard &schema_guard,
      const uint64_t start_grantee_id,
      common::ObMySQLTransaction &trans,
      const share::schema::ObObjPrivSortKey &old_key,
      share::ObRawObjPrivArray &old_array);

  int drop_obj_privs(
      const uint64_t tenant_id,
      const uint64_t obj_id,
      const uint64_t obj_ypte,
      common::ObMySQLTransaction &trans);

  int revoke_table_all(
      share::schema::ObSchemaGetterGuard &schema_guard,
      const uint64_t tenant_id,
      const share::schema::ObObjPrivSortKey &obj_priv_key,
      common::ObString &ddl_sql,
      common::ObMySQLTransaction &trans);
  int build_table_and_col_priv_array_for_revoke_all(
      share::schema::ObSchemaGetterGuard &schema_guard,
      const share::schema::ObObjPrivSortKey &obj_priv_key,
      share::ObPackedObjPriv &packed_table_priv,
      common::ObSEArray<uint64_t, 4> &col_id_array,
      common::ObSEArray<share::ObPackedObjPriv, 4> &packed_privs_array);
  int check_obj_privs_exists(
      share::schema::ObSchemaGetterGuard &schema_guard,
      const share::schema::ObObjPrivSortKey &obj_priv_key,          /* in: obj priv key*/
      const share::ObRawObjPrivArray &obj_priv_array,
      share::ObRawObjPrivArray &option_priv_array,
      bool &is_all);
  int check_obj_privs_exists_including_col_privs(
      share::schema::ObSchemaGetterGuard &schema_guard,
      const share::schema::ObObjPrivSortKey &obj_priv_key,
      const share::ObRawObjPrivArray &obj_priv_array,
      ObIArray<share::schema::ObObjPrivSortKey> &new_key_array,
      ObIArray<share::ObPackedObjPriv> &new_packed_privs_array,
      ObIArray<bool> &is_all);
  int build_raw_priv_info_inner_user(
      uint64_t grantee_id,
      share::ObRawPrivArray &raw_priv_array,
      uint64_t &option);

  int init_inner_user_privs(
      const uint64_t tenant_id,
      share::schema::ObUserInfo &user,
      common::ObMySQLTransaction &trans,
      const bool is_oracle_mode);

  // Create a keystore object for MYSQL tenants by default for master key version management
  int init_tenant_keystore(int64_t tenant_id,
                           const share::schema::ObSysVariableSchema &sys_variable,
                           common::ObMySQLTransaction &trans);
  // Create a built-in default profile for Oracle tenants
  int init_tenant_profile(int64_t tenant_id,
                          const share::schema::ObSysVariableSchema &sys_variable,
                          common::ObMySQLTransaction &trans);
  int init_tenant_optimizer_stats_info(const share::schema::ObSysVariableSchema &sys_variable,
                                       uint64_t tenant_id,
                                       ObMySQLTransaction &trans);
  int init_tenant_spm_configure(uint64_t tenant_id, ObMySQLTransaction &trans);
private:
  static const int64_t ENCRYPT_KEY_LENGTH = 15;
protected:
  share::schema::ObMultiVersionSchemaService &schema_service_;
  common::ObMySQLProxy &sql_proxy_;
};

template <typename T>
int ObDDLOperator::construct_new_name_for_recyclebin(const T &schema,
    common::ObSqlString &new_object_name)
{
  int ret = common::OB_SUCCESS;
  new_object_name.reset();
  auto *tsi_value = GET_TSI(share::schema::TSIDDLVar);
  if (OB_ISNULL(tsi_value)) {
    ret = OB_ERR_UNEXPECTED;
    RS_LOG(WARN, "Failed to get TSIDDLVar", K(ret));
  } else {
    const common::ObString *ddl_id_str = tsi_value->ddl_id_str_;
    lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::INVALID;
    if (share::schema::ObSchemaService::g_liboblog_mode_) {
      // do nothing
    } else if (OB_FAIL(share::ObCompatModeGetter::get_tenant_mode(schema.get_tenant_id(), compat_mode))) {
      RS_LOG(WARN, "fail to get tenant mode", K(ret));
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(ddl_id_str)) {
        ret = new_object_name.append_fmt((lib::Worker::CompatMode::ORACLE == compat_mode) ? "RECYCLE_$_%lu_%ld" : "__recycle_$_%lu_%ld",
            GCONF.cluster_id.get_value(),
            schema.get_schema_version());
      } else if (is_aux_object(schema)) {
        // Requires special handling of global indexes
        if (is_global_index_object(schema)) {
          common::ObString index_name;
          if (OB_FAIL(dynamic_cast<const share::schema::ObTableSchema &>(schema).get_index_name(index_name))) {
            RS_LOG(WARN, "failed to get index_name", K(ret));
          } else {
            // Specify ddl id, use ddl id
            ret = new_object_name.append_fmt((lib::Worker::CompatMode::ORACLE == compat_mode) ? "RECYCLE_$_%.*s_%.*s" : "__recycle_$_%.*s_%.*s",
                ddl_id_str->length(), ddl_id_str->ptr(),
                index_name.length(), index_name.ptr());
          }
        } else {
          // indexes or VP tables only need the current schema version
          ret = new_object_name.append_fmt((lib::Worker::CompatMode::ORACLE == compat_mode) ? "RECYCLE_$_%lu_%ld" : "__recycle_$_%lu_%ld",
              GCONF.cluster_id.get_value(),
              schema.get_schema_version());
        }
      } else {
        // Specify ddl id, then use ddl id to generate object name.
        ret = new_object_name.append_fmt((lib::Worker::CompatMode::ORACLE == compat_mode) ? "RECYCLE_$_%.*s" : "__recycle_$_%.*s",
            ddl_id_str->length(),
            ddl_id_str->ptr());
      }
    }
    if (OB_SUCCESS != ret) {
      RS_LOG(WARN, "append new object name failed", K(ret));
    }
  }
  return ret;
}

}//end namespace rootserver
}//end namespace oceanbase
#endif //OCEANBASE_ROOTSERVER_OB_DDL_OPERATOR_H_
