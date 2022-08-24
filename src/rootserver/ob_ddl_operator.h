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
#include "share/schema/ob_dependency_info.h"
#include "share/config/ob_server_config.h"
#include "share/ob_get_compat_mode.h"
#include "share/ob_partition_modify.h"

namespace oceanbase {
namespace obrpc {
class ObAccountArg;
class ObSplitPartitionArg;
class ObAlterTablegroupArg;
class ObSetPasswdArg;
class ObAlterIndexParallelArg;
}  // namespace obrpc

namespace common {
class ObIAllocator;
class ObMySQLTransaction;
class ObMySQLProxy;
class ObISQLClient;
}  // namespace common

namespace sql {
class ObSchemaChecker;
}

namespace share {
class ObSplitInfo;
namespace schema {
class ObMultiVersionSchemaService;
class ObTenantSchema;
class ObDatabaseSchema;
class ObTablegroupSchema;
class ObUser;
class ObTableSchema;
class AlterTableSchema;
class ObColumnSchemaV2;
class AlterColumnSchema;
class ObSchemaOperation;
class ObSchemaService;
class ObErrorInfo;
class ObConstraint;
class ObUDF;
}  // namespace schema
}  // namespace share

namespace obrpc {
class ObSrvRpcProxy;
class ObDropIndexArg;
class ObAlterIndexArg;
class ObRenameIndexArg;
class ObCreateTenantArg;
class ObDropForeignKeyArg;
}  // namespace obrpc

namespace rootserver {
struct ObSysStat {
  struct Item;
  typedef common::ObDList<Item> ItemList;

  struct Item : public common::ObDLinkBase<Item> {
    Item() : name_(NULL), value_(), info_(NULL)
    {}
    Item(ItemList& list, const char* name, const char* info);

    TO_STRING_KV("name", common::ObString(name_), K_(value), "info", common::ObString(info_));
    const char* name_;
    common::ObObj value_;
    const char* info_;
  };

  ObSysStat();

  // set values after bootstrap
  int set_initial_values(const uint64_t tenant_id);

  TO_STRING_KV(K_(item_list));

  ItemList item_list_;

  // only root tenant own
  Item ob_max_used_tenant_id_;
  Item ob_max_used_unit_config_id_;
  Item ob_max_used_resource_pool_id_;
  Item ob_max_used_unit_id_;
  Item ob_max_used_server_id_;

  // all tenant own
  Item ob_max_used_database_id_;
  Item ob_max_used_tablegroup_id_;
  Item ob_max_used_table_id_;
  Item ob_max_used_user_id_;
  Item ob_max_used_outline_id_;
  Item ob_max_used_sequence_id_;
  Item ob_max_used_synonym_id_;
  Item ob_max_used_udf_id_;
  Item ob_max_used_constraint_id_;
  Item ob_max_used_profile_id_;
  Item ob_max_used_dblink_id_;
};

class ObDDLOperator {
public:
  ObDDLOperator(share::schema::ObMultiVersionSchemaService& schema_service, common::ObMySQLProxy& sql_proxy);
  virtual ~ObDDLOperator();

  virtual int create_tenant(share::schema::ObTenantSchema& tenant_schema, const share::schema::ObSchemaOperationType op,
      common::ObMySQLTransaction& trans, const common::ObString* ddl_stmt_str = NULL);

  virtual int drop_tenant(
      const uint64_t tenant_id, common::ObMySQLTransaction& trans, const common::ObString* ddl_stmt_str = NULL);
  virtual int alter_tenant(share::schema::ObTenantSchema& tenant_schema, common::ObMySQLTransaction& trans,
      const common::ObString* ddl_stmt_str = NULL);
  virtual int rename_tenant(share::schema::ObTenantSchema& tenant_schema, common::ObMySQLTransaction& trans,
      const common::ObString* ddl_stmt_str = NULL);
  virtual int delay_to_drop_tenant(share::schema::ObTenantSchema& tenant_schema, common::ObMySQLTransaction& trans,
      const common::ObString* ddl_stmt_str = NULL);
  virtual int drop_tenant_to_recyclebin(common::ObSqlString& new_tenant_name,
      share::schema::ObTenantSchema& tenant_schema, common::ObMySQLTransaction& trans,
      const common::ObString* ddl_stmt_str = NULL);
  virtual int flashback_tenant_from_recyclebin(const share::schema::ObTenantSchema& tenant_schema,
      common::ObMySQLTransaction& trans, const common::ObString& new_tenant_name,
      share::schema::ObSchemaGetterGuard& schema_guard, const common::ObString& ddl_stmt_str);
  virtual int purge_tenant_in_recyclebin(const share::schema::ObTenantSchema& tenant_schema,
      common::ObMySQLTransaction& trans, const common::ObString* ddl_stmt_str);
  virtual int replace_sys_variable(share::schema::ObSysVariableSchema& sys_variable_schema,
      const int64_t schema_version, common::ObMySQLTransaction& trans,
      const share::schema::ObSchemaOperationType& operation_type, const common::ObString* ddl_stmt_str = NULL);
  int check_var_schema_options(const share::schema::ObSysVariableSchema& sys_variable_schema) const;
  virtual int clear_tenant_partition_table(const uint64_t tenant_id, common::ObISQLClient& sql_client);
  virtual int create_database(share::schema::ObDatabaseSchema& database_schema, common::ObMySQLTransaction& trans,
      const common::ObString* ddl_stmt_str = NULL);
  virtual int alter_database(share::schema::ObDatabaseSchema& new_database_schema, common::ObMySQLTransaction& trans,
      const share::schema::ObSchemaOperationType op_type, const common::ObString* ddl_stmt_str = NULL,
      const bool need_update_schema_version = true);
  virtual int drop_database(const share::schema::ObDatabaseSchema& db_schema, common::ObMySQLTransaction& trans,
      share::schema::ObSchemaGetterGuard& guard, const common::ObString* ddl_stmt_str = NULL);
  virtual int create_tablegroup(share::schema::ObTablegroupSchema& tablegroup_schema, common::ObMySQLTransaction& trans,
      const common::ObString* ddl_stmt_str = NULL);
  virtual int drop_tablegroup(const share::schema::ObTablegroupSchema& tablegroup_schema,
      common::ObMySQLTransaction& trans, const common::ObString* ddl_stmt_str = NULL);
  virtual int alter_tablegroup(share::schema::ObTablegroupSchema& new_schema, common::ObMySQLTransaction& trans,
      const common::ObString* ddl_stmt_str = NULL);
  virtual int drop_restore_point(const uint64_t tenant_id, common::ObMySQLTransaction& trans);
  int alter_tablegroup(share::schema::ObSchemaGetterGuard& schema_guard, share::schema::ObTableSchema& new_table_schema,
      common::ObMySQLTransaction& trans, const common::ObString* ddl_stmt_str = NULL);
  int add_tablegroup_partitions(const share::schema::ObTablegroupSchema& orig_tablegroup_schema,
      const share::schema::ObTablegroupSchema& inc_tablegroup_schema, const int64_t new_schema_version,
      share::schema::ObTablegroupSchema& new_tablegroup_schema, common::ObMySQLTransaction& trans,
      const common::ObString* ddl_stmt_str);
  int drop_tablegroup_partitions(const share::schema::ObTablegroupSchema& orig_tablegroup_schema,
      share::schema::ObTablegroupSchema& inc_tablegroup_schema, const int64_t new_schema_version,
      share::schema::ObTablegroupSchema& new_tablegroup_schema, common::ObMySQLTransaction& trans,
      const common::ObString* ddl_stmt_str);
  int add_dropped_part_to_partition_schema(const common::ObIArray<int64_t>& part_ids,
      const share::schema::ObPartitionSchema& orig_part_schema, share::schema::ObPartitionSchema& alter_part_schema);
  int add_dropped_subpart_to_partition_schema(const common::ObIArray<int64_t>& part_ids,
      const share::schema::ObPartitionSchema& orig_part_schema, share::schema::ObPartitionSchema& alter_part_schema);
  int filter_part_id_to_be_dropped(share::schema::ObSchemaGetterGuard& schema_guard,
      const common::ObIArray<int64_t>& part_ids, common::ObIArray<int64_t>& part_ids_for_delay_delete_part,
      const share::schema::ObTableSchema& orig_table_schema);
  int force_drop_table_and_partitions(const uint64_t table_id, share::schema::ObSchemaGetterGuard& schema_guard,
      const common::ObIArray<int64_t>& partition_ids, const common::ObIArray<int64_t>& subpartition_ids,
      common::ObMySQLTransaction& trans);
  int force_drop_database(
      const uint64_t db_id, share::schema::ObSchemaGetterGuard& schema_guard, common::ObMySQLTransaction& trans);
  static int check_part_equal(const share::schema::ObPartitionFuncType part_type,
      const share::schema::ObPartition* r_part, const share::schema::ObPartition* l_part, bool& is_equal);
  static bool is_list_values_equal(
      const common::ObRowkey& fir_values, const common::ObIArray<common::ObNewRow>& sed_values);
  int drop_tablegroup_for_inspection(
      const share::schema::ObTablegroupSchema& tablegroup_schema, common::ObMySQLTransaction& trans);
  int drop_tablegroup_partitions_for_inspection(const share::schema::ObTablegroupSchema& orig_tablegroup_schema,
      const share::schema::ObTablegroupSchema& inc_tablegroup_schema,
      share::schema::ObTablegroupSchema& new_tablegroup_schema, common::ObMySQLTransaction& trans);
  int add_table_partitions(const share::schema::ObTableSchema& orig_table_schema,
      const share::schema::ObTableSchema& inc_table_schema, const int64_t schema_version,
      common::ObMySQLTransaction& trans);
  int drop_table_partitions(const share::schema::ObTableSchema& orig_table_schema,
      share::schema::ObTableSchema& inc_table_schema, const int64_t schema_version, common::ObMySQLTransaction& trans);
  int modify_check_constraints_state(const share::schema::ObTableSchema& orig_table_schema,
      const share::schema::ObTableSchema& inc_table_schema, share::schema::ObTableSchema& new_table_schema,
      common::ObMySQLTransaction& trans);
  int add_table_constraints(const share::schema::ObTableSchema& orig_table_schema,
      const share::schema::ObTableSchema& inc_table_schema, share::schema::ObTableSchema& new_table_schema,
      common::ObMySQLTransaction& trans);
  int add_table_foreign_keys(const share::schema::ObTableSchema& orig_table_schema,
      share::schema::ObTableSchema& inc_table_schema, share::schema::ObTableSchema& new_table_schema,
      common::ObMySQLTransaction& trans);
  int drop_table_constraints(const share::schema::ObTableSchema& orig_table_schema,
      const share::schema::ObTableSchema& inc_table_schema, share::schema::ObTableSchema& new_table_schema,
      common::ObMySQLTransaction& trans);
  virtual int create_user(
      share::schema::ObUserInfo& user, const common::ObString* ddl_stmt_str, common::ObMySQLTransaction& trans);
  int alter_user_default_role(const common::ObString& ddl_str, const share::schema::ObUserInfo& schema,
      common::ObIArray<uint64_t>& role_id_array, common::ObIArray<uint64_t>& disable_flag_array,
      common::ObMySQLTransaction& trans);
  int alter_user_profile(
      const common::ObString& ddl_str, share::schema::ObUserInfo& schema, common::ObMySQLTransaction& trans);
  virtual int create_table(share::schema::ObTableSchema& table_schema, common::ObMySQLTransaction& trans,
      const common::ObString* ddl_stmt_str = NULL, const bool need_sync_schema_version = true,
      const bool is_truncate_table = false);
  int split_tablegroup_partitions(const share::schema::ObTablegroupSchema& tablegroup_schema,
      const share::schema::ObTablegroupSchema& new_tablegroup_schema, const share::ObSplitInfo& split_info,
      common::ObMySQLTransaction& trans, const common::ObString* ddl_str,
      share::schema::ObSchemaOperationType opt_type);

  int split_table_partitions(share::schema::ObTableSchema& new_schema,
      const share::schema::ObTableSchema& orig_table_schema, const share::schema::ObTableSchema& inc_schema,
      const share::ObSplitInfo& split_info, const bool is_alter_tablegroup, common::ObMySQLTransaction& trans);
  int truncate_table_partitions(const share::schema::ObTableSchema& orig_table_schema,
      share::schema::ObTableSchema& inc_table_schema, common::ObMySQLTransaction& trans);
  int truncate_table_subpartitions(const share::schema::ObTableSchema& orig_table_schema,
      share::schema::ObTableSchema& inc_table_schema, common::ObMySQLTransaction& trans);
  int add_table_partitions(const share::schema::ObTableSchema& orig_table_schema,
      const share::schema::ObTableSchema& inc_table_schema, share::schema::ObTableSchema& new_table_schema,
      common::ObMySQLTransaction& trans);
  int add_table_subpartitions(const share::schema::ObTableSchema& orig_table_schema,
      share::schema::ObTableSchema& inc_table_schema, share::schema::ObTableSchema& new_table_schema,
      common::ObMySQLTransaction& trans);
  int drop_table_partitions(const share::schema::ObTableSchema& orig_table_schema,
      share::schema::ObTableSchema& inc_table_schema, share::schema::ObTableSchema& new_table_schema,
      common::ObMySQLTransaction& trans);
  int drop_table_subpartitions(const share::schema::ObTableSchema& orig_table_schema,
      share::schema::ObTableSchema& inc_table_schema, share::schema::ObTableSchema& new_table_schema,
      common::ObMySQLTransaction& trans);
  int drop_table_for_inspection(
      const share::schema::ObTableSchema& orig_table_schema, common::ObMySQLTransaction& trans);
  int drop_database_for_inspection(share::schema::ObSchemaGetterGuard& schema_guard,
      const share::schema::ObDatabaseSchema& orig_database_schema, common::ObMySQLTransaction& trans);
  int drop_table_partitions_for_inspection(const share::schema::ObTableSchema& orig_table_schema,
      const share::schema::ObTableSchema& inc_table_schema, common::ObMySQLTransaction& trans);
  int drop_table_subpartitions_for_inspection(const share::schema::ObTableSchema& orig_table_schema,
      const share::schema::ObTableSchema& inc_table_schema, common::ObMySQLTransaction& trans);
  int update_partition_cnt_within_partition_table(
      share::schema::ObTableSchema& new_table_schema, common::ObMySQLTransaction& trans);
  int get_part_array_from_table(const share::schema::ObTableSchema& orig_table_schema,
      const share::schema::ObTableSchema& inc_table_schema, common::ObIArray<share::schema::ObPartition*>& part_array);
  int update_max_used_part_id(const bool is_add, const share::schema::ObTableSchema& orig_table_schema,
      const share::schema::ObTableSchema& inc_table_schema, common::ObMySQLTransaction& trans);
  int update_max_used_subpart_id(const bool is_add, const share::schema::ObTableSchema& orig_table_schema,
      const share::schema::ObTableSchema& inc_table_schema, common::ObMySQLTransaction& trans);
  int insert_single_column(common::ObMySQLTransaction& trans, const share::schema::ObTableSchema& new_table_schema,
      share::schema::ObColumnSchemaV2& new_column);
  int delete_single_column(common::ObMySQLTransaction& trans, share::schema::ObTableSchema& new_table_schema,
      const common::ObString& column_name);
  int modify_func_expr_column_name(const share::schema::ObColumnSchemaV2& orig_column,
      const share::schema::ObColumnSchemaV2& alter_column, share::schema::ObTableSchema& table_schema,
      const common::ObTimeZoneInfo& tz_info, common::ObIAllocator& allocator, bool is_sub_part);
  /**
   * Rebuild check constraint expr, if the expr str contains the old column name
   *
   * @new_check_expr_str [out] new check expr str, if contains old column name
   * @need_modify_check_expr [out] is true, if contains old column name
   */
  int rebuild_constraint_check_expr(const share::schema::ObColumnSchemaV2& orig_column,
      const share::schema::ObColumnSchemaV2& alter_column, const share::schema::ObConstraint& cst,
      share::schema::ObTableSchema& table_schema, const common::ObTimeZoneInfo& tz_info,
      common::ObIAllocator& allocator, common::ObString& new_check_expr_str, bool& need_modify_check_expr);
  // Traverse all the check constraint expr str,
  // replacing the old column name with new name
  int modify_constraint_check_expr(const share::schema::ObColumnSchemaV2& orig_column,
      const share::schema::ObColumnSchemaV2& alter_column, share::schema::ObTableSchema& table_schema,
      const common::ObTimeZoneInfo& tz_info, common::ObIAllocator& allocator, common::ObMySQLTransaction& trans);
  int modify_part_func_expr(const share::schema::ObColumnSchemaV2& orig_column,
      const share::schema::ObColumnSchemaV2& alter_column, share::schema::ObTableSchema& table_schema,
      const common::ObTimeZoneInfo& tz_info, common::ObIAllocator& allocator);
  int modify_part_func_expr_for_global_index(const share::schema::ObColumnSchemaV2& orig_column,
      const share::schema::ObColumnSchemaV2& alter_column, share::schema::ObTableSchema& table_schema,
      const common::ObTimeZoneInfo& tz_info, common::ObIAllocator& allocator, common::ObMySQLTransaction& trans,
      common::ObIArray<share::schema::ObTableSchema>* global_idx_schema_array = NULL);

  int modify_generated_column_default_value(share::schema::ObColumnSchemaV2& generated_column,
      common::ObString& column_name, const common::ObString& new_column_name,
      const share::schema::ObTableSchema& table_schema, const common::ObTimeZoneInfo& tz_info);
  virtual int alter_table_column(const share::schema::ObTableSchema& origin_table_schema,
      const share::schema::AlterTableSchema& alter_table_schema, share::schema::ObTableSchema& new_table_schema,
      const common::ObTimeZoneInfoWrap& tz_info_wrap, const common::ObString* nls_formats,
      common::ObMySQLTransaction& trans, common::ObIAllocator& allocator,
      common::ObIArray<share::schema::ObTableSchema>* global_idx_schema_array = NULL);

  virtual int alter_table_create_index(const share::schema::ObTableSchema& new_table_schema,
      const int64_t frozen_version, common::ObIArray<share::schema::ObColumnSchemaV2*>& gen_columns,
      share::schema::ObTableSchema& index_schema, common::ObMySQLTransaction& trans);

  virtual int alter_table_drop_index(const uint64_t data_table_id, const uint64_t database_id,
      const obrpc::ObDropIndexArg& drop_index_arg, share::schema::ObTableSchema& new_data_table_schema,
      common::ObMySQLTransaction& trans);
  virtual int alter_table_alter_index(const uint64_t data_table_id, const uint64_t database_id,
      const obrpc::ObAlterIndexArg& alter_index_arg, common::ObMySQLTransaction& trans);
  virtual int alter_table_drop_foreign_key(const share::schema::ObTableSchema& table_schema,
      const obrpc::ObDropForeignKeyArg& drop_foreign_key_arg, common::ObMySQLTransaction& trans);
  virtual int alter_index_drop_options(const share::schema::ObTableSchema& index_table_schema,
      const common::ObString& table_name, share::schema::ObTableSchema& new_index_table_schema,
      common::ObMySQLTransaction& trans);
  virtual int alter_table_rename_index(const uint64_t data_table_id, const uint64_t database_id,
      const obrpc::ObRenameIndexArg& rename_index_arg, common::ObMySQLTransaction& trans);
  virtual int alter_index_table_parallel(const uint64_t data_table_id, const uint64_t database_id,
      const obrpc::ObAlterIndexParallelArg& alter_parallel_arg, common::ObMySQLTransaction& trans);

  virtual int alter_table_options(share::schema::ObSchemaGetterGuard& schema_guard,
      share::schema::ObTableSchema& new_table_schema, const share::schema::ObTableSchema& table_schema,
      const bool update_index_table, common::ObMySQLTransaction& trans,
      const common::ObIArray<share::schema::ObTableSchema>* global_idx_schema_array = NULL);

  int update_table_schema_version(
      common::ObMySQLTransaction& trans, const share::schema::ObTableSchema& new_table_schema);
  virtual int update_aux_table(const share::schema::ObTableSchema& table_schema,
      const share::schema::ObTableSchema& new_table_schema, share::schema::ObSchemaGetterGuard& schema_guard,
      common::ObMySQLTransaction& trans, const share::schema::ObTableType table_type,
      const common::ObIArray<share::schema::ObTableSchema>* global_idx_schema_array = NULL);

  virtual int update_table_attribute(share::schema::ObTableSchema& new_table_schema, common::ObMySQLTransaction& trans,
      const share::schema::ObSchemaOperationType operation_type, const common::ObString* ddl_stmt_str = NULL);
  virtual int insert_ori_schema_version(
      common::ObMySQLTransaction& trans, const uint64_t table_id, const int64_t& ori_schema_version);
  virtual int update_prev_id_for_add_column(const share::schema::ObTableSchema& origin_table_schema,
      share::schema::ObTableSchema& new_table_schema, share::schema::AlterColumnSchema& alter_column_schema,
      share::schema::ObSchemaService& schema_service, common::ObMySQLTransaction& trans);
  virtual int update_prev_id_for_delete_column(const share::schema::ObTableSchema& origin_table_schema,
      share::schema::ObTableSchema& new_table_schema, const share::schema::ObColumnSchemaV2& ori_column_schema,
      share::schema::ObSchemaService& schema_service, common::ObMySQLTransaction& trans);
  virtual int drop_inner_generated_index_column(common::ObMySQLTransaction& trans,
      share::schema::ObSchemaGetterGuard& schema_guard, const share::schema::ObTableSchema& index_schema,
      share::schema::ObTableSchema& new_data_table_schema);

  //  virtual int log_ddl_operation(share::schema::ObSchemaOperation &ddl_operation,
  //                                       common::ObMySQLTransaction &trans);

  virtual int drop_table(const share::schema::ObTableSchema& table_schema, common::ObMySQLTransaction& trans,
      const common::ObString* ddl_stmt_str = NULL, const bool is_truncate_table = false,
      share::schema::DropTableIdHashSet* drop_table_set = NULL, const bool is_drop_db = false,
      bool* is_delay_delete = NULL /* Bring out the delayed delete behavior */);
  virtual int drop_table_for_not_dropped_schema(const share::schema::ObTableSchema& table_schema,
      common::ObMySQLTransaction& trans, const common::ObString* ddl_stmt_str = NULL,
      const bool is_truncate_table = false, share::schema::DropTableIdHashSet* drop_table_set = NULL,
      const bool is_drop_db = false);
  virtual int drop_table_to_recyclebin(const share::schema::ObTableSchema& table_schema,
      share::schema::ObSchemaGetterGuard& schema_guard, common::ObMySQLTransaction& trans,
      const common::ObString* ddl_stmt_str, /*= NULL*/
      const bool is_truncate_table = false);
  virtual int flashback_table_from_recyclebin(const share::schema::ObTableSchema& table_schema,
      common::ObMySQLTransaction& trans, const uint64_t new_db_id, const common::ObString& new_table_name,
      const common::ObString* ddl_stmt_str, share::schema::ObSchemaGetterGuard& guard);
  virtual int purge_table_in_recyclebin(const share::schema::ObTableSchema& table_schema,
      common::ObMySQLTransaction& trans, const common::ObString* ddl_stmt_str /*=NULL*/);
  // create_table_in_recylebin only applies to truncate table.
  virtual int create_index_in_recyclebin(share::schema::ObTableSchema& table_schema,
      share::schema::ObSchemaGetterGuard& schema_guard, common::ObMySQLTransaction& trans,
      const common::ObString* ddl_stmt_str);

  virtual int drop_database_to_recyclebin(const share::schema::ObDatabaseSchema& database_schema,
      common::ObMySQLTransaction& trans, share::schema::ObSchemaGetterGuard& guard,
      const common::ObString* ddl_stmt_str);
  virtual int flashback_database_from_recyclebin(const share::schema::ObDatabaseSchema& database_schema,
      common::ObMySQLTransaction& trans, const common::ObString& new_db_name,
      share::schema::ObSchemaGetterGuard& schema_guard, const common::ObString& ddl_stmt_str);
  virtual int purge_table_of_database(const share::schema::ObDatabaseSchema& db_schema,
      common::ObMySQLTransaction& trans, share::schema::ObSchemaGetterGuard& guard);
  virtual int purge_database_in_recyclebin(const share::schema::ObDatabaseSchema& database_schema,
      common::ObMySQLTransaction& trans, share::schema::ObSchemaGetterGuard& guard,
      const common::ObString* ddl_stmt_str);
  virtual int purge_table_with_aux_table(const share::schema::ObTableSchema& table_schema,
      share::schema::ObSchemaGetterGuard& schema_guard, common::ObMySQLTransaction& trans,
      const common::ObString* ddl_stmt_str);
  virtual int purge_aux_table(const share::schema::ObTableSchema& table_schema,
      share::schema::ObSchemaGetterGuard& schema_guard, common::ObMySQLTransaction& trans,
      const share::schema::ObTableType table_type);

  virtual int fetch_expire_recycle_objects(const uint64_t tenant_id, const int64_t expire_time,
      common::ObIArray<share::schema::ObRecycleObject>& recycle_objs);

  virtual int finish_schema_split_v2(common::ObMySQLTransaction& trans, const uint64_t tenant_id);
  virtual int init_tenant_env(const share::schema::ObTenantSchema& tenant_schema,
      const share::schema::ObSysVariableSchema& sys_variable, common::ObMySQLTransaction& trans);
  virtual int rename_table(const share::schema::ObTableSchema& table_schema, const common::ObString& new_table_name,
      const uint64_t new_db_id, common::ObMySQLTransaction& trans, const common::ObString* ddl_stmt_str);
  virtual int update_index_status(const uint64_t data_table_id, const uint64_t index_table_id,
      const share::schema::ObIndexStatus status, const int64_t create_mem_version, common::ObMySQLTransaction& trans);

  //----Functions for managing privileges----
  virtual int create_user(const share::schema::ObUserInfo& user_info, const common::ObString* ddl_stmt_str,
      common::ObMySQLTransaction& trans);

  virtual int drop_user(const uint64_t tenant_id, const uint64_t user_id, const common::ObString* ddl_stmt_str,
      common::ObMySQLTransaction& trans);
  virtual int drop_db_table_privs(const uint64_t tenant_id, const uint64_t user_id, common::ObMySQLTransaction& trans);
  int get_drop_db_table_privs_count(const int64_t tenant_id, const int64_t user_id, int64_t& ddl_count);
  virtual int rename_user(const uint64_t tenant_id, const uint64_t user_id, const obrpc::ObAccountArg& new_account,
      const common::ObString* ddl_stmt_str, common::ObMySQLTransaction& trans);
  virtual int set_passwd(const uint64_t tenant_id, const uint64_t user_id, const common::ObString& passwd,
      const common::ObString* ddl_stmt_str, common::ObMySQLTransaction& trans);
  virtual int set_max_connections(const uint64_t tenant_id, const uint64_t user_id, const uint64_t max_connections_per_hour,
      const uint64_t max_user_connections, const common::ObString *ddl_stmt_str, common::ObMySQLTransaction &trans);
  virtual int alter_user_require(const uint64_t tenant_id, const uint64_t user_id, const obrpc::ObSetPasswdArg& arg,
      const common::ObString* ddl_stmt_str, common::ObMySQLTransaction& trans);
  virtual int grant_revoke_user(const uint64_t tenant_id, const uint64_t user_id, const ObPrivSet priv_set,
      const bool grant, const common::ObString* ddl_stmt_str, common::ObMySQLTransaction& trans);
  virtual int lock_user(const uint64_t tenant_id, const uint64_t user_id, const bool locked,
      const common::ObString* ddl_stmt_str, common::ObMySQLTransaction& trans);
  virtual int grant_database(const share::schema::ObOriginalDBKey& db_priv_key, const ObPrivSet priv_set,
      const common::ObString* ddl_stmt_str, common::ObMySQLTransaction& trans);
  virtual int revoke_database(
      const share::schema::ObOriginalDBKey& db_priv_key, const ObPrivSet priv_set, common::ObMySQLTransaction& trans);
  virtual int grant_table(const share::schema::ObTablePrivSortKey& table_priv_key, const ObPrivSet priv_set,
      const common::ObString* ddl_stmt_str, common::ObMySQLTransaction& trans,
      const share::ObRawObjPrivArray& obj_priv_array, const uint64_t option,
      const share::schema::ObObjPrivSortKey& obj_priv_key);
  virtual int revoke_table(const share::schema::ObTablePrivSortKey& table_priv_key, const ObPrivSet priv_set,
      common::ObMySQLTransaction& trans, const share::schema::ObObjPrivSortKey& obj_priv_key,
      const share::ObRawObjPrivArray& obj_priv_array, const bool revoke_all_ora);
  virtual int grant_revoke_role(const uint64_t tenant_id, const share::schema::ObUserInfo& user_info,
      const common::ObIArray<uint64_t>& role_ids, const share::schema::ObUserInfo* specified_role_info,
      common::ObMySQLTransaction& trans, const bool is_grant, const uint64_t option);
  virtual int get_flush_role_array(const uint64_t option, const common::ObIArray<uint64_t>& org_role_ids,
      bool& need_flush, bool is_grant, const share::schema::ObUserInfo& user_info,
      common::ObIArray<uint64_t>& role_ids);
  virtual int get_flush_priv_array(const uint64_t option, const share::ObRawPrivArray& priv_array,
      const share::schema::ObSysPriv* sys_priv, share::ObRawPrivArray& new_priv_array, bool& need_flush,
      const bool is_grant, const share::schema::ObUserInfo& user_info);
  virtual int grant_sys_priv_to_ur(const uint64_t tenant_id, const uint64_t grantee_id,
      const share::schema::ObSysPriv* sys_priv, const uint64_t option, const share::ObRawPrivArray priv_array,
      common::ObMySQLTransaction& trans, const bool is_grant, const common::ObString* ddl_stmt_str,
      share::schema::ObSchemaGetterGuard& schema_guard);
  static int drop_obj_privs(const uint64_t tenant_id, const uint64_t obj_id, const uint64_t obj_ypte,
      common::ObMySQLTransaction& trans, share::schema::ObMultiVersionSchemaService& schema_service,
      share::schema::ObSchemaGetterGuard& schema_guard);
  //----End of functions for managing privileges----
  //----Functions for managing outlines----
  int create_outline(share::schema::ObOutlineInfo& outline_info, common::ObMySQLTransaction& trans,
      const common::ObString* ddl_stmt_str /*=NULL*/);
  int replace_outline(share::schema::ObOutlineInfo& outline_info, common::ObMySQLTransaction& trans,
      const common::ObString* ddl_stmt_str /*=NULL*/);
  int alter_outline(share::schema::ObOutlineInfo& outline_info, common::ObMySQLTransaction& trans,
      const common::ObString* ddl_stmt_str /*=NULL*/);
  int drop_outline(const uint64_t tenant_id, const uint64_t database_id, const uint64_t outline_id,
      common::ObMySQLTransaction& trans, const common::ObString* ddl_stmt_str /*=NULL*/);
  //----End of functions for managing outlines----
  //

  //----Functions for managing dblinks----
  int create_dblink(share::schema::ObDbLinkBaseInfo& dblink_info, common::ObMySQLTransaction& trans,
      const common::ObString* ddl_stmt_str /*=NULL*/);
  int drop_dblink(share::schema::ObDbLinkBaseInfo& dblink_info, common::ObMySQLTransaction& trans,
      const common::ObString* ddl_stmt_str /*=NULL*/);
  //----End of functions for managing dblinks----

  int create_synonym(share::schema::ObSynonymInfo& synonym_info, common::ObMySQLTransaction& trans,
      const common::ObString* ddl_stmt_str /*=NULL*/);
  int replace_synonym(share::schema::ObSynonymInfo& synonym_info, common::ObMySQLTransaction& trans,
      const common::ObString* ddl_stmt_str /*=NULL*/);
  int drop_synonym(const uint64_t tenant_id, const uint64_t database_id, const uint64_t synonym_info_id,
      common::ObMySQLTransaction& trans, const common::ObString* ddl_stmt_str /*=NULL*/);

  //----Functions for managing UDF----
  int create_user_defined_function(share::schema::ObUDF& udf_info, common::ObMySQLTransaction& trans,
      const common::ObString* ddl_stmt_str /*=NULL*/);
  int drop_user_defined_function(const uint64_t tenant_id, const common::ObString& name,
      common::ObMySQLTransaction& trans, const common::ObString* ddl_stmt_str /*=NULL*/);
  //----End of functions for managing UDF----

  //----Functions for managing profile----
  int handle_profile_function(share::schema::ObProfileSchema& schema, common::ObMySQLTransaction& trans,
      share::schema::ObSchemaOperationType ddl_type, const common::ObString& ddl_stmt_str,
      share::schema::ObSchemaGetterGuard& schema_guard);
  //----End of functions for managing profile----

  int insert_dependency_infos(common::ObMySQLTransaction& trans,
      common::ObIArray<share::schema::ObDependencyInfo>& dep_infos, uint64_t tenant_id, uint64_t dep_obj_id,
      uint64_t schema_version, uint64_t owner_id);

  // FIXME: After the schema split, the interface is outdated and replaced with a new interface.
  int64_t get_last_operation_schema_version() const;
  int get_tenant_last_operation_schema_version(const uint64_t tenant_id, int64_t& schema_version) const;

  int split_partition_finish(const share::schema::ObTableSchema& table_schema, const obrpc::ObSplitPartitionArg& arg,
      const share::ObSplitProgress split_status, common::ObMySQLTransaction& trans);
  int split_tablegroup_partition_finish(const share::schema::ObTablegroupSchema& tablegroup_schema,
      const share::ObSplitProgress split_status, common::ObMySQLTransaction& trans);
  int batch_update_max_used_part_id(common::ObISQLClient& trans, share::schema::ObSchemaGetterGuard& schema_guard,
      const int64_t schema_version, const share::schema::ObTablegroupSchema& tg_schema,
      const obrpc::ObAlterTablegroupArg& arg);
  virtual int insert_temp_table_info(
      common::ObMySQLTransaction& trans, const share::schema::ObTableSchema& table_schema);
  virtual int delete_temp_table_info(
      common::ObMySQLTransaction& trans, const share::schema::ObTableSchema& table_schema);
  int check_is_delay_delete(const int64_t tenant_id, bool& is_delay_delete);
  template <typename T>
  int construct_new_name_for_recyclebin(const T& schema, common::ObSqlString& new_table_name);
  static int replace_sys_stat(const uint64_t tenant_id, ObSysStat& sys_stat, common::ObISQLClient& trans);

private:
  virtual int set_need_flush_ora(share::schema::ObSchemaGetterGuard& schema_guard,
      const share::schema::ObObjPrivSortKey& obj_priv_key, /* in: obj priv key*/
      const uint64_t option,                               /* in: new option */
      const share::ObRawObjPrivArray& obj_priv_array,      /* in: new privs used want to add */
      share::ObRawObjPrivArray& new_obj_priv_array);
  virtual int init_tenant_tablegroup(const uint64_t tenant_id, common::ObMySQLTransaction& trans);
  virtual int init_tenant_database(const share::schema::ObTenantSchema& tenant_schema, const common::ObString& db_name,
      const uint64_t pure_db_id, const common::ObString& db_comment, common::ObMySQLTransaction& trans,
      const bool is_oracle_mode = false);
  virtual int init_tenant_databases(const share::schema::ObTenantSchema& tenant_schema,
      const share::schema::ObSysVariableSchema& sys_variable, common::ObMySQLTransaction& trans);
  virtual int init_tenant_user(const uint64_t tenant_id, const common::ObString& user_name, const char* password,
      const uint64_t pure_user_id, const common::ObString& user_comment, common::ObMySQLTransaction& trans,
      const bool set_locked = false, const bool is_user = true, const bool is_oracle_mode = false);
  virtual int init_tenant_users(const share::schema::ObTenantSchema& tenant_schema,
      const share::schema::ObSysVariableSchema& sys_variable, common::ObMySQLTransaction& trans);
  virtual int init_tenant_sys_stats(const uint64_t tenant_id, common::ObMySQLTransaction& trans);
  virtual int init_tenant_config(const uint64_t tenant_id, common::ObMySQLTransaction& trans);
  virtual int init_tenant_cgroup(const uint64_t tenant_id, common::ObMySQLTransaction& trans);
  virtual int init_sys_tenant_charset(common::ObMySQLTransaction& trans);
  virtual int init_sys_tenant_collation(common::ObMySQLTransaction& trans);
  virtual int init_sys_tenant_privilege(common::ObMySQLTransaction& trans);
  int check_tenant_exist(
      share::schema::ObSchemaGetterGuard& schema_guard, const common::ObString& tenant_name, bool& is_exist);
  // virtual int init_tenant_profile(common::ObMySQLTransaction &trans);
  //
  // materialized view related
  //
  virtual int alter_table_update_index_and_view_column(share::schema::ObSchemaService& schema_service,
      const share::schema::ObTableSchema& new_table_schema, const share::schema::ObColumnSchemaV2& new_column_schema,
      common::ObMySQLTransaction& trans,
      const common::ObIArray<share::schema::ObTableSchema>* global_idx_schema_array = NULL);

  virtual int alter_table_update_aux_column(share::schema::ObSchemaService& schema_service,
      const share::schema::ObTableSchema& new_table_schema, const share::schema::ObColumnSchemaV2& new_column_schema,
      common::ObMySQLTransaction& trans, const share::schema::ObTableType table_type,
      const common::ObIArray<share::schema::ObTableSchema>* global_idx_schema_array = NULL);

  virtual int validate_update_column_for_materialized_view(const share::schema::ObTableSchema& origin_table_schema,
      const share::schema::ObColumnSchemaV2& orig_column_schema);

  int sync_version_for_cascade_table(const common::ObIArray<uint64_t>& table_ids, common::ObMySQLTransaction& trans);

  int cleanup_autoinc_cache(const share::schema::ObTableSchema& table_schema);

  virtual int alter_table_drop_aux_column(share::schema::ObSchemaService& schema_service,
      share::schema::ObTableSchema& new_table_schema, const share::schema::ObColumnSchemaV2& new_column_schema,
      common::ObMySQLTransaction& trans, const share::schema::ObTableType table_type);
  virtual int check_new_column_for_index(common::ObIArray<share::schema::ObTableSchema>& idx_schemas,
      const share::schema::ObColumnSchemaV2& new_column_schema);
  int generate_tmp_idx_schemas(const share::schema::ObTableSchema& new_table_schema,
      common::ObIArray<share::schema::ObTableSchema>& idx_schemas);
  int resolve_timestamp_column(share::schema::AlterColumnSchema* alter_column_schema,
      share::schema::ObTableSchema& new_table_schema, share::schema::ObColumnSchemaV2& new_column_schema,
      const common::ObTimeZoneInfoWrap& tz_info_wrap, const common::ObString* nls_formats,
      common::ObIAllocator& allocator);
  int check_not_null_attribute(const share::schema::ObTableSchema& table_schema,
      const share::schema::ObColumnSchemaV2& old_column_schema,
      const share::schema::ObColumnSchemaV2& new_column_schema);
  int check_generated_column_modify_authority(const share::schema::ObColumnSchemaV2& old_column_schema,
      const share::schema::AlterColumnSchema& alter_column_schema);
  int resolve_orig_default_value(share::schema::ObColumnSchemaV2& column_schema,
      const common::ObTimeZoneInfoWrap& tz_info_wrap, const common::ObString* nls_formats,
      common::ObIAllocator& allocator);
  int deal_default_value_padding(share::schema::ObColumnSchemaV2& column_schema, common::ObIAllocator& allocator);
  bool is_aux_object(const share::schema::ObDatabaseSchema& schema);
  bool is_aux_object(const share::schema::ObTableSchema& schema);
  bool is_aux_object(const share::schema::ObTenantSchema& schema);
  bool is_global_index_object(const share::schema::ObDatabaseSchema& schema);
  bool is_global_index_object(const share::schema::ObTableSchema& schema);
  bool is_global_index_object(const share::schema::ObTenantSchema& schema);
  int update_tablegroup_id_of_tables(const share::schema::ObDatabaseSchema& database_schema,
      common::ObMySQLTransaction& trans, share::schema::ObSchemaGetterGuard& schema_guard);
  int update_table_version_of_db(const share::schema::ObDatabaseSchema& database_schema,
      common::ObMySQLTransaction& trans, share::schema::ObSchemaGetterGuard& schema_guard);
  int check_is_change_column_type(const share::schema::ObColumnSchemaV2& src_column,
      const share::schema::ObColumnSchemaV2& dst_column, bool& is_change_column_type);
  int check_column_in_index(
      const uint64_t column_id, const share::schema::ObTableSchema& table_schema, bool& is_in_index);
  int check_can_alter_column_type(const share::schema::ObColumnSchemaV2& src_column,
      const share::schema::ObColumnSchemaV2& dst_column, const share::schema::ObTableSchema& table_schema);
  int create_new_name_for_delay_delete_database(common::ObString& new_name,
      const share::schema::ObDatabaseSchema* db_schema, common::ObIAllocator& allocator,
      share::schema::ObSchemaGetterGuard& guard);
  int create_new_name_for_delay_delete_table(common::ObString& new_name,
      const share::schema::ObTableSchema* table_schema, common::ObIAllocator& allocator,
      share::schema::ObSchemaGetterGuard& guard);
  int create_new_name_for_delay_delete_table_partition(
      common::ObString& new_name, const share::schema::ObTableSchema* table_schema, common::ObIAllocator& allocator);
  int create_new_name_for_delay_delete_tablegroup(common::ObString& new_name,
      const share::schema::ObTablegroupSchema* tablegroup_schema, common::ObIAllocator& allocator,
      share::schema::ObSchemaGetterGuard& guard);
  int create_new_name_for_delay_delete_tablegroup_partition(common::ObString& new_name,
      const share::schema::ObTablegroupSchema* tablegroup_schema, common::ObIAllocator& allocator);
  template <typename SchemaType>
  int build_flashback_object_name(const SchemaType& object_schema, const char* data_table_prifix,
      const char* object_type_prefix, share::schema::ObSchemaGetterGuard& schema_guard, common::ObIAllocator& allocator,
      common::ObString& object_name);
  int get_user_id_for_inner_ur(share::schema::ObUserInfo& user, bool& is_inner_ur, uint64_t& new_user_id);

private:
  int drop_fk_cascade(uint64_t tenant_id, share::schema::ObSchemaGetterGuard& schema_guard, bool has_ref_priv,
      bool has_no_cascade, const common::ObString& grantee_name, const common::ObString& parent_db_name,
      const common::ObString& parent_tab_name, common::ObMySQLTransaction& trans);

  int build_fk_array_by_parent_table(uint64_t tenant_id, share::schema::ObSchemaGetterGuard& schema_guard,
      const common::ObString& grantee_name, const common::ObString& db_name, const common::ObString& tab_name,
      common::ObIArray<obrpc::ObDropForeignKeyArg>& drop_fk_array, common::ObIArray<uint64_t>& ref_tab_id_array);

  int build_next_level_revoke_obj(share::schema::ObSchemaGetterGuard& schema_guard,
      const share::schema::ObObjPrivSortKey& old_key, share::schema::ObObjPrivSortKey& new_key,
      common::ObIArray<const share::schema::ObObjPriv*>& obj_privs);

  int revoke_obj_cascade(share::schema::ObSchemaGetterGuard& schema_guard, const uint64_t start_grantee_id,
      common::ObMySQLTransaction& trans, const share::schema::ObObjPrivSortKey& old_key,
      share::ObRawObjPrivArray& old_array);

  int drop_obj_privs(
      const uint64_t tenant_id, const uint64_t obj_id, const uint64_t obj_ypte, common::ObMySQLTransaction& trans);

  int revoke_table_all(share::schema::ObSchemaGetterGuard& schema_guard, const uint64_t tenant_id,
      const share::schema::ObObjPrivSortKey& obj_priv_key, common::ObString& ddl_sql,
      common::ObMySQLTransaction& trans);
  int build_table_and_col_priv_array_for_revoke_all(share::schema::ObSchemaGetterGuard& schema_guard,
      const share::schema::ObObjPrivSortKey& obj_priv_key, share::ObPackedObjPriv& packed_table_priv,
      common::ObSEArray<uint64_t, 4>& col_id_array, common::ObSEArray<share::ObPackedObjPriv, 4>& packed_privs_array);
  int check_obj_privs_exists(share::schema::ObSchemaGetterGuard& schema_guard,
      const share::schema::ObObjPrivSortKey& obj_priv_key, /* in: obj priv key*/
      const share::ObRawObjPrivArray& obj_priv_array, share::ObRawObjPrivArray& option_priv_array, bool& is_all);

  int build_raw_priv_info_inner_user(uint64_t grantee_id, share::ObRawPrivArray& raw_priv_array, uint64_t& option);

  int init_inner_user_privs(const uint64_t tenant_id, share::schema::ObUserInfo& user,
      common::ObMySQLTransaction& trans, const bool is_oracle_mode);

  // Create a built-in default profile for Oracle tenants
  int init_tenant_profile(
      int64_t tenant_id, const share::schema::ObSysVariableSchema& sys_variable, common::ObMySQLTransaction& trans);
  int check_modify_column_when_upgrade(
      const share::schema::ObColumnSchemaV2& new_column, const share::schema::ObColumnSchemaV2& orig_column);

private:
  share::schema::ObMultiVersionSchemaService& schema_service_;
  common::ObMySQLProxy& sql_proxy_;
};

template <typename T>
int ObDDLOperator::construct_new_name_for_recyclebin(const T& schema, common::ObSqlString& new_object_name)
{
  int ret = common::OB_SUCCESS;
  new_object_name.reset();
  auto* tsi_value = GET_TSI(share::schema::TSIDDLVar);
  if (OB_ISNULL(tsi_value)) {
    ret = OB_ERR_UNEXPECTED;
    RS_LOG(WARN, "Failed to get TSIDDLVar", K(ret));
  } else {
    const common::ObString* ddl_id_str = tsi_value->ddl_id_str_;
    share::ObWorker::CompatMode compat_mode = share::ObWorker::CompatMode::INVALID;
    if (share::schema::ObSchemaService::g_liboblog_mode_) {
      // do nothing
    } else if (OB_FAIL(share::ObCompatModeGetter::get_tenant_mode(schema.get_tenant_id(), compat_mode))) {
      RS_LOG(WARN, "fail to get tenant mode", K(ret));
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(ddl_id_str)) {
        ret = new_object_name.append_fmt(
            (share::ObWorker::CompatMode::ORACLE == compat_mode) ? "RECYCLE_$_%lu_%ld" : "__recycle_$_%lu_%ld",
            GCONF.cluster_id.get_value(),
            schema.get_schema_version());
      } else if (is_aux_object(schema)) {
        // Requires special handling of global indexes
        if (is_global_index_object(schema)) {
          common::ObString index_name;
          if (OB_FAIL(dynamic_cast<const share::schema::ObTableSchema&>(schema).get_index_name(index_name))) {
            RS_LOG(WARN, "failed to get index_name", K(ret));
          } else {
            // Specify ddl id, use ddl id
            ret = new_object_name.append_fmt(
                (share::ObWorker::CompatMode::ORACLE == compat_mode) ? "RECYCLE_$_%.*s_%.*s" : "__recycle_$_%.*s_%.*s",
                ddl_id_str->length(),
                ddl_id_str->ptr(),
                index_name.length(),
                index_name.ptr());
          }
        } else {
          // indexes or VP tables only need the current schema version
          ret = new_object_name.append_fmt(
              (share::ObWorker::CompatMode::ORACLE == compat_mode) ? "RECYCLE_$_%lu_%ld" : "__recycle_$_%lu_%ld",
              GCONF.cluster_id.get_value(),
              schema.get_schema_version());
        }
      } else {
        // Specify ddl id, then use ddl id to generate object name.
        ret = new_object_name.append_fmt(
            (share::ObWorker::CompatMode::ORACLE == compat_mode) ? "RECYCLE_$_%.*s" : "__recycle_$_%.*s",
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

}  // end namespace rootserver
}  // end namespace oceanbase
#endif  // OCEANBASE_ROOTSERVER_OB_DDL_OPERATOR_H_
