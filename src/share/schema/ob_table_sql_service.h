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

#ifndef OCEANBASE_SHARE_SCHEMA_OB_TABLE_SQL_SERVICE_H_
#define OCEANBASE_SHARE_SCHEMA_OB_TABLE_SQL_SERVICE_H_

#include "ob_ddl_sql_service.h"

namespace oceanbase {
namespace common {
class ObMySQLProxy;
}
namespace obrpc {
class ObSplitPartitionArg;
}
namespace share {
class ObDMLSqlSplicer;
class ObSplitInfo;
namespace schema {
struct ObSchemaOperation;
class ObTableSchema;
class ObColumnSchemaV2;

class ObTableSqlService : public ObDDLSqlService {
public:
  explicit ObTableSqlService(ObSchemaService& schema_service) : ObDDLSqlService(schema_service), sql_proxy_(NULL)
  {}
  virtual ~ObTableSqlService()
  {}

  virtual int create_table(ObTableSchema& table, common::ObISQLClient& sql_client,
      const common::ObString* ddl_stmt_str = NULL, const bool need_sync_schema_version = true,
      const bool is_truncate_table = false);
  // update table option
  int update_table_options(common::ObISQLClient& sql_client, const ObTableSchema& table_schema,
      ObTableSchema& alter_table_schema, share::schema::ObSchemaOperationType operation_type,
      const common::ObString* ddl_stmt_str = NULL);
  int update_table_schema_version(common::ObISQLClient& sql_client, const ObTableSchema& table_schema,
      share::schema::ObSchemaOperationType operation_type, const common::ObString* ddl_stmt_st);

  // update schema version and max used_column_id
  int update_table_attribute(common::ObISQLClient& sql_client, ObTableSchema& new_table_schema,
      const ObSchemaOperationType operation_type, const common::ObString* ddl_stmt_str = NULL);
  int update_partition_option(
      common::ObISQLClient& sql_client, ObTableSchema& table, const common::ObString* ddl_stmt_str = NULL);
  int update_partition_option(common::ObISQLClient& sql_client, ObTableSchema& table, const int64_t new_schema_version);
  int update_partition_cnt_within_partition_table(common::ObISQLClient& sql_client, ObTableSchema& table);
  int update_max_used_part_id(common::ObISQLClient& sql_client, ObTableSchema& table);
  int update_subpartition_option(
      common::ObISQLClient& sql_client, const ObTableSchema& table, const ObIArray<ObPartition*>& update_part_array);
  int update_all_part_for_subpart(
      ObISQLClient& sql_client, const ObTableSchema& table, const ObIArray<ObPartition*>& update_part_array);
  int drop_table_for_inspection(
      ObISQLClient& sql_client, const share::schema::ObTableSchema& table_schema, const int64_t new_schema_version);

  virtual int drop_table(const ObTableSchema& table_schema, const int64_t new_schema_version,
      common::ObISQLClient& sql_client, const common::ObString* ddl_stmt_str = NULL,
      const bool is_truncate_table = false, const bool is_drop_db = false,
      share::schema::ObSchemaGetterGuard* schema_guard = NULL, share::schema::DropTableIdHashSet* drop_table_set = NULL,
      const bool is_delay_delete = false, const common::ObString* delay_deleted_name = NULL);

  virtual int update_tablegroup(ObSchemaGetterGuard& schema_guard, ObTableSchema& new_table_schema,
      common::ObISQLClient& sql_client, const common::ObString* ddl_stmt_str = NULL);
  virtual int log_core_operation(common::ObISQLClient& sql_client, const int64_t schema_version);

  int add_single_column(
      common::ObISQLClient& sql_client, const ObColumnSchemaV2& column, const bool only_history = false);
  int add_single_constraint(common::ObISQLClient& sql_client, const ObConstraint& constraint,
      const bool only_history = false, const bool need_to_deal_with_cst_cols = true);

  // alter table add column
  int insert_single_column(
      common::ObISQLClient& sql_client, const ObTableSchema& new_table_schema, const ObColumnSchemaV2& column_schema);
  // alter table add constraint
  int insert_single_constraint(
      common::ObISQLClient& sql_client, const ObTableSchema& new_table_schema, const ObConstraint& constraint_schema);
  // alter table modify column change column alter column
  int update_single_column(common::ObISQLClient& sql_client, const ObTableSchema& origin_table_schema,
      const ObTableSchema& new_table_schema, const ObColumnSchemaV2& column_schema);
  // alter table drop column
  int delete_single_column(const int64_t new_schema_version, common::ObISQLClient& sql_client,
      const ObTableSchema& table_schema, const ObColumnSchemaV2& column_schema);
  // alter table drop constraint
  int delete_single_constraint(const int64_t new_schema_version, common::ObISQLClient& sql_client,
      const ObTableSchema& table_schema, const ObConstraint& constraint_schema);

  virtual int update_index_status(const uint64_t data_table_id, const uint64_t index_table_id,
      const ObIndexStatus status, const int64_t create_mem_version, const int64_t new_schema_version,
      common::ObISQLClient& sql_client);

  // TODO: merge these two API
  int sync_aux_schema_version_for_history(
      common::ObISQLClient& sql_client, const ObTableSchema& index_schema1, const uint64_t new_schema_version);
  int split_part_info(common::ObISQLClient& sql_client, const ObPartitionSchema& new_schema,
      const int64_t schema_version, const share::ObSplitInfo& split_info, bool has_physical_partition = true);

  int add_inc_part_info(common::ObISQLClient& sql_client, const ObTableSchema& ori_table,
      const ObTableSchema& inc_table, const int64_t schema_version, bool is_truncate_table);
  int add_inc_subpart_info(common::ObISQLClient& sql_client, const ObTableSchema& ori_table,
      const ObTableSchema& inc_table, const int64_t schema_version);
  int drop_inc_part_info(common::ObISQLClient& sql_client, const ObTableSchema& ori_table,
      const ObTableSchema& inc_table, const int64_t schema_version, bool is_delay_delete, bool is_truncate_table);
  int drop_inc_subpart_info(common::ObISQLClient& sql_client, const ObTableSchema& ori_table,
      const ObTableSchema& inc_table, const int64_t schema_version, bool is_delay_delete);
  int drop_inc_partition(
      common::ObISQLClient& sql_client, const ObTableSchema& ori_table, const ObTableSchema& inc_table);
  int drop_inc_sub_partition(
      common::ObISQLClient& sql_client, const ObTableSchema& ori_table, const ObTableSchema& inc_table);
  int drop_inc_all_sub_partition(
      common::ObISQLClient& sql_client, const ObTableSchema& ori_table, const ObTableSchema& inc_table);
  int truncate_part_info(common::ObISQLClient& sql_client, const ObTableSchema& ori_table, ObTableSchema& inc_table,
      const int64_t schema_version, bool is_delay_delete);

  int truncate_subpart_info(common::ObISQLClient& sql_client, const ObTableSchema& ori_table, ObTableSchema& inc_table,
      const int64_t schema_version, bool is_delay_delete);

  int drop_part_info_for_inspection(ObISQLClient& sql_client, const ObTableSchema& table_schema,
      const ObTableSchema& inc_table_schema, const int64_t new_schema_version);

  int drop_subpart_info_for_inspection(ObISQLClient& sql_client, const ObTableSchema& table_schema,
      const ObTableSchema& inc_table_schema, const int64_t new_schema_version);

  int sync_schema_version_for_history(
      common::ObISQLClient& sql_client, ObTableSchema& schema, uint64_t new_schema_version);

  int update_data_table_schema_version(common::ObISQLClient& sql_client, const uint64_t data_table_id,
      int64_t new_schema_version = common::OB_INVALID_VERSION);
  int insert_ori_schema_version(
      common::ObISQLClient& sql_client, const uint64_t table_id, const int64_t& ori_schema_version);
  int remove_source_partition(common::ObISQLClient& client, const share::ObSplitInfo& split_info,
      const int64_t schema_version, const ObTablegroupSchema* tablegroup_schema, const ObTableSchema* table_schema,
      bool is_delay_delete, const ObArray<ObPartition>& split_part_array);
  int modify_dest_partition(common::ObISQLClient& client, const ObSimpleTableSchemaV2& table_schema,
      const obrpc::ObSplitPartitionArg& arg, const int64_t schema_version);

  int add_partition_key(common::ObISQLClient& client, const ObTableSchema& origin_table_schema,
      const ObTableSchema& table_schema, const int64_t schema_version);
  int batch_update_partition_option(common::ObISQLClient& client, const uint64_t tenant_id,
      const common::ObIArray<const ObTableSchema*>& table_schemas, const int64_t schema_version,
      const ObPartitionStatus status);

  int batch_modify_dest_partition(common::ObISQLClient& client, const int64_t tenant_id,
      const common::ObIArray<const ObTableSchema*>& table_schemas, const int64_t schema_version);
  int batch_update_max_used_part_id(common::ObISQLClient& trans, const int64_t tenant_id, const int64_t schema_version,
      const common::ObIArray<const ObTableSchema*>& table_schemas, const ObTablegroupSchema& tablegroup_schema);
  int drop_foreign_key(const int64_t new_schema_version, common::ObISQLClient& sql_client, const ObTableSchema& table,
      const common::ObString& foreign_key_name);
  int add_foreign_key(common::ObISQLClient& sql_client, const ObTableSchema& table, const bool only_history = false);
  int update_foreign_key(common::ObISQLClient& sql_client, const ObTableSchema& table);
  int update_check_constraint_state(
      common::ObISQLClient& sql_client, const ObTableSchema& table, const ObConstraint& cst);

private:
  int add_table(common::ObISQLClient& sql_client, const ObTableSchema& table, const bool only_history = false);
  int delete_from_all_table(common::ObISQLClient& sql_client, const uint64_t table_id);
  int delete_from_all_table_stat(common::ObISQLClient& sql_client, const uint64_t table_id);
  int delete_from_all_column_stat(
      common::ObISQLClient& sql_client, uint64_t table_id, int64_t column_count, bool check_affect_rows);
  int delete_from_all_histogram_stat(common::ObISQLClient& sql_client, const uint64_t table_id);
  int delete_from_all_column(common::ObISQLClient& sql_client, const uint64_t table_id, const int64_t column_count,
      bool check_affect_rows = true);
  int delete_from_all_table_history(
      common::ObISQLClient& sql_client, const ObTableSchema& table_schema, const int64_t new_schema_version);
  int delete_from_all_column_history(
      common::ObISQLClient& sql_client, const ObTableSchema& table_schema, const int64_t new_schema_version);
  int delete_table_part_info(const ObTableSchema& table_schema, const int64_t new_schema_version,
      common::ObISQLClient& sql_client, bool is_delay_delete);
  int add_columns(common::ObISQLClient& sql_client, const ObTableSchema& table);
  int add_columns_for_core(common::ObISQLClient& sql_client, const ObTableSchema& table);
  int add_columns_for_not_core(common::ObISQLClient& sql_client, const ObTableSchema& table);

  int add_constraints(common::ObISQLClient& sql_client, const ObTableSchema& table);
  int add_constraints_for_not_core(common::ObISQLClient& sql_client, const ObTableSchema& table);
  // deal_whih_csts_when_drop_table_to_recyclebin
  int rename_csts_in_inner_table(
      common::ObISQLClient& sql_client, const ObTableSchema& table_schema, const int64_t new_schema_version);
  int delete_constraint(
      common::ObISQLClient& sql_client, const ObTableSchema& table_schema, const int64_t new_schema_version);
  int delete_from_all_constraint(common::ObISQLClient& sql_client, const uint64_t tenant_id,
      const int64_t new_schema_version, const ObConstraint& cst);
  int delete_from_all_constraint_column(common::ObISQLClient& sql_client, const uint64_t tenant_id,
      const int64_t new_schema_version, const ObConstraint& cst, const uint64_t column_id);
  int add_sequence(
      const uint64_t tenant_id, const uint64_t table_id, const uint64_t column_id, const uint64_t auto_increment);
  int gen_table_dml(const uint64_t exec_tenant_id, const ObTableSchema& table, share::ObDMLSqlSplicer& dml);
  int gen_table_options_dml(const uint64_t exec_tenant_id, const ObTableSchema& table, share::ObDMLSqlSplicer& dml);
  int gen_column_dml(const uint64_t exec_tenant_id, const ObColumnSchemaV2& column, share::ObDMLSqlSplicer& dml);
  int gen_constraint_dml(const uint64_t exec_tenant_id, const ObConstraint& constraint, share::ObDMLSqlSplicer& dml);
  int gen_constraint_column_dml(
      const uint64_t exec_tenant_id, const ObConstraint& constraint, uint64_t column_id, share::ObDMLSqlSplicer& dml);

  // modify constraint name in __all_constraint while drop table to recyclebin
  int gen_constraint_update_name_dml(const uint64_t exec_tenant_id, const ObString& cst_name,
      const int64_t new_schema_version, const ObConstraint& constraint, share::ObDMLSqlSplicer& dml);
  // modify constraint name in __all_constraint_history while drop table to recyclebin
  int gen_constraint_insert_new_name_row_dml(const uint64_t exec_tenant_id, const ObString& cst_name,
      const int64_t new_schema_version, const ObConstraint& constraint, share::ObDMLSqlSplicer& dml);
  int gen_partition_option_dml(const ObTableSchema& table, share::ObDMLSqlSplicer& dml);
  int add_column(common::ObISQLClient& sql_client, const ObTableSchema& table);

  int add_table_part_info(common::ObISQLClient& sql_client, const ObTableSchema& table);
  int add_split_partition_for_gc(common::ObISQLClient& sql_client, const ObPartitionSchema& table);
  int delete_foreign_key(
      common::ObISQLClient& sql_client, const ObTableSchema& table_schema, const int64_t new_schema_version);
  int delete_from_all_foreign_key(common::ObISQLClient& sql_client, const uint64_t tenant_id,
      const int64_t new_schema_version, const ObForeignKeyInfo& foreign_key_info);
  int delete_from_all_foreign_key_column(common::ObISQLClient& sql_client, const uint64_t tenant_id,
      const uint64_t foreign_key_id, const uint64_t child_column_id, const uint64_t parent_column_id,
      const int64_t new_schema_version);
  int gen_foreign_key_dml(const uint64_t exec_tenant_id, uint64_t tenant_id, const ObForeignKeyInfo& foreign_key_info,
      share::ObDMLSqlSplicer& dml);
  int gen_foreign_key_column_dml(const uint64_t exec_tenant_id, uint64_t tenant_id, uint64_t foreign_key_id,
      uint64_t child_column_id, uint64_t parent_column_id, int64_t position, share::ObDMLSqlSplicer& dml);
  int check_table_options(const share::schema::ObTableSchema& table_schema);

public:
  int log_operation_wrapper(ObSchemaOperation& opt, common::ObISQLClient& sql_client);
  int insert_temp_table_info(common::ObISQLClient& trans, const ObTableSchema& table_schema);
  int delete_from_all_temp_table(common::ObISQLClient& sql_client, const uint64_t tenant_id, const uint64_t table_id);

private:
  int exec_update(common::ObISQLClient& sql_client, const uint64_t table_id, const char* table_name,
      share::ObDMLSqlSplicer& dml, int64_t& affected_rows);
  int exec_insert(common::ObISQLClient& sql_client, const uint64_t table_id, const char* table_name,
      share::ObDMLSqlSplicer& dml, int64_t& affected_rows);
  int exec_delete(common::ObISQLClient& sql_client, const uint64_t table_id, const char* table_name,
      share::ObDMLSqlSplicer& dml, int64_t& affected_rows);
  int supplement_for_core_table(
      common::ObISQLClient& sql_client, const bool is_all_table, const ObColumnSchemaV2& column);
  bool is_user_partition_table(const ObTableSchema& table_schema);
  bool is_user_subpartition_table(const ObTableSchema& table);

public:
  void init(common::ObMySQLProxy* sql_proxy)
  {
    sql_proxy_ = sql_proxy;
  }

private:
  common::ObMySQLProxy* sql_proxy_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableSqlService);
};

}  // end of namespace schema
}  // end of namespace share
}  // end of namespace oceanbase

#endif  // OCEANBASE_SHARE_SCHEMA_OB_TABLE_SQL_SERVICE_H_
