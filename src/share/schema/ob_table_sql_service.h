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

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
}
namespace share
{
class ObDMLSqlSplicer;
namespace schema
{
struct ObSchemaOperation;
class ObTableSchema;
class ObColumnSchemaV2;

class ObTableSqlService : public ObDDLSqlService
{
public:
  explicit ObTableSqlService(ObSchemaService &schema_service)
    : ObDDLSqlService(schema_service), sql_proxy_(NULL)
    {}
  virtual ~ObTableSqlService() {}

  virtual int create_table(ObTableSchema &table,
                           common::ObISQLClient &sql_client,
                           const common::ObString *ddl_stmt_str = NULL,
                           const bool need_sync_schema_version = true,
                           const bool is_truncate_table = false);
  //update table option
  int update_table_options(common::ObISQLClient &sql_client,
                          const ObTableSchema &table_schema,
                          ObTableSchema &alter_table_schema,
                          share::schema::ObSchemaOperationType operation_type,
                          const common::ObString *ddl_stmt_str = NULL);
  int update_table_schema_version(common::ObISQLClient &sql_client,
                                  const ObTableSchema &table_schema,
                                  share::schema::ObSchemaOperationType operation_type,
                                  const common::ObString *ddl_stmt_st);

  //update schema version and max used_column_id
  int update_table_attribute(common::ObISQLClient &sql_client,
                             const ObTableSchema &new_table_schema,
                             const ObSchemaOperationType operation_type,
                             const bool update_object_status_ignore_version,
                             const common::ObString *ddl_stmt_str = nullptr);
  int update_partition_option(common::ObISQLClient &sql_client,
                              ObTableSchema &table,
                              const common::ObString *ddl_stmt_str = NULL);
  int update_partition_option(common::ObISQLClient &sql_client,
                              ObTableSchema &table,
                              const int64_t new_schema_version);
  int update_subpartition_option(common::ObISQLClient &sql_client,
                                 const ObTableSchema &table,
                                 const ObIArray<ObPartition*> &update_part_array);
  int update_all_part_for_subpart(ObISQLClient &sql_client,
                                  const ObTableSchema &table,
                                  const ObIArray<ObPartition*> &update_part_array);

  virtual int drop_table(const ObTableSchema &table_schema,
                         const int64_t new_schema_version,
                         common::ObISQLClient &sql_client,
                         const common::ObString *ddl_stmt_str,
                         const bool is_truncate_table,
                         const bool is_drop_db,
                         share::schema::ObSchemaGetterGuard *schema_guard,
                         share::schema::DropTableIdHashSet *drop_table_set);

  virtual int update_tablegroup(ObSchemaGetterGuard &schema_guard,
                                ObTableSchema &new_table_schema,
                                common::ObISQLClient &sql_client,
                                const common::ObString *ddl_stmt_str = NULL);
  virtual int log_core_operation(common::ObISQLClient &sql_client,
                                 const uint64_t tenant_id,
                                 const int64_t schema_version);

  int add_single_constraint(common::ObISQLClient &sql_client,
                            const ObConstraint &constraint,
                            const bool only_history,
                            const bool need_to_deal_with_cst_cols,
                            const bool do_cst_revise);

  //alter table add column
  int insert_single_column(common::ObISQLClient &sql_client,
                           const ObTableSchema &new_table_schema,
                           const ObColumnSchemaV2 &column_schema,
                           const bool record_ddl_operation);
  //alter table add constraint
  int insert_single_constraint(common::ObISQLClient &sql_client,
                           const ObTableSchema &new_table_schema,
                           const ObConstraint &constraint_schema);
  //alter table modify column change column alter column
  int update_single_column(common::ObISQLClient &sql_client,
                           const ObTableSchema &origin_table_schema,
                           const ObTableSchema &new_table_schema,
                           const ObColumnSchemaV2 &column_schema,
                           const bool record_ddl_operation);
  //alter table drop column
  int delete_single_column(const int64_t new_schema_version,
                           common::ObISQLClient &sql_client,
                           const ObTableSchema &table_schema,
                           const ObColumnSchemaV2 &column_schema);
  //alter table drop constraint
  int delete_single_constraint(const int64_t new_schema_version,
                               common::ObISQLClient &sql_client,
                               const ObTableSchema &table_schema,
                               const ObConstraint &constraint_schema);

  int revise_check_cst_column_info(
      common::ObISQLClient &sql_client,
      const ObTableSchema &table_schema,
      const ObIArray<ObConstraint> &csts);

  virtual int update_index_status(const ObTableSchema &data_table_schema,
                                  const uint64_t index_table_id,
                                  const ObIndexStatus status,
                                  const int64_t new_schema_version,
                                  common::ObISQLClient &sql_client,
                                  const common::ObString *ddl_stmt_str);

  // TODO: merge these two API
  int sync_aux_schema_version_for_history(common::ObISQLClient &sql_client,
                                          const ObTableSchema &index_schema1,
                                          const uint64_t new_schema_version);

  int add_inc_partition_info(common::ObISQLClient &sql_client,
                             const ObTableSchema &ori_table,
                             ObTableSchema &inc_table,
                             const int64_t schema_version,
                             bool is_truncate_table,
                             bool is_subpart);
  int add_inc_part_info(common::ObISQLClient &sql_client,
                        const ObTableSchema &ori_table,
                        const ObTableSchema &inc_table,
                        const int64_t schema_version,
                        bool is_truncate_table);
  int add_inc_subpart_info(common::ObISQLClient &sql_client,
                        const ObTableSchema &ori_table,
                        const ObTableSchema &inc_table,
                        const int64_t schema_version);
  int rename_inc_part_info(common::ObISQLClient &sql_client,
                           const ObTableSchema &ori_table,
                           const ObTableSchema &inc_table,
                           const int64_t schema_version);
  int rename_inc_subpart_info(common::ObISQLClient &sql_client,
                           const ObTableSchema &ori_table,
                           const ObTableSchema &inc_table,
                           const int64_t schema_version);

  int drop_inc_part_info(
      common::ObISQLClient &sql_client,
      const ObTableSchema &ori_table,
      const ObTableSchema &inc_table,
      const int64_t schema_version,
      bool is_truncate_partition,
      bool is_truncate_table);
  int drop_inc_subpart_info(
      common::ObISQLClient &sql_client,
      const ObTableSchema &ori_table,
      const ObTableSchema &inc_table,
      const int64_t schema_version);
  int drop_inc_partition_add_extra_str(const ObTableSchema &inc_table,
                                       ObSqlString &sql,
                                       ObSqlString &condition_str,
                                       ObSqlString &dml_info_cond_str);
  int drop_inc_partition(common::ObISQLClient &sql_client,
                         const ObTableSchema &ori_table,
                         const ObTableSchema &inc_table,
                         bool is_truncate_table);
  int drop_inc_sub_partition(common::ObISQLClient &sql_client,
                             const ObTableSchema &ori_table,
                             const ObTableSchema &inc_table);
  int drop_inc_all_sub_partition_add_extra_str(const ObTableSchema &inc_table,
                                               ObSqlString &sql);
  int drop_inc_all_sub_partition(common::ObISQLClient &sql_client,
                             const ObTableSchema &ori_table,
                             const ObTableSchema &inc_table,
                             bool is_truncate_table);
  int truncate_part_info(
      common::ObISQLClient &sql_client,
      const ObTableSchema &ori_table,
      ObTableSchema &inc_table,
      ObTableSchema &del_table,
      const int64_t schema_version);

  int truncate_subpart_info(
      common::ObISQLClient &sql_client,
      const ObTableSchema &ori_table,
      ObTableSchema &inc_table,
      ObTableSchema &del_table,
      const int64_t schema_version);

  int sync_schema_version_for_history(
      common::ObISQLClient &sql_client,
      ObTableSchema &schema,
      uint64_t new_schema_version);

  int update_data_table_schema_version(common::ObISQLClient &sql_client,
                                       const uint64_t tenant_id,
                                       const uint64_t data_table_id,
                                       const bool in_offline_ddl_white_list,
                                       int64_t new_schema_version = common::OB_INVALID_VERSION);
  int insert_ori_schema_version(
      common::ObISQLClient &sql_client,
      const uint64_t tenant_id,
      const uint64_t table_id,
      const int64_t &ori_schema_version);

  int drop_foreign_key(const int64_t new_schema_version,
                       common::ObISQLClient &sql_client,
                       const ObTableSchema &table,
                       const ObForeignKeyInfo *foreign_key_info,
                       const bool parent_table_in_offline_ddl_white_list);
  int add_foreign_key(common::ObISQLClient &sql_client,
                      const ObTableSchema &table,
                      const bool only_history);
  int update_foreign_key_columns(
      common::ObISQLClient &sql_client, const uint64_t tenant_id,
      const ObForeignKeyInfo &ori_foreign_key_info, const ObForeignKeyInfo &new_foreign_key_info,
      const int64_t new_schema_version_1,
      const int64_t new_schema_version_2);
  int add_foreign_key_columns(
      common::ObISQLClient &sql_client,
      const uint64_t tenant_id,
      const ObForeignKeyInfo &foreign_key_info,
      const int64_t new_schema_version,
      const bool only_history);
  int drop_foreign_key_columns(
      common::ObISQLClient &sql_client,
      const uint64_t tenant_id,
      const ObForeignKeyInfo &ori_foreign_key_info,
      const int64_t new_schema_version);
  int update_foreign_key_state(common::ObISQLClient &sql_client, const ObTableSchema &table);
  int update_check_constraint_state(
      common::ObISQLClient &sql_client,
      const ObTableSchema &table,
      const ObConstraint &cst);

private:

  int add_table(common::ObISQLClient &sql_client, const ObTableSchema &table,
                const bool update_object_status_ignore_version,
                const bool only_history = false);
  int delete_from_all_table(common::ObISQLClient &sql_client,
                            const uint64_t tenant_id,
                            const uint64_t table_id);
  int delete_from_all_table_stat(common::ObISQLClient &sql_client,
                                 const uint64_t tenant_id,
                                 const uint64_t table_id,
                                 ObSqlString *extra_condition = NULL);
  int delete_from_all_column_stat(common::ObISQLClient &sql_client,
                                  const uint64_t tenant_id,
                                  const uint64_t table_id,
                                  ObSqlString *extra_condition = NULL);
  int delete_from_all_histogram_stat(common::ObISQLClient &sql_client,
                                     const uint64_t tenant_id,
                                     const uint64_t table_id,
                                     ObSqlString *extra_condition = NULL);
  int delete_from_all_column(common::ObISQLClient &sql_client,
                             const uint64_t tenant_id,
                             const uint64_t table_id,
                             const int64_t column_count,
                             bool check_affect_rows = true);
  int delete_from_all_table_history(common::ObISQLClient &sql_client,
                                    const ObTableSchema &table_schema,
                                    const int64_t new_schema_version);
  int delete_from_all_column_history(common::ObISQLClient &sql_client,
                                     const ObTableSchema &table_schema,
                                     const int64_t new_schema_version);
  int delete_table_part_info(const ObTableSchema &table_schema,
                             const int64_t new_schema_version,
                             common::ObISQLClient &sql_client);
  int add_columns(common::ObISQLClient &sql_client, const ObTableSchema &table);
  int add_columns_for_core(common::ObISQLClient &sql_client, const ObTableSchema &table);
  int add_columns_for_not_core(common::ObISQLClient &sql_client, const ObTableSchema &table);
  int add_constraints(common::ObISQLClient &sql_client, const ObTableSchema &table);
  int add_constraints_for_not_core(common::ObISQLClient &sql_client, const ObTableSchema &table);
  // deal_whih_csts_when_drop_table_to_recyclebin
  int rename_csts_in_inner_table(common::ObISQLClient &sql_client,
                                 const ObTableSchema &table_schema,
                                 const int64_t new_schema_version);
  int delete_constraint(common::ObISQLClient &sql_client,
                        const ObTableSchema &table_schema,
                        const int64_t new_schema_version);
  int add_sequence(const uint64_t tenant_id,
                   const uint64_t table_id,
                   const uint64_t column_id,
                   const uint64_t auto_increment,
                   const int64_t truncate_version);
  int add_transition_point_val(share::ObDMLSqlSplicer &dml,
                               const ObTableSchema &table);
  int add_interval_range_val(share::ObDMLSqlSplicer &dml,
                               const ObTableSchema &table);
  int gen_table_dml(const uint64_t exec_tenant_id, const ObTableSchema &table,
                    const bool update_object_status_ignore_version, share::ObDMLSqlSplicer &dml);
  int gen_table_options_dml(const uint64_t exec_tenant_id,
                            const ObTableSchema &table,
                            const bool update_object_status_ignore_version,
                            share::ObDMLSqlSplicer &dml);
  int gen_column_dml(const uint64_t exec_tenant_id, const ObColumnSchemaV2 &column, share::ObDMLSqlSplicer &dml);
  int gen_constraint_dml(const uint64_t exec_tenant_id, const ObConstraint &constraint, share::ObDMLSqlSplicer &dml);
  int gen_constraint_column_dml(
      const uint64_t exec_tenant_id,
      const ObConstraint &constraint,
      uint64_t column_id, share::ObDMLSqlSplicer &dml);

  // modify constraint name in __all_constraint while drop table to recyclebin
  int gen_constraint_update_name_dml(const uint64_t exec_tenant_id,
                                     const ObString &cst_name,
                                     const ObNameGeneratedType name_generated_type,
                                     const int64_t new_schema_version,
                                     const ObConstraint &constraint,
                                     share::ObDMLSqlSplicer &dml);
  // modify constraint name in __all_constraint_history while drop table to recyclebin
  int gen_constraint_insert_new_name_row_dml(const uint64_t exec_tenant_id,
                                             const ObString &cst_name,
                                             const ObNameGeneratedType name_generated_type,
                                             const int64_t new_schema_version,
                                             const ObConstraint &constraint,
                                             share::ObDMLSqlSplicer &dml);
  int gen_partition_option_dml(const ObTableSchema &table, share::ObDMLSqlSplicer &dml);
  int add_column(common::ObISQLClient &sql_client, const ObTableSchema &table);

  int add_table_part_info(common::ObISQLClient &sql_client, const ObTableSchema &table);
  int delete_foreign_key(common::ObISQLClient &sql_client,
      const ObTableSchema &table_schema,
      const int64_t new_schema_version,
      const bool is_truncate_table);
  int delete_from_all_foreign_key(common::ObISQLClient &sql_client,
      const uint64_t tenant_id,
      const int64_t new_schema_version,
      const ObForeignKeyInfo &foreign_key_info);
  int delete_from_all_foreign_key_column(common::ObISQLClient &sql_client,
      const uint64_t tenant_id,
      const uint64_t foreign_key_id,
      const uint64_t child_column_id,
      const uint64_t parent_column_id,
      const uint64_t fk_column_pos,
      const int64_t new_schema_version);
  int gen_foreign_key_dml(const uint64_t exec_tenant_id,
                          uint64_t tenant_id,
                          const ObForeignKeyInfo &foreign_key_info,
                          share::ObDMLSqlSplicer &dml);
  int gen_foreign_key_column_dml(const uint64_t exec_tenant_id,
                                 uint64_t tenant_id, uint64_t foreign_key_id,
                                 uint64_t child_column_id, uint64_t parent_column_id,
                                 int64_t position, share::ObDMLSqlSplicer &dml);
  int check_table_options(const share::schema::ObTableSchema &table_schema);
  int add_single_column(common::ObISQLClient &sql_client, const ObColumnSchemaV2 &column,
                        const bool only_history = false);

  int delete_from_all_column_usage(ObISQLClient &sql_client,
                                   const uint64_t tenant_id,
                                   const uint64_t table_id);
  int delete_from_all_monitor_modified(ObISQLClient &sql_client,
                                       const uint64_t tenant_id,
                                       const uint64_t table_id,
                                       const ObSqlString *extra_condition = NULL);
  int delete_from_all_optstat_user_prefs(ObISQLClient &sql_client,
                                         const uint64_t tenant_id,
                                         const uint64_t table_id);

public:
  int insert_temp_table_info(common::ObISQLClient &trans, const ObTableSchema &table_schema);
  int delete_from_all_temp_table(common::ObISQLClient &sql_client,
                                 const uint64_t tenant_id,
                                 const uint64_t table_id);
private:
  int log_operation_wrapper(
      ObSchemaOperation &opt,
      common::ObISQLClient &sql_client);
  int exec_update(common::ObISQLClient &sql_client,
                  const uint64_t tenant_id,
                  const uint64_t table_id,
                  const char *table_name,
                  share::ObDMLSqlSplicer &dml,
                  int64_t &affected_rows);
  int exec_insert(common::ObISQLClient &sql_client,
                  const uint64_t tenant_id,
                  const uint64_t table_id,
                  const char *table_name,
                  share::ObDMLSqlSplicer &dml,
                  int64_t &affected_rows);
  int exec_delete(common::ObISQLClient &sql_client,
                  const uint64_t tenant_id,
                  const uint64_t table_id,
                  const char *table_name,
                  share::ObDMLSqlSplicer &dml,
                  int64_t &affected_rows);
  int supplement_for_core_table(common::ObISQLClient &sql_client,
                                const bool is_all_table,
                                const ObColumnSchemaV2 &column);
  bool is_user_partition_table(const ObTableSchema &table_schema);
  bool is_user_subpartition_table(const ObTableSchema &table);
  int check_ddl_allowed(const ObSimpleTableSchemaV2 &table_schema);

// MockFKParentTable begin
public:
  int add_mock_fk_parent_table(
      common::ObISQLClient *sql_client,
      const ObMockFKParentTableSchema &mock_fk_parent_table_schema,
      const bool need_update_foreign_key);
  int alter_mock_fk_parent_table(
      common::ObISQLClient *sql_client,
      ObMockFKParentTableSchema &mock_fk_parent_table_schema);
  int drop_mock_fk_parent_table(
      common::ObISQLClient *sql_client,
      const ObMockFKParentTableSchema &mock_fk_parent_table_schema);
  int replace_mock_fk_parent_table(
      common::ObISQLClient *sql_client,
      const ObMockFKParentTableSchema &mock_fk_parent_table_schema,
      const ObMockFKParentTableSchema *ori_mock_fk_parent_table_schema_ptr);
  int update_mock_fk_parent_table_schema_version(
      common::ObISQLClient *sql_client,
      ObMockFKParentTableSchema &mock_fk_parent_table_schema);
  int update_view_columns(ObISQLClient &sql_client,
                          const ObTableSchema &table);

private:
  int update_foreign_key_in_mock_fk_parent_table(
      common::ObISQLClient *sql_client,
      const ObMockFKParentTableSchema &new_mock_fk_parent_table_schema,
      const ObMockFKParentTableSchema *ori_mock_fk_parent_table_schema_ptr,
      const bool need_update_foreign_key_columns);
  int insert_mock_fk_parent_table(
      common::ObISQLClient &sql_client,
      const ObMockFKParentTableSchema &mock_fk_parent_table_schema,
      const bool only_history);
  int delete_mock_fk_parent_table(
      common::ObISQLClient &sql_client,
      const ObMockFKParentTableSchema &mock_fk_parent_table_schema,
      const bool only_history);
  int insert_mock_fk_parent_table_column(
      common::ObISQLClient &sql_client,
      const ObMockFKParentTableSchema &mock_fk_parent_table_schema,
      const bool only_history);
  int delete_mock_fk_parent_table_column(
      common::ObISQLClient &sql_client,
      const ObMockFKParentTableSchema &mock_fk_parent_table_schema,
      const bool only_history);
  int format_insert_mock_table_dml_sql(
      const ObMockFKParentTableSchema &mock_fk_parent_table_schema,
      ObDMLSqlSplicer &dml,
      bool &is_history);
  int format_delete_mock_table_dml_sql(
      const ObMockFKParentTableSchema &mock_fk_parent_table_schema,
      const bool is_history,
      ObSqlString &column_sql);
  int format_insert_mock_table_column_dml_sql(
      const ObMockFKParentTableSchema &mock_fk_parent_table_schema,
      const bool is_history,
      ObSqlString &column_sql);
  int format_delete_mock_table_column_dml_sql(
      const ObMockFKParentTableSchema &mock_fk_parent_table_schema,
      const bool is_history,
      ObSqlString &column_sql);
// MockFKParentTable end
//
  int check_table_history_matched_(
      ObISQLClient &sql_client,
      const uint64_t tenant_id,
      const uint64_t table_id,
      const int64_t schema_version);

public:
  void init(common::ObMySQLProxy *sql_proxy) { sql_proxy_ = sql_proxy; }
private:
  common::ObMySQLProxy *sql_proxy_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableSqlService);
};

} //end of namespace schema
} //end of namespace share
} //end of namespace oceanbase

#endif //OCEANBASE_SHARE_SCHEMA_OB_TABLE_SQL_SERVICE_H_
