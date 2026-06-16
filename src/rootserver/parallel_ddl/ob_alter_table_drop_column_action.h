/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_ROOTSERVER_OB_ALTER_TABLE_DROP_COLUMN_ACTION_H_
#define OCEANBASE_ROOTSERVER_OB_ALTER_TABLE_DROP_COLUMN_ACTION_H_

#include "rootserver/parallel_ddl/ob_alter_table_helper.h"
#include "lib/hash/ob_hashset.h"
#include "share/schema/ob_column_schema.h"
#include "share/schema/ob_dependency_info.h"

namespace oceanbase
{
namespace rootserver
{

class ObDDLOperator;

class ObAlterTableDropColumnAction : public ObIAlterTableAction
{
public:
  explicit ObAlterTableDropColumnAction(ObAlterTableHelper &helper)
    : ObIAlterTableAction(helper),
      drop_cols_id_arr_(),
      new_tbl_visible_cols_cnt_after_alter_(0),
      last_drop_column_id_(OB_INVALID_ID),
      update_column_name_set_(),
      affected_index_ids_(),
      drop_index_schema_version_cnt_(0),
      aux_column_update_schema_version_cnt_(0),
      sensitive_rule_schema_version_cnt_(0),
      drop_constraint_schema_version_cnt_(0),
      drop_sequence_schema_version_cnt_(0),
      rls_schema_version_cnt_(0),
      dep_objs_before_lock_(),
      dep_objs_(),
      dep_obj_schema_version_cnt_(0)
  {}
  virtual ~ObAlterTableDropColumnAction() { update_column_name_set_.destroy(); }
  virtual int init() override;
  virtual int register_ddl_object_locks() override;
  virtual int lock_table_and_online_ddl() override;
  virtual int check_legitimacy() override;
  virtual int generate_schemas() override;
  virtual int calc_schema_version_cnt() override;
  virtual int operate_schemas() override;
  virtual int before_commit() override { return common::OB_SUCCESS; }
  virtual int clean_on_fail() override { return common::OB_SUCCESS; }
  virtual int adjust_result() override;
  virtual const char *get_action_name() const override { return "drop_column_action"; }

private:
  // register_ddl_object_locks helpers (per-category enumerators)
  int register_main_table_lock_(const share::schema::ObTableSchema &table_schema);
  int register_index_locks_(const share::schema::ObTableSchema &table_schema);
  int register_search_def_child_locks_(const share::schema::ObTableSchema &index_schema);
  int register_lob_aux_locks_(const share::schema::ObTableSchema &table_schema);
  int register_trigger_locks_(const share::schema::ObTableSchema &table_schema);
  int register_identity_sequence_locks_(const share::schema::ObTableSchema &table_schema);
  int register_rls_locks_(const share::schema::ObTableSchema &table_schema);
  int register_table_audit_locks_(const share::schema::ObTableSchema &table_schema);
  int register_sensitive_rule_locks_(const share::schema::ObTableSchema &table_schema);
  int register_dep_view_locks_(const share::schema::ObTableSchema &table_schema);
  // check() sub-steps (formerly in generate_schemas)
  int check_can_drop_column_();
  int check_dep_objs_consistency_();
  int collect_drop_column_related_infos_();
  int collect_affected_index_infos_();
  // Per-column checks shared with the serial path by reusing ObDDLService methods
  // via friend access:
  //   - ObDDLService::check_alter_unused_column (same method the serial path calls)
  //   - ObDDLService::check_can_drop_column (v2 overload, same method the serial
  //     path's drop_column_online calls)
  // Runs after collect_drop_column_related_infos_() since it depends on
  // new_tbl_visible_cols_cnt_after_alter_ and last_drop_column_id_ populated there.
  int check_can_drop_columns_();
  int check_drop_column_with_index_in_mysql_mode_();
  int check_drop_column_constraints_and_fk_();
  int check_fk_related_table_ddl_(const share::schema::ObTableSchema &data_table_schema,
                                  const share::ObDDLType &ddl_type);
  // calc_schema_version_cnt sub-steps
  int calc_sequence_schema_version_cnt_();
  int calc_constraint_schema_version_cnt_();
  int calc_index_schema_version_cnt_();
  int calc_sensitive_rule_schema_version_cnt_();
  int calc_rls_schema_version_cnt_();
  int calc_dep_obj_schema_version_cnt_();

private:
  common::ObArray<uint64_t> drop_cols_id_arr_;
  int64_t new_tbl_visible_cols_cnt_after_alter_;
  int64_t last_drop_column_id_;
  common::hash::ObHashSet<share::schema::ObColumnNameHashWrapper> update_column_name_set_;
  common::ObArray<uint64_t> affected_index_ids_; // index IDs that contain at least one dropped column
  int64_t drop_index_schema_version_cnt_;        // 2 per affected index (rename + update_data_table_schema_version)
  int64_t aux_column_update_schema_version_cnt_; // 2 per (dropped_col, aux_table) hit
  int64_t sensitive_rule_schema_version_cnt_;     // per sensitive rule on the table
  int64_t drop_constraint_schema_version_cnt_;   // 1 per constraint dropped caused by drop column
  int64_t drop_sequence_schema_version_cnt_;    // per identity column: 1 + obj_priv_count + audit_count
  int64_t rls_schema_version_cnt_;              // RLS policy drop/create/update versions
  common::ObArray<std::pair<uint64_t, share::schema::ObObjectType>> dep_objs_before_lock_;
  common::ObArray<std::pair<uint64_t, share::schema::ObObjectType>> dep_objs_;
  int64_t dep_obj_schema_version_cnt_;          // 1 per non-INVALID VIEW dep obj

private:
  DISALLOW_COPY_AND_ASSIGN(ObAlterTableDropColumnAction);
};

} // namespace rootserver
} // namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_OB_ALTER_TABLE_DROP_COLUMN_ACTION_H_
