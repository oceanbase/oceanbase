/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_ROOTSERVER_OB_ALTER_TABLE_ADD_COLUMN_ACTION_H_
#define OCEANBASE_ROOTSERVER_OB_ALTER_TABLE_ADD_COLUMN_ACTION_H_

#include "rootserver/parallel_ddl/ob_alter_table_column_action.h"
#include "share/ob_ddl_common.h"
#include "share/schema/ob_dependency_info.h"

namespace oceanbase
{
namespace rootserver
{

class ObDDLOperator;

class ObAlterTableAddColumnAction : public ObAlterTableColumnAction
{
public:
  explicit ObAlterTableAddColumnAction(ObAlterTableHelper &helper)
    : ObAlterTableColumnAction(helper),
      ddl_type_(share::ObDDLType::DDL_INVALID),
      is_oracle_mode_(false),
      orig_table_has_lob_column_(false),
      has_add_lob_column_(false),
      curr_udt_set_id_(0),
      gen_col_expr_arr_(),
      local_schema_guard_(),
      aux_table_ids_for_round_(),
      dep_objs_before_lock_(),
      dep_objs_(),
      dep_obj_schema_version_cnt_(0)
  {}
  virtual ~ObAlterTableAddColumnAction() {}
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
  virtual const char *get_action_name() const override { return "add_column_action"; }

private:
  // register_ddl_object_locks helpers (per-category).
  // Add column modifies the main table, auxiliary tables whose round changes, and
  // dependent views whose status is invalidated by the serial path.
  int register_main_table_lock_(const share::schema::ObTableSchema &table_schema);
  int register_aux_table_locks_(const share::schema::ObTableSchema &table_schema);
  int register_dep_view_locks_(const share::schema::ObTableSchema &table_schema);

  // check_legitimacy sub-steps.
  int check_dep_objs_consistency_();
  // collect_add_column_infos_: detect oracle mode, lob-column presence,
  // duplicate column names. Same-name ADD/DROP is already gated by executor /
  // schema utils, so the duplicate check here is defensive.
  int collect_add_column_infos_();
  int init_local_schema_guard_for_default_value_();

  int calc_main_column_schema_version_cnt_(int64_t &cnt) const;
  int calc_column_group_added_column_cnt_(int64_t &cnt) const;
  int calc_column_group_schema_version_cnt_(int64_t &cnt) const;
  int calc_lob_aux_schema_version_cnt_(int64_t &cnt) const;
  int calc_dep_obj_schema_version_cnt_();
  int calc_table_attribute_schema_version_cnt_(int64_t &cnt) const;

  int persist_added_columns_(ObDDLOperator &ddl_operator);
  int sync_aux_table_progressive_merge_round_(ObDDLOperator &ddl_operator);
  int modify_dep_obj_status_(ObDDLOperator &ddl_operator);

private:
  share::ObDDLType ddl_type_;
  bool is_oracle_mode_;
  bool orig_table_has_lob_column_;
  bool has_add_lob_column_;
  uint64_t curr_udt_set_id_;
  common::ObSEArray<common::ObString, 4> gen_col_expr_arr_;
  share::schema::ObSchemaGetterGuard local_schema_guard_;
  common::ObSEArray<uint64_t, 4> aux_table_ids_for_round_;
  common::ObSEArray<std::pair<uint64_t, share::schema::ObObjectType>, 4> dep_objs_before_lock_;
  common::ObSEArray<std::pair<uint64_t, share::schema::ObObjectType>, 4> dep_objs_;
  int64_t dep_obj_schema_version_cnt_;

  DISALLOW_COPY_AND_ASSIGN(ObAlterTableAddColumnAction);
};

} // namespace rootserver
} // namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_OB_ALTER_TABLE_ADD_COLUMN_ACTION_H_
