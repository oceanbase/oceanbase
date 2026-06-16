/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_ROOTSERVER_OB_ALTER_TABLE_HELPER_H_
#define OCEANBASE_ROOTSERVER_OB_ALTER_TABLE_HELPER_H_

#include "rootserver/parallel_ddl/ob_ddl_helper.h"
#include "lib/container/ob_array.h"
#include "rootserver/ddl_task/ob_ddl_task.h"

namespace oceanbase
{
namespace obrpc
{
class ObAlterTableArg;
class ObAlterTableRes;
}
namespace rootserver
{

class ObAlterTableHelper;

enum ObAlterTableActionType
{
  INVALID_ACTION = 0,
  ADD_COLUMN_ACTION,
  DROP_COLUMN_ACTION,
  PARTITION_ACTION,
  MAX_ACTION
};

class ObIAlterTableAction
{
public:
  explicit ObIAlterTableAction(ObAlterTableHelper &helper) : helper_(helper) {}
  virtual ~ObIAlterTableAction() {}
  virtual int init() = 0;
  virtual int check() { return OB_SUCCESS; }
  // Object lock layer: actions enumerate ids into the helper's maps;
  // the helper itself is responsible for sort / flush.
  virtual int register_ddl_object_locks() { return common::OB_SUCCESS; }
  // Called after object locks are flushed. Actions call ObDDLLock helpers
  // directly (e.g. lock_for_online_drop_column_in_trans) here.
  virtual int lock_table_and_online_ddl() { return common::OB_SUCCESS; }
  virtual int check_legitimacy() { return common::OB_SUCCESS; }
  virtual int lock_objects() { return common::OB_SUCCESS; }
  virtual int generate_schemas() = 0;
  virtual int calc_schema_version_cnt() = 0;
  virtual int operate_schemas() = 0;
  virtual int before_commit() = 0;
  virtual int clean_on_fail() = 0;
  virtual int adjust_result() { return common::OB_SUCCESS; }
  virtual const char *get_action_name() const = 0;
  TO_STRING_KV("action_name", get_action_name());
protected:
  int check_inner_stat_() const;
  const obrpc::ObAlterTableArg &get_arg() const;
  obrpc::ObAlterTableRes &get_res();
  const share::schema::ObTableSchema *get_orig_table_schema() const;
  share::schema::ObTableSchema &get_new_table_schema();
  share::schema::ObSchemaGuardWrapper &get_schema_guard_wrapper();
  ObDDLSQLTransaction &get_trans();
  common::ObArenaAllocator &get_allocator();
  uint64_t get_tenant_id() const;
  common::ObIArray<ObDDLTaskRecord> &get_ddl_task_records();
  void add_schema_version_cnt(const int64_t cnt);
  share::schema::ObMultiVersionSchemaService *get_schema_service();
  common::ObMySQLProxy *get_sql_proxy();
  ObDDLService *get_ddl_service();
  int add_ddl_object_lock_by_id(
      const uint64_t lock_obj_id,
      const share::schema::ObSchemaType schema_type,
      const transaction::tablelock::ObTableLockMode lock_mode);
protected:
  ObAlterTableHelper &helper_;
};

class ObAlterTableHelper : public ObDDLHelper
{
public:
  ObAlterTableHelper(share::schema::ObMultiVersionSchemaService *schema_service,
                     const uint64_t tenant_id,
                     const obrpc::ObAlterTableArg &arg,
                     obrpc::ObAlterTableRes &res);
  virtual ~ObAlterTableHelper();
  TO_STRING_KV(K_(arg), K_(res), K_(action_types), K_(actions), KP_(orig_table_schema));

  const obrpc::ObAlterTableArg &get_arg() const { return arg_; }
  obrpc::ObAlterTableRes &get_res() { return res_; }
  const share::schema::ObTableSchema *get_orig_table_schema() const { return orig_table_schema_; }
  void set_orig_table_schema(const share::schema::ObTableSchema *schema) { orig_table_schema_ = schema; }
  share::schema::ObTableSchema &get_new_table_schema() { return new_table_schema_; }
  uint64_t get_tenant_id() const { return tenant_id_; }
  common::ObIArray<ObDDLTaskRecord> &get_ddl_task_records() { return ddl_task_records_; }
  const common::ObIArray<ObAlterTableActionType> &get_action_types() const { return action_types_; }
  void add_schema_version_cnt(const int64_t cnt) { schema_version_cnt_ += cnt; }
  // accessors for actions to use framework services
  share::schema::ObMultiVersionSchemaService *get_schema_service() { return schema_service_; }
  common::ObMySQLProxy *get_sql_proxy() { return sql_proxy_; }
  ObDDLService *get_ddl_service() { return ddl_service_; }
  ObDDLSQLTransaction &get_trans() { return get_trans_(); }
  share::schema::ObSchemaGuardWrapper &get_schema_guard_wrapper() { return schema_guard_wrapper_; }
  common::ObArenaAllocator &get_allocator() { return allocator_; }
  int check_inner_stat() { return ObDDLHelper::check_inner_stat_(); }
  int add_ddl_object_lock_by_id(
      const uint64_t lock_obj_id,
      const share::schema::ObSchemaType schema_type,
      const transaction::tablelock::ObTableLockMode lock_mode)
  { return add_lock_object_by_id_(lock_obj_id, schema_type, lock_mode); }
  // push objects whose schema was read pre-lock; framework verifies versions in check_after_lock_.
  int add_based_schema_object_info(const share::schema::ObBasedSchemaObjectInfo &info)
  { return action_based_schema_object_infos_.push_back(info); }
private:
  virtual int init_() override;
  virtual int lock_objects_() override;
  virtual int check_after_lock_() override;
  virtual int generate_schemas_() override;
  virtual int calc_schema_version_cnt_() override;
  virtual int operate_schemas_() override;
  virtual int operation_before_commit_() override;
  virtual int clean_on_fail_commit_() override;
  virtual int construct_and_adjust_result_(int &return_ret) override;

  int resolve_action_types_();
  int resolve_column_action_type_(common::ObArray<ObAlterTableActionType> &action_types);
  int resolve_partition_action_type_(common::ObArray<ObAlterTableActionType> &action_types);
  // Gate parallel DDL by a whitelist of allowed action types AND enforce that a
  // single ALTER stmt maps to exactly one action. This aligns with the serial
  // path constraint in ObDDLService::alter_table_column (ob_ddl_service.cpp),
  // which rejects DROP COLUMN coexisting with any other alter type in the same
  // statement. Keep this invariant unless the serial constraint is relaxed.
  int check_supported_action_types_();
  int build_actions_();
  int check_table_legitimacy_();
  int check_alter_table_column_common_gates_();
  int prepare_alter_table_arg_();
  int lock_table_and_online_ddl_();

  // Lock framework helpers.
  int fetch_orig_table_schema_();

private:
  const obrpc::ObAlterTableArg &arg_;
  obrpc::ObAlterTableRes &res_;
  common::ObArray<ObIAlterTableAction *> actions_;
  common::ObArray<ObAlterTableActionType> action_types_;
  const share::schema::ObTableSchema *orig_table_schema_;
  common::ObArray<share::schema::ObBasedSchemaObjectInfo> action_based_schema_object_infos_;
  share::schema::ObTableSchema new_table_schema_;
  common::ObArray<ObDDLTaskRecord> ddl_task_records_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObAlterTableHelper);
};

} // namespace rootserver
} // namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_OB_ALTER_TABLE_HELPER_H_
