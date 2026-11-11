/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_OB_ALTER_TABLE_DDL_PLAN_H
#define OCEANBASE_SHARE_OB_ALTER_TABLE_DDL_PLAN_H

#include "lib/ob_define.h"
#include "share/ob_ddl_common.h"

namespace oceanbase
{
namespace common
{
class ObSqlString;
}
namespace obrpc
{
struct ObAlterTableArg;
}
namespace share
{
namespace schema
{
class ObTableSchema;
class AlterTableSchema;
}
enum ObAlterTableExecutionPath : uint8_t //FARM COMPAT WHITELIST
{
  ALTER_TABLE_PATH_INVALID = 0,
  ALTER_TABLE_PATH_IN_TRANS,
  ALTER_TABLE_PATH_OFFLINE,
  ALTER_TABLE_PATH_EXCHANGE_PARTITION,
};

enum ObAlterTableLockStrategy : uint8_t //FARM COMPAT WHITELIST
{
  ALTER_TABLE_LOCK_NOT_DECIDE = 0,
  ALTER_TABLE_LOCK_EXCLUSIVE,              // ObTableLockMode::EXCLUSIVE (0x1)
  ALTER_TABLE_LOCK_SHARE,                  // ObTableLockMode::SHARE (0x2)
  ALTER_TABLE_LOCK_SHARE_ROW_EXCLUSIVE,    // ObTableLockMode::SHARE_ROW_EXCLUSIVE (0x6)
  ALTER_TABLE_LOCK_ROW_EXCLUSIVE,          // ObTableLockMode::ROW_EXCLUSIVE (0x4)
  ALTER_TABLE_LOCK_ROW_SHARE,              // ObTableLockMode::ROW_SHARE (0x8)
};

enum ObAlterTableAsyncTaskFlag : uint64_t //FARM COMPAT WHITELIST
{
  ALTER_TABLE_ASYNC_TASK_NOT_DECIDE = 0,
  ALTER_TABLE_ASYNC_TASK_CREATE_INDEX = 1ULL << 0,
  ALTER_TABLE_ASYNC_TASK_CHECK_CONSTRAINT = 1ULL << 1,
  ALTER_TABLE_ASYNC_TASK_FK_VALIDATE = 1ULL << 2,
  ALTER_TABLE_ASYNC_TASK_SPLIT_PARTITION = 1ULL << 3,
  ALTER_TABLE_ASYNC_TASK_DROP_COL_AUX = 1ULL << 4,
  ALTER_TABLE_ASYNC_TASK_MODIFY_AUTOINC = 1ULL << 5,
};

// Analyze ALTER TABLE attributes and build the EXPLAIN message.
class ObAlterTableDDLPlan
{
public:
  enum ObDDLImpact : uint8_t //FARM COMPAT WHITELIST
  {
    DDL_IMPACT_UNKNOWN = 0,
    DDL_IMPACT_POSITIVE,
    DDL_IMPACT_NEGATIVE,
  };
  ObAlterTableDDLPlan();
  void reset();
  // --- explain prediction (build_plan / analyze_explain_attributes) ---
  void analyze_explain_attributes(const obrpc::ObAlterTableArg *alter_table_arg = nullptr,
                                  const schema::ObTableSchema *orig_table_schema = nullptr);
  bool is_offline_category() const;
  bool is_online_category() const;
  const char *get_explain_ddl_type_str() const;
  const char *get_explain_category_str() const;
  const char *get_explain_blocks_dml_str() const;
  const char *get_explain_algorithm_str() const;
  int build_explain_message(common::ObSqlString &message) const;
  OB_INLINE void set_async_task_flags(const uint64_t flag)
  {
    async_task_flags_ = flag;
  }
  OB_INLINE void set_explain_ddl_type(const ObDDLType ddl_type)
  {
    explain_ddl_type_ = ddl_type;
  }
  OB_INLINE void set_has_runtime_dependency(const bool has_runtime_dependency)
  {
    has_runtime_dependency_ = has_runtime_dependency;
  }
  OB_INLINE void set_ddl_need_retry_at_executor(const bool ddl_need_retry_at_executor)
  {
    ddl_need_retry_at_executor_ = ddl_need_retry_at_executor;
  }
  OB_INLINE ObDDLType get_execution_ddl_type() const
  {
    return execution_ddl_type_;
  }
  OB_INLINE ObAlterTableExecutionPath get_execution_path() const
  {
    return execution_path_;
  }
  OB_INLINE bool get_ddl_need_retry_at_executor() const
  {
    return ddl_need_retry_at_executor_;
  }

  // Assign attributes while building an ALTER TABLE plan.
  OB_INLINE void set_execution_path(const ObAlterTableExecutionPath path, const ObDDLType ddl_type)
  {
    execution_path_ = path;
    execution_ddl_type_ = ddl_type;
  }
  OB_INLINE void set_lock_strategy(const ObAlterTableLockStrategy strategy = ALTER_TABLE_LOCK_EXCLUSIVE)
  {
    if (ALTER_TABLE_LOCK_NOT_DECIDE == lock_strategy_ || strategy < lock_strategy_) {
      lock_strategy_ = strategy;
    }
  }
  TO_STRING_KV(K_(async_task_flags), K_(execution_ddl_type), K_(explain_ddl_type), K_(execution_path),
               K_(lock_strategy), K_(blocks_dml), K_(data_accessed),
               K_(table_rewritten), K_(ddl_need_retry_at_executor),
               K_(has_runtime_dependency));
private:
  uint64_t async_task_flags_;
  ObDDLType execution_ddl_type_;
  ObDDLType explain_ddl_type_;
  ObAlterTableExecutionPath execution_path_;
  ObAlterTableLockStrategy lock_strategy_;
  ObDDLImpact blocks_dml_;
  ObDDLImpact data_accessed_;
  ObDDLImpact table_rewritten_;
  bool ddl_need_retry_at_executor_;
  bool has_runtime_dependency_;
};

class ObAlterTableDDLHelper
{
public:
  // Returns true when alter_table_arg mixes more than one category of sub-operations:
  // column / index / partition / option / constraint / foreign key / mview / mlog /
  // convert-to-character / column-group / auto-partition-attr, or multiple distinct
  // index action types (e.g. add index + rename index) in a single statement.
  static bool is_hybrid(const obrpc::ObAlterTableArg &alter_table_arg);
  static bool has_multiple_index_action_types(const obrpc::ObAlterTableArg &alter_table_arg);
  static bool is_rename_column_only(
      const obrpc::ObAlterTableArg &alter_table_arg,
      const schema::ObTableSchema *orig_table_schema);
  static bool is_only_alter_table_option(const obrpc::ObAlterTableArg &alter_table_arg);
  static bool is_fk_validate_required(const obrpc::ObAlterTableArg &alter_table_arg);
  static bool has_single_validate_data_fk(const obrpc::ObAlterTableArg &alter_table_arg);
  static bool is_rename_and_need_table_lock(
      const schema::AlterTableSchema &alter_table_schema,
      const schema::ObTableSchema &orig_table_schema,
      const uint64_t tenant_data_version);
  static bool is_alter_merge_engine(
      const schema::AlterTableSchema &alter_table_schema,
      const schema::ObTableSchema &orig_table_schema,
      const uint64_t tenant_data_version);
  static bool is_change_to_compaction_ttl_table(
      const schema::AlterTableSchema &alter_table_schema,
      const schema::ObTableSchema &orig_table_schema);
  static bool need_exclusive_table_lock_for_alter_table(
      const obrpc::ObAlterTableArg &alter_table_arg,
      const schema::ObTableSchema &orig_table_schema,
      const uint64_t tenant_data_version);
  static bool need_exclusive_partition_lock_for_alter_table(
      const obrpc::ObAlterTableArg &alter_table_arg);
  static bool need_check_constraint_validity(const obrpc::ObAlterTableArg &alter_table_arg);
  static bool has_add_or_rebuild_index_in_alter_table(const obrpc::ObAlterTableArg &alter_table_arg);
  static bool need_alter_table_auto_increment_in_trans(
      const schema::AlterTableSchema &alter_table_schema,
      const uint64_t autoinc_column_id);
  // True when alter_table_auto_increment acquires table-level EXCLUSIVE lock.
  static bool need_exclusive_table_lock_for_reduce_auto_increment(
      const obrpc::ObAlterTableArg &alter_table_arg,
      const schema::ObTableSchema &orig_table_schema);
  // True when alter_table_auto_increment reads the main table (SELECT MAX).
  static bool need_read_main_table_for_modify_autoinc(
      const obrpc::ObAlterTableArg &alter_table_arg,
      const schema::ObTableSchema &orig_table_schema);
  static bool need_main_table_data_access_from_async_task(const uint64_t async_task_flags);
  static bool need_drop_column_aux_async_task(
      const obrpc::ObAlterTableArg &alter_table_arg,
      const schema::ObTableSchema &orig_table_schema);
  // True when creating rowkey doc/vid aux index acquires table-level EXCLUSIVE lock.
  static int need_exclusive_table_lock_for_rowkey_aux_index(
      const obrpc::ObAlterTableArg &alter_table_arg,
      const schema::ObTableSchema &orig_table_schema,
      bool &need_lock);
  static void refine_explain_ddl_type(
      const obrpc::ObAlterTableArg &alter_table_arg,
      const schema::ObTableSchema *orig_table_schema,
      ObDDLType execution_ddl_type,
      ObDDLType &explain_ddl_type);
  static int analyze_lock_strategy(
      const obrpc::ObAlterTableArg &alter_table_arg,
      const schema::ObTableSchema &orig_table_schema,
      const uint64_t tenant_data_version,
      ObAlterTableLockStrategy &lock_strategy);
  static int analyze_async_tasks(
      const obrpc::ObAlterTableArg &alter_table_arg,
      const schema::ObTableSchema &orig_table_schema,
      const ObAlterTableDDLPlan &plan,
      uint64_t &async_task_flags);
  static int analyze_runtime_dependency(
      const obrpc::ObAlterTableArg &alter_table_arg,
      const schema::ObTableSchema &orig_table_schema,
      bool &has_runtime_dependency);
  static int build_plan(
      obrpc::ObAlterTableArg &alter_table_arg,
      const uint64_t tenant_data_version,
      const schema::ObTableSchema *orig_table_schema,
      ObDDLType execution_ddl_type,
      const bool ddl_need_retry_at_executor,
      ObAlterTableDDLPlan &plan);
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_OB_ALTER_TABLE_DDL_PLAN_H
