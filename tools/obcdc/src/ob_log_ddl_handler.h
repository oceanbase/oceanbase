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

#ifndef OCEANBASE_LIBOBLOG_OB_LOG_DDL_HANDLER_H__
#define OCEANBASE_LIBOBLOG_OB_LOG_DDL_HANDLER_H__

#include "lib/lock/ob_small_spin_lock.h"          // ObByteLock
#include "lib/utility/ob_macro_utils.h"           // CACHE_ALIGNED, DISALLOW_COPY_AND_ASSIGN
#include "common/ob_queue_thread.h"               // ObCond
#include <LogRecord.h>                            // ILogRecord

#include "ob_log_utils.h"                         // _SEC_
#include "ob_log_part_trans_task.h"               // PartTransTask
#include "ob_log_part_trans_task_queue.h"         // SafePartTransTaskQueue

namespace oceanbase
{
namespace liboblog
{
class IObLogDDLHandler
{
public:
  virtual ~IObLogDDLHandler() {}

public:
  virtual int start() = 0;
  virtual void stop() = 0;
  virtual void mark_stop_flag() = 0;
  virtual int push(PartTransTask *task, const int64_t timeout) = 0;

  /// Get the processing progress, note that it is not the output progress, a DDL task can update the progress after the processing is completed
  ////
  /// 1. If there is a backlog of data to be processed in the queue, return the timestamp of the next pending task - 1 as the processing progress
  /// 2. If there is no backlog in the queue, return OB_INVALID_PROGRESS
  ////
  /// @param [out] ddl_min_progress_key   the partition key of the slowest ddl processing progress corresponding to the tenant __all_ddl_operation table
  /// @param [out] ddl_min_progress       slowest ddl_progress
  /// @param [out] ddl_min_handle_log_id  slowest ddl processing log ID
  ///
  /// @retval OB_SUCCESS                  success
  /// @retval other error code            fail
  virtual int get_progress(uint64_t &ddl_min_progress_tenant_id,
      int64_t &ddl_min_progress,
      uint64_t &ddl_min_handle_log_id) = 0;

  virtual int64_t get_part_trans_task_count() const = 0;
};

///////////////////////////////// ObLogDDLHandler /////////////////////////////////

class IObLogDdlParser;
class IObLogErrHandler;
class IObLogSequencer;
class IObLogSchemaGetter;
class IStmtTask;
class PartTransTask;
class DdlStmtTask;
class ObLogSchemaGuard;
class IObLogTenantMgr;
class ObLogTenant;
class ObLogTenantGuard;
class ObLogDDLHandler : public IObLogDDLHandler
{
  enum
  {
    DATA_OP_TIMEOUT = 1 * _SEC_,
  };

public:
  ObLogDDLHandler();
  virtual ~ObLogDDLHandler();

public:
  int start();
  void stop();
  void mark_stop_flag() { stop_flag_ = true; }
  int push(PartTransTask *task, const int64_t timeout);
  virtual int get_progress(uint64_t &ddl_min_progress_tenant_id,
      int64_t &ddl_min_progress,
      uint64_t &ddl_min_handle_log_id);
  virtual int64_t get_part_trans_task_count() const;

public:
  int init(IObLogDdlParser *ddl_parser,
      IObLogSequencer *committer,
      IObLogErrHandler *err_handler,
      IObLogSchemaGetter *schema_getter,
      const bool skip_reversed_schema_version);
  void destroy();

public:
  void handle_ddl_routine();

private:
  static void *handle_thread_func_(void *arg);
  void mark_stmt_binlog_record_invalid_(DdlStmtTask &stmt_task);
  void mark_all_binlog_records_invalid_(PartTransTask &task);
  int get_old_schema_version_(const uint64_t tenant_id,
      PartTransTask &task,
      const int64_t tenant_ddl_cur_schema_version,
      int64_t &old_schema_version);
  int get_schema_version_by_timestamp_util_succ_(const uint64_t tenant_id,
      const int64_t ddl_schema_version,
      int64_t &old_schema_version);
  int filter_ddl_stmt_(ObLogTenant &tenant,
      DdlStmtTask &ddl_stmt,
      IObLogTenantMgr &tenant_mgr,
      bool &chosen,
      const bool is_filter_by_tenant_id = false);
  int handle_ddl_trans_(PartTransTask &task,
      bool &is_schema_split_mode,
      ObLogTenant &tenant);
  int handle_tenant_ddl_task_(PartTransTask &task,
      bool &is_schema_split_mode,
      ObLogTenant &tenant);
  int handle_ddl_stmt_(ObLogTenant &tenant,
      PartTransTask &task,
      DdlStmtTask &ddl_stmt,
      const int64_t old_schema_version,
      const int64_t new_schema_version,
      bool &is_schema_split_mode);
  int handle_ddl_stmt_direct_output_(ObLogTenant &tenant,
      DdlStmtTask &ddl_stmt,
      const int64_t schema_version);
  int commit_ddl_stmt_(ObLogTenant &tenant,
      DdlStmtTask &ddl_stmt,
      const int64_t schema_version,
      const char *tenant_name = NULL,
      const char *db_name = NULL,
      const bool filter_ddl_stmt = false);
  int get_schemas_for_ddl_stmt_(const uint64_t ddl_tenant_id,
      DdlStmtTask &ddl_stmt,
      const int64_t schema_version,
      ObLogSchemaGuard &schema_guard,
      const char *&tenant_name,
      const char *&db_name);
  bool is_use_new_schema_version(DdlStmtTask &ddl_stmt,
      const int64_t schema_version);
  int set_binlog_record_db_name_(ILogRecord &br,
      const int64_t ddl_operation_type,
      const char * const tenant_name,
      const char * const db_name);
  int handle_ddl_stmt_drop_table_(ObLogTenant &tenant,
      DdlStmtTask &ddl_stmt,
      const int64_t old_schema,
      const int64_t new_schema);
  int handle_ddl_stmt_drop_table_to_recyclebin_(ObLogTenant &tenant,
      DdlStmtTask &ddl_stmt,
      const int64_t old_schema,
      const int64_t new_schema);
  // alter table DDL
  // Currently only dynamic partitioning is supported. No longer maintain support for updating a table's db_id, to avoid db_id dependency
  // Currently two ways to modify table's db_id, move a table to another db
  // 1. alter table
  // 2. rename table
  int handle_ddl_stmt_alter_table_(ObLogTenant &tenant,
      DdlStmtTask &ddl_stmt,
      const int64_t old_schema,
      const int64_t new_schema,
      const char *event);
  int handle_ddl_stmt_create_table_(ObLogTenant &tenant,
      DdlStmtTask &ddl_stmt,
      const int64_t new_schema);
  int handle_ddl_stmt_rename_table_(ObLogTenant &tenant,
      DdlStmtTask &ddl_stmt,
      const int64_t old_schema,
      const int64_t new_schema);
  // Support global index, unique index
  int handle_ddl_stmt_create_index_(ObLogTenant &tenant,
      DdlStmtTask &ddl_stmt,
      const int64_t new_schema);
  // Support global index, unique index
  int handle_ddl_stmt_drop_index_(ObLogTenant &tenant,
      DdlStmtTask &ddl_stmt,
      const int64_t old_schema,
      const int64_t new_schema);
  int handle_ddl_stmt_add_tablegroup_partition_(ObLogTenant &tenant,
      DdlStmtTask &ddl_stmt,
      const int64_t new_schema);
  int handle_ddl_stmt_drop_tablegroup_partition_(ObLogTenant &tenant,
      DdlStmtTask &ddl_stmt,
      const int64_t old_schema,
      const int64_t new_schema);
  int handle_ddl_stmt_split_tablegroup_partition_(ObLogTenant &tenant,
      DdlStmtTask &ddl_stmt,
      const int64_t new_schema);
  int handle_ddl_stmt_change_tablegroup_(ObLogTenant &tenant,
      DdlStmtTask &ddl_stmt,
      const int64_t old_schema,
      const int64_t new_schema);
  int handle_ddl_stmt_alter_tablegroup_partition_(ObLogTenant &tenant,
      DdlStmtTask &ddl_stmt,
      const int64_t old_schema,
      const int64_t new_schema);
  int handle_ddl_stmt_truncate_table_drop_(ObLogTenant &tenant,
      DdlStmtTask &ddl_stmt,
      const int64_t old_schema,
      const int64_t new_schema);
  int handle_ddl_stmt_truncate_drop_table_to_recyclebin_(ObLogTenant &tenant,
      DdlStmtTask &ddl_stmt,
      const int64_t old_schema,
      const int64_t new_schema);
  int handle_ddl_stmt_truncate_table_create_(ObLogTenant &tenant,
      DdlStmtTask &ddl_stmt,
      const int64_t new_schema);
  int handle_ddl_stmt_drop_index_to_recyclebin_(ObLogTenant &tenant,
      DdlStmtTask &ddl_stmt,
      const int64_t old_schema,
      const int64_t new_schema);
  int handle_ddl_stmt_add_tenant_(ObLogTenant &tenant,
      DdlStmtTask &ddl_stmt,
      const int64_t new_schema);
  int handle_ddl_stmt_drop_tenant_(ObLogTenant &tenant,
      DdlStmtTask &ddl_stmt,
      const int64_t old_schema,
      const int64_t new_schema,
      const bool is_schema_split_mode = false,
      const bool is_del_tenant_start_op = false);
  int handle_ddl_stmt_alter_tenant_(ObLogTenant &tenant,
      DdlStmtTask &ddl_stmt,
      const int64_t old_schema_version,
      const int64_t new_schema_version);
  int handle_ddl_stmt_rename_tenant_(ObLogTenant &tenant,
      DdlStmtTask &ddl_stmt,
      const int64_t old_schema_version,
      const int64_t new_schema_version);
  int handle_ddl_stmt_drop_tenant_to_recyclebin_(ObLogTenant &tenant,
      DdlStmtTask &ddl_stmt,
      const int64_t old_schema,
      const int64_t new_schema);
  int handle_ddl_stmt_alter_database_(ObLogTenant &tenant,
      DdlStmtTask &ddl_stmt,
      const int64_t old_schema,
      const int64_t new_schema);
  int handle_ddl_stmt_drop_database_(ObLogTenant &tenant,
      DdlStmtTask &ddl_stmt,
      const int64_t old_schema,
      const int64_t new_schema);
  int handle_ddl_stmt_drop_database_to_recyclebin_(ObLogTenant &tenant,
      DdlStmtTask &ddl_stmt,
      const int64_t old_schema,
      const int64_t new_schema);
  int handle_ddl_stmt_rename_database_(ObLogTenant &tenant,
      DdlStmtTask &ddl_stmt,
      const int64_t old_schema,
      const int64_t new_schema);
  int handle_ddl_stmt_split_begin_(ObLogTenant &tenant,
      DdlStmtTask &ddl_stmt,
      const int64_t new_schema_version);
  int handle_ddl_stmt_finish_schema_split_(ObLogTenant &tenant,
      DdlStmtTask &ddl_stmt,
      const int64_t new_schema_version,
      bool &is_schema_split_mode);
  int64_t get_start_serve_timestamp_(const int64_t new_schema_version,
      const int64_t prepare_log_timestamp);
  int decide_ddl_tenant_id_(const PartTransTask &task,
      const bool is_schema_split_mode,
      uint64_t &ddl_tenant_id);
  int get_tenant_(
      PartTransTask &task,
      const bool is_schema_split_mode,
      uint64_t &ddl_tenant_id,
      ObLogTenantGuard &guard,
      ObLogTenant *&tenant,
      bool &is_tenant_served);
  int update_ddl_info_(PartTransTask &task,
      const bool is_schema_split_mode,
      ObLogTenant &tenant);
  int get_lazy_schema_guard_(const uint64_t tenant_id,
      const int64_t version,
      ObLogSchemaGuard &schema_guard);
  int decide_ddl_stmt_database_id_(DdlStmtTask &ddl_stmt,
      const int64_t schema_version,
      ObLogSchemaGuard &schema_guard,
      uint64_t &db_id);
  template <typename Func>
  int for_each_tenant_(Func &func);
  int handle_ddl_offline_task_(const PartTransTask &task);
  int next_task_(PartTransTask *&task);
  int handle_task_(PartTransTask &task,
      bool &is_schema_split_mode,
      const uint64_t ddl_tenant_id,
      ObLogTenant *tenant,
      const bool is_tenant_served);
  int dispatch_task_(PartTransTask *task, ObLogTenant *tenant, const bool is_tenant_served);
  // Parsing ddl stmt at the moment of recovery completion of backup recovery tenant
  // format: schema_version=${schema_version};teannt_gts=${tenant_gts}
  // tenant can be treated created from restore if ddl_stmt of create_tenant_end contains key_schema_version(schema_version) and key_tenant_gts(tenant_gts)
  // if error while parsing ddl_stmt_str, this method will return OB_ERR_UNEXPECTED, otherwise return is is_create_tenant_by_restore_ddl
  // if not create by restore, value of is_create_tenant_by_restore_ddl and tenant_gts_value won't change
  int parse_tenant_ddl_stmt_for_restore_(DdlStmtTask &ddl_stmt, int64_t &tenant_schema_version,
                                        int64_t &tenant_gts_value, bool &is_create_tenant_by_restore_ddl);
  int64_t decide_ddl_tenant_id_for_schema_non_split_mode_(const PartTransTask &task) const;

public:
  // Task queue
  // Supports single-threaded production and multi-threaded consumption with lock protection
  struct TaskQueue
  {
  public:
    TaskQueue();
    ~TaskQueue();

    int push(PartTransTask *task);
    void pop();
    int64_t size() const;

    // Wait for top to be ready to process
    // If the top task is ready, return OB_SUCCESS and return top_task
    int next_ready_to_handle(const int64_t timeout, PartTransTask *&top_task, common::ObCond &cond);

  private:
    SafePartTransTaskQueue  queue_;
  };

private:
  bool                inited_;
  IObLogDdlParser     *ddl_parser_;
  IObLogSequencer     *sequencer_;
  IObLogErrHandler    *err_handler_;
  IObLogSchemaGetter  *schema_getter_;

  bool                skip_reversed_schema_version_;

  // thread id of ddl handler
  pthread_t           handle_pid_;
  volatile bool       stop_flag_ CACHE_ALIGNED;

  // Queue of pending tasks exported from Fetcher
  TaskQueue           ddl_fetch_queue_;
  common::ObCond      wait_formatted_cond_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogDDLHandler);
};

}
}

#endif
