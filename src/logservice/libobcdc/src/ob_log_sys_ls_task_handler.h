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

#ifndef OCEANBASE_LIBOBCDC_OB_LOG_SYS_LS_TASK_HANDLER_H_
#define OCEANBASE_LIBOBCDC_OB_LOG_SYS_LS_TASK_HANDLER_H_

#include "lib/lock/ob_small_spin_lock.h"          // ObByteLock
#include "lib/utility/ob_macro_utils.h"           // CACHE_ALIGNED, DISALLOW_COPY_AND_ASSIGN
#include "common/ob_queue_thread.h"               // ObCond

#include "ob_log_utils.h"                         // _SEC_
#include "ob_log_part_trans_task.h"               // PartTransTask
#include "ob_log_part_trans_task_queue.h"         // SafePartTransTaskQueue
#include "ob_log_ddl_processor.h"                 // ObLogDDLProcessor
#include "ob_log_config.h"                        // ObLogConfig

namespace oceanbase
{
namespace libobcdc
{
class IObLogSysLsTaskHandler
{
public:
  virtual ~IObLogSysLsTaskHandler() {}

public:
  virtual int start() = 0;
  virtual void stop() = 0;
  virtual void mark_stop_flag() = 0;
  // Handle SYS LS Task, include:
  // 1. DDL Trans
  // 2. LS Table Task
  // 3. SYS LS HeartBeat
  // 4. SYS Offline Task
  virtual int push(PartTransTask *task, const int64_t timeout) = 0;

  /// Get the processing progress, note that it is not the output progress,
  /// a SYS LS task can update the progress after the processing is completed
  ////
  /// 1. If there is a backlog of data to be processed in the queue,
  ///    return the timestamp of the next pending task - 1 as the processing progress
  /// 2. If there is no backlog in the queue, return OB_INVALID_PROGRESS
  ////
  /// @param [out] sys_min_progress_tenant_id the tenant id of the slowest sys ls processing progress
  /// @param [out] sys_ls_min_progress       slowest sys ls progress
  /// @param [out] sys_ls_min_handle_log_lsn slowest sys ls processing log LSN
  ///
  /// @retval OB_SUCCESS                  success
  /// @retval other error code            fail
  virtual int get_progress(
      uint64_t &sys_min_progress_tenant_id,
      int64_t &sys_ls_min_progress,
      palf::LSN &sys_ls_min_handle_log_lsn) = 0;

  virtual void get_task_count(int64_t &total_part_trans_count, int64_t &ddl_part_trans_count) const = 0;

  virtual void configure(const ObLogConfig &config) = 0;
};

///////////////////////////////// ObLogSysLsTaskHandler /////////////////////////////////

class IObLogDdlParser;
class IObLogErrHandler;
class IObLogSequencer;
class IObLogSchemaGetter;
class IStmtTask;
class PartTransTask;
class DdlStmtTask;
class IObLogTenantMgr;
class ObLogTenant;
class ObLogTenantGuard;
class ObLogSysLsTaskHandler : public IObLogSysLsTaskHandler
{
  enum
  {
    DATA_OP_TIMEOUT = 1 * _SEC_,
  };
  static int64_t g_sys_ls_task_op_timeout_msec;

public:
  ObLogSysLsTaskHandler();
  virtual ~ObLogSysLsTaskHandler();

public:
  int start();
  void stop();
  void mark_stop_flag() { stop_flag_ = true; }
  int push(PartTransTask *task, const int64_t timeout);
  virtual int get_progress(
      uint64_t &sys_min_progress_tenant_id,
      int64_t &sys_ls_min_progress,
      palf::LSN &sys_ls_min_handle_log_lsn);
  virtual void get_task_count(int64_t &total_part_trans_count, int64_t &ddl_part_trans_count) const;
  virtual void configure(const ObLogConfig &config);

public:
  int init(
      IObLogDdlParser *ddl_parser,
      ObLogDDLProcessor *ddl_processor,
      IObLogSequencer *committer,
      IObLogErrHandler *err_handler,
      IObLogSchemaGetter *schema_getter,
      const bool skip_reversed_schema_version);
  void destroy();

public:
  void handle_task_routine();

private:
  static void *handle_thread_func_(void *arg);
  int handle_ddl_trans_(
      PartTransTask &task,
      ObLogTenant &tenant);
  int get_tenant_(
      PartTransTask &task,
      const uint64_t ddl_tenant_id,
      ObLogTenantGuard &guard,
      ObLogTenant *&tenant,
      bool &is_tenant_served);
  int update_sys_ls_info_(
      PartTransTask &task,
      ObLogTenant &tenant);
  int handle_sys_ls_offline_task_(const PartTransTask &task);
  int next_task_(PartTransTask *&task);
  int handle_task_(
      PartTransTask &task,
      const uint64_t ddl_tenant_id,
      ObLogTenant *tenant,
      const bool is_tenant_served);
  int dispatch_task_(
      PartTransTask *task,
      ObLogTenant *tenant,
      const bool is_tenant_served);
  void inc_ddl_part_trans_count_() { ATOMIC_INC(&ddl_part_trans_count_); }
  void dec_ddl_part_trans_count_() { ATOMIC_DEC(&ddl_part_trans_count_); }

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
    int next_ready_to_handle(
        const int64_t timeout,
        PartTransTask *&top_task,
        common::ObCond &cond);

  private:
    SafePartTransTaskQueue  queue_;
  };

private:
  bool                is_inited_;
  IObLogDdlParser     *ddl_parser_;
  IObLogSequencer     *sequencer_;
  IObLogErrHandler    *err_handler_;
  IObLogSchemaGetter  *schema_getter_;

  ObLogDDLProcessor   *ddl_processor_;

  // thread id of ddl handler
  pthread_t           handle_pid_;
  volatile bool       stop_flag_ CACHE_ALIGNED;

  // Queue of pending tasks exported from Fetcher
  TaskQueue           sys_ls_fetch_queue_;
  int64_t             ddl_part_trans_count_;
  common::ObCond      wait_formatted_cond_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogSysLsTaskHandler);
};

}
}

#endif
