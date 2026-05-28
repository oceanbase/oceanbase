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
 *
 * Redo Read Task Dispatch Module. Dispatch read_redo_task into RedoReader module if redo data is in storage
 * Module Input: TransCtx
 * A Plugin Called by Sequencer
 */

#ifndef OCEANBASE_LIBOBCDC_TRNAS_REDO_DISPATCHER_
#define OCEANBASE_LIBOBCDC_TRNAS_REDO_DISPATCHER_

#include "ob_log_config.h"
#include "ob_log_trans_ctx.h"
#include "ob_log_part_trans_task.h"
#include "ob_log_dml_parser.h"
#include "ob_log_reader.h"
#include "ob_log_entry_task_pool.h"
#include "ob_log_trans_dispatch_ctx.h"
#include "lib/queue/ob_fixed_queue.h"
#include "lib/thread/thread_pool.h"

namespace oceanbase
{
namespace libobcdc
{

class IObLogErrHandler;

class IObLogTransRedoDispatcher
{
public:
  virtual ~IObLogTransRedoDispatcher() {}
public:
  /**
   * @brief dispatch dml_task to load all redo content of a DML trans
   *
   * @param  [in]  trans  Transaction to be handled(ONLY support DML Trans)
   * @retval OB_SUCCESS    Dispatch success
   * @retval OB_NEED_RETRY Redo of trans not finish dispatch, need retry(continue dispatch from last dispatched redo)
   * @retval other         Dispatch unexpected fail
   */
  virtual int dispatch_trans_redo(TransCtx &trans) = 0;
  virtual int dec_dispatched_redo_memory(const int64_t &log_size) = 0;
  virtual int64_t get_dispatched_memory_size() const = 0;
  virtual bool is_dispatched_memory_over_limit() const = 0;
  virtual int64_t get_queue_capacity() const = 0;
  // Get task statistics: task_count (queued + processing) and queue_size (queued only)
  // The difference (task_count - queue_size) represents the number of tasks currently being processed
  virtual void get_task_stat(int64_t &task_count, int64_t &queue_size) const = 0;
  // reeoad config that can dynamicly modify
  virtual void configure(const ObLogConfig &config) = 0;
  // Module lifecycle management
  virtual int start() = 0;
  virtual void stop() = 0;
  virtual void mark_stop_flag() = 0;
};

class ObLogTransRedoDispatcher: public IObLogTransRedoDispatcher, public lib::ThreadPool
{
public:
  ObLogTransRedoDispatcher();
  virtual ~ObLogTransRedoDispatcher();
  int init(const int64_t redo_dispatcher_memory_limit,
           const bool enable_sort_by_seq_no,
           const int64_t thread_num,
           const int64_t queue_size,
           IObLogTransStatMgr &trans_stat_mgr,
           IObLogErrHandler &err_handler);
  void destroy();

public:
  int start() override;
  void stop() override;
  void mark_stop_flag() override { lib::ThreadPool::stop(); }

public:
  int dispatch_trans_redo(TransCtx &trans);
  int dec_dispatched_redo_memory(const int64_t &log_size);
  int64_t get_dispatched_memory_size() const { return ATOMIC_LOAD(&cur_dispatched_redo_memory_); }
  bool is_dispatched_memory_over_limit() const;
  int64_t get_queue_capacity() const override { return task_queue_.capacity(); }
  void get_task_stat(int64_t &task_count, int64_t &queue_size) const override;
  void configure(const ObLogConfig &config);

private:
  // Thread pool method
  virtual void run(int64_t idx) override;
  // Process a TransCtx dispatch task
  int process_dispatch_task_(TransCtx &trans_ctx);

private:
  static const int64_t DISPATCH_OP_TIMEOUT = 1 * 1000 * 1000;

private:
  int (ObLogTransRedoDispatcher::*dispatch_func_)(TransCtx &trans);

private:
  // dispatch redo_read_task by partition order if enable_output_trans_order_by_sql_operation = 0;
  int dispatch_by_partition_order_(TransCtx &trans);
  // dispatch redo_read_task by turn if enable_output_trans_order_by_sql_operation = 1;
  // Note: TransDispatchCtx is created internally on stack
  int dispatch_by_turn_(TransCtx &trans);
  // reblance redo memory distribute
  int reblance_part_redo_memory_to_alloc_(TransCtx &trans, TransDispatchCtx &dispatch_ctx);
  /// dispatch redo of trans with a batch
  /// @retval OB_SUCCESS      success to process current batch task
  /// @retval other_err_code  Failed to process batch task, if budget used up,
  /// return OB_SUCCESS and wait for another batch
  int dispatch_trans_batch_redo_(TransCtx &trans, TransDispatchCtx &dispatch_ctx);
  /// dispatch by priority with budget
  /// @retval OB_SUCCESS     cur_batch dispatch finish
  /// @retval OB_NEED_RETRY  need retry (cause current redo memory budget used up)
  int dispatch_part_redo_with_budget_(
      TransCtx &trans,
      TransDispatchCtx &dispatch_ctx,
      PartBudgetArray &part_task_with_budget_array);
  /// try dispatch one redo of part_trans_task
  /// note: try next part directly if is_part_dispatch_finish = true, avoid use part_trans_task
  ///       because part_trans_task may be recycled anytime after dispatch.
  ///
  int try_get_and_dispatch_single_redo_(
      PartTransTask &part_trans_task,
      bool &has_memory_to_dispatch_redo,
      bool &is_part_dispatch_finish,
      PartTransDispatchBudget *part_budget = NULL);
  // check redo can dispatch or not
  int check_redo_can_dispatch_(bool &can_dispatch, PartTransDispatchBudget *part_budget = NULL);
  // dispatch redo, part_budget should not be null if dispatch by turn
  int dispatch_redo_(PartTransTask &part_trans, DmlRedoLogNode &redo_node);
  // alloc an ObLogEntryTask as redo_read_task
  int alloc_task_for_redo_(PartTransTask &part_task,
      DmlRedoLogNode &redo_node,
      ObLogEntryTask *&log_entry_task);
  // push redo_read_task(ObLogEntryTask) to ObLogRedoReader
  int push_task_to_reader_(ObLogEntryTask &log_entry_task);
  // push ObLogEntryTask to dml_parser
  int push_task_to_parser_(ObLogEntryTask &log_entry_task);

private:
  typedef common::ObFixedQueue<TransCtx> DispatchTaskQueue;

  int64_t                   redo_memory_limit_; // can dynamicly modify
  int64_t                   cur_dispatched_redo_memory_; // can dynamicly modify
  int64_t                   task_count_ CACHE_ALIGNED; // atomic task count (queued + processing)
  IObLogTransStatMgr        *trans_stat_mgr_;
  IObLogErrHandler          *err_handler_;
  bool                      enable_sort_by_seq_no_; // config by init and can't change unless progress restart
  DispatchTaskQueue         task_queue_;
  bool                      inited_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObLogTransRedoDispatcher);
};

} // end namespace libobcdc
} // end namespace oceanbase
#endif // end OCEANBASE_LIBOBCDC_TRNAS_REDO_DISPATCHER_
