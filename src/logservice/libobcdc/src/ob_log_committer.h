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
 * Committer
 */

#ifndef OCEANBASE_LIBOBCDC_COMMITTER_H__
#define OCEANBASE_LIBOBCDC_COMMITTER_H__

#include "common/ob_queue_thread.h"               // ObCond
#include "lib/container/ob_ext_ring_buffer.h"     // ObExtendibleRingBuffer
#include "lib/atomic/ob_atomic.h"                 // ATOMIC_LOAD

#include "ob_log_utils.h"                         // _SEC_
#include "ob_log_part_trans_task.h"               // PartTransTask, DdlStmtTask
#include "ob_log_trans_ctx.h"                     // TransCtx

namespace oceanbase
{
namespace transaction
{
class ObTransID;
}

namespace libobcdc
{
///////////////////////////////////////////////////////////////////////////////////////
// IObLogCommitter

class PartTransTask;
class ObLogConfig;
class BRQueue;
class ObLogTenant;

class IObLogCommitter
{
public:
  virtual ~IObLogCommitter() {}

public:
  virtual int start() = 0;
  virtual void stop() = 0;
  virtual void mark_stop_flag() = 0;
  virtual int push(PartTransTask *task,
      const int64_t task_count,
      const int64_t timeout,
      ObLogTenant *tenant = NULL) = 0;

  virtual int64_t get_dml_trans_count() const = 0;

  virtual void get_part_trans_task_count(int64_t &ddl_part_trans_task_count,
      int64_t &dml_part_trans_task_count) const = 0;

  // update config of committer
  virtual void configure(const ObLogConfig &cfg) = 0;

};

///////////////////////////////////////////////////////////////////////////////////////
// ObLogCommitter

class IObLogResourceCollector;
class IObLogErrHandler;
class IObLogTransCtxMgr;
class IObLogTransStatMgr;
class DdlStmtTask;
class IObLogBRPool;

class ObLogCommitter : public IObLogCommitter
{
  typedef common::ObExtendibleRingBuffer<PartTransTask> TransCommitterQueue;
  struct CheckpointTask;
  typedef common::ObExtendibleRingBuffer<CheckpointTask> CheckpointQueue;
  // Record global heartbeat corresponding checkpoint_seq, easy to troubleshoot global checkpoint not advancing problem
  typedef common::ObExtendibleRingBuffer<const int64_t> GlobalHeartbeatInfoQueue;
  typedef common::ObConcurrentFIFOAllocator CheckpointQueueAllocator;

  static const int64_t DATA_OP_TIMEOUT = 1L * _SEC_;
  // No memory limit for checkpoint queue
  static const int64_t CHECKPOINT_QUEUE_ALLOCATOR_TOTAL_LIMIT = INT64_MAX;
  static const int64_t CHECKPOINT_QUEUE_ALLOCATOR_HOLD_LIMIT = 32 * _M_;
  static const int64_t CHECKPOINT_QUEUE_ALLOCATOR_PAGE_SIZE = 2 * _M_;
  static const int64_t COMMITTER_TRANS_COUNT_UPPER_LIMIT = 1000;
  static int64_t g_output_heartbeat_interval;
  static const int64_t PRINT_GLOBAL_HEARTBEAT_CHECKPOINT_INTERVAL = 10L * _SEC_;

public:
  ObLogCommitter();
  virtual ~ObLogCommitter();

public:
  int start();
  void stop();
  void mark_stop_flag();
  int push(PartTransTask *task,
      const int64_t task_count,
      const int64_t timeout,
      ObLogTenant *tenant = NULL);
  int64_t get_dml_trans_count() const { return ATOMIC_LOAD(&dml_trans_count_); }
  void get_part_trans_task_count(int64_t &ddl_part_trans_task_count,
      int64_t &dml_part_trans_task_count) const;
  void configure(const ObLogConfig &cfg);

public:
  int init(const int64_t start_seq,
      BRQueue *br_queue,
      IObLogResourceCollector *resource_collector,
      IObLogBRPool *tag_br_alloc,
      IObLogTransCtxMgr *trans_ctx_mgr,
      IObLogTransStatMgr *trans_stat_mgr,
      IObLogErrHandler *err_handler);
  void destroy();
  void commit_routine();
  void heartbeat_routine();

private:
  static void *commit_thread_func_(void *args);
  static void *heartbeat_thread_func_(void *args);

private:
  int alloc_checkpoint_task_(PartTransTask &task, CheckpointTask *&checkpoint_task);
  void free_checkpoint_task_(CheckpointTask *checkpoint_task);
  int update_checkpoint_info_(PartTransTask &task);
  int push_heartbeat_(PartTransTask &task);
  // For not served PartTransTask:
  // 1. If don't contain any data, can recycle directly
  // 2. Otherwise, need to wait LogEntryTask callback
  int handle_not_served_trans_(PartTransTask &task);
  int push_offline_ls_task_(PartTransTask &task);
  int push_ls_table_task_(PartTransTask &task);
  int next_checkpoint_task_(CheckpointTask *&task);
  int handle_checkpoint_task_(CheckpointTask &task);
  int next_global_heartbeat_info_(const int64_t cur_checkpoint_seq);
  void print_global_heartbeat_info_();
  int dispatch_heartbeat_binlog_record_(const int64_t heartbeat_timestamp);
  int handle_task_(PartTransTask *task);
  int handle_dml_task_(PartTransTask *task);
  int handle_ddl_task_(PartTransTask *ddl_task);
  int handle_ddl_stmt_(DdlStmtTask &stmt_task);
  int revert_binlog_record_(ObLogBR *br);
  int do_trans_stat_(const logservice::TenantLSID &pkey, const int64_t total_stmt_cnt);
  int commit_binlog_record_list_(TransCtx &trans_ctx,
      const uint64_t cluster_id,
      const int64_t part_trans_task_count,
      const uint64_t tenant_id,
      const int64_t trans_commit_version);
  int push_br_queue_(ObLogBR *br);
  int handle_offline_checkpoint_task_(CheckpointTask &task);
  int recycle_task_directly_(PartTransTask &task, const bool can_async_recycle = true);
  int record_global_heartbeat_info_(PartTransTask &task);
  int handle_when_trans_ready_(PartTransTask *task,
      int64_t &commit_trans_count);
  int update_tenant_trans_commit_version_(const PartTransTask &participants);
  int after_trans_handled_(PartTransTask *participants);
  int next_ready_br_task_(TransCtx &trans_ctx, ObLogBR *&br_task);
  int calculate_output_checkpoint_(int64_t &output_checkpoint);

private:
  struct CheckpointTask
  {
    logservice::TenantLSID  tenant_ls_id_;  // tenant_id of corresponding PartTransTask
    PartTransTask::TaskType task_type_;     // PartTransTask::TaskType
    int64_t                 timestamp_;     // timestamp of task

    bool is_global_heartbeat() const
    {
      return PartTransTask::TASK_TYPE_GLOBAL_HEARTBEAT == task_type_;
    }
    bool is_offline_ls_task() const { return PartTransTask::TASK_TYPE_OFFLINE_LS == task_type_; }

    explicit CheckpointTask(PartTransTask &task);
    ~CheckpointTask();

    TO_STRING_KV(
        K_(tenant_ls_id),
        "task_type", PartTransTask::print_task_type(task_type_),
        K_(timestamp));
  };

  struct CheckpointQueuePopFunc
  {
    // Operators to determine if Ready
    bool operator()(const int64_t sn, CheckpointTask *task)
    {
      UNUSED(sn);
      return NULL != task;
    }
  };

  struct CommitQueuePopFunc
  {
    // Operators to determine if Ready
    bool operator()(const int64_t sn, PartTransTask *task)
    {
      UNUSED(sn);
      return NULL != task;
    }
  };

  struct GHeartbeatInfoQueuePopFunc
  {
    // Operators to determine if Ready
    bool operator()(const int64_t sn, const int64_t *checkpoint_seq)
    {
      UNUSED(sn);
      return NULL != checkpoint_seq;
    }
  };

private:
  bool                      inited_;
  BRQueue                   *br_queue_;
  IObLogBRPool              *tag_br_alloc_;
  IObLogErrHandler          *err_handler_;
  IObLogTransCtxMgr         *trans_ctx_mgr_;
  IObLogTransStatMgr        *trans_stat_mgr_;
  IObLogResourceCollector   *resource_collector_;

  // threads
  pthread_t                 commit_pid_;      // commit thread
  pthread_t                 heartbeat_pid_;   // heartbeat thread

  volatile bool             stop_flag_ CACHE_ALIGNED;

  TransCommitterQueue       trans_committer_queue_; // Queue of distribute trans
  common::ObCond            trans_committer_queue_cond_;

  // Globally unique sequence queue for generating checkpoint
  //
  // Fetcher assigns checkpoint seq to all tasks that are sent down and periodically calculates checkpoint information to be sent down via heartbeat tasks
  // Committer sorts the tasks that arrive in disorder based on the checkpoint seq, and maintains the overall checkpoint by processing the tasks sequentially
  CheckpointQueue           checkpoint_queue_;
  common::ObCond            checkpoint_queue_cond_;
  CheckpointQueueAllocator  checkpoint_queue_allocator_;

  int64_t                   last_output_checkpoint_;
  int64_t                   global_heartbeat_seq_;
  GlobalHeartbeatInfoQueue  global_heartbeat_info_queue_;

  // Count the number of DML partition transaction tasks
  int64_t                   dml_part_trans_task_count_ CACHE_ALIGNED;
  int64_t                   ddl_part_trans_task_count_;
  int64_t                   dml_trans_count_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogCommitter);
};
} // namespace libobcdc
} // namespace oceanbase
#endif /* OCEANBASE_LIBOBCDC_COMMITTER_H__ */
