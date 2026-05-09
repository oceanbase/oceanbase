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
#include "lib/atomic/ob_atomic.h"                 // ATOMIC_LOAD, ATOMIC_AAF
#include "lib/queue/ob_fixed_queue.h"             // ObFixedQueue
#include "lib/thread/threads.h"                   // lib::Threads
#include "ob_log_utils.h"                         // set_cdc_thread_name, _SEC_
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
// CommitterStatInfo

struct CommitterStatInfo
{
  int64_t total_processed_trans_count_ CACHE_ALIGNED;
  int64_t empty_trans_count_ CACHE_ALIGNED;
  int64_t last_total_processed_trans_count_;
  int64_t last_empty_trans_count_;
  int64_t last_stat_time_;

  CommitterStatInfo() { reset(); }
  ~CommitterStatInfo() { reset(); }

  void reset()
  {
    ATOMIC_SET(&total_processed_trans_count_, 0);
    ATOMIC_SET(&empty_trans_count_, 0);
    last_total_processed_trans_count_ = 0;
    last_empty_trans_count_ = 0;
    last_stat_time_ = 0;
  }

  void inc_total_processed_trans()
  {
    ATOMIC_INC(&total_processed_trans_count_);
  }

  void inc_empty_trans()
  {
    ATOMIC_INC(&empty_trans_count_);
  }

  void calc_and_print_stat();

  TO_STRING_KV(K_(total_processed_trans_count),
               K_(empty_trans_count),
               K_(last_total_processed_trans_count),
               K_(last_empty_trans_count),
               K_(last_stat_time));
};

///////////////////////////////////////////////////////////////////////////////////////
// CommitterPerfStat - per-phase latency and backlog diagnostics
//
// COMMITTER_PERF log line: meaning of each metric
// -----------------------------------------------
// [PUSH_TO_RC_AVG_US]
//   Aggregate: (queue_wait + output + update_tenant_version + after_trans_handled) / count.
//   Meaning: Average time (us) from when a transaction is pushed into the committer queue
//   until it is fully handed to the RC module (revert done). One number for "whole committer
//   latency" per trans.
//
// [QUEUE_WAIT_US] sum, count, avg_us
//   Time (us) from push into trans_committer_queue_ until commit_routine gets the task (get() returns
//   a valid task). High avg_us or high backlog (see BACKLOG) means the single commit thread cannot
//   keep up or upstream is faster than commit throughput.
//
// [OUTPUT_US] sum, count, avg_us
//   Time (us) spent inside commit_binlog_record_list_(): iterating BRs via next_ready_br_task_(),
//   pushing them to the user BR queue. Only counted for DML. High value means BR production
//   (formatter/sorter) or push_br_queue_ is slow.
//
// [UPDATE_TENANT_VERSION_US] sum, count, avg_us
//   Time (us) spent in update_tenant_trans_commit_version_(): updating tenant commit version for
//   the participant LS. Only for DML. Usually small; spikes may indicate tenant/version logic cost.
//
// [AFTER_TRANS_HANDLED_US] sum, count, avg_us
//   Time (us) spent in after_trans_handled_(): per participant, update_checkpoint_info_() and
//   resource_collector_->revert(PartTransTask). Revert pushes the task back to the RC queue and
//   is often the hot path; high avg_us can indicate RC queue or downstream pressure.
//
// [BACKLOG]
//   queue_not_ready_count: Number of times commit_routine called get() but "data not ready"
//   (OB_ERR_OUT_OF_UPPER_BOUND or NULL task), then timedwait. High value = commit thread often
//   idles waiting for the next sequential task (upstream ordering/throughput or empty queue).
//   next_ready_br_eagain_count: Number of times next_ready_br_task_() returned EAGAIN inside
//   commit_binlog_record_list_ (waiting for sorter to produce the next BR). High value = BR
//   production cannot keep up with commit thread demand.
//   cur_queue_depth: Current queue depth at print time (end_sn - begin_sn).
//   last_depth: Queue depth last time we successfully got a task in this window.
//   max_depth: Maximum queue depth observed in this stat window (peak backlog).
//

struct CommitterPerfStat
{
  // Phase latency: sum_us / count => avg_us per trans (DML only for output/update phases)
  int64_t queue_wait_sum_us_ CACHE_ALIGNED;
  int64_t queue_wait_count_ CACHE_ALIGNED;
  int64_t output_sum_us_ CACHE_ALIGNED;
  int64_t output_count_ CACHE_ALIGNED;
  int64_t update_tenant_version_sum_us_ CACHE_ALIGNED;
  int64_t update_tenant_version_count_ CACHE_ALIGNED;
  int64_t after_trans_handled_sum_us_ CACHE_ALIGNED;  // update_checkpoint_info_ + resource_collector_->revert
  int64_t after_trans_handled_count_ CACHE_ALIGNED;

  // Backlog / stall diagnostics
  int64_t queue_not_ready_count_ CACHE_ALIGNED;   // commit_routine timedwait times (waiting for next seq)
  int64_t next_ready_br_eagain_count_ CACHE_ALIGNED;  // waiting for sorter BR in commit_binlog_record_list_
  int64_t last_queue_depth_;   // last sampled (end_sn - begin_sn) when we got a task
  int64_t max_queue_depth_;   // max queue depth seen in this stat window

  // Phase 0: Fine-grained breakdown of after_trans_handled_ (ns precision via CLOCK_MONOTONIC)
  int64_t ckpt_alloc_set_sum_ns_ CACHE_ALIGNED;   // alloc_checkpoint_task_ + checkpoint_queue_.set
  int64_t ckpt_signal_sum_ns_ CACHE_ALIGNED;       // checkpoint_queue_cond_.signal
  int64_t revert_sum_ns_ CACHE_ALIGNED;            // resource_collector_->revert (when dec_ref_cnt hits 0)
  int64_t revert_call_count_ CACHE_ALIGNED;        // how many times revert was actually called
  int64_t participant_count_ CACHE_ALIGNED;         // total participant count
  // Phase 2: Post-commit queue diagnostics
  int64_t post_commit_queue_push_count_ CACHE_ALIGNED;
  int64_t post_commit_queue_pop_count_ CACHE_ALIGNED;

  CommitterPerfStat() { reset(); }

  void reset()
  {
    ATOMIC_SET(&queue_wait_sum_us_, 0);
    ATOMIC_SET(&queue_wait_count_, 0);
    ATOMIC_SET(&output_sum_us_, 0);
    ATOMIC_SET(&output_count_, 0);
    ATOMIC_SET(&update_tenant_version_sum_us_, 0);
    ATOMIC_SET(&update_tenant_version_count_, 0);
    ATOMIC_SET(&after_trans_handled_sum_us_, 0);
    ATOMIC_SET(&after_trans_handled_count_, 0);
    ATOMIC_SET(&queue_not_ready_count_, 0);
    ATOMIC_SET(&next_ready_br_eagain_count_, 0);
    ATOMIC_SET(&last_queue_depth_, 0);
    ATOMIC_SET(&max_queue_depth_, 0);
    ATOMIC_SET(&ckpt_alloc_set_sum_ns_, 0);
    ATOMIC_SET(&ckpt_signal_sum_ns_, 0);
    ATOMIC_SET(&revert_sum_ns_, 0);
    ATOMIC_SET(&revert_call_count_, 0);
    ATOMIC_SET(&participant_count_, 0);
    ATOMIC_SET(&post_commit_queue_push_count_, 0);
    ATOMIC_SET(&post_commit_queue_pop_count_, 0);
  }

  void calc_and_print_stat(int64_t begin_sn, int64_t end_sn);
};

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

  // Get post-commit queue depth (number of transactions pending after_trans_handled_)
  virtual int64_t get_post_commit_queue_count() const = 0;

  // update config of committer
  virtual void configure(const ObLogConfig &cfg) = 0;

  // print statistics information
  virtual void print_stat_info() = 0;

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
  int64_t get_post_commit_queue_count() const { return post_commit_queue_.get_curr_total(); }
  void configure(const ObLogConfig &cfg);
  void print_stat_info();

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
  void post_commit_routine();

private:
  static void *commit_thread_func_(void *args);
  static void *heartbeat_thread_func_(void *args);

private:
  int alloc_checkpoint_task_(PartTransTask &task, CheckpointTask *&checkpoint_task);
  void free_checkpoint_task_(CheckpointTask *checkpoint_task);
  int update_checkpoint_info_(PartTransTask &task);
  // Phase 1: same as update_checkpoint_info_ but does NOT signal checkpoint_queue_cond_
  int update_checkpoint_info_no_signal_(PartTransTask &task);
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
  // Phase 1: Batched signal version - only signal checkpoint_queue_cond_ once per transaction
  // Returns the number of participants processed via output parameter
  int after_trans_handled_batch_signal_(PartTransTask *participants, int64_t &participant_count);
  int next_ready_br_task_(TransCtx &trans_ctx, ObLogBR *&br_task);
  int calculate_output_checkpoint_(int64_t &output_checkpoint);
  // Phase 2: push participants list to post-commit queue
  int push_to_post_commit_queue_(PartTransTask *participants);

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

  // Phase 2: Thread pool for post-commit processing (using lib::Threads)
  class PostCommitThreadPool : public lib::Threads
  {
  public:
    explicit PostCommitThreadPool(ObLogCommitter &host)
        : lib::Threads(), host_(host) {}
    void run(int64_t idx) override
    {
      set_cdc_thread_name("CDC-POST-COMMIT", idx);
      host_.post_commit_routine();
    }
  private:
    ObLogCommitter &host_;
    DISALLOW_COPY_AND_ASSIGN(PostCommitThreadPool);
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
  PostCommitThreadPool      post_commit_thread_pool_;  // Phase 2: post-commit thread pool

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

  // Phase 2: Post-commit queue for async after_trans_handled_ processing
  common::ObFixedQueue<PartTransTask> post_commit_queue_;
  common::ObCond            post_commit_queue_cond_;

  // Count the number of DML partition transaction tasks
  int64_t                   dml_part_trans_task_count_ CACHE_ALIGNED;
  int64_t                   ddl_part_trans_task_count_;
  int64_t                   dml_trans_count_;

  // TPS statistics
  CommitterStatInfo         stat_info_ CACHE_ALIGNED;
  // Per-phase latency and backlog diagnostics
  CommitterPerfStat         perf_stat_ CACHE_ALIGNED;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogCommitter);
};
} // namespace libobcdc
} // namespace oceanbase
#endif /* OCEANBASE_LIBOBCDC_COMMITTER_H__ */
