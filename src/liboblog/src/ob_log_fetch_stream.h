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

#ifndef OCEANBASE_LIBOBLOG_OB_LOG_FETCH_STREAM_H__
#define OCEANBASE_LIBOBLOG_OB_LOG_FETCH_STREAM_H__

#include "lib/net/ob_addr.h"                // ObAddr
#include "lib/hash/ob_linear_hash_map.h"    // ObLinearHashMap
#include "lib/hash/ob_ext_iter_hashset.h"   // ObExtIterHashSet
#include "lib/lock/ob_small_spin_lock.h"    // ObByteLock
#include "common/ob_partition_key.h"        // ObPartitionKey
#include "clog/ob_log_external_rpc.h"       // ObStreamSeq, ObLogStreamFetchLogResp
#include "clog/ob_log_entry.h"              // ObLogEntry

#include "ob_log_part_fetch_ctx.h"          // FetchTaskList
#include "ob_map_queue.h"                   // ObMapQueue
#include "ob_log_fetch_log_rpc.h"           // FetchLogARpc, FetchLogARpcResult, FetchLogSRpc, IFetchLogARpcResultPool
#include "ob_log_utils.h"                   // _SEC_
#include "ob_log_timer.h"                   // ObLogTimerTask
#include "ob_log_dlist.h"                   // ObLogDList
#include "ob_log_fetch_stream_type.h"       // FetchStreamType
#include "ob_log_fetch_stat_info.h"         // FetchStatInfo

namespace oceanbase
{
namespace liboblog
{

class IObLogStreamWorker;
class IObLogRpc;
class IObLogSvrFinder;
class IObLogFetcherHeartbeatWorker;
class PartFetchCtx;
class PartProgressController;
class ObLogConfig;

class FetchStream;
typedef ObLogDListNode<FetchStream> FSListNode;

// Fetch log stream bi-directional linked list
typedef ObLogDList<FetchStream> FSList;

// Fetch log stream
class FetchStream : public FSListNode, public ObLogTimerTask
{
private:
  static const int64_t DEFAULT_MISSING_LOG_STREAM_LIFE_TIME = 10 * _SEC_;
  static const int64_t STAT_INTERVAL = 30 * _SEC_;
  static const int64_t DEFAULT_TASK_SET_SIZE = 16;

  typedef ObMapQueue<PartFetchCtx *> TaskQueue;
  typedef common::ObLinearHashMap<common::ObPartitionKey, PartFetchCtx *> TaskMap;
  struct KickOutTask;
  typedef common::hash::ObExtIterHashSet<KickOutTask, DEFAULT_TASK_SET_SIZE> KickOutTaskSet;
  typedef obrpc::ObLogStreamFetchLogResp::FeedbackPartition Feedback;

  // Class global variables
public:
  static int64_t g_stream_max_part_count;
  static int64_t g_stream_life_time;
  static int64_t g_rpc_timeout;
  static int64_t g_fetch_log_cnt_per_part_per_round;
  static int64_t g_dml_progress_limit;
  // Survival time of server added to blacklist
  static int64_t g_blacklist_survival_time;
  static int64_t g_check_switch_server_interval;
  static bool g_print_rpc_handle_info;
  static bool g_print_stream_dispatch_info;

  /////////// Fetch log stream status //////////
  // IDLE:        Idle state, not waiting for any asynchronous RPC
  // FETCH_LOG:   Launch an asynchronous RPC request to fetch the log, waiting for the request to return
  enum State
  {
    IDLE = 0,
    FETCH_LOG = 1,
  };

  static const char *print_state(State state);

public:
  FetchStream();
  virtual ~FetchStream();

public:
  void reset();

  void reset(const common::ObAddr &svr,
      const FetchStreamType stream_type,
      IObLogRpc &rpc,
      IObLogSvrFinder &svr_finder,
      IObLogFetcherHeartbeatWorker &heartbeater,
      IObLogStreamWorker &stream_worker,
      IFetchLogARpcResultPool &rpc_result_pool,
      PartProgressController &progress_controller);

  bool is_ddl_stream() const { return FETCH_STREAM_TYPE_DDL == stype_; }

  /// Add a fetch log task, multi-threaded operation
  /// If this fetch log stream task is new, assign it to a worker thread for processing
  ///
  /// "New FetchStream task" includes two cases:
  /// 1. A new FetchStream is created, the partition task is the first partition object, and the FetchStream is new
  /// 2. A new partition task is added after an existing FetchStream has kicked out all the partitions, in which case it is also new
  ///
  /// @retval OB_SUCCESS        added successfully
  /// @retval OB_SIZE_OVERFLOW  The task is full and cannot be added further
  /// @retval Other error codes Adding failed
  int add_fetch_task(PartFetchCtx &task);

  /// Processes the fetch log stream and advances the progress of each partition
  /// Requirement: only one thread can call this function at the same time, multi-threading is not supported
  ///
  /// @retval OB_SUCCESS        added successfully
  /// @retval OB_IN_STOP_STATE  exit
  /// @retval Other error codes Add failed
  int handle(volatile bool &stop_flag);

  /// Implement timer tasks
  /// Assign it to a worker thread
  void process_timer_task();

  void switch_state(State state) { ATOMIC_STORE(&state_, state); }

  int get_upper_limit(int64_t &upper_limit_us);

  // Execution Statistics
  void do_stat();

  int64_t get_fetch_task_count() const { return fetch_task_pool_.total_count(); }

public:
  static void configure(const ObLogConfig & config);

private:
  // 切流原因定义
  // Switch fetch log stream definition
  enum KickOutReason
  {
    NONE                         = -1,

    OPEN_STREAM_FAIL             = 0,   // Open stream failure
    FETCH_LOG_FAIL_ON_RPC        = 1,   // Rpc failure
    FETCH_LOG_FAIL_ON_SERVER     = 2,   // Server failure

    // read missing redo log
    MISSING_LOG_OPEN_STREAM_FAIL = 3,   // Open stream failure for missing redo log
    MISSING_LOG_FETCH_FAIL       = 4,   // Fetch missing redo log failure

    // Feedback
    LAGGED_FOLLOWER              = 5,   // Follow lag behind
    LOG_NOT_IN_THIS_SERVER       = 6,   // This log is not served in the server
    PARTITION_OFFLINED           = 7,   // Partition offline

    // Progress timeout, long time to fetch logs
    PROGRESS_TIMEOUT                      = 8,   // Partition fetch log timeout
    // Progress timeout and detection of lagging replica
    PROGRESS_TIMEOUT_ON_LAGGED_REPLICA    = 9,   //  Partition fetch log timeout on lagging replica

    NEED_SWITCH_SERVER           = 10,  // There is a higher priority server that actively switch
    DISCARDED                    = 11,  // Partition is discard
  };
  static const char *print_kick_out_reason_(const KickOutReason reason);
  // Determine if the server needs to be blacklisted,
  // NEED_SWITCH_SERVER and DISCARDED do not need to be added, all others do
  bool need_add_into_blacklist_(const KickOutReason reason);

private:
  void handle_when_leave_(const char *leave_reason) const;
  int handle_idle_task_(volatile bool &stop_flag);
  int open_stream_(bool &rpc_succeed, int64_t &part_count);
  int kick_out_all_(KickOutReason kick_out_reason);
  int dispatch_fetch_task_(PartFetchCtx &task,
      KickOutReason dispatch_reason);
  int check_need_fetch_log_(const int64_t limit, bool &need_fetch_log);
  int hibernate_();
  int async_fetch_log_(bool &rpc_send_succeed);
  void print_handle_info_(FetchLogARpcResult &result,
      const int64_t handle_rpc_time,
      const int64_t read_log_time,
      const int64_t decode_log_entry_time,
      const bool rpc_is_flying,
      const bool is_stream_valid,
      const char *stream_invalid_reason,
      PartFetchCtx *min_progress_task,
      const TransStatInfo &tsi,
      const bool need_stop_request);
  bool has_new_fetch_task_() const;
  int process_result_(FetchLogARpcResult &result,
      volatile bool &stop_flag,
      const bool rpc_is_flying,
      bool &need_hibernate,
      bool &is_stream_valid);
  int handle_fetch_log_task_(volatile bool &stop_flag);
  void update_fetch_stat_info_(FetchLogARpcResult &result,
      const int64_t handle_rpc_time,
      const int64_t read_log_time,
      const int64_t decode_log_entry_time,
      const int64_t flush_time,
      const TransStatInfo &tsi);
  int handle_fetch_log_result_(FetchLogARpcResult &result,
      volatile bool &stop_flag,
      bool &is_stream_valid,
      const char *&stream_invalid_reason,
      bool &need_hibernate,
      int64_t &read_log_time,
      int64_t &decode_log_entry_time,
      TransStatInfo &tsi,
      int64_t &flush_time,
      PartFetchCtx *&task_with_min_progress);
  int update_rpc_request_params_();
  int handle_fetch_log_error_(const obrpc::ObStreamSeq &seq,
      const obrpc::ObRpcResultCode &rcode,
      const obrpc::ObLogStreamFetchLogResp &resp);
  bool exist_(KickOutTaskSet &kick_out_set, const common::ObPartitionKey &pkey);
  int set_(KickOutTaskSet &kick_out_set,
      const common::ObPartitionKey &pkey,
      KickOutReason kick_out_reason);
  int read_log_(const obrpc::ObLogStreamFetchLogResp &resp,
      volatile bool &stop_flag,
      KickOutTaskSet &kick_out_set,
      int64_t &read_log_time,
      int64_t &decode_log_entry_time,
      TransStatInfo &tsi);
  int handle_missing_log_(PartFetchCtx &task,
      const clog::ObLogEntry &prepare_log_entry,
      const IObLogPartTransResolver::ObLogMissingInfo &org_missing_logs,
      volatile bool &stop_flag,
      KickOutReason &fail_reason);
  int open_stream_(const common::ObPartitionKey &pkey,
      const uint64_t start_log_id,
      obrpc::ObStreamSeq &seq,
      const int64_t stream_life_time,
      bool &rpc_succeed);
  int fetch_missing_log_(PartFetchCtx &task,
      const obrpc::ObStreamSeq &seq,
      const ObLogIdArray &missing_logs,
      const IObLogPartTransResolver::ObTransIDArray &missing_log_trans_id_array,
      int64_t &fetched_missing_log_cnt,
      const int64_t upper_limit);
  int alloc_fetch_log_srpc_(FetchLogSRpc *&fetch_log_srpc);
  int read_missing_log_(PartFetchCtx &task,
      const obrpc::ObLogStreamFetchLogResp &resp,
      const ObLogIdArray &missing_logs,
      const IObLogPartTransResolver::ObTransIDArray &missing_log_trans_id_array,
      int64_t &fetched_missing_log_cnt);
  void free_fetch_log_srpc_(FetchLogSRpc *fetch_log_srpc);
  KickOutReason get_feedback_reason_(const Feedback &feedback) const;
  int check_feedback_(const obrpc::ObLogStreamFetchLogResp &resp,
      KickOutTaskSet &kick_out_set);
  int check_fetch_log_heartbeat_(const obrpc::ObLogStreamFetchLogResp &resp,
      KickOutTaskSet &kick_out_set);
  int kick_out_task_(const KickOutTaskSet &kick_out_set);
  int update_fetch_task_state_(KickOutTaskSet &kick_out_set,
      volatile bool &stop_flag,
      PartFetchCtx *&task_with_min_progress,
      int64_t &flush_time);
  int publish_progress_(PartFetchCtx &task);
  int check_fetch_timeout_(PartFetchCtx &task, KickOutTaskSet &kick_out_set);
  int check_switch_server_(PartFetchCtx &task, KickOutTaskSet &kick_out_set);
  int prepare_rpc_request_(const int64_t part_count);
  bool check_need_feedback_();
  bool check_need_switch_server_();

private:
  struct KickOutTask
  {
    common::ObPartitionKey pkey_;
    KickOutReason kick_out_reason_;

    KickOutTask() : pkey_(), kick_out_reason_(NONE) {}
    explicit KickOutTask(const common::ObPartitionKey &pkey) :
        pkey_(pkey),
        kick_out_reason_(NONE)
    {}
    KickOutTask(const common::ObPartitionKey &pkey, KickOutReason kick_out_reason) :
        pkey_(pkey),
        kick_out_reason_(kick_out_reason)
    {}

    uint64_t hash() const
    {
      return pkey_.hash();
    }

    bool operator == (const KickOutTask &task) const
    {
      return pkey_ == task.pkey_;
    }

    TO_STRING_KV(K_(pkey), "kick_out_reason", print_kick_out_reason_(kick_out_reason_));
  };

  ////////////////////////// Fetch Log Task Pool ////////////////////////
  /// Thread model: multi-threaded push task, single-threaded operation
  struct FetchTaskPool
  {
    ///////////// Task pool status ///////////
    // IDLE:      无任务
    // READY:     There are tasks to be processed
    // HANDLING:   There are threads that are consuming tasks
    //
    // State transitions:
    // 1. initial IDLE state
    // 2. IDLE/READY/HANDLING -> READY: with task input, unconditionally converted to READY
    // 3. READY -> HANDLING: A thread starts consuming the task
    // 4. HANDLING -> IDLE: After the thread has consumed all the data, if the status is still HANDLING, it is converted to IDLE
    // 5. HANDLING -> HANDLING: Threads can change from HANDLING to HANDLING several times during consumption
    enum State
    {
      IDLE = 0,
      READY = 1,
      HANDLING = 2,
    };

    ////////////////////////////// Member Variables //////////////////////////////
    // Multi-threaded operation structure
    State         state_;
    TaskMap       map_;           // All tasks Map
    TaskQueue     queue_;         // Pending Task Queue

    volatile int64_t total_cnt_ CACHE_ALIGNED; // Total number of tasks

    // Single-threaded operation structure
    FetchTaskList list_;          // LinkList of tasks being processed

    ////////////////////////////// Member functions //////////////////////////////
    FetchTaskPool();
    virtual ~FetchTaskPool();
    static const char *print_state(State state);

    TO_STRING_KV("state", print_state(state_),
        K_(total_cnt),
        "queue_cnt", queue_.count(),
        "list_cnt", list_.count(),
        "map_cnt", map_.count());

    void reset();

    // 获取分区任务总数量: 包括在queue中的未开始取日志的分区任务数量
    // Get the total number of partition task that include in the queue that have not started fetching logs
    int64_t total_count() const { return ATOMIC_LOAD(&total_cnt_); }

    // Get the number of unconsumed partition tasks in the queue
    int64_t queued_count() const { return queue_.count(); }

    /// push new task to queue
    /// Multi-threaded calls
    ///
    /// 1. Unconditionally set the task pool state to READY
    /// 2. Return whether the task pool was in IDLE state before
    /// 3. If it is in IDLE state, there is no thread currently consuming and a new thread needs to be allocated
    ///
    /// @param [in]  task         Target task
    /// @param [out] is_pool_idle Return variable that marks whether the task pool was in IDLE state before the push
    //
    /// @retval OB_SUCCESS        successful insertion
    /// @retval OB_SIZE_OVERFLOW  The task pool is full
    /// @retval Other error codes Fail
    int push(PartFetchCtx &task, bool &is_pool_idle);

    /// Kick out and return the entire task list
    /// Single threaded call
    int kick_out_task_list(FetchTaskList &list);

    /// Kick out task
    /// Single threaded call
    int kick_out_task(const common::ObPartitionKey &pkey,
        PartFetchCtx *&task);

    /// Update task pool status and prepare task list
    ///
    /// 1. Take the pending tasks from the queue and put them in the linklist
    /// 2. First set the status to HANDLING unconditionally
    /// 3. Set the status to IDLE when it is confirmed that there are no tasks in the task pool
    /// 4. After the status is successfully set to IDLE, the task pool can no longer be operated, as a thread may have already taken over the task pool
    ///
    /// @param [out] pool_become_idle  Return a variable that marks whether the task pool has changed to IDLE status, i.e., whether it is empty.
    /// @param [out] task_list_changed Return a variable that if the task list has changed
    ///
    /// @retval OB_SUCCESS        Success
    /// @retval Other error codes Fail
    int update(bool &pool_become_idle, bool &task_list_changed);

    FetchTaskList &get_task_list() { return list_; }

    /// Get the corresponding fetch log task based on pkey
    int get_task(const common::ObPartitionKey &pkey, PartFetchCtx *&task) const;

  private:
    // Increase the total number of tasks, and return OB_SIZE_OVERFLOW if the total number reaches the upper limit
    int inc_total_cnt_(const int64_t cnt);
    // Decrease the total number of tasks
    int dec_total_cnt_(const int64_t cnt);

  private:
    DISALLOW_COPY_AND_ASSIGN(FetchTaskPool);
  };

public:
  TO_STRING_KV("type", "FETCH_STREAM",
      "stype", print_fetch_stream_type(stype_),
      "state", print_state(state_),
      K_(svr),
      "upper_limit", TS_TO_STR(upper_limit_),
      K_(need_open_stream),
      K_(stream_seq),
      K_(last_feedback_tstamp),
      K_(fetch_task_pool),
      K_(fetch_log_arpc),
      KP_(next),
      KP_(prev));

private:
  State                         state_;                             // Fetch log state
  FetchStreamType               stype_;                             // Stream type
  common::ObAddr                svr_;                               // Target server
  IObLogRpc                     *rpc_;                              // RPC Processor
  IObLogSvrFinder               *svr_finder_;                       // SvrFinder
  IObLogFetcherHeartbeatWorker  *heartbeater_;                      // Heartbeat Manager
  IObLogStreamWorker            *stream_worker_;                    // Stream master
  IFetchLogARpcResultPool       *rpc_result_pool_;                  // RPC result object pool
  PartProgressController        *progress_controller_;              // Progress Controller

  int64_t                       upper_limit_  CACHE_ALIGNED;        // Stream upper limit
  bool                          need_open_stream_ CACHE_ALIGNED;    // Need to open stream
  obrpc::ObStreamSeq            stream_seq_;                        // Current stream identifier, valid after opening the stream

  int64_t                       last_feedback_tstamp_ CACHE_ALIGNED;      // Last FEEDBACK time
  int64_t                       last_switch_server_tstamp_ CACHE_ALIGNED; // Last switching server time

  // Fetch log task pool
  // Includes: queue of pending tasks and list of tasks being processed
  FetchTaskPool                 fetch_task_pool_;

  // Fetch Log Asynchronous RPC
  FetchLogARpc                  fetch_log_arpc_;

  // Statistical Information
  int64_t                       last_stat_time_;
  FetchStatInfo                 cur_stat_info_;
  FetchStatInfo                 last_stat_info_;
  common::ObByteLock            stat_lock_;      // Mutex lock that statistical information update and access to

private:
  DISALLOW_COPY_AND_ASSIGN(FetchStream);
};

}
}

#endif
