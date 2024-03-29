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
 * Fetch Log Stream
 */

#ifndef OCEANBASE_LIBOBCDC_OB_LOG_FETCH_STREAM_H__
#define OCEANBASE_LIBOBCDC_OB_LOG_FETCH_STREAM_H__

#include "lib/net/ob_addr.h"                // ObAddr
#include "lib/lock/ob_small_spin_lock.h"    // ObByteLock

#include "ob_log_ls_fetch_ctx.h"            // FetchTaskList
#include "ob_log_fetch_log_rpc.h"           // FetchLogARpc, FetchLogARpcResult, FetchLogSRpc, IFetchLogARpcResultPool
#include "ob_log_utils.h"                   // _SEC_
#include "ob_log_timer.h"                   // ObLogTimerTask
#include "ob_log_dlist.h"                   // ObLogDList
#include "ob_log_fetch_stream_type.h"       // FetchStreamType
#include "logservice/logfetcher/ob_log_fetch_stat_info.h"         // logfetcher::FetchStatInfo

namespace oceanbase
{
namespace libobcdc
{

class IObLSWorker;
class IObLogRpc;
class LSFetchCtx;
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
  static const int64_t DEFAULT_MISSING_LS_LIFE_TIME = 10 * _SEC_;
  static const int64_t STAT_INTERVAL = 30 * _SEC_;
  static const int64_t DEFAULT_TASK_SET_SIZE = 16;

  typedef obrpc::FeedbackType Feedback;

  // Class global variables
public:
  static int64_t g_rpc_timeout;
  static int64_t g_dml_progress_limit;
  static int64_t g_ddl_progress_limit;
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

  int init(
      const uint64_t tenant_id,
      LSFetchCtx &ls_fetch_ctx,
      const FetchStreamType stream_type,
      IObLogRpc &rpc,
      IObLSWorker &stream_worker,
      IFetchLogARpcResultPool &rpc_result_pool,
      PartProgressController &progress_controller);

  bool is_sys_log_stream() const { return FETCH_STREAM_TYPE_SYS_LS == stype_; }

  // Prepare to fetch logs on the specified server, single threaded operation
  // Assign this fetch log stream to a worker thread for processing
  //
  // @retval OB_SUCCESS        Success
  // @retval Other error codes Failed
  int prepare_to_fetch_logs(
      LSFetchCtx &ls_fetch_ctx,
      const common::ObAddr &svr);

  /// Processes the fetch log stream and advances the progress of the LS
  /// Requirement: only one thread can call this function at the same time, multi-threading is not supported
  ///
  /// @retval OB_SUCCESS        Success
  /// @retval OB_IN_STOP_STATE  exit
  /// @retval Other error codes Failed
  int handle(volatile bool &stop_flag);

  /// Implement timer tasks
  /// Assign it to a worker thread
  void process_timer_task();

  void switch_state(State state) { ATOMIC_STORE(&state_, state); }

  int get_upper_limit(int64_t &upper_limit_ns);

  // Execution Statistics
  void do_stat(int64_t &traffic);

  int64_t get_fetch_task_count() const { return 1; }

  // is rpc response ready
  bool is_rpc_ready() const { return fetch_log_arpc_.is_rpc_ready(); }

public:
  static void configure(const ObLogConfig & config);

private:
  // Switch fetch log stream definition
  enum KickOutReason
  {
    NONE                         = -1,

    FETCH_LOG_FAIL_ON_RPC        = 1,   // Rpc failure
    FETCH_LOG_FAIL_ON_SERVER     = 2,   // Server failure
    FETCH_LOG_FAIL_IN_DIRECT_MODE = 3,  // Fetch log in direct mode failure

    // TODO Add misslog log reason
    MISSING_LOG_FETCH_FAIL       = 4,   // Fetch missing redo log failure

    // Feedback
    LAGGED_FOLLOWER              = 5,   // Follow lag behind
    LOG_NOT_IN_THIS_SERVER       = 6,   // This log is not served in the server
    LS_OFFLINED                  = 7,   // LS offline

    // Progress timeout, long time to fetch logs
    PROGRESS_TIMEOUT                      = 8,   // Partition fetch log timeout

    NEED_SWITCH_SERVER           = 9,  // There is a higher priority server that actively switch
    DISCARDED                    = 10,  // Partition is discard

    // Feedback
    ARCHIVE_ITER_END_BUT_LS_NOT_EXIST_IN_PALF        = 11,  //same as ARCHIVE_ITER_END_BUT_LS_NOT_EXIST_IN_PALF
  };
  static const char *print_kick_out_reason_(const KickOutReason reason);
  // Determine if the server needs to be blacklisted,
  // NEED_SWITCH_SERVER and DISCARDED do not need to be added, all others do
  bool need_add_into_blacklist_(const KickOutReason reason);

private:
  void handle_when_leave_(const char *leave_reason) const;
  int handle_idle_task_(volatile bool &stop_flag);
  int dispatch_fetch_task_(LSFetchCtx &task,
      KickOutReason dispatch_reason);
  int check_need_fetch_log_(const int64_t limit, bool &need_fetch_log);
  int check_need_fetch_log_with_upper_limit_(bool &need_fetch_log);
  int hibernate_();
  int async_fetch_log_(
      const palf::LSN &req_start_lsn,
      bool &rpc_send_succeed);
  struct KickOutInfo;
  void print_handle_info_(
      FetchLogARpcResult &result,
      const int64_t handle_rpc_time,
      const int64_t read_log_time,
      const int64_t decode_log_entry_time,
      const bool rpc_is_flying,
      const bool is_stream_valid,
      const char *stream_invalid_reason,
      const KickOutInfo &kickout_info,
      const logfetcher::TransStatInfo &tsi,
      const bool need_stop_request);
  bool has_new_fetch_task_() const;
  int process_result_(FetchLogARpcResult &result,
      volatile bool &stop_flag,
      const bool rpc_is_flying,
      KickOutInfo &kickout_info,
      bool &need_hibernate,
      bool &is_stream_valid);
  int handle_fetch_log_task_(volatile bool &stop_flag);
  int handle_fetch_archive_task_(volatile bool &stop_flag);
  void update_fetch_stat_info_(
      FetchLogARpcResult &result,
      const int64_t handle_rpc_time,
      const int64_t read_log_time,
      const int64_t decode_log_entry_time,
      const int64_t flush_time,
      const logfetcher::TransStatInfo &tsi);
  void update_fetch_stat_info_(
      const int64_t fetch_log_cnt,
      const int64_t fetch_log_size,
      const int64_t fetch_and_read_time,
      const int64_t fetch_log_time,
      const int64_t flush_time,
      const logfetcher::TransStatInfo &tsi);
  int handle_fetch_log_result_(FetchLogARpcResult &result,
      volatile bool &stop_flag,
      bool &is_stream_valid,
      const char *&stream_invalid_reason,
      KickOutInfo &kickout_info,
      bool &need_hibernate,
      int64_t &read_log_time,
      int64_t &decode_log_entry_time,
      logfetcher::TransStatInfo &tsi,
      int64_t &flush_time);
  int update_rpc_request_params_();
  int handle_fetch_log_error_(
      const obrpc::ObRpcResultCode &rcode,
      const obrpc::ObCdcLSFetchLogResp &resp,
      KickOutInfo &kickout_info);
  bool exist_(KickOutInfo &kick_out_info,
      const logservice::TenantLSID &tls_id);
  int set_(KickOutInfo &kick_out_info,
      const logservice::TenantLSID &tls_id,
      KickOutReason kick_out_reason);
  int read_group_entry_(
      palf::LogGroupEntry &group_entry,
      palf::LSN &group_start_lsn,
      volatile bool &stop_flag,
      KickOutInfo &kick_out_info,
      logfetcher::TransStatInfo &tsi);
  int read_log_(
      const obrpc::ObCdcLSFetchLogResp &resp,
      volatile bool &stop_flag,
      KickOutInfo &kick_out_info,
      int64_t &read_log_time,
      int64_t &decode_log_entry_time,
      logfetcher::TransStatInfo &tsi);
  int fetch_miss_log_direct_(
      const ObIArray<obrpc::ObCdcLSFetchMissLogReq::MissLogParam> &miss_log_array,
      const int64_t timeout,
      FetchLogSRpc &fetch_log_srpc,
      LSFetchCtx &ls_fetch_ctx);
  int fetch_miss_log_(
      FetchLogSRpc &fetch_srpc,
      IObLogRpc &rpc,
      LSFetchCtx &ctx,
      const ObIArray<obrpc::ObCdcLSFetchMissLogReq::MissLogParam> &miss_log_array,
      const common::ObAddr &svr,
      const int64_t timeout);
  // handle if found misslog while read_log_
  //
  // @param [in] log_entry         LogEntry
  // @param [in] missing_info      MissingLogInfo
  // @param [in] tsi               TransStatInfo
  // @param [out] fail_reason      KickOutReason
  //
  // @retval OB_SUCCESS                   success
  // @retval OB_NEED_RETRY                RPC failed, need retry
  // @retval other error code             fail
  int handle_log_miss_(
      palf::LogEntry &log_entry,
      IObCDCPartTransResolver::MissingLogInfo &missing_info,
      logfetcher::TransStatInfo &tsi,
      volatile bool &stop_flag,
      KickOutReason &fail_reason);
  int handle_miss_record_or_state_log_(
      FetchLogSRpc &fetch_log_srpc,
      IObCDCPartTransResolver::MissingLogInfo &missing_info,
      logfetcher::TransStatInfo &tsi,
      volatile bool &stop_flag,
      KickOutReason &fail_reason);
  int handle_miss_redo_log_(
      FetchLogSRpc &fetch_log_srpc,
      IObCDCPartTransResolver::MissingLogInfo &missing_info,
      logfetcher::TransStatInfo &tsi,
      volatile bool &stop_flag,
      KickOutReason &fail_reason);
  // split all miss_logs by batch
  int build_batch_misslog_lsn_arr_(
      const int64_t fetched_log_idx,
      IObCDCPartTransResolver::MissingLogInfo &missing_log_info,
      ObIArray<obrpc::ObCdcLSFetchMissLogReq::MissLogParam> &batched_misslog_lsn_arr);
  // read batched misslog
  int read_batch_misslog_(
      const obrpc::ObCdcLSFetchLogResp &resp,
      int64_t &fetched_missing_log_cnt,
      logfetcher::TransStatInfo &tsi,
      IObCDCPartTransResolver::MissingLogInfo &missing_info);
  int alloc_fetch_log_srpc_(FetchLogSRpc *&fetch_log_srpc);
  void free_fetch_log_srpc_(FetchLogSRpc *fetch_log_srpc);
  // TODO @bohou handle missing log end

  KickOutReason get_feedback_reason_(const Feedback &feedback) const;
  int check_feedback_(
      const obrpc::ObCdcLSFetchLogResp &resp,
      KickOutInfo &kick_out_info);
  int kick_out_task_(const KickOutInfo &kick_out_info);
  int update_fetch_task_state_(
      KickOutInfo &kick_out_info,
      volatile bool &stop_flag,
      int64_t &flush_time);
  int publish_progress_(LSFetchCtx &task);
  int check_fetch_timeout_(LSFetchCtx &task, KickOutInfo &kick_out_info);
  int check_switch_server_(LSFetchCtx &task, KickOutInfo &kick_out_info);
  int prepare_rpc_request_();
  bool check_need_switch_server_();

private:
  struct KickOutInfo
  {
    logservice::TenantLSID tls_id_;
    KickOutReason kick_out_reason_;

    KickOutInfo() : tls_id_(), kick_out_reason_(NONE) {}
    explicit KickOutInfo(const logservice::TenantLSID &tls_id) :
        tls_id_(tls_id),
        kick_out_reason_(NONE)
    {}
    KickOutInfo(const logservice::TenantLSID &tls_id, KickOutReason kick_out_reason) :
        tls_id_(tls_id),
        kick_out_reason_(kick_out_reason)
    {}

    void reset(const logservice::TenantLSID &tls_id, const KickOutReason kick_out_reason)
    {
      tls_id_ = tls_id;
      kick_out_reason_ = kick_out_reason;
    }

    bool need_kick_out() const { return NONE != kick_out_reason_; }

    uint64_t hash() const
    {
      return tls_id_.hash();
    }

    bool operator == (const KickOutInfo &task) const
    {
      return tls_id_ == task.tls_id_;
    }

    TO_STRING_KV(K_(tls_id), "kick_out_reason", print_kick_out_reason_(kick_out_reason_));
  };

public:
  TO_STRING_KV("type", "FETCH_STREAM",
      "stype", print_fetch_stream_type(stype_),
      "state", print_state(state_),
      K_(svr),
      "upper_limit", NTS_TO_STR(upper_limit_),
      K_(fetch_log_arpc),
      KP_(next),
      KP_(prev));

private:
  bool                          is_inited_;
  State                         state_;                             // Fetch log state
  FetchStreamType               stype_;                             // Stream type
  LSFetchCtx                    *ls_fetch_ctx_;                     // LSFetchCtx
  common::ObAddr                svr_;                               // Target server
  IObLogRpc                     *rpc_;                              // RPC Processor
  IObLSWorker                   *stream_worker_;                    // Stream master
  IFetchLogARpcResultPool       *rpc_result_pool_;                  // RPC result object pool
  PartProgressController        *progress_controller_;              // Progress Controller

  int64_t                       upper_limit_  CACHE_ALIGNED;        // Stream upper limit

  int64_t                       last_switch_server_tstamp_ CACHE_ALIGNED; // Last switching server time

  // Fetch Log Asynchronous RPC
  FetchLogARpc                  fetch_log_arpc_;

  // Statistical Information
  int64_t                       last_stat_time_;
  logfetcher::FetchStatInfo     cur_stat_info_;
  logfetcher::FetchStatInfo     last_stat_info_;
  common::ObByteLock            stat_lock_;      // Mutex lock that statistical information update and access to

private:
  DISALLOW_COPY_AND_ASSIGN(FetchStream);
};

}
}

#endif
