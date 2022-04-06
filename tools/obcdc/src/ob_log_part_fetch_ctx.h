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

#ifndef OCEANBASE_LIBOBLOG_OB_LOG_PART_FETCH_CTX_H__
#define OCEANBASE_LIBOBLOG_OB_LOG_PART_FETCH_CTX_H__

#include "lib/atomic/ob_atomic.h"             // ATOMIC_STORE
#include "lib/net/ob_addr.h"                  // ObAddr
#include "lib/utility/ob_print_utils.h"       // TO_STRING_KV
#include "lib/container/ob_array.h"           // ObArray
#include "common/ob_partition_key.h"          // ObPartitionKey
#include "clog/ob_log_entry.h"                // ObLogEntry
#include "storage/ob_partition_split.h"       // ObPartitionSplitSourceLog, ObPartitionSplitDestLog

#include "ob_log_utils.h"                     // _SEC_
#include "ob_log_part_svr_list.h"             // PartSvrList
#include "ob_log_svr_finder.h"                // SvrFindReq
#include "ob_log_start_log_id_locator.h"      // StartLogIdLocateReq
#include "ob_log_fetcher_heartbeat_worker.h"  // HeartbeatRequest
#include "ob_log_dlist.h"                     // ObLogDList, ObLogDListNode
#include "ob_log_fetch_stream_type.h"         // FetchStreamType
#include "ob_log_part_trans_resolver.h"       // IObLogPartTransResolver
#include "ob_log_part_serve_info.h"           // PartServeInfo
#include "ob_log_part_trans_dispatcher.h"     // PartTransDispatchInfo

namespace oceanbase
{
namespace liboblog
{

/////////////////////////////// PartFetchCtx /////////////////////////////////

class IObLogSvrFinder;
class IObLogFetcherHeartbeatWorker;
class ObLogConfig;
class FetchStream;
class IObLogPartFetchMgr;

struct TransStatInfo;

class PartFetchCtx;
typedef ObLogDListNode<PartFetchCtx> FetchTaskListNode;

// Two-way linked list of fetch log tasks
typedef ObLogDList<PartFetchCtx> FetchTaskList;

class PartFetchCtx : public FetchTaskListNode
{
  static const int64_t DEFAULT_SVR_LIST_UPDATE_MIN_INTERVAL = 1 * _SEC_;
  static const int64_t DEFAULT_LEADER_INFO_UPDATE_MIN_INTERVAL = 200 * _MSEC_;

// variables of class
public:
  // server list update interval
  static int64_t g_svr_list_update_interval;
  // leader info update interval
  static int64_t g_leader_info_update_interval;
  // heartbeat update interval
  static int64_t g_heartbeat_interval;
  // Clear Blacklist History Intervals
  static int64_t g_blacklist_history_clear_interval;

public:
  PartFetchCtx();
  virtual ~PartFetchCtx();

public:
  static void configure(const ObLogConfig &config);

public:
  void reset();
  int init(const common::ObPartitionKey &pkey,
      const int64_t start_tstamp,
      const int64_t start_log_id,
      const int64_t progress_id,
      IObLogPartTransResolver &part_trans_resolver,
      IObLogPartFetchMgr &part_fetch_mgr);

  /// Synchronising data to downstream
  ///
  /// Note that.
  /// 1. The flush() operation is only called by this function and not by other functions, i.e. there is only one flush() globally in oblog
  ////
  //// @retval OB_SUCCESS         Success
  //// @retval OB_IN_STOP_STATE   exit
  //// @retval Other error codes  Fail
  int sync(volatile bool &stop_flag);

  // read log entry
  ///
  /// @param [in] log_entry       Target log entry
  /// @param [out] missing        An array of missing logs and an array of trans_id, the missing logs are guaranteed to be ordered and not duplicated.
  /// @param [out] tsi            Transaction resolution statistics
  /// @param [in] stop_flag       The stop flag
  /// @param [in] need_filter_pg_no_missing_redo_trans
  /// @param [in] log_indexs
  ///
  /// @retval OB_SUCCESS          success
  /// @retval OB_ITEM_NOT_SETTED  redo log incomplete
  /// @retval OB_LOG_NOT_SYNC     Logs are not continuous
  /// @retval OB_IN_STOP_STATE    Entered stop state
  /// @retval Other error codes   Failed
  int read_log(const clog::ObLogEntry &log_entry,
      IObLogPartTransResolver::ObLogMissingInfo &missing,
      TransStatInfo &tsi,
      const bool need_filter_pg_no_missing_redo_trans,
      const IObLogPartTransResolver::ObAggreLogIndexArray &log_indexs,
      volatile bool &stop_flag);

  // Read missing redo log entries
  ///
  /// @param log_entry              Target log entries
  ///
  /// @retval OB_SUCCESS            success
  /// @retval Other error codes     fail
  int read_missing_redo(const clog::ObLogEntry &log_entry,
      const IObLogPartTransResolver::ObTransIDArray &missing_log_trans_id_array);

  /// Offline partition, clear all unexported tasks and issue OFFLINE type tasks
  ///
  /// @retval OB_SUCCESS          success
  /// @retval OB_IN_STOP_STATE    Entered stop state
  /// @retval Other error codes   Failed
  int offline(volatile bool &stop_flag);

  // Check if relevant information needs to be updated
  bool need_update_svr_list() const;
  bool need_update_leader_info() const;
  bool need_locate_start_log_id() const;
  bool need_heartbeat(const int64_t upper_limit);

  // Update server list
  int update_svr_list(IObLogSvrFinder &svr_finder, const bool need_print_info = true);

  // Updata leader info
  int update_leader_info(IObLogSvrFinder &svr_finder);

  // Reset leader info
  void reset_leader_info()
  {
    has_leader_ = false;
    leader_.reset();
  }

  // locatestart log id
  int locate_start_log_id(IObLogStartLogIdLocator &start_log_id_locator);

  /// Update heartbeat information
  /// Note: only updates progress, no heartbeat tasks are generated
  ///
  /// @retval OB_SUCCESS          success
  /// @retval Other error codes   Failed
  int update_heartbeat_info(IObLogFetcherHeartbeatWorker &heartbeater, IObLogSvrFinder &svr_finder);

  /// Update log heartbeat information progress
  ///
  /// @retval OB_SUCCESS          success
  /// @retval OB_LOG_NOT_SYNC     Logs are not continuous
  /// @retval Other error codes   Failed
  int update_log_heartbeat(const uint64_t next_log_id, const int64_t log_progress);

  /// Iterate over the next server in the service log
  /// 1. If the server has completed one round of iteration (all servers have been iterated over), then OB_ITER_END is returned
  /// 2. After returning OB_ITER_END, the list of servers will be iterated over from the beginning next time
  /// 3. If no servers are available, return OB_ITER_END
  ///
  /// @retval OB_SUCCESS          success
  /// @retval OB_ITER_END         Server list iterations complete one round
  /// @retval Other error codes   Failed
  int next_server(common::ObAddr &svr);

  /// Get the number of servers in the server list
  int64_t get_server_count() const { return get_cur_svr_list().count(); }

  const PartSvrList &get_cur_svr_list() const;
  PartSvrList &get_cur_svr_list();
  PartSvrList &get_bak_svr_list();
  void switch_svr_list();

  void mark_svr_list_update_flag(const bool need_update);

  uint64_t hash() const;

  FetchStreamType get_fetch_stream_type() const { return stype_; }
  void set_fetch_stream_type(FetchStreamType stype) { stype_ = stype; }

  IObLogPartTransResolver *get_part_trans_resolver() { return part_trans_resolver_; }

  const common::ObPartitionKey &get_pkey() const { return pkey_; }

  int64_t get_progress_id() const { return progress_id_; }

  // Check if the logs are timed out on the target server
  // i.e. if the logs are not fetched for a long time on the target server, this result will be used as a basis for switching servers
  //
  // Timeout conditions.
  // 1. the progress is not updated on the target server for a long time
  // 2. progress is less than the upper limit
  int check_fetch_timeout(const common::ObAddr &svr,
      const int64_t upper_limit,
      const int64_t fetcher_resume_tstamp,
      bool &is_fetch_timeout,                        // Is the log fetch timeout
      bool &is_fetch_timeout_on_lagged_replica);     // Is the log fetch timeout on a lagged replica

  // Get the progress of a transaction
  // 1. When there is a transaction ready to be sent, the timestamp of the transaction to be sent - 1 is taken as the progress of the sending
  // 2. When no transaction is ready to be sent, the log progress is taken as the progress
  //
  // This value will be used as the basis for sending the heartbeat timestamp downstream
  int get_dispatch_progress(int64_t &progress, PartTransDispatchInfo &dispatch_info);

  struct PartProgress;
  void get_progress_struct(PartProgress &prog) const { progress_.atomic_copy(prog); }
  int64_t get_progress() const { return progress_.get_progress(); }
  uint64_t get_next_log_id() const { return progress_.get_next_log_id(); }
  struct FetchModule;
  const FetchModule &get_cur_fetch_module() const { return fetch_info_.cur_mod_; }
  void update_touch_tstamp_if_progress_beyond_upper_limit(const int64_t upper_limit)
  {
    progress_.update_touch_tstamp_if_progress_beyond_upper_limit(upper_limit);
  }

  double get_tps()
  {
    return NULL == part_trans_resolver_ ? 0 : part_trans_resolver_->get_tps();
  }

  bool is_discarded() const { return ATOMIC_LOAD(&discarded_); }
  void set_discarded() { ATOMIC_STORE(&discarded_, true); }

  // Whether the FetcherDeadPool can clean up the PartFetchCtx
  // whether there are asynchronous requests pending
  // including: heartbeat requests, locate requests, svr_list and leader update requests, etc.
  bool is_in_use() const;

  void print_dispatch_info() const;
  void dispatch_in_idle_pool();
  void dispatch_in_fetch_stream(const common::ObAddr &svr, FetchStream &fs);
  void dispatch_in_dead_pool();

  void dispatch_out(const char *reason)
  {
    fetch_info_.dispatch_out(reason);
  }

  // Get the start fetch log time on the current server
  int get_cur_svr_start_fetch_tstamp(const common::ObAddr &svr,
      int64_t &svr_start_fetch_tstamp) const;

  // add server to blacklist
  ///
  /// @param [in] svr               blacklisted sever
  /// @param [in] svr_service_time  Current server service partition time
  /// @param [in] survival_time     server survival time in blacklist (may be modified based on history)
  ///
  /// @retval OB_SUCCESS            add svr to blacklist success
  /// @retval Other error codes     Fail
  int add_into_blacklist(const common::ObAddr &svr,
      const int64_t svr_service_time,
      int64_t &survival_time);

  // Determine if the server needs to be switched
  //
  /// @param [in]  cur_svr  The fetch log stream where the partition task is currently located - target server
  ///
  /// @retval true
  /// @retval false
  bool need_switch_server(const common::ObAddr &cur_svr);

  // Determining if the split is complete
  bool is_split_done(const uint64_t split_log_id, const int64_t split_log_ts) const;

  // Source partition split processing complete, advance this partition status
  int handle_when_src_split_done(volatile bool &stop_flag);

  // Internal member functions
private:
  int dispatch_heartbeat_if_need_();
  // Periodic deletion of history
  int clear_blacklist_history_(IBlackList::SvrHistoryArray &clear_svr_array);
  int init_locate_req_svr_list_(StartLogIdLocateReq &req, PartSvrList &svr_list);
  int launch_heartbeat_request_(IObLogFetcherHeartbeatWorker &heartbeater,
      IObLogSvrFinder &svr_finder);
  int handle_heartbeat_request_(IObLogSvrFinder &svr_finder);
  int dispatch_(volatile bool &stop_flag, int64_t &pending_trans_count);
  int handle_offline_partition_log_(const clog::ObLogEntry &log_entry, volatile bool &stop_flag);
  int handle_split_src_log_(const clog::ObLogEntry &log_entry, volatile bool &stop_flag);
  int prepare_split_(const storage::ObPartitionSplitSourceLog &split_src_log,
      bool &split_in_process);
  int process_split_(volatile bool &stop_flag);
  int mark_split_done_();
  int process_split_dest_part_(volatile bool &stop_flag);
  int clear_wait_state_(bool &old_state_mark_wait, bool &old_state_in_split);
  int handle_split_dst_log_(const clog::ObLogEntry &log_entry);
  int mark_state_wait_from_normal_();
  int check_src_part_split_state_(const storage::ObPartitionSplitDestLog &split_dst_log,
      bool &src_part_split_done);
  int mark_state_normal_from_wait_();

public:
  ///////////////////////////////// PartProgress /////////////////////////////////
  //
  // At the moment of startup, only the startup timestamp of the partition is known, not the specific log progress, using the following convention.
  // 1. set the start timestamp to the global progress: progress
  // 2. next_log_id and log_progress are invalid
  // 3. wait for the start log id locator to look up the start log id and set it to next_log_id
  // 4. start log id may have fallback, during the fallback log, the log_progress is updated,
  // since the log progress is less than the global progress, the global progress remains unchanged; but touch_tstamp remains updated
  struct PartProgress
  {
    // Global progress and update timestamps
    // Update scenarios.
    // 1. If the global progress is advanced when the log progress or heartbeat progress is updated, the global progress value should be updated, along with the corresponding update timestamp
    // 2. If the global progress is greater than the upper limit, the corresponding update timestamp should be updated in real time to avoid future misconceptions that the progress update has timed out
    //
    // In general, the progress value is equal to either the log progress or the heartbeat progress; the following cases will cause the progress value to differ from both the log progress and the heartbeat progress
    // 1. next_log_id = 8
    // 2. next_served_log_id = 8
    // 3. log_progress = 10:00
    // 4. next_served_tstamp = 11:00
    // 5. progress = 11:00
    // 6. is_log_progress = false
    // i.e. the timestamp of log #7 is less than or equal to 10:00 and no logs were written between 10:00 and 11:00
    // The current global progress is 11:00, representing the heartbeat progress
    //
    // At 11:01 the division wrote log #8 and the individual progress became.
    // 1. next_log_id = 8
    // 2. next_served_log_id = 9
    // 3. log_progress = 10:00
    // 4. next_served_tstamp = 11:01
    // 5. progress = 11:00
    // 6. is_log_progress = false
    // That is, the log progress remains unchanged, the heartbeat progress advances by one log, and the global progress remains unchanged at 11 points, which is different from both the log and heartbeat progress
    int64_t   progress_;
    int64_t   touch_tstamp_;
    bool      is_log_progress_;   // Current progress represents log progress or heartbeat progress

    // Log progress
    // 1. log_progress normally refers to the lower bound of the next log timestamp
    // 2. log_progress and next_log_id are invalid at startup
    uint64_t  next_log_id_;         // next log id
    int64_t   log_progress_;        // log progress
    int64_t   log_touch_tstamp_;    // Log progress last update time

    // heartbeat progress
    uint64_t  next_served_log_id_;  // The next log ID of the server-side service
    int64_t   next_served_tstamp_;  // Lower bound on the next log timestamp of the server-side service
    int64_t   hb_touch_tstamp_;     // heartbeat progress last update time

    // Lock: Keeping read and write operations atomic
     mutable common::ObByteLock  lock_;

    PartProgress() { reset(); }
    ~PartProgress() { reset(); }

    TO_STRING_KV(
        "progress", TS_TO_STR(progress_),
        "touch_tstamp", TS_TO_STR(touch_tstamp_),
        K_(is_log_progress),
        K_(next_log_id),
        "log_progress", TS_TO_STR(log_progress_),
        "log_touch_tstamp", TS_TO_STR(log_touch_tstamp_),
        K_(next_served_log_id),
        "next_served_tstamp", TS_TO_STR(next_served_tstamp_),
        "hb_touch_tstamp", TS_TO_STR(hb_touch_tstamp_));

    void reset();

    // Note: start_log_id may be invalid, but start_tstamp should be valid
    void reset(const uint64_t start_log_id, const int64_t start_tstamp);

    uint64_t get_next_log_id() const { return next_log_id_; }
    void set_next_log_id(const uint64_t start_log_id) { next_log_id_ = start_log_id; }
    uint64_t get_next_served_log_id() const { return next_served_log_id_; }
    // Get heartbeat update timestamp
    int64_t get_hb_touch_tstamp() const { return hb_touch_tstamp_; }

    // Get current progress
    int64_t get_progress() const { return progress_; }
    int64_t get_touch_tstamp() const { return touch_tstamp_; }
    bool is_log_progress() const { return is_log_progress_; }

    // Copy the entire progress item to ensure atomicity
    void atomic_copy(PartProgress &prog) const;

    // Update the touch timestamp if progress is greater than the upper limit
    void update_touch_tstamp_if_progress_beyond_upper_limit(const int64_t upper_limit);

    // Whether a heartbeat request needs to be initiated
    bool need_heartbeat(const int64_t upper_limit, const int64_t hb_interval) const;

    // Update Heartbeat Progress
    void update_heartbeat_progress(const uint64_t ns_log_id, const int64_t ns_tstamp);

    // Update log progress
    // Update both the log ID and the log progress
    // Require log ID to be updated sequentially, otherwise return OB_LOG_NOT_SYNC
    //
    // Update log progress once for each log parsed to ensure sequential update
    int update_log_progress(const uint64_t new_next_log_id, const int64_t log_progress);

    // Update the log heartbeat
    // Update log progress only, keeping the log ID unchanged
    // Require that the log ID provided matches the log ID saved, otherwise return OB_LOG_NOT_SYNC
    //
    // The usage scenario is to update the progress when the server side returns the lower bound timestamp of the next log
    int update_log_heartbeat(const uint64_t next_log_id, const int64_t log_progress);

  private:
    void update_progress_();
  };

public:
  ///////////////////////////////// FetchModule /////////////////////////////////
  // Module where the partition is located
  struct FetchModule
  {
    enum ModuleName
    {
      FETCH_MODULE_NONE = 0,          // Not part of any module
      FETCH_MODULE_IDLE_POOL = 1,     // IDLE POOL module
      FETCH_MODULE_FETCH_STREAM = 2,  // FetchStream module
      FETCH_MODULE_DEAD_POOL = 3,     // DEAD POOL module
    };

    ModuleName      module_;     // module name

    // FetchStream info
    common::ObAddr  svr_;
    void            *fetch_stream_;       // Pointer to FetchStream, object may be invalid, cannot reference content
    int64_t         start_fetch_tstamp_;

    void reset();
    void reset_to_idle_pool();
    void reset_to_fetch_stream(const common::ObAddr &svr, FetchStream &fs);
    void reset_to_dead_pool();
    int64_t to_string(char *buffer, const int64_t size) const;
  };

  ///////////////////////////////// FetchInfo /////////////////////////////////
  // Fetching log stream information
  struct FetchInfo
  {
    FetchModule     cur_mod_;               // The module to which currently belong to

    FetchModule     out_mod_;               // module that dispatch out from
    const char      *out_reason_;           // reason for dispatch out

    FetchInfo() { reset(); }

    void reset();
    void dispatch_in_idle_pool();
    void dispatch_in_fetch_stream(const common::ObAddr &svr, FetchStream &fs);
    void dispatch_in_dead_pool();
    void dispatch_out(const char *reason);

    // Get the start fetch time  of the log on the current server
    // Requires FETCH_STREAM for the fetch log module; requiring the server to match
    int get_cur_svr_start_fetch_tstamp(const common::ObAddr &svr,
        int64_t &svr_start_fetch_ts) const;

    TO_STRING_KV(K_(cur_mod), K_(out_mod), K_(out_reason));
  };

private:
  // state convert:
  // 1. STATE_NORMAL ->  STATE_WAIT
  //    Received "Split target partition" log, entered wait state, blocked sending data
  //
  // 2. STATE_WAIT   ->  STATE_NORMAL
  //    The "split source partition" has been processed and is ready to send data
  //
  // 3. STATE_WAIT   ->  STATE_WAIT_AND_SPLIT_IN_PROCESS
  //    The "split target partition" receives the "split source partition" log in the wait state and enters the split processing state
  //
  // 4. STATE_WAIT_AND_SPLIT_IN_PROCESS  -> STATE_SPLIT_IN_PROCESS
  //    The "split source partition" has been processed and the partition is officially in the split processing state
  //
  // 5. STATE_NORMAL ->  STATE_SPLIT_IN_PROCESS
  //    Partition receives "Split Source Partition" log and enters split processing state
  //
  // 6. STATE_SPLIT_IN_PROCESS -> STATE_SPLIT_DONE
  //    split done
  enum FetchState
  {
    // noral state
    STATE_NORMAL = 0,

    // wait state
    // Wait for source partition splitting to complete
    STATE_WAIT = 1,

    // partition spliting(dealing with partition split)
    STATE_SPLIT_IN_PROCESS = 2,

    // Waiting state while entering a split state
    // Wait for source partition splitting to complete，Meanwhile split log (self) is ready, ready to split
    //
    // STATE_WAIT & STATE_SPLIT_IN_PROCESS
    STATE_WAIT_AND_SPLIT_IN_PROCESS = 3,

    // splitting completion state
    // final state, no further transitions to other states
    STATE_SPLIT_DONE = 4,

    STATE_MAX
  };

  // Is it in waiting status
  bool is_in_wait_state(const int state)
  {
    return (STATE_WAIT_AND_SPLIT_IN_PROCESS == state) || (STATE_WAIT == state);
  }

  const char *print_state(int state) const
  {
    const char *str = "UNKNOWN";
    switch (state) {
      case STATE_NORMAL:
        str = "NORMAL";
        break;
      case STATE_WAIT:
        str = "STATE_WAIT";
        break;
      case STATE_SPLIT_IN_PROCESS:
        str = "SPLIT_IN_PROCESS";
        break;
      case STATE_WAIT_AND_SPLIT_IN_PROCESS:
        str = "WAIT_AND_SPLIT_IN_PROCESS";
        break;
      case STATE_SPLIT_DONE:
        str = "SPLIT_DONE";
        break;
      default:
        str = "UNKNOWN";
        break;
    }
    return str;
  }

public:
  TO_STRING_KV("type", "FETCH_TASK",
      "stype", print_fetch_stream_type(stype_),
      K_(state),
      "state_str", print_state(state_),
      K_(discarded),
      K_(pkey),
      K_(serve_info),
      K_(progress_id),
      KP_(part_fetch_mgr),
      KP_(part_trans_resolver),
      K_(last_sync_progress),
      K_(progress),
      K_(fetch_info),
      "blacklist_svr_count", blacklist_.count(),
      K_(blacklist),
      "svr_count", get_server_count(),
      "svr_list", get_cur_svr_list(),
      K_(svr_list_need_update),
      "svr_list_last_update_tstamp", TS_TO_STR(svr_list_last_update_tstamp_),
      "svr_find_req",
      svr_find_req_.is_state_idle() ? "IDLE" : to_cstring(svr_find_req_),      // won't print IDLE state
      "start_log_id_locate_req",
      start_log_id_locate_req_.is_state_idle() ? "IDLE" : to_cstring(start_log_id_locate_req_),
      K_(leader),
      K_(has_leader),
      "leader_last_update_tstamp", TS_TO_STR(leader_last_update_tstamp_),
      "leader_find_req", leader_find_req_.is_state_idle() ? "IDLE" : to_cstring(leader_find_req_),
      "heartbeat_req", heartbeat_req_.is_state_idle() ? "IDLE" : to_cstring(heartbeat_req_),
      "heartbeat_last_update_tstamp", TS_TO_STR(heartbeat_last_update_tstamp_),
      K_(split_dest_array),
      KP_(next),
      KP_(prev));

private:
  FetchStreamType         stype_;
  FetchState              state_;
  bool                    discarded_; // partition is deleted or not

  common::ObPartitionKey  pkey_;
  PartServeInfo           serve_info_;
  int64_t                 progress_id_;             // Progress Unique Identifier
  IObLogPartFetchMgr      *part_fetch_mgr_;         // PartFetchCtx manager
  IObLogPartTransResolver *part_trans_resolver_;    // Partitioned transaction resolvers, one for each partition exclusively

  // Last synced progress
  int64_t                 last_sync_progress_ CACHE_ALIGNED;

  // partition progress
  PartProgress            progress_;

  /// fetch log info
  FetchInfo               fetch_info_;

  //////////// server list ////////////
  // Update timing.
  // 1. when the svr list is exhausted, the svr_list_need_update_ flag is set to true, check that the flag is true to require an update
  // 2. periodic update: to ensure the svr list is always up to date, it needs to be updated periodically
  // 3. ensure that updates are not too frequent
  //
  // Implementation options
  // 1. In order to update the server list asynchronously, a dual-server list mechanism is used, with one serving as the official list and the other as a "standby list" to store the updated data; after a successful update, the standby list is switched to the official list atomically
  // 2.
  // 2. blacklist is a partition-level blacklist of servers that are not available in the partition, the purpose of which is to ensure that each access is filtered for servers that are not currently available.
  BlackList               blacklist_;
  PartSvrList             svr_list_[2];
  int64_t                 cur_svr_list_idx_;
  bool                    svr_list_need_update_;
  int64_t                 svr_list_last_update_tstamp_;

  /// Update server list request
  SvrFindReq              svr_find_req_;

  /// start log id locator request
  StartLogIdLocateReq     start_log_id_locate_req_;

  /////////// Leader info ////////////
  common::ObAddr          leader_;
  bool                    has_leader_;
  int64_t                 leader_last_update_tstamp_;

  /// request to update leader info
  LeaderFindReq           leader_find_req_;

  /// heartbeat request
  HeartbeatRequest        heartbeat_req_;
  int64_t                 heartbeat_last_update_tstamp_;

  // partition arrays of Split target
  typedef common::ObArray<common::ObPartitionKey> PartArray;
  PartArray               split_dest_array_;

private:
  DISALLOW_COPY_AND_ASSIGN(PartFetchCtx);
};

//////////////////////////////////////// PartFetchInfoForPrint //////////////////////////////////

// For printing fetch log information
struct PartFetchInfoForPrint
{
  double                      tps_;
  bool                        is_discarded_;
  common::ObPartitionKey      pkey_;
  PartFetchCtx::PartProgress  progress_;
  PartFetchCtx::FetchModule   fetch_mod_;
  int64_t                     dispatch_progress_;
  PartTransDispatchInfo       dispatch_info_;

  PartFetchInfoForPrint();
  int init(PartFetchCtx &ctx);

  // for printing fetch progress
  void print_fetch_progress(const char *description,
      const int64_t idx,
      const int64_t array_cnt,
      const int64_t cur_time) const;

  // for printing partition dispatch progress
  void print_dispatch_progress(const char *description,
      const int64_t idx,
      const int64_t array_cnt,
      const int64_t cur_time) const;

  int64_t get_progress() const { return progress_.get_progress(); }
  int64_t get_dispatch_progress() const { return dispatch_progress_; }

  TO_STRING_KV(K_(tps), K_(is_discarded), K_(pkey), K_(progress), K_(fetch_mod),
      K_(dispatch_progress), K_(dispatch_info));
};

} // namespace liboblog
} // namespace oceanbase
#endif /* OCEANBASE_LIBOBLOG_OB_LOG_PART_FETCH_CTX_H__ */
