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

#ifndef OCEANBASE_LIBOBCDC_OB_LOG_LS_FETCH_CTX_H__
#define OCEANBASE_LIBOBCDC_OB_LOG_LS_FETCH_CTX_H__

#include "lib/atomic/ob_atomic.h"             // ATOMIC_STORE
#include "lib/net/ob_addr.h"                  // ObAddr
#include "lib/utility/ob_print_utils.h"       // TO_STRING_KV
#include "lib/container/ob_array.h"           // ObArray
#include "logservice/palf/lsn.h"              // LSN
#include "logservice/logrouteservice/ob_log_route_service.h" // ObLogRouteService
#include "logservice/palf/log_iterator_storage.h"
#include "logservice/palf/palf_iterator.h"
#include "logservice/ob_log_base_header.h"
#include "logservice/data_dictionary/ob_data_dict_iterator.h"  // ObDataDictIterator
#include "logservice/restoreservice/ob_remote_log_iterator.h"  // ObRemoteLogIterator
#include "logservice/restoreservice/ob_remote_log_source.h"    // ObRemoteLogParent
#include "ob_log_fetching_mode.h"
#include "ob_log_utils.h"                     // _SEC_
#include "ob_log_start_lsn_locator.h"         // StartLSNLocateReq
#include "ob_log_dlist.h"                     // ObLogDList, ObLogDListNode
#include "ob_log_fetch_stream_type.h"         // FetchStreamType
#include "ob_cdc_part_trans_resolver.h"       // IObCDCPartTransResolver
#include "logservice/logfetcher/ob_log_part_serve_info.h"           // logfetcher::PartServeInfo
#include "ob_log_part_trans_dispatcher.h"     // PartTransDispatchInfo
#include "logservice/common_util/ob_log_ls_define.h"                 // logservice::TenantLSID
#include "logservice/logfetcher/ob_log_fetcher_start_parameters.h"  // logfetcher::ObLogFetcherStartParameters

namespace oceanbase
{
namespace libobcdc
{

using logservice::ObRemoteLogParent;
using logservice::ObRemoteLogGroupEntryIterator;
/////////////////////////////// LSFetchCtx /////////////////////////////////

class ObLogConfig;
class FetchStream;
class IObLogLSFetchMgr;

struct TransStatInfo;

class IObLogFetcher;
class LSFetchCtx;
typedef ObLogDListNode<LSFetchCtx> FetchTaskListNode;

// Two-way linked list of fetch log tasks
typedef ObLogDList<LSFetchCtx> FetchTaskList;

class LSFetchCtxGetSourceFunctor
{
public:
  explicit LSFetchCtxGetSourceFunctor(LSFetchCtx &ctx): ls_fetch_ctx_(ctx) {}
  int operator()(const ObLSID &id, logservice::ObRemoteSourceGuard &guard);
private:
  LSFetchCtx &ls_fetch_ctx_;
};

class LSFetchCtxUpdateSourceFunctor
{
public:
  explicit LSFetchCtxUpdateSourceFunctor(LSFetchCtx &ctx): ls_fetch_ctx_(ctx) {}
  int operator()(const ObLSID &id, ObRemoteLogParent *source);
private:
  LSFetchCtx &ls_fetch_ctx_;
};

// LSFetchCtx
// LS fetch context, managing the fetch status of LS in the fetcher module
class LSFetchCtx : public FetchTaskListNode
{
public:
  LSFetchCtx();
  virtual ~LSFetchCtx();

public:
  static void configure(const ObLogConfig &config);

public:
  void reset();
  int init(
      const logservice::TenantLSID &tls_id,
      const logfetcher::ObLogFetcherStartParameters &start_parameters,
      const bool is_loading_data_dict_baseline_data,
      const int64_t progress_id,
      const ClientFetchingMode fetching_mode,
      const ObBackupPathString &archive_dest_str,
      IObCDCPartTransResolver &part_trans_resolver,
      IObLogLSFetchMgr &ls_fetch_mgr);

  int append_log(const char *buf, const int64_t buf_len);
  void reset_memory_storage();
  int get_next_group_entry(palf::LogGroupEntry &group_entry, palf::LSN &lsn);
  int get_next_remote_group_entry(
      palf::LogGroupEntry &group_entry,
      palf::LSN &lsn,
      const char *&buf,
      int64_t &buf_size);
  int get_log_entry_iterator(palf::LogGroupEntry &group_entry,
      palf::LSN &start_lsn,
      palf::MemPalfBufferIterator &entry_iter);

  /// Synchronising data to downstream
  ///
  /// Note that.
  /// 1. The flush() operation is only called by this function and not by other functions, i.e. there is only one flush() globally in oblog
  ////
  //// @retval OB_SUCCESS         Success
  //// @retval OB_IN_STOP_STATE   exit
  //// @retval Other error codes  Fail
  int sync(volatile bool &stop_flag);

  /// read palf::LogEntry
  ///
  /// @param [in]  log_entry       Target log entry
  /// @param [in]  lsn             lsn of log_entry
  /// @param [out] missing         missing_log_info generated while resolving log_entry
  /// @param [out] tsi             Transaction resolution statistics
  /// @param [in]  stop_flag       The stop flag
  ///
  /// @retval OB_SUCCESS          success
  /// @retval OB_ITEM_NOT_SETTED  redo log incomplete
  /// @retval OB_IN_STOP_STATE    Entered stop state
  /// @retval Other error codes   Failed
  int read_log(
      const palf::LogEntry &log_entry,
      const palf::LSN &lsn,
      IObCDCPartTransResolver::MissingLogInfo &missing,
      logfetcher::TransStatInfo &tsi,
      volatile bool &stop_flag);

  /// Read missing redo log entries(only support trans_log_entry)
  ///
  /// @param [in]   log_entry         Target log entry
  /// @param [in]   log_lsn           LSN of target log_entry
  /// @param [out]  tsi
  /// @param [out]  missing           MissingLogInfo found while resolving log_entry
  ///
  /// @retval OB_SUCCESS            success
  /// @retval Other error codes     fail
  /// @note won't throw OB_ITEM_NOT_SETTED if found missing_log, invoker should check missing_info is_empty or not
  int read_miss_tx_log(
      const palf::LogEntry &log_entry,
      const palf::LSN &lsn,
      logfetcher::TransStatInfo &tsi,
      IObCDCPartTransResolver::MissingLogInfo &missing);

  int update_progress(
      const palf::LogGroupEntry &group_entry,
      const palf::LSN &group_entry_lsn);

  /// Offline LS, clear all unexported tasks and issue OFFLINE type tasks
  ///
  /// @retval OB_SUCCESS          success
  /// @retval OB_IN_STOP_STATE    Entered stop state
  /// @retval Other error codes   Failed
  int offline(volatile bool &stop_flag);

  // Check if relevant information needs to be updated
  bool need_update_svr_list();
  bool need_locate_start_lsn() const;
  bool need_locate_end_lsn() const;

  // Update server list
  int update_svr_list(const bool need_print_info = true);

  // locate start LSN
  int locate_start_lsn(IObLogStartLSNLocator &start_lsn_locator);

  // locate end LSN
  int locate_end_lsn(IObLogStartLSNLocator &start_lsn_locator);

  int get_large_buffer_pool(archive::LargeBufferPool *&large_buffer_pool);
  int get_log_ext_handler(logservice::ObLogExternalStorageHandler *&log_ext_handler);
  ObRemoteLogParent *get_archive_source() { return source_; }

  int init_remote_iter();

  bool is_remote_iter_inited() { return remote_iter_.is_init(); }

  void reset_remote_iter() {
    remote_iter_.update_source_cb();
    remote_iter_.reset();
  }

  /// Iterate over the next server in the service log
  /// 1. If the server has completed one round of iteration (all servers have been iterated over), then OB_ITER_END is returned
  /// 2. After returning OB_ITER_END, the list of servers will be iterated over from the beginning next time
  /// 3. If no servers are available, return OB_ITER_END
  /// will set principal svr of specified log id into LSFetchCtx::CtxDesc::cur_principal_svr_
  ///
  /// @param  [out]  request_svr  include next availbale svr(constains desired logs) ans actual rpc execute svr
  //                              to fetch log (log may serve by agent_svr in ofs-single-zone mode),
  ///			          will equal to svr.get_addr() if doesn't find agent_svr.
  ///
  /// @retval OB_SUCCESS          success
  /// @retval OB_ITER_END         Server list iterations complete one round
  /// @retval Other error codes   Failed
  int next_server(common::ObAddr &request_svr);

  void mark_svr_list_update_flag(const bool need_update);

  uint64_t hash() const;

  FetchStreamType get_fetch_stream_type() const { return stype_; }
  void set_fetch_stream_type(FetchStreamType stype) { stype_ = stype; }

  IObCDCPartTransResolver *get_part_trans_resolver() { return part_trans_resolver_; }

  const logservice::TenantLSID &get_tls_id() const { return tls_id_; }

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
      bool &is_fetch_timeout);                        // Is the log fetch timeout

  // Get the progress of a transaction
  // 1. When there is a transaction ready to be sent, the timestamp of the transaction to be sent - 1 is taken as the progress of the sending
  // 2. When no transaction is ready to be sent, the log progress is taken as the progress
  //
  // This value will be used as the basis for sending the heartbeat timestamp downstream
  int get_dispatch_progress(int64_t &progress, logfetcher::PartTransDispatchInfo &dispatch_info);

  struct LSProgress;
  void get_progress_struct(LSProgress &prog) const { progress_.atomic_copy(prog); }
  int64_t get_progress() const { return progress_.get_progress(); }
  const palf::LSN &get_next_lsn() const { return progress_.get_next_lsn(); }
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

  // Whether the FetcherDeadPool can clean up the LSFetchCtx
  // whether there are asynchronous requests pending
  // including: heartbeat requests, locate requests, svr_list and leader update requests, etc.
  bool is_in_use() const;

  void print_dispatch_info() const;
  void dispatch_in_idle_pool();
  void dispatch_in_fetch_stream(const common::ObAddr &svr, FetchStream &fs);
  void dispatch_in_dead_pool();

  bool is_in_fetching_log() const { return FETCHING_LOG == ATOMIC_LOAD(&state_); }
  void set_not_in_fetching_log() { ATOMIC_SET(&state_, NOT_FETCHING_LOG); }

  void dispatch_out(const char *reason)
  {
    fetch_info_.dispatch_out(reason);
  }

  ClientFetchingMode get_fetching_mode() const { return fetching_mode_; }

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

  int64_t get_fetched_log_size() { return ATOMIC_LOAD(&fetched_log_size_); }

  // Internal member functions
private:
  int init_group_iterator_(const palf::LSN &start_lsn);

  int init_archive_dest_(const ObBackupPathString &archve_dest_str,
      ObBackupDest &archive_dest);

  int init_archive_source_(const ObBackupDest &archive_dest);

  int dispatch_heartbeat_if_need_();

  static const int64_t DEFAULT_SERVER_NUM = 16;
  typedef common::ObSEArray<common::ObAddr, DEFAULT_SERVER_NUM> LocateSvrList;
  // Periodic deletion of history
  int init_locate_req_svr_list_(StartLSNLocateReq &req, LocateSvrList &svr_list);
  int dispatch_(volatile bool &stop_flag, int64_t &pending_trans_count);
  int handle_offline_ls_log_(const palf::LogEntry &log_entry, volatile bool &stop_flag);
  int deserialize_log_entry_base_header_(const char *buf, const int64_t buf_len, int64_t &pos,
                                         logservice::ObLogBaseHeader &log_base_header);


public:
  ///////////////////////////////// LSProgress /////////////////////////////////
  //
  // At the moment of startup, only the startup timestamp of the LS is known, not the specific log progress, using the following convention.
  // 1. set the start timestamp to the log_progress
  // 2. next_lsn is invalid
  // 3. wait for the start lsn locator to look up the start_lsn and set it to next_lsn
  // 4. start lsn may have fallback, during fetch the fallback log, the log_progress is not updated,
  // since the lag log progress is less than the start timestamp, the log progress remains unchanged; but touch_tstamp remains updated
  struct LSProgress
  {
    // Log progress
    // 1. log_progress normally refers to the lower bound of the next log timestamp
    // 2. log_progress and next_lsn are invalid at startup
    palf::LSN next_lsn_;            // next LSN
    int64_t   log_progress_;        // log progress(nanosecond)
    int64_t   log_touch_tstamp_;    // Log progress last update time

    // Lock: Keeping read and write operations atomic
     mutable common::ObByteLock  lock_;

    LSProgress() { reset(); }
    ~LSProgress() { reset(); }

    TO_STRING_KV(K_(next_lsn),
        "log_progress", NTS_TO_STR(log_progress_),
        "log_touch_tstamp", TS_TO_STR(log_touch_tstamp_));

    void reset();
    // Note: start_lsn may be invalid, but start_tstamp_ns should be valid
    void reset(const palf::LSN start_lsn, const int64_t start_tstamp_ns);

    const palf::LSN &get_next_lsn() const { return next_lsn_; }
    void set_next_lsn(const palf::LSN start_lsn) { next_lsn_ = start_lsn; }

    // Get current progress
    int64_t get_progress() const { return log_progress_; }
    int64_t get_touch_tstamp() const { return log_touch_tstamp_; }

    // Copy the entire progress item to ensure atomicity
    void atomic_copy(LSProgress &prog) const;

    // Update the touch timestamp if progress is greater than the upper limit
    void update_touch_tstamp_if_progress_beyond_upper_limit(const int64_t upper_limit);

    // Update log progress
    // Update both the LSN and the log progress
    // Require LSN to be updated sequentially, otherwise return OB_LOG_NOT_SYNC
    //
    // Update log progress once for each log parsed to ensure sequential update
    int update_log_progress(const palf::LSN &new_next_lsn,
        const int64_t new_lsn_length,
        const int64_t log_progress);
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

    void set_module(ModuleName module_name) { ATOMIC_SET(&module_, module_name); }
    ModuleName get_module() const { return ATOMIC_LOAD(&module_); }
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

    bool is_from_idle_to_idle() const
    {
      return (FetchModule::ModuleName::FETCH_MODULE_IDLE_POOL == cur_mod_.module_)
        && (FetchModule::ModuleName::FETCH_MODULE_IDLE_POOL == out_mod_.module_);
    }

    // Get the start fetch time  of the log on the current server
    // Requires FETCH_STREAM for the fetch log module; requiring the server to match
    int get_cur_svr_start_fetch_tstamp(const common::ObAddr &svr,
        int64_t &svr_start_fetch_ts) const;

    TO_STRING_KV(K_(cur_mod), K_(out_mod), K_(out_reason));
  };

  ///////////////////////////////// FetchCtxDesc /////////////////////////////////
  // for detail info descript LSFetchCtx
  struct LSFetchCtxDesc
  {
    void reset() {}
  };

private:
  enum FetchState
  {
    // noral state
    STATE_NORMAL = 0,

    FETCHING_LOG = 1,
    NOT_FETCHING_LOG = 2,

    STATE_MAX
  };

  const char *print_state(int state) const
  {
    const char *str = "UNKNOWN";
    switch (state) {
      case STATE_NORMAL:
        str = "NORMAL";
        break;
      case FETCHING_LOG:
        str = "FETCHING_LOG";
        break;
      case NOT_FETCHING_LOG:
        str = "NOT_FETCHING_LOG";
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
      K_(tls_id),
      K_(serve_info),
      K_(progress_id),
      KP_(ls_fetch_mgr),
      KP_(part_trans_resolver),
      KP_(source),
      K_(remote_iter),
      K_(last_sync_progress),
      K_(progress),
      K_(fetch_info),
      K_(svr_list_need_update),
      "start_log_id_locate_req",
      start_lsn_locate_req_.is_state_idle() ? "IDLE" : to_cstring(start_lsn_locate_req_),
      KP_(next),
      KP_(prev));

private:
  int locate_lsn_(
      StartLSNLocateReq &lsn_locate_req,
      const int64_t tstamp_ns,
      const bool is_start_tstamp,
      IObLogStartLSNLocator &start_lsn_locator);
  int set_end_lsn_and_init_dict_iter_(const palf::LSN &start_lsn);
  int get_log_route_service_(logservice::ObLogRouteService *&log_route_service);
  int get_large_buffer_pool_(archive::LargeBufferPool *&large_buffer_pool);
#ifdef OB_BUILD_LOG_STORAGE_COMPRESS
  int decompress_log_(const char *buf, const int64_t buf_len, int64_t pos,
                      char *&decompression_buf, int64_t &decompressed_len,
                      IObLogFetcher *fetcher);
#endif

private:
  FetchStreamType         stype_;
  FetchState              state_;
  ClientFetchingMode      fetching_mode_;
  bool                    discarded_; // LS is deleted or not
  bool                    is_loading_data_dict_baseline_data_;

  logservice::TenantLSID  tls_id_;
  logfetcher::PartServeInfo serve_info_;
  int64_t                 progress_id_;             // Progress Unique Identifier
  IObLogLSFetchMgr        *ls_fetch_mgr_;           // LSFetchCtx manager
  IObCDCPartTransResolver *part_trans_resolver_;    // Partitioned transaction resolvers, one for each partition exclusively

  ObRemoteLogParent             *source_;
  ObRemoteLogGroupEntryIterator remote_iter_;

  // Last synced progress
  int64_t                 last_sync_progress_ CACHE_ALIGNED;

  // LS progress
  LSProgress              progress_;

  // Contains data dictionary in log info(Tenant SYS LS)
  logfetcher::ObLogFetcherStartParameters start_parameters_;

  /// fetch log info
  FetchInfo               fetch_info_;

  bool                    svr_list_need_update_;

  /// start lsn locator request
  StartLSNLocateReq       start_lsn_locate_req_;
  /// end lsn locator request
  StartLSNLocateReq       end_lsn_locate_req_;

  palf::MemoryStorage     mem_storage_;
  palf::MemPalfGroupBufferIterator group_iterator_;

  datadict::ObDataDictIterator data_dict_iterator_;

  /////////// Stat ////////////
  int64_t                 fetched_log_size_;

  // extent description of LSFetchCtx
  LSFetchCtxDesc          ctx_desc_;

private:
  DISALLOW_COPY_AND_ASSIGN(LSFetchCtx);
};

//////////////////////////////////////// LSFetchInfoForPrint //////////////////////////////////

// For printing fetch log information
struct LSFetchInfoForPrint
{
  double                      tps_;
  bool                        is_discarded_;
  logservice::TenantLSID      tls_id_;
  LSFetchCtx::LSProgress      progress_;
  LSFetchCtx::FetchModule     fetch_mod_;
  int64_t                     dispatch_progress_;
  logfetcher::PartTransDispatchInfo dispatch_info_;

  LSFetchInfoForPrint();
  int init(LSFetchCtx &ctx);

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

  TO_STRING_KV(K_(tps), K_(is_discarded), K_(tls_id), K_(progress), K_(fetch_mod),
      K_(dispatch_progress), K_(dispatch_info));
};

} // namespace libobcdc
} // namespace oceanbase
#endif /* OCEANBASE_LIBOBCDC_OB_LOG_PART_FETCH_CTX_H__ */
