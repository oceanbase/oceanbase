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
 * TransLogResolver: identify TransLog type and handle based on the type
 */

#ifndef OCEANBASE_LIBOBCDC_PART_TRANS_RESOLVER_H__
#define OCEANBASE_LIBOBCDC_PART_TRANS_RESOLVER_H__

#include "share/ob_ls_id.h"                       // ObLSID
#include "storage/tx/ob_tx_log.h"                 // transaction::ObTxLog
#include "logservice/palf/lsn.h"                  // LSN
#include "logservice/palf/log_entry.h"            // palf::LogEntry

#include "ob_log_trans_log.h"                     // RedoLogList
#include "ob_log_task_pool.h"                     // ObLogTransTaskPool
#include "ob_log_part_trans_dispatcher.h"         // PartTransDispatcher
#include "logservice/logfetcher/ob_log_fetch_stat_info.h"
#include "ob_log_utils.h"                         // ObLogLSNArray / _SEC_
#include "logservice/logfetcher/ob_log_fetcher_ls_ctx_additional_info.h" // ObILogFetcherLSCtxAddInfo
#include "logservice/logfetcher/ob_log_part_serve_info.h"       // logfetcher::PartServeInfo

namespace oceanbase
{
namespace libobcdc
{
struct TransStatInfo;
class IObLogFetcherDispatcher;
class IObLogClusterIDFilter;
struct PartServeInfo;
};

namespace libobcdc
{

class IObCDCPartTransResolver
{
public:
static bool test_mode_on;
static bool test_checkpoint_mode_on;
static int64_t test_mode_ignore_redo_count;

public:
  virtual ~IObCDCPartTransResolver() {}

public:
  // options of ignore_log_type in test_mode, to test different case of misslog
  // TODO currently not used, and should be applied if TransLog support LogEntryNode
  enum class IgnoreLogType: int64_t
  {
    NOT_IGNORE = 0,
    // not support ignore redo:
    // The REDO log may be in the same LogEntry with transaction status logs (such as COMMIT logs).
    // In this case, the REDO log cannot be ignored, as it may cause the transaction COMMIT log to be ignored as well,
    // but it is difficult to identify this scenario.
    IGNORE_SPECIFIED_REDO = 1,
    IGNORE_UNTIL_RECORD = 2,
    IGNORE_UNTIL_COMMIT_INFO = 3,
    IGNORE_UNTIL_PREPARE = 4,
    IGNORE_UNTIL_COMMIT = 5,
    INVALID_TYPE
  };
  static IgnoreLogType test_mode_ignore_log_type;

  // MissingLogInfo used for detect/fetch/handle miss_log
  class MissingLogInfo
  {
  public:
    MissingLogInfo();
    ~MissingLogInfo();

    MissingLogInfo &operator=(const MissingLogInfo &miss_log_info);

    void reset()
    {
      miss_redo_lsn_arr_.reset();
      miss_record_or_state_log_lsn_.reset();
      need_reconsume_commit_log_entry_ = false;
      is_resolving_miss_log_ = false;
      is_reconsuming_ = false;
    }
  public:
    /// has misslog or not
    /// @retval bool      ture if has miss_log(including redo/commit_info/prepare/commit and record_log)
    bool is_empty() const { return miss_redo_lsn_arr_.count() <= 0 && !miss_record_or_state_log_lsn_.is_valid(); }
    /// set need reconsume the state log(currently need reconsume commit_info(currently enable reentrant)/commit log)
    void set_need_reconsume_commit_log_entry() { need_reconsume_commit_log_entry_ = true; }
    bool need_reconsume_commit_log_entry() const { return need_reconsume_commit_log_entry_; }
    /// mark while resolving miss_log, marked in FetchStream module
    void set_resolving_miss_log() { is_resolving_miss_log_ = true; }
    bool is_resolving_miss_log() const { return is_resolving_miss_log_; }
    void set_reconsuming() { is_reconsuming_ = true; }
    bool is_reconsuming() const { return is_reconsuming_; }

    int set_miss_record_or_state_log_lsn(const palf::LSN &record_log_lsn);
    bool has_miss_record_or_state_log() const { return miss_record_or_state_log_lsn_.is_valid(); }
    int get_miss_record_or_state_log_lsn(palf::LSN &miss_record_lsn) const;
    ObLogLSNArray &get_miss_redo_lsn_arr() { return miss_redo_lsn_arr_; }
    void reset_miss_record_or_state_log_lsn() { miss_record_or_state_log_lsn_.reset(); }
    int push_back_single_miss_log_lsn(const palf::LSN &misslog_lsn);

    template<typename LSN_ARRAY>
    int push_back_missing_log_lsn_arr(const LSN_ARRAY &miss_log_lsn_arr);

    int64_t get_total_misslog_cnt() const;
    int sort_and_unique_missing_log_lsn();

    TO_STRING_KV(
        "miss_redo_count", miss_redo_lsn_arr_.count(),
        K_(miss_redo_lsn_arr),
        K_(miss_record_or_state_log_lsn),
        K_(need_reconsume_commit_log_entry),
        K_(is_resolving_miss_log),
        K_(is_reconsuming));

  private:
    // miss redo log lsn array
    ObLogLSNArray miss_redo_lsn_arr_;
    // miss record log or state log(commit_info/prepare) lsn
    palf::LSN miss_record_or_state_log_lsn_;
    // need reconsume the log_entry or not after handling miss_log or not.
    // reconsume if:
    //    (1) find miss_log by check redo is complete or not while resolving commit_log
    //    (2) find miss_log not empty while resolving commit_log(miss_log found while resolving prepare/commit_info log
    //        with the the same log_entry with commit_log).
    bool need_reconsume_commit_log_entry_;

    // resolving miss log
    // will directly append prev_log lsn while resolving miss_log
    bool is_resolving_miss_log_;
    // is reconsuming commit_log_entry
    // will ignore other type log while reconsuming commit_log_entry
    bool is_reconsuming_;
    // TODO use a int8_t instead above bool variable, may add is_reconsuming var for handle commit_info and commit log
  };

public:
  // init part_trans_resolver
  virtual int init(
      const logservice::TenantLSID &ls_id,
      const int64_t start_commit_version,
      const bool enable_direct_load_inc) = 0;

public:
  /// read log entry
  /// @param [in]   buf                   buf contains trans_log
  /// @param [in]   buf_len               length of buf
  /// @param [in]   pos_after_log_header  pos in buf that after ObLogBaseHeader
  /// @param [in]   submit_ts             submit_ts of log_entry(group_entry_log)
  /// @param [in]   serve_info            PartServeInfo to decide trans is served or not
  /// @param [out]  missing_log_info      hold miss_log_info which may be generated while read log
  /// @param [out]  tsi
  ///
  /// @retval OB_SUCCESS          handle all tx_log in log_entry success
  /// @retval OB_ITEM_NOT_SETTED  found misslog while resolving tx_log in log_entry
  /// @retval OB_IN_STOP_STATE    EXIT
  /// @retval other_err_code      unexpected error
  virtual int read(
      const char *buf,
      const int64_t buf_len,
      const int64_t pos_after_log_header,
      const palf::LSN &lsn,
      const int64_t submit_ts,
      const logfetcher::PartServeInfo &serve_info,
      MissingLogInfo &missing_log_info,
      logfetcher::TransStatInfo &tsi) = 0;

  /// dispatch ready PartTransTask. READY means:
  /// 1. Trans(DML/DDL) that already handle commit log and all redo of trans have persisted if working_mode is storage
  /// 2. all kinds of other type of PartTransTask(LS_HEARTBEAT/LS_OFFLINED/GLOBAL_HEARTBEAT)
  ///
  /// @param [in]   stop_flag
  /// @param [out]  pending_task_count: task_only_in_map(part_trans not committed) + task_in_queue(part_trans committed but not dispatched)
  /// @retval OB_SUCCESS        dispatch task success
  /// @retval OB_IN_STOP_STATE  EXIT
  /// @retval other_err_code    dispatch taks fail
  virtual int dispatch(volatile bool &stop_flag, int64_t &pending_task_count) = 0;

  /// generate LS Offline Task and push into part_trans_dispatcher
  /// 1. clean tasks in part_trans_dispatcher(both task_map_ and task_queue_)
  /// 2. init offline_ls_task as the last task of ls and dispatch into task_queue_
  ///
  /// @retval OB_SUCCESS      dispatch offline task success
  /// @retval other_err_code  generate and dispatch offline taks fail
  virtual int offline(volatile bool &stop_flag) = 0;

  /// get tps info of current ls
  virtual double get_tps() = 0;

  /// get dispatch progress and dispatch info of current LS
  virtual int get_dispatch_progress(int64_t &progress, logfetcher::PartTransDispatchInfo &dispatch_info) = 0;

  // generate LS heartbeat task and push into part_trans_dispatcher
  virtual int heartbeat(const int64_t hb_tstamp) = 0;

};

class ObCDCPartTransResolver : public IObCDCPartTransResolver
{
public:
  ObCDCPartTransResolver(
      const char *ls_id_str,
      TaskPool &task_pool,
      PartTransTaskMap &task_map,
      IObLogFetcherDispatcher &dispatcher,
      IObLogClusterIDFilter &cluster_id_filter);
  virtual ~ObCDCPartTransResolver();

public:
  static const int64_t DATA_OP_TIMEOUT = 10 * _SEC_;

public:
  virtual int init(
      const logservice::TenantLSID &tls_id,
      const int64_t start_commit_version,
      const bool enable_direct_load_inc);

  virtual int read(
      const char *buf,
      const int64_t buf_len,
      const int64_t pos_after_log_header,
      const palf::LSN &lsn,
      const int64_t submit_ts,
      const logfetcher::PartServeInfo &serve_info,
      MissingLogInfo &missing_log_info,
      logfetcher::TransStatInfo &tsi);

  virtual int dispatch(volatile bool &stop_flag, int64_t &pending_task_count);

  virtual int offline(volatile bool &stop_flag);

  virtual double get_tps() { return part_trans_dispatcher_.get_tps(); }

  virtual int get_dispatch_progress(int64_t &progress, logfetcher::PartTransDispatchInfo &dispatch_info)
  {
    return part_trans_dispatcher_.get_dispatch_progress(progress, dispatch_info);
  }

  virtual int heartbeat(const int64_t hb_tstamp) override
  {
    return part_trans_dispatcher_.heartbeat(hb_tstamp);
  }

public:
  TO_STRING_KV(K_(tls_id), K_(offlined), K_(part_trans_dispatcher));

private:

private:
  // ******* tx log handler ******** //
  int read_trans_header_(
      const palf::LSN &lsn,
      const transaction::ObTransID &tx_id,
      const bool is_resolving_miss_log,
      transaction::ObTxLogBlock &tx_log_block,
      transaction::ObTxLogHeader &tx_header,
      int64_t &tx_log_idx_in_entry,
      bool &has_redo_in_cur_entry);
  bool need_ignore_trans_log_(
      const palf::LSN &lsn,
      const transaction::ObTransID &tx_id,
      const transaction::ObTxLogHeader &tx_header,
      const MissingLogInfo &missing_info,
      const int64_t tx_log_idx_in_entry,
      bool &stop_resolve_cur_log_entry);
  // read trans log from tx_log_block as ObTxxxxLog and resolve the tx log.
  int read_trans_log_(
      const transaction::ObTxLogBlockHeader &tx_log_block_header,
      transaction::ObTxLogBlock &tx_log_block,
      const transaction::ObTxLogHeader &tx_log_header,
      const palf::LSN &lsn,
      const int64_t submit_ts,
      const logfetcher::PartServeInfo &serve_info,
      MissingLogInfo &missing_info,
      bool &has_redo_in_cur_entry);

  // read ObTxRedoLog
  int handle_redo_(
      const transaction::ObTransID &tx_id,
      const palf::LSN &lsn,
      const int64_t submit_ts,
      const bool handling_miss_log,
      transaction::ObTxLogBlock &tx_log_block);

  int handle_multi_data_source_log_(
      const transaction::ObTransID &tx_id,
      const palf::LSN &lsn,
      const bool handling_miss_log,
      transaction::ObTxLogBlock &tx_log_block);

  // read ObTxDirectLoadIncLog
  int handle_direct_load_inc_log_(
      const transaction::ObTransID &tx_id,
      const palf::LSN &lsn,
      const int64_t submit_ts,
      const bool handling_miss_log,
      transaction::ObTxLogBlock &tx_log_block);

  // read ObTxRecordLog
  int handle_record_(
      const transaction::ObTransID &tx_id,
      const palf::LSN &lsn,
      MissingLogInfo &missing_info,
      transaction::ObTxLogBlock &tx_log_block);

  // handle rollback_to log: push rollback_to info into PartTransTask
  int handle_rollback_to_(
      const transaction::ObTransID &tx_id,
      const palf::LSN &lsn,
      const bool is_resolving_miss_log,
      transaction::ObTxLogBlock &tx_log_block);

  /// collect missingInfo:
  /// 1. push prev_redo_lsn into SortedRedoList::recorded_redo_lsn_arr
  /// 2. push current log_entry_lsn into SortedLinkList::fetched_redo_lsn_arr_ if redo_log exist in current log_entry
  /// 3. collect misslog info
  /// CommitInfoLog should not reconsume!
  int handle_commit_info_(
      const transaction::ObTransID &tx_id,
      const palf::LSN &lsn,
      const int64_t submit_ts,
      const bool has_redo_in_cur_entry,
      MissingLogInfo &missing_info,
      transaction::ObTxLogBlock &tx_log_block);

  /// handle prepare log.
  /// if hasn't read commit_info log, push prev_lsn(expect prev_tx_log is commit_info log) into missing_info
  int handle_prepare_(
      const transaction::ObTransID &tx_id,
      const palf::LSN &prepare_lsn,
      const int64_t prepare_ts,
      MissingLogInfo &missing_info,
      transaction::ObTxLogBlock &tx_log_block);

  /// read commit_log:
  /// 1. check trans is served or not
  /// 2. check missing_info
  /// 3. set commit info of PartTransTask
  /// 4. submit part_trans_task into PartTransDispatcher(will push task into task_queue_)
  ///
  /// about phase 2(collect missingLogInfo):
  /// 1. if commit_info_log not resolved,:
  ///   1.1. push prev_log_lsn into miss log and return, after misslog handled, reconsume commit log(invoke by FetchStream)
  ///   1.2. if find misslog while reconsuming, return OB_ERR_UNEXPECTED
  /// 2. if commit_info_log is resolved, then assume all log before commit_info_log are fetch and resolved.
  ///    if find misslog in this case, return OB_ERR_UNEXPECTED
  /// TODO somethins should consider in transfer case:
  /// 1. if transfer after parpare, comit_log may not record the transfer-target ls, cause the
  ///     transfer_in ls don't have prepare_log, should consider remove the ls_trans in this case.
  ///
  /// @retval OB_SUCCESS          handle SUCCESS
  /// @retval OB_ITEM_NOT_SETTED  found
  /// @retval other_err_code      process failed
  int handle_commit_(
      const int64_t cluster_id,
      const transaction::ObTransID &tx_id,
      const palf::LSN &lsn,
      const int64_t submit_ts,
      const logfetcher::PartServeInfo &serve_info,
      MissingLogInfo &missing_info,
      transaction::ObTxLogBlock &tx_log_block,
      bool &is_served);

  /// handle abort log
  int handle_abort_(
      const transaction::ObTransID &tx_id,
      const bool is_resolving_miss_log,
      transaction::ObTxLogBlock &tx_log_block);

  // ********** util functions ********* //

  /// try get PartTransTask, try alloc a PartTransTask if get_task fail and try_alloc_task = true
  /// if obtain_task_ invoked while resolving miss log, expect must get_task success
  /// @param  part_trans_id         [in]  ls_id+trans_id
  /// @param  part_trans_task       [out] the PartTransTask of part_trans_id
  /// @param  is_resolving_miss_log [in]  is handing miss log
  ///
  /// @retval OB_SUCCESS            op success
  /// @retval OB_ENTRY_NOT_EXIST    can't find PartTransTask (unexpected if try_alloc_task = true)
  /// @retval other_err_code        fail
  int obtain_task_(
      const transaction::ObTransID &tx_id,
      PartTransTask* &part_trans_task,
      const bool is_resolving_miss_log);

  // Mark log_entry is fetched
  // Call by:
  // 1. handle_redo_()
  // 2. handle_multi_data_source_log_()
  // 3. handle_rollback_to_()
  int push_fetched_log_entry_(
      const palf::LSN &lsn,
      PartTransTask &part_trans_task);

  /// check if all redo log_entry recorded in prev_redo_lsn_arr are fetched, and push missed log_lsn into missing_info
  /// invoke by handle_commit_: expect no log missing TODO: currently not invoked in handle_commit_, need discuss if double check in commit_log is necessary
  /// invoke by handle_commit_info_: check miss redo between commit_info and first redo(if not exist record log) or last record_log(if exist record log)
  /// invoke by handle_record_: TODO: currently handle_record_ is not implied
  /// otherwise only redo between last RecordLog(if exist RecordLog) and current ComitInfoLog
  /// @note should not check and set prev_record_lsn into missing_info in this function, because should check if should set prev_record_lsn into missing_info by invoker.
  ///     which means prev_record_log is missed or not is not relate to missing_info is empty or not.
  /// @param   [in]  record_redo_lsn    redo_lsn_arr recored in trans log(TX_RECORD_LOG or TX_COMMIT_INFO_LOG)
  /// @param   [in]  part_trans_task    part_trans_task that contains sorted_redo_list which maintained  while push_redo_log
  /// @param   [out] missing_info       push miss redo lsn if exist miss log.
  ///
  /// @retval  OB_SUCCESS      check success
  /// @retval  other_err_code  check failed
  int check_redo_log_list_(
      const transaction::ObRedoLSNArray &record_redo_lsn,
      PartTransTask &part_trans_task,
      MissingLogInfo &missing_info);

  inline bool is_valid_trans_type_(const int32_t trans_type) const
  {
    return transaction::TransType::SP_TRANS == trans_type || transaction::TransType::DIST_TRANS == trans_type;
  }

  // trans_commit_version of single LS trans is log submit_ts, otherwise is trans_commit_version recorded in trans_log.
  inline int64_t get_trans_commit_version_(
      const int64_t commit_log_submit_ts,
      const int64_t commit_version_in_tx_log) const
  {
    return transaction::ObTransVersion::INVALID_TRANS_VERSION == commit_version_in_tx_log ?
        commit_log_submit_ts : commit_version_in_tx_log;
  }

private:
  bool                      offlined_ CACHE_ALIGNED;     // Is the partition deleted
  logservice::TenantLSID    tls_id_;
  PartTransDispatcher       part_trans_dispatcher_;
  IObLogClusterIDFilter     &cluster_id_filter_;
  bool                      enable_direct_load_inc_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObCDCPartTransResolver);
};

} // end namespace libobcdc
} // end namespace oceanbase
#endif
