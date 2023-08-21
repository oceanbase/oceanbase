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

#ifndef OCEANBASE_TRANSACTION_OB_TX_CTX_MGR
#define OCEANBASE_TRANSACTION_OB_TX_CTX_MGR

#include "ob_ls_tx_ctx_mgr_stat.h"
#include "ob_tx_stat.h"
#include "storage/tx_table/ob_tx_table_define.h"
#include "common/ob_simple_iterator.h"
#include "storage/tx/ob_trans_ctx.h"
#include "storage/tx/ob_tx_ls_log_writer.h"
#include "storage/tx/ob_tx_retain_ctx_mgr.h"
#include "storage/tablelock/ob_lock_table.h"
#include "storage/tx/ob_keep_alive_ls_handler.h"

namespace oceanbase
{

namespace common
{
class SpinRWLock;
class ObTabletID;
}

namespace storage
{
class ObLSTxService;
class ObTransSubmitLogFunctor;
class ObTxCtxTable;
}

namespace memtable
{
class ObIMemtable;
class ObMemtable;
class ObIMemtableCtx;
}

namespace transaction
{
class ObLSTxCtxMgrStat;
class ObTransCtx;
class ObPartTransCtx;
class ObITsMgr;
class ObITxLogParam;
class ObITxLogAdapter;
class ObLSTxLogAdapter;
class ObTxLockStat;
class ObTxStat;
}

namespace unittest {
class TestTxCtxTable;
};

namespace transaction
{

// Is used to store and traverse all ls id maintained
typedef common::ObSimpleIterator<share::ObLSID,
        ObModIds::OB_TRANS_VIRTUAL_TABLE_PARTITION, 16> ObLSIDIterator;

// Is used to store and travserse all TxCtx's Stat information;
typedef common::ObSimpleIterator<ObTxStat,
        ObModIds::OB_TRANS_VIRTUAL_TABLE_TRANS_STAT, 16> ObTxStatIterator;

// Is used to store and travserse all ObLSTxCtxMgr's Stat information
typedef common::ObSimpleIterator<ObLSTxCtxMgrStat,
        ObModIds::OB_TRANS_VIRTUAL_TABLE_PARTITION_STAT, 16> ObTxCtxMgrStatIterator;

// Is used to travserse all TxCtx's lock information
typedef common::ObSimpleIterator<ObTxLockStat,
        ObModIds::OB_TRANS_VIRTUAL_TABLE_TRANS_STAT, 16> ObTxLockStatIterator;

typedef ObTransHashMap<ObTransID, ObTransCtx, TransCtxAlloc, common::SpinRWLock, 1 << 14 /*bucket_num*/> ObLSTxCtxMap;

typedef common::LinkHashNode<share::ObLSID> ObLSTxCtxMgrHashNode;
typedef common::LinkHashValue<share::ObLSID> ObLSTxCtxMgrHashValue;

struct ObTxCreateArg
{
  ObTxCreateArg(const bool for_replay,
                const bool for_special_tx,
                const uint64_t tenant_id,
                const ObTransID &trans_id,
                const share::ObLSID &ls_id,
                const uint64_t cluster_id,
                const uint64_t cluster_version,
                const uint32_t session_id,
                const common::ObAddr &scheduler,
                const int64_t trans_expired_time,
                ObTransService *trans_service)
      : for_replay_(for_replay),
        for_special_tx_(for_special_tx),
        tenant_id_(tenant_id),
        tx_id_(trans_id),
        ls_id_(ls_id),
        cluster_id_(cluster_id),
        cluster_version_(cluster_version),
        session_id_(session_id),
        scheduler_(scheduler),
        trans_expired_time_(trans_expired_time),
        trans_service_(trans_service) {}
  bool is_valid() const
  {
    return ls_id_.is_valid()
        && tx_id_.is_valid()
        && trans_expired_time_ > 0
        && NULL != trans_service_;
  }
  TO_STRING_KV(K_(for_replay), K_(for_special_tx),
                 K_(tenant_id), K_(tx_id),
                 K_(ls_id), K_(cluster_id), K_(cluster_version),
                 K_(session_id), K_(scheduler), K_(trans_expired_time), KP_(trans_service));
  bool for_replay_;
  bool for_special_tx_;
  uint64_t tenant_id_;
  ObTransID tx_id_;
  share::ObLSID ls_id_;
  uint64_t cluster_id_;
  uint64_t cluster_version_;
  uint32_t session_id_;
  const common::ObAddr &scheduler_;
  int64_t trans_expired_time_;
  ObTransService *trans_service_;
};

// Is used to store and traverse ObTxID
const static char OB_SIMPLE_ITERATOR_LABEL_FOR_TX_ID[] = "ObTxCtxMgr";
typedef common::ObSimpleIterator<ObTransID, OB_SIMPLE_ITERATOR_LABEL_FOR_TX_ID, 16> ObTxIDIterator;

// LogStream Transaction Context Manager
class ObLSTxCtxMgr: public ObTransHashLink<ObLSTxCtxMgr>
{
// ut
  friend class unittest::TestTxCtxTable;

  friend class PrintFunctor;
  friend class ObTransCtx;
  friend class ObTransCtx;
  friend class ObTransTimer;
  friend class IterateLSTxCtxMgrStatFunctor;

public:
  typedef common::RWLock RWLock;
  typedef RWLock::RLockGuard RLockGuard;
  typedef RWLock::WLockGuard WLockGuard;
  typedef RWLock::WLockGuardWithRetryInterval WLockGuardWithRetryInterval;

  ObLSTxCtxMgr()
      : tx_log_adapter_(&log_adapter_def_), rwlock_(ObLatchIds::DEFAULT_SPIN_RWLOCK),
        minor_merge_lock_(ObLatchIds::DEFAULT_SPIN_RWLOCK)

  {
    reset();
  }

  virtual ~ObLSTxCtxMgr() { destroy(); }

  // @param [in] tenant_id: ls's tenant_id, currently used by ts_mgr;
  // @param [in] ls_id, associated ls_id;
  // @param [in] ts_mgr: used to get gts, see: update_max_replay_commit_version function;
  // @param [in] txs: transaction service which hold the ObTxCtxMgr;
  // @param [in] log_param: the params which is used to init ObLSTxCtxMgr's log_adapter_def_;
  int init(const int64_t tenant_id,
           const share::ObLSID &ls_id,
           ObTxTable *tx_table,
           ObLockTable *lock_table,
           ObITsMgr *ts_mgr,
           ObTransService *txs,
           ObITxLogParam *log_param,
           ObITxLogAdapter * log_adapter);

  // Destroy the ObLSTxCtxMgr
  void destroy();

  // Reset the ObLSTxCtxMgr
  void reset();

  // Offline the in-memory state of the ObLSTxCtxMgr
  int offline();
public:
  // Create a TxCtx whose tx_id is specified
  // @param [in] tx_id: transaction ID
  // @param [in] for_replay: Identifies whether the TxCtx is created for replay processing;
  // @param [out] existed: if it's true, means that an existing TxCtx with the same tx_id has
  //        been found, and the found TxCtx will be returned through the outgoing parameter tx_ctx;
  // TODO insert_and_get return existed object ptr;
  // @param [out] tx_ctx: newly allocated or already exsited transaction context
  // Return Values That Need Attention:
  // @return OB_SUCCESS, if the tx_ctx newly allocated or already existed
  // @return OB_NOT_MASTER, if this ls is a follower replica
  int create_tx_ctx(const ObTxCreateArg &arg,
                    bool& existed,
                    ObPartTransCtx *&tx_ctx);

  // Find specified TxCtx from the ObLSTxCtxMgr;
  // @param [in] tx_id: transaction ID
  // @param [in] for_replay: Identifies whether the TxCtx is used by replay processing;
  // @param [out] tx_ctx: context found through ObLSTxCtxMgr's hash table
  // Return Values That Need Attention:
  // @return OB_NOT_MASTER, if the LogStream is follower replica
  // @return OB_TRANS_CTX_NOT_EXIST, if the specified TxCtx is not found;
  int get_tx_ctx(const ObTransID &tx_id, const bool for_replay, ObPartTransCtx *&tx_ctx);

  // Find specified TxCtx directly from the ObLSTxCtxMgr's hash_map
  // @param [in] tx_id: transaction ID
  // @param [out] tx_ctx: context found through ObLSTxCtxMgr's hash table
  // @return OB_TRANS_CTX_NOT_EXIST, if the specified TxCtx is not found;
  int get_tx_ctx_directly_from_hash_map(const ObTransID &tx_id, ObPartTransCtx *&ctx);

  // Decrease the specified tx_ctx's reference count
  // @param [in] tx_ctx: the TxCtx will be revert
  int revert_tx_ctx(ObPartTransCtx *tx_ctx);

  // Decrease the specified tx_ctx's reference count
  // @param [in] tx_ctx: the TxCtx will be revert
  int revert_tx_ctx(ObTransCtx *tx_ctx);

  // Decrease the specified tx_ctx's reference count without acquiring ObLSTxCtxMgr's lock
  // @param [in] tx_ctx: the TxCtx will be revert
  int revert_tx_ctx_without_lock(ObTransCtx *ctx);
  int del_tx_ctx(ObTransCtx *ctx);

  // Freeze process needs to traverse TxCtx to submit log
  int traverse_tx_to_submit_redo_log(ObTransID &fail_tx_id);
  int traverse_tx_to_submit_next_log();

  // Get the min prepare version of transaction module of current observer for slave read
  // @param [out] min_prepare_version: MIN(uncommitted tx_ctx's prepare version | ObLSTxCtxMgr)
  int get_ls_min_uncommit_tx_prepare_version(share::SCN &min_prepare_version);

  // Check the ObLSTxCtxMgr's status; if it's stopped, the new transaction ctx will not be created;
  // otherwise, the tx_ctx is created normally
  bool is_stopped() const { return is_stopped_(); }

  // Stop the ObLSTxCtxMgr, the function will
  // 1. stop the ls_log_writer_;
  // 2. kill all tx in the ls and callback sql;
  // @param [in] graceful: indicate the kill behavior
  int stop(const bool graceful);

  // Kill all the tx in this ObLSTxCtxMgr
  // @param [in] graceful: indicate the kill behavior
  // @param [out] is_all_tx_cleaned_up: set it to true, when all tx are killed
  int kill_all_tx(const bool graceful, bool &is_all_tx_cleaned_up);

  // Block this ObLSTxCtxMgr, it can no longer create new tx_ctx;
  // @param [out] is_all_tx_cleaned_up: set it to true, when all transactions are cleaned up;
  int block_tx(bool &is_all_tx_cleaned_up);
  int block_all(bool &is_all_tx_cleaned_up);

  // Block this ObLSTxCtxMgr, it can no longer create normal tx_ctx;
  // Allow create special tx_ctx, exp: mds_trans;
  // @param [out] is_all_tx_cleaned_up: set it to true, when all transactions are cleaned up;
  int block_normal(bool &is_all_tx_cleaned_up);
  int unblock_normal();
  int online();

  // Get the TxCtx count in this ObLSTxCtxMgr;
  int64_t get_tx_ctx_count() const { return get_tx_ctx_count_(); }

  // Get the count of active transactions which have not been committed or aborted
  int64_t get_active_tx_count() const { return ATOMIC_LOAD(&active_tx_count_); }

  // Check all active and not "for_replay" tx_ctx in this ObLSTxCtxMgr
  // whether all the transactions that modify the specified tablet before
  // a schema version are finished.
  // @param [in] schema_version: the schema_version to check
  // @param [out] block_tx_id: a running transaction that modify the tablet before schema version.
  // Return Values That Need Attention:
  // @return OB_EAGAIN: Some TxCtx that has modify the tablet before schema
  // version is running;
  int check_modify_schema_elapsed(const common::ObTabletID &tablet_id,
                                  const int64_t schema_version,
                                  ObTransID &block_tx_id);

  // Check all active and not "for_replay" tx_ctx in this ObLSTxCtxMgr
  // whether all the transactions that modify the specified tablet before
  // a timestamp are finished.
  // @param [in] timestamp: the timestamp to check
  // @param [out] block_tx_id: a running transaction that modify the tablet before timestamp.
  // Return Values That Need Attention:
  // @return OB_EAGAIN: Some TxCtx that has modify the tablet before timestamp
  // is running;
  int check_modify_time_elapsed(const common::ObTabletID &tablet_id,
                                const int64_t timestamp,
                                ObTransID &block_tx_id);

  // check schduler status for tx gc
  int check_scheduler_status(share::SCN &min_start_scn, MinStartScnStatus &status);

  // Get this ObLSTxCtxMgr's ls_id_
  const share::ObLSID &get_ls_id() const { return ls_id_; }

  // Get this ObLSTxCtxMgr's ls_log_writer
  ObTxLSLogWriter *get_ls_log_writer() { return &ls_log_writer_; }

  ObITxLogAdapter *get_ls_log_adapter() { return tx_log_adapter_; }

  // Get the tx_table of this LogStream
  int get_tx_table_guard(ObTxTableGuard &guard) {
    return tx_table_->get_tx_table_guard(guard);
  }

  ObTxTable *get_tx_table() {
    return tx_table_;
  }

  // Get the LockTable of this LogStream
  int get_lock_memtable(ObTableHandleV2 &handle) {
    return lock_table_->get_lock_memtable(handle);
  }
  // check this ObLSTxCtxMgr is in leader serving state
  bool in_leader_serving_state();

  // dump a single tx data to text
  // @param [in] tx_id
  // @param [in] fd
  int dump_single_tx_data_2_text(const int64_t tx_id, FILE *fd);
  int start_readonly_request();
  int end_readonly_request();

  void dump_readonly_request(const int64_t max_req_number);

  // check this ObLSTxCtxMgr contains the specified ObLSID
  bool contain(const share::ObLSID &ls_id)
  {
    return ls_id_ == ls_id;
  }

public:
  // Increase this ObLSTxCtxMgr's total_tx_ctx_count
  void inc_total_tx_ctx_count() { (void)ATOMIC_AAF(&total_tx_ctx_count_, 1); }

  // Decrease this ObLSTxCtxMgr's total_tx_ctx_count
  void dec_total_tx_ctx_count() { (void)ATOMIC_AAF(&total_tx_ctx_count_, -1); }

  // Increase active trx count in this ls
  void inc_active_tx_count() { (void)ATOMIC_AAF(&active_tx_count_, 1); }

  // Decrease active trx count in this ls
  void dec_active_tx_count() { (void)ATOMIC_AAF(&active_tx_count_, -1); }

  void inc_total_active_readonly_request_count()
  {
    const int64_t count = ATOMIC_AAF(&total_active_readonly_request_count_, 1);
  }
  void dec_total_active_readonly_request_count()
  {
    int ret = common::OB_ERR_UNEXPECTED;
    const int64_t count = ATOMIC_AAF(&total_active_readonly_request_count_, -1);
    if (OB_UNLIKELY(count < 0)) {
      TRANS_LOG(ERROR, "unexpected total_active_readonly_request_count", KP(this), K(count));
    }
  }
  int64_t get_total_active_readonly_request_count() { return ATOMIC_LOAD(&total_active_readonly_request_count_); }

  // Get all tx obj lock information in this ObLSTxCtxMgr
  // @param [out] iter: all tx obj lock op information
  int iterate_tx_obj_lock_op(ObLockOpIterator &iter);

  // Get all tx lock information in this ObLSTxCtxMgr
  // @param [out] trans_lock_stat_iter: all tx lock information
  int iterate_tx_lock_stat(ObTxLockStatIterator &tx_lock_stat_iter);

  // Get all tx_ctx stat information in this ObLSTxCtxMgr
  // @param [out] tx_stat_iter: all TxCtx stat information
  int iterate_tx_ctx_stat(ObTxStatIterator &tx_stat_iter);

  // Get all tx_ids in one hash bucket from the hashtable of this ObLSTxCtxMgr, and store them in
  // ObTxIDIterator for subsequent iterative access;
  // @param [in] iter: tx_id information;
  // @param [in] bucket_pos: the specified bucket pos;
  int iterator_tx_id_in_one_bucket(ObTxIDIterator& iter, int bucket_pos);

  // Get all tx_ids from the hashtable of this ObLSTxCtxMgr, and store them in
  // ObTxIDIterator for subsequent iterative access;
  // @param [in] iter: tx_id information;
  int iterator_tx_id(ObTxIDIterator& iter);

  // Get the buckets cnt of ObLSTxCtxMgr'hashtable, which is a constant value;
  static int64_t get_tx_ctx_map_buckets_cnt()
  { return ObLSTxCtxMap::get_buckets_cnt(); }

  // Get the minimum value of rec_scn of TxCtx in this ObLSTxCtxMgr;
  // This value is used as the starting point of the redo replay required by the ObTxCtxTable file;
  // @param [out] rec_scnMIN(TxCtx's rec_scn in ObLSTxCtxMgr)
  int get_rec_scn(share::SCN &rec_scn);

  // Notify that the ObTxCtxTable corresponding to this ObLSTxCtxMgr has completed the flush operation;
  // Traverse ObLSTxCtxMgr and set the prev_rec_scn of each ObTxCtx to OB_INVALID_TIMESTAMP;
  int on_tx_ctx_table_flushed();

  // Print all transaction information in this ObLSTxCtxMgr to the log;
  // @param [in] max_print: The Max Count of TxCtx to Print
  // @param [in] verbose: if it set to true, print the TxCtx's Detailed information
  void print_all_tx_ctx(const int64_t max_print, const bool verbose);

  // Before Deleting the memtable, remove its associated callback from the callback list of all the
  // active transactions;
  // When the memtable has mini-merged, the commit_version of its associated
  // transaction may be undecided;
  // @param [in] mt: the memtable point which is going to be deleted;
  int remove_callback_for_uncommited_tx(
    const memtable::ObMemtableSet *memtable_set);

  // Find the TxCtx and execute the check functor with the tx_data retained on the TxCtx;
  // Called by the ObTxCtxTable
  // @param [in] tx_id: transaction ID
  // @param [in] fn: the check functor
  int check_with_tx_data(const ObTransID &tx_id, ObITxDataCheckFunctor &fn);

  // TODO Remove
  int get_min_undecided_scn(share::SCN &scn);

  //1. During the minor merge process, the status of uncommitted transactions will be actively
  //   queried; if ObPartTransCtx is released due to Rebuild, and the query cannot be performed,
  //   the minor merge process will record an exception failure log and exit;
  //2. The high risk point is that in the process of minor merge memtable, it is necessary to
  //   query the status of uncommitted transactions. At this time, the status of Trans is directly
  //   queried through the pointer to TransCtx in ObMvccTransNode; the use of this pointer is not
  //   currently protected by reference counting. If ObPartTransCtx is released due to Rebuild
  //   during use, it will cause a wild pointer;
  //3. The minor_merge_lock_ is used to prevent this from happening
  int lock_minor_merge_lock() { return minor_merge_lock_.rdlock(); }
  int unlock_minor_merge_lock() { return minor_merge_lock_.rdunlock(); };

  // Through this interface, the PALF notifies this ObLSTxCtxMgr to forcibly switch to follower;
  // When the PALF calls this interface,
  // all log entries status associated with this ls has been confirmed;
  // In all normal execution scenarios, the interface does not return failure;
  // The PALF layer assumes that the operation must succeed, and does not handle the failure;
  // TODO If this interface return failure, should set the replica to Manual Status;
  int switch_to_follower_forcedly();

  // Through this interface, the PALF notifies ObLSTxCtxMgr to switch to leader, then it will start
  // executing switch_to_leader routine;
  // 1. switch ObLSTxCtxMgr to LEADER_SWITCHING state, submit start_working log entry;
  // 2. switch ObLSTxCtxMgr to L_WORKING state;
  //    Travesal ObLSTxCtxMgr and call the switch_to_leader interface of each TxCtx;
  int switch_to_leader();

  // Through this interface, the PALF notifies ObLSTxCtxMgr to *gracefully* switch to follower;
  // This interface will be called when actively triggers the replica role switch action;
  // It is necessary to ensure that the switching process does not kill the transaction;
  // If the execution fails, the function will try to do the resume leader work by itself;
  // If the function cannot complete the resume_leader action by itself, it needs to return
  // OB_NEED_REVOKE, so that PALF will trigger the election process;
  int switch_to_follower_gracefully();

  // This interface will be called when other module switch_to_follower_gracefully executes failed;
  // When this interface call is triggered, it can ensure that each TxCtx in ObLSTxCtxMgr
  // is complete, and its status can be directly reset to Leader status;
  int resume_leader();

  // Replay the start working log entry. Traverse ObLSTxCtxMgr and set the data_complete_ of each
  // ObTxCtx to false. Make all the preceding ACTIVE_INFO log entry invalid.
  // @param [in] log: The start working log entry
  int replay_start_working_log(const ObTxStartWorkingLog &log, share::SCN start_working_scn);

  // START_WORKING log entry is successfully written to the PALF; According to the current
  // ObLSTxCtxMgr's status, drive ObLSTxCtxMgr to continue to execute the switch_to_leader routine
  // or resume_leader routine;
  int on_start_working_log_cb_succ(share::SCN start_working_scn);

  // START_WORKING log entry failed to written to the PALF;
  // Break the switch_to_leader routine or resume_leader routine; switch the ObLSTxCtxMgr's state to F_WORKING
  int on_start_working_log_cb_fail();

  // 1.In order to support slave-read, it is necessary to ensure the partial order relationship
  //   between log_id and scn; then before the transaction submits redolog to the PALF,
  //   The log entry's log id and scn ts will be generated by the PALF, and the partial order
  //   relationship between log_id and scn is guaranteed by the PALF;
  // 2.So the redolog entry's commit version may be bigger than the GTS version;
  //   In the normal transaction execution process, you need to wait for GTS version >= commit version
  //   before executing the inc the publish_version and responding to the client commit success;
  // 3.Distributed transactions can handle this situation normally in the recovery process;
  //   Single ls transactions require special handling;
  // 4.When the single ls transaction's redolog entry is replayed, the largest
  //   batch_commit_version on the ls will be calculated
  //   When start executing swith_to_leader routine, wait_gts_elapse will be called to ensure that
  //   GTS has exceeded this batch_commit_version;
  // @param [in] replay_commit_version : replayed single-ls-tx's log entry commit version;
  void update_max_replay_commit_version(const share::SCN &replay_commit_version)
  {
    max_replay_commit_version_.inc_update(replay_commit_version);
  }

  // Iterate all tx ctx in this ls and get the min_start_scn
  int get_min_start_scn(share::SCN &min_start_scn);

  // Get the trans_service corresponding to this ObLSTxCtxMgr;
  transaction::ObTransService *get_trans_service() { return txs_; }

  ObTxRetainCtxMgr &get_retain_ctx_mgr() { return ls_retain_ctx_mgr_; }

  // Get the tenant_id corresponding to this ObLSTxCtxMgr;
  int64_t get_tenant_id() { return tenant_id_; }

  // Get the state of this ObLSTxCtxMgr
  int64_t get_state() { return get_state_(); }

  // check is master
  bool is_master() const { return is_master_(); }

  // check is blocked
  bool is_tx_blocked() const { return is_tx_blocked_(); }

  // Switch the prev_aggre_log_ts and aggre_log_ts during dump starts
  int refresh_aggre_rec_scn();

  // Update aggre_log_ts without lock, because we canot lock using the order of
  // ObPartTransCtx -> ObLSTxCtxMgr, It will be a deadlock with normal order.
  int update_aggre_log_ts_wo_lock(share::SCN rec_log_ts);

  int get_max_decided_scn(share::SCN & scn);

  int do_standby_cleanup();

  TO_STRING_KV(KP(this),
               K_(ls_id),
               K_(tenant_id),
               "state",
               State::state_str(state_),
               K_(total_tx_ctx_count),
               K_(active_tx_count),
               K_(ls_retain_ctx_mgr),
               K_(aggre_rec_scn),
               K_(prev_aggre_rec_scn),
               "uref",
               (!is_inited_ ? -1 : get_ref()));
private:
  DISALLOW_COPY_AND_ASSIGN(ObLSTxCtxMgr);

private:
  static const int64_t OB_TRANS_STATISTICS_INTERVAL = 60 * 1000 * 1000;
  static const int64_t OB_PARTITION_AUDIT_LOCAL_STORAGE_COUNT = 4;
  static const int64_t TRY_THRESOLD_US = 1 * 1000 *1000;
  static const int64_t RETRY_INTERVAL_US = 10 *1000;

private:
  int process_callback_(ObIArray<ObTxCommitCallback> &cb_array) const;
  void print_all_tx_ctx_(const int64_t max_print, const bool verbose);
  int64_t get_tx_ctx_count_() const { return ATOMIC_LOAD(&total_tx_ctx_count_); }
  int create_tx_ctx_(const ObTxCreateArg &arg,
                     bool &existed,
                     ObPartTransCtx *&ctx);
  int get_tx_ctx_(const ObTransID &tx_id, const bool for_replay, ObPartTransCtx *&tx_ctx);
  int submit_start_working_log_();
  int try_wait_gts_and_inc_max_commit_ts_();
  share::SCN get_aggre_rec_scn_();
public:
  static const int64_t MAX_HASH_ITEM_PRINT = 16;
  static const int64_t WAIT_SW_CB_TIMEOUT = 100 * 1000; // 100 ms
  static const int64_t WAIT_SW_CB_INTERVAL = 10 * 1000; // 10 ms
  static const int64_t WAIT_READONLY_REQUEST_TIME = 10 * 1000 * 1000;
  static const int64_t READONLY_REQUEST_TRACE_ID_NUM = 8192;
private:
  class State
  {
  public:
    static const int64_t INVALID = -1;
    static const int64_t INIT = 0;
    static const int64_t F_WORKING = 1;
    static const int64_t T_PENDING = 2;
    static const int64_t R_PENDING = 3;
    static const int64_t L_WORKING = 4;
    static const int64_t F_TX_BLOCKED = 5;
    static const int64_t L_TX_BLOCKED = 6;
    static const int64_t L_BLOCKED_NORMAL = 7;
    static const int64_t T_TX_BLOCKED_PENDING = 8;
    static const int64_t R_TX_BLOCKED_PENDING = 9;
    static const int64_t F_ALL_BLOCKED = 10;
    static const int64_t T_ALL_BLOCKED_PENDING = 11;
    static const int64_t R_ALL_BLOCKED_PENDING = 12;
    static const int64_t L_ALL_BLOCKED = 13;
    static const int64_t STOPPED = 14;
    static const int64_t END = 15;
    static const int64_t MAX = 16;
  public:
    static bool is_valid(const int64_t state)
    { return state > INVALID && state < MAX; }

    #define TCM_STATE_CASE_TO_STR(state)        \
      case state:                               \
        str = #state;                           \
        break;

    static const char* state_str(uint64_t state)
    {
      const char* str = "INVALID";
      switch (state) {
        TCM_STATE_CASE_TO_STR(INIT);
        TCM_STATE_CASE_TO_STR(F_WORKING);
        TCM_STATE_CASE_TO_STR(T_PENDING);
        TCM_STATE_CASE_TO_STR(R_PENDING);
        TCM_STATE_CASE_TO_STR(L_WORKING);
        TCM_STATE_CASE_TO_STR(F_TX_BLOCKED);
        TCM_STATE_CASE_TO_STR(L_TX_BLOCKED);
        TCM_STATE_CASE_TO_STR(L_BLOCKED_NORMAL);
        TCM_STATE_CASE_TO_STR(T_TX_BLOCKED_PENDING);
        TCM_STATE_CASE_TO_STR(R_TX_BLOCKED_PENDING);

        TCM_STATE_CASE_TO_STR(F_ALL_BLOCKED);
        TCM_STATE_CASE_TO_STR(T_ALL_BLOCKED_PENDING);
        TCM_STATE_CASE_TO_STR(R_ALL_BLOCKED_PENDING);
        TCM_STATE_CASE_TO_STR(L_ALL_BLOCKED);

        TCM_STATE_CASE_TO_STR(STOPPED);
        TCM_STATE_CASE_TO_STR(END);
        default:
          break;
      }
      return str;
    }
    #undef TCM_STATE_CASE_TO_STR
  };

  class Ops
  {
  public:
    static const int64_t INVALID = -1;
    static const int64_t START = 0;
    static const int64_t LEADER_REVOKE = 1;
    static const int64_t SWL_CB_SUCC = 2;// start working log callback success
    static const int64_t SWL_CB_FAIL = 3;// start working log callback failed
    static const int64_t LEADER_TAKEOVER = 4;
    static const int64_t RESUME_LEADER = 5;
    static const int64_t BLOCK_TX = 6;
    static const int64_t BLOCK_NORMAL = 7;
    static const int64_t BLOCK_ALL = 8;
    static const int64_t STOP = 9;
    static const int64_t ONLINE = 10;
    static const int64_t UNBLOCK_NORMAL = 11;
    static const int64_t MAX = 12;

  public:
    static bool is_valid(const int64_t op)
    { return op > INVALID && op < MAX; }

    #define TCM_OP_CASE_TO_STR(op)              \
      case op:                                  \
        str = #op;                              \
        break;

    static const char* op_str(uint64_t op)
    {
      const char* str = "INVALID";
      switch (op) {
        TCM_OP_CASE_TO_STR(START);
        TCM_OP_CASE_TO_STR(LEADER_REVOKE);
        TCM_OP_CASE_TO_STR(SWL_CB_SUCC);
        TCM_OP_CASE_TO_STR(SWL_CB_FAIL);
        TCM_OP_CASE_TO_STR(LEADER_TAKEOVER);
        TCM_OP_CASE_TO_STR(RESUME_LEADER);
        TCM_OP_CASE_TO_STR(BLOCK_TX);
        TCM_OP_CASE_TO_STR(BLOCK_NORMAL);
        TCM_OP_CASE_TO_STR(BLOCK_ALL);
        TCM_OP_CASE_TO_STR(STOP);
        TCM_OP_CASE_TO_STR(ONLINE);
        TCM_OP_CASE_TO_STR(UNBLOCK_NORMAL);
      default:
        break;
      }
      return str;
    }
    #undef TCM_OP_CASE_TO_STR
  };

  class StateHelper
  {
  public:
    explicit StateHelper(const share::ObLSID &ls_id, int64_t &state)
        : ls_id_(ls_id), state_(state), last_state_(State::INVALID), is_switching_(false) {}
    ~StateHelper() {}

    int switch_state(const int64_t op);
    void restore_state();
    int64_t get_state() const { return state_; }
  private:
    const share::ObLSID &ls_id_;
    int64_t &state_;
    int64_t last_state_;
    bool is_switching_;
  };

private:
  inline bool is_master_() const
  { return is_master_(ATOMIC_LOAD(&state_)); }
  inline bool is_master_(int64_t state) const
  { return State::L_WORKING == state ||
           State::L_TX_BLOCKED == state ||
           State::L_BLOCKED_NORMAL == state ||
           State::L_ALL_BLOCKED == state; }

  inline bool is_follower_() const
  { return is_follower_(ATOMIC_LOAD(&state_)); }
  inline bool is_follower_(int64_t state) const
  { return State::F_WORKING == state ||
           State::F_TX_BLOCKED == state ||
           State::F_ALL_BLOCKED == state; }

  inline bool is_tx_blocked_() const
  { return is_tx_blocked_(ATOMIC_LOAD(&state_)); }
  inline bool is_tx_blocked_(int64_t state) const
  {
    return State::F_TX_BLOCKED == state ||
           State::L_TX_BLOCKED == state ||
           State::T_TX_BLOCKED_PENDING == state ||
           State::R_TX_BLOCKED_PENDING == state ||
           State::F_ALL_BLOCKED == state ||
           State::T_ALL_BLOCKED_PENDING == state ||
           State::R_ALL_BLOCKED_PENDING == state ||
           State::L_ALL_BLOCKED == state;
  }

  inline bool is_normal_blocked_() const
  { return is_normal_blocked_(ATOMIC_LOAD(&state_)); }
  inline bool is_normal_blocked_(int64_t state) const
  { return State::L_BLOCKED_NORMAL == state; }

  inline bool is_all_blocked_() const
  { return is_all_blocked_(ATOMIC_LOAD(&state_)); }
  inline bool is_all_blocked_(const int64_t state) const
  {
    return State::F_ALL_BLOCKED == state ||
           State::T_ALL_BLOCKED_PENDING == state ||
           State::R_ALL_BLOCKED_PENDING == state ||
           State::L_ALL_BLOCKED == state;
  }

  // check pending substate
  inline bool is_t_pending_() const
  { return is_t_pending_(ATOMIC_LOAD(&state_)); }
  inline bool is_t_pending_(int64_t state) const
  {
    return State::T_PENDING == state ||
           State::T_TX_BLOCKED_PENDING == state ||
           State::T_ALL_BLOCKED_PENDING == state;
  }

  inline bool is_r_pending_() const
  { return is_r_pending_(ATOMIC_LOAD(&state_)); }
  inline bool is_r_pending_(int64_t state) const
  {
    return State::R_PENDING == state ||
           State::R_TX_BLOCKED_PENDING == state ||
           State::R_ALL_BLOCKED_PENDING == state;
  }

  inline bool is_pending_() const
  { return is_pending_(ATOMIC_LOAD(&state_)); }
  inline bool is_pending_(int64_t state) const
  {
    return is_t_pending_(state) || is_r_pending_(state);
  }

  inline bool is_stopped_() const
  { return is_stopped_(ATOMIC_LOAD(&state_)); }
  inline bool is_stopped_(int64_t state) const
  { return State::STOPPED == state; }

  int64_t get_state_() const
  { return ATOMIC_LOAD(&state_); }

private:
  // Identifies this ObLSTxCtxMgr is inited or not;
  bool is_inited_;

  // See the ObLSTxCtxMgr's internal class State
  int64_t state_;

  // A thread-safe hashmap, used to find and traverse TxCtx in this ObLSTxCtxMgr
  ObLSTxCtxMap ls_tx_ctx_map_;

  // The tenant ID to which this ObLSTxCtxMgr belongs
  int64_t tenant_id_;

  // The ls ID associated with this ObLSTxCtxMgr
  share::ObLSID ls_id_;

  // The tx table associated with this ObLSTxCtxMgr
  ObTxTable *tx_table_;
  // The Lock Table associate with this ObLSTxCtxMgr
  ObLockTable *lock_table_;

  // Used to submit START_WORKING LogEntry
  ObTxLSLogWriter ls_log_writer_;
  ObITxLogAdapter *tx_log_adapter_;
  ObLSTxLogAdapter log_adapter_def_;

  ObTxRetainCtxMgr ls_retain_ctx_mgr_;

  mutable RWLock rwlock_;
  // lock for concurrency between minor merge and remove / rebuild this LS
  // ATTENTION: the order between locks should be:
  //                     rwlock_ -> minor_merge_lock_
  mutable RWLock minor_merge_lock_;

  // Total TxCtx count in this ObLSTxCtxMgr
  int64_t total_tx_ctx_count_ CACHE_ALIGNED;

  int64_t total_active_readonly_request_count_ CACHE_ALIGNED;

  int64_t active_tx_count_;

  // It is used to record the time point of leader takeover
  // gts must be refreshed to the newest before the leader provides services
  MonotonicTs leader_takeover_ts_;
  bool is_leader_serving_;

  // Transaction service which hold the ObTxCtxMgr;
  ObTransService *txs_;

  // The time source
  ObITsMgr *ts_mgr_;

  // See the ObLSTxCtxMgr's member function update_max_replay_commit_version
  share::SCN max_replay_commit_version_;

  // Recover log timestamp for aggregated tx ctx. For the purpose of Durability,
  // we need a space to store rec_log_ts for checkpoint[1]. Although tx ctx
  // itself can store rec_log_ts, it may release its ctx when txn itself
  // finishes. So we decide to store all tx ctx, whose rec_log_ts should be
  // remembered while must be released soon, in the ctx_mgr.
  //
  // [1]: It you are interested in rec_log_ts, you can see ARIES paper.
  share::SCN aggre_rec_scn_;
  share::SCN prev_aggre_rec_scn_;

  // Online timestamp for ObLSTxCtxMgr
  int64_t online_ts_;
  ObCurTraceId::TraceId readonly_request_trace_id_set_[READONLY_REQUEST_TRACE_ID_NUM];
};

// Used to iteratively access TxCtx in ObLSTxCtxMgr;
//
// ObLSTxCtxIterator holds an ObTxIDIterator; ObLSTxCtxIterator will batch out all tx_ids in a
// single hash_bucket from the hashtable of the ObLSTxCtxMgr and store them in the ObTxIDIterator
// it holds;
// In ObLSTxCtxIterator::get_next_tx_ctx function, take out the next tx_id from ObTxIDIterator,
// and then obtain TxCtx by searching ObLSTxCtxMgr; if there is a concurrent delete
// TxCtx request from generating tx_id_array to searching for TxCtx, it is normal that TxCtx
// cannot be found;
// When the tx_id in ObTxIDIterator is exhausted, go to the next hash bucket to batch out all tx_ids;
//
// In this way, the additional memory can be controlled on the number of tx_id of a single bucket;
// the current hashmap is divided into 16384 buckets; it can achieve a small additional memory footprint;
class ObLSTxCtxIterator
{
public:
  ObLSTxCtxIterator() : BUCKETS_CNT_(ObLSTxCtxMgr::get_tx_ctx_map_buckets_cnt()) { reset(); }
  ~ObLSTxCtxIterator() {}
  void reset();

  int set_ready(ObLSTxCtxMgr* ls_tx_ctx_mgr);
  bool is_ready() const {return is_ready_; }

  int get_next_tx_ctx(ObPartTransCtx *&tx_ctx);
  int revert_tx_ctx(ObPartTransCtx *tx_ctx);

private:
  int get_next_tx_id_(ObTransID& tx_id);

private:
  bool is_ready_;
  int64_t current_bucket_pos_;
  const int64_t BUCKETS_CNT_;
  ObLSTxCtxMgr* ls_tx_ctx_mgr_;
  ObTxIDIterator tx_id_iter_;
};

class ObLSTxCtxMgrAlloc
{
public:
  static ObLSTxCtxMgr* alloc_value() { return NULL; }
  static void free_value(ObLSTxCtxMgr* p)
  {
    if (NULL != p) {
      TRANS_LOG(INFO, "ObLSTxCtxMgr release", K(*p));
      ObLSTxCtxMgrFactory::release(p);
    }
  }
  static ObLSTxCtxMgrHashNode* alloc_node(ObLSTxCtxMgr* p)
  {
    UNUSED(p);
    return op_alloc(ObLSTxCtxMgrHashNode);
  }
  static void free_node(ObLSTxCtxMgrHashNode* node)
  {
    if (NULL != node) {
      op_free(node);
      node = NULL;
    }
  }
};

typedef transaction::ObTransHashMap<share::ObLSID, ObLSTxCtxMgr,
        ObLSTxCtxMgrAlloc, common::ObQSyncLock> ObLSTxCtxMgrMap;

class ObTxCtxMgr
{
public:
  ObTxCtxMgr() { reset(); }
  ~ObTxCtxMgr() { destroy(); }
  // @param [in] tenant_id: tenant id
  // @param [in] ts_mgr: used to get gts, see: update_max_replay_commit_version function;
  // @param [in] txs: transaction service which hold the ObTxCtxMgr;
  int init(const int64_t tenant_id, ObITsMgr *ts_mgr, ObTransService *txs);

  // Mark ObTxCtxMgr as running
  int start();

  // Stop the ObTxCtxMgr; call the stop function of each ObLSTxCtxMgr and mark ObTxCtxMgr is not running;
  int stop();

  // Destroy the ObTxCtxMgr;
  void destroy();

  // Wait until each ObLSTxCtxMgr has stopped and the number of TxCtx held is 0;
  int wait();

  // Reset the ObTxCtxMgr;
  void reset();
public:

  // Find specified TxCtx from the specified ObLSTxCtxMgr;
  // @param [in] ls_id: the specified ls ID
  // @param [in] tx_id: transaction ID
  // @param [in] for_replay: Identifies whether the TxCtx is used by replay processing
  // @param [out] tx_ctx: context found through ls transaction context manager's hash table
  // @return OB_SUCCESS, if found the transaction tx_ctx
  // @return OB_NOT_MASTER, if the LogStream is follower replica
  int get_tx_ctx(const share::ObLSID &ls_id,
                 const ObTransID &tx_id,
                 const bool for_replay,
                 ObPartTransCtx *&tx_ctx);

  // Create a TxCtx whose tx_id is specified in the specified ObLSTxCtxMgr
  // @param [in] ls_id: the specified ls ID
  // @param [in] tx_id: transaction ID
  // @param [in] for_replay: Identifies whether the TxCtx is created for replay processing;
  // @param [out] existed:
  //              if it's true, means that an existing TxCtx with the same tx_id has been found;
  // TODO insert_and_get return existed object ptr;
  // @param [out] tx_ctx: newly allocated or already exsited transaction context
  // @return OB_SUCCESS, if the transaction tx_ctx newly allocated or already existed
  // @return OB_NOT_MASTER, if this LogStream is a follower replica
  int create_tx_ctx(const ObTxCreateArg &arg,
                    bool& existed,
                    ObPartTransCtx *&tx_ctx);

  // Decrease the specified tx_ctx's reference count
  // @param [in] tx_ctx: the TxCtx will be revert
  int revert_tx_ctx(ObPartTransCtx *tx_ctx);

  // Create ObLSTxCtxMgr;
  // @param [in] tenant_id: the tenant ID;
  // @param [in] ls_id: the specifiied ls ID
  // @param [in] log_param: the params which is used to init ObLSTxCtxMgr's log_adapter_def_;
  int create_ls(const int64_t tenant_id,
                const share::ObLSID &ls_id,
                ObTxTable *tx_table,
                ObLockTable *lock_table,
                storage::ObLSTxService &ls_tx_svr, /* TODO remove this argument*/
                ObITxLogParam *param,
                ObITxLogAdapter *log_adapter);

  // Delete the specified ObLSTxCtxMgr, the stop_ls function will be called internally and ensure
  // that there is no active TxCtx, then the ObLSTxCtxMgr will be deleted from the hash table
  // of ObTxCtxMgr;
  // @param [in] ls_id: the specifiied ls ID
  // @param [in] graceful: indicate the kill behavior
  int remove_ls(const share::ObLSID &ls_id, const bool graceful);

  // Block the specified ObLSTxCtxMgr, so that it can no longer create a new TxCtx,
  // and returns whether there are still active transactions through out parameters;
  // @param [in] ls_id: the specifiied ls ID
  // @param [out] is_all_tx_cleaned_up, set it to true, when all transactions are cleaned up;
  int block_tx(const share::ObLSID &ls_id, bool &is_all_tx_cleaned_up);
  // block tx and readonly request
  int block_all(const share::ObLSID &ls_id, bool &is_all_tx_cleaned_up);

  // Traverse the specified ObLSTxCtxMgr and kill all transactions it holds;
  // @param [in] ls_id: the specifiied ls ID
  int clear_all_tx(const share::ObLSID &ls_id);

  // kill the specified ls's transaction
  // @param [in] ls_id: the specifiied ls ID
  // @param [in] graceful: indicate the kill behavior
  // @param [out] is_all_tx_cleaned_up: set it to true, when all transactions are killed
  int kill_all_tx(const share::ObLSID &ls_id, bool graceful, bool &is_all_tx_cleaned_up);

  //int get_store_ctx(const storage::ObStoreAccessType access_type,
  //                  ObTxDesc &tx,
  //                  const ObLSID &ls_id,
  //                  storage::ObStoreCtx &store_ctx,
  //                  const int64_t &snapshot_version);

  // revert_store_ctx
  //int revert_store_ctx(ObTxDesc &ctx, storage::ObStoreCtx &store_ctx);

  // Get the min prepare version of transaction module of current observer for slave read
  // @param [in] indicates the specified ls;
  // @param [out] min_prepare_version: MIN(uncommitted transaction ctx's prepare version)
  int get_ls_min_uncommit_tx_prepare_version(const share::ObLSID &ls_id, share::SCN &min_prepare_version);

  // TODO
  int get_min_undecided_scn(const share::ObLSID &ls_id, share::SCN &scn);

  // Get all ls ID in the observer
  // param [out] ls_id_iter: ObLSIDIterator is Used to return all ls id in the observer,
  //             which is subsequently used for iterative access of virtual table;
  int iterate_ls_id(ObLSIDIterator &ls_id_iter);

  // Get all ObLSTxCtxMgr's stat info in the observer
  // @param [in] addr: the address of the observer, which is used to populate each line of output;
  // @param [out] tx_ctx_mgr_stat_iter: Used to return all the collected ObLSTxCtxMgr's Stat
  //              information, and then used to iteratively output to the virtual table;
  int iterate_tx_ctx_mgr_stat(const ObAddr &addr,
                              ObTxCtxMgrStatIterator &tx_ctx_mgr_stat_iter);

  // Get all transaction information at the server level
  // @param [out] tx_stat_iter: Used to return all the collected TxCtx's Stat information,
  //             and then used to iteratively output to the virtual table;
  int iterate_all_observer_tx_stat(ObTxStatIterator &tx_stat_iter);

  // Print all TxCtx in the specific ObLSTxCtxMgr
  // @param [in] ls_id: the specific ls ID;
  int print_all_tx_ctx(const share::ObLSID &ls_id);

  // Get specific ObLSTxCtxMgr's transaction lock stat
  // @param [in] ls_id: the specified ls_id
  // @param [out] tx_lock_stat_iter: Used to return all the collected TxLockStat information,
  //              and then used to iteratively output to the virtual table;
  int iterate_ls_tx_lock_stat(const share::ObLSID &ls_id,
                              ObTxLockStatIterator &tx_lock_stat_iter);

  // Before Deleting the memtable, remove its associated callback from the callback list of all the
  // active transactions; When the memtable has mini-merged, the commit_version of its associated
  // transaction may be undecided;
  // @param [in] ls_id: the specified ls_id;
  // @param [in] mt: the memtable point which will be deleted;
  int remove_callback_for_uncommited_tx(
    const ObLSID ls_id,
    const memtable::ObMemtableSet *memtable_set);

  TO_STRING_KV(K(is_inited_), K(tenant_id_), KP(this));


  // Find specified ObLSTxCtxMgr from the ObTxCtxMgr and increase its reference count;
  // @param [in] ls_id: the specified ls_id
  // @param [out] ls_tx_ctx_mgr: the specified ls_tx_ctx_mgr
  int get_ls_tx_ctx_mgr(const share::ObLSID &ls_id, ObLSTxCtxMgr *&ls_tx_ctx_mgr);

  // Decrease specified ObLSTxCtxMgr's reference count
  // @param [in] mgr: indicates the ObLSTxCtxMgr whose reference count needs to be decreased;
  int revert_ls_tx_ctx_mgr(ObLSTxCtxMgr *ls_tx_ctx_mgr);

  // @param [in] ls_id: the specified ls_id
  int check_scheduler_status(share::ObLSID ls_id);

  int get_max_decided_scn(const share::ObLSID &ls_id, share::SCN & scn);

  int do_all_ls_standby_cleanup(ObTimeGuard &cleanup_timeguard);
private:
  int create_ls_(const int64_t tenant_id,
                 const share::ObLSID &ls_id,
                 ObLSTxService &ls_tx_svr,
                 ObITxLogParam *param);

  int remove_ls_(const share::ObLSID &ls_id);

  int stop_ls_(const share::ObLSID &ls_id, const bool graceful);

  int wait_ls_(const share::ObLSID &ls_id);

  int remove_all_ls_();
  template <typename Fn> int foreach_ls_(Fn &fn) { return ls_tx_ctx_mgr_map_.for_each(fn); }
  template <typename Fn> int remove_if_(Fn &fn) { return ls_tx_ctx_mgr_map_.remove_if(fn); }

  int print_all_ls_tx_ctx_();
private:
  bool is_inited_;

  bool is_running_;

  // The tenant ID to which this ObTxCtxMgr belongs
  int64_t tenant_id_;

  // A thread-safe hashmap, used to find and traverse ObLSTxCtxMgr
  ObLSTxCtxMgrMap ls_tx_ctx_mgr_map_;

  // The time source
  ObITsMgr *ts_mgr_;

  // Statistical variable that records the number of ObLSTxCtxMgr allocated;
  int64_t ls_alloc_cnt_;

  // Statistical variable that records the number of ObLSTxCtxMgr released;
  int64_t ls_release_cnt_;

  // Transaction service which hold the ObTxCtxMgr;
  ObTransService *txs_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTxCtxMgr);
};

} // transaction
} // oceanbase

#endif // OCEANBASE_TRANSACTION_OB_TX_CTX_MGR
