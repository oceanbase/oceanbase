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

#ifndef OCEANBASE_TRANSACTION_OB_TRANS_PART_CTX_
#define OCEANBASE_TRANSACTION_OB_TRANS_PART_CTX_

#include "ob_trans_ctx.h"
#include "ob_ts_mgr.h"
#include "ob_trans_ctx_mgr.h"
#include "ob_ctx_tx_data.h"
#include "lib/container/ob_mask_set2.h"
#include "lib/list/ob_dlist.h"
#include "share/ob_ls_id.h"
#include "logservice/palf/lsn.h"
#include "ob_tx_ctx_mds.h"
#include "ob_one_phase_committer.h"
#include "ob_two_phase_committer.h"
#include "ob_dup_table_base.h"
#include <cstdint>
#include "storage/multi_data_source/buffer_ctx.h"


namespace oceanbase
{

namespace common
{
class ObAddr;
}

namespace storage
{
struct ObStoreRowLockState;
class ObDDLRedoLog;
}

namespace transaction
{
class ObITransCtxMgr;
class ObTransMsg;
class ObRedoLogSyncResponseMsg;
class ObTxLogBlock;
enum class ObTxLogType : int64_t;
class ObTxRedoLog;
class ObTxCommitInfoLog;
class ObTxPrepareLog;
class ObTxCommitLog;
class ObTxAbortLog;
class ObTxClearLog;
class ObIRetainCtxCheckFunctor;
struct ObTxMsg;
}
namespace palf
{
class LSN;
}
namespace memtable
{
class ObIMemtableCtxFactory;
class ObIMemtableCtx;
};

namespace storage
{
class ObTxData;
};

enum
{
  USER_REQUEST_UNKNOWN = -1,
  USER_COMMIT = 0,
  USER_ABORT = 1,
};

namespace transaction
{

const static int64_t OB_TX_MAX_LOG_CBS = 15;
const static int64_t PREALLOC_LOG_CALLBACK_COUNT = 3;
const static int64_t RESERVE_LOG_CALLBACK_COUNT_FOR_FREEZING = 1;

template<typename T, typename fn>
int64_t search(const ObIArray<T> &array, fn &equal_func)
{
  int ret = OB_SUCCESS;
  int64_t search_index = -1;

  ARRAY_FOREACH_X(array, i, cnt, search_index == -1) {
    if (equal_func(array.at(i))) {
      search_index = i;
    }
  }

  return search_index;
}
template<typename T>
class EqualToTransferPartFunctor
{
public:
  EqualToTransferPartFunctor(const ObStandbyCheckInfo &tmp_info) :
                            tmp_info_(tmp_info)
  {}
  bool operator()(const T& item) {
    bool bool_ret = false;
    if (item.check_info_ == tmp_info_) {
      bool_ret = true;
    }
    return bool_ret;
  }
private:
  const ObStandbyCheckInfo &tmp_info_;
};

template<typename T>
class EqualToStateInfoFunctor
{
public:
  EqualToStateInfoFunctor(const T &tmp_info) :
                          tmp_info_(tmp_info)
  {}
  bool operator()(const T& item) {
    bool bool_ret = false;
    if (tmp_info_.ls_id_ == item.ls_id_) {
      if (tmp_info_.check_info_.is_valid()) {
        if (tmp_info_.check_info_ == item.check_info_) {
          bool_ret = true;
        }
      } else { // for old version msg compat
        bool_ret = true;
      }
    }
    return bool_ret;
  }
private:
  const T &tmp_info_;
};

template <typename>
class ObTxCtxLogOperator;

// participant transaction context
class ObPartTransCtx : public ObTransCtx,
                       public ObTsCbTask,
                       public ObTxCycleTwoPhaseCommitter
{
  friend class ObTransService;
  friend class IterateTxStatFunctor;
  friend class IterateTransStatForKeyFunctor;
  friend class StandbyCleanUpFunctor;
  friend class MockObTxCtx;
  friend class ObTxELRHandler;
  friend class ObIRetainCtxCheckFunctor;
  friend class memtable::ObRedoLogGenerator;
  template<typename T>
  friend class ObTxCtxLogOperator;
public:
  ObPartTransCtx()
      : ObTransCtx("participant", ObTransCtxType::PARTICIPANT), ObTsCbTask(),
        ObTxCycleTwoPhaseCommitter(),
        is_inited_(false), mt_ctx_(), reserve_allocator_("PartCtx", MTL_ID()),
        exec_info_(reserve_allocator_),
        mds_cache_(reserve_allocator_),
        role_state_(TxCtxRoleState::FOLLOWER),
        coord_prepare_info_arr_(OB_MALLOC_NORMAL_BLOCK_SIZE,
                                ModulePageAllocator(reserve_allocator_, "PREPARE_INFO")),
        standby_part_collected_(), ask_state_info_interval_(100 * 1000), refresh_state_info_interval_(100 * 1000),
        transfer_deleted_(false),
        last_rollback_to_request_id_(0),
        last_rollback_to_timestamp_(0),
        last_transfer_in_timestamp_(0)
  { /*reset();*/ }
  ~ObPartTransCtx() { destroy(); }
  void destroy();
  int init(const uint64_t tenant_id,
           const common::ObAddr &scheduler,
           const uint32_t session_id,
           const uint32_t associated_session_id,
           const ObTransID &trans_id,
           const int64_t trans_expired_time,
           const share::ObLSID &ls_id,
           const uint64_t cluster_version,
           ObTransService *trans_service,
           const uint64_t cluster_id,
           const int64_t epoch,
           ObLSTxCtxMgr *ls_ctx_mgr,
           const bool for_replay,
           const PartCtxSource ctx_source,
           ObXATransID xid);
  void reset() { }
  int construct_context(const ObTransMsg &msg);
public:
  int start_trans();
  bool is_inited() const;
  int handle_timeout(const int64_t delay);
  /*
   * graceful kill: wait trx finish logging
   */
  int kill(const KillTransArg &arg, ObTxCommitCallback *&cb_list);
  memtable::ObMemtableCtx *get_memtable_ctx() { return &mt_ctx_; }
  int commit(const ObTxCommitParts &parts,
             const MonotonicTs &commit_time,
             const int64_t &expire_ts,
             const common::ObString &app_trace_info,
             const int64_t &request_id);
  int abort(const int reason);
  int one_phase_commit_();
  int get_prepare_version_if_prepared(bool &is_prepared, share::SCN &prepare_version);
  const share::SCN get_commit_version() const { return ctx_tx_data_.get_commit_version(); }
  uint64_t hash() const { return trans_id_.hash(); }
  int gts_callback_interrupted(const int errcode, const share::ObLSID target_ls_id);
  int get_gts_callback(const MonotonicTs srr, const share::SCN &gts, const MonotonicTs receive_gts_ts);
  int gts_elapse_callback(const MonotonicTs srr, const share::SCN &gts);
  MonotonicTs get_stc() const { return stc_; }
  uint64_t get_tenant_id() const { return tenant_id_; }
  int64_t get_role_state() const { return role_state_; }
  // for xa
  int sub_prepare(const ObTxCommitParts &parts,
                  const MonotonicTs &commit_time,
                  const int64_t &expire_ts,
                  const common::ObString &app_trace_info,
                  const int64_t &request_id,
                  const ObXATransID &xid);
  int sub_end_tx(const int64_t &request_id,
                 const ObXATransID &xid,
                 const common::ObAddr &tmp_scheduler,
                 const bool is_rollback);

  int dump_2_text(FILE *fd);
  int init_for_transfer_move(const ObTxCtxMoveArg &arg);
public:
  int replay_start_working_log(const share::SCN start_working_ts);
  int set_trans_app_trace_id_str(const ObString &app_trace_id_str);
  const ObString &get_trans_app_trace_id_str() const { return trace_info_.get_app_trace_id(); }
  int check_modify_schema_elapsed(const ObTabletID &tablet_id,
                                  const int64_t schema_version);
  int check_modify_time_elapsed(const ObTabletID &tablet_id,
                                const int64_t timestamp);
  int iterate_tx_obj_lock_op(ObLockOpIterator &iter) const;
  int iterate_tx_lock_stat(ObTxLockStatIterator &iter);
  int get_memtable_key_arr(ObMemtableKeyArray &memtable_key_arr);
  uint64_t get_lock_for_read_retry_count() const { return mt_ctx_.get_lock_for_read_retry_count(); }

  int check_scheduler_status();
  int remove_callback_for_uncommited_txn(const memtable::ObMemtableSet *memtable_set);
  int64_t get_trans_mem_total_size() const { return mt_ctx_.get_trans_mem_total_size(); }
  int check_with_tx_data(ObITxDataCheckFunctor &fn);
  const share::SCN get_rec_log_ts() const;
  int on_tx_ctx_table_flushed();

  int64_t get_applying_log_ts() const;
  int64_t get_pending_log_size() { return mt_ctx_.get_pending_log_size(); }
  int64_t get_flushed_log_size() { return mt_ctx_.get_flushed_log_size(); }
  void get_audit_info(int64_t &lock_for_read_elapse) const;
  virtual int64_t get_part_trans_action() const override;
  inline bool has_pending_write() const { return pending_write_; }

  // table lock
  void set_table_lock_killed()
  { mt_ctx_.set_table_lock_killed(); }
  bool is_table_lock_killed() const;

  share::ObLSID get_ls_id() const { return ls_id_; }
  uint32_t get_session_id() const { return session_id_; }

  // for elr
  bool is_can_elr() const { return can_elr_; }

  int check_for_standby(const share::SCN &snapshot,
                        bool &can_read,
                        share::SCN &trans_version);
  int handle_trans_ask_state(const ObAskStateMsg &req, ObAskStateRespMsg &resp);
  int handle_trans_ask_state_resp(const ObAskStateRespMsg &msg);
  int handle_trans_collect_state(ObCollectStateRespMsg &resp, const ObCollectStateMsg &req);
  int handle_trans_collect_state_resp(const ObCollectStateRespMsg &msg);

  // tx state check for 4377
  int handle_ask_tx_state_for_4377(bool &is_alive);
public:
  // thread safe
  int64_t to_string(char* buf, const int64_t buf_len) const;
private:
  // thread unsafe
  ON_DEMAND_TO_STRING_KV_("self_ls_id",
                          ls_id_,
                          K_(session_id),
                          K_(associated_session_id),
                          K_(part_trans_action),
                          K_(pending_write),
                          "2pc_role",
                          to_str_2pc_role(get_2pc_role()),
                          K(ctx_tx_data_),
                          K(role_state_),
                          K(create_ctx_scn_),
                          "ctx_source",
                          ctx_source_,
                          K(epoch_),
                          K(replay_completeness_),
                          "upstream_state",
                          to_str_tx_state(upstream_state_),
                          K_(collected),
                          K_(rec_log_ts),
                          K_(prev_rec_log_ts),
                          K_(lastest_snapshot),
                          K_(last_request_ts),
                          KP_(block_frozen_memtable),
                          K_(max_2pc_commit_scn),
                          K(mt_ctx_));


public:
  static const int64_t OP_LOCAL_NUM = 16;
  static const int64_t RESERVED_MEM_SIZE = 256;
private:
  void default_init_();
  int init_memtable_ctx_(const uint64_t tenant_id, const share::ObLSID &ls_id);
  // Please use it carefully, because it only refer to the downstream_state_
  bool is_in_durable_2pc_() const;
  bool is_logging_() const;

  // It is decided based on both durable 2pc state and on the fly logging.
  // So it can be used safely at any time.
  bool is_in_2pc_() const;

  // force abort but not submit abort log
  bool need_force_abort_() const;
  // force abort but wait abort log_cb
  bool is_force_abort_logging_() const;

  bool need_record_log_() const;
  void reset_redo_lsns_();
  void set_prev_record_lsn_(const LogOffSet &prev_record_lsn);
  int trans_clear_();
  int trans_kill_();
  int tx_end_(const bool commit);
  int trans_replay_commit_(const share::SCN &commit_version,
                           const share::SCN &final_log_ts,
                           const uint64_t log_cluster_version,
                           const uint64_t checksum);
  int trans_replay_abort_(const share::SCN &final_log_ts);
  int update_publish_version_(const share::SCN &publish_version, const bool for_replay);
  bool can_be_recycled_();
  bool need_to_ask_scheduler_status_();
  int check_rs_scheduler_is_alive_(bool &is_alive);
  int gc_ctx_();

  int common_on_success_(ObTxLogCb * log_cb);
  int on_success_ops_(ObTxLogCb * log_cb);
  void check_and_register_timeout_task_();
  int recover_ls_transfer_status_();

  int check_dli_batch_completed_(ObTxLogType submit_log_type);
public:
  // ========================================================
  // newly added for 4.0

  bool is_decided() const { return ctx_tx_data_.is_decided(); }
  // check data is rollbacked either partially or totally
  // in which case the reader should be failed
  bool is_data_rollbacked() const { return mt_ctx_.is_tx_rollbacked(); }
  int retry_dup_trx_before_prepare(
      const share::SCN &before_prepare_version,
      const ObDupTableBeforePrepareRequest::BeforePrepareScnSrc before_prepare_src);
  // int merge_tablet_modify_record(const common::ObTabletID &tablet_id);
  int set_scheduler(const common::ObAddr &scheduler);
  const common::ObAddr &get_scheduler() const;
  int on_success(ObTxLogCb *log_cb);
  int on_failure(ObTxLogCb *log_cb);

  virtual int submit_log(const ObTwoPhaseCommitLogType &log_type) override;
  int try_submit_next_log();
  // for instant logging and freezing
  int submit_redo_after_write(const bool force, const ObTxSEQ &write_seq_no);
  int submit_redo_log_for_freeze(const uint32_t freeze_clock);
  int submit_direct_load_inc_redo_log(storage::ObDDLRedoLog &ddl_redo_log,
                                 logservice::AppendCb *extra_cb,
                                 const int64_t replay_hint,
                                 share::SCN &scn);
  int submit_direct_load_inc_start_log(storage::ObDDLIncStartLog &ddl_start_log,
                                 logservice::AppendCb *extra_cb,
                                 share::SCN &scn);
  int submit_direct_load_inc_commit_log(storage::ObDDLIncCommitLog &ddl_commit_log,
                                 logservice::AppendCb *extra_cb,
                                 share::SCN &scn,
                                 bool need_free_extra_cb = false);
  int return_redo_log_cb(ObTxLogCb *log_cb);
  int push_replaying_log_ts(const share::SCN log_ts_ns, const int64_t log_entry_no);
  int push_replayed_log_ts(const share::SCN log_ts_ns,
                           const palf::LSN &offset,
                           const int64_t log_entry_no);
  int iter_next_log_for_replay(ObTxLogBlock &log_block,
                               ObTxLogHeader &log_header,
                               const share::SCN log_scn);
  int replay_one_part_of_big_segment(const palf::LSN &offset,
                                     const share::SCN &timestamp,
                                     const int64_t &part_log_no);

  int replay_redo_in_ctx(const ObTxRedoLog &redo_log,
                         const palf::LSN &offset,
                         const share::SCN &timestamp,
                         const int64_t &part_log_no,
                         const bool is_tx_log_queue,
                         const bool serial_final,
                         const ObTxSEQ &max_seq_no);
  int replay_rollback_to(const ObTxRollbackToLog &log,
                         const palf::LSN &offset,
                         const share::SCN &timestamp,
                         const int64_t &part_log_no,
                         const bool is_tx_log_queue,
                         const bool pre_barrier);
  int replay_active_info(const ObTxActiveInfoLog &active_info_log,
                         const palf::LSN &offset,
                         const share::SCN &timestamp,
                         const int64_t &part_log_no);
  int replay_commit_info(const ObTxCommitInfoLog &commit_info_log,
                         const palf::LSN &offset,
                         const share::SCN &timestamp,
                         const int64_t &part_log_no,
                         const bool pre_barrier);
  int replay_prepare(const ObTxPrepareLog &prepare_log,
                     const palf::LSN &offset,
                     const share::SCN &timestamp,
                     const int64_t &part_log_no);
  int replay_commit(const ObTxCommitLog &commit_log,
                    const palf::LSN &offset,
                    const share::SCN &timestamp,
                    const int64_t &part_log_no,
                    const share::SCN &replay_compact_version);
  int replay_clear(const ObTxClearLog &clear_log,
                   const palf::LSN &offset,
                   const share::SCN &timestamp,
                   const int64_t &part_log_no);
  int replay_abort(const ObTxAbortLog &abort_log,
                   const palf::LSN &offset,
                   const share::SCN &timestamp,
                   const int64_t &part_log_no);

  int replay_multi_data_source(const ObTxMultiDataSourceLog &log,
                               const palf::LSN &lsn,
                               const share::SCN &timestamp,
                               const int64_t &part_log_no);

  int replay_record(const ObTxRecordLog &log,
                    const palf::LSN &lsn,
                    const share::SCN &timestamp,
                    const int64_t &part_log_no);

  void force_no_need_replay_checksum(const bool parallel_replay, const share::SCN &log_ts);

  void check_no_need_replay_checksum(const share::SCN &log_ts, const int index);
  bool is_replay_complete_unknown() const { return replay_completeness_.is_unknown(); }
  int set_replay_incomplete(const share::SCN log_ts);
  // return the min log ts of those logs which are submitted but
  // not callbacked yet, if there is no such log return INT64_MAX
  const share::SCN get_min_undecided_log_ts() const;

  // dump and recover tx ctx table using the following functions.
  // get_tx_ctx_table_info returns OB_TRANS_CTX_NOT_EXIST if the tx ctx table need not to be
  // dumped.
  int get_tx_ctx_table_info(ObTxCtxTableInfo &info);
  int serialize_tx_ctx_to_buffer(ObTxLocalBuffer &buffer, int64_t &serialize_size);
  int recover_tx_ctx_table_info(ObTxCtxTableInfo &ctx_info);

  int correct_cluster_version_(uint64_t cluster_version_in_log);

  // leader switch related
  bool need_callback_scheduler_();
  int switch_to_follower_forcedly(ObTxCommitCallback *&cb_list);
  int switch_to_leader(const share::SCN &start_working_ts);
  int switch_to_follower_gracefully(ObTxCommitCallback *&cb_list);
  int resume_leader(const share::SCN &start_working_ts);
  int supplement_tx_op_if_exist_(const bool for_replay, const share::SCN replay_scn);
  int recover_tx_ctx_from_tx_op_(ObTxOpVector &tx_op_list, const share::SCN replay_scn);

  void set_role_state(const bool for_replay)
  {
    role_state_ = for_replay ? TxCtxRoleState::FOLLOWER : TxCtxRoleState::LEADER;
  }

  int register_multi_data_source(const ObTxDataSourceType type,
                                 const char *buf,
                                 const int64_t len,
                                 const bool try_lock,
                                 const ObTxSEQ seq_no,
                                 const ObRegisterMdsFlag &register_flag);

  int dup_table_tx_redo_sync(const bool need_retry_by_task);
  const share::SCN get_start_log_ts()
  {
    return ctx_tx_data_.get_start_log_ts();
  }

  const share::SCN get_tx_end_log_ts() const
  {
    return ctx_tx_data_.get_end_log_ts();
  }

  void set_retain_cause(RetainCause cause)
  {
    ATOMIC_CAS(&retain_cause_, static_cast<int16_t>(RetainCause::UNKOWN),
               static_cast<int16_t>(cause));
  }
  RetainCause get_retain_cause() const { return static_cast<RetainCause>(ATOMIC_LOAD(&retain_cause_)); };

  int del_retain_ctx();

  // ========================================================
private:
  // ========================================================
  // newly added for 4.0
  int submit_log_impl_(const ObTxLogType log_type);
  void handle_submit_log_err_(const ObTxLogType log_type, int &ret);
  typedef logservice::ObReplayBarrierType ObReplayBarrierType;
  int submit_log_block_out_(ObTxLogBlock &block,
                            const share::SCN &base_scn,
                            ObTxLogCb *&log_cb,
                            const int64_t replay_hint = 0,
                            const ObReplayBarrierType barrier = ObReplayBarrierType::NO_NEED_BARRIER,
                            const int64_t retry_timeout_us = 1000);
  int after_submit_log_(ObTxLogBlock &log_block,
                        ObTxLogCb *log_cb,
                        memtable::ObRedoLogSubmitHelper *redo_helper);
  int submit_commit_log_();
  int submit_abort_log_();
  int submit_prepare_log_();
  int submit_clear_log_();
  int submit_record_log_();
  int submit_redo_commit_info_log_();
  int submit_redo_active_info_log_();
  int submit_redo_if_serial_logging_(ObTxLogBlock &log_block,
                                     bool &has_redo,
                                     memtable::ObRedoLogSubmitHelper &helper);
  int submit_redo_if_parallel_logging_();
  int submit_redo_commit_info_log_(ObTxLogBlock &log_block,
                                   bool &has_redo,
                                   memtable::ObRedoLogSubmitHelper &helper,
                                   logservice::ObReplayBarrierType &barrier);
  int submit_pending_log_block_(ObTxLogBlock &log_block,
                                memtable::ObRedoLogSubmitHelper &helper,
                                const logservice::ObReplayBarrierType &barrier);
  template <typename DLI_LOG>
  int submit_direct_load_inc_log_(DLI_LOG &dli_log,
                                  const ObTxDirectLoadIncLog::DirectLoadIncLogType dli_log_type,
                                  const ObDDLIncLogBasic &batch_key,
                                  logservice::AppendCb *extra_cb,
                                  const logservice::ObReplayBarrierType replay_barrier,
                                  const int64_t replay_hint,
                                  share::SCN &scn,
                                  bool need_free_extra_cb = false);
  bool should_switch_to_parallel_logging_();
  int switch_to_parallel_logging_(const share::SCN serial_final_scn,
                                  const ObTxSEQ max_seq_no);
  bool has_replay_serial_final_() const;
  void recovery_parallel_logging_();
  int check_can_submit_redo_();
  void force_no_need_replay_checksum_(const bool parallel_replay, const share::SCN &log_ts);
  int serial_submit_redo_after_write_(int &submitted_cnt);
  int submit_big_segment_log_();
  int prepare_big_segment_submit_(ObTxLogCb *segment_cb,
                                  const share::SCN &base_scn,
                                  logservice::ObReplayBarrierType barrier_type,
                                  const ObTxLogType & segment_log_type);
  int add_unsynced_segment_cb_(ObTxLogCb *log_cb);
  int remove_unsynced_segment_cb_(const share::SCN &remove_scn);
  share::SCN get_min_unsyncd_segment_scn_();
  int init_log_block_(ObTxLogBlock &log_block,
                      const int64_t suggested_buf_size = ObTxAdaptiveLogBuf::NORMAL_LOG_BUF_SIZE,
                      const bool serial_final = false);
  int reuse_log_block_(ObTxLogBlock &log_block);
  int compensate_abort_log_();
  int validate_commit_info_log_(const ObTxCommitInfoLog &commit_info_log);

  int switch_log_type_(const ObTwoPhaseCommitLogType &log_type,
                       ObTxLogType &ret_log_type);
  int switch_log_type_(const ObTxLogType ret_log_type,
                       ObTwoPhaseCommitLogType &log_type);
  int64_t get_redo_log_no_() const;
  bool has_persisted_log_() const;

  int update_replaying_log_no_(const share::SCN &log_ts_ns, int64_t part_log_no);
  int check_and_merge_redo_lsns_(const palf::LSN &offset);
  int try_submit_next_log_(const bool for_freeze = false);
  // redo lsns is stored when submit log, when log fails to majority
  // and is callbacked via on_failure, redo lsns should be fixed
  int fix_redo_lsns_(const ObTxLogCb *log_cb);

  int search_unsubmitted_dup_table_redo_() __attribute__((__noinline__));
  int dup_table_tx_redo_sync_(const bool need_retry_by_task = true);
  int check_dup_trx_with_submitting_all_redo(ObTxLogBlock &log_block,
                                             memtable::ObRedoLogSubmitHelper &helper);
  bool is_dup_table_redo_sync_completed_();
  int dup_table_tx_pre_commit_();
  // int merge_tablet_modify_record_(const common::ObTabletID &tablet_id);
  // int check_tablet_modify_record_();
  void set_dup_table_tx_()
  {
    exec_info_.is_dup_tx_ = true;
    exec_info_.trans_type_ = TransType::DIST_TRANS;
  }
  int dup_table_before_preapre_(
      const share::SCN &before_prepare_version,
      const bool after_redo_completed,
      const ObDupTableBeforePrepareRequest::BeforePrepareScnSrc before_prepare_src);
  int clear_dup_table_redo_sync_result_();

  int do_local_tx_end_(TxEndAction tx_end_action);
  // int on_local_tx_end_(TxEndAction tx_end_action);
  int do_local_commit_tx_();
  int do_local_abort_tx_();
  int do_force_kill_tx_();
  int on_local_commit_tx_();
  int after_local_commit_succ_();
  int on_local_abort_tx_();

  // int local_tx_abort_();
  int abort_(int reason);

  int update_max_commit_version_();
  // int local_tx_end_(const bool commit);
  // int local_tx_end_side_effect_(const bool commit);
  // int on_local_commit_();
  // int on_local_abort_();
  int on_dist_end_(const bool commit);

  int generate_prepare_version_();
  int generate_commit_version_();

  bool is_committing_() const;
  // int insert_to_tx_table_(ObTxData *tx_data);
  int replay_update_tx_data_(const bool commit,
                             const share::SCN &log_ts,
                             const share::SCN &commit_version);
  int replace_tx_data_with_backup_(const ObTxDataBackup &backup, share::SCN log_ts_ns);
  int check_trans_type_for_replay_(const int32_t &trans_type, const share::SCN &commit_log_ts);
  void set_durable_state_(const ObTxState state)
  { exec_info_.state_ = state; }

  bool is_2pc_logging_() const
  { return sub_state_.is_state_log_submitting() || sub_state_.is_gts_waiting(); }

  int notify_table_lock_(const SCN &log_ts,
                         const bool for_replay,
                         const ObTxBufferNodeArray &notify_array,
                         const bool is_force_kill);
  int notify_data_source_(const NotifyType type,
                          const share::SCN &log_ts,
                          const bool for_replay,
                          const ObTxBufferNodeArray &notify_array,
                          const bool is_force_kill = false);
  int gen_total_mds_array_(ObTxBufferNodeArray &mds_array);
  int deep_copy_mds_array_(const ObTxBufferNodeArray &mds_array,
                           ObTxBufferNodeArray &incremental_array,
                           bool need_replace = false);
  int prepare_mds_tx_op_(const ObTxBufferNodeArray &mds_array,
                         share::SCN op_scn,
                         share::ObTenantTxDataOpAllocator &tx_op_allocator,
                         ObTxOpArray &tx_op_list,
                         bool is_replay);
  int replay_mds_to_tx_table_(const ObTxBufferNodeArray &mds_node_array, const share::SCN op_scn);
  int insert_mds_to_tx_table_(ObTxLogCb &log_cb);
  int insert_undo_action_to_tx_table_(ObUndoAction &undo_action,
                                      ObTxDataGuard &new_tx_data_guard,
                                      storage::ObUndoStatusNode *&undo_node,
                                      const share::SCN op_scn);
  int replay_undo_action_to_tx_table_(ObUndoAction &undo_action, const share::SCN op_scn);
  int decide_state_log_barrier_type_(const ObTxLogType &state_log_type,
                                     logservice::ObReplayBarrierType &final_barrier_type);
  bool is_contain_mds_type_(const ObTxDataSourceType target_type);
  int submit_multi_data_source_();
  int submit_multi_data_source_(ObTxLogBlock &log_block);
  void clean_retain_cause_()
  {
    ATOMIC_STORE(&retain_cause_, static_cast<int16_t>(RetainCause::UNKOWN));
  }

  int try_alloc_retain_ctx_func_();
  int try_gc_retain_ctx_func_();
  int insert_into_retain_ctx_mgr_(RetainCause cause,
                                  const share::SCN &log_ts,
                                  palf::LSN lsn,
                                  bool for_replay);

  int prepare_mul_data_source_tx_end_(bool is_commit);

  int errism_dup_table_redo_sync_();
  int errism_submit_prepare_log_();
  int replay_redo_in_ctx_compat_(const ObTxRedoLog &redo_log,
                                 const palf::LSN &offset,
                                 const share::SCN &timestamp,
                                 const int64_t &part_log_no);
  bool is_support_parallel_replay_() const;
  int set_replay_completeness_(const bool complete, const share::SCN replay_scn);
  int errsim_notify_mds_();
  bool is_support_tx_op_() const;
protected:
  virtual int get_gts_(share::SCN &gts);
  virtual int wait_gts_elapse_commit_version_(bool &need_wait);
  virtual int get_local_max_read_version_(share::SCN &local_max_read_version);
  virtual int update_local_max_commit_version_(const share::SCN &commit_version);
  virtual int check_and_response_scheduler_(ObTxState next_phase, int result);
private:

  int init_log_cbs_(const share::ObLSID&ls_id, const ObTransID &tx_id);
  int extend_log_cbs_(ObTxLogCb *&log_cb);
  void reset_log_cb_list_(common::ObDList<ObTxLogCb> &cb_list);
  void reset_log_cbs_();
  int prepare_log_cb_(const bool need_final_cb, ObTxLogCb *&log_cb);
  int get_log_cb_(const bool need_final_cb, ObTxLogCb *&log_cb);
  int return_log_cb_(ObTxLogCb *log_cb, bool release_final_cb = false);
  int get_max_submitting_log_info_(palf::LSN &lsn, share::SCN &log_ts);
  int get_prev_log_lsn_(const ObTxLogBlock &log_block, ObTxPrevLogType &prev_log_type, palf::LSN &lsn);
  int set_start_scn_in_commit_log_(ObTxCommitLog &commit_log);

  // int init_tx_data_(const share::ObLSID&ls_id, const ObTransID &tx_id);

  bool is_local_tx_() const { return TransType::SP_TRANS == exec_info_.trans_type_; }
  void set_trans_type_(int64_t trans_type) { exec_info_.trans_type_ = trans_type; }
  int set_scheduler_(const common::ObAddr &scheduler);

  int check_replay_avaliable_(const palf::LSN &offset,
                              const share::SCN &timestamp,
                              const int64_t &part_log_no,
                              bool &need_replay);
  bool is_leader_() const { return TxCtxRoleState::LEADER == role_state_; } // inaccurate state when switch leader
  bool is_follower_() const { return TxCtxRoleState::FOLLOWER == role_state_; } //inaccurate state when switch leader
  int update_rec_log_ts_(bool for_replay, const share::SCN &rec_log_ts);
  int refresh_rec_log_ts_();
  int get_tx_ctx_table_info_(ObTxCtxTableInfo &info);
  const share::SCN get_rec_log_ts_() const;

  int check_is_aborted_in_tx_data_(const ObTransID tx_id,
                                   bool &is_aborted);

  // ========================================================

  // ======================== C2PC MSG HANDLER BEGIN ========================
public:
  int handle_tx_2pc_prepare_req(const Ob2pcPrepareReqMsg &msg);
  int handle_tx_2pc_prepare_resp(const Ob2pcPrepareRespMsg &msg);
  int handle_tx_2pc_pre_commit_req(const Ob2pcPreCommitReqMsg &msg);
  int handle_tx_2pc_pre_commit_resp(const Ob2pcPreCommitRespMsg &msg);
  int handle_tx_2pc_commit_req(const Ob2pcCommitReqMsg &msg);
  int handle_tx_2pc_commit_resp(const Ob2pcCommitRespMsg &msg);
  int handle_tx_2pc_abort_req(const Ob2pcAbortReqMsg &msg);
  int handle_tx_2pc_abort_resp(const Ob2pcAbortRespMsg &msg);
  int handle_tx_2pc_clear_req(const Ob2pcClearReqMsg &msg);
  int handle_tx_2pc_clear_resp(const Ob2pcClearRespMsg &msg);
  static int handle_tx_orphan_2pc_msg(const ObTxMsg &recv_msg,
                                      const common::ObAddr& self_addr,
                                      ObITransRpc* rpc,
                                      const bool ls_deleted);
  // for xa
  int handle_tx_2pc_prepare_redo_req(const Ob2pcPrepareRedoReqMsg &msg);
  int handle_tx_2pc_prepare_redo_resp(const Ob2pcPrepareRedoRespMsg &msg);
  int handle_tx_2pc_prepare_version_req(const Ob2pcPrepareVersionReqMsg &msg);
  int handle_tx_2pc_prepare_version_resp(const Ob2pcPrepareVersionRespMsg &msg);
protected:
  // virtual int post_msg(const ObTwoPhaseCommitMsgType &msg_type);
  virtual int post_msg(const ObTwoPhaseCommitMsgType& msg_type,
                       const int64_t participant_id) override;
private:
  int apply_2pc_msg_(const ObTwoPhaseCommitMsgType msg_type);
  int set_2pc_upstream_(const share::ObLSID&upstream);
  int set_2pc_participants_(const ObTxCommitParts &participants);
  int set_2pc_incremental_participants_(const share::ObLSArray &participants);
  int set_2pc_request_id_(const int64_t request_id);
  int update_2pc_prepare_version_(const share::SCN &prepare_version);
  int merge_prepare_log_info_(const ObLSLogInfoArray &info_array);
  int merge_prepare_log_info_(const ObLSLogInfo &prepare_info);
  int set_2pc_commit_version_(const share::SCN &commit_version);
  int find_participant_id_(const share::ObLSID&participant,
                           int64_t &participant_id);
  int post_tx_commit_resp_(const int status);
  int post_msg_(const ObTwoPhaseCommitMsgType& msg_type,
                const share::ObLSID&ls);

  void build_tx_common_msg_(const share::ObLSID&receiver,
                            ObTxMsg &msg);
  static void build_tx_common_msg_(const ObTxMsg &recv_msg,
                                   const common::ObAddr &self_addr,
                                   ObTxMsg &msg);
  static void build_tx_common_msg_(const share::ObLSID &receiver,
                                   const int64_t cluster_version,
                                   const int64_t tenant_id,
                                   const int64_t tx_id,
                                   const common::ObAddr& self_addr,
                                   const share::ObLSID &self_ls_id,
                                   const int64_t cluster_id,
                                   ObTxMsg &msg);
  static int post_orphan_msg_(const ObTwoPhaseCommitMsgType &msg_type,
                              const ObTxMsg &recv_msg,
                              const common::ObAddr &self_addr,
                              ObITransRpc* rpc,
                              const bool ls_deleted);
  static int get_max_decided_scn_(const share::ObLSID &ls_id, share::SCN &scn);
  int get_stat_for_virtual_table(share::ObLSArray &participants, int &busy_cbs_cnt);
  // for xa
  int post_tx_sub_prepare_resp_(const int status);
  int post_tx_sub_commit_resp_(const int status);
  int post_tx_sub_rollback_resp_(const int status);

  int submit_log_if_allow(const char *buf,
                          const int64_t size,
                          const share::SCN &base_ts,
                          ObTxBaseLogCb *cb,
                          const bool need_nonblock,
                          const ObTxCbArgArray &cb_arg_array);
  virtual bool is_2pc_blocking() const override {
    return sub_state_.is_transfer_blocking();
  }

// ======================= for transfer ===============================
public:
  int check_need_transfer(const share::SCN data_end_scn,
                          ObIArray<ObTabletID> &tablet_list,
                          bool &need_transfer);
  int do_transfer_out_tx_op(const share::SCN data_end_scn,
                            const share::SCN op_scn,
                            const NotifyType op_type,
                            const bool is_replay,
                            const ObLSID dest_ls_id,
                            const int64_t transfer_epoch,
                            bool &is_operated);
  int collect_tx_ctx(const share::ObLSID dest_ls_id,
                     const SCN data_end_scn,
                     const ObIArray<ObTabletID> &tablet_list,
                     ObTxCtxMoveArg &arg,
                     bool &is_collected);
  int wait_tx_write_end();
  int move_tx_op(const ObTransferMoveTxParam &move_tx_param,
                 const ObTxCtxMoveArg &arg,
                 const bool is_new_created);
  bool is_exec_complete(ObLSID ls_id, int64_t epoch, int64_t transfer_epoch);
  bool is_exec_complete_without_lock(ObLSID ls_id, int64_t epoch, int64_t transfer_epoch);
private:
  int transfer_op_log_cb_(share::SCN op_scn, NotifyType op_type);
  int load_tx_op_if_exist_();
  int update_tx_data_start_and_end_scn_(const share::SCN start_scn,
                                        const share::SCN end_scn,
                                        const share::SCN transfer_scn);

protected:
  virtual int post_msg_(const share::ObLSID&receiver, ObTxMsg &msg);
  virtual int post_msg_(const ObAddr &server, ObTxMsg &msg);
private:
  static ObTwoPhaseCommitMsgType switch_msg_type_(const int16_t msg_type);
  // ========================= C2PC MSG HANDLER END =========================

  // ========================== TX COMMITTER BEGIN ==========================
protected:
  virtual Ob2PCRole get_2pc_role() const  override;
  virtual int64_t get_downstream_size() const override;
  virtual int64_t get_self_id();

  virtual bool is_2pc_logging() const override;
  virtual ObTxState get_downstream_state() const override
  { return exec_info_.state_; }
  virtual int set_downstream_state(const ObTxState state) override
  { set_durable_state_(state); return OB_SUCCESS; }
  virtual ObTxState get_upstream_state() const override
  { return upstream_state_; }
  virtual int set_upstream_state(const ObTxState state) override
  {
    upstream_state_ = state;
    return OB_SUCCESS;
  }
  // for xa
  //virtual bool is_prepared_sub2pc() override
  //{
  //  return is_sub2pc() && ObTxState::PREPARE == coord_state_
  //         && all_downstream_collected_();
  //}

  // Caller need ensuere the participants array has already been set and the
  // size of the participants array is larger or equal than one.
  virtual int do_prepare(bool &no_need_submit_log) override;
  virtual int on_prepare() override;
  virtual int do_pre_commit(bool &need_wait) override;
  virtual int do_commit() override;
  virtual int on_commit() override;
  virtual int do_abort() override;
  virtual int on_abort() override;
  virtual int do_clear() override;
  virtual int on_clear() override;
  // for xa
  virtual int reply_to_scheduler_for_sub2pc(int64_t msg_type) override;

private:
  // int tx_end_(const bool commit, const int64_t commit_version);
  void register_gts_callback_();
  int restart_2pc_trans_timer_();
  // ============================ TX COMMITTER END ============================
public:
  /*
   * check_status - check txn ctx is ready to acccept access
   *
   * the precoditions include:
   *  is leader, un-terminated, not changing leader, etc.
   */
  int check_status();
  /*
   * start_access - start txn protected resources access
   * @data_seq: the sequence_no of current access will be alloced
   *            new created data will marked with this seq no
   * @branch: branch id of this access
   */
  int start_access(const ObTxDesc &tx_desc, ObTxSEQ &data_seq, const int16_t branch);
  /*
   * end_access - end of txn protected resources access
   */
  int end_access();
  int rollback_to_savepoint(const int64_t op_sn,
                            ObTxSEQ from_seq,
                            const ObTxSEQ to_seq,
                            const int64_t seq_base,
                            const int64_t request_id,
                            ObIArray<ObTxLSEpochPair> &downstream_parts);
  bool is_xa_trans() const { return !exec_info_.xid_.empty(); }
  bool is_transfer_deleted() const { return transfer_deleted_; }
  int handle_tx_keepalive_response(const int64_t status);
private:
  bool fast_check_need_submit_redo_for_freeze_() const;
  int check_status_();
  int tx_keepalive_response_(const int64_t status);
  void post_keepalive_msg_(const int status);
  void notify_scheduler_tx_killed_(const int kill_reason);
  int rollback_to_savepoint_(const ObTxSEQ from_scn,
                             const ObTxSEQ to_scn,
                             const share::SCN replay_scn = share::SCN::invalid_scn());
  int submit_rollback_to_log_(const ObTxSEQ from_scn,
                              const ObTxSEQ to_scn);
  int set_state_info_array_();
  int update_state_info_array_(const ObStateInfo& state_info);
  int update_state_info_array_with_transfer_parts_(const ObTxCommitParts &parts, const ObLSID &ls_id);
  void build_and_post_collect_state_msg_(const share::SCN &snapshot);
  int build_and_post_ask_state_msg_(const share::SCN &snapshot,
                                    const share::ObLSID &ori_ls_id, const ObAddr &ori_addr);
  int check_ls_state_(const SCN &snapshot, const ObLSID &ls_id, const ObStandbyCheckInfo &check_info);
  int process_errsim_for_standby_read_(int err_code, int ret_code);
  int get_ls_replica_readable_scn_(const ObLSID &ls_id, SCN &snapshot_version);
  int submit_redo_log_for_freeze_(bool &try_submit, const uint32_t freeze_clock);
  void print_first_mvcc_callback_();
public:
  int prepare_for_submit_redo(ObTxLogCb *&log_cb,
                              ObTxLogBlock &log_block,
                              const bool serial_final = false);
  int submit_redo_log_out(ObTxLogBlock &log_block,
                          ObTxLogCb *&log_cb,
                          memtable::ObRedoLogSubmitHelper &helper,
                          const int64_t replay_hint,
                          const bool has_hold_ctx_lock,
                          share::SCN &submitted_scn);
  bool is_parallel_logging() const;
  int assign_commit_parts(const share::ObLSArray &log_participants,
                          const ObTxCommitParts &log_commit_parts);
protected:
  // for xa
  virtual bool is_sub2pc() const override
  { return exec_info_.is_sub2pc_; }
  virtual bool is_dup_tx() const override
  { return exec_info_.is_dup_tx_; }

  // =========================== TREE COMMITTER START ===========================
public:
  // merge the intermediate_participants into participants during 2pc state transfer
  virtual int merge_intermediate_participants() override;
  // is_real_upstream presents whether we are handling requests from the real
  // upstream:
  // - If the sender equals to the upstream, it means we that are handling the
  //   real leader and we need collect all responses from the downstream before
  //   responsing to the upstream
  // - If the sender is different from the upstream, it means we are handling
  //   requests from the upstream other than the real upstream. To prevent from
  //   the deadlock in the cycle commit, we only need consider the situation of
  //   myself before responsing to the upstream
  // - It may be no sender during handle_timeout, it means we are retransmitting
  //   the requests and responses, so we only need pay attention to the upstream
  //   and all downstreams for retransmitting
  virtual bool is_real_upstream() override;
  // add_intermediate_participants means add participant into intermediate_participants,
  // which is important to ensure the consistency of participants during tree commit
  int add_intermediate_participants(const ObLSID ls_id, int64_t transfer_epoch);
private:
  bool is_real_upstream_(const ObLSID upstream);

private:
  DISALLOW_COPY_AND_ASSIGN(ObPartTransCtx);
private:
  //0x0078746374726170 means reset partctx
  static const int64_t PART_CTX_MAGIC_NUM = 0x0078746374726170;
  static const int64_t REPLAY_PRINT_TRACE_THRESHOLD = 10 * 1000; // 10 ms
  static const int64_t REDO_SYNC_TASK_RETRY_INTERVAL_US = 10 * 1000; // 10ms
  static const int64_t PRE_COMMIT_TASK_RETRY_INTERVAL_US = 5 * 1000; // 5ms
  static const int64_t END_STMT_SLEEP_US = 10 * 1000; // 10ms
  static const int64_t MAX_END_STMT_RETRY_TIMES = 100;
  static const uint64_t MAX_PREV_LOG_IDS_COUNT = 1024;
  static const bool NEED_FINAL_CB = true;
private:
  bool is_inited_;
  memtable::ObMemtableCtx mt_ctx_;
  uint64_t cluster_id_;
  share::SCN end_log_ts_;
  int64_t stmt_expired_time_;

  int64_t last_ask_scheduler_status_ts_;
  int64_t cur_query_start_time_;
  // when cluster_version is unknown at ctx created time, will choice
  // CLUSTER_CURRENT_VERSION, which may not the real cluster_version
  // of this transaction
  // this can only happen when create ctx for replay and create ctx
  // for recovery before v.4.3
  bool cluster_version_accurate_;
  /*
   * used during txn protected data access
   */
  // number of in-progress access
  int pending_write_;
  // LogHandler's epoch at which this part_ctx was created, used to detect
  // participant amnesia: crash and then recreated by obsolete message or operation
  int64_t epoch_;
  // latest operation sequence no, used to detect duplicate operation
  int64_t last_op_sn_;
  // data sequence no of latest access
  ObTxSEQ last_scn_;
  // data sequence no of first access
  ObTxSEQ first_scn_;
private:
  TransModulePageAllocator reserve_allocator_;
  // ========================================================
  // newly added for 4.0
  // persistent state
  ObTxExecInfo exec_info_;
  ObCtxTxData ctx_tx_data_;
  // when multi source data is registered, it is stored in the array below,
  // it is transfered to exec_info_.multi_source_data_ when corresponding
  // redo log callbacked.
  ObTxMDSCache mds_cache_;
  ObIRetainCtxCheckFunctor *retain_ctx_func_ptr_;
  // sub_state_ is volatile
  ObTxSubState sub_state_;
  ObTxLogCb log_cbs_[PREALLOC_LOG_CALLBACK_COUNT];
  common::ObDList<ObTxLogCb> free_cbs_;
  common::ObDList<ObTxLogCb> busy_cbs_;
  ObTxLogCb final_log_cb_;
  ObSpinLock log_cb_lock_;
  ObTxLogBigSegmentInfo big_segment_info_;
  // flag if the first callback is linked to a logging_block memtable
  // to prevent unnecessary submit_log actions for freeze
  memtable::ObMemtable *block_frozen_memtable_;
  // The semantic of the rec_log_ts means the log ts of the first state change
  // after the previous checkpoint. So we use the current strategy to maintain
  // the rec_log_ts:
  //
  // 1. Txn ctx maintians two variables named rec_log_ts and prev_rec_log_ts
  // 2. When the txn submits the log, if rec_log_ts is the default value,
  //    update it
  // 3. Each time the tx ctx table is checkpointed, set prev_rec_log_ts to be
  //    the rec_log_ts if unset and rec_log_ts to be default value
  // 4. When the checkpoint is succeed, we set prev_rec_log_ts to be the default value
  // 5. Get rec_log_ts: if prev_rec_log_ts exists, return prev_rec_log_ts,
  //    otherwise return rec_log_ts
  // 6. NB(TODO(handora.qc): Should we maintain it): Requirement is neccessary
  //    that the merge of the tx ctx table must be one at a time
  share::SCN rec_log_ts_;
  share::SCN prev_rec_log_ts_;
  bool is_ctx_table_merged_;
  // trace_info_
  int64_t role_state_;

  // +-------------------+                   +---------------------------------+                      +-------+     +-----------------+     +----------------------+
  // | tx_ctx A exiting  |                   |                                 |                      |       |     |   replay from   |     |                      |
  // | start_log_ts = n  |  recover_ts = n   | remove from tx_ctx_table & dump |  recover_ts = n+10   | crash |     | min_ckpt_ts n+m |     | tx_ctx is incomplete |
  // | end_log_ts = n+10 | ----------------> |                                 | -------------------> |       | --> |    (0<m<10)     | --> |                      |
  // +-------------------+                   +---------------------------------+                      +-------+     +-----------------+     +----------------------+
  struct ReplayCompleteness {
    ReplayCompleteness(): complete_(C::UNKNOWN) {}
    void reset() { complete_ = C::UNKNOWN; }
    enum class C : int { COMPLETE = 1, INCOMPLETE = 0, UNKNOWN = -1 } complete_;
    void set(const bool complete) { complete_ = complete ? C::COMPLETE : C::INCOMPLETE; }
    bool is_unknown() const { return complete_ == C::UNKNOWN; }
    bool is_complete() const { return complete_ == C::COMPLETE; }
    bool is_incomplete() const { return complete_ == C::INCOMPLETE; }
    DECLARE_TO_STRING { int64_t pos = 0; BUF_PRINTF("%d", complete_); return pos; };
  } replay_completeness_;
  // set true when submitting redo log for freezing and reset after freezing
  bool is_submitting_redo_log_for_freeze_;
  share::SCN create_ctx_scn_; // replay or recover debug
  PartCtxSource ctx_source_; // For CDC - prev_lsn

  share::SCN start_working_log_ts_;

  share::SCN dup_table_follower_max_read_version_;

  int16_t retain_cause_;

  ObTxState upstream_state_;
  const ObTxMsg * msg_2pc_cache_;
  share::SCN max_2pc_commit_scn_;
  ObLSLogInfoArray coord_prepare_info_arr_;
  // tmp scheduler addr is used to post response for the second phase of xa commit/rollback
  common::ObAddr tmp_scheduler_;
  // for standby
  ObStateInfoArray state_info_array_;
  share::SCN lastest_snapshot_; /* for coord */
  common::ObBitSet<> standby_part_collected_; /* for coord */
  common::ObTimeInterval ask_state_info_interval_;
  common::ObTimeInterval refresh_state_info_interval_;
  // this is used to denote the time of last request including start_access, commit, rollback
  // this is a tempoary variable which is set to now by default
  // therefore, if a follower switchs to leader, the variable is set to now
  int64_t last_request_ts_;

  // for transfer move tx ctx to clean for abort
  bool transfer_deleted_;

  // TODO(handora.qc): remove after fix the transfer bwteen rollback_to bug
  int64_t last_rollback_to_request_id_;
  int64_t last_rollback_to_timestamp_;
  int64_t last_transfer_in_timestamp_;
  // ========================================================
};

// reserve log callback for freezing and other two log callbacks for normal
STATIC_ASSERT(OB_TX_MAX_LOG_CBS >= PREALLOC_LOG_CALLBACK_COUNT &&
    PREALLOC_LOG_CALLBACK_COUNT >= (RESERVE_LOG_CALLBACK_COUNT_FOR_FREEZING + 2), "log callback is not enough");

#if defined(__x86_64__)
/* uncomment this block to error messaging real size
template<int s> struct size_of_xxx_;
static size_of_xxx_<sizeof(ObPartTransCtx)> _x;
*/
// orinally 11264 -> 10000 -> 11264(for 32 log_cbs)
//STATIC_ASSERT(sizeof(ObPartTransCtx) < 15000, "ObPartTransCtx is too big ");
#endif

} // transaction
} // oceanbase

#endif // OCEANBASE_TRANSACTION_OB_TRANS_PART_CTX_
