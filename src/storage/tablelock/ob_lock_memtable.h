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

#ifndef OCEANBASE_STORAGE_OB_LOCK_MEMTABLE_
#define OCEANBASE_STORAGE_OB_LOCK_MEMTABLE_

#include "storage/checkpoint/ob_common_checkpoint.h"
#include "storage/memtable/ob_memtable_interface.h"
#include "storage/tablelock/ob_obj_lock.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "logservice/ob_append_callback.h"
#include "logservice/ob_log_base_type.h"
#include "logservice/ob_log_base_header.h"
#include "logservice/ob_log_handler.h"
#include "share/scn.h"

namespace oceanbase
{

namespace storage
{
class ObFreezer;
}

namespace memtable
{
class ObMemtableMutatorIterator;
class ObMvccAccessCtx;
class ObMemtableCtx;
}

namespace transaction
{
namespace tablelock
{
struct ObLockParam;

class ObLockTableSplitLog final
{
public:
  OB_UNIS_VERSION(1);
public:
  ObLockTableSplitLog() { reset(); }
  ~ObLockTableSplitLog() { reset(); }

  int init(common::ObTabletID &src_tablet_id, const ObSArray<common::ObTabletID> &dst_tablet_ids);

  void reset() {
    src_tablet_id_ = 0;
    dst_tablet_ids_.reset();
  }

  const ObTabletID &get_src_tablet_id() { return src_tablet_id_; }
  ObSArray<ObTabletID> &get_dst_tablet_ids() { return dst_tablet_ids_; }
  TO_STRING_KV(K(src_tablet_id_), K(dst_tablet_ids_));

private:
  ObTabletID src_tablet_id_;
  ObSArray<ObTabletID> dst_tablet_ids_;
};

class ObLockTableSplitLogCb : public logservice::AppendCb
{
friend class ObOBJLock;
public:
  ObLockTableSplitLogCb()
    : is_inited_(false),
      memtable_(nullptr),
      ls_id_(share::ObLSID::INVALID_LS_ID),
      src_tablet_id_(OB_INVALID_ID),
      dst_tablet_ids_(),
      is_logging_(false),
      cb_success_(false),
      last_submit_log_ts_(OB_INVALID_TIMESTAMP),
      last_submit_scn_() {}
  virtual ~ObLockTableSplitLogCb() {}
  int init(ObLockMemtable *memtable, const share::ObLSID &ls_id);
  int set(const ObTabletID &src_tablet_id, const ObSArray<common::ObTabletID> &dst_tablet_ids);
  bool is_valid();
  bool cb_success() { return cb_success_; };
  // we should ensure that the lock memtable won't be released
  // after submit_log, until the callback on_success or on_failure
  virtual int on_success() override;
  virtual int on_failure() override;
  const char *get_cb_name() const override { return "LockTableSplitLogCb"; }
  TO_STRING_KV(K(is_inited_),
               K(memtable_),
               K(ls_id_),
               K(src_tablet_id_),
               K(dst_tablet_ids_),
               K(last_submit_log_ts_),
               K(last_submit_scn_),
               K(is_logging_),
               K(cb_success_));
private:
  bool is_valid_(const ObLockMemtable *memtable, const share::ObLSID &ls_id);
private:
  bool is_inited_;

  ObLockMemtable *memtable_;
  share::ObLSID ls_id_;
  ObTabletID src_tablet_id_;
  ObSArray<ObTabletID> dst_tablet_ids_;

  // To ensure only one writer can append log to LS at the same time,
  // and only submit_log will change it to be true.
  bool is_logging_;
  bool cb_success_;
  // params get from the LS
  int64_t last_submit_log_ts_;
  share::SCN last_submit_scn_;
};

class ObLockMemtable
  : public storage::ObIMemtable,
    public storage::checkpoint::ObCommonCheckpoint
{
public:
  enum ObTableLockSplitStatus
  {
    INVALID_STATE = 0,
    NO_SPLIT = 1,  // maybe has splitted into as a dst_tablet
    WAIT_FOR_CALLBACK = 2,
    SPLITTING_OUT = 3,  // for src_tablet only
    SPLITTING_IN = 4,  // the cb_ has been callbacked
    SPLITTED = 5
  };

  ObLockMemtable();
  ~ObLockMemtable();

  int init(const ObITable::TableKey &table_key,
           const share::ObLSID &ls_id,
           storage::ObFreezer *freezer);
  void reset();
  // =================== LOCK FUNCTIONS =====================
  // try to lock a object.
  int lock(const ObLockParam &param,
           storage::ObStoreCtx &ctx,
           ObTableLockOp &lock_op);
  int unlock(storage::ObStoreCtx &ctx,
             const ObTableLockOp &unlock_op,
             const bool is_try_lock = true,
             const int64_t expired_time = 0);
  int replace(storage::ObStoreCtx &ctx,
              const ObReplaceLockParam &param,
              const ObTableLockOp &unlock_op,
              ObTableLockOp &new_lock_op);
  // pre_check_lock to reduce useless lock and lock rollback
  // 1. check_lock_exist in mem_ctx
  // 2. check_lock_conflict in obj_map
  int check_lock_conflict(const memtable::ObMemtableCtx *mem_ctx,
                          const ObTableLockOp &lock_op,
                          ObTxIDSet &conflict_tx_set,
                          const int64_t expired_time,
                          const bool include_finish_tx = true,
                          const bool only_check_dml_lock = false);
  // remove all the locks of the specified tablet
  // @param[in] tablet_id, which tablet will be removed.
  int remove_tablet_lock(const ObTabletID &tablet_id);
  // recover a lock op from lock op info. used for restart.
  // @param[in] op_info, store all the information need by a lock op.
  int recover_obj_lock(const ObTableLockOp &op_info);
  // used by callback. remove lock records while the transaction is
  // aborted or a unlock op commited.
  // @param[in] op_info, the commited unlock op or the lock op which is aborted.
  void remove_lock_record(const ObTableLockOp &op_info);
  // update the lock status.
  // @param[in] op_info, which lock op need update.
  // @param[in] commit_version, if it is called by commit, set the commit version too.
  // @param[in] commit_scn, if it is called by commit, set the commit logts too.
  // @param[in] status, the lock op status will be set.
  int update_lock_status(const ObTableLockOp &op_info,
                         const share::SCN &commit_version,
                         const share::SCN &commit_scn,
                         const ObTableLockOpStatus status);

  void update_rec_and_max_committed_scn(const share::SCN &scn);
  int get_table_lock_store_info(ObIArray<ObTableLockOp> &store_arr);

  // get all the lock id in the lock map
  // @param[out] iter, the iterator returned.
  // int get_lock_id_iter(ObLockIDIterator &iter);
  int get_lock_id_iter(ObLockIDIterator &iter);

  // get the lock op iterator of a obj lock
  // @param[in] lock_id, which obj lock's lock op will be iterated.
  // @param[out] iter, the iterator returned.
  int get_lock_op_iter(const ObLockID &lock_id,
                       ObLockOpIterator &iter);

  // Iterate obj lock in lock map, and check 2 status of it:
  // 1. Check whether the lock ops in the obj lock can be compacted.
  // If it can be compacted (i.e. there're paired lock op and unlock
  // op), remove them from the obj lock and recycle resources.
  // 2. Check whether the obj lock itself is empty.
  // If it's empty (i.e. no lock ops in it), remove it from the lock
  // map and recycle resources.
  int check_and_clear_obj_lock(const bool force_compact);
  // ================ INHERITED FROM ObIMemtable ===============
  // We need to inherient the memtable method for merge process to iterate the
  // lock for dumping the lock table.
  virtual int scan(const storage::ObTableIterParam &param,
                   storage::ObTableAccessContext &context,
                   const blocksstable::ObDatumRange &key_range,
                   storage::ObStoreRowIterator *&row_iter) override;

  virtual bool can_be_minor_merged() override;

  int on_memtable_flushed() override;
  bool is_frozen_memtable() override;
  bool is_active_memtable() override;

  // =========== INHERITED FROM ObCommonCheckPoint ==========
  virtual share::SCN get_rec_scn() override;
  virtual int flush(share::SCN recycle_scn, int64_t trace_id, bool need_freeze = true);

  virtual ObTabletID get_tablet_id() const;

  virtual bool is_flushing() const;

  // ====================== REPLAY LOCK ======================
  virtual int replay_row(storage::ObStoreCtx &ctx,
                         const share::SCN &scn,
                         memtable::ObMemtableMutatorIterator *mmi);
  // replay lock to lock map and trans part ctx.
  // used by the replay process of multi data source.
  int replay_lock(memtable::ObMemtableCtx *mem_ctx,
                  const ObTableLockOp &lock_op,
                  const share::SCN &scn);

  // ====================== SPLIT TABLE LOCK ======================
  int table_lock_split(const common::ObTabletID &src_tablet_id,
                       const ObSArray<common::ObTabletID> &dst_tablet_ids,
                       const transaction::ObTransID &trans_id);
  int replay_split_log(const void *buffer,
                       const int64_t nbytes,
                       const palf::LSN &lsn,
                       const share::SCN &scn);
  int get_split_status(const ObTabletID src_tablet_id,
                       const ObSArray<ObTabletID> dst_tablet_ids,
                       ObTableLockSplitStatus &src_split_status,
                       ObTableLockSplitStatus &dst_split_status);
  // The split_epoch here is the submit scn of the log of table lock split,
  // identifies the time at which table lock split begins.
  // The table locks which are generated by dml will be stored in destination
  // tablets, until all of them have finished, i.e. the transactions which
  // start before this split_epoch have been committed/aborted.
  int add_split_epoch(const share::SCN &split_epoch,
                      const ObSArray<common::ObTabletID> &tablet_ids,
                      const bool for_replay = false);
  int add_split_epoch(const share::SCN &split_epoch,
                      const common::ObTabletID &tablet_id,
                      const bool for_replay = false);

  // ================ NOT SUPPORTED INTERFACE ===============

  virtual int get(const storage::ObTableIterParam &param,
                  storage::ObTableAccessContext &context,
                  const blocksstable::ObDatumRowkey &rowkey,
                  blocksstable::ObDatumRow &row) override;

  virtual int get(const storage::ObTableIterParam &param,
                  storage::ObTableAccessContext &context,
                  const blocksstable::ObDatumRowkey &rowkey,
                  storage::ObStoreRowIterator *&row_iter) override;

  virtual int multi_get(const storage::ObTableIterParam &param,
                        storage::ObTableAccessContext &context,
                        const common::ObIArray<blocksstable::ObDatumRowkey> &rowkeys,
                        storage::ObStoreRowIterator *&row_iter) override;

  virtual int multi_scan(const storage::ObTableIterParam &param,
                         storage::ObTableAccessContext &context,
                         const common::ObIArray<blocksstable::ObDatumRange> &ranges,
                         storage::ObStoreRowIterator *&row_iter) override;

  virtual int get_frozen_schema_version(int64_t &schema_version) const override;

  void set_flushed_scn(const share::SCN &flushed_scn) { flushed_scn_ = flushed_scn; }
  int add_priority_task(const ObLockParam &param,
                        ObStoreCtx &ctx,
                        ObTableLockOp &lock_op);
  int prepare_priority_task(const ObTableLockPrioArg &arg,
                            const ObTableLockOp &lock_op);
  int remove_priority_task(const ObTableLockPrioArg &arg,
                           const ObTableLockOp &op_info);
  int switch_to_leader();
  int switch_to_follower();

  void enable_check_tablet_status(const bool need_check);

  INHERIT_TO_STRING_KV("ObITable", ObITable, KP(this), K_(snapshot_version), K_(ls_id));
private:
  enum ObLockStep {
    STEP_BEGIN = 0,
    STEP_IN_LOCK_MGR,
    STEP_IN_MEM_CTX,
    STEP_AT_CALLBACK_LIST
  };
private:
  int lock_(const ObLockParam &param,
            storage::ObStoreCtx &ctx,
            ObTableLockOp &lock_op);
  int unlock_(storage::ObStoreCtx &ctx,
              const ObTableLockOp &unlock_op,
              const bool is_try_lock = true,
              const int64_t expired_time = 0);
  int check_lock_need_replay_(memtable::ObMemtableCtx *mem_ctx,
                              const ObTableLockOp &lock_op,
                              const share::SCN &scn,
                              bool &need_replay);
  int replay_lock_(memtable::ObMemtableCtx *mem_ctx,
                   const ObTableLockOp &lock_op,
                   const share::SCN &scn);
  int post_obj_lock_conflict_(memtable::ObMvccAccessCtx &acc_ctx,
                              const ObLockID &lock_id,
                              const ObTableLockMode &lock_mode,
                              const ObTransID &conflict_tx_id,
                              ObFunction<int(bool &need_wait)> &recheck_f,
                              const ObTxSEQ &lock_seq);
  // table lock split just support in_trans dml lock,
  // table split should be failed when exist_cannot_split_lock
  int check_exist_cannot_split_lock_(const common::ObTabletID &src_tablet_id,
                                     const ObTransID &split_start_trans_id,
                                     bool &exist_cannot_split_lock);
  int need_split_(const common::ObTabletID &src_tablet_id,
                  bool &need_split);
  int submit_log_(common::ObTabletID src_tablet_id,
                  const ObSArray<common::ObTabletID> &dst_tablet_ids);
  // cannot add OUT_TRANS_LOCK when tablet is splitting
  // except split_start_transaction
  int check_table_lock_split_(const ObTableLockOp &lock_op,
                              const ObStoreCtx &ctx);
  int check_valid_for_table_lock_split_(const common::ObTabletID src_tablet_id,
                                        const ObSArray<common::ObTabletID> &dst_tablet_ids);
  int get_split_status_(const ObTabletID tablet_id, ObTableLockSplitStatus &split_status);

  int register_into_deadlock_detector_(const storage::ObStoreCtx &ctx,
                                       const ObTableLockOp &lock_op);
  int unregister_from_deadlock_detector_(const ObTableLockOp &lock_op);
  int check_tablet_write_allow_(const ObTableLockOp &lock_op,
                                const int64_t input_transfer_counter,
                                int64_t &output_transfer_counter);
  int get_lock_wait_expire_ts_(const int64_t lock_wait_start_ts);
  int check_and_set_tx_lock_timeout_(const memtable::ObMvccAccessCtx &acc_ctx, int64_t &lock_wait_start_ts);
private:
  typedef common::SpinRWLock RWLock;
  typedef common::SpinRLockGuard RLockGuard;
  typedef common::SpinWLockGuard WLockGuard;
  typedef common::ObLinearHashMap<ObString, uint64_t> LockHandleMap;

  bool is_inited_;

  // the lock map store lock data
  ObOBJLockMap obj_lock_map_;
  LockHandleMap allocated_lockhandle_map_;

  share::SCN freeze_scn_;
  // data before the flushed_scn_ have been flushed
  share::SCN flushed_scn_;
  share::SCN rec_scn_;
  share::SCN pre_rec_scn_;
  share::SCN max_committed_scn_;
  bool is_frozen_;
  // for transfer to enable tablet status check
  bool need_check_tablet_status_;
  // for detect the transfer between the table lock operation
  int64_t transfer_counter_;

  storage::ObFreezer *freezer_;
  RWLock flush_lock_;        // lock before change ts
};

} // tablelock
} // transaction
} // oceanbase
#endif
