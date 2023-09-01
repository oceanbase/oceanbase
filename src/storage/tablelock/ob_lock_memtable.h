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

class ObLockMemtable
  : public memtable::ObIMemtable,
    public storage::checkpoint::ObCommonCheckpoint
{
public:
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
  // pre_check_lock to reduce useless lock and lock rollback
  // 1. check_lock_exist in mem_ctx
  // 2. check_lock_conflict in obj_map
  int check_lock_conflict(const memtable::ObMemtableCtx *mem_ctx,
                          const ObTableLockOp &lock_op,
                          ObTxIDSet &conflict_tx_set,
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
  bool is_frozen_memtable() const override;
  bool is_active_memtable() const override;

  // =========== INHERITED FROM ObCommonCheckPoint ==========
  virtual share::SCN get_rec_scn();
  virtual int flush(share::SCN recycle_scn, bool need_freeze = true);

  virtual ObTabletID get_tablet_id() const;

  virtual bool is_flushing() const;

  // ====================== REPLAY LOCK ======================
  virtual int replay_row(storage::ObStoreCtx &ctx,
                         memtable::ObMemtableMutatorIterator *mmi);
  // replay lock to lock map and trans part ctx.
  // used by the replay process of multi data source.
  int replay_lock(memtable::ObMemtableCtx *mem_ctx,
                  const ObTableLockOp &lock_op,
                  const share::SCN &scn);

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
                              ObFunction<int(bool &need_wait)> &recheck_f);
  int register_into_deadlock_detector_(const ObStoreCtx &ctx,
                                       const ObTableLockOp &lock_op);
  int unregister_from_deadlock_detector_(const ObTableLockOp &lock_op);
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

  storage::ObFreezer *freezer_;
  RWLock flush_lock_;        // lock before change ts
};

} // tablelock
} // transaction
} // oceanbase
#endif
