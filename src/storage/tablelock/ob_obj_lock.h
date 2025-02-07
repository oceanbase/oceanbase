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

#ifndef OCEANBASE_STORAGE_TABLELOCK_OB_OBJ_LOCK_
#define OCEANBASE_STORAGE_TABLELOCK_OB_OBJ_LOCK_

#include <stdint.h>
#include "share/ob_force_print_log.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/hash/ob_iteratable_hashset.h"
#include "lib/hash/ob_link_hashmap.h"
#include "lib/hash/ob_hashmap.h"
#include "share/scn.h"
#include "storage/tablelock/ob_table_lock_common.h"
#include "storage/ob_i_store.h"

namespace oceanbase
{

namespace share
{
class ObLSID;
}

namespace storage
{
struct ObStoreCtx;
}

namespace blocksstable
{
class ObMacroBlockWriter;
}

namespace transaction
{
class ObTransID;

namespace tablelock
{
struct ObLockParam;
class ObLockTableSplitLogCb;
//typedef common::ObSEArray<ObTransID, 16> ObTxIDArray;
typedef common::hash::ObIteratableHashSet<ObTransID, 16> ObTxIDSet;

using RWLock = common::TCRWLock;
using RDLockGuard = common::TCRLockGuard;
using WRLockGuard = common::TCWLockGuard;
using RDLockGuardWithTimeout = common::TCRWLock::RLockGuardWithTimeout;
using WRLockGuardWithTimeout = common::TCRWLock::WLockGuardWithTimeout;

class ObTableLockOpLinkNode : public common::ObDLinkBase<ObTableLockOpLinkNode>
{
public:
  ObTableLockOpLinkNode()
    : lock_op_()
  {}
  bool is_complete_outtrans_lock() const;
  bool is_complete_outtrans_unlock() const;
  int init(const ObTableLockOp &op_info);
  void set_lock_op_status(const ObTableLockOpStatus status)
  {
    lock_op_.lock_op_status_ = status;
  }
  int assign(const ObTableLockOpLinkNode &other);
  ObTableLockOwnerID get_owner_id() const { return lock_op_.owner_id_; };
  ObTransID get_trans_id() const { return lock_op_.create_trans_id_; };
  bool is_valid() const { return lock_op_.is_valid(); }
  int get_table_lock_store_info(ObTableLockOp &info);
  TO_STRING_KV(K_(lock_op));
public:
  ObTableLockOp lock_op_;
};

typedef ObDList<ObTableLockOpLinkNode> ObTableLockOpList;
class ObOBJLock : public share::ObLightHashLink<ObOBJLock>
{
public:
  static const int64_t SUBMIT_LOG_ALARM_INTERVAL = 100 * 1000;
  ObOBJLock(const ObLockID &lock_id);
  int lock(
      const ObLockParam &param,
      storage::ObStoreCtx &ctx,
      const ObTableLockOp &lock_op,
      const uint64_t lock_mode_cnt_in_same_trans[],
      ObMalloc &allocator,
      ObTxIDSet &conflict_tx_set);
  int unlock(
      const ObTableLockOp &unlock_op,
      const bool is_try_lock,
      const int64_t expired_time,
      ObMalloc &allocator);
  void remove_lock_op(
      const ObTableLockOp &lock_op,
      ObMalloc &allocator);
  int recover_lock(
      const ObTableLockOp &lock_op,
      ObMalloc &allocator);
  // only update the status of the exact one with the same lock_op
  int update_lock_status(
      const ObTableLockOp &lock_op,
      const share::SCN commit_version,
      const share::SCN commit_scn,
      const ObTableLockOpStatus status,
      ObMalloc &allocator);
  int check_allow_lock(
      const ObTableLockOp &lock_op,
      const uint64_t lock_mode_cnt_in_same_trans[],
      ObTxIDSet &conflict_tx_set,
      bool &conflict_with_dml_lock,
      const int64_t expired_time,
      ObMalloc &allocator,
      const bool include_finish_tx = true,
      const bool only_check_dml_lock = false);
  share::SCN get_min_ddl_lock_committed_scn(const share::SCN &flushed_scn) const;
  int get_table_lock_store_info(
      ObIArray<ObTableLockOp> &store_arr,
      const share::SCN &freeze_scn);
  int compact_tablelock(ObMalloc &allocator,
                        bool &is_compacted,
                        const bool is_force = false);
  void reset(ObMalloc &allocator);
  void reset_without_lock(ObMalloc &allocator);
  int size_without_lock() const;
  void set_deleted() { is_deleted_ = true; }
  bool is_deleted() const { return is_deleted_; }
  void print() const;
  void print_without_lock() const;
  int get_lock_op_iter(
      const ObLockID &lock_id,
      ObLockOpIterator &iter) const;
  const ObLockID &get_lock_id() const { return lock_id_; }
  void set_lock_id(const ObLockID &lock_id) { lock_id_ = lock_id; }
  bool contain(const ObLockID &lock_id) { return lock_id_ == lock_id; }
  TO_STRING_KV(K_(lock_id), K_(is_deleted), K_(row_share), K_(row_exclusive), K_(max_split_epoch));
  int table_lock_split(const ObTabletID &src_tablet_id,
                       const ObSArray<common::ObTabletID> &dst_tablet_ids,
                       const transaction::ObTransID &trans_id,
                       ObLockTableSplitLogCb &callback);
  bool has_splitted() const { return max_split_epoch_.is_max(); }
  int set_split_epoch(const share::SCN &scn, const bool for_replay = false);
  int get_split_epoch(share::SCN &scn);
  int reset_split_epoch();
private:
  void print_() const;
  void reset_(ObMalloc &allocator);
  int eliminate_conflict_caused_by_split_if_need_(
    const ObTableLockOp &lock_op,
    storage::ObStoreCtx &ctx);
  int check_allow_lock_(
      const ObTableLockOp &lock_op,
      const uint64_t lock_mode_cnt_in_same_trans[],
      ObTxIDSet &conflict_tx_set,
      bool &conflict_with_dml_lock,
      const bool include_finish_tx = true,
      const bool only_check_dml_lock = false,
      const bool check_for_replace = false);
  int update_lock_status_(
      const ObTableLockOp &lock_op,
      const share::SCN &commit_version,
      const share::SCN &commit_scn,
      const ObTableLockOpStatus status,
      ObTableLockOpList *op_list);
  void wakeup_waiters_(const ObTableLockOp &lock_op);
  int recover_(
      const ObTableLockOp &lock_op,
      ObMalloc &allocator);
  int fast_lock(
      const ObLockParam &param,
      const ObTableLockOp &lock_op,
      const uint64_t lock_mode_cnt_in_same_trans[],
      ObMalloc &allocator,
      ObTxIDSet &conflict_tx_set);
  int slow_lock(
      const ObLockParam &param,
      const ObTableLockOp &lock_op,
      const uint64_t lock_mode_cnt_in_same_trans[],
      ObMalloc &allocator,
      ObTxIDSet &conflict_tx_set);
  int try_fast_lock_(
      const ObTableLockOp &lock_op,
      const uint64_t lock_mode_cnt_in_same_trans[],
      ObTxIDSet &conflict_tx_set);
  int unlock_(
      const ObTableLockOp &unlock_op,
      ObMalloc &allocator);
  int check_allow_unlock_(const ObTableLockOp &unlock_op);
  int check_op_allow_lock_(const ObTableLockOp &lock_op);
  int get_exist_lock_mode_without_curr_trans(const uint64_t lock_mode_cnt_in_same_trans[],
                                             ObTableLockMode &lock_mode_without_curr_trans,
                                             const ObTransID &trans_id,
                                             const bool is_for_replace = false);
  void check_need_recover_(
      const ObTableLockOp &lock_op,
      const ObTableLockOpList *op_list,
      bool &need_recover);
  int get_or_create_op_list(
      const ObTableLockMode mode,
      const uint64_t tenant_id,
      ObMalloc &allocator,
      ObTableLockOpList *&op_list);
  int get_op_list(
      const ObTableLockMode mode,
      ObTableLockOpList *&op_list);
  void drop_op_list_if_empty_(
      const ObTableLockMode mode,
      ObTableLockOpList *&op_list,
      ObMalloc &allocator);
  void delete_lock_op_from_list_(
      const ObTableLockOp &lock_op,
      ObTableLockOpList *&op_list,
      ObMalloc &allocator);
  void free_op_list(ObTableLockOpList *op_list, ObMalloc &allocator);
  void print_op_list(const ObTableLockOpList *op_list) const;
  int get_lock_op_list_iter_(
      const ObTableLockOpList *op_list,
      ObLockOpIterator &iter) const;
  int get_tx_id_set_(
      const ObTransID &myself_tx,
      const int64_t lock_modes,
      const bool include_finish_tx,
      ObTxIDSet &tx_id_set);
  int get_tx_id_set_(
      const ObTransID &myself_tx,
      const ObTableLockOpList *op_list,
      const bool include_finish_tx,
      ObTxIDSet &tx_id_set);
  int get_first_complete_unlock_op_(
      const ObTableLockOpList *op_list,
      ObTableLockOp &unlock_op) const;
  int compact_tablelock_(
      const ObTableLockOp &unlock_op,
      ObTableLockOpList *&op_list,
      ObMalloc &allocator,
      bool &is_compact,
      const bool is_force = false);
  int compact_tablelock_(
      ObTableLockOpList *&op_list,
      ObMalloc &allocator,
      bool &is_compact,
      const bool is_force = false);
  int compact_tablelock_(
      ObMalloc &allocator,
      bool &is_compact,
      const bool is_force = false);
  int register_into_deadlock_detector_(
      const storage::ObStoreCtx &ctx,
      const ObTableLockOp &lock_op);
  int unregister_from_deadlock_detector_(const ObTableLockOp &lock_op);
  // Find out if there're table locks that are not generated by dml,
  // and return true if there're.
  // RS and RX table locks for OUT_TRANS may exist in the source tablet,
  // but to avoid the complexity of cascading unlocks, copying these
  // locks to the new tablet is not considered for now.
  // And other table locks (except for table locks which are generated by
  // dml, and RS/RX table locks for OUR_TRANS) is illegal for tablet split,
  // so we need to check it by this function.
  bool exist_cannot_split_lock_(const ObTransID &split_start_trans_id) const;
  bool need_split_() const;
  int submit_log_(ObLockTableSplitLogCb &callback,
                  common::ObTabletID src_tablet_id,
                  const ObSArray<common::ObTabletID> &dst_tablet_ids);
  int set_split_epoch_(const share::SCN &scn, const bool for_replay = false);
  int get_split_epoch_(share::SCN &scn);
  int reset_split_epoch_();
  void check_curr_trans_lock_is_valid_(const uint64_t lock_mode_cnt_in_same_trans[], const int64_t *lock_mode_cnt);

private:
  int check_op_allow_lock_from_list_(
      const ObTableLockOp &lock_op,
      const ObTableLockOpList *op_list);
  int check_op_allow_unlock_from_list_(
      const ObTableLockOp &lock_op,
      const ObTableLockOpList *op_list);
  int check_allow_replace_from_list_(ObTableLockOpList *op_list, const ObTransID &trans_id, bool &allow_replace);
  void lock_row_share_()
  {
    ATOMIC_INC(&row_share_);
  }
  void unlock_row_share_()
  {
    ATOMIC_DEC(&row_share_);
  }
  void lock_row_exclusive_()
  {
    ATOMIC_INC(&row_exclusive_);
  }
  void unlock_row_exclusive_()
  {
    ATOMIC_DEC(&row_exclusive_);
  }
  int get_exist_lock_mode_cnt_without_curr_trans_(
      const ObTableLockMode &lock_mode,
      const uint64_t lock_mode_cnt_in_same_trans[]);
public:
  RWLock rwlock_;
  bool is_deleted_;
  // indicate dml table lock info from src_tablet when tablet split,
  // and persistence by a special table lock op
  // case 1: !max_split_epoch_.is_valid(), means that there's no any splitting on the obj_lock;
  // case 2: max_split_epoch_.is_min(), means that this obj_lock (or tablet) needs split as
  // a dst_tablet, but the callback hasn't been executed;
  // case 3: max_split_epoch_.is_max(), means that this obj_lock (or tablet) has splitted or is
  // splitting as a src_tablet;
  // case 4: max_split_epoch_.is_valid() && !max_split_epoch_.is_min() && !max_split_epoch_.is_max(),
  // means that callback has been executed, but the splitting is not completed.
  share::SCN max_split_epoch_;
private:
  static constexpr const int64_t OB_LOCK_OP_MAP_VERSION = 1;
  ObTableLockOpList *map_[TABLE_LOCK_MODE_COUNT];
  int64_t row_share_;
  int64_t row_exclusive_;
  ObLockID lock_id_;
};

class ObOBJLockFactory
{
public:
  static ObOBJLock *alloc(const uint64_t tenant_id, const ObLockID &lock_id);
  static void release(ObOBJLock *e);
  static int64_t alloc_count_;
  static int64_t release_count_;
};

class ObOBJLockAlloc
{
public:
  static ObOBJLock* alloc_value()
  {
    // do not allow alloc val in hashmap
    return NULL;
  }
  static void free_value(ObOBJLock* p)
  {
    if (NULL != p) {
      ObOBJLockFactory::release(p);
      p = NULL;
    }
  }
};

class ObOBJLockMap
{
  typedef share::ObLightHashMap<ObLockID, ObOBJLock, ObOBJLockAlloc, common::SpinRWLock, 1 << 10> Map;
public:
  ObOBJLockMap() :
      lock_map_(),
      allocator_("ObOBJLockMap"),
      is_inited_(false)
  {}
  int init();
  void reset();
  int lock(
      const ObLockParam &param,
      storage::ObStoreCtx &ctx,
      const ObTableLockOp &lock_op,
      const uint64_t lock_mode_cnt_in_same_trans[],
      ObTxIDSet &conflict_tx_set);
  int unlock(
      const ObTableLockOp &lock_op,
      const bool is_try_lock,
      const int64_t expired_time);
  int remove_lock(const ObLockID &lock_id);
  void remove_lock_record(const ObTableLockOp &lock_op);
  int recover_obj_lock(
      const ObTableLockOp &lock_op);
  int update_lock_status(
      const ObTableLockOp &lock_op,
      const share::SCN commit_version,
      const share::SCN commit_scn,
      const ObTableLockOpStatus status);
  bool is_inited() const { return is_inited_; }
  int check_allow_lock(
      const ObTableLockOp &lock_op,
      const uint64_t lock_mode_cnt_in_same_trans[],
      ObTxIDSet &conflict_tx_set,
      const int64_t expired_time,
      const bool include_finish_tx = true,
      const bool only_check_dml_lock = false);
  void print();
  share::SCN get_min_ddl_committed_scn(share::SCN &flushed_scn);
  int get_table_lock_store_info(ObIArray<ObTableLockOp> &store_arr, share::SCN freeze_scn);
  // get all the lock id in the lock map
  // @param[out] iter, the iterator returned.
  // int get_lock_id_iter(ObLockIDIterator &iter);
  int get_lock_id_iter(ObLockIDIterator &iter);

  // get the lock op iterator of a obj lock
  // @param[in] lock_id, which obj lock's lock op will be iterated.
  // @param[out] iter, the iterator returned.
  int get_lock_op_iter(const ObLockID &lock_id,
                       ObLockOpIterator &iter);
  // check all obj locks in the lock map, and clear it if it's empty.
  int check_and_clear_obj_lock(const bool force_compact);

  int table_lock_split(const ObTabletID &src_tablet_id,
                       const ObSArray<common::ObTabletID> &dst_tablet_ids,
                       const transaction::ObTransID &trans_id,
                       ObLockTableSplitLogCb &callback);
  int add_split_epoch(const ObLockID &lock_id,
                      const share::SCN &split_scn,
                      const bool for_replay = false);
  int add_split_epoch(const ObSArray<ObLockID> &lock_ids,
                      const share::SCN &split_epoch,
                      const bool for_replay = false);
  int get_split_epoch(const ObLockID &lock_id,
                      share::SCN &split_scn);
  int reset_split_epoch(const ObLockID &lock_id);
private:
  class LockIDIterFunctor
  {
  public:
    explicit LockIDIterFunctor(ObLockIDIterator &iter)
      : err_code_(OB_SUCCESS),
        iter_(iter)
    {}
    bool operator()(ObOBJLock *obj_lock)
    {
      int ret = OB_SUCCESS;
      UNUSED(obj_lock);
      bool need_continue = true;
      if (OB_FAIL(iter_.push(obj_lock->get_lock_id()))) {
        TABLELOCK_LOG(WARN, "push lock id into iterator failed", "lock_id", obj_lock->get_lock_id(), K(ret));
        need_continue = false;
        err_code_ = ret;
      }
      return need_continue;
    }
    int get_ret_code() const { return err_code_; }
  private:
    int err_code_;
    ObLockIDIterator &iter_;
  };
  class PrintLockFunctor
  {
  public:
    bool operator()(ObOBJLock *obj_lock)
    {
      bool bool_ret = true;
      TABLELOCK_LOG(INFO, "LockID: ", "lock_id", obj_lock->get_lock_id());
      obj_lock->print();
      return bool_ret;
    }
  };
  class ResetLockFunctor
  {
  public:
    explicit ResetLockFunctor(ObMalloc &allocator) : allocator_(allocator)
    {}
    bool operator()(ObOBJLock *obj_lock)
    {
      bool bool_ret = true;
      obj_lock->reset(allocator_);
      return bool_ret;
    }
  private:
    ObMalloc &allocator_;
  };
  class GetMinCommittedDDLLogtsFunctor
  {
  public:
    explicit GetMinCommittedDDLLogtsFunctor(share::SCN &flushed_scn)
      : min_committed_scn_(share::SCN::max_scn()),
        flushed_scn_(flushed_scn) {}
    bool operator()(ObOBJLock *obj_lock);
    share::SCN get_min_committed_scn() { return min_committed_scn_; }

  private:
    share::SCN min_committed_scn_;
    share::SCN flushed_scn_;
  };
  class GetTableLockStoreInfoFunctor
  {
  public:
    explicit GetTableLockStoreInfoFunctor(ObIArray<ObTableLockOp> &store_arr,
                                          share::SCN freeze_scn)
      : store_arr_(store_arr),
        freeze_scn_(freeze_scn) {}
    bool operator()(ObOBJLock *obj_lock);

  private:
    ObIArray<ObTableLockOp> &store_arr_;
    share::SCN freeze_scn_;
  };

private:
  int get_or_create_obj_lock_with_ref_(
      const ObLockID &lock_id,
      ObOBJLock *&obj_lock);
  int get_obj_lock_with_ref_(
      const ObLockID &lock_id,
      ObOBJLock *&obj_lock);
  void drop_obj_lock_if_empty_(
      const ObLockID &lock_id,
      ObOBJLock *obj_lock);
private:
  Map lock_map_;
  ObMalloc allocator_;
  bool is_inited_;
};

} // tablelock
} // transaction
} // oceanbase

#endif /* OCEANBASE_STORAGE_TABLELOCK_OB_OBJ_LOCK_ */
