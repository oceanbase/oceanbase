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

#ifndef OCEANBASE_STORAGE_OB_TX_DATA_MEMTABLE_MGR
#define OCEANBASE_STORAGE_OB_TX_DATA_MEMTABLE_MGR

#include "storage/ob_i_memtable_mgr.h"
#include "storage/tx_table/ob_tx_data_memtable.h"
#include "storage/checkpoint/ob_common_checkpoint.h"

// ObTxDataMemtableMgr manages all tx data memtables.
// It provides all operations related to tx data memtable.
namespace oceanbase
{
namespace storage
{
class TxDataMemtableMgrFreezeGuard;

class ObTxDataMemtableWriteGuard
{
public:
  ObTxDataMemtableWriteGuard() : size_(0)
  {
  }
  ~ObTxDataMemtableWriteGuard() { reset(); }

  int push_back_table(memtable::ObIMemtable *i_memtable, ObTenantMetaMemMgr *t3m, const ObITable::TableType table_type)
  {
    int ret = OB_SUCCESS;
    ObTxDataMemtable *tx_data_memtable = nullptr;
    if (OB_FAIL(handles_[size_].set_table(static_cast<ObITable *const>(i_memtable), t3m, table_type))) {
      STORAGE_LOG(WARN, "set i memtable to handle failed", KR(ret), KP(i_memtable), KP(t3m), K(table_type));
    } else if (OB_FAIL(handles_[size_].get_tx_data_memtable(tx_data_memtable))) {
      STORAGE_LOG(ERROR, "get tx data memtable from memtable handle failed", KR(ret), K(handles_[size_]));
    } else if (OB_ISNULL(tx_data_memtable)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "tx data memtable is unexpected nullptr", K(ret), KPC(tx_data_memtable));
    } else {
      tx_data_memtable->inc_write_ref();
      size_++;
    }
    return ret;
  }

  void reset()
  {
    for (int i = 0; i < MAX_TX_DATA_MEMTABLE_CNT; i++) {
      if (handles_[i].is_valid()) {
        ObTxDataMemtable *tx_data_memtable = nullptr;
        handles_[i].get_tx_data_memtable(tx_data_memtable);
        tx_data_memtable->dec_write_ref();
      }
      handles_[i].reset();
    }
    size_ = 0;
  }

  TO_STRING_KV(K(size_), K(handles_[0]), K(handles_[1]));

public:
  int64_t size_;
  ObTableHandleV2 handles_[MAX_TX_DATA_MEMTABLE_CNT];
};

class ObTxDataMemtableMgr : public ObIMemtableMgr, public checkpoint::ObCommonCheckpoint
{
friend TxDataMemtableMgrFreezeGuard;
using SliceAllocator = ObSliceAlloc;

private:
  static const int TX_DATA_MEMTABLE_MAX_NUM = 64;
  static const int TX_DATA_MEMTABLE_NUM_MOD_MASK = TX_DATA_MEMTABLE_MAX_NUM - 1;
  static const int64_t TX_DATA_MEMTABLE_MAX_FREEZE_WAIT_TIME = 1000; // 1ms

public:  // ObTxDataMemtableMgr
  ObTxDataMemtableMgr()
    : ObIMemtableMgr(LockType::OB_SPIN_RWLOCK, &lock_def_),
      is_freezing_(false),
      ls_id_(0),
      mini_merge_recycle_commit_versions_ts_(0),
      tx_data_table_(nullptr),
      ls_tablet_svr_(nullptr),
      slice_allocator_(nullptr) {}
  virtual ~ObTxDataMemtableMgr() = default;
  int init(const common::ObTabletID &tablet_id,
           const share::ObLSID &ls_id,
           ObFreezer *freezer,
           ObTenantMetaMemMgr *t3m) override;
  virtual void destroy() override;

  int offline();


  /**
   * @brief This function do the following operations:
   * 1. check some parameters which is required by freeze;
   * 2. lock the tx data memtable list in write mode;
   * 3. check the count of tx data memtables and the state of active memtable;
   * 4. create a new memtable;
   * 5. wait read and write operations on freezing tx data memtable done(using a while loop);
   * 6. set some variables of freezing.
   */
  int freeze();
  /**
   * @brief Using to create a new active tx data memtable
   *
   * @param[in] clog_checkpoint_ts clog_checkpoint_ts, using to init multiversion_start,
   * base_version and start_scn. The start_scn will be modified if this function is called by
   * freeze().
   * @param[in] schema_version  schema_version, not used
   */
  virtual int create_memtable(const share::SCN clog_checkpoint_scn,
                              const int64_t schema_version,
                              const share::SCN newest_clog_checkpoint_scn,
                              const bool for_replay=false) override;
  /**
   * @brief Get the last tx data memtable in memtable list.
   *
   * @param[out] handle the memtable handle which contains the active memtable
   */
  virtual int get_active_memtable(ObTableHandleV2 &handle) const override;
  /**
   * @brief Get all tx data memtable handles
   *
   * @param[out] handles the memtable handles of all tx data memtables
   */
  virtual int get_all_memtables(ObTableHdlArray &handles) override;

  int get_all_memtables_with_range(ObTableHdlArray &handles,
                                   int64_t &memtable_head,
                                   int64_t &memtable_tail);
  int get_all_memtables_for_write(ObTxDataMemtableWriteGuard &write_guard);

  int get_memtable_range(int64_t &memtable_head, int64_t &memtable_tail);

  // ================ INHERITED FROM ObCommonCheckpoint ===============
  virtual share::SCN get_rec_scn() override;

  virtual int flush(share::SCN recycle_scn, bool need_freeze = true) override;

  virtual ObTabletID get_tablet_id() const override;

  virtual bool is_flushing() const override;

  INHERIT_TO_STRING_KV("ObIMemtableMgr",
                       ObIMemtableMgr,
                       K_(is_freezing),
                       K_(ls_id),
                       K_(mini_merge_recycle_commit_versions_ts),
                       KP_(tx_data_table),
                       KP_(ls_tablet_svr),
                       KP_(slice_allocator));

public: // getter and setter
  ObLSTabletService *get_ls_tablet_svr() { return ls_tablet_svr_; }
  ObTxDataTable *get_tx_data_table() { return tx_data_table_; }
  int64_t get_mini_merge_recycle_commit_versions_ts() { return mini_merge_recycle_commit_versions_ts_; }

  void set_slice_allocator(SliceAllocator *slice_allocator) { slice_allocator_ = slice_allocator; }

protected:
  virtual int release_head_memtable_(memtable::ObIMemtable *imemtable,
                                     const bool force);

private:  // ObTxDataMemtableMgr
  int create_memtable_(const share::SCN clog_checkpoint_scn,
                       const int64_t schema_version,
                       const int64_t buckets_cnt);

  int freeze_();
  int calc_new_memtable_buckets_cnt_(const double load_factory,
                                     const int64_t old_buckests_cnt,
                                     int64_t &new_buckest_cnt);

  int get_all_memtables_(ObTableHdlArray &handles);

  int flush_all_frozen_memtables_(ObTableHdlArray &memtable_handles);

  ObTxDataMemtable *get_tx_data_memtable_(const int64_t pos) const;

private:  // ObTxDataMemtableMgr
  bool is_freezing_;
  share::ObLSID ls_id_;
  int64_t mini_merge_recycle_commit_versions_ts_;
  ObTxDataTable *tx_data_table_;
  ObLSTabletService *ls_tablet_svr_;
  SliceAllocator *slice_allocator_;
  common::SpinRWLock lock_def_;
};

class TxDataMemtableMgrFreezeGuard
{
public:
  TxDataMemtableMgrFreezeGuard() : can_freeze_(false), memtable_mgr_(nullptr) {}
  ~TxDataMemtableMgrFreezeGuard() { reset(); }

  int init(ObTxDataMemtableMgr *memtable_mgr)
  {
    int ret = OB_SUCCESS;
    reset();
    if (OB_ISNULL(memtable_mgr)) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid tx data table", KR(ret));
    } else {
      can_freeze_ = (false == ATOMIC_CAS(&(memtable_mgr->is_freezing_), false, true));
      if (can_freeze_) {
        memtable_mgr_ = memtable_mgr;
      }
    }
    return ret;
  }

  void reset()
  {
    can_freeze_ = false;
    if (OB_NOT_NULL(memtable_mgr_)) {
      ATOMIC_STORE(&(memtable_mgr_->is_freezing_), false);
      memtable_mgr_ = nullptr;
    }
  }

  bool can_freeze() { return can_freeze_; }

public:
  bool can_freeze_;
  ObTxDataMemtableMgr *memtable_mgr_;
};

}  // namespace storage

}  // namespace oceanbase

#endif
