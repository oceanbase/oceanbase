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

#include "storage/tx_table/ob_tx_data_memtable_mgr.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{
using namespace share;
using namespace palf;
namespace storage
{
class ObILS;
namespace checkpoint
{
  class ObCheckpointExecutor;
}

ObTxDataMemtableMgr::ObTxDataMemtableMgr()
  : is_freezing_(false),
    ls_id_(),
    mini_merge_recycle_commit_versions_ts_(0),
    tx_data_table_(nullptr),
    ls_tablet_svr_(nullptr)
{
  lock_.lock_type_ = LockType::OB_SPIN_RWLOCK;
  lock_.lock_ = &lock_def_;
}

ObTxDataMemtableMgr::~ObTxDataMemtableMgr()
{
  destroy();
}

void ObTxDataMemtableMgr::destroy()
{
  int ret = OB_SUCCESS;
  const int64_t ref_cnt = get_ref();
  if (OB_UNLIKELY(0 != ref_cnt)) {
    STORAGE_LOG(ERROR, "ref cnt is NOT 0", K(ret), K(ref_cnt), K_(ls_id), KPC(this));
  }

  MemMgrWLockGuard guard(lock_);
  reset_tables();
  ls_id_.reset();
  tablet_id_.reset();
  tx_data_table_ = nullptr;
  ls_tablet_svr_ = nullptr;
  freezer_ = nullptr;
  mini_merge_recycle_commit_versions_ts_ = 0;
  is_inited_ = false;
}

int ObTxDataMemtableMgr::init(const common::ObTabletID &tablet_id,
                              const ObLSID &ls_id,
                              ObFreezer *freezer,
                              ObTenantMetaMemMgr *t3m)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObTxTable *tx_table = nullptr;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObTxDataMemtableMgr has been initialized.", KR(ret));
  } else if (OB_UNLIKELY(!tablet_id.is_valid()) || OB_ISNULL(freezer) || OB_ISNULL(t3m)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(tablet_id), KP(freezer), KP(t3m));
  } else if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::TRANS_MOD))){
    STORAGE_LOG(WARN, "Get ls from ls service failed.", KR(ret));
  } else if (OB_ISNULL(tx_table = ls_handle.get_ls()->get_tx_table())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Get tx table from ls failed.", KR(ret));
  } else {
    reset_tables();
    ls_id_ = ls_id;
    tablet_id_ = tablet_id;
    t3m_ = t3m;
    freezer_ = freezer;
    tx_data_table_ = tx_table->get_tx_data_table();
    ls_tablet_svr_ = ls_handle.get_ls()->get_tablet_svr();
    mini_merge_recycle_commit_versions_ts_ = 0;
    ObLSTxService *ls_tx_svr = nullptr;
    if (OB_ISNULL(ls_tx_svr = freezer_->get_ls_tx_svr())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "ls_tx_svr is null", K(ret), KP(freezer_));
    } else if (OB_FAIL(ls_tx_svr->register_common_checkpoint(
                      checkpoint::TX_DATA_MEMTABLE_TYPE, this))) {
      STORAGE_LOG(WARN, "tx_data register_common_checkpoint failed", K(ret), K(ls_id));
    } else if (OB_ISNULL(tx_data_table_) || OB_ISNULL(ls_tablet_svr_)) {
      ret = OB_ERR_NULL_VALUE;
      STORAGE_LOG(WARN, "Init tx data memtable mgr failed.", KR(ret));
    } else {
      is_inited_ = true;
    }
  }

  if (IS_NOT_INIT) {
    destroy();
  }
  return ret;
}

int ObTxDataMemtableMgr::offline()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(release_memtables())) {
    STORAGE_LOG(WARN, "release tx data memtables failed", KR(ret));
  } else {
    mini_merge_recycle_commit_versions_ts_ = 0;
    memtable_head_ = 0;
    memtable_tail_ = 0;
  }
  return ret;
}

int ObTxDataMemtableMgr::release_head_memtable_(ObIMemtable *imemtable,
                                                const bool force)
{
  int ret = OB_SUCCESS;
  ObTxDataMemtable *memtable = static_cast<ObTxDataMemtable *>(imemtable);
  STORAGE_LOG(INFO, "tx data memtable mgr release head memtable", K(get_memtable_count_()),
              KPC(memtable));

  if (OB_LIKELY(get_memtable_count_() > 0)) {
    const int64_t idx = get_memtable_idx(memtable_head_);
    if (nullptr != tables_[idx] && memtable == tables_[idx]) {
      memtable->set_state(ObTxDataMemtable::State::RELEASED);
      memtable->set_release_time();
      if (true == memtable->do_recycled()) {
        mini_merge_recycle_commit_versions_ts_ = ObClockGenerator::getClock();
      }
      STORAGE_LOG(INFO, "[TX DATA MERGE]tx data memtable mgr release head memtable", K(ls_id_), KP(memtable), KPC(memtable));
      release_head_memtable();
    } else {
      ret = OB_INVALID_ARGUMENT;
      ObTxDataMemtable *head_memtable = static_cast<ObTxDataMemtable *>(tables_[get_memtable_idx(idx)]);
      STORAGE_LOG(WARN, "trying to release an invalid tx data memtable.", KR(ret), K(idx),
                  K(memtable), KP(memtable), KPC(head_memtable));
    }
  }
  return ret;
}

int ObTxDataMemtableMgr::create_memtable(const CreateMemtableArg &arg)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTxDataMemtableMgr has not initialized", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(arg.schema_version_ < 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(arg.schema_version_));
  } else {
    MemMgrWLockGuard lock_guard(lock_);
    if (OB_FAIL(
            create_memtable_(arg.clog_checkpoint_scn_, arg.schema_version_, ObTxDataHashMap::DEFAULT_BUCKETS_CNT))) {
      STORAGE_LOG(WARN, "create memtable fail.", KR(ret));
    } else {
      // create memtable success
    }
  }
  return ret;
}

int ObTxDataMemtableMgr::create_memtable_(const SCN clog_checkpoint_scn,
                                          int64_t schema_version,
                                          const int64_t buckets_cnt)
{
  UNUSED(schema_version);
  int ret = OB_SUCCESS;
  ObTableHandleV2 handle;
  ObITable::TableKey table_key;
  table_key.table_type_ = ObITable::TX_DATA_MEMTABLE;
  table_key.tablet_id_ = ObTabletID(ObTabletID::LS_TX_DATA_TABLET_ID);
  table_key.scn_range_.start_scn_ = clog_checkpoint_scn; //TODO(SCN) clog_checkpoint_ts should be SCN
  table_key.scn_range_.end_scn_.set_max();
  ObITable *table = nullptr;
  ObTxDataMemtable *tx_data_memtable = nullptr;

  if (OB_FAIL(t3m_->acquire_tx_data_memtable(handle))) {
    STORAGE_LOG(WARN, "failed to create memtable", KR(ret), KP(t3m_));
  } else if (OB_ISNULL(table = handle.get_table())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "table is nullptr", KR(ret), K(handle));
  } else if (FALSE_IT(tx_data_memtable = dynamic_cast<ObTxDataMemtable *>(table))) {
  } else if (OB_ISNULL(tx_data_memtable)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "dynamic cast failed", KR(ret), KPC(this));
  } else if (OB_FAIL(tx_data_memtable->init(table_key, this, freezer_, buckets_cnt))) {
    STORAGE_LOG(WARN, "memtable init fail.", KR(ret), KPC(tx_data_memtable));
  } else if (OB_FAIL(add_memtable_(handle))) {
    STORAGE_LOG(WARN, "add memtable fail.", KR(ret));
  } else {
    // create memtable success
    STORAGE_LOG(INFO, "create tx data memtable done", KR(ret), KPC(tx_data_memtable), KPC(this));
  }
  return ret;
}

int ObTxDataMemtableMgr::freeze()
{
  int ret = OB_SUCCESS;
  STORAGE_LOG(INFO, "start freeze tx data memtable", K(ls_id_));

  if (IS_NOT_INIT) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "tx data memtable container is not inited.", KR(ret));
  } else {
    MemMgrWLockGuard lock_guard(lock_);
    if (get_memtable_count_() <= 0) {
      ret = OB_ENTRY_NOT_EXIST;
      STORAGE_LOG(WARN, "Empty tx data memtable mgr. This ls may offline", KR(ret), K(memtable_head_), K(memtable_tail_));
    } else if (OB_FAIL(freeze_())) {
      STORAGE_LOG(WARN, "freeze tx data memtable fail.", KR(ret));
    } else {
      // freeze success
    }
  }
  return ret;
}

int ObTxDataMemtableMgr::freeze_()
{
  int ret = OB_SUCCESS;

  ObTxDataMemtable *freeze_memtable = static_cast<ObTxDataMemtable *>(tables_[get_memtable_idx(memtable_tail_ - 1)]);
  int64_t pre_memtable_tail = memtable_tail_;
  SCN clog_checkpoint_scn = SCN::base_scn();
  int64_t schema_version = 1;
  int64_t new_buckets_cnt = ObTxDataHashMap::DEFAULT_BUCKETS_CNT;

  // FIXME : @gengli remove this condition after upper_trans_version is not needed
  if (get_memtable_count_() >= MAX_TX_DATA_MEMTABLE_CNT) {
    ret = OB_EAGAIN;
    STORAGE_LOG(INFO, "There is a freezed memetable existed. Try freeze after flushing it.", KR(ret), K(get_memtable_count_()));
  } else if (get_memtable_count_() >= MAX_MEMSTORE_CNT) {
    ret = OB_SIZE_OVERFLOW;
    STORAGE_LOG(WARN, "tx data memtable size is overflow.", KR(ret), K(get_memtable_count_()));
  } else if (OB_ISNULL(freeze_memtable)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "freeze memtable is nullptr", KR(ret), KP(freeze_memtable));
  } else if (ObTxDataMemtable::State::ACTIVE != freeze_memtable->get_state()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "trying to freeze an inactive tx data memtable.", KR(ret),
                KPC(freeze_memtable));
  } else if (0 == freeze_memtable->get_tx_data_count()) {
    ret = OB_STATE_NOT_MATCH;
    STORAGE_LOG(WARN, "tx data memtable is empty. do not need freeze.", KR(ret), KPC(freeze_memtable));
  } else if (OB_FAIL(calc_new_memtable_buckets_cnt_(
                 freeze_memtable->load_factory(), freeze_memtable->get_buckets_cnt(), new_buckets_cnt))) {
    STORAGE_LOG(WARN,
                "calculate new memtable buckets cnt failed",
                KR(ret),
                "load_factory", freeze_memtable->load_factory(),
                "old_buckets_cnt", freeze_memtable->get_buckets_cnt(),
                K(new_buckets_cnt));
  } else if (OB_FAIL(create_memtable_(clog_checkpoint_scn, schema_version, new_buckets_cnt))) {
    STORAGE_LOG(WARN,
                "create memtable fail.",
                KR(ret),
                K(clog_checkpoint_scn),
                K(schema_version),
                "old_buckets_cnt", freeze_memtable->get_buckets_cnt(),
                K(new_buckets_cnt));
  } else {
    ObTxDataMemtable *new_memtable = static_cast<ObTxDataMemtable *>(tables_[get_memtable_idx(memtable_tail_ - 1)]);
    if (OB_ISNULL(new_memtable) && OB_UNLIKELY(new_memtable->is_tx_data_memtable())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "get tx data memtable from handle fail.", KR(ret), KPC(new_memtable));
    } else {
      int64_t start_ts = ObTimeUtil::fast_current_time();
      while (freeze_memtable->get_write_ref() > 0) {
        // waiting for all write operation done.
        if (TC_REACH_TIME_INTERVAL(TX_DATA_MEMTABLE_MAX_FREEZE_WAIT_TIME)) {
          int64_t freeze_wait_time_ms = (ObTimeUtil::fast_current_time() - start_ts) / 1000;
          STORAGE_LOG_RET(WARN, OB_ERR_TOO_MUCH_TIME, "freeze tx data memtable cost too much time. has wait for(ms) : ",
                      K(freeze_wait_time_ms), KPC(freeze_memtable));
        }
        PAUSE();
      }
      freeze_memtable->set_end_scn();

      freeze_memtable->set_state(ObTxDataMemtable::State::FREEZING);
      freeze_memtable->set_freeze_time();
      new_memtable->set_start_scn(freeze_memtable->get_end_scn());
      new_memtable->set_state(ObTxDataMemtable::State::ACTIVE);

      STORAGE_LOG(INFO,
                  "[TX DATA MERGE]tx data memtable freeze success",
                  KR(ret),
                  K(ls_id_),
                  KP(freeze_memtable),
                  K(get_memtable_count_()),
                  KPC(freeze_memtable),
                  KPC(new_memtable));
    }
  }

  if (OB_FAIL(ret)) {
    if (memtable_tail_ != pre_memtable_tail) {
      STORAGE_LOG(ERROR, "unexpected error happened.", KR(ret), K(pre_memtable_tail),
                  K(memtable_tail_), KPC(freeze_memtable));
      memtable_tail_ = pre_memtable_tail;
    }
  }

  return ret;
}

int ObTxDataMemtableMgr::calc_new_memtable_buckets_cnt_(const double load_factory,
                                                        const int64_t old_buckets_cnt,
                                                        int64_t &new_buckets_cnt)
{
  // acquire the max memory which tx data memtable buckets can use
  int64_t remain_memory = lib::get_tenant_memory_remain(MTL_ID());
  int64_t buckets_size_limit = remain_memory >> 4; /* remain_memory * (1/16) */

  int64_t expect_buckets_cnt = old_buckets_cnt;
  if (load_factory > ObTxDataHashMap::LOAD_FACTORY_MAX_LIMIT &&
      expect_buckets_cnt < ObTxDataHashMap::MAX_BUCKETS_CNT) {
    expect_buckets_cnt <<= 1;
  } else if (load_factory < ObTxDataHashMap::LOAD_FACTORY_MIN_LIMIT &&
             expect_buckets_cnt > ObTxDataHashMap::MIN_BUCKETS_CNT) {
    expect_buckets_cnt >>= 1;
  }

  int64_t expect_buckets_size = expect_buckets_cnt * sizeof(ObTxDataHashMap::ObTxDataHashHeader);

  while (expect_buckets_size > buckets_size_limit && expect_buckets_cnt > ObTxDataHashMap::MIN_BUCKETS_CNT) {
    expect_buckets_cnt >>= 1;
    expect_buckets_size = expect_buckets_cnt * sizeof(ObTxDataHashMap::ObTxDataHashHeader);
  }

  new_buckets_cnt = expect_buckets_cnt;
  STORAGE_LOG(INFO,
              "finish calculate new tx data memtable buckets cnt",
              K(ls_id_),
              K(load_factory),
              K(old_buckets_cnt),
              K(new_buckets_cnt),
              K(remain_memory));
  return OB_SUCCESS;
}

int ObTxDataMemtableMgr::get_active_memtable(ObTableHandleV2 &handle) const
{
  int ret = OB_SUCCESS;
  MemMgrRLockGuard lock_guard(lock_);
  if (0 == memtable_tail_) {
    ret = OB_EAGAIN;
    STORAGE_LOG(INFO, "tx data memtable is not created yet. try agagin.", K(ret), K(ls_id_), K(memtable_tail_));
  } else if (0 == get_memtable_count_()) {
    ret = OB_ENTRY_NOT_EXIST;
    STORAGE_LOG(WARN, "the tx data memtable manager is empty. may be offline", KR(ret), K(get_memtable_count_()));
  } else if (OB_FAIL(get_ith_memtable(memtable_tail_ - 1, handle))) {
    STORAGE_LOG(WARN, "fail to get ith memtable", K(ret), K(memtable_tail_));
  } else {
    ObTxDataMemtable *tx_data_memtable = nullptr;
    if (OB_FAIL(handle.get_tx_data_memtable(tx_data_memtable))) {
      STORAGE_LOG(ERROR, "get tx data memtable from handle failed.", KR(ret), K(handle));
    } else if (ObTxDataMemtable::State::ACTIVE != tx_data_memtable->get_state()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "the last tx data memtable in manager is not an active memtable", KR(ret), KPC(tx_data_memtable));
    }
  }
  return ret;
}

int ObTxDataMemtableMgr::get_all_memtables_(ObTableHdlArray &handles)
{
  int ret = OB_SUCCESS;
  for (int64_t i = memtable_head_; OB_SUCC(ret) && i < memtable_tail_; ++i) {
    ObTableHandleV2 handle;
    if (OB_FAIL(get_ith_memtable(i, handle))) {
      STORAGE_LOG(WARN, "fail to get ith memtable", K(ret), K(i));
    } else if (OB_FAIL(handles.push_back(handle))) {
      STORAGE_LOG(WARN, "push back into handles failed.", K(ret));
    }
  }
  return ret;
}

int ObTxDataMemtableMgr::get_all_memtables(ObTableHdlArray &handles)
{
  int ret = OB_SUCCESS;
  MemMgrRLockGuard lock_guard(lock_);
  if (OB_FAIL(get_all_memtables_(handles))) {
    handles.reset();
    STORAGE_LOG(WARN, "get all memtables failed.", KR(ret));
  }
  return ret;
}

int ObTxDataMemtableMgr::get_all_memtables_with_range(ObTableHdlArray &handles, int64_t &memtable_head, int64_t &memtable_tail)
{
  int ret = OB_SUCCESS;
  MemMgrRLockGuard lock_guard(lock_);
  if (OB_FAIL(get_all_memtables_(handles))) {
    handles.reset();
    STORAGE_LOG(WARN, "get all memtables failed.", KR(ret));
  } else {
    memtable_head = memtable_head_;
    memtable_tail = memtable_tail_;
  }
  return ret;
}

int ObTxDataMemtableMgr::get_all_memtables_for_write(ObTxDataMemtableWriteGuard &write_guard)
{
  int ret = OB_SUCCESS;
  write_guard.reset();
  MemMgrRLockGuard lock_guard(lock_);
  for (int64_t i = memtable_head_; OB_SUCC(ret) && i < memtable_tail_; ++i) {
    if (OB_FAIL(write_guard.push_back_table(tables_[get_memtable_idx(i)], t3m_))) {
      STORAGE_LOG(WARN, "push back table to write guard failed", KR(ret), K(ls_id_));
    }
  }
  return ret;
}

SCN ObTxDataMemtableMgr::get_rec_scn()
{
  int ret = OB_SUCCESS;
  SCN rec_scn;
  rec_scn.set_max();

  ObTxDataMemtable *oldest_memtable = nullptr;
  ObSEArray<ObTableHandleV2, 2> memtable_handles;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_all_memtables(memtable_handles))) {
    STORAGE_LOG(WARN, "get all memtables failed", KR(ret), KP(this));
  } else if (memtable_handles.count() == 0) {
  } else {
    ObTableHandleV2 &oldest_memtable_handle = memtable_handles.at(0);
    if (OB_FAIL(oldest_memtable_handle.get_tx_data_memtable(oldest_memtable))) {
      STORAGE_LOG(WARN, "get tx data memtable from table handle fail.", KR(ret),
                  K(oldest_memtable_handle));
    } else {
      rec_scn = oldest_memtable->get_rec_scn();
    }
  }

  return rec_scn;
}

int ObTxDataMemtableMgr::flush_all_frozen_memtables_(ObTableHdlArray &memtable_handles,
    const int64_t trace_id)
{
  int ret = OB_SUCCESS;

  // flush all frozen memtable
  for (int i = 0; OB_SUCC(ret) && i < memtable_handles.count() - 1; i++) {
    ObTableHandleV2 &memtable_handle = memtable_handles.at(i);
    ObTxDataMemtable *memtable = nullptr;
    if (OB_FAIL(memtable_handle.get_tx_data_memtable(memtable))) {
      STORAGE_LOG(WARN, "get tx data memtable from table handle fail.", KR(ret), K(memtable));
    } else if (memtable->get_state() != ObTxDataMemtable::State::FROZEN
               && !memtable->ready_for_flush()) {
      // on need return error
      STORAGE_LOG(INFO, "the tx data memtable is not frozen", KPC(memtable));
    } else if (OB_FAIL(memtable->flush(trace_id))) {
      STORAGE_LOG(WARN, "the tx data memtable flush failed", KR(ret), KPC(memtable));
    }
  }
  return ret;
}

int ObTxDataMemtableMgr::flush(SCN recycle_scn, const int64_t trace_id, bool need_freeze)
{
  int ret = OB_SUCCESS;

  // do freeze if needed
  // recycle_scn == INT64_MAX && need_freeze == true means this flush is called by tx data table
  // self freeze task
  if (need_freeze) {
    TxDataMemtableMgrFreezeGuard freeze_guard;
    SCN rec_scn = get_rec_scn();
    if (rec_scn >= recycle_scn) {
      STORAGE_LOG(INFO, "no need freeze", K(recycle_scn), K(rec_scn));
    } else if (OB_FAIL(freeze_guard.init(this))) {
      STORAGE_LOG(WARN, "init tx data memtable mgr freeze guard failed", KR(ret), K(recycle_scn),
                  K(rec_scn));
    } else if (!freeze_guard.can_freeze()) {
      STORAGE_LOG(INFO, "there is a freeze task is running. skip once.", K(recycle_scn),
                  K(rec_scn));
    } else if(OB_FAIL(freeze())) {
      STORAGE_LOG(WARN, "freeze failed", KR(ret), KP(this));
    }
  }

  ObSEArray<ObTableHandleV2, 2> memtable_handles;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_all_memtables(memtable_handles))) {
    STORAGE_LOG(WARN, "get all memtables failed", KR(ret), KP(this));
  } else if (memtable_handles.count() == 0) {
    STORAGE_LOG(INFO, "memtable handles is empty. skip flush once.");
  } else if (OB_FAIL(flush_all_frozen_memtables_(memtable_handles, trace_id))) {
    STORAGE_LOG(WARN, "flush all frozen memtables failed", KR(ret), KP(this));
  } else if (OB_NOT_NULL(tx_data_table_) && OB_FAIL(tx_data_table_->update_memtables_cache())) {
    STORAGE_LOG(WARN, "update memtables cache failed.", KR(ret), KP(this));
  }

  return ret;
}

ObTabletID ObTxDataMemtableMgr::get_tablet_id() const
{
  return LS_TX_DATA_TABLET;
}

bool ObTxDataMemtableMgr::is_flushing() const
{
  return memtable_tail_ - memtable_head_ > 1;
}

int ObTxDataMemtableMgr::get_memtable_range(int64_t &memtable_head, int64_t &memtable_tail)
{
  int ret = OB_SUCCESS;
  memtable_head = memtable_head_;
  memtable_tail = memtable_tail_;
  return ret;
}

}  // namespace storage

}  // namespace oceanbase
