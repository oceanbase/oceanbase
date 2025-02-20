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

#define USING_LOG_PREFIX STORAGE

#include "ob_storage_table_guard.h"
#include "share/allocator/ob_shared_memory_allocator_mgr.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/ob_relative_table.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{
namespace storage
{
using namespace share;
ObStorageTableGuard::ObStorageTableGuard(
    ObTablet *tablet,
    ObStoreCtx &store_ctx,
    const bool need_control_mem,
    const bool for_replay,
    const SCN replay_scn)
  : tablet_(tablet),
    store_ctx_(store_ctx),
    need_control_mem_(need_control_mem),
    memtable_(nullptr),
    retry_count_(0),
    last_ts_(0),
    for_replay_(for_replay),
    replay_scn_(replay_scn)
{
  init_ts_ = ObClockGenerator::getClock();
  share::memstore_throttled_alloc() = 0;
}

ObStorageTableGuard::~ObStorageTableGuard()
{
  (void)throttle_if_needed_();
  reset();
  share::memstore_throttled_alloc() = 0;
}

void ObStorageTableGuard::throttle_if_needed_()
{
  int ret = OB_SUCCESS;
  if (!need_control_mem_) {
    // skip throttle
  } else {
    TxShareThrottleTool &throttle_tool = MTL(ObSharedMemAllocMgr *)->share_resource_throttle_tool();
    ObThrottleInfoGuard share_ti_guard;
    ObThrottleInfoGuard module_ti_guard;
    if (throttle_tool.is_throttling<ObMemstoreAllocator>(share_ti_guard, module_ti_guard)) {

      // only do throttle on active memtable
      if (OB_NOT_NULL(memtable_) && memtable_->is_active_memtable()) {
        reset();
        ObLSHandle ls_handle;
        ObLS *ls = nullptr;
        const ObLSID &ls_id = tablet_->get_tablet_meta().ls_id_;
        if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
          STORAGE_LOG(WARN, "get ls handle failed", KR(ret), K(ls_id));
        } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
        } else {
          (void)TxShareMemThrottleUtil::do_throttle<ObMemstoreAllocator>(for_replay_,
                                                                         store_ctx_.timeout_,
                                                                         share::memstore_throttled_alloc(),
                                                                         *ls,
                                                                         throttle_tool,
                                                                         share_ti_guard,
                                                                         module_ti_guard);
        }
      }

      // if throttle is skipped due to some reasons, advance clock by call skip_throttle() and clean throttle status
      // record in throttle info
      if (throttle_tool.still_throttling<ObMemstoreAllocator>(share_ti_guard, module_ti_guard)){
        int64_t skip_size = share::memstore_throttled_alloc();
        (void)throttle_tool.skip_throttle<ObMemstoreAllocator>(skip_size, share_ti_guard, module_ti_guard);

        if (OB_NOT_NULL(module_ti_guard.throttle_info())) {
          module_ti_guard.throttle_info()->reset();
        }
      }
    }
  }
}

int ObStorageTableGuard::refresh_and_protect_memtable_for_write(ObRelativeTable &relative_table)
{
  int ret = OB_SUCCESS;
  ObTabletTableIterator &iter = relative_table.tablet_iter_;
  const share::ObLSID &ls_id = tablet_->get_tablet_meta().ls_id_;
  const common::ObTabletID &tablet_id = tablet_->get_tablet_meta().tablet_id_;
  if (OB_ISNULL(store_ctx_.ls_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", K(ret), K(ls_id));
  }

  while (OB_SUCC(ret) && need_to_refresh_table(*iter.table_iter())) {
    const int64_t remain_timeout = THIS_WORKER.get_timeout_remain();
    if (OB_UNLIKELY(remain_timeout <= 0)) {
      ret = OB_TIMEOUT;
    } else if (OB_FAIL(store_ctx_.ls_->get_tablet_svr()->get_read_tables(
        tablet_id,
        remain_timeout,
        // snapshot_for_tablet retrieves the versioned tablet status. For write
        // operations, we need to acquire the latest tablet status; otherwise,
        // we may obtain an outdated tablet status during the transfer process.
        share::SCN::max_scn().get_val_for_tx(),
        // snapshot_for_tables filters the tables during get_read_tables
        store_ctx_.mvcc_acc_ctx_.get_snapshot_version().get_val_for_tx(),
        iter,
        relative_table.allow_not_ready()))) {
      LOG_WARN("fail to get read tables", K(ret), K(ls_id), K(remain_timeout),
           "table_id", relative_table.get_table_id());
    } else {
      // no worry. iter will hold tablet reference and its life cycle is longer than guard
      tablet_ = iter.get_tablet();
    }
  }

  return ret;
}

int ObStorageTableGuard::refresh_and_protect_memtable_for_replay()
{
  int ret = OB_SUCCESS;
  ObIMemtableMgr *memtable_mgr = tablet_->get_memtable_mgr();
  memtable::ObIMemtable *memtable = nullptr;
  ObTableHandleV2 handle;
  const share::ObLSID &ls_id = tablet_->get_tablet_meta().ls_id_;
  const common::ObTabletID &tablet_id = tablet_->get_tablet_meta().tablet_id_;
  SCN clog_checkpoint_scn;
  bool bool_ret = true;
  bool for_replace_tablet_meta = false;
  const int64_t start = ObTimeUtility::current_time();

  if (OB_ISNULL(memtable_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("memtable mgr is null", K(ret), KP(memtable_mgr));
  } else {
    do {
      if (OB_FAIL(memtable_mgr->get_boundary_memtable(handle))) {
        // if there is no memtable, create a new one
        if (OB_ENTRY_NOT_EXIST == ret) {
          LOG_DEBUG("there is no boundary memtable", K(ret), K(ls_id), K(tablet_id));
          if (OB_FAIL(memtable_mgr->get_newest_clog_checkpoint_scn(clog_checkpoint_scn))) {
            LOG_WARN("fail to get newest clog_checkpoint_scn", K(ret), K(ls_id), K(tablet_id));
          } else if (replay_scn_ > clog_checkpoint_scn) {
            // TODO: get the newest schema_version from tablet
            ObLSHandle ls_handle;
            if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
              LOG_WARN("failed to get log stream", K(ret), K(ls_id), K(tablet_id));
            } else if (OB_UNLIKELY(!ls_handle.is_valid())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected error, invalid ls handle", K(ret), K(ls_handle), K(ls_id), K(tablet_id));
            } else if (OB_FAIL(ls_handle.get_ls()->get_tablet_svr()->create_memtable(
                                 tablet_id,
                                 0/*schema version*/,
                                 for_replay_,
                                 clog_checkpoint_scn))) {
              LOG_WARN("fail to create a boundary memtable", K(ret), K(ls_id), K(tablet_id));
            }
            // In situation that replay_log_scn_ <= clog_checkpoint_scn, we have
            // no need to create the memtable. While we need double check to
            // decide whether another thread has created the memtable that we
            // need replay. And if it does, we must replay on the memtable.
          } else if (OB_FAIL(double_check_get_memtable_for_replay_(replay_scn_, bool_ret))) {
            LOG_WARN("fail to double check replay memtable", K(ret), K(ls_id), K(tablet_id),
                     K(replay_scn_), K(clog_checkpoint_scn));
          }
        } else { // OB_ENTRY_NOT_EXIST != ret
          LOG_WARN("fail to get boundary memtable", K(ret), K(ls_id), K(tablet_id));
        }
      } else if (OB_FAIL(handle.get_memtable(memtable))) {
        LOG_WARN("fail to get memtable from ObTableHandle", K(ret), K(ls_id), K(tablet_id));
      } else if (OB_FAIL(check_freeze_to_inc_write_ref(memtable, bool_ret, for_replace_tablet_meta))) {
        if (OB_EAGAIN == ret) {
        } else if (OB_MINOR_FREEZE_NOT_ALLOW != ret) {
          LOG_WARN("fail to check_freeze", K(ret), K(tablet_id), K(bool_ret), KPC(memtable));
        }
      } else {
        // do nothing
      }
      const int64_t cost_time = ObTimeUtility::current_time() - start;
      if (cost_time > 10 * 1000) {
        if (TC_REACH_TIME_INTERVAL(10 * 1000)) {
          TRANS_LOG_RET(WARN, OB_ERR_TOO_MUCH_TIME, "refresh replay table too much times", K(ret),
                    K(ls_id), K(tablet_id), K(cost_time));
        }
      }
    } while ((OB_SUCC(ret) || OB_ENTRY_NOT_EXIST == ret || OB_EAGAIN == ret) && bool_ret);
  }
  return ret;
}

int ObStorageTableGuard::double_check_get_memtable_for_replay_(const share::SCN replay_scn,
                                                               bool &need_retry)
{
  int ret = OB_SUCCESS;
  ObTableHandleV2 handle;
  ObIMemtableMgr *memtable_mgr = tablet_->get_memtable_mgr();

  if (OB_FAIL(memtable_mgr->get_memtable_for_replay(replay_scn_, handle))) {
    if (OB_NO_NEED_UPDATE == ret) {
      // no need to replay the log
      need_retry = false;
      ret = OB_SUCCESS;
    } else if (OB_ENTRY_NOT_EXIST == ret) {
      // memtable_mgr not exist, it means nothing need replay
      need_retry = false;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get memtable for replay", K(ret), K(need_retry), K(replay_scn));
    }
  }

  return ret;
}

void ObStorageTableGuard::reset()
{
  if (NULL != memtable_) {
    memtable_->dec_write_ref();
    int64_t used_time = ObClockGenerator::getClock() - init_ts_;
    if (used_time >= 10L * 1000L * 1000L /*10s*/) {
      TRANS_LOG_RET(WARN, OB_ERR_TOO_MUCH_TIME,
                    "dec write ref cost too much time",
                    KPC(memtable_), K(lbt()), K(used_time));
    }
    memtable_ = NULL;
  }
}

void ObStorageTableGuard::double_check_inc_write_ref(
    const uint32_t old_freeze_flag,
    const bool is_tablet_freeze,
    memtable::ObIMemtable *memtable,
    bool &bool_ret)
{
  if (OB_ISNULL(memtable)) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "memtable is null when inc write ref");
  } else {
    memtable->inc_write_ref();
    const uint32 new_freeze_flag = memtable->get_freeze_flag();
    const bool new_is_tablet_freeze = memtable->get_is_tablet_freeze();
    // do double-check to prevent concurrency problems
    if (old_freeze_flag != new_freeze_flag || is_tablet_freeze != new_is_tablet_freeze) {
      memtable->dec_write_ref();
    } else {
      bool_ret = false;
      memtable_ = memtable;
    }
  }
}

int ObStorageTableGuard::get_memtable_for_replay(memtable::ObIMemtable *&memtable)
{
  int ret = OB_SUCCESS;
  const common::ObTabletID &tablet_id = tablet_->get_tablet_meta().tablet_id_;
  memtable = nullptr;

  if (OB_NOT_NULL(memtable_)) {
    memtable = memtable_;
  } else {
    ret = OB_NO_NEED_UPDATE;
  }

  return ret;
}

int ObStorageTableGuard::check_freeze_to_inc_write_ref(ObITable *table, bool &bool_ret, bool &for_replace_tablet_meta)
{
  int ret = OB_SUCCESS;
  bool_ret = true;
  const share::ObLSID &ls_id = tablet_->get_tablet_meta().ls_id_;
  const common::ObTabletID &tablet_id = tablet_->get_tablet_meta().tablet_id_;
  ObIMemtableMgr *memtable_mgr = tablet_->get_memtable_mgr();
  // need to make sure the memtable is a right boundary memtable
  memtable::ObIMemtable *memtable = static_cast<memtable::ObIMemtable *>(table);
  memtable::ObIMemtable *old_memtable = memtable;
  // get freeze_flag before memtable->is_active_memtable() to double-check
  // prevent that the memtable transforms from active to frozen before inc_write_ref
  uint32_t old_freeze_flag = 0;
  bool is_tablet_freeze = false;

  if (OB_ISNULL(memtable_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("memtable mgr is null", K(ret), K(bool_ret), K(ls_id), K(tablet_id), KP(memtable_mgr));
  } else if (OB_ISNULL(table)) {
    LOG_INFO("table is null, need to refresh", K(bool_ret), K(ls_id), K(tablet_id));
  } else if (FALSE_IT(old_freeze_flag = memtable->get_freeze_flag())) {
  } else if (FALSE_IT(is_tablet_freeze = memtable->get_is_tablet_freeze())) {
  } else if (memtable->is_active_memtable() || for_replace_tablet_meta) {
    // the most recent memtable is active
    // no need to create a new memtable
    if (for_replay_) {
      // filter memtables for replay or multi_source_data according to scn
      ObTableHandleV2 handle;
      if (OB_FAIL(memtable_mgr->get_memtable_for_replay(replay_scn_, handle))) {
        if (OB_NO_NEED_UPDATE == ret) {
          // no need to replay the log
          bool_ret = false;
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get memtable for replay", K(ret), K(bool_ret), K(ls_id), K(tablet_id));
        }
      } else if (OB_FAIL(handle.get_memtable(memtable))) {
        LOG_WARN("fail to get memtable from ObTableHandle", K(ret), K(bool_ret), K(ls_id), K(tablet_id));
      } else {
        if (memtable != old_memtable) {
          is_tablet_freeze = memtable->get_is_tablet_freeze();
        }
        double_check_inc_write_ref(old_freeze_flag, is_tablet_freeze, memtable, bool_ret);
      }
    } else {
      double_check_inc_write_ref(old_freeze_flag, is_tablet_freeze, memtable, bool_ret);
    }
  } else {
    // the most recent memtable is frozen
    // need to create a new memtable
    // TODO: allow to write frozen memtables except for the boundary one when replaying
    const int64_t write_ref = memtable->get_write_ref();
    if (0 == write_ref) {
      SCN clog_checkpoint_scn;
      bool need_create_memtable = true;
      ObIMemtableMgr *memtable_mgr = tablet_->get_memtable_mgr();

      if (OB_ISNULL(memtable_mgr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("memtable mgr is null", K(ret), K(bool_ret), K(ls_id), K(tablet_id), KP(memtable_mgr));
      } else if (OB_FAIL(memtable_mgr->get_newest_clog_checkpoint_scn(clog_checkpoint_scn))) {
        LOG_WARN("failed to get newest clog_checkpoint_scn", K(ret), K(ls_id), K(tablet_id), K(clog_checkpoint_scn));
      } else if (for_replay_ && replay_scn_ <= clog_checkpoint_scn) {
        need_create_memtable = false;
      }

      // create a new memtable if no write in the old memtable
      if (OB_FAIL(ret)) {
      } else if (need_create_memtable) {
        ObLSHandle ls_handle;
        if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
          LOG_WARN("failed to get log stream", K(ret), K(bool_ret), K(ls_id), K(tablet_id));
        } else if (OB_UNLIKELY(!ls_handle.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error, invalid ls handle", K(ret), K(bool_ret), K(ls_handle), K(ls_id), K(tablet_id));
        } else if (OB_FAIL(ls_handle.get_ls()->get_tablet_svr()->create_memtable(tablet_id,
                                                                                 memtable->get_max_schema_version()/*schema version*/,
                                                                                 for_replay_,
                                                                                 clog_checkpoint_scn))) {
          if (OB_EAGAIN == ret) {
          } else if (OB_MINOR_FREEZE_NOT_ALLOW != ret) {
            LOG_WARN("fail to create new memtable for freeze", K(ret), K(bool_ret), K(ls_id), K(tablet_id));
          }
        }
      } else if (for_replay_) {
        ObTableHandleV2 handle;
        if (OB_FAIL(memtable_mgr->get_memtable_for_replay(replay_scn_, handle))) {
          if (OB_NO_NEED_UPDATE == ret) {
            // no need to replay the log
            bool_ret = false;
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("fail to get memtable for replay", K(ret), K(bool_ret), K(ls_id), K(tablet_id));
          }
        } else if (OB_FAIL(handle.get_memtable(memtable))) {
          LOG_WARN("fail to get memtable from ObTableHandle", K(ret), K(bool_ret), K(ls_id), K(tablet_id));
        } else {
          if (memtable != old_memtable) {
            is_tablet_freeze = memtable->get_is_tablet_freeze();
          }
          double_check_inc_write_ref(old_freeze_flag, is_tablet_freeze, memtable, bool_ret);
        }
      }
    }
  }

  return ret;
}

bool ObStorageTableGuard::need_to_refresh_table(ObTableStoreIterator &iter)
{
  int ret = OB_SUCCESS;
  bool bool_ret = false;
  bool for_replace_tablet_meta = false;
  int exit_flag = -1;
  ObITable *table = iter.get_last_memtable();
  bool need_create_memtable = false;
  if (NULL == table || !table->is_memtable()) {
    need_create_memtable = true;
  } else {
    memtable::ObIMemtable *memtable = static_cast<memtable::ObIMemtable *>(table);
    ObLSID ls_id = memtable->get_ls_id();
    if (OB_UNLIKELY(!ls_id.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get memtable ls_id", K(ret), KPC(table));
    } else if (ls_id != store_ctx_.ls_->get_ls_id()) {
      if (OB_UNLIKELY(!table->is_data_memtable())) {
        ret =OB_ERR_UNEXPECTED;
        ObLSID curr_ls_id = store_ctx_.ls_->get_ls_id();
        LOG_WARN("table is not data memtable, it does not allow ls_id to be different", K(ret), K(ls_id), K(curr_ls_id), KPC(table));
      } else {
        need_create_memtable = true;
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (need_create_memtable) {
    const common::ObTabletID &tablet_id = tablet_->get_tablet_meta().tablet_id_;
    if (OB_FAIL(store_ctx_.ls_->get_tablet_svr()->create_memtable(tablet_id, 0/*schema version*/, for_replay_))) {
      LOG_WARN("fail to create a boundary memtable", K(ret), K(tablet_id));
    }
    bool_ret = true;
    exit_flag = 0;
  } else if (!table->is_data_memtable()) {
    bool_ret = false;
  } else if (iter.check_store_expire()) {
    bool_ret = true;
    exit_flag = 1;
  } else if (OB_FAIL(check_freeze_to_inc_write_ref(table, bool_ret, for_replace_tablet_meta))) {
    bool_ret = true;
    exit_flag = 2;
    if (OB_MINOR_FREEZE_NOT_ALLOW != ret) {
      LOG_WARN("fail to inc write ref", K(ret));
    }
  } else {
    exit_flag = 3;
  }

  if (bool_ret) {
    bool need_log = false;
    bool need_log_error = false;
    check_if_need_log_(need_log, need_log_error);
    if (need_log) {
      const share::ObLSID &ls_id = tablet_->get_tablet_meta().ls_id_;
      const common::ObTabletID &tablet_id = tablet_->get_tablet_meta().tablet_id_;
      if (need_log_error) {
        LOG_ERROR_RET(OB_ERR_TOO_MUCH_TIME, "refresh table too much times", K(ret), K(exit_flag), K(ls_id), K(tablet_id), KP(table));
        LOG_DBA_ERROR_V2(OB_STORAGE_MEMTABLE_REFRESH_TIMEOUT,
                         OB_ERR_TOO_MUCH_TIME,
                         "refresh table too much times",
                         ", with ls_id=", ls_id,
                         ", with tablet_id=", tablet_id,
                         ", with exit_flag=", exit_flag);
      } else {
        LOG_WARN_RET(OB_ERR_TOO_MUCH_TIME, "refresh table too much times", K(ret), K(exit_flag), K(ls_id), K(tablet_id), KP(table));
        LOG_DBA_WARN_V2(OB_STORAGE_MEMTABLE_REFRESH_TOO_MUCH_TIME,
                        OB_ERR_TOO_MUCH_TIME,
                        "refresh table too much times",
                        ", with ls_id=", ls_id,
                        ", with tablet_id=", tablet_id,
                        ", with exit_flag=", exit_flag);
      }
      if (0 == exit_flag) {
        LOG_WARN("table is null or not memtable", K(ret), K(ls_id), K(tablet_id), KP(table));
      } else if (1 == exit_flag) {
        LOG_WARN("iterator store is expired", K(ret), K(ls_id), K(tablet_id), K(iter.check_store_expire()), K(iter.count()), K(iter));
      } else if (2 == exit_flag) {
        LOG_WARN("failed to check_freeze_to_inc_write_ref", K(ret), K(ls_id), K(tablet_id), KPC(table));
      } else if (3 == exit_flag) {
        LOG_WARN_RET(OB_ERR_TOO_MUCH_TIME, "check_freeze_to_inc_write_ref costs too much time", K(ret), K(ls_id), K(tablet_id), KPC(table));
      } else {
        LOG_WARN("unexpect exit_flag", K(exit_flag), K(ret), K(ls_id), K(tablet_id));
      }
    }
  }
  return bool_ret;
}

void ObStorageTableGuard::check_if_need_log_(bool &need_log,
                                             bool &need_log_error)
{
  need_log = false;
  need_log_error = false;
  if ((++retry_count_ % GET_TS_INTERVAL) == 0) {
    const int64_t cur_ts = common::ObTimeUtility::current_time();
    if (0 >= last_ts_) {
      last_ts_ = cur_ts;
    } else if (cur_ts - last_ts_ >= LOG_ERROR_INTERVAL_US) {
      last_ts_ = cur_ts;
      need_log = true;
      need_log_error = true;
    } else if (cur_ts - last_ts_ >= LOG_INTERVAL_US) {
      last_ts_ = cur_ts;
      need_log = true;
    } else {
      // do nothing
    }
  }
}
} // namespace storage
} // namespace oceanbase
