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

#include "storage/ob_storage_table_guard.h"
#include "share/allocator/ob_memstore_allocator_mgr.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/ob_i_table.h"
#include "storage/ob_relative_table.h"
#include "storage/tablet/ob_table_store_util.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx_storage/ob_ls_handle.h" //ObLSHandle
#include "storage/tx_storage/ob_tenant_freezer.h"

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
    const SCN replay_scn,
    const bool for_multi_source_data)
  : tablet_(tablet),
    store_ctx_(store_ctx),
    need_control_mem_(need_control_mem),
    memtable_(nullptr),
    retry_count_(0),
    last_ts_(0),
    for_replay_(for_replay),
    replay_scn_(replay_scn),
    for_multi_source_data_(for_multi_source_data)
{
  init_ts_ = ObTimeUtility::current_time();
}

ObStorageTableGuard::~ObStorageTableGuard()
{
  bool &need_speed_limit = tl_need_speed_limit();
  if (need_control_mem_ && need_speed_limit) {
    bool need_sleep = true;
    int64_t left_interval = SPEED_LIMIT_MAX_SLEEP_TIME;
    if (!for_replay_) {
      left_interval = min(SPEED_LIMIT_MAX_SLEEP_TIME, store_ctx_.timeout_ - ObTimeUtility::current_time());
    }
    if (NULL != memtable_) {
      need_sleep = memtable_->is_active_memtable();
    }
    uint64_t timeout = 10000;//10s
    common::ObWaitEventGuard wait_guard(common::ObWaitEventIds::MEMSTORE_MEM_PAGE_ALLOC_WAIT, timeout, 0, 0, left_interval);

    reset();
    int tmp_ret = OB_SUCCESS;
    bool has_sleep = false;
    int64_t sleep_time = 0;
    int time = 0;
    int64_t &seq = get_seq();
    if (store_ctx_.mvcc_acc_ctx_.is_write()) {
      ObGMemstoreAllocator* memstore_allocator = NULL;
      if (OB_SUCCESS != (tmp_ret = ObMemstoreAllocatorMgr::get_instance().get_tenant_memstore_allocator(
          MTL_ID(), memstore_allocator))) {
      } else if (OB_ISNULL(memstore_allocator)) {
        LOG_WARN_RET(OB_ALLOCATE_MEMORY_FAILED, "get_tenant_mutil_allocator failed", K(store_ctx_.tablet_id_), K(tmp_ret));
      } else {
        while (need_sleep &&
               !memstore_allocator->check_clock_over_seq(seq) &&
               (left_interval > 0)) {
          if (for_replay_) {
            if(MTL(ObTenantFreezer *)->exist_ls_freezing()) {
              break;
            }
          }
          //because left_interval and SLEEP_INTERVAL_PER_TIME both are greater than
          //zero, so it's safe to convert to uint32_t, be careful with comparation between int and uint
          int64_t expected_wait_time = memstore_allocator->expected_wait_time(seq);
          if (expected_wait_time == 0) {
            break;
          }
          uint32_t sleep_interval =
            static_cast<uint32_t>(min(min(left_interval, SLEEP_INTERVAL_PER_TIME), expected_wait_time));
          // don't use ob_usleep, as we are already in the scope of 'wait_guard'
          ::usleep(sleep_interval);
          sleep_time += sleep_interval;
          time++;
          left_interval -= sleep_interval;
          has_sleep = true;
          need_sleep = memstore_allocator->need_do_writing_throttle();
        }
      }
    }

    if (REACH_TIME_INTERVAL(100 * 1000L)) {
      int64_t cost_time = ObTimeUtility::current_time() - init_ts_;
      LOG_INFO("throttle situation", K(sleep_time), K(time), K(seq), K(for_replay_), K(cost_time));
    }

    if (for_replay_ && has_sleep) {
      // avoid print replay_timeout
      get_replay_is_writing_throttling() = true;
    }
  }
  reset();
}

int ObStorageTableGuard::refresh_and_protect_table(ObRelativeTable &relative_table)
{
  int ret = OB_SUCCESS;
  ObTabletTableIterator &iter = relative_table.tablet_iter_;
  const share::ObLSID &ls_id = tablet_->get_tablet_meta().ls_id_;
  const common::ObTabletID &tablet_id = tablet_->get_tablet_meta().tablet_id_;
  if (OB_ISNULL(store_ctx_.ls_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", K(ret), K(ls_id));
  }

  while (OB_SUCC(ret) && need_to_refresh_table(iter.table_iter_)) {
    if (OB_FAIL(store_ctx_.ls_->get_tablet_svr()->get_read_tables(
        tablet_id,
        store_ctx_.mvcc_acc_ctx_.get_snapshot_version().get_val_for_tx(),
        iter,
        relative_table.allow_not_ready()))) {
      LOG_WARN("fail to get read tables", K(ret), K(ls_id), K(tablet_id),
           "table id", relative_table.get_table_id());
    } else {
      // no worry. iter will hold tablet reference and its life cycle is longer than guard
      tablet_ = iter.tablet_handle_.get_obj();
      // TODO: check if seesion is killed
      if (store_ctx_.timeout_ > 0) {
        const int64_t query_left_time = store_ctx_.timeout_ - ObTimeUtility::current_time();
        if (query_left_time <= 0) {
          ret = OB_TRANS_STMT_TIMEOUT;
        }
      }
    }
  }

  return ret;
}

int ObStorageTableGuard::refresh_and_protect_memtable()
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
                                                                                     tablet_id, 0/*schema version*/, for_replay_, clog_checkpoint_scn))) {
              LOG_WARN("fail to create a boundary memtable", K(ret), K(ls_id), K(tablet_id));
            }
          } else { // replay_log_scn_ <= clog_checkpoint_scn
            // no need to create a boundary memtable
            ret = OB_SUCCESS;
            break;
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

void ObStorageTableGuard::reset()
{
  if (NULL != memtable_) {
    memtable_->dec_write_ref();
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
    if (for_replay_ || for_multi_source_data_) {
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
      SCN migration_clog_checkpoint_scn;
      ObIMemtableMgr *memtable_mgr = tablet_->get_memtable_mgr();

      if (OB_ISNULL(memtable_mgr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("memtable mgr is null", K(ret), K(bool_ret), K(ls_id), K(tablet_id), KP(memtable_mgr));
      } else if (OB_FAIL(memtable_mgr->get_newest_clog_checkpoint_scn(clog_checkpoint_scn))) {
        LOG_WARN("failed to get newest clog_checkpoint_scn", K(ret), K(ls_id), K(tablet_id), K(clog_checkpoint_scn));
      } else if (FALSE_IT(migration_clog_checkpoint_scn = static_cast<memtable::ObMemtable *>(memtable)->get_migration_clog_checkpoint_scn())) {
      } else if (for_replay_ && !migration_clog_checkpoint_scn.is_min()) {
        static_cast<memtable::ObMemtable *>(memtable)->resolve_right_boundary();
        if (replay_scn_ <= clog_checkpoint_scn) {
          for_replace_tablet_meta = true;
          need_create_memtable = false;
        }
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

  ObITable *table = iter.get_boundary_table(true);
  if (NULL == table || !table->is_memtable()) {
    // TODO: get the newest schema_version from tablet
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

  if (bool_ret && check_if_need_log()) {
    const share::ObLSID &ls_id = tablet_->get_tablet_meta().ls_id_;
    const common::ObTabletID &tablet_id = tablet_->get_tablet_meta().tablet_id_;
    LOG_WARN_RET(OB_ERR_TOO_MUCH_TIME, "refresh table too much times", K(ret), K(exit_flag), K(ls_id), K(tablet_id), KP(table));
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

  return bool_ret;
}

bool ObStorageTableGuard::check_if_need_log()
{
  bool need_log = false;
  if ((++retry_count_ % GET_TS_INTERVAL) == 0) {
    const int64_t cur_ts = common::ObTimeUtility::current_time();
    if (0 >= last_ts_) {
      last_ts_ = cur_ts;
    } else if (cur_ts - last_ts_ >= LOG_INTERVAL_US) {
      last_ts_ = cur_ts;
      need_log = true;
    } else {
      // do nothing
    }
  }
  return need_log;
}
} // namespace storage
} // namespace oceanbase
