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
#include "storage/tablet/ob_tablet_memtable_mgr.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/memtable/ob_multi_source_data.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/ls/ob_freezer.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{
using namespace common;
using namespace rootserver;
using namespace blocksstable;
using namespace memtable;
using namespace transaction;
using namespace clog;
using namespace share::schema;
using namespace share;
using namespace obrpc;
namespace storage
{
using namespace mds;

ObTabletMemtableMgr::ObTabletMemtableMgr()
  : ObIMemtableMgr(LockType::OB_SPIN_RWLOCK, &lock_def_),
    ls_(nullptr),
    lock_def_(common::ObLatchIds::TABLET_MEMTABLE_LOCK),
    retry_times_(0),
    schema_recorder_(),
    medium_info_recorder_()
{
#if defined(__x86_64__)
  static_assert(sizeof(ObTabletMemtableMgr) <= 2048, "The size of ObTabletMemtableMgr will affect the meta memory manager, and the necessity of adding new fields needs to be considered.");
#endif
}

ObTabletMemtableMgr::~ObTabletMemtableMgr()
{
  destroy();
}

void ObTabletMemtableMgr::destroy()
{
  STORAGE_LOG(DEBUG, "destroy tablet memtable mgr", KP(this), KPC(this));
  MemMgrWLockGuard lock_guard(lock_);
  // release memtable
  memtable::ObIMemtable *imemtable = nullptr;
  int ret = OB_SUCCESS;
  for (int64_t pos = memtable_head_; pos < memtable_tail_; ++pos) {
    imemtable = tables_[get_memtable_idx(pos)];
    if (OB_ISNULL(imemtable)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "memtable is nullptr", K(ret), KP(imemtable), K(pos));
    } else if (imemtable->is_data_memtable()) {
      memtable::ObMemtable *memtable = static_cast<memtable::ObMemtable *>(imemtable);
      unlink_memtable_mgr_and_memtable_(memtable);
      memtable->remove_from_data_checkpoint();
      memtable->set_frozen();
    }
  }

  reset_tables();
  tablet_id_ = 0;
  ls_ = NULL;
  freezer_ = nullptr;
  schema_recorder_.destroy();
  medium_info_recorder_.destroy();
  retry_times_ = 0;
  is_inited_ = false;
}

int ObTabletMemtableMgr::init(const common::ObTabletID &tablet_id,
                              const ObLSID &ls_id,
                              ObFreezer *freezer,
                              ObTenantMetaMemMgr *t3m)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = MTL(storage::ObLSService *);
  ObLSHandle ls_handle;
  ObMdsTableMgr *mds_table_mgr = nullptr;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("this has been initialized, not init again", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!tablet_id.is_valid())
             || OB_ISNULL(t3m)
             || OB_ISNULL(freezer)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tablet_id), KP(freezer), KP(t3m));
  } else if (OB_ISNULL(ls_service)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "ls service should not be NULL", K(ret), KP(ls_service));
  } else if (OB_FAIL(ls_service->get_ls(ls_id,
                                        ls_handle,
                                        ObLSGetMod::TABLET_MOD))) {
    LOG_WARN("failed to get ls", K(ret), K(MTL_ID()));
  } else if (OB_ISNULL(ls_ = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "ls should not be NULL", K(ret), KP(ls_));
  } else {
    tablet_id_ = tablet_id;
    t3m_ = t3m;
    table_type_ = ObITable::TableType::DATA_MEMTABLE;
    freezer_ = freezer;
    retry_times_ = 0;
    is_inited_ = true;
    TRANS_LOG(DEBUG, "succeeded to init tablet memtable mgr", K(ret), K(ls_id), K(tablet_id), KP(this), KPC(this));
  }

  if (OB_UNLIKELY(!is_inited_)) {
    destroy();
  }
  return ret;
}

int ObTabletMemtableMgr::init_storage_recorder(
    const ObTabletID &tablet_id,
    const share::ObLSID &ls_id,
    const int64_t max_saved_schema_version,
    const int64_t max_saved_medium_scn,
    const lib::Worker::CompatMode compat_mode,
    logservice::ObLogHandler *log_handler)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(schema_recorder_.init(ls_id, tablet_id, max_saved_schema_version, compat_mode, log_handler))) {
    TRANS_LOG(WARN, "failed to init schema recorder", K(ret), K(max_saved_schema_version), KP(log_handler));
  } else if (OB_FAIL(medium_info_recorder_.init(ls_id, tablet_id, max_saved_medium_scn, log_handler))) {
    TRANS_LOG(WARN, "failed to init medium info recorder", K(ret), K(max_saved_medium_scn), KP(log_handler));
  } else {
    TRANS_LOG(INFO, "success to init storage recorder", K(ret), K(ls_id), K(tablet_id), K(max_saved_schema_version),
      K(max_saved_medium_scn), K(compat_mode));
  }
  return ret;
}

int ObTabletMemtableMgr::reset_storage_recorder()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!schema_recorder_.is_inited() || !medium_info_recorder_.is_inited())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema recorder or medium recorder is not init", K(ret), K_(schema_recorder),
        K_(medium_info_recorder));
  } else {
    schema_recorder_.reset();
    medium_info_recorder_.reset();
  }
  return ret;
}

// There are two cases:
// 1. create the first memtable for tablet
// 2. create the new memtable after freezing the old memtable
int ObTabletMemtableMgr::create_memtable(const SCN clog_checkpoint_scn,
                                         const int64_t schema_version,
                                         const SCN new_clog_checkpoint_scn,
                                         const bool for_replay)
{
  ObTimeGuard time_guard("ObTabletMemtableMgr::create_memtable", 10 * 1000);
  // Write lock
  MemMgrWLockGuard lock_guard(lock_);
  time_guard.click("lock");

  int ret = OB_SUCCESS;
  ObTableHandleV2 memtable_handle;
  const bool has_memtable = has_memtable_();
  const uint32_t logstream_freeze_clock = freezer_->get_freeze_clock();
  memtable::ObMemtable *active_memtable = nullptr;
  uint32_t memtable_freeze_clock = UINT32_MAX;
  share::ObLSID ls_id;
  int64_t memtable_count = get_memtable_count_();
  if (has_memtable && OB_NOT_NULL(active_memtable = get_active_memtable_())) {
    memtable_freeze_clock = active_memtable->get_freeze_clock();
  }

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_ISNULL(ls_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", K(ret));
  } else if (FALSE_IT(ls_id = ls_->get_ls_id())) {
  } else if (logstream_freeze_clock == memtable_freeze_clock) {
    // new memtable has already existed
    ret = OB_ENTRY_EXIST;
  } else if (memtable_count >= MAX_MEMSTORE_CNT) {
    ret = OB_MINOR_FREEZE_NOT_ALLOW;
    ob_usleep(1 * 1000);
    if ((++retry_times_ % (60 * 1000)) == 0) { // 1 min
      ObTableHandleV2 first_frozen_memtable;
      get_first_frozen_memtable_(first_frozen_memtable);
      LOG_ERROR("cannot create more memtable", K(ret), K(ls_id), K(tablet_id_), K(MAX_MEMSTORE_CNT),
                K(get_memtable_count_()),
                KPC(first_frozen_memtable.get_table()));
    }
  } else if (for_replay && clog_checkpoint_scn != new_clog_checkpoint_scn) {
    ret = OB_EAGAIN;
    LOG_INFO("clog_checkpoint_scn changed, need retry to replay", K(ls_id), K(tablet_id_), K(clog_checkpoint_scn), K(new_clog_checkpoint_scn));
  } else {
    ObITable::TableKey table_key;
    table_key.table_type_ = ObITable::DATA_MEMTABLE;
    table_key.tablet_id_ = tablet_id_;
    table_key.scn_range_.start_scn_ = clog_checkpoint_scn;
    table_key.scn_range_.end_scn_.set_max();
    memtable::ObMemtable *memtable = NULL;
    ObLSHandle ls_handle;
    retry_times_ = 0;

    if (OB_FAIL(t3m_->acquire_memtable(memtable_handle))) {
      LOG_WARN("failed to create memtable", K(ret), K(ls_id), K(tablet_id_));
    } else if (FALSE_IT(time_guard.click("acquire_memtable"))) {
    } else if (OB_ISNULL(memtable = static_cast<memtable::ObMemtable *>(memtable_handle.get_table()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get memtable", K(ret), K(ls_id), K(tablet_id_), K(memtable_handle));
    } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::DATA_MEMTABLE_MOD))) {
          LOG_WARN("failed to get log stream", K(ret), K(ls_id), K(tablet_id_));
    } else if (OB_UNLIKELY(!ls_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, invalid ls handle", K(ret), K(ls_handle), K(ls_id), K(tablet_id_));
    } else if (OB_FAIL(memtable->init(table_key,
                                      ls_handle,
                                      freezer_,
                                      this,
                                      schema_version,
                                      logstream_freeze_clock))) {
      LOG_WARN("failed to init memtable", K(ret), K(ls_id), K(table_key), KP(freezer_), KP(this),
               K(schema_version), K(logstream_freeze_clock));
    } else {
      ObTableHandleV2 last_frozen_memtable_handle;
      memtable::ObMemtable *last_frozen_memtable = nullptr;
      if (OB_FAIL(get_last_frozen_memtable_(last_frozen_memtable_handle))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("fail to get last frozen memtable", K(ret));
        } else {
          ret = OB_SUCCESS;
        }
      } else if (OB_FAIL(last_frozen_memtable_handle.get_data_memtable(last_frozen_memtable))) {
        LOG_WARN("fail to get memtable", K(ret));
      }

      if (OB_FAIL(ret)) {
      } else if (OB_NOT_NULL(last_frozen_memtable)) {
        // keep the check order: is_frozen, write_ref_cnt, then unsubmitted_cnt and unsynced_cnt
        int64_t write_ref = last_frozen_memtable->get_write_ref();
        int64_t unsubmitted_cnt = last_frozen_memtable->get_unsubmitted_cnt();
        int64_t unsynced_cnt = last_frozen_memtable->get_unsynced_cnt();
        if (write_ref > 0 || unsubmitted_cnt > 0) {
          memtable->set_logging_blocked();
          TRANS_LOG(INFO, "set logging_block", KPC(last_frozen_memtable), KPC(memtable));
        }
        if (write_ref > 0 || unsynced_cnt > 0) {
          last_frozen_memtable->set_resolve_active_memtable_left_boundary(false);
        }
        // for follower, must decide the boundary of frozen memtable
        // for leader, decide the boundary of frozen memtable that meets ready_for_flush
        if (for_replay || (0 == write_ref &&
                           0 == unsubmitted_cnt &&
                           0 == unsynced_cnt)) {
          last_frozen_memtable->resolve_right_boundary();
          TRANS_LOG(INFO, "[resolve_right_boundary] last_frozen_memtable in create_memtable", K(for_replay), K(ls_id), KPC(last_frozen_memtable));
          if (memtable != last_frozen_memtable) {
            const SCN &new_start_scn = MAX(last_frozen_memtable->get_end_scn(), last_frozen_memtable->get_migration_clog_checkpoint_scn());
            memtable->resolve_left_boundary(new_start_scn);
          }
        }
      // there is no frozen memtable and new sstable will not be generated,
      // meaning that clog_checkpoint_scn will not be updated now,
      // so get newest clog_checkpoint_scn to set left boundary
      } else {
        memtable->resolve_left_boundary(new_clog_checkpoint_scn);
      }

      time_guard.click("init memtable");
      if (OB_SUCC(ret)) {
        if (MAX_MEMSTORE_CNT - 1 == memtable_count) {
          // if the new memtable is last one in memtable_array
          // not allow it to be freezed
          // otherwise the number of memtables will be out of limit
          memtable->set_allow_freeze(false);
          FLOG_INFO("not allow memtable to be freezed", K(memtable_count), K(MAX_MEMSTORE_CNT), KPC(memtable));
        }
        if (OB_FAIL(add_memtable_(memtable_handle))) {
          LOG_WARN("failed to add memtable", K(ret), K(ls_id), K(tablet_id_), K(memtable_handle));
        } else if (FALSE_IT(time_guard.click("add memtable"))) {
        } else if (OB_FAIL(memtable->add_to_data_checkpoint(freezer_->get_ls_data_checkpoint()))) {
          LOG_WARN("add to data_checkpoint failed", K(ret), K(ls_id), KPC(memtable));
          clean_tail_memtable_();
        } else if (FALSE_IT(time_guard.click("add to data_checkpoint"))) {
        } else {
          LOG_INFO("succeed to create memtable", K(ret), K(ls_id), KPC(memtable), KPC(this));
        }
      }
    }
  }

  return ret;
}

uint32_t ObTabletMemtableMgr::get_ls_freeze_clock()
{
  return freezer_->get_freeze_clock();
}

bool ObTabletMemtableMgr::has_active_memtable()
{
  int ret = OB_SUCCESS;
  bool bool_ret = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else {
    MemMgrRLockGuard lock_guard(lock_);
    if (NULL != get_active_memtable_()) {
      bool_ret = true;
    }
  }
  return bool_ret;
}

int64_t ObTabletMemtableMgr::get_memtable_count() const
{
  MemMgrRLockGuard lock_guard(lock_);
  return get_memtable_count_();
}

int ObTabletMemtableMgr::get_boundary_memtable(ObTableHandleV2 &handle)
{
  int ret = OB_SUCCESS;
  MemMgrRLockGuard lock_guard(lock_);
  memtable::ObIMemtable *memtable = nullptr;
  handle.reset();

  if (OB_UNLIKELY(get_memtable_count_() == 0)) {
    ret = OB_ENTRY_NOT_EXIST;
  } else if (memtable_tail_ > memtable_head_) {
    if (OB_FAIL(get_ith_memtable(memtable_tail_ - 1, handle))) {
      STORAGE_LOG(WARN, "fail to get ith memtable", K(ret), K(memtable_tail_));
    } else if (OB_FAIL(handle.get_memtable(memtable))) {
      handle.reset();
      LOG_WARN("there is no boundary memtable", K(ret));
    }
  }

  return ret;
}

int ObTabletMemtableMgr::get_active_memtable(ObTableHandleV2 &handle) const
{
  int ret = OB_SUCCESS;
  MemMgrRLockGuard lock_guard(lock_);
  handle.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(get_active_memtable_(handle))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to get active memtable", K(ret));
    }
  }
  return ret;
}

int ObTabletMemtableMgr::get_active_memtable_(ObTableHandleV2 &handle) const
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(get_memtable_count_() == 0)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_DEBUG("no memtable exists", K(ret));
  } else if (memtable_tail_ > memtable_head_) {
    memtable::ObMemtable *memtable = nullptr;
    if (OB_FAIL(get_ith_memtable(memtable_tail_ - 1, handle))) {
      STORAGE_LOG(WARN, "fail to get ith memtable", K(ret), K(memtable_tail_));
    } else if (OB_FAIL(handle.get_data_memtable(memtable))) {
      LOG_WARN("fail to get memtable", K(ret));
    } else if (!memtable->is_active_memtable()) {
      handle.reset();
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

int ObTabletMemtableMgr::get_last_frozen_memtable(ObTableHandleV2 &handle) const
{
  int ret = OB_SUCCESS;
  MemMgrRLockGuard lock_guard(lock_);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(get_last_frozen_memtable_(handle))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to get last frozen memtable", K(ret));
    }
  }

  return ret;
}

int ObTabletMemtableMgr::get_last_frozen_memtable_(ObTableHandleV2 &handle) const
{
  int ret = OB_SUCCESS;
  handle.reset();

  if (OB_UNLIKELY(get_memtable_count_() == 0)) {
    ret = OB_ENTRY_NOT_EXIST;
  } else if (memtable_tail_ > memtable_head_) {
    for (int64_t i = memtable_tail_ - 1; OB_SUCC(ret) && i >= memtable_head_; --i) {
      ObTableHandleV2 m_handle;
      const ObMemtable *memtable = nullptr;
      if (OB_FAIL(get_ith_memtable(i, m_handle))) {
        STORAGE_LOG(WARN, "fail to get ith memtable", K(ret), K(i));
      } else if (OB_UNLIKELY(!m_handle.is_valid())) {
        ret = OB_ERR_SYS;
        LOG_ERROR("memtable handle is invalid", K(ret), K(m_handle));
      } else if (OB_FAIL(m_handle.get_data_memtable(memtable))) {
        LOG_WARN("fail to get memtable", K(ret), K(m_handle));
      } else if (OB_ISNULL(memtable)) {
        ret = OB_ERR_SYS;
        LOG_ERROR("memtable must not null", K(ret), K(m_handle));
      } else if (memtable->is_frozen_memtable()) {
        handle = m_handle;
        break;
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!handle.is_valid()) {
    ret = OB_ENTRY_NOT_EXIST;
  }

  return ret;
}

int ObTabletMemtableMgr::resolve_left_boundary_for_active_memtable(memtable::ObIMemtable *memtable,
                                                                   SCN start_scn,
                                                                   SCN snapshot_scn)
{
  ObTableHandleV2 handle;
  ObIMemtable *active_memtable = nullptr;
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(get_active_memtable(handle))) {
    LOG_WARN( "fail to get active memtable", K(ret));
  } else if (OB_FAIL(handle.get_memtable(active_memtable))) {
    LOG_WARN("fail to get active memtable", K(ret));
  } else {
    // set the start_scn of the new memtable
    static_cast<ObMemtable*>(active_memtable)->resolve_left_boundary(start_scn);
  }
  if (OB_ENTRY_NOT_EXIST== ret) {
    ret = OB_SUCCESS;
  }
  if (OB_SUCC(ret)) {
    static_cast<ObMemtable*>(memtable)->set_resolve_active_memtable_left_boundary(true);
  }

  return ret;
}

int ObTabletMemtableMgr::unset_logging_blocked_for_active_memtable(memtable::ObIMemtable *memtable)
{
  ObTableHandleV2 handle;
  ObIMemtable *active_memtable = nullptr;
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(get_active_memtable(handle))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to get active memtable", K(ret));
    }
  } else if (OB_FAIL(handle.get_memtable(active_memtable))) {
    LOG_WARN("fail to get active memtable", K(ret));
  } else {
    // allow the new memtable to submit log
    static_cast<ObMemtable*>(active_memtable)->unset_logging_blocked();
  }
  if (OB_ENTRY_NOT_EXIST== ret) {
    ret = OB_SUCCESS;
  }
  if (OB_SUCC(ret)) {
    static_cast<ObMemtable*>(memtable)->unset_active_memtable_logging_blocked();
  }

  return ret;
}

int ObTabletMemtableMgr::set_is_tablet_freeze_for_active_memtable(ObTableHandleV2 &handle, bool is_force_freeze)
{
  handle.reset();
  memtable::ObIMemtable *active_memtable = nullptr;
  memtable::ObMemtable *memtable = nullptr;
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(get_active_memtable(handle))) {
    LOG_DEBUG("fail to get active memtable", K(ret));
  } else if (OB_FAIL(handle.get_memtable(active_memtable))) {
    LOG_WARN("fail to get active memtable", K(ret));
    if (ret == OB_NOT_INIT) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("active memtable is null", K(ret));
    }
  } else if (FALSE_IT(memtable = static_cast<ObMemtable*>(active_memtable))) {
  } else if (memtable->allow_freeze()) {
    memtable->set_is_tablet_freeze();
    if (is_force_freeze) {
      memtable->set_is_force_freeze();
    }
  } else {
    handle.reset();
    ret = OB_ENTRY_NOT_EXIST;
    TRANS_LOG(INFO, "not set is_tablet_freeze because the memtable cannot be freezed", KPC(memtable));
  }

  return ret;
}

int ObTabletMemtableMgr::get_memtable_for_replay(SCN replay_scn,
                                                 ObTableHandleV2 &handle)
{
  int ret = OB_SUCCESS;
  ObMemtable *memtable = nullptr;
  handle.reset();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_ISNULL(ls_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ls is null", K(ret));
  } else {
    const share::ObLSID &ls_id = ls_->get_ls_id();
    MemMgrRLockGuard lock_guard(lock_);
    int64_t i = 0;
    for (i = memtable_tail_ - 1; OB_SUCC(ret) && i >= memtable_head_; --i) {
      if (OB_FAIL(get_ith_memtable(i, handle))) {
        STORAGE_LOG(WARN, "fail to get ith memtable", K(ret), K(i));
      } else if (OB_FAIL(handle.get_data_memtable(memtable))) {
        handle.reset();
        LOG_WARN("fail to get data memtable", K(ret));
      } else {
        if (replay_scn > memtable->get_start_scn() && replay_scn <= memtable->get_end_scn()) {
          break;
        } else {
          handle.reset();
        }
      }
    }
    if (OB_SUCC(ret) && !handle.is_valid() && i < memtable_head_) {
      SCN clog_checkpoint_scn;
      if (OB_FAIL(get_newest_clog_checkpoint_scn(clog_checkpoint_scn))) {
      } else if (replay_scn <= clog_checkpoint_scn) {
        // no need to replay the log
        ret = OB_NO_NEED_UPDATE;
      } else {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("fail to get memtable for replay", K(ret), K(ls_id), K(tablet_id_), K(replay_scn), K(clog_checkpoint_scn), K(memtable_tail_ - memtable_head_));
      }
    }
  }

  return ret;
}

int ObTabletMemtableMgr::get_memtables(
    ObTableHdlArray &handle,
    const bool reset_handle,
    const int64_t start_point,
    const bool include_active_memtable)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else {
    MemMgrRLockGuard lock_guard(lock_);
    if (reset_handle) {
      handle.reset();
    }
    if (OB_FAIL(get_memtables_(handle, start_point, include_active_memtable))) {
      LOG_WARN("fail to get memtables", K(ret), K(start_point), K(include_active_memtable));
    }
  }
  return ret;
}

int ObTabletMemtableMgr::get_memtables_nolock(ObTableHdlArray &handle)
{
  int ret = OB_SUCCESS;
  const int64_t start_point = -1;
  const bool include_active_memtable = true;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(get_memtables_(handle, start_point, include_active_memtable))) {
    LOG_WARN("fail to get memtables", K(ret), K(start_point), K(include_active_memtable));
  }
  return ret;
}

int ObTabletMemtableMgr::get_all_memtables(ObTableHdlArray &handle)
{
  int ret = OB_SUCCESS;
  MemMgrRLockGuard lock_guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(get_memtables_nolock(handle))) {
    LOG_WARN("failed to get all memtables", K(ret));
  }
  return ret;
}

int ObTabletMemtableMgr::release_head_memtable_(memtable::ObIMemtable *imemtable,
                                                const bool force)
{
  UNUSED(force);
  int ret = OB_SUCCESS;
  memtable::ObMemtable *memtable = static_cast<memtable::ObMemtable *>(imemtable);

  if (OB_UNLIKELY(get_memtable_count_() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(ls_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ls is null", K(ret));
  } else {
    const share::ObLSID &ls_id = ls_->get_ls_id();
    const int64_t idx = get_memtable_idx(memtable_head_);
    if (nullptr != tables_[idx] && memtable == tables_[idx]) {
      LOG_INFO("release head memtable", K(ret), K(ls_id), KPC(memtable));
      memtable::ObMtStat& mt_stat = memtable->get_mt_stat();
      if (0 == mt_stat.release_time_) {
        mt_stat.release_time_ = ObTimeUtility::current_time();
      } else {
        LOG_WARN("cannot set release_time twice", K(ls_id), KPC(memtable));
      }
      if (!memtable->is_empty()) {
        memtable->set_read_barrier();
      }
      unlink_memtable_mgr_and_memtable_(memtable);
      memtable->remove_from_data_checkpoint();
      memtable->set_is_flushed();
      memtable->set_freeze_state(ObMemtableFreezeState::RELEASED);
      memtable->set_frozen();
      release_head_memtable();
      memtable::ObMemtable *active_memtable = get_active_memtable_();
      if (OB_NOT_NULL(active_memtable) && !active_memtable->allow_freeze()) {
        active_memtable->set_allow_freeze(true);
        FLOG_INFO("allow active memtable to be freezed", K(ls_id), KPC(active_memtable));
      }
      FLOG_INFO("succeed to release head data memtable", K(ret), K(ls_id), K(tablet_id_));
    }
  }

  return ret;
}

void ObTabletMemtableMgr::unlink_memtable_mgr_and_memtable_(memtable::ObMemtable *memtable)
{
  // unlink memtable_mgr and memtable
  // and wait the running ops about memtable_mgr in the memtable
  int ret = OB_SUCCESS;
  if (OB_ISNULL(memtable)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("memtable is null", K(ret), KPC(this));
  } else {
    memtable->clear_memtable_mgr();
    wait_memtable_mgr_op_cnt_(memtable);
  }
}

void ObTabletMemtableMgr::wait_memtable_mgr_op_cnt_(memtable::ObMemtable *memtable)
{
  if (OB_NOT_NULL(memtable)) {
    const int64_t start = ObTimeUtility::current_time();
    int ret = OB_SUCCESS;
    while (0 != memtable->get_memtable_mgr_op_cnt()) {
      const int64_t cost_time = ObTimeUtility::current_time() - start;
      if (cost_time > 1000 * 1000) {
        if (TC_REACH_TIME_INTERVAL(1000 * 1000)) {
          LOG_WARN("wait_memtable_mgr_op_cnt costs too much time", KPC(memtable));
        }
      }
    }
  }
}

int ObTabletMemtableMgr::get_first_frozen_memtable(ObTableHandleV2 &handle) const
{
  int ret = OB_SUCCESS;
  memtable::ObMemtable *memtable = NULL;
  MemMgrRLockGuard guard(lock_);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(get_first_frozen_memtable_(handle))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to get first frozen memtable", K(ret));
    }
  }

  return ret;
}

memtable::ObMemtable *ObTabletMemtableMgr::get_active_memtable_()
{
  int ret = OB_SUCCESS;
  memtable::ObMemtable *memtable = nullptr;
  if (memtable_tail_ > memtable_head_) {
    memtable = static_cast<memtable::ObMemtable *>(tables_[get_memtable_idx(memtable_tail_ - 1)]);
    if (OB_ISNULL(memtable)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "memtable is nullptr", K(ret), KP(memtable), K(memtable_tail_));
    } else if (!memtable->is_active_memtable()) {
      memtable = NULL;
    }
  }

  return memtable;
}

memtable::ObMemtable *ObTabletMemtableMgr::get_memtable_(const int64_t pos) const
{
  int ret = OB_SUCCESS;
  memtable::ObMemtable *memtable = nullptr;
  memtable::ObMemtable *table = static_cast<memtable::ObMemtable *>(tables_[get_memtable_idx(pos)]);
  if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("table is nullptr", K(ret), KP(table), K(pos));
  } else if (!table->is_data_memtable()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("not data memtable", K(ret), K(pos), K(table->get_key()));
  } else {
    memtable = table;
  }
  return const_cast<memtable::ObMemtable *>(memtable);
}

int64_t ObTabletMemtableMgr::get_unmerged_memtable_count_() const
{
  int64_t cnt = 0;

  for (int64_t i = memtable_head_; i < memtable_tail_; i++) {
    ObMemtable *memtable = get_memtable_(i);
    if (NULL == memtable) {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "memtable must not null");
    } else if (0 == memtable->get_minor_merged_time()) {
      cnt++;
    }
  }

  return cnt;
}

void ObTabletMemtableMgr::clean_tail_memtable_()
{
  if (memtable_tail_ > memtable_head_) {
    ObMemtable *memtable = get_memtable_(memtable_tail_ - 1);
    if (OB_NOT_NULL(memtable)) {
      unlink_memtable_mgr_and_memtable_(memtable);
      memtable->set_frozen();
    } else {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "memtable is null when clean_tail_memtable_", KPC(this));
    }
    ObIMemtableMgr::release_tail_memtable();
  }
}

int ObTabletMemtableMgr::get_memtables_(ObTableHdlArray &handle, const int64_t start_point,
    const bool include_active_memtable)
{
  int ret = OB_SUCCESS;
  int64_t start_pos = memtable_head_;
  if (-1 != start_point) {
    if (OB_FAIL(find_start_pos_(start_point, start_pos))) {
      LOG_WARN("failed to find_start_pos_", K(ret), K(start_point));
    }
  }

  if (OB_SUCC(ret) && OB_FAIL(add_tables_(start_pos, include_active_memtable, handle))) {
    LOG_WARN("failed to add tables", K(ret), K(start_point), K(include_active_memtable));
  }
  return ret;
}

int ObTabletMemtableMgr::add_tables_(
    const int64_t start_pos,
    const bool include_active_memtable,
    ObTableHdlArray &handle)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(start_pos > -1)) {
    const int64_t last_pos = memtable_tail_ - 1;
    for (int64_t pos = start_pos; OB_SUCC(ret) && pos < last_pos; ++pos) {
      ObTableHandleV2 memtable_handle;
      if (OB_FAIL(get_ith_memtable(pos, memtable_handle))) {
        STORAGE_LOG(WARN, "fail to get ith memtable", K(ret), K(pos));
      } else if (OB_UNLIKELY(!memtable_handle.is_valid())) {
        ret = OB_ERR_SYS;
        LOG_ERROR("invalid memtable handle", K(ret), K(memtable_handle));
      } else if (OB_FAIL(handle.push_back(memtable_handle))) {
        LOG_WARN("failed to add memtable", K(ret), K(memtable_handle));
      }
    }
    if (OB_SUCC(ret) && memtable_tail_ > memtable_head_) {
      ObTableHandleV2 last_memtable_handle;
      if (OB_FAIL(get_ith_memtable(last_pos, last_memtable_handle))) {
        STORAGE_LOG(WARN, "fail to get ith memtable", K(ret), K(last_pos));
      } else if (OB_UNLIKELY(!last_memtable_handle.is_valid())) {
        ret = OB_ERR_SYS;
        LOG_ERROR("invalid memtable handle", K(ret), K(last_memtable_handle));
      } else if (include_active_memtable || last_memtable_handle.get_table()->is_frozen_memtable()) {
        if (OB_FAIL(handle.push_back(last_memtable_handle))) {
          LOG_WARN("failed to add last memtable to handle", K(ret), K(start_pos),
                   K(include_active_memtable), K(last_memtable_handle));
        }
      }
    }
  }
  return ret;
}

int ObTabletMemtableMgr::find_start_pos_(const int64_t start_point, int64_t &start_pos)
{
  int ret = OB_SUCCESS;
  start_pos = -1;
  if (OB_UNLIKELY(start_point < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid start_point", K(ret), K(start_point));
  }
  for (int64_t i = memtable_head_; OB_SUCC(ret) && i < memtable_tail_; ++i) {
    ObMemtable *memtable = get_memtable_(i);
    if (OB_ISNULL(memtable)) {
      ret = OB_ERR_SYS;
      LOG_ERROR("memtable must not null", K(ret));
    } else if (memtable->get_snapshot_version() > start_point) {
      start_pos = i;
      break;
    }
  }
  return ret;
}

int64_t ObTabletMemtableMgr::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
  } else {
    J_OBJ_START();
    J_NAME("ObIMemtableMgr");
    J_COLON();
    pos += ObIMemtableMgr::to_string(buf + pos, buf_len - pos);
    J_COMMA();
    J_KV(K_(schema_recorder));
    J_OBJ_END();
  }
  return pos;
}

int ObTabletMemtableMgr::find_start_pos_(const int64_t start_log_ts,
                                         const int64_t start_snapshot_version,
                                         int64_t &start_pos)
{
  int ret = OB_SUCCESS;
  start_pos = -1;
  if (OB_UNLIKELY(start_log_ts <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid start_snapshot_version", K(ret), K(start_snapshot_version), K(start_log_ts));
  }
  for (int64_t i = memtable_head_; OB_SUCC(ret) && i < memtable_tail_; ++i) {
    ObMemtable *memtable = get_memtable_(i);
    if (OB_ISNULL(memtable)) {
      ret = OB_ERR_SYS;
      LOG_ERROR("memtable must not null", K(ret));
    } else if (memtable->get_end_scn().get_val_for_tx() == start_log_ts) {
      if (memtable->get_snapshot_version() > start_snapshot_version) {
        start_pos = i;
        break;
      }
    } else if (memtable->get_end_scn().get_val_for_tx() > start_log_ts) {
      start_pos = i;
      break;
    }
  }
  return ret;
}

int ObTabletMemtableMgr::get_memtables_v2(
    ObTableHdlArray &handle,
    const int64_t start_log_ts,
    const int64_t start_snapshot_version,
    const bool reset_handle,
    const bool include_active_memtable)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else {
    MemMgrRLockGuard guard(lock_);
    if (reset_handle) {
      handle.reset();
    }
    int64_t start_pos = memtable_head_;
    if (0 < start_log_ts) {
      if (OB_FAIL(find_start_pos_(start_log_ts, start_snapshot_version, start_pos))) {
        LOG_WARN("failed to find_start_pos_", K(ret), K(start_log_ts), K(start_snapshot_version));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(add_tables_(start_pos, include_active_memtable, handle))) {
      LOG_WARN("failed to add_tables", K(ret), K(start_log_ts), K(start_snapshot_version),
               K(include_active_memtable), K(reset_handle));
    }
  }
  return ret;
}

int ObTabletMemtableMgr::get_first_frozen_memtable_(ObTableHandleV2 &handle) const
{
  int ret = OB_SUCCESS;

  for (int64_t i = memtable_head_; OB_SUCC(ret) && i < memtable_tail_; i++) {
    ObTableHandleV2 m_handle;
    const ObMemtable *memtable = nullptr;
    if (OB_FAIL(get_ith_memtable(i, m_handle))) {
      STORAGE_LOG(WARN, "fail to get ith memtable", K(ret), K(i));
    } else if (OB_UNLIKELY(!m_handle.is_valid())) {
      ret = OB_ERR_SYS;
      LOG_ERROR("memtable handle is invalid", K(ret), K(m_handle));
    } else if (OB_FAIL(m_handle.get_data_memtable(memtable))) {
      LOG_WARN("fail to get memtable", K(ret), K(m_handle));
    } else if (OB_ISNULL(memtable)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("memtable is nullptr", K(ret), K(m_handle));
    } else if (memtable->is_frozen_memtable()) {
      handle = m_handle;
      break;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!handle.is_valid()) {
    ret = OB_ENTRY_NOT_EXIST;
  }

  return ret;
}

int ObTabletMemtableMgr::set_frozen_for_all_memtables()
{
  int ret = OB_SUCCESS;
  MemMgrWLockGuard lock_guard(lock_);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_ISNULL(ls_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ls is null", K(ret));
  } else {
    const share::ObLSID &ls_id = ls_->get_ls_id();
    for (int64_t i = memtable_head_; OB_SUCC(ret) && i < memtable_tail_; ++i) {
      // memtable that cannot be released will block memtables behind it
      ObMemtable *memtable = static_cast<memtable::ObMemtable *>(tables_[get_memtable_idx(i)]);
      if (OB_ISNULL(memtable)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "memtable is nullptr", K(ret), K(ls_id), KP(memtable), K(i));
      } else {
        STORAGE_LOG(INFO, "set frozen for offline", K(ls_id), K(i), KPC(memtable));
        memtable->set_frozen();
      }
    }
  }

  return ret;
}

}  // namespace storage
}  // namespace oceanbase
