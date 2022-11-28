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

#include "storage/ob_i_memtable_mgr.h"
#include "share/ob_task_define.h"
#include "storage/ls/ob_freezer.h"
#include "storage/memtable/ob_memtable_interface.h"
#include "storage/memtable/ob_multi_source_data.h"
#include "storage/tablet/ob_tablet.h"

using namespace oceanbase::share;
using namespace oceanbase::memtable;

namespace oceanbase
{
namespace storage
{

int ObIMemtableMgr::add_memtable_(ObTableHandleV2 &memtable_handle)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!memtable_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "memtable is null, unexpected error", KR(ret), K(memtable_handle), KPC(this));
  } else {
    const int64_t idx = get_memtable_idx_(memtable_tail_);
    memtable_tail_++;
    memtables_[idx] = memtable_handle;
    ObTaskController::get().allow_next_syslog();
  }

  return ret;
}

int ObIMemtableMgr::get_active_memtable(ObTableHandleV2 &handle) const
{
  int ret = OB_SUCCESS;
  TCRLockGuard lock_guard(lock_);
  if (memtable_tail_ <= 0) {
    ret = OB_ENTRY_NOT_EXIST;
    STORAGE_LOG(WARN, "There is no memtable in ObLockMemtableMgr.");
  } else {
    handle = memtables_[get_memtable_idx_(memtable_tail_ - 1)];

    if (OB_UNLIKELY(!handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "get invalid table handle", K(ret), K(handle), K(memtable_head_), K(memtable_tail_));
    }
  }
  return ret;
}

int ObIMemtableMgr::get_first_memtable(ObTableHandleV2 &handle) const
{
  int ret = OB_SUCCESS;
  TCRLockGuard lock_guard(lock_);
  if (memtable_head_ == memtable_tail_) {
    ret = OB_ENTRY_NOT_EXIST;
    STORAGE_LOG(WARN, "There is no memtable in ObLockMemtableMgr.");
  } else {
    handle = memtables_[get_memtable_idx_(memtable_head_)];
  }
  return ret;
}

int ObIMemtableMgr::get_all_memtables(ObTableHdlArray &handles)
{
  int ret = OB_SUCCESS;
  // TODO(handora.qc): oblatchid
  TCRLockGuard lock_guard(lock_);
  for (int i = memtable_head_; OB_SUCC(ret) && i < memtable_tail_; i++) {
    if (OB_FAIL(handles.push_back(memtables_[get_memtable_idx_(i)]))) {
      STORAGE_LOG(WARN, "push back into handles failed.", K(ret));
    }
  }
  return ret;
}

int ObIMemtableMgr::get_newest_clog_checkpoint_scn(palf::SCN &clog_checkpoint_scn)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_ISNULL(freezer_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "freezer should not be null", K(ret), K(tablet_id_));
  } else if (OB_FAIL(freezer_->get_newest_clog_checkpoint_scn(tablet_id_,
                                                              clog_checkpoint_scn))) {
    STORAGE_LOG(WARN, "fail to get newest clog_checkpoint_ts", K(ret), K(tablet_id_));
  }

  return ret;
}

int ObIMemtableMgr::get_newest_snapshot_version(palf::SCN &snapshot_version)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_ISNULL(freezer_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "freezer should not be null", K(ret), K(tablet_id_));
  } else if (OB_FAIL(freezer_->get_newest_snapshot_version(tablet_id_, snapshot_version))) {
    STORAGE_LOG(WARN, "fail to get newest snapshot_version", K(ret), K(tablet_id_));
  }

  return ret;
}

int ObIMemtableMgr::release_memtables(const int64_t log_ts)
{
  TCWLockGuard lock_guard(lock_);
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_UNLIKELY(log_ts < 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid log ts", K(ret), K(log_ts));
  } else {
    for (int64_t i = memtable_head_; OB_SUCC(ret) && i < memtable_tail_; ++i) {
      // memtable that cannot be released will block memtables behind it
      ObIMemtable *memtable = nullptr;
      if (OB_FAIL(memtables_[get_memtable_idx_(i)].get_memtable(memtable))) {
        STORAGE_LOG(WARN, "fail to get memtable", K(ret));
      } else {
        if (memtable->is_data_memtable()
            && memtable->is_empty()
            && !memtable->get_is_force_freeze()) {
          break;
        } else if (memtable->get_end_log_ts() <= log_ts
            && memtable->can_be_minor_merged()) {
          if (OB_FAIL(release_head_memtable_(memtable))) {
            STORAGE_LOG(WARN, "fail to release memtable", K(ret), KPC(memtable));
            break;
          } else {
            STORAGE_LOG(INFO, "succeed to release memtable", K(ret), K(i), K(log_ts));
          }
        } else {
          break;
        }
      }
    }
  }

  return ret;
}

int ObIMemtableMgr::release_memtables()
{
  int ret = OB_SUCCESS;
  const bool force_release = true;
  TCWLockGuard lock_guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else {
    for (int64_t i = memtable_head_; OB_SUCC(ret) && i < memtable_tail_; ++i) {
      // memtable that cannot be released will block memtables behind it
      ObIMemtable *memtable = nullptr;
      if (OB_FAIL(memtables_[get_memtable_idx_(i)].get_memtable(memtable))) {
        STORAGE_LOG(WARN, "fail to get memtable", K(ret), K(i), K(memtable_head_),
                    K(memtable_tail_), K(memtables_[get_memtable_idx_(i)]));
      } else {
        STORAGE_LOG(INFO, "force release memtable", K(i), K(*memtable));
        if (OB_FAIL(release_head_memtable_(memtable, force_release))) {
          STORAGE_LOG(WARN, "fail to release memtable", K(ret), K(i));
          break;
        } else {
          STORAGE_LOG(INFO, "succeed to release memtable", K(ret), K(i), K(*memtable), KP(this));
        }
      }
    }
  }

  return ret;
}

int ObIMemtableMgr::get_multi_source_data_unit(
    ObIMultiSourceDataUnit *const multi_source_data_unit,
    ObIAllocator *allocator) const
{
  UNUSED(multi_source_data_unit);
  UNUSED(allocator);
  int ret = OB_NOT_SUPPORTED;
  return ret;
}

int ObIMemtableMgr::get_memtable_for_multi_source_data_unit(
    memtable::ObMemtable *&memtable,
    const memtable::MultiSourceDataUnitType type) const
{
  UNUSED(memtable);
  UNUSED(type);
  int ret = OB_NOT_SUPPORTED;
  return ret;
}

int ObIMemtableMgr::init(
    const ObTabletID &tablet_id,
    const share::ObLSID &ls_id,
    const int64_t max_saved_schema_version,
    logservice::ObLogHandler *log_handler,
    ObFreezer *freezer,
    ObTenantMetaMemMgr *t3m,
    ObTabletDDLKvMgr *ddl_kv_mgr)
{
  int ret = OB_SUCCESS;
  if (!tablet_id.is_special_merge_tablet()
      && OB_FAIL(init_storage_schema_recorder(tablet_id, ls_id, max_saved_schema_version, log_handler))) {
    TRANS_LOG(WARN, "failed to init schema recorder", K(ret), K(max_saved_schema_version), KP(log_handler));
  } else {
    ret = init(tablet_id, ls_id, freezer, t3m, ddl_kv_mgr);
  }
  return ret;
}

ObMemtableMgrHandle::ObMemtableMgrHandle()
  : memtable_mgr_(nullptr),
    pool_(nullptr)
{
}

ObMemtableMgrHandle::ObMemtableMgrHandle(ObIMemtableMgr *memtable_mgr, ObITenantMetaObjPool *pool)
  : memtable_mgr_(memtable_mgr),
    pool_(pool)
{
}

ObMemtableMgrHandle::~ObMemtableMgrHandle()
{
  reset();
}

bool ObMemtableMgrHandle::is_valid() const
{
  return nullptr != memtable_mgr_;
}

void ObMemtableMgrHandle::reset()
{
  if (nullptr != memtable_mgr_) {
    if (nullptr == pool_) {
      STORAGE_LOG(DEBUG, "this memory manager is a special handle", KPC(memtable_mgr_));
      // at present, inner tablet's memtable_mgr_ is not managed by pool,
      // just decrease ref and leave the release to the owner of memtable_mgr.
      memtable_mgr_->dec_ref();
      memtable_mgr_ = nullptr;
    } else {
      if (0 == memtable_mgr_->dec_ref()) {
        pool_->free_obj(static_cast<void *>(memtable_mgr_));
      }
      memtable_mgr_ = nullptr;
      pool_ = nullptr;
    }
  }
}

ObMemtableMgrHandle::ObMemtableMgrHandle(const ObMemtableMgrHandle &other)
  : memtable_mgr_(nullptr),
    pool_(nullptr)
{
  *this = other;
}

ObMemtableMgrHandle &ObMemtableMgrHandle::operator= (const ObMemtableMgrHandle &other)
{
  reset();
  pool_ = other.pool_;
  if (nullptr != other.memtable_mgr_) {
    memtable_mgr_ = other.memtable_mgr_;
    other.memtable_mgr_->inc_ref();
  }
  return *this;
}

int ObMemtableMgrHandle::set_memtable_mgr(ObIMemtableMgr *memtable_mgr, ObITenantMetaObjPool *pool)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_ISNULL(memtable_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(memtable_mgr), KP(pool));
  } else {
    pool_ = pool;
    memtable_mgr_ = memtable_mgr;
    memtable_mgr_->inc_ref();
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
