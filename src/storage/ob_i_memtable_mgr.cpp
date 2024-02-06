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
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"

using namespace oceanbase::share;
using namespace oceanbase::memtable;

namespace oceanbase
{
namespace storage
{

int ObIMemtableMgr::get_active_memtable(ObTableHandleV2 &handle) const
{
  int ret = OB_SUCCESS;
  MemMgrRLockGuard lock_guard(lock_);
  if (OB_UNLIKELY(memtable_tail_ < memtable_head_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected error, tail < head", K(ret), K(memtable_tail_), K(memtable_head_));
  } else if (OB_UNLIKELY(memtable_tail_ == memtable_head_)) {
    ret = OB_ENTRY_NOT_EXIST;
    STORAGE_LOG(WARN, "There is no memtable in MemtableMgr", K(ret), K(memtable_head_), K(memtable_tail_));
  } else if (OB_FAIL(get_ith_memtable(memtable_tail_ - 1, handle))) {
    STORAGE_LOG(WARN, "fail to get ith memtable", K(ret), K(memtable_tail_));
  } else if (OB_UNLIKELY(!handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "get invalid table handle", K(ret), K(handle));
  }
  return ret;
}

int ObIMemtableMgr::get_first_nonempty_memtable(ObTableHandleV2 &handle) const
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  MemMgrRLockGuard lock_guard(lock_);

  for (int64_t i = memtable_head_; OB_SUCC(ret) && i < memtable_tail_; ++i) {
    ObTableHandleV2 tmp_handle;
    memtable::ObMemtable *mt = NULL;
    if (OB_FAIL(get_ith_memtable(i, tmp_handle))) {
      STORAGE_LOG(WARN, "fail to get ith memtable", KR(ret), K(i));
    } else if (OB_UNLIKELY(!tmp_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "get invalid tmp table handle", KR(ret), K(i), K(tmp_handle));
    } else if (OB_FAIL(tmp_handle.get_data_memtable(mt))) {
      STORAGE_LOG(WARN, "failed to get_data_memtable", KR(ret), K(i), K(tmp_handle));
    } else if (OB_ISNULL(mt)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "mt is NULL", KR(ret), K(i), K(tmp_handle));
    } else if (mt->get_rec_scn().is_max()) {
    } else if (OB_FAIL(get_ith_memtable(i, handle))) {
      STORAGE_LOG(WARN, "fail to get ith memtable", KR(ret), K(i));
    } else if (OB_UNLIKELY(!handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "get invalid table handle", KR(ret), K(i), K(handle));
    } else {
      is_exist = true;
      break;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!is_exist) {
    ret = OB_ENTRY_NOT_EXIST;
  }


  return ret;
}

int ObIMemtableMgr::get_all_memtables(ObTableHdlArray &handles)
{
  int ret = OB_SUCCESS;
  // TODO(handora.qc): oblatchid
  MemMgrRLockGuard lock_guard(lock_);
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

int ObIMemtableMgr::get_newest_clog_checkpoint_scn(SCN &clog_checkpoint_scn)
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

int ObIMemtableMgr::get_newest_snapshot_version(SCN &snapshot_version)
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

int ObIMemtableMgr::release_memtables(const SCN &scn)
{
  MemMgrWLockGuard lock_guard(lock_);
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_UNLIKELY(!scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid log ts", K(ret), K(scn));
  } else {
    for (int64_t i = memtable_head_; OB_SUCC(ret) && i < memtable_tail_; ++i) {
      // memtable that cannot be released will block memtables behind it
      ObIMemtable *memtable = tables_[get_memtable_idx(i)];
      if (OB_ISNULL(memtable)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "memtable is nullptr", K(ret), KP(memtable), K(i));
      } else {
        if (memtable->get_end_scn() <= scn
            && memtable->can_be_minor_merged()) {
          if (OB_FAIL(release_head_memtable_(memtable))) {
            STORAGE_LOG(WARN, "fail to release memtable", K(ret), KPC(memtable));
            break;
          } else {
            STORAGE_LOG(INFO, "succeed to release memtable", K(ret), K(i), K(scn));
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
  MemMgrWLockGuard lock_guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else {
    for (int64_t i = memtable_head_; OB_SUCC(ret) && i < memtable_tail_; ++i) {
      // memtable that cannot be released will block memtables behind it
      ObIMemtable *memtable = tables_[get_memtable_idx(i)];
      if (OB_ISNULL(memtable)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "memtable is nullptr", K(ret), KP(memtable), K(i));
      } else {
        STORAGE_LOG(INFO, "force release memtable", K(i), K(*memtable));
        if (OB_FAIL(release_head_memtable_(memtable, force_release))) {
          STORAGE_LOG(WARN, "fail to release memtable", K(ret), K(i));
          break;
        } else {
          // NOTICE: the memtable may live longer than tablet.
          // And the ref of memtable mgr may be the last one.
          STORAGE_LOG(INFO, "succeed to release memtable", K(ret), K(i), KP(memtable), KP(this));
        }
      }
    }
  }

  return ret;
}

int ObIMemtableMgr::init(
    const ObTabletID &tablet_id,
    const share::ObLSID &ls_id,
    const int64_t max_saved_schema_version,
    const int64_t max_saved_medium_scn,
    const lib::Worker::CompatMode compat_mode,
    logservice::ObLogHandler *log_handler,
    ObFreezer *freezer,
    ObTenantMetaMemMgr *t3m)
{
  int ret = OB_SUCCESS;
  if (!tablet_id.is_special_merge_tablet()
      && OB_FAIL(init_storage_recorder(tablet_id, ls_id, max_saved_schema_version, max_saved_medium_scn, compat_mode, log_handler))) {
    TRANS_LOG(WARN, "failed to init schema recorder", K(ret), K(max_saved_schema_version), K(max_saved_medium_scn), K(compat_mode), KP(log_handler));
  } else {
    ret = init(tablet_id, ls_id, freezer, t3m);
  }
  return ret;
}

void ObIMemtableMgr::reset_tables()
{
  if (OB_NOT_NULL(t3m_)) {
    for (int64_t pos = memtable_head_; pos < memtable_tail_; ++pos) {
      memtable::ObIMemtable *memtable = tables_[get_memtable_idx(pos)];
      const int64_t ref_cnt = memtable->dec_ref();
      if (0 == ref_cnt) {
        t3m_->push_table_into_gc_queue(memtable, table_type_);
      }
    }
  }
  memset(tables_, 0, sizeof(tables_));
  memtable_head_ = 0;
  memtable_tail_ = 0;
  t3m_ = nullptr;
  table_type_ = ObITable::TableType::MAX_TABLE_TYPE;
}

void ObIMemtableMgr::release_head_memtable()
{
  if (OB_ISNULL(t3m_)) {
    STORAGE_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "t3m is nullptr", KP_(t3m));
  } else {
    memtable::ObIMemtable *memtable = tables_[get_memtable_idx(memtable_head_)];
    tables_[get_memtable_idx(memtable_head_)] = nullptr;
    ++memtable_head_;
    const int64_t ref_cnt = memtable->dec_ref();
    if (0 == ref_cnt) {
      t3m_->push_table_into_gc_queue(memtable, table_type_);
    }
  }
}

void ObIMemtableMgr::release_tail_memtable()
{
  if (memtable_tail_ > memtable_head_) {
    if (OB_ISNULL(t3m_)) {
      STORAGE_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "t3m is nullptr", KP_(t3m));
    } else {
      memtable::ObIMemtable *memtable = tables_[get_memtable_idx(memtable_tail_ - 1)];
      tables_[get_memtable_idx(memtable_tail_ - 1)] = nullptr;
      --memtable_tail_;
      const int64_t ref_cnt = memtable->dec_ref();
      if (0 == ref_cnt) {
        t3m_->push_table_into_gc_queue(memtable, table_type_);
      }
    }
  }
}

int64_t ObIMemtableMgr::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
    // do nothing
  } else {
    J_OBJ_START();
    J_NAME("Memtables");
    J_COLON();
    J_KV(KP(this),
         K_(ref_cnt),
         K_(is_inited),
         K_(tablet_id),
         KP_(freezer),
         K_(table_type),
         K_(memtable_head),
         K_(memtable_tail),
         KP_(t3m));
    J_COMMA();
    BUF_PRINTF("tables");
    J_COLON();
    J_ARRAY_START();
    if (memtable_tail_ - memtable_head_ > 0) {
      for (int64_t i = 0; i < MAX_MEMSTORE_CNT; ++i) {
        if (i > 0) {
          J_COMMA();
        }
        BUF_PRINTO(OB_P(tables_[get_memtable_idx(i)]));
      }
    } else {
      J_EMPTY_OBJ();
    }
    J_ARRAY_END();
    J_OBJ_END();
  }
  return pos;
}

int ObIMemtableMgr::add_memtable_(ObTableHandleV2 &memtable_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == t3m_ || !ObITable::is_memtable(table_type_))) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "Don't initialize memtable array", K(ret), KP(t3m_), K(table_type_));
  } else if (OB_UNLIKELY(!memtable_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(memtable_handle));
  } else {
    const int64_t idx = get_memtable_idx(memtable_tail_);
    if (OB_FAIL(memtable_handle.get_memtable(tables_[idx]))) {
      STORAGE_LOG(WARN, "fail to get memtable", K(ret), K(memtable_handle));
    } else {
      tables_[idx]->inc_ref();
      memtable_tail_++;
    }
  }
  return ret;
}

int ObIMemtableMgr::get_ith_memtable(const int64_t pos, ObTableHandleV2 &handle) const
{
  return handle.set_table(static_cast<ObITable *const>(tables_[get_memtable_idx(pos)]), t3m_, table_type_);
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
      STORAGE_LOG(DEBUG, "this memory manager is a special handle", KP(memtable_mgr_), "ref_cnt",
          memtable_mgr_->get_ref(), K(lbt()));
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
