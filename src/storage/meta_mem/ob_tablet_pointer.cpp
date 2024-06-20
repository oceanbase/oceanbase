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

#include "lib/ob_errno.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/container/ob_se_array.h"
#include "lib/hash/ob_cuckoo_hashmap.h"
#include "lib/stat/ob_latch_define.h"
#include "common/ob_tablet_id.h"
#include "share/ob_ls_id.h"
#include "storage/ls/ob_ls.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_persister.h"
#include "storage/tablet/ob_tablet_common.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/meta_mem/ob_tablet_pointer.h"
#include "storage/ddl/ob_tablet_ddl_kv_mgr.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/slog_ckpt/ob_tenant_checkpoint_slog_handler.h"

#define USING_LOG_PREFIX STORAGE

using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::share;

namespace oceanbase
{
namespace storage
{
ObTabletPointer::ObTabletPointer()
  : phy_addr_(),
    obj_(),
    ls_handle_(),
    ddl_kv_mgr_handle_(),
    protected_memtable_mgr_handle_(),
    ddl_info_(),
    initial_state_(true),
    ddl_kv_mgr_lock_(),
    mds_table_handler_(),
    old_version_chain_(nullptr),
    attr_()
{
#if defined(__x86_64__) && !defined(ENABLE_OBJ_LEAK_CHECK)
  static_assert(sizeof(ObTabletPointer) == 320, "The size of ObTabletPointer will affect the meta memory manager, and the necessity of adding new fields needs to be considered.");
#endif
}

ObTabletPointer::ObTabletPointer(
    const ObLSHandle &ls_handle,
    const ObMemtableMgrHandle &memtable_mgr_handle)
  : phy_addr_(),
    obj_(),
    ls_handle_(ls_handle),
    protected_memtable_mgr_handle_(memtable_mgr_handle),
    ddl_info_(),
    initial_state_(true),
    ddl_kv_mgr_lock_(),
    mds_table_handler_(),
    old_version_chain_(nullptr)
{
}

ObTabletPointer::~ObTabletPointer()
{
  reset();
}

void ObTabletPointer::reset()
{
  {
    ObByteLockGuard guard(ddl_kv_mgr_lock_);
    ddl_kv_mgr_handle_.reset();
  }
  mds_table_handler_.reset();
  protected_memtable_mgr_handle_.reset();
  ddl_info_.reset();
  ATOMIC_STORE(&initial_state_, true);
  old_version_chain_ = nullptr;
  reset_obj();
  phy_addr_.reset();
  ls_handle_.reset();
}

void ObTabletPointer::reset_obj()
{
  if (nullptr != obj_.ptr_) {
    if (OB_UNLIKELY(nullptr == obj_.pool_ && nullptr == obj_.allocator_)) {
      STORAGE_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "object pool is nullptr", K_(obj));
      ob_abort();
    } else {
      const int64_t ref_cnt = obj_.ptr_->dec_ref();
      if (0 == ref_cnt) {
        if (nullptr != obj_.pool_) {
          obj_.pool_->free_obj(obj_.ptr_);
        } else {
          obj_.ptr_->~ObTablet();
          obj_.allocator_->free(obj_.ptr_);
        }
      } else if (OB_UNLIKELY(ref_cnt < 0)) {
        STORAGE_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "obj ref cnt may be leaked", K(ref_cnt), KPC(this));
      }
      // The pool ptr on tablet pointer cann't be reset nullptr here. Otherwise, you will
      // encounter the following bug when the tablet is deleted from the map.
      //
      // Bug timeline:
      //  - Thread 1 load tablet from meta.
      //  - Thread 2 remove tablet from map.
      //  - Thread 1 fail to hook loaded tablet into pointer.
      //  - Thread 1 rolls back and releases the tablet and encounters an error.
      obj_.ptr_ = nullptr;
      obj_.allocator_ = nullptr;
    }
  }
}

int ObTabletPointer::read_from_disk(
    const bool is_full_load,
    common::ObArenaAllocator &allocator,
    char *&r_buf,
    int64_t &r_len,
    ObMetaDiskAddr &addr)
{
  int ret = OB_SUCCESS;
  const int64_t buf_len = phy_addr_.size();
  const ObMemAttr mem_attr(MTL_ID(), "MetaPointer");
  ObTenantCheckpointSlogHandler *ckpt_slog_hanlder = MTL(ObTenantCheckpointSlogHandler*);
  if (OB_ISNULL(ckpt_slog_hanlder)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "slog handler is nullptr", K(ret), KP(ckpt_slog_hanlder));
  } else {
    ObMetaDiskAddr real_load_addr = phy_addr_;
    if (!is_full_load && addr.is_raw_block()) {
      if (phy_addr_.size() > ObTabletCommon::MAX_TABLET_FIRST_LEVEL_META_SIZE) {
        real_load_addr.set_size(ObTabletCommon::MAX_TABLET_FIRST_LEVEL_META_SIZE);
      }
    }
    if (OB_FAIL(ckpt_slog_hanlder->read_from_disk(phy_addr_, allocator, r_buf, r_len))) {
      if (OB_SEARCH_NOT_FOUND != ret) {
        STORAGE_LOG(WARN, "fail to read from addr", K(ret), K(phy_addr_));
      }
    } else {
      addr = phy_addr_;
    }
  }
  return ret;
}

int ObTabletPointer::hook_obj(const ObTabletAttr &attr, ObTablet *&t,  ObMetaObjGuard<ObTablet> &guard)
{
  int ret = OB_SUCCESS;
  guard.reset();

  if (OB_ISNULL(t)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "load null obj from disk", K(ret), K(phy_addr_));
  } else if (OB_NOT_NULL(obj_.ptr_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "obj already hooked", K(ret), K(phy_addr_), KP(t), KP(obj_.ptr_));
  } else if (OB_UNLIKELY(0 != t->get_ref())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "obj ref cnt not 0", K(ret), K(phy_addr_), K(t->get_ref()));
  } else {
    t->inc_ref();
    t->set_tablet_addr(phy_addr_);
    obj_.ptr_ = t;
    guard.set_obj(obj_);
    ObMetaObjBufferHelper::set_in_map(reinterpret_cast<char *>(t), true/*in_map*/);
    if (!is_attr_valid() && OB_FAIL(set_tablet_attr(attr))) { // only set tablet attr when first hook obj
      STORAGE_LOG(WARN, "failed to update tablet attr", K(ret), K(guard));
    }
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(t)) {
    obj_.pool_->free_obj(t);
    obj_.ptr_ = nullptr;
    t = nullptr;
  }

  return ret;
}

int ObTabletPointer::get_in_memory_obj(ObMetaObjGuard<ObTablet> &guard)
{
  int ret = OB_SUCCESS;
  guard.reset();

  if (OB_UNLIKELY(phy_addr_.is_none())) {
    ret = OB_ITEM_NOT_SETTED;
    STORAGE_LOG(DEBUG, "meta disk addr is none, no object to be got", K(ret), K(phy_addr_));
  } else if (OB_UNLIKELY(!is_in_memory())) {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(ERROR, "object isn't in memory, not support", K(ret), K(phy_addr_));
  } else {
    guard.set_obj(obj_);
  }
  return ret;
}

void ObTabletPointer::get_obj(ObMetaObjGuard<ObTablet> &guard)
{
  guard.set_obj(obj_);
}

bool ObTabletPointer::is_in_memory() const
{
  return nullptr != obj_.ptr_;
}

void ObTabletPointer::set_obj_pool(ObITenantMetaObjPool &obj_pool)
{
  obj_.pool_ = &obj_pool;
}

void ObTabletPointer::set_addr_without_reset_obj(const ObMetaDiskAddr &addr)
{
  phy_addr_ = addr;
}

void ObTabletPointer::set_addr_with_reset_obj(const ObMetaDiskAddr &addr)
{
  reset_obj();
  phy_addr_ = addr;
}

void ObTabletPointer::set_obj(const ObMetaObjGuard<ObTablet> &guard)
{
  reset_obj();
  guard.get_obj(obj_);
  get_attr_for_obj(obj_.ptr_);
  if (nullptr != obj_.ptr_) {
    if (OB_UNLIKELY(nullptr == obj_.pool_ && nullptr == obj_.allocator_)) {
      STORAGE_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "object pool is nullptr", K_(obj));
      ob_abort();
    } else {
      obj_.ptr_->inc_ref();
    }
  }
}

int ObTabletPointer::deserialize(
    common::ObArenaAllocator &allocator,
    const char *buf,
    const int64_t buf_len,
    ObTablet *t)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_UNLIKELY(buf_len <= 0) || OB_ISNULL(buf) || OB_ISNULL(t)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(buf), K(buf_len), KP(t));
  } else if (OB_FAIL(get_attr_for_obj(t))) {
    STORAGE_LOG(WARN, "fail to set attr for obj", K(ret));
  } else if (OB_FAIL(t->load_deserialize(allocator, buf, buf_len, pos))) {
    STORAGE_LOG(WARN, "fail to de-serialize T", K(ret), KP(buf), K(buf_len), KP(t));
  }
  return ret;
}

int ObTabletPointer::deserialize(
    const char *buf,
    const int64_t buf_len,
    ObTablet *t)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_UNLIKELY(buf_len <= 0) || OB_ISNULL(buf) || OB_ISNULL(t)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(buf), K(buf_len), KP(t));
  } else if (OB_FAIL(get_attr_for_obj(t))) {
    STORAGE_LOG(WARN, "fail to set attr for obj", K(ret));
  } else if (OB_FAIL(t->deserialize(buf, buf_len, pos))) {
    STORAGE_LOG(WARN, "fail to de-serialize T", K(ret), KP(buf), K(buf_len), KP(t));
  }
  return ret;
}

int ObTabletPointer::get_attr_for_obj(ObTablet *tablet)
{
  int ret = OB_SUCCESS;
  logservice::ObLogHandler *log_handler = nullptr;

  if (OB_ISNULL(tablet)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet is null", K(ret), KP(tablet));
  } else if (OB_UNLIKELY(!ls_handle_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls handle is invalid", K(ret), K_(ls_handle));
  } else if (OB_ISNULL(log_handler = ls_handle_.get_ls()->get_log_handler())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log handler is null", K(ret), KP(log_handler));
  } else {
    tablet->log_handler_ = log_handler;
  }

  return ret;
}

int ObTabletPointer::dump_meta_obj(ObMetaObjGuard<ObTablet> &guard, void *&free_obj)
{
  int ret = OB_SUCCESS;
  ObMetaObj<ObTablet> meta_obj;
  if (OB_ISNULL(obj_.ptr_)) {
    LOG_INFO("tablet may be washed", KPC(obj_.ptr_));
  } else if (OB_UNLIKELY(obj_.ptr_->get_ref() > 1)) {
    LOG_INFO("tablet may be attached again, continue", KPC(obj_.ptr_));
  } else if (OB_UNLIKELY(obj_.ptr_->get_ref() < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, tablet ref is less than 1", K(ret), KPC(obj_.ptr_));
  } else if (OB_ISNULL(obj_.pool_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("obj is not allocated from pool", K(ret), K(*this));
  } else if (OB_UNLIKELY(!phy_addr_.is_disked())) {
    LOG_INFO("tablet may be removed, and created again, continue", K(phy_addr_));
  } else if (0 != obj_.ptr_->dec_ref()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("obj is still being used", K(ret), K(*this));
  } else {
    const ObLSID ls_id = obj_.ptr_->tablet_meta_.ls_id_;
    const ObTabletID tablet_id = obj_.ptr_->tablet_meta_.tablet_id_;
    const int64_t wash_score = obj_.ptr_->get_wash_score();
    guard.get_obj(meta_obj);
    ObTablet *tmp_obj = obj_.ptr_;
    if (OB_NOT_NULL(meta_obj.ptr_) && obj_.ptr_->get_try_cache_size() <= ObTenantMetaMemMgr::NORMAL_TABLET_POOL_SIZE) {
      char *buf = reinterpret_cast<char*>(meta_obj.ptr_);
      const int64_t buf_len = ObMetaObjBufferHelper::get_buffer_header(buf).buf_len_;
      const int64_t cur_buf_len = ObMetaObjBufferHelper::get_buffer_header(reinterpret_cast<char*>(tmp_obj)).buf_len_;
      if (OB_UNLIKELY(cur_buf_len != ObTenantMetaMemMgr::LARGE_TABLET_POOL_SIZE
            || buf_len != ObTenantMetaMemMgr::NORMAL_TABLET_POOL_SIZE)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid tablet buffer length", K(ret), K(cur_buf_len), K(buf_len), KP(tmp_obj), KP(meta_obj.ptr_));
      } else if (OB_FAIL(get_attr_for_obj(meta_obj.ptr_))) {
        LOG_WARN("fail to set attr for object", K(ret), K(meta_obj));
      } else if (OB_FAIL(ObTabletPersister::transform_tablet_memory_footprint(*obj_.ptr_, buf, buf_len))) {
        LOG_WARN("fail to degrade tablet memory", K(ret), KPC(obj_.ptr_), KP(buf), K(buf_len));
      } else {
        meta_obj.ptr_->inc_ref();
        obj_ = meta_obj;
      }
    } else {
      obj_.ptr_ = nullptr;
    }
    if (OB_FAIL(ret)) {
      obj_.ptr_->inc_ref();
    } else {
      ObMetaObjBufferHelper::del_meta_obj(tmp_obj);
      free_obj = ObMetaObjBufferHelper::get_meta_obj_buffer_ptr(reinterpret_cast<char*>(tmp_obj));
      FLOG_INFO("wash one tablet",K(ret), KP(free_obj), K(ls_id), K(tablet_id), K(wash_score), K(obj_), K(meta_obj), K(phy_addr_));
    }
  }
  return ret;
}

int64_t ObTabletPointer::get_deep_copy_size() const
{
  return sizeof(ObTabletPointer);
}

int ObTabletPointer::deep_copy(char *buf, const int64_t buf_len, ObTabletPointer *&value) const
{
  int ret = OB_SUCCESS;
  const int64_t deep_copy_size = get_deep_copy_size();
  if (OB_ISNULL(buf) || buf_len < deep_copy_size) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_len), K(deep_copy_size));
  } else {
    ObTabletPointer *pvalue = new (buf) ObTabletPointer();
    pvalue->phy_addr_ = phy_addr_;
    pvalue->obj_.pool_ = obj_.pool_;
    pvalue->obj_.ptr_  = obj_.ptr_;
    pvalue->obj_.allocator_ = obj_.allocator_;
    if (nullptr != obj_.ptr_) {
      if (OB_UNLIKELY(nullptr == obj_.pool_ && nullptr == obj_.allocator_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("object pool is nullptr", K(ret), K_(obj));
        ob_abort();
      } else {
        obj_.ptr_->inc_ref();
      }
    }

    if (OB_SUCC(ret)) {
      pvalue->ls_handle_ = ls_handle_;
      pvalue->ddl_kv_mgr_handle_ = ddl_kv_mgr_handle_;
      pvalue->protected_memtable_mgr_handle_ = protected_memtable_mgr_handle_;
      pvalue->ddl_info_ = ddl_info_;
      pvalue->initial_state_ = initial_state_;
      pvalue->mds_table_handler_ = mds_table_handler_;// src ObTabletPointer will destroy soon
      pvalue->old_version_chain_ = old_version_chain_;
      value = pvalue;
      // NOTICE: cond and rw lock cannot be copied
    } else {
      pvalue->~ObTabletPointer();
    }
  }
  return ret;
}

bool ObTabletPointer::get_initial_state() const
{
  return ATOMIC_LOAD(&initial_state_);
}

void ObTabletPointer::set_initial_state(const bool initial_state)
{
  ATOMIC_STORE(&initial_state_, initial_state);
}

int ObTabletPointer::create_ddl_kv_mgr(
    const share::ObLSID &ls_id,
    const ObTabletID &tablet_id,
    ObDDLKvMgrHandle &ddl_kv_mgr_handle)
{
  int ret = OB_SUCCESS;
  ddl_kv_mgr_handle.reset();
  if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(tablet_id));
  } else {
    ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
    ObByteLockGuard guard(ddl_kv_mgr_lock_);
    if (ddl_kv_mgr_handle_.is_valid()) {
      // do nothing
    } else {
      ObDDLKvMgrHandle tmp_handle;
      if (OB_FAIL(t3m->acquire_tablet_ddl_kv_mgr(tmp_handle))) {
        LOG_WARN("fail to acquire tablet ddl kv mgr", K(ret));
      } else if (OB_FAIL(tmp_handle.get_obj()->init(ls_id, tablet_id))) {
        LOG_WARN("init ddl kv mgr failed", K(ret), K(ls_id),K(tablet_id));
      } else {
        ddl_kv_mgr_handle_ = tmp_handle;
      }
    }
    if (OB_SUCC(ret)) {
      ddl_kv_mgr_handle = ddl_kv_mgr_handle_;
    }
  }
  return ret;
}

void ObTabletPointer::get_ddl_kv_mgr(ObDDLKvMgrHandle &ddl_kv_mgr_handle)
{
  ObByteLockGuard guard(ddl_kv_mgr_lock_);
  ddl_kv_mgr_handle = ddl_kv_mgr_handle_;
}

int ObTabletPointer::set_ddl_kv_mgr(const ObDDLKvMgrHandle &ddl_kv_mgr_handle)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(!ddl_kv_mgr_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ddl_kv_mgr_handle));
  } else {
    ObByteLockGuard guard(ddl_kv_mgr_lock_);
    if (ddl_kv_mgr_handle_.get_obj() != ddl_kv_mgr_handle.get_obj()) {
      LOG_INFO("ddl kv mgr changed", KPC(ddl_kv_mgr_handle_.get_obj()));
    }
    ddl_kv_mgr_handle_ = ddl_kv_mgr_handle;
  }
  return ret;
}

int ObTabletPointer::remove_ddl_kv_mgr(const ObDDLKvMgrHandle &ddl_kv_mgr_handle)
{
  int ret = OB_SUCCESS;
  ObByteLockGuard guard(ddl_kv_mgr_lock_);
  if (OB_FAIL(!ddl_kv_mgr_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ddl_kv_mgr_handle));
  } else if (ddl_kv_mgr_handle_.get_obj() != ddl_kv_mgr_handle.get_obj()) {
    ret = OB_ITEM_NOT_MATCH;
    LOG_WARN("ddl kv mgr changed", K(ret), KP(ddl_kv_mgr_handle_.get_obj()), KPC(ddl_kv_mgr_handle.get_obj()));
  } else {
    ddl_kv_mgr_handle_.reset();
  }
  return ret;
}

int ObTabletPointer::get_mds_table(const ObTabletID &tablet_id,
    mds::MdsTableHandle &handle,
    bool not_exist_create)
{
  int ret = OB_SUCCESS;
  if (!ls_handle_.is_valid()) {
    ret = OB_NOT_INIT;
    LOG_ERROR("invalid ls_handle_, maybe not init yet", K(ret));
  } else if (!tablet_id.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "tablet_id is invalid", K(ret));
  } else if (OB_FAIL(mds_table_handler_.get_mds_table_handle(handle,
                                                             tablet_id,
                                                             ls_handle_.get_ls()->get_ls_id(),
                                                             not_exist_create,
                                                             this))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to get mds table", K(ret), K(not_exist_create));
    }
  }
  return ret;
}

int ObTabletPointer::try_release_mds_nodes_below(const share::SCN &scn)
{
  return mds_table_handler_.try_release_nodes_below(scn);
}

int ObTabletPointer::try_gc_mds_table()
{
  return mds_table_handler_.try_gc_mds_table();
}

void ObTabletPointer::set_tablet_status_written()
{
  mds_table_handler_.set_tablet_status_written();
}

void ObTabletPointer::reset_tablet_status_written()
{
  mds_table_handler_.reset_tablet_status_written();
}

bool ObTabletPointer::is_tablet_status_written() const
{
  return mds_table_handler_.is_tablet_status_written();
}

void ObTabletPointer::mark_mds_table_deleted()
{
  return mds_table_handler_.mark_removed_from_t3m(this);
}

int ObTabletPointer::release_memtable_and_mds_table_for_ls_offline(const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  mds::MdsTableHandle mds_table;
  reset_tablet_status_written();
  if (tablet_id.is_ls_inner_tablet()) {
    LOG_INFO("skip inner tablet", K(tablet_id));
  } else if (OB_FAIL(protected_memtable_mgr_handle_.reset())) {
    LOG_WARN("failed to reset protected_memtable_mgr_handle", K(ret));
  } else if (OB_FAIL(get_mds_table(tablet_id, mds_table, false/*not_exist_create*/))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get mds table", K(ret));
    }
  } else if (OB_FAIL(mds_table.forcely_reset_mds_table("OFFLINE"))) {
    LOG_WARN("fail to release mds nodes in mds table", K(ret));
  }

  return ret;
}

int ObTabletPointer::add_tablet_to_old_version_chain(ObTablet *tablet)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == tablet)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("add invalid tablet to old version chain", K(ret), KPC(tablet));
  } else {
    //defensive code
    ObTablet *cur = old_version_chain_;
    bool found = false;
    while (OB_NOT_NULL(cur) && !found) {
      found = tablet == cur;
      cur = cur->get_next_tablet();
    }
    if (found) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet has been in old version chain, some wrong occurs",
          K(ret), KP(tablet), KP_(old_version_chain));
    } else {
      tablet->set_next_tablet(old_version_chain_);
      old_version_chain_ = tablet;
    }
  }
  LOG_DEBUG("add_tablet_to_old_version_chain", K(ret), KP(tablet));
  return ret;
}

int ObTabletPointer::remove_tablet_from_old_version_chain(ObTablet *tablet)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tablet)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet to remove", K(ret), KP(tablet));
  } else if (OB_ISNULL(old_version_chain_)) {
    // do nothing
  } else if (old_version_chain_ == tablet) {
    old_version_chain_ = old_version_chain_->get_next_tablet();
  } else {
    ObTablet *cur = old_version_chain_;
    ObTablet *next = nullptr;
    while (OB_SUCC(ret) && OB_NOT_NULL(next = cur->get_next_tablet())) {
      if (next == tablet) {
        next = next->get_next_tablet();
        cur->set_next_tablet(next);
        break;
      }
      cur = next;
    }
  }
  LOG_DEBUG("remove_tablet_from_old_version_chain", K(ret), KP(tablet), KP(old_version_chain_));
  return ret;
}

int ObTabletPointer::get_min_mds_ckpt_scn(share::SCN &scn)
{
  int ret = OB_SUCCESS;
  scn.set_max();
  ObTablet *cur = old_version_chain_;
  while (OB_SUCC(ret) && OB_NOT_NULL(cur)) {
    scn = MIN(scn, cur->get_tablet_meta().mds_checkpoint_scn_);
    cur = cur->get_next_tablet();

  }
  return ret;
}

ObLS *ObTabletPointer::get_ls() const
{
  return ls_handle_.get_ls();
}

int ObTabletPointer::acquire_obj(ObTablet *&t)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_ISNULL(obj_.pool_) || OB_ISNULL(obj_.t3m_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "object pool is nullptr", K(ret), K(obj_));
  } else if (OB_FAIL(obj_.t3m_->acquire_tablet(obj_.pool_, t))) {
    STORAGE_LOG(WARN, "fail to acquire tablet buffer", K(ret), K(phy_addr_));
  }
  return ret;
}

int ObTabletPointer::release_obj(ObTablet *&t)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(t)) {
    // do nothing
  } else if (OB_UNLIKELY(nullptr == obj_.pool_ && nullptr == obj_.allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "object pool or allocator is nullptr", K(ret), K(obj_));
  } else if (nullptr == t->get_allocator()) {
    obj_.t3m_->release_tablet_from_pool(t, true/*give_back_tablet_into_pool*/);
    t = nullptr;
  } else {
    t->~ObTablet();
    obj_.allocator_->free(t);
    t = nullptr;
  }
  return ret;
}

int ObTabletPointer::set_tablet_attr(const ObTabletAttr &attr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!attr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(attr));
  } else {
    attr_ = attr;
  }
  return ret;
}

ObTabletResidentInfo::ObTabletResidentInfo(const ObTabletMapKey &key, const ObTabletPointer &tablet_ptr)
  : attr_(tablet_ptr.attr_), tablet_addr_(tablet_ptr.phy_addr_)
{
  tablet_id_ = key.tablet_id_;
  ls_id_ = key.ls_id_;
}

int64_t ObITabletFilterOp::total_skip_cnt_ = 0;
int64_t ObITabletFilterOp::total_tablet_cnt_ = 0;
int64_t ObITabletFilterOp::not_in_mem_tablet_cnt_ = 0;
int64_t ObITabletFilterOp::invalid_attr_tablet_cnt_ = 0;
ObITabletFilterOp::~ObITabletFilterOp()
{
  total_skip_cnt_ += skip_cnt_;
  total_tablet_cnt_ += total_cnt_;
  not_in_mem_tablet_cnt_ += not_in_mem_cnt_;
  invalid_attr_tablet_cnt_ += invalid_attr_cnt_;
  LOG_INFO("tablet skip filter destructed",
      K_(total_cnt), K_(skip_cnt), K_(not_in_mem_cnt), K_(invalid_attr_cnt),
      K_(total_tablet_cnt), K_(total_skip_cnt), K_(not_in_mem_tablet_cnt), K_(invalid_attr_tablet_cnt));
}

int ObITabletFilterOp::operator()(const ObTabletResidentInfo &info, bool &is_skipped)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("try to skip tablet with invalid resident info", K(ret), K(info));
  } else if (OB_FAIL((do_filter(info, is_skipped)))) {
    LOG_WARN("fail to do filter", K(ret), K(info), K_(skip_cnt), K_(total_cnt));
  } else if (is_skipped) {
    ++skip_cnt_;
  }
  ++total_cnt_;
  return ret;
}



} // namespace storage
} // namespace oceanbase
