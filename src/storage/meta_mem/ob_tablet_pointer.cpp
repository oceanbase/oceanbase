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

#define USING_LOG_PREFIX STORAGE

using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::share;

namespace oceanbase
{
namespace storage
{
ObTabletPointer::ObTabletPointer()
  : ObMetaPointer<ObTablet>(),
    ls_handle_(),
    ddl_kv_mgr_handle_(),
    memtable_mgr_handle_(),
    ddl_info_(),
    initial_state_(true),
    ddl_kv_mgr_lock_(),
    mds_table_handler_(),
    old_version_chain_(nullptr)
{
#if defined(__x86_64__) && !defined(ENABLE_OBJ_LEAK_CHECK)
  static_assert(sizeof(ObTabletPointer) == 272, "The size of ObTabletPointer will affect the meta memory manager, and the necessity of adding new fields needs to be considered.");
#endif
}

ObTabletPointer::ObTabletPointer(
    const ObLSHandle &ls_handle,
    const ObMemtableMgrHandle &memtable_mgr_handle)
  : ObMetaPointer<ObTablet>(),
    ls_handle_(ls_handle),
    memtable_mgr_handle_(memtable_mgr_handle),
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
  ls_handle_.reset();
  {
    ObByteLockGuard guard(ddl_kv_mgr_lock_);
    ddl_kv_mgr_handle_.reset();
  }
  mds_table_handler_.reset();
  memtable_mgr_handle_.reset();
  ddl_info_.reset();
  initial_state_ = true;
  ATOMIC_STORE(&initial_state_, true);
  old_version_chain_ = nullptr;

  ObMetaPointer<ObTablet>::reset();
}

int ObTabletPointer::set_attr_for_obj(ObTablet *tablet)
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
    tablet->memtable_mgr_ = memtable_mgr_handle_.get_memtable_mgr();
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
  } else if (OB_UNLIKELY(phy_addr_.is_file())) {
    LOG_INFO("obj is empty shell, don't be wash", K(ret), K(phy_addr_));
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
      } else if (OB_FAIL(set_attr_for_obj(meta_obj.ptr_))) {
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

int ObTabletPointer::deep_copy(char *buf, const int64_t buf_len, ObMetaPointer<ObTablet> *&value) const
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
      pvalue->memtable_mgr_handle_ = memtable_mgr_handle_;
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

int ObTabletPointer::create_ddl_kv_mgr(const share::ObLSID &ls_id, const ObTabletID &tablet_id, ObDDLKvMgrHandle &ddl_kv_mgr_handle)
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

int ObTabletPointer::get_mds_table(mds::MdsTableHandle &handle, bool not_exist_create)
{
  int ret = OB_SUCCESS;
  if (!ls_handle_.is_valid()) {
    ret = OB_NOT_INIT;
    LOG_ERROR("invalid ls_handle_, maybe not init yet", K(ret));
  } else if (!memtable_mgr_handle_.is_valid()) {
    ret = OB_NOT_INIT;
    LOG_ERROR("invalid memtable_mgr_handle_, maybe not init yet", K(ret));
  } else if (OB_ISNULL(ls_handle_.get_ls())) {
    ret = OB_BAD_NULL_ERROR;
    LOG_ERROR("ls in handle is nullptr", K(ret));
  } else if (OB_ISNULL(memtable_mgr_handle_.get_memtable_mgr())) {
    ret = OB_BAD_NULL_ERROR;
    LOG_ERROR("memtable mgr in handle is nullptr", K(ret));
  } else if (OB_FAIL(mds_table_handler_.get_mds_table_handle(handle,
                                                             memtable_mgr_handle_.get_memtable_mgr()->get_tablet_id(),
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

bool ObTabletPointer::is_tablet_status_written() const
{
  return mds_table_handler_.is_tablet_status_written();
}

void ObTabletPointer::mark_mds_table_deleted()
{
  return mds_table_handler_.mark_removed_from_t3m(this);
}

int ObTabletPointer::release_memtable_and_mds_table_for_ls_offline()
{
  int ret = OB_SUCCESS;
  ObIMemtableMgr *memtable_mgr = memtable_mgr_handle_.get_memtable_mgr();
  mds::MdsTableHandle mds_table;
  if (OB_ISNULL(memtable_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("memtable mgr is null", K(ret));
  } else if (OB_FAIL(memtable_mgr->release_memtables())) {
    LOG_WARN("failed to release memtables", K(ret));
  } else if (OB_FAIL(memtable_mgr->reset_storage_recorder())) {
    LOG_WARN("failed to destroy storage recorder", K(ret), KPC(memtable_mgr));
  } else if (OB_FAIL(get_mds_table(mds_table, false/*not_exist_create*/))) {
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
    obj_.t3m_->release_tablet(t);
    t = nullptr;
  } else {
    t->~ObTablet();
    obj_.allocator_->free(t);
    t = nullptr;
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
