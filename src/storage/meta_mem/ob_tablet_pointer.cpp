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

#include "storage/meta_mem/ob_tablet_pointer.h"

#include "lib/container/ob_se_array.h"
#include "lib/hash/ob_cuckoo_hashmap.h"
#include "lib/stat/ob_latch_define.h"
#include "storage/ls/ob_ls.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_common.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/ddl/ob_tablet_ddl_kv_mgr.h"

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
    tx_data_(),
    cond_(),
    msd_lock_(),
    ddl_kv_mgr_lock_()
{
}

ObTabletPointer::ObTabletPointer(
    const ObLSHandle &ls_handle,
    const ObMemtableMgrHandle &memtable_mgr_handle)
  : ObMetaPointer<ObTablet>(),
    ls_handle_(ls_handle),
    memtable_mgr_handle_(memtable_mgr_handle),
    ddl_info_(),
    tx_data_(),
    cond_(),
    msd_lock_(ObLatchIds::TABLET_MULTI_SOURCE_DATA_LOCK),
    ddl_kv_mgr_lock_()
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
    ObMutexGuard guard(ddl_kv_mgr_lock_);
    ddl_kv_mgr_handle_.reset();
  }
  memtable_mgr_handle_.reset();
  ddl_info_.reset();
  tx_data_.reset();
  cond_.destroy();

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

int ObTabletPointer::do_post_work_for_load()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(obj_.ptr_->deserialize_post_work())) {
    LOG_WARN("fail to deserialize post work", K(ret), K(obj_));
  } else if (OB_FAIL(obj_.ptr_->dec_macro_disk_ref())) {
    LOG_WARN("fail to dec macro disk ref", K(ret), K(obj_));
  }
  return ret;
}

int ObTabletPointer::dump_meta_obj(bool &is_washed)
{
  int ret = OB_SUCCESS;
  is_washed = false;
  ObTablet *tablet = nullptr;

  if (OB_ISNULL(obj_.ptr_)) {
    LOG_INFO("tablet may be washed", KPC(obj_.ptr_));
  } else if (FALSE_IT(tablet = obj_.ptr_)) {
  } else if (OB_UNLIKELY(tablet->get_ref() > 1)) {
    LOG_INFO("tablet may be attached again, continue", KPC(tablet));
  } else if (OB_UNLIKELY(tablet->get_ref() < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, tablet ref is less than 1", K(ret), KPC(tablet));
  } else if (OB_UNLIKELY(!phy_addr_.is_disked())) {
    LOG_INFO("tablet may be removed, and created again, continue", K(phy_addr_));
  } else if (OB_FAIL(wash_obj())) {
    LOG_WARN("fail to wash obj", K(ret));
  } else {
    is_washed = true;
  }
  return ret;
}

int ObTabletPointer::wash_obj()
{
  int ret = OB_SUCCESS;
  const ObLSID ls_id = obj_.ptr_->tablet_meta_.ls_id_;
  const ObTabletID tablet_id = obj_.ptr_->tablet_meta_.tablet_id_;
  const int64_t wash_score = obj_.ptr_->get_wash_score();

  if (OB_FAIL(obj_.ptr_->inc_macro_disk_ref())) {
    LOG_WARN("fail to inc macro disk ref", K(ret), K(obj_));
  } else {
    LOG_DEBUG("succeed to wash one tablet", KP(obj_.ptr_), K(ls_id), K(tablet_id), K(wash_score), K(phy_addr_));
    reset_obj();
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
    if (nullptr != obj_.ptr_) {
      if (nullptr == obj_.pool_) {
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
      pvalue->tx_data_ = tx_data_;
      value = pvalue;
      // NOTICE: cond and rw lock cannot be copied
    } else {
      pvalue->~ObTabletPointer();
    }
  }
  return ret;
}

int ObTabletPointer::set_tx_data(const ObTabletTxMultiSourceDataUnit &tx_data)
{
  int ret = OB_SUCCESS;
  TCWLockGuard guard(msd_lock_);
  tx_data_ = tx_data;
  return ret;
}

int ObTabletPointer::get_tx_data(ObTabletTxMultiSourceDataUnit &tx_data) const
{
  int ret = OB_SUCCESS;
  TCRLockGuard guard(msd_lock_);
  tx_data = tx_data_;
  return ret;
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
    ObMutexGuard guard(ddl_kv_mgr_lock_);
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
  ObMutexGuard guard(ddl_kv_mgr_lock_);
  ddl_kv_mgr_handle = ddl_kv_mgr_handle_;
}

int ObTabletPointer::set_ddl_kv_mgr(const ObDDLKvMgrHandle &ddl_kv_mgr_handle)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(!ddl_kv_mgr_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ddl_kv_mgr_handle));
  } else {
    ObMutexGuard guard(ddl_kv_mgr_lock_);
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
  ObMutexGuard guard(ddl_kv_mgr_lock_);
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

} // namespace storage
} // namespace oceanbase
