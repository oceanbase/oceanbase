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

#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
namespace oceanbase
{
namespace storage
{
ObTabletHandle::ObTabletHandle()
  : Base(),
    wash_priority_(WashTabletPriority::WTP_MAX),
    allow_copy_and_assign_(true)
{
  INIT_OBJ_LEAK_DEBUG_NODE(node_, this, share::LEAK_CHECK_OBJ_TABLET_HANDLE, MTL_ID());
}

ObTabletHandle::ObTabletHandle(
    const Base &other,
    const WashTabletPriority priority)
  : Base(other),
    wash_priority_(priority),
    allow_copy_and_assign_(true)
{
}

ObTabletHandle::ObTabletHandle(const ObTabletHandle &other)
  : Base(),
    wash_priority_(WashTabletPriority::WTP_MAX),
    allow_copy_and_assign_(true)
{
  INIT_OBJ_LEAK_DEBUG_NODE(node_, this, share::LEAK_CHECK_OBJ_TABLET_HANDLE, MTL_ID());
  *this = other;
}

ObTabletHandle::~ObTabletHandle()
{
  reset();
}

ObTabletHandle &ObTabletHandle::operator=(const ObTabletHandle &other)
{
  if (this != &other) {
    abort_unless(other.allow_copy_and_assign_);
    reset();
    Base::operator=(other);
    wash_priority_ = other.wash_priority_;
    allow_copy_and_assign_ = other.allow_copy_and_assign_;
  }
  return *this;
}

void ObTabletHandle::reset()
{
  if (nullptr != obj_) {
    int ret = OB_SUCCESS;
    obj_->update_wash_score(calc_wash_score(wash_priority_));
    if (OB_UNLIKELY(!is_valid())) {
      STORAGE_LOG(ERROR, "object pool and allocator is nullptr", K_(obj), K_(obj_pool), K_(allocator));
      ob_abort();
    } else {
      const int64_t ref_cnt = obj_->dec_ref();
      const int64_t hold_time = ObClockGenerator::getClock() - hold_start_time_;
      if (OB_UNLIKELY(hold_time > HOLD_OBJ_MAX_TIME && need_hold_time_check())) {
        int ret = OB_ERR_TOO_MUCH_TIME;
        STORAGE_LOG(WARN, "The meta obj reference count was held for more "
            "than two hours ", K(ref_cnt), KP(this), K(hold_time), K(hold_start_time_), KPC(this), K(common::lbt()));
      }
      if (OB_UNLIKELY(ref_cnt < 0)) {
        STORAGE_LOG(ERROR, "obj ref cnt may be leaked", K(ref_cnt), KPC(this));
      } else if (0 == ref_cnt) {
        if (!allow_copy_and_assign_) {
          // all in memory, no need to dec macro ref
          if (OB_NOT_NULL(obj_pool_)) {
            ob_usleep(1000 * 1000);
            ob_abort();
          }
          obj_->dec_macro_ref_cnt();
          obj_->~ObTablet();
          allocator_->free(obj_);
        } else if (OB_FAIL(t3m_->gc_tablet(obj_))) {
          STORAGE_LOG(ERROR, "fail to gc tablet", K(ret), KPC_(obj), K_(obj_pool), K_(allocator));
        }
      }
      obj_ = nullptr;
    }
  }
  wash_priority_ = WashTabletPriority::WTP_MAX;
  allow_copy_and_assign_ = true;
  obj_pool_ = nullptr;
  allocator_ = nullptr;
  t3m_ = nullptr;
}

int64_t ObTabletHandle::calc_wash_score(const WashTabletPriority priority) const
{
  int ret = OB_SUCCESS;
  int64_t score = INT64_MIN;
  if (OB_UNLIKELY(WashTabletPriority::WTP_HIGH != priority
               && WashTabletPriority::WTP_LOW != priority)) {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("this priority value hasn't be supported", K(ret), KPC(this));
  } else {
    // Formula:
    //   score = (1 - priority) * t + priority * (t - INT64_MAX)
    const int64_t t = ObTimeUtility::current_time_ns();
    score = WashTabletPriority::WTP_HIGH == priority ? t : t - INT64_MAX;
  }
  return score;
}

int64_t ObTabletHandle::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP_(obj),
       KP_(obj_pool),
       KP_(allocator),
       K_(wash_priority),
       K_(allow_copy_and_assign));
  J_OBJ_END();
  return pos;
}

void ObTabletTableIterator::operator=(const ObTabletTableIterator& other)
{
  if (this != &other) {
    if (other.tablet_handle_.is_valid()) {
      tablet_handle_ = other.tablet_handle_;
    } else if (tablet_handle_.is_valid()) {
      tablet_handle_.reset();
    }
    if (other.table_store_iter_.is_valid()) {
      table_store_iter_ = other.table_store_iter_;
    } else if (table_store_iter_.is_valid()) {
      table_store_iter_.reset();
    }
    if (OB_UNLIKELY(nullptr != other.transfer_src_handle_)) {
      if (nullptr == transfer_src_handle_) {
        void *tablet_hdl_buf = ob_malloc(sizeof(ObTabletHandle), ObMemAttr(MTL_ID(), "TransferMetaH"));
        transfer_src_handle_ = new (tablet_hdl_buf) ObTabletHandle();
      }
      *transfer_src_handle_ = *(other.transfer_src_handle_);
    }
  }
}

ObTableStoreIterator *ObTabletTableIterator::table_iter()
{
  return &table_store_iter_;
}

int ObTabletTableIterator::set_tablet_handle(const ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(tablet_handle_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet table iterator already has a valid tablet handle", K(ret));
  } else {
    tablet_handle_ = tablet_handle;
  }
  return ret;
}

int ObTabletTableIterator::set_transfer_src_tablet_handle(const ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  if (nullptr == transfer_src_handle_) {
    void *tablet_hdl_buf = ob_malloc(sizeof(ObTabletHandle), ObMemAttr(MTL_ID(), "TransferTblH"));
    if (OB_ISNULL(tablet_hdl_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocator memory for handles");
    } else {
      transfer_src_handle_ = new (tablet_hdl_buf) ObTabletHandle();
    }
  }
  if (OB_SUCC(ret)) {
    *transfer_src_handle_ = tablet_handle;
  }
  return ret;
}

int ObTabletTableIterator::refresh_read_tables_from_tablet(
    const int64_t snapshot_version,
    const bool allow_no_ready_read,
    const bool major_sstable_only)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!tablet_handle_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("try to refresh tables in tablet table iter with invalid tablet handle", K(ret));
  } else if (major_sstable_only) {
    if (OB_FAIL(tablet_handle_.get_obj()->get_read_major_sstable(snapshot_version, *this))) {
      LOG_WARN("failed to get read major sstable from tablet",
          K(ret), K(snapshot_version), K_(tablet_handle));
    }
  } else {
    if (OB_FAIL(tablet_handle_.get_obj()->get_read_tables(
        snapshot_version, *this, allow_no_ready_read))) {
      LOG_WARN("failed to get read tables from tablet", K(ret), K_(tablet_handle));
    }
  }
  return ret;
}

} // end namespace storage
} // end namespace oceanbase
