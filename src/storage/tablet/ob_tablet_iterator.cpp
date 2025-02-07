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

#include "storage/tablet/ob_tablet_iterator.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/ls/ob_ls.h"
#include "storage/ls/ob_ls_tablet_service.h"
#include "storage/meta_mem/ob_meta_obj_struct.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/meta_mem/ob_tablet_map_key.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/tablet/ob_tablet_multi_source_data.h"

namespace oceanbase
{
namespace storage
{
ObLSTabletIterator::ObLSTabletIterator(const ObMDSGetTabletMode mode)
  : ls_tablet_service_(nullptr),
    tablet_ids_(),
    idx_(0),
    mode_(mode)
{
}

ObLSTabletIterator::~ObLSTabletIterator()
{
  reset();
}

void ObLSTabletIterator::reset()
{
  ls_tablet_service_ = nullptr;
  tablet_ids_.reset();
  idx_ = 0;
}

bool ObLSTabletIterator::is_valid() const
{
  return nullptr != ls_tablet_service_
      && mode_ >= ObMDSGetTabletMode::READ_ALL_COMMITED;
}

int ObLSTabletIterator::get_next_tablet(ObTabletHandle &handle)
{
  int ret = OB_SUCCESS;

  handle.reset();
  if (OB_ISNULL(ls_tablet_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls tablet service is nullptr", K(ret), KP(ls_tablet_service_));
  } else {
    do {
      if (OB_UNLIKELY(tablet_ids_.count() == idx_)) {
        ret = OB_ITER_END;
      } else {
        const common::ObTabletID &tablet_id = tablet_ids_.at(idx_);
        if (OB_FAIL(ls_tablet_service_->get_tablet(tablet_id, handle, ObTabletCommon::DEFAULT_GET_TABLET_DURATION_10_S, mode_))
            && OB_TABLET_NOT_EXIST != ret) {
          LOG_WARN("fail to get tablet", K(ret), K(idx_), K(tablet_id), K_(mode));
        } else {
          handle.set_wash_priority(WashTabletPriority::WTP_LOW);
          ++idx_;
        }
      }
    } while (OB_TABLET_NOT_EXIST == ret);
  }

  return ret;
}

int ObLSTabletIterator::get_next_ddl_kv_mgr(ObDDLKvMgrHandle &ddl_kv_mgr_handle)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ls_tablet_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls tablet service is nullptr", K(ret), KP(ls_tablet_service_));
  } else {
    ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
    do {
      ObTabletMapKey key;
      key.ls_id_ = ls_tablet_service_->ls_->get_ls_id();
      if (OB_UNLIKELY(tablet_ids_.count() == idx_)) {
        ret = OB_ITER_END;
      } else {
        key.tablet_id_ = tablet_ids_.at(idx_);

        if (OB_FAIL(t3m->get_tablet_ddl_kv_mgr(key, ddl_kv_mgr_handle))
            && OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("fail to get tablet ddl kv mgr", K(ret), K(idx_), K(key));
        } else {
          ++idx_;
        }
      }
    } while (OB_ENTRY_NOT_EXIST == ret);
  }

  return ret;
}


int ObLSTabletIterator::get_tablet_ids(ObIArray<ObTabletID> &ids) const
{
  int ret = OB_SUCCESS;
  ids.reset();
  if (OB_FAIL(ids.assign(tablet_ids_))) {
    LOG_WARN("fail to get tablet ids", K(ret));
  }
  return ret;
}

ObHALSTabletIDIterator::ObHALSTabletIDIterator(
    const share::ObLSID &ls_id,
    const bool need_initial_state,
    const bool need_sorted_tablet_id)
  : ls_id_(ls_id),
    tablet_ids_(),
    idx_(0),
    need_initial_state_(need_initial_state),
    need_sorted_tablet_id_(need_sorted_tablet_id)
{
}

ObHALSTabletIDIterator::~ObHALSTabletIDIterator()
{
  reset();
}

bool ObHALSTabletIDIterator::is_valid() const
{
  return ls_id_.is_valid();
}

void ObHALSTabletIDIterator::reset()
{
  ls_id_.reset();
  tablet_ids_.reset();
  idx_ = 0;
}

int ObHALSTabletIDIterator::sort_tablet_ids_if_need()
{
  int ret = OB_SUCCESS;
  if (!need_sorted_tablet_id_) {
    // do nothing
  } else if (OB_UNLIKELY(0 != idx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get next tablet id before sort", K(ret), K_(idx));
  } else {
    lib::ob_sort(tablet_ids_.begin(), tablet_ids_.end());
    LOG_INFO("sort tablet ids if need");
  }
  return ret;
}

int ObHALSTabletIDIterator::get_next_tablet_id(common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  ObTabletMapKey key;
  key.ls_id_ = ls_id_;

  bool initial_state = true;
  while (OB_SUCC(ret)) {
    if (OB_UNLIKELY(tablet_ids_.count() == idx_)) {
      ret = OB_ITER_END;
    } else {
      initial_state = true;
      key.tablet_id_ = tablet_ids_.at(idx_);
      if (OB_FAIL(t3m->get_tablet_pointer_initial_state(key, initial_state))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ++idx_;
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get tablet status from tablet pointer", K(ret), K(key));
        }
      } else if (initial_state && !need_initial_state_) {
        LOG_INFO("tablet is in initial state, should skip", K(ret), K(key));
        ++idx_;
      } else {
        ++idx_;
        tablet_id = key.tablet_id_;
        break;
      }
    }
  }

  return ret;
}


ObHALSTabletIterator::ObHALSTabletIterator(
    const share::ObLSID &ls_id,
    const bool need_initial_state,
    const bool need_sorted_tablet_id)
  : ls_tablet_service_(nullptr),
    tablet_id_iter_(ls_id, need_initial_state, need_sorted_tablet_id)
{}


ObHALSTabletIterator::~ObHALSTabletIterator()
{}

bool ObHALSTabletIterator::is_valid() const
{
  return tablet_id_iter_.is_valid();
}

void ObHALSTabletIterator::reset()
{
  tablet_id_iter_.reset();
}

int ObHALSTabletIterator::get_next_tablet(ObTabletHandle &handle)
{
  int ret = OB_SUCCESS;
  ObTabletID tablet_id;
  handle.reset();
  if (OB_ISNULL(ls_tablet_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls tablet service is nullptr", K(ret), KP(ls_tablet_service_));
  } else if (OB_FAIL(tablet_id_iter_.get_next_tablet_id(tablet_id))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next tablet id", K(ret));
    }
  } else if (OB_FAIL(ls_tablet_service_->get_tablet(tablet_id, handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    LOG_WARN("failed to get tablet", K(ret), K(tablet_id));
  }

  return ret;
}



ObLSTabletFastIter::ObLSTabletFastIter(ObITabletFilterOp &op, const ObMDSGetTabletMode mode)
  : ls_tablet_service_(nullptr),
    tablet_ids_(),
    idx_(0),
    mode_(mode),
    op_(op)
{
}

bool ObLSTabletFastIter::is_valid() const
{
  return nullptr != ls_tablet_service_
      && mode_ <= ObMDSGetTabletMode::READ_WITHOUT_CHECK; // READ_READABLE_COMMITED is not supported
}

void ObLSTabletFastIter::reset()
{
  ls_tablet_service_ = nullptr;
  tablet_ids_.reset();
  idx_ = 0;
}

int ObLSTabletFastIter::get_next_tablet(ObTabletHandle &handle)
{
  int ret = OB_SUCCESS;

  handle.reset();
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  if (OB_ISNULL(t3m)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant meta mem mgr is nullptr", K(ret), KP(t3m));
  } else {
    do {
      if (OB_UNLIKELY(tablet_ids_.count() == idx_)) {
        ret = OB_ITER_END;
      } else {
        const common::ObTabletID &tablet_id = tablet_ids_.at(idx_);
        const ObTabletMapKey key(ls_tablet_service_->ls_->get_ls_id(), tablet_id);
        if (OB_FAIL(t3m->get_tablet_with_filter(WashTabletPriority::WTP_LOW, key, op_, handle))) {
          if (OB_ENTRY_NOT_EXIST != ret && OB_ITEM_NOT_SETTED != ret && OB_NOT_THE_OBJECT != ret) {
            LOG_WARN("fail to get tablet", K(ret), K_(idx), K(key));
          } else {
            ++idx_;
          }
        } else {
          handle.set_wash_priority(WashTabletPriority::WTP_LOW);
          ++idx_;
        }
      }
    } while (OB_ENTRY_NOT_EXIST == ret || OB_ITEM_NOT_SETTED == ret || OB_NOT_THE_OBJECT == ret);
  }

  return ret;
}

ObLSTabletAddrIterator::ObLSTabletAddrIterator()
  : ls_tablet_service_(nullptr),
    tablet_ids_(),
    idx_(0)
{
}

ObLSTabletAddrIterator::~ObLSTabletAddrIterator()
{
  reset();
}

// for write_checkpoint in SN and active_tablet_arr in SS
int ObLSTabletAddrIterator::get_next_tablet_addr(ObTabletMapKey &key, ObMetaDiskAddr &addr)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(ls_tablet_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls tablet service is nullptr", K(ret), KP(ls_tablet_service_));
  } else {
    key.ls_id_ = ls_tablet_service_->ls_->get_ls_id();
    do {
      if (OB_UNLIKELY(tablet_ids_.count() == idx_)) {
        ret = OB_ITER_END;
      } else {
        key.tablet_id_ = tablet_ids_.at(idx_);

        if (OB_FAIL(ls_tablet_service_->get_tablet_addr(key, addr))
            && OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("fail to get tablet address", K(ret), K(idx_), K(key));
        } else {
          ++idx_;
        }
      }
    } while (OB_ENTRY_NOT_EXIST == ret);
  }

  return ret;
}

} // namespace storage
} // namespace oceanbase
