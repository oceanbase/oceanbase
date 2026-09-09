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

#include "ob_protected_memtable_mgr_handle.h"
#include "storage/ls/ob_ls.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"

using namespace oceanbase::share;
using namespace oceanbase::memtable;

namespace oceanbase
{
namespace storage
{

int ObProtectedMemtableMgrHandle::create_tablet_memtable_mgr_(const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    lib::Worker::CompatMode compat_mode)
{
  int ret = OB_SUCCESS;
  ObTabletMemtableMgr *mgr = NULL;
  ObTabletMemtableMgrPool *pool = MTL(ObTabletMemtableMgrPool*);
  if (memtable_mgr_handle_.is_valid()) {
  } else if (OB_ISNULL(mgr = pool->acquire())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to memtable manager alloc", KR(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(mgr->ObIMemtableMgr::init(ls_id, tablet_id, compat_mode))) {
    pool->release(mgr);
    STORAGE_LOG(WARN, "failed to init memtable mgr", KR(ret), K(tablet_id), K(ls_id));
  } else {
    memtable_mgr_handle_.set_memtable_mgr(mgr, pool);
  }
  return ret;
}

bool ObProtectedMemtableMgrHandle::need_reset_()
{
  bool ret = false;
  SpinRLockGuard guard(memtable_mgr_handle_lock_);
  ret = need_reset_without_lock_();
  return ret;
}

bool ObProtectedMemtableMgrHandle::need_reset_without_lock_()
{
  bool ret = false;
  ObIMemtableMgr *mgr = NULL;
  if (!memtable_mgr_handle_.is_valid()) {
  } else if (FALSE_IT(mgr = memtable_mgr_handle_.get_memtable_mgr())) {
  } else if (!mgr->has_memtable()) {
    ret = true;
  }
  return ret;
}
// only for non-inner tablet
int ObProtectedMemtableMgrHandle::try_reset_memtable_mgr_handle_()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(memtable_mgr_handle_lock_);
  if (need_reset_without_lock_()) {
    memtable_mgr_handle_.reset();
  }
  return ret;
}

int ObProtectedMemtableMgrHandle::reset()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(memtable_mgr_handle_lock_);
  if (memtable_mgr_handle_.is_valid()) {
    ObIMemtableMgr *mgr = memtable_mgr_handle_.get_memtable_mgr();
    if (OB_FAIL(mgr->has_memtable() && mgr->release_memtables())) {
      STORAGE_LOG(ERROR, "failed to release memtables", KR(ret), KPC(this));
    }
    memtable_mgr_handle_.reset();
    STORAGE_LOG(INFO, "protected_memtable_mgr_handle reset", KR(ret), KPC(this));
  }
  return ret;
}

int ObProtectedMemtableMgrHandle::update_max_saved_medium_scn_if_ls_online(
    const ObTabletMeta &tablet_meta,
    const ObLS &ls,
    const int64_t medium_scn)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(memtable_mgr_handle_lock_);
  const int64_t ls_switch_epoch = ls.get_switch_epoch();
  if (!(ls_switch_epoch & 1)) {
    // Do not recreate volatile tablet resources once the LS starts going offline.
    STORAGE_LOG(INFO, "skip updating max saved medium scn for offline ls",
        K(ls_switch_epoch), K(medium_scn), K(tablet_meta.ls_id_), K(tablet_meta.tablet_id_));
  } else {
    // If offline starts after this check, its reset waits for this lock and
    // releases the manager created here as part of the normal cleanup.
    if (!memtable_mgr_handle_.is_valid()
        && OB_FAIL(create_tablet_memtable_mgr_(
            tablet_meta.ls_id_, tablet_meta.tablet_id_, tablet_meta.compat_mode_))) {
      STORAGE_LOG(WARN, "failed to create tablet memtable mgr for medium scn update",
          K(ret), K(medium_scn), K(tablet_meta.ls_id_), K(tablet_meta.tablet_id_));
    } else {
      ObTabletMemtableMgr *tablet_memtable_mgr =
          static_cast<ObTabletMemtableMgr*>(memtable_mgr_handle_.get_memtable_mgr());
      if (OB_FAIL(tablet_memtable_mgr->get_medium_info_recorder()
          .update_max_saved_medium_scn(medium_scn))) {
        STORAGE_LOG(WARN, "failed to update max saved medium scn",
            K(ret), K(medium_scn), K(tablet_meta.ls_id_), K(tablet_meta.tablet_id_));
      }
    }
  }
  return ret;
}

int ObProtectedMemtableMgrHandle::release_memtables_and_try_reset_memtable_mgr_handle(
    const ObTabletID &tablet_id,
    const SCN &scn)
{
  int ret = OB_SUCCESS;
  if (scn.is_valid() && OB_FAIL(release_memtables(scn))) {
    STORAGE_LOG(WARN, "failed to release_memtables", KR(ret), K(tablet_id), K(scn));
  } else if (!scn.is_valid() && OB_FAIL(release_memtables())) {
    STORAGE_LOG(WARN, "failed to release_memtables", KR(ret), K(tablet_id));
  } else if (tablet_id.is_ls_inner_tablet()) {
    // do nothing
  } else if (!need_reset_()) {
  } else if (OB_FAIL(try_reset_memtable_mgr_handle_())) {
    STORAGE_LOG(WARN, "failed to try_reset_memtable_mgr_handle", KR(ret), K(tablet_id), K(scn), KPC(this));
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
