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

#define USING_LOG_PREFIX TABLELOCK
#include "storage/tablelock/ob_lock_memtable.h"
#include "storage/tablelock/ob_lock_memtable_mgr.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/ls/ob_ls_tx_service.h"
#include "storage/ls/ob_freezer.h"

namespace oceanbase
{
using namespace share;
using namespace memtable;
using namespace storage;

namespace transaction
{
namespace tablelock
{

ObLockMemtableMgr::ObLockMemtableMgr() : ObIMemtableMgr(LockType::OB_QSYNC_LOCK, &lock_def_)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(lock_def_.init(lib::ObMemAttr(MTL_ID(), "LockMemtableMgr")))) {
    LOG_WARN("lock memtable mgr lock init error", K(ret), "tenant_id", MTL_ID());
  }
  UNUSED(ret);
}

int ObLockMemtableMgr::init(
    const common::ObTabletID &tablet_id,
    const ObLSID &ls_id,
    ObFreezer *freezer,
    ObTenantMetaMemMgr *t3m)
{
  UNUSEDx(tablet_id);
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("lock memtable mgr init twice.", K(ret), K_(ls_id));
  } else if (OB_UNLIKELY(!ls_id.is_valid()) ||
             OB_ISNULL(freezer) ||
             OB_ISNULL(t3m)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), KP(freezer), KP(t3m));
  } else if (!lock_def_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("lock memtable mgr lock not init", K(ret), K(tablet_id), K(ls_id));
  } else {
    ls_id_ = ls_id;
    freezer_ = freezer;
    t3m_ = t3m;
    table_type_ = ObITable::TableType::LOCK_MEMTABLE;
    is_inited_ = true;
    LOG_INFO("lock memtable mgr init successfully", K(ls_id), K(tablet_id), K(this));
  }
  return ret;
}

void ObLockMemtableMgr::destroy()
{
  reset();
}

void ObLockMemtableMgr::reset()
{
  MemMgrWLockGuard lock_guard(lock_);
  reset_tables();
  freezer_ = NULL;
  is_inited_ = false;
}

int ObLockMemtableMgr::create_memtable(const SCN clog_checkpoint_scn,
                                       const int64_t schema_version,
                                       const SCN newest_clog_checkpoint_scn,
                                       const bool for_replay)
{
  UNUSED(clog_checkpoint_scn);
  UNUSED(schema_version);
  UNUSED(newest_clog_checkpoint_scn);
  UNUSED(for_replay);

  int ret = OB_SUCCESS;
  ObTableHandleV2 handle;
  ObITable::TableKey table_key;
  ObITable *table = nullptr;
  ObLockMemtable *memtable = nullptr;
  ObLSTxService *ls_tx_svr = nullptr;

  MemMgrWLockGuard lock_guard(lock_);

  table_key.table_type_ = ObITable::LOCK_MEMTABLE;
  table_key.tablet_id_ = LS_LOCK_TABLET;

  table_key.scn_range_.start_scn_ = SCN::base_scn();//fake
  table_key.scn_range_.end_scn_ = SCN::plus(table_key.scn_range_.start_scn_, 1);//fake

  if (get_memtable_count_() > 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lock memtable already exists, should not create again", K(ret), K_(ls_id));
  } else if (OB_FAIL(t3m_->acquire_lock_memtable(handle))) {
    LOG_WARN("failed to create memtable", K(ret));
  } else if (OB_ISNULL(table = handle.get_table())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("table is nullptr", K(ret));
  } else if (OB_ISNULL(memtable = static_cast<ObLockMemtable *>(table))) {
    LOG_ERROR("this is not lock memtable", K(ret), KPC(table));
  } else if (OB_FAIL(memtable->init(table_key, ls_id_, freezer_))) {
    LOG_WARN("memtable init fail.", K(ret));
  } else if (OB_FAIL(add_memtable_(handle))) {
    LOG_WARN("add memtable fail.", K(ret));
  } else if (OB_ISNULL(ls_tx_svr = freezer_->get_ls_tx_svr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls_tx_svr is null", K(ret));
  } else if (OB_FAIL(ls_tx_svr->register_common_checkpoint(checkpoint::LOCK_MEMTABLE_TYPE, memtable))) {
    LOG_WARN("lock memtable register_common_checkpoint failed", K(ret), K(ls_id_));
  } else {
    LOG_INFO("create lock memtable successfully", K_(ls_id), K(memtable), KPC(this));
  }

  return ret;
}

const ObLockMemtable *ObLockMemtableMgr::get_memtable_(const int64_t pos) const
{
  int ret = OB_SUCCESS;
  const ObIMemtable *imemtable = tables_[get_memtable_idx(pos)];
  const ObLockMemtable *memtable = nullptr;
  if (OB_ISNULL(imemtable)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (!imemtable->is_lock_memtable()) {
    ret = OB_ENTRY_NOT_EXIST;
    STORAGE_LOG(WARN, "not lock memtable", K(ret), K(imemtable->get_key()));
  } else {
    memtable = static_cast<const ObLockMemtable*>(imemtable);
  }

  return memtable;
}

int64_t ObLockMemtableMgr::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;

  if (OB_ISNULL(buf) || buf_len <= 0) {
  } else {
    J_OBJ_START();
    J_ARRAY_START();
    for (int64_t i = memtable_head_; i < memtable_tail_; ++i) {
      const ObLockMemtable *memtable = get_memtable_(i);
      if (nullptr != memtable) {
        J_OBJ_START();
        J_KV(K(i), "table_key", memtable->get_key(), "ref", memtable->get_ref());
        J_OBJ_END();
        J_COMMA();
      }
    }
    J_ARRAY_END();
    J_OBJ_END();
  }
  return pos;
}

int ObLockMemtableMgr::unregister_from_common_checkpoint_(const ObLockMemtable *memtable)
{
  int ret = OB_SUCCESS;
  ObLSTxService *ls_tx_svr = nullptr;
  if (OB_ISNULL(ls_tx_svr = freezer_->get_ls_tx_svr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls_tx_svr is null", K(ret));
  } else if (OB_FAIL(ls_tx_svr->unregister_common_checkpoint(checkpoint::LOCK_MEMTABLE_TYPE,
                                                             memtable))) {
    LOG_WARN("lock memtable unregister_common_checkpoint failed", K(ret), K(ls_id_), K(memtable));
  } else {
    LOG_INFO("unregister from common checkpoint successfully", K_(ls_id), K(memtable));
  }
  return ret;
}

int ObLockMemtableMgr::release_head_memtable_(memtable::ObIMemtable *imemtable,
                                              const bool force)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  LOG_INFO("lock memtable mgr release head memtable", KP(imemtable), K(force));
  ObLockMemtable *memtable = static_cast<ObLockMemtable *>(imemtable);
  if (get_memtable_count_() > 0 && force) {
    // for force
    const int64_t idx = get_memtable_idx(memtable_head_);
    if (nullptr != tables_[idx] && memtable == tables_[idx]) {
      LOG_INFO("release head memtable", K(ret), K_(ls_id), KP(memtable));
      if (OB_TMP_FAIL(unregister_from_common_checkpoint_(memtable))) {
        LOG_WARN("unregister from common checkpoint failed", K(tmp_ret), K_(ls_id), K(memtable));
      }
      release_head_memtable();
      FLOG_INFO("succeed to release head lock table memtable", K(ret),
                K_(ls_id), KP(imemtable), K(memtable_head_), K(memtable_tail_));
    }
  } else if (!force) {
    // just for flush
    ret = memtable->on_memtable_flushed();
  }
  return ret;
}

} // tablelock
} // transaction
} // oceanbase
