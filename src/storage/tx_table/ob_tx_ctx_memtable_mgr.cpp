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

#include "storage/tx_table/ob_tx_ctx_memtable_mgr.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/ls/ob_ls_tx_service.h"
#include "storage/ls/ob_freezer.h"


namespace oceanbase
{
using namespace share;
using namespace palf;
namespace storage
{

int ObTxCtxMemtableMgr::init(const common::ObTabletID &tablet_id,
                             const ObLSID &ls_id,
                             ObFreezer *freezer,
                             ObTenantMetaMemMgr *t3m)
{
  UNUSED(tablet_id);
  UNUSED(freezer);

  int ret = OB_SUCCESS;

  ls_id_ = ls_id;
  freezer_ = freezer;
  t3m_ = t3m;
  table_type_ = ObITable::TableType::TX_CTX_MEMTABLE;
  is_inited_ = true;
  LOG_INFO("tx ctx memtable mgr init successfully", K(ls_id), K(tablet_id), K(this));

  return ret;
}

void ObTxCtxMemtableMgr::destroy()
{
  reset();
}

void ObTxCtxMemtableMgr::reset()
{
  MemMgrWLockGuard lock_guard(lock_);
  reset_tables();
  freezer_ = NULL;
  is_inited_ = false;
}

int ObTxCtxMemtableMgr::create_memtable(const SCN last_replay_scn,
                                        const int64_t schema_version,
                                        const SCN newest_clog_checkpoint_scn,
                                        const bool for_replay)
{
  UNUSED(last_replay_scn);
  UNUSED(schema_version);
  UNUSED(newest_clog_checkpoint_scn);
  UNUSED(for_replay);

  int ret = OB_SUCCESS;
  ObTableHandleV2 handle;
  ObITable::TableKey table_key;
  ObITable *table = nullptr;
  ObTxCtxMemtable *tx_ctx_memtable = nullptr;
  ObLSTxService *ls_tx_svr = nullptr;

  MemMgrWLockGuard lock_guard(lock_);

  table_key.table_type_ = ObITable::TX_CTX_MEMTABLE;
  table_key.tablet_id_ = ObTabletID(ObTabletID::LS_TX_CTX_TABLET_ID);
  table_key.scn_range_.start_scn_ = SCN::base_scn();
  table_key.scn_range_.end_scn_ = SCN::plus(table_key.scn_range_.start_scn_, 1);

  // TODO: Donot use pool to create the only memtable
  if (get_memtable_count_() > 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tx ctx memtable already exists, should not create again", K(ret), K_(ls_id));
  } else if (OB_FAIL(t3m_->acquire_tx_ctx_memtable(handle))) {
    LOG_WARN("failed to create memtable", K(ret));
  } else if (OB_ISNULL(table = handle.get_table())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "table is nullptr", K(ret));
  } else {
    tx_ctx_memtable = dynamic_cast<ObTxCtxMemtable *>(table);
    if (NULL == tx_ctx_memtable) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "invalid tx_ctx_memtable", K(ret), KPC(table));
    } else if (OB_FAIL(tx_ctx_memtable->init(table_key, ls_id_))) {
      LOG_WARN("memtable init fail.", KR(ret));
    } else if (OB_FAIL(add_memtable_(handle))) {
      LOG_WARN("add memtable fail.", KR(ret));
    } else if (OB_ISNULL(ls_tx_svr = freezer_->get_ls_tx_svr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls_tx_svr is null", K(ret));
    } else if (OB_FAIL(ls_tx_svr->register_common_checkpoint(checkpoint::TX_CTX_MEMTABLE_TYPE, tx_ctx_memtable))) {
      LOG_WARN("tx ctx memtable register_common_checkpoint failed", K(ret), K(ls_id_));
    } else {
      LOG_INFO("tx ctx memtable mgr create memtable successfully",
               K(ls_id_), KPC(tx_ctx_memtable));
    }
  }

  return ret;
}

const ObTxCtxMemtable *ObTxCtxMemtableMgr::get_tx_ctx_memtable_(const int64_t pos) const
{
  int ret = OB_SUCCESS;
  const memtable::ObIMemtable *imemtable = tables_[get_memtable_idx(pos)];
  const ObTxCtxMemtable *memtable = nullptr;
  if (OB_ISNULL(imemtable)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (!imemtable->is_tx_ctx_memtable()) {
    ret = OB_ENTRY_NOT_EXIST;
    STORAGE_LOG(WARN, "not tx ctx memtable", K(ret), K(imemtable->get_key()));
  } else {
    memtable = static_cast<const ObTxCtxMemtable*>(imemtable);
  }
  return memtable;
}

int64_t ObTxCtxMemtableMgr::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;

  if (OB_ISNULL(buf) || buf_len <= 0) {
  } else {
    MemMgrRLockGuard lock_guard(lock_);
    J_OBJ_START();
    J_ARRAY_START();
    for (int64_t i = memtable_head_; i < memtable_tail_; ++i) {
      const ObTxCtxMemtable *memtable = get_tx_ctx_memtable_(i);
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

int ObTxCtxMemtableMgr::unregister_from_common_checkpoint_(const ObTxCtxMemtable *memtable)
{
  int ret = OB_SUCCESS;
  ObLSTxService *ls_tx_svr = nullptr;
  if (OB_ISNULL(ls_tx_svr = freezer_->get_ls_tx_svr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls_tx_svr is null", K(ret));
  } else if (OB_FAIL(ls_tx_svr->unregister_common_checkpoint(checkpoint::TX_CTX_MEMTABLE_TYPE,
                                                             memtable))) {
    LOG_WARN("tx ctx unregister_common_checkpoint failed", K(ret), K(ls_id_), K(memtable));
  } else {
    LOG_INFO("unregister from common checkpoint successfully", K_(ls_id), K(memtable));
  }
  return ret;
}

int ObTxCtxMemtableMgr::release_head_memtable_(memtable::ObIMemtable *imemtable,
                                               const bool force)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  LOG_INFO("tx ctx memtable mgr release head memtable", K(*imemtable));
  ObTxCtxMemtable *memtable = static_cast<ObTxCtxMemtable *>(imemtable);
  if (get_memtable_count_() > 0 && force) {
    // for force
    const int64_t idx = get_memtable_idx(memtable_head_);
    if (nullptr != tables_[idx] && memtable == tables_[idx]) {
      LOG_INFO("release head memtable", K(ret), K_(ls_id), KP(memtable));
      if (OB_TMP_FAIL(unregister_from_common_checkpoint_(memtable))) {
        LOG_WARN("unregister from common checkpoint failed", K(tmp_ret), K_(ls_id), K(memtable));
      }
      release_head_memtable();
      FLOG_INFO("succeed to release tx ctx memtable", K(ret), K_(ls_id));
    }
  } else if (!force) {
    // just for flush
    ret = memtable->on_memtable_flushed();
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase

