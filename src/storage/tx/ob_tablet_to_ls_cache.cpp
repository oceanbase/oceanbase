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

#include "storage/tx/ob_tablet_to_ls_cache.h"

namespace oceanbase
{
namespace transaction
{
int ObTabletToLSCache::init(int64_t tenant_id, ObTxCtxMgr *tx_ctx_mgr)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    TRANS_LOG(WARN, "ObTabletToLSCache init twice", KR(ret), K(tenant_id), K(tx_ctx_mgr));
  } else if (!is_valid_tenant_id(tenant_id) || OB_ISNULL(tx_ctx_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id), K(tx_ctx_mgr));
  } else if (OB_FAIL(map_.init(lib::ObMemAttr(tenant_id, lib::ObLabel("TabletToLS"))))) {
    TRANS_LOG(WARN, "map init fail", KR(ret), K(tenant_id), K(tx_ctx_mgr));
  } else {
    tx_ctx_mgr_ = tx_ctx_mgr;
    is_inited_ = true;
    TRANS_LOG(INFO, "ObTabletToLSCache init success", KR(ret), K(tenant_id), KPC(this));
  }
  return ret;
}

void ObTabletToLSCache::destroy()
{
  if (is_inited_) {
    map_.destroy();
    tx_ctx_mgr_ = NULL;
    is_inited_ = false;
    TRANS_LOG(INFO, "ObTabletToLSCache destroy");
  }
}

void ObTabletToLSCache::reset()
{
  if (is_inited_) {
    map_.reset();
    tx_ctx_mgr_ = NULL;
    is_inited_ = false;
    TRANS_LOG(INFO, "ObTabletToLSCache reset");
  }
}

int ObTabletToLSCache::create_tablet(const common::ObTabletID &tablet_id, const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ObLSTxCtxMgr *ls_tx_ctx_mgr = NULL;
  ObTabletLSNode *ls_cache = NULL;
  ObTimeGuard tg("ObTabletToLSCache::create_tablet", 5 * 1000); // 5ms

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObTabletToLSCache has not inited", KR(ret), K(tablet_id), K(ls_id), KPC(this), K(lbt()));
  } else if (!tablet_id.is_valid() || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tablet_id), K(ls_id));
  } else if (OB_FAIL(tx_ctx_mgr_->get_ls_tx_ctx_mgr(ls_id, ls_tx_ctx_mgr))) {
    TRANS_LOG(WARN, "get ls tx ctx mgr fail", KR(ret), K_(tx_ctx_mgr), K(tablet_id), K(ls_id));
  } else if (OB_ISNULL(ls_tx_ctx_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected ls_tx_ctx_mgr is null ", KR(ret), K(tablet_id), K(ls_id));
  } else {
    if (OB_FAIL(map_.alloc_value(ls_cache))) {
      TRANS_LOG(WARN, "alloc ls cache fail", KR(ret), K(tablet_id), K(ls_id));
    } else {
      if (OB_FAIL(ls_cache->init(tablet_id, ls_tx_ctx_mgr, tx_ctx_mgr_))) {
        TRANS_LOG(WARN, "init ls cache fail", KR(ret), K(tablet_id), K(ls_id), KP(ls_tx_ctx_mgr));
      } else if (OB_FAIL(map_.insert(tablet_id, ls_cache))) {
        tg.click();
        if (OB_ENTRY_EXIST == ret) {
          // try to delete old and insert new again
          static const int MAX_RETRY_CNT = 10;
          int loop_cnt = 0;
          ObTabletLSNode *ls_cache_old = NULL;
          while (OB_ENTRY_EXIST == ret) {
            tg.click();
            if (OB_FAIL(map_.get(tablet_id, ls_cache_old))) {
              if (OB_ENTRY_NOT_EXIST == ret) {
                // entry not exist: insert
                ret = map_.insert(tablet_id, ls_cache);
              } else {
                // fail
              }
            } else {
              // entry exist: delete old and insert new
              if (OB_FAIL(map_.del(tablet_id, ls_cache_old)) && OB_ENTRY_NOT_EXIST != ret) {
                // fail
              } else {
                // entry deleted or not exist: insert
                ret = map_.insert(tablet_id, ls_cache);
              }
              // call revert after get from map
              map_.revert(ls_cache_old);
            }
            if (++loop_cnt > MAX_RETRY_CNT) {
              ret = OB_EAGAIN;
              TRANS_LOG(WARN, "insert retry too much times", KR(ret), K(tablet_id), K(ls_cache), K(lbt()));
            }
          }
        }
        if (OB_FAIL(ret)) {
          TRANS_LOG(WARN, "map insert fail", KR(ret), K(tablet_id), K(ls_cache), KP(ls_tx_ctx_mgr));
          // call clear after init if failed
          ls_cache->clear();
        }
        tg.click();
      }
      if (OB_FAIL(ret)) {
        // call free after alloc if failed
        map_.free_value(ls_cache);
      }
    }
    if (OB_FAIL(ret)) {
      // call revert after get ls_tx_ctx_mgr if failed
      tx_ctx_mgr_->revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr);
      if (OB_ENTRY_EXIST == ret) {
        ret = OB_SUCCESS;
      }
    }
  }
  TRANS_LOG(INFO, "create tablet cache", KR(ret), K(tablet_id), K(ls_id));

  return ret;
}

int ObTabletToLSCache::remove_tablet(const common::ObTabletID &tablet_id, const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ObTabletLSNode *ls_cache = NULL;
  ObTimeGuard tg("ObTabletToLSCache::remove_tablet", 5 * 1000); // 5ms

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObTabletToLSCache has not inited", KR(ret), K(tablet_id), K(ls_id), KPC(this), K(lbt()));
  } else if (!tablet_id.is_valid() || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tablet_id), K(ls_id));
  } else if (OB_FAIL(map_.get(tablet_id, ls_cache))) {
    // do nothing
  } else {
    if (ls_cache->get_ls_tx_ctx_mgr()->get_ls_id() == ls_id) {
      ret = map_.del(tablet_id, ls_cache);
    }
    // call revert after get from map
    map_.revert(ls_cache);
  }
  if (OB_FAIL(ret)) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      TRANS_LOG(ERROR, "remove tablet cache fail", KR(ret), K(tablet_id), K(ls_id));
    }
  }
  TRANS_LOG(INFO, "remove tablet cache", KR(ret), K(tablet_id), K(ls_id));

  return ret;
}

int ObTabletToLSCache::remove_ls_tablets(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  RemoveLSTabletFunctor functor(&map_, ls_id);
  ObTimeGuard tg("ObTabletToLSCache::remove_ls_tablets", 1000 * 1000); // 1s

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObTabletToLSCache has not inited", KR(ret), K(ls_id), KPC(this), K(lbt()));
  } else if (!ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(ls_id));
  } else if (OB_FAIL(map_.remove_if(functor))) {
    TRANS_LOG(ERROR, "remove ls tablets cache fail", KR(ret), K(ls_id));
  }
  TRANS_LOG(INFO, "remove ls tablets cache", KR(ret), K(ls_id));

  return ret;
}

int ObTabletToLSCache::check_and_get_ls_info(const common::ObTabletID &tablet_id,
                          share::ObLSID &ls_id,
                          bool &is_local_leader)
{
  int ret = OB_SUCCESS;
  ObTabletLSNode *ls_cache = NULL;
  ObLSTxCtxMgr *ls_tx_ctx_mgr = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObTabletToLSCache has not inited", KR(ret), K(tablet_id), KPC(this), K(lbt()));
  } else if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tablet_id));
  } else if (OB_FAIL(map_.get(tablet_id, ls_cache))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      TRANS_LOG(WARN, "get ls cache fail", KR(ret), K(tablet_id));
    }
  } else {
    if (OB_ISNULL(ls_tx_ctx_mgr = ls_cache->get_ls_tx_ctx_mgr())) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected ls_tx_ctx_mgr is null", KR(ret), K(tablet_id));
    } else {
      ls_id = ls_tx_ctx_mgr->get_ls_id();
      is_local_leader = ls_tx_ctx_mgr->is_master();
    }
    // call revert after get from map
    map_.revert(ls_cache);
  }
  TRANS_LOG(DEBUG, "check and get ls info", K(tablet_id), K(ls_id), K(is_local_leader), K(ret));
  return ret;
}

int64_t ObTabletToLSCache::size()
{
  return map_.get_total_cnt();
}

} // transaction
} // oceanbase
