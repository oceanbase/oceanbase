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

#ifndef OCEANBASE_TABLET_TO_LS_CACHE_
#define OCEANBASE_TABLET_TO_LS_CACHE_

#include "storage/tx_storage/ob_ls_map.h"

namespace oceanbase
{
namespace transaction
{

class ObTabletLSNode: public ObLightHashLink<ObTabletLSNode>
{
public:
  ObTabletLSNode() : ls_tx_ctx_mgr_(NULL), tx_ctx_mgr_(NULL) {}
  ~ObTabletLSNode() { reset(); }
  int init(const common::ObTabletID &tablet_id, ObLSTxCtxMgr *ls_tx_ctx_mgr, ObTxCtxMgr *tx_ctx_mgr)
  {
    int ret = OB_SUCCESS;
    if (!tablet_id.is_valid() || OB_ISNULL(ls_tx_ctx_mgr) || OB_ISNULL(tx_ctx_mgr)) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      tablet_id_ = tablet_id;
      ls_tx_ctx_mgr_ = ls_tx_ctx_mgr;
      tx_ctx_mgr_ = tx_ctx_mgr;
    }
    return ret;
  }
  void reset()
  {
    if (OB_NOT_NULL(ls_tx_ctx_mgr_) && OB_NOT_NULL(tx_ctx_mgr_)) {
      tx_ctx_mgr_->revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr_);
      ls_tx_ctx_mgr_ = NULL;
      tx_ctx_mgr_ = NULL;
    }
    tablet_id_.reset();
  }
  void clear()
  {
    ls_tx_ctx_mgr_ = NULL;
    tx_ctx_mgr_ = NULL;
    tablet_id_.reset();
  }
  ObLSTxCtxMgr *get_ls_tx_ctx_mgr() const
  {
    return ls_tx_ctx_mgr_;
  }
  bool contain(const common::ObTabletID &tablet_id) const
  {
    return tablet_id_ == tablet_id;
  }
  TO_STRING_KV(K_(tablet_id),KP_(ls_tx_ctx_mgr),KP_(tx_ctx_mgr),KP(this));

private:
  common::ObTabletID tablet_id_;
  ObLSTxCtxMgr *ls_tx_ctx_mgr_;
  ObTxCtxMgr *tx_ctx_mgr_;
};

class ObTabletLSNodeAlloc
{
public:
  ObTabletLSNode* alloc_value()
  {
    return op_alloc(ObTabletLSNode);
  }
  void free_value(ObTabletLSNode *val)
  {
    if (OB_NOT_NULL(val)) {
      op_free(val);
    }
  }
};

typedef ObLightHashMap<ObTabletID, ObTabletLSNode, ObTabletLSNodeAlloc, common::ObQSyncLock, 16 * 1024, 256> TabletToLSMap;

class RemoveLSTabletFunctor
{
public:
  RemoveLSTabletFunctor(TabletToLSMap *map, const share::ObLSID &ls_id) : map_(map), ls_id_(ls_id) {}
  bool operator() (ObTabletLSNode *val)
  {
    bool need_remove = false;
    if (OB_NOT_NULL(map_) && OB_NOT_NULL(val) && ls_id_ == val->get_ls_tx_ctx_mgr()->get_ls_id()) {
      need_remove = true;
    }
    return need_remove;
  }
private:
  TabletToLSMap *map_;
  share::ObLSID ls_id_;
};

class ObTabletToLSCache final
{
public:
  ObTabletToLSCache() : is_inited_(false), tx_ctx_mgr_(NULL), map_() { }
  ~ObTabletToLSCache() { destroy(); }

  int init(int64_t tenant_id, ObTxCtxMgr *tx_ctx_mgr);
  void destroy();
  void reset();
  int create_tablet(const common::ObTabletID &tablet_id, const share::ObLSID &ls_id);
  int remove_tablet(const common::ObTabletID &tablet_id, const share::ObLSID &ls_id);
  int remove_ls_tablets(const share::ObLSID &ls_id);
  int check_and_get_ls_info(const common::ObTabletID &tablet_id,
                            share::ObLSID &ls_id,
                            bool &is_local_leader);
  int64_t size();
  TO_STRING_KV(K_(is_inited),KP_(tx_ctx_mgr),KP(this));

private:
  bool is_inited_;
  ObTxCtxMgr *tx_ctx_mgr_;
  TabletToLSMap map_;
};

} // transaction
} // oceanbase

#endif // OCEANBASE_TABLET_TO_LS_CACHE_
