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

#ifndef OCEANBASE_TRANSACTION_XA_CTX_MGR_
#define OCEANBASE_TRANSACTION_XA_CTX_MGR_

#include "share/ob_errno.h"
#include "lib/lock/ob_tc_rwlock.h"
#include "lib/hash/ob_link_hashmap.h"
//#include "ob_trans_define.h"
// #include "ob_xa_define.h"
#include "ob_xa_ctx.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{

namespace transaction
{
typedef common::LinkHashNode<ObTransID> XACtxHashNode;

class XACtxAlloc
{
public:
  ObXACtx* alloc_value() { return NULL; }
  void free_value(ObXACtx* ctx);
  XACtxHashNode* alloc_node(ObXACtx* node)
  {
    UNUSED(node);
    return op_reclaim_alloc(XACtxHashNode);
  }
  void free_node(XACtxHashNode* node)
  {
    if (NULL != node) {
      op_reclaim_free(node);
      node = NULL;
    }
  }
};

static const int64_t XA_CTX_MGR_SHRINK_THRESHOLD = 128;
typedef common::ObLinkHashMap<ObTransID, ObXACtx, XACtxAlloc, common::RefHandle, XA_CTX_MGR_SHRINK_THRESHOLD> XACtxMap;

class ObXACtxMgr
{
public:
  ObXACtxMgr(): ctx_map_(1<<10) { reset(); }
  virtual ~ObXACtxMgr() { destroy(); }
  int init();
  void destroy();
  void reset();
public:
  int get_xa_ctx(const ObTransID &trans_id, bool &alloc, ObXACtx*& ctx);
  int revert_xa_ctx(ObXACtx *ctx);
  int erase_xa_ctx(const ObTransID &trans_id);
  // this is used to add ref of ctx only
  int acquire_ctx_ref(const ObTransID &trans_id);
  // this is used to dec ref of ctx only
  int release_ctx_ref(ObXACtx *ctx);
  // this should be called in destroy of xa ctx
  void inc_total_ctx_count() { (void)ATOMIC_AAF(&total_ctx_count_, 1); }
  int64_t des_total_ctx_count() { return ATOMIC_AAF(&total_ctx_count_, -1); }
  int xa_scheduler_hb_req();
  int check_scheduler_exist(const ObTransID &trans_id);
  TO_STRING_KV(KP(this), K_(total_ctx_count));
private:
  DISALLOW_COPY_AND_ASSIGN(ObXACtxMgr);
private:
  int get_xa_ctx_(const ObTransID &trans_id, bool &alloc, ObXACtx*& xa_ctx);
  int release_all_xa_ctx_();
private:
  bool is_inited_;
  XACtxMap ctx_map_;
  int64_t total_ctx_count_;
};

class ReleaseAllXACtxFunctor
{
public:
  ReleaseAllXACtxFunctor() : released_count_(0) {}
  int64_t get_released_count() const { return released_count_; }
  bool operator()(const ObTransID &trans_id, ObXACtx *ctx_base)
  {
    bool bool_ret = false;
    int ret = common::OB_SUCCESS;

    if (!trans_id.is_valid() || OB_ISNULL(ctx_base)) {
      TRANS_LOG(WARN, "invalid argument", K(trans_id), KP(ctx_base));
    } else {
      if (OB_FAIL(ctx_base->kill())) {
        TRANS_LOG(WARN, "kill xa trans error", KR(ret), K(*ctx_base));
      } else {
        TRANS_LOG(WARN, "kill xa trans success", K(*ctx_base));
      }
      ++released_count_;
      bool_ret = true;
    }

    return bool_ret;
  }
private:
  int64_t released_count_;
};

class ObXAScheHbFunctor
{
public:
  ObXAScheHbFunctor() {}
  ~ObXAScheHbFunctor() {}
  bool operator()(const ObTransID &trans_id, ObXACtx *xa_ctx)
  {
    // always return true so as to traverse all ctx
    int tmp_ret = common::OB_SUCCESS;
    if (!trans_id.is_valid() || OB_ISNULL(xa_ctx)) {
      TRANS_LOG_RET(WARN, common::OB_INVALID_ARGUMENT, "invalid argument", K(trans_id), KP(xa_ctx));
    } else {
      bool is_original = (GCTX.self_addr() == xa_ctx->get_original_sche_addr());
      if (is_original) {
        if (OB_SUCCESS != (tmp_ret = xa_ctx->try_heartbeat())) {
          TRANS_LOG_RET(WARN, tmp_ret, "xa scheduler hb failed", K(tmp_ret), K(*xa_ctx));
        }
      }
    }
    return true;
  }
};

}//transaction
}//oceanbase

#endif
