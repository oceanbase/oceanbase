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

#include "ob_xa_ctx_mgr.h"
#include "ob_xa_service.h"
//#include "ob_xa_ctx.h"

namespace oceanbase
{

using namespace common;

namespace transaction
{

void XACtxAlloc::free_value(ObXACtx* ctx)
{
  if (NULL != ctx) {
    ctx->destroy();
    op_reclaim_free(ctx);
    ctx = NULL;
    XA_ACTIVE_DECREMENT_XA_CTX_COUNT();
  }
}

int ObXACtxMgr::init()
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    TRANS_LOG(WARN, "ObXACtxMgr inited twice", KR(ret));
  } else if (OB_FAIL(ctx_map_.init("ObXACtxMgr"))) {
    TRANS_LOG(WARN, "xa trans ctx hash map create error", KR(ret));
  } else {
    is_inited_ = true;
  }

  return ret;
}

void ObXACtxMgr::reset()
{
  is_inited_ = false;
  total_ctx_count_ = 0;
}

void ObXACtxMgr::destroy()
{
  if (IS_INIT) {
    release_all_xa_ctx_();
    ctx_map_.destroy();
    is_inited_ = false;
    TRANS_LOG(INFO, "ObXACtxMgr destroyed");
  }
}

int ObXACtxMgr::get_xa_ctx(const ObTransID &trans_id, bool &alloc, ObXACtx*& ctx)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObXACtxMgr not inited", KR(ret));
  } else if (OB_UNLIKELY(!trans_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(trans_id));
  } else if (OB_FAIL(get_xa_ctx_(trans_id, alloc, ctx))) {
    TRANS_LOG(WARN, "get xa trans context error", KR(ret), K(trans_id));
  } else {
    // do nothing
  }

  return ret;
}

int ObXACtxMgr::revert_xa_ctx(ObXACtx *ctx)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "OBXACtxMgr not inited", KR(ret));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), KP(ctx));
  } else {
    ctx_map_.revert(ctx);
  }

  return ret;
}

int ObXACtxMgr::erase_xa_ctx(const ObTransID &trans_id)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!trans_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(trans_id));
  } else if (OB_FAIL(ctx_map_.del(trans_id))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      TRANS_LOG(WARN, "erase xa trans context from hashmap error",
          KR(ret), K(trans_id));
    }
  } else {
    // do nothing
  }

  return ret;
}

int ObXACtxMgr::get_xa_ctx_(const ObTransID &trans_id, bool &alloc, ObXACtx*& ctx)
{
  int ret = OB_SUCCESS;
  ObXACtx *tmp_ctx = NULL;
  int64_t count = 0;
  const int64_t MAX_LOOP_COUNT = 100;

  if (OB_SUCC(ctx_map_.get(trans_id, tmp_ctx))) {
    if (OB_ISNULL(tmp_ctx)) {
      TRANS_LOG(WARN, "ctx is NULL", "ctx", OB_P(tmp_ctx));
      ret = OB_ERR_UNEXPECTED;
    } else {
      bool is_inited = tmp_ctx->is_inited();
      while (!is_inited && count < MAX_LOOP_COUNT) {
        count++;
        ob_usleep(100);
        is_inited = tmp_ctx->is_inited();
      }
      if (!is_inited) {
        TRANS_LOG(WARN, "get xa trans context not inited", K(trans_id));
      }
      ctx = tmp_ctx;
      alloc = false;
    }
  } else if (OB_ENTRY_NOT_EXIST != ret) {
    TRANS_LOG(ERROR, "get xa trans context error", KR(ret), K(trans_id));
  } else {
    tmp_ctx = NULL;
    if (!alloc) {
      ret = OB_TRANS_CTX_NOT_EXIST;
    } else if (NULL == (tmp_ctx = op_reclaim_alloc(ObXACtx))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TRANS_LOG(WARN, "alloc xa trans context error", KR(ret), K(trans_id));
    // TODO: gjw228474, preset for tmp_ctx
    } else if (OB_FAIL(ctx_map_.insert_and_get(trans_id, tmp_ctx))) {
      op_reclaim_free(static_cast<ObXACtx*>(tmp_ctx));
      tmp_ctx = NULL;
      if (OB_ENTRY_EXIST != ret) {
        TRANS_LOG(WARN, "insert xa trans context error", KR(ret), K(trans_id));
      } else if (OB_SUCC(ctx_map_.get(trans_id, tmp_ctx))) {
        if (OB_ISNULL(tmp_ctx)) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(WARN, "ctx is NULL", KR(ret), "ctx", OB_P(tmp_ctx));
        } else {
          bool is_inited = tmp_ctx->is_inited();
          while (!is_inited && count < MAX_LOOP_COUNT) {
            count++;
            ob_usleep(100);
            is_inited = tmp_ctx->is_inited();
          }
          if (!is_inited) {
            TRANS_LOG(WARN, "get xa trans context not inited", K(trans_id));
          }
          ctx = tmp_ctx;
          alloc = false;
        }
      } else {
        // do nothing
      }
    } else {
      ctx = tmp_ctx;
      inc_total_ctx_count();
      XA_ACTIVE_INCREMENT_XA_CTX_COUNT();
    }
  }

  return ret;
}

int ObXACtxMgr::release_all_xa_ctx_()
{
  int ret = OB_SUCCESS;

  ReleaseAllXACtxFunctor fn;
  if (OB_FAIL(ctx_map_.remove_if(fn))) {
    TRANS_LOG(WARN, "for each xa trans context error", KR(ret), "manager", *this);
  } else {
    TRANS_LOG(DEBUG, "for each xa trans context success", "manager", *this,
        "released_count", fn.get_released_count());
  }

  return ret;
}

int ObXACtxMgr::acquire_ctx_ref(const ObTransID &trans_id)
{
  int ret = OB_SUCCESS;
  ObXACtx* ctx = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObXACtxMgr not inited", KR(ret));
  } else if (OB_UNLIKELY(!trans_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(trans_id));
  } else if (OB_FAIL(ctx_map_.get(trans_id, ctx))) {
    TRANS_LOG(WARN, "get xa trans context from hashmap error", KR(ret), K(trans_id));
  } else {
    // do nothing
  }

  return ret;
}

int ObXACtxMgr::release_ctx_ref(ObXACtx *ctx)
{
  int ret = OB_SUCCESS;
  
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObXACtxMgr not inited", KR(ret));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), KP(ctx));
  } else {
    ctx_map_.revert(ctx);
  }

  return ret;
}

int ObXACtxMgr::xa_scheduler_hb_req()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObXACtxMgr not inited", K(*this));
  } else {
    ObXAScheHbFunctor functor;
    if (OB_FAIL(ctx_map_.for_each(functor))) {
      TRANS_LOG(WARN, "for each transaction context error", K(ret), "manager", *this);
    }
  }
  return ret;
}

int ObXACtxMgr::check_scheduler_exist(const ObTransID &trans_id)
{
  int ret = OB_SUCCESS;
  ObXACtx* ctx = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObXACtxMgr not inited", KR(ret));
  } else if (OB_UNLIKELY(!trans_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(trans_id));
  } else if (OB_FAIL(ctx_map_.get(trans_id, ctx))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      TRANS_LOG(WARN, "get xa trans context from hashmap error", KR(ret), K(trans_id));
    } else {
      TRANS_LOG(INFO, "xa trans context doesn't exist", KR(ret), K(trans_id));
    }
  } else if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), KP(ctx));
  } else {
    ctx_map_.revert(ctx);
  }

  return ret;
}

}//transaction
}//oceanbase
