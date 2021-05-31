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

#include "lib/allocator/ob_pcounter.h"

#include "storage/memtable/ob_memtable_context.h"
#include "storage/memtable/ob_memtable.h"

namespace oceanbase {
using namespace common;
namespace memtable {
ObMemtableCtxFactory::ObMemtableCtxFactory()
    : is_inited_(false),
      mod_(ObModIds::OB_MEMTABLE_CTX_OBJECT),
      ctx_obj_allocator_(OBJ_ALLOCATOR_PAGE, mod_),
      ctx_dynamic_allocator_(),
      malloc_allocator_(ObModIds::OB_MEMTABLE_CTX),
      free_list_(),
      alloc_count_(0),
      free_count_(0)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ctx_dynamic_allocator_.init(DYNAMIC_ALLOCATOR_PAGE,
          ObModIds::OB_MEMTABLE_CTX,
          common::OB_SERVER_TENANT_ID,
          DYNAMIC_ALLOCATOR_PAGE_NUM))) {
    TRANS_LOG(ERROR, "ctx dynamic allocator init fail", K(ret));
  } else if (OB_FAIL(free_list_.init(MAX_CTX_HOLD_COUNT))) {
    TRANS_LOG(ERROR, "free list init fail", K(ret));
  } else if (OB_FAIL(id_map_.init(MAX_CTX_COUNT))) {
    TRANS_LOG(ERROR, "id map init fail", K(ret));
  } else {
    static ObMemtableCtx dummy_ctx;
    if (OB_FAIL(id_map_.assign(&dummy_ctx, ObMemtableCtx::UID_FOR_PURGE))) {
      TRANS_LOG(ERROR, "assign id to dummy_ctx fail", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < MAX_CTX_HOLD_COUNT; ++i) {
      ObIMemtableCtx* ctx = NULL;
      void* ctx_buffer = ctx_obj_allocator_.alloc(sizeof(ObMemtableCtx));
      if (NULL == ctx_buffer) {
        TRANS_LOG(ERROR, "ctx obj allocator alloc fail");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else if (NULL == (ctx = new (ctx_buffer) ObMemtableCtx(id_map_, ctx_dynamic_allocator_))) {
        TRANS_LOG(ERROR, "new ObMemtableCtx fail");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else if (OB_FAIL(free_list_.push(ctx))) {
        TRANS_LOG(ERROR, "free list push fail", K(ret));
      } else {
        ctx->set_alloc_type(CTX_ALLOC_FIX);
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObMemAttr attr;
    attr.label_ = ObModIds::OB_MEMTABLE_CTX;
    ctx_dynamic_allocator_.set_attr(attr);
    is_inited_ = true;
  }
}

ObMemtableCtxFactory::~ObMemtableCtxFactory()
{
  is_inited_ = false;
  ObIMemtableCtx* ctx = NULL;
  while (OB_SUCCESS == free_list_.pop(ctx)) {
    if (NULL != ctx) {
      int alloc_type = ctx->get_alloc_type();
      ctx->~ObIMemtableCtx();
      if (CTX_ALLOC_VAR == alloc_type) {
        ctx_dynamic_allocator_.free(ctx);
      }
      ctx = NULL;
    }
  }
  id_map_.destroy();
  free_list_.destroy();
  ctx_dynamic_allocator_.destroy();
}

ObIMemtableCtx* ObMemtableCtxFactory::alloc(const uint64_t tenant_id /* OB_SERVER_TENANT_ID */)
{
  ObIMemtableCtx* ctx = NULL;
  int tmp_ret = free_list_.pop(ctx);
  if (OB_SUCCESS != tmp_ret || NULL == ctx) {
    void* ctx_buffer = ctx_dynamic_allocator_.alloc(sizeof(ObMemtableCtx));
    if (NULL == ctx_buffer) {
      TRANS_LOG(WARN, "ctx obj allocator alloc fail");
    } else if (NULL == (ctx = new (ctx_buffer) ObMemtableCtx(id_map_, ctx_dynamic_allocator_))) {
      TRANS_LOG(WARN, "new ObMemtableCtx fail");
    } else {
      ctx->set_alloc_type(CTX_ALLOC_VAR);
    }
  }
  if (NULL != ctx) {
    if (OB_SUCCESS != (tmp_ret = static_cast<ObMemtableCtx*>(ctx)->init(tenant_id, id_map_, malloc_allocator_))) {
      TRANS_LOG(WARN, "memtable context init error", "ret", tmp_ret);
    } else {
      uint32_t ctx_descriptor = IDMAP_INVALID_ID;
      int tmp_ret = id_map_.assign(ctx, ctx_descriptor);
      if (OB_SUCCESS != tmp_ret) {
        TRANS_LOG(WARN, "id map assign fail", "ret", tmp_ret);
      } else {
        ctx->set_ctx_descriptor(ctx_descriptor);
      }
      if (OB_SUCCESS != tmp_ret) {
        free(ctx);
        ctx = NULL;
      }
    }
  }
  if (NULL != ctx) {
    PC_ADD(MTCTX, 1);
    (void)ATOMIC_AAF(&alloc_count_, 1);
    if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
      TRANS_LOG(INFO, "alloc memtable context", K_(alloc_count), K_(free_count));
    }
    GARL_ADD(&ctx->resource_link_, "memctx");
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
      PC_REPORT();
    }
  }
  return ctx;
}

void ObMemtableCtxFactory::free(ObIMemtableCtx* ctx)
{
  if (NULL != ctx) {
    GARL_DEL(&ctx->resource_link_);
    id_map_.erase(ctx->get_ctx_descriptor());
    ctx->reset();
    if (CTX_ALLOC_VAR == ctx->get_alloc_type()) {
      ctx->~ObIMemtableCtx();
      ctx_dynamic_allocator_.free(ctx);
    } else if (CTX_ALLOC_FIX == ctx->get_alloc_type()) {
      int tmp_ret = free_list_.push(ctx);
      if (OB_SUCCESS != tmp_ret) {
        TRANS_LOG(ERROR, "free list push fail, ctx will leek", "ret", tmp_ret, KP(ctx));
      }
    } else {
      TRANS_LOG(ERROR, "unknow ctx alloc type", "alloc_type", ctx->get_alloc_type());
    }
    ctx = NULL;
    (void)ATOMIC_AAF(&free_count_, 1);
  }
  PC_ADD(MTCTX, -1);
}

////////////////////////////////////////////////////////////////////////////////////////////////////

int64_t ObMemtableFactory::alloc_count_ = 0;
int64_t ObMemtableFactory::free_count_ = 0;

ObMemtableFactory::ObMemtableFactory()
{}

ObMemtableFactory::~ObMemtableFactory()
{}

ObMemtable* ObMemtableFactory::alloc(const uint64_t tenant_id)
{
  ObMemAttr memattr(tenant_id, ObModIds::OB_MEMTABLE_OBJECT, ObCtxIds::MEMSTORE_CTX_ID);
  ObMemtable* mt = NULL;
  void* mt_buffer = ob_malloc(sizeof(ObMemtable), memattr);
  if (NULL == mt_buffer) {
    TRANS_LOG(WARN, "alloc memory for memtable fail");
  } else if (NULL == (mt = new (mt_buffer) ObMemtable())) {
    TRANS_LOG(WARN, "construct object of memtable fail");
    ob_free(mt_buffer);
    mt_buffer = NULL;
  } else {
    PC_ADD(MT, 1);
    (void)ATOMIC_AAF(&alloc_count_, 1);
  }
  TRANS_LOG(INFO, "alloc memtable", KP(mt), K_(alloc_count), K_(free_count));
  return mt;
}

void ObMemtableFactory::free(ObMemtable* mt)
{
  if (NULL != mt) {
    mt->~ObMemtable();
    ob_free(mt);
    (void)ATOMIC_AAF(&free_count_, 1);
    TRANS_LOG(INFO, "free memtable", KP(mt), K_(alloc_count), K_(free_count));
    mt = NULL;
    PC_ADD(MT, -1);
  }
}

}  // namespace memtable
}  // namespace oceanbase
