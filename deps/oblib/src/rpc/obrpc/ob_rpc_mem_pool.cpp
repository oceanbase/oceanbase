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

#define USING_LOG_PREFIX RPC_OBRPC
#include "rpc/obrpc/ob_rpc_mem_pool.h"


namespace oceanbase
{
namespace obrpc
{

struct ObRpcMemPool::Page
{
  Page(int64_t limit): next_(NULL), limit_(limit - sizeof(*this)), cur_(0) {}
  ~Page() {}
  void* alloc(int64_t sz) {
    void* ret = NULL;
    if (cur_ + sz <= limit_) {
      ret = base_ + cur_;
      cur_ += sz;
    }
    return ret;
  }
  void reset() { cur_ = 0; }
  Page* next_;
  int64_t limit_;
  int64_t cur_;
  char base_[];
};
static void* rpc_mem_pool_direct_alloc(int64_t tenant_id, const char* label, int64_t sz) {
  if (OB_INVALID_TENANT_ID == tenant_id) {
    tenant_id = OB_SERVER_TENANT_ID;
  }
  ObMemAttr attr(tenant_id, label, common::ObCtxIds::RPC_CTX_ID);
  lib::ObTenantCtxAllocatorGuard allocator = lib::ObMallocAllocator::get_instance()->get_tenant_ctx_allocator(tenant_id, common::ObCtxIds::RPC_CTX_ID);
  if (OB_ISNULL(allocator)) {
    attr.tenant_id_ = OB_SERVER_TENANT_ID;
  }
  return common::ob_malloc(sz, attr);
}
static void rpc_mem_pool_direct_free(void* p) { common::ob_free(p); }
static ObRpcMemPool::Page* rpc_mem_pool_create_page(int64_t tenant_id, const char* label, int64_t sz) {
  int64_t alloc_sz = std::max(sizeof(ObRpcMemPool::Page) + sz, (uint64_t)ObRpcMemPool::RPC_POOL_PAGE_SIZE);
  ObRpcMemPool::Page* page = (typeof(page))rpc_mem_pool_direct_alloc(tenant_id, label, alloc_sz);
  if (OB_ISNULL(page)) {
    LOG_WARN_RET(common::OB_ALLOCATE_MEMORY_FAILED, "rpc memory pool alloc memory failed", K(sz), K(alloc_sz));
  } else {
    new(page)ObRpcMemPool::Page(alloc_sz);
  }
  return page;
}
static void rpc_mem_pool_destroy_page(ObRpcMemPool::Page* page) {
  if (OB_NOT_NULL(page)) {
    page->ObRpcMemPool::Page::~Page();
    common::ob_free(page);
  }
}

ObRpcMemPool* ObRpcMemPool::create(int64_t tenant_id, const char* label, int64_t req_sz)
{
  Page* page = nullptr;
  ObRpcMemPool* pool = nullptr;
  if (OB_NOT_NULL(page = rpc_mem_pool_create_page(tenant_id, label, req_sz + sizeof(ObRpcMemPool)))) {
    if (OB_NOT_NULL(pool = (typeof(pool))page->alloc(sizeof(ObRpcMemPool)))) {
      new(pool)ObRpcMemPool(tenant_id, label); // can not be null
      pool->add_page(page);
    } else {
      rpc_mem_pool_destroy_page(page);
    }
  }
  return pool;
}

void* ObRpcMemPool::alloc(int64_t sz)
{
  void* ret = NULL;
  Page* page = NULL;
  if (NULL != last_ && NULL != (ret = last_->alloc(sz))) {
  } else if (NULL == (page = rpc_mem_pool_create_page(tenant_id_, mem_label_, sz))) {
  } else {
    ret = page->alloc(sz);
    add_page(page);
  }
  return ret;
}

void ObRpcMemPool::destroy()
{
  Page* cur = last_;
  last_ = NULL;
  while(NULL != cur) {
    Page* next = cur->next_;
    rpc_mem_pool_direct_free(cur);
    cur = next;
  }
}

void ObRpcMemPool::reuse()
{
  Page* cur = last_;
  Page* next = NULL;
  last_ = NULL;
  while(NULL != cur && NULL != (next = cur->next_)) {
    rpc_mem_pool_direct_free(cur);
    cur = next;
  }
  if (NULL != cur) {
    cur->reset();
    last_ = cur;
  }
}

void ObRpcMemPool::add_page(Page* page)
{
  page->next_ = last_;
  last_ = page;
}

}; // end namespace obrpc
}; // end namespace oceanbase
