/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBRPC_OB_RPC_MEM_POOL_H_
#define OCEANBASE_OBRPC_OB_RPC_MEM_POOL_H_
#include "lib/allocator/ob_malloc.h"

namespace oceanbase
{
namespace obrpc
{
class ObRpcMemPool
{
public:
  enum {
    RPC_POOL_PAGE_SIZE = (1<<14) - 128,
    RPC_CACHE_SIZE = 3968
  };
  struct Page;
  explicit ObRpcMemPool(): last_(NULL), tenant_id_(OB_INVALID_TENANT_ID), mem_label_("RpcDefault") {}
  explicit ObRpcMemPool(int64_t tenant_id, const char* label): last_(NULL), tenant_id_(tenant_id), mem_label_(label) {}
  ~ObRpcMemPool() { destroy(); }
  static ObRpcMemPool* create(int64_t tenant_id, const char* label, int64_t req_sz, int64_t cache_sz = ObRpcMemPool::RPC_POOL_PAGE_SIZE);
  void* alloc(int64_t sz);
  void set_tenant_id(int64_t tenant_id) { tenant_id_ = tenant_id; }
  void reuse();
  void destroy();
private:
  void add_page(Page* page);
private:
  Page* last_;
  int64_t tenant_id_;
  const char* mem_label_;
};

}; // end namespace obrpc
}; // end namespace oceanbase

#endif /* OCEANBASE_OBRPC_OB_RPC_MEM_POOL_H_ */
