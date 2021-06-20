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

#ifndef _OB_MALLOC_ALLOCATOR_H_
#define _OB_MALLOC_ALLOCATOR_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/alloc/ob_tenant_ctx_allocator.h"
#include "lib/alloc/alloc_func.h"
#include "lib/lock/tbrwlock.h"

namespace oceanbase {
namespace lib {
class ObMemoryCutter;
// It's the implement of ob_malloc/ob_free/ob_realloc interface.  The
// class separates allocator for each tenant thus tenant vaolates each
// other.
class ObMallocAllocator : public common::ObIAllocator {
  friend class ObMemoryCutter;
  static const uint64_t PRESERVED_TENANT_COUNT = 10000;

public:
  ObMallocAllocator();
  virtual ~ObMallocAllocator();

  int init();

  void* alloc(const int64_t size);
  void* alloc(const int64_t size, const ObMemAttr& attr);
  void* realloc(const void* ptr, const int64_t size, const ObMemAttr& attr);
  void free(void* ptr);

  int set_root_allocator(ObTenantCtxAllocator* allocator);
  static ObMallocAllocator* get_instance();

  ObTenantCtxAllocator* get_tenant_ctx_allocator(uint64_t tenant_id, uint64_t ctx_id = 0) const;

  int create_tenant_ctx_allocator(uint64_t tenant_id, uint64_t ctx_id = 0);
  int delete_tenant_ctx_allocator(uint64_t tenant_id);

  IBlockMgr* get_tenant_ctx_block_mgr(uint64_t tenant_id, uint64_t ctx_id);

  // statistic relating
  void set_urgent(int64_t bytes);
  int64_t get_urgent() const;
  void set_reserved(int64_t bytes);
  int64_t get_reserved() const;
  static int set_tenant_limit(uint64_t tenant_id, int64_t bytes);
  static int64_t get_tenant_limit(uint64_t tenant_id);
  static int64_t get_tenant_hold(uint64_t tenant_id);
  static int64_t get_tenant_rpc_hold(uint64_t tenant_id);
  int64_t get_tenant_ctx_hold(const uint64_t tenant_id, const uint64_t ctx_id) const;
  void get_tenant_mod_usage(uint64_t tenant_id, int mod_id, common::ObModItem& item) const;

  void print_tenant_ctx_memory_usage(uint64_t tenant_id) const;
  void print_tenant_memory_usage(uint64_t tenant_id) const;
  int set_tenant_ctx_idle(
      const uint64_t tenant_id, const uint64_t ctx_id, const int64_t size, const bool reserve = false);
  int get_chunks(AChunk** chunks, int cap, int& cnt);

private:
  using InvokeFunc = std::function<int(ObTenantMemoryMgr*)>;
  static int with_resource_handle_invoke(uint64_t tenant_id, InvokeFunc func);

private:
  DISALLOW_COPY_AND_ASSIGN(ObMallocAllocator);

private:
  obsys::CRWLock locks_[PRESERVED_TENANT_COUNT];
  ObTenantCtxAllocator* allocators_[PRESERVED_TENANT_COUNT][common::ObCtxIds::MAX_CTX_ID];
  int64_t reserved_;
  int64_t urgent_;

  static ObMallocAllocator* instance_;
};  // end of class ObMallocAllocator

}  // end of namespace lib
}  // end of namespace oceanbase

#endif /* _OB_MALLOC_ALLOCATOR_H_ */
