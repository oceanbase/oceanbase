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

#ifndef _OB_SHARE_MEMSTORE_ALLOCATOR_MGR_H_
#define _OB_SHARE_MEMSTORE_ALLOCATOR_MGR_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/alloc/alloc_func.h"
#include "lib/hash/ob_hashmap.h"

namespace oceanbase
{
namespace lib
{
class ObMallocAllocator;
}
namespace common
{
class ObGMemstoreAllocator;

class ObMemstoreAllocatorMgr
{
public:
  typedef ObGMemstoreAllocator TAllocator;
  typedef common::hash::ObHashMap<uint64_t, TAllocator *> TenantMemostoreAllocatorMap;
  ObMemstoreAllocatorMgr();
  virtual ~ObMemstoreAllocatorMgr();
  int init();
  int get_tenant_memstore_allocator(uint64_t tenant_id, TAllocator *&out_allocator);
  int64_t get_all_tenants_memstore_used();
  static ObMemstoreAllocatorMgr &get_instance();
public:
  void set_malloc_allocator(lib::ObMallocAllocator *malloc_allocator) { malloc_allocator_ = malloc_allocator; }
private:
  static const uint64_t PRESERVED_TENANT_COUNT = 10000;
  static const uint64_t ALLOCATOR_MAP_BUCKET_NUM = 64;
  bool is_inited_;
  TAllocator *allocators_[PRESERVED_TENANT_COUNT];
  TenantMemostoreAllocatorMap allocator_map_;
  lib::ObMallocAllocator *malloc_allocator_;
  int64_t all_tenants_memstore_used_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObMemstoreAllocatorMgr);
}; // end of class ObMemstoreAllocatorMgr

} // end of namespace share
} // end of namespace oceanbase
#endif /* _OB_SHARE_MEMSTORE_ALLOCATOR_MGR_H_ */
