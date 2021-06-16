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

#ifndef _OB_SHARE_TENANT_MUTIL_ALLOCATOR_MGR_H_
#define _OB_SHARE_TENANT_MUTIL_ALLOCATOR_MGR_H_

#include "share/ob_define.h"
#include "share/ob_unit_getter.h"
#include "lib/allocator/ob_block_alloc_mgr.h"
#include "lib/allocator/ob_vslice_alloc.h"

namespace oceanbase {
namespace common {
class ObILogAllocator;
class ObIReplayTaskAllocator;
class ObTenantMutilAllocator;

class ObTenantMutilAllocatorMgr {
public:
  typedef ObTenantMutilAllocator TMA;
  ObTenantMutilAllocatorMgr()
      : is_inited_(false),
        locks_(),
        tma_array_(),
        clog_entry_alloc_(ObMemAttr(OB_SERVER_TENANT_ID, ObModIds::OB_LOG_TASK_BODY), OB_MALLOC_MIDDLE_BLOCK_SIZE,
            clog_body_blk_alloc_)
  {}
  ~ObTenantMutilAllocatorMgr()
  {}
  int init();
  int get_tenant_mutil_allocator(const uint64_t tenant_id, ObTenantMutilAllocator*& out_allocator);
  int get_tenant_log_allocator(const uint64_t tenant_id, ObILogAllocator*& out_allocator);
  int get_tenant_replay_allocator(const uint64_t tenant_id, ObIReplayTaskAllocator*& out_allocator);
  int get_tenant_limit(const uint64_t tenant_id, int64_t& limit);
  int set_tenant_limit(const uint64_t tenant_id, const int64_t new_limit);
  void* alloc_log_entry_buf(const int64_t size)
  {
    return clog_entry_alloc_.alloc(size);
  }
  void free_log_entry_buf(void* ptr)
  {
    if (NULL != ptr) {
      clog_entry_alloc_.free(ptr);
    }
  }
  // a tricky interface, ugly but save memory
  void free_log_task_buf(void* ptr);
  int update_tenant_mem_limit(const share::TenantUnits& all_tenant_units);

public:
  static ObTenantMutilAllocatorMgr& get_instance();

private:
  int construct_allocator_(const uint64_t tenant_id, TMA*& out_allocator);
  int create_tenant_mutil_allocator_(const uint64_t tenant_id, TMA*& out_allocator);

private:
  static const uint64_t PRESERVED_TENANT_COUNT = 10000;

private:
  bool is_inited_;
  obsys::CRWLock locks_[PRESERVED_TENANT_COUNT];
  ObTenantMutilAllocator* tma_array_[PRESERVED_TENANT_COUNT];
  ObBlockAllocMgr clog_body_blk_alloc_;
  ObVSliceAlloc clog_entry_alloc_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTenantMutilAllocatorMgr);
};  // end of class ObTenantMutilAllocatorMgr

#define TMA_MGR_INSTANCE (::oceanbase::common::ObTenantMutilAllocatorMgr::get_instance())

}  // end of namespace common
}  // end of namespace oceanbase
#endif /* _OB_SHARE_TENANT_MUTIL_ALLOCATOR_MGR_H_ */
