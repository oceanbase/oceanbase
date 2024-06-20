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

#include "lib/allocator/ob_malloc.h"
#include "share/allocator/ob_shared_memory_allocator_mgr.h"
#include "share/allocator/ob_tenant_mutil_allocator.h"
#include "share/allocator/ob_tenant_mutil_allocator_mgr.h"
#include "share/config/ob_server_config.h"
#include "share/rc/ob_tenant_base.h"
#include "ob_memstore_allocator.h"

using namespace oceanbase::share;

namespace oceanbase
{
namespace common
{

int ObTenantMutilAllocatorMgr::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else {
    for (int64_t i = 0; i < ARRAY_SIZE; ++i) {
      tma_array_[i] = NULL;
    }
    is_inited_ = true;
  }
  return ret;
}

// Get the log allocator for specified tenant, create it when tenant not exist
int ObTenantMutilAllocatorMgr::get_tenant_log_allocator(const uint64_t tenant_id,
                                                        ObILogAllocator *&out_allocator)
{
  int ret = OB_SUCCESS;
  ObTenantMutilAllocator *allocator = NULL;
  if (OB_FAIL(get_tenant_mutil_allocator_(tenant_id, allocator))) {
  } else {
    out_allocator = allocator;
  }
  return ret;
}

int ObTenantMutilAllocatorMgr::delete_tenant_log_allocator(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(delete_tenant_mutil_allocator_(tenant_id))) {
    OB_LOG(WARN, "delete_tenant_mutil_allocator_ failed", K(ret), K(tenant_id));
  } else {
    OB_LOG(INFO, "delete_tenant_mutil_allocator_ success", K(tenant_id));
  }
  return ret;
}

int64_t ObTenantMutilAllocatorMgr::get_slot_(const int64_t tenant_id) const
{
  // The first slot by idx==0 won't be used.
  return (tenant_id % PRESERVED_TENANT_COUNT) + 1;
}
int ObTenantMutilAllocatorMgr::get_tenant_mutil_allocator_(const uint64_t tenant_id,
                                                           TMA *&out_allocator)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(tenant_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", K(ret), K(tenant_id));
  } else if (tenant_id <= PRESERVED_TENANT_COUNT) {
    // Need rlock
    do {
      obsys::ObRLockGuard guard(locks_[tenant_id]);
      out_allocator = ATOMIC_LOAD(&tma_array_[tenant_id]);
    } while(0);

    if (NULL == out_allocator) {
      // Need create new allocator
      if (OB_FAIL(create_tenant_mutil_allocator_(tenant_id, out_allocator))) {
        OB_LOG(WARN, "fail to create_tenant_mutil_allocator_", K(ret), K(tenant_id));
      }
    }
  } else {
    // Need lock
    // slot must be > 0.
    const int64_t slot = get_slot_(tenant_id);
    bool is_need_create = false;
    do {
      // rdlock
      obsys::ObRLockGuard guard(locks_[slot]);
      TMA **cur = &tma_array_[slot];
      while ((NULL != cur) && (NULL != *cur) && (*cur)->get_tenant_id() < tenant_id) {
        cur = &((*cur)->get_next());
      }
      if (NULL != cur) {
        if (NULL != (*cur) && (*cur)->get_tenant_id() == tenant_id) {
          out_allocator = *cur;
        } else {
          // (*cur) is NULL || (*cur)->tenant_id_ > tenant_id
          is_need_create = true;
        }
      }
    } while (0);

    if (is_need_create) {
      if (OB_FAIL(create_tenant_mutil_allocator_(tenant_id, out_allocator))) {
        OB_LOG(WARN, "fail to create_tenant_mutil_allocator_", K(ret), K(tenant_id));
      }
    }
  }

  if (OB_SUCC(ret) && OB_ISNULL(out_allocator)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "got allocator is NULL", K(ret), K(tenant_id));
  }

  return ret;
}

int ObTenantMutilAllocatorMgr::get_tenant_memstore_limit_percent_(const uint64_t tenant_id,
                                                                  int64_t &limit_percent) const
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(tenant_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", K(ret), K(tenant_id));
  } else {
    MTL_SWITCH(tenant_id) {
      limit_percent = MTL(ObTenantFreezer*)->get_memstore_limit_percentage();
    }
  }
  return ret;
}

int ObTenantMutilAllocatorMgr::construct_allocator_(const uint64_t tenant_id,
                                                    TMA *&out_allocator)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(tenant_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", K(ret), K(tenant_id));
  } else {
    ObMemAttr attr(OB_SERVER_TENANT_ID, ObModIds::OB_TENANT_MUTIL_ALLOCATOR);
    SET_USE_500(attr);
    void *buf = ob_malloc(sizeof(TMA), attr);
    if (NULL == buf) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "failed to alloc memory", K(ret), K(tenant_id));
    } else {
      TMA *allocator = new (buf) TMA(tenant_id);
      out_allocator = allocator;
      OB_LOG(INFO, "ObTenantMutilAllocator init success", K(tenant_id));
    }
  }
  return ret;
}

int ObTenantMutilAllocatorMgr::create_tenant_mutil_allocator_(const uint64_t tenant_id,
                                                              TMA *&out_allocator)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(tenant_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", K(ret), K(tenant_id));
  } else if (tenant_id <= PRESERVED_TENANT_COUNT) {
    // wlock
    obsys::ObWLockGuard guard(locks_[tenant_id]);
    if (NULL != (out_allocator = ATOMIC_LOAD(&tma_array_[tenant_id]))) {
    } else {
      TMA *tmp_tma = NULL;
      if (OB_FAIL(construct_allocator_(tenant_id, tmp_tma))) {
        OB_LOG(WARN, "fail to construct_allocator_", K(ret), K(tenant_id));
      } else if (!ATOMIC_BCAS(&tma_array_[tenant_id], NULL, tmp_tma)) {
        out_allocator = ATOMIC_LOAD(&tma_array_[tenant_id]);
        if (NULL != tmp_tma) {
          tmp_tma->~TMA();
          ob_free(tmp_tma);
        }
      } else {
        out_allocator = ATOMIC_LOAD(&tma_array_[tenant_id]);
      }
    }
  } else {
    // slot must be > 0.
    const int64_t slot = get_slot_(tenant_id);
    do {
      // Need lock when modify slog list
      obsys::ObWLockGuard guard(locks_[slot]);
      if (NULL == ATOMIC_LOAD(&tma_array_[slot])) {
        // slot's head node is NULL, need construct
        TMA *tmp_tma = NULL;
        if (OB_FAIL(construct_allocator_(slot, tmp_tma))) {
          OB_LOG(WARN, "fail to construct_allocator_", K(ret), K(slot));
        } else if (!ATOMIC_BCAS(&tma_array_[slot], NULL, tmp_tma)) {
          if (NULL != tmp_tma) {
            tmp_tma->~TMA();
            ob_free(tmp_tma);
          }
        } else {}
      }
      // create tenant's allocator
      if (OB_SUCC(ret)) {
        bool is_need_create = false;
        TMA **prev = NULL;
        TMA **cur = &tma_array_[slot];
        while ((NULL != cur) && (NULL != *cur) && (*cur)->get_tenant_id() < tenant_id) {
          prev = cur;
          cur = &((*cur)->get_next());
        }
        if (NULL != cur) {
          if (NULL != (*cur) && (*cur)->get_tenant_id() == tenant_id) {
            out_allocator = *cur;
          } else {
            is_need_create = true;
          }
        }
        if (is_need_create) {
          TMA *tmp_tma = NULL;
          if (OB_FAIL(construct_allocator_(tenant_id, tmp_tma))) {
            OB_LOG(WARN, "fail to construct_allocator_", K(ret), K(tenant_id));
          } else {
            OB_ASSERT(NULL != prev);
            OB_ASSERT(NULL != (*prev));
            OB_ASSERT(prev != cur);
            // record cur's value(new next tma ptr)
            TMA *next_allocator = *cur;
            // set cur to NULL
            cur = NULL;
            // Let prev->next_ points to new tma.
            ((*prev)->get_next()) = tmp_tma;
            // Let tmp_tma->next_ points to old cur.
            (tmp_tma->get_next()) = next_allocator;
            out_allocator = tmp_tma;
          }
        }
      }
    } while (0);
  }

  return ret;
}

int ObTenantMutilAllocatorMgr::delete_tenant_mutil_allocator_(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(tenant_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", K(ret), K(tenant_id));
  } else if (tenant_id <= PRESERVED_TENANT_COUNT) {
    // Need wlock
    obsys::ObWLockGuard guard(locks_[tenant_id]);
    TMA *tma_allocator = NULL;
    if (NULL != (tma_allocator = ATOMIC_LOAD(&tma_array_[tenant_id]))) {
      if (NULL != tma_allocator->get_next()) {
        OB_LOG(INFO, "next_ ptr is not NULL, skip deleting this allocator", K(ret), K(tenant_id));
        // Purge cached blocks of this allocator.
        tma_allocator->try_purge();
      } else {
        tma_array_[tenant_id] = NULL;
        // destroy tma object
        tma_allocator->~TMA();
        ob_free(tma_allocator);
        tma_allocator = NULL;
      }
    }
  } else {
    // slot must be > 0.
    const int64_t slot = get_slot_(tenant_id);
    do {
      // wlock
      obsys::ObWLockGuard guard(locks_[slot]);
      TMA *prev = NULL;
      TMA *cur = tma_array_[slot];
      while ((NULL != cur) && cur->get_tenant_id() < tenant_id) {
        prev = cur;
        cur = cur->get_next();
      }
      if (NULL != cur && cur->get_tenant_id() == tenant_id) {
        OB_ASSERT(NULL != prev);
        OB_ASSERT(prev != cur);
        prev->get_next() = cur->get_next();
        cur->get_next() = NULL;
        // destroy tma object
        cur->~TMA();
        ob_free(cur);
        cur = NULL;
      }
    } while (0);
  }

  return ret;
}

ObTenantMutilAllocatorMgr &ObTenantMutilAllocatorMgr::get_instance()
{
  static ObTenantMutilAllocatorMgr instance_;
  return instance_;
}
/*
int ObTenantMutilAllocatorMgr::get_tenant_limit(const uint64_t tenant_id,
                                                int64_t &limit)
{
  int ret = OB_SUCCESS;
  ObTenantMutilAllocator *allocator = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(tenant_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(get_tenant_mutil_allocator_(tenant_id, allocator))) {
    ret = OB_TENANT_NOT_EXIST;
  } else {
    limit = allocator->get_limit();
  }

  return ret;
}

int ObTenantMutilAllocatorMgr::set_tenant_limit(const uint64_t tenant_id,
                                                const int64_t new_limit)
{
  int ret = OB_SUCCESS;
  ObTenantMutilAllocator *allocator = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(tenant_id <= 0) || OB_UNLIKELY(new_limit <= 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(get_tenant_mutil_allocator_(tenant_id, allocator))) {
  } else if (OB_ISNULL(allocator)) {
    ret = OB_TENANT_NOT_EXIST;
  } else {
    allocator->set_limit(new_limit);
  }

  return ret;
}
*/
int ObTenantMutilAllocatorMgr::update_tenant_mem_limit(const share::TenantUnits &all_tenant_units)
{
  // Update mem_limit for each tenant, called when the chane unit specifications or
  // memstore_limite_percentage
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else {
    int64_t unit_cnt = all_tenant_units.count();
    for (int64_t i = 0; i < unit_cnt && OB_SUCC(ret); ++i) {
      const share::ObUnitInfoGetter::ObTenantConfig &tenant_config = all_tenant_units.at(i);
      int64_t cur_memstore_limit_percent = 0;
      const uint64_t tenant_id = tenant_config.tenant_id_;
      const bool has_memstore = tenant_config.has_memstore_;
      int32_t nway = (int32_t)(tenant_config.config_.max_cpu());
      if (nway == 0) {
        nway = 1;
      }
      const int64_t memory_size = tenant_config.config_.memory_size();
      int64_t new_tma_limit = memory_size;
      if (has_memstore) {
        // If the unit type of tenant is not Log, need to subtract
        // the reserved memory of memstore
        if (OB_TMP_FAIL(get_tenant_memstore_limit_percent_(tenant_id, cur_memstore_limit_percent))) {
          OB_LOG(WARN, "memstore_limit_percentage val is unexpected", K(cur_memstore_limit_percent));
        } else if (cur_memstore_limit_percent > 100 || cur_memstore_limit_percent <= 0) {
          OB_LOG(WARN, "memstore_limit_percentage val is unexpected", K(cur_memstore_limit_percent));
        } else {
          new_tma_limit = memory_size / 100 * ( 100 - cur_memstore_limit_percent);
        }
      }
      ObTenantMutilAllocator *tma= NULL;
      if (OB_TMP_FAIL(get_tenant_mutil_allocator_(tenant_id, tma))) {
        OB_LOG(WARN, "get_tenant_mutil_allocator_ failed", K(tmp_ret), K(tenant_id));
      } else if (NULL == tma) {
        OB_LOG(WARN, "get_tenant_mutil_allocator_ failed", K(tenant_id));
      } else {
        tma->set_nway(nway);
        int64_t pre_tma_limit = tma->get_limit();
        if (pre_tma_limit != new_tma_limit) {
          tma->set_limit(new_tma_limit);
        }
        OB_LOG(INFO, "ObTenantMutilAllocator update tenant mem_limit finished", K(ret),
            K(tenant_id),  K(nway), K(new_tma_limit), K(pre_tma_limit), K(cur_memstore_limit_percent), K(tenant_config));
      }

      //update memstore threshold of MemstoreAllocator
      MTL_SWITCH(tenant_id) {
        ObMemstoreAllocator &memstore_allocator = MTL(ObSharedMemAllocMgr *)->memstore_allocator();
        if (OB_FAIL(memstore_allocator.set_memstore_threshold())) {
          OB_LOG(WARN, "failed to set_memstore_threshold of memstore allocator", K(tenant_id), K(ret));
        } else {
          OB_LOG(INFO, "succ to set_memstore_threshold of memstore allocator", K(tenant_id), K(ret));
        }
      }
    }
  }
  return ret;
}

}
}
