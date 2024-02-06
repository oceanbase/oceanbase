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

#define USING_LOG_PREFIX STORAGE

#include "lib/utility/ob_print_utils.h"
#include "lib/alloc/memory_dump.h"
#include "observer/omt/ob_multi_tenant.h"                  // ObMultiTenant
#include "share/ob_tenant_mgr.h"                           // get_virtual_memory_used
#include "share/allocator/ob_memstore_allocator_mgr.h"     // ObMemstoreAllocatorMgr
#include "storage/tx_storage/ob_tenant_freezer.h"          // ObTenantFreezer
#include "storage/tx_storage/ob_tenant_memory_printer.h"
#include "deps/oblib/src/lib/alloc/malloc_hook.h"

namespace oceanbase
{
using namespace share;
namespace storage
{
void ObPrintTenantMemoryUsage::runTimerTask()
{
  GMEMCONF.check_500_tenant_hold(GCONF._ignore_system_memory_over_limit_error);
  LOG_INFO("=== Run print tenant memory usage task ===");
  ObTenantMemoryPrinter &printer = ObTenantMemoryPrinter::get_instance();
  printer.print_tenant_usage();
}

ObTenantMemoryPrinter &ObTenantMemoryPrinter::get_instance()
{
  static ObTenantMemoryPrinter instance_;
  return instance_;
}

int ObTenantMemoryPrinter::register_timer_task(int tg_id)
{
  int ret = OB_SUCCESS;
  const bool is_repeated = true;
  const int64_t print_delay = 10 * 1000000; // 10s
  if (OB_FAIL(TG_SCHEDULE(tg_id,
                          print_task_,
                          print_delay,
                          is_repeated))) {
    LOG_WARN("fail to schedule print task of tenant manager", K(ret));
  }
  return ret;
}

int ObTenantMemoryPrinter::print_tenant_usage()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  common::ObMemstoreAllocatorMgr *allocator_mgr = &ObMemstoreAllocatorMgr::get_instance();
  static const int64_t BUF_LEN = 64LL << 10;
  static char print_buf[BUF_LEN] = "";
  int64_t pos = 0;
  omt::ObMultiTenant *omt = GCTX.omt_;
  common::ObSEArray<uint64_t, 8> mtl_tenant_ids;
  if (OB_FAIL(print_mutex_.trylock())) {
    // Guaranteed serial printing
    // do-nothing
  } else {
    if (OB_FAIL(databuff_printf(print_buf, BUF_LEN, pos,
                                "=== TENANTS MEMORY INFO ===\n"
                                "all_tenants_memstore_used=% '15ld, divisive_memory_used=% '15ld\n",
                                allocator_mgr->get_all_tenants_memstore_used(),
                                get_divisive_mem_size()))) {
      LOG_WARN("print failed", K(ret));
    } else if (OB_FAIL(ObVirtualTenantManager::get_instance().print_tenant_usage(print_buf,
                                                                                 BUF_LEN,
                                                                                 pos))) {
      LOG_WARN("print virtual tenant memory info failed.", K(ret));
    } else if (OB_ISNULL(omt)) {
      // do nothing
    } else if (OB_FAIL(omt->get_mtl_tenant_ids(mtl_tenant_ids))) {
      LOG_WARN("get mtl tenant ids failed", K(ret));
    } else {
      for (int i = 0; i < mtl_tenant_ids.count(); ++i) {
        uint64_t tenant_id = mtl_tenant_ids[i];
        if (OB_SUCCESS != (tmp_ret = print_tenant_usage_(tenant_id,
                                                         print_buf,
                                                         BUF_LEN,
                                                         pos))) {
          LOG_WARN("print mtl tenant usage failed", K(tmp_ret), K(tenant_id));
        }
      }
      int tenant_cnt = 0;
      static uint64_t all_tenant_ids[OB_MAX_SERVER_TENANT_CNT] = {0};
      common::get_tenant_ids(all_tenant_ids, OB_MAX_SERVER_TENANT_CNT, tenant_cnt);
      lib::ObMallocAllocator *mallocator = lib::ObMallocAllocator::get_instance();
      for (int64_t i = 0; OB_SUCC(ret) && i < tenant_cnt; ++i) {
        uint64_t id = all_tenant_ids[i];
        if (!is_virtual_tenant_id(id)) {
          bool is_deleted_tenant = true;
          for (int j = 0; j < mtl_tenant_ids.count(); ++j) {
            if (id == mtl_tenant_ids[j]) {
              is_deleted_tenant = false;
              break;
            }
          }
          if (is_deleted_tenant) {
            mallocator->print_tenant_memory_usage(id);
            mallocator->print_tenant_ctx_memory_usage(id);
          }
        }
      }
    }

    if (OB_SIZE_OVERFLOW == ret) {
      // If the buffer is not enough, truncate directly
      ret = OB_SUCCESS;
      print_buf[BUF_LEN - 2] = '\n';
      print_buf[BUF_LEN - 1] = '\0';
    }
    if (OB_SUCCESS == ret) {
      _STORAGE_LOG(INFO, "====== tenants memory info ======\n%s", print_buf);
    }

    // print global chunk freelist
    int64_t resident_size = 0;
    int64_t memory_used = get_virtual_memory_used(&resident_size);
    _STORAGE_LOG(INFO,
        "[CHUNK_MGR] free=%ld pushes=%ld pops=%ld limit=%'15ld hold=%'15ld total_hold=%'15ld used=%'15ld" \
        " freelist_hold=%'15ld large_freelist_hold=%'15ld" \
        " maps=%'15ld unmaps=%'15ld" \
        " large_maps=%'15ld large_unmaps=%'15ld" \
        " huge_maps=%'15ld huge_unmaps=%'15ld" \
        " memalign=%d resident_size=%'15ld"
#ifndef ENABLE_SANITY
        " virtual_memory_used=%'15ld\n",
#else
        " virtual_memory_used=%'15ld actual_virtual_memory_used=%'15ld\n",
#endif
        CHUNK_MGR.get_free_chunk_count(),
        CHUNK_MGR.get_free_chunk_pushes(),
        CHUNK_MGR.get_free_chunk_pops(),
        CHUNK_MGR.get_limit(),
        CHUNK_MGR.get_hold(),
        CHUNK_MGR.get_total_hold(),
        CHUNK_MGR.get_used(),
        CHUNK_MGR.get_freelist_hold() + CHUNK_MGR.get_large_freelist_hold(),
        CHUNK_MGR.get_large_freelist_hold(),
        CHUNK_MGR.get_maps(),
        CHUNK_MGR.get_unmaps(),
        CHUNK_MGR.get_large_maps(),
        CHUNK_MGR.get_large_unmaps(),
        CHUNK_MGR.get_huge_maps(),
        CHUNK_MGR.get_huge_unmaps(),
        0,
        resident_size,
#ifndef ENABLE_SANITY
        memory_used
#else
        memory_used - CHUNK_MGR.get_shadow_hold(), memory_used
#endif
        );
    print_mutex_.unlock();
  }
  return ret;
}

int ObTenantMemoryPrinter::print_tenant_usage_(
    const uint64_t tenant_id,
    char *print_buf,
    int64_t buf_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(tenant_id) {
    storage::ObTenantFreezer *freezer = nullptr;
    if (FALSE_IT(freezer = MTL(storage::ObTenantFreezer *))) {
    } else if (OB_FAIL(freezer->print_tenant_usage(print_buf,
                                                   buf_len,
                                                   pos))) {
      LOG_WARN("print tenant usage failed", K(ret), K(tenant_id));
    } else {
      // do nothing
    }
  }
  return ret;
}

} // storage
} // oceanbase
