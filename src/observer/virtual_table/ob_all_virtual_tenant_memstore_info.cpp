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

#include "observer/virtual_table/ob_all_virtual_tenant_memstore_info.h"
#include "storage/tx_storage/ob_tenant_freezer.h"
#include "share/allocator/ob_shared_memory_allocator_mgr.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace observer
{

ObAllVirtualTenantMemstoreInfo::ObAllVirtualTenantMemstoreInfo()
    : ObVirtualTableScannerIterator(),
      current_pos_(0),
      addr_()
{
}

ObAllVirtualTenantMemstoreInfo::~ObAllVirtualTenantMemstoreInfo()
{
  reset();
}

void ObAllVirtualTenantMemstoreInfo::reset()
{
  current_pos_ = 0;
  addr_.reset();
  ObVirtualTableScannerIterator::reset();
}

int ObAllVirtualTenantMemstoreInfo::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (NULL == allocator_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator_ shouldn't be NULL", K(allocator_), K(ret));
  } else if (!start_to_read_) {
    ObObj *cells = NULL;
    // allocator_ is allocator of PageArena type, no need to free
    if (NULL == (cells = cur_row_.cells_)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
    } else {
      uint64_t tenant_id = OB_INVALID_ID;
      char ip_buf[common::OB_IP_STR_BUFF];
      omt::ObMultiTenant *omt = GCTX.omt_;
      omt::TenantIdList current_ids(nullptr, ObModIds::OMT);
      if (OB_ISNULL(omt)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "omt is null", K(ret));
      } else {
        omt->get_tenant_ids(current_ids);
      }
      // does not check ret code, we need iter all the tenant.
      for (int64_t i = 0; i < current_ids.size(); ++i) {
        tenant_id = current_ids.at(i);
        int64_t active_span = 0;
        int64_t memstore_used = 0;
        int64_t freeze_trigger = 0;
        int64_t memstore_limit = 0;
        int64_t freeze_cnt = 0;
        int64_t throttle_trigger_percentage = 0;
        if (is_virtual_tenant_id(tenant_id) || (!is_sys_tenant(effective_tenant_id_) && tenant_id != effective_tenant_id_)) {
          continue;
        }
        MTL_SWITCH(tenant_id) {
          storage::ObTenantFreezer *freezer = nullptr;
          if (FALSE_IT(freezer = MTL(storage::ObTenantFreezer *))) {
          } else if (OB_FAIL(freezer->get_tenant_memstore_cond(active_span,
                                                               memstore_used,
                                                               freeze_trigger,
                                                               memstore_limit,
                                                               freeze_cnt,
                                                               throttle_trigger_percentage))) {
            SERVER_LOG(WARN, "fail to get memstore used", K(ret), K(tenant_id));
          }
          for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_.count(); ++i) {
            uint64_t col_id = output_column_ids_.at(i);
            switch (col_id) {
              case SERVER_IP:
                if (!addr_.ip_to_string(ip_buf, sizeof(ip_buf))) {
                  STORAGE_LOG(ERROR, "ip to string failed");
                  ret = OB_ERR_UNEXPECTED;
                } else {
                  cells[i].set_varchar(ip_buf);
                  cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
                }
                break;
              case SERVER_PORT:
                cells[i].set_int(addr_.get_port());
                break;
              case TENANT_ID:
                cells[i].set_int(tenant_id);
                break;
              case ACTIVE_SPAN:
                cells[i].set_int(active_span);
                break;
              case FREEZE_TRIGGER:
                cells[i].set_int(freeze_trigger);
                break;
              case FREEZE_CNT:
                cells[i].set_int(freeze_cnt);
                break;
              case MEMSTORE_USED:
                cells[i].set_int(memstore_used);
                break;
              case MEMSTORE_LIMIT:
                cells[i].set_int(memstore_limit);
                break;
              default:
                // abnormal column id
                ret = OB_ERR_UNEXPECTED;
                SERVER_LOG(WARN, "unexpected column id", K(ret));
                break;
            }
          }
          if (OB_SUCCESS == ret
              && OB_SUCCESS != (ret = scanner_.add_row(cur_row_))) {
            SERVER_LOG(WARN, "fail to add row", K(ret), K(cur_row_));
          }
        }
      }
      // always start to read, event it failed.
      scanner_it_ = scanner_.begin();
      start_to_read_ = true;
    }
  }
  // always get next row, if we have start to read.
  if (start_to_read_) {
    if (OB_SUCCESS != (ret = scanner_it_.get_next_row(cur_row_))) {
      if (OB_ITER_END != ret) {
        SERVER_LOG(WARN, "fail to get next row", K(ret));
      }
    } else {
      row = &cur_row_;
    }
  }
  return ret;
}

ObAllVirtualTenantMemstoreDiagnoseInfo::ObAllVirtualTenantMemstoreDiagnoseInfo()
    : ObVirtualTableScannerIterator(),
      current_pos_(0),
      addr_()
{
}

ObAllVirtualTenantMemstoreDiagnoseInfo::~ObAllVirtualTenantMemstoreDiagnoseInfo()
{
  reset();
}

void ObAllVirtualTenantMemstoreDiagnoseInfo::reset()
{
  current_pos_ = 0;
  addr_.reset();
  ObVirtualTableScannerIterator::reset();
}

int ObAllVirtualTenantMemstoreDiagnoseInfo::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (NULL == allocator_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator_ shouldn't be NULL", K(allocator_), K(ret));
  } else if (!start_to_read_) {
    ObObj *cells = NULL;
    // allocator_ is allocator of PageArena type, no need to free
    if (NULL == (cells = cur_row_.cells_)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
    } else {
      uint64_t tenant_id = OB_INVALID_ID;
      char ip_buf[common::OB_IP_STR_BUFF];
      omt::ObMultiTenant *omt = GCTX.omt_;
      omt::TenantIdList current_ids(nullptr, ObModIds::OMT);
      if (OB_ISNULL(omt)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "omt is null", K(ret));
      } else {
        omt->get_tenant_ids(current_ids);
      }
      // does not check ret code, we need iter all the tenant.
      for (int64_t i = 0; i < current_ids.size(); ++i) {
        tenant_id = current_ids.at(i);
        int64_t active_span = 0;
        int64_t memstore_used = 0;
        int64_t freeze_trigger = 0;
        int64_t memstore_limit = 0;
        int64_t freeze_cnt = 0;
        int64_t throttle_trigger_percentage = 0;
        int64_t real_used = 0;
        int64_t page_util_pct = 0;
        int64_t frozen_used = 0;
        int64_t merging_used = 0;
        int64_t released_used = 0;
        int64_t page_alloc_fail_cnt = 0;
        int64_t page_create_rate = 0;
        int64_t page_reclaim_rate = 0;
        int64_t large_arena_hold = 0;
        int64_t large_real_used = 0;
        int64_t large_arena_max_cached_size = 0;
        int64_t large_retired_pending_hold = 0;
        int64_t small_arena_hold = 0;
        int64_t small_real_used = 0;
        int64_t small_arena_max_cached_size = 0;
        int64_t small_retired_pending_hold = 0;
        int64_t small_page_util_pct = 0;
        int64_t small_page_alloc_fail_cnt = 0;
        int64_t promoted_cnt = 0;
        int64_t total_promoted_cnt = 0;
        int64_t batch_freeze_tablet_cnt = 0;
        int64_t pressure_freeze_round_cnt = 0;
        bool metrics_valid = false;
        if (is_virtual_tenant_id(tenant_id)
            || (!is_sys_tenant(effective_tenant_id_) && tenant_id != effective_tenant_id_)) {
          continue;
        }
        MTL_SWITCH(tenant_id) {
          int tmp_ret = OB_SUCCESS;
          storage::ObTenantFreezer *freezer = nullptr;
          share::ObSharedMemAllocMgr *share_mem_mgr = nullptr;
          if (OB_ISNULL(freezer = MTL(storage::ObTenantFreezer *))) {
            SERVER_LOG(WARN, "tenant freezer is null, skip metrics", K(tenant_id));
          } else if (OB_SUCCESS != (tmp_ret = freezer->get_tenant_memstore_cond(active_span,
                                                                                memstore_used,
                                                                                freeze_trigger,
                                                                                memstore_limit,
                                                                                freeze_cnt,
                                                                                throttle_trigger_percentage))) {
            SERVER_LOG(WARN, "fail to get memstore used", K(tmp_ret), K(tenant_id));
          } else {
            batch_freeze_tablet_cnt = freezer->get_small_pool_batch_freeze_tablet_cnt();
            pressure_freeze_round_cnt = freezer->get_small_pool_pressure_freeze_round_cnt();
            if (OB_ISNULL(share_mem_mgr = MTL(share::ObSharedMemAllocMgr *))) {
              SERVER_LOG(WARN, "shared mem alloc mgr is null, skip metrics", K(tenant_id));
            } else {
              share::ObMemstoreAllocator &tenant_allocator = share_mem_mgr->memstore_allocator();
              real_used = tenant_allocator.get_total_real_used();
              page_util_pct = memstore_used > 0 ? real_used * 100 / memstore_used : 0;
              frozen_used = tenant_allocator.get_frozen_used();
              merging_used = tenant_allocator.get_merging_used();
              released_used = tenant_allocator.get_released_used();
              page_alloc_fail_cnt = tenant_allocator.get_page_alloc_fail_cnt();
              page_create_rate = tenant_allocator.get_page_create_rate();
              page_reclaim_rate = tenant_allocator.get_page_reclaim_rate();
              large_arena_hold = tenant_allocator.get_large_arena_hold();
              large_real_used = tenant_allocator.get_large_arena_real_used();
              large_arena_max_cached_size = tenant_allocator.get_large_arena_max_cached_size();
              large_retired_pending_hold = tenant_allocator.get_large_arena_retired_pending_hold();
              small_arena_hold = tenant_allocator.get_small_arena_hold();
              small_real_used = tenant_allocator.get_small_arena_real_used();
              small_arena_max_cached_size = tenant_allocator.get_small_arena_max_cached_size();
              small_retired_pending_hold = tenant_allocator.get_small_arena_retired_pending_hold();
              small_page_util_pct = small_arena_hold > 0 ? small_real_used * 100 / small_arena_hold : 0;
              small_page_alloc_fail_cnt = tenant_allocator.get_small_arena_page_alloc_fail_cnt();
              promoted_cnt = tenant_allocator.get_promoted_cnt();
              total_promoted_cnt = tenant_allocator.get_total_promoted_cnt();
              metrics_valid = true;
            }
          }
          const int64_t pipeline_used = frozen_used + merging_used + released_used;
          // Pages are shared by handles in the same group and way. Low utilization may
          // come from scattered live refs, or from retired pages whose remaining refs
          // prevent whole-page reclamation. The maximum normal cached hold is excluded,
          // and all live bytes are credited to the remaining hold, making the diagnosis
          // conservative when the actual cached hold is below its maximum.
          const bool large_page_waste = metrics_valid &&
                                        large_arena_hold > large_arena_max_cached_size &&
                                        large_real_used * 100 < (large_arena_hold - large_arena_max_cached_size) * PAGE_WASTE_UTIL_PCT;
          const bool small_page_waste = metrics_valid &&
                                        small_arena_hold > small_arena_max_cached_size &&
                                        small_real_used * 100 < (small_arena_hold - small_arena_max_cached_size) * PAGE_WASTE_UTIL_PCT;
          const bool mem_pressure = metrics_valid && freeze_trigger > 0 && memstore_used >= freeze_trigger;
          const bool pipeline_backlog = mem_pressure &&
                                        memstore_used > 0 &&
                                        pipeline_used * 100 >= memstore_used * PIPELINE_BACKLOG_HOLD_PCT;
          const bool large_retired_pending_dominates = large_arena_hold > 0 && large_retired_pending_hold * 2 >= large_arena_hold;
          const bool small_retired_pending_dominates = small_arena_hold > 0 && small_retired_pending_hold * 2 >= small_arena_hold;
          const char *status = "NORMAL";
          const char *diagnose_info = "No actionable issue.";
          if (!metrics_valid) {
            status = "UNKNOWN";
            diagnose_info = "Memstore analysis metrics are unavailable.";
          } else if (pipeline_backlog) {
            status = "PIPELINE_BACKLOG";
            if (released_used >= merging_used && released_used >= frozen_used) {
              diagnose_info = "Released is the largest pipeline stage under pressure; "
                              "long-lived memtable refs may be delaying reclamation.";
            } else if (merging_used >= frozen_used) {
              diagnose_info = "Merging is the largest pipeline stage under pressure; slow mini-merge execution "
                              "or insufficient merge concurrency may be delaying reclamation.";
            } else {
              diagnose_info = "Frozen is the largest pipeline stage under pressure; pending writes, "
                              "unsubmitted logs, or delayed mini-merge scheduling may be causing the backlog.";
            }
          } else if (mem_pressure) {
            status = "MEM_PRESSURE";
            diagnose_info = large_page_waste || small_page_waste
                ? "Memstore has reached the freeze trigger and page utilization is low; page fragmentation "
                  "or unreclaimed retired pages may be amplifying pressure."
                : "Memstore has reached the freeze trigger; active writes or delayed freeze and merge "
                  "reclamation may be sustaining pressure.";
          } else if (large_page_waste || small_page_waste) {
            status = "PAGE_WASTE";
            if (large_page_waste && small_page_waste) {
              diagnose_info = "Both large and small arenas have low page utilization; allocations spread across "
                              "group/way slots or retired pages retained by shared refs may be causing fragmentation.";
            } else if (large_page_waste) {
              if (large_retired_pending_dominates) {
                diagnose_info = "Large arena utilization is low and retired pages dominate its hold; "
                                "shared memtable refs may be delaying whole-page reclamation.";
              } else if (promoted_cnt > 0) {
                diagnose_info = "Large arena utilization is low with promoted memtables present; group/way "
                                "dispersion, promotion, or dedicated large allocations may be causing fragmentation.";
              } else {
                diagnose_info = "Large arena utilization is low; sparse memtable writes, group/way dispersion, "
                                "or dedicated large allocations may be causing fragmentation.";
              }
            } else if (small_retired_pending_dominates) {
              diagnose_info = "Small arena utilization is low and retired pages dominate its hold; tablet-level "
                              "freezing may have released scattered refs without clearing every shared page.";
            } else {
              diagnose_info = "Small arena utilization is low; allocations spread across group/way slots "
                              "may be fragmenting live pages.";
            }
          }
          for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_.count(); ++i) {
            uint64_t col_id = output_column_ids_.at(i);
            switch (col_id) {
              case SERVER_IP:
                if (!addr_.ip_to_string(ip_buf, sizeof(ip_buf))) {
                  STORAGE_LOG(ERROR, "ip to string failed");
                  ret = OB_ERR_UNEXPECTED;
                } else {
                  cells[i].set_varchar(ip_buf);
                  cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
                }
                break;
              case SERVER_PORT:
                cells[i].set_int(addr_.get_port());
                break;
              case TENANT_ID:
                cells[i].set_int(tenant_id);
                break;
              case MEMSTORE_USED:
                cells[i].set_int(memstore_used);
                break;
              case MEMSTORE_LIMIT:
                cells[i].set_int(memstore_limit);
                break;
              case FREEZE_TRIGGER:
                cells[i].set_int(freeze_trigger);
                break;
              case REAL_USED:
                cells[i].set_int(real_used);
                break;
              case PAGE_UTIL_PCT:
                cells[i].set_int(page_util_pct);
                break;
              case FROZEN_USED:
                cells[i].set_int(frozen_used);
                break;
              case MERGING_USED:
                cells[i].set_int(merging_used);
                break;
              case RELEASED_USED:
                cells[i].set_int(released_used);
                break;
              case PAGE_ALLOC_FAIL_CNT:
                cells[i].set_int(page_alloc_fail_cnt);
                break;
              case PAGE_CREATE_BYTES_PER_SEC:
                cells[i].set_int(page_create_rate);
                break;
              case PAGE_RECLAIM_BYTES_PER_SEC:
                cells[i].set_int(page_reclaim_rate);
                break;
              case SMALL_ARENA_HOLD:
                cells[i].set_int(small_arena_hold);
                break;
              case SMALL_REAL_USED:
                cells[i].set_int(small_real_used);
                break;
              case SMALL_PAGE_UTIL_PCT:
                cells[i].set_int(small_page_util_pct);
                break;
              case SMALL_PAGE_ALLOC_FAIL_CNT:
                cells[i].set_int(small_page_alloc_fail_cnt);
                break;
              case PROMOTED_CNT:
                cells[i].set_int(promoted_cnt);
                break;
              case TOTAL_PROMOTED_CNT:
                cells[i].set_int(total_promoted_cnt);
                break;
              case SMALL_POOL_BATCH_FREEZE_TABLET_CNT:
                cells[i].set_int(batch_freeze_tablet_cnt);
                break;
              case SMALL_POOL_PRESSURE_FREEZE_ROUND_CNT:
                cells[i].set_int(pressure_freeze_round_cnt);
                break;
              case LARGE_RETIRED_PENDING_HOLD:
                cells[i].set_int(large_retired_pending_hold);
                break;
              case SMALL_RETIRED_PENDING_HOLD:
                cells[i].set_int(small_retired_pending_hold);
                break;
              case STATUS:
                cells[i].set_varchar(status);
                cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
                break;
              case DIAGNOSE_INFO:
                cells[i].set_varchar(diagnose_info);
                cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
                break;
              default:
                ret = OB_ERR_UNEXPECTED;
                SERVER_LOG(WARN, "unexpected column id", K(ret));
                break;
            }
          }
          if (OB_SUCCESS == ret && OB_SUCCESS != (ret = scanner_.add_row(cur_row_))) {
            SERVER_LOG(WARN, "fail to add row", K(ret), K(cur_row_));
          }
        }
      }
      if (OB_SUCC(ret)) {
        scanner_it_ = scanner_.begin();
        start_to_read_ = true;
      }
    }
  }
  if (OB_SUCC(ret) && start_to_read_) {
    if (OB_SUCCESS != (ret = scanner_it_.get_next_row(cur_row_))) {
      if (OB_ITER_END != ret) {
        SERVER_LOG(WARN, "fail to get next row", K(ret));
      }
    } else {
      row = &cur_row_;
    }
  }
  return ret;
}

}/* ns observer*/
}/* ns oceanbase */
