/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "observer/virtual_table/ob_all_virtual_ss_gc_detect_info.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/incremental/garbage_collector/ob_ss_garbage_collector_service.h"
#endif
using namespace oceanbase::common;
using namespace oceanbase::storage;
namespace oceanbase
{
namespace observer
{
ObAllVirtualSSGCDetectInfo::ObAllVirtualSSGCDetectInfo() : ObVirtualTableScannerIterator() {}

ObAllVirtualSSGCDetectInfo::~ObAllVirtualSSGCDetectInfo()
{
  reset();
}

void ObAllVirtualSSGCDetectInfo::reset()
{
  omt::ObMultiTenantOperator::reset();
  ObVirtualTableScannerIterator::reset();
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObAllVirtualSSGCDetectInfo::prepare_start_to_read()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator_ shouldn't be NULL", K(allocator_), K(ret));
  } else if (OB_FAIL(MTL(ObSSGarbageCollectorService *)->get_detect_gc_infos_iter(detect_info_iter_))) {
    SERVER_LOG(WARN, "get last_succ_scns failed", KR(ret));
  } else {
    start_to_read_ = true;
  }
  return ret;
}
#endif

int ObAllVirtualSSGCDetectInfo::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_SHARED_STORAGE
  ret = OB_ITER_END;
#else
  if (!GCTX.is_shared_storage_mode()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(execute(row))) {
    SERVER_LOG(WARN, "execute fail", K(ret));
  }
#endif
  return ret;
}

bool ObAllVirtualSSGCDetectInfo::is_need_process(uint64_t tenant_id)
{
  if (!is_virtual_tenant_id(tenant_id) && (is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_)) {
    return true;
  }
  return false;
}

void ObAllVirtualSSGCDetectInfo::release_last_tenant()
{
#ifdef OB_BUILD_SHARED_STORAGE
  detect_info_iter_.reset();
#endif
  start_to_read_ = false;
}

int ObAllVirtualSSGCDetectInfo::process_curr_tenant(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_SHARED_STORAGE
  ret = OB_ITER_END;
#else
  const common::hash::ObHashMap<ObSSPreciseGCTablet, share::SCN> &gc_start_scn_map =
    MTL(ObSSGarbageCollectorService *)->get_gc_start_scn_map();
  if (!GCTX.is_shared_storage_mode()) {
    ret = OB_ITER_END;
  } else if (!start_to_read_ && OB_FAIL(prepare_start_to_read())) {
    SERVER_LOG(WARN, "prepare start to read failed", K(ret));
    ret = OB_ITER_END;  // to avoid throw error code to client
  } else if (OB_FAIL(detect_info_iter_.get_next(gc_info_))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "get next last_succ_scn failed", K(ret));
    }
    ret = OB_ITER_END;  // to avoid throw error code to client
  } else {
    const int64_t col_count = output_column_ids_.count();
    const ObSSPreciseGCTablet gc_tablet = gc_info_.gc_tablet_;
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
      case TENANT_ID: {
        cur_row_.cells_[i].set_int(MTL_ID());
        break;
      }
      case LS_ID: {
        cur_row_.cells_[i].set_int(gc_tablet.ls_id_.id());
        break;
      }
      case TABLET_ID: {
        cur_row_.cells_[i].set_int(gc_tablet.tablet_id_.id());
        break;
      }
      case TRANSFER_SCN: {
        cur_row_.cells_[i].set_int(gc_tablet.transfer_scn_.get_val_for_inner_table_field());
        break;
      }
      case IS_COLLECTED: {
        cur_row_.cells_[i].set_bool(gc_info_.is_collected_);
        break;
      }
      case GC_END_SCN: {
        const share::SCN *gc_end_scn_last_time = gc_start_scn_map.get(gc_tablet);
        const share::SCN gc_start_scn = OB_ISNULL(gc_end_scn_last_time) ? share::SCN::min_scn() : *gc_end_scn_last_time;
        cur_row_.cells_[i].set_int(gc_start_scn.get_val_for_inner_table_field());
        break;
      }
      case SVR_IP: {
        // svr_ip
        if (gc_info_.addr_.ip_to_string(ip_buf_, sizeof(ip_buf_))) {
          cur_row_.cells_[i].set_varchar(ip_buf_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        } else {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "fail to execute ip_to_string", K(ret));
        }
        break;
      }
      case SVR_PORT: {
        // svr_port
        cur_row_.cells_[i].set_int(gc_info_.addr_.get_port());
        break;
      }
      case SAFE_RECYCLE_SCN: {
        cur_row_.cells_[i].set_int(gc_info_.row_scn_.get_val_for_inner_table_field());
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "meet unexpected column", KR(ret), K(col_id));
      }
      }
    }
    if (OB_SUCC(ret)) {
      row = &cur_row_;
    }
  }
#endif
  return ret;
}
}  // namespace observer
}  // namespace oceanbase
