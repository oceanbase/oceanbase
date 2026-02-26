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

#include "observer/virtual_table/ob_all_virtual_ss_gc_status.h"
#ifdef OB_BUILD_SHARED_STORAGE
#  include "storage/incremental/garbage_collector/ob_ss_garbage_collector_service.h"
#endif
using namespace oceanbase::common;
using namespace oceanbase::storage;
namespace oceanbase
{
namespace observer
{
ObAllVirtualSSGCStatus::ObAllVirtualSSGCStatus() : ObVirtualTableScannerIterator(), is_for_sslog_table_(false) {}

ObAllVirtualSSGCStatus::~ObAllVirtualSSGCStatus()
{
  reset();
}

void ObAllVirtualSSGCStatus::reset()
{
  omt::ObMultiTenantOperator::reset();
  ObVirtualTableScannerIterator::reset();
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObAllVirtualSSGCStatus::prepare_start_to_read()
{
  int ret = OB_SUCCESS;
  LastSuccSCNs last_succ_scns;
  ObSSGarbageCollectorService *ss_gc_srv = nullptr;
  bool is_gc_sswriter = false;

  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator_ shouldn't be NULL", K(allocator_), KR(ret));
  } else if (OB_ISNULL(ss_gc_srv = MTL(ObSSGarbageCollectorService *))) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "ObSSGarbageCollectorService is NULL", KR(ret));
  } else if (OB_FAIL(ss_gc_srv->check_is_gc_sswriter(is_gc_sswriter))) {
    SERVER_LOG(WARN, "check whether is sswriter failed", KR(ret));
  } else if (!is_gc_sswriter) {
    // if this server is not sswriter, do not need to iterate
    ret = OB_ITER_END;
  } else if (OB_FAIL(ss_gc_srv->get_last_succ_scns(is_for_sslog_table_, last_succ_scns))) {
    SERVER_LOG(WARN, "get last_succ_scns failed", KR(ret));
  } else if (OB_UNLIKELY(!last_succ_scns.is_valid())) {
    SERVER_LOG(WARN, "last_succ_scns is invalid", KR(ret), K(last_succ_scns));
  } else if (OB_FAIL(last_succ_scns.get_last_succ_scn_iter(last_succ_scn_iter_))) {
    SERVER_LOG(WARN, "get last_succ_scns_iter failed", KR(ret), K(last_succ_scns));
  } else {
    start_to_read_ = true;
  }
  return ret;
}

int ObAllVirtualSSGCStatus::get_next_last_succ_scn(LastSuccSCN &last_succ_scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!last_succ_scn_iter_.is_ready())) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "last_succ_scn_iter is not ready", KR(ret));
  } else if (OB_FAIL(last_succ_scn_iter_.get_next(last_succ_scn))) {
    if (OB_ITER_END == ret) {
      // get is_for_sslog_table iter
      if (MTL_ID() == OB_SYS_TENANT_ID && !is_for_sslog_table_) {
        is_for_sslog_table_ = true;
        ret = OB_SUCCESS;
        if (OB_FAIL(prepare_start_to_read())) {
          SERVER_LOG(WARN, "prepare start to read for sslog_table failed", KR(ret));
        } else if (OB_FAIL(last_succ_scn_iter_.get_next(last_succ_scn))) {
          SERVER_LOG(WARN, "get next last_succ_scn failed", KR(ret));
        }
      }
    } else {
      SERVER_LOG(WARN, "get next last_succ_scn failed", KR(ret));
    }
  }
  return ret;
}
#endif

int ObAllVirtualSSGCStatus::inner_get_next_row(common::ObNewRow *&row)
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

bool ObAllVirtualSSGCStatus::is_need_process(uint64_t tenant_id)
{
  if (!is_virtual_tenant_id(tenant_id) && (is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_)) {
    return true;
  }
  return false;
}

void ObAllVirtualSSGCStatus::release_last_tenant()
{
  start_to_read_ = false;
  is_for_sslog_table_ = false;
  memset(gc_type_buf_, '\0', sizeof(gc_type_buf_));
  memset(extra_info_buf_, '\0', sizeof(extra_info_buf_));
#ifdef OB_BUILD_SHARED_STORAGE
  last_succ_scn_iter_.reset();
  last_succ_scn_.reset();
#endif
}

int ObAllVirtualSSGCStatus::process_curr_tenant(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_SHARED_STORAGE
  ret = OB_ITER_END;
#else
  LastSuccSCN last_succ_scn;
  if (!GCTX.is_shared_storage_mode()) {
    ret = OB_ITER_END;
  } else if (!start_to_read_ && OB_FAIL(prepare_start_to_read())) {
    SERVER_LOG(WARN, "prepare start to read failed", K(ret));
    ret = OB_ITER_END;  // to avoid throw error code to client
  } else if (OB_FAIL(get_next_last_succ_scn(last_succ_scn))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "get next last_succ_scn failed", K(ret));
    }
    ret = OB_ITER_END;  // to avoid throw error code to client
  } else {
    const int64_t col_count = output_column_ids_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
      case TENANT_ID: {
        cur_row_.cells_[i].set_int(MTL_ID());
        break;
      }
      case GC_TYPE: {
        if (OB_FAIL(ss_gc_type_to_string(last_succ_scn.gc_type_, sizeof(gc_type_buf_), gc_type_buf_))) {
          SERVER_LOG(WARN, "get lock mode buf failed", K(ret), K(last_succ_scn));
        } else {
          gc_type_buf_[OB_SS_GC_TASK_TYPE_LENGTH - 1] = '\0';
          cur_row_.cells_[i].set_varchar(gc_type_buf_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        }
        break;
      }
      case LAST_SUCC_SCN: {
        last_succ_scn_ = last_succ_scn.last_succ_scn_;
        int64_t v = last_succ_scn_.get_val_for_inner_table_field();
        cur_row_.cells_[i].set_int(v);
        break;
      }
      case EXTRA_INFO: {
        if (is_for_sslog_table_) {
          snprintf(extra_info_buf_, sizeof(extra_info_buf_), "is_for_sslog_table");
        } else {
          memset(extra_info_buf_, '\0', sizeof(extra_info_buf_));
        }
        extra_info_buf_[MAX_VALUE_LENGTH - 1] = '\0';
        cur_row_.cells_[i].set_varchar(extra_info_buf_);
        cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      }
      case SVR_IP: {
        if (false == (GCTX.self_addr().ip_to_string(ip_buffer_, MAX_IP_ADDR_LENGTH))) {
          ret = OB_ERR_UNEXPECTED;
          SSLOG_LOG(WARN, "ip_to_string failed", KR(ret));
        } else {
          cur_row_.cells_[i].set_varchar(ObString(ip_buffer_));
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        }
        break;
      }
      case SVR_PORT: {
        cur_row_.cells_[i].set_int(GCTX.self_addr().get_port());
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
