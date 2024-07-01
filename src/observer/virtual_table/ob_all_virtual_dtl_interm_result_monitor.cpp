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

#include "observer/omt/ob_tenant.h"
#include "observer/omt/ob_multi_tenant.h"
#include "observer/virtual_table/ob_all_virtual_dtl_interm_result_monitor.h"
#include "observer/ob_server_utils.h"
#include "sql/session/ob_sql_session_info.h"
#include "lib/hash/ob_hashmap.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/dtl/ob_dtl_interm_result_manager.h"

namespace oceanbase
{
using namespace common;
using namespace sql;
using namespace sql::dtl;
namespace observer
{

ObAllDtlIntermResultMonitor::ObAllDtlIntermResultMonitor():addr_(NULL)
{
}
ObAllDtlIntermResultMonitor::~ObAllDtlIntermResultMonitor()
{
}
void ObAllDtlIntermResultMonitor::reset()
{
  addr_ = NULL;
}

#define GET_CHUNK_STORE_INFO(store)         \
  hold_mem = store->get_mem_hold();         \
  max_hold_mem = store->get_max_hold_mem(); \
  dump_size = store->get_file_size();       \
  dump_fd = store->is_file_open() ? store->get_file_fd() : -1;           \
  dump_dir_id = store->is_file_open() ? store->get_file_dir_id() : -1;   \
  owner_len = strlen(store->get_label());   \
  owner = store->get_label();

int ObDTLIntermResultMonitorInfoGetter::operator() (common::hash::HashMapPair<ObDTLIntermResultKey, ObDTLIntermResultInfo *> &entry)
{
  int ret = OB_SUCCESS;
  const ObDTLIntermResultInfo &info = *entry.second;
  const ObDTLIntermResultKey &key = entry.first;
  int64_t tenant_id = info.is_store_valid() ? info.get_tenant_id() : OB_INVALID_ID;
  if (OB_SYS_TENANT_ID == effective_tenant_id_ || tenant_id == effective_tenant_id_) {
    int64_t hold_mem = 0;
    int64_t max_hold_mem = 0;
    int64_t dump_size = 0;
    int64_t dump_fd = -1;
    int64_t dump_dir_id = -1;
    int64_t owner_len = 0;
    const char *owner = NULL;
    ObObj *cells = cur_row_.cells_;
    if (info.is_store_valid()) {
      if (info.use_rich_format_) {
        GET_CHUNK_STORE_INFO(info.col_store_);
      } else {
        GET_CHUNK_STORE_INFO(info.datum_store_);
      }
    }
    for (int64_t cell_idx = 0;
        OB_SUCC(ret) && cell_idx < output_column_ids_.count();
        ++cell_idx) {
      const uint64_t column_id = output_column_ids_.at(cell_idx);
      switch(column_id) {
        case ObAllDtlIntermResultMonitor::INSPECT_COLUMN::SVR_IP: {
          cells[cell_idx].set_varchar(addr_ip_);
          cells[cell_idx].set_collation_type(
              ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case ObAllDtlIntermResultMonitor::INSPECT_COLUMN::SVR_PORT: {
          cells[cell_idx].set_int(addr_.get_port());
          break;
        }
        case ObAllDtlIntermResultMonitor::INSPECT_COLUMN::TENANT_ID: {
          cells[cell_idx].set_int(tenant_id);
          break;
        }
        case ObAllDtlIntermResultMonitor::INSPECT_COLUMN::TRACE_ID: {
          char *buf = NULL;
          if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(OB_MAX_TRACE_ID_BUFFER_SIZE)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            SERVER_LOG(WARN, "allocate memory failed", K(ret));
          } else {
            int len = info.trace_id_.to_string(buf, OB_MAX_TRACE_ID_BUFFER_SIZE);
            cells[cell_idx].set_varchar(buf, len);
            cells[cell_idx].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        case ObAllDtlIntermResultMonitor::INSPECT_COLUMN::OWNER: {
          char *buf = NULL;
          if (OB_UNLIKELY(0 == owner_len || NULL == owner)) {
            cells[cell_idx].set_varchar(ObString());
            cells[cell_idx].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
          } else if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(owner_len)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            SERVER_LOG(WARN, "allocate memory failed", K(ret), K(owner_len));
          } else {
            MEMCPY(buf, owner, owner_len);
            cells[cell_idx].set_varchar(buf, owner_len);
            cells[cell_idx].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        case ObAllDtlIntermResultMonitor::INSPECT_COLUMN::START_TIME: {
          int64_t start_time = key.start_time_;
          cells[cell_idx].set_timestamp(start_time);
          break;
        }
        case ObAllDtlIntermResultMonitor::INSPECT_COLUMN::EXPIRE_TIME: {
          int64_t expire_time = key.timeout_ts_;
          cells[cell_idx].set_timestamp(expire_time);
          break;
        }
        case ObAllDtlIntermResultMonitor::INSPECT_COLUMN::HOLD_MEMORY: {
          cells[cell_idx].set_int(hold_mem);
          break;
        }
        case ObAllDtlIntermResultMonitor::INSPECT_COLUMN::DUMP_SIZE: {
          cells[cell_idx].set_int(dump_size);
          break;
        }
        case ObAllDtlIntermResultMonitor::INSPECT_COLUMN::DUMP_COST: {
          cells[cell_idx].set_int(info.dump_cost_);
          break;
        }
        case ObAllDtlIntermResultMonitor::INSPECT_COLUMN::DUMP_TIME: {
          if (0 != info.dump_time_) {
            cells[cell_idx].set_timestamp(info.dump_time_);
          } else {
            // not dump.
            cells[cell_idx].set_null();
          }
          break;
        }
        case ObAllDtlIntermResultMonitor::INSPECT_COLUMN::DUMP_FD: {
          cells[cell_idx].set_int(dump_fd);
          break;
        }
        case ObAllDtlIntermResultMonitor::INSPECT_COLUMN::DUMP_DIR_ID: {
          cells[cell_idx].set_int(dump_dir_id);
          break;
        }
        case ObAllDtlIntermResultMonitor::INSPECT_COLUMN::CHANNEL_ID: {
          cells[cell_idx].set_int(key.channel_id_);
          break;
        }
        case ObAllDtlIntermResultMonitor::INSPECT_COLUMN::QC_ID: {
          cells[cell_idx].set_int(info.monitor_info_.qc_id_);
          break;
        }
        case ObAllDtlIntermResultMonitor::INSPECT_COLUMN::DFO_ID: {
          cells[cell_idx].set_int(info.monitor_info_.dfo_id_);
          break;
        }
        case ObAllDtlIntermResultMonitor::INSPECT_COLUMN::SQC_ID: {
          cells[cell_idx].set_int(info.monitor_info_.sqc_id_);
          break;
        }
        case ObAllDtlIntermResultMonitor::INSPECT_COLUMN::BATCH_ID: {
          cells[cell_idx].set_int(key.batch_id_);
          break;
        }
        case ObAllDtlIntermResultMonitor::INSPECT_COLUMN::MAX_HOLD_MEM: {
          cells[cell_idx].set_int(max_hold_mem);
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid column id", K(cell_idx),
              K_(output_column_ids), K(ret));
          break;
        }
      }
    }
    if (OB_SUCCESS == ret && OB_FAIL(scanner_.add_row(cur_row_))) {
      SERVER_LOG(WARN, "fail to add row", K(ret), K(cur_row_));
    }
  }
  return ret;
}

int ObAllDtlIntermResultMonitor::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObObj *cells = cur_row_.cells_;
  if (!start_to_read_) {
    if (OB_FAIL(fill_scanner())) {
      SERVER_LOG(WARN, "fill scanner failed", K(ret));
    } else {
      start_to_read_ = true;
    }
  }

  if (OB_SUCCESS == ret && start_to_read_) {
    if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
      if (OB_ITER_END != ret) {
        SERVER_LOG(WARN, "fail to get next row", K(ret));
      }
    } else {
      row = &cur_row_;
    }
  }
  return ret;
}

int ObAllDtlIntermResultMonitor::fill_scanner()
{
  int ret = OB_SUCCESS;
  ObObj *cells = NULL;
  ObString ipstr;
  if (OB_ISNULL(allocator_) || OB_ISNULL(addr_) || OB_ISNULL(session_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator_ or addr_ is null", K_(allocator), K_(addr), K(ret));
  } else if (OB_ISNULL(cells = cur_row_.cells_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "cur row cell is NULL", K(ret));
  } else if (OB_FAIL(ObServerUtils::get_server_ip(allocator_, ipstr))) {
    SERVER_LOG(ERROR, "get server ip failed", K(ret));
  } else {
    uint64_t cur_tenant_id = MTL_ID();
    if(is_sys_tenant(cur_tenant_id)) {
      omt::TenantIdList all_tenants;
      GCTX.omt_->get_tenant_ids(all_tenants);
      for (int i = 0; i < all_tenants.size(); ++i) {
        uint64_t tmp_tenant_id = all_tenants[i];
        if(!is_virtual_tenant_id(tmp_tenant_id)) {
          ObDTLIntermResultMonitorInfoGetter monitor_getter(scanner_, *allocator_, output_column_ids_,
                                  cur_row_, *addr_, ipstr, tmp_tenant_id);
          MTL_SWITCH(tmp_tenant_id) {
            if (OB_FAIL(MTL(ObDTLIntermResultManager*)->generate_monitor_info_rows(monitor_getter))) {
              SERVER_LOG(WARN, "generate monitor info array failed", K(ret));
            }
          } else {
            // During the iteration process, tenants may be deleted,
            // so we need to ignore the error code of MTL_SWITCH.
            ret = OB_SUCCESS;
          }
        }
      }
    } else {
      ObDTLIntermResultMonitorInfoGetter monitor_getter(scanner_, *allocator_, output_column_ids_,
                                  cur_row_, *addr_, ipstr, cur_tenant_id);
      MTL_SWITCH(cur_tenant_id) {
        if (OB_FAIL(MTL(ObDTLIntermResultManager*)->generate_monitor_info_rows(monitor_getter))) {
          SERVER_LOG(WARN, "generate monitor info array failed", K(ret));
        }
      }
    }
    if(OB_SUCC(ret)) {
      scanner_it_ = scanner_.begin();
      start_to_read_ = true;
    }
  }
  return ret;
}

}/* ns observer*/
}/* ns oceanbase */

