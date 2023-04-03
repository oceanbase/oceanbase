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

#include "observer/virtual_table/ob_all_virtual_px_worker_stat.h"
#include "observer/ob_server_utils.h"

namespace oceanbase
{
using namespace common;
using namespace sql;
namespace observer
{

ObAllPxWorkerStatTable::ObAllPxWorkerStatTable():addr_(NULL), start_to_read_(false),
    stat_array_(), index_(0)
{
}
ObAllPxWorkerStatTable::~ObAllPxWorkerStatTable()
{
}
void ObAllPxWorkerStatTable::reset() 
{
  addr_ = NULL;
  start_to_read_ = false;
  stat_array_.reset();
  index_ = 0;
}
int ObAllPxWorkerStatTable::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObObj *cells = cur_row_.cells_;
  ObString ipstr;
  if (OB_ISNULL(allocator_) || OB_ISNULL(addr_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator_ or addr_ is null", K_(allocator), K_(addr), K(ret));
  } else if (OB_ISNULL(cells)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "cur row cell is NULL", K(ret));
  } else if (OB_FAIL(ObServerUtils::get_server_ip(allocator_, ipstr))) {
    SERVER_LOG(ERROR, "get server ip failed", K(ret));
  } else {
    if (!start_to_read_) {
      ObPxWorkerStatList::instance().list_to_array(stat_array_, effective_tenant_id_);
    }
    if (index_ >= stat_array_.size()) {
      ret = OB_ITER_END;
    }
    for (int64_t cell_idx = 0;
        OB_SUCC(ret) && cell_idx < output_column_ids_.count();
        ++cell_idx) {
      const uint64_t column_id = output_column_ids_.at(cell_idx);
      switch(column_id) {
        case SESSION_ID: {
          int64_t session_id = stat_array_.at(index_).get_session_id();
          cells[cell_idx].set_int(session_id);
          break; 
        }
        case TENANT_ID: {
         int64_t tenant_id = stat_array_.at(index_).get_tenant_id();
         cells[cell_idx].set_int(tenant_id);
         break;
        }
        case SVR_IP: {
          cells[cell_idx].set_varchar(ipstr);
          cells[cell_idx].set_collation_type(
              ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        } 
        case SVR_PORT: {
          cells[cell_idx].set_int(addr_->get_port());
          break;
        }
        case TRACE_ID: {
          int len = stat_array_.at(index_).get_trace_id().to_string(trace_id_, sizeof(trace_id_));
          cells[cell_idx].set_varchar(trace_id_, len);
          cells[cell_idx].set_collation_type(
              ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case QC_ID: {
          uint64_t qc_id = stat_array_.at(index_).get_qc_id();
          cells[cell_idx].set_int(qc_id);
          break;
        }
        case SQC_ID: {
          int64_t sqc_id = stat_array_.at(index_).get_sqc_id();
          cells[cell_idx].set_int(sqc_id);
          break;
        }
        case WORKER_ID: {
          int64_t worker_id = stat_array_.at(index_).get_worker_id();
          cells[cell_idx].set_int(worker_id);
          break;
        }
        case DFO_ID: {
          int64_t dfo_id = stat_array_.at(index_).get_dfo_id();
          cells[cell_idx].set_int(dfo_id);
          break;
        }        
        case START_TIME: {
          int64_t start_time = stat_array_.at(index_).get_start_time();
          cells[cell_idx].set_timestamp(start_time);            
          break;
        }
        case THREAD_ID: {
          int64_t thread_id = stat_array_.at(index_).get_thread_id();
          cells[cell_idx].set_int(thread_id);
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
    ++index_; 
  } 
  if (OB_SUCC(ret)) {
    start_to_read_ = true;
    row = &cur_row_;
  }
  return ret;
}

}/* ns observer*/
}/* ns oceanbase */

