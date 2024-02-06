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

#include "observer/virtual_table/ob_all_virtual_opt_stat_gather_monitor.h"
#include "observer/ob_server_utils.h"

namespace oceanbase
{
using namespace common;
using namespace sql;
namespace observer
{

ObAllVirtualOptStatGatherMonitor::ObAllVirtualOptStatGatherMonitor():
  addr_(NULL),
  start_to_read_(false),
  stat_array_(),
  index_(0),
  ipstr_(),
  port_(0)
{
  MEMSET(svr_ip_, 0, sizeof(svr_ip_));
}

ObAllVirtualOptStatGatherMonitor::~ObAllVirtualOptStatGatherMonitor()
{
  reset();
}

int ObAllVirtualOptStatGatherMonitor::set_ip()
{
  int ret = OB_SUCCESS;
  MEMSET(svr_ip_, 0, sizeof(svr_ip_));
  if (NULL == addr_) {
    ret = OB_ENTRY_NOT_EXIST;
  } else if (!addr_->ip_to_string(svr_ip_, sizeof(svr_ip_))) {
    SERVER_LOG(ERROR, "ip to string failed");
    ret = OB_ERR_UNEXPECTED;
  } else {
    ipstr_ = ObString::make_string(svr_ip_);
    port_ = addr_->get_port();
  }
  return ret;
}

void ObAllVirtualOptStatGatherMonitor::reset()
{
  addr_ = NULL;
  start_to_read_ = false;
  stat_array_.reset();
  index_ = 0;
  ipstr_.reset();
  port_ = 0;
  MEMSET(svr_ip_, 0, sizeof(svr_ip_));
}

int ObAllVirtualOptStatGatherMonitor::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObObj *cells = cur_row_.cells_;
  if (OB_ISNULL(allocator_) || OB_ISNULL(addr_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator_ or addr_ is null", K_(allocator), K_(addr), K(ret));
  } else if (OB_ISNULL(cells)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "cur row cell is NULL", K(ret));
  } else {
    if (!start_to_read_) {
      if (OB_FAIL(set_ip())) {
        SERVER_LOG(WARN, "failed to set ip", K(ret));
      } else if (OB_FAIL(ObOptStatGatherStatList::instance().list_to_array(*allocator_,
                                                                           effective_tenant_id_,
                                                                           stat_array_))) {
        SERVER_LOG(WARN, "failed to list to array", K(ret));
      }
    }
    if (OB_SUCC(ret) && index_ >= stat_array_.size()) {
      ret = OB_ITER_END;
    }
    for (int64_t cell_idx = 0;
        OB_SUCC(ret) && cell_idx < output_column_ids_.count();
        ++cell_idx) {
      const uint64_t column_id = output_column_ids_.at(cell_idx);
      switch(column_id) {
        case TENANT_ID: {
          cells[cell_idx].set_int(stat_array_.at(index_).get_tenant_id());
          break;
        }
        case SVR_IP: {
          cells[cell_idx].set_varchar(ipstr_);
          cells[cell_idx].set_collation_type(
              ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case SVR_PORT: {
          cells[cell_idx].set_int(port_);
          break;
        }
        case SESSION_ID: {
          cells[cell_idx].set_int(stat_array_.at(index_).get_session_id());
          break;
        }
        case TRACE_ID: {
          cells[cell_idx].set_varchar(stat_array_.at(index_).get_trace_id());
          cells[cell_idx].set_collation_type(
              ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case TASK_ID: {
          cells[cell_idx].set_varchar(stat_array_.at(index_).get_task_id());
          cells[cell_idx].set_collation_type(
              ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case TYPE: {
          cells[cell_idx].set_int(stat_array_.at(index_).get_type());
          break;
        }
        case TASK_START_TIME: {
          cells[cell_idx].set_timestamp(stat_array_.at(index_).get_task_start_time());
          break;
        }
        case TASK_TABLE_COUNT: {
          cells[cell_idx].set_int(stat_array_.at(index_).get_task_table_count());
          break;
        }
        case TASK_DURATION_TIME: {
          cells[cell_idx].set_int(stat_array_.at(index_).get_task_duration_time());
          break;
        }
        case COMPLETED_TABLE_COUNT: {
          cells[cell_idx].set_int(stat_array_.at(index_).get_completed_table_count());
          break;
        }
        case RUNNING_TABLE_OWNER: {
          cells[cell_idx].set_varchar(stat_array_.at(index_).get_database_name());
          cells[cell_idx].set_collation_type(
              ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case RUNNING_TABLE_NAME: {
          cells[cell_idx].set_varchar(stat_array_.at(index_).get_table_name());
          cells[cell_idx].set_collation_type(
              ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case RUNNING_TABLE_DURATION_TIME: {
          cells[cell_idx].set_int(stat_array_.at(index_).get_running_table_duration_time());
          break;
        }
        case SPARE1: {
          cells[cell_idx].set_null();
          break;
        }
        case SPARE2: {
          cells[cell_idx].set_null();
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