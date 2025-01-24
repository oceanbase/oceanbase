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

#include "observer/virtual_table/ob_all_virtual_server.h"

#include "observer/ob_service.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/shared_storage/ob_disk_space_manager.h"
#endif

using namespace oceanbase;
using namespace oceanbase::observer;
using namespace oceanbase::common;

ObAllVirtualServer::ObAllVirtualServer()
    : ObVirtualTableScannerIterator(),
      addr_()
{
  ip_buf_[0] = '\0';
}

ObAllVirtualServer::~ObAllVirtualServer()
{
  addr_.reset();
  ip_buf_[0] = '\0';
}

int ObAllVirtualServer::init(common::ObAddr &addr)
{
  addr_ = addr;
  ip_buf_[0] = '\0';
  return OB_SUCCESS;
}

int ObAllVirtualServer::inner_open()
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObAllVirtualServer::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  share::ObServerResourceInfo resource_info;
  // server resource info are get in ObService::get_server_resource_info()

  ObDeviceHealthStatus dhs = DEVICE_HEALTH_NORMAL;
  int64_t data_disk_abnormal_time = 0;

  if (start_to_read_) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(cur_row_.cells_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "cur row cell is NULL", KR(ret));
  } else if (OB_ISNULL(GCTX.ob_service_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "ob_service_ is NULL", KR(ret), KP(GCTX.ob_service_));
  } else if (OB_FAIL(GCTX.ob_service_->get_server_resource_info(resource_info))) {
    SERVER_LOG(ERROR, "fail to get_server_resource_info", KR(ret));
  } else if (OB_FAIL(ObIOManager::get_instance().get_device_health_status(dhs,
      data_disk_abnormal_time))) {
    SERVER_LOG(WARN, "get device health status fail", KR(ret));
  } else {
    const int64_t col_count = output_column_ids_.count();
    const double hard_limit = GCONF.resource_hard_limit;
    const int64_t data_disk_allocated =
        OB_STORAGE_OBJECT_MGR.get_total_macro_block_count() * OB_STORAGE_OBJECT_MGR.get_macro_block_size();
    const char *data_disk_health_status = device_health_status_to_str(dhs);
    const int64_t ssl_cert_expired_time = GCTX.ssl_key_expired_time_;

    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
        case SVR_IP:
          if (addr_.ip_to_string(ip_buf_, sizeof(ip_buf_))) {
            cur_row_.cells_[i].set_varchar(ip_buf_);
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          } else {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "fail to execute ip_to_string", K(addr_), KR(ret));
          }
          break;
        case SVR_PORT:
          cur_row_.cells_[i].set_int(addr_.get_port());
          break;
        case ZONE:
          cur_row_.cells_[i].set_varchar(GCONF.zone.str());
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case SQL_PORT:
          cur_row_.cells_[i].set_int(GCONF.mysql_port);
          break;
        case CPU_CAPACITY:
          cur_row_.cells_[i].set_int(resource_info.cpu_);
          break;
        case CPU_CAPACITY_MAX:
          cur_row_.cells_[i].set_double((resource_info.cpu_ * hard_limit) / 100);
          break;
        case CPU_ASSIGNED:
          cur_row_.cells_[i].set_double(resource_info.report_cpu_assigned_);
          break;
        case CPU_ASSIGNED_MAX:
          cur_row_.cells_[i].set_double(resource_info.report_cpu_max_assigned_);
          break;
        case MEM_CAPACITY:
          cur_row_.cells_[i].set_int(resource_info.mem_total_);
          break;
        case MEM_ASSIGNED:
          cur_row_.cells_[i].set_int(resource_info.report_mem_assigned_);
          break;
        case DATA_DISK_CAPACITY:
          cur_row_.cells_[i].set_int(resource_info.data_disk_total_);
          break;
        case DATA_DISK_ASSIGNED:
          if (GCTX.is_shared_storage_mode()) {
            cur_row_.cells_[i].set_int(resource_info.report_data_disk_assigned_);
          } else {
            cur_row_.cells_[i].set_null();
          }
          break;
        case DATA_DISK_IN_USE:
          cur_row_.cells_[i].set_int(resource_info.data_disk_in_use_);
          break;
        case DATA_DISK_ALLOCATED:
          cur_row_.cells_[i].set_int(data_disk_allocated);
          break;
        case DATA_DISK_HEALTH_STATUS:
          cur_row_.cells_[i].set_varchar(data_disk_health_status);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case DATA_DISK_ABNORMAL_TIME:
          cur_row_.cells_[i].set_int(data_disk_abnormal_time);
          break;
        case LOG_DISK_CAPACITY:
          cur_row_.cells_[i].set_int(resource_info.log_disk_total_);
          break;
        case LOG_DISK_ASSIGNED:
          cur_row_.cells_[i].set_int(resource_info.report_log_disk_assigned_);
          break;
        case LOG_DISK_IN_USE:
          cur_row_.cells_[i].set_int(resource_info.log_disk_in_use_);
          break;
        case SSL_CERT_EXPIRED_TIME:
          cur_row_.cells_[i].set_int(ssl_cert_expired_time);
          break;
        case MEMORY_LIMIT:
          cur_row_.cells_[i].set_int(GMEMCONF.get_server_memory_limit());
          break;
        default: {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid col_id", KR(ret), K(col_id));
          break;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    row = &cur_row_;
    start_to_read_ = true;
  }
  return ret;
}
