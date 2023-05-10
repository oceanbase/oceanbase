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
#include "observer/omt/ob_multi_tenant.h"

#include "observer/omt/ob_tenant_node_balancer.h"     // ObTenantNodeBalancer
#include "share/io/ob_io_manager.h"                   // ObIOManager
#include "share/io/ob_io_struct.h"                    // device_health_status_to_str
#include "observer/omt/ob_multi_tenant.h"
#include "observer/ob_server_struct.h"
#include "logservice/ob_server_log_block_mgr.h"
#include "storage/slog/ob_storage_logger_manager.h"

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
  omt::ObTenantNodeBalancer::ServerResource svr_res_assigned;
  ObDeviceHealthStatus dhs = DEVICE_HEALTH_NORMAL;
  int64_t data_disk_abnormal_time = 0;
  logservice::ObServerLogBlockMgr *log_block_mgr = GCTX.log_block_mgr_;

  int64_t clog_in_use_size_byte = 0;
  int64_t clog_total_size_byte = 0;

  int64_t reserved_size = 4 * 1024 * 1024 * 1024L; // default RESERVED_DISK_SIZE -> 4G

  if (start_to_read_) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(cur_row_.cells_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "cur row cell is NULL", KR(ret));
  } else if (OB_FAIL(omt::ObTenantNodeBalancer::get_instance().get_server_allocated_resource(svr_res_assigned))) {
    SERVER_LOG(ERROR, "fail to get server allocated resource", KR(ret));
  } else if (OB_ISNULL(GCTX.omt_) || OB_ISNULL(log_block_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "omt is NULL", KR(ret), K(GCTX.omt_), K(log_block_mgr));
  } else if (OB_FAIL(log_block_mgr->get_disk_usage(clog_in_use_size_byte, clog_total_size_byte))) {
    SERVER_LOG(ERROR, "Failed to get clog stat ", KR(ret));
  } else if (OB_FAIL(ObIOManager::get_instance().get_device_health_status(dhs,
      data_disk_abnormal_time))) {
    SERVER_LOG(WARN, "get device health status fail", KR(ret));
  } else if (OB_FAIL(SLOGGERMGR.get_reserved_size(reserved_size))) {
    SERVER_LOG(WARN, "Fail to get reserved size", K(ret));
  } else {
    const int64_t col_count = output_column_ids_.count();
    const double hard_limit = GCONF.resource_hard_limit;
    const int64_t cpu_capacity = get_cpu_count();
    const double cpu_capacity_max = (cpu_capacity * hard_limit) / 100;
    const double cpu_assigned = svr_res_assigned.min_cpu_;
    const double cpu_assigned_max = svr_res_assigned.max_cpu_;
    const int64_t mem_capacity = GMEMCONF.get_server_memory_avail();
    const int64_t mem_assigned = svr_res_assigned.memory_size_;
    const int64_t data_disk_allocated =
        OB_SERVER_BLOCK_MGR.get_total_macro_block_count() * OB_SERVER_BLOCK_MGR.get_macro_block_size();
    const int64_t data_disk_capacity =
        OB_SERVER_BLOCK_MGR.get_max_macro_block_count(reserved_size) * OB_SERVER_BLOCK_MGR.get_macro_block_size();
    const int64_t log_disk_assigned = svr_res_assigned.log_disk_size_;
    const int64_t log_disk_capacity = clog_total_size_byte;
    const int64_t data_disk_in_use =
        OB_SERVER_BLOCK_MGR.get_used_macro_block_count() * OB_SERVER_BLOCK_MGR.get_macro_block_size();
    const int64_t clog_disk_in_use = clog_in_use_size_byte;
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
          cur_row_.cells_[i].set_int(cpu_capacity);
          break;
        case CPU_CAPACITY_MAX:
          cur_row_.cells_[i].set_double(cpu_capacity_max);
          break;
        case CPU_ASSIGNED:
          cur_row_.cells_[i].set_double(cpu_assigned);
          break;
        case CPU_ASSIGNED_MAX:
          cur_row_.cells_[i].set_double(cpu_assigned_max);
          break;
        case MEM_CAPACITY:
          cur_row_.cells_[i].set_int(mem_capacity);
          break;
        case MEM_ASSIGNED:
          cur_row_.cells_[i].set_int(mem_assigned);
          break;
        case DATA_DISK_CAPACITY:
          cur_row_.cells_[i].set_int(data_disk_capacity);
          break;
        case DATA_DISK_IN_USE:
          cur_row_.cells_[i].set_int(data_disk_in_use);
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
          cur_row_.cells_[i].set_int(log_disk_capacity);
          break;
        case LOG_DISK_ASSIGNED:
          cur_row_.cells_[i].set_int(log_disk_assigned);
          break;
        case LOG_DISK_IN_USE:
          cur_row_.cells_[i].set_int(clog_disk_in_use);
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
