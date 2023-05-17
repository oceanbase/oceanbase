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

#include "observer/virtual_table/ob_all_virtual_server_clog_stat.h"

#include "clog/ob_clog_mgr.h"
#include "observer/ob_server.h"
#include "share/ob_locality_info.h"

namespace oceanbase {
namespace observer {
using namespace storage;
using namespace common;
using namespace share;

void ObAllVirtualServerClogStat::LocalLocalityInfo::reset()
{
  region_.reset();
  zone_.reset();
  idc_.reset();
  zone_type_ = common::ObZoneType::ZONE_TYPE_INVALID;
  merge_status_ = share::ObZoneInfo::MERGE_STATUS_MAX;
  zone_status_ = share::ObZoneStatus::UNKNOWN;
}

int ObAllVirtualServerClogStat::LocalLocalityInfo::assign(const ObLocalityInfo &locality_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(region_.assign(locality_info.local_region_))) {
    SERVER_LOG(WARN, "fail to assign region", K(ret));
  } else if (OB_FAIL(zone_.assign(locality_info.local_zone_))) {
    SERVER_LOG(WARN, "fail to assign zone", K(ret));
  } else if (OB_FAIL(idc_.assign(locality_info.local_idc_))) {
    SERVER_LOG(WARN, "fail to assign idc", K(ret));
  } else {
    zone_type_ = locality_info.local_zone_type_;
    merge_status_ = locality_info.local_merge_status_;
    zone_status_ = locality_info.local_zone_status_;
  }
  return ret;
}

ObAllVirtualServerClogStat::ObAllVirtualServerClogStat()
    : ObVirtualTableScannerIterator(), addr_(NULL), ipstr_(), port_(0), is_end_(false), local_locality_info_()
{}

ObAllVirtualServerClogStat::~ObAllVirtualServerClogStat()
{
  reset();
}

void ObAllVirtualServerClogStat::reset()
{
  ObVirtualTableScannerIterator::reset();
  addr_ = NULL;
  port_ = 0;
  ipstr_.reset();
  is_end_ = false;
  local_locality_info_.reset();
}

int ObAllVirtualServerClogStat::inner_open()
{
  int ret = OB_SUCCESS;
  if (!is_end_) {
    share::ObLocalityInfo locality_info;
    if (OB_FAIL(set_ip(addr_))) {
      SERVER_LOG(WARN, "can't get ip", K(ret));
    } else if (OB_FAIL(observer::ObServer::get_instance().get_partition_service().get_locality_info(locality_info))) {
      SERVER_LOG(WARN, "fail to get locality info", K(ret));
    } else if (OB_FAIL(local_locality_info_.assign(locality_info))) {
      SERVER_LOG(WARN, "fail to assign local locality info", K(ret));
    }
  }
  return ret;
}

int ObAllVirtualServerClogStat::set_ip(common::ObAddr* addr)
{
  int ret = OB_SUCCESS;
  char ipbuf[common::OB_IP_STR_BUFF];
  if (NULL == addr) {
    ret = OB_ENTRY_NOT_EXIST;
  } else if (!addr_->ip_to_string(ipbuf, sizeof(ipbuf))) {
    SERVER_LOG(ERROR, "ip to string failed");
    ret = OB_ERR_UNEXPECTED;
  } else {
    ipstr_ = ObString::make_string(ipbuf);
    if (OB_FAIL(ob_write_string(*allocator_, ipstr_, ipstr_))) {
      SERVER_LOG(WARN, "failed to write string", K(ret));
    }
    port_ = addr_->get_port();
  }
  return ret;
}

int ObAllVirtualServerClogStat::inner_get_next_row(ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  ObObj* cells = cur_row_.cells_;
  if (OB_ISNULL(allocator_) || OB_ISNULL(cells)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator is NULL", KP(allocator_), KP(cells), K(ret));
  } else if (is_end_) {
    ret = OB_ITER_END;
  } else {
    const int64_t col_count = output_column_ids_.count();
    for (int64_t cell_idx = 0; OB_SUCC(ret) && cell_idx < col_count; ++cell_idx) {
      uint64_t col_id = output_column_ids_.at(cell_idx);
      clog::ObICLogMgr* clog_mgr = observer::ObServer::get_instance().get_partition_service().get_clog_mgr();
      switch (col_id) {
        case SVR_IP: {
          cells[cell_idx].set_varchar(ipstr_);
          cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case SVR_PORT: {
          cells[cell_idx].set_int(port_);
          break;
        }
        case SYSTEM_CLOG_MIN_USING_FILE_ID: {
          if (OB_ISNULL(clog_mgr)) {
            cells[cell_idx].set_int(OB_INVALID_ID);
          } else {
            cells[cell_idx].set_int(clog_mgr->get_clog_min_using_file_id());
          }
          break;
        }
        case USER_CLOG_MIN_USING_FILE_ID: {
          if (OB_ISNULL(clog_mgr)) {
            cells[cell_idx].set_int(OB_INVALID_ID);
          } else {
            cells[cell_idx].set_int(clog_mgr->get_clog_min_using_file_id());
          }
          break;
        }
        case SYSTEM_ILOG_MIN_USING_FILE_ID: {
          if (OB_ISNULL(clog_mgr)) {
            cells[cell_idx].set_int(OB_INVALID_ID);
          } else {
            cells[cell_idx].set_int(0);
          }
          break;
        }
        case USER_ILOG_MIN_USING_FILE_ID: {
          if (OB_ISNULL(clog_mgr)) {
            cells[cell_idx].set_int(OB_INVALID_ID);
          } else {
            cells[cell_idx].set_int(0);
          }
          break;
        }
        case ZONE: {
          cells[cell_idx].set_varchar(local_locality_info_.zone_.ptr());
          cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case REGION: {
          cells[cell_idx].set_varchar(local_locality_info_.region_.ptr());
          cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case IDC: {
          cells[cell_idx].set_varchar(local_locality_info_.idc_.ptr());
          cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case ZONE_TYPE: {
          cells[cell_idx].set_varchar(zone_type_to_str(local_locality_info_.zone_type_));
          cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case MERGE_STATUS: {
          cells[cell_idx].set_varchar(ObZoneInfo::get_merge_status_str(local_locality_info_.merge_status_));
          cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case ZONE_STATUS: {
          cells[cell_idx].set_varchar(ObZoneStatus::get_status_str(local_locality_info_.zone_status_));
          cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case SVR_MIN_LOG_TIMESTAMP: {
          if (OB_ISNULL(clog_mgr)) {
            cells[cell_idx].set_int(OB_INVALID_TIMESTAMP);
          } else {
            // display server_min_log_ts 60s ahead of ts get from clog_mgr
            static int64_t DISPLAY_AHEAD_TIME = 60 * 1000 * 1000;
            int64_t svr_min_log_ts = OB_INVALID_TIMESTAMP;
            if (OB_FAIL(clog_mgr->get_server_min_log_ts(svr_min_log_ts))) {
              SERVER_LOG(WARN, "get_svr_min_log_ts failed", KR(ret), K(svr_min_log_ts));
              svr_min_log_ts = OB_INVALID_TIMESTAMP;
              ret = OB_SUCCESS;
            } else {
              // svr_min_log_ts is min clog ts on this server, the log may not confirmed
              svr_min_log_ts += DISPLAY_AHEAD_TIME;
            }
            cells[cell_idx].set_int(svr_min_log_ts);
          }
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid column id", K(ret), K(cell_idx), K(output_column_ids_), K(col_id));
          break;
        }
      }
    }  // end for

    if (OB_SUCC(ret)) {
      row = &cur_row_;
      is_end_ = true;
    }
  }
  return ret;
}
}  // namespace observer
}  // namespace oceanbase
