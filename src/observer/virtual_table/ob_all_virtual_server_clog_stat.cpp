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
#include "share/ob_zone_info.h"

namespace oceanbase {
namespace observer {
using namespace storage;
using namespace common;
using namespace share;
ObAllVirtualServerClogStat::ObAllVirtualServerClogStat()
    : ObVirtualTableScannerIterator(), addr_(NULL), ipstr_(), port_(0), is_end_(false)
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
  } else {
    const int64_t col_count = output_column_ids_.count();

    if (OB_SUCCESS != (ret = set_ip(addr_))) {
      SERVER_LOG(WARN, "can't get ip", K(ret));
    } else if (is_end_) {
      ret = OB_ITER_END;
    }
    if (OB_SUCC(ret)) {
      share::ObLocalityInfo locality_info;
      for (int64_t cell_idx = 0; OB_SUCC(ret) && cell_idx < col_count; ++cell_idx) {
        uint64_t col_id = output_column_ids_.at(cell_idx);
        clog::ObICLogMgr* clog_mgr = observer::ObServer::get_instance().get_partition_service().get_clog_mgr();
        locality_info.reset();
        if (OB_FAIL(observer::ObServer::get_instance().get_partition_service().get_locality_info(locality_info))) {
          SERVER_LOG(WARN, "fail to get locality info", K(ret));
          break;
        }
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
            cells[cell_idx].set_varchar(locality_info.get_local_zone());
            break;
          }
          case REGION: {
            cells[cell_idx].set_varchar(locality_info.get_local_region());
            break;
          }
          case IDC: {
            cells[cell_idx].set_varchar(locality_info.get_local_idc());
            break;
          }
          case ZONE_TYPE: {
            cells[cell_idx].set_varchar(zone_type_to_str(locality_info.get_local_zone_type()));
            break;
          }
          case MERGE_STATUS: {
            cells[cell_idx].set_varchar(ObZoneInfo::get_merge_status_str(locality_info.local_merge_status_));
            break;
          }
          case ZONE_STATUS: {
            cells[cell_idx].set_varchar(ObZoneStatus::get_status_str(locality_info.local_zone_status_));
            break;
          }
          default: {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "invalid column id", K(ret), K(cell_idx), K(output_column_ids_), K(col_id));
            break;
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      row = &cur_row_;
      is_end_ = true;
    }
  }
  return ret;
}
}  // namespace observer
}  // namespace oceanbase
