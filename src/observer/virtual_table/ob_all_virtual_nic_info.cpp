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

#include "ob_all_virtual_nic_info.h"
#include "observer/ob_server.h"

namespace oceanbase
{
namespace observer
{
ObAllVirtualNicInfo::ObAllVirtualNicInfo()
    : ObVirtualTableScannerIterator(),
      is_end_(false),
      svr_port_(0)
{
  MEMSET(svr_ip_, 0, sizeof(svr_ip_));
  MEMSET(devname_, 0, sizeof(devname_));
}

ObAllVirtualNicInfo::~ObAllVirtualNicInfo()
{
  reset();
}

void ObAllVirtualNicInfo::reset()
{
  is_end_ = false;
  MEMSET(svr_ip_, 0, sizeof(svr_ip_));
  MEMSET(devname_, 0, sizeof(devname_));
  svr_port_ = 0;
  ObVirtualTableScannerIterator::reset();
}

int ObAllVirtualNicInfo::inner_open()
{
  int ret = OB_SUCCESS;
  if (!start_to_read_) {
    const common::ObAddr &svr_addr = ObServerConfig::get_instance().self_addr_;
    ObString tmp_devname;
    common::ObArenaAllocator tmp_allocator(lib::ObLabel("NicInfo"));
    if (OB_UNLIKELY(false == svr_addr.ip_to_string(svr_ip_, sizeof(svr_ip_)))) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "ip to string failed");
    } else if (OB_FAIL(GCONF.devname.deep_copy_value_string(tmp_allocator, tmp_devname))) {
      SERVER_LOG(WARN, "fail to deep copy GCONF.devname", K(GCONF.devname), K(ret));
    } else if (sizeof(devname_) < tmp_devname.length() + 1) {
      ret = OB_SIZE_OVERFLOW;
      SERVER_LOG(WARN, "buff is not enough to hold devname",
          K(sizeof(devname_)), K(tmp_devname.length()), K(ret));
    } else {
      svr_port_ = svr_addr.get_port();
      common::ObString::obstr_size_t src_len = tmp_devname.length();
      MEMCPY(devname_, tmp_devname.ptr(), src_len);
      devname_[src_len] = '\0';
      start_to_read_ = true;
    }
  }
  return ret;
}

int ObAllVirtualNicInfo::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (!start_to_read_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "not inited", K(ret));
  } else if (is_end_) {
    ret = OB_ITER_END;
  } else {
    ObObj *cells = cur_row_.cells_;
    if (OB_UNLIKELY(nullptr == cells)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "cur row cell is NULL", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_.count(); i++) {
        uint64_t col_id = output_column_ids_.at(i);
        switch (col_id) {
          case SVR_IP: {
            cells[i].set_varchar(svr_ip_);
            cells[i].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case SVR_PORT: {
            cells[i].set_int(svr_port_);
            break;
          }
          case DEVNAME: {
            cells[i].set_varchar(devname_);
            cells[i].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case SPEED_MBPS: {
            // bytes/sec --> Mbits/sec: speed_Mbps = speed_byte_ps * 8 / 1024 / 1024
            cells[i].set_int((ObServer::get_instance().get_network_speed()) >> 17);
            break;
          }
          default: {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "unexpected column id", K(col_id), K(i), K(ret));
            break;
          }
        }
      }
      if (OB_SUCC(ret)) {
        // currently, there is only one devname for an OBServer, so there is only one row
        is_end_ = true;
        row = &cur_row_;
      }
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase