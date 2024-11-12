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

#include "ob_all_virtual_logstore_service_status.h"
#include "common/row/ob_row.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/string/ob_string.h"
#include "logservice/ob_logstore_mgr.h"

namespace oceanbase
{
namespace observer
{
const char* LOGSTORE_STATUS_ACTIVE = "ACTIVE";
const char* LOGSTORE_STATUS_INACTIVE = "INACTIVE";

ObAllVirtualLogstoreServiceStatus::ObAllVirtualLogstoreServiceStatus()
  : self_addr_(),
    is_inited_(false)
{}

ObAllVirtualLogstoreServiceStatus::~ObAllVirtualLogstoreServiceStatus()
{
  destroy();
}

int ObAllVirtualLogstoreServiceStatus::init(const common::ObAddr &self_addr)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    SERVER_LOG(WARN, "init twice", K(ret));
  } else if (!self_addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(ERROR, "invalid arguments", K(ret), K(self_addr));
  } else {
    self_addr_ = self_addr;
    is_inited_ = true;
    SERVER_LOG(INFO, "ObAllVirtualLogstoreServiceStatus init finished", K(ret), K(self_addr), K_(is_inited));
  }
  return ret;
}

void ObAllVirtualLogstoreServiceStatus::destroy()
{
  is_inited_ = false;
  self_addr_.reset();
}

// int ObAllVirtualLogstoreServiceStatus::get_logstore_service_addr_(common::ObAddr &logstore_addr)
// {
//   int ret = OB_SUCCESS;
//   const ObString &logstore_addr_str = GCONF._ob_logstore_service_addr.str();
//   if (logstore_addr_str.empty()) {
//     ret = OB_INVALID_ARGUMENT;
//     SERVER_LOG(WARN, "config param _ob_logstore_service_addr is empty", K(ret), K(logstore_addr_str));
//   } else if (OB_FAIL(logstore_addr.parse_from_string(logstore_addr_str))) {
//     SERVER_LOG(WARN, "failed to parse addr from string", K(ret), K(logstore_addr_str));
//   }
//   return ret;
// }

int ObAllVirtualLogstoreServiceStatus::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (false == start_to_read_) {
    bool is_active = false;
    ObAddr logstore_service_addr;
    int64_t last_active_ts = 0;  // defalut last_active_ts is 0
    if (!logservice::ObLogstoreMgr::is_logstore_mode()) {
      ret = OB_ITER_END;
      SERVER_LOG(INFO, "Self is not in logstore mode", K(ret));
    } else if (OB_FAIL(LOGSTORE_MGR.get_logstore_service_status(logstore_service_addr, is_active, last_active_ts))) {
      if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
        SERVER_LOG(WARN, "get_logstore_service_info failed", K(ret));
      }
      // no row, return OB_ITER_END
      ret = OB_ITER_END;
    } else {
      if (OB_FAIL(insert_row_(self_addr_, logstore_service_addr, is_active, last_active_ts, &cur_row_))){
        SERVER_LOG(WARN, "insert_row_ failed", K(ret), K(self_addr_));
      } else {
        SERVER_LOG(TRACE, "insert_row_ success");
        scanner_.add_row(cur_row_);
        scanner_it_ = scanner_.begin();
        start_to_read_ = true;
      }
    }
  }
  if (OB_SUCC(ret) && true == start_to_read_) {
    if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
      if (OB_ITER_END != ret) {
        SERVER_LOG(WARN, "failed to get_next_row", K(ret));
      }
    } else {
      row = &cur_row_;
    }
  }
  return ret;
}

void ObAllVirtualLogstoreServiceStatus::reset_buf_()
{
  memset(ip_, '\0', common::OB_IP_PORT_STR_BUFF);
  memset(logstore_service_addr_buf_, '\0', VARCHAR_128);
  memset(logstore_service_status_buf_, '\0', VARCHAR_32);
}

int ObAllVirtualLogstoreServiceStatus::insert_row_(
    const common::ObAddr &self_addr,
    const common::ObAddr &logstore_service_addr,
    const bool is_active,
    const int64_t last_active_ts,
    common::ObNewRow *row)
{
  int ret = OB_SUCCESS;
  const int64_t count = output_column_ids_.count();
  // clear buf members
  reset_buf_();
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    uint64_t col_id = output_column_ids_.at(i);
    switch (col_id) {
      case OB_APP_MIN_COLUMN_ID: {
        if (false == self_addr.ip_to_string(ip_, common::OB_IP_PORT_STR_BUFF)) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "ip_to_string failed", K(ret), K(self_addr));
        } else {
          cur_row_.cells_[i].set_varchar(ObString::make_string(ip_));
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(
                                                ObCharset::get_default_charset()));
        }
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 1: {
        cur_row_.cells_[i].set_int(self_addr.get_port());
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 2: {
        int32_t out_len = 0;
        if (OB_FAIL(logstore_service_addr.addr_to_buffer(logstore_service_addr_buf_, VARCHAR_128, out_len))) {
          SERVER_LOG(WARN, "logstore_service_addr to_string failed", K(ret), K(logstore_service_addr));
        } else {
          cur_row_.cells_[i].set_varchar(ObString::make_string(logstore_service_addr_buf_));
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(
                                                ObCharset::get_default_charset()));
        }
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 3: {
        if (is_active) {
          strncpy(logstore_service_status_buf_, LOGSTORE_STATUS_ACTIVE, VARCHAR_32);
        } else {
          strncpy(logstore_service_status_buf_, LOGSTORE_STATUS_INACTIVE, VARCHAR_32);
        }
        cur_row_.cells_[i].set_varchar(ObString::make_string(logstore_service_status_buf_));
        cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(
                                                ObCharset::get_default_charset()));
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 4: {
        cur_row_.cells_[i].set_timestamp(last_active_ts);
        cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(
                                                ObCharset::get_default_charset()));
        break;
      }
    }
  }
  return ret;
}

}//namespace observer
}//namespace oceanbase
