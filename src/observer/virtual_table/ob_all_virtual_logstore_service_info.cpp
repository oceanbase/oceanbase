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

#include "ob_all_virtual_logstore_service_info.h"
#include "common/row/ob_row.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/string/ob_string.h"
#include "logservice/ob_logstore_mgr.h"

namespace oceanbase
{
namespace observer
{
ObAllVirtualLogstoreServiceInfo::ObAllVirtualLogstoreServiceInfo()
  : self_addr_(),
    is_inited_(false)
{}

ObAllVirtualLogstoreServiceInfo::~ObAllVirtualLogstoreServiceInfo()
{
  destroy();
}

void ObAllVirtualLogstoreServiceInfo::destroy()
{
  is_inited_ = false;
  self_addr_.reset();
}

int ObAllVirtualLogstoreServiceInfo::init(const common::ObAddr &self_addr)
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
    SERVER_LOG(INFO, "ObAllVirtualLogstoreServiceInfo init finished", K(ret), K(self_addr), K_(is_inited));
  }
  return ret;
}

int ObAllVirtualLogstoreServiceInfo::get_logstore_service_addr_(common::ObAddr &logstore_addr)
{
  int ret = OB_SUCCESS;
  const ObString &logstore_addr_str = GCONF._ob_logstore_service_addr.str();
  if (logstore_addr_str.empty()) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "config param _ob_logstore_service_addr is empty", K(ret), K(logstore_addr_str));
  } else if (OB_FAIL(logstore_addr.parse_from_string(logstore_addr_str))) {
    SERVER_LOG(WARN, "failed to parse addr from string", K(ret), K(logstore_addr_str));
  }
  return ret;
}

int ObAllVirtualLogstoreServiceInfo::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (false == start_to_read_) {
    ObAddr logstore_service_addr;
    logservice::LogstoreServiceInfo logstore_info;
    if (!logservice::ObLogstoreMgr::is_logstore_mode()) {
      ret = OB_ITER_END;
      SERVER_LOG(INFO, "Self is not in logstore mode", K(ret));
    } else if (OB_FAIL(get_logstore_service_addr_(logstore_service_addr))) {
      if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
        SERVER_LOG(WARN, "logstore_service_addr is invalid", K(ret));
      }
      // no row, return OB_ITER_END
      ret = OB_ITER_END;
    } else if (OB_FAIL(LOGSTORE_MGR.get_logstore_service_info(logstore_info))) {
      if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
        SERVER_LOG(WARN, "get_logstore_service_info failed", K(ret));
      }
      // no row, return OB_ITER_END
      ret = OB_ITER_END;
    } else if (OB_FAIL(insert_row_(self_addr_, logstore_service_addr, logstore_info, &cur_row_))){
      SERVER_LOG(WARN, "ObAllVirtualLogstoreServiceInfo insert_row_ failed", K(ret), K(logstore_info));
    } else {
      scanner_.add_row(cur_row_);
      scanner_it_ = scanner_.begin();
      start_to_read_ = true;
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

int ObAllVirtualLogstoreServiceInfo::insert_row_(
    const ObAddr &self_addr,
    const ObAddr &logstore_service_addr,
    const logservice::LogstoreServiceInfo &logstore_info,
    common::ObNewRow *row)
{
  int ret = OB_SUCCESS;
  const int64_t count = output_column_ids_.count();
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
        cur_row_.cells_[i].set_int(logstore_info.memory_limit_);
        cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(
                                              ObCharset::get_default_charset()));
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 4: {
        cur_row_.cells_[i].set_int(logstore_info.memory_used_);
        cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(
                                              ObCharset::get_default_charset()));
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 5: {
        cur_row_.cells_[i].set_int(logstore_info.shm_limit_);
        cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(
                                              ObCharset::get_default_charset()));
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 6: {
        cur_row_.cells_[i].set_int(logstore_info.shm_used_);
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
