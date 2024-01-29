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

#include "ob_all_virtual_arbitration_service_status.h"
#include "common/row/ob_row.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/string/ob_string.h"
#include "observer/ob_server_struct.h"
#include "rpc/obrpc/ob_net_keepalive.h"
#include "storage/ob_locality_manager.h"

namespace oceanbase
{
namespace observer
{
const char* ARB_STATUS_ACTIVE = "ACTIVE";
const char* ARB_STATUS_INACTIVE = "INACTIVE";

ObAllVirtualArbServiceStatus::ObAllVirtualArbServiceStatus()
{}

ObAllVirtualArbServiceStatus::~ObAllVirtualArbServiceStatus()
{
  destroy();
}

void ObAllVirtualArbServiceStatus::destroy()
{
}

int ObAllVirtualArbServiceStatus::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (false == start_to_read_) {
#ifdef OB_BUILD_ARBITRATION
    ObAddr self = GCTX.self_addr();
    bool is_in_blacklist = false;
    obrpc::ObNetKeepAliveData alive_data;
    storage::ObLocalityManager *locality_manager = GCTX.locality_manager_;
    ObAddr arb_service_addr;
    int tmp_ret = OB_SUCCESS;
    if (OB_ISNULL(locality_manager)) {
      ret = OB_ITER_END;
      SERVER_LOG(WARN, "locality_manager ptr is NULL", K(ret), KP(locality_manager));
    } else if (OB_FAIL(locality_manager->get_arb_service_addr(arb_service_addr))) {
      SERVER_LOG(WARN, "get_arb_service_addr failed", K(ret), K(self));
    } else if (!arb_service_addr.is_valid()) {
      // no row, return OB_ITER_END
      ret = OB_ITER_END;
      if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
        SERVER_LOG(WARN, "arb_service_addr is invalid", K(ret), K(self));
      }
    } else {
      if (OB_SUCCESS != (tmp_ret = obrpc::ObNetKeepAlive::get_instance().in_black(arb_service_addr, \
            is_in_blacklist, &alive_data))) {
        // in_black may return err code(-4016) when arg server has not been detected.
        // we can ignore err code here, and keep default value for is_in_blacklist.
        SERVER_LOG(WARN, "ObNetKeepAlive in_black failed", K(tmp_ret), K(arb_service_addr));
      }
      if (OB_FAIL(insert_row_(self, arb_service_addr, is_in_blacklist, &cur_row_))){
        SERVER_LOG(WARN, "insert_row_ failed", K(ret), K(self));
      } else {
        SERVER_LOG(TRACE, "insert_row_ success");
        scanner_.add_row(cur_row_);
        scanner_it_ = scanner_.begin();
        start_to_read_ = true;
      }
    }
#else
    ret = OB_ITER_END;
#endif
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

#ifdef OB_BUILD_ARBITRATION
void ObAllVirtualArbServiceStatus::reset_buf_()
{
  memset(ip_, '\0', common::OB_IP_PORT_STR_BUFF);
  memset(arb_service_addr_buf_, '\0', VARCHAR_128);
  memset(arb_service_status_buf_, '\0', VARCHAR_32);
}

int ObAllVirtualArbServiceStatus::insert_row_(
    const common::ObAddr &self,
    const common::ObAddr &arb_service_addr,
    const bool is_in_blacklist,
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
        if (false == self.ip_to_string(ip_, common::OB_IP_PORT_STR_BUFF)) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "ip_to_string failed", K(ret), K(self));
        } else {
          cur_row_.cells_[i].set_varchar(ObString::make_string(ip_));
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(
                                                ObCharset::get_default_charset()));
        }
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 1: {
        cur_row_.cells_[i].set_int(self.get_port());
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 2: {
        int32_t out_len = 0;
        if (OB_FAIL(arb_service_addr.addr_to_buffer(arb_service_addr_buf_, VARCHAR_128, out_len))) {
          SERVER_LOG(WARN, "arb_service_addr to_string failed", K(ret), K(arb_service_addr));
        } else {
          cur_row_.cells_[i].set_varchar(ObString::make_string(arb_service_addr_buf_));
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(
                                                ObCharset::get_default_charset()));
        }
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 3: {
        if (is_in_blacklist) {
          strncpy(arb_service_status_buf_, ARB_STATUS_INACTIVE, VARCHAR_32 - 1);
        } else {
          strncpy(arb_service_status_buf_, ARB_STATUS_ACTIVE, VARCHAR_32 - 1);
        }
        cur_row_.cells_[i].set_varchar(ObString::make_string(arb_service_status_buf_));
        cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(
                                                ObCharset::get_default_charset()));
        break;
      }
    }
  }
  return ret;
}
#endif

}//namespace observer
}//namespace oceanbase
