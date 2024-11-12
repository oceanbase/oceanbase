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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_LOGSTORE_SERVICE_STATUS_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_LOGSTORE_SERVICE_STATUS_
#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/ob_scanner.h"
#include "common/row/ob_row.h"
#include "lib/net/ob_addr.h"

namespace oceanbase
{

namespace observer
{
class ObAllVirtualLogstoreServiceStatus : public common::ObVirtualTableScannerIterator
{
public:
  ObAllVirtualLogstoreServiceStatus();
  virtual ~ObAllVirtualLogstoreServiceStatus();
  int init(const common::ObAddr &self_addr);
  virtual int inner_get_next_row(common::ObNewRow *&row);
  void destroy();
private:
  // int get_logstore_service_addr_(common::ObAddr &logstore_addr);
  int insert_row_(const common::ObAddr &self_addr,
    const common::ObAddr &logstore_service_addr,
    const bool is_in_blacklist,
    const int64_t last_active_ts,
    common::ObNewRow *row);
  void reset_buf_();
private:
  static const int64_t VARCHAR_32 = 32;
  static const int64_t VARCHAR_128 = 128;
private:
  common::ObAddr self_addr_;
  char ip_[common::OB_IP_PORT_STR_BUFF] = {'\0'};
  char logstore_service_addr_buf_[VARCHAR_128] = {'\0'};
  char logstore_service_status_buf_[VARCHAR_32] = {'\0'};
  bool is_inited_;
};
}//namespace observer
}//namespace oceanbase
#endif
