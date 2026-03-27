/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_ARB_SERVICE_STATUS_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_ARB_SERVICE_STATUS_
#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/ob_scanner.h"
#include "common/row/ob_row.h"

namespace oceanbase
{
namespace observer
{
class ObAllVirtualArbServiceStatus : public common::ObVirtualTableScannerIterator
{
public:
  ObAllVirtualArbServiceStatus();
  virtual ~ObAllVirtualArbServiceStatus();
public:
  virtual int inner_get_next_row(common::ObNewRow *&row);
  void destroy();
private:
#ifdef OB_BUILD_ARBITRATION
  int insert_row_(const common::ObAddr &self,
      const common::ObAddr &arb_service_addr,
      const bool is_in_blacklist,
      common::ObNewRow *row);
  void reset_buf_();
#endif
private:
  static const int64_t VARCHAR_32 = 32;
  static const int64_t VARCHAR_128 = 128;
private:
#ifdef OB_BUILD_ARBITRATION
  char ip_[common::OB_IP_PORT_STR_BUFF] = {'\0'};
  char arb_service_addr_buf_[VARCHAR_128] = {'\0'};
  char arb_service_status_buf_[VARCHAR_32] = {'\0'};
#endif
};
}//namespace observer
}//namespace oceanbase
#endif
