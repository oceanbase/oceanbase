/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_ALL_TRACEPOINT_INFO_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_ALL_TRACEPOINT_INFO_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "lib/net/ob_addr.h"
#include "lib/utility/ob_tracepoint.h"

namespace oceanbase
{
namespace observer
{

class ObAllTracepointInfo : public common::ObVirtualTableScannerIterator
{
public:
  ObAllTracepointInfo();
  virtual ~ObAllTracepointInfo();
  virtual void reset();
  virtual int inner_get_next_row(common::ObNewRow *&row) override;
  inline void set_addr(common::ObAddr &addr) { addr_ = &addr; }
private:
  common::ObAddr *addr_;
  enum INSPECT_COLUMN
  {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TP_NO,
    TP_NAME,
    TP_DESCRIBE,
    TP_FREQUENCY,
    TP_ERROR_CODE,
    TP_OCCUR,
    TP_MATCH,
  };
private:
  int fill_scanner();
  int get_rows_from_tracepoint_info_list();
  DISALLOW_COPY_AND_ASSIGN(ObAllTracepointInfo);
};


} // namespace observer
} // namespace oceanbase
#endif // OCEANBASE_OBSERVER_VIRTUAL_TABLE_ALL_TRACEPOINT_INFO_
