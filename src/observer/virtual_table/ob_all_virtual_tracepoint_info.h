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
