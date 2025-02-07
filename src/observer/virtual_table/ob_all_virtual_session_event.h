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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_SESSION_EVENT_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_SESSION_EVENT_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "lib/stat/ob_session_stat.h"
#include "observer/virtual_table/ob_all_virtual_diag_index_scan.h"

namespace oceanbase
{
namespace common
{
class ObObj;
}

namespace observer
{

class ObAllVirtualSessionEvent : public common::ObVirtualTableScannerIterator
{
public:
  ObAllVirtualSessionEvent();
  virtual ~ObAllVirtualSessionEvent();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
  inline void set_addr(common::ObAddr &addr) {addr_ = &addr;}
  virtual int set_ip(common::ObAddr *addr);

protected:
  virtual int get_all_diag_info();
  common::ObSEArray<std::pair<uint64_t, common::ObDISessionCollect>,
  8> session_status_;

private:
  enum EVENT_COLUMN
  {
    SESSION_ID = common::OB_APP_MIN_COLUMN_ID,
    SVR_IP,
    SVR_PORT,
    EVENT_ID,
    TENANT_ID,
    EVENT,
    WAIT_CLASS_ID,
    WAIT_CLASS_NO,
    WAIT_CLASS,
    TOTAL_WAITS,
    TOTAL_TIMEOUTS,
    TIME_WAITED,
    MAX_WAIT,
    AVERAGE_WAIT,
    TIME_WAITED_MICRO
  };
  common::ObAddr *addr_;
  common::ObString ipstr_;
  int32_t port_;
  uint32_t session_iter_;
  int32_t event_iter_;
  common::ObDISessionCollect *collect_;
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualSessionEvent);
};

class ObAllVirtualSessionEventI1 : public ObAllVirtualSessionEvent, public ObAllVirtualDiagIndexScan
{
public:
  ObAllVirtualSessionEventI1() {}
  virtual ~ObAllVirtualSessionEventI1() {}
  virtual int inner_open() override
  {
    return set_index_ids(key_ranges_);
  }

protected:
  virtual int get_all_diag_info();
private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualSessionEventI1);
};

}
}
#endif /* OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_SESSION_EVENT */

