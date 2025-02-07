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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_SESSION_WAIT_HISTORY_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_SESSION_WAIT_HISTORY_

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

class ObAllVirtualSessionWaitHistory : public common::ObVirtualTableScannerIterator
{
public:
  ObAllVirtualSessionWaitHistory();
  virtual ~ObAllVirtualSessionWaitHistory();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
  inline void set_addr(common::ObAddr &addr) {addr_ = &addr;}
  virtual int set_ip(common::ObAddr *addr);

protected:
  virtual int get_all_diag_info();
  common::ObSEArray<std::pair<uint64_t, common::ObDISessionCollect>,
  8> session_status_;

private:
  enum HISTORY_COLUMN
  {
    SESSION_ID = common::OB_APP_MIN_COLUMN_ID,
    SVR_IP,
    SVR_PORT,
    SEQ_NO,
    TENANT_ID,
    EVENT_NO,
    EVENT,
    P1TEXT,
    P1,
    P2TEXT,
    P2,
    P3TEXT,
    P3,
    LEVEL,
    WAIT_TIME_MICRO,
    TIME_SINCE_LAST_WAIT_MICRO,
    WAIT_TIME
  };
  common::ObAddr *addr_;
  common::ObString ipstr_;
  int32_t port_;
  uint32_t session_iter_;
  int32_t event_iter_;
  common::ObWaitEventHistoryIter history_iter_;
  common::ObDISessionCollect *collect_;
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualSessionWaitHistory);
};

class ObAllVirtualSessionWaitHistoryI1 : public ObAllVirtualSessionWaitHistory, public ObAllVirtualDiagIndexScan
{
public:
  ObAllVirtualSessionWaitHistoryI1() {}
  virtual ~ObAllVirtualSessionWaitHistoryI1() {}
  virtual int inner_open() override
  {
    return set_index_ids(key_ranges_);
  }

protected:
  virtual int get_all_diag_info();
private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualSessionWaitHistoryI1);
};

}
}
#endif /* OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_SESSION_WAIT_HISTORY */
