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

#ifndef OCEANBASE_OBSERVER_OB_VIRTUAL_ASH_H
#define OCEANBASE_OBSERVER_OB_VIRTUAL_ASH_H
#include "lib/container/ob_se_array.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "lib/net/ob_addr.h"
#include "share/ash/ob_active_sess_hist_list.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
}
namespace observer
{

class ObVirtualASH : public common::ObVirtualTableScannerIterator
{
public:
  ObVirtualASH();
  virtual ~ObVirtualASH();
  virtual int inner_open() override;
  virtual int inner_get_next_row(common::ObNewRow *&row) override;
  virtual void reset();
  void set_addr(const common::ObAddr &addr) { addr_ = addr; }
private:
  int set_ip(const common::ObAddr &addr);
  int convert_node_to_row(const common::ActiveSessionStat &node, ObNewRow *&row);
private:
  enum COLUMN_ID
  {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    SAMPLE_ID,
    SAMPLE_TIME,
    TENANT_ID,
    USER_ID,
    SESSION_ID,
    SESSION_TYPE,
    SQL_ID,
    TRACE_ID,
    EVENT_NO,
    WAIT_TIME,
    P1,
    P2,
    P3,
    SQL_PLAN_LINE_ID,
    IN_PARSE,
    IN_PL_PARSE,
    IN_PLAN_CACHE,
    IN_SQL_OPTIMIZE,
    IN_SQL_EXECUTION,
    IN_PX_EXECUTION,
    IN_SEQUENCE_LOAD,
    MODULE,
    ACTION,
    CLIENT_ID,
    BACKTRACE,
    PLAN_ID
  };
  DISALLOW_COPY_AND_ASSIGN(ObVirtualASH);
  share::ObActiveSessHistList::Iterator iterator_;
  common::ObAddr addr_;
  common::ObString ipstr_;
  int32_t port_;
  char server_ip_[common::MAX_IP_ADDR_LENGTH + 2];
  char trace_id_[common::OB_MAX_TRACE_ID_BUFFER_SIZE];
  bool is_first_get_;
};

} //namespace observer
} //namespace oceanbase
#endif
