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

#ifndef _OCEABASE_OBSERVER_VIRTUAL_TABLE_OB_VIRTUAL_OBRPC_SEND_STAT_H_
#define _OCEABASE_OBSERVER_VIRTUAL_TABLE_OB_VIRTUAL_OBRPC_SEND_STAT_H_

#include "share/ob_virtual_table_iterator.h"
#include "observer/omt/ob_multi_tenant.h"

namespace oceanbase
{
namespace observer
{

class ObVirtualObRpcSendStat
    : public common::ObVirtualTableIterator
{
public:
  ObVirtualObRpcSendStat();
  virtual ~ObVirtualObRpcSendStat();

  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
private:
  enum CACHE_COLUMN
  {
    TENANT_ID = common::OB_APP_MIN_COLUMN_ID,
    SVR_IP,
    SVR_PORT,
    DEST_IP,
    DEST_PORT,
    INDEX,
    ZONE,
    PCODE,
    PCODE_NAME,
    COUNT,
    TOTAL_TIME,
    TOTAL_SIZE,
    MAX_TIME,
    MIN_TIME,
    MAX_SIZE,
    MIN_SIZE,
    FAILURE,
    TIMEOUT,
    SYNC,
    ASYNC,
    LAST_TIMESTAMP,
    ISIZE,
    ICOUNT,
    NET_TIME,
    WAIT_TIME,
    QUEUE_TIME,
    PROCESS_TIME,
    ILAST_TIMESTAMP,
    DCOUNT
  };

  int64_t pcode_idx_;
  int tenant_idx_;
  int tenant_cnt_;
  omt::TenantIdList tenant_ids_;
  bool has_start_;
}; // end of class ObVirtualObRpcSendStat


} // end of namespace observer
} // end of namespace oceanbase

#endif /* _OCEABASE_OBSERVER_VIRTUAL_TABLE_OB_VIRTUAL_RPC_SEND_STAT_H_ */
