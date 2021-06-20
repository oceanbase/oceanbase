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

#ifndef OB_ALL_VIRTUAL_LOCK_WAIT_STAT_H_
#define OB_ALL_VIRTUAL_LOCK_WAIT_STAT_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "rpc/ob_request.h"

namespace oceanbase {
namespace observer {
class ObAllVirtualLockWaitStat : public common::ObVirtualTableScannerIterator {
public:
  ObAllVirtualLockWaitStat() : node_iter_(NULL)
  {}
  virtual ~ObAllVirtualLockWaitStat()
  {
    reset();
  }

public:
  virtual int inner_open();
  virtual int inner_get_next_row(common::ObNewRow*& row);
  virtual void reset();

private:
  int make_this_ready_to_read();

private:
  enum {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TABLE_ID,
    ROWKEY,
    ADDR,
    NEED_WAIT,
    RECV_TS,
    LOCK_TS,
    ABS_TIMEOUT,
    TRY_LOCK_TIMES,
    TIME_AFTER_RECV,
    SESSION_ID,
    BLOCK_SESSION_ID,
    TYPE,
    LMODE,
    TOTAL_UPDATE_CNT,
  };
  rpc::ObLockWaitNode cur_node_;
  rpc::ObLockWaitNode* node_iter_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualLockWaitStat);
};

}  // namespace observer
}  // namespace oceanbase
#endif /* OB_ALL_VIRTUAL_LOCK_WAIT_STAT_H */
