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

#ifndef OB_ALL_VIRTUAL_DEADLOCK_STAT_H_
#define OB_ALL_VIRTUAL_DEADLOCK_STAT_H_

#include "share/ob_virtual_table_scanner_iterator.h"

namespace oceanbase {
namespace observer {
class ObAllVirtualDeadlockStat : public common::ObVirtualTableScannerIterator {
public:
  ObAllVirtualDeadlockStat() : start_to_read_(false), cycle_offset_(0), cur_id_(-1), end_id_(0)
  {}
  virtual ~ObAllVirtualDeadlockStat()
  {
    destroy();
  }

public:
  virtual int inner_get_next_row(common::ObNewRow*& row);
  virtual void reset();
  virtual void destroy();

private:
  enum {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    CYCLE_ID,
    CYCLE_SEQ,
    SESSION_ID,
    TABLE_ID,
    ROW_KEY,
    WAITER_TRANS_ID,
    HOLDER_TRANS_ID,
    DEADLOCK_ROLLBACKED,
    CYCLE_DETECT_TS,
    LOCK_WAIT_TS,
  };

private:
  bool start_to_read_;
  uint32_t cycle_offset_;
  int32_t cur_id_;
  int32_t end_id_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualDeadlockStat);
};
}  // namespace observer
}  // namespace oceanbase

#endif /* OB_ALL_VIRTUAL_DEADLOCK_STAT_H_ */
