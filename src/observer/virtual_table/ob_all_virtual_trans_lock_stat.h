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

#ifndef OB_ALL_VIRTUAL_TRANS_LOCK_STAT_H_
#define OB_ALL_VIRTUAL_TRANS_LOCK_STAT_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/ob_scanner.h"
#include "common/row/ob_row.h"
#include "lib/container/ob_se_array.h"
#include "storage/transaction/ob_trans_ctx_mgr.h"
#include "common/ob_clock_generator.h"

namespace oceanbase {

namespace transaction {
class ObTransService;
class ObTransLockStat;
}  // namespace transaction

namespace observer {
class ObGVTransLockStat : public common::ObVirtualTableScannerIterator {
public:
  explicit ObGVTransLockStat(transaction::ObTransService* trans_service) : trans_service_(trans_service)
  {
    reset();
  }
  ~ObGVTransLockStat()
  {
    destroy();
  }
  int inner_get_next_row(common::ObNewRow*& row);
  void reset();
  void destroy();

private:
  int prepare_start_to_read_();
  int get_next_trans_lock_stat_(transaction::ObTransLockStat& trans_lock_stat);
  static const int64_t OB_MIN_BUFFER_SIZE = 128;
  static const int64_t OB_MEMTABLE_KEY_BUFFER_SIZE = 512;
  char ip_buffer_[common::OB_IP_STR_BUFF];
  char partition_buffer_[OB_MIN_BUFFER_SIZE];
  char trans_id_buffer_[OB_MIN_BUFFER_SIZE];
  char proxy_sessid_buffer_[OB_MIN_BUFFER_SIZE];
  char memtable_key_buffer_[OB_MEMTABLE_KEY_BUFFER_SIZE];

private:
  transaction::ObTransService* trans_service_;
  transaction::ObPartitionIterator partition_iter_;
  transaction::ObTransLockStatIterator trans_lock_stat_iter_;
};
}  // namespace observer
}  // namespace oceanbase

#endif /* OB_ALL_VIRTUAL_TRANS_LOCK_STAT_H */
