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

#ifndef OB_ALL_VIRTUAL_DUPLICATED_PARTITION_MGR_STAT_H
#define OB_ALL_VIRTUAL_DUPLICATED_PARTITION_MGR_STAT_H

#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/ob_scanner.h"
#include "common/row/ob_row.h"
#include "lib/container/ob_se_array.h"
#include "storage/transaction/ob_dup_table.h"
#include "storage/transaction/ob_trans_ctx_mgr.h"

namespace oceanbase {

namespace transaction {
class ObDupTablePartitionMgr;
class ObTransService;
}  // namespace transaction

namespace observer {
class ObGVDuplicatePartitionMgrStat : public common::ObVirtualTableScannerIterator {
public:
  ObGVDuplicatePartitionMgrStat(transaction::ObTransService* trans_service) : trans_service_(trans_service)
  {
    reset();
  }
  ~ObGVDuplicatePartitionMgrStat()
  {
    destroy();
  }
  int inner_get_next_row(common::ObNewRow*& row);
  void reset();
  void destroy();

private:
  int prepare_start_to_read_();
  int get_next_duplicate_partition_mgr_stat_(transaction::ObDuplicatePartitionStat& stat);
  static const int64_t OB_MIN_BUFFER_SIZE = 128;
  static const int64_t OB_MAX_BUFFER_SIZE = 1024;
  char ip_buffer_[common::OB_IP_STR_BUFF];
  char dup_table_lease_info_buffer_[OB_MAX_BUFFER_SIZE];

private:
  transaction::ObTransService* trans_service_;
  transaction::ObPartitionIterator partition_iter_;
  transaction::ObDuplicatePartitionStatIterator duplicate_partition_stat_iter_;
};

}  // namespace observer
}  // namespace oceanbase

#endif
