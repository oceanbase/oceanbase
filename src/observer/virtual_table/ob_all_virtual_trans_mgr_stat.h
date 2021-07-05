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

#ifndef OB_ALL_VIRTUAL_TRANS_PARTITION_STAT_H_
#define OB_ALL_VIRTUAL_TRANS_PARTITION_STAT_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/ob_scanner.h"
#include "common/row/ob_row.h"
#include "lib/container/ob_se_array.h"
#include "storage/transaction/ob_trans_ctx_mgr.h"

namespace oceanbase {
namespace transaction {
class ObTransService;
class ObTransID;
class ObStartTransParam;
class ObTransPartitonStat;
}  // namespace transaction
namespace observer {
class ObGVTransPartitionMgrStat : public common::ObVirtualTableScannerIterator {
public:
  explicit ObGVTransPartitionMgrStat(transaction::ObTransService* trans_service) : trans_service_(trans_service)
  {
    reset();
  }
  virtual ~ObGVTransPartitionMgrStat()
  {
    destroy();
  }

public:
  int inner_get_next_row(common::ObNewRow*& row);
  void reset();
  void destroy();

private:
  int prepare_start_to_read_();

private:
  char ip_buffer_[common::OB_IP_STR_BUFF];
  char memstore_version_buffer_[common::MAX_VERSION_LENGTH];

private:
  transaction::ObTransService* trans_service_;
  transaction::ObTransPartitionMgrStatIterator trans_partition_mgr_stat_iter_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObGVTransPartitionMgrStat);
};

}  // namespace observer
}  // namespace oceanbase
#endif /* OB_ALL_VIRTUAL_TRANS_PARTITION_STAT_H */
