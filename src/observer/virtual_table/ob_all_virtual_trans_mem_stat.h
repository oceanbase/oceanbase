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

#ifndef OB_ALL_VIRTUAL_TRANS_MEMORY_STAT_H_
#define OB_ALL_VIRTUAL_TRANS_MEMORY_STAT_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/ob_scanner.h"
#include "common/row/ob_row.h"
#include "lib/container/ob_se_array.h"
#include "storage/transaction/ob_trans_ctx_mgr.h"
#include "storage/transaction/ob_trans_service.h"

namespace oceanbase {
namespace transaction {
class ObTransService;
class ObTransMemoryStat;
}  // namespace transaction
namespace observer {
class ObGVTransMemoryStat : public common::ObVirtualTableScannerIterator {
public:
  explicit ObGVTransMemoryStat(transaction::ObTransService* trans_service) : trans_service_(trans_service)
  {
    reset();
  }
  virtual ~ObGVTransMemoryStat()
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
  char mod_type_[transaction::ObTransMemoryStat::OB_TRANS_MEMORY_MOD_TYPE_SIZE];

private:
  transaction::ObTransService* trans_service_;
  transaction::ObTransMemStatIterator mem_iter_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObGVTransMemoryStat);
};
}  // namespace observer
}  // namespace oceanbase
#endif /* OB_ALL_VIRTUAL_TRANS_PARTITION_STAT_H */
