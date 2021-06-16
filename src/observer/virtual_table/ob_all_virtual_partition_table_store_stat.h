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

#ifndef OB_ALL_VIRTUAL_PARTITION_TABLE_STORE_STAT_H_
#define OB_ALL_VIRTUAL_PARTITION_TABLE_STORE_STAT_H_
#include "share/ob_virtual_table_scanner_iterator.h"
#include "storage/ob_table_store_stat_mgr.h"
#include "storage/ob_partition_storage.h"
#include "storage/ob_partition_service.h"

namespace oceanbase {
namespace storage {
class ObPartitionService;
}
namespace observer {

class ObAllVirtualPartitionTableStoreStat : public common::ObVirtualTableScannerIterator {
public:
  ObAllVirtualPartitionTableStoreStat();
  virtual ~ObAllVirtualPartitionTableStoreStat();
  int init(storage::ObPartitionService* partition_service);
  virtual int inner_get_next_row(common::ObNewRow*& row);
  virtual void reset();

protected:
  int fill_cells(const storage::ObTableStoreStat& stat);

private:
  int get_rowkey_prefix_info(const common::ObPartitionKey& pkey);
  char ip_buf_[common::OB_IP_STR_BUFF];
  char rowkey_prefix_info_[common::COLUMN_DEFAULT_LENGTH];  // json format
  storage::ObTableStoreStat stat_;
  storage::ObTableStoreStatIterator stat_iter_;
  storage::ObPartitionService* partition_service_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualPartitionTableStoreStat);
};

} /* namespace observer */
} /* namespace oceanbase */
#endif /* OB_ALL_VIRTUAL_PARTITION_TABLE_STORE_STAT_H_ */
