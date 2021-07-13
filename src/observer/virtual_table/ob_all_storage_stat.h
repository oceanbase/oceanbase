/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software
 * according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *
 * http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A
 * PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_STORAGE_STAT_TABLE_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_STORAGE_STAT_TABLE_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "storage/ob_i_partition_storage.h"

namespace oceanbase {
namespace common {
class ObObj;
}
namespace storage {
class ObPGPartitionIterator;
class ObPGPartition;
}  // namespace storage

namespace observer {

class ObInfoSchemaStorageStatTable : public common::ObVirtualTableScannerIterator {
public:
  ObInfoSchemaStorageStatTable();
  virtual ~ObInfoSchemaStorageStatTable();
  virtual int inner_get_next_row(common::ObNewRow*& row);
  virtual void reset();
  inline void set_addr(common::ObAddr& addr)
  {
    addr_ = &addr;
  }
  virtual int set_ip(common::ObAddr* addr);

private:
  enum STORAGE_COLUMN {
    TENANT_ID = common::OB_APP_MIN_COLUMN_ID,
    SVR_IP,
    SVR_PORT,
    TABLE_ID,
    PARTITION_CNT,
    PARTITION_ID,
    MAJOR_VERSION,
    MINOR_VERSION,
    SSTABLE_ID,
    ROLE,
    DATA_CHECKSUM,
    COLUM_CHECKSUM,
    MACRO_BLOCKS,
    OCCUPY_SIZE,
    USED_SIZE,
    ROW_COUNT,
    STORE_TYPE,
    PROGRESSIVE_MERGE_START_VERSION,
    PROGRESSIVE_MERGE_END_VERSION
  };
  common::ObAddr* addr_;
  uint32_t sstable_iter_;
  common::ObString ipstr_;
  int32_t port_;
  storage::ObPGPartitionIterator* pg_partition_iter_;
  storage::ObPGPartition* cur_partition_;
  storage::ObTablesHandle stores_handle_;
  common::ObSEArray<storage::ObSSTable*, common::OB_MAX_INDEX_PER_TABLE + 1> sstable_list_;
  char column_checksum_[common::OB_MAX_VARCHAR_LENGTH + 1];
  char macro_blocks_[common::OB_MAX_VARCHAR_LENGTH + 1];
  DISALLOW_COPY_AND_ASSIGN(ObInfoSchemaStorageStatTable);
};

}  // namespace observer
}  // namespace oceanbase
#endif /* OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_STORAGE_STAT_TABLE */
