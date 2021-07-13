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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_PARTITION_STORE_INFO_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_PARTITION_STORE_INFO_

#include "share/ob_virtual_table_scanner_iterator.h"

namespace oceanbase {
namespace storage {
class ObPartitionStoreInfo;
class ObPartitionService;
class ObIPartitionGroupIterator;
}  // namespace storage

namespace observer {

class ObAllPartitionStoreInfo : public common::ObVirtualTableScannerIterator {
public:
  ObAllPartitionStoreInfo();
  virtual ~ObAllPartitionStoreInfo();
  virtual int inner_get_next_row(common::ObNewRow*& row) override;
  int init(common::ObAddr& addr, storage::ObPartitionService* partition_service)
  {
    int ret = OB_SUCCESS;
    addr_ = addr;
    partition_service_ = partition_service;
    return ret;
  }
  virtual void reset() override;

private:
  enum TABLE_COLUMN {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TENANT_ID,
    TABLE_ID,
    PARTITION_IDX,
    PARTITION_CNT,
    IS_RESTORE,
    MIGRATE_STATUS,
    MIGRATE_TIMESTAMP,
    REPLICA_TYPE,
    SPLIT_STATE,
    MULTI_VERSION_START,
    REPORT_VERSION,
    REPORT_ROW_COUNT,
    REPORT_DATA_CHECKSUM,
    REPORT_DATA_SIZE,
    REPORT_REQUIRED_SIZE,
    READABLE_TS,
  };
  int get_next_partition_store_info(storage::ObPartitionStoreInfo& info);

  common::ObAddr addr_;
  storage::ObPartitionService* partition_service_;
  storage::ObIPartitionGroupIterator* ptt_iter_;
  common::ObPartitionKey pkey_;
  storage::ObIPartitionGroup* pg_;
  common::ObPartitionArray pkeys_;
  int64_t iter_;

  char ip_buf_[common::OB_IP_STR_BUFF];

  DISALLOW_COPY_AND_ASSIGN(ObAllPartitionStoreInfo);
};

}  // namespace observer
}  // namespace oceanbase

#endif /* OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_PARTITION_STORE_INFO_ */
