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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_PARTITION_AMPLIFICATION_STAT_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_PARTITION_AMPLIFICATION_STAT_

#include "share/ob_virtual_table_scanner_iterator.h"

namespace oceanbase {
namespace storage {
class ObPartitionService;
class ObPGPartitionIterator;
}  // namespace storage
namespace observer {

class ObAllPartitionAmplificationStat : public common::ObVirtualTableScannerIterator {
public:
  ObAllPartitionAmplificationStat();
  virtual ~ObAllPartitionAmplificationStat();
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
    PARTITION_ID,
    PARTITION_CNT,
    DIRTY_1,
    DIRTY_3,
    DIRTY_5,
    DIRTY_10,
    DIRTY_15,
    DIRTY_20,
    DIRTY_30,
    DIRTY_50,
    DIRTY_75,
    DIRTY_100,
    BLOCK_CNT
  };

  common::ObAddr addr_;
  storage::ObPartitionService* partition_service_;
  storage::ObPGPartitionIterator* ptt_iter_;

  char ip_buf_[common::OB_IP_STR_BUFF];

  DISALLOW_COPY_AND_ASSIGN(ObAllPartitionAmplificationStat);
};

}  // namespace observer
}  // namespace oceanbase

#endif /* OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_PARTITION_AMPLIFICATION_STAT_ */
