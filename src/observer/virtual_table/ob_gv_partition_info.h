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

#ifndef OB_ALL_VIRTUAL_PARTITION_INFO_H_
#define OB_ALL_VIRTUAL_PARTITION_INFO_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/ob_scanner.h"
#include "common/row/ob_row.h"
#include "storage/ob_pg_mgr.h"
#include "lib/container/ob_se_array.h"

namespace oceanbase {
namespace observer {
class ObGVPartitionInfo : public common::ObVirtualTableScannerIterator {
public:
  ObGVPartitionInfo();
  virtual ~ObGVPartitionInfo();

public:
  virtual int inner_get_next_row(common::ObNewRow*& row);
  virtual void reset();
  inline void set_partition_service(storage::ObPartitionService* partition_service)
  {
    partition_service_ = partition_service;
  }
  inline void set_addr(common::ObAddr& addr)
  {
    addr_ = addr;
  }

private:
  int freeze_status_to_string(int64_t freeze_status, char* buf, int64_t buf_len);
  int partition_state_to_string(int64_t partition_state, char* buf, int16_t buf_len);

private:
  storage::ObPartitionService* partition_service_;
  common::ObAddr addr_;
  storage::ObIPartitionGroupIterator* pg_iter_;
  char ip_buf_[common::OB_IP_STR_BUFF];
  char freeze_status_buf_[common::TABLE_MAX_VALUE_LENGTH];
  char partition_state_buf_[common::TABLE_MAX_VALUE_LENGTH];
  char frozen_version_buf_[common::TABLE_MAX_VALUE_LENGTH];
  char merged_version_buf_[common::TABLE_MAX_VALUE_LENGTH];
  char active_version_buf_[common::TABLE_MAX_VALUE_LENGTH];
  char pkey_buf_[common::OB_TMP_BUF_SIZE_256];

private:
  DISALLOW_COPY_AND_ASSIGN(ObGVPartitionInfo);
};

}  // namespace observer
}  // namespace oceanbase
#endif /* OB_ALL_VIRTUAL_PARTITION_INFO_H */
