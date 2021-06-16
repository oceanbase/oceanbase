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

#ifndef OB_ALL_VIRTUAL_PARTITION_REPLAY_STATUS_H_
#define OB_ALL_VIRTUAL_PARTITION_REPLAY_STATUS_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/ob_scanner.h"
#include "common/row/ob_row.h"
#include "lib/container/ob_se_array.h"

namespace oceanbase {
namespace storage {
class ObReplayStatus;
class ObPartitionService;
class ObIPartitionGroupIterator;
}  // namespace storage
namespace observer {
class ObAllVirtualPartitionReplayStatus : public common::ObVirtualTableScannerIterator {
public:
  ObAllVirtualPartitionReplayStatus();
  virtual ~ObAllVirtualPartitionReplayStatus();

public:
  virtual int inner_get_next_row(common::ObNewRow*& row);
  void reset();
  inline void set_partition_service(storage::ObPartitionService* partition_service)
  {
    ps_ = partition_service;
  }

  inline void set_addr(common::ObAddr& addr)
  {
    addr_ = addr;
  }

private:
  int get_last_replay_log_type(int64_t last_replay_log_type, char* buf, int64_t buf_len);
  int get_post_barrier_status(int64_t post_barrier_status, char* buf, int64_t buf_len);

private:
  storage::ObPartitionService* ps_;
  common::ObAddr addr_;
  storage::ObIPartitionGroupIterator* ptt_iter_;
  char ip_buf_[common::OB_IP_STR_BUFF];
  char post_barrier_status_[common::MAX_FREEZE_SUBMIT_STATUS_LENGTH];
  char last_replay_log_type_[common::MAX_REPLAY_LOG_TYPE_LENGTH];

private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualPartitionReplayStatus);
};

}  // namespace observer
}  // namespace oceanbase
#endif /* OB_ALL_VIRTUAL_PARTITION_REPLAY_STATUS_H_ */
