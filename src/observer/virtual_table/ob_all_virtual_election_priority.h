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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_ELECTION_PRIORITY_H_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_ELECTION_PRIORITY_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/ob_scanner.h"
#include "common/row/ob_row.h"

namespace oceanbase {
namespace storage {
class ObPartitionService;
class ObIPartitionGroupIterator;
}  // namespace storage
namespace observer {
class ObAllVirtualElectionPriority : public common::ObVirtualTableScannerIterator {
public:
  explicit ObAllVirtualElectionPriority(storage::ObPartitionService* partition_service);
  virtual ~ObAllVirtualElectionPriority();

public:
  virtual int inner_get_next_row(common::ObNewRow*& row);
  void set_addr(common::ObAddr& addr)
  {
    addr_ = addr;
  }
  void destroy();

private:
  int inner_get_next_row_(common::ObNewRow*& row);
  int prepare_get_election_priority_();
  int finish_get_election_priority_();

private:
  storage::ObPartitionService* partition_service_;
  storage::ObIPartitionGroupIterator* partition_iter_;

private:
  common::ObAddr addr_;
  char server_ip_buff_[common::OB_IP_STR_BUFF];
};  // class ObAllVirtualElectionPriority
}  // namespace observer
}  // namespace oceanbase
#endif  // OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_ELECTION_PRIORITY_H_
