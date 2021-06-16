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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_CLOG_STAT_H_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_CLOG_STAT_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/ob_scanner.h"
#include "common/row/ob_row.h"

namespace oceanbase {
namespace storage {
class ObPartitionService;
class ObIPartitionGroupIterator;
}  // namespace storage
namespace observer {
class ObAllVirtualClogStat : public common::ObVirtualTableScannerIterator {
public:
  explicit ObAllVirtualClogStat(storage::ObPartitionService* partition_service);
  virtual ~ObAllVirtualClogStat();

public:
  virtual int inner_get_next_row(common::ObNewRow*& row);
  void destroy();

private:
  int prepare_get_clog_stat_();
  int finish_get_clog_stat_();

private:
  storage::ObPartitionService* partition_service_;
  storage::ObIPartitionGroupIterator* partition_iter_;

private:
  char server_ip_buff_[common::OB_IP_PORT_STR_BUFF];
  char leader_buff_[common::OB_IP_PORT_STR_BUFF];
  char freeze_version_buff_[common::MAX_VERSION_LENGTH];
  char member_list_buff_[common::MAX_MEMBER_LIST_LENGTH];
  char parent_buff_[common::OB_IP_PORT_STR_BUFF];
  char children_list_buff_[common::MAX_MEMBER_LIST_LENGTH];
};  // class ObAllVirtualClogStat
}  // namespace observer
}  // namespace oceanbase
#endif  // OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_CLOG_STAT_H_
