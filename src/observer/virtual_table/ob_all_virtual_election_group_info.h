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

#ifndef OB_ALL_VIRTUAL_ELECTION_GROUP_INFO_H_
#define OB_ALL_VIRTUAL_ELECTION_GROUP_INFO_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/ob_scanner.h"
#include "common/row/ob_row.h"
#include "share/ob_define.h"
#include "election/ob_election_group_mgr.h"

namespace oceanbase {
namespace observer {
class ObGVElectionGroupInfo : public common::ObVirtualTableScannerIterator {
public:
  ObGVElectionGroupInfo() : ObVirtualTableScannerIterator()
  {
    reset();
  }
  explicit ObGVElectionGroupInfo(election::ObElectionMgr* election_mgr)
      : ObVirtualTableScannerIterator(), election_mgr_(election_mgr)
  {
    reset();
  }
  virtual ~ObGVElectionGroupInfo()
  {
    destroy();
  }

public:
  int inner_get_next_row(common::ObNewRow*& row);
  void reset();
  void destroy();

private:
  int prepare_to_read_();

private:
  election::ObElectionMgr* election_mgr_;
  char ip_buffer_[common::OB_IP_STR_BUFF];
  char current_leader_ip_port_buffer_[common::OB_IP_PORT_STR_BUFF];
  char previous_leader_ip_port_buffer_[common::OB_IP_PORT_STR_BUFF];
  char proposal_leader_ip_port_buffer_[common::OB_IP_PORT_STR_BUFF];
  char member_list_buffer_[common::MAX_MEMBER_LIST_LENGTH];
  election::ObElectionGroupInfoIterator election_info_iter_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObGVElectionGroupInfo);
};

}  // namespace observer
}  // namespace oceanbase

#endif
