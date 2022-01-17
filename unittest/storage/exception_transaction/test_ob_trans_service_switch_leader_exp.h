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

#ifndef OB_TEST_OB_TEST_ERROR_TRANSACTION_SWITCH_LEADER_EXP_H_
#define OB_TEST_OB_TEST_ERROR_TRANSACTION_SWITCH_LEADER_EXP_H_

#include "share/ob_errno.h"
#include "share/ob_define.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_print_utils.h"
#include "common/ob_partition_key.h"
#include "lib/allocator/ob_mod_define.h"

namespace oceanbase {
namespace unittest {
class ObTransSwitchLeader {
public:
  ObTransSwitchLeader() : partitions_(common::ObModIds::OB_TRANS_PARTITION_ARRAY)
  {
    reset();
  }
  virtual ~ObTransSwitchLeader()
  {}
  void init(const bool need_change, const bool change_back, const int64_t change_num)
  {
    need_change_ = need_change;
    change_num_ = change_num;
    if (1 < change_num) {
      change_back_ = change_back;
    }
  }
  void set_partitions(const common::ObPartitionArray& partitions)
  {
    partitions_.assign(partitions);
  }
  void reset()
  {
    need_change_ = false;
    change_back_ = false;
    change_num_ = 0;
    partitions_.reset();
  }
  bool check_switch_leader();
  bool check_switch_leader_partition(const common::ObPartitionKey& partition);

  TO_STRING_KV(K_(need_change), K_(change_back), K_(change_num), K_(partitions));

public:
  bool need_change_;
  bool change_back_;
  int64_t change_num_;
  common::ObPartitionArray partitions_;
};

enum ObTransSwitchLeaderType {
  OB_SWITCH_LEADER_UNKNOWN = -1,
  OB_START_PARTICIPANT_SWITCH_LEADER = 0,
  OB_END_PARTICIPANT_SWITCH_LEADER,
  OB_CREATE_CTX_SWITCH_LEADER,
  OB_STMT_ROLLBACK_SWITCH_LEADER,
  OB_COMMIT_REQUEST_SWITCH_LEADER,
  OB_COMMIT_RESPONSE_SWITCH_LEADER,
  OB_ABORT_REQUEST_SWITCH_LEADER,
  OB_ABORT_RESPONSE_SWITCH_LEADER,
  OB_PREPARE_2PC_REQUEST_SWITCH_LEADER,
  OB_PREPARE_2PC_RESPONSE_SWITCH_LEADER,
  OB_COMMIT_2PC_REQUEST_SWITCH_LEADER,
  OB_COMMIT_2PC_RESPONSE_SWITCH_LEADER,
  OB_ABORT_2PC_REQUEST_SWITCH_LEADER,
  OB_ABORT_2PC_RESPONSE_SWITCH_LEADER,
  OB_CLEAR_2PC_REQUEST_SWITCH_LEADER,
  OB_CLEAR_2PC_RESPONSE_SWITCH_LEADER,
  OB_SWITCH_LEADER_COUNT,
};

class ObTransSwitchLeaderException {
public:
  ObTransSwitchLeaderException()
  {
    reset();
  }
  virtual ~ObTransSwitchLeaderException()
  {}

public:
  void reset();
  ObTransSwitchLeader* get_switch_leader_exp(const int64_t type);
  int set_switch_leader(const int64_t type, const bool need_change, const bool change_back, const int64_t change_num);
  int set_switch_leader_partitions(const int64_t type, const common::ObPartitionArray& partitions);

public:
  ObTransSwitchLeader switch_leaders_[OB_SWITCH_LEADER_COUNT];
};

}  // namespace unittest
}  // namespace oceanbase
#endif
