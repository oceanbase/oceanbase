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

#include "ob_election_group_info.h"
#include "ob_election_info.h"

namespace oceanbase {
namespace election {

void ObElectionGroupInfo::reset()
{
  is_running_ = false;
  eg_id_.reset();
  eg_version_ = 0;
  eg_part_cnt_ = 0;
  is_all_part_merged_in_ = false;
  is_priority_allow_reappoint_ = false;
  self_.reset();
  tenant_id_ = 0;
  priority_.reset();
  curr_leader_.reset();
  member_list_.reset();
  replica_num_ = 0;
  takeover_t1_timestamp_ = 0;
  T1_timestamp_ = 0;
  leader_lease_.first = 0;
  leader_lease_.second = 0;
  role_ = ROLE_UNKNOWN;
  state_ = ObElectionInfo::State::V_IDLE;  // state setted V_IDLE in initial
  pre_destroy_state_ = false;
}

}  // namespace election
}  // namespace oceanbase
