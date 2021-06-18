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
int64_t ObElectionGroupInfo::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(is_running), K_(eg_id), K_(eg_version), K_(eg_part_cnt), K_(is_all_part_merged_in),K_(is_priority_allow_reappoint), K_(self), K_(tenant_id), K_(priority), K_(curr_leader), K_(member_list),K_(replica_num), K_(takeover_t1_timestamp), K_(T1_timestamp), K_(leader_lease), K_(role), K_(state),K_(pre_destroy_state));
  J_OBJ_END();
  return pos;
}

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
