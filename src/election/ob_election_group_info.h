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

#ifndef OCEANBASE_ELECTION_OB_ELECTION_GROUP_INFO_
#define OCEANBASE_ELECTION_OB_ELECTION_GROUP_INFO_

#include <stdint.h>
#include "lib/net/ob_addr.h"
#include "lib/list/ob_list.h"
#include "lib/utility/utility.h"
#include "common/ob_member_list.h"
#include "ob_election_group_id.h"
#include "ob_election_group_priority.h"
#include "ob_election_base.h"

namespace oceanbase {
namespace election {

class ObElectionGroupInfo {
public:
  ObElectionGroupInfo()
  {
    reset();
  }
  ~ObElectionGroupInfo()
  {
    reset();
  }
  void reset();

public:
  bool is_running() const
  {
    return is_running_;
  }
  void set_is_running(const bool is_running)
  {
    is_running_ = is_running;
  }
  int64_t get_eg_id_hash() const
  {
    return eg_id_.hash();
  }
  int64_t get_create_time() const
  {
    return eg_id_.get_create_time();
  }
  void set_eg_id(const ObElectionGroupId& eg_id)
  {
    eg_id_ = eg_id;
  }
  int64_t get_eg_version() const
  {
    return eg_version_;
  }
  void set_eg_version(const int64_t eg_version)
  {
    eg_version_ = eg_version;
  }
  int64_t get_eg_part_cnt() const
  {
    return eg_part_cnt_;
  }
  void set_eg_part_cnt(const int64_t eg_part_cnt)
  {
    eg_part_cnt_ = eg_part_cnt;
  }
  bool is_all_part_merged_in() const
  {
    return is_all_part_merged_in_;
  }
  void set_is_all_part_merged_in(const bool is_all_part_merged_in)
  {
    is_all_part_merged_in_ = is_all_part_merged_in;
  }
  bool is_priority_allow_reappoint() const
  {
    return is_priority_allow_reappoint_;
  }
  void set_is_priority_allow_reappoint(const bool is_priority_allow_reappoint)
  {
    is_priority_allow_reappoint_ = is_priority_allow_reappoint;
  }
  const common::ObAddr& get_self() const
  {
    return self_;
  }
  void set_self_addr(const common::ObAddr& self)
  {
    self_ = self;
  }
  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  void set_tenant_id(const uint64_t tenant_id)
  {
    tenant_id_ = tenant_id;
  }
  void set_priority(const ObElectionGroupPriority& priority)
  {
    priority_ = priority;
  }
  bool is_candidate() const
  {
    return priority_.is_candidate();
  }
  int64_t get_system_score() const
  {
    return priority_.get_system_score();
  }
  const common::ObAddr& get_current_leader() const
  {
    return curr_leader_;
  }
  void set_current_leader(const common::ObAddr& leader)
  {
    curr_leader_ = leader;
  }
  const common::ObMemberList& get_member_list() const
  {
    return member_list_;
  }
  void set_member_list(const common::ObMemberList& mlist)
  {
    member_list_ = mlist;
  }
  int64_t get_replica_num() const
  {
    return replica_num_;
  }
  void set_replica_num(const int64_t replica_num)
  {
    replica_num_ = replica_num;
  }
  int64_t get_takeover_t1_timestamp() const
  {
    return takeover_t1_timestamp_;
  }
  void set_takeover_t1_timestamp(const int64_t takeover_t1_timestamp)
  {
    takeover_t1_timestamp_ = takeover_t1_timestamp;
  }
  int64_t get_T1_timestamp() const
  {
    return T1_timestamp_;
  }
  void set_T1_timestamp(const int64_t T1_timestamp)
  {
    T1_timestamp_ = T1_timestamp;
  }
  void set_leader_lease(const lease_t lease)
  {
    leader_lease_ = lease;
  }
  int64_t get_lease_start() const
  {
    return leader_lease_.first;
  }
  int64_t get_lease_end() const
  {
    return leader_lease_.second;
  }
  int64_t get_role() const
  {
    return role_;
  }
  void set_role(const ObElectionRole& role)
  {
    role_ = role;
  }
  int64_t get_state() const
  {
    return state_;
  }
  void set_state(const int32_t state)
  {
    state_ = state;
  }
  void set_pre_destroy_state(const bool pre_destroy_state)
  {
    pre_destroy_state_ = pre_destroy_state;
  }
  bool is_pre_destroy_state() const
  {
    return pre_destroy_state_;
  }

  TO_STRING_KV(K_(is_running), K_(eg_id), K_(eg_version), K_(eg_part_cnt), K_(is_all_part_merged_in),
      K_(is_priority_allow_reappoint), K_(self), K_(tenant_id), K_(priority), K_(curr_leader), K_(member_list),
      K_(replica_num), K_(takeover_t1_timestamp), K_(T1_timestamp), K_(leader_lease), K_(role), K_(state),
      K_(pre_destroy_state));

private:
  bool is_running_;
  ObElectionGroupId eg_id_;
  int64_t eg_version_;
  int64_t eg_part_cnt_;
  bool is_all_part_merged_in_;
  bool is_priority_allow_reappoint_;
  common::ObAddr self_;
  uint64_t tenant_id_;
  ObElectionGroupPriority priority_;
  common::ObAddr curr_leader_;
  common::ObMemberList member_list_;  // only valid on leader
  int64_t replica_num_;               // only valid on leader
  int64_t takeover_t1_timestamp_;
  int64_t T1_timestamp_;
  lease_t leader_lease_;
  ObElectionRole role_;
  int32_t state_;
  bool pre_destroy_state_;
};

}  // namespace election
}  // namespace oceanbase
#endif  // OCEANBASE_ELECTION_OB_ELECTION_GROUP_INFO_
