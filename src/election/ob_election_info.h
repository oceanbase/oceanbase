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

#ifndef OCEANBASE_ELECTION_OB_ELECTION_INFO_
#define OCEANBASE_ELECTION_OB_ELECTION_INFO_

#include "common/ob_member_list.h"
#include "common/ob_partition_key.h"
#include "lib/net/ob_addr.h"
#include "share/ob_define.h"
#include "ob_election_base.h"
#include "ob_election_group_id.h"

namespace oceanbase {

namespace election {
class ObElectionInfo {
public:
  ObElectionInfo()
  {
    reset();
  }
  virtual ~ObElectionInfo()
  {
    destroy();
  }
  void reset();
  void destroy()
  {
    reset();
  }

public:
  const common::ObAddr& get_self() const
  {
    return self_;
  }
  uint64_t get_table_id() const
  {
    return partition_.get_table_id();
  }
  int64_t get_partition_id() const
  {
    return partition_.get_partition_id();
  }
  bool is_running() const
  {
    return is_running_;
  }
  bool is_changing_leader() const
  {
    return is_changing_leader_;
  }
  const common::ObAddr& get_current_leader() const
  {
    return current_leader_;
  }
  const common::ObAddr& get_proposal_leader() const
  {
    return proposal_leader_;
  }
  const common::ObAddr& get_previous_leader() const
  {
    return previous_leader_;
  }
  const common::ObMemberList& get_member_list() const
  {
    return curr_candidates_;
  }
  int64_t get_replica_num() const
  {
    return replica_num_;
  }
  int64_t get_lease_start() const
  {
    return leader_lease_.first;
  }
  int64_t get_lease_end() const
  {
    return leader_lease_.second;
  }
  int64_t get_time_offset() const
  {
    return election_time_offset_;
  }
  int64_t get_active_timestamp() const
  {
    return active_timestamp_;
  }
  int64_t get_leader_epoch() const
  {
    return leader_epoch_;
  }
  int64_t get_T1_timestamp() const
  {
    return T1_timestamp_;
  }
  int64_t get_takeover_t1_timestamp() const
  {
    return takeover_t1_timestamp_;
  }
  int64_t get_state() const
  {
    return state_;
  }
  int64_t get_role() const
  {
    return role_;
  }
  int64_t get_stage() const
  {
    return stage_;
  }
  const common::ObPartitionKey& get_partition() const
  {
    return partition_;
  }
  uint64_t get_eg_id_hash() const
  {
    return eg_id_.hash();
  }
  int64_t get_last_leader_revoke_ts() const
  {
    return ATOMIC_LOAD(&last_leader_revoke_time_);
  }

  TO_STRING_KV(K_(is_running), K_(is_changing_leader), K_(self), K_(proposal_leader), K_(current_leader),
      K_(previous_leader), K_(curr_candidates), K_(curr_membership_version), K_(replica_num), K_(leader_lease),
      K_(election_time_offset), K_(active_timestamp), K_(T1_timestamp), K_(leader_epoch), K_(state), K_(role),
      K_(stage), K_(type), K_(change_leader_timestamp), K_(eg_id), K_(last_leader_revoke_time));

public:
  class State {
  public:
    static const int32_t INVALID = -1;
    static const int32_t D_IDLE = 0;
    static const int32_t V_IDLE = 1;
    // query state
    static const int32_t QUERY = 2;
    // decentralized state
    static const int32_t D_PREPARE = 3;
    static const int32_t D_VOTING = 4;
    static const int32_t D_COUNTING = 5;
    static const int32_t D_SUCCESS = 6;
    // centralized state
    static const int32_t L_V_PREPARE = 7;
    static const int32_t L_V_VOTING = 8;
    static const int32_t L_V_COUNTING = 9;
    static const int32_t F_V_PREPARE = 10;
    static const int32_t F_V_VOTING = 11;
    static const int32_t V_SUCCESS = 12;
    static const int32_t MAX = 13;

  public:
    static bool is_valid(const int32_t state)
    {
      return state > INVALID && state < MAX;
    }
    static bool is_decentralized_state(const int32_t state)
    {
      return (D_IDLE == state || (D_PREPARE <= state && D_SUCCESS >= state));
    }
    static bool is_centralized_state(const int32_t state)
    {
      return ((V_IDLE == state) || (L_V_PREPARE <= state && V_SUCCESS >= state));
    }
  };

  class Ops {
  public:
    static const int32_t INVALID = -1;
    static const int32_t RESET = 0;
    static const int32_t D_ELECT = 1;
    static const int32_t D_PREPARE = 2;
    static const int32_t D_VOTE = 3;
    static const int32_t D_COUNT = 4;
    static const int32_t D_SUCCESS = 5;
    static const int32_t V_ELECT = 6;
    static const int32_t V_PREPARE = 7;
    static const int32_t V_VOTE = 8;
    static const int32_t V_COUNT = 9;
    static const int32_t V_SUCCESS = 10;
    static const int32_t CHANGE_LEADER = 11;
    static const int32_t QUERY_RESPONSE = 12;
    static const int32_t LEADER_REVOKE = 13;
    static const int32_t MAX = 14;

  public:
    static bool is_valid(const int32_t state)
    {
      return state > INVALID && state < MAX;
    }
  };

  class StateHelper {
  public:
    explicit StateHelper(int32_t& state) : state_(state)
    {}
    ~StateHelper()
    {}
    int switch_state(const int32_t op);

  private:
    int32_t& state_;
  };

protected:
  bool is_self_(const common::ObAddr& addr) const
  {
    return self_ == addr;
  }

protected:
  bool is_running_;
  // mark need change leader or not
  bool need_change_leader_;
  // mark is in changing leader process
  bool is_changing_leader_;
  int64_t change_leader_timestamp_;
  // partition
  common::ObPartitionKey partition_;
  // myself
  common::ObAddr self_;
  // The proposal leader. proposed by current leader
  common::ObAddr proposal_leader_;
  // The current legal leader
  common::ObAddr current_leader_;
  // The previous legal leader
  common::ObAddr previous_leader_;
  // members
  common::ObMemberList curr_candidates_;
  // membership version
  int64_t curr_membership_version_;
  // replication number
  int64_t replica_num_;
  // current leader's lease
  lease_t leader_lease_;
  // elected by changing leader
  bool is_elected_by_changing_leader_;
  // virtual election time offset
  int64_t election_time_offset_;
  // election is active after active time
  int64_t active_timestamp_;
  // election start time
  int64_t start_timestamp_;
  int64_t T1_timestamp_;
  // leader takeover timestamp
  int64_t takeover_t1_timestamp_;
  // leader epoch timestamp
  int64_t leader_epoch_;
  // current election state
  int32_t state_;
  // current election role
  ObElectionRole role_;
  // current election stage. prepare/vote/success
  ObElectionStage stage_;
  // current election member type
  ObElectionMemberType type_;
  // id of group(as a column of virtual table)
  ObElectionGroupId eg_id_;
  int64_t last_leader_revoke_time_;
};

}  // namespace election
}  // namespace oceanbase

#endif  // OCEANBASE_ELECTION_OB_ELECTION_INFO_
