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

#ifndef OCEANBASE_ELECTION_OB_ELECTION_MSG_POOL_
#define OCEANBASE_ELECTION_OB_ELECTION_MSG_POOL_

#include "lib/net/ob_addr.h"
#include "lib/list/ob_list.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/utility/ob_print_utils.h"
#include "common/data_buffer.h"
#include "ob_election_base.h"
#include "ob_election_msg.h"
#include "ob_election_time_def.h"
#include "ob_election_async_log.h"

namespace oceanbase {
namespace election {
class ObIElection;
class ObElectionVoteMsgPool {
public:
  ObElectionVoteMsgPool()
      : election_(nullptr),
        deprepare_recorder_(election_, partition_),
        devote_recorder_(election_, partition_),
        vote_recorder_(election_, partition_)
  {
    reset();
  }
  virtual ~ObElectionVoteMsgPool()
  {
    destroy();
  }
  int init(const common::ObAddr& self, ObIElection* election);
  void reset();
  void destroy()
  {
    reset();
  }
  void correct_recordT1_if_necessary(int64_t T1_timestamp);
  int store(const ObElectionVoteMsg& msg);
  int get_decentralized_candidate(common::ObAddr& server,
      ObElectionPriority& priority,  // get the highest prioirity of server
      const int64_t replica_num, const int64_t t1, int64_t& lease_time);
  int check_decentralized_majority(common::ObAddr& new_leader,
      int64_t& ticket,  // check if votes for self reach majority
      const int64_t replica_num, const int64_t t1);
  int check_centralized_majority(common::ObAddr& cur_leader,
      common::ObAddr& new_leader,  // check if votes for self reach majority
      ObElectionPriority& priority, const int64_t replica_num, const int64_t t1);
  int get_centralized_candidate(common::ObAddr& cur_leader, common::ObAddr& new_leader,  // get the leader vote for
      const int64_t t1);

private:
  class MsgRecorder {
    static const int64_t OB_MAX_ELECTION_MSG_COUNT = common::OB_MAX_MEMBER_NUMBER / 2 + 1;

  public:
    MsgRecorder(ObIElection*& election, const common::ObPartitionKey& partition)
        : cur_idx_(0), record_T1_timestamp_(common::OB_INVALID_TIMESTAMP), election_(election), partition_(partition)
    {}
    ~MsgRecorder() = default;
    void reset();
    void correct_recordT1_if_necessary(int64_t T1_timestamp);  // correct T1 ts if machine's time justed
    int record_msg(const ObElectionVoteMsg& msg);
    bool reach_majority(int replica_num_) const;
    int64_t get_record_T1_timestamp() const;
    int size() const;
    const common::ObAddr& get_new_leader() const;

  private:
    common::ObAddr record_member_[OB_MAX_ELECTION_MSG_COUNT];
    common::ObAddr new_leader_;
    int cur_idx_;
    int64_t record_T1_timestamp_;  // current record's T1
    ObIElection*& election_;       // get the diff of time, judge if msg valid
    const common::ObPartitionKey& partition_;
  };
  common::ObAddr self_;
  ObIElection* election_;  // get the diff of time, judge if msg valid
  bool is_inited_ = false;
  common::ObPartitionKey partition_;  // self partition, for logging
  MsgRecorder deprepare_recorder_;
  MsgRecorder devote_recorder_;
  MsgRecorder vote_recorder_;
  ObElectionMsgDEPrepare cached_devote_prepare_msg_;
  ObElectionMsgPrepare cached_vote_prepare_msg_;
  ObElectionMsgVote cached_new_leader_vote_msg_;
};

}  // namespace election
}  // namespace oceanbase

#endif  // OCEANBASE_ELECTION_OB_ELECTION_MSG_POOL_
