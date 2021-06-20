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

#ifndef _OB_MOCK_ELECTION_H
#define _OB_MOCK_ELECTION_H

#include "election/ob_election.h"

namespace oceanbase {
namespace unittest {
class MockElection : public election::ObIElection {
public:
  virtual int init(const common::ObPartitionKey&, const common::ObAddr&, election::ObIElectionRpc*,
      common::ObTimeWheel*, const int64_t, election::ObIElectionCallback*, election::ObIElectionGroupMgr*,
      election::ObElectionEventHistoryArray*)
  {
    return 0;
  }
  virtual void destroy()
  {}
  virtual int start()
  {
    return 0;
  }
  virtual int stop()
  {
    return 0;
  }

public:
  virtual int handle_devote_prepare(const election::ObElectionMsgDEPrepare&, obrpc::ObElectionRpcResult&)
  {
    return 0;
  }
  virtual int handle_devote_vote(const election::ObElectionMsgDEVote&, obrpc::ObElectionRpcResult&)
  {
    return 0;
  }
  virtual int handle_devote_success(const election::ObElectionMsgDESuccess&, obrpc::ObElectionRpcResult&)
  {
    return 0;
  }
  virtual int handle_vote_prepare(const election::ObElectionMsgPrepare&, obrpc::ObElectionRpcResult&)
  {
    return 0;
  }
  virtual int handle_vote_vote(const election::ObElectionMsgVote&, obrpc::ObElectionRpcResult&)
  {
    return 0;
  }
  virtual int handle_vote_success(const election::ObElectionMsgSuccess&, obrpc::ObElectionRpcResult&)
  {
    return 0;
  }
  virtual int handle_query_leader(const election::ObElectionQueryLeader&, obrpc::ObElectionRpcResult&)
  {
    return 0;
  }
  virtual int handle_query_leader_response(const election::ObElectionQueryLeaderResponse&, obrpc::ObElectionRpcResult&)
  {
    return 0;
  }

public:
  virtual int set_candidate(const int64_t, const common::ObMemberList&, const int64_t)
  {
    return 0;
  }
  virtual int change_leader_async(const common::ObAddr&, common::ObTsWindows&)
  {
    return 0;
  }
  virtual int change_leader_to_self_async() override
  {
    return 0;
  }
  virtual int force_leader_async()
  {
    return 0;
  }
  virtual int get_curr_candidate(common::ObMemberList&) const
  {
    return 0;
  }
  virtual int get_valid_candidate(common::ObMemberList&) const
  {
    return 0;
  }
  virtual int get_leader(common::ObAddr&, common::ObAddr&, int64_t&, bool&) const
  {
    return 0;
  }
  virtual int get_leader(common::ObAddr&, int64_t&, bool&, common::ObTsWindows&) const
  {
    return 0;
  }
  virtual int get_current_leader(common::ObAddr&) const
  {
    return 0;
  }
  virtual int get_election_info(election::ObElectionInfo&) const
  {
    return 0;
  }
  virtual void leader_revoke(const uint32_t)
  {}
  virtual int inc_replica_num()
  {
    return 0;
  }
  virtual int dec_replica_num()
  {
    return 0;
  }
  virtual int set_replica_num(const int64_t)
  {
    return 0;
  }
  virtual int64_t get_election_time_offset() const
  {
    return 0;
  }
  virtual int set_offline()
  {
    return 0;
  }

public:
  virtual int leader_takeover(const common::ObAddr&, const int64_t, int64_t&)
  {
    return 0;
  }
  virtual int64_t get_current_ts() const
  {
    return ts_;
  }
  virtual int get_priority(election::ObElectionPriority&) const
  {
    return 0;
  }
  virtual int get_timestamp(int64_t&, common::ObAddr&) const
  {
    return 0;
  }
  virtual int64_t get_last_leader_revoke_time() const
  {
    return 0;
  }

public:
  // for election group
  virtual int move_out_election_group(const election::ObElectionGroupId&)
  {
    return 0;
  }
  virtual int move_into_election_group(const election::ObElectionGroupId&)
  {
    return 0;
  }
  virtual const election::lease_t& get_leader_lease() const
  {
    return lease_;
  }
  virtual int64_t get_replica_num() const
  {
    return 0;
  }
  virtual uint64_t get_eg_id_hash() const
  {
    return 0;
  }
  virtual bool is_offline() const
  {
    return 0;
  }
  void set_current_ts(int64_t ts)
  {
    ts_ = ts;
  }

private:
  election::lease_t lease_;
  int64_t ts_;
};
}  // namespace unittest
}  // namespace oceanbase
#endif
