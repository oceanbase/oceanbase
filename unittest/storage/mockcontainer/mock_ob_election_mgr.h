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

#ifndef OCEANBASE_ELECTION_MOCK_OB_I_ELECTION_MGR_H_
#define OCEANBASE_ELECTION_MOCK_OB_I_ELECTION_MGR_H_

#include "election/ob_election_mgr.h"

namespace oceanbase {
namespace election {
class MockObIElectionMgr : public ObIElectionMgr {
public:
  MockObIElectionMgr()
  {}
  virtual ~MockObIElectionMgr()
  {}
  virtual int init(const common::ObAddr& self, obrpc::ObElectionRpcProxy* rpc_proxy, common::ObMySQLProxy& proxy)
  {
    UNUSED(self);
    UNUSED(rpc_proxy);
    UNUSED(proxy);
    return common::OB_SUCCESS;
  }
  virtual void destroy()
  {}
  virtual int start_partition(const common::ObPartitionKey& partition)
  {
    UNUSED(partition);
    return common::OB_SUCCESS;
  }
  virtual int start_partition(const common::ObPartitionKey& partition, const common::ObAddr& leader,
      const int64_t lease_start, int64_t& leader_epoch)
  {
    UNUSED(partition);
    UNUSED(leader);
    UNUSED(lease_start);
    UNUSED(leader_epoch);
    return common::OB_SUCCESS;
  }
  virtual int leader_revoke(const common::ObPartitionKey& partition)
  {
    UNUSED(partition);
    return common::OB_SUCCESS;
  }
  virtual int stop_partition(const common::ObPartitionKey& partition)
  {
    UNUSED(partition);
    return common::OB_SUCCESS;
  }
  virtual int start()
  {
    return common::OB_SUCCESS;
  }
  virtual int wait()
  {
    return common::OB_SUCCESS;
  }
  virtual int stop()
  {
    return common::OB_SUCCESS;
  }

public:
  virtual int add_partition(const common::ObPartitionKey& partition, const int64_t replica_num,
      ObIElectionCallback* election_cb, ObIElection*& election)
  {
    UNUSED(partition);
    UNUSED(replica_num);
    UNUSED(election_cb);
    UNUSED(election);
    return common::OB_SUCCESS;
  }
  virtual int remove_partition(const common::ObPartitionKey& partition)
  {
    UNUSED(partition);
    return common::OB_SUCCESS;
  }
  virtual int change_leader_async(const common::ObPartitionKey& partition, const common::ObAddr& leader)
  {
    UNUSED(partition);
    UNUSED(leader);
    return common::OB_SUCCESS;
  }
  virtual int change_leader_async(const common::ObPartitionKey& partition)
  {
    UNUSED(partition);
    return common::OB_SUCCESS;
  }
  virtual int revoke_all()
  {
    return common::OB_SUCCESS;
  }
  virtual int force_leader_async(const common::ObPartitionKey& partition)
  {
    UNUSED(partition);
    return common::OB_SUCCESS;
  }
  virtual int get_leader(
      const common::ObPartitionKey& partition, common::ObAddr& addr, common::ObAddr& previous_leader, int64_t& a) const
  {
    UNUSED(partition);
    UNUSED(addr);
    UNUSED(previous_leader);
    UNUSED(a);
    return common::OB_SUCCESS;
  }

public:
  virtual int handle_election_msg(const ObElectionMsgBuffer& msgbuf, obrpc::ObElectionRpcResult& result)
  {
    UNUSED(msgbuf);
    UNUSED(result);
    return common::OB_SUCCESS;
  }

public:
  virtual int set_candidate(const common::ObPartitionKey& partition, const common::ObMemberList& prev_mlist,
      const common::ObMemberList& curr_mlist)
  {
    UNUSED(partition);
    UNUSED(prev_mlist);
    UNUSED(curr_mlist);
    return common::OB_SUCCESS;
  }
  virtual int set_candidate(const common::ObPartitionKey& partition, const common::ObMemberList& curr_mlist)
  {
    UNUSED(partition);
    UNUSED(curr_mlist);
    return common::OB_SUCCESS;
  }
  virtual int set_candidate(
      const common::ObPartitionKey& partition, const int64_t a, const common::ObMemberList& curr_mlist, int64_t b)
  {
    UNUSED(partition);
    UNUSED(curr_mlist);
    UNUSED(a);
    UNUSED(b);
    return common::OB_SUCCESS;
  }
  virtual int get_prev_candidate(const common::ObPartitionKey& partition, common::ObMemberList& mlist) const
  {
    UNUSED(partition);
    UNUSED(mlist);
    return common::OB_SUCCESS;
  }
  virtual int get_curr_candidate(const common::ObPartitionKey& partition, common::ObMemberList& mlist) const
  {
    UNUSED(partition);
    UNUSED(mlist);
    return common::OB_SUCCESS;
  }
  virtual int set_offline(const common::ObPartitionKey& partition)
  {
    UNUSED(partition);
    return common::OB_SUCCESS;
  }
  virtual int get_valid_candidate(const common::ObPartitionKey& partition, common::ObMemberList& mlist) const
  {
    UNUSED(partition);
    UNUSED(mlist);
    return common::OB_SUCCESS;
  }
  virtual int get_leader(const common::ObPartitionKey& partition, common::ObAddr& leader, int64_t& leader_epoch,
      bool& is_changing_leader) const
  {
    UNUSED(partition);
    UNUSED(leader);
    UNUSED(leader_epoch);
    UNUSED(is_changing_leader);
    return common::OB_SUCCESS;
  }
  virtual int get_leader(const common::ObPartitionKey& partition, int64_t a, common::ObAddr& leader,
      int64_t& leader_epoch, bool& is_changing_leader) const
  {
    UNUSED(a);
    UNUSED(partition);
    UNUSED(leader);
    UNUSED(leader_epoch);
    UNUSED(is_changing_leader);
    return common::OB_SUCCESS;
  }
  virtual int inc_replica_num(const common::ObPartitionKey& partition)
  {
    UNUSED(partition);
    return common::OB_SUCCESS;
  }
  virtual int dec_replica_num(const common::ObPartitionKey& partition)
  {
    UNUSED(partition);
    return common::OB_SUCCESS;
  }
  virtual int get_all_partition_status(int64_t& inactive_num, int64_t& total_num)
  {
    UNUSED(inactive_num);
    UNUSED(total_num);
    return common::OB_SUCCESS;
  }
  virtual int get_replica_type(const common::ObPartitionKey& pkey, common::ObReplicaType& type) const
  {
    UNUSED(pkey);
    type = common::REPLICA_TYPE_FULL;
    return common::OB_SUCCESS;
  }
  virtual int get_election_priority(const common::ObPartitionKey& partition, ObElectionPriority& priority) const
  {
    UNUSED(partition);
    UNUSED(priority);
    return common::OB_SUCCESS;
  }
  virtual int get_timestamp(const common::ObPartitionKey& partition, int64_t& gts, common::ObAddr& leader) const
  {
    UNUSED(partition);
    UNUSED(gts);
    UNUSED(leader);
    return common::OB_SUCCESS;
  }
};

}  // namespace election
}  // namespace oceanbase

#endif
