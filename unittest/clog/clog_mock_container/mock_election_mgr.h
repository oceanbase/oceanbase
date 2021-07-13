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

#ifndef OCEANBASE_UNITTEST_CLOG_MOCK_OB_ELECTION_MGR_H
#define OCEANBASE_UNITTEST_CLOG_MOCK_OB_ELECTION_MGR_H

#include "election/ob_election_mgr.h"
#include "election/ob_election_base.h"
#include "election/ob_election_rpc.h"

#include "common/ob_partition_key.h"
#include "common/ob_member_list.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_spin_rwlock.h"

#include "storage/transaction/ob_time_wheel.h"

namespace oceanbase {

namespace common {
class ObPartitionKey;
class ObMemberList;
class ObAddr;
}  // namespace common

namespace obrpc {
class ObElectionRpcResult;
class ObElectionRpcProxy;
}  // namespace obrpc

namespace election {
class ObIElection;
class ObElectionMsgBuffer;
class ObIElectionCallback;
}  // namespace election

namespace election {

class MockObElectionMgr : public ObElectionMgr {
public:
  MockObElectionMgr()
  {}
  ~MockObElectionMgr()
  {
    destroy();
  }
  int init(const common::ObAddr& self, obrpc::ObElectionRpcProxy* rpc_proxy)
  {
    UNUSED(self);
    UNUSED(rpc_proxy);
    return OB_SUCCESS;
  }
  void destroy()
  {}

  int start_partition(const common::ObPartitionKey& partition_key)
  {
    UNUSED(partition_key);
    return OB_SUCCESS;
  }
  int start_partition(
      const common::ObPartitionKey& partition_key, const common::ObAddr& leader, const int64_t& lease_start)
  {
    UNUSED(partition_key);
    UNUSED(leader);
    UNUSED(lease_start);
    return OB_SUCCESS;
  }
  int stop_partition(const common::ObPartitionKey& partition_key)
  {
    UNUSED(partition_key);
    return OB_SUCCESS;
  }
  int start()
  {
    return OB_SUCCESS;
  }
  int wait()
  {
    return OB_SUCCESS;
  }
  int stop()
  {
    return OB_SUCCESS;
  }

public:
  int add_partition(
      const common::ObPartitionKey& partition_key, const int64_t replica_num, ObIElectionCallback* election_cb)
  {
    UNUSED(partition_key);
    UNUSED(replica_num);
    UNUSED(election_cb);
    return OB_SUCCESS;
  }
  int remove_partition(const common::ObPartitionKey& partition_key)
  {
    UNUSED(partition_key);
    return OB_SUCCESS;
  }
  int change_leader_async(const common::ObPartitionKey& partition_key, const common::ObAddr& leader)
  {
    UNUSED(partition_key);
    UNUSED(leader);
    return OB_SUCCESS;
  }
  int leader_revoke(const common::ObPartitionKey& partition)
  {
    UNUSED(partition);
    return OB_SUCCESS;
  }

public:
  int handle_election_msg(const ObElectionMsgBuffer& msgbuf, obrpc::ObElectionRpcResult& result)
  {
    UNUSED(msgbuf);
    UNUSED(result);
    return OB_SUCCESS;
  }

public:
  int set_candidate(const common::ObPartitionKey& partition_key, const common::ObMemberList& prev_mlist,
      const common::ObMemberList& curr_mlist)
  {
    UNUSED(partition_key);
    UNUSED(prev_mlist);
    UNUSED(curr_mlist);
    return OB_SUCCESS;
  }
  int get_prev_candidate(const common::ObPartitionKey& partition_key, common::ObMemberList& mlist) const
  {
    UNUSED(partition_key);
    UNUSED(mlist);
    return OB_SUCCESS;
  }
  int get_curr_candidate(const common::ObPartitionKey& partition_key, common::ObMemberList& mlist) const
  {
    UNUSED(partition_key);
    UNUSED(mlist);
    return OB_SUCCESS;
  }
  int get_leader(const common::ObPartitionKey& partition_key, common::ObAddr& leader) const
  {
    UNUSED(partition_key);
    UNUSED(leader);
    return OB_SUCCESS;
  }
  int get_leader(const common::ObPartitionKey& partition_key, common::ObAddr& leader, int64_t& leader_epoch,
      bool& is_changing_leader) const
  {
    UNUSED(partition_key);
    UNUSED(leader);
    UNUSED(leader_epoch);
    UNUSED(is_changing_leader);
    return OB_SUCCESS;
  }

private:
  ObIElection* get_election_(const common::ObPartitionKey& partition_key) const
  {
    UNUSED(partition_key);
    return NULL;
  }
  int stop_()
  {
    return OB_SUCCESS;
  }
  int wait_()
  {
    return OB_SUCCESS;
  }
};
}  // namespace election
}  // namespace oceanbase

#endif  // OCEANBASE_UNITTEST_CLOG_MOCK_OB_ELECTION_MGR_H
