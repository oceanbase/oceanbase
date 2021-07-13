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

#ifndef OB_UNITTEST_MOCK_ELECTION_SERVER_H_
#define OB_UNITTEST_MOCK_ELECTION_SERVER_H_

#include "hf_mock_election_mgr.h"
#include "hf_mock_election_cb.h"
#include "hf_mock_election_rpc.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/utility/ob_macro_utils.h"

namespace oceanbase {
namespace unittest {

class ObRpcLoss {
public:
  ObRpcLoss() : loss_cnt_(0), total_cnt_(0), loss_rate_(0), end_time_(0)
  {}
  ~ObRpcLoss()
  {}
  void set_loss_rate(const int64_t loss_rate)
  {
    ATOMIC_STORE(&loss_rate_, loss_rate);
  }
  void set_loss_end_time(const int64_t end_time)
  {
    ATOMIC_STORE(&end_time_, end_time);
  }
  bool is_lost(const int64_t now)
  {
    bool bool_ret = false;
    if (now <= end_time_ && loss_rate_ > 0) {
      if ((random() % 100) < loss_rate_) {
        bool_ret = true;
        ATOMIC_INC(&loss_cnt_);
        ATOMIC_INC(&total_cnt_);
      }
    }
    return bool_ret;
  }

public:
  int64_t loss_cnt_;
  int64_t total_cnt_;
  int64_t loss_rate_;
  int64_t end_time_;
};

class MockElectionServer {
public:
  MockElectionServer() : is_inited_(false)
  {}
  virtual ~MockElectionServer()
  {
    destroy();
  }

  int init(const common::ObAddr& addr);
  int start();
  int stop();
  int wait();
  void destroy()
  {
    is_inited_ = false;
  }
  int add_to_black_list(const common::ObAddr& dst);
  int remove_from_black_list(const common::ObAddr& dst);

  int recv_msg_buf(const ObAddr& sender, election::ObElectionMsgBuffer& msgbuf);
  int add_partition(const common::ObPartitionKey& pkey, const int64_t replica_num);
  int remove_partition(const common::ObPartitionKey& pkey);
  int set_candidate(const ObPartitionKey& pkey, const int64_t replica_num, const ObMemberList& curr_mlist,
      const int64_t membership_version);
  int start_partition(
      const ObPartitionKey& pkey, const ObAddr& leader, const int64_t lease_start, int64_t& leader_epoch);
  const common::ObAddr& get_addr() const
  {
    return self_;
  }
  uint64_t get_eg_id_hash(const ObPartitionKey& pkey);

public:
  bool is_inited_;
  common::ObAddr self_;
  ObRpcLoss rpc_loss_;
  MockElectionMgr election_mgr_;
  MockElectionRpc election_rpc_;
  MockElectionCallback election_cb_;
};

}  // namespace unittest
}  // namespace oceanbase
#endif
