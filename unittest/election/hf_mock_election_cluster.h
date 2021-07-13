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

#ifndef OB_UNITTEST_MOCK_ELECTION_CLUSTER_H_
#define OB_UNITTEST_MOCK_ELECTION_CLUSTER_H_

#include <gtest/gtest.h>
#include "hf_mock_election_mgr.h"
#include "hf_mock_election_rpc.h"
#include "hf_mock_election_cb.h"
#include "election/ob_election_mgr.h"
#include "lib/utility/ob_macro_utils.h"
#include "hf_mock_election_server.h"

namespace oceanbase {
using namespace common;
using namespace obrpc;

namespace unittest {
static const char* const TEST_IP = "127.0.0.1";
static const int32_t BASE_PORT = 10001;
static const int64_t MAX_SERVER_NUM = 7;
// quorum
static const int64_t REPLICA_NUM = 3;

class MockElectionCluster {
public:
  MockElectionCluster() : is_inited_(false), svr_cnt_(0)
  {}
  virtual ~MockElectionCluster()
  {
    destroy();
  }

  static MockElectionCluster& get_instance();

  static int get_addr_by_idx(ObAddr& addr, const int32_t idx);
  static int gen_member_list(ObMemberList& mlist, const int64_t replica_num, int32_t id1 = 1, int32_t id2 = 2,
      int32_t id3 = 3, int32_t id4 = 4, int32_t id5 = 5, int32_t id6 = 6, int32_t id7 = 7);
  int init();
  void destroy();
  int start();
  int stop();
  int wait();
  int add_partition(const ObPartitionKey& partition, const int64_t replica_num);
  int remove_partition(const ObPartitionKey& partition);
  int block_net(const common::ObAddr& source, const common::ObAddr& dst);
  int reset_block_net(const common::ObAddr& source, const common::ObAddr& dst);
  int post_election_msg_buf(const ObAddr& sender, const ObAddr& dst, ObElectionMsgBuffer& msgbuf);
  int set_candidates(const ObPartitionKey& pkey, const int64_t replica_num, const ObMemberList& mlist,
      const int64_t membership_version);
  int start_partition(
      const ObPartitionKey& pkey, const ObAddr& leader, const int64_t lease_start, int64_t& leader_epoch);

private:
  int init_server_(const char* ip, const int32_t rpc_port);
  int start_server_(const char* ip, const int32_t rpc_port);

private:
  DISALLOW_COPY_AND_ASSIGN(MockElectionCluster);

public:
  bool is_inited_;
  int64_t svr_cnt_;
  obrpc::ObBatchRpc batch_rpc_;
  obrpc::ObElectionRpcProxy proxy_;
  common::ObMySQLProxy sql_proxy_;
  MockElectionServer election_svr_[MAX_SERVER_NUM];
};

}  // namespace unittest
}  // namespace oceanbase

#endif
