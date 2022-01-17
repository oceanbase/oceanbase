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

#ifndef OB_TEST_ELECTION_TEST_ELECTION_CLUSTER_H_
#define OB_TEST_ELECTION_TEST_ELECTION_CLUSTER_H_

#include <gtest/gtest.h>
#include "mock_ob_election_mgr.h"
#include "mock_ob_election_rpc.h"
#include "mock_ob_election_cb.h"
#include "election/ob_election_mgr.h"

namespace oceanbase {
using namespace common;
using namespace obrpc;
namespace unittest {
static const int64_t MAX_OB_TRANS_SERVICE_NUM = 4;
static const int64_t OB_REPLICA_NUM = 3;

class ObRpcLoss {
public:
  ObRpcLoss() : times_(0)
  {}
  ~ObRpcLoss()
  {}

public:
  int64_t times_;
};

class ObTestElectionCluster : public ObIElectionRpc {
public:
  ObTestElectionCluster()
  {}
  virtual ~ObTestElectionCluster()
  {}
  int init();
  void destroy();
  int start();
  int add_partition(const ObPartitionKey& partition, const ObAddr& addr);
  int remove_partition(const ObPartitionKey& partition);
  int post_election_msg(const ObAddr& server, const ObPartitionKey& partition, const election::ObElectionMsg& msg);
  int post_election_group_msg(const common::ObAddr& server, const ObElectionGroupId& eg_id,
      const ObPartArrayBuffer& part_array_buf, const ObElectionMsg& msg);
  int init(obrpc::ObElectionRpcProxy* rpc_proxy, ObIElectionMgr* election_mgr, const common::ObAddr& self)
  {
    UNUSED(rpc_proxy);
    UNUSED(election_mgr);
    UNUSED(self);
    return OB_SUCCESS;
  }

private:
  static const int32_t MAX_ELECTION_MSG_COUNT = 6;
  int start_one_(const int64_t idx, const char* ip, const int32_t rpc_port);

public:
  bool inited_;
  int32_t base_rpc_port_;
  const char* ip_;
  bool is_force_leader_;
  ObRpcLoss rpc_loss_[MAX_ELECTION_MSG_COUNT];
  MockObElectionCallback election_cb_[MAX_OB_TRANS_SERVICE_NUM];
  obrpc::ObBatchRpc batch_rpc_;
  MockEGPriorityGetter eg_cb_;
  MockObElectionMgr election_mgr_[MAX_OB_TRANS_SERVICE_NUM];
  MockObElectionRpc election_rpc_[MAX_OB_TRANS_SERVICE_NUM];
};

class TestObElectionCluster : public ::testing::Test {
public:
  TestObElectionCluster()
  {}
  virtual ~TestObElectionCluster()
  {}
  virtual void SetUp();
  virtual void TearTown();
  static void SetUpTestCase()
  {}
  static void TearDownTestCase()
  {}

public:
  ObTestElectionCluster cluster_;
  common::ObPartitionKey test_partition_;
  common::ObAddr leader_;
};

}  // namespace unittest
}  // namespace oceanbase

#endif
