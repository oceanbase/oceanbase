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

#include <gtest/gtest.h>
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "election/ob_election_rpc.h"
#include "election/ob_election_msg.h"
#include "election/ob_election_mgr.h"
#include "election/ob_election_priority.h"
#include "lib/oblog/ob_log.h"
#include "share/ob_common_rpc_proxy.h"
#include "lib/container/ob_array_iterator.h"
#include "share/config/ob_server_config.h"
#include "observer/ob_server_struct.h"

namespace oceanbase {
namespace unittest {

using namespace obrpc;
using namespace election;

class MockObElectionProxy : public obrpc::ObElectionRpcProxy {
public:
  DEFINE_TO(MockObElectionProxy);
  virtual int post_election_msg(const oceanbase::election::ObElectionMsgBuffer& msg_buf,
      AsyncCB<(oceanbase::obrpc::ObRpcPacketCode)1537>* async_cb, const oceanbase::obrpc::ObRpcOpts& opts)
  {
    UNUSED(msg_buf);
    UNUSED(async_cb);
    UNUSED(opts);

    return OB_SUCCESS;
  }
};

class TestObElectionRpc : public ::testing::Test {
public:
  TestObElectionRpc() : proxy_(&proxy_)
  {}
  virtual ~TestObElectionRpc()
  {}

  virtual void SetUp()
  {
    init();
  }
  virtual void TearDown()
  {}

  int init();

protected:
  ObAddr addr_;
  ObElectionRpc rpc_;
  ObElectionMgr mgr_;
  ObPartitionKey pkey_;
  MockObElectionProxy proxy_;
};

int TestObElectionRpc::init()
{
  addr_.set_ip_addr("127.0.0.1", 34460);
  pkey_.init(combine_id(1, 3001), 0, 1);
  // MockObElectionProxy proxy_(&proxy_);
  rpc_.init(&proxy_, &mgr_, addr_);

  return OB_SUCCESS;
}

TEST_F(TestObElectionRpc, post_election_deprepare_msg)
{
  int64_t ts = ObClockGenerator::getClock();
  ObElectionPriority priority;
  priority.init(true, ts, 1, 0);
  const int64_t fake_cluster_id = 1;

  ts = ((ts + T_ELECT2) / T_ELECT2) * T_ELECT2;
  ObElectionMsgDEPrepare de_pre_msg1(priority, ts + 100, ts, addr_, T_CENTRALIZED_VOTE_EXTENDS * T_ELECT2);
  ELECT_LOG(INFO, "post election msg", K(addr_), K(pkey_), K(de_pre_msg1));
  EXPECT_EQ(OB_INVALID_ARGUMENT, rpc_.post_election_msg(addr_, fake_cluster_id, pkey_, de_pre_msg1));
  // ObElectionMsgDEPrepare de_pre_msg2(priority, ts, ts, addr_);
  // ELECT_LOG(INFO, "post election msg", K(addr_), K(pkey_), K(de_pre_msg2));
  // EXPECT_EQ(OB_SUCCESS, rpc_.post_election_msg(addr_, pkey_, de_pre_msg2));
}

TEST_F(TestObElectionRpc, election_callback)
{
  obrpc::ObElectionRPCCB<obrpc::OB_ELECTION> election_cb_;
  EXPECT_EQ(OB_SUCCESS, election_cb_.init(&mgr_));
  // EXPECT_EQ(OB_SUCCESS, election_cb_.process());
  election_cb_.on_timeout();
}

}  // namespace unittest
}  // namespace oceanbase

int main(int argc, char** argv)
{
  int ret = -1;

  oceanbase::election::ASYNC_LOG_INIT("test_election_rpc.log", OB_LOG_LEVEL_INFO, true);

  if (OB_FAIL(ObClockGenerator::init())) {
    ELECT_LOG(WARN, "clock generator init error.", K(ret));
  } else {
    testing::InitGoogleTest(&argc, argv);
    ret = RUN_ALL_TESTS();
  }

  oceanbase::election::ASYNC_LOG_DESTROY();
  (void)oceanbase::common::ObClockGenerator::destroy();

  return ret;
}
