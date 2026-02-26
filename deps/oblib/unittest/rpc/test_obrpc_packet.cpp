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
#define USING_LOG_PREFIX RPC_TEST

#include <gtest/gtest.h>
#include "rpc/obrpc/ob_rpc_packet.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_net_handler.h"
#include "test_obrpc_util.h"

using namespace oceanbase;
using namespace oceanbase::rpc;
using namespace oceanbase::obrpc;
using namespace oceanbase::common;

class TestObrpcPacket
    : public ::testing::Test
{
public:
  virtual void SetUp()
  {
  }

  virtual void TearDown()
  {
  }
};

class TestPacketProxy
    : public ObRpcProxy
{
public:
  DEFINE_TO(TestPacketProxy);
  RPC_S(@PR5 test_overflow, OB_TEST2_PCODE, (uint64_t));
};
class SimpleProcessor
    : public TestPacketProxy::Processor<OB_TEST2_PCODE>
{
public:
  SimpleProcessor(): ret_code_(OB_SUCCESS) {}
  int ret_code_;
protected:
  int process()
  {
    return OB_SUCCESS;
  }
  int response(const int retcode) {
    ret_code_ = retcode;
    return OB_SUCCESS;
  }

};

TEST_F(TestObrpcPacket, NameIndex)
{
  ObRpcPacketSet &set = ObRpcPacketSet::instance();
  EXPECT_EQ(OB_RENEW_LEASE, set.pcode_of_idx(set.idx_of_pcode(OB_RENEW_LEASE)));
  EXPECT_EQ(OB_BOOTSTRAP, set.pcode_of_idx(set.idx_of_pcode(OB_BOOTSTRAP)));
  EXPECT_STREQ("OB_BOOTSTRAP", set.name_of_idx(set.idx_of_pcode(OB_BOOTSTRAP)));
}

TEST_F(TestObrpcPacket, CheckClusterId)
{
  int ret = OB_SUCCESS;
  ObRpcNetHandler::CLUSTER_ID = 100;

  TestPacketProxy proxy;
  frame::ObReqTransport transport(NULL, NULL);
  ASSERT_EQ(OB_SUCCESS, proxy.init(&transport));
  ObRpcPacket pkt;
  oceanbase::obrpc::ObRpcOpts opts;
  SimpleProcessor p;
  ObRequest req(ObRequest::OB_RPC);
  req.set_packet(&pkt);
  p.set_ob_request(req);

  ASSERT_EQ(OB_SUCCESS, init_packet(proxy, pkt, OB_TEST2_PCODE, opts, 1));

  ASSERT_EQ(ObRpcNetHandler::CLUSTER_ID, pkt.get_src_cluster_id());
  ASSERT_EQ(ObRpcNetHandler::CLUSTER_ID, pkt.get_dst_cluster_id());

  ASSERT_EQ(OB_SUCCESS, p.run());
  ASSERT_EQ(OB_ERR_UNEXPECTED, p.ret_code_);

  // dst_cluster_id mismatch
  ObRpcNetHandler::CLUSTER_ID = 200;
  ASSERT_EQ(OB_SUCCESS, p.run());
  ASSERT_EQ(OB_PACKET_CLUSTER_ID_NOT_MATCH, p.ret_code_);

  // rpc from 4.x old version observer which is belong to the same cluster
  TestPacketProxy proxy2;
  proxy2.init(&transport);
  ASSERT_EQ(OB_SUCCESS, init_packet(proxy2, pkt, OB_TEST2_PCODE, opts, 1));
  pkt.set_dst_cluster_id(INVALID_CLUSTER_ID);
  ASSERT_EQ(OB_SUCCESS, p.run());
  ASSERT_NE(OB_PACKET_CLUSTER_ID_NOT_MATCH, p.ret_code_);

  // rpc from 4.x old version observer which is belong to the different cluster
  pkt.set_dst_cluster_id(INVALID_CLUSTER_ID);
  pkt.set_src_cluster_id(ObRpcNetHandler::CLUSTER_ID + 1);
  ASSERT_EQ(OB_SUCCESS, p.run());
  ASSERT_EQ(OB_PACKET_CLUSTER_ID_NOT_MATCH, p.ret_code_);

  // rpc is not from observer
  pkt.set_dst_cluster_id(INVALID_CLUSTER_ID);
  pkt.set_src_cluster_id(INVALID_CLUSTER_ID);
  ASSERT_EQ(OB_SUCCESS, p.run());
  ASSERT_NE(OB_PACKET_CLUSTER_ID_NOT_MATCH, p.ret_code_);
}

int main(int argc, char *argv[])
{
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_obrpc_packet.log", true);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
