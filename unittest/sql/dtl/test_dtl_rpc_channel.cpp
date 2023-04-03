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
#include "lib/coro/testing.h"
#include "rpc/testing.h"
#include "sql/dtl/ob_dtl_rpc_processor.h"
#include "sql/dtl/ob_dtl_channel_group.h"
#include "sql/dtl/ob_dtl_rpc_channel.h"
#include "sql/dtl/ob_dtl_task.h"
#include "sql/dtl/ob_dtl_msg.h"
#include "sql/dtl/ob_dtl_channel.h"
#include "sql/dtl/ob_dtl.h"
#include "sql/dtl/ob_dtl_channel_loop.h"

using namespace oceanbase::sql::dtl;
using namespace oceanbase::common;
using namespace oceanbase::lib;

ObAddr self_addr;

class Msg
    : public ObDtlMsgTemp<ObDtlMsgType::TESTING>
{
  OB_UNIS_VERSION(1);
public:
  void reset() {}
  int ret_;
};
OB_SERIALIZE_MEMBER(Msg, ret_);

#if 0
TEST(TestDtlRpcChannel, Basic)
{
  // Initialize the rpctesting service
  rpctesting::Service service;
  ASSERT_EQ(OB_SUCCESS, service.init());
  self_addr = ObAddr(ObAddr::IPV4, "127.0.0.1", service.get_listen_port());

  // Register the corresponding RPC processor
  ObDtlSendMessageP p;
  service.reg_processor(&p);

  // Initialize the DTL, and use rpctesting to initialize the rpc proxy in the DTL
  ASSERT_EQ(OB_SUCCESS, DTL.init());
  ASSERT_EQ(OB_SUCCESS, service.get_proxy(DTL.get_rpc_proxy()));

  // Create a DtlChannelInfo object, ci1 and ci2 are the channel information at both ends of the same channel.
  ObDtlChannelInfo ci1, ci2;
  const uint64_t tenant_id = 1;
  int ret = ObDtlChannelGroup::make_channel(tenant_id, self_addr, self_addr, ci1, ci2);
  ASSERT_EQ(OB_SUCCESS, ret);

  // Create a Channel according to the DtlChannelInfo obtained in the previous step, here according to ci1 in the previous step
  // And ci2 created two Channels (ChannelPort).
  ObDtlChannel *ch1 = nullptr, *ch2 = nullptr;
  ret = ObDtlChannelGroup::link_channel(ci1, ch1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObDtlChannelGroup::link_channel(ci2, ch2);
  ASSERT_EQ(OB_SUCCESS, ret);

  // Simulate sending a message in one of the ChannelPort (ch1)
  Msg msg1;
  msg1.ret_ = 10;
  ASSERT_EQ(OB_SUCCESS, ch1->send(msg1));
  msg1.ret_ = 0;
  for (int i = 0; i < 65536; i++) {
    ASSERT_EQ(OB_SUCCESS, ch1->send(msg1));
  }
  ch1->flush();

  // Define a specific DtlMsg processing function, and monitor and process it on another Port of the same Channel (ch2).
  class : public ObDtlPacketProc<Msg> {
    int process(const Msg &pkt) override
    {
      return pkt.ret_;
    }
  } proc;
  ObDtlChannelLoop loop;
  loop.register_channel(*ch2).register_processor(proc);
  ret = loop.process_one(1000000);
  ASSERT_EQ(10, ret);
  for (int i = 0; i < 65536; i++) {
    ret = loop.process_one(1000000);
    ASSERT_EQ(0, ret);
  }

  // After the processing is over, you need to release the corresponding ChannelPort respectively
  ret = ObDtlChannelGroup::unlink_channel(ci1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObDtlChannelGroup::unlink_channel(ci2);
  ASSERT_EQ(OB_SUCCESS, ret);
}
#endif

// TEST(TestDtlRpcChannel, memory_owner)
// {
//   // Initialize the rpctesting service
//   rpctesting::Service service;
//   ASSERT_EQ(OB_SUCCESS, service.init());
//   self_addr = ObAddr(ObAddr::IPV4, "127.0.0.1", service.get_listen_port());

//   // Register the corresponding RPC processor
//   ObDtlSendMessageP p;
//   service.reg_processor(&p);

//   // Initialize the DTL, and use rpctesting to initialize the rpc proxy in the DTL
//   ASSERT_EQ(OB_SUCCESS, DTL.init());
//   ASSERT_EQ(OB_SUCCESS, service.get_proxy(DTL.get_rpc_proxy()));

//   // Create a DtlChannelInfo object, ci1 and ci2 are the channel information at both ends of the same channel.
//   ObDtlChannelInfo ci1, ci2;
//   const uint64_t tenant_id = OB_SERVER_TENANT_ID;
//   int ret = ObDtlChannelGroup::make_channel(tenant_id, self_addr, self_addr, ci1, ci2);
//   ASSERT_EQ(OB_SUCCESS, ret);

//   // Create a Channel based on the DtlChannelInfo obtained in the previous step, here based on the ci1 in the previous step
//   // Create two Channels (ChannelPort) with ci2.
//   ObDtlChannel *ch1 = nullptr, *ch2 = nullptr;
//   ret = ObDtlChannelGroup::link_channel(ci1, ch1);
//   ASSERT_EQ(OB_SUCCESS, ret);
//   ret = ObDtlChannelGroup::link_channel(ci2, ch2);
//   ASSERT_EQ(OB_SUCCESS, ret);

//   // Simulate sending a message in one of the ChannelPort (ch1)
//   ObMallocAllocator *malloc_allocator = ObMallocAllocator::get_instance();
//   ASSERT_NE(nullptr, malloc_allocator);
//   auto ta = malloc_allocator->get_tenant_ctx_allocator(tenant_id);
//   ASSERT_NE(nullptr, ta);
//   const uint64_t hold = ta->get_hold();
//   Msg msg1;
//   msg1.ret_ = 10;
//   const int msg_cnt = 10;
//   ASSERT_EQ(OB_SUCCESS, ch1->send(msg1));
//   msg1.ret_ = 0;
//   for (int i = 0; i < msg_cnt; i++) {
//     ASSERT_EQ(OB_SUCCESS, ch1->send(msg1));
//   }
//   ch1->flush();
//   ASSERT_TRUE(ta->get_hold() - hold > sizeof(Msg) * msg_cnt);

//   // Define a specific DtlMsg processing function, and monitor and process it on another Port of the same Channel (ch2).
//   class : public ObDtlPacketProc<Msg> {
//     int process(const Msg &pkt) override
//     {
//       return pkt.ret_;
//     }
//   } proc;
//   ObDtlChannelLoop loop;
//   loop.register_channel(*ch2).register_processor(proc);
//   ret = loop.process_one(1000000);
//   ASSERT_EQ(10, ret);
//   for (int i = 0; i < msg_cnt; i++) {
//     ret = loop.process_one(1000000);
//     ASSERT_EQ(0, ret);
//   }

//   // After the processing is over, you need to release the corresponding ChannelPort respectively
//   ret = ObDtlChannelGroup::unlink_channel(ci1);
//   ASSERT_EQ(OB_SUCCESS, ret);
//   ret = ObDtlChannelGroup::unlink_channel(ci2);
//   ASSERT_EQ(OB_SUCCESS, ret);

//   ASSERT_TRUE(ta->get_hold() - hold < sizeof(Msg) * msg_cnt);
// }

int main(int argc, char *argv[])
{
  // OB_LOGGER.set_log_level("info");
  system("rm -f test_dtl_rpc_channel.log*");
  OB_LOGGER.set_file_name("test_dtl_rpc_channel.log", true, true);
  ::testing::InitGoogleTest(&argc, argv);
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  return RUN_ALL_TESTS();
}
