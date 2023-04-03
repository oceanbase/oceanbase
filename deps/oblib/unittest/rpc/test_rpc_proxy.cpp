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
#include "rpc/obrpc/ob_net_client.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::obrpc;
using namespace oceanbase::rpc::frame;

template <int64_t SZ>
struct ArgT
{
public:
  int64_t sz_ = SZ;
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const
  {
    UNUSEDx(buf, buf_len, pos);
    return OB_SUCCESS;
  }
  int deserialize(const char *buf, const int64_t data_len, int64_t &pos) const
  {
    UNUSEDx(buf, data_len, pos);
    return OB_SUCCESS;
  }
  int64_t get_serialize_size(void) const
  {
    return SZ;
  }
};

typedef ArgT<(OB_MAX_RPC_PACKET_LENGTH) + 1> Arg_overflow;
typedef ArgT<(OB_MAX_RPC_PACKET_LENGTH) - 1> Arg_succ;
typedef ArgT<(OB_MAX_RPC_PACKET_LENGTH)> Arg;

static Arg_overflow arg_overflow;
static Arg_succ arg_succ;
static Arg arg;

class TestProxy
    : public ObRpcProxy
{
public:
  DEFINE_TO(TestProxy);

  RPC_AP(@PR5 test, OB_TEST_PCODE, (uint64_t));
  RPC_S(@PR5 test_overflow, OB_TEST2_PCODE, (Arg_overflow));
  RPC_S(@PR5 test_succ, OB_TEST4_PCODE, (Arg_succ));
  RPC_S(@PR5 test, OB_TEST5_PCODE, (Arg));
  RPC_S(@PR5 test_overflowx, OB_TEST6_PCODE, (Arg_overflow), int);
  RPC_S(@PR5 test_succx, OB_TEST8_PCODE, (Arg_succ), int);
  RPC_S(@PR5 testx, OB_TEST9_PCODE, (Arg), int);
};

class TestCB
    : public TestProxy::AsyncCB<OB_TEST_PCODE>
{
public:
  TestCB()
      : processed_(false)
  { }

  int process() {
    processed_ = true;
    return OB_SUCCESS;
  }

  ObReqTransport::AsyncCB *clone(const SPAlloc &alloc) const {
    UNUSED(alloc);
    return const_cast<TestCB *>(this);
  }

  void set_args(const Request &arg)
  {
    UNUSED(arg);
  }


  bool processed_;
};

class TestRpcProxy
    : public ::testing::Test
{
public:
  virtual void SetUp()
  {
    r_.user_data = &cb_;
    r_.ipacket = &pkt_;
    r_.ms = NULL;
  }

  virtual void TearDown()
  {
  }

protected:
  void serialize_rcode(const ObRpcResultCode &rc)
  {
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, rc.serialize(buf_, 1024, pos));
    pkt_.set_content(buf_, pos);
  }

protected:
  TestCB cb_;
  ObRpcPacket pkt_;
  easy_request_t r_;
  char buf_[1024];
};

int async_process(easy_request_t *r);

TEST_F(TestRpcProxy, AsyncCB)
{
  ObRpcResultCode rc;

  // server response successfully
  cb_.processed_ = false;
  serialize_rcode(rc);
  pkt_.calc_checksum();
  async_process(&r_);
  EXPECT_TRUE(cb_.processed_);

  // server reponse process failed
  cb_.processed_ = false;
  rc.rcode_ = OB_ERROR;
  serialize_rcode(rc);
  pkt_.calc_checksum();
  async_process(&r_);
  EXPECT_TRUE(cb_.processed_);

  // server response not well-formed data
  cb_.processed_ = false;
  pkt_.set_content(buf_, 1);
  async_process(&r_);
  EXPECT_FALSE(cb_.processed_);
}


TEST_F(TestRpcProxy, PacketSize)
{
  ObNetClient net;
  TestProxy p;
  ObAddr dst(ObAddr::IPV4, "127.0.0.1", 1);

  ASSERT_EQ(OB_SUCCESS, net.init());
  ASSERT_EQ(OB_SUCCESS, net.get_proxy(p));
  {
    EXPECT_EQ(arg_overflow.sz_, arg_overflow.get_serialize_size());
    EXPECT_EQ(OB_RPC_PACKET_TOO_LONG, p.to(dst).test_overflow(arg_overflow));
  }
  {
    EXPECT_EQ(arg_succ.sz_, arg_succ.get_serialize_size());
    EXPECT_EQ(OB_RPC_SEND_ERROR, p.to(dst).test_succ(arg_succ));
  }
  {
    EXPECT_EQ(arg.sz_, arg.get_serialize_size());
    EXPECT_EQ(OB_RPC_SEND_ERROR, p.to(dst).test(arg));
  }

  int not_used;
  {
    EXPECT_EQ(arg_overflow.sz_, arg_overflow.get_serialize_size());
    EXPECT_EQ(OB_RPC_PACKET_TOO_LONG, p.to(dst).test_overflowx(arg_overflow, not_used));
  }
  {
    EXPECT_EQ(arg_succ.sz_, arg_succ.get_serialize_size());
    EXPECT_EQ(OB_RPC_SEND_ERROR, p.to(dst).test_succx(arg_succ, not_used));
  }
  {
    EXPECT_EQ(arg.sz_, arg.get_serialize_size());
    EXPECT_EQ(OB_RPC_SEND_ERROR, p.to(dst).testx(arg, not_used));
  }
}

int main(int argc, char *argv[])
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
