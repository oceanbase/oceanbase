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
#include "rpc/ob_request.h"
#include "observer/ob_srv_deliver.h"
#include "observer/omt/ob_worker_processor.h"
#include "observer/omt/ob_multi_tenant.h"

using namespace oceanbase::common;
using namespace oceanbase::observer;
using namespace oceanbase::obrpc;
using namespace oceanbase::rpc;
using namespace oceanbase::omt;

class ObMockReqHandler
    : public ObiReqQHandler
{
public:
  virtual int onThreadCreated(obsys::CThread *)
  {
    return OB_SUCCESS;
  }
  virtual int onThreadDestroy(obsys::CThread *)
  {
    return OB_SUCCESS;
  }

  virtual bool handlePacketQueue(ObRequest *, void*)
  {
    return OB_SUCCESS;
  }
};

ObRpcSessionHandler sh;
ObMockReqHandler mrh;
ObFakeWorkerProcessor fwp;
ObMultiTenant omt(fwp);

class TestDeliver
    : public ::testing::Test, public ObSrvDeliver
{
public:
  TestDeliver()
      : ObSrvDeliver(mrh, sh, GCTX)
  {}

  virtual void SetUp()
  {
    ASSERT_EQ(OB_SUCCESS, init());
  }

  virtual void TearDown()
  {
  }
};


TEST_F(TestDeliver, NotInit)
{
  ObRpcPacket pkt;
  ObRequest req(ObRequest::OB_RPC);
  req.set_packet(&pkt);
  int ret = deliver(req);
  EXPECT_EQ(OB_NOT_INIT, ret);
}

TEST_F(TestDeliver, Norm)
{
  GCTX.omt_ = &omt;
  GCTX.status_ = SS_SERVING;
  ObRpcPacket pkt;
  ObRequest req(ObRequest::OB_RPC);
  req.set_packet(&pkt);
  int ret = deliver(req);
  EXPECT_EQ(OB_TENANT_NOT_IN_SERVER, ret);
  pkt.set_pcode(OB_RENEW_LEASE);
  ret = deliver(req);
  EXPECT_EQ(OB_SUCCESS, ret);
  pkt.set_priority(10);
  ret = deliver(req);
  EXPECT_EQ(OB_SUCCESS, ret);
  sleep(1);
}

int main(int argc, char *argv[])
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
