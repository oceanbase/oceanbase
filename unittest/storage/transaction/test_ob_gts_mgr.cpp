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
#include "share/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "common/ob_partition_key.h"
#include "lib/net/ob_addr.h"
#include "storage/transaction/ob_gts_mgr.h"
#include "storage/transaction/ob_gts_rpc.h"
#include "storage/transaction/ob_gts_define.h"

namespace oceanbase {
using namespace common;
using namespace transaction;
using namespace obrpc;
namespace unittest {

class MyTimestampService : public ObITimestampService {
public:
  MyTimestampService()
  {}
  ~MyTimestampService()
  {}
  void set(const ObAddr& self, const ObAddr& leader)
  {
    self_ = self;
    leader_ = leader;
  }
  void reset()
  {
    self_.reset();
    leader_.reset();
  }

public:
  int get_timestamp(const ObPartitionKey& partition, int64_t& gts, ObAddr& leader) const
  {
    UNUSEDx(partition);
    int ret = OB_SUCCESS;
    if (!leader_.is_valid()) {
      ret = OB_NOT_MASTER;
    } else if (self_ == leader_) {
      gts = ObTimeUtility::current_time();
      leader = leader_;
    } else {
      leader = leader_;
      ret = OB_NOT_MASTER;
    }
    return ret;
  }

private:
  ObAddr self_;
  ObAddr leader_;
};

class MyResponseRpc : public ObIGtsResponseRpc {
public:
  MyResponseRpc()
  {}
  ~MyResponseRpc()
  {}

public:
  void set_valid_arg(
      const uint64_t tenant_id, const int status, const ObAddr& leader, const ObAddr& sender, const ObAddr self)
  {
    tenant_id_ = tenant_id;
    status_ = status;
    leader_ = leader;
    sender_ = sender;
    self_ = self;
  }
  int post(const uint64_t tenant_id, const ObAddr& server, const ObGtsErrResponse& msg)
  {
    int ret = OB_SUCCESS;
    if (!msg.is_valid() || tenant_id != tenant_id_ || msg.get_status() != status_ || msg.get_leader() != leader_ ||
        msg.get_sender() != sender_ || server != self_) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "invalid argument", K(ret), K(tenant_id), K(server), K(msg), K(*this));
    }
    return ret;
  }
  TO_STRING_KV(K_(tenant_id), K_(status), K_(leader), K_(sender), K_(self));

private:
  uint64_t tenant_id_;
  int status_;
  ObAddr leader_;
  ObAddr sender_;
  ObAddr self_;
};

class TestObGtsMgr : public ::testing::Test {
public:
  virtual void SetUp()
  {}
  virtual void TearDown()
  {}
};

//////////////////////basic function test//////////////////////////////////////////

TEST_F(TestObGtsMgr, handle_gts_request_by_leader)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  const ObAddr client(ObAddr::IPV4, "10.0.0.1", 10000);
  const ObAddr server(ObAddr::IPV4, "10.0.0.1", 20000);
  const ObAddr other_leader(ObAddr::IPV4, "10.0.0.2", 20000);
  MyTimestampService ts_service;
  MyResponseRpc response_rpc;
  ObGlobalTimestampService gts;

  EXPECT_EQ(OB_SUCCESS, gts.init(&ts_service, &response_rpc, server));
  EXPECT_EQ(OB_SUCCESS, gts.start());

  const uint64_t tenant_id = 1001;
  const MonotonicTs srr = MonotonicTs::current_time();
  const int64_t ts_range = 1;
  ts_service.set(server, server);
  response_rpc.set_valid_arg(tenant_id, OB_SUCCESS, server, server, client);
  ObGtsRequest request;
  ObGtsRpcResult result;
  ObPartitionKey gts_pkey;
  EXPECT_EQ(OB_SUCCESS, get_gts_pkey(tenant_id, gts_pkey));
  EXPECT_EQ(OB_SUCCESS, request.init(tenant_id, srr, ts_range, gts_pkey, client));
  EXPECT_EQ(OB_SUCCESS, gts.handle_request(request, result));
  EXPECT_EQ(tenant_id, result.get_tenant_id());
  EXPECT_EQ(OB_SUCCESS, result.get_status());
  EXPECT_EQ(srr, result.get_srr());
  EXPECT_EQ(ts_range - 1, result.get_gts_end() - result.get_gts_start());
  // EXPECT_TRUE(result.get_gts_start() >= srr);

  EXPECT_EQ(OB_SUCCESS, gts.stop());
  EXPECT_EQ(OB_SUCCESS, gts.wait());
}

TEST_F(TestObGtsMgr, handle_local_gts_request)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  const ObAddr server(ObAddr::IPV4, "10.0.0.1", 20000);
  MyTimestampService ts_service;
  MyResponseRpc response_rpc;
  ObGlobalTimestampService gts;

  EXPECT_EQ(OB_SUCCESS, gts.init(&ts_service, &response_rpc, server));
  EXPECT_EQ(OB_SUCCESS, gts.start());

  const uint64_t tenant_id = 1001;
  const MonotonicTs srr = MonotonicTs::current_time();
  const int64_t ts_range = 1;
  ts_service.set(server, server);
  response_rpc.set_valid_arg(tenant_id, OB_SUCCESS, server, server, server);
  ObGtsRequest request;
  ObGtsRpcResult result;
  ObPartitionKey gts_pkey;
  EXPECT_EQ(OB_SUCCESS, get_gts_pkey(tenant_id, gts_pkey));
  EXPECT_EQ(OB_SUCCESS, request.init(tenant_id, srr, ts_range, gts_pkey, server));
  EXPECT_EQ(OB_SUCCESS, gts.handle_request(request, result));
  EXPECT_EQ(tenant_id, result.get_tenant_id());
  EXPECT_EQ(OB_SUCCESS, result.get_status());
  EXPECT_EQ(srr, result.get_srr());
  EXPECT_EQ(ts_range - 1, result.get_gts_end() - result.get_gts_start());
  // EXPECT_TRUE(result.get_gts_start() >= srr);

  EXPECT_EQ(OB_SUCCESS, gts.stop());
  EXPECT_EQ(OB_SUCCESS, gts.wait());
}

TEST_F(TestObGtsMgr, handle_gts_request_by_follower)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  const ObAddr client(ObAddr::IPV4, "10.0.0.1", 10000);
  const ObAddr server(ObAddr::IPV4, "10.0.0.1", 20000);
  const ObAddr other_leader(ObAddr::IPV4, "10.0.0.2", 20000);
  MyTimestampService ts_service;
  MyResponseRpc response_rpc;
  ObGlobalTimestampService gts;

  EXPECT_EQ(OB_SUCCESS, gts.init(&ts_service, &response_rpc, server));
  EXPECT_EQ(OB_SUCCESS, gts.start());

  const uint64_t tenant_id = 1001;
  const MonotonicTs srr = MonotonicTs::current_time();
  const int64_t ts_range = 1;
  ts_service.set(server, other_leader);
  response_rpc.set_valid_arg(tenant_id, OB_NOT_MASTER, other_leader, server, client);
  ObGtsRequest request;
  ObGtsRpcResult result;
  ObPartitionKey gts_pkey;
  EXPECT_EQ(OB_SUCCESS, get_gts_pkey(tenant_id, gts_pkey));
  EXPECT_EQ(OB_SUCCESS, request.init(tenant_id, srr, ts_range, gts_pkey, client));
  EXPECT_EQ(OB_NOT_MASTER, gts.handle_request(request, result));
  EXPECT_EQ(tenant_id, result.get_tenant_id());
  EXPECT_EQ(OB_NOT_MASTER, result.get_status());
  EXPECT_EQ(srr, result.get_srr());
  EXPECT_EQ(0, result.get_gts_start());
  EXPECT_EQ(0, result.get_gts_end());

  EXPECT_EQ(OB_SUCCESS, gts.stop());
  EXPECT_EQ(OB_SUCCESS, gts.wait());
}

TEST_F(TestObGtsMgr, gts_not_master)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  const ObAddr client(ObAddr::IPV4, "10.0.0.1", 10000);
  const ObAddr server(ObAddr::IPV4, "10.0.0.1", 20000);
  MyTimestampService ts_service;
  MyResponseRpc response_rpc;
  ObGlobalTimestampService gts;

  EXPECT_EQ(OB_SUCCESS, gts.init(&ts_service, &response_rpc, server));
  EXPECT_EQ(OB_SUCCESS, gts.start());

  const uint64_t tenant_id = 1001;
  const MonotonicTs srr = MonotonicTs::current_time();
  const int64_t ts_range = 1;
  ts_service.set(server, ObAddr());
  response_rpc.set_valid_arg(tenant_id, OB_NOT_MASTER, ObAddr(), server, client);
  ObGtsRequest request;
  ObGtsRpcResult result;
  ObPartitionKey gts_pkey;
  EXPECT_EQ(OB_SUCCESS, get_gts_pkey(tenant_id, gts_pkey));
  EXPECT_EQ(OB_SUCCESS, request.init(tenant_id, srr, ts_range, gts_pkey, client));
  EXPECT_EQ(OB_NOT_MASTER, gts.handle_request(request, result));
  EXPECT_EQ(tenant_id, result.get_tenant_id());
  EXPECT_EQ(OB_NOT_MASTER, result.get_status());
  EXPECT_EQ(srr, result.get_srr());
  EXPECT_EQ(0, result.get_gts_start());
  EXPECT_EQ(0, result.get_gts_end());

  EXPECT_EQ(OB_SUCCESS, gts.stop());
  EXPECT_EQ(OB_SUCCESS, gts.wait());
}

//////////////////////////boundary test/////////////////////////////////////////

TEST_F(TestObGtsMgr, not_init)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  const ObAddr client(ObAddr::IPV4, "10.0.0.1", 10000);
  const ObAddr server(ObAddr::IPV4, "10.0.0.1", 20000);
  MyTimestampService ts_service;
  MyResponseRpc response_rpc;
  ObGlobalTimestampService gts;

  EXPECT_EQ(OB_NOT_INIT, gts.start());
  EXPECT_EQ(OB_NOT_INIT, gts.stop());
  EXPECT_EQ(OB_NOT_INIT, gts.wait());

  const uint64_t tenant_id = 1001;
  const MonotonicTs srr = MonotonicTs::current_time();
  const int64_t ts_range = 1;
  ts_service.set(server, server);
  response_rpc.set_valid_arg(tenant_id, OB_SUCCESS, server, server, client);
  ObGtsRequest request;
  ObGtsRpcResult result;
  ObPartitionKey gts_pkey;
  EXPECT_EQ(OB_SUCCESS, get_gts_pkey(tenant_id, gts_pkey));
  EXPECT_EQ(OB_SUCCESS, request.init(tenant_id, srr, ts_range, gts_pkey, client));
  EXPECT_EQ(OB_NOT_INIT, gts.handle_request(request, result));
}

TEST_F(TestObGtsMgr, not_start)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  const ObAddr client(ObAddr::IPV4, "10.0.0.1", 10000);
  const ObAddr server(ObAddr::IPV4, "10.0.0.1", 20000);
  MyTimestampService ts_service;
  MyResponseRpc response_rpc;
  ObGlobalTimestampService gts;

  EXPECT_EQ(OB_SUCCESS, gts.init(&ts_service, &response_rpc, server));

  const uint64_t tenant_id = 1001;
  const MonotonicTs srr = MonotonicTs::current_time();
  const int64_t ts_range = 1;
  ts_service.set(server, server);
  response_rpc.set_valid_arg(tenant_id, OB_SUCCESS, server, server, client);
  ObGtsRequest request;
  ObGtsRpcResult result;
  ObPartitionKey gts_pkey;
  EXPECT_EQ(OB_SUCCESS, get_gts_pkey(tenant_id, gts_pkey));
  EXPECT_EQ(OB_SUCCESS, request.init(tenant_id, srr, ts_range, gts_pkey, client));
  EXPECT_EQ(OB_NOT_RUNNING, gts.handle_request(request, result));

  EXPECT_EQ(OB_ERR_UNEXPECTED, gts.stop());
  EXPECT_EQ(OB_SUCCESS, gts.wait());
}

TEST_F(TestObGtsMgr, restart_after_stop)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  const ObAddr server(ObAddr::IPV4, "10.0.0.1", 20000);
  MyTimestampService ts_service;
  MyResponseRpc response_rpc;
  ObGlobalTimestampService gts;

  EXPECT_EQ(OB_SUCCESS, gts.init(&ts_service, &response_rpc, server));
  EXPECT_EQ(OB_SUCCESS, gts.start());
  EXPECT_EQ(OB_SUCCESS, gts.stop());
  EXPECT_EQ(OB_SUCCESS, gts.wait());

  EXPECT_EQ(OB_SUCCESS, gts.start());
  EXPECT_EQ(OB_SUCCESS, gts.stop());
  EXPECT_EQ(OB_SUCCESS, gts.wait());
}

TEST_F(TestObGtsMgr, restart_after_destroy)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  const ObAddr server(ObAddr::IPV4, "10.0.0.1", 20000);
  MyTimestampService ts_service;
  MyResponseRpc response_rpc;
  ObGlobalTimestampService gts;

  EXPECT_EQ(OB_SUCCESS, gts.init(&ts_service, &response_rpc, server));
  EXPECT_EQ(OB_SUCCESS, gts.start());

  gts.destroy();

  EXPECT_EQ(OB_SUCCESS, gts.init(&ts_service, &response_rpc, server));
  EXPECT_EQ(OB_SUCCESS, gts.start());
  EXPECT_EQ(OB_SUCCESS, gts.stop());
  EXPECT_EQ(OB_SUCCESS, gts.wait());
}

TEST_F(TestObGtsMgr, wait_before_stop)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  const ObAddr server(ObAddr::IPV4, "10.0.0.1", 20000);
  MyTimestampService ts_service;
  MyResponseRpc response_rpc;
  ObGlobalTimestampService gts;

  EXPECT_EQ(OB_SUCCESS, gts.init(&ts_service, &response_rpc, server));
  EXPECT_EQ(OB_SUCCESS, gts.start());
  EXPECT_EQ(OB_ERR_UNEXPECTED, gts.wait());
  EXPECT_EQ(OB_SUCCESS, gts.stop());
  EXPECT_EQ(OB_SUCCESS, gts.wait());
}

TEST_F(TestObGtsMgr, invalid_argument)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  const ObAddr client(ObAddr::IPV4, "10.0.0.1", 10000);
  const ObAddr server(ObAddr::IPV4, "10.0.0.1", 20000);
  MyTimestampService ts_service;
  MyResponseRpc response_rpc;
  ObGlobalTimestampService gts;

  EXPECT_EQ(OB_INVALID_ARGUMENT, gts.init(NULL, &response_rpc, server));
  EXPECT_EQ(OB_INVALID_ARGUMENT, gts.init(&ts_service, NULL, server));
  EXPECT_EQ(OB_INVALID_ARGUMENT, gts.init(&ts_service, &response_rpc, ObAddr()));

  EXPECT_EQ(OB_SUCCESS, gts.init(&ts_service, &response_rpc, server));
  EXPECT_EQ(OB_SUCCESS, gts.start());

  const uint64_t tenant_id = 1001;
  const MonotonicTs srr = MonotonicTs::current_time();
  const MonotonicTs stc;
  const int64_t ts_range = 1;
  ObGtsRequest request;
  ObPartitionKey gts_pkey;
  EXPECT_EQ(OB_SUCCESS, get_gts_pkey(tenant_id, gts_pkey));
  EXPECT_EQ(OB_INVALID_ARGUMENT, request.init(0, srr, ts_range, gts_pkey, client));
  EXPECT_EQ(OB_INVALID_ARGUMENT, request.init(tenant_id, stc, ts_range, gts_pkey, client));
  EXPECT_EQ(OB_INVALID_ARGUMENT, request.init(tenant_id, srr, 0, gts_pkey, client));
  EXPECT_EQ(OB_INVALID_ARGUMENT, request.init(tenant_id, srr, ts_range, ObPartitionKey(), client));
  EXPECT_EQ(OB_INVALID_ARGUMENT, request.init(tenant_id, srr, ts_range, gts_pkey, ObAddr()));

  EXPECT_EQ(OB_SUCCESS, gts.stop());
  EXPECT_EQ(OB_SUCCESS, gts.wait());
}

}  // namespace unittest
}  // namespace oceanbase

using namespace oceanbase;
using namespace oceanbase::common;

int main(int argc, char** argv)
{
  int ret = 1;
  ObLogger& logger = ObLogger::get_logger();
  logger.set_file_name("test_ob_gts_mgr.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc, argv);
  ret = RUN_ALL_TESTS();
  return ret;
}
