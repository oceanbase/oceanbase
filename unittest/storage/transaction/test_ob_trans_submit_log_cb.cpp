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

#include "storage/transaction/ob_trans_submit_log_cb.h"
#include <gtest/gtest.h>
#include "share/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "common/ob_partition_key.h"
#include "storage/transaction/ob_trans_ctx_mgr.h"
#include "storage/transaction/ob_trans_rpc.h"
#include "storage/ob_partition_service.h"
#include "storage/transaction/ob_trans_sche_ctx.h"
#include "storage/transaction/ob_trans_coord_ctx.h"
#include "storage/transaction/ob_trans_part_ctx.h"
#include "../mockcontainer/mock_ob_trans_service.h"
#include "../mockcontainer/mock_ob_partition_service.h"

namespace oceanbase {
using namespace common;
using namespace transaction;
using namespace storage;
using namespace memtable;
using namespace share;
namespace unittest {

LeaderActiveArg leader_active_arg;

class MyMockObPartitionService2 : public MockObIPartitionService {
public:
  MyMockObPartitionService2()
  {}

  ~MyMockObPartitionService2()
  {}

  int get_partition(const common::ObPartitionKey& pkey, ObIPartitionGroup*& partition) const
  {
    UNUSED(pkey);
    UNUSED(partition);
    return OB_SUCCESS;
  }
  int get_partition(const common::ObPartitionKey& pkey, ObIPartitionGroupGuard& guard) const
  {
    UNUSED(pkey);
    UNUSED(guard);
    return OB_SUCCESS;
  }
  virtual int get_curr_member_list(const common::ObPartitionKey& pkey, common::ObMemberList& member_list) const
  {
    UNUSED(pkey);
    UNUSED(member_list);
    return OB_SUCCESS;
  }
  transaction::ObTransService* get_trans_service()
  {
    return &txs_;
  }

private:
  transaction::ObTransService txs_;
};

class MockObPartTransCtx : public ObPartTransCtx {
public:
  MockObPartTransCtx()
  {}
  virtual ~MockObPartTransCtx()
  {}
  int set_coordinator(const common::ObPartitionKey& coordinator)
  {
    int ret = OB_SUCCESS;
    if (!coordinator.is_valid()) {
      TRANS_LOG(WARN, "invalid argument", K(coordinator));
      ret = OB_INVALID_ARGUMENT;
    } else {
      coordinator_ = coordinator;
    }
    return ret;
  }
};

class MockObLocationAdapter : public ObILocationAdapter {
public:
  MockObLocationAdapter()
  {}
  ~MockObLocationAdapter()
  {}
  int init(ObIPartitionLocationCache* location_cache)
  {
    UNUSED(location_cache);
    return OB_SUCCESS;
  }
  void destroy()
  {}
  int nonblock_get_leader(const ObPartitionKey& partition, ObAddr& server)
  {
    int ret = OB_SUCCESS;
    UNUSED(partition);
    UNUSED(server);

    return ret;
  }
  int get_leader(const ObPartitionKey& partition, ObAddr& server)
  {
    int ret = OB_SUCCESS;
    UNUSED(partition);
    UNUSED(server);

    return ret;
  }
  int nonblock_renew(const ObPartitionKey& partition, const bool clear_cache)
  {
    UNUSED(partition);
    UNUSED(clear_cache);
    return OB_SUCCESS;
  }
};
class MockObTransRpc : public ObITransRpc {
public:
  MockObTransRpc()
  {}
  virtual ~MockObTransRpc()
  {}
  int init(obrpc::ObTransRpcProxy* rpc_proxy, ObTransService* trans_service, const common::ObAddr& self)
  {
    UNUSED(rpc_proxy);
    UNUSED(trans_service);
    UNUSED(self);
    start();
    return OB_SUCCESS;
  }
  int start()
  {
    return OB_SUCCESS;
  }
  void stop()
  {}
  void wait()
  {}
  void destroy()
  {}
  int post_trans_msg(
      const uint64_t tenant_id, const common::ObAddr& server, const ObTransMsg& msg, const int64_t msg_type)
  {
    UNUSED(tenant_id);
    UNUSED(server);
    UNUSED(msg);
    UNUSED(msg_type);
    return OB_SUCCESS;
  }
  int post_trans_msg(
      const uint64_t tenant_id, const common::ObAddr& server, const ObTrxMsgBase& msg, const int64_t msg_type)
  {
    UNUSED(tenant_id);
    UNUSED(server);
    UNUSED(msg);
    UNUSED(msg_type);
    return OB_SUCCESS;
  }
  virtual int post_batch_msg(const uint64_t tenant_id, const ObAddr& server, const obrpc::ObIFill& msg,
      const int64_t msg_type, const ObPartitionKey& pkey)
  {
    UNUSED(tenant_id);
    UNUSED(server);
    UNUSED(msg);
    UNUSED(msg_type);
    UNUSED(pkey);
    return OB_SUCCESS;
  }

  int post_trans_resp_msg(const uint64_t tenant_id, const common::ObAddr& server, const ObTransMsg& msg)
  {
    UNUSED(tenant_id);
    UNUSED(server);
    UNUSED(msg);
    return OB_SUCCESS;
  }
};

class TestObTransSubmitLogCb : public ::testing::Test {
public:
  virtual void SetUp()
  {}
  virtual void TearDown()
  {}

public:
  static const int64_t PARTITION_KEY_COUNT = 16;
  static const int64_t TENANT_ID = 1001;
  // valid parameter partition
  static const uint64_t VALID_TABLE_ID = 1;
  static const int32_t VALID_PARTITION_ID = 1;
  static const int32_t VALID_PARTITION_COUNT = 100;
  // invalid parameter partition
  static const int64_t INVALID_TABLE_ID = -1;
  static const int32_t INVALID_PARTITION_ID = -1;
  static const int32_t INVALID_PARTITION_COUNT = -100;

  // define port
  static const int32_t PORT = 8080;
  // type of ip address type for test, i.e., ipv4
  static const ObAddr::VER IP_TYPE = ObAddr::IPV4;
  // ip address for test
  static const char* LOCAL_IP;
};
const char* TestObTransSubmitLogCb::LOCAL_IP = "127.0.0.1";
ObTransService trans_service;
MyMockObPartitionService2 partition_service;

//////////////////////basic function test//////////////////////////////////////////
// create an object of ObTransSubmitLogCb during participant logging in 2pc
// log_type is repuired to be set in callback object due to multiple log records
//     with different log types which are written by a participant
// test init, set_log_type and reset in normal cases
TEST_F(TestObTransSubmitLogCb, init_set_log_type_reset)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  // create objects of ObPartTransCtxMgr, ObPartitionKey and ObTransID as input parameters
  ObPartTransCtxMgr part_ctx_mgr;
  ObLtsSource lts_source;
  MockObTsMgr ts_mgr(lts_source);
  EXPECT_EQ(OB_SUCCESS, part_ctx_mgr.init(2, &ts_mgr, &partition_service));
  ObPartitionKey partition_key(combine_id(TestObTransSubmitLogCb::TENANT_ID, TestObTransSubmitLogCb::VALID_TABLE_ID),
      TestObTransSubmitLogCb::VALID_PARTITION_ID,
      TestObTransSubmitLogCb::VALID_PARTITION_COUNT);
  ObAddr observer(TestObTransSubmitLogCb::IP_TYPE, TestObTransSubmitLogCb::LOCAL_IP, TestObTransSubmitLogCb::PORT);
  ObTransID trans_id(observer);

  ObTransSubmitLogCb submit_log_cb;
  ObPartTransCtx* ctx = new ObPartTransCtx();
  EXPECT_EQ(OB_SUCCESS, submit_log_cb.init(&trans_service, partition_key, trans_id, ctx));
  EXPECT_EQ(OB_SUCCESS, submit_log_cb.set_log_type(OB_LOG_TRANS_REDO));
  submit_log_cb.reset();
  if (NULL != ctx) {
    delete ctx;
    ctx = NULL;
  }
}
// test on_success of ObTransSubmitLogCb which is callbacked when log is persisted successfully
TEST_F(TestObTransSubmitLogCb, submit_log_on_success)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  // create objects of ObPartTransCtxMgr, ObPartitionKey and ObTransID as input parameters
  ObPartTransCtxMgr part_ctx_mgr;
  ObLtsSource lts_source;
  MockObTsMgr ts_mgr(lts_source);
  EXPECT_EQ(OB_SUCCESS, part_ctx_mgr.init(2, &ts_mgr, &partition_service));
  ObPartitionKey partition_key(combine_id(TestObTransSubmitLogCb::TENANT_ID, TestObTransSubmitLogCb::VALID_TABLE_ID),
      TestObTransSubmitLogCb::VALID_PARTITION_ID,
      TestObTransSubmitLogCb::VALID_PARTITION_COUNT);
  ObAddr observer(TestObTransSubmitLogCb::IP_TYPE, TestObTransSubmitLogCb::LOCAL_IP, TestObTransSubmitLogCb::PORT);
  ObTransID trans_id(observer);

  // part_ctx_mgr creates a trans ctx first; otherwise, fail to execute get_trans_ctx when log callback
  bool alloc = true;
  ObTransCtx* ctx = NULL;

  EXPECT_TRUE(OB_SUCCESS == part_ctx_mgr.add_partition(partition_key));
  EXPECT_TRUE(OB_SUCCESS == part_ctx_mgr.leader_takeover(partition_key, 100));
  EXPECT_TRUE(OB_SUCCESS != part_ctx_mgr.leader_active(partition_key, leader_active_arg));
  EXPECT_EQ(OB_NOT_RUNNING, part_ctx_mgr.get_trans_ctx(partition_key, trans_id, false, false, alloc, ctx));

  ObTransSubmitLogCb submit_log_cb;
  ObPartTransCtx* trans_part_ctx = new ObPartTransCtx();
  EXPECT_EQ(OB_SUCCESS, submit_log_cb.init(&trans_service, partition_key, trans_id, trans_part_ctx));
  EXPECT_EQ(OB_SUCCESS, submit_log_cb.set_log_type(OB_LOG_TRANS_COMMIT));
  // commit_log is submitted by a participant
  MockObPartTransCtx* part_ctx = static_cast<MockObPartTransCtx*>(trans_part_ctx);
  MockObTransRpc rpc;
  // MockObLocationAdapter location_adapter;
  ObClogAdapter clog_adapter;

  ObStartTransParam parms;
  parms.set_access_mode(ObTransAccessMode::READ_ONLY);
  parms.set_type(ObTransType::TRANS_USER);
  parms.set_isolation(ObTransIsolation::READ_COMMITED);

  ObTransTimer timer;
  EXPECT_EQ(OB_SUCCESS, timer.init());
  EXPECT_EQ(OB_SUCCESS, timer.start());
  ObMemtableCtxFactory mt_ctx_factory;
  ObPartitionService partition_service;
  // const uint64_t thread_id = 1000;
  // const uint64_t cluster_version = 100;
  // const int64_t leader_epoch = 100;
  // EXPECT_EQ(OB_ERR_UNEXPECTED, part_ctx->init(TestObTransSubmitLogCb::TENANT_ID, trans_id,
  // ObClockGenerator::getClock() + 100000000,
  //      partition_key, &part_ctx_mgr, parms, cluster_version, &trans_service, thread_id, leader_epoch));
  EXPECT_EQ(OB_SUCCESS, part_ctx->set_coordinator(partition_key));
  // log version is generated by log_service
  // const int64_t LOG_VERSION = 1;
  // EXPECT_EQ(OB_SUCCESS, submit_log_cb.on_success(partition_key, 1, LOG_VERSION));
  submit_log_cb.reset();
  rpc.stop();
  EXPECT_EQ(OB_SUCCESS, timer.stop());
  timer.wait();
  timer.destroy();
  if (NULL != trans_part_ctx) {
    delete trans_part_ctx;
    trans_part_ctx = NULL;
  }
}
///////////////////////////////////////boundary test//////////////////////////////////////////////
// test init
// 1. repeated init
// 2. invalid input parameters
TEST_F(TestObTransSubmitLogCb, init_twice_invalid_args)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  // create objects of ObPartTransCtxMgr, ObPartitionKey and ObTransID as input parameters
  ObPartTransCtxMgr part_ctx_mgr;
  ObLtsSource lts_source;
  MockObTsMgr ts_mgr(lts_source);
  EXPECT_EQ(OB_SUCCESS, part_ctx_mgr.init(2, &ts_mgr, &partition_service));
  // create an invalid partition_key
  ObPartitionKey invalid_partition_key(TestObTransSubmitLogCb::INVALID_TABLE_ID,
      TestObTransSubmitLogCb::INVALID_PARTITION_ID,
      TestObTransSubmitLogCb::INVALID_PARTITION_COUNT);
  ObAddr observer(TestObTransSubmitLogCb::IP_TYPE, TestObTransSubmitLogCb::LOCAL_IP, TestObTransSubmitLogCb::PORT);
  ObTransID trans_id(observer);

  ObTransSubmitLogCb submit_log_cb;
  ObPartTransCtx* ctx = new ObPartTransCtx();
  EXPECT_EQ(OB_INVALID_ARGUMENT, submit_log_cb.init(&trans_service, invalid_partition_key, trans_id, ctx));
  // create a valid partition
  ObPartitionKey valid_partition_key(
      combine_id(TestObTransSubmitLogCb::TENANT_ID, TestObTransSubmitLogCb::VALID_TABLE_ID),
      TestObTransSubmitLogCb::VALID_PARTITION_ID,
      TestObTransSubmitLogCb::VALID_PARTITION_COUNT);
  EXPECT_EQ(OB_SUCCESS, submit_log_cb.init(&trans_service, valid_partition_key, trans_id, ctx));
  EXPECT_EQ(OB_INIT_TWICE, submit_log_cb.init(&trans_service, valid_partition_key, trans_id, ctx));
  if (NULL != ctx) {
    delete ctx;
    ctx = NULL;
  }
}
// test set_log_type:
// 1. uninit
// 2. invalid log_type
TEST_F(TestObTransSubmitLogCb, set_log_type_invalid_args)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  // create objects of ObPartTransCtxMgr, ObPartitionKey and ObTransID as input parameters
  ObPartTransCtxMgr part_ctx_mgr;
  ObLtsSource lts_source;
  MockObTsMgr ts_mgr(lts_source);
  EXPECT_EQ(OB_SUCCESS, part_ctx_mgr.init(2, &ts_mgr, &partition_service));
  // create valid partition
  ObPartitionKey valid_partition_key(
      combine_id(TestObTransSubmitLogCb::TENANT_ID, TestObTransSubmitLogCb::VALID_TABLE_ID),
      TestObTransSubmitLogCb::VALID_PARTITION_ID,
      TestObTransSubmitLogCb::VALID_PARTITION_COUNT);
  ObAddr observer(TestObTransSubmitLogCb::IP_TYPE, TestObTransSubmitLogCb::LOCAL_IP, TestObTransSubmitLogCb::PORT);
  ObTransID trans_id(observer);

  ObTransSubmitLogCb submit_log_cb;
  ObPartTransCtx* ctx = new ObPartTransCtx();
  const int64_t log_type = storage::OB_LOG_UNKNOWN;
  EXPECT_EQ(OB_NOT_INIT, submit_log_cb.set_log_type(log_type));
  EXPECT_EQ(OB_SUCCESS, submit_log_cb.init(&trans_service, valid_partition_key, trans_id, ctx));
  EXPECT_EQ(OB_INVALID_ARGUMENT, submit_log_cb.set_log_type(log_type));
  if (NULL != ctx) {
    delete ctx;
    ctx = NULL;
  }
}
// test on_suceess is callbacked by log module
// 1. uninit
// 2. invalid input parameters partition_key and version
//    partition key not match in submit_log_cb object
TEST_F(TestObTransSubmitLogCb, on_success_invalid_args)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  // create objects of ObPartTransCtxMgr, ObPartitionKey and ObTransID as input parameters
  ObPartTransCtxMgr part_ctx_mgr;
  ObLtsSource lts_source;
  MockObTsMgr ts_mgr(lts_source);
  EXPECT_EQ(OB_SUCCESS, part_ctx_mgr.init(2, &ts_mgr, &partition_service));
  // create valid partition
  ObPartitionKey valid_partition_key(
      combine_id(TestObTransSubmitLogCb::TENANT_ID, TestObTransSubmitLogCb::VALID_TABLE_ID),
      TestObTransSubmitLogCb::VALID_PARTITION_ID,
      TestObTransSubmitLogCb::VALID_PARTITION_COUNT);
  ObAddr observer(TestObTransSubmitLogCb::IP_TYPE, TestObTransSubmitLogCb::LOCAL_IP, TestObTransSubmitLogCb::PORT);
  ObTransID trans_id(observer);

  ObTransSubmitLogCb submit_log_cb;
  ObPartTransCtx* ctx = new ObPartTransCtx();
  int64_t log_version = -1;
  EXPECT_EQ(OB_NOT_INIT, submit_log_cb.on_submit_log_success(false, 1, log_version));
  EXPECT_EQ(OB_SUCCESS, submit_log_cb.init(&trans_service, valid_partition_key, trans_id, ctx));
  // EXPECT_EQ(OB_INVALID_ARGUMENT, submit_log_cb.on_submit_log_success(1, log_version));

  log_version = 1;
  ObPartitionKey new_valid_partition_key(
      combine_id(TestObTransSubmitLogCb::TENANT_ID, TestObTransSubmitLogCb::VALID_TABLE_ID + 1),
      TestObTransSubmitLogCb::VALID_PARTITION_ID,
      TestObTransSubmitLogCb::VALID_PARTITION_COUNT);
  // EXPECT_EQ(OB_ERR_UNEXPECTED, submit_log_cb.on_submit_log_success(1, log_version));
  // because partition_key is not operated by add_partition,
  // ctx_mgr_not_exist is returned if on_success is called directly
  // EXPECT_EQ(OB_NOT_RUNNING, submit_log_cb.on_submit_log_success(1, log_version));

  bool alloc = true;
  ObTransCtx* part_ctx = NULL;
  EXPECT_TRUE(OB_SUCCESS == part_ctx_mgr.add_partition(valid_partition_key));
  EXPECT_TRUE(OB_SUCCESS == part_ctx_mgr.leader_takeover(valid_partition_key, 100));
  EXPECT_TRUE(OB_SUCCESS != part_ctx_mgr.leader_active(valid_partition_key, leader_active_arg));
  EXPECT_EQ(OB_NOT_RUNNING, part_ctx_mgr.get_trans_ctx(valid_partition_key, trans_id, false, false, alloc, part_ctx));

  // EXPECT_NE(OB_SUCCESS, submit_log_cb.on_success(valid_partition_key, 1, log_version));
  if (NULL != ctx) {
    delete ctx;
    ctx = NULL;
  }
}

}  // namespace unittest
}  // namespace oceanbase

using namespace oceanbase;
using namespace oceanbase::common;

int main(int argc, char** argv)
{
  int ret = 1;
  ObLogger& logger = ObLogger::get_logger();
  logger.set_file_name("test_ob_trans_submit_log_cb.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc, argv);
  ret = RUN_ALL_TESTS();
  return ret;
}
