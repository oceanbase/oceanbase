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

#include "storage/transaction/ob_trans_rpc.h"
#include "../mockcontainer/mock_ob_partition_service.h"
#include "../mockcontainer/mock_ob_location_cache.h"
#include "../mockcontainer/mock_ob_trans_service.h"
#include <gtest/gtest.h>
#include "share/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "common/ob_clock_generator.h"
#include "share/ob_common_rpc_proxy.h"
#include "lib/container/ob_array_iterator.h"

namespace oceanbase {
using namespace common;
using namespace transaction;
using namespace storage;
using namespace obrpc;
using namespace share;
namespace unittest {
class MyMockObPartitionService : public MockObIPartitionService {
public:
  MyMockObPartitionService(transaction::ObTransService* trans_service) : trans_service_(trans_service)
  {}
  ~MyMockObPartitionService()
  {}
  transaction::ObTransService* get_trans_service()
  {
    return trans_service_;
  }

private:
  transaction::ObTransService* trans_service_;
};

class ProcessorMgr : public ObTransP {
public:
  ProcessorMgr(MyMockObPartitionService* partition_service) : ObTransP(partition_service)
  {}
  int do_response_msg()
  {
    int ret = OB_SUCCESS;
    if (OB_SUCCESS != (ret = process())) {
      TRANS_LOG(INFO, "processor execute error.");
    } else {
      // do nothing.
    }
    return ret;
  }
};
class MockObTransRpcProxy : public ObTransRpcProxy {
  virtual int post_trans_msg(const oceanbase::transaction::ObTransMsg& trans_msg,
      AsyncCB<(oceanbase::obrpc::ObRpcPacketCode)1793>* async_cb, const oceanbase::obrpc::ObRpcOpts& rpc_opt)
  {
    UNUSED(trans_msg);
    UNUSED(async_cb);
    UNUSED(rpc_opt);
    return OB_SUCCESS;
  }
};

class TestObTransRpc : public ::testing::Test {
public:
  virtual void SetUp()
  {}
  virtual void TearDown()
  {}

  // define port
  static const int32_t PORT = 8080;
  // ObAddr using ipv4 for test
  static const ObAddr::VER IP_TYPE = ObAddr::IPV4;
  // ip address for test
  static const char* LOCAL_IP;

  // valid partition parameters
  static const int64_t VALID_TABLE_ID = 1;
  static const int32_t VALID_PARTITION_ID = 1;
  static const int32_t VALID_PARTITION_COUNT = 100;
  // invalid partition parameters
  static const int64_t INVALID_TABLE_ID = -1;
  static const int32_t INVALID_PARTITION_ID = -1;
  static const int32_t INVALID_PARTITION_COUNT = -100;
};
const char* TestObTransRpc::LOCAL_IP = "127.0.0.1";
MockObTransService trans_service;

//////////////////////basic function test//////////////////////////////////////////
/*
TEST_F(TestObTransRpc, trans_processor_process)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  MockObTransService trans_service;
  MyMockObPartitionService partition_service(&trans_service);

  ProcessorMgr process_mgr(&partition_service);
  EXPECT_EQ(OB_SUCCESS, process_mgr.do_response_msg());
}
*/
// memory allocation and collection of TransRpcTask in local rpc
TEST_F(TestObTransRpc, trans_rpc_task_alloc_free)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  TransRpcTask* rpc_task = TransRpcTaskFactory::alloc();
  EXPECT_TRUE(NULL != rpc_task);
  TransRpcTaskFactory::release(rpc_task);
}
/*
// test the init , post_trans_msg and destroy of ObTranssRpc
TEST_F(TestObTransRpc, trans_rpc_init_postmsg_destroy)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObAddr observer(TestObTransRpc::IP_TYPE, TestObTransRpc::LOCAL_IP, TestObTransRpc::PORT);
  MockObTransRpcProxy rpc_proxy;
  MockObTransService trans_service;
  ObTransRpc rpc;
  EXPECT_EQ(OB_SUCCESS, rpc.init(&rpc_proxy, &trans_service, observer));
  EXPECT_EQ(OB_SUCCESS, rpc.start());

  //-------------------------------------------------------------------------
  const int64_t PARTICIPANT_NUM = 10;
  ObTransID trans_id(observer);
  ObPartitionArray participants;
  for (int64_t i = 0; i < PARTICIPANT_NUM; ++i) {
    ObPartitionKey partition_key(VALID_TABLE_ID + i, VALID_PARTITION_ID, VALID_PARTITION_COUNT);
    EXPECT_EQ(OB_SUCCESS, participants.push_back(partition_key));
  }

  ObAddr &scheduler = observer;
  const ObPartitionKey &coordinator = participants.at(0);

  const ObPartitionKey &sender = participants.at(0);
  const ObPartitionKey &receiver = participants.at(1);

  ObStartTransParam parms;
  parms.set_access_mode(ObTransAccessMode::READ_ONLY);
  parms.set_type(ObTransType::TRANS_USER);
  parms.set_isolation(ObTransIsolation::READ_COMMITED);
  parms.set_timeout(1);
  parms.set_idle_timeout(1);

  int64_t msg_type = OB_TRANS_COMMIT_REQUEST;
  int64_t trans_time = 9870;
  int64_t sql_no = 1;
  int32_t status = 1;
  int64_t state = Ob2PCState::COMMIT;
  const int64_t trans_version = 1;

  ObTransMsg msg;
  EXPECT_EQ(OB_SUCCESS, msg.init(trans_id, msg_type, trans_time, sender, receiver, scheduler,
      coordinator, participants, parms, sql_no, status, state, trans_version));
  EXPECT_EQ(OB_SUCCESS, rpc.post_trans_msg(observer, msg, msg_type));
  EXPECT_EQ(OB_SUCCESS, rpc.stop());
  rpc.destroy();
}
*/

/*
TEST_F(TestObTransRpc, trans_rpc_processor_init_valid)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  MockObTransService *trans_service = NULL;
  MyMockObPartitionService partition_service(trans_service);

  ProcessorMgr process_mgr(&partition_service);
  EXPECT_EQ(OB_ERR_UNEXPECTED, process_mgr.do_response_msg());
}
*/

// test the init failure of ObTranssRpc
// invalid input parameters and repeated init
TEST_F(TestObTransRpc, trans_rpc_init_invalid_args)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObAddr observer(TestObTransRpc::IP_TYPE, TestObTransRpc::LOCAL_IP, TestObTransRpc::PORT);
  ObTransRpcProxy* rpc_proxy = NULL;
  ObAddr addr;
  ObTransMsg msg;
  ObTransRpc rpc;
  obrpc::ObBatchRpc* batch_rpc = NULL;
  // rpc without init
  EXPECT_EQ(OB_NOT_INIT, rpc.start());
  EXPECT_EQ(OB_NOT_INIT, rpc.post_trans_msg(1001, addr, msg, OB_TRANS_COMMIT_REQUEST));
  EXPECT_EQ(OB_NOT_INIT, rpc.post_trans_resp_msg(1001, addr, msg));

  EXPECT_EQ(OB_INVALID_ARGUMENT, rpc.init(rpc_proxy, &trans_service, observer, batch_rpc));
  rpc_proxy = new ObTransRpcProxy();
  batch_rpc = new obrpc::ObBatchRpc();
  EXPECT_TRUE(NULL != rpc_proxy);
  EXPECT_TRUE(NULL != batch_rpc);
  EXPECT_EQ(OB_SUCCESS, rpc.init(rpc_proxy, &trans_service, observer, batch_rpc));
  EXPECT_EQ(OB_INIT_TWICE, rpc.init(rpc_proxy, &trans_service, observer, batch_rpc));

  // rpc without start
  EXPECT_EQ(OB_NOT_RUNNING, rpc.post_trans_msg(1001, addr, msg, OB_TRANS_COMMIT_REQUEST));
  EXPECT_EQ(OB_NOT_RUNNING, rpc.post_trans_resp_msg(1001, addr, msg));

  // repeat start
  EXPECT_EQ(OB_SUCCESS, rpc.start());
  EXPECT_EQ(OB_ERR_UNEXPECTED, rpc.start());

  TransRpcTask* trans_task = new TransRpcTask();
  EXPECT_TRUE(NULL != trans_task);

  EXPECT_EQ(OB_INVALID_ARGUMENT, rpc.post_trans_msg(1001, addr, msg, OB_TRANS_COMMIT_REQUEST));
  EXPECT_EQ(OB_INVALID_ARGUMENT, rpc.post_trans_resp_msg(1001, addr, msg));

  rpc.stop();
  rpc.wait();
  rpc.destroy();
  // delete rpc_proxy; // to make gcc 5.2 happy
  rpc_proxy = NULL;
}

/*
TEST_F(TestObTransRpc, trans_rpc_post_msg_error)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObAddr observer(TestObTransRpc::IP_TYPE, TestObTransRpc::LOCAL_IP, TestObTransRpc::PORT);
  ObAddr dest_observer(TestObTransRpc::IP_TYPE, TestObTransRpc::LOCAL_IP, TestObTransRpc::PORT);
  MockObTransRpcProxy rpc_proxy;
  MockObTransService trans_service;
  ObTransRpc rpc;

  ObTransMsg msg;
  EXPECT_EQ(OB_NOT_INIT, rpc.post_trans_msg(observer, msg, OB_TRANS_COMMIT_REQUEST));
  EXPECT_EQ(OB_SUCCESS, rpc.init(&rpc_proxy, &trans_service, observer));
  EXPECT_EQ(OB_SUCCESS, rpc.start());
  EXPECT_EQ(OB_INVALID_ARGUMENT, rpc.post_trans_msg(observer, msg, OB_TRANS_COMMIT_REQUEST));

  //-------------------------------------------------------------------------
  const int64_t PARTICIPANT_NUM = 10;
  ObTransID trans_id(observer);
  ObPartitionArray participants;
  for (int64_t i = 0; i < PARTICIPANT_NUM; ++i) {
    ObPartitionKey partition_key(VALID_TABLE_ID + i, VALID_PARTITION_ID, VALID_PARTITION_COUNT);
    EXPECT_EQ(OB_SUCCESS, participants.push_back(partition_key));
  }

  ObAddr &scheduler = observer;
  const ObPartitionKey &coordinator = participants.at(0);

  const ObPartitionKey &sender = participants.at(0);
  const ObPartitionKey &receiver = participants.at(1);

  ObStartTransParam parms;
  parms.set_access_mode(ObTransAccessMode::READ_ONLY);
  parms.set_type(ObTransType::TRANS_USER);
  parms.set_isolation(ObTransIsolation::READ_COMMITED);
  parms.set_timeout(1);
  parms.set_idle_timeout(1);

  int64_t msg_type = OB_TRANS_COMMIT_REQUEST;
  int64_t trans_time = 9870;
  int64_t sql_no = 1;
  int32_t status = 1;
  int64_t state = Ob2PCState::COMMIT;
  const int64_t trans_version = 1;

  EXPECT_EQ(OB_SUCCESS, msg.init(trans_id, msg_type, trans_time, sender, receiver, scheduler,
      coordinator, participants, parms, sql_no, status, state, trans_version));
  EXPECT_EQ(OB_SUCCESS, rpc.post_trans_msg(dest_observer, msg, msg_type));
  EXPECT_EQ(OB_SUCCESS, rpc.stop());
  rpc.destroy();
}
*/
}  // namespace unittest
}  // namespace oceanbase

using namespace oceanbase;
using namespace oceanbase::common;

int main(int argc, char** argv)
{
  int ret = 1;
  ObLogger& logger = ObLogger::get_logger();
  logger.set_file_name("test_ob_trans_rpc.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);

  if (OB_SUCCESS != ObClockGenerator::init()) {
    TRANS_LOG(WARN, "init ob_clock_generator error!");
  } else {
    testing::InitGoogleTest(&argc, argv);
    ret = RUN_ALL_TESTS();
  }
  return ret;
}
