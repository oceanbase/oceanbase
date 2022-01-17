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

#include "storage/transaction/ob_trans_msg.h"
#include "storage/transaction/ob_trans_msg_type.h"
#include <gtest/gtest.h>
#include "share/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "common/ob_partition_key.h"
#include "common/ob_clock_generator.h"

namespace oceanbase {
using namespace common;
using namespace transaction;
namespace unittest {
class TestObTransMsg : public ::testing::Test {
public:
  virtual void SetUp()
  {
    init_();
  }
  virtual void TearDown()
  {}

private:
  int init_();

public:
  // valid partition parameters
  static const int64_t VALID_TABLE_ID = 1;
  static const int32_t VALID_PARTITION_ID = 1;
  static const int32_t VALID_PARTITION_COUNT = 100;

  // invalid partition parameters
  static const int64_t INVALID_TABLE_ID = -1;
  static const int32_t INVALID_PARTITION_ID = -1;
  static const int32_t INVALID_PARTITION_COUNT = -100;

  static const int32_t PORT = 8080;
  static const ObAddr::VER IP_TYPE = ObAddr::IPV4;
  static const char* LOCAL_IP;
  static const uint64_t TENANT_ID = 1001;

public:
  common::ObAddr observer_;
};
const char* TestObTransMsg::LOCAL_IP = "127.0.0.1";

int TestObTransMsg::init_()
{
  int ret = OB_SUCCESS;
  observer_ = ObAddr(TestObTransMsg::IP_TYPE, TestObTransMsg::LOCAL_IP, TestObTransMsg::PORT);

  return ret;
}

//////////////////////basic function test//////////////////////////////////////////
// test the init of ObTransMsg
TEST_F(TestObTransMsg, trans_msg_init_valid)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  const int64_t PARTICIPANT_NUM = 10;
  // create an object of ObTtransID
  ObTransID trans_id(observer_);
  // create all participants
  ObPartitionArray participants;
  for (int64_t i = 0; i < PARTICIPANT_NUM; ++i) {
    ObPartitionKey partition_key(VALID_TABLE_ID + i, VALID_PARTITION_ID, VALID_PARTITION_COUNT);
    ASSERT_EQ(OB_SUCCESS, participants.push_back(partition_key));
  }

  // create scheduler and coordinator
  ObAddr& scheduler = observer_;
  const ObPartitionKey& coordinator = participants.at(0);

  // create sender and receiver
  const ObPartitionKey& sender = participants.at(0);
  const ObPartitionKey& receiver = participants.at(1);

  // create an object of ObStartTransParm
  ObStartTransParam parms;
  parms.set_access_mode(ObTransAccessMode::READ_ONLY);
  parms.set_type(ObTransType::TRANS_USER);
  parms.set_isolation(ObTransIsolation::READ_COMMITED);

  int64_t msg_type = OB_TRANS_COMMIT_REQUEST;
  int64_t trans_time = 9870;
  int64_t sql_no = 1;
  int32_t status = 1;
  int64_t state = Ob2PCState::COMMIT;
  const int64_t trans_version = 1;
  const int64_t request_id = ObClockGenerator::getClock();
  const ObStmtRollbackInfo stmt_rollback_info;
  const ObString trace_id = "trance_id=xxx";

  ObTransMsg msg;
  ASSERT_EQ(OB_SUCCESS,
      msg.init(TestObTransMsg::TENANT_ID,
          trans_id,
          msg_type,
          trans_time,
          sender,
          receiver,
          scheduler,
          coordinator,
          participants,
          parms,
          observer_,
          sql_no,
          status,
          state,
          trans_version,
          request_id,
          MonotonicTs(0)));
  ASSERT_TRUE(msg.is_valid());

  // test the serialization of ObTransMsg
  int64_t pos = 0;
  const int64_t BUFFER_SIZE = 10240;
  char buffer[BUFFER_SIZE];
  ASSERT_EQ(OB_SUCCESS, msg.serialize(buffer, BUFFER_SIZE, pos));

  // test the deserialization of ObTransMsg
  ObTransMsg msg1;
  int64_t start_index = 0;
  ASSERT_EQ(OB_SUCCESS, msg1.deserialize(buffer, pos, start_index));
  EXPECT_EQ(trans_id, msg1.get_trans_id());
  EXPECT_EQ(msg_type, msg1.get_msg_type());
  EXPECT_EQ(trans_time, msg1.get_trans_time());
  EXPECT_EQ(sender, msg1.get_sender());
  EXPECT_EQ(receiver, msg1.get_receiver());
  EXPECT_EQ(scheduler, msg1.get_scheduler());
  EXPECT_EQ(coordinator, msg1.get_coordinator());
  EXPECT_EQ(sql_no, msg1.get_sql_no());
  EXPECT_EQ(status, msg1.get_status());
  EXPECT_EQ(observer_, msg1.get_sender_addr());
  EXPECT_EQ(state, msg1.get_state());
  EXPECT_EQ(trans_version, msg1.get_trans_version());
  EXPECT_EQ(request_id, msg1.get_request_id());

  // results of deserializing ObStartTransParam
  ObStartTransParam start_trans_param = msg1.get_trans_param();
  EXPECT_EQ(parms.get_access_mode(), start_trans_param.get_access_mode());
  EXPECT_EQ(parms.get_type(), start_trans_param.get_type());
  EXPECT_EQ(parms.get_isolation(), start_trans_param.get_isolation());
  // results of deserializing participants
  EXPECT_EQ(participants.count(), msg1.get_participants().count());

  // operations of serializing and deserializing ObPartitionToLogId
  ObPartitionKey partition(VALID_TABLE_ID, VALID_PARTITION_ID, VALID_PARTITION_COUNT);
  int64_t log_id = 1;
  ObPartitionLogInfo partition_log_info(partition, log_id, ObClockGenerator::getClock());
  pos = 0;
  start_index = 0;
  ASSERT_EQ(OB_SUCCESS, partition_log_info.serialize(buffer, BUFFER_SIZE, pos));
  // test the reulst of deserialization
  ObPartitionLogInfo partition_log_info1;
  ASSERT_EQ(OB_SUCCESS, partition_log_info1.deserialize(buffer, pos, start_index));
  EXPECT_EQ(partition, partition_log_info1.get_partition());
  EXPECT_EQ(log_id, partition_log_info1.get_log_id());

  // invalid input parameters of init
  ObStartTransParam parms1;
  ObTransMsg msg2;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      msg2.init(TestObTransMsg::TENANT_ID,
          trans_id,
          msg_type,
          trans_time,
          sender,
          receiver,
          scheduler,
          coordinator,
          participants,
          parms1,
          observer_,
          sql_no,
          status,
          state,
          trans_version,
          ObClockGenerator::getClock(),
          MonotonicTs(0)));
  PartitionLogInfoArray log_arr;
  EXPECT_EQ(OB_SUCCESS, log_arr.push_back(partition_log_info));
  EXPECT_EQ(OB_NOT_INIT, msg2.set_partition_log_info_arr(log_arr));
  EXPECT_EQ(OB_NOT_INIT, msg2.set_prepare_log_id(log_id));
  EXPECT_FALSE(msg2.is_valid());
  // repeated init
  EXPECT_EQ(OB_SUCCESS,
      msg2.init(TestObTransMsg::TENANT_ID,
          trans_id,
          msg_type,
          trans_time,
          sender,
          receiver,
          scheduler,
          coordinator,
          participants,
          parms,
          observer_,
          sql_no,
          status,
          state,
          trans_version,
          ObClockGenerator::getClock(),
          MonotonicTs(0)));
  EXPECT_TRUE(msg2.is_valid());
  EXPECT_EQ(OB_INIT_TWICE,
      msg2.init(TestObTransMsg::TENANT_ID,
          trans_id,
          msg_type,
          trans_time,
          sender,
          receiver,
          scheduler,
          coordinator,
          participants,
          parms,
          observer_,
          sql_no,
          status,
          state,
          trans_version,
          ObClockGenerator::getClock(),
          MonotonicTs(0)));
}

// OB_TRANS_COMMIT_REQUEST/RESPONSE
// OB_TRANS_ABORT_REQUEST/RESPONSE
TEST_F(TestObTransMsg, trans_commit_abort_request_response_msg)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  const int64_t PARTICIPANT_NUM = 10;
  ObTransID trans_id(observer_);
  ObPartitionArray participants;
  ObTransLocationCache cache;
  for (int64_t i = 0; i < PARTICIPANT_NUM; ++i) {
    ObPartitionKey partition_key(VALID_TABLE_ID + i, VALID_PARTITION_ID, VALID_PARTITION_COUNT);
    ASSERT_EQ(OB_SUCCESS, participants.push_back(partition_key));
    ObPartitionLeaderInfo info(partition_key, observer_);
    ASSERT_EQ(OB_SUCCESS, cache.push_back(info));
  }
  const ObPartitionKey& sender = participants.at(0);
  const ObPartitionKey& receiver = participants.at(1);
  const ObAddr& scheduler = observer_;
  const ObPartitionKey coordinator = participants.at(0);

  // create an object of ObStartTransParm
  ObStartTransParam parms;
  parms.set_access_mode(ObTransAccessMode::READ_ONLY);
  parms.set_type(ObTransType::TRANS_USER);
  parms.set_isolation(ObTransIsolation::READ_COMMITED);
  int64_t trans_time = 9870;
  const int32_t status = OB_SUCCESS;

  // OB_TRANS_COMMIT_REQUEST
  int64_t msg_type = OB_TRANS_STMT_ROLLBACK_REQUEST;
  const int64_t commit_times = 1;
  ObTransMsg trans_commit_request_msg;
  const int64_t need_wait_interval_us = 0;
  const ObStmtRollbackInfo stmt_rollback_info;
  const ObString trace_id = "trance_id=xxx";
  const ObXATransID xid;

  EXPECT_EQ(OB_INVALID_ARGUMENT,
      trans_commit_request_msg.init(TestObTransMsg::TENANT_ID,
          trans_id,
          msg_type,
          trans_time,
          sender,
          receiver,
          scheduler,
          coordinator,
          participants,
          parms,
          observer_,
          cache,
          commit_times,
          MonotonicTs(0),
          false,
          stmt_rollback_info,
          trace_id,
          xid));
  msg_type = OB_TRANS_COMMIT_REQUEST;
  EXPECT_EQ(OB_SUCCESS,
      trans_commit_request_msg.init(TestObTransMsg::TENANT_ID,
          trans_id,
          msg_type,
          trans_time,
          sender,
          receiver,
          scheduler,
          coordinator,
          participants,
          parms,
          observer_,
          cache,
          commit_times,
          MonotonicTs(0),
          false,
          stmt_rollback_info,
          trace_id,
          xid));
  EXPECT_EQ(OB_INIT_TWICE,
      trans_commit_request_msg.init(TestObTransMsg::TENANT_ID,
          trans_id,
          msg_type,
          trans_time,
          sender,
          receiver,
          scheduler,
          coordinator,
          participants,
          parms,
          observer_,
          cache,
          commit_times,
          MonotonicTs(0),
          false,
          stmt_rollback_info,
          trace_id,
          xid));
  EXPECT_EQ(true, trans_commit_request_msg.is_valid());

  // OB_TRANS_COMMIT_RESPONSE
  ObTransMsg trans_commit_response_msg;
  msg_type = OB_TRANS_COMMIT_RESPONSE;
  trans_time = -1;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      trans_commit_response_msg.init(TestObTransMsg::TENANT_ID,
          trans_id,
          msg_type,
          trans_time,
          sender,
          receiver,
          parms,
          observer_,
          status,
          commit_times,
          need_wait_interval_us));
  trans_time = 1;
  EXPECT_EQ(OB_SUCCESS,
      trans_commit_response_msg.init(TestObTransMsg::TENANT_ID,
          trans_id,
          msg_type,
          trans_time,
          sender,
          receiver,
          parms,
          observer_,
          status,
          commit_times,
          need_wait_interval_us));
  EXPECT_EQ(OB_INIT_TWICE,
      trans_commit_response_msg.init(TestObTransMsg::TENANT_ID,
          trans_id,
          msg_type,
          trans_time,
          sender,
          receiver,
          parms,
          observer_,
          status,
          commit_times,
          need_wait_interval_us));
  EXPECT_EQ(true, trans_commit_response_msg.is_valid());

  // OB_TRANS_ABORT_REQUEST
  msg_type = OB_TRANS_STMT_ROLLBACK_REQUEST;
  ObTransMsg trans_abort_request_msg;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      trans_abort_request_msg.init(TestObTransMsg::TENANT_ID,
          trans_id,
          msg_type,
          trans_time,
          sender,
          receiver,
          scheduler,
          coordinator,
          participants,
          parms,
          observer_,
          cache,
          commit_times,
          MonotonicTs(0),
          false,
          stmt_rollback_info,
          trace_id,
          xid));
  msg_type = OB_TRANS_ABORT_REQUEST;
  EXPECT_EQ(OB_SUCCESS,
      trans_abort_request_msg.init(TestObTransMsg::TENANT_ID,
          trans_id,
          msg_type,
          trans_time,
          sender,
          receiver,
          scheduler,
          coordinator,
          participants,
          parms,
          observer_,
          cache,
          commit_times,
          MonotonicTs(0),
          false,
          stmt_rollback_info,
          trace_id,
          xid));
  EXPECT_EQ(OB_INIT_TWICE,
      trans_abort_request_msg.init(TestObTransMsg::TENANT_ID,
          trans_id,
          msg_type,
          trans_time,
          sender,
          receiver,
          scheduler,
          coordinator,
          participants,
          parms,
          observer_,
          cache,
          commit_times,
          MonotonicTs(0),
          false,
          stmt_rollback_info,
          trace_id,
          xid));
  EXPECT_EQ(true, trans_abort_request_msg.is_valid());

  // OB_TRANS_ABORT_RESPONSE
  ObTransMsg trans_abort_response_msg;
  msg_type = OB_TRANS_ABORT_RESPONSE;
  trans_time = -1;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      trans_abort_response_msg.init(TestObTransMsg::TENANT_ID,
          trans_id,
          msg_type,
          trans_time,
          sender,
          receiver,
          parms,
          observer_,
          status,
          commit_times,
          need_wait_interval_us));
  trans_time = 1;
  EXPECT_EQ(OB_SUCCESS,
      trans_abort_response_msg.init(TestObTransMsg::TENANT_ID,
          trans_id,
          msg_type,
          trans_time,
          sender,
          receiver,
          parms,
          observer_,
          status,
          commit_times,
          need_wait_interval_us));
  EXPECT_EQ(OB_INIT_TWICE,
      trans_abort_response_msg.init(TestObTransMsg::TENANT_ID,
          trans_id,
          msg_type,
          trans_time,
          sender,
          receiver,
          parms,
          observer_,
          status,
          commit_times,
          need_wait_interval_us));
  EXPECT_EQ(true, trans_abort_response_msg.is_valid());
}

// test OB_TRANS_ERROR_MSG
TEST_F(TestObTransMsg, trans_error_msg)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  int64_t msg_type = OB_TRANS_MSG_UNKNOWN;
  int64_t error_msg_type = OB_TRANS_MSG_UNKNOWN;
  ObTransID trans_id(observer_);
  ObPartitionKey partition_key(VALID_TABLE_ID, VALID_PARTITION_ID, VALID_PARTITION_COUNT);
  const ObPartitionKey& sender = partition_key;
  const int32_t status = OB_SUCCESS;
  const ObStmtRollbackInfo stmt_rollback_info;

  ObTransMsg error_msg;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      error_msg.init(msg_type, error_msg_type, TestObTransMsg::TENANT_ID, trans_id, sender, observer_, status, 1, 1));
  msg_type = OB_TRANS_ERROR_MSG;
  // error_msg_type = OB_TRANS_MSG_TYPE_UNKNOWN
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      error_msg.init(msg_type, error_msg_type, TestObTransMsg::TENANT_ID, trans_id, sender, observer_, status, 1, 1));
  ASSERT_FALSE(error_msg.is_valid());
  error_msg.reset();

  error_msg_type = OB_TRANS_STMT_CREATE_CTX_REQUEST;
  EXPECT_EQ(OB_SUCCESS,
      error_msg.init(msg_type, error_msg_type, TestObTransMsg::TENANT_ID, trans_id, sender, observer_, status, 1, 1));
  EXPECT_EQ(true, error_msg.is_valid());
  error_msg.reset();

  error_msg_type = OB_TRANS_STMT_CREATE_CTX_RESPONSE;
  EXPECT_EQ(OB_SUCCESS,
      error_msg.init(msg_type, error_msg_type, TestObTransMsg::TENANT_ID, trans_id, sender, observer_, status, 1, 1));
  EXPECT_EQ(true, error_msg.is_valid());
  error_msg.reset();

  error_msg_type = OB_TRANS_STMT_ROLLBACK_REQUEST;
  EXPECT_EQ(OB_SUCCESS,
      error_msg.init(msg_type, error_msg_type, TestObTransMsg::TENANT_ID, trans_id, sender, observer_, status, 1, 1));
  EXPECT_EQ(true, error_msg.is_valid());
  error_msg.reset();

  error_msg_type = OB_TRANS_STMT_ROLLBACK_RESPONSE;
  EXPECT_EQ(OB_SUCCESS,
      error_msg.init(msg_type, error_msg_type, TestObTransMsg::TENANT_ID, trans_id, sender, observer_, status, 1, 1));
  EXPECT_EQ(true, error_msg.is_valid());
  error_msg.reset();

  error_msg_type = OB_TRANS_COMMIT_REQUEST;
  EXPECT_EQ(OB_SUCCESS,
      error_msg.init(msg_type, error_msg_type, TestObTransMsg::TENANT_ID, trans_id, sender, observer_, status, 1, 1));
  EXPECT_EQ(true, error_msg.is_valid());
  error_msg.reset();

  error_msg_type = OB_TRANS_COMMIT_RESPONSE;
  EXPECT_EQ(OB_SUCCESS,
      error_msg.init(msg_type, error_msg_type, TestObTransMsg::TENANT_ID, trans_id, sender, observer_, status, 1, 1));
  EXPECT_EQ(true, error_msg.is_valid());
  error_msg.reset();

  error_msg_type = OB_TRANS_ABORT_REQUEST;
  EXPECT_EQ(OB_SUCCESS,
      error_msg.init(msg_type, error_msg_type, TestObTransMsg::TENANT_ID, trans_id, sender, observer_, status, 1, 1));
  EXPECT_EQ(true, error_msg.is_valid());
  error_msg.reset();

  error_msg_type = OB_TRANS_ABORT_RESPONSE;
  EXPECT_EQ(OB_SUCCESS,
      error_msg.init(msg_type, error_msg_type, TestObTransMsg::TENANT_ID, trans_id, sender, observer_, status, 1, 1));
  EXPECT_EQ(true, error_msg.is_valid());
  error_msg.reset();

  error_msg_type = OB_TRANS_2PC_PREPARE_REQUEST;
  EXPECT_EQ(OB_SUCCESS,
      error_msg.init(msg_type, error_msg_type, TestObTransMsg::TENANT_ID, trans_id, sender, observer_, status, 1, 1));
  EXPECT_EQ(true, error_msg.is_valid());
  error_msg.reset();

  error_msg_type = OB_TRANS_2PC_PREPARE_RESPONSE;
  EXPECT_EQ(OB_SUCCESS,
      error_msg.init(msg_type, error_msg_type, TestObTransMsg::TENANT_ID, trans_id, sender, observer_, status, 1, 1));
  EXPECT_EQ(true, error_msg.is_valid());
  error_msg.reset();

  error_msg_type = OB_TRANS_2PC_COMMIT_REQUEST;
  EXPECT_EQ(OB_SUCCESS,
      error_msg.init(msg_type, error_msg_type, TestObTransMsg::TENANT_ID, trans_id, sender, observer_, status, 1, 1));
  EXPECT_EQ(true, error_msg.is_valid());
  error_msg.reset();

  error_msg_type = OB_TRANS_2PC_COMMIT_RESPONSE;
  EXPECT_EQ(OB_SUCCESS,
      error_msg.init(msg_type, error_msg_type, TestObTransMsg::TENANT_ID, trans_id, sender, observer_, status, 1, 1));
  EXPECT_EQ(true, error_msg.is_valid());
  error_msg.reset();

  error_msg_type = OB_TRANS_2PC_ABORT_REQUEST;
  EXPECT_EQ(OB_SUCCESS,
      error_msg.init(msg_type, error_msg_type, TestObTransMsg::TENANT_ID, trans_id, sender, observer_, status, 1, 1));
  EXPECT_EQ(true, error_msg.is_valid());
  error_msg.reset();

  error_msg_type = OB_TRANS_2PC_ABORT_RESPONSE;
  EXPECT_EQ(OB_SUCCESS,
      error_msg.init(msg_type, error_msg_type, TestObTransMsg::TENANT_ID, trans_id, sender, observer_, status, 1, 1));
  EXPECT_EQ(true, error_msg.is_valid());
  error_msg.reset();

  error_msg_type = OB_TRANS_2PC_CLEAR_REQUEST;
  EXPECT_EQ(OB_SUCCESS,
      error_msg.init(msg_type, error_msg_type, TestObTransMsg::TENANT_ID, trans_id, sender, observer_, status, 1, 1));
  EXPECT_EQ(true, error_msg.is_valid());
  error_msg.reset();

  error_msg_type = OB_TRANS_2PC_CLEAR_RESPONSE;
  EXPECT_EQ(OB_SUCCESS,
      error_msg.init(msg_type, error_msg_type, TestObTransMsg::TENANT_ID, trans_id, sender, observer_, status, 1, 1));
  EXPECT_EQ(true, error_msg.is_valid());
  error_msg.reset();
}

// OB_TRANS_2PC_PREPARE_REQUEST/RESPONSE
// OB_TRANS_2PC_COMMIT_REQUEST/RESPONSE
TEST_F(TestObTransMsg, trans_2pc_prepare_commit_request_response_msg)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  const int64_t PARTICIPANT_NUM = 10;
  ObTransID trans_id(observer_);
  ObPartitionArray participants;
  for (int64_t i = 0; i < PARTICIPANT_NUM; ++i) {
    ObPartitionKey partition_key(VALID_TABLE_ID + i, VALID_PARTITION_ID, VALID_PARTITION_COUNT);
    ASSERT_EQ(OB_SUCCESS, participants.push_back(partition_key));
  }
  const ObPartitionKey& sender = participants.at(0);
  const ObPartitionKey& receiver = participants.at(1);
  const ObAddr& scheduler = observer_;
  const ObPartitionKey coordinator = participants.at(0);

  // create an object of ObStartTransParm
  ObStartTransParam parms;
  parms.set_access_mode(ObTransAccessMode::READ_ONLY);
  parms.set_type(ObTransType::TRANS_USER);
  parms.set_isolation(ObTransIsolation::READ_COMMITED);
  int64_t trans_time = 9870;
  const int32_t status = OB_SUCCESS;
  const int64_t request_id = ObClockGenerator::getClock();
  const int64_t prepare_log_id = 100;
  const int64_t trans_version = ObClockGenerator::getClock();
  int64_t state = Ob2PCState::PREPARE;
  const PartitionLogInfoArray arr;
  const ObStmtRollbackInfo stmt_rollback_info;
  const ObString trace_id = "trance_id=xxx";
  const ObXATransID xid;
  const bool is_xa_prepare = false;

  // OB_TRANS_2PC_PREPARE_REQUEST
  int64_t msg_type = OB_TRANS_STMT_ROLLBACK_REQUEST;
  ObTransMsg trans_2pc_prepare_request;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      trans_2pc_prepare_request.init(TestObTransMsg::TENANT_ID,
          trans_id,
          msg_type,
          trans_time,
          sender,
          receiver,
          scheduler,
          coordinator,
          participants,
          parms,
          observer_,
          status,
          request_id,
          MonotonicTs(0),
          arr,
          stmt_rollback_info,
          trace_id,
          xid,
          is_xa_prepare));
  msg_type = OB_TRANS_2PC_PREPARE_REQUEST;
  EXPECT_EQ(OB_SUCCESS,
      trans_2pc_prepare_request.init(TestObTransMsg::TENANT_ID,
          trans_id,
          msg_type,
          trans_time,
          sender,
          receiver,
          scheduler,
          coordinator,
          participants,
          parms,
          observer_,
          status,
          request_id,
          MonotonicTs(0),
          arr,
          stmt_rollback_info,
          trace_id,
          xid,
          is_xa_prepare));
  EXPECT_EQ(OB_INIT_TWICE,
      trans_2pc_prepare_request.init(TestObTransMsg::TENANT_ID,
          trans_id,
          msg_type,
          trans_time,
          sender,
          receiver,
          scheduler,
          coordinator,
          participants,
          parms,
          observer_,
          status,
          request_id,
          MonotonicTs(0),
          arr,
          stmt_rollback_info,
          trace_id,
          xid,
          is_xa_prepare));
  EXPECT_EQ(true, trans_2pc_prepare_request.is_valid());

  // OB_TRANS_2PC_PREPARE_RESPONSE
  ObTransMsg trans_2pc_prepare_response;
  msg_type = OB_TRANS_2PC_PREPARE_RESPONSE;
  PartitionLogInfoArray tmp_array;
  trans_time = -1;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      trans_2pc_prepare_response.init(TestObTransMsg::TENANT_ID,
          trans_id,
          msg_type,
          trans_time,
          sender,
          receiver,
          scheduler,
          coordinator,
          participants,
          parms,
          prepare_log_id,
          ObClockGenerator::getClock(),
          observer_,
          status,
          state,
          trans_version,
          request_id,
          tmp_array,
          0,
          trace_id,
          xid,
          is_xa_prepare));
  trans_time = 1;
  EXPECT_EQ(OB_SUCCESS,
      trans_2pc_prepare_response.init(TestObTransMsg::TENANT_ID,
          trans_id,
          msg_type,
          trans_time,
          sender,
          receiver,
          scheduler,
          coordinator,
          participants,
          parms,
          prepare_log_id,
          ObClockGenerator::getClock(),
          observer_,
          status,
          state,
          trans_version,
          request_id,
          tmp_array,
          0,
          trace_id,
          xid,
          is_xa_prepare));
  EXPECT_EQ(OB_INIT_TWICE,
      trans_2pc_prepare_response.init(TestObTransMsg::TENANT_ID,
          trans_id,
          msg_type,
          trans_time,
          sender,
          receiver,
          scheduler,
          coordinator,
          participants,
          parms,
          prepare_log_id,
          ObClockGenerator::getClock(),
          observer_,
          status,
          state,
          trans_version,
          request_id,
          tmp_array,
          0,
          trace_id,
          xid,
          is_xa_prepare));

  // OB_TRANS_2PC_COMMIT_REQUEST
  ObPartitionLogInfo partition_log_info(receiver, prepare_log_id, ObClockGenerator::getClock());
  PartitionLogInfoArray partition_log_info_arr;
  EXPECT_EQ(OB_SUCCESS, partition_log_info_arr.push_back(partition_log_info));

  msg_type = OB_TRANS_ABORT_REQUEST;
  ObTransMsg trans_2pc_commit_request;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      trans_2pc_commit_request.init(TestObTransMsg::TENANT_ID,
          trans_id,
          msg_type,
          trans_time,
          sender,
          receiver,
          scheduler,
          coordinator,
          participants,
          parms,
          trans_version,
          observer_,
          request_id,
          partition_log_info_arr,
          OB_SUCCESS));

  msg_type = OB_TRANS_2PC_COMMIT_REQUEST;
  EXPECT_EQ(OB_SUCCESS,
      trans_2pc_commit_request.init(TestObTransMsg::TENANT_ID,
          trans_id,
          msg_type,
          trans_time,
          sender,
          receiver,
          scheduler,
          coordinator,
          participants,
          parms,
          trans_version,
          observer_,
          request_id,
          partition_log_info_arr,
          OB_SUCCESS));
  EXPECT_EQ(OB_INIT_TWICE,
      trans_2pc_commit_request.init(TestObTransMsg::TENANT_ID,
          trans_id,
          msg_type,
          trans_time,
          sender,
          receiver,
          scheduler,
          coordinator,
          participants,
          parms,
          trans_version,
          observer_,
          request_id,
          partition_log_info_arr,
          OB_SUCCESS));
  EXPECT_EQ(true, trans_2pc_commit_request.is_valid());

  // OB_TRANS_2PC_COMMIT_RESPONSE
  ObTransMsg trans_2pc_commit_response;
  msg_type = OB_TRANS_2PC_COMMIT_RESPONSE;
  trans_time = -1;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      trans_2pc_commit_response.init(TestObTransMsg::TENANT_ID,
          trans_id,
          msg_type,
          trans_time,
          sender,
          receiver,
          scheduler,
          coordinator,
          participants,
          parms,
          trans_version,
          observer_,
          request_id,
          partition_log_info_arr,
          OB_SUCCESS));
  trans_time = 1;
  EXPECT_EQ(OB_SUCCESS,
      trans_2pc_commit_response.init(TestObTransMsg::TENANT_ID,
          trans_id,
          msg_type,
          trans_time,
          sender,
          receiver,
          scheduler,
          coordinator,
          participants,
          parms,
          trans_version,
          observer_,
          request_id,
          partition_log_info_arr,
          OB_SUCCESS));
  EXPECT_EQ(OB_INIT_TWICE,
      trans_2pc_commit_response.init(TestObTransMsg::TENANT_ID,
          trans_id,
          msg_type,
          trans_time,
          sender,
          receiver,
          scheduler,
          coordinator,
          participants,
          parms,
          trans_version,
          observer_,
          request_id,
          partition_log_info_arr,
          OB_SUCCESS));
  EXPECT_EQ(true, trans_2pc_commit_response.is_valid());
}
}  // namespace unittest
}  // namespace oceanbase

using namespace oceanbase;
using namespace oceanbase::common;

int main(int argc, char** argv)
{
  int ret = 1;
  ObLogger& logger = ObLogger::get_logger();
  logger.set_file_name("test_ob_trans_msg.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc, argv);
  ret = RUN_ALL_TESTS();
  return ret;
}
