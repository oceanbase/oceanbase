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

#include "storage/transaction/ob_trans_log.h"
#include "storage/transaction/ob_trans_ctx.h"
#include <gtest/gtest.h>
#include "share/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "common/ob_partition_key.h"

namespace oceanbase {
using namespace common;
using namespace transaction;
using namespace storage;
namespace unittest {
class TestObTransLog : public ::testing::Test {
public:
  virtual void SetUp()
  {}
  virtual void TearDown()
  {}

public:
  static const char* LOCAL_IP;
  static const int32_t PORT = 8080;
  static const ObAddr::VER IP_TYPE = ObAddr::IPV4;

  static const int64_t VALID_TABLE_ID = 1;
  static const int32_t VALID_PARTITION_ID = 1;
  static const int32_t VALID_PARTITION_COUNT = 100;
};
const char* TestObTransLog::LOCAL_IP = "127.0.0.1";
const int64_t TIME_OUT = 1;

class FakeObTransLog : public ObTransLog {
public:
  virtual int replace_tenant_id(const uint64_t tenant_id) override
  {
    (void)ObTransLog::inner_replace_tenant_id(tenant_id);
    return OB_SUCCESS;
  }
};

////////////////////test for ObTransLog///////////////////
// test the init of ObTransLog
TEST_F(TestObTransLog, trans_log_init_invalid)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  // invalid parameters
  ObTransID trans_id;
  ObPartitionKey partition_key;
  FakeObTransLog trans_log;
  int64_t log_type = OB_LOG_TRANS_REDO;
  const uint64_t cluster_id = 1000;
  EXPECT_EQ(OB_INVALID_ARGUMENT, trans_log.init(log_type, partition_key, trans_id, cluster_id));

  // create an object of ObTtransID
  ObAddr observer(TestObTransLog::IP_TYPE, TestObTransLog::LOCAL_IP, TestObTransLog::PORT);
  trans_id = ObTransID(observer);
  partition_key = ObPartitionKey(VALID_TABLE_ID, VALID_PARTITION_ID, VALID_PARTITION_COUNT);

  EXPECT_EQ(OB_SUCCESS, trans_log.init(log_type, partition_key, trans_id, cluster_id));
  EXPECT_TRUE(trans_log.is_valid());
}

// test the serialization and deserialization of ObTransLog
TEST_F(TestObTransLog, trans_log_encode_decode)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  int64_t log_type = OB_LOG_TRANS_REDO;
  FakeObTransLog trans_log;
  const uint64_t cluster_id = 1000;
  ObAddr observer(TestObTransLog::IP_TYPE, TestObTransLog::LOCAL_IP, TestObTransLog::PORT);
  ObTransID trans_id(observer);
  ObPartitionKey partition_key(VALID_TABLE_ID, VALID_PARTITION_ID, VALID_PARTITION_COUNT);
  ASSERT_EQ(OB_SUCCESS, trans_log.init(log_type, partition_key, trans_id, cluster_id));
  EXPECT_TRUE(trans_log.is_valid());

  // serialization of ObTransLog
  int64_t pos = 0;
  const int64_t BUFFER_SIZE = 10240;
  char buffer[BUFFER_SIZE];
  ASSERT_EQ(OB_SUCCESS, trans_log.serialize(buffer, BUFFER_SIZE, pos));

  // deserialization of ObTransLog
  FakeObTransLog trans_log1;
  int64_t start_index = 0;
  ASSERT_EQ(OB_SUCCESS, trans_log1.deserialize(buffer, pos, start_index));
  EXPECT_TRUE(trans_log1.is_valid());
  // test the consistency
  EXPECT_EQ(trans_log.get_log_type(), trans_log1.get_log_type());
  EXPECT_EQ(trans_log.get_trans_id(), trans_log1.get_trans_id());
  EXPECT_EQ(trans_log.get_partition(), trans_log1.get_partition());
  EXPECT_EQ(trans_log.get_cluster_id(), trans_log1.get_cluster_id());
}
// test the init of ObTransMutator
TEST_F(TestObTransLog, trans_mutator_init)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObTransMutator mutator;
  bool use_mutator_buf = true;
  void* log_buf = NULL;
  int64_t log_buffer_size = 0;

  // test the default values
  // use MutatorBuf to store data by default
  EXPECT_EQ(NULL, mutator.get_mutator_buf());
  EXPECT_TRUE(mutator.get_use_mutator_buf());
  EXPECT_EQ(NULL, mutator.get_data());
  EXPECT_EQ(0, mutator.get_capacity());
  EXPECT_EQ(0, mutator.get_position());

  EXPECT_EQ(OB_SUCCESS, mutator.init());
  ASSERT_TRUE(NULL != mutator.get_mutator_buf());
  EXPECT_TRUE(mutator.get_use_mutator_buf());
  EXPECT_EQ(mutator.get_mutator_buf()->get_buf(), mutator.get_data());
  EXPECT_EQ(mutator.get_mutator_buf()->get_size(), mutator.get_capacity());
  EXPECT_EQ(0, mutator.get_position());

  // test allocator
  // 1. test the max buffer length
  log_buffer_size = mutator.get_mutator_buf()->get_size();
  log_buf = mutator.alloc(log_buffer_size);
  EXPECT_TRUE(NULL != log_buf);
  EXPECT_EQ(log_buffer_size, mutator.get_position());
  mutator.free(log_buf);
  log_buf = NULL;
  log_buffer_size = 0;
  // 2. test the unexpected input
  log_buffer_size = mutator.get_mutator_buf()->get_size() + 1;
  log_buf = mutator.alloc(log_buffer_size);
  EXPECT_EQ(NULL, log_buf);
  EXPECT_EQ(0, mutator.get_position());

  // test reset
  mutator.reset();
  EXPECT_EQ(NULL, mutator.get_mutator_buf());
  EXPECT_TRUE(mutator.get_use_mutator_buf());
  EXPECT_EQ(NULL, mutator.get_data());
  EXPECT_EQ(0, mutator.get_capacity());
  EXPECT_EQ(0, mutator.get_position());

  // test use_mutator_buf=true
  use_mutator_buf = true;
  EXPECT_EQ(OB_SUCCESS, mutator.init(use_mutator_buf));
  ASSERT_TRUE(NULL != mutator.get_mutator_buf());
  EXPECT_TRUE(mutator.get_use_mutator_buf());
  EXPECT_EQ(mutator.get_mutator_buf()->get_buf(), mutator.get_data());
  EXPECT_EQ(mutator.get_mutator_buf()->get_size(), mutator.get_capacity());
  EXPECT_EQ(0, mutator.get_position());

  // test reset
  mutator.reset();
  EXPECT_EQ(NULL, mutator.get_mutator_buf());
  EXPECT_TRUE(mutator.get_use_mutator_buf());
  EXPECT_EQ(NULL, mutator.get_data());
  EXPECT_EQ(0, mutator.get_capacity());
  EXPECT_EQ(0, mutator.get_position());

  // test use_mutator_buf=false
  use_mutator_buf = false;
  EXPECT_EQ(OB_SUCCESS, mutator.init(use_mutator_buf));
  ASSERT_EQ(NULL, mutator.get_mutator_buf());
  EXPECT_FALSE(mutator.get_use_mutator_buf());
  EXPECT_EQ(NULL, mutator.get_data());
  EXPECT_EQ(0, mutator.get_capacity());
  EXPECT_EQ(0, mutator.get_position());
}

// test the serialization and deserialization of ObTransMutator
TEST_F(TestObTransLog, trans_mutator_serialization)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObTransMutator orig_mutator;
  bool use_mutator_buf = true;
  void* log_data = NULL;
  int64_t log_data_size = 0;
  char ser_buffer[common::OB_MAX_LOG_ALLOWED_SIZE];
  int64_t ser_buffer_size = common::OB_MAX_LOG_ALLOWED_SIZE;
  int64_t ser_data_size = 0;

  // 1. init Mutator
  EXPECT_EQ(OB_SUCCESS, orig_mutator.init());
  ASSERT_TRUE(NULL != orig_mutator.get_mutator_buf());
  EXPECT_EQ(orig_mutator.get_mutator_buf()->get_buf(), orig_mutator.get_data());
  EXPECT_EQ(orig_mutator.get_mutator_buf()->get_size(), orig_mutator.get_capacity());
  EXPECT_EQ(0, orig_mutator.get_position());

  // 2. alloc Buffer and init the log
  log_data_size = 1 << 20;
  log_data = orig_mutator.alloc(log_data_size);
  EXPECT_TRUE(NULL != log_data);
  EXPECT_EQ(log_data_size, orig_mutator.get_position());
  (void)memset(log_data, 'a', log_data_size);

  // 3. serialize Mutator
  ser_data_size = 0;
  ASSERT_EQ(OB_SUCCESS, orig_mutator.serialize(ser_buffer, ser_buffer_size, ser_data_size));

  // 4. create a new Mutator
  ObTransMutator mutator_use_mb;
  use_mutator_buf = true;
  EXPECT_EQ(OB_SUCCESS, mutator_use_mb.init(use_mutator_buf));
  ASSERT_TRUE(NULL != mutator_use_mb.get_mutator_buf());
  EXPECT_EQ(mutator_use_mb.get_mutator_buf()->get_buf(), mutator_use_mb.get_data());
  EXPECT_EQ(mutator_use_mb.get_mutator_buf()->get_size(), mutator_use_mb.get_capacity());
  EXPECT_EQ(0, mutator_use_mb.get_position());

  // 5. deserialize log
  int64_t pos_use_mb = 0;
  EXPECT_EQ(OB_SUCCESS, mutator_use_mb.deserialize(ser_buffer, ser_data_size, pos_use_mb));
  EXPECT_EQ(ser_data_size, pos_use_mb);
  EXPECT_EQ(log_data_size, mutator_use_mb.get_position());
  EXPECT_EQ(0, memcmp(mutator_use_mb.get_data(), log_data, log_data_size));

  // 6. create a new Mutator without using MutatorBuf
  ObTransMutator mutator_no_use_mb;
  use_mutator_buf = false;
  EXPECT_EQ(OB_SUCCESS, mutator_no_use_mb.init(use_mutator_buf));
  ASSERT_EQ(NULL, mutator_no_use_mb.get_mutator_buf());
  EXPECT_FALSE(mutator_no_use_mb.get_use_mutator_buf());
  EXPECT_EQ(NULL, mutator_no_use_mb.get_data());
  EXPECT_EQ(0, mutator_no_use_mb.get_capacity());
  EXPECT_EQ(0, mutator_no_use_mb.get_position());

  // 7. deserialize log
  int64_t pos_no_use_mb = 0;

  EXPECT_EQ(OB_SUCCESS, mutator_no_use_mb.deserialize(ser_buffer, ser_data_size, pos_no_use_mb));
  EXPECT_EQ(ser_data_size, pos_no_use_mb);
  EXPECT_TRUE(NULL != mutator_no_use_mb.get_data());
  EXPECT_EQ(log_data_size, mutator_no_use_mb.get_position());
  EXPECT_EQ(0, memcmp(mutator_no_use_mb.get_data(), log_data, log_data_size));
}

//////////////////////test for ObTransRedoLog////////////////////
// test the init ObTransRedoLog
TEST_F(TestObTransLog, redo_log_init_invalid)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  // invalid parameters
  int64_t log_type = -1;
  ObPartitionKey partition_key;
  ObAddr scheduler;
  ObPartitionKey coordinator;
  ObPartitionArray participants;
  ObTransID trans_id;
  const uint64_t tenant_id = 100;
  const int64_t log_no = 1;
  const uint64_t cluster_id = 1000;
  ObStartTransParam parms;
  ObElrTransInfoArray arr;
  const bool can_elr = false;
  ObXATransID xid;
  const bool is_last = false;
  ObTransRedoLog redo_log(log_type,
      partition_key,
      trans_id,
      tenant_id,
      log_no,
      scheduler,
      coordinator,
      participants,
      parms,
      cluster_id,
      arr,
      can_elr,
      xid,
      is_last);
  EXPECT_FALSE(redo_log.is_valid());

  parms.set_access_mode(ObTransAccessMode::READ_ONLY);
  parms.set_type(ObTransType::TRANS_USER);
  parms.set_isolation(ObTransIsolation::READ_COMMITED);

  // create an object of ObTransID
  ObAddr observer(TestObTransLog::IP_TYPE, TestObTransLog::LOCAL_IP, TestObTransLog::PORT);
  trans_id = ObTransID(observer);
  partition_key = ObPartitionKey(VALID_TABLE_ID, VALID_PARTITION_ID, VALID_PARTITION_COUNT);

  log_type = OB_LOG_TRANS_REDO;
  coordinator = partition_key;
  participants.push_back(partition_key);
  scheduler = observer;

  ObTransRedoLog redo_log2(log_type,
      partition_key,
      trans_id,
      tenant_id,
      log_no,
      scheduler,
      coordinator,
      participants,
      parms,
      cluster_id,
      arr,
      can_elr,
      xid,
      is_last);
  EXPECT_TRUE(redo_log2.is_valid());
}
// test the serialization and deserialization of ObTransRedoLog
TEST_F(TestObTransLog, trans_redo_log_encode_decode)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  int64_t log_type = OB_LOG_TRANS_REDO;
  ObPartitionKey partition_key(VALID_TABLE_ID, VALID_PARTITION_ID, VALID_PARTITION_COUNT);
  ObAddr observer(TestObTransLog::IP_TYPE, TestObTransLog::LOCAL_IP, TestObTransLog::PORT);
  ObTransID trans_id(observer);
  const int64_t log_no = 1;
  const uint64_t tenant_id = 100;
  ObAddr scheduler;
  ObPartitionKey coordinator;
  ObPartitionArray participants;
  ObStartTransParam parms;
  parms.set_access_mode(ObTransAccessMode::READ_ONLY);
  parms.set_type(ObTransType::TRANS_USER);
  parms.set_isolation(ObTransIsolation::READ_COMMITED);
  coordinator = partition_key;
  participants.push_back(partition_key);
  scheduler = observer;
  const uint64_t cluster_id = 1000;
  ObElrTransInfoArray arr;
  const bool can_elr = false;
  ObXATransID xid;
  const bool is_last = false;

  ObTransRedoLog redo_log(log_type,
      partition_key,
      trans_id,
      tenant_id,
      log_no,
      scheduler,
      coordinator,
      participants,
      parms,
      cluster_id,
      arr,
      can_elr,
      xid,
      is_last);
  ASSERT_TRUE(redo_log.is_valid());

  // serialization of ObTransRedoLog
  int64_t pos = 0;
  const int64_t BUFFER_SIZE = 10240;
  char buffer[BUFFER_SIZE];
  ASSERT_EQ(OB_SUCCESS, redo_log.serialize(buffer, BUFFER_SIZE, pos));

  // deserialization of ObTransRedoLog
  ObTransRedoLogHelper redo_helper1;
  ObTransRedoLog redo_log1(redo_helper1);
  redo_log1.get_mutator().init();
  int64_t start_index = 0;
  ASSERT_EQ(OB_SUCCESS, redo_log1.deserialize(buffer, pos, start_index));
  EXPECT_TRUE(redo_log1.is_valid());

  // test the consistency
  EXPECT_EQ(redo_log.get_log_type(), redo_log1.get_log_type());
  EXPECT_EQ(redo_log.get_trans_id(), redo_log1.get_trans_id());
  EXPECT_EQ(redo_log.get_partition(), redo_log1.get_partition());
  EXPECT_EQ(redo_log.get_log_no(), redo_log1.get_log_no());
  EXPECT_EQ(redo_log.get_cluster_id(), redo_log1.get_cluster_id());
  EXPECT_EQ(redo_log.is_last(), redo_log1.is_last());
  EXPECT_EQ(redo_log.get_xid(), redo_log1.get_xid());

  // check ObStartTransParam
  EXPECT_EQ(parms.get_access_mode(), redo_log1.get_trans_param().get_access_mode());
  EXPECT_EQ(parms.get_type(), redo_log1.get_trans_param().get_type());
  EXPECT_EQ(parms.get_isolation(), redo_log1.get_trans_param().get_isolation());
}

//////////////////////test for ObTransPrepareLog////////////////////
// test the init of ObTransPrepareLog
TEST_F(TestObTransLog, prepare_log_init_invalid)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  // invalid parameters
  int64_t log_type = -1;
  const uint64_t tenant_id = 100;
  ObPartitionKey partition_key;
  ObTransID trans_id;
  ObAddr scheduler;
  ObPartitionKey coordinator;
  ObPartitionArray participants;

  ObRedoLogIdArray redo_log_ids;
  EXPECT_EQ(OB_SUCCESS, redo_log_ids.push_back(1));
  EXPECT_EQ(OB_SUCCESS, redo_log_ids.push_back(2));
  EXPECT_EQ(OB_SUCCESS, redo_log_ids.push_back(3));

  ObStartTransParam trans_param;
  trans_param.set_access_mode(ObTransAccessMode::READ_WRITE);
  trans_param.set_type(ObTransType::TRANS_USER);
  trans_param.set_isolation(ObTransIsolation::READ_COMMITED);
  const int prepare_status = true;
  const uint64_t cluster_id = 1000;
  PartitionLogInfoArray partition_log_info;
  const int64_t checkpoint = 1000;

  ObString trace_id = "trance_id=xxx";
  ObElrTransInfoArray arr;
  const bool can_elr = false;
  ObXATransID xid;
  ObTransPrepareLog prepare_log(log_type,
      partition_key,
      trans_id,
      tenant_id,
      scheduler,
      coordinator,
      participants,
      trans_param,
      prepare_status,
      redo_log_ids,
      cluster_id,
      trace_id,
      partition_log_info,
      checkpoint,
      arr,
      can_elr,
      trace_id,
      xid);
  EXPECT_FALSE(prepare_log.is_valid());

  // create an object of ObTransID
  ObAddr observer(TestObTransLog::IP_TYPE, TestObTransLog::LOCAL_IP, TestObTransLog::PORT);
  trans_id = ObTransID(observer);
  partition_key = ObPartitionKey(VALID_TABLE_ID, VALID_PARTITION_ID, VALID_PARTITION_COUNT);
  coordinator = partition_key;
  participants.push_back(partition_key);
  scheduler = observer;
  ObTransPrepareLog prepare_log1(log_type,
      partition_key,
      trans_id,
      tenant_id,
      scheduler,
      coordinator,
      participants,
      trans_param,
      prepare_status,
      redo_log_ids,
      cluster_id,
      trace_id,
      partition_log_info,
      checkpoint,
      arr,
      can_elr,
      trace_id,
      xid);
  EXPECT_FALSE(prepare_log1.is_valid());

  log_type = OB_LOG_TRANS_PREPARE;
  ObTransPrepareLog prepare_log2(log_type,
      partition_key,
      trans_id,
      tenant_id,
      scheduler,
      coordinator,
      participants,
      trans_param,
      prepare_status,
      redo_log_ids,
      cluster_id,
      trace_id,
      partition_log_info,
      checkpoint,
      arr,
      can_elr,
      trace_id,
      xid);
  EXPECT_TRUE(prepare_log2.is_valid());
}
// test the serialization and deserialization of ObTransPrepareLog
TEST_F(TestObTransLog, trans_prepare_log_encode_decode)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  int64_t log_type = OB_LOG_TRANS_PREPARE;
  const uint64_t cluster_id = 1000;
  ObPartitionKey partition_key(VALID_TABLE_ID, VALID_PARTITION_ID, VALID_PARTITION_COUNT);
  ObAddr observer(TestObTransLog::IP_TYPE, TestObTransLog::LOCAL_IP, TestObTransLog::PORT);
  ObTransID trans_id(observer);
  ObAddr& scheduler = observer;
  ObPartitionKey& coordinator = partition_key;
  ObPartitionArray participants;
  PartitionLogInfoArray partition_log_info;
  const int64_t checkpoint = 1000;
  participants.push_back(partition_key);

  ObRedoLogIdArray redo_log_ids;
  EXPECT_EQ(OB_SUCCESS, redo_log_ids.push_back(1));
  EXPECT_EQ(OB_SUCCESS, redo_log_ids.push_back(2));
  EXPECT_EQ(OB_SUCCESS, redo_log_ids.push_back(3));

  ObStartTransParam trans_param;
  trans_param.set_access_mode(ObTransAccessMode::READ_WRITE);
  trans_param.set_type(ObTransType::TRANS_USER);
  trans_param.set_isolation(ObTransIsolation::READ_COMMITED);

  const int prepare_status = true;
  const uint64_t tenant_id = 100;
  ObString trace_id = "trace_id=xxx";
  ObElrTransInfoArray arr;
  const bool can_elr = false;
  ObXATransID xid;
  ObTransPrepareLog prepare_log(log_type,
      partition_key,
      trans_id,
      tenant_id,
      scheduler,
      coordinator,
      participants,
      trans_param,
      prepare_status,
      redo_log_ids,
      cluster_id,
      trace_id,
      partition_log_info,
      checkpoint,
      arr,
      can_elr,
      trace_id,
      xid);
  EXPECT_TRUE(prepare_log.is_valid());

  // serialization of ObTransPrepareLog
  int64_t pos = 0;
  const int64_t BUFFER_SIZE = 10240;
  char buffer[BUFFER_SIZE];
  ASSERT_EQ(OB_SUCCESS, prepare_log.serialize(buffer, BUFFER_SIZE, pos));

  // deserialization of ObTransPrepareLog
  ObTransPrepareLogHelper prepare_helper1;
  ObTransPrepareLog prepare_log1(prepare_helper1);
  int64_t start_index = 0;
  ASSERT_EQ(OB_SUCCESS, prepare_log1.deserialize(buffer, pos, start_index));
  EXPECT_TRUE(prepare_log1.is_valid());

  // test the consistency
  EXPECT_EQ(prepare_log.get_log_type(), prepare_log1.get_log_type());
  EXPECT_EQ(prepare_log.get_trans_id(), prepare_log1.get_trans_id());
  EXPECT_EQ(prepare_log.get_partition(), prepare_log1.get_partition());
  EXPECT_EQ(prepare_log.get_scheduler(), prepare_log1.get_scheduler());
  EXPECT_EQ(prepare_log.get_coordinator(), prepare_log1.get_coordinator());
  EXPECT_EQ(prepare_log.get_prepare_status(), prepare_log1.get_prepare_status());
  EXPECT_EQ(prepare_log.get_cluster_id(), prepare_log1.get_cluster_id());

  EXPECT_EQ(prepare_log.get_redo_log_ids().count(), prepare_log1.get_redo_log_ids().count());
  ObRedoLogIdArray redo_logid_arr = prepare_log1.get_redo_log_ids();
  ASSERT_EQ(3, redo_logid_arr.count());
  EXPECT_EQ(1, redo_logid_arr[0]);
  EXPECT_EQ(2, redo_logid_arr[1]);
  EXPECT_EQ(3, redo_logid_arr[2]);

  // check ObStartTransParam
  EXPECT_EQ(trans_param.get_access_mode(), prepare_log1.get_trans_param().get_access_mode());
  EXPECT_EQ(trans_param.get_type(), prepare_log1.get_trans_param().get_type());
  EXPECT_EQ(trans_param.get_isolation(), prepare_log1.get_trans_param().get_isolation());
}
/////////////////test for TransCommitLog///////////////////
// test the init of ObTranscommitLog
TEST_F(TestObTransLog, commit_log_init_invalid)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  // invalid parameters
  int64_t log_type = -1;
  ObPartitionKey partition_key;
  ObTransID trans_id;
  const int64_t global_trans_version = 1000;
  const uint64_t cluster_id = 1000;

  PartitionLogInfoArray array;
  ObTransCommitLog commit_log(log_type, partition_key, trans_id, array, global_trans_version, 0, cluster_id);
  EXPECT_FALSE(commit_log.is_valid());

  // create an object of ObTransID
  ObAddr observer(TestObTransLog::IP_TYPE, TestObTransLog::LOCAL_IP, TestObTransLog::PORT);
  trans_id = ObTransID(observer);
  partition_key = ObPartitionKey(VALID_TABLE_ID, VALID_PARTITION_ID, VALID_PARTITION_COUNT);
  ObTransCommitLog commit_log2(log_type, partition_key, trans_id, array, global_trans_version, 0, cluster_id);
  EXPECT_FALSE(commit_log2.is_valid());

  log_type = OB_LOG_TRANS_COMMIT;
  ObTransCommitLog commit_log3(log_type, partition_key, trans_id, array, global_trans_version, 0, cluster_id);
  EXPECT_TRUE(commit_log3.is_valid());
}
// test the serialization and deserialization of ObTransCommitLog
TEST_F(TestObTransLog, trans_commit_log_encode_decode)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  int64_t log_type = OB_LOG_TRANS_COMMIT;
  const uint64_t cluster_id = 1000;
  const int64_t global_trans_version = 1000;
  ObPartitionKey partition_key(VALID_TABLE_ID, VALID_PARTITION_ID, VALID_PARTITION_COUNT);
  ObAddr observer(TestObTransLog::IP_TYPE, TestObTransLog::LOCAL_IP, TestObTransLog::PORT);
  ObTransID trans_id(observer);
  PartitionLogInfoArray array;
  ObTransCommitLog commit_log(log_type, partition_key, trans_id, array, global_trans_version, 0, cluster_id);
  ASSERT_TRUE(commit_log.is_valid());

  // serialization of ObTransCommitLog
  int64_t pos = 0;
  const int64_t BUFFER_SIZE = 10240;
  char buffer[BUFFER_SIZE];
  ASSERT_EQ(OB_SUCCESS, commit_log.serialize(buffer, BUFFER_SIZE, pos));

  // deserialization of ObTransCommitLog
  PartitionLogInfoArray array1;
  ObTransCommitLog commit_log1(array1);
  int64_t start_index = 0;
  ASSERT_EQ(OB_SUCCESS, commit_log1.deserialize(buffer, pos, start_index));
  EXPECT_TRUE(commit_log1.is_valid());

  // test the consistency
  EXPECT_EQ(commit_log.get_log_type(), commit_log1.get_log_type());
  EXPECT_EQ(commit_log.get_trans_id(), commit_log1.get_trans_id());
  EXPECT_EQ(commit_log.get_partition(), commit_log1.get_partition());
  EXPECT_EQ(commit_log.get_cluster_id(), commit_log1.get_cluster_id());
}

/////////////////////test for ObTransAbortLog///////////////////////
// test the init of ObTransAbortLog
TEST_F(TestObTransLog, abort_log_init_invalid)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  // invalid parameters
  int64_t log_type = -1;
  ObPartitionKey partition_key;
  ObTransID trans_id;
  const uint64_t cluster_id = 1000;
  PartitionLogInfoArray array;

  ObTransAbortLog abort_log;
  EXPECT_EQ(OB_INVALID_ARGUMENT, abort_log.init(log_type, partition_key, trans_id, array, cluster_id));

  // create an object of ObTransID
  ObAddr observer(TestObTransLog::IP_TYPE, TestObTransLog::LOCAL_IP, TestObTransLog::PORT);
  trans_id = ObTransID(observer);
  partition_key = ObPartitionKey(VALID_TABLE_ID, VALID_PARTITION_ID, VALID_PARTITION_COUNT);
  EXPECT_EQ(OB_INVALID_ARGUMENT, abort_log.init(log_type, partition_key, trans_id, array, cluster_id));
  log_type = OB_LOG_TRANS_ABORT;
  EXPECT_EQ(OB_SUCCESS, abort_log.init(log_type, partition_key, trans_id, array, cluster_id));

  EXPECT_TRUE(abort_log.is_valid());
}
// test the serialization and deserialization of ObTransAbortlog
TEST_F(TestObTransLog, trans_abort_log_encode_decode)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  int64_t log_type = OB_LOG_TRANS_ABORT;
  const uint64_t cluster_id = 1000;
  PartitionLogInfoArray array;
  ObPartitionKey partition_key(VALID_TABLE_ID, VALID_PARTITION_ID, VALID_PARTITION_COUNT);
  ObAddr observer(TestObTransLog::IP_TYPE, TestObTransLog::LOCAL_IP, TestObTransLog::PORT);
  ObTransID trans_id(observer);
  ObTransAbortLog abort_log;
  ASSERT_EQ(OB_SUCCESS, abort_log.init(log_type, partition_key, trans_id, array, cluster_id));
  EXPECT_TRUE(abort_log.is_valid());

  // serialization of ObTransAbortLog
  int64_t pos = 0;
  const int64_t BUFFER_SIZE = 10240;
  char buffer[BUFFER_SIZE];
  ASSERT_EQ(OB_SUCCESS, abort_log.serialize(buffer, BUFFER_SIZE, pos));

  // deserialization of ObTransAbortLog
  ObTransAbortLog abort_log1;
  int64_t start_index = 0;
  ASSERT_EQ(OB_SUCCESS, abort_log1.deserialize(buffer, pos, start_index));
  EXPECT_TRUE(abort_log1.is_valid());

  // test the consistency
  EXPECT_EQ(abort_log.get_log_type(), abort_log1.get_log_type());
  EXPECT_EQ(abort_log.get_trans_id(), abort_log1.get_trans_id());
  EXPECT_EQ(abort_log.get_partition(), abort_log1.get_partition());
  EXPECT_EQ(abort_log.get_cluster_id(), abort_log1.get_cluster_id());
}
//////////////////////test for ObTransClearLog/////////////////////////////
// test the init ObTransClearLog
TEST_F(TestObTransLog, clear_log_init_invalid)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  // invalid parameters
  int64_t log_type = -1;
  ObPartitionKey partition_key;
  ObTransID trans_id;
  const uint64_t cluster_id = 1000;

  ObTransClearLog clear_log;
  EXPECT_EQ(OB_INVALID_ARGUMENT, clear_log.init(log_type, partition_key, trans_id, cluster_id));

  // create an object of ObTransID
  ObAddr observer(TestObTransLog::IP_TYPE, TestObTransLog::LOCAL_IP, TestObTransLog::PORT);
  trans_id = ObTransID(observer);
  partition_key = ObPartitionKey(VALID_TABLE_ID, VALID_PARTITION_ID, VALID_PARTITION_COUNT);
  TRANS_LOG(INFO, "arguments info", K(log_type), K(partition_key), K(trans_id));
  EXPECT_EQ(OB_INVALID_ARGUMENT, clear_log.init(log_type, partition_key, trans_id, cluster_id));

  log_type = OB_LOG_TRANS_CLEAR;
  EXPECT_EQ(OB_SUCCESS, clear_log.init(log_type, partition_key, trans_id, cluster_id));

  EXPECT_TRUE(clear_log.is_valid());
}

// test the serialization and deserialization of ObTransClearlog
TEST_F(TestObTransLog, trans_clear_log_encode_decode)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  int64_t log_type = OB_LOG_TRANS_CLEAR;
  ObPartitionKey partition_key(VALID_TABLE_ID, VALID_PARTITION_ID, VALID_PARTITION_COUNT);
  ObAddr observer(TestObTransLog::IP_TYPE, TestObTransLog::LOCAL_IP, TestObTransLog::PORT);
  ObTransID trans_id(observer);
  ObTransClearLog clear_log;
  const uint64_t cluster_id = 1000;
  ASSERT_EQ(OB_SUCCESS, clear_log.init(log_type, partition_key, trans_id, cluster_id));
  ASSERT_TRUE(clear_log.is_valid());

  // serialization of ObTransClearLog
  int64_t pos = 0;
  const int64_t BUFFER_SIZE = 10240;
  char buffer[BUFFER_SIZE];
  EXPECT_EQ(OB_SUCCESS, clear_log.serialize(buffer, BUFFER_SIZE, pos));

  // deserialization of ObTransClearLog
  ObTransClearLog clear_log1;
  int64_t start_index = 0;
  EXPECT_EQ(OB_SUCCESS, clear_log1.deserialize(buffer, pos, start_index));
  EXPECT_TRUE(clear_log1.is_valid());

  // test the consistency
  EXPECT_EQ(clear_log.get_log_type(), clear_log1.get_log_type());
  EXPECT_EQ(clear_log.get_trans_id(), clear_log1.get_trans_id());
  EXPECT_EQ(clear_log.get_partition(), clear_log1.get_partition());
  EXPECT_EQ(clear_log.get_cluster_id(), clear_log1.get_cluster_id());
}

TEST_F(TestObTransLog, sp_redo_log_init_invalid)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  // invalid parameters
  int64_t log_type = -1;
  ObPartitionKey partition_key;
  ObTransID trans_id;
  const uint64_t tenant_id = 100;
  const int64_t log_no = 1;
  const uint64_t cluster_id = 1000;
  ObStartTransParam parms;
  ObElrTransInfoArray arr;
  const bool can_elr = false;
  ObSpTransRedoLog redo_log;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      redo_log.init(log_type, partition_key, trans_id, tenant_id, log_no, parms, cluster_id, arr, can_elr));

  parms.set_access_mode(ObTransAccessMode::READ_ONLY);
  parms.set_type(ObTransType::TRANS_USER);
  parms.set_isolation(ObTransIsolation::READ_COMMITED);

  // create an object of ObTtransID
  ObAddr observer(TestObTransLog::IP_TYPE, TestObTransLog::LOCAL_IP, TestObTransLog::PORT);
  trans_id = ObTransID(observer);
  partition_key = ObPartitionKey(VALID_TABLE_ID, VALID_PARTITION_ID, VALID_PARTITION_COUNT);

  log_type = OB_LOG_SP_TRANS_REDO;
  EXPECT_EQ(
      OB_SUCCESS, redo_log.init(log_type, partition_key, trans_id, tenant_id, log_no, parms, cluster_id, arr, can_elr));
  EXPECT_TRUE(redo_log.is_valid());
}

// test the serialization and deserialization of ObSpTransRedoLog
TEST_F(TestObTransLog, sp_trans_redo_log_encode_decode)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  int64_t log_type = OB_LOG_SP_TRANS_REDO;
  ObPartitionKey partition_key(VALID_TABLE_ID, VALID_PARTITION_ID, VALID_PARTITION_COUNT);
  ObAddr observer(TestObTransLog::IP_TYPE, TestObTransLog::LOCAL_IP, TestObTransLog::PORT);
  ObTransID trans_id(observer);
  const int64_t log_no = 1;
  const uint64_t tenant_id = 100;
  ObStartTransParam parms;
  parms.set_access_mode(ObTransAccessMode::READ_ONLY);
  parms.set_type(ObTransType::TRANS_USER);
  parms.set_isolation(ObTransIsolation::READ_COMMITED);
  const uint64_t cluster_id = 1000;

  ObElrTransInfoArray arr;
  const bool can_elr = false;
  ObSpTransRedoLog redo_log;
  ASSERT_EQ(
      OB_SUCCESS, redo_log.init(log_type, partition_key, trans_id, tenant_id, log_no, parms, cluster_id, arr, can_elr));
  ASSERT_TRUE(redo_log.is_valid());

  // serialization of ObSpTransRedoLog
  int64_t pos = 0;
  const int64_t BUFFER_SIZE = 10240;
  char buffer[BUFFER_SIZE];
  ASSERT_EQ(OB_SUCCESS, redo_log.serialize(buffer, BUFFER_SIZE, pos));

  // deserialization of ObSpTransRedoLog
  ObSpTransRedoLog redo_log1;
  redo_log1.get_mutator().init();
  int64_t start_index = 0;
  ASSERT_EQ(OB_SUCCESS, redo_log1.deserialize(buffer, pos, start_index));
  EXPECT_TRUE(redo_log1.is_valid());

  // test the consistency
  EXPECT_EQ(redo_log.get_log_type(), redo_log1.get_log_type());
  EXPECT_EQ(redo_log.get_trans_id(), redo_log1.get_trans_id());
  EXPECT_EQ(redo_log.get_partition(), redo_log1.get_partition());
  EXPECT_EQ(redo_log.get_log_no(), redo_log1.get_log_no());
  EXPECT_EQ(redo_log.get_cluster_id(), redo_log1.get_cluster_id());

  // check ObStartTransParam
  EXPECT_EQ(parms.get_access_mode(), redo_log1.get_trans_param().get_access_mode());
  EXPECT_EQ(parms.get_type(), redo_log1.get_trans_param().get_type());
  EXPECT_EQ(parms.get_isolation(), redo_log1.get_trans_param().get_isolation());
}

// test the init of ObSpTransCommitLog
TEST_F(TestObTransLog, sp_commit_log_init_invalid)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  // invalid parameters
  int64_t log_type = -1;
  ObPartitionKey partition_key;
  ObTransID trans_id;
  const uint64_t cluster_id = 1000;
  ObRedoLogIdArray redo_log_ids;
  EXPECT_EQ(OB_SUCCESS, redo_log_ids.push_back(1));
  EXPECT_EQ(OB_SUCCESS, redo_log_ids.push_back(2));
  EXPECT_EQ(OB_SUCCESS, redo_log_ids.push_back(3));
  const uint64_t tenant_id = 100;
  const ObString trace_id = "trace_id=xxx";
  ObStartTransParam parms;
  parms.set_access_mode(ObTransAccessMode::READ_ONLY);
  parms.set_type(ObTransType::TRANS_USER);
  parms.set_isolation(ObTransIsolation::READ_COMMITED);

  ObElrTransInfoArray arr;
  const bool can_elr = false;
  ObSpTransCommitLog commit_log;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      commit_log.init(log_type,
          partition_key,
          tenant_id,
          trans_id,
          0,
          cluster_id,
          redo_log_ids,
          parms,
          0,
          trace_id,
          100,
          arr,
          can_elr,
          trace_id));

  // create an object of ObTransID
  ObAddr observer(TestObTransLog::IP_TYPE, TestObTransLog::LOCAL_IP, TestObTransLog::PORT);
  trans_id = ObTransID(observer);
  partition_key = ObPartitionKey(VALID_TABLE_ID, VALID_PARTITION_ID, VALID_PARTITION_COUNT);
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      commit_log.init(log_type,
          partition_key,
          tenant_id,
          trans_id,
          0,
          cluster_id,
          redo_log_ids,
          parms,
          0,
          trace_id,
          100,
          arr,
          can_elr,
          trace_id));

  log_type = OB_LOG_SP_TRANS_COMMIT;
  EXPECT_EQ(OB_SUCCESS,
      commit_log.init(log_type,
          partition_key,
          tenant_id,
          trans_id,
          0,
          cluster_id,
          redo_log_ids,
          parms,
          0,
          trace_id,
          100,
          arr,
          can_elr,
          trace_id));

  EXPECT_TRUE(commit_log.is_valid());
}

// test the serialization and deserialization of ObSpTransCommitLog
TEST_F(TestObTransLog, sp_trans_commit_log_encode_decode)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  int64_t log_type = OB_LOG_SP_TRANS_COMMIT;
  const uint64_t cluster_id = 1000;
  ObRedoLogIdArray redo_log_ids;
  EXPECT_EQ(OB_SUCCESS, redo_log_ids.push_back(1));
  EXPECT_EQ(OB_SUCCESS, redo_log_ids.push_back(2));
  EXPECT_EQ(OB_SUCCESS, redo_log_ids.push_back(3));
  ObPartitionKey partition_key(VALID_TABLE_ID, VALID_PARTITION_ID, VALID_PARTITION_COUNT);
  ObAddr observer(TestObTransLog::IP_TYPE, TestObTransLog::LOCAL_IP, TestObTransLog::PORT);
  ObTransID trans_id(observer);
  const uint64_t tenant_id = 100;
  ObStartTransParam parms;
  parms.set_access_mode(ObTransAccessMode::READ_ONLY);
  parms.set_type(ObTransType::TRANS_USER);
  parms.set_isolation(ObTransIsolation::READ_COMMITED);
  const ObString trace_id = "trace_id=xxx";
  ObElrTransInfoArray arr;
  const bool can_elr = false;

  ObSpTransCommitLog commit_log;
  ASSERT_EQ(OB_SUCCESS,
      commit_log.init(log_type,
          partition_key,
          tenant_id,
          trans_id,
          0,
          cluster_id,
          redo_log_ids,
          parms,
          0,
          trace_id,
          100,
          arr,
          can_elr,
          trace_id));
  ASSERT_TRUE(commit_log.is_valid());

  // serialization of ObSpTransCommitLog
  int64_t pos = 0;
  const int64_t BUFFER_SIZE = 10240;
  char buffer[BUFFER_SIZE];
  ASSERT_EQ(OB_SUCCESS, commit_log.serialize(buffer, BUFFER_SIZE, pos));

  // deserialization of ObSpTransCommitLog
  ObSpTransCommitLog commit_log1;
  commit_log1.get_mutator().init();
  commit_log1.get_mutator().reset();
  int64_t start_index = 0;
  ASSERT_EQ(OB_NOT_INIT, commit_log1.deserialize(buffer, pos, start_index));
  EXPECT_TRUE(commit_log1.is_valid());
}

// test the init of ObSpTransAbortLog
TEST_F(TestObTransLog, sp_abort_log_init_invalid)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  // invalid parameters
  int64_t log_type = -1;
  ObPartitionKey partition_key;
  ObTransID trans_id;
  const uint64_t cluster_id = 1000;

  ObSpTransAbortLog abort_log;
  EXPECT_EQ(OB_INVALID_ARGUMENT, abort_log.init(log_type, partition_key, trans_id, cluster_id));

  // create an object of ObTtransID
  ObAddr observer(TestObTransLog::IP_TYPE, TestObTransLog::LOCAL_IP, TestObTransLog::PORT);
  trans_id = ObTransID(observer);
  partition_key = ObPartitionKey(VALID_TABLE_ID, VALID_PARTITION_ID, VALID_PARTITION_COUNT);
  EXPECT_EQ(OB_INVALID_ARGUMENT, abort_log.init(log_type, partition_key, trans_id, cluster_id));
  log_type = OB_LOG_SP_TRANS_ABORT;
  EXPECT_EQ(OB_SUCCESS, abort_log.init(log_type, partition_key, trans_id, cluster_id));

  EXPECT_TRUE(abort_log.is_valid());
}
// test the serialization and deserialization of ObSpTransAbortlog
TEST_F(TestObTransLog, sp_trans_abort_log_encode_decode)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  int64_t log_type = OB_LOG_SP_TRANS_ABORT;
  const uint64_t cluster_id = 1000;
  ObPartitionKey partition_key(VALID_TABLE_ID, VALID_PARTITION_ID, VALID_PARTITION_COUNT);
  ObAddr observer(TestObTransLog::IP_TYPE, TestObTransLog::LOCAL_IP, TestObTransLog::PORT);
  ObTransID trans_id(observer);
  ObSpTransAbortLog abort_log;
  ASSERT_EQ(OB_SUCCESS, abort_log.init(log_type, partition_key, trans_id, cluster_id));
  EXPECT_TRUE(abort_log.is_valid());

  // serialization of ObSpTransAbortLog
  int64_t pos = 0;
  const int64_t BUFFER_SIZE = 10240;
  char buffer[BUFFER_SIZE];
  ASSERT_EQ(OB_SUCCESS, abort_log.serialize(buffer, BUFFER_SIZE, pos));

  // deserialization of ObSpTransAbortLog
  ObSpTransAbortLog abort_log1;
  int64_t start_index = 0;
  ASSERT_EQ(OB_SUCCESS, abort_log1.deserialize(buffer, pos, start_index));
  EXPECT_TRUE(abort_log1.is_valid());

  // test the consistency
  EXPECT_EQ(abort_log.get_log_type(), abort_log1.get_log_type());
  EXPECT_EQ(abort_log.get_trans_id(), abort_log1.get_trans_id());
  EXPECT_EQ(abort_log.get_partition(), abort_log1.get_partition());
  EXPECT_EQ(abort_log.get_cluster_id(), abort_log1.get_cluster_id());
}
}  // namespace unittest
}  // namespace oceanbase

using namespace oceanbase;
using namespace oceanbase::common;

int main(int argc, char** argv)
{
  int ret = 1;
  ObLogger& logger = ObLogger::get_logger();
  logger.set_file_name("test_ob_trans_log.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc, argv);
  ret = RUN_ALL_TESTS();
  return ret;
}
