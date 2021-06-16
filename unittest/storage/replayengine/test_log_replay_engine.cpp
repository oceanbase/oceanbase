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

#include "share/ob_define.h"

#include <gtest/gtest.h>
#include "lib/utility/ob_tracepoint.h"
#include "common/ob_partition_key.h"
#include "storage/replayengine/ob_log_replay_engine.h"
#include "../mockcontainer/mock_ob_trans_service.h"
#include "replayengine/mock_partition_service.h"
#include "share/allocator/ob_tenant_mutil_allocator_mgr.h"
#include "share/ob_common_rpc_proxy.h"
#include "lib/container/ob_array_iterator.h"

namespace oceanbase {
using namespace common;
using namespace transaction;
using namespace storage;
namespace unittest {
class ObLogReplayEngineTest : public testing::Test {
public:
  void init();
  void destroy();
  char* get_log_buf(storage::ObStorageLogType log_type, int64_t trans_id, int64_t& buf_len);
  char* get_log_buf(storage::ObStorageLogType log_type, int64_t& buf_len);
  char* get_invalid_log_buf(storage::ObStorageLogType log_type, int64_t& buf_len);

protected:
  replayengine::ObLogReplayEngine* log_replay_engine_;
  common::ObPartitionKey partition_key_;
  common::ObPartitionKey ignored_partition_key_;

protected:
  char buffer[1024];
  MockPartitionService mock_partition_service_;
  MockObTransService mock_trans_replay_service_;
  ObAddr self_addr_;
  int64_t submit_timestamp_;
  uint64_t log_id_;

  replayengine::ObLogReplayEngine::ObLogReplayEngineConfig config;

  static const int64_t ALLOCATOR_TOTAL_LIMIT = 5L * 1024L * 1024L * 1024L;
  // 1.5G //ALLOCATOR_TOTAL_LIMIT / 2;
  static const int64_t ALLOCATOR_HOLD_LIMIT = static_cast<int64_t>(1.5 * 1024L * 1024L * 1024L);
  static const int64_t ALLOCATOR_PAGE_SIZE = OB_MALLOC_BIG_BLOCK_SIZE;
};

void ObLogReplayEngineTest::init()
{
  config.total_limit_ = ALLOCATOR_TOTAL_LIMIT;
  config.hold_limit_ = ALLOCATOR_HOLD_LIMIT;
  config.page_size_ = ALLOCATOR_PAGE_SIZE;

  memset(buffer, 0, sizeof(buffer));
  mock_partition_service_.reset();
  mock_partition_service_.start();
  partition_key_.init(1, 1, 1);
  ignored_partition_key_.init(2, 2, 2);
  log_replay_engine_ = static_cast<replayengine::ObLogReplayEngine*>(
      ob_malloc_align(32, sizeof(replayengine::ObLogReplayEngine), ObModIds::OB_UPS_LOG));
  new (log_replay_engine_) replayengine::ObLogReplayEngine();
  log_replay_engine_->init(&mock_trans_replay_service_, &mock_partition_service_, config);
  mock_partition_service_.init_partition(log_replay_engine_);

  submit_timestamp_ = ObTimeUtility::current_time();
  log_id_ = 128;
}

void ObLogReplayEngineTest::destroy()
{
  mock_partition_service_.destroy();
  log_replay_engine_->destroy();
  ob_free_align(log_replay_engine_);
  log_replay_engine_ = NULL;
}

char* ObLogReplayEngineTest::get_log_buf(storage::ObStorageLogType log_type, int64_t trans_id, int64_t& buf_len)
{
  int64_t pos = 0;
  serialization::encode_i64(buffer, 1024, pos, log_type);
  serialization::encode_i64(buffer, 1024, pos, trans_id);
  buf_len = pos;
  return buffer;
}

char* ObLogReplayEngineTest::get_log_buf(storage::ObStorageLogType log_type, int64_t& buf_len)
{
  int64_t pos = 0;
  serialization::encode_i64(buffer, 1024, pos, log_type);
  buf_len = pos;
  return buffer;
}

char* ObLogReplayEngineTest::get_invalid_log_buf(storage::ObStorageLogType log_type, int64_t& buf_len)
{
  UNUSED(log_type);
  int64_t pos = 0;
  serialization::encode_i64(buffer, 1024, pos, -1);
  buf_len = pos;
  return buffer;
}

TEST_F(ObLogReplayEngineTest, smoke_freeze_commit)
{
  init();
  int64_t buf_len = 0;
  char* log_buf = NULL;

  log_buf = get_log_buf(OB_LOG_TRANS_REDO, 1, buf_len);
  EXPECT_EQ(OB_SUCCESS,
      log_replay_engine_->submit_replay_task(
          partition_key_, log_buf, buf_len, submit_timestamp_, log_id_, 0, 0, false));

  log_buf = get_log_buf(OB_LOG_TRANS_PREPARE, 1, buf_len);
  EXPECT_EQ(OB_SUCCESS,
      log_replay_engine_->submit_replay_task(
          partition_key_, log_buf, buf_len, submit_timestamp_, log_id_, 0, 0, false));

  log_buf = get_log_buf(OB_LOG_TRANS_COMMIT, 1, buf_len);
  EXPECT_EQ(OB_SUCCESS,
      log_replay_engine_->submit_replay_task(
          partition_key_, log_buf, buf_len, submit_timestamp_, log_id_, 0, 0, false));

  log_buf = get_log_buf(OB_LOG_TRANS_CLEAR, 1, buf_len);
  EXPECT_EQ(OB_SUCCESS,
      log_replay_engine_->submit_replay_task(
          partition_key_, log_buf, buf_len, submit_timestamp_, log_id_, 0, 0, false));

  sleep(2);
  destroy();
}

TEST_F(ObLogReplayEngineTest, test_not_init)
{
  log_replay_engine_ = static_cast<replayengine::ObLogReplayEngine*>(
      ob_malloc_align(32, sizeof(replayengine::ObLogReplayEngine), ObModIds::OB_UPS_LOG));
  new (log_replay_engine_) replayengine::ObLogReplayEngine();
  EXPECT_EQ(OB_NOT_INIT,
      log_replay_engine_->submit_replay_task(partition_key_, NULL, 0, submit_timestamp_, log_id_, 0, 0, false));
  log_replay_engine_->destroy();
  ob_free_align(log_replay_engine_);
  log_replay_engine_ = NULL;
}

TEST_F(ObLogReplayEngineTest, test_invalid_arg_init_twice)
{
  log_replay_engine_ = static_cast<replayengine::ObLogReplayEngine*>(
      ob_malloc_align(32, sizeof(replayengine::ObLogReplayEngine), ObModIds::OB_UPS_LOG));
  new (log_replay_engine_) replayengine::ObLogReplayEngine();
  config.total_limit_ = ALLOCATOR_TOTAL_LIMIT;
  config.hold_limit_ = ALLOCATOR_HOLD_LIMIT;
  config.page_size_ = ALLOCATOR_PAGE_SIZE;

  EXPECT_EQ(OB_INVALID_ARGUMENT, log_replay_engine_->init(NULL, NULL, config));
  EXPECT_EQ(OB_SUCCESS, log_replay_engine_->init(&mock_trans_replay_service_, &mock_partition_service_, config));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      log_replay_engine_->submit_replay_task(partition_key_, NULL, 0, submit_timestamp_, log_id_, 0, 0, false));
  EXPECT_EQ(OB_INIT_TWICE, log_replay_engine_->init(&mock_trans_replay_service_, &mock_partition_service_, config));
  log_replay_engine_->destroy();
  ob_free_align(log_replay_engine_);
  log_replay_engine_ = NULL;
}

TEST_F(ObLogReplayEngineTest, test_deserialize_err)
{
  init();
  int64_t buf_len = 0;
  char* log_buf = NULL;
  log_buf = get_log_buf(OB_LOG_TRANS_REDO, 1, buf_len);
  EXPECT_EQ(OB_DESERIALIZE_ERROR,
      log_replay_engine_->submit_replay_task(partition_key_, log_buf, 2, submit_timestamp_, log_id_, 0, 0, false));

  destroy();
}

TEST_F(ObLogReplayEngineTest, test_entry_not_exist)
{
  init();
  int64_t buf_len = 0;
  char* log_buf = NULL;

  log_buf = get_log_buf(OB_LOG_TRANS_REDO, 1, buf_len);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST,
      log_replay_engine_->submit_replay_task(
          ignored_partition_key_, log_buf, buf_len, submit_timestamp_, log_id_, 0, 0, false));

  log_buf = get_log_buf(OB_LOG_TRANS_PREPARE, 1, buf_len);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST,
      log_replay_engine_->submit_replay_task(
          ignored_partition_key_, log_buf, buf_len, submit_timestamp_, log_id_, 0, 0, false));

  log_buf = get_log_buf(OB_LOG_TRANS_COMMIT, 1, buf_len);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST,
      log_replay_engine_->submit_replay_task(
          ignored_partition_key_, log_buf, buf_len, submit_timestamp_, log_id_, 0, 0, false));

  log_buf = get_log_buf(OB_LOG_TRANS_CLEAR, 1, buf_len);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST,
      log_replay_engine_->submit_replay_task(
          ignored_partition_key_, log_buf, buf_len, submit_timestamp_, log_id_, 0, 0, false));

  destroy();
}

TEST_F(ObLogReplayEngineTest, test8)
{
  init();
  TP_SET_ERROR("ob_log_replay_engine.cpp", "handle", "a", OB_ENTRY_NOT_EXIST);
  int64_t buf_len = 0;
  char* log_buf = NULL;

  log_buf = get_log_buf(OB_LOG_TRANS_REDO, 1, buf_len);
  EXPECT_EQ(OB_SUCCESS,
      log_replay_engine_->submit_replay_task(
          partition_key_, log_buf, buf_len, submit_timestamp_, log_id_, 0, 0, false));
  log_buf = get_log_buf(OB_LOG_TRANS_PREPARE, 1, buf_len);
  EXPECT_EQ(OB_SUCCESS,
      log_replay_engine_->submit_replay_task(
          partition_key_, log_buf, buf_len, submit_timestamp_, log_id_, 0, 0, false));
  log_buf = get_log_buf(OB_LOG_TRANS_COMMIT, 1, buf_len);
  EXPECT_EQ(OB_SUCCESS,
      log_replay_engine_->submit_replay_task(
          partition_key_, log_buf, buf_len, submit_timestamp_, log_id_, 0, 0, false));
  log_buf = get_log_buf(OB_LOG_TRANS_CLEAR, 1, buf_len);
  EXPECT_EQ(OB_SUCCESS,
      log_replay_engine_->submit_replay_task(
          partition_key_, log_buf, buf_len, submit_timestamp_, log_id_, 0, 0, false));

  sleep(5);
  TP_SET("ob_log_replay_engine.cpp", "handle", "a", NULL);
  destroy();
}

TEST_F(ObLogReplayEngineTest, test10)
{
  init();
  sleep(5);
  destroy();
}

TEST_F(ObLogReplayEngineTest, test12)
{
  init();
  TP_SET_ERROR("ob_log_replay_engine.cpp", "submit_trans_log", "c", OB_ERROR);
  TP_SET_ERROR("ob_log_replay_engine.cpp", "submit_major_freeze_log", "c", OB_ERROR);

  int64_t buf_len = 0;
  char* log_buf = NULL;

  // log_buf = get_log_buf(OB_LOG_TRANS_REDO, 1, buf_len);
  // EXPECT_EQ(OB_ERROR, log_replay_engine_->submit_replay_task(partition_key_, log_buf, buf_len,
  //      submit_timestamp_, log_id_, 0, 0, false));
  // log_buf = get_log_buf(OB_LOG_TRANS_PREPARE, 1, buf_len);
  // EXPECT_EQ(OB_ERROR, log_replay_engine_->submit_replay_task(partition_key_, log_buf, buf_len,
  //      submit_timestamp_, log_id_, 0, 0, false));
  // log_buf = get_log_buf(OB_LOG_TRANS_COMMIT, 1, buf_len);
  // EXPECT_EQ(OB_ERROR, log_replay_engine_->submit_replay_task(partition_key_, log_buf, buf_len,
  //      submit_timestamp_, log_id_, 0, 0, false));
  // log_buf = get_log_buf(OB_LOG_TRANS_CLEAR, 1, buf_len);
  // EXPECT_EQ(OB_ERROR, log_replay_engine_->submit_replay_task(partition_key_, log_buf, buf_len,
  //      submit_timestamp_, log_id_, 0, 0, false));

  log_buf = get_log_buf(storage::OB_LOG_UNKNOWN, buf_len);
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      log_replay_engine_->submit_replay_task(
          partition_key_, log_buf, buf_len, submit_timestamp_, log_id_, 0, 0, false));

  TP_SET("ob_log_replay_engine.cpp", "submit_trans_log", "c", NULL);
  TP_SET("ob_log_replay_engine.cpp", "submit_major_freeze_log", "c", NULL);
  destroy();
}

TEST_F(ObLogReplayEngineTest, test14)
{
  init();
  int64_t buf_len = 0;
  char* log_buf = NULL;

  log_buf = get_invalid_log_buf(OB_LOG_TRANS_PREPARE, buf_len);
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      log_replay_engine_->submit_replay_task(
          partition_key_, log_buf, buf_len, submit_timestamp_, log_id_, 0, 0, false));

  destroy();
}

struct ThreadFuncSubmitTaskArg {
  ThreadFuncSubmitTaskArg()
  {
    reset();
  }
  ~ThreadFuncSubmitTaskArg()
  {
    reset();
  }
  int init(replayengine::ObLogReplayEngine* log_replay_engine, common::ObPartitionKey& partition_key)
  {
    rp_eg_ = log_replay_engine;
    pk_ = partition_key;
    return OB_SUCCESS;
  }
  void reset()
  {
    rp_eg_ = NULL;
    stop_ = false;
  }
  void set_stop()
  {
    stop_ = true;
  }
  bool is_stop()
  {
    return stop_;
  }
  replayengine::ObLogReplayEngine* rp_eg_;
  common::ObPartitionKey pk_;
  bool stop_;
};

struct ThreadFuncGetLogIdArg {
  ThreadFuncGetLogIdArg()
  {
    reset();
  }
  ~ThreadFuncGetLogIdArg()
  {
    reset();
  }
  int init(MockPartitionService* mock_partition_service, common::ObPartitionKey& partition_key)
  {
    mock_partition_service_ = mock_partition_service;
    pk_ = partition_key;
    return OB_SUCCESS;
  }
  void reset()
  {
    mock_partition_service_ = NULL;
    stop_ = false;
  }
  void set_stop()
  {
    stop_ = true;
  }
  bool is_stop()
  {
    return stop_;
  }
  MockPartitionService* mock_partition_service_;
  common::ObPartitionKey pk_;
  bool stop_;
};

void* thread_func_submit_task(void* data)
{
  ThreadFuncSubmitTaskArg* arg = reinterpret_cast<ThreadFuncSubmitTaskArg*>(data);
  replayengine::ObLogReplayEngine* log_replay_engine = arg->rp_eg_;
  common::ObPartitionKey& partition_key = arg->pk_;
  uint64_t log_id = 128;
  int ret = OB_SUCCESS;
  while (!arg->is_stop()) {
    char buf[1024] = {0};
    int trans_id = rand() % 256;
    int64_t pos = 0;
    serialization::encode_i64(buf, 1024, pos, OB_LOG_TRANS_COMMIT);
    serialization::encode_i64(buf, 1024, pos, trans_id);
    int64_t buf_len = pos;
    int64_t submit_timestamp = ObTimeUtility::current_time();
    REPLAY_LOG(INFO, "submit_task", K(log_id));
    EXPECT_EQ(OB_SUCCESS,
        ret =
            log_replay_engine->submit_replay_task(partition_key, buf, buf_len, submit_timestamp, log_id, 0, 0, false));
    REPLAY_LOG(INFO, "done submit_task", K(log_id));
    log_id++;
  }
  return NULL;
}

void* thread_get_log_id_task(void* data)
{
  ThreadFuncGetLogIdArg* arg = reinterpret_cast<ThreadFuncGetLogIdArg*>(data);
  MockPartitionService* mock_partition_service = arg->mock_partition_service_;
  storage::ObIPartitionGroup* partition = NULL;
  common::ObPartitionKey& partition_key = arg->pk_;
  int ret = mock_partition_service->get_partition(partition_key, partition);
  EXPECT_EQ(OB_SUCCESS, ret);
  storage::ObReplayStatus& replay_status = *(partition->get_replay_status());
  while (!arg->is_stop()) {
    uint64_t min_log_id = replay_status.get_min_unreplay_log_id();
    EXPECT_GT(min_log_id, 0);
  }
  return NULL;
}

TEST_F(ObLogReplayEngineTest, test_submit_task)
{
  REPLAY_LOG(INFO, "start to test_submit_task");
  init();
  const uint64_t THREAD_NUM = 8;
  pthread_t submit_task_thread;
  ThreadFuncSubmitTaskArg submit_task_arg;
  submit_task_arg.init(log_replay_engine_, partition_key_);

  EXPECT_EQ(0, pthread_create(&submit_task_thread, NULL, thread_func_submit_task, &submit_task_arg));

  pthread_t get_log_id_threads[THREAD_NUM];
  ThreadFuncGetLogIdArg get_log_id_arg;
  get_log_id_arg.init(&mock_partition_service_, partition_key_);

  for (uint64_t i = 0; i < THREAD_NUM; ++i) {
    EXPECT_EQ(0, pthread_create(&get_log_id_threads[i], NULL, thread_get_log_id_task, &get_log_id_arg));
  }

  sleep(5);
  get_log_id_arg.set_stop();
  for (uint64_t i = 0; i < THREAD_NUM; ++i) {
    pthread_join(get_log_id_threads[i], NULL);
  }

  submit_task_arg.set_stop();
  pthread_join(submit_task_thread, NULL);
  REPLAY_LOG(INFO, "end to test_submit_task");
  sleep(3);
  destroy();
}

}  // namespace unittest
}  // namespace oceanbase

int main(int argc, char** argv)
{
  /*
   I will fix this later :)
   */
  UNUSED(argc);
  UNUSED(argv);

  OB_LOGGER.set_file_name("test_log_replay_engine.log", true);
  OB_LOGGER.set_log_level("INFO");
  TMA_MGR_INSTANCE.init();
  TRANS_LOG(INFO, "begin unittest: test_log_replay_engine");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();

  return 0;
}
