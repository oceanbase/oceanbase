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

#include "storage/transaction/ob_clog_adapter.h"
#include "../mockcontainer/mock_ob_partition_service.h"
#include "../mock_ob_partition_component_factory.h"
#include "../../clog/mock_ob_partition_log_service.h"
#include "../mockcontainer/mock_ob_partition.h"
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "share/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "share/ob_common_rpc_proxy.h"
#include "lib/container/ob_array_iterator.h"

namespace oceanbase {
using namespace common;
using namespace storage;
using namespace clog;
using namespace transaction;
using namespace oceanbase::share;

//#pragma pack(8)

// add by dongjian.
int hack_test_count = 0;
const int SUBMIT_EAGAIN_COUNT = 6;

namespace clog {
// add by dongjian
class MockSubmitLogCb : public ObITransSubmitLogCb {
public:
  MockSubmitLogCb()
  {}
  ~MockSubmitLogCb()
  {}
  virtual int on_success(const common::ObPartitionKey& partition_key, const clog::ObLogType log_type,
      const uint64_t log_id, const int64_t version, const bool batch_committed, const bool batch_last_succeed)
  {
    UNUSED(partition_key);
    UNUSED(log_type);
    UNUSED(log_id);
    UNUSED(version);
    UNUSED(batch_committed);
    UNUSED(batch_last_succeed);
    return OB_SUCCESS;
  }
  virtual int on_submit_log_success(const bool b, const uint64_t cur_log_id, const int64_t cur_log_timestamp)
  {
    UNUSED(b);
    UNUSED(cur_log_id);
    UNUSED(cur_log_timestamp);
    return OB_SUCCESS;
  }
  virtual int on_submit_log_fail(const int retcode)
  {
    UNUSED(retcode);
    return OB_SUCCESS;
  }
  virtual int set_real_submit_timestamp(const int64_t timestamp)
  {
    UNUSED(timestamp);
    return OB_SUCCESS;
  }
  virtual int64_t get_log_type() const
  {
    return OB_SUCCESS;
  }
  virtual int64_t to_string(char* buf, const int64_t buf_len) const
  {
    UNUSED(buf);
    UNUSED(buf_len);
    return 0;
  }
};
}  // namespace clog

namespace unittest {
class TestObClogAdapter : public ::testing::Test {
public:
  virtual void SetUp()
  {}
  virtual void TearDown()
  {}

public:
  // valid partition parameters
  static const int64_t VALID_TABLE_ID = 1;
  static const int32_t VALID_PARTITION_ID = 1;
  static const int32_t VALID_PARTITION_COUNT = 100;
  // invalid partition parameters
  static const int64_t INVALID_TABLE_ID = -1;
  static const int32_t INVALID_PARTITION_ID = -1;
  static const int32_t INVALID_PARTITION_COUNT = -100;
};

class MyMockObPartitionLogService : public MockPartitionLogService {
public:
  int submit_log(const char* buff, const int64_t size, const storage::ObStorageLogType log_type,
      const common::ObVersion& version, ObITransSubmitLogCb* cb)
  {
    UNUSED(buff);
    UNUSED(cb);
    UNUSED(log_type);
    UNUSED(version);
    int ret = OB_SUCCESS;
    if (1 == size) {
      TRANS_LOG(WARN, "invalid argument", K(size));
      ret = OB_INVALID_ARGUMENT;
    } else {
      TRANS_LOG(INFO, "submit log success.");
    }
    return ret;
  }
};

// add by dongjian
using clog::MockSubmitLogCb;
class MyMockObPartitionLogService2 : public MockPartitionLogService, public ObSimpleThreadPool {
  // inner class
  class LogServiceSubmitTask {
  public:
    LogServiceSubmitTask() : cb_(NULL)
    {}
    ~LogServiceSubmitTask()
    {}
    void set_submit_cb(ObITransSubmitLogCb* cb)
    {
      cb_ = cb;
    }
    ObITransSubmitLogCb* get_submit_cb()
    {
      return cb_;
    }

  private:
    ObITransSubmitLogCb* cb_;
  };

public:
  MyMockObPartitionLogService2()
  {
    ObAddr self;  // we don't care about this field.
    ObSimpleThreadPool::init(1, 100);
  }
  ~MyMockObPartitionLogService2()
  {
    ObSimpleThreadPool::destroy();
  }

  int get_curr_member_list(const common::ObPartitionKey& pkey, common::ObMemberList& member_list) const
  {
    UNUSED(pkey);
    UNUSED(member_list);
    return OB_SUCCESS;
  }

  int submit_log(const char* buff, const int64_t size, const storage::ObStorageLogType log_type,
      const common::ObVersion& version, ObITransSubmitLogCb* cb)
  {
    UNUSED(buff);
    UNUSED(size);
    UNUSED(log_type);
    UNUSED(version);
    UNUSED(cb);
    int ret = OB_SUCCESS;
    if (hack_test_count < SUBMIT_EAGAIN_COUNT) {
      hack_test_count++;
      ret = OB_EAGAIN;
    } else {
      LogServiceSubmitTask* submit_task = new LogServiceSubmitTask();
      submit_task->set_submit_cb(cb);
      push(submit_task);  // async task. maybe executed before line 132 return ret;
      ret = OB_SUCCESS;
    }
    return ret;
  }

  void handle(void* task)
  {
    LogServiceSubmitTask* submit_task = static_cast<LogServiceSubmitTask*>(task);
    ObPartitionKey partition_key;  // we do not care about these field, just set it to default value.
    const uint64_t log_id = 100;
    const uint64_t trans_version = 200;
    clog::ObLogType log_type = OB_LOG_SUBMIT;
    submit_task->get_submit_cb()->on_success(partition_key, log_type, log_id, trans_version, false, false);
    delete submit_task;
  }
};

class MyMockObPartitionComponentFactory : public MockObIPartitionComponentFactory {
public:
  virtual void free(ObIPartitionGroup* partition)
  {
    UNUSED(partition);
    // delete partition;
  }
};

class MyMockObPartition : public MockObIPartitionGroup {
public:
  MyMockObPartition() : partition_log_service_(NULL), pg_file_(NULL)
  {}
  virtual ~MyMockObPartition()
  {}
  void set_log_service(ObIPartitionLogService* log_service)
  {
    partition_log_service_ = log_service;
  }
  // get partition log service
  ObIPartitionLogService* get_log_service()
  {
    return partition_log_service_;
  }
  virtual blocksstable::ObStorageFile* get_storage_file()
  {
    return pg_file_;
  }
  virtual const blocksstable::ObStorageFile* get_storage_file() const
  {
    return pg_file_;
  }
  virtual blocksstable::ObStorageFileHandle& get_storage_file_handle()
  {
    return file_handle_;
  }
  virtual int get_table_store_cnt(int64_t& table_cnt) const override
  {
    table_cnt = 0;
    return OB_SUCCESS;
  }
  virtual int remove_election_from_mgr()
  {
    return OB_SUCCESS;
  }

private:
  ObIPartitionLogService* partition_log_service_;
  blocksstable::ObStorageFile* pg_file_;
  blocksstable::ObStorageFileHandle file_handle_;
};

class MyMockObPartitionService : public MockObIPartitionService {
public:
  MyMockObPartitionService() : partition_(NULL)
  {
    partition_ = new MyMockObPartition();
    cp_fty_ = new MyMockObPartitionComponentFactory();
  }
  virtual ~MyMockObPartitionService()
  {
    if (NULL != partition_) {
      delete partition_;
      delete cp_fty_;
      partition_ = NULL;
      cp_fty_ = NULL;
    }
  }
  int get_partition(const common::ObPartitionKey& pkey, ObIPartitionGroup*& partition) const
  {
    MyMockObPartitionLogService* log_service = NULL;
    int ret = OB_SUCCESS;
    if (!pkey.is_valid()) {
      TRANS_LOG(WARN, "invalid argument, pkey is invalid.", K(pkey));
      ret = OB_INVALID_ARGUMENT;
    } else if (1 == pkey.table_id_) {
      TRANS_LOG(INFO, "get partition success.", K(pkey.table_id_));
      partition = partition_;
      partition_->set_log_service(new MyMockObPartitionLogService());
    } else if (2 == pkey.table_id_) {
      TRANS_LOG(WARN, "invalid argument, get partition error.", K(pkey.table_id_));
      partition = partition_;
      partition_->set_log_service(new MyMockObPartitionLogService());
      // ret = OB_ERR_UNEXPECTED;
    } else if (3 == pkey.table_id_) {
      TRANS_LOG(INFO, "get partition success.", K(pkey.table_id_));
      partition = partition_;
      partition_->set_log_service(new MyMockObPartitionLogService());
    } else if (4 == pkey.table_id_) {
      TRANS_LOG(INFO, "get partition success.", K(pkey.table_id_));
      partition = partition_;
      partition_->set_log_service(new MyMockObPartitionLogService());
    } else {
      TRANS_LOG(INFO, "test info", K(pkey));
      // do nothing.
    }
    if (NULL != log_service) {
      delete log_service;
      log_service = NULL;
    }

    return ret;
  }
  int get_partition(const common::ObPartitionKey& pkey, ObIPartitionGroupGuard& guard) const
  {
    int ret = common::OB_SUCCESS;
    ObIPartitionGroup* partition = NULL;
    if (OB_FAIL(get_partition(pkey, partition))) {
      STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
    } else {
      guard.set_partition_group(this->get_pg_mgr(), *partition);
    }
    return ret;
  }
  virtual int get_curr_member_list(const common::ObPartitionKey& pkey, common::ObMemberList& member_list) const
  {
    UNUSED(pkey);
    UNUSED(member_list);
    return OB_SUCCESS;
  }

private:
  MyMockObPartition* partition_;
};

// add by dongjian
class MyMockObPartitionService2 : public MockObIPartitionService {
public:
  MyMockObPartitionService2()
  {
    partition_ = new MyMockObPartition();
    cp_fty_ = new MyMockObPartitionComponentFactory();
    fprintf(stdout, "new partition addr = %p\n", partition_);
  }

  ~MyMockObPartitionService2()
  {
    if (NULL != partition_) {
      fprintf(stdout, "free partition addr = %p\n", partition_);
      delete partition_;
      delete cp_fty_;
      partition_ = NULL;
      cp_fty_ = NULL;
    }
  }

  int set_log_service(MyMockObPartitionLogService2* log_service2)
  {
    int ret = OB_SUCCESS;
    if (NULL == partition_) {
      TRANS_LOG(WARN, "parititon is null");
      ret = OB_NOT_INIT;
    } else {
      partition_->set_log_service(log_service2);
      ret = OB_SUCCESS;
    }
    return ret;
  }

  int get_partition(const common::ObPartitionKey& pkey, ObIPartitionGroup*& partition) const
  {
    UNUSED(pkey);
    partition = partition_;
    return OB_SUCCESS;
  }
  int get_partition(const common::ObPartitionKey& pkey, ObIPartitionGroupGuard& guard) const
  {
    int ret = common::OB_SUCCESS;
    ObIPartitionGroup* partition = NULL;
    if (OB_FAIL(get_partition(pkey, partition))) {
      STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
    } else {
      guard.set_partition_group(this->get_pg_mgr(), *partition);
    }
    return ret;
  }
  virtual int get_curr_member_list(const common::ObPartitionKey& pkey, common::ObMemberList& member_list) const
  {
    UNUSED(pkey);
    UNUSED(member_list);
    return OB_SUCCESS;
  }

private:
  MyMockObPartition* partition_;
};

TEST(TestClogAdapter, submit_EAGAIN)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  MyMockObPartitionLogService2 log_service2;
  MyMockObPartitionService2 partition_service2;
  ObClogAdapter clog_adapter;

  partition_service2.set_log_service(&log_service2);
  clog_adapter.init(&partition_service2);
  EXPECT_EQ(OB_SUCCESS, clog_adapter.start());

  char log_buffer[1024];
  memset(log_buffer, 'X', 1024);
  int64_t size = 1024;
  MockSubmitLogCb submit_log_cb;
  ObVersion version(100, 100);
  const ObPartitionKey partition_key(1, 1, 1);  // we do not care about this field
  uint64_t ret_log_id = 0;
  int64_t ret_ts = 0;
  EXPECT_EQ(OB_SUCCESS,
      clog_adapter.submit_log(partition_key, version, log_buffer, size, &submit_log_cb, NULL, ret_log_id, ret_ts));
  sleep(1);  // wait ObSimpleThreadPool handling task
  clog_adapter.stop();
  clog_adapter.wait();
}

//////////////////////basic function test//////////////////////////////////////////
// test the init and destroy of ObClogAdapter
TEST_F(TestObClogAdapter, init_destroy)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObClogAdapter clog_adapter;
  MyMockObPartitionService partition_service;

  EXPECT_EQ(OB_SUCCESS, clog_adapter.init(&partition_service));
  EXPECT_EQ(OB_SUCCESS, clog_adapter.start());
  clog_adapter.stop();
  clog_adapter.destroy();
}

// test the function sutmit_log
TEST_F(TestObClogAdapter, submit_log)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObClogAdapter clog_adapter;
  MyMockObPartitionService partition_service;
  EXPECT_EQ(OB_SUCCESS, clog_adapter.init(&partition_service));
  ObPartitionKey partition_key(VALID_TABLE_ID, VALID_PARTITION_ID, VALID_PARTITION_COUNT);
  MyMockObPartition mock_partition;
  MockSubmitLogCb submit_log_cb;
  ObVersion version(100, 100);
  const int64_t BUFFER_SIZE = 1024;
  char buffer[BUFFER_SIZE];
  uint64_t ret_log_id = 0;
  int64_t ret_ts = 0;
  EXPECT_EQ(OB_NOT_RUNNING,
      clog_adapter.submit_log(partition_key, version, buffer, BUFFER_SIZE, &submit_log_cb, NULL, ret_log_id, ret_ts));
  EXPECT_EQ(OB_SUCCESS, clog_adapter.start());
  EXPECT_EQ(OB_SUCCESS,
      clog_adapter.submit_log(partition_key, version, buffer, BUFFER_SIZE, &submit_log_cb, NULL, ret_log_id, ret_ts));
  clog_adapter.stop();
  clog_adapter.destroy();
}

///////////////////////////////boundary test//////////////////////////////////////
// test error cases of init of TestObClogAdapter: uninit, init failure, repeated init
TEST_F(TestObClogAdapter, repeat_init)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObClogAdapter clog_adapter;
  EXPECT_EQ(OB_NOT_INIT, clog_adapter.start());
  ObPartitionKey partition_key(VALID_TABLE_ID, VALID_PARTITION_ID, VALID_PARTITION_COUNT);
  int clog_status = -1;
  const bool check_election = true;
  int64_t leader_epoch = 0;
  ObTsWindows changing_leader_windows;
  EXPECT_EQ(OB_NOT_INIT,
      clog_adapter.get_status(partition_key, check_election, clog_status, leader_epoch, changing_leader_windows));

  MyMockObPartitionService* partition_service = NULL;
  EXPECT_EQ(OB_INVALID_ARGUMENT, clog_adapter.init(partition_service));

  partition_service = new MyMockObPartitionService();
  ASSERT_TRUE(NULL != partition_service);
  EXPECT_EQ(OB_SUCCESS, clog_adapter.init(partition_service));
  EXPECT_EQ(OB_INIT_TWICE, clog_adapter.init(partition_service));

  delete partition_service;
  partition_service = NULL;
  clog_adapter.destroy();
}

// test the wrong order of operations start, wait, stop of ObClogAdapter
// test other exceptions of calling other functions  due to the wrong order of operations
TEST_F(TestObClogAdapter, start_stop_wait)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObClogAdapter clog_adapter;
  MyMockObPartitionService partition_service;
  EXPECT_EQ(OB_SUCCESS, clog_adapter.init(&partition_service));

  // get a partition status before start
  ObPartitionKey partition_key(INVALID_TABLE_ID, INVALID_PARTITION_ID, INVALID_PARTITION_COUNT);
  int clog_status = -1;
  const bool check_election = true;
  int64_t leader_epoch = 0;
  ObTsWindows changing_leader_windows;
  EXPECT_EQ(OB_NOT_RUNNING,
      clog_adapter.get_status(partition_key, check_election, clog_status, leader_epoch, changing_leader_windows));

  // repeat start after start without wait
  EXPECT_EQ(OB_SUCCESS, clog_adapter.start());
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      clog_adapter.get_status(partition_key, check_election, clog_status, leader_epoch, changing_leader_windows));

  // repeat stop operation
  clog_adapter.stop();

  // restart
  EXPECT_EQ(OB_SUCCESS, clog_adapter.start());
  clog_adapter.destroy();
}

// test the failure case for ObClogAdapter
// clog_adapter is not initialized
TEST_F(TestObClogAdapter, submit_log_not_init_error)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObClogAdapter clog_adapter;
  MyMockObPartitionService partition_service;

  int64_t test_valid_table_id = VALID_TABLE_ID + 1;
  ObPartitionKey partition_key(test_valid_table_id, VALID_PARTITION_ID, VALID_PARTITION_COUNT);
  MyMockObPartition mock_partition;
  MockSubmitLogCb submit_log_cb;
  ObVersion version(100, 100);
  const int64_t BUFFER_SIZE = 1024;
  char buffer[BUFFER_SIZE];
  uint64_t ret_log_id = 0;
  int64_t ret_ts = 0;
  EXPECT_EQ(OB_NOT_INIT,
      clog_adapter.submit_log(partition_key, version, buffer, BUFFER_SIZE, &submit_log_cb, NULL, ret_log_id, ret_ts));

  clog_adapter.destroy();
}
// test the failure case for ObClogAdapter
// the input parameters partition_key and submit_log_cb are invalid
TEST_F(TestObClogAdapter, submit_log_invalid_argument)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObClogAdapter clog_adapter;
  MyMockObPartitionService partition_service;
  EXPECT_EQ(OB_SUCCESS, clog_adapter.init(&partition_service));
  EXPECT_EQ(OB_SUCCESS, clog_adapter.start());

  ObPartitionKey invalid_partition_key(INVALID_TABLE_ID, INVALID_PARTITION_ID, INVALID_PARTITION_COUNT);
  MyMockObPartition mock_partition;
  MockSubmitLogCb submit_log_cb;
  ObVersion version(100, 100);
  const int64_t BUFFER_SIZE = 1024;
  char buffer[BUFFER_SIZE];
  uint64_t ret_log_id = 0;
  int64_t ret_ts = 0;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      clog_adapter.submit_log(
          invalid_partition_key, version, buffer, BUFFER_SIZE, &submit_log_cb, NULL, ret_log_id, ret_ts));
  clog_adapter.stop();
  clog_adapter.destroy();
}
// test the failure case for ObClogAdapter
// fail to execute get_partition of partition_service
TEST_F(TestObClogAdapter, submit_log_partition_service_error)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObClogAdapter clog_adapter;
  MyMockObPartitionService partition_service;
  EXPECT_EQ(OB_SUCCESS, clog_adapter.init(&partition_service));
  EXPECT_EQ(OB_SUCCESS, clog_adapter.start());

  int64_t test_valid_table_id = VALID_TABLE_ID + 1;
  ObPartitionKey partition_key1(test_valid_table_id, VALID_PARTITION_ID, VALID_PARTITION_COUNT);
  MyMockObPartition mock_partition;
  MockSubmitLogCb submit_log_cb;
  ObVersion version(100, 100);
  const int64_t BUFFER_SIZE = 1024;
  char buffer[BUFFER_SIZE];
  int clog_status = -1;
  const bool check_election = true;
  int64_t leader_epoch = 0;
  ObTsWindows changing_leader_windows;
  uint64_t ret_log_id = 0;
  int64_t ret_ts = 0;
  EXPECT_EQ(OB_SUCCESS,
      clog_adapter.get_status(partition_key1, check_election, clog_status, leader_epoch, changing_leader_windows));
  EXPECT_EQ(OB_SUCCESS,
      clog_adapter.submit_log(partition_key1, version, buffer, BUFFER_SIZE, &submit_log_cb, NULL, ret_log_id, ret_ts));
  // create a valid partition whose table_id is 3
  test_valid_table_id = VALID_TABLE_ID + 2;
  ObPartitionKey partition_key2(test_valid_table_id, VALID_PARTITION_ID, VALID_PARTITION_COUNT);
  EXPECT_EQ(OB_SUCCESS,
      clog_adapter.submit_log(partition_key2, version, buffer, BUFFER_SIZE, &submit_log_cb, NULL, ret_log_id, ret_ts));

  clog_adapter.stop();
  clog_adapter.destroy();
}

// test the failure cases for ObClogAdapter
// fail to execute get_log_service called by mock_ob_partition
// fail to execute submit_log called by log_service
TEST_F(TestObClogAdapter, submit_log_get_log_service_error)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObClogAdapter clog_adapter;
  MyMockObPartitionService partition_service;
  EXPECT_EQ(OB_SUCCESS, clog_adapter.init(&partition_service));
  EXPECT_EQ(OB_SUCCESS, clog_adapter.start());

  int64_t test_valid_table_id = VALID_TABLE_ID + 3;
  ObPartitionKey partition_key1(test_valid_table_id, VALID_PARTITION_ID, VALID_PARTITION_COUNT);
  MyMockObPartition mock_partition;
  MockSubmitLogCb submit_log_cb;
  ObVersion version(100, 100);
  int64_t buffer_size = 1024;
  char buffer[buffer_size];
  int clog_status = -1;
  const bool check_election = true;
  int64_t leader_epoch = 0;
  ObTsWindows changing_leader_windows;
  uint64_t ret_log_id = 0;
  int64_t ret_ts = 0;
  EXPECT_EQ(OB_SUCCESS,
      clog_adapter.get_status(partition_key1, check_election, clog_status, leader_epoch, changing_leader_windows));
  EXPECT_EQ(OB_SUCCESS,
      clog_adapter.submit_log(partition_key1, version, buffer, buffer_size, &submit_log_cb, NULL, ret_log_id, ret_ts));

  // create a valid partition whose table_id is 1
  test_valid_table_id = VALID_TABLE_ID;
  ObPartitionKey partition_key2(test_valid_table_id, VALID_PARTITION_ID, VALID_PARTITION_COUNT);
  buffer_size = 1;
  EXPECT_EQ(OB_SUCCESS,
      clog_adapter.submit_log(partition_key2, version, buffer, buffer_size, &submit_log_cb, NULL, ret_log_id, ret_ts));
  buffer_size = 1024;
  EXPECT_EQ(OB_SUCCESS,
      clog_adapter.submit_log(partition_key2, version, buffer, buffer_size, &submit_log_cb, NULL, ret_log_id, ret_ts));
  clog_adapter.stop();
  clog_adapter.destroy();
}
}  // namespace unittest
}  // namespace oceanbase

using namespace oceanbase;
using namespace oceanbase::common;

int main(int argc, char** argv)
{
  int ret = 1;
  ObLogger& logger = ObLogger::get_logger();
  logger.set_file_name("test_ob_clog_adapter.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  //::testing::FLAGS_gmock_verbose = "error";
  if (OB_SUCCESS != (ret = ObClockGenerator::init())) {
    TRANS_LOG(WARN, "init ObClockGenerator error!");
  } else {
    testing::InitGoogleTest(&argc, argv);
    ret = RUN_ALL_TESTS();
  }
  return ret;
}
