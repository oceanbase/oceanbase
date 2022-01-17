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

#include "storage/transaction/ob_trans_result_info_mgr.h"
#include "../mockcontainer/mock_ob_partition_service.h"
#include "../mock_ob_partition_component_factory.h"
#include "../mockcontainer/mock_ob_partition.h"
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "share/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "lib/ob_define.h"

namespace oceanbase {
using namespace common;
using namespace storage;
using namespace clog;
using namespace transaction;

namespace unittest {

// define local_ip
static const char* LOCAL_IP = "127.0.0.1";
static const int32_t PORT = 8080;
static const int32_t PORT2 = 8888888;
static const ObAddr::VER IP_TYPE = ObAddr::IPV4;

class MyMockObPartition : public MockObIPartitionGroup {
public:
  MyMockObPartition() : pg_file_(NULL)
  {}
  virtual ~MyMockObPartition()
  {}
  int get_weak_read_timestamp(int64_t& timestamp)
  {
    timestamp = ObClockGenerator::getClock();
    return OB_SUCCESS;
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
  virtual int remove_election_from_mgr()
  {
    return OB_SUCCESS;
  }

private:
  blocksstable::ObStorageFile* pg_file_;
  blocksstable::ObStorageFileHandle file_handle_;
};

class MyMockObPartitionComponentFactory : public MockObIPartitionComponentFactory {
public:
  virtual void free(ObIPartitionGroup* partition)
  {
    UNUSED(partition);
    // delete partition;
  }
};

class MyMockObPartitionService2 : public MockObIPartitionService {
public:
  MyMockObPartitionService2()
  {
    partition_ = new MyMockObPartition();
    cp_fty_ = new MyMockObPartitionComponentFactory();
  }

  ~MyMockObPartitionService2()
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

TEST(TestObTransResultInfoMgr, insert_gc)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObTransResultInfoMgr info_mgr;
  MyMockObPartitionService2 partition_service;
  ObPartitionKey pkey(2, 1, 100);
  int64_t log_id = 1000;
  ObAddr addr_(IP_TYPE, LOCAL_IP, PORT);
  ObTransID trans_id(addr_);
  ObTransResultInfo* info = NULL;

  EXPECT_EQ(OB_SUCCESS, info_mgr.init(&partition_service, pkey));
  int i = 0;
  bool registered = false;
  while (i < 20) {
    info = ObTransResultInfoFactory::alloc();
    info->init(ObTransResultState::UNKNOWN, ObClockGenerator::getClock(), log_id--, 0, trans_id);
    EXPECT_EQ(OB_SUCCESS, info_mgr.insert(info, registered));
    // EXPECT_EQ(1, info_mgr.get_result_info_count());
    usleep(500000);
    i++;
  }
  i = 0;
  info = ObTransResultInfoFactory::alloc();
  info->init(ObTransResultState::UNKNOWN, ObClockGenerator::getClock(), log_id--, 0, trans_id);
  EXPECT_EQ(OB_SUCCESS, info_mgr.insert(info, registered));
  usleep(250000);
  while (i < 20) {
    info = ObTransResultInfoFactory::alloc();
    info->init(ObTransResultState::UNKNOWN, ObClockGenerator::getClock(), log_id--, 0, trans_id);
    EXPECT_EQ(OB_SUCCESS, info_mgr.insert(info, registered));
    // EXPECT_EQ(2, info_mgr.get_result_info_count());
    usleep(250000);
    i++;
  }
}

TEST(TestObTransResultInfoMgr, invalid_test)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObTransResultInfoMgr info_mgr;
  ObPartitionKey pkey1;
  ObPartitionKey pkey2(2, 1, 100);
  MyMockObPartitionService2 partition_service;
  ObTransResultInfo* result_info;
  ObTransID trans_id;
  int state = 0;
  uint64_t min_log_id = 0;
  int64_t unused = 0;
  EXPECT_EQ(OB_INVALID_ARGUMENT, info_mgr.init(&partition_service, pkey1));
  EXPECT_EQ(OB_INVALID_ARGUMENT, info_mgr.init(NULL, pkey2));
  EXPECT_EQ(OB_SUCCESS, info_mgr.init(&partition_service, pkey2));
  EXPECT_EQ(OB_INVALID_ARGUMENT, info_mgr.rdlock(trans_id));
  EXPECT_EQ(OB_INVALID_ARGUMENT, info_mgr.wrlock(trans_id));
  EXPECT_EQ(OB_INVALID_ARGUMENT, info_mgr.unlock(trans_id));
  EXPECT_EQ(OB_INVALID_ARGUMENT, info_mgr.find_unsafe(trans_id, result_info));
  ObTransResultInfo* info = ObTransResultInfoFactory::alloc();
  info->init(-1, -1, -1, 0, trans_id);
  bool registered = false;
  EXPECT_EQ(OB_SUCCESS, info_mgr.insert(info, registered));
  EXPECT_EQ(OB_INVALID_ARGUMENT, info_mgr.get_state(trans_id, state));
  EXPECT_EQ(OB_SUCCESS, info_mgr.get_min_log(min_log_id, unused));
}

TEST(TestObTransResultInfoMgr, find)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObTransResultInfoMgr info_mgr;
  MyMockObPartitionService2 partition_service;
  ObPartitionKey pkey(2, 1, 100);
  uint64_t log_id = 1;

  EXPECT_EQ(OB_SUCCESS, info_mgr.init(&partition_service, pkey));
  int i = 1;
  bool registered = false;
  while (i < 100000) {
    ObAddr addr_(IP_TYPE, LOCAL_IP, i);
    ObAddr addr2_(IP_TYPE, LOCAL_IP, PORT2);
    ObTransID trans_id(addr_);
    ObTransID trans_id2(addr2_);
    ObTransResultInfo* info = ObTransResultInfoFactory::alloc();
    info->init(ObTransResultState::UNKNOWN, ObClockGenerator::getClock(), log_id++, 0, trans_id);
    EXPECT_EQ(OB_SUCCESS, info_mgr.insert(info, registered));
    EXPECT_EQ(OB_SUCCESS, info_mgr.rdlock(trans_id));
    ObTransResultInfo* result = NULL;
    EXPECT_EQ(OB_SUCCESS, info_mgr.find_unsafe(trans_id, result));
    EXPECT_EQ(trans_id, result->get_trans_id());
    EXPECT_EQ(log_id - 1, result->get_min_log_id());
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, info_mgr.find_unsafe(trans_id2, result));
    EXPECT_EQ(OB_SUCCESS, info_mgr.unlock(trans_id));
    i++;
  }
}

TEST(TestObTransResultInfoMgr, get_state)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObTransResultInfoMgr info_mgr;
  MyMockObPartitionService2 partition_service;
  ObPartitionKey pkey(2, 1, 100);
  int64_t log_id = 1;
  ObAddr addr_(IP_TYPE, LOCAL_IP, PORT);

  EXPECT_EQ(OB_SUCCESS, info_mgr.init(&partition_service, pkey));
  int i = 0;
  bool registered = false;
  while (i < 1000) {
    ObTransID trans_id(addr_);
    ObTransResultInfo* info = ObTransResultInfoFactory::alloc();
    info->init(ObTransResultState::COMMIT, ObClockGenerator::getClock(), log_id++, 0, trans_id);
    EXPECT_EQ(OB_SUCCESS, info_mgr.insert(info, registered));
    int state;
    EXPECT_EQ(OB_SUCCESS, info_mgr.get_state(trans_id, state));
    EXPECT_EQ(1, state);
    i++;
  }
  EXPECT_EQ(1000, info_mgr.get_result_info_count());
}

TEST(TestObTransResultInfoMgr, del)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObTransResultInfoMgr info_mgr;
  MyMockObPartitionService2 partition_service;
  ObPartitionKey pkey(2, 1, 100);
  int64_t log_id = 1;
  ObAddr addr_(IP_TYPE, LOCAL_IP, PORT);
  ObAddr addr2_(IP_TYPE, LOCAL_IP, PORT2);

  EXPECT_EQ(OB_SUCCESS, info_mgr.init(&partition_service, pkey));
  int i = 0;
  bool registered = false;
  while (i < 1000) {
    ObTransID trans_id(addr_);
    ObTransID trans_id2(addr2_);
    ObTransResultInfo* info = ObTransResultInfoFactory::alloc();
    info->init(ObTransResultState::COMMIT, ObClockGenerator::getClock(), log_id++, 0, trans_id);
    EXPECT_EQ(OB_SUCCESS, info_mgr.insert(info, registered));
    EXPECT_EQ(OB_SUCCESS, info_mgr.del(trans_id));
    EXPECT_EQ(OB_SUCCESS, info_mgr.del(trans_id));
    EXPECT_EQ(OB_SUCCESS, info_mgr.del(trans_id2));
    i++;
  }
  EXPECT_EQ(0, info_mgr.get_result_info_count());
}

TEST(TestObTransResultInfoMgr, get_min_log_id)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObTransResultInfoMgr info_mgr;
  MyMockObPartitionService2 partition_service;
  ObPartitionKey pkey(2, 1, 100);
  int64_t log_id = 20000;

  EXPECT_EQ(OB_SUCCESS, info_mgr.init(&partition_service, pkey));
  int i = 1;
  bool registered = false;
  while (i < 10000) {
    ObAddr addr_(IP_TYPE, LOCAL_IP, i);
    ObTransID trans_id(addr_);
    ObTransResultInfo* info = ObTransResultInfoFactory::alloc();
    info->init(ObTransResultState::COMMIT, ObClockGenerator::getClock(), log_id--, 0, trans_id);
    EXPECT_EQ(OB_SUCCESS, info_mgr.insert(info, registered));
    i++;
  }
  uint64_t min_log_id;
  int64_t unused;
  EXPECT_EQ(OB_SUCCESS, info_mgr.get_min_log(min_log_id, unused));
  EXPECT_EQ(log_id + 1, min_log_id);
}

TEST(TestObTransResultInfoMgr, update)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObTransResultInfoMgr info_mgr;
  MyMockObPartitionService2 partition_service;
  ObPartitionKey pkey(2, 1, 100);
  EXPECT_EQ(OB_SUCCESS, info_mgr.init(&partition_service, pkey));
  // int64_t min_log_id = -1;
  int i = 1;
  bool registered = false;
  while (i < 10000) {
    ObAddr addr_(IP_TYPE, LOCAL_IP, i);
    ObAddr addr2_(IP_TYPE, LOCAL_IP, PORT2);
    ObTransID trans_id(addr_);
    ObTransID trans_id2(addr2_);
    ObTransResultInfo* info = ObTransResultInfoFactory::alloc();
    info->init(ObTransResultState::UNKNOWN, INT64_MAX, i, 0, trans_id);
    EXPECT_EQ(OB_SUCCESS, info_mgr.insert(info, registered));
    EXPECT_EQ(OB_ENTRY_NOT_EXIST,
        info_mgr.update(ObTransResultState::COMMIT, ObClockGenerator::getClock(), i + 110, 0, trans_id2));
    EXPECT_EQ(
        OB_SUCCESS, info_mgr.update(ObTransResultState::COMMIT, ObClockGenerator::getClock(), i + 110, 0, trans_id));
    int state = -1;
    EXPECT_EQ(OB_SUCCESS, info_mgr.get_state(trans_id, state));
    EXPECT_EQ(1, state);
    i++;
  }
  // EXPECT_EQ(OB_SUCCESS, info_mgr.get_min_log_id(min_log_id));
  // EXPECT_EQ(111, min_log_id);
}

static void* thr_fn(void* arg)
{
  ObTransResultInfoMgr* info_mgr = static_cast<ObTransResultInfoMgr*>(arg);
  ObAddr addr_(IP_TYPE, LOCAL_IP, PORT);
  ObTransID trans_id(addr_);
  const int64_t commit_version = ObClockGenerator::getClock();
  static int64_t log_id = 110;
  bool registered = false;
  ObTransResultInfo* info = ObTransResultInfoFactory::alloc();
  info->init(ObTransResultState::COMMIT, commit_version, log_id++, 0, trans_id);
  EXPECT_EQ(OB_SUCCESS, info_mgr->insert(info, registered));
  ObTransID* trans_id_res = new ObTransID(trans_id);
  return (void*)trans_id_res;
}

TEST(TestObTransResultInfoMgr, concurrency_insert_and_get)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObTransResultInfoMgr info_mgr;
  MyMockObPartitionService2 partition_service;
  ObPartitionKey pkey(2, 1, 100);
  const int64_t THREAD_NUM = 100;
  pthread_t tids[THREAD_NUM];

  EXPECT_EQ(OB_SUCCESS, info_mgr.init(&partition_service, pkey));
  for (int i = 0; i < THREAD_NUM; i++) {
    EXPECT_EQ(0, pthread_create(&tids[i], NULL, thr_fn, &info_mgr));
  }

  for (int i = 0; i < THREAD_NUM; i++) {
    ObTransID* trans_id_res = NULL;
    int state = 0;
    void* tmp = static_cast<void*>(trans_id_res);
    EXPECT_EQ(0, pthread_join(tids[i], &tmp));
    trans_id_res = static_cast<ObTransID*>(tmp);
    EXPECT_EQ(OB_SUCCESS, info_mgr.get_state(*trans_id_res, state));
    EXPECT_EQ(1, state);
    uint64_t min_log_id = 0;
    int64_t unused = 0;
    EXPECT_EQ(OB_SUCCESS, info_mgr.get_min_log(min_log_id, unused));
    EXPECT_EQ(110, min_log_id);
    delete trans_id_res;
  }

  EXPECT_EQ(100, info_mgr.get_result_info_count());
}

}  // namespace unittest
}  // namespace oceanbase

using namespace oceanbase;
using namespace oceanbase::common;

int main(int argc, char** argv)
{
  int ret = 1;
  ObLogger& logger = ObLogger::get_logger();
  logger.set_file_name("test_ob_trans_result_info_mgr.log", true);
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
