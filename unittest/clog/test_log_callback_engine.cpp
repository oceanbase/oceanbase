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

//This file is for the unit test of log_callback_engine and the related	
//threadpool and handler.

#include <gtest/gtest.h>

#include "clog/ob_log_callback_engine.h"
#include "clog/ob_log_callback_task.h"
#include "clog/ob_log_callback_thread_pool.h"
#include "clog/ob_log_callback_handler.h"
#include "clog/ob_log_define.h"
//#include "storage/ob_replay_status.h"
#include "storage/ob_partition_component_factory.h"
#include "storage/ob_i_partition_group.h"
#include "common/storage/ob_freeze_define.h"
#include "../storage/mockcontainer/mock_ob_partition.h"
#include "../storage/mockcontainer/mock_ob_partition_service.h"
#include "common/ob_queue_thread.h"
#include "common/ob_partition_key.h"
#include "share/ob_errno.h"
#include "lib/net/ob_addr.h"
#include "lib/allocator/ob_concurrent_fifo_allocator.h"
#include "lib/utility/ob_tracepoint.h"
#include "gtest/gtest.h"
#include "share/ob_i_ps_cb.h"
#include "clog_mock_container/mock_ps_cb.h"
namespace oceanbase
{

using namespace common;
using namespace clog;
using namespace storage;
namespace storage
{
class ObPartitionComponentFactory;
}

namespace unittest
{
class MockPartition: public MockObIPartition
{
public:
  MockPartition() {partition_key_.init(1, 1, 1);}
  ~MockPartition() {}
  const common::ObPartitionKey &get_partition_key() const
  {
    return partition_key_;
  }
  virtual int get_safe_publish_version(int64_t& publish_version)
  {
    UNUSED(publish_version);
    return 0;
  }
private:
  ObPartitionKey partition_key_;
  ObReplayStatus relay_ststus_;
  //Uncommenting this line will cause coverity to report an error, if necessary,
  //please perform related initialization operations.
  //  ObPartitionMCState partition_mc_state_;
};

class MockPartitionMgr : public MockObIPartitionService
{
public:
  MockPartitionMgr(): is_inited_(true) {}
  int get_partition(const ObPartitionKey &partition_key,
                    ObIPartitionGroup *&partition)
  {
    int ret = OB_SUCCESS;
    if (partition_key == mock_partition_.get_partition_key()) {
      partition = &mock_partition_;
    } else {
      ret = OB_ENTRY_NOT_EXIST;
    }
    return ret;
  }
private:
  bool is_inited_;
  MockPartition mock_partition_;
};

//class MockObPSCb : public share::ObIPSCb
//{
//public:
//  int on_leader_revoke(const ObPartitionKey &partition_key)
//  {
//    UNUSED(partition_key);
//    return OB_SUCCESS;
//  }
//  int on_leader_takeover(const ObPartitionKey &partition_key)
//  {
//    UNUSED(partition_key);
//    return OB_SUCCESS;
//
//  }
//  int on_leader_active(const ObPartitionKey &partition_key)
//  {
//    UNUSED(partition_key);
//    return OB_SUCCESS;
//
//  }
//  int on_member_change_success(
//      const ObPartitionKey &partition_key,
//      const int64_t mc_timestamp,
//      const ObMemberList &prev_member_list,
//      const ObMemberList &curr_member_list)
//  {
//    UNUSED(partition_key);
//    UNUSED(mc_timestamp);
//    UNUSED(prev_member_list);
//    UNUSED(curr_member_list);
//    return OB_SUCCESS;
//  }
//  int64_t get_min_using_file_id() const { return 0; }
//};
class ObTestLogCallback : public ::testing::Test
{
public :
  void SetUp()
  {
    int ret = OB_SUCCESS;

    const uint64_t TABLE_ID = 198;
    const int32_t PARTITION_IDX = 125;
    const int32_t PARTITION_CNT = 168;
    const int64_t TOTAL_LIMIT = 1 << 30;
    const int64_t HOLD_LIMIT = 1 << 29;
    const int64_t PAGE_SIZE = 1 << 16;
    const char *ip = "127.0.0.1";
    const int32_t PORT = 80;

    if (OB_SUCCESS != (ret = partition_key_.init(TABLE_ID, PARTITION_IDX, PARTITION_CNT))) {
      CLOG_LOG(ERROR, "partition_key_.init failed");
    }
    if (OB_SUCCESS != (ret = allocator_.init(TOTAL_LIMIT, HOLD_LIMIT, PAGE_SIZE))) {
      CLOG_LOG(ERROR, "allocator_.init failed");
    }
    self_addr_.set_ip_addr(ip, PORT);
  }
  void TearDown()
  {
    allocator_.destroy();
  }

  MockPartitionMgr partition_service_;
  common::ObAddr self_addr_ ;
  common::ObConcurrentFIFOAllocator allocator_;
  common::ObPartitionKey partition_key_;
  MockObPSCb partition_service_cb_;
  clog::ObLogCallbackThreadPool log_callback_thread_pool_;
  clog::ObLogCallbackThreadPool worker_thread_pool_;
  clog::ObLogCallbackThreadPool sp_thread_pool_;
};// class ObTestLogCallback

TEST_F(ObTestLogCallback, init_test)
{
  const int64_t THREAD_NUM = 15;
  const int64_t TASK_LIMIT_NUM = 12;
  clog::ObLogCallbackEngine log_callback_engine;
  clog::ObLogCallbackHandler log_callback_handler;

  //test the init method of ObLogCallbackHandler
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_callback_handler.init(NULL, NULL));
  EXPECT_EQ(OB_SUCCESS, log_callback_handler.init(&partition_service_, &partition_service_cb_));
  EXPECT_EQ(OB_INIT_TWICE, log_callback_handler.init(&partition_service_, &partition_service_cb_));

  //test the init method of ObLogCallbackThreadPool
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_callback_thread_pool_.init(NULL, THREAD_NUM, TASK_LIMIT_NUM,
                                                                self_addr_));
  TP_SET_ERROR("ob_log_callback_thread_pool_.cpp", "init", "test_a", OB_ERROR);
  //  EXPECT_EQ(OB_ERROR, log_callback_thread_pool_.init(&log_callback_handler, THREAD_NUM,
  //                                                     TASK_LIMIT_NUM,
  //                                                     self_addr_));
  TP_SET("ob_log_callback_thread_pool_.cpp", "init", "test_a", NULL);
  EXPECT_EQ(OB_SUCCESS, log_callback_thread_pool_.init(&log_callback_handler, THREAD_NUM,
                                                       TASK_LIMIT_NUM, self_addr_));
  EXPECT_EQ(OB_INIT_TWICE, log_callback_thread_pool_.init(&log_callback_handler, THREAD_NUM,
                                                          TASK_LIMIT_NUM, self_addr_));

  //test the init method of ObLogCallbackEngine
  worker_thread_pool_.init(&log_callback_handler, THREAD_NUM, TASK_LIMIT_NUM, self_addr_);
  sp_thread_pool_.init(&log_callback_handler, THREAD_NUM, TASK_LIMIT_NUM, self_addr_);
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_callback_engine.init(NULL, NULL));
  EXPECT_EQ(OB_SUCCESS, log_callback_engine.init(&worker_thread_pool_, &sp_thread_pool_));
  EXPECT_EQ(OB_INIT_TWICE, log_callback_engine.init(&worker_thread_pool_, &sp_thread_pool_));
  sleep(10);

  log_callback_engine.destroy();
  log_callback_thread_pool_.destroy();
  worker_thread_pool_.destroy();
  sp_thread_pool_.destroy();
}

//TEST_F(ObTestLogCallback, submit_flush_cb_task_test)
//{
//  const uint64_t LOG_ID = 190;
//  const int64_t MC_TIMESTAMP = 865;
//  const common::ObProposalID PROPOSAL_ID;
//  const int64_t THREAD_NUM = 15;
//  const int64_t TASK_LIMIT_NUM = 12;
//  bool NEED_ACK = false;
//
//  clog::ObLogCallbackEngine log_callback_engine;
//  clog::ObLogCallbackThreadPool log_callback_thread_pool_;
//  clog::ObLogCallbackHandler log_callback_handler;
//  clog::ObLogCallbackThreadPool worker_thread_pool_;
//  clog::ObLogCallbackThreadPool sp_thread_pool_;
//
//  ObLogType type = OB_LOG_MEMBER_CHANGE;
//  clog::ObLogCursor log_cursor;
//  log_cursor.file_id_ = 0;
//  log_cursor.offset_ = 0;
//  log_cursor.size_ = 0;
//  clog::ObLogFlushCbArg flush_cb_arg(type, LOG_ID, MC_TIMESTAMP, PROPOSAL_ID, NEED_ACK, self_addr_,
//                                     log_cursor, 0);
//
//  EXPECT_EQ(OB_NOT_INIT, log_callback_engine.submit_flush_cb_task(partition_key_, flush_cb_arg));
//  ASSERT_EQ(OB_SUCCESS, log_callback_handler.init(&partition_service_, &partition_service_cb_));
//  ASSERT_EQ(OB_SUCCESS, log_callback_thread_pool_.init(&log_callback_handler, THREAD_NUM,
//                                                      TASK_LIMIT_NUM, self_addr_));
//  worker_thread_pool_.init(&log_callback_handler, THREAD_NUM, TASK_LIMIT_NUM, self_addr_);
//  sp_thread_pool_.init(&log_callback_handler, THREAD_NUM, TASK_LIMIT_NUM, self_addr_);
//  ASSERT_EQ(OB_SUCCESS, log_callback_engine.init(&worker_thread_pool_, &sp_thread_pool_));
//  EXPECT_EQ(OB_SUCCESS, log_callback_engine.submit_flush_cb_task(partition_key_, flush_cb_arg));
//
//  sleep(10);
//  log_callback_engine.destroy();
//  log_callback_thread_pool_.destroy();
//  worker_thread_pool_.destroy();
//  sp_thread_pool_.destroy();
//}
TEST_F(ObTestLogCallback, submit_member_change_success_cb_task_test)
{
  const int64_t THREAD_NUM = 15;
  const int64_t TASK_LIMIT_NUM = 12;
  clog::ObLogCallbackEngine log_callback_engine;
  clog::ObLogCallbackHandler log_callback_handler;
  const int64_t MC_TIMESTAMP = 64;
  const common::ObMemberList PREV_MEMBER_LIST;
  const common::ObMemberList CURR_MEMBER_LIST;

  EXPECT_EQ(OB_NOT_INIT, log_callback_engine.submit_member_change_success_cb_task(partition_key_,
                                                                                  MC_TIMESTAMP, PREV_MEMBER_LIST, CURR_MEMBER_LIST));
  ASSERT_EQ(OB_SUCCESS, log_callback_handler.init(&partition_service_, &partition_service_cb_));
  ASSERT_EQ(OB_SUCCESS, log_callback_thread_pool_.init(&log_callback_handler, THREAD_NUM,
                                                       TASK_LIMIT_NUM, self_addr_));
  worker_thread_pool_.init(&log_callback_handler, THREAD_NUM, TASK_LIMIT_NUM, self_addr_);
  sp_thread_pool_.init(&log_callback_handler, THREAD_NUM, TASK_LIMIT_NUM, self_addr_);
  ASSERT_EQ(OB_SUCCESS, log_callback_engine.init(&worker_thread_pool_, &sp_thread_pool_));
  EXPECT_EQ(OB_SUCCESS, log_callback_engine.submit_member_change_success_cb_task(partition_key_,
                                                                                 MC_TIMESTAMP, PREV_MEMBER_LIST, CURR_MEMBER_LIST));

  sleep(10);
  log_callback_engine.destroy();
  log_callback_thread_pool_.destroy();
  worker_thread_pool_.destroy();
  sp_thread_pool_.destroy();
}

//TEST_F(ObTestLogCallback, submit_leader_takeover_cb_task_test)
//{
//  const int64_t THREAD_NUM = 15;
//  const int64_t TASK_LIMIT_NUM = 12;
//  clog::ObLogCallbackEngine log_callback_engine;
//  clog::ObLogCallbackThreadPool log_callback_thread_pool_;
//  clog::ObLogCallbackHandler log_callback_handler;
//  clog::ObLogCallbackThreadPool worker_thread_pool_;
//  clog::ObLogCallbackThreadPool sp_thread_pool_;
//
//  EXPECT_EQ(OB_NOT_INIT, log_callback_engine.submit_leader_takeover_cb_task(partition_key_));
//  ASSERT_EQ(OB_SUCCESS, log_callback_handler.init(&partition_service_, &partition_service_cb_));
//  ASSERT_EQ(OB_SUCCESS, log_callback_thread_pool_.init(&log_callback_handler, THREAD_NUM,
//                                                      TASK_LIMIT_NUM, self_addr_));
//  worker_thread_pool_.init(&log_callback_handler, THREAD_NUM, TASK_LIMIT_NUM, self_addr_);
//  sp_thread_pool_.init(&log_callback_handler, THREAD_NUM, TASK_LIMIT_NUM, self_addr_);
//  ASSERT_EQ(OB_SUCCESS, log_callback_engine.init(&worker_thread_pool_, &sp_thread_pool_));
//  EXPECT_EQ(OB_SUCCESS, log_callback_engine.submit_leader_takeover_cb_task(partition_key_));
//
//  sleep(10);
//  log_callback_engine.destroy();
//  log_callback_thread_pool_.destroy();
//  worker_thread_pool_.destroy();
//  sp_thread_pool_.destroy();
//}
//TEST_F(ObTestLogCallback, submit_leader_revoke_cb_task_test)
//{
//  const int64_t THREAD_NUM = 15;
//  const int64_t TASK_LIMIT_NUM = 12;
//  clog::ObLogCallbackEngine log_callback_engine;
//  clog::ObLogCallbackThreadPool log_callback_thread_pool_;
//  clog::ObLogCallbackHandler log_callback_handler;
//  clog::ObLogCallbackThreadPool worker_thread_pool_;
//  clog::ObLogCallbackThreadPool sp_thread_pool_;
//
//  EXPECT_EQ(OB_NOT_INIT, log_callback_engine.submit_leader_revoke_cb_task(partition_key_));
//  ASSERT_EQ(OB_SUCCESS, log_callback_handler.init(&partition_service_, &partition_service_cb_));
//  ASSERT_EQ(OB_SUCCESS, log_callback_thread_pool_.init(&log_callback_handler, THREAD_NUM,
//                                                      TASK_LIMIT_NUM, self_addr_));
//  worker_thread_pool_.init(&log_callback_handler, THREAD_NUM, TASK_LIMIT_NUM, self_addr_);
//  sp_thread_pool_.init(&log_callback_handler, THREAD_NUM, TASK_LIMIT_NUM, self_addr_);
//  ASSERT_EQ(OB_SUCCESS, log_callback_engine.init(&worker_thread_pool_, &sp_thread_pool_));
//  EXPECT_EQ(OB_SUCCESS, log_callback_engine.submit_leader_revoke_cb_task(partition_key_));
//
//  sleep(10);
//  log_callback_engine.destroy();
//  log_callback_thread_pool_.destroy();
//  worker_thread_pool_.destroy();
//  sp_thread_pool_.destroy();
//}

}//namespace unittest
}//namespace oceanbase
int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_log_callback_engine.log", true);
  OB_LOGGER.set_log_level("INFO");
  CLOG_LOG(INFO, "begin unittest: test_ob_log_callback_engine");

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
