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

#include "clog/ob_log_task.h"
#include <gtest/gtest.h>
#include "lib/allocator/ob_concurrent_fifo_allocator.h"
#include "share/allocator/ob_tenant_mutil_allocator.h"
#include "clog/ob_clog_config.h"
#include "clog/ob_clog_mgr.h"
#include "clog/ob_partition_log_service.h"
#include "clog_mock_container/mock_submit_log_cb.h"
#include "clog_mock_container/mock_log_sliding_window.h"
namespace oceanbase {
using namespace common;
namespace clog {
class MockSlidingCallBack : public MockObSlidingCallBack {
public:
  MockSlidingCallBack() : err_no_(OB_SUCCESS)
  {}
  virtual ~MockSlidingCallBack()
  {}
  int sliding_cb(const ObLogTask& log_task)
  {
    UNUSED(log_task);
    return err_no_;
  }
  void set_err_no(const int err_no)
  {
    err_no_ = err_no;
  }

private:
  int err_no_;
};
}  // namespace clog
namespace unittest {
class ObLogTaskTest : public ::testing::Test {
protected:
  ObLogTaskTest() : log_task_(NULL), alloc_mgr_(NULL)
  {}
  virtual ~ObLogTaskTest()
  {}

protected:
  virtual void SetUp()
  {
    const uint64_t TABLE_ID = 15;
    const uint32_t PARTITION_IDX = 5;
    const int32_t PARTITION_CNT = 3;
    partition_key_.init(TABLE_ID, PARTITION_IDX, PARTITION_CNT);
    const uint64_t tenant_id = OB_SERVER_TENANT_ID;

    ObMemAttr attr(tenant_id, ObModIds::OB_TENANT_MUTIL_ALLOCATOR);
    void* buf = ob_malloc(sizeof(common::ObTenantMutilAllocator), attr);
    if (NULL == buf) {
      CLOG_LOG(WARN, "alloc memory failed");
      OB_ASSERT(FALSE);
    }
    alloc_mgr_ = new (buf) common::ObTenantMutilAllocator(tenant_id);
    if (NULL == alloc_mgr_) {
      CLOG_LOG(WARN, "alloc_mgr_ construct failed");
      OB_ASSERT(FALSE);
    }
    if (NULL != (log_task_ = static_cast<clog::ObLogTask*>(alloc_mgr_->alloc_log_task_buf()))) {
      new (log_task_) clog::ObLogTask();
    } else {
      CLOG_LOG(INFO, "log_task_ init fail");
      OB_ASSERT(false);
    }
  }
  virtual void TearDown()
  {
    ob_free(alloc_mgr_);
    alloc_mgr_ = NULL;
  }

protected:
  clog::ObLogTask* log_task_;
  clog::MockSlidingCallBack mock_sliding_cb_;
  common::ObTenantMutilAllocator* alloc_mgr_;
  common::ObPartitionKey partition_key_;
  common::ObMemberList curr_member_list_;
  common::ObVersion freeze_version_;
};

// test for init, reset and destroy
TEST_F(ObLogTaskTest, init_reset_destroy_test)
{
  // test for init and reset
  const int64_t REPLICA_NUM = 3;
  const bool need_replay = true;
  clog::ObISubmitLogCb* submit_cb = NULL;

  EXPECT_EQ(OB_SUCCESS, log_task_->init(submit_cb, REPLICA_NUM, need_replay));

  // log_task_ -> reset();
  // EXPECT_EQ(OB_SUCCESS, log_task_ -> init(alloc_mgr_, &mock_sliding_cb_, submit_cb,REPLICA_NUM, curr_member_list_));

  OB_ASSERT(log_task_ != NULL);
  log_task_->destroy();
}
// set_log
TEST_F(ObLogTaskTest, set_log_test)
{
  clog::ObLogEntryHeader header;
  clog::ObConfirmedInfo confirmed_info;
  const ObProposalID PROPOSAL_ID;
  const uint64_t LOG_ID = 150;
  const char* BUF = "abc";
  const int64_t DATA_LEN = 18;
  const int64_t GENERATION_TIMESTAMP = 1000;
  const int64_t EPOCH_ID = 800;
  const int64_t SUBMIT_TS = 1300;
  const int64_t REPLICA_NUM = 3;
  const bool need_replay = true;
  clog::ObISubmitLogCb* submit_cb = NULL;
  ASSERT_EQ(OB_SUCCESS, log_task_->init(submit_cb, REPLICA_NUM, need_replay));

  const char* buff = "This is a test log";
  ASSERT_EQ(OB_SUCCESS,
      header.generate_header(clog::OB_LOG_SUBMIT,
          partition_key_,
          LOG_ID,
          BUF,
          DATA_LEN,
          GENERATION_TIMESTAMP,
          EPOCH_ID,
          PROPOSAL_ID,
          SUBMIT_TS,
          freeze_version_,
          true));
  EXPECT_EQ(OB_SUCCESS, log_task_->set_log(header, buff, true));
  log_task_->set_log_confirmed();
  log_task_->set_confirmed_info(confirmed_info);

  EXPECT_TRUE(log_task_->is_submit_log_exist());
  EXPECT_TRUE(log_task_->is_confirmed_info_exist());
  EXPECT_TRUE(log_task_->is_submit_log_body_exist());
  log_task_->destroy();
}

// log_cursor
TEST_F(ObLogTaskTest, log_cursor_test)
{
  const int64_t REPLICA_NUM = 3;
  const bool need_replay = true;
  clog::ObISubmitLogCb* submit_cb = NULL;
  ASSERT_EQ(OB_SUCCESS, log_task_->init(submit_cb, REPLICA_NUM, need_replay));
  clog::ObLogCursor log_cursor;
  log_cursor.file_id_ = 1;
  log_cursor.offset_ = 1000;
  log_task_->set_log_cursor(log_cursor);
  log_task_->get_log_cursor();
  log_task_->destroy();
}

TEST_F(ObLogTaskTest, cb_test)
{
  const int64_t REPLICA_NUM = 3;
  const bool need_replay = true;
  clog::ObISubmitLogCb* submit_cb = NULL;
  ASSERT_EQ(OB_SUCCESS, log_task_->init(submit_cb, REPLICA_NUM, need_replay));
  common::ObPartitionKey pkey;
  uint64_t log_id = 0;
  const bool batch_committed = false;
  const bool batch_last_succeed = false;
  EXPECT_EQ(OB_SUCCESS, log_task_->submit_log_succ_cb(pkey, log_id, batch_committed, batch_last_succeed));
  clog::MockObSubmitLogCb submit_cb_obj;
  // log_task_ -> reset();
  // ASSERT_EQ(OB_SUCCESS, log_task_ -> init(alloc_mgr_, &mock_sliding_cb_, &submit_cb_obj,
  // REPLICA_NUM,curr_member_list_)); EXPECT_EQ(OB_SUCCESS, log_task_ -> submit_log_succ_cb()); EXPECT_TRUE(log_task_ ->
  // is_on_success_cb_called()); log_task_ -> destroy();
}

TEST_F(ObLogTaskTest, ack_majority_test)
{
  common::ObAddr member1;
  common::ObAddr member2;
  common::ObAddr member3;
  common::ObAddr member4;  // not include
  member1.parse_from_cstring("127.0.0.1:8000");
  member2.parse_from_cstring("127.0.0.1:8001");
  member3.parse_from_cstring("127.0.0.1:8002");
  curr_member_list_.add_server(member1);
  curr_member_list_.add_server(member2);
  curr_member_list_.add_server(member3);

  EXPECT_EQ(0, log_task_->ack_log(member3));
  const int64_t REPLICA_NUM = 3;
  const bool need_replay = true;
  clog::ObISubmitLogCb* submit_cb = NULL;
  ASSERT_EQ(OB_SUCCESS, log_task_->init(submit_cb, REPLICA_NUM, need_replay));
  EXPECT_EQ(OB_SUCCESS, log_task_->ack_log(member1));
  EXPECT_EQ(OB_SUCCESS, log_task_->ack_log(member2));
  EXPECT_EQ(OB_SUCCESS, log_task_->ack_log(member3));

  log_task_->set_flush_local_finished();
  log_task_->set_flush_local_finished();
  EXPECT_TRUE(log_task_->try_set_majority_finished());
  ASSERT_TRUE(log_task_->is_majority_finished());
  log_task_->destroy();
}

TEST_F(ObLogTaskTest, on_pop_test)
{
  mock_sliding_cb_.set_err_no(OB_SUCCESS);
  const int64_t REPLICA_NUM = 3;
  const bool need_replay = true;
  clog::ObISubmitLogCb* submit_cb = NULL;
  ASSERT_EQ(OB_SUCCESS, log_task_->init(submit_cb, REPLICA_NUM, need_replay));
  // EXPECT_EQ(OB_SUCCESS, log_task_ -> on_pop());
  log_task_->destroy();
}

TEST_F(ObLogTaskTest, misc_test)
{
  const int64_t REPLICA_NUM = 3;
  const bool need_replay = false;
  clog::ObISubmitLogCb* submit_cb = NULL;
  clog::ObConfirmedInfo confirmed_info;
  ASSERT_EQ(OB_SUCCESS, log_task_->init(submit_cb, REPLICA_NUM, need_replay));

  // CONFIRM_SUBMITTED
  log_task_->set_log_confirmed();
  ASSERT_TRUE(log_task_->is_log_confirmed());

  // can be removed = CONFIRM_SUBMITTED + LOCAL_FLUSHED
  EXPECT_FALSE(log_task_->can_be_removed());
  log_task_->set_confirmed_info(confirmed_info);
  log_task_->set_index_log_submitted();
  EXPECT_TRUE(log_task_->can_be_removed());

  clog::ObILogExtRingBufferData* log_task = NULL;
  ASSERT_FALSE(log_task_->can_overwrite(log_task));

  ASSERT_FALSE(log_task_->need_replay());
  log_task_->try_set_need_replay(false);
  ASSERT_FALSE(log_task_->need_replay());
  log_task_->try_set_need_replay(true);
  ASSERT_TRUE(log_task_->need_replay());

  // reset_state
  log_task_->reset_state(false);

  // reset_log_cursor
  log_task_->reset_log_cursor();
  log_task_->destroy();
}

}  // namespace unittest
}  // namespace oceanbase
int main(int argc, char** argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
