// owner: zjf225077
// owner group: log

/*
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

#define private public
#define protected public
#include "env/ob_simple_log_cluster_env.h"
#undef private
#undef protected

const std::string TEST_NAME = "log_shared_storage_upload";

using namespace oceanbase::common;
using namespace oceanbase;
namespace oceanbase
{
using namespace logservice;
namespace unittest
{
class TestObSimpleSharedLogUpload : public ObSimpleLogClusterTestEnv
  {
  public:
    TestObSimpleSharedLogUpload() : ObSimpleLogClusterTestEnv()
    {
      int ret = init();
      if (OB_SUCCESS != ret) {
        throw std::runtime_error("TestObSimpleLogDiskMgr init failed");
      }
    }
    ~TestObSimpleSharedLogUpload()
    {
      destroy();
    }
    int init()
    {
      return OB_SUCCESS;
    }
    void destroy()
    {}
    int64_t id_;
  };

int64_t ObSimpleLogClusterTestBase::member_cnt_ = 3;
int64_t ObSimpleLogClusterTestBase::node_cnt_ = 3;
bool ObSimpleLogClusterTestBase::need_shared_storage_ = true;

std::string ObSimpleLogClusterTestBase::test_name_ = TEST_NAME;
bool ObSimpleLogClusterTestBase::need_add_arb_server_  = false;

TEST_F(TestObSimpleSharedLogUpload, basic)
{
  SET_CASE_LOG_FILE(TEST_NAME, "basic");
  ObSimpleLogServer *first_server = NULL;
  ObLogService *first_log_service = NULL;
  ObSimpleLogServer *secord_server = NULL;
  ObLogService *secord_log_service = NULL;
  int64_t first_leader_idx = 0;
  int64_t secord_leader_idx = 0;
  int64_t new_leader_idx = 0;
  int64_t first_id = ATOMIC_AAF(&palf_id_, 1);//ls_id:2
  int64_t secord_id = ATOMIC_AAF(&palf_id_, 1);//ls_id:3
  ObSharedLogService *first_shared_log = NULL;
  ObSharedLogService *secord_shared_log = NULL;
  ObSharedLogUploadHandler *first_handler = NULL;
  ObSharedLogUploadHandler *secord_handler = NULL;
  CLOG_LOG(INFO, "TEST_CASE-begin_prepare");
  {
    PalfHandleImplGuard first_leader;
    int64_t leader_idx = 0;
    share::SCN create_scn = share::SCN::base_scn();
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(first_id, create_scn, leader_idx, first_leader));
    CLOG_LOG(INFO, "TEST_CASE-begin_prepare-leader of first is:", K(leader_idx));
    ObISimpleLogServer *i_server = get_cluster()[leader_idx]; first_server = dynamic_cast<ObSimpleLogServer *>(i_server); first_log_service = &(first_server->log_service_);
    first_shared_log = first_log_service->get_shared_log_service();
    new_leader_idx = (leader_idx + 1) % 3;
    PalfHandleImplGuard secord_leader;
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(secord_id, create_scn, leader_idx, secord_leader));
    PalfHandleImplGuard new_leader;
    ASSERT_EQ(OB_SUCCESS, switch_leader(secord_id, new_leader_idx, new_leader));
  //normal scene
    ASSERT_EQ(OB_SUCCESS, first_shared_log->get_log_ss_handler(ObLSID(first_id), first_handler));
    EXPECT_EQ(OB_ENTRY_EXIST, first_shared_log->add_ls(ObLSID(first_id)));
    EXPECT_EQ(OB_ENTRY_EXIST, first_shared_log->add_ls(ObLSID(secord_id)));

    EXPECT_TRUE(first_handler->is_inited_);
    CLOG_LOG(INFO, "TEST_CASE-end_prepare");
    logservice::LogUploadCtx &ctx = first_handler->log_upload_ctx_;
    CLOG_LOG(INFO, "TEST_CASE-normal_case");
    EXPECT_EQ(OB_SUCCESS, submit_log_with_expected_size(first_leader, first_id, 2));
    sleep(10);
    while (LOG_INVALID_BLOCK_ID == ctx.max_block_id_on_ss_ || ctx.max_block_id_on_ss_ < 1) {
      sleep(2);
      CLOG_LOG(INFO, "wait for upload", K(ctx));
    }
    ASSERT_EQ(1, ctx.max_block_id_on_ss_);
    EXPECT_EQ(2, ctx.start_block_id_);
    EXPECT_EQ(2, ctx.end_block_id_);
    EXPECT_EQ(true, ctx.has_file_on_ss_);
    first_shared_log->revert_log_ss_handler(first_handler);
    CLOG_LOG(INFO, "TEST_CASE-normal_case_end");
  }

  {
   //switch leader
    CLOG_LOG(INFO, "TEST_CASE-switch_leader");
    PalfHandleImplGuard new_leader;
    PalfHandleImplGuard cur_leader;
    int64_t secord_leader_idx = 0;
    ASSERT_EQ(OB_SUCCESS, switch_leader(first_id, new_leader_idx, new_leader));
    EXPECT_EQ(OB_SUCCESS, get_leader(first_id, cur_leader, secord_leader_idx));
    CLOG_LOG(INFO, "TEST_CASE-switch_leader-leader of first is:", K(new_leader_idx), K(secord_leader_idx));
    ObISimpleLogServer *i_server = get_cluster()[secord_leader_idx];
    secord_server = dynamic_cast<ObSimpleLogServer *>(i_server);
    secord_log_service = &(secord_server->log_service_);
    secord_shared_log = secord_log_service->get_shared_log_service();
    EXPECT_EQ(OB_SUCCESS, secord_shared_log->get_log_ss_handler(ObLSID(first_id), first_handler));
    EXPECT_TRUE(first_handler->is_inited_);
    logservice::LogUploadCtx &ctx = first_handler->log_upload_ctx_;
    sleep (2);

    EXPECT_EQ(OB_SUCCESS, submit_log_with_expected_size(cur_leader, first_id, 2));
    while (ctx.max_block_id_on_ss_ != 3) {
      sleep(1);
      CLOG_LOG(INFO, "wait for upload after switch_leader", K(ctx));
    }
    EXPECT_EQ(3, ctx.max_block_id_on_ss_);
    EXPECT_EQ(4, ctx.start_block_id_);
    EXPECT_EQ(4, ctx.end_block_id_);
    secord_shared_log->revert_log_ss_handler(secord_handler);
    CLOG_LOG(INFO, "TEST_CASE-switch_leader-end");
  }

  {
    //restart scene
    CLOG_LOG(INFO, "TEST_CASE-restart");
    EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());
    PalfHandleImplGuard first_leader;
    EXPECT_EQ(OB_SUCCESS, get_leader(first_id, first_leader, first_leader_idx));
    sleep(5);

    CLOG_LOG(INFO, "after restart");
    ObISimpleLogServer *i_server = get_cluster()[first_leader_idx];
    first_server = dynamic_cast<ObSimpleLogServer *>(i_server);
    first_log_service = &(first_server->log_service_);
    first_shared_log = first_log_service->get_shared_log_service();
    EXPECT_EQ(OB_SUCCESS, first_shared_log->get_log_ss_handler(ObLSID(first_id), first_handler));
    EXPECT_TRUE(first_handler->is_inited_);
    logservice::LogUploadCtx &ctx = first_handler->log_upload_ctx_;
    while (!ctx.is_valid()) {
      sleep(1);
      CLOG_LOG(INFO, "wait for init ctx", K(ctx));
    }
    EXPECT_EQ(3, ctx.max_block_id_on_ss_);
    EXPECT_EQ(4, ctx.start_block_id_);
    EXPECT_EQ(4, ctx.end_block_id_);
    EXPECT_EQ(true, ctx.has_file_on_ss_);
    EXPECT_EQ(OB_SUCCESS, submit_log_with_expected_size(first_leader, first_id, 2));
    while (ctx.max_block_id_on_ss_ != 5) {
      sleep(1);
      CLOG_LOG(INFO, "wait for upload after restart", K(ctx));
    }
    EXPECT_EQ(5, ctx.max_block_id_on_ss_);
    EXPECT_EQ(6, ctx.start_block_id_);
    EXPECT_EQ(6, ctx.end_block_id_);

//secord ls
    PalfHandleImplGuard secord_leader;
    EXPECT_EQ(OB_SUCCESS, get_leader(secord_id, secord_leader, secord_leader_idx));
    sleep(5);

    i_server = get_cluster()[secord_leader_idx];
    secord_server = dynamic_cast<ObSimpleLogServer *>(i_server);
    secord_log_service = &(secord_server->log_service_);
    secord_shared_log = secord_log_service->get_shared_log_service();
    EXPECT_EQ(OB_SUCCESS, secord_shared_log->get_log_ss_handler(ObLSID(secord_id), secord_handler));
    EXPECT_TRUE(secord_handler->is_inited_);
    logservice::LogUploadCtx &ctx_1 = secord_handler->log_upload_ctx_;
    EXPECT_TRUE(ctx_1.is_valid());
    EXPECT_EQ(LOG_INVALID_BLOCK_ID, ctx_1.max_block_id_on_ss_);
    EXPECT_EQ(0, ctx_1.start_block_id_);
    EXPECT_EQ(0, ctx_1.end_block_id_);
    EXPECT_EQ(false, ctx_1.has_file_on_ss_);
    EXPECT_EQ(OB_SUCCESS, submit_log_with_expected_size(secord_leader, first_id, 2));
     while (ctx_1.max_block_id_on_ss_ != 1) {
      sleep(1);
      CLOG_LOG(INFO, "wait for upload after restart", K(ctx));
    }
    CLOG_LOG(INFO, "ctx for secord ls", K(ctx_1), K(secord_leader_idx));
    EXPECT_EQ(1, ctx_1.max_block_id_on_ss_);
    EXPECT_EQ(2, ctx_1.start_block_id_);
    EXPECT_EQ(2, ctx_1.end_block_id_);
    first_shared_log->revert_log_ss_handler(first_handler);
    secord_shared_log->revert_log_ss_handler(secord_handler);
    CLOG_LOG(INFO, "TEST_CASE-restart-end");
  }
  delete_paxos_group(first_id);
  delete_paxos_group(secord_id);
}


TEST_F(TestObSimpleSharedLogUpload, standby)
{
  SET_CASE_LOG_FILE(TEST_NAME, "standby");
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  const int64_t id_raw_write = ATOMIC_AAF(&palf_id_, 1);
  int64_t leader_idx = 0;
  int64_t raw_leader_idx = 0;
  {
    PalfHandleImplGuard leader;
    PalfHandleImplGuard leader_raw_write;
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(id_raw_write, raw_leader_idx, leader_raw_write));
    EXPECT_EQ(OB_SUCCESS, submit_log_with_expected_size(leader, id, 2));
    EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, LSN(2*PALF_BLOCK_SIZE)));
    CLOG_LOG(INFO, "switch to raw_write");
    int64_t cur_time = common::ObTimeUtility::current_time();
    SCN replayable_scn;
    EXPECT_EQ(OB_SUCCESS, replayable_scn.convert_from_ts(cur_time));

    ObLogService *raw_write_log_service = NULL;
    ObISimpleLogServer *i_server = get_cluster()[raw_leader_idx];
    ObSimpleLogServer *raw_write_server = dynamic_cast<ObSimpleLogServer *>(i_server);
    raw_write_log_service = &(raw_write_server->log_service_);
    ObSharedLogService *shared_log_service = raw_write_log_service->get_shared_log_service();
    EXPECT_EQ(OB_SUCCESS, raw_write_log_service->update_replayable_point(replayable_scn));
    EXPECT_EQ(OB_SUCCESS, submit_log_with_expected_size(leader, id, 2));
    EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, LSN(2*PALF_BLOCK_SIZE)));

    EXPECT_EQ(OB_SUCCESS, change_access_mode_to_raw_write(leader_raw_write));
    EXPECT_EQ(OB_ITER_END, read_and_submit_group_log(leader, leader_raw_write));
    ObSharedLogUploadHandler *shared_log_handler = NULL;
    ASSERT_EQ(OB_SUCCESS, shared_log_service->get_log_ss_handler(ObLSID(id_raw_write),
                                                                 shared_log_handler));

    EXPECT_TRUE(shared_log_handler->is_inited_);
    logservice::LogUploadCtx &ctx = shared_log_handler->log_upload_ctx_;
    while (ctx.max_block_id_on_ss_ != 1) {
      sleep(1);
      CLOG_LOG(INFO, "wait for upload ", K(ctx));
    }
    sleep(10);
    EXPECT_TRUE(ctx.is_valid());
    EXPECT_EQ(1, ctx.max_block_id_on_ss_);
    EXPECT_EQ(2, ctx.start_block_id_);
    EXPECT_EQ(2, ctx.end_block_id_);
    EXPECT_EQ(true, ctx.has_file_on_ss_);

    CLOG_LOG(INFO, "TEST-update replayable_scn ", K(ctx));
    cur_time = common::ObTimeUtility::current_time();
    EXPECT_EQ(OB_SUCCESS, replayable_scn.convert_from_ts(cur_time));
    EXPECT_EQ(OB_SUCCESS, raw_write_log_service->update_replayable_point(replayable_scn));

    while (ctx.max_block_id_on_ss_ != 3) {
      sleep(1);
      CLOG_LOG(INFO, "wait for upload ", K(ctx));
    }
    EXPECT_EQ(3, ctx.max_block_id_on_ss_);
    EXPECT_EQ(4, ctx.start_block_id_);
    EXPECT_EQ(4, ctx.end_block_id_);
    shared_log_service->revert_log_ss_handler(shared_log_handler);
  }
  delete_paxos_group(id);
  delete_paxos_group(id_raw_write);
}

TEST_F(TestObSimpleSharedLogUpload, test_upload)
{
  SET_CASE_LOG_FILE(TEST_NAME, "test_upload");
  ObSimpleLogServer *first_server = NULL;
  ObLogService *first_log_service = NULL;
  int64_t first_leader_idx = 0;
  int64_t new_leader_idx = 0;
  int64_t first_id = ATOMIC_AAF(&palf_id_, 1);//ls_id:2
  ObSharedLogService *first_shared_log = NULL;
  ObSharedLogUploadHandler *first_handler = NULL;
  CLOG_LOG(INFO, "TEST_CASE-begin_prepare");
  {
    PalfHandleImplGuard first_leader;
    int64_t leader_idx = 0;
    share::SCN create_scn = share::SCN::base_scn();
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(first_id, create_scn, leader_idx, first_leader));
    CLOG_LOG(INFO, "TEST_CASE-begin_prepare-leader of first is:", K(leader_idx));
    ObISimpleLogServer *i_server = get_cluster()[leader_idx];
    first_server = dynamic_cast<ObSimpleLogServer *>(i_server);
    first_log_service = &(first_server->log_service_);
    first_shared_log = first_log_service->get_shared_log_service();
    new_leader_idx = (leader_idx + 1) % 3;
  //normal scene
    ASSERT_EQ(OB_SUCCESS, first_shared_log->get_log_ss_handler(ObLSID(first_id), first_handler));
    EXPECT_EQ(OB_ENTRY_EXIST, first_shared_log->add_ls(ObLSID(first_id)));

    EXPECT_TRUE(first_handler->is_inited_);
    CLOG_LOG(INFO, "TEST_CASE-end_prepare");
    logservice::LogUploadCtx &ctx = first_handler->log_upload_ctx_;
    CLOG_LOG(INFO, "TEST_CASE-normal_case");
    EXPECT_EQ(OB_SUCCESS, submit_log_with_expected_size(first_leader, first_id, 2));
    sleep(10);
    while (LOG_INVALID_BLOCK_ID == ctx.max_block_id_on_ss_ || ctx.max_block_id_on_ss_ < 1) {
      sleep(2);
      CLOG_LOG(INFO, "wait for upload", K(ctx));
    }
    ASSERT_EQ(1, ctx.max_block_id_on_ss_);
    EXPECT_EQ(2, ctx.start_block_id_);
    EXPECT_EQ(2, ctx.end_block_id_);
    EXPECT_EQ(true, ctx.has_file_on_ss_);
    first_shared_log->revert_log_ss_handler(first_handler);
    CLOG_LOG(INFO, "TEST_CASE-normal_case_end");
    PalfIterator<LogEntry> iterator;
    EXPECT_EQ(OB_SUCCESS, seek_log_iterator(ObLSID(first_id), LSN(0), iterator));
    first_leader.palf_handle_impl_->log_engine_.log_storage_.block_mgr_.min_block_id_ =
      first_leader.palf_handle_impl_->log_engine_.log_storage_.block_mgr_.max_block_id_;
    first_leader.palf_handle_impl_->log_engine_.log_storage_.log_cache_ = nullptr;
    int ret = OB_SUCCESS;
    LSN curr_lsn;
    LogEntry curr_entry;
    LSN expected_lsn((ctx.max_block_id_on_ss_ + 1) * PALF_BLOCK_SIZE);
    while (OB_SUCC(iterator.next())) {
      EXPECT_EQ(OB_SUCCESS, iterator.get_entry(curr_entry, curr_lsn));
    }
    EXPECT_EQ(OB_ERR_OUT_OF_LOWER_BOUND, ret);
    EXPECT_EQ(expected_lsn, curr_lsn+curr_entry.get_serialize_size());
  }
  delete_paxos_group(first_id);
}


} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  RUN_SIMPLE_LOG_CLUSTER_TEST(TEST_NAME);
}
