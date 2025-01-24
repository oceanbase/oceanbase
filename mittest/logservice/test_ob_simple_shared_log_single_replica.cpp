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
#include "close_modules/shared_storage/log/ob_shared_log_utils.h"

const std::string TEST_NAME = "log_shared_storage_single_replica";

using namespace oceanbase::common;
using namespace oceanbase;
namespace oceanbase
{
using namespace logservice;
namespace unittest
{
class TestObSimpleSharedLogSingleReplica : public ObSimpleLogClusterTestEnv
  {
  public:
    TestObSimpleSharedLogSingleReplica() : ObSimpleLogClusterTestEnv()
    {
      int ret = init();
      if (OB_SUCCESS != ret) {
        throw std::runtime_error("TestObSimpleSharedLogSingleReplica init failed");
      }
    }
    ~TestObSimpleSharedLogSingleReplica()
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

int64_t ObSimpleLogClusterTestBase::member_cnt_ = 1;
int64_t ObSimpleLogClusterTestBase::node_cnt_ = 1;
bool ObSimpleLogClusterTestBase::need_shared_storage_ = true;

std::string ObSimpleLogClusterTestBase::test_name_ = TEST_NAME;
bool ObSimpleLogClusterTestBase::need_add_arb_server_  = false;

void init_log_handler(PalfHandleImplGuard &leader, ObLogHandler &log_handler)
{
  PalfHandle palf_handle;
  palf_handle.palf_handle_impl_ = leader.palf_handle_impl_;
  log_handler.palf_handle_ = palf_handle;
  log_handler.id_ = leader.palf_id_;
  log_handler.is_inited_ = true;
  log_handler.is_in_stop_state_ = false;
}

bool operator==(const PalfBaseInfo &lhs, const PalfBaseInfo &rhs)
{
  return lhs.prev_log_info_ == rhs.prev_log_info_ && lhs.curr_lsn_ == rhs.curr_lsn_;
}


TEST_F(TestObSimpleSharedLogSingleReplica, test_shared_log_interface)
{
  SET_CASE_LOG_FILE(TEST_NAME, "test_shared_log_interface");
  CLOG_LOG(INFO, "TEST_CASE-test_get_begin_lsn");
  ObISimpleLogServer *i_server = get_cluster()[0];
  ObSimpleLogServer *log_server = dynamic_cast<ObSimpleLogServer *>(i_server);
  ObLogService *log_service = &(log_server->log_service_);
  CLOG_LOG(INFO, "TEST_CASE-test_get_begin_lsn");
  {
    int64_t id = ATOMIC_AAF(&palf_id_, 1);
    {
    int64_t leader_idx = 0;
    PalfHandleImplGuard leader;
    ObLogHandler log_handler;
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
    ObSharedLogService *shared_log_service = log_service->get_shared_log_service();
    ObSharedLogUploadHandler *shared_log_handler = nullptr;
    EXPECT_EQ(OB_SUCCESS, shared_log_service->get_log_ss_handler(ObLSID(id), shared_log_handler));
    LogUploadCtx &ctx = shared_log_handler->log_upload_ctx_;
    init_log_handler(leader, log_handler);
    LSN begin_lsn, base_lsn;
    CLOG_LOG(INFO, "CASE1 test initial lsn");
    EXPECT_EQ(OB_SUCCESS, log_handler.get_begin_lsn(begin_lsn));
    EXPECT_EQ(LSN(PALF_INITIAL_LSN_VAL), begin_lsn);

    CLOG_LOG(INFO, "CASE2 test after upload");
    EXPECT_EQ(OB_SUCCESS, submit_log_with_expected_size(leader, leader_idx, 1));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, leader_idx, 1024));
    while (ctx.max_block_id_on_ss_ != 0) {
      sleep(1);
      CLOG_LOG(INFO, "wait for upload", K(ctx));
    }
    EXPECT_EQ(OB_SUCCESS, log_handler.get_begin_lsn(begin_lsn));
    EXPECT_EQ(LSN(PALF_INITIAL_LSN_VAL), begin_lsn);

    CLOG_LOG(INFO, "CASE3 test after delete local");
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->delete_block(0));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->set_base_lsn(LSN(PALF_BLOCK_SIZE)));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, leader_idx, 1024));
    EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, leader.palf_handle_impl_->get_max_lsn()));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_begin_lsn(begin_lsn));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_base_lsn(base_lsn));
    EXPECT_EQ(LSN(PALF_BLOCK_SIZE), begin_lsn);
    EXPECT_EQ(begin_lsn, base_lsn);
    EXPECT_EQ(OB_SUCCESS, log_handler.get_begin_lsn(begin_lsn));
    EXPECT_EQ(LSN(PALF_INITIAL_LSN_VAL), begin_lsn);

    CLOG_LOG(INFO, "CASE4 test after delete shared");
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::delete_blocks(MTL_ID(), ObLSID(id), 0, 1));
    block_id_t oldest_block;
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, ObSharedLogUtils::get_oldest_block(MTL_ID(), ObLSID(id), oldest_block));
    EXPECT_EQ(OB_SUCCESS, log_handler.get_begin_lsn(begin_lsn));
    EXPECT_EQ(LSN(PALF_BLOCK_SIZE), begin_lsn);

    shared_log_service->revert_log_ss_handler(shared_log_handler);
    }
    delete_paxos_group(id);
  }

  CLOG_LOG(INFO, "TEST_CASE-get_palf_base_info");
  {
    int64_t id = ATOMIC_AAF(&palf_id_, 1);
    {
    int64_t leader_idx = 0;
    PalfHandleImplGuard leader;
    ObLogHandler log_handler;
    PalfBaseInfo default_palf_base_info;
    default_palf_base_info.generate_by_default();
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
    ObSharedLogService *shared_log_service = log_service->get_shared_log_service();
    ObSharedLogUploadHandler *shared_log_handler = nullptr;
    EXPECT_EQ(OB_SUCCESS, shared_log_service->get_log_ss_handler(ObLSID(id), shared_log_handler));
    LogUploadCtx &ctx = shared_log_handler->log_upload_ctx_;
    init_log_handler(leader, log_handler);
    PalfBaseInfo base_info;
    CLOG_LOG(INFO, "CASE1 test initial palf_base_info");
    EXPECT_EQ(OB_SUCCESS, log_handler.get_palf_base_info(LSN(PALF_INITIAL_LSN_VAL), base_info));
    EXPECT_EQ(true, operator==(default_palf_base_info, base_info));
    CLOG_LOG(INFO, "CASE1 result", K(default_palf_base_info), K(base_info));

    CLOG_LOG(INFO, "CASE2 test after upload");
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, leader_idx, 1024));
    LSN tmp_lsn = leader.palf_handle_impl_->get_max_lsn();
    wait_until_has_committed(leader, tmp_lsn);
    PalfBaseInfo tmp_base_info;
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_base_info(tmp_lsn, tmp_base_info));
    EXPECT_EQ(OB_SUCCESS, submit_log_with_expected_size(leader, leader_idx, 1));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, leader_idx, 1024));
    while (ctx.max_block_id_on_ss_ != 0) {
      sleep(1);
      CLOG_LOG(INFO, "wait for upload", K(ctx));
    }

    CLOG_LOG(INFO, "CASE3 test after set base lsn");
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->set_base_lsn(LSN(PALF_BLOCK_SIZE)));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, leader_idx, 1024));
    EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, leader.palf_handle_impl_->get_max_lsn()));
    EXPECT_EQ(OB_SUCCESS, log_handler.get_palf_base_info(LSN(PALF_INITIAL_LSN_VAL), base_info));
    EXPECT_EQ(true, operator==(default_palf_base_info, base_info));

    CLOG_LOG(INFO, "CASE4 test after delete block");
    LSN begin_lsn, base_lsn;
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->delete_block(0));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_begin_lsn(begin_lsn));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_base_lsn(base_lsn));
    EXPECT_EQ(LSN(PALF_BLOCK_SIZE), begin_lsn);
    EXPECT_EQ(begin_lsn, base_lsn);
    EXPECT_EQ(OB_SUCCESS, log_handler.get_palf_base_info(LSN(PALF_INITIAL_LSN_VAL), base_info));
    EXPECT_EQ(true, operator==(default_palf_base_info, base_info));
    LSN local_begin_lsn;
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_begin_lsn(local_begin_lsn));
    EXPECT_NE(local_begin_lsn, LSN(PALF_INITIAL_LSN_VAL));
    EXPECT_EQ(OB_SUCCESS, log_handler.get_palf_base_info(LSN(tmp_lsn), base_info));
    EXPECT_EQ(true, operator==(default_palf_base_info, base_info));

    CLOG_LOG(INFO, "CASE5 test after delete shared");
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::delete_blocks(MTL_ID(), ObLSID(id), 0, 1));
    block_id_t oldest_block;
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, ObSharedLogUtils::get_oldest_block(MTL_ID(), ObLSID(id), oldest_block));
    // LSN(0)时，会产生默认的PalfBaseInfo
    EXPECT_EQ(OB_SUCCESS, log_handler.get_palf_base_info(LSN(PALF_INITIAL_LSN_VAL), base_info));
    EXPECT_EQ(OB_ERR_OUT_OF_LOWER_BOUND, log_handler.get_palf_base_info(LSN(PALF_BLOCK_SIZE), base_info));

    shared_log_service->revert_log_ss_handler(shared_log_handler);
    }
    delete_paxos_group(id);
  }

  CLOG_LOG(INFO, "TEST_CASE-test_locate_by_lsn");
  {
    int64_t id = ATOMIC_AAF(&palf_id_, 1);
    {
    int64_t leader_idx = 0;
    PalfHandleImplGuard leader;
    ObLogHandler log_handler;
    PalfBaseInfo default_palf_base_info;
    default_palf_base_info.generate_by_default();
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
    ObSharedLogService *shared_log_service = log_service->get_shared_log_service();
    ObSharedLogUploadHandler *shared_log_handler = nullptr;
    EXPECT_EQ(OB_SUCCESS, shared_log_service->get_log_ss_handler(ObLSID(id), shared_log_handler));
    LogUploadCtx &ctx = shared_log_handler->log_upload_ctx_;
    init_log_handler(leader, log_handler);
    SCN local_result_scn;
    CLOG_LOG(INFO, "CASE1 test initial lsn");
    EXPECT_EQ(OB_ERR_OUT_OF_UPPER_BOUND, log_handler.locate_by_lsn_coarsely(LSN(PALF_INITIAL_LSN_VAL), local_result_scn));

    CLOG_LOG(INFO, "CASE2 test after upload");
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, leader_idx, 1024));
    LSN tmp_lsn = leader.palf_handle_impl_->get_max_lsn();
    wait_until_has_committed(leader, tmp_lsn);
    PalfBaseInfo tmp_base_info;
    EXPECT_EQ(OB_SUCCESS, log_handler.locate_by_lsn_coarsely(LSN(PALF_INITIAL_LSN_VAL), local_result_scn));
    EXPECT_EQ(OB_SUCCESS, submit_log_with_expected_size(leader, leader_idx, 1));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, leader_idx, 1024));
    while (ctx.max_block_id_on_ss_ != 0) {
      sleep(1);
      CLOG_LOG(INFO, "wait for upload", K(ctx));
    }

    CLOG_LOG(INFO, "CASE3 test after set base lsn");
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->set_base_lsn(LSN(PALF_BLOCK_SIZE)));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, leader_idx, 1024));
    EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, leader.palf_handle_impl_->get_max_lsn()));
    EXPECT_EQ(OB_SUCCESS, log_handler.locate_by_lsn_coarsely(LSN(PALF_INITIAL_LSN_VAL), local_result_scn));

    SCN result_scn;
    CLOG_LOG(INFO, "CASE4 test after delete block");
    LSN begin_lsn, base_lsn;
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->delete_block(0));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_begin_lsn(begin_lsn));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_base_lsn(base_lsn));
    EXPECT_EQ(LSN(PALF_BLOCK_SIZE), begin_lsn);
    EXPECT_EQ(begin_lsn, base_lsn);
    EXPECT_EQ(OB_SUCCESS, log_handler.locate_by_lsn_coarsely(LSN(PALF_INITIAL_LSN_VAL), result_scn));
    EXPECT_EQ(local_result_scn, result_scn);

    CLOG_LOG(INFO, "CASE5 test after delete shared");
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::delete_blocks(MTL_ID(), ObLSID(id), 0, 1));
    block_id_t oldest_block;
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, ObSharedLogUtils::get_oldest_block(MTL_ID(), ObLSID(id), oldest_block));
    EXPECT_EQ(OB_ERR_OUT_OF_LOWER_BOUND, log_handler.locate_by_lsn_coarsely(LSN(PALF_INITIAL_LSN_VAL), result_scn));
    EXPECT_EQ(OB_SUCCESS, log_handler.locate_by_lsn_coarsely(LSN(PALF_BLOCK_SIZE), result_scn));

    shared_log_service->revert_log_ss_handler(shared_log_handler);
    }
    delete_paxos_group(id);
  }

  CLOG_LOG(INFO, "TEST_CASE-test_locate_by_scn");
  {
    int64_t id = ATOMIC_AAF(&palf_id_, 1);
    {
    int64_t leader_idx = 0;
    PalfHandleImplGuard leader;
    ObLogHandler log_handler;
    PalfBaseInfo default_palf_base_info;
    default_palf_base_info.generate_by_default();
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
    ObSharedLogService *shared_log_service = log_service->get_shared_log_service();
    ObSharedLogUploadHandler *shared_log_handler = nullptr;
    EXPECT_EQ(OB_SUCCESS, shared_log_service->get_log_ss_handler(ObLSID(id), shared_log_handler));
    LogUploadCtx &ctx = shared_log_handler->log_upload_ctx_;
    init_log_handler(leader, log_handler);
    LSN local_result_lsn;
    CLOG_LOG(INFO, "CASE1 test initial scn");
    EXPECT_EQ(OB_ERR_OUT_OF_LOWER_BOUND, log_handler.locate_by_scn_coarsely(SCN::min_scn(), local_result_lsn));
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, log_handler.locate_by_scn_coarsely(SCN::max_scn(), local_result_lsn));

    CLOG_LOG(INFO, "CASE2 test after upload");
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, leader_idx, 1024));
    LSN tmp_lsn = leader.palf_handle_impl_->get_max_lsn();
    SCN tmp_scn = leader.palf_handle_impl_->get_max_scn();
    wait_until_has_committed(leader, tmp_lsn);
    PalfBaseInfo tmp_base_info;
    EXPECT_EQ(OB_SUCCESS, log_handler.locate_by_scn_coarsely(tmp_scn, local_result_lsn));
    EXPECT_EQ(local_result_lsn, LSN(PALF_INITIAL_LSN_VAL));
    EXPECT_EQ(OB_SUCCESS, submit_log_with_expected_size(leader, leader_idx, 1));
    while (ctx.max_block_id_on_ss_ != 0) {
      sleep(1);
      CLOG_LOG(INFO, "wait for upload", K(ctx));
    }

    CLOG_LOG(INFO, "CASE3 test after set base lsn");
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->set_base_lsn(LSN(PALF_BLOCK_SIZE)));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, leader_idx, 1024));
    EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, leader.palf_handle_impl_->get_max_lsn()));
    EXPECT_EQ(OB_SUCCESS, log_handler.locate_by_scn_coarsely(tmp_scn, local_result_lsn));
    EXPECT_EQ(LSN(PALF_INITIAL_LSN_VAL), local_result_lsn);

    LSN result_lsn;
    CLOG_LOG(INFO, "CASE4 test after delete block");
    LSN begin_lsn, base_lsn;
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->delete_block(0));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_begin_lsn(begin_lsn));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_base_lsn(base_lsn));
    EXPECT_EQ(LSN(PALF_BLOCK_SIZE), begin_lsn);
    EXPECT_EQ(begin_lsn, base_lsn);
    EXPECT_EQ(OB_ERR_OUT_OF_LOWER_BOUND, log_handler.palf_handle_.locate_by_scn_coarsely(tmp_scn, result_lsn));
    EXPECT_EQ(OB_SUCCESS, log_handler.locate_by_scn_coarsely(tmp_scn, result_lsn));
    EXPECT_EQ(local_result_lsn, result_lsn);

    CLOG_LOG(INFO, "CASE5 test after delete shared");
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::delete_blocks(MTL_ID(), ObLSID(id), 0, 1));
    block_id_t oldest_block;
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, ObSharedLogUtils::get_oldest_block(MTL_ID(), ObLSID(id), oldest_block));
    EXPECT_EQ(OB_ERR_OUT_OF_LOWER_BOUND, log_handler.locate_by_scn_coarsely(tmp_scn, result_lsn));

    shared_log_service->revert_log_ss_handler(shared_log_handler);
    }
    delete_paxos_group(id);
  }
  CLOG_LOG(INFO, "TEST_CASE-test_advance_base_lsn");
  {
    int64_t id = ATOMIC_AAF(&palf_id_, 1);
    {
    int64_t leader_idx = 0;
    PalfHandleImplGuard leader;
    ObLogHandler log_handler;
    PalfBaseInfo default_palf_base_info;
    default_palf_base_info.generate_by_default();
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
    ObSharedLogService *shared_log_service = log_service->get_shared_log_service();
    ObSharedLogUploadHandler *shared_log_handler = nullptr;
    EXPECT_EQ(OB_SUCCESS, shared_log_service->get_log_ss_handler(ObLSID(id), shared_log_handler));
    LogUploadCtx &ctx = shared_log_handler->log_upload_ctx_;
    init_log_handler(leader, log_handler);
    LSN local_result_lsn;
    // advance_base_lsn do nothing on shared storage;
    EXPECT_EQ(OB_SUCCESS, log_handler.advance_base_lsn(LSN(PALF_BLOCK_SIZE)));
    LSN base_lsn;
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_base_lsn(base_lsn));
    EXPECT_EQ(LSN(PALF_INITIAL_LSN_VAL), base_lsn);
    }
    delete_paxos_group(id);
  }
  CLOG_LOG(INFO, "TEST_CASE-interface for restore tenant or migrate");
  {
    int64_t id = ATOMIC_AAF(&palf_id_, 1);
    {
    int64_t leader_idx = 0;
    PalfHandleImplGuard leader;
    ObLogHandler log_handler;
    PalfBaseInfo default_palf_base_info;
    const LSN curr_lsn = LSN(10*PALF_BLOCK_SIZE);
    const LSN prev_lsn = LSN(10*PALF_BLOCK_SIZE-200);
    default_palf_base_info.generate_by_default();
    default_palf_base_info.curr_lsn_ = curr_lsn;
    SCN curr_scn; curr_scn.convert_for_sql(ObTimeUtility::current_time());
    default_palf_base_info.prev_log_info_.lsn_ = prev_lsn;
    default_palf_base_info.prev_log_info_.log_id_ = 9999;
    default_palf_base_info.prev_log_info_.scn_ = curr_scn;
    default_palf_base_info.prev_log_info_.log_proposal_id_ = 100;
    default_palf_base_info.prev_log_info_.accum_checksum_ = 12012312;

    EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, default_palf_base_info, leader_idx, leader));
    init_log_handler(leader, log_handler);
    ObSharedLogService *shared_log_service = log_service->get_shared_log_service();
    ObSharedLogUploadHandler *shared_log_handler = nullptr;
    SCN locate_scn;
    EXPECT_EQ(OB_SUCCESS, shared_log_service->get_log_ss_handler(ObLSID(id), shared_log_handler));
    shared_log_service->file_upload_mgr_.stop();
    shared_log_service->file_upload_mgr_.wait();
    LogUploadCtx &ctx = shared_log_handler->log_upload_ctx_;
    PalfBaseInfo curr_base_info;
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_base_info(curr_lsn, curr_base_info));
    EXPECT_EQ(curr_lsn,leader.palf_handle_impl_->get_max_lsn());
    EXPECT_EQ(curr_base_info.prev_log_info_, default_palf_base_info.prev_log_info_);
    EXPECT_EQ(OB_ERR_OUT_OF_LOWER_BOUND, log_handler.locate_by_lsn_coarsely(curr_lsn, locate_scn));
    // palf内部会将传入的lsn替换为committed_end_lsn
    EXPECT_EQ(OB_ERR_OUT_OF_LOWER_BOUND, log_handler.locate_by_lsn_coarsely(curr_lsn+2*PALF_BLOCK_SIZE, locate_scn));
    EXPECT_EQ(OB_SUCCESS, submit_log_with_expected_size(leader, id, 3));
    EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, leader.palf_handle_impl_->get_max_lsn()));
    LSN local_result_lsn;
    // advance_base_lsn do nothing on shared storage;
    EXPECT_EQ(OB_SUCCESS, log_handler.advance_base_lsn(LSN(12 * PALF_BLOCK_SIZE)));
    LSN base_lsn;
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_base_lsn(base_lsn));
    EXPECT_EQ(LSN(10*PALF_BLOCK_SIZE), base_lsn);

    curr_base_info.reset();
    EXPECT_EQ(OB_SUCCESS, log_handler.get_palf_base_info(curr_lsn, curr_base_info));
    EXPECT_EQ(curr_base_info.prev_log_info_, default_palf_base_info.prev_log_info_);
    EXPECT_EQ(true, operator==(default_palf_base_info, curr_base_info));
    LogInfo log_info;
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_prev_log_info_for_fetch_(prev_lsn, curr_lsn, log_info));
    EXPECT_EQ(log_info, default_palf_base_info.prev_log_info_);

    EXPECT_EQ(OB_SUCCESS, log_handler.locate_by_lsn_coarsely(curr_lsn, locate_scn));
    EXPECT_EQ(OB_SUCCESS, log_handler.locate_by_lsn_coarsely(curr_lsn+PALF_BLOCK_SIZE, locate_scn));
    shared_log_service->file_upload_mgr_.start();
    CLOG_LOG(INFO, "runlin trace for upload", K(ctx));
    while (!(is_valid_block_id(ctx.max_block_id_on_ss_)) || ctx.max_block_id_on_ss_ < 12 ) {
      sleep(1);
      CLOG_LOG(INFO, "wait for upload", K(ctx));
    }
    EXPECT_EQ(OB_SUCCESS, log_handler.palf_handle_.palf_handle_impl_->set_base_lsn(LSN(12 * PALF_BLOCK_SIZE)));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, id, 1024));
    EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, leader.palf_handle_impl_->get_max_lsn()));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_base_lsn(base_lsn));
    EXPECT_NE(LSN(10*PALF_BLOCK_SIZE), base_lsn);
    curr_base_info.reset();
    EXPECT_EQ(OB_SUCCESS, log_handler.get_palf_base_info(curr_lsn, curr_base_info));
    EXPECT_EQ(curr_base_info.prev_log_info_, default_palf_base_info.prev_log_info_);
    EXPECT_EQ(true, operator==(default_palf_base_info, curr_base_info));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_prev_log_info_for_fetch_(prev_lsn, curr_lsn, log_info));
    EXPECT_EQ(log_info, default_palf_base_info.prev_log_info_);

    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->delete_block(10));
    EXPECT_EQ(OB_SUCCESS, log_handler.locate_by_lsn_coarsely(curr_lsn, locate_scn));
    EXPECT_EQ(OB_SUCCESS, log_handler.locate_by_lsn_coarsely(curr_lsn+PALF_BLOCK_SIZE, locate_scn));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->delete_block(11));
    EXPECT_EQ(OB_SUCCESS, log_handler.locate_by_lsn_coarsely(curr_lsn, locate_scn));
    EXPECT_EQ(OB_SUCCESS, log_handler.locate_by_lsn_coarsely(curr_lsn+PALF_BLOCK_SIZE, locate_scn));
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::delete_blocks(MTL_ID(), ObLSID(id), 10, 11));
    EXPECT_EQ(OB_ERR_OUT_OF_LOWER_BOUND, log_handler.locate_by_lsn_coarsely(curr_lsn, locate_scn));
    EXPECT_EQ(OB_SUCCESS, log_handler.locate_by_lsn_coarsely(curr_lsn+PALF_BLOCK_SIZE, locate_scn));
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::delete_blocks(MTL_ID(), ObLSID(id), 10, 12));
    EXPECT_EQ(OB_ERR_OUT_OF_LOWER_BOUND, log_handler.locate_by_lsn_coarsely(curr_lsn, locate_scn));
    EXPECT_EQ(OB_ERR_OUT_OF_LOWER_BOUND, log_handler.locate_by_lsn_coarsely(curr_lsn+PALF_BLOCK_SIZE, locate_scn));
    EXPECT_EQ(OB_SUCCESS, log_handler.get_palf_base_info(curr_lsn, curr_base_info));
    EXPECT_EQ(curr_base_info.prev_log_info_, default_palf_base_info.prev_log_info_);
    EXPECT_EQ(true, operator==(default_palf_base_info, curr_base_info));

    }
    delete_paxos_group(id);
  }
}

TEST_F(TestObSimpleSharedLogSingleReplica, test_shared_log_upload)
{
  SET_CASE_LOG_FILE(TEST_NAME, "test_shared_log_upload");
  CLOG_LOG(INFO, "TEST_CASE-test_upload_after_restore");
  ObISimpleLogServer *i_server = get_cluster()[0];
  ObSimpleLogServer *log_server = dynamic_cast<ObSimpleLogServer *>(i_server);
  ObLogService *log_service = &(log_server->log_service_);
  {
    int64_t id = ATOMIC_AAF(&palf_id_, 1);
    {
    int64_t leader_idx = 0;
    PalfHandleImplGuard leader;
    ObLogHandler log_handler;
    PalfBaseInfo default_palf_base_info;
    default_palf_base_info.generate_by_default();
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
    ObSharedLogService *shared_log_service = log_service->get_shared_log_service();
    ObSharedLogUploadHandler *shared_log_handler = nullptr;
    EXPECT_EQ(OB_SUCCESS, shared_log_service->get_log_ss_handler(ObLSID(id), shared_log_handler));
    LogUploadCtx &ctx = shared_log_handler->log_upload_ctx_;
    init_log_handler(leader, log_handler);
    EXPECT_EQ(OB_SUCCESS, submit_log_with_expected_size(leader, leader_idx, 2));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, id, 1024));
    wait_until_has_committed(leader, leader.palf_handle_impl_->get_max_lsn());
    while (ctx.max_block_id_on_ss_ != 1) {
      sleep(1);
      CLOG_LOG(INFO, "wait for upload", K(ctx));
    }
    LSN begin_lsn;
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_begin_lsn(begin_lsn));
    EXPECT_EQ(LSN(PALF_INITIAL_LSN_VAL), begin_lsn);
    EXPECT_EQ(OB_SUCCESS, log_handler.advance_base_lsn(LSN(2*PALF_BLOCK_SIZE)));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, id, 1024));
    wait_until_has_committed(leader, leader.palf_handle_impl_->get_max_lsn());
    LSN base_lsn;
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_base_lsn(base_lsn));
    EXPECT_EQ(LSN(2*PALF_BLOCK_SIZE), base_lsn);
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->delete_block(0));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_begin_lsn(begin_lsn));
    EXPECT_EQ(LSN(PALF_BLOCK_SIZE), begin_lsn);
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->delete_block(1));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_begin_lsn(begin_lsn));
    EXPECT_EQ(LSN(2*PALF_BLOCK_SIZE), begin_lsn);
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::delete_blocks(MTL_ID(), ObLSID(id), 0, 1));
    block_id_t oldest_block_id;
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::get_oldest_block(MTL_ID(), ObLSID(id), oldest_block_id));
    EXPECT_EQ(1, oldest_block_id);
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::delete_blocks(MTL_ID(), ObLSID(id), 1, 2));
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, ObSharedLogUtils::get_oldest_block(MTL_ID(), ObLSID(id), oldest_block_id));
    }
    {
    PalfHandleImplGuard leader;
    int64_t leader_idx;
    EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());
    EXPECT_EQ(OB_SUCCESS, get_leader(id, leader, leader_idx));
    LSN begin_lsn;
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_begin_lsn(begin_lsn));
    EXPECT_EQ(LSN(2*PALF_BLOCK_SIZE), begin_lsn);
    block_id_t oldest_block_id;
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, ObSharedLogUtils::get_oldest_block(MTL_ID(), ObLSID(id), oldest_block_id));
    EXPECT_EQ(OB_SUCCESS, submit_log_with_expected_size(leader, leader_idx, 1));
    ObSharedLogService *shared_log_service = log_service->get_shared_log_service();
    ObSharedLogUploadHandler *shared_log_handler = nullptr;
    EXPECT_EQ(OB_SUCCESS, shared_log_service->get_log_ss_handler(ObLSID(id), shared_log_handler));
    LogUploadCtx &ctx = shared_log_handler->log_upload_ctx_;
    while (ctx.max_block_id_on_ss_ != 2) {
      sleep(1);
      CLOG_LOG(INFO, "wait for upload", K(ctx));
    }
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::get_oldest_block(MTL_ID(), ObLSID(id), oldest_block_id));
    EXPECT_EQ(2, oldest_block_id);
    }
    delete_paxos_group(id);
  }
  CLOG_LOG(INFO, "TEST_CASE-test_upload_after_rebuild");
  {
    int64_t id = ATOMIC_AAF(&palf_id_, 1);
    {
    int64_t leader_idx = 0;
    PalfHandleImplGuard leader;
    ObLogHandler log_handler;
    PalfBaseInfo default_palf_base_info;
    default_palf_base_info.generate_by_default();
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
    ObSharedLogService *shared_log_service = log_service->get_shared_log_service();
    ObSharedLogUploadHandler *shared_log_handler = nullptr;
    EXPECT_EQ(OB_SUCCESS, shared_log_service->get_log_ss_handler(ObLSID(id), shared_log_handler));
    LogUploadCtx &ctx = shared_log_handler->log_upload_ctx_;
    init_log_handler(leader, log_handler);
    EXPECT_EQ(OB_SUCCESS, submit_log_with_expected_size(leader, leader_idx, 2));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, id, 1024));
    wait_until_has_committed(leader, leader.palf_handle_impl_->get_max_lsn());
    while (ctx.max_block_id_on_ss_ != 1) {
      sleep(1);
      CLOG_LOG(INFO, "wait for upload", K(ctx));
    }
    LSN begin_lsn;
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_begin_lsn(begin_lsn));
    EXPECT_EQ(LSN(PALF_INITIAL_LSN_VAL), begin_lsn);
    EXPECT_EQ(OB_SUCCESS, log_handler.advance_base_lsn(LSN(PALF_BLOCK_SIZE)));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, id, 1024));
    wait_until_has_committed(leader, leader.palf_handle_impl_->get_max_lsn());
    LSN base_lsn;
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_base_lsn(base_lsn));
    EXPECT_EQ(LSN(2*PALF_BLOCK_SIZE), base_lsn);
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->delete_block(0));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_begin_lsn(begin_lsn));
    EXPECT_EQ(LSN(PALF_BLOCK_SIZE), begin_lsn);
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->delete_block(1));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_begin_lsn(begin_lsn));
    EXPECT_EQ(LSN(2*PALF_BLOCK_SIZE), begin_lsn);
    block_id_t oldest_block_id;
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::get_oldest_block(MTL_ID(), ObLSID(id), oldest_block_id));
    EXPECT_EQ(0, oldest_block_id);
    block_id_t newest_block_id;
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::get_newest_block(MTL_ID(), ObLSID(id), 1, newest_block_id));
    EXPECT_EQ(1, newest_block_id);
    }
    {
    PalfHandleImplGuard leader;
    int64_t leader_idx;
    EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());
    EXPECT_EQ(OB_SUCCESS, get_leader(id, leader, leader_idx));
    LSN begin_lsn;
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_begin_lsn(begin_lsn));
    EXPECT_EQ(LSN(2*PALF_BLOCK_SIZE), begin_lsn);
    block_id_t oldest_block_id;
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::get_oldest_block(MTL_ID(), ObLSID(id), oldest_block_id));
    EXPECT_EQ(0, oldest_block_id);
    EXPECT_EQ(OB_SUCCESS, submit_log_with_expected_size(leader, leader_idx, 1));
    ObSharedLogService *shared_log_service = log_service->get_shared_log_service();
    ObSharedLogUploadHandler *shared_log_handler = nullptr;
    EXPECT_EQ(OB_SUCCESS, shared_log_service->get_log_ss_handler(ObLSID(id), shared_log_handler));
    LogUploadCtx &ctx = shared_log_handler->log_upload_ctx_;
    while (ctx.max_block_id_on_ss_ != 2) {
      sleep(1);
      CLOG_LOG(INFO, "wait for upload", K(ctx));
    }
    block_id_t newest_block_id;
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::get_newest_block(MTL_ID(), ObLSID(id), 0, newest_block_id));
    EXPECT_EQ(2, newest_block_id);
    }
    delete_paxos_group(id);
  }
}

TEST_F(TestObSimpleSharedLogSingleReplica, test_log_cache_for_shared_storage)
{
  SET_CASE_LOG_FILE(TEST_NAME, "test_log_cache_for_shared_storage");
  OB_LOGGER.set_log_level("TRACE");
  ObISimpleLogServer *i_server = get_cluster()[0];
  ObSimpleLogServer *log_server = dynamic_cast<ObSimpleLogServer *>(i_server);
  ObLogService *log_service = &(log_server->log_service_);
  {
    // close hot cache
    disable_hot_cache_ = true;
    int64_t id = ATOMIC_AAF(&palf_id_, 1);
    int64_t leader_idx = 0;
    PalfHandleImplGuard leader;
    ObLogHandler log_handler;
    PalfBaseInfo default_palf_base_info;
    default_palf_base_info.generate_by_default();
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
    ObSharedLogService *shared_log_service = log_service->get_shared_log_service();
    ObSharedLogUploadHandler *shared_log_handler = nullptr;
    EXPECT_EQ(OB_SUCCESS, shared_log_service->get_log_ss_handler(ObLSID(id), shared_log_handler));
    LogUploadCtx &ctx = shared_log_handler->log_upload_ctx_;
    init_log_handler(leader, log_handler);
    EXPECT_EQ(OB_SUCCESS, submit_log_with_expected_size(leader, leader_idx, 1));
    EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, leader.palf_handle_impl_->get_max_lsn()));

    {
      CLOG_LOG(INFO, "case 1: totally read from local storage");
      PalfIterator<LogEntry> iterator;
      EXPECT_EQ(OB_SUCCESS, seek_log_iterator(ObLSID(id), LSN(0), iterator));
      LSN curr_lsn;
      LogEntry curr_entry;
      int ret = OB_SUCCESS;
      while (OB_SUCC(iterator.next())) {
        EXPECT_EQ(OB_SUCCESS, iterator.get_entry(curr_entry, curr_lsn));
      }
      EXPECT_EQ(OB_ITER_END, ret);
      EXPECT_EQ(false, ctx.has_file_on_ss_);
      // expected to fill cache
      EXPECT_NE(0, leader.palf_handle_impl_->log_engine_.log_storage_.log_cache_->cold_cache_.kv_cache_->store_size(1002));
    }

    {
      CLOG_LOG(INFO, "case 2: read from local storage and shared storage");
      EXPECT_EQ(OB_SUCCESS, submit_log_with_expected_size(leader, leader_idx, 2));
      EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, leader.palf_handle_impl_->get_max_lsn()));

      while (!ctx.has_file_on_ss_) {
        sleep(1);
        CLOG_LOG(INFO, "wait for uploading logs");
      }

      leader.palf_handle_impl_->log_engine_.log_storage_.log_cache_->cold_cache_.log_cache_stat_.reset();
      PalfIterator<LogEntry> iterator;
      EXPECT_EQ(OB_SUCCESS, seek_log_iterator(ObLSID(id), LSN(0), iterator));
      LSN curr_lsn;
      LogEntry curr_entry;
      int ret = OB_SUCCESS;
      while (OB_SUCC(iterator.next())) {
        EXPECT_EQ(OB_SUCCESS, iterator.get_entry(curr_entry, curr_lsn));
      }
      EXPECT_EQ(OB_ITER_END, ret);

      EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, leader_idx, 1024));
      while(2 != ctx.max_block_id_on_ss_) {
        sleep(1);
        CLOG_LOG(INFO, "wait for uploading logs");
      }
      leader.palf_handle_impl_->log_engine_.log_storage_.log_cache_->cold_cache_.log_cache_stat_.reset();
      PalfIterator<LogEntry> iterator1;
      EXPECT_EQ(OB_SUCCESS, seek_log_iterator(ObLSID(id), LSN(0), iterator1));
      ret = OB_SUCCESS;
      while (OB_SUCC(iterator1.next())) {
        EXPECT_EQ(OB_SUCCESS, iterator1.get_entry(curr_entry, curr_lsn));
      }
      EXPECT_EQ(OB_ITER_END, ret);
    }

    {
      CLOG_LOG(INFO, "case 3: totally read from shared storage");
      PalfIterator<LogEntry> iterator;
      EXPECT_EQ(OB_SUCCESS, seek_log_iterator(ObLSID(id), LSN(0), iterator));
      LSN expected_lsn(PALF_BLOCK_SIZE);
      iterator.iterator_storage_.get_file_end_lsn_ = []() {
        return LSN(PALF_BLOCK_SIZE);
      };
      LSN curr_lsn;
      LogEntry curr_entry;
      int ret = OB_SUCCESS;
      while (OB_SUCC(iterator.next())) {
        EXPECT_EQ(OB_SUCCESS, iterator.get_entry(curr_entry, curr_lsn));
      }
      EXPECT_EQ(OB_ITER_END, ret);
      EXPECT_EQ(expected_lsn, curr_lsn + curr_entry.get_serialize_size());
    }
  }

  {
    disable_hot_cache_ = false;
    CLOG_LOG(INFO, "case 4: test fill_cache_when_slide for upload");
    int64_t id = ATOMIC_AAF(&palf_id_, 1);
    int64_t leader_idx = 0;
    PalfHandleImplGuard leader;
    ObLogHandler log_handler;
    PalfBaseInfo default_palf_base_info;
    default_palf_base_info.generate_by_default();
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
    ObSharedLogService *shared_log_service = log_service->get_shared_log_service();
    ObSharedLogUploadHandler *shared_log_handler = nullptr;
    EXPECT_EQ(OB_SUCCESS, shared_log_service->get_log_ss_handler(ObLSID(id), shared_log_handler));
    LogUploadCtx &ctx = shared_log_handler->log_upload_ctx_;
    init_log_handler(leader, log_handler);
    sleep(3);
    // make sure that only record stat about upload
    leader.palf_handle_impl_->log_engine_.log_storage_.log_cache_->cold_cache_.log_cache_stat_.reset();
    EXPECT_EQ(OB_SUCCESS, submit_log_with_expected_size(leader, leader_idx, 3));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, leader_idx, 1024));
    EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, leader.palf_handle_impl_->get_max_lsn()));

    while (2 != ctx.max_block_id_on_ss_) {
      sleep(1);
      CLOG_LOG(INFO, "wait for uploading logs");
    }
  }
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  RUN_SIMPLE_LOG_CLUSTER_TEST(TEST_NAME);
}
