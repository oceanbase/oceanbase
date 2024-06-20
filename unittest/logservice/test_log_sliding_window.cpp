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

#include "lib/ob_errno.h"
#include "logservice/palf/log_writer_utils.h"
#include "logservice/palf/log_io_task_cb_utils.h"
#include <gtest/gtest.h>
#include <random>
#include <string>
#define private public
#include "logservice/palf/log_sliding_window.h"
#include "mock_logservice_container/mock_log_config_mgr.h"
#include "mock_logservice_container/mock_log_mode_mgr.h"
#include "mock_logservice_container/mock_log_engine.h"
#include "mock_logservice_container/mock_log_state_mgr.h"
#include "mock_logservice_container/mock_palf_fs_cb_wrapper.h"
#include "share/allocator/ob_tenant_mutil_allocator.h"
#undef private
#include "logservice/palf/palf_options.h"

namespace oceanbase
{
using namespace common;
using namespace palf;
using namespace share;

namespace unittest
{

class TestLogSlidingWindow : public ::testing::Test
{
public:
  TestLogSlidingWindow();
  virtual ~TestLogSlidingWindow();
public:
  virtual void SetUp();
  virtual void TearDown();
public:
  class MockPublicLogSlidingWindow : public LogSlidingWindow
  {
  public:
    MockPublicLogSlidingWindow() {}
    virtual ~MockPublicLogSlidingWindow() {}
    virtual bool is_handle_thread_lease_expired(const int64_t thread_lease_begin_ts) const override final
    {
      UNUSED(thread_lease_begin_ts);
      return false;
    }
  };
  class MockLocCb : public PalfLocationCacheCb
  {
    virtual int get_leader(const int64_t id, common::ObAddr &leader)
    {
      UNUSED(id);
      UNUSED(leader);
      return OB_SUCCESS;
    }
    virtual int nonblock_get_leader(const int64_t id, common::ObAddr &leader)
    {
      UNUSED(id);
      UNUSED(leader);
      return OB_SUCCESS;
    }
    virtual int nonblock_renew_leader(const int64_t id)
    {
      UNUSED(id);
      return OB_SUCCESS;
    }
  };
public:
  int64_t palf_id_;
  common::ObAddr self_;
  MockLogStateMgr mock_state_mgr_;
  MockLogConfigMgr mock_mm_;
  MockLogModeMgr mock_mode_mgr_;
  MockLogEngine mock_log_engine_;
  MockPalfFSCbWrapper palf_fs_cb_;
  ObTenantMutilAllocator *alloc_mgr_;
  LogPlugins *plugins_;
  MockPublicLogSlidingWindow log_sw_;
  char *data_buf_;
};

TestLogSlidingWindow::TestLogSlidingWindow() {}
TestLogSlidingWindow::~TestLogSlidingWindow() {}

const static uint64_t tenant_id = 1001;
void TestLogSlidingWindow::SetUp()
{
  palf_id_ = 1001;
  self_.set_ip_addr("127.0.0.1", 12345);

  int ret = ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(tenant_id);
  OB_ASSERT(OB_SUCCESS == ret);
  ObMemAttr attr(tenant_id, ObModIds::OB_TENANT_MUTIL_ALLOCATOR);
  void *buf = ob_malloc(sizeof(common::ObTenantMutilAllocator), attr);
  if (NULL == buf) {
    CLOG_LOG_RET(WARN, OB_ALLOCATE_MEMORY_FAILED, "alloc memory failed");
    OB_ASSERT(FALSE);
  }
  alloc_mgr_ = new (buf) common::ObTenantMutilAllocator(tenant_id);
  plugins_ = new LogPlugins();
  data_buf_ = (char*)ob_malloc(64 * 1024 * 1024, attr);
  // init MTL
  ObTenantBase tbase(tenant_id);
  ObTenantEnv::set_tenant(&tbase);
}

void TestLogSlidingWindow::TearDown()
{
  ob_free(alloc_mgr_);
  ob_free(data_buf_);
  ObMallocAllocator::get_instance()->recycle_tenant_allocator(tenant_id);
}

void gen_default_palf_base_info_(PalfBaseInfo &palf_base_info)
{
  palf_base_info.reset();
  LSN default_prev_lsn(PALF_INITIAL_LSN_VAL);
  LogInfo prev_log_info;
  prev_log_info.log_id_ = 0;
  prev_log_info.scn_.set_min();
  prev_log_info.lsn_ = default_prev_lsn;
  prev_log_info.log_proposal_id_ = INVALID_PROPOSAL_ID;
  prev_log_info.accum_checksum_ = -1;
  palf_base_info.prev_log_info_ = prev_log_info;
  palf_base_info.curr_lsn_ = default_prev_lsn;
}

TEST_F(TestLogSlidingWindow, test_log_checksum)
{
  int64_t last_log_acc_checksum = 1000310830;
  int64_t data_checksum = 3265973353;
  int64_t cal_checksum = -1;
  int64_t expected_acc_checksum = 389839115;
  LogChecksum checksum_obj;
  EXPECT_EQ(OB_SUCCESS, checksum_obj.init(1, last_log_acc_checksum));
  EXPECT_EQ(OB_SUCCESS, checksum_obj.acquire_accum_checksum(data_checksum, cal_checksum));
  EXPECT_EQ(OB_SUCCESS, checksum_obj.verify_accum_checksum(data_checksum, expected_acc_checksum));
  PALF_LOG(INFO, "finish test checksum", K(last_log_acc_checksum), K(data_checksum), K(cal_checksum), K(expected_acc_checksum));
}

TEST_F(TestLogSlidingWindow, test_init)
{
  PalfBaseInfo base_info;
  gen_default_palf_base_info_(base_info);

  EXPECT_EQ(OB_INVALID_ARGUMENT, log_sw_.init(palf_id_, self_, NULL,
        &mock_mm_, &mock_mode_mgr_, &mock_log_engine_, &palf_fs_cb_, alloc_mgr_, plugins_, base_info, true));
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_sw_.init(palf_id_, self_, &mock_state_mgr_,
        &mock_mm_, &mock_mode_mgr_, NULL, &palf_fs_cb_, NULL, plugins_, base_info, true));
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_sw_.init(palf_id_, self_, &mock_state_mgr_,
        NULL, &mock_mode_mgr_, &mock_log_engine_, &palf_fs_cb_, alloc_mgr_, plugins_, base_info, true));
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_sw_.init(palf_id_, self_, &mock_state_mgr_,
        &mock_mm_, NULL, NULL, &palf_fs_cb_, NULL, plugins_, base_info, true));
  // init succ
  EXPECT_EQ(OB_SUCCESS, log_sw_.init(palf_id_, self_, &mock_state_mgr_,
        &mock_mm_, &mock_mode_mgr_, &mock_log_engine_, &palf_fs_cb_, alloc_mgr_, plugins_, base_info, true));
  // init twice
  EXPECT_EQ(OB_INIT_TWICE, log_sw_.init(palf_id_, self_, &mock_state_mgr_,
        &mock_mm_, &mock_mode_mgr_, &mock_log_engine_, &palf_fs_cb_, alloc_mgr_, plugins_, base_info, true));
}

TEST_F(TestLogSlidingWindow, test_private_func_batch_01)
{
  LSN lsn, end_lsn;
  int64_t log_id = OB_INVALID_LOG_ID;
  int64_t log_pid = INVALID_PROPOSAL_ID;
  EXPECT_EQ(OB_NOT_INIT, log_sw_.get_last_submit_log_info(lsn, log_id, log_pid));
  EXPECT_EQ(OB_NOT_INIT, log_sw_.get_max_flushed_log_info(lsn, end_lsn, log_pid));
  EXPECT_EQ(OB_NOT_INIT, log_sw_.get_last_slide_end_lsn(end_lsn));
  PalfBaseInfo base_info;
  gen_default_palf_base_info_(base_info);
  // init succ
  EXPECT_EQ(OB_SUCCESS, log_sw_.init(palf_id_, self_, &mock_state_mgr_,
        &mock_mm_, &mock_mode_mgr_, &mock_log_engine_, &palf_fs_cb_, alloc_mgr_, plugins_, base_info, true));
  log_id = 10 + PALF_SLIDING_WINDOW_SIZE;
  EXPECT_EQ(false, log_sw_.can_receive_larger_log_(log_id));
  EXPECT_EQ(false, log_sw_.leader_can_submit_larger_log_(log_id));
  EXPECT_EQ(false, log_sw_.leader_can_submit_larger_log_(PALF_SLIDING_WINDOW_SIZE + 1));
  EXPECT_EQ(OB_SUCCESS, log_sw_.get_last_submit_log_info(lsn, log_id, log_pid));
  EXPECT_EQ(OB_SUCCESS, log_sw_.get_max_flushed_log_info(lsn, end_lsn, log_pid));
  EXPECT_EQ(OB_SUCCESS, log_sw_.get_last_slide_end_lsn(end_lsn));
  share::SCN scn = log_sw_.get_last_slide_scn();
}

TEST_F(TestLogSlidingWindow, test_to_follower_pending)
{
  LSN last_lsn;
  EXPECT_EQ(OB_NOT_INIT, log_sw_.to_follower_pending(last_lsn));
  PalfBaseInfo base_info;
  gen_default_palf_base_info_(base_info);
  // init succ
  EXPECT_EQ(OB_SUCCESS, log_sw_.init(palf_id_, self_, &mock_state_mgr_,
        &mock_mm_, &mock_mode_mgr_, &mock_log_engine_, &palf_fs_cb_, alloc_mgr_, plugins_, base_info, true));
  char *buf = data_buf_;
  int64_t buf_len = 1 * 1024 * 1024;
  share::SCN ref_scn;
  ref_scn.convert_for_logservice(99);
  LSN lsn;
  share::SCN scn;
  buf_len = 2 * 1024 * 1024;
  EXPECT_EQ(OB_SUCCESS, log_sw_.submit_log(buf, buf_len, ref_scn, lsn, scn));
  EXPECT_EQ(OB_SUCCESS, log_sw_.to_follower_pending(last_lsn));
}

TEST_F(TestLogSlidingWindow, test_fetch_log)
{
  FetchTriggerType fetch_log_type = NOTIFY_REBUILD;
  LSN prev_lsn;
  LSN fetch_start_lsn;
  int64_t fetch_start_log_id = OB_INVALID_LOG_ID;
  EXPECT_EQ(OB_NOT_INIT, log_sw_.try_fetch_log(fetch_log_type, prev_lsn, fetch_start_lsn, fetch_start_log_id));
  common::ObAddr dest;
  LSN fetch_end_lsn;
  bool is_fetched = false;
  EXPECT_EQ(OB_NOT_INIT, log_sw_.try_fetch_log_for_reconfirm(dest, fetch_end_lsn, is_fetched));
  PalfBaseInfo base_info;
  gen_default_palf_base_info_(base_info);
  // init succ
  MockLocCb cb;
  EXPECT_EQ(OB_SUCCESS, plugins_->add_plugin(static_cast<PalfLocationCacheCb*>(&cb)));
  EXPECT_EQ(OB_SUCCESS, log_sw_.init(palf_id_, self_, &mock_state_mgr_,
        &mock_mm_, &mock_mode_mgr_, &mock_log_engine_, &palf_fs_cb_, alloc_mgr_, plugins_, base_info, true));
  prev_lsn.val_ = 1;
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_sw_.try_fetch_log(fetch_log_type, prev_lsn, fetch_start_lsn, fetch_start_log_id));
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_sw_.try_fetch_log_for_reconfirm(dest, fetch_end_lsn, is_fetched));
  dest = self_;
  fetch_end_lsn.val_ = 100;
  EXPECT_EQ(OB_SUCCESS, log_sw_.try_fetch_log_for_reconfirm(dest, fetch_end_lsn, is_fetched));
  EXPECT_EQ(true, is_fetched);
  fetch_start_lsn.val_ = PALF_INITIAL_LSN_VAL;
  prev_lsn = fetch_start_lsn;
  fetch_start_log_id = 1;
  EXPECT_EQ(OB_SUCCESS, log_sw_.try_fetch_log(fetch_log_type, prev_lsn, fetch_start_lsn, fetch_start_log_id));
  mock_state_mgr_.disable_sync();
  EXPECT_EQ(OB_SUCCESS, log_sw_.try_fetch_log(fetch_log_type, prev_lsn, fetch_start_lsn, fetch_start_log_id));
}

TEST_F(TestLogSlidingWindow, test_report_log_task_trace)
{
  EXPECT_EQ(OB_NOT_INIT, log_sw_.report_log_task_trace(1));
  PalfBaseInfo base_info;
  gen_default_palf_base_info_(base_info);
  // init succ
  EXPECT_EQ(OB_SUCCESS, log_sw_.init(palf_id_, self_, &mock_state_mgr_,
        &mock_mm_, &mock_mode_mgr_, &mock_log_engine_, &palf_fs_cb_, alloc_mgr_, plugins_, base_info, true));
  EXPECT_EQ(OB_SUCCESS, log_sw_.report_log_task_trace(1));
  char *buf = data_buf_;
  int64_t buf_len = 2 * 1024 * 1024;
  LSN lsn;
  share::SCN scn;
  share::SCN ref_scn;
  ref_scn.convert_for_logservice(99);
  buf_len = 2 * 1024 * 1024;
  EXPECT_EQ(OB_SUCCESS, log_sw_.submit_log(buf, buf_len, ref_scn, lsn, scn));
  EXPECT_EQ(OB_SUCCESS, log_sw_.report_log_task_trace(1));
}

TEST_F(TestLogSlidingWindow, test_set_location_cache_cb)
{
  MockLocCb cb;
  MockLocCb *null_cb = NULL;
  EXPECT_EQ(OB_INVALID_ARGUMENT, plugins_->add_plugin(static_cast<PalfLocationCacheCb*>(null_cb)));
  EXPECT_EQ(OB_SUCCESS, plugins_->add_plugin(static_cast<PalfLocationCacheCb*>(&cb)));
  EXPECT_EQ(OB_OP_NOT_ALLOW, plugins_->add_plugin(static_cast<PalfLocationCacheCb*>(&cb)));
}

TEST_F(TestLogSlidingWindow, test_submit_log)
{
  PALF_LOG(INFO, "begin test_submit_log");
  PalfBaseInfo base_info;
  gen_default_palf_base_info_(base_info);
  char *buf = data_buf_;
  int64_t buf_len = 1000;
  share::SCN ref_scn;
  ref_scn.convert_for_logservice(99);
  LSN lsn;
  share::SCN scn;
  EXPECT_EQ(OB_NOT_INIT, log_sw_.submit_log(buf, buf_len, ref_scn, lsn, scn));
  EXPECT_EQ(OB_SUCCESS, log_sw_.init(palf_id_, self_, &mock_state_mgr_,
        &mock_mm_, &mock_mode_mgr_, &mock_log_engine_, &palf_fs_cb_, alloc_mgr_, plugins_, base_info, true));
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_sw_.submit_log(NULL, buf_len, ref_scn, lsn, scn));
  buf_len = 0;
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_sw_.submit_log(buf, buf_len, ref_scn, lsn, scn));
  buf_len = 64 * 1024 * 1024;
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_sw_.submit_log(buf, buf_len, ref_scn, lsn, scn));
  buf_len = 1000;
  ref_scn.reset();
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_sw_.submit_log(buf, buf_len, ref_scn, lsn, scn));
  ref_scn.convert_for_logservice(99);
  buf_len = 2 * 1024 * 1024;
  for (int i = 0; i < 8; ++i) {
    EXPECT_EQ(OB_SUCCESS, log_sw_.submit_log(buf, buf_len, ref_scn, lsn, scn));
  }
  // append to last group log
  buf_len = 1 * 1024 * 1024;
  EXPECT_EQ(OB_SUCCESS, log_sw_.submit_log(buf, buf_len, ref_scn, lsn, scn));
  buf_len = 2 * 1024 * 1024;
  for (int i = 0; i < 11; ++i) {
    EXPECT_EQ(OB_SUCCESS, log_sw_.submit_log(buf, buf_len, ref_scn, lsn, scn));
  }
  PALF_LOG(INFO, "current lsn", K(lsn), K(buf_len));
  // 40M已填充39M，无法继续submit 2M log
  EXPECT_EQ(OB_EAGAIN, log_sw_.submit_log(buf, buf_len, ref_scn, lsn, scn));
}

TEST_F(TestLogSlidingWindow, test_submit_group_log)
{
  PALF_LOG(INFO, "begin test_submit_group_log");
  PalfBaseInfo base_info;
  gen_default_palf_base_info_(base_info);
  EXPECT_EQ(OB_SUCCESS, log_sw_.init(palf_id_, self_, &mock_state_mgr_,
        &mock_mm_, &mock_mode_mgr_, &mock_log_engine_, &palf_fs_cb_, alloc_mgr_, plugins_, base_info, true));
  mock_state_mgr_.mock_proposal_id_ = 100;
  LSN lsn(10);
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_sw_.submit_group_log(lsn, NULL, 1024));
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_sw_.submit_group_log(lsn, data_buf_, -1));
  // generate log entry and group entry
  LogEntryHeader log_entry_header;
  LogGroupEntryHeader group_header;
  share::SCN max_scn;
  max_scn.convert_for_logservice(111111);
  int64_t log_id = 1;
  LSN committed_end_lsn(0);
  int64_t log_proposal_id = 10;
  char log_data[2048];
  int64_t log_data_len = 2048;
  int64_t group_data_checksum = -1;
  EXPECT_EQ(OB_SUCCESS, log_entry_header.generate_header(log_data, log_data_len, max_scn));
  static const int64_t DATA_BUF_LEN = 64 * 1024 * 1024;
  int64_t group_header_size = LogGroupEntryHeader::HEADER_SER_SIZE;
  int64_t pos = 0;
  log_entry_header.serialize(data_buf_ + group_header_size, DATA_BUF_LEN, pos);
  EXPECT_TRUE(pos > 0);
  memcpy(data_buf_ + group_header_size + pos, log_data, log_data_len);
  LogWriteBuf write_buf;
  int64_t log_entry_size = pos + log_data_len;
  EXPECT_EQ(OB_SUCCESS, write_buf.push_back(data_buf_, log_entry_size+group_header_size));
  EXPECT_EQ(OB_SUCCESS, group_header.generate(false, false, write_buf, log_entry_size, max_scn, log_id,
      committed_end_lsn, log_proposal_id, group_data_checksum));
  int64_t accum_checksum = 100;
  (void) group_header.update_accumulated_checksum(accum_checksum);
  // calculate header parity flag
  (void) group_header.update_header_checksum();
  pos = 0;
  group_header.serialize(data_buf_, DATA_BUF_LEN, pos);
  EXPECT_TRUE(pos > 0);
  int64_t group_entry_size = pos + log_entry_size;
  // submit group_log
  EXPECT_EQ(OB_SUCCESS, log_sw_.submit_group_log(lsn, data_buf_, group_entry_size));
}

TEST_F(TestLogSlidingWindow, test_receive_log)
{
  PALF_LOG(INFO, "begin test_receive_log=======================");
  int64_t curr_proposal_id = 10;
  mock_state_mgr_.mock_proposal_id_ = curr_proposal_id;
  ObAddr src_server = self_;
  PushLogType push_log_type = PUSH_LOG;
  LSN prev_lsn(0);
  int64_t prev_log_proposal_id = curr_proposal_id;
  LSN lsn;
  int64_t log_data_len = 2048;
  int64_t log_entry_size = log_data_len;
  int64_t group_entry_size = log_entry_size;
  TruncateLogInfo truncate_log_info;
  EXPECT_EQ(OB_NOT_INIT, log_sw_.receive_log(src_server, push_log_type, prev_lsn, prev_log_proposal_id, lsn, data_buf_, \
        group_entry_size, true, truncate_log_info));
  PalfBaseInfo base_info;
  gen_default_palf_base_info_(base_info);
  EXPECT_EQ(OB_SUCCESS, log_sw_.init(palf_id_, self_, &mock_state_mgr_,
        &mock_mm_, &mock_mode_mgr_, &mock_log_engine_, &palf_fs_cb_, alloc_mgr_, plugins_, base_info, true));

  char *buf = data_buf_;
  int64_t buf_len = 2 * 1024 * 1024;
  share::SCN ref_scn;
  ref_scn.convert_for_logservice(999);
  share::SCN scn;
  EXPECT_EQ(OB_SUCCESS, log_sw_.submit_log(buf, buf_len, ref_scn, lsn, scn));
  // update lsn for next group entry
  lsn.val_ = lsn.val_ + LogEntryHeader::HEADER_SER_SIZE + buf_len;
  // generate new group entry
  LogEntryHeader log_entry_header;
  LogGroupEntryHeader group_header;
  share::SCN max_scn;
  max_scn.convert_for_logservice(111111);
  int64_t log_id = 2;
  LSN committed_end_lsn(0);
  int64_t log_proposal_id = 20;
  char log_data[2048];
  log_data_len = 2048;
  int64_t group_data_checksum = -1;
  EXPECT_EQ(OB_SUCCESS, log_entry_header.generate_header(log_data, log_data_len, max_scn));
  static const int64_t DATA_BUF_LEN = 64 * 1024 * 1024;
  int64_t group_header_size = LogGroupEntryHeader::HEADER_SER_SIZE;
  int64_t pos = 0;
  log_entry_header.serialize(data_buf_ + group_header_size, DATA_BUF_LEN, pos);
  EXPECT_TRUE(pos > 0);
  memcpy(data_buf_ + group_header_size + pos, log_data, log_data_len);
  log_entry_size = pos + log_data_len;
  // gen 2nd log entry
  max_scn.convert_for_logservice(222222);
  EXPECT_EQ(OB_SUCCESS, log_entry_header.generate_header(log_data, log_data_len, max_scn));
  pos = 0;
  log_entry_header.serialize(data_buf_ + group_header_size + log_entry_size, DATA_BUF_LEN, pos);
  EXPECT_TRUE(pos > 0);
  memcpy(data_buf_ + group_header_size + log_entry_size + pos, log_data, log_data_len);
  log_entry_size += (pos + log_data_len);
  // gen group log
  LogWriteBuf write_buf;
  EXPECT_EQ(OB_SUCCESS, write_buf.push_back(data_buf_, log_entry_size + group_header_size));
  EXPECT_EQ(OB_SUCCESS, group_header.generate(false, false, write_buf, log_entry_size, max_scn, log_id,
      committed_end_lsn, log_proposal_id, group_data_checksum));
  int64_t accum_checksum = 100;
  (void) group_header.update_accumulated_checksum(accum_checksum);
  // calculate header parity flag
  (void) group_header.update_header_checksum();
  pos = 0;
  group_header.serialize(data_buf_, DATA_BUF_LEN, pos);
  EXPECT_TRUE(pos > 0);
  group_entry_size = pos + log_entry_size;
  EXPECT_TRUE(group_header.check_integrity(data_buf_ + group_header_size, group_entry_size - group_header_size));
  // receive group_log
  src_server.reset();
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_sw_.receive_log(src_server, push_log_type, prev_lsn, prev_log_proposal_id, lsn, data_buf_, group_entry_size, true, truncate_log_info));
  src_server = self_;
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_sw_.receive_log(src_server, push_log_type, prev_lsn, prev_log_proposal_id, lsn, NULL, group_entry_size, true, truncate_log_info));
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_sw_.receive_log(src_server, push_log_type, prev_lsn, prev_log_proposal_id, lsn, data_buf_, 0, true, truncate_log_info));
  LSN tmp_lsn;
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_sw_.receive_log(src_server, push_log_type, prev_lsn, prev_log_proposal_id, tmp_lsn, data_buf_, 0, true, truncate_log_info));
  // prev_log_proposal_id not match
  prev_log_proposal_id = curr_proposal_id - 1;
  EXPECT_EQ(OB_STATE_NOT_MATCH, log_sw_.receive_log(src_server, push_log_type, prev_lsn, prev_log_proposal_id, lsn, data_buf_, group_entry_size, true, truncate_log_info));
  // use correct prev_log_proposal_id
  prev_log_proposal_id = curr_proposal_id;
  // handle submit log
  EXPECT_EQ(OB_SUCCESS, log_sw_.period_freeze_last_log());
  EXPECT_EQ(OB_EAGAIN, log_sw_.receive_log(src_server, push_log_type, prev_lsn, prev_log_proposal_id, lsn, data_buf_, group_entry_size, true, truncate_log_info));
  LSN old_lsn = lsn;
  // test lsn > group_buffer capacity case, will return -4023
  lsn.val_ = log_sw_.group_buffer_.get_available_buffer_size();
  EXPECT_EQ(OB_EAGAIN, log_sw_.receive_log(src_server, push_log_type, prev_lsn, prev_log_proposal_id, lsn, data_buf_, group_entry_size, true, truncate_log_info));
  // reset correct lsn
  lsn = old_lsn;
  EXPECT_EQ(OB_EAGAIN, log_sw_.receive_log(src_server, push_log_type, prev_lsn, prev_log_proposal_id, lsn, data_buf_, group_entry_size, true, truncate_log_info));
  // ignore clean check, receive log_id=2 success
  PALF_LOG(INFO, "begin receive log with log_id=2, and ignore clean check");
  EXPECT_EQ(OB_SUCCESS, log_sw_.receive_log(src_server, push_log_type, prev_lsn, prev_log_proposal_id, lsn, data_buf_, group_entry_size, false, truncate_log_info));
  // update lsn to next log
  lsn.val_ = lsn.val_ + group_entry_size;
  // test log_id exceeds range case
  log_id = 999999;
  EXPECT_EQ(OB_SUCCESS, group_header.generate(false, false, write_buf, log_entry_size, max_scn, log_id,
      committed_end_lsn, log_proposal_id, group_data_checksum));
  // calculate header parity flag
  (void) group_header.update_header_checksum();
  pos = 0;
  group_header.serialize(data_buf_, DATA_BUF_LEN, pos);
  EXPECT_TRUE(pos > 0);
  group_entry_size = pos + log_entry_size;
  EXPECT_TRUE(group_header.check_integrity(data_buf_ + group_header_size, group_entry_size - group_header_size));
  EXPECT_EQ(OB_EAGAIN, log_sw_.receive_log(src_server, push_log_type, prev_lsn, prev_log_proposal_id, lsn, data_buf_, group_entry_size, true, truncate_log_info));
  // test cache log case
  log_id = 102;
  lsn.val_ += 4096 * 1024;
  EXPECT_EQ(OB_SUCCESS, group_header.generate(false, false, write_buf, log_entry_size, max_scn, log_id,
      committed_end_lsn, log_proposal_id, group_data_checksum));
  // calculate header parity flag
  (void) group_header.update_header_checksum();
  pos = 0;
  group_header.serialize(data_buf_, DATA_BUF_LEN, pos);
  EXPECT_TRUE(pos > 0);
  group_entry_size = pos + log_entry_size;
  EXPECT_TRUE(group_header.check_integrity(data_buf_ + group_header_size, group_entry_size - group_header_size));
  EXPECT_EQ(OB_SUCCESS, log_sw_.receive_log(src_server, push_log_type, prev_lsn, prev_log_proposal_id, lsn, data_buf_, group_entry_size, true, truncate_log_info));
  // test need truncate cached log_task case
  log_id = 3;
  lsn.val_ -= 4096 * 1024;
  prev_lsn = old_lsn;
  prev_log_proposal_id = log_proposal_id;
  EXPECT_EQ(OB_SUCCESS, group_header.generate(false, false, write_buf, log_entry_size, max_scn, log_id,
      committed_end_lsn, log_proposal_id, group_data_checksum));
  // inc log_proposal_id to trigger clean cached log_task
  log_proposal_id += 1;
  group_header.update_log_proposal_id(log_proposal_id);
  // calculate header parity flag
  (void) group_header.update_header_checksum();
  pos = 0;
  group_header.serialize(data_buf_, DATA_BUF_LEN, pos);
  EXPECT_TRUE(pos > 0);
  EXPECT_TRUE(group_header.check_integrity(data_buf_ + group_header_size, group_entry_size - group_header_size));
  PALF_LOG(INFO, "begin receive log with log_id=3, and proposal_id 21");
  // handle submit log
  EXPECT_EQ(OB_SUCCESS, log_sw_.period_freeze_last_log());
  EXPECT_EQ(OB_EAGAIN, log_sw_.receive_log(src_server, push_log_type, prev_lsn, prev_log_proposal_id, lsn, data_buf_, group_entry_size, true, truncate_log_info));
  EXPECT_TRUE(TRUNCATE_CACHED_LOG_TASK == truncate_log_info.truncate_type_);
  EXPECT_EQ(log_id, truncate_log_info.truncate_log_id_);
  EXPECT_EQ(OB_SUCCESS, log_sw_.clean_cached_log(truncate_log_info.truncate_log_id_, lsn, prev_lsn, prev_log_proposal_id));
  // do not check need clean log
  EXPECT_EQ(OB_SUCCESS, log_sw_.receive_log(src_server, push_log_type, prev_lsn, prev_log_proposal_id, lsn, data_buf_, group_entry_size, false, truncate_log_info));
  log_sw_.max_flushed_end_lsn_ = lsn + group_entry_size;
  EXPECT_EQ(OB_SUCCESS, log_sw_.receive_log(src_server, push_log_type, prev_lsn, prev_log_proposal_id, lsn, data_buf_, group_entry_size, true, truncate_log_info));
  // test <log_id, lsn> not match case
  PALF_LOG(INFO, "begin tese <log_id, lsn> not match case");
  // 改大flush lsn
  log_sw_.max_flushed_end_lsn_.val_ += 100;
  // 增大log_id，构造prev log空洞
  log_id += 100;
  uint64_t new_val = max_scn.get_val_for_logservice() - 10;
  max_scn.convert_for_logservice(new_val);
  LogWriteBuf write_buf1;
  EXPECT_EQ(OB_SUCCESS, write_buf1.push_back(data_buf_, log_entry_size+group_header_size));
  EXPECT_EQ(OB_SUCCESS, group_header.generate(false, false, write_buf1, log_entry_size, max_scn, log_id,
      committed_end_lsn, log_proposal_id, group_data_checksum));
  // calculate header parity flag
  (void) group_header.update_header_checksum();
  pos = 0;
  group_header.serialize(data_buf_, DATA_BUF_LEN, pos);
  EXPECT_TRUE(pos > 0);
  EXPECT_TRUE(group_header.check_integrity(data_buf_ + group_header_size, group_entry_size - group_header_size));
  // return -4109 because lsn < max_flushed_end_lsn
  EXPECT_EQ(OB_STATE_NOT_MATCH, log_sw_.receive_log(src_server, push_log_type, prev_lsn, prev_log_proposal_id, lsn, data_buf_, group_entry_size, true, truncate_log_info));
  PALF_LOG(INFO, "finish test_receive_log=======================");
}

TEST_F(TestLogSlidingWindow, test_after_flush_log)
{
  PALF_LOG(INFO, "begin test_after_flush_log");
  FlushLogCbCtx flush_log_ctx;
  EXPECT_EQ(OB_NOT_INIT, log_sw_.after_flush_log(flush_log_ctx));

  PalfBaseInfo base_info;
  gen_default_palf_base_info_(base_info);
  EXPECT_EQ(OB_SUCCESS, log_sw_.init(palf_id_, self_, &mock_state_mgr_,
        &mock_mm_, &mock_mode_mgr_, &mock_log_engine_, &palf_fs_cb_, alloc_mgr_, plugins_, base_info, true));
  int64_t curr_proposal_id = 10;

  // set default config meta
  ObMemberList default_mlist;
  default_mlist.add_server(self_);
  GlobalLearnerList learners;
  LogConfigMeta config_meta;
  LogConfigInfoV2 init_config_info;
  LogConfigVersion init_config_version;
  init_config_version.generate(curr_proposal_id, 0);
  EXPECT_EQ(OB_SUCCESS, init_config_info.generate(default_mlist, 1, learners, init_config_version));
  config_meta.curr_ = init_config_info;
  mock_mm_.log_ms_meta_ = config_meta;
  mock_mm_.sw_ = &log_sw_;

  mock_state_mgr_.mock_proposal_id_ = curr_proposal_id;

  char *buf = data_buf_;
  int64_t buf_len = 2 * 1024 * 1024;
  share::SCN ref_scn;
  ref_scn.convert_for_logservice(999);
  LSN lsn;
  share::SCN scn;
  EXPECT_EQ(OB_SUCCESS, log_sw_.submit_log(buf, buf_len, ref_scn, lsn, scn));
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_sw_.after_flush_log(flush_log_ctx));

  flush_log_ctx.log_id_ = PALF_SLIDING_WINDOW_SIZE + 100;
  flush_log_ctx.scn_ = scn;
  LSN group_log_lsn;
  group_log_lsn.val_ = lsn.val_ - LogGroupEntryHeader::HEADER_SER_SIZE;
  flush_log_ctx.lsn_ = group_log_lsn;
  flush_log_ctx.log_proposal_id_ = curr_proposal_id;
  flush_log_ctx.total_len_ = LogGroupEntryHeader::HEADER_SER_SIZE + LogEntryHeader::HEADER_SER_SIZE + buf_len;;
  flush_log_ctx.curr_proposal_id_ = curr_proposal_id + 1;
  flush_log_ctx.begin_ts_ = ObTimeUtility::current_time();
  EXPECT_EQ(OB_ERR_OUT_OF_UPPER_BOUND, log_sw_.after_flush_log(flush_log_ctx));
  flush_log_ctx.log_id_ = 2;
  EXPECT_EQ(OB_SUCCESS, log_sw_.after_flush_log(flush_log_ctx));
  log_sw_.is_truncating_ = true;
  log_sw_.last_truncate_lsn_ = lsn;
  flush_log_ctx.log_id_ = 1;
  flush_log_ctx.curr_proposal_id_ = curr_proposal_id;
  EXPECT_EQ(OB_SUCCESS, log_sw_.after_flush_log(flush_log_ctx));
  log_sw_.is_truncating_ = false;
  EXPECT_EQ(OB_SUCCESS, log_sw_.after_flush_log(flush_log_ctx));
  mock_state_mgr_.mock_proposal_id_ = curr_proposal_id + 1;
  EXPECT_EQ(OB_SUCCESS, log_sw_.after_flush_log(flush_log_ctx));
  flush_log_ctx.log_proposal_id_ = curr_proposal_id - 1;
  EXPECT_EQ(OB_STATE_NOT_MATCH, log_sw_.after_flush_log(flush_log_ctx));
  mock_state_mgr_.role_ = FOLLOWER;
  mock_state_mgr_.state_ = ACTIVE;
  flush_log_ctx.log_proposal_id_ = curr_proposal_id;
  EXPECT_EQ(OB_SUCCESS, log_sw_.after_flush_log(flush_log_ctx));
  mock_state_mgr_.role_ = LEADER;
  EXPECT_EQ(OB_SUCCESS, log_sw_.after_flush_log(flush_log_ctx));
}

TEST_F(TestLogSlidingWindow, test_truncate_log)
{
  PALF_LOG(INFO, "begin test_truncate_log");
  PalfBaseInfo base_info;
  gen_default_palf_base_info_(base_info);
  EXPECT_EQ(OB_SUCCESS, log_sw_.init(palf_id_, self_, &mock_state_mgr_,
        &mock_mm_, &mock_mode_mgr_, &mock_log_engine_, &palf_fs_cb_, alloc_mgr_, plugins_, base_info, true));
  int64_t curr_proposal_id = 10;
  mock_state_mgr_.mock_proposal_id_ = curr_proposal_id;

  char *buf = data_buf_;
  int64_t buf_len = 2 * 1024 * 1024;
  share::SCN ref_scn;
  ref_scn.convert_for_logservice(999);
  LSN lsn;
  share::SCN scn;
  // submit first log
  EXPECT_EQ(OB_SUCCESS, log_sw_.submit_log(buf, buf_len, ref_scn, lsn, scn));
  EXPECT_EQ(OB_SUCCESS, log_sw_.period_freeze_last_log());
  // generate new group entry
  LogEntryHeader log_entry_header;
  LogGroupEntryHeader group_header;
  share::SCN max_scn;
  max_scn.convert_for_logservice(111111);
  int64_t log_id = 2;
  LSN committed_end_lsn(0);
  int64_t log_proposal_id = 20;
  char log_data[2048];
  int64_t log_data_len = 2048;
  int64_t group_data_checksum = -1;
  EXPECT_EQ(OB_SUCCESS, log_entry_header.generate_header(log_data, log_data_len, max_scn));
  static const int64_t DATA_BUF_LEN = 64 * 1024 * 1024;
  int64_t group_header_size = LogGroupEntryHeader::HEADER_SER_SIZE;
  int64_t pos = 0;
  log_entry_header.serialize(data_buf_ + group_header_size, DATA_BUF_LEN, pos);
  EXPECT_TRUE(pos > 0);
  memcpy(data_buf_ + group_header_size + pos, log_data, log_data_len);
  int64_t log_entry_size = pos + log_data_len;
  // gen 2nd log entry
  max_scn.convert_for_logservice(222222);
  EXPECT_EQ(OB_SUCCESS, log_entry_header.generate_header(log_data, log_data_len, max_scn));
  pos = 0;
  log_entry_header.serialize(data_buf_ + group_header_size + log_entry_size, DATA_BUF_LEN, pos);
  EXPECT_TRUE(pos > 0);
  memcpy(data_buf_ + group_header_size + log_entry_size + pos, log_data, log_data_len);
  log_entry_size += (pos + log_data_len);
  // gen group log
  LogWriteBuf write_buf;
  EXPECT_EQ(OB_SUCCESS, write_buf.push_back(data_buf_, log_entry_size+group_header_size));
  EXPECT_EQ(OB_SUCCESS, group_header.generate(false, false, write_buf, log_entry_size, max_scn, log_id,
      committed_end_lsn, log_proposal_id, group_data_checksum));
  int64_t accum_checksum = 100;
  (void) group_header.update_accumulated_checksum(accum_checksum);
  // calculate header parity flag
  (void) group_header.update_header_checksum();
  pos = 0;
  group_header.serialize(data_buf_, DATA_BUF_LEN, pos);
  EXPECT_TRUE(pos > 0);
  int64_t group_entry_size = pos + log_entry_size;
  EXPECT_TRUE(group_header.check_integrity(data_buf_ + group_header_size, group_entry_size - group_header_size));
  // receive group_log
  ObAddr src_server = self_;
  PushLogType push_log_type = PUSH_LOG;
  LSN prev_lsn(0);
  int64_t prev_log_proposal_id = curr_proposal_id;
  lsn = log_sw_.get_max_lsn();
  src_server.reset();
  TruncateLogInfo truncate_log_info;
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_sw_.receive_log(src_server, push_log_type, prev_lsn, prev_log_proposal_id, lsn, data_buf_, group_entry_size, true, truncate_log_info));
  src_server = self_;
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_sw_.receive_log(src_server, push_log_type, prev_lsn, prev_log_proposal_id, lsn, NULL, group_entry_size, true, truncate_log_info));
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_sw_.receive_log(src_server, push_log_type, prev_lsn, prev_log_proposal_id, lsn, data_buf_, 0, true, truncate_log_info));
  LSN tmp_lsn;
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_sw_.receive_log(src_server, push_log_type, prev_lsn, prev_log_proposal_id, tmp_lsn, data_buf_, 0, true, truncate_log_info));
  prev_log_proposal_id = curr_proposal_id;
  EXPECT_EQ(OB_SUCCESS, log_sw_.receive_log(src_server, push_log_type, prev_lsn, prev_log_proposal_id, lsn, data_buf_, group_entry_size, false, truncate_log_info));
  // test need truncate case
  PALF_LOG(INFO, "begin test need truncate case");
  group_header.update_log_proposal_id(log_proposal_id + 1);
  // calculate header parity flag
  (void) group_header.update_header_checksum();
  pos = 0;
  group_header.serialize(data_buf_, DATA_BUF_LEN, pos);
  EXPECT_TRUE(pos > 0);
  EXPECT_TRUE(group_header.check_integrity(data_buf_ + group_header_size, group_entry_size - group_header_size));
  EXPECT_EQ(OB_EAGAIN, log_sw_.receive_log(src_server, push_log_type, prev_lsn, prev_log_proposal_id, lsn, data_buf_, group_entry_size, true, truncate_log_info));
  EXPECT_TRUE(TRUNCATE_LOG == truncate_log_info.truncate_type_);
  EXPECT_EQ(log_id, truncate_log_info.truncate_log_id_);
  EXPECT_EQ(lsn, truncate_log_info.truncate_begin_lsn_);
  EXPECT_EQ(log_proposal_id, truncate_log_info.truncate_log_proposal_id_);
  // update flush end lsn
  log_sw_.max_flushed_end_lsn_ = lsn + group_entry_size;
  LSN test_lsn(0);
  int64_t saved_truncate_log_id = truncate_log_info.truncate_log_id_;
  truncate_log_info.truncate_log_id_ = 0;
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_sw_.truncate(truncate_log_info, test_lsn, prev_log_proposal_id));
  truncate_log_info.truncate_log_id_ = saved_truncate_log_id;
  EXPECT_EQ(0, prev_lsn.val_);
  EXPECT_EQ(OB_SUCCESS, log_sw_.truncate(truncate_log_info, prev_lsn, prev_log_proposal_id));

  TruncateLogCbCtx test_truncate_ctx(test_lsn);
  EXPECT_EQ(OB_STATE_NOT_MATCH, log_sw_.after_truncate(test_truncate_ctx));
  TruncateLogCbCtx invalid_ctx;
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_sw_.after_truncate(invalid_ctx));
  TruncateLogCbCtx truncate_log_cb_ctx(truncate_log_info.truncate_begin_lsn_);
  EXPECT_EQ(OB_SUCCESS, log_sw_.after_truncate(truncate_log_cb_ctx));
}

TEST_F(TestLogSlidingWindow, test_ack_log)
{
  PALF_LOG(INFO, "begin test_ack_log");
  PalfBaseInfo base_info;
  gen_default_palf_base_info_(base_info);
  EXPECT_EQ(OB_SUCCESS, log_sw_.init(palf_id_, self_, &mock_state_mgr_,
        &mock_mm_, &mock_mode_mgr_, &mock_log_engine_, &palf_fs_cb_, alloc_mgr_, plugins_, base_info, true));
  int64_t curr_proposal_id = 10;
  mock_state_mgr_.mock_proposal_id_ = curr_proposal_id;
  log_sw_.self_ = self_;

  // set default config meta
  ObMemberList default_mlist;
  default_mlist.add_server(self_);
  GlobalLearnerList learners;
  LogConfigMeta config_meta;
  LogConfigInfoV2 init_config_info;
  LogConfigVersion init_config_version;
  init_config_version.generate(curr_proposal_id, 0);
  EXPECT_EQ(OB_SUCCESS, init_config_info.generate(default_mlist, 1, learners, init_config_version));
  config_meta.curr_ = init_config_info;
  mock_mm_.log_ms_meta_ = config_meta;
  mock_mm_.sw_ = &log_sw_;

  char *buf = data_buf_;
  int64_t buf_len = 2 * 1024 * 1024;
  share::SCN ref_scn;
  ref_scn.convert_for_logservice(999);
  LSN lsn;
  share::SCN scn;
  EXPECT_EQ(OB_SUCCESS, log_sw_.submit_log(buf, buf_len, ref_scn, lsn, scn));
  LSN end_lsn = lsn + LogEntryHeader::HEADER_SER_SIZE + buf_len;
  ObAddr server;
  server.set_ip_addr("127.0.0.1", 12346);
  default_mlist.add_server(server);
  LSN invalid_lsn;
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_sw_.ack_log(server, invalid_lsn));
  LogConfigInfoV2 new_config_info = init_config_info;
  new_config_info.config_.log_sync_memberlist_ = default_mlist;
  mock_mm_.log_ms_meta_.curr_ = new_config_info;

  mock_state_mgr_.role_ = LEADER;
  mock_state_mgr_.state_ = ACTIVE;

  FlushLogCbCtx flush_log_ctx;
  flush_log_ctx.log_id_ = 1;
  flush_log_ctx.scn_ = scn;
  LSN group_log_lsn;
  group_log_lsn.val_ = lsn.val_ - LogGroupEntryHeader::HEADER_SER_SIZE;
  flush_log_ctx.lsn_ = group_log_lsn;
  flush_log_ctx.log_proposal_id_ = curr_proposal_id;
  flush_log_ctx.total_len_ = LogGroupEntryHeader::HEADER_SER_SIZE + LogEntryHeader::HEADER_SER_SIZE + buf_len;;
  flush_log_ctx.curr_proposal_id_ = curr_proposal_id;
  flush_log_ctx.begin_ts_ = ObTimeUtility::current_time();
  EXPECT_EQ(OB_SUCCESS, log_sw_.after_flush_log(flush_log_ctx));

  EXPECT_EQ(OB_SUCCESS, log_sw_.ack_log(server, end_lsn));
}

TEST_F(TestLogSlidingWindow, test_truncate_for_rebuild)
{
  PALF_LOG(INFO, "begin test_truncate_for_rebuild");
  PalfBaseInfo base_info;
  gen_default_palf_base_info_(base_info);
  EXPECT_EQ(OB_SUCCESS, log_sw_.init(palf_id_, self_, &mock_state_mgr_,
        &mock_mm_, &mock_mode_mgr_, &mock_log_engine_, &palf_fs_cb_, alloc_mgr_, plugins_, base_info, true));
  int64_t curr_proposal_id = 10;
  mock_state_mgr_.mock_proposal_id_ = curr_proposal_id;

  char *buf = data_buf_;
  int64_t buf_len = 2 * 1024 * 1024;
  share::SCN ref_scn;
  ref_scn.convert_for_logservice(999);
  LSN lsn;
  share::SCN scn;
  EXPECT_EQ(OB_SUCCESS, log_sw_.submit_log(buf, buf_len, ref_scn, lsn, scn));
  // generate new group entry
  LogEntryHeader log_entry_header;
  LogGroupEntryHeader group_header;
  share::SCN max_scn;
  max_scn.convert_for_logservice(111111);
  int64_t log_id = 2;
  LSN committed_end_lsn(0);
  int64_t log_proposal_id = 20;
  char log_data[2048];
  int64_t log_data_len = 2048;
  int64_t group_data_checksum = -1;
  EXPECT_EQ(OB_SUCCESS, log_entry_header.generate_header(log_data, log_data_len, max_scn));
  static const int64_t DATA_BUF_LEN = 64 * 1024 * 1024;
  int64_t group_header_size = LogGroupEntryHeader::HEADER_SER_SIZE;
  int64_t pos = 0;
  log_entry_header.serialize(data_buf_ + group_header_size, DATA_BUF_LEN, pos);
  EXPECT_TRUE(pos > 0);
  memcpy(data_buf_ + group_header_size + pos, log_data, log_data_len);
  int64_t log_entry_size = pos + log_data_len;
  // gen 2nd log entry
  max_scn.convert_for_logservice(222222);
  EXPECT_EQ(OB_SUCCESS, log_entry_header.generate_header(log_data, log_data_len, max_scn));
  pos = 0;
  log_entry_header.serialize(data_buf_ + group_header_size + log_entry_size, DATA_BUF_LEN, pos);
  EXPECT_TRUE(pos > 0);
  memcpy(data_buf_ + group_header_size + log_entry_size + pos, log_data, log_data_len);
  log_entry_size += (pos + log_data_len);
  // gen group log
  LogWriteBuf write_buf;
  EXPECT_EQ(OB_SUCCESS, write_buf.push_back(data_buf_, log_entry_size+group_header_size));
  EXPECT_EQ(OB_SUCCESS, group_header.generate(false, false, write_buf, log_entry_size, max_scn, log_id,
      committed_end_lsn, log_proposal_id, group_data_checksum));
  int64_t accum_checksum = 100;
  (void) group_header.update_accumulated_checksum(accum_checksum);
  // calculate header parity flag
  (void) group_header.update_header_checksum();
  pos = 0;
  group_header.serialize(data_buf_, DATA_BUF_LEN, pos);
  EXPECT_TRUE(pos > 0);
  int64_t group_entry_size = pos + log_entry_size;
  EXPECT_TRUE(group_header.check_integrity(data_buf_ + group_header_size, group_entry_size - group_header_size));
  // receive group_log
  ObAddr src_server = self_;
  PushLogType push_log_type = PUSH_LOG;
  LSN prev_lsn(0);
  int64_t prev_log_proposal_id = curr_proposal_id;
  lsn = log_sw_.get_max_lsn();
  src_server.reset();
  src_server = self_;
  TruncateLogInfo truncate_log_info;
  // handle submit log
  EXPECT_EQ(OB_SUCCESS, log_sw_.period_freeze_last_log());
  PALF_LOG(INFO, "begin receive log with log_id=2");
  EXPECT_EQ(OB_SUCCESS, log_sw_.receive_log(src_server, push_log_type, prev_lsn, prev_log_proposal_id, lsn, data_buf_, group_entry_size, false, truncate_log_info));
  // gen next group log
  log_id = 10;
  uint64_t new_val = max_scn.get_val_for_logservice() + 100;
  max_scn.convert_for_logservice(new_val);
  LogWriteBuf write_buf1;
  EXPECT_EQ(OB_SUCCESS, write_buf1.push_back(data_buf_, log_entry_size+group_header_size));
  EXPECT_EQ(OB_SUCCESS, group_header.generate(false, false, write_buf1, log_entry_size, max_scn, log_id,
      committed_end_lsn, log_proposal_id, group_data_checksum));
  accum_checksum += 100;
  (void) group_header.update_accumulated_checksum(accum_checksum);
  // calculate header parity flag
  (void) group_header.update_header_checksum();
  pos = 0;
  group_header.serialize(data_buf_, DATA_BUF_LEN, pos);
  EXPECT_TRUE(pos > 0);
  group_entry_size = pos + log_entry_size;
  EXPECT_TRUE(group_header.check_integrity(data_buf_ + group_header_size, group_entry_size - group_header_size));
  // lsn < last_submit_end_lsn, return -4016
  EXPECT_EQ(OB_ERR_UNEXPECTED, log_sw_.receive_log(src_server, push_log_type, prev_lsn, prev_log_proposal_id, lsn, data_buf_, group_entry_size, false, truncate_log_info));

  // update flush end lsn
  log_sw_.max_flushed_end_lsn_ = lsn + group_entry_size;

  PalfBaseInfo new_base_info;
  new_base_info.prev_log_info_.log_id_ = 0;

  EXPECT_EQ(OB_SUCCESS, log_sw_.truncate_for_rebuild(new_base_info));
  new_base_info.prev_log_info_.log_id_ = 6;
  new_val = max_scn.get_val_for_logservice() - 50;
  new_base_info.prev_log_info_.scn_.convert_for_logservice(new_val);
  new_base_info.prev_log_info_.log_proposal_id_ = curr_proposal_id;
  new_base_info.prev_log_info_.lsn_ = lsn - 200;
  new_base_info.curr_lsn_ = lsn - 100;
  // test prev_log not exist case
  EXPECT_EQ(OB_SUCCESS, log_sw_.truncate_for_rebuild(new_base_info));
}

TEST_F(TestLogSlidingWindow, test_append_disk_log)
{
  PALF_LOG(INFO, "begin test_append_disk_log");
  LSN lsn(0);
  LogGroupEntry group_entry;
  EXPECT_EQ(OB_NOT_INIT, log_sw_.append_disk_log(lsn, group_entry));
  PalfBaseInfo base_info;
  gen_default_palf_base_info_(base_info);
  EXPECT_EQ(OB_SUCCESS, log_sw_.init(palf_id_, self_, &mock_state_mgr_,
        &mock_mm_, &mock_mode_mgr_, &mock_log_engine_, &palf_fs_cb_, alloc_mgr_, plugins_, base_info, true));
  int64_t curr_proposal_id = 10;
  mock_state_mgr_.mock_proposal_id_ = curr_proposal_id;
  // generate new group entry
  LogEntry log_entry;
  LogEntryHeader log_entry_header;
  LogGroupEntryHeader group_header;
  share::SCN max_scn;
  max_scn.convert_for_logservice(111111);
  int64_t log_id = 1;
  LSN committed_end_lsn(0);
  int64_t log_proposal_id = 20;
  char log_data[2048];
  int64_t log_data_len = 2048;
  int64_t group_data_checksum = -1;
  EXPECT_EQ(OB_SUCCESS, log_entry_header.generate_header(log_data, log_data_len, max_scn));
  static const int64_t DATA_BUF_LEN = 64 * 1024 * 1024;
  int64_t group_header_size = LogGroupEntryHeader::HEADER_SER_SIZE;
  int64_t pos = 0;
  log_entry_header.serialize(data_buf_ + group_header_size, DATA_BUF_LEN, pos);
  EXPECT_TRUE(pos > 0);
  memcpy(data_buf_ + group_header_size + pos, log_data, log_data_len);
  int64_t dser_pos = 0;
  // test log_entry serialize/deserialize
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_entry.deserialize(NULL, DATA_BUF_LEN - group_header_size, dser_pos));
  int64_t short_buf_size = log_entry_header.get_serialize_size() - 10;
  EXPECT_EQ(OB_BUF_NOT_ENOUGH, log_entry.deserialize(data_buf_ + group_header_size, short_buf_size, dser_pos));
  EXPECT_EQ(OB_SUCCESS, log_entry.deserialize(data_buf_ + group_header_size, DATA_BUF_LEN - group_header_size, dser_pos));
  EXPECT_EQ(true, log_entry.check_integrity());
  int64_t new_ser_pos = 0;
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_entry.serialize(NULL, DATA_BUF_LEN - group_header_size, new_ser_pos));
  EXPECT_EQ(OB_BUF_NOT_ENOUGH, log_entry.serialize(data_buf_ + group_header_size, dser_pos - 10, new_ser_pos));
  EXPECT_EQ(OB_SUCCESS, log_entry.serialize(data_buf_ + group_header_size, DATA_BUF_LEN - group_header_size, new_ser_pos));
  EXPECT_EQ(dser_pos, new_ser_pos);

  int64_t log_entry_size = pos + log_data_len;
  // gen 2nd log entry
  max_scn.convert_for_logservice(222222);
  EXPECT_EQ(OB_SUCCESS, log_entry_header.generate_header(log_data, log_data_len, max_scn));
  pos = 0;
  log_entry_header.serialize(data_buf_ + group_header_size + log_entry_size, DATA_BUF_LEN, pos);
  EXPECT_TRUE(pos > 0);
  memcpy(data_buf_ + group_header_size + log_entry_size + pos, log_data, log_data_len);
  log_entry_size += (pos + log_data_len);
  // gen group log
  LogWriteBuf write_buf;
  EXPECT_EQ(OB_INVALID_ARGUMENT, group_header.generate(false, false, write_buf, log_entry_size, max_scn, log_id,
      committed_end_lsn, log_proposal_id, group_data_checksum));
  const int64_t total_group_log_size = group_header_size + log_entry_size;
  const int64_t first_part_len = total_group_log_size / 2;
  EXPECT_TRUE(first_part_len > 0);
  const int64_t second_part_len = total_group_log_size - first_part_len;
  // continous buf
  EXPECT_EQ(OB_SUCCESS, write_buf.push_back(data_buf_, first_part_len));
  EXPECT_EQ(OB_SUCCESS, write_buf.push_back(data_buf_ + first_part_len, second_part_len));
  EXPECT_EQ(OB_SUCCESS, group_header.generate(false, false, write_buf, log_entry_size, max_scn, log_id,
      committed_end_lsn, log_proposal_id, group_data_checksum));
  // non-continous buf
  group_header.reset();
  write_buf.reset();
  char *second_buf = (char *)ob_malloc(second_part_len, ObNewModIds::TEST);
  EXPECT_TRUE(NULL != second_buf);
  memcpy(second_buf, data_buf_ + first_part_len, second_part_len);
  EXPECT_EQ(OB_SUCCESS, write_buf.push_back(data_buf_, first_part_len));
  EXPECT_EQ(OB_SUCCESS, write_buf.push_back(second_buf, second_part_len));
  EXPECT_EQ(OB_SUCCESS, group_header.generate(false, false, write_buf, log_entry_size, max_scn, log_id,
      committed_end_lsn, log_proposal_id, group_data_checksum));
  int64_t accum_checksum = 100;
  (void) group_header.update_accumulated_checksum(accum_checksum);
  // calculate header parity flag
  (void) group_header.update_header_checksum();
  pos = 0;
  EXPECT_EQ(OB_INVALID_ARGUMENT, group_header.serialize(NULL, DATA_BUF_LEN, pos));
  EXPECT_EQ(OB_INVALID_ARGUMENT, group_header.serialize(data_buf_, 0, pos));
  EXPECT_EQ(OB_SUCCESS, group_header.serialize(data_buf_, DATA_BUF_LEN, pos));
  EXPECT_TRUE(pos > 0);
  int64_t group_entry_size = pos + log_entry_size;
  EXPECT_TRUE(group_header.check_integrity(data_buf_ + group_header_size, group_entry_size - group_header_size));
  // append disk log
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_sw_.append_disk_log(lsn, group_entry));
  EXPECT_EQ(OB_SUCCESS, group_entry.generate(group_header, data_buf_ + group_header_size));
  lsn.reset();
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_sw_.append_disk_log(lsn, group_entry));
  lsn.val_ = 0;
  EXPECT_EQ(OB_SUCCESS, log_sw_.append_disk_log(lsn, group_entry));
  // gen new group entry
  log_id++;
  uint64_t new_val = max_scn.get_val_for_logservice() + 100;
  max_scn.convert_for_logservice(new_val);
  lsn.val_ += group_entry_size;
  // gen group log
  LogWriteBuf write_buf1;
  EXPECT_EQ(OB_SUCCESS, write_buf1.push_back(data_buf_, log_entry_size+group_header_size));
  EXPECT_EQ(OB_SUCCESS, group_header.generate(false, false, write_buf, log_entry_size, max_scn, log_id,
      committed_end_lsn, log_proposal_id, group_data_checksum));
  accum_checksum += 100;
  (void) group_header.update_accumulated_checksum(accum_checksum);
  // calculate header parity flag
  (void) group_header.update_header_checksum();
  pos = 0;
  group_header.serialize(data_buf_, DATA_BUF_LEN, pos);
  EXPECT_TRUE(pos > 0);
  group_entry_size = pos + log_entry_size;
  EXPECT_TRUE(group_header.check_integrity(data_buf_ + group_header_size, group_entry_size - group_header_size));
  EXPECT_EQ(OB_SUCCESS, group_entry.generate(group_header, data_buf_ + group_header_size));
  EXPECT_EQ(OB_SUCCESS, log_sw_.append_disk_log(lsn, group_entry));
}

TEST_F(TestLogSlidingWindow, test_group_entry_truncate)
{
  PALF_LOG(INFO, "begin test_group_entry_truncate");
  LSN lsn(0);
  LogGroupEntry group_entry;
  LogGroupEntryHeader group_header;
  share::SCN truncate_scn;
  truncate_scn.convert_for_logservice(111113);
  int64_t pre_accum_checksum = 123456;
  EXPECT_EQ(OB_INVALID_ARGUMENT, group_header.truncate(NULL, 1024, truncate_scn, pre_accum_checksum));
  EXPECT_EQ(OB_INVALID_ARGUMENT, group_header.truncate(data_buf_, 0, truncate_scn, pre_accum_checksum));
  PalfBaseInfo base_info;
  gen_default_palf_base_info_(base_info);
  EXPECT_EQ(OB_SUCCESS, log_sw_.init(palf_id_, self_, &mock_state_mgr_,
        &mock_mm_, &mock_mode_mgr_, &mock_log_engine_, &palf_fs_cb_, alloc_mgr_, plugins_, base_info, true));
  int64_t curr_proposal_id = 10;
  mock_state_mgr_.mock_proposal_id_ = curr_proposal_id;
  // generate new group entry
  LogEntry log_entry;
  LogEntryHeader log_entry_header;
  share::SCN max_scn;
  max_scn.convert_for_logservice(111111);
  int64_t log_id = 1;
  LSN committed_end_lsn(0);
  int64_t log_proposal_id = 20;
  char log_data[2048];
  int64_t log_data_len = 2048;
  int64_t group_data_checksum = -1;
  EXPECT_EQ(OB_SUCCESS, log_entry_header.generate_header(log_data, log_data_len, max_scn));
  static const int64_t DATA_BUF_LEN = 64 * 1024 * 1024;
  int64_t group_header_size = LogGroupEntryHeader::HEADER_SER_SIZE;
  int64_t pos = 0;
  log_entry_header.serialize(data_buf_ + group_header_size, DATA_BUF_LEN, pos);
  EXPECT_TRUE(pos > 0);
  memcpy(data_buf_ + group_header_size + pos, log_data, log_data_len);
  int64_t dser_pos = 0;
  int64_t log_entry_size = pos + log_data_len;
  // gen 2nd log entry
  max_scn.convert_for_logservice(222222);
  EXPECT_EQ(OB_SUCCESS, log_entry_header.generate_header(log_data, log_data_len, max_scn));
  pos = 0;
  log_entry_header.serialize(data_buf_ + group_header_size + log_entry_size, DATA_BUF_LEN, pos);
  EXPECT_TRUE(pos > 0);
  memcpy(data_buf_ + group_header_size + log_entry_size + pos, log_data, log_data_len);
  log_entry_size += (pos + log_data_len);
  // gen group log
  LogWriteBuf write_buf;
  EXPECT_EQ(OB_INVALID_ARGUMENT, group_header.generate(false, false, write_buf, log_entry_size, max_scn, log_id,
      committed_end_lsn, log_proposal_id, group_data_checksum));
  const int64_t total_group_log_size = group_header_size + log_entry_size;
  const int64_t first_part_len = total_group_log_size / 2;
  EXPECT_TRUE(first_part_len > 0);
  const int64_t second_part_len = total_group_log_size - first_part_len;
  // continous buf
  EXPECT_EQ(OB_SUCCESS, write_buf.push_back(data_buf_, first_part_len));
  EXPECT_EQ(OB_SUCCESS, write_buf.push_back(data_buf_ + first_part_len, second_part_len));
  EXPECT_EQ(OB_SUCCESS, group_header.generate(false, false, write_buf, log_entry_size, max_scn, log_id,
      committed_end_lsn, log_proposal_id, group_data_checksum));
  int64_t accum_checksum = 100;
  (void) group_header.update_accumulated_checksum(accum_checksum);
  // calculate header parity flag
  (void) group_header.update_header_checksum();
  pos = 0;
  EXPECT_EQ(OB_INVALID_ARGUMENT, group_header.serialize(NULL, DATA_BUF_LEN, pos));
  EXPECT_EQ(OB_INVALID_ARGUMENT, group_header.serialize(data_buf_, 0, pos));
  EXPECT_EQ(OB_SUCCESS, group_header.serialize(data_buf_, DATA_BUF_LEN, pos));
  EXPECT_TRUE(pos == group_header_size);
  int64_t group_entry_size = pos + log_entry_size;
  EXPECT_TRUE(group_header.check_integrity(data_buf_ + group_header_size, group_entry_size - group_header_size));
  EXPECT_EQ(OB_SUCCESS, group_entry.generate(group_header, data_buf_ + group_header_size));
  EXPECT_TRUE(group_entry.check_integrity());
  EXPECT_EQ(OB_SUCCESS, group_header.truncate(data_buf_ + group_header_size, log_entry_size, truncate_scn, pre_accum_checksum));
}

} // END of unittest
} // end of oceanbase

int main(int argc, char **argv)
{
  system("rm -f ./test_log_sliding_window.log");
  OB_LOGGER.set_file_name("test_log_sliding_window.log", true);
  OB_LOGGER.set_log_level("TRACE");
  PALF_LOG(INFO, "begin unittest::test_log_sliding_window");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
