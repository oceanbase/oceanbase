/**
 * Copyright (c) 2026 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include <gtest/gtest.h>
#define private public
#include "logservice/ob_log_handler.h"
#undef private
#include "mock_palf_handle_impl_for_async.h"

namespace oceanbase
{
namespace unittest
{
using namespace common;
using namespace logservice;
using namespace palf;

// Exercise the real LogHandler and PalfHandle forwarding paths. Only PALF's
// storage/consensus implementation is replaced by a synchronous test double.
class SyncModePalfHandleImpl : public MockAsyncPalfHandleImpl
{
public:
  int get_role(ObRole &role, int64_t &proposal_id, bool &pending) const override
  {
    ++role_queries_;
    role = role_;
    proposal_id = proposal_id_ + (change_proposal_on_append_ && role_queries_ > 1 ? 1 : 0);
    pending = pending_;
    return role_ret_;
  }

  int submit_log(const PalfAppendOptions &opts, const char *buf, const int64_t len,
                 const share::SCN &ref_scn, AppendCb *cb, LSN &lsn, share::SCN &scn) override
  {
    EXPECT_EQ(proposal_id_, opts.proposal_id);
    EXPECT_EQ(nullptr, cb);
    ObSyncModeLog log;
    int64_t pos = 0;
    const int ret = log.deserialize(buf, len, pos);
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_EQ(ObSyncModeLogType::ASYNC, log.get_log_type());
    ++append_count_;
    lsn = end_lsn_;
    end_lsn_ = end_lsn_ + len;
    scn = share::SCN::plus(ref_scn, 1);
    return ret;
  }

  const LSN get_end_lsn() const override { return end_lsn_; }

  int change_sync_mode(const int64_t proposal_id, const int64_t mode_version,
                       const SyncMode &mode, int64_t &new_mode_version,
                       int64_t &out_proposal_id) override
  {
    EXPECT_EQ(proposal_id_, proposal_id);
    EXPECT_EQ(SyncMode::ASYNC, mode);
    ++mode_change_count_;
    mode_ = mode;
    new_mode_version = mode_version + 1;
    out_proposal_id = proposal_id_;
    return OB_SUCCESS;
  }

  ObRole role_ = LEADER;
  int64_t proposal_id_ = 10;
  bool pending_ = false;
  int role_ret_ = OB_SUCCESS;
  bool change_proposal_on_append_ = false;
  mutable int64_t role_queries_ = 0;
  int64_t append_count_ = 0;
  int64_t mode_change_count_ = 0;
  SyncMode mode_ = SyncMode::PRE_ASYNC;
  LSN end_lsn_ = LSN(0);
};

class TestLogSyncModeManager : public ::testing::Test
{
protected:
  void SetUp() override
  {
    palf_handle_.palf_handle_impl_ = &palf_impl_;
    log_handler_.palf_handle_ = &palf_handle_;
    log_handler_.id_ = 1001;
    log_handler_.is_inited_ = true;
    log_handler_.is_in_stop_state_ = false;
    log_handler_.is_offline_ = false;
    log_handler_.switch_role_and_sync_mode(FOLLOWER, 9, SyncMode::PRE_ASYNC);
    ASSERT_EQ(OB_SUCCESS, manager_.init(&log_handler_));
  }

  int finish_downgrade()
  {
    share::SCN end_scn;
    int64_t new_mode_version = INVALID_PROPOSAL_ID;
    share::ObSyncStandbyStatusAttr protection_info;
    return manager_.handle_sync_mode_downgrade_finish(1, true /*need_write_log*/, share::ObLSID(1001),
        share::SCN::base_scn(), protection_info, share::SCN::min_scn(), end_scn,
        new_mode_version, ObTimeUtility::current_time() + 5 * 1000 * 1000);
  }

  SyncModePalfHandleImpl palf_impl_;
  PalfHandle palf_handle_;
  ObLogHandler log_handler_;
  ObSyncModeManager manager_;
};

TEST_F(TestLogSyncModeManager, downgrade_with_cached_follower)
{
  ObRole role = INVALID_ROLE;
  int64_t proposal_id = INVALID_PROPOSAL_ID;
  ASSERT_EQ(OB_SUCCESS, log_handler_.get_role(role, proposal_id));
  ASSERT_EQ(FOLLOWER, role);
  ASSERT_EQ(9, proposal_id);
  EXPECT_EQ(OB_SUCCESS, finish_downgrade());
  EXPECT_EQ(1, palf_impl_.append_count_);
  EXPECT_EQ(SyncMode::ASYNC, palf_impl_.mode_);
  // A downgrade must not promote the LogHandler before its role callback runs.
  EXPECT_EQ(OB_SUCCESS, log_handler_.get_role(role, proposal_id));
  EXPECT_EQ(FOLLOWER, role);
  EXPECT_EQ(9, proposal_id);
}

TEST_F(TestLogSyncModeManager, downgrade_with_stale_cached_proposal)
{
  log_handler_.switch_role_and_sync_mode(LEADER, 9, SyncMode::PRE_ASYNC);
  EXPECT_EQ(OB_SUCCESS, finish_downgrade());
  EXPECT_EQ(1, palf_impl_.append_count_);
  EXPECT_EQ(SyncMode::ASYNC, palf_impl_.mode_);
}

TEST_F(TestLogSyncModeManager, reject_palf_follower)
{
  palf_impl_.role_ = FOLLOWER;
  EXPECT_EQ(OB_NOT_MASTER, finish_downgrade());
  EXPECT_EQ(0, palf_impl_.append_count_);
  EXPECT_EQ(0, palf_impl_.mode_change_count_);
}

TEST_F(TestLogSyncModeManager, reject_pending_leader)
{
  palf_impl_.pending_ = true;
  EXPECT_EQ(OB_NOT_MASTER, finish_downgrade());
  EXPECT_EQ(0, palf_impl_.append_count_);
  EXPECT_EQ(0, palf_impl_.mode_change_count_);
}

TEST_F(TestLogSyncModeManager, propagate_palf_query_error)
{
  palf_impl_.role_ret_ = OB_EAGAIN;
  EXPECT_EQ(OB_EAGAIN, finish_downgrade());
  EXPECT_EQ(0, palf_impl_.append_count_);
  EXPECT_EQ(0, palf_impl_.mode_change_count_);
}

TEST_F(TestLogSyncModeManager, reject_proposal_change_before_append)
{
  palf_impl_.change_proposal_on_append_ = true;
  EXPECT_EQ(OB_NOT_MASTER, finish_downgrade());
  EXPECT_EQ(0, palf_impl_.append_count_);
  EXPECT_EQ(0, palf_impl_.mode_change_count_);
}

TEST_F(TestLogSyncModeManager, reject_uninitialized_or_stopped_handler)
{
  log_handler_.is_inited_ = false;
  EXPECT_EQ(OB_NOT_INIT, finish_downgrade());
  log_handler_.is_inited_ = true;
  log_handler_.is_in_stop_state_ = true;
  EXPECT_EQ(OB_NOT_RUNNING, finish_downgrade());
  EXPECT_EQ(0, palf_impl_.role_queries_);
}
} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_log_sync_mode_mgr.log", true);
  OB_LOGGER.set_log_level("WARN");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
