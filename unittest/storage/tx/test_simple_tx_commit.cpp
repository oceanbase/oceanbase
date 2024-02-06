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

#include <gtest/gtest.h>
#include <vector>
#define private public
#define protected public
#include "ob_mock_2pc_ctx.h"

namespace oceanbase
{
using namespace ::testing;
using namespace transaction;

namespace unittest
{
class TestMockOb2pcCtx : public ::testing::Test
{
protected:
  virtual void SetUp() override
  {
    mailbox_mgr_.reset();
  }
  virtual void TearDown() override
  {
    mailbox_mgr_.reset();
  }
public:
  ObMailBoxMgr<ObTwoPhaseCommitMsgType> mailbox_mgr_;
};

TEST_F(TestMockOb2pcCtx, test_simple_commit1)
{
  MockOb2pcCtx ctx1;
  MockOb2pcCtx ctx2;
  ctx1.init(&mailbox_mgr_);
  ctx2.init(&mailbox_mgr_);
  auto addr1 = ctx1.get_addr();
  auto addr2 = ctx2.get_addr();
  MockObParticipants participants;
  participants.push_back(addr1);
  participants.push_back(addr2);

  // ========== Two Phase Commit prepare Phase ==========
  // ctx1 start to commit
  ctx1.commit(participants);
  // ctx2 handle prepare request
  EXPECT_EQ(OB_SUCCESS, ctx2.handle());
  // ctx2 handle prepare request
  EXPECT_EQ(OB_SUCCESS, ctx2.apply());
  // ctx1 handle prepare response
  EXPECT_EQ(OB_SUCCESS, ctx1.handle());
  // ctx1 apply prepare log
  EXPECT_EQ(OB_SUCCESS, ctx1.apply());

  // ========== Two Phase Commit pre commit Phase ======
  EXPECT_EQ(OB_SUCCESS, ctx2.handle());
  EXPECT_EQ(OB_SUCCESS, ctx1.handle());

  // ========== Two Phase Commit commit Phase ==========
  // ctx2 handle commit request
  EXPECT_EQ(OB_SUCCESS, ctx2.handle());
  // ctx2 apply commit log
  EXPECT_EQ(OB_SUCCESS, ctx2.apply());
  // ctx1 handle commit response
  EXPECT_EQ(OB_SUCCESS, ctx1.handle());
  // ctx1 apply commit log
  EXPECT_EQ(OB_SUCCESS, ctx1.apply());

  // ========== Two Phase Commit clear Phase ==========
  // ctx2 handle clear request
  EXPECT_EQ(OB_SUCCESS, ctx2.handle());
  // ctx2 apply clear log
  EXPECT_EQ(OB_SUCCESS, ctx2.apply());
  // ctx1 apply clear log
  EXPECT_EQ(OB_SUCCESS, ctx1.apply());

  // ========== Check Test Valid ==========
  EXPECT_EQ(true, ctx1.check_status_valid(true/*should commit*/));
  EXPECT_EQ(true, ctx2.check_status_valid(true/*should commit*/));
}

TEST_F(TestMockOb2pcCtx, test_simple_commit2)
{
  MockOb2pcCtx ctx1;
  MockOb2pcCtx ctx2;
  ctx1.init(&mailbox_mgr_);
  ctx2.init(&mailbox_mgr_);
  auto addr1 = ctx1.get_addr();
  auto addr2 = ctx2.get_addr();
  MockObParticipants participants;
  participants.push_back(addr1);
  participants.push_back(addr2);

  // ========== Two Phase Commit prepare Phase ==========
  // ctx1 start to commit
  ctx1.commit(participants);
  // ctx2 handle prepare request
  EXPECT_EQ(OB_SUCCESS, ctx2.handle());
  // ctx1 apply prepare log
  EXPECT_EQ(OB_SUCCESS, ctx1.apply());
  // ctx2 handle prepare request
  EXPECT_EQ(OB_SUCCESS, ctx2.apply());
  // ctx1 handle prepare response
  EXPECT_EQ(OB_SUCCESS, ctx1.handle());

  // ========== Two Phase Commit pre commit Phase ======
  EXPECT_EQ(OB_SUCCESS, ctx2.handle());
  EXPECT_EQ(OB_SUCCESS, ctx1.handle());

  // ========== Two Phase Commit commit Phase ==========
  // ctx2 handle commit request
  EXPECT_EQ(OB_SUCCESS, ctx2.handle());
  // ctx1 apply commit log
  EXPECT_EQ(OB_SUCCESS, ctx1.apply());
  // ctx2 apply commit log
  EXPECT_EQ(OB_SUCCESS, ctx2.apply());
  // ctx1 handle commit response
  EXPECT_EQ(OB_SUCCESS, ctx1.handle());

  // ========== Two Phase Commit clear Phase ==========
  // ctx1 apply clear log
  EXPECT_EQ(OB_SUCCESS, ctx1.apply());
  // ctx2 handle clear request
  EXPECT_EQ(OB_SUCCESS, ctx2.handle());
  // ctx2 apply clear log
  EXPECT_EQ(OB_SUCCESS, ctx2.apply());

  // ========== Check Test Valid ==========
  EXPECT_EQ(true, ctx1.check_status_valid(true/*should commit*/));
  EXPECT_EQ(true, ctx2.check_status_valid(true/*should commit*/));
}

TEST_F(TestMockOb2pcCtx, test_simple_abort1)
{
  MockOb2pcCtx ctx1;
  MockOb2pcCtx ctx2;
  ctx1.init(&mailbox_mgr_);
  ctx2.init(&mailbox_mgr_);
  auto addr1 = ctx1.get_addr();
  auto addr2 = ctx2.get_addr();
  MockObParticipants participants;
  participants.push_back(addr1);
  participants.push_back(addr2);

  // ========== Two Phase Commit prepare Phase ==========
  // ctx2 abnormally abort
  EXPECT_EQ(OB_SUCCESS, ctx2.abort());
  // ctx2 apply abort log
  EXPECT_EQ(OB_SUCCESS, ctx2.apply());
  // ctx1 start to commit
  ctx1.commit(participants);
  // ctx2 handle prepare request
  EXPECT_EQ(OB_SUCCESS, ctx2.handle());
  // ctx1 apply prepare log
  EXPECT_EQ(OB_SUCCESS, ctx1.apply());
  // ctx1 handle abort response
  EXPECT_EQ(OB_SUCCESS, ctx1.handle());

  // ========== Two Phase Commit abort Phase ==========
  // ctx2 handle abort request
  EXPECT_EQ(OB_SUCCESS, ctx2.handle());
  // ctx1 apply abort log
  EXPECT_EQ(OB_SUCCESS, ctx1.apply());

  // ========== Two Phase Commit clear Phase ==========
  // ctx1 apply clear log
  EXPECT_EQ(OB_SUCCESS, ctx1.apply());
  // ctx2 handle clear request
  EXPECT_EQ(OB_SUCCESS, ctx2.handle());
  // ctx2 apply clear log
  EXPECT_EQ(OB_SUCCESS, ctx2.apply());

  // ========== Check Test Valid ==========
  EXPECT_EQ(true, ctx1.check_status_valid(false/*should commit*/));
  EXPECT_EQ(true, ctx2.check_status_valid(false/*should commit*/));
}

TEST_F(TestMockOb2pcCtx, test_simple_abort2)
{
  MockOb2pcCtx ctx1;
  MockOb2pcCtx ctx2;
  ctx1.init(&mailbox_mgr_);
  ctx2.init(&mailbox_mgr_);
  auto addr1 = ctx1.get_addr();
  auto addr2 = ctx2.get_addr();
  MockObParticipants participants;
  participants.push_back(addr1);
  participants.push_back(addr2);

  // ========== Two Phase Commit prepare Phase ==========
  // ctx1 start to commit
  ctx1.commit(participants);
  // ctx1 apply prepare log
  EXPECT_EQ(OB_SUCCESS, ctx1.apply());
  // ctx2 abnormally abort
  EXPECT_EQ(OB_SUCCESS, ctx2.abort());
  // ctx1 apply abort log
  EXPECT_EQ(OB_SUCCESS, ctx2.apply());
  // ctx2 handle prepare request
  EXPECT_EQ(OB_SUCCESS, ctx2.handle());
  // ctx1 handle abort response
  EXPECT_EQ(OB_SUCCESS, ctx1.handle());

  // ========== Two Phase Commit abort Phase ==========
  // ctx1 apply abort log
  EXPECT_EQ(OB_SUCCESS, ctx1.apply());
  // ctx2 handle abort request
  EXPECT_EQ(OB_SUCCESS, ctx2.handle());

  // ========== Two Phase Commit clear Phase ==========
  // ctx2 handle clear request
  EXPECT_EQ(OB_SUCCESS, ctx2.handle());
  // ctx2 apply clear log
  EXPECT_EQ(OB_SUCCESS, ctx2.apply());
  // ctx1 apply clear log
  EXPECT_EQ(OB_SUCCESS, ctx1.apply());

  // ========== Check Test Valid ==========
  EXPECT_EQ(true, ctx1.check_status_valid(false/*should commit*/));
  EXPECT_EQ(true, ctx2.check_status_valid(false/*should commit*/));
}

TEST_F(TestMockOb2pcCtx, test_single_participants_prepare)
{
  MockOb2pcCtx ctx1;
  ctx1.init(&mailbox_mgr_);
  auto addr1 = ctx1.get_addr();
  MockObParticipants participants;
  participants.push_back(addr1);
  ctx1.participants_.assign(participants.begin(), participants.end());

  // ========== Two Phase Commit prepare Phase ==========
  // ctx1 start prepare state
  ctx1.downstream_state_ = ObTxState::PREPARE;
  ctx1.set_upstream_state(ObTxState::PREPARE);
  ctx1.handle_timeout();
  EXPECT_EQ(ObTxState::COMMIT, ctx1.get_upstream_state());
  EXPECT_EQ(ObTxState::PRE_COMMIT, ctx1.get_downstream_state());
}

TEST_F(TestMockOb2pcCtx, test_single_participants_precommit)
{
  MockOb2pcCtx ctx1;
  ctx1.init(&mailbox_mgr_);
  auto addr1 = ctx1.get_addr();
  MockObParticipants participants;
  participants.push_back(addr1);
  ctx1.participants_.assign(participants.begin(), participants.end());

  // ========== Two Phase Commit precommit Phase ==========
  ctx1.downstream_state_ = ObTxState::PREPARE;
  ctx1.set_upstream_state(ObTxState::PRE_COMMIT);
  ctx1.handle_timeout();
  EXPECT_EQ(ObTxState::COMMIT, ctx1.get_upstream_state());
  EXPECT_EQ(ObTxState::PRE_COMMIT, ctx1.get_downstream_state());
}

TEST_F(TestMockOb2pcCtx, test_single_participants_precommit2)
{
  MockOb2pcCtx ctx1;
  ctx1.init(&mailbox_mgr_);
  auto addr1 = ctx1.get_addr();
  MockObParticipants participants;
  participants.push_back(addr1);
  ctx1.participants_.assign(participants.begin(), participants.end());

  // ========== Two Phase Commit precommit Phase ==========
  ctx1.downstream_state_ = ObTxState::PRE_COMMIT;
  ctx1.set_upstream_state(ObTxState::PRE_COMMIT);
  ctx1.handle_timeout();
  EXPECT_EQ(ObTxState::COMMIT, ctx1.get_upstream_state());
  EXPECT_EQ(ObTxState::PRE_COMMIT, ctx1.get_downstream_state());
  ctx1.apply();
  EXPECT_EQ(ObTxState::CLEAR, ctx1.get_upstream_state());
  EXPECT_EQ(ObTxState::COMMIT, ctx1.get_downstream_state());
}

TEST_F(TestMockOb2pcCtx, test_single_participants_commit)
{
  MockOb2pcCtx ctx1;
  ctx1.init(&mailbox_mgr_);
  auto addr1 = ctx1.get_addr();
  MockObParticipants participants;
  participants.push_back(addr1);
  ctx1.participants_.assign(participants.begin(), participants.end());

  // ========== Two Phase Commit precommit Phase ==========
  ctx1.downstream_state_ = ObTxState::COMMIT;
  ctx1.set_upstream_state(ObTxState::COMMIT);
  ctx1.handle_timeout();
  EXPECT_EQ(ObTxState::CLEAR, ctx1.get_upstream_state());
  EXPECT_EQ(ObTxState::COMMIT, ctx1.get_downstream_state());
  ctx1.apply();
  EXPECT_EQ(ObTxState::CLEAR, ctx1.get_upstream_state());
  EXPECT_EQ(ObTxState::CLEAR, ctx1.get_downstream_state());
}

}
}

int main(int argc, char **argv)
{
  system("rm -rf test_simple_tx_commit.log*");
  OB_LOGGER.set_file_name("test_simple_tx_commit.log");
  OB_LOGGER.set_log_level("INFO");
  STORAGE_LOG(INFO, "begin unittest: test simple mock ob tx ctx");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
