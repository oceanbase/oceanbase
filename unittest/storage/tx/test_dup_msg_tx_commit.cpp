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
#include "ob_mock_2pc_ctx.h"

// You can grep [DUP_MSG] to find all duplicated msg for testing

namespace oceanbase
{
using namespace ::testing;
using namespace transaction;

namespace unittest
{
class TestDupMsgMockOb2pcCtx : public ::testing::Test
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
  int dup_and_handle_msg(const ObMail<ObTwoPhaseCommitMsgType> &dup_mail,
                         MockOb2pcCtx *ctx)
  {
    mailbox_mgr_.send_to_head(dup_mail, dup_mail.to_);
    return ctx->handle();
  }
public:
  ObMailBoxMgr<ObTwoPhaseCommitMsgType> mailbox_mgr_;
};

TEST_F(TestDupMsgMockOb2pcCtx, test_dup_tx_commit_req)
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
  EXPECT_EQ(OB_SUCCESS, ctx2.handle_all());
  // [DUP_MSG]: ctx1 start to commit
  ctx1.commit(participants);
  // ctx2 handle prepare request
  EXPECT_EQ(OB_SUCCESS, ctx2.apply());
  // [DUP_MSG]: ctx1 start to commit
  ctx1.commit(participants);
  // ctx1 handle prepare response
  EXPECT_EQ(OB_SUCCESS, ctx1.handle_all());
  // [DUP_MSG]: ctx1 start to commit
  ctx1.commit(participants);
  // ctx1 apply prepare log
  EXPECT_EQ(OB_SUCCESS, ctx1.apply());

  // ========== Two Phase Commit pre commit Phase ======
  // [DUP_MSG]: ctx1 start to commit
  ctx1.commit(participants);
  // ctx2 handle commit request
  EXPECT_EQ(OB_SUCCESS, ctx2.handle_all());
  // [DUP_MSG]: ctx1 start to commit
  ctx1.commit(participants);
  // ctx1 handle commit response
  EXPECT_EQ(OB_SUCCESS, ctx1.handle_all());
  // [DUP_MSG]: ctx1 start to commit
  ctx1.commit(participants);

  // ========== Two Phase Commit commit Phase ==========
  // [DUP_MSG]: ctx1 start to commit
  ctx1.commit(participants);
  // ctx2 handle commit request
  EXPECT_EQ(OB_SUCCESS, ctx2.handle_all());
  // [DUP_MSG]: ctx1 start to commit
  ctx1.commit(participants);
  // ctx2 apply commit log
  EXPECT_EQ(OB_SUCCESS, ctx2.apply());
  // [DUP_MSG]: ctx1 start to commit
  ctx1.commit(participants);
  // ctx1 handle commit response
  EXPECT_EQ(OB_SUCCESS, ctx1.handle_all());
  // [DUP_MSG]: ctx1 start to commit
  ctx1.commit(participants);
  // ctx1 apply commit log
  EXPECT_EQ(OB_SUCCESS, ctx1.apply());

  // ========== Two Phase Commit clear Phase ==========
  // [DUP_MSG]: ctx1 start to commit
  ctx1.commit(participants);
  // ctx2 handle clear request
  EXPECT_EQ(OB_SUCCESS, ctx2.handle_all());
  // [DUP_MSG]: ctx1 start to commit
  ctx1.commit(participants);
  // ctx2 apply clear log
  EXPECT_EQ(OB_SUCCESS, ctx2.apply());
  // [DUP_MSG]: ctx1 start to commit
  ctx1.commit(participants);
  // ctx1 apply clear log
  EXPECT_EQ(OB_SUCCESS, ctx1.apply());

  // ========== Check Test Valid ==========
  EXPECT_EQ(true, ctx1.check_status_valid(true/*should commit*/));
  EXPECT_EQ(true, ctx2.check_status_valid(true/*should commit*/));
}

TEST_F(TestDupMsgMockOb2pcCtx, test_dup_2pc_prepare_req)
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

  // ========== Prepare Duplicated Messages ==========
  // Mock duplicated 2pc prepare mail
  ObMail<ObTwoPhaseCommitMsgType> dup_prepare_mail;
  EXPECT_EQ(OB_SUCCESS, dup_prepare_mail.init(addr1 /*from*/,
                                              addr2 /*to*/,
                                              sizeof(ObTwoPhaseCommitMsgType),
                                              ObTwoPhaseCommitMsgType::OB_MSG_TX_PREPARE_REQ));


  // ========== Two Phase Commit prepare Phase ==========
  // ctx1 start to commit
  ctx1.commit(participants);
  // ctx2 handle prepare request
  EXPECT_EQ(OB_SUCCESS, ctx2.handle_all());
  // [DUP_MSG]: ctx2 handle duplicated prepare request
  EXPECT_EQ(OB_SUCCESS, dup_and_handle_msg(dup_prepare_mail, &ctx2));
  // ctx1 apply prepare log
  EXPECT_EQ(OB_SUCCESS, ctx1.apply());
  // [DUP_MSG]: ctx2 handle duplicated prepare request
  EXPECT_EQ(OB_SUCCESS, dup_and_handle_msg(dup_prepare_mail, &ctx2));
  // ctx2 handle prepare request
  EXPECT_EQ(OB_SUCCESS, ctx2.apply());
  // [DUP_MSG]: ctx2 handle duplicated prepare request
  EXPECT_EQ(OB_SUCCESS, dup_and_handle_msg(dup_prepare_mail, &ctx2));
  // ctx1 handle prepare response
  EXPECT_EQ(OB_SUCCESS, ctx1.handle_all());

  // ========== Two Phase Commit pre commit Phase ======
  // [DUP_MSG]: ctx2 handle duplicated prepare request
  EXPECT_EQ(OB_SUCCESS, dup_and_handle_msg(dup_prepare_mail, &ctx2));
  // ctx2 handle commit request
  EXPECT_EQ(OB_SUCCESS, ctx2.handle_all());
  // [DUP_MSG]: ctx2 handle duplicated prepare request
  EXPECT_EQ(OB_SUCCESS, dup_and_handle_msg(dup_prepare_mail, &ctx2));
  // [DUP_MSG]: ctx2 handle duplicated prepare request
  EXPECT_EQ(OB_SUCCESS, dup_and_handle_msg(dup_prepare_mail, &ctx2));
  // [DUP_MSG]: ctx2 handle duplicated prepare request
  EXPECT_EQ(OB_SUCCESS, dup_and_handle_msg(dup_prepare_mail, &ctx2));
  // ctx1 handle commit response
  EXPECT_EQ(OB_SUCCESS, ctx1.handle_all());

  // ========== Two Phase Commit commit Phase ==========
  // [DUP_MSG]: ctx2 handle duplicated prepare request
  EXPECT_EQ(OB_SUCCESS, dup_and_handle_msg(dup_prepare_mail, &ctx2));
  // ctx2 handle commit request
  EXPECT_EQ(OB_SUCCESS, ctx2.handle_all());
  // [DUP_MSG]: ctx2 handle duplicated prepare request
  EXPECT_EQ(OB_SUCCESS, dup_and_handle_msg(dup_prepare_mail, &ctx2));
  // ctx2 apply commit log
  EXPECT_EQ(OB_SUCCESS, ctx2.apply());
  // [DUP_MSG]: ctx2 handle duplicated prepare request
  EXPECT_EQ(OB_SUCCESS, dup_and_handle_msg(dup_prepare_mail, &ctx2));
  // ctx1 handle commit response
  EXPECT_EQ(OB_SUCCESS, ctx1.handle_all());
  // [DUP_MSG]: ctx2 handle duplicated prepare request
  EXPECT_EQ(OB_SUCCESS, dup_and_handle_msg(dup_prepare_mail, &ctx2));
  // ctx1 apply commit log
  EXPECT_EQ(OB_SUCCESS, ctx1.apply());

  // ========== Two Phase Commit clear Phase ==========
  // [DUP_MSG]: ctx2 handle duplicated prepare request
  EXPECT_EQ(OB_SUCCESS, dup_and_handle_msg(dup_prepare_mail, &ctx2));
  // ctx1 apply clear log
  EXPECT_EQ(OB_SUCCESS, ctx1.apply());
  // [DUP_MSG]: ctx2 handle duplicated prepare request
  EXPECT_EQ(OB_SUCCESS, dup_and_handle_msg(dup_prepare_mail, &ctx2));
  // ctx2 handle clear request
  EXPECT_EQ(OB_SUCCESS, ctx2.handle_all());
  // [DUP_MSG]: ctx2 handle duplicated prepare request
  EXPECT_EQ(OB_SUCCESS, dup_and_handle_msg(dup_prepare_mail, &ctx2));
  // ctx2 apply clear log
  EXPECT_EQ(OB_SUCCESS, ctx2.apply());

  // ========== Check Test Valid ==========
  EXPECT_EQ(true, ctx1.check_status_valid(true/*should commit*/));
  EXPECT_EQ(true, ctx2.check_status_valid(true/*should commit*/));
}

TEST_F(TestDupMsgMockOb2pcCtx, test_dup_2pc_prepare_resp)
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

  // ========== Prepare Duplicated Messages ==========
  // Mock duplicated 2pc prepare mail
  ObMail<ObTwoPhaseCommitMsgType> dup_prepare_mail;
  EXPECT_EQ(OB_SUCCESS, dup_prepare_mail.init(addr2 /*from*/,
                                              addr1 /*to*/,
                                              sizeof(ObTwoPhaseCommitMsgType),
                                              ObTwoPhaseCommitMsgType::OB_MSG_TX_PREPARE_RESP));


  // ========== Two Phase Commit prepare Phase ==========
  // ctx1 start to commit
  ctx1.commit(participants);
  // ctx2 handle prepare request
  EXPECT_EQ(OB_SUCCESS, ctx2.handle_all());
  // ctx2 handle prepare request
  EXPECT_EQ(OB_SUCCESS, ctx2.apply());
  // ctx1 handle prepare response
  EXPECT_EQ(OB_SUCCESS, ctx1.handle_all());
  // [DUP_MSG]: ctx1 handle duplicated prepare response
  EXPECT_EQ(OB_SUCCESS, dup_and_handle_msg(dup_prepare_mail, &ctx1));
  // ctx1 apply prepare log
  EXPECT_EQ(OB_SUCCESS, ctx1.apply());
  // [DUP_MSG]: ctx1 handle duplicated prepare response
  EXPECT_EQ(OB_SUCCESS, dup_and_handle_msg(dup_prepare_mail, &ctx1));

  // ========== Two Phase Commit pre commit Phase ======
  // [DUP_MSG]: ctx1handle duplicated prepare response
  EXPECT_EQ(OB_SUCCESS, dup_and_handle_msg(dup_prepare_mail, &ctx1));
  // ctx2 handle commit request
  EXPECT_EQ(OB_SUCCESS, ctx2.handle_all());
  // [DUP_MSG]: ctx1 handle duplicated prepare response
  EXPECT_EQ(OB_SUCCESS, dup_and_handle_msg(dup_prepare_mail, &ctx1));
  // [DUP_MSG]: ctx1 handle duplicated prepare response
  EXPECT_EQ(OB_SUCCESS, dup_and_handle_msg(dup_prepare_mail, &ctx1));
  // [DUP_MSG]: ctx1 handle duplicated prepare response
  EXPECT_EQ(OB_SUCCESS, dup_and_handle_msg(dup_prepare_mail, &ctx1));
  // ctx1 handle commit response
  EXPECT_EQ(OB_SUCCESS, ctx1.handle_all());

  // ========== Two Phase Commit commit Phase ==========
  // [DUP_MSG]: ctx1handle duplicated prepare response
  EXPECT_EQ(OB_SUCCESS, dup_and_handle_msg(dup_prepare_mail, &ctx1));
  // ctx2 handle commit request
  EXPECT_EQ(OB_SUCCESS, ctx2.handle_all());
  // [DUP_MSG]: ctx1 handle duplicated prepare response
  EXPECT_EQ(OB_SUCCESS, dup_and_handle_msg(dup_prepare_mail, &ctx1));
  // ctx1 apply commit log
  EXPECT_EQ(OB_SUCCESS, ctx1.apply());
  // [DUP_MSG]: ctx1 handle duplicated prepare response
  EXPECT_EQ(OB_SUCCESS, dup_and_handle_msg(dup_prepare_mail, &ctx1));
  // ctx2 apply commit log
  EXPECT_EQ(OB_SUCCESS, ctx2.apply());
  // [DUP_MSG]: ctx1 handle duplicated prepare response
  EXPECT_EQ(OB_SUCCESS, dup_and_handle_msg(dup_prepare_mail, &ctx1));
  // ctx1 handle commit response
  EXPECT_EQ(OB_SUCCESS, ctx1.handle_all());

  // ========== Two Phase Commit clear Phase ==========
  // [DUP_MSG]: ctx1 handle duplicated prepare response
  EXPECT_EQ(OB_SUCCESS, dup_and_handle_msg(dup_prepare_mail, &ctx1));
  // ctx1 apply clear log
  EXPECT_EQ(OB_SUCCESS, ctx1.apply());
  // [DUP_MSG]: ctx1 handle duplicated prepare response
  EXPECT_EQ(OB_SUCCESS, dup_and_handle_msg(dup_prepare_mail, &ctx1));
  // ctx2 handle clear request
  EXPECT_EQ(OB_SUCCESS, ctx2.handle_all());
  // [DUP_MSG]: ctx1 handle duplicated prepare response
  EXPECT_EQ(OB_SUCCESS, dup_and_handle_msg(dup_prepare_mail, &ctx1));
  // ctx2 apply clear log
  EXPECT_EQ(OB_SUCCESS, ctx2.apply());

  // ========== Check Test Valid ==========
  EXPECT_EQ(true, ctx1.check_status_valid(true/*should commit*/));
  EXPECT_EQ(true, ctx2.check_status_valid(true/*should commit*/));
}

TEST_F(TestDupMsgMockOb2pcCtx, test_dup_2pc_commit_req)
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

  // ========== Prepare Duplicated Messages ==========
  // Mock duplicated 2pc commit mail
  ObMail<ObTwoPhaseCommitMsgType> dup_commit_mail;
  EXPECT_EQ(OB_SUCCESS, dup_commit_mail.init(addr1 /*from*/,
                                             addr2 /*to*/,
                                             sizeof(ObTwoPhaseCommitMsgType),
                                             ObTwoPhaseCommitMsgType::OB_MSG_TX_COMMIT_REQ));

  // ========== Two Phase Commit prepare Phase ==========
  // ctx1 start to commit
  ctx1.commit(participants);
  // ctx2 handle prepare request
  EXPECT_EQ(OB_SUCCESS, ctx2.handle_all());
  // ctx2 handle prepare request
  EXPECT_EQ(OB_SUCCESS, ctx2.apply());
  // ctx1 handle prepare response
  EXPECT_EQ(OB_SUCCESS, ctx1.handle_all());
  // ctx1 apply prepare log
  EXPECT_EQ(OB_SUCCESS, ctx1.apply());

  // ========== Two Phase Commit pre commit Phase ======
  // ctx2 handle commit request
  EXPECT_EQ(OB_SUCCESS, ctx2.handle_all());
  // [DUP_MSG]: ctx2 handle duplicated commit request
  EXPECT_EQ(OB_SUCCESS, dup_and_handle_msg(dup_commit_mail, &ctx2));
  // [DUP_MSG]: ctx2 handle duplicated commit request
  EXPECT_EQ(OB_SUCCESS, dup_and_handle_msg(dup_commit_mail, &ctx2));
  // [DUP_MSG]: ctx2 handle duplicated commit request
  EXPECT_EQ(OB_SUCCESS, dup_and_handle_msg(dup_commit_mail, &ctx2));
  // ctx1 handle commit response
  EXPECT_EQ(OB_SUCCESS, ctx1.handle_all());

  // ========== Two Phase Commit commit Phase ==========
  // ctx2 handle commit request
  EXPECT_EQ(OB_SUCCESS, ctx2.handle_all());
  // [DUP_MSG]: ctx2 handle duplicated commit request
  EXPECT_EQ(OB_SUCCESS, dup_and_handle_msg(dup_commit_mail, &ctx2));
  // ctx1 apply commit log
  EXPECT_EQ(OB_SUCCESS, ctx1.apply());
  // [DUP_MSG]: ctx2 handle duplicated commit request
  EXPECT_EQ(OB_SUCCESS, dup_and_handle_msg(dup_commit_mail, &ctx2));
  // ctx2 apply commit log
  EXPECT_EQ(OB_SUCCESS, ctx2.apply());
  // [DUP_MSG]: ctx2 handle duplicated commit request
  EXPECT_EQ(OB_SUCCESS, dup_and_handle_msg(dup_commit_mail, &ctx2));
  // ctx1 handle commit response
  EXPECT_EQ(OB_SUCCESS, ctx1.handle_all());

  // ========== Two Phase Commit clear Phase ==========
  // [DUP_MSG]: ctx2 handle duplicated commit request
  EXPECT_EQ(OB_SUCCESS, dup_and_handle_msg(dup_commit_mail, &ctx2));
  // ctx1 apply clear log
  EXPECT_EQ(OB_SUCCESS, ctx1.apply());
  // [DUP_MSG]: ctx2 handle duplicated commit request
  EXPECT_EQ(OB_SUCCESS, dup_and_handle_msg(dup_commit_mail, &ctx2));
  // ctx2 handle clear request
  EXPECT_EQ(OB_SUCCESS, ctx2.handle_all());
  // [DUP_MSG]: ctx2 handle duplicated commit request
  EXPECT_EQ(OB_SUCCESS, dup_and_handle_msg(dup_commit_mail, &ctx2));
  // ctx2 apply clear log
  EXPECT_EQ(OB_SUCCESS, ctx2.apply());

  // ========== Check Test Valid ==========
  EXPECT_EQ(true, ctx1.check_status_valid(true/*should commit*/));
  EXPECT_EQ(true, ctx2.check_status_valid(true/*should commit*/));
}

TEST_F(TestDupMsgMockOb2pcCtx, test_dup_2pc_commit_response)
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

  // ========== Prepare Duplicated Messages ==========
  // Mock duplicated 2pc commit mail
  ObMail<ObTwoPhaseCommitMsgType> dup_commit_mail;
  EXPECT_EQ(OB_SUCCESS, dup_commit_mail.init(addr2 /*from*/,
                                             addr1 /*to*/,
                                             sizeof(ObTwoPhaseCommitMsgType),
                                             ObTwoPhaseCommitMsgType::OB_MSG_TX_COMMIT_RESP));

  // ========== Two Phase Commit prepare Phase ==========
  // ctx1 start to commit
  ctx1.commit(participants);
  // ctx2 handle prepare request
  EXPECT_EQ(OB_SUCCESS, ctx2.handle_all());
  // ctx2 handle prepare request
  EXPECT_EQ(OB_SUCCESS, ctx2.apply());
  // ctx1 handle prepare response
  EXPECT_EQ(OB_SUCCESS, ctx1.handle_all());
  // ctx1 apply prepare log
  EXPECT_EQ(OB_SUCCESS, ctx1.apply());

  // ========== Two Phase Commit pre commit Phase ======
  // ctx2 handle commit request
  EXPECT_EQ(OB_SUCCESS, ctx2.handle_all());
  // ctx1 handle commit response
  EXPECT_EQ(OB_SUCCESS, ctx1.handle_all());
  // [DUP_MSG]: ctx1 handle duplicated commit response
  EXPECT_EQ(OB_SUCCESS, dup_and_handle_msg(dup_commit_mail, &ctx1));
  // [DUP_MSG]: ctx1 handle duplicated commit response
  EXPECT_EQ(OB_SUCCESS, dup_and_handle_msg(dup_commit_mail, &ctx1));

  // ========== Two Phase Commit commit Phase ==========
  // ctx2 handle commit request
  EXPECT_EQ(OB_SUCCESS, ctx2.handle_all());
  // ctx2 apply commit log
  EXPECT_EQ(OB_SUCCESS, ctx2.apply());
  // ctx1 handle commit response
  EXPECT_EQ(OB_SUCCESS, ctx1.handle_all());
  // [DUP_MSG]: ctx1 handle duplicated commit response
  EXPECT_EQ(OB_SUCCESS, dup_and_handle_msg(dup_commit_mail, &ctx1));
  // ctx1 apply commit log
  EXPECT_EQ(OB_SUCCESS, ctx1.apply());
  // [DUP_MSG]: ctx1 handle duplicated commit response
  EXPECT_EQ(OB_SUCCESS, dup_and_handle_msg(dup_commit_mail, &ctx1));

  // ========== Two Phase Commit clear Phase ==========
  // [DUP_MSG]: ctx1 handle duplicated commit response
  EXPECT_EQ(OB_SUCCESS, dup_and_handle_msg(dup_commit_mail, &ctx1));
  // ctx1 apply clear log
  EXPECT_EQ(OB_SUCCESS, ctx1.apply());
  // [DUP_MSG]: ctx1 handle duplicated commit response
  EXPECT_EQ(OB_SUCCESS, dup_and_handle_msg(dup_commit_mail, &ctx1));
  // ctx2 handle clear request
  EXPECT_EQ(OB_SUCCESS, ctx2.handle_all());
  // [DUP_MSG]: ctx1 handle duplicated commit response
  EXPECT_EQ(OB_SUCCESS, dup_and_handle_msg(dup_commit_mail, &ctx1));
  // ctx2 apply clear log
  EXPECT_EQ(OB_SUCCESS, ctx2.apply());
  // [DUP_MSG]: ctx1 handle duplicated commit response
  EXPECT_EQ(OB_SUCCESS, dup_and_handle_msg(dup_commit_mail, &ctx1));

  // ========== Check Test Valid ==========
  EXPECT_EQ(true, ctx1.check_status_valid(true/*should commit*/));
  EXPECT_EQ(true, ctx2.check_status_valid(true/*should commit*/));
}

TEST_F(TestDupMsgMockOb2pcCtx, test_dup_2pc_commit_response2)
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

  // ========== Prepare Duplicated Messages ==========
  // Mock duplicated 2pc commit mail
  ObMail<ObTwoPhaseCommitMsgType> dup_commit_mail;
  EXPECT_EQ(OB_SUCCESS, dup_commit_mail.init(addr2 /*from*/,
                                             addr1 /*to*/,
                                             sizeof(ObTwoPhaseCommitMsgType),
                                             ObTwoPhaseCommitMsgType::OB_MSG_TX_COMMIT_RESP));
  // Mock duplicated 2pc abort mail
  ObMail<ObTwoPhaseCommitMsgType> dup_abort_mail;
  EXPECT_EQ(OB_SUCCESS, dup_abort_mail.init(addr2 /*from*/,
                                            addr1 /*to*/,
                                            sizeof(ObTwoPhaseCommitMsgType),
                                            ObTwoPhaseCommitMsgType::OB_MSG_TX_ABORT_RESP));

  // ========== Two Phase Commit prepare Phase ==========
  // ctx1 start to commit
  ctx1.commit(participants);
  // ctx2 handle prepare request
  EXPECT_EQ(OB_SUCCESS, ctx2.handle_all());
  // ctx2 handle prepare request
  EXPECT_EQ(OB_SUCCESS, ctx2.apply());
  // ctx1 handle prepare response
  EXPECT_EQ(OB_SUCCESS, ctx1.handle_all());
  // ctx1 apply prepare log
  EXPECT_EQ(OB_SUCCESS, ctx1.apply());

  // ========== Two Phase Commit pre commit Phase ======
  // ctx2 handle commit request
  EXPECT_EQ(OB_SUCCESS, ctx2.handle_all());
  // ctx1 handle commit response
  EXPECT_EQ(OB_SUCCESS, ctx1.handle_all());
  // [DUP_MSG]: ctx1 handle duplicated commit response
  EXPECT_EQ(OB_SUCCESS, dup_and_handle_msg(dup_commit_mail, &ctx1));
  // [DUP_MSG]: ctx1 handle duplicated commit response
  EXPECT_EQ(OB_SUCCESS, dup_and_handle_msg(dup_commit_mail, &ctx1));

  // ========== Two Phase Commit commit Phase ==========
  // ctx2 handle commit request
  EXPECT_EQ(OB_SUCCESS, ctx2.handle_all());
  // ctx2 apply commit log
  EXPECT_EQ(OB_SUCCESS, ctx2.apply());
  // ctx1 handle commit response
  EXPECT_EQ(OB_SUCCESS, ctx1.handle_all());
  // [DUP_MSG]: ctx1 handle duplicated commit response
  EXPECT_EQ(OB_SUCCESS, dup_and_handle_msg(dup_commit_mail, &ctx1));
  // ctx1 apply commit log
  EXPECT_EQ(OB_SUCCESS, ctx1.apply());
  // // [DUP_MSG]: ctx1 handle duplicated commit response
  // EXPECT_EQ(OB_SUCCESS, dup_and_handle_msg(dup_commit_mail, &ctx1));

  TRANS_LOG(INFO, "qc debug");
  // [DUP_MSG]: ctx1 handle duplicated abort response
  EXPECT_EQ(OB_SUCCESS, dup_and_handle_msg(dup_abort_mail, &ctx1));

  // ========== Two Phase Commit clear Phase ==========
  // [DUP_MSG]: ctx1 handle duplicated commit response
  EXPECT_EQ(OB_SUCCESS, dup_and_handle_msg(dup_commit_mail, &ctx1));
  // ctx1 apply clear log
  EXPECT_EQ(OB_SUCCESS, ctx1.apply());
  // [DUP_MSG]: ctx1 handle duplicated commit response
  EXPECT_EQ(OB_SUCCESS, dup_and_handle_msg(dup_commit_mail, &ctx1));
  // ctx2 handle clear request
  EXPECT_EQ(OB_SUCCESS, ctx2.handle_all());
  // [DUP_MSG]: ctx1 handle duplicated commit response
  EXPECT_EQ(OB_SUCCESS, dup_and_handle_msg(dup_commit_mail, &ctx1));
  // ctx2 apply clear log
  EXPECT_EQ(OB_SUCCESS, ctx2.apply());
  // [DUP_MSG]: ctx1 handle duplicated commit response
  EXPECT_EQ(OB_SUCCESS, dup_and_handle_msg(dup_commit_mail, &ctx1));

  // ========== Check Test Valid ==========
  EXPECT_EQ(true, ctx1.check_status_valid(true/*should commit*/));
  EXPECT_EQ(true, ctx2.check_status_valid(true/*should commit*/));
}

TEST_F(TestDupMsgMockOb2pcCtx, test_dup_2pc_clear_request)
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

  // ========== Prepare Duplicated Messages ==========
  // Mock duplicated 2pc commit mail
  ObMail<ObTwoPhaseCommitMsgType> dup_clear_mail;
  EXPECT_EQ(OB_SUCCESS, dup_clear_mail.init(addr1 /*from*/,
                                            addr2 /*to*/,
                                            sizeof(ObTwoPhaseCommitMsgType),
                                            ObTwoPhaseCommitMsgType::OB_MSG_TX_CLEAR_REQ));

  // ========== Two Phase Commit prepare Phase ==========
  // ctx1 start to commit
  ctx1.commit(participants);
  // ctx2 handle prepare request
  EXPECT_EQ(OB_SUCCESS, ctx2.handle_all());
  // ctx2 handle prepare request
  EXPECT_EQ(OB_SUCCESS, ctx2.apply());
  // ctx1 handle prepare response
  EXPECT_EQ(OB_SUCCESS, ctx1.handle_all());
  // ctx1 apply prepare log
  EXPECT_EQ(OB_SUCCESS, ctx1.apply());

  // ========== Two Phase Commit pre commit Phase ======
  // ctx2 handle commit request
  EXPECT_EQ(OB_SUCCESS, ctx2.handle_all());
  // ctx1 handle commit response
  EXPECT_EQ(OB_SUCCESS, ctx1.handle_all());

  // ========== Two Phase Commit commit Phase ==========
  // ctx2 handle commit request
  EXPECT_EQ(OB_SUCCESS, ctx2.handle_all());
  // ctx2 apply commit log
  EXPECT_EQ(OB_SUCCESS, ctx2.apply());
  // ctx1 handle commit response
  EXPECT_EQ(OB_SUCCESS, ctx1.handle_all());
  // ctx1 apply commit log
  EXPECT_EQ(OB_SUCCESS, ctx1.apply());

  // ========== Two Phase Commit clear Phase ==========
  // ctx1 apply clear log
  EXPECT_EQ(OB_SUCCESS, ctx1.apply());
  // ctx2 handle clear request
  EXPECT_EQ(OB_SUCCESS, ctx2.handle_all());
  // [DUP_MSG]: ctx2 handle duplicated clear request
  EXPECT_EQ(OB_SUCCESS, dup_and_handle_msg(dup_clear_mail, &ctx2));
  // ctx2 apply clear log
  EXPECT_EQ(OB_SUCCESS, ctx2.apply());
  // [DUP_MSG]: ctx2 handle duplicated clear request
  EXPECT_EQ(OB_SUCCESS, dup_and_handle_msg(dup_clear_mail, &ctx2));

  // ========== Check Test Valid ==========
  EXPECT_EQ(true, ctx1.check_status_valid(true/*should commit*/));
  EXPECT_EQ(true, ctx2.check_status_valid(true/*should commit*/));
}

}
}

int main(int argc, char **argv)
{
  system("rm -rf test_dup_msg_tx_commit.log*");
  OB_LOGGER.set_file_name("test_dup_msg_tx_commit.log");
  OB_LOGGER.set_log_level("INFO");
  STORAGE_LOG(INFO, "begin unittest: test dup msg mock ob tx ctx");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
