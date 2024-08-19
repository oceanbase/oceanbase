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
#include "lib/random/ob_random.h"
#include "lib/function/ob_function.h"

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

TEST_F(TestDupMsgMockOb2pcCtx, test_random_dup_tree_commit)
{
  // root coordinator
  MockOb2pcCtx root_ctx;
  root_ctx.init(&mailbox_mgr_);
  int64_t root_addr = root_ctx.get_addr();

  // normal participants
  MockOb2pcCtx ctx1;
  MockOb2pcCtx ctx2;
  ctx1.init(&mailbox_mgr_);
  ctx2.init(&mailbox_mgr_);
  int64_t addr1 = ctx1.get_addr();
  int64_t addr2 = ctx2.get_addr();

  // incremental participants
  const int64_t MAX_INC_CTX_COUNT = 100;
  MockOb2pcCtx inc_ctx[MAX_INC_CTX_COUNT];
  int64_t inc_addr[MAX_INC_CTX_COUNT];
  int64_t inc_index = 0;
  for (int i = 0; i < MAX_INC_CTX_COUNT; i++) {
    inc_ctx[i].init(&mailbox_mgr_);
    inc_addr[i] = inc_ctx[i].get_addr();
  }

  ObFunction<MockOb2pcCtx *(const int64_t participant)> get_ctx_op =
    [&](const int64_t participant) -> MockOb2pcCtx * {
      if (participant == root_addr) {
        return &root_ctx;
      } else if (participant == addr1) {
        return &ctx1;
      } else if (participant == addr2) {
        return &ctx2;
      } else {
        for (int64_t i = 0; i < inc_index; i++) {
          if (participant == inc_addr[i]) {
            return &inc_ctx[i];
          }
        }
      }

      return NULL;
    };

  ObFunction<bool()> transfer_op =
    [&]() -> bool {
      if (inc_index >= MAX_INC_CTX_COUNT) {
        TRANS_LOG(INFO, "[TREE_COMMIT_GEAR] Transfer Op Limited", K(inc_index));
        return false;
      }

      int64_t src_ctx_idx = ObRandom::rand(0, inc_index + 2);
      int64_t dst_ctx_idx = inc_index;
      MockOb2pcCtx *ctx;

      if (src_ctx_idx == 0) {
        ctx = &root_ctx;
      } else if (src_ctx_idx == 1) {
        ctx = &ctx1;
      } else if (src_ctx_idx == 2) {
        ctx = &ctx2;
      } else {
        ctx = &inc_ctx[src_ctx_idx - 3];
      }

      if (ctx->is_2pc_logging()) {
        TRANS_LOG(INFO, "[TREE_COMMIT_GEAR] Transfer Op Failed", K(src_ctx_idx), K(dst_ctx_idx), KPC(ctx));
        return false;
      }

      inc_ctx[dst_ctx_idx].downstream_state_ = ctx->downstream_state_;
      inc_ctx[dst_ctx_idx].upstream_state_ = ctx->upstream_state_;
      inc_ctx[dst_ctx_idx].tx_state_ = ctx->tx_state_;
      inc_ctx[dst_ctx_idx].coordinator_ = ctx->get_addr();
      ctx->add_intermediate_participants(inc_addr[dst_ctx_idx]);
      inc_index++;

      TRANS_LOG(INFO, "[TREE_COMMIT_GEAR] Transfer Op Succeed", K(src_ctx_idx), K(dst_ctx_idx), KPC(ctx), K(inc_ctx[dst_ctx_idx]));

      return true;
    };

  ObFunction<bool()> dup_msg_op =
    [&]() -> bool {
      bool ret = mailbox_mgr_.random_dup_and_send();
      TRANS_LOG(INFO, "[TREE_COMMIT_GEAR] Dup Op Succeed", K(ret));
      return ret;
    };

  ObFunction<bool()> drive_op =
    [&]() -> bool {
      int64_t ctx_idx = ObRandom::rand(0, inc_index + 2);
      MockOb2pcCtx *ctx;

      if (ctx_idx == 0) {
        ctx = &root_ctx;
      } else if (ctx_idx == 1) {
        ctx = &ctx1;
      } else if (ctx_idx == 2) {
        ctx = &ctx2;
      } else {
        ctx = &inc_ctx[ctx_idx - 3];
      }

      bool is_mail_empty = ctx->mailbox_.empty();
      bool is_log_empty = ctx->log_queue_.empty();
      int64_t job = 0;
      if (is_mail_empty && is_log_empty) {
        TRANS_LOG(INFO, "[TREE_COMMIT_GEAR] Drive Op Failed", KPC(ctx));
        return false;
      } else if (!is_mail_empty && !is_log_empty) {
        job = ObRandom::rand(0, 1);
      } else if (!is_mail_empty) {
        job = 0;
      } else if (!is_log_empty) {
        job = 1;
      }

      if (job == 0) {
        // has mail to drive
        ctx->handle();
        TRANS_LOG(INFO, "[TREE_COMMIT_GEAR] Drive Mail Op Succeed", KPC(ctx));
        return true;
      } else if (job == 1) {
        // has log to drive
        ctx->apply();
        TRANS_LOG(INFO, "[TREE_COMMIT_GEAR] Drive Log Op Succeed", KPC(ctx));
        return true;
      }

      ob_abort();
      return false;
    };

  ObFunction<bool()> is_all_released =
    [&]() -> bool {
      if (root_ctx.downstream_state_ != ObTxState::CLEAR) {
        return false;
      } else if (ctx1.downstream_state_ != ObTxState::CLEAR) {
        return false;
      } else if (ctx2.downstream_state_ != ObTxState::CLEAR) {
        return false;
      }

      for (int i = 0; i < inc_index; i++) {
        if (inc_ctx[i].downstream_state_ != ObTxState::CLEAR) {
          return false;
        }
      }

      return true;
    };


  ObFunction<bool()> print_tree =
    [&]() -> bool {
      root_ctx.print_downstream();
      ctx1.print_downstream();
      ctx2.print_downstream();

      for (int i = 0; i < inc_index; i++) {
        inc_ctx[i].print_downstream();
      }

      return true;
    };

  ObFunction<bool()> validate_tree =
    [&]() -> bool {
      for (int i = 0; i < inc_index; i++) {
        for (int j = 0; j < inc_ctx[i].participants_.size(); j++) {
          int64_t participant = inc_ctx[i].participants_[j];
          EXPECT_EQ(inc_ctx[i].addr_, get_ctx_op(participant)->get_coordinator());
        }
      }

      return true;
    };

  MockObParticipants participants;
  participants.push_back(addr1);
  participants.push_back(addr2);
  participants.push_back(root_addr);

  // ctx start to commit
  root_ctx.commit(participants);

  while (!is_all_released()) {
    bool enable = false;
    int64_t job = ObRandom::rand(0, 4);
    TRANS_LOG(INFO, "[TREE_COMMIT_GEAR] Decide Job", K(job));
    if (0 == job || 1 == job) {
      transfer_op();
    } else if (2 == job || 3 == job) {
      drive_op();
    } else {
      dup_msg_op();
    }
  }

  // ========== Check Test Valid ==========
  EXPECT_EQ(true, root_ctx.check_status_valid(true/*should commit*/));
  EXPECT_EQ(true, ctx1.check_status_valid(true/*should commit*/));
  EXPECT_EQ(true, ctx2.check_status_valid(true/*should commit*/));

  EXPECT_EQ(Ob2PCRole::ROOT, root_ctx.get_2pc_role());

  print_tree();

  validate_tree();
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
