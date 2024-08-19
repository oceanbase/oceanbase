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
#define UNITTEST

#include "storage/tx/ob_mock_tx_ctx.h"

namespace oceanbase
{
using namespace ::testing;
using namespace transaction;
using namespace share;

namespace unittest
{
class TestMockObTxCtx : public ::testing::Test
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
  int check_mail(ObMailBox<ObTxMsg> &mailbox,
                 int64_t from,
                 int64_t to,
                 int64_t type)
  {
    int ret = OB_SUCCESS;
    ObMail<ObTxMsg> mail;
    EXPECT_EQ(OB_SUCCESS, mailbox.fetch_mail(mail));

    EXPECT_EQ(from, mail.from_);
    EXPECT_EQ(to, mail.to_);
    EXPECT_EQ(type, mail.mail_->get_msg_type());

    switch (type) {
    case TX_COMMIT_RESP: {
      const ObTxCommitRespMsg *tx_commit_resp = dynamic_cast<const ObTxCommitRespMsg*>(mail.mail_);
      EXPECT_EQ(OB_SUCCESS, tx_commit_resp->ret_);
      TRANS_LOG(INFO, "check tx commit resp", K(mail), KPC(tx_commit_resp));
      break;
    }
    default: {
      TRANS_LOG(ERROR, "invalid msg type", K(mail));
      ret = OB_TRANS_INVALID_STATE;
      break;
    }
    }

    return ret;
  }

  int build_scheduler_mailbox()
  {
    int ret = OB_SUCCESS;

    if (OB_FAIL(mailbox_mgr_.register_mailbox(scheduler_addr_,
                                              scheduler_mailbox_,
                                              NULL))) {
      TRANS_LOG(ERROR, "mock ctx register mailbox failed");
    }

    return ret;
  }

public:
  ObMailBoxMgr<ObTxMsg> mailbox_mgr_;
  int64_t scheduler_addr_ = 0;
  ObMailBox<ObTxMsg> scheduler_mailbox_;
};

TEST_F(TestMockObTxCtx, test_simple_tx_ctx1)
{
  int64_t ls_id_gen = ObLSID::MIN_USER_LS_ID;
  MockObTxCtx ctx1;
  MockObTxCtx ctx2;
  ObTransID trans_id1(1);
  ObLSID ls_id0(++ls_id_gen);
  ObLSID ls_id1(++ls_id_gen);
  ObLSID ls_id2(++ls_id_gen);

  ObTxData data1;
  ObTxData data2;
  ctx1.change_to_leader();
  ctx2.change_to_leader();
  EXPECT_EQ(OB_SUCCESS, ctx1.init(ls_id1, trans_id1, nullptr, &data1, &mailbox_mgr_));
  EXPECT_EQ(OB_SUCCESS, ctx2.init(ls_id2, trans_id1, nullptr, &data2, &mailbox_mgr_));

  EXPECT_EQ(OB_SUCCESS, build_scheduler_mailbox());

  ctx1.addr_memo_[ls_id2] = ctx2.addr_;
  ctx1.ls_memo_[ctx2.addr_] = ls_id2;
  ctx1.set_trans_type_(TransType::DIST_TRANS);
  ctx1.upstream_state_ = ObTxState::INIT;
  // set self to root
  ctx1.exec_info_.upstream_ = ls_id1;
  ctx1.set_downstream_state(ObTxState::REDO_COMPLETE);
  ctx1.exec_info_.participants_.push_back(ls_id1);
  ctx1.exec_info_.participants_.push_back(ls_id2);
  ctx1.exec_info_.commit_parts_.push_back(ObTxExecPart(ls_id1, ctx1.epoch_, 0));
  ctx1.exec_info_.commit_parts_.push_back(ObTxExecPart(ls_id2, ctx2.epoch_, 0));
  ctx1.scheduler_addr_ = scheduler_mailbox_.addr_;

  // ctx2.addr_memo_[ls_id1] = ctx1.addr_;
  // ctx2.ls_memo_[ctx1.addr_] = ls_id1;
  // ctx2.set_trans_type_(TransType::DIST_TRANS);
  // ctx2.upstream_state_ = ObTxState::PREPARE;
  // ctx2.exec_info_.upstream_ = ls_id1;

  // EXPECT_NE(share::SCN::max_scn(), ctx1.mt_ctx_.trans_version_);

  // ========== Two Phase Commit prepare Phase ==========
  // ctx1 start to commit
  EXPECT_EQ(OB_SUCCESS, ctx1.two_phase_commit());

    // ctx2 handle prepare request
  EXPECT_EQ(OB_SUCCESS, ctx2.handle());
  // ctx2 apply prepare log
  EXPECT_EQ(OB_SUCCESS, ctx2.apply());
  // ctx1 handle prepare response
  EXPECT_EQ(OB_SUCCESS, ctx1.handle());
  // ctx1 apply prepare log
  EXPECT_EQ(OB_SUCCESS, ctx1.apply());

  // ========== Two Phase Commit pre commit Phase ======
  // ctx2 handle pre commit request
  EXPECT_EQ(OB_SUCCESS, ctx2.handle());
  // ctx1 handle pre commit response
  EXPECT_EQ(OB_SUCCESS, ctx1.handle());

  EXPECT_EQ(OB_SUCCESS, check_mail(scheduler_mailbox_,
                                   ctx1.get_mailbox_addr() /*from*/,
                                   scheduler_addr_,
                                   TX_COMMIT_RESP));

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
  EXPECT_EQ(2, ctx1.coord_prepare_info_arr_.count());
  int64_t id1 = ctx1.coord_prepare_info_arr_[0].id_.id_;
  int64_t id2 = ctx1.coord_prepare_info_arr_[1].id_.id_;
  EXPECT_EQ(true, id1 == 1 || id1 == 2);
  EXPECT_EQ(true, id2 == 1 || id2 == 2);
  TRANS_LOG(INFO, "qianchen debug", K(id1), K(id2), K(ctx1));
}

TEST_F(TestMockObTxCtx, test_transfer_tx_ctx)
{
  int64_t ls_id_gen = ObLSID::MIN_USER_LS_ID;
  MockObTxCtx ctx1;
  MockObTxCtx ctx2;
  MockObTxCtx ctx3;
  ObTransID trans_id1(1);
  ObLSID ls_id0(++ls_id_gen);
  ObLSID ls_id1(++ls_id_gen);
  ObLSID ls_id2(++ls_id_gen);
  ObLSID ls_id3(++ls_id_gen);

  ObTxData data1;
  ObTxData data2;
  ObTxData data3;
  ctx1.change_to_leader();
  ctx2.change_to_leader();
  ctx3.change_to_leader();
  EXPECT_EQ(OB_SUCCESS, ctx1.init(ls_id1, trans_id1, nullptr, &data1, &mailbox_mgr_));
  EXPECT_EQ(OB_SUCCESS, ctx2.init(ls_id2, trans_id1, nullptr, &data2, &mailbox_mgr_));
  EXPECT_EQ(OB_SUCCESS, ctx3.init(ls_id3, trans_id1, nullptr, &data3, &mailbox_mgr_));

  EXPECT_EQ(OB_SUCCESS, build_scheduler_mailbox());

  ctx1.addr_memo_[ls_id2] = ctx2.addr_;
  ctx1.ls_memo_[ctx2.addr_] = ls_id2;
  ctx2.addr_memo_[ls_id3] = ctx3.addr_;
  ctx2.ls_memo_[ctx3.addr_] = ls_id3;
  ctx1.set_trans_type_(TransType::DIST_TRANS);
  ctx1.upstream_state_ = ObTxState::INIT;
  // set self to root
  ctx1.exec_info_.upstream_ = ls_id1;
  ctx1.set_downstream_state(ObTxState::REDO_COMPLETE);
  ctx1.exec_info_.participants_.push_back(ls_id1);
  ctx1.exec_info_.participants_.push_back(ls_id2);
  ctx1.scheduler_addr_ = scheduler_mailbox_.addr_;
  ctx1.exec_info_.commit_parts_.push_back(ObTxExecPart(ls_id1, ctx1.epoch_, 0));
  ctx1.exec_info_.commit_parts_.push_back(ObTxExecPart(ls_id2, ctx2.epoch_, 0));

  // ctx2.addr_memo_[ls_id1] = ctx1.addr_;
  // ctx2.ls_memo_[ctx1.addr_] = ls_id1;
  // ctx2.set_trans_type_(TransType::DIST_TRANS);
  // ctx2.upstream_state_ = ObTxState::PREPARE;
  // ctx2.exec_info_.upstream_ = ls_id1;

  // EXPECT_NE(share::SCN::max_scn(), ctx1.mt_ctx_.trans_version_);

  EXPECT_EQ(OB_SUCCESS, ctx2.add_intermediate_participants(ls_id3, 1000));

  // ========== Two Phase Commit prepare Phase ==========
  // ctx1 start to commit
  EXPECT_EQ(OB_SUCCESS, ctx1.two_phase_commit());

  // ctx2 handle prepare request
  EXPECT_EQ(OB_SUCCESS, ctx2.handle());
  // ctx2 apply prepare log
  EXPECT_EQ(OB_SUCCESS, ctx2.apply());
  // ctx3 handle prepare request
  EXPECT_EQ(OB_SUCCESS, ctx3.handle());
  // ctx3 apply prepare log
  EXPECT_EQ(OB_SUCCESS, ctx3.apply());
  // ctx2 handle prepare response
  EXPECT_EQ(OB_SUCCESS, ctx2.handle());
  // ctx1 handle prepare response
  EXPECT_EQ(OB_SUCCESS, ctx1.handle());
  // ctx1 apply prepare log
  EXPECT_EQ(OB_SUCCESS, ctx1.apply());

  // ========== Two Phase Commit pre commit Phase ======
  // ctx2 handle pre commit request
  EXPECT_EQ(OB_SUCCESS, ctx2.handle());
  // ctx3 handle pre commit request
  EXPECT_EQ(OB_SUCCESS, ctx3.handle());
  // ctx2 handle pre commit response
  EXPECT_EQ(OB_SUCCESS, ctx2.handle());
  // ctx1 handle pre commit response
  EXPECT_EQ(OB_SUCCESS, ctx1.handle());

  EXPECT_EQ(OB_SUCCESS, check_mail(scheduler_mailbox_,
                                   ctx1.get_mailbox_addr() /*from*/,
                                   scheduler_addr_,
                                   TX_COMMIT_RESP));

  // ========== Two Phase Commit commit Phase ==========
  // ctx2 handle commit request
  EXPECT_EQ(OB_SUCCESS, ctx2.handle());
  // ctx2 apply commit log
  EXPECT_EQ(OB_SUCCESS, ctx2.apply());
  // ctx3 handle commit request
  EXPECT_EQ(OB_SUCCESS, ctx3.handle());
  // ctx3 apply commit log
  EXPECT_EQ(OB_SUCCESS, ctx3.apply());
  // ctx2 handle commit response
  EXPECT_EQ(OB_SUCCESS, ctx2.handle());
  // ctx1 handle commit response
  EXPECT_EQ(OB_SUCCESS, ctx1.handle());
  // ctx1 apply commit log
  EXPECT_EQ(OB_SUCCESS, ctx1.apply());

  // ========== Two Phase Commit clear Phase ==========
  // ctx2 handle clear request
  EXPECT_EQ(OB_SUCCESS, ctx2.handle());
  // ctx2 apply clear log
  EXPECT_EQ(OB_SUCCESS, ctx2.apply());
  // ctx3 handle commit request
  EXPECT_EQ(OB_SUCCESS, ctx3.handle());
  // ctx3 apply commit log
  EXPECT_EQ(OB_SUCCESS, ctx3.apply());
  // ctx1 apply clear log
  EXPECT_EQ(OB_SUCCESS, ctx1.apply());

  // ========== Check Test Valid ==========
  EXPECT_EQ(true, ctx1.check_status_valid(true/*should commit*/));
  EXPECT_EQ(true, ctx2.check_status_valid(true/*should commit*/));
  EXPECT_EQ(true, ctx3.check_status_valid(true/*should commit*/));
  EXPECT_EQ(2, ctx1.coord_prepare_info_arr_.count());
  int64_t id1 = ctx1.coord_prepare_info_arr_[0].id_.id_;
  int64_t id2 = ctx1.coord_prepare_info_arr_[1].id_.id_;
  EXPECT_EQ(true, id1 == 1 || id1 == 2 || id1 == 3);
  EXPECT_EQ(true, id2 == 1 || id2 == 2 || id2 == 3);
  TRANS_LOG(INFO, "qianchen debug", K(id1), K(id2), K(ctx1));
}
}

namespace transaction
{
  void ObTransCtx::after_unlock(CtxLockArg &)
  {
  }

  void ObTransCtx::set_exiting_()
  {
    int tmp_ret = OB_SUCCESS;

    if (!is_exiting_) {
      is_exiting_ = true;
      print_trace_log_if_necessary_();

      const int64_t ctx_ref = get_ref();
      if (NULL == ls_tx_ctx_mgr_) {
        TRANS_LOG_RET(ERROR, tmp_ret, "ls_tx_ctx_mgr_ is null, unexpected error", KP(ls_tx_ctx_mgr_), "context", *this);
      } else {
        ls_tx_ctx_mgr_->dec_active_tx_count();
        ls_tx_ctx_mgr_->del_tx_ctx(this);
        TRANS_LOG(DEBUG, "transaction exiting", "context", *this, K(lbt()));
        REC_TRANS_TRACE_EXT2(tlog_, exiting, OB_ID(ctx_ref), ctx_ref, OB_ID(arg1), session_id_);
      }
    }
  }

}

}

int main(int argc, char **argv)
{
  system("rm -rf test_simple_tx_ctx.log*");
  OB_LOGGER.set_file_name("test_simple_tx_ctx.log");
  OB_LOGGER.set_log_level("INFO");
  STORAGE_LOG(INFO, "begin unittest: test simple mock ob tx ctx");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
