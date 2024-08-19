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

namespace oceanbase
{
using namespace ::testing;
using namespace transaction;

namespace unittest
{
class TestCycleCtx : public ::testing::Test
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

TEST_F(TestCycleCtx, test_basic_cycle_commit)
{
  // normal participants
  MockOb2pcCtx ctx1;
  MockOb2pcCtx ctx2;

  // incremental participants
  MockOb2pcCtx ctx3;

  ctx1.init(&mailbox_mgr_);
  ctx2.init(&mailbox_mgr_);
  ctx3.init(&mailbox_mgr_);

  auto addr1 = ctx1.get_addr();
  auto addr2 = ctx2.get_addr();
  auto addr3 = ctx3.get_addr();

  MockObParticipants participants;
  participants.push_back(addr1);
  participants.push_back(addr2);

  ctx2.add_intermediate_participants(addr1);
  ctx1.add_intermediate_participants(addr2);

  // ========== Two Phase Commit prepare Phase ==========
  // ctx1 start to commit
  ctx1.commit(participants);
  // ctx2 handle prepare request
  EXPECT_EQ(OB_SUCCESS, ctx2.handle());
  // ctx2 apply prepare log
  EXPECT_EQ(OB_SUCCESS, ctx2.apply());
  // ctx1 apply prepare log
  EXPECT_EQ(OB_SUCCESS, ctx1.apply());
  // ctx1 handle prepare request
  EXPECT_EQ(OB_SUCCESS, ctx1.handle());
  // ctx2 handle prepare response
  EXPECT_EQ(OB_SUCCESS, ctx2.handle());
  // ctx1 handle prepare response
  EXPECT_EQ(OB_SUCCESS, ctx1.handle());

  // ========== Two Phase Commit pre commit Phase ======
  // ctx2 handle pre-commit request
  EXPECT_EQ(OB_SUCCESS, ctx2.handle());
  // ctx1 handle pre-commit request
  EXPECT_EQ(OB_SUCCESS, ctx1.handle());
  // ctx2 handle pre-commit response
  EXPECT_EQ(OB_SUCCESS, ctx2.handle());
  // ctx1 handle pre-commit response
  EXPECT_EQ(OB_SUCCESS, ctx1.handle());

  // ========== Two Phase Commit commit Phase ==========
  // ctx2 handle commit request
  EXPECT_EQ(OB_SUCCESS, ctx2.handle());
  // ctx1 apply commit log
  EXPECT_EQ(OB_SUCCESS, ctx1.apply());
  // ctx1 handle commit request
  EXPECT_EQ(OB_SUCCESS, ctx1.handle());
  // ctx2 handle commit response
  EXPECT_EQ(OB_SUCCESS, ctx2.handle());
  // ctx2 apply commit log
  EXPECT_EQ(OB_SUCCESS, ctx2.apply());
  // ctx1 handle commit response
  EXPECT_EQ(OB_SUCCESS, ctx1.handle());

  // ========== Two Phase Commit clear Phase ==========
  // ctx2 handle clear request
  EXPECT_EQ(OB_SUCCESS, ctx2.handle());
  // ctx3 handle clear request
  EXPECT_EQ(OB_SUCCESS, ctx1.handle());
  // ctx2 apply clear log
  EXPECT_EQ(OB_SUCCESS, ctx2.apply());
  // ctx1 apply clear log
  EXPECT_EQ(OB_SUCCESS, ctx1.apply());

  // ========== Check Test Valid ==========
  EXPECT_EQ(true, ctx1.check_status_valid(true/*should commit*/));
  EXPECT_EQ(true, ctx2.check_status_valid(true/*should commit*/));

  EXPECT_EQ(Ob2PCRole::ROOT, ctx1.get_2pc_role());
  EXPECT_EQ(Ob2PCRole::INTERNAL, ctx2.get_2pc_role());
}

TEST_F(TestCycleCtx, test_random_cycle_commit)
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
  const int64_t MAX_OLD_CTX_COUNT = 100;
  MockOb2pcCtx inc_ctx[MAX_INC_CTX_COUNT];
  int64_t inc_addr[MAX_INC_CTX_COUNT];
  int64_t inc_index = 0;
  int64_t old_index = 0;
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

  ObFunction<bool()> transfer_to_new_op =
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
      ctx->add_intermediate_participants(inc_addr[dst_ctx_idx]);
      inc_index++;

      TRANS_LOG(INFO, "[TREE_COMMIT_GEAR] Transfer Op Succeed", K(src_ctx_idx), K(dst_ctx_idx), KPC(ctx), K(inc_ctx[dst_ctx_idx]));

      return true;
    };

  ObFunction<bool()> transfer_to_old_op =
    [&]() -> bool {
      if (old_index >= MAX_OLD_CTX_COUNT) {
        TRANS_LOG(INFO, "[TREE_COMMIT_GEAR] Transfer Op Limited", K(old_index));
        return false;
      }

      int64_t src_ctx_idx = 0;
      int64_t dst_ctx_idx = 0;
      while (src_ctx_idx == dst_ctx_idx) {
        src_ctx_idx = ObRandom::rand(0, inc_index + 2);
        dst_ctx_idx = ObRandom::rand(0, inc_index + 2);
      }
      EXPECT_NE(src_ctx_idx, dst_ctx_idx);
      MockOb2pcCtx *src_ctx = NULL;
      MockOb2pcCtx *dst_ctx = NULL;

      if (src_ctx_idx == 0) {
        src_ctx = &root_ctx;
      } else if (src_ctx_idx == 1) {
        src_ctx = &ctx1;
      } else if (src_ctx_idx == 2) {
        src_ctx = &ctx2;
      } else {
        src_ctx = &inc_ctx[src_ctx_idx - 3];
      }

      if (dst_ctx_idx == 0) {
        dst_ctx = &root_ctx;
      } else if (dst_ctx_idx == 1) {
        dst_ctx = &ctx1;
      } else if (dst_ctx_idx == 2) {
        dst_ctx = &ctx2;
      } else {
        dst_ctx = &inc_ctx[dst_ctx_idx - 3];
      }

      if (src_ctx->is_2pc_logging()) {
        TRANS_LOG(INFO, "[TREE_COMMIT_GEAR] Transfer Op Failed", K(src_ctx_idx), K(dst_ctx_idx), KPC(src_ctx));
        return false;
      }

      int64_t dst_addr = 0;
      if (dst_ctx_idx == 0) {
        dst_addr = root_addr;
      } else if (dst_ctx_idx == 1) {
        dst_addr = addr1;
      } else if (dst_ctx_idx == 2) {
        dst_addr = addr2;
      } else {
        dst_addr = inc_addr[dst_ctx_idx - 3];
      }

      EXPECT_EQ(dst_addr, dst_ctx->get_addr());

      src_ctx->add_intermediate_participants(dst_addr);
      old_index++;

      TRANS_LOG(INFO, "[TREE_COMMIT_GEAR] Transfer Op Succeed", K(src_ctx_idx), K(dst_ctx_idx), KPC(src_ctx), KPC(dst_ctx), K(dst_addr));

      return true;
    };

  ObFunction<MockOb2pcCtx *()> get_advancing_ctx =
    [&]() -> MockOb2pcCtx * {
      if (!root_ctx.mailbox_.empty() || !root_ctx.log_queue_.empty()) {
        return &root_ctx;
      } else if (!ctx1.mailbox_.empty() || !ctx1.log_queue_.empty()) {
        return &ctx1;
      } else if (!ctx2.mailbox_.empty() || !ctx2.log_queue_.empty()) {
        return &ctx2;
      }

      for (int i = 0; i < inc_index; i++) {
        if (!inc_ctx[i].mailbox_.empty() || !inc_ctx[i].log_queue_.empty()) {
          return &inc_ctx[i];
        }
      }

      return NULL;
    };

  ObFunction<MockOb2pcCtx *()> get_to_advance_ctx =
    [&]() -> MockOb2pcCtx * {
      if (root_ctx.need_to_advance()) {
        return &root_ctx;
      } else if (ctx1.need_to_advance()) {
        return &ctx1;
      } else if (ctx2.need_to_advance()) {
        return &ctx2;
      }

      for (int i = 0; i < inc_index; i++) {
        if (inc_ctx[i].need_to_advance()) {
          return &inc_ctx[i];
        }
      }

      return NULL;
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
        MockOb2pcCtx *advancing_ctx = get_advancing_ctx();
        if (NULL == advancing_ctx) {
          ctx = get_to_advance_ctx();
          bool is_ok = ctx->need_to_advance();
          ctx->handle_timeout();
          TRANS_LOG(INFO, "[TREE_COMMIT_GEAR] Drive Handle Timeout Op Succeed", KPC(ctx), K(is_ok));
          return true;
        } else {
          ctx = advancing_ctx;
          TRANS_LOG(INFO, "[TREE_COMMIT_GEAR] Drive Op Asing advancing Ctx", KPC(ctx));
        }
      }

      is_mail_empty = ctx->mailbox_.empty();
      is_log_empty = ctx->log_queue_.empty();

      if (is_mail_empty && is_log_empty) {
        ob_abort();
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
        int64_t upstream = inc_ctx[i].coordinator_;
        MockOb2pcCtx *upstream_ctx = NULL;
        if (upstream == root_addr) {
          upstream_ctx = &root_ctx;
        } else if (upstream == addr1) {
          upstream_ctx = &ctx1;
        } else if (upstream == addr2) {
          upstream_ctx = &ctx2;
        } else {
          for (int i = 0; i < inc_index; i++) {
            if (inc_addr[i] == upstream) {
              upstream_ctx = &inc_ctx[i];
              break;
            }
          }
        }

        bool found = false;
        for (int j = 0; j < upstream_ctx->participants_.size(); j++) {
          if (upstream_ctx->participants_[j] == inc_ctx[i].get_addr()) {
            found = true;
            break;
          }
        }
        EXPECT_EQ(true, found);
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
    int64_t job = ObRandom::rand(0, 2);
    TRANS_LOG(INFO, "[TREE_COMMIT_GEAR] Decide Job", K(job));
    if (0 == job) {
      enable = transfer_to_new_op();
    } else if (1 == job) {
      enable = transfer_to_old_op();
    } else {
      enable = drive_op();
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

} // namespace transaction
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_simple_tx_commit.log*");
  OB_LOGGER.set_file_name("test_simple_tx_commit.log");
  OB_LOGGER.set_log_level("INFO");
  STORAGE_LOG(INFO, "begin unittest: test simple mock ob tx ctx");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
