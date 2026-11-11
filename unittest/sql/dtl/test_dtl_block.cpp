/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#include <gtest/gtest.h>
#include <thread>
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/rc/ob_tenant_base.h"
#include "sql/dtl/ob_dtl.h"
#include "sql/dtl/ob_dtl_channel_agent.h"
#include "sql/dtl/ob_dtl_channel_loop.h"
#include "sql/dtl/ob_dtl_rpc_channel.h"
#include "sql/dtl/ob_dtl_rpc_processor.h"
#include "sql/dtl/ob_dtl_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::sql::dtl;

// Assert correct behavior: consistent blocking state and bounded EOF waiting.
// Stage the two public state updates made by block_channel() separately to
// deterministically schedule the RPC between them. Production code is unchanged.
class TestDtlBatchUnblockRace : public ::testing::Test
{
protected:
  static void SetUpTestCase()
  {
    ASSERT_EQ(OB_SUCCESS,
        oceanbase::omt::ObTenantConfigMgr::get_instance().add_tenant_config(OB_SYS_TENANT_ID));
    ASSERT_EQ(OB_SUCCESS, DTL.init());
  }
  void SetUp() override
  {
    saved_tenant_ = ObTenantEnv::get_tenant();
    ObTenantEnv::set_tenant(&tenant_ctx_);
    ASSERT_EQ(OB_SUCCESS, ObTenantDfc::mtl_new(tenant_dfc_));
    ASSERT_NE(nullptr, tenant_dfc_);
    tenant_ctx_.set(tenant_dfc_);
    // Refresh the thread-local module snapshot after installing tenant DFC.
    ObTenantEnv::set_tenant(&tenant_ctx_);
    ASSERT_EQ(OB_SUCCESS, ObTenantDfc::mtl_init(tenant_dfc_));
    peer_.set_ip_addr("127.0.0.2", 2882);
    dfc_.set_dtl_channel_watcher(&loop_);
    ASSERT_EQ(OB_SUCCESS, dfc_.init(OB_SYS_TENANT_ID, 1));
    dfc_.set_transmit();
    ASSERT_TRUE(dfc_.is_transmit());
    dfc_.set_timeout_ts(ObTimeUtility::current_time() + 30000000);
    loop_.set_tenant_id(OB_SYS_TENANT_ID);
    loop_.register_processor(unblock_proc_);
    ASSERT_EQ(OB_SUCCESS, DTL.create_rpc_channel(OB_SYS_TENANT_ID, 42000, peer_, channel_, &dfc_));
    ASSERT_NE(nullptr, channel_);
    rpc_ = static_cast<ObDtlRpcChannel *>(channel_);
  }
  void TearDown() override
  {
    if (channel_ != nullptr) {
      EXPECT_EQ(OB_SUCCESS, DTL.get_dfc_server().unregister_dfc_channel(dfc_, channel_));
      EXPECT_EQ(OB_SUCCESS, loop_.unregister_channel(*channel_));
      EXPECT_EQ(OB_SUCCESS, DTL.destroy_channel(channel_->get_id()));
      channel_ = nullptr;
      rpc_ = nullptr;
    }
    ObTenantDfc::mtl_destroy(tenant_dfc_);
    tenant_ctx_.set(tenant_dfc_);
    ObTenantEnv::set_tenant(saved_tenant_);
  }
  void batch_unblock_on_rpc_thread()
  {
    ASSERT_NE(nullptr, channel_);
    ObDtlBatchMsg msg;
    msg.contained_msg_type_ = ObDtlMsgType::UNBLOCKING_DATA_FLOW;
    ASSERT_EQ(OB_SUCCESS, msg.ch_ids_.push_back(channel_->get_id()));
    int rpc_ret = OB_ERR_UNEXPECTED;
    std::thread rpc_thread([&] {
      // MTL is thread-local; feedup() needs the same tenant on the RPC thread.
      ObTenantEnv::set_tenant(&tenant_ctx_);
      rpc_ret = ObDtlBatchSendMessageP::process_batch_control_msg(msg);
      ObTenantEnv::set_tenant(nullptr);
    });
    rpc_thread.join();
    ASSERT_EQ(OB_SUCCESS, rpc_ret);
  }
  void reproduce_interleaving()
  {
    ASSERT_NE(nullptr, channel_);
    ASSERT_FALSE(dfc_.is_block(channel_));
    ASSERT_FALSE(channel_->is_blocked());
    dfc_.set_block(channel_->get_dfc_idx()); // first write in block_channel()
    ASSERT_TRUE(dfc_.is_block(channel_));
    ASSERT_NO_FATAL_FAILURE(batch_unblock_on_rpc_thread());
    // Do not require the buggy RPC-side state clear. A queued UNBLOCK may leave
    // DFC blocked until the sender processes it after completing both writes.
    channel_->set_blocked(); // second write in block_channel(), sender resumes
  }
  ObTenantBase tenant_ctx_{OB_SYS_TENANT_ID};
  ObTenantBase *saved_tenant_{nullptr};
  ObTenantDfc *tenant_dfc_{nullptr};
  ObAddr peer_;
  ObDtlFlowControl dfc_;
  ObDtlUnblockingMsgP unblock_proc_{dfc_};
  ObDtlChannelLoop loop_;
  ObDtlChannel *channel_{nullptr};
  ObDtlRpcChannel *rpc_{nullptr};
};

TEST_F(TestDtlBatchUnblockRace, serialized_unblock_preserves_consistency)
{
  ASSERT_NE(nullptr, channel_);
  ASSERT_NE(nullptr, rpc_);
  ASSERT_EQ(OB_SUCCESS, dfc_.block_channel(channel_));
  ASSERT_NO_FATAL_FAILURE(batch_unblock_on_rpc_thread());
  // The RPC queues UNBLOCK; the transmit worker consumes it and clears both bits.
  ASSERT_EQ(OB_SUCCESS, rpc_->wait_unblocking_if_blocked());
  EXPECT_FALSE(dfc_.is_block(channel_));
  EXPECT_FALSE(channel_->is_blocked());
  EXPECT_EQ(0, dfc_.get_blocked_cnt());
}

TEST_F(TestDtlBatchUnblockRace, interleaved_batch_unblock_preserves_consistency)
{
  ASSERT_NE(nullptr, channel_);
  ASSERT_NE(nullptr, rpc_);
  ASSERT_NO_FATAL_FAILURE(reproduce_interleaving());
  EXPECT_EQ(dfc_.is_block(channel_), channel_->is_blocked())
      << "DFC and channel blocking state must agree after the interleaving";
  ASSERT_EQ(OB_SUCCESS, rpc_->wait_unblocking_if_blocked());
  EXPECT_FALSE(dfc_.is_block(channel_));
  EXPECT_FALSE(channel_->is_blocked())
      << "Successful UNBLOCK processing must clear the channel blocked bit";
  EXPECT_EQ(0, dfc_.get_blocked_cnt());
}

class BoundedEofSender : public ObTransmitEofAsynSender
{
public:
  BoundedEofSender(ObIArray<ObDtlChannel *> &channels,
                  const ObIArray<ObDtlServerChannelGroup *> *groups,
                  oceanbase::sql::ObEvalCtx &eval_ctx)
    : ObTransmitEofAsynSender(channels, nullptr, true,
        ObTimeUtility::current_time() - 1000000, eval_ctx,
        ObDtlMsgType::PX_NEW_ROW, groups) {}
  int check_need_send(ObDtlChannel *, bool &need) override
  {
    // Skip EOF serialization; the stuck branch cannot reach buffer consumption.
    need = true;
    return OB_SUCCESS;
  }
  int fill_batch_msg(ObDtlPeerCtlBatch &slot) override
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(rounds_ == 10000)) {
      watchdog_fired_ = true;
      ret = OB_CANCELED; // Safety guard only: hitting it is a test failure.
    } else {
      ++rounds_;
      ret = ObTransmitEofAsynSender::fill_batch_msg(slot);
      if (ret == OB_SUCCESS && batch_msg_.ch_ids_.empty()
          && slot.next_send_ch_idx_ == 0 && !slot.posted_flags_.empty()
          && !slot.posted_flags_.at(0)) {
        ++stalled_rounds_;
      }
    }
    return ret;
  }
  int rounds_{0};
  int stalled_rounds_{0};
  bool watchdog_fired_{false};
};

TEST_F(TestDtlBatchUnblockRace, eof_returns_timeout_after_deadline)
{
  ASSERT_NE(nullptr, channel_);
  ASSERT_NE(nullptr, rpc_);
  // Inject the stale state left by the old race independently of the RPC fix.
  // No queued UNBLOCK should repair it before the EOF timeout guard is exercised.
  ASSERT_EQ(OB_SUCCESS, dfc_.block_channel(channel_));
  dfc_.unblock(channel_->get_dfc_idx());
  ASSERT_FALSE(dfc_.is_block(channel_));
  ASSERT_TRUE(channel_->is_blocked());
  dfc_.set_timeout_ts(ObTimeUtility::current_time() - 1000000);
  ObArenaAllocator allocator;
  oceanbase::sql::ObExecContext exec_ctx(allocator);
  ASSERT_EQ(OB_SUCCESS, exec_ctx.create_physical_plan_ctx());
  auto *plan_ctx = exec_ctx.get_physical_plan_ctx();
  ASSERT_NE(nullptr, plan_ctx);
  plan_ctx->set_timeout_timestamp(dfc_.get_timeout_ts());
  oceanbase::sql::ObEvalCtx eval_ctx(exec_ctx);
  ObSEArray<ObDtlChannel *, 1> channels;
  ObSEArray<ObDtlServerChannelGroup *, 1> groups;
  ObDtlServerChannelGroup group;
  ASSERT_EQ(OB_SUCCESS, channels.push_back(channel_));
  ASSERT_EQ(OB_SUCCESS, group.basic_channels_.push_back(rpc_));
  ASSERT_EQ(OB_SUCCESS, groups.push_back(&group));
  BoundedEofSender sender(channels, &groups, eval_ctx);
  const int64_t start = ObTimeUtility::current_time();
  const int ret = sender.async_send_batch();
  printf("EOF result: ret=%d expected_timeout=%d watchdog_fired=%d rounds=%d stalled=%d elapsed_us=%ld dfc=%d channel=%d\n",
      ret, OB_TIMEOUT, sender.watchdog_fired_, sender.rounds_, sender.stalled_rounds_, ObTimeUtility::current_time() - start,
      dfc_.is_block(channel_), channel_->is_blocked());
  EXPECT_EQ(OB_TIMEOUT, ret)
      << "An expired query must return its timeout, not the test watchdog error";
  EXPECT_FALSE(sender.watchdog_fired_)
      << "EOF loop made no progress for " << sender.stalled_rounds_ << " rounds";
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_dtl_batch_unblock_race.log", true, true);
  OB_LOGGER.set_log_level("WARN");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
