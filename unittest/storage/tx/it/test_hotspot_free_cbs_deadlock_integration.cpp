/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You may use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

// Integration test for free_hotspot_cbs_ deadlock fix
//
// This test verifies that the abort path succeeds without deadlock after fix.
// Based on test_hotspot_tx_leader_switch.cpp pattern.

#include <gtest/gtest.h>
#define protected public
#define private public
#define USING_LOG_PREFIX TRANS
#include "storage/tx/ob_tx_hotspot_define.h"
#include "test_tx_dsl.h"
#include "tx_node.h"
#include "lib/utility/ob_tracepoint.h"

namespace oceanbase
{
using namespace ::testing;
using namespace transaction;
using namespace share;

static ObSharedMemAllocMgr MTL_MEM_ALLOC_MGR;

namespace share
{
int ObTenantTxDataAllocator::init(const char *label)
{
  int ret = OB_SUCCESS;
  ObMemAttr mem_attr;
  throttle_tool_ = &(MTL_MEM_ALLOC_MGR.share_resource_throttle_tool());
  if (OB_FAIL(slice_allocator_.init(
                 storage::TX_DATA_SLICE_SIZE, OB_MALLOC_NORMAL_BLOCK_SIZE, block_alloc_, mem_attr))) {
    SHARE_LOG(WARN, "init slice allocator failed", KR(ret));
  } else {
    slice_allocator_.set_nway(ObTenantTxDataAllocator::ALLOC_TX_DATA_MAX_CONCURRENCY);
    is_inited_ = true;
  }
  return ret;
}

int ObMemstoreAllocator::init()
{
  throttle_tool_ = &MTL_MEM_ALLOC_MGR.share_resource_throttle_tool();
  return arena_.init();
}

int ObMemstoreAllocator::AllocHandle::init()
{
  int ret = OB_SUCCESS;
  ObSharedMemAllocMgr *mtl_alloc_mgr = &MTL_MEM_ALLOC_MGR;
  ObMemstoreAllocator &host = mtl_alloc_mgr->memstore_allocator();
  (void)host.init_handle(*this);
  return ret;
}
}  // namespace share

namespace concurrent_control
{
int check_sequence_set_violation(const concurrent_control::ObWriteFlag,
                                 const int64_t,
                                 const ObTransID,
                                 const blocksstable::ObDmlFlag,
                                 const int64_t,
                                 const ObTransID,
                                 const blocksstable::ObDmlFlag,
                                 const int64_t)
{
  return OB_SUCCESS;
}
} // namespace concurrent_control

class ObTestHotspotFreeCbsDeadlockIntegration : public ::testing::Test
{
public:
  virtual void SetUp() override
  {
    oceanbase::ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
    ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(1001);
    const uint64_t tv = ObTimeUtility::current_time();
    ObCurTraceId::set(&tv);
    GCONF._ob_trans_rpc_timeout = 500;
    ObClockGenerator::init();
    MTL_MEM_ALLOC_MGR.init();
  }

  virtual void TearDown() override
  {
    // Reset errsim
    oceanbase::common::EventItem item;
    item.error_code_ = 0;
    item.occur_ = 0;
    item.trigger_freq_ = 0;
    item.cond_ = 0;
    (void)oceanbase::common::EventTable::set_event(
        "ERRSIM_SWITCH_TO_FOLLOWER_GRACEFULLY_RETRY_TIMEOUT_US", item);

    ObClockGenerator::destroy();
    ObMallocAllocator::get_instance()->recycle_tenant_allocator(1001);
  }

  void set_errsim_retry_timeout(const int64_t timeout_us)
  {
    oceanbase::common::EventItem item;
    item.error_code_ = timeout_us;
    item.occur_ = 1;
    item.trigger_freq_ = 1;
    item.cond_ = 0;
    (void)oceanbase::common::EventTable::set_event(
        "ERRSIM_SWITCH_TO_FOLLOWER_GRACEFULLY_RETRY_TIMEOUT_US", item);
  }

  MsgBus bus_;
};

// ============================================================================
// Integration Test: Verify abort path succeeds without deadlock
// ============================================================================
// This test is based on test_hotspot_tx_leader_switch pattern.
// It verifies that after leader switch (forcedly), the abort path completes
// without deadlock. The fix ensures free CBs are released to idle list.
// ============================================================================
TEST_F(ObTestHotspotFreeCbsDeadlockIntegration,
       hotspot_abort_no_deadlock_after_force_switch)
{
  ObTxNode::reset_localtion_adapter();
  START_ONE_TX_NODE(n1);

  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000 * 1000 * 1000;
  ObTxDescGuard primary_guard = n1->get_tx_guard();
  ObTxDesc &primary_tx = primary_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary_tx, tx_param));

  ObTxReadSnapshot snapshot;
  ASSERT_EQ(OB_SUCCESS,
            n1->get_read_snapshot(primary_tx, tx_param.isolation_, n1->ts_after_ms(100), snapshot));

  const int SECONDARY_TX_COUNT = 2;
  ObTransID secondary_tx_ids[SECONDARY_TX_COUNT];
  ObTxDescGuard sec_guard0 = n1->get_tx_guard();
  ObTxDescGuard sec_guard1 = n1->get_tx_guard();
  ObTxDesc *sec_txs[SECONDARY_TX_COUNT] = {&sec_guard0.get_tx_desc(), &sec_guard1.get_tx_desc()};
  for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
    ASSERT_EQ(OB_SUCCESS, n1->start_tx(*sec_txs[i], tx_param));
    secondary_tx_ids[i] = sec_txs[i]->tx_id_;
    ASSERT_EQ(OB_SUCCESS, n1->write(*sec_txs[i], snapshot, 200 + i, 300 + i));
  }
  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, snapshot, 1000, 2000));
  const ObTransID primary_tx_id = primary_tx.tx_id_;

  ObTxPart participant;
  participant.id_ = n1->ls_id_;
  participant.addr_ = n1->addr_;
  participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;
  ObAggregatedTxIDArray aggre_members;
  for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
    ASSERT_EQ(OB_SUCCESS, aggre_members.push_back(secondary_tx_ids[i]));
  }
  ASSERT_EQ(OB_SUCCESS,
            n1->txs_.sync_hotspot_legality_validation(participant, primary_tx.tx_id_, aggre_members));
  n1->wait_all_msg_consumed();

  ObLSTxCtxMgr *ls_tx_ctx_mgr = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr));

  // Flush once before role switch to ensure part of redo has been submitted.
  ObTransID fail_tx_id;
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->traverse_tx_to_submit_redo_log(fail_tx_id, UINT32_MAX));
  n1->wait_all_msg_consumed();

  // Shrink retry timeout to make graceful revoke timeout deterministic and fast.
  set_errsim_retry_timeout(20 * 1000);  // 20ms

  ASSERT_EQ(OB_LS_NEED_REVOKE, ls_tx_ctx_mgr->switch_to_follower_gracefully());
  // Wait for resume to finish before forced revoke.
  {
    const int64_t start_ts = ObTimeUtility::fast_current_time();
    const int64_t timeout_us = 3 * 1000 * 1000;
    while (!ls_tx_ctx_mgr->is_master()
           && ObTimeUtility::fast_current_time() - start_ts < timeout_us) {
      n1->wait_all_redolog_applied();
      n1->wait_all_msg_consumed();
      usleep(10 * 1000);
    }
    ASSERT_TRUE(ls_tx_ctx_mgr->is_master());
  }

  // Force revoke should succeed.
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->switch_to_follower_forcedly());
  ASSERT_FALSE(ls_tx_ctx_mgr->is_master());

  // Trigger one more takeover path
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->switch_to_leader());
  for (int i = 0; i < 300; i++) {
    n1->wait_all_msg_consumed();
    n1->wait_all_redolog_applied();
    usleep(1000);
  }

  // Verify primary tx ctx enters waiting_delay_abort state
  bool primary_waiting_delay_abort = false;
  for (int i = 0; i < 300; i++) {
    ObPartTransCtx *primary_part_ctx = nullptr;
    int get_ret = ls_tx_ctx_mgr->get_tx_ctx_directly_from_hash_map(primary_tx_id, primary_part_ctx);
    if (OB_TRANS_CTX_NOT_EXIST == get_ret) {
      // do nothing
    } else {
      ASSERT_EQ(OB_SUCCESS, get_ret);
      ASSERT_TRUE(primary_part_ctx != nullptr);
      primary_waiting_delay_abort =
          primary_part_ctx->sub_state_.is_force_abort() && !primary_part_ctx->is_exiting_;
      ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->revert_tx_ctx(primary_part_ctx));
      if (primary_waiting_delay_abort) {
        break;
      }
    }
    n1->wait_all_msg_consumed();
    n1->wait_all_redolog_applied();
    usleep(1000);
  }
  ASSERT_TRUE(primary_waiting_delay_abort);

  // Commit triggers the real rollback - this should complete without deadlock
  TRANS_LOG(INFO, "[integration test] before commit_tx", K(primary_tx_id));
  const int commit_ret = n1->commit_tx(primary_tx, n1->ts_after_ms(500));
  TRANS_LOG(INFO, "[integration test] after commit_tx", K(primary_tx_id), K(commit_ret));
  ASSERT_EQ(OB_TRANS_ROLLBACKED, commit_ret);
  n1->wait_all_msg_consumed();

  // Verify primary ctx exits after commit
  bool primary_ctx_exiting_after_commit = false;
  for (int i = 0; i < 300; i++) {
    ObPartTransCtx *primary_part_ctx = nullptr;
    int get_ret = ls_tx_ctx_mgr->get_tx_ctx_directly_from_hash_map(primary_tx_id, primary_part_ctx);
    if (OB_TRANS_CTX_NOT_EXIST == get_ret) {
      primary_ctx_exiting_after_commit = true;
      break;
    } else {
      ASSERT_EQ(OB_SUCCESS, get_ret);
      ASSERT_TRUE(primary_part_ctx != nullptr);
      primary_ctx_exiting_after_commit = primary_part_ctx->is_exiting_;
      ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->revert_tx_ctx(primary_part_ctx));
      if (primary_ctx_exiting_after_commit) {
        break;
      }
    }
    n1->wait_all_msg_consumed();
    n1->wait_all_redolog_applied();
    usleep(1000);
  }
  ASSERT_TRUE(primary_ctx_exiting_after_commit);

  ASSERT_EQ(OB_SUCCESS, primary_guard.release());
  ASSERT_EQ(OB_SUCCESS, sec_guard0.release());
  ASSERT_EQ(OB_SUCCESS, sec_guard1.release());
  n1->wait_all_msg_consumed();

  // Verify all tx ctxs eventually exit (no deadlock)
  auto wait_tx_ctx_exiting = [&](const ObTransID &tx_id) {
    ObPartTransCtx *part_ctx = nullptr;
    int get_ret = ls_tx_ctx_mgr->get_tx_ctx_directly_from_hash_map(tx_id, part_ctx);
    if (OB_TRANS_CTX_NOT_EXIST == get_ret) {
      return;
    }
    ASSERT_EQ(OB_SUCCESS, get_ret);
    ASSERT_TRUE(part_ctx != nullptr);
    int i = 0;
    while (!part_ctx->is_exiting_ && i++ < 10000) {
      n1->wait_all_msg_consumed();
      n1->wait_all_redolog_applied();
      usleep(1000);
    }
    ASSERT_TRUE(part_ctx->is_exiting_) << "tx ctx not exiting, tx_id=" << tx_id;
    ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->revert_tx_ctx(part_ctx));
  };
  wait_tx_ctx_exiting(primary_tx_id);
  for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
    wait_tx_ctx_exiting(secondary_tx_ids[i]);
  }

  int wait_ret = OB_SUCCESS;
  for (int i = 0; i < 60; i++) {
    wait_ret = n1->wait_all_tx_ctx_is_destoryed();
    if (OB_SUCCESS == wait_ret) {
      break;
    }
    n1->wait_all_msg_consumed();
    n1->wait_all_redolog_applied();
    usleep(100 * 1000);
  }
  ASSERT_EQ(OB_SUCCESS, wait_ret);
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr));

  TRANS_LOG(INFO, "[integration test] hotspot_abort_no_deadlock_after_force_switch PASSED");
}

} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_hotspot_free_cbs_deadlock_integration.log*");
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_hotspot_free_cbs_deadlock_integration.log", true, false,
                       "test_hotspot_free_cbs_deadlock_integration.log",
                       "test_hotspot_free_cbs_deadlock_integration.log",
                       "test_hotspot_free_cbs_deadlock_integration.log");
  logger.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}