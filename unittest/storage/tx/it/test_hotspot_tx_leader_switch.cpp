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

class ObTestHotspotTxLeaderSwitch : public ::testing::Test
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
    // reset errsim to avoid contaminating other tests
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

class ReplayLogEntryFunctor
{
public:
  ReplayLogEntryFunctor(ObTxNode *n) : n_(n) {}

  int operator()(const void *buffer,
                 const int64_t nbytes,
                 const palf::LSN &lsn,
                 const int64_t ts_ns)
  {
    return n_->replay(buffer, nbytes, lsn, ts_ns);
  }

private:
  ObTxNode *n_;
};

TEST_F(ObTestHotspotTxLeaderSwitch,
       hotspot_leader_switch_force_revoke_primary_delay_abort_needs_commit_to_finish)
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

  bool has_partial_redo = false;
  auto check_has_redo = [&](const ObTransID &tx_id) {
    ObPartTransCtx *part_ctx = nullptr;
    ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->get_tx_ctx(tx_id, false /*for_replay*/, part_ctx));
    ASSERT_TRUE(part_ctx != nullptr);
    if (part_ctx->exec_info_.redo_lsns_.count() > 0) {
      has_partial_redo = true;
    }
    ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->revert_tx_ctx(part_ctx));
  };
  check_has_redo(primary_tx.tx_id_);
  for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
    check_has_redo(secondary_tx_ids[i]);
  }
  ASSERT_TRUE(has_partial_redo);

  // Shrink retry timeout to make graceful revoke timeout deterministic and fast.
  set_errsim_retry_timeout(20 * 1000);  // 20ms

  ASSERT_EQ(OB_LS_NEED_REVOKE, ls_tx_ctx_mgr->switch_to_follower_gracefully());
  // switch_to_follower_gracefully() now kicks off an async resume flow before returning
  // OB_LS_NEED_REVOKE. Wait for resume to finish before forced revoke.
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
  // Trigger one more takeover path to drain role-change related async callbacks.
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->switch_to_leader());
  for (int i = 0; i < 300; i++) {
    n1->wait_all_msg_consumed();
    n1->wait_all_redolog_applied();
    usleep(1000);
  }

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

  // After takeover, primary tx ctx enters delayed rollback. Commit triggers the real rollback
  // and drives the ctx into exiting.
  TRANS_LOG(INFO, "[hotspot leader switch test] before commit_tx",
            K(primary_tx_id), K(primary_tx), "phase", "before_commit_tx");
  const int commit_ret = n1->commit_tx(primary_tx, n1->ts_after_ms(500));
  TRANS_LOG(INFO, "[hotspot leader switch test] after commit_tx",
            K(primary_tx_id), K(commit_ret), K(primary_tx), "phase", "after_commit_tx");
  ASSERT_EQ(OB_TRANS_ROLLBACKED, commit_ret);
  n1->wait_all_msg_consumed();
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
}

TEST_F(ObTestHotspotTxLeaderSwitch,
       hotspot_force_revoke_new_leader_abort_log_replay_cleans_old_leader_aggregated_ctx)
{
  ObTxNode::reset_localtion_adapter();
  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  auto n2 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_);
  DEFER(delete(n1));
  DEFER(delete(n2));
  ASSERT_EQ(OB_SUCCESS, n1->start());
  n2->set_as_follower_replica(*n1);
  ASSERT_EQ(OB_SUCCESS, n2->start());

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
    ASSERT_EQ(OB_SUCCESS, n1->write(*sec_txs[i], snapshot, 400 + i, 500 + i));
  }
  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, snapshot, 3000, 4000));
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

  ObLSTxCtxMgr *ls_tx_ctx_mgr1 = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr1));

  ObTransID fail_tx_id;
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr1->traverse_tx_to_submit_redo_log(fail_tx_id, UINT32_MAX));
  n1->wait_all_msg_consumed();
  n1->wait_all_redolog_applied();

  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr1->switch_to_follower_forcedly());
  ASSERT_FALSE(ls_tx_ctx_mgr1->is_master());
  ASSERT_GT(ls_tx_ctx_mgr1->get_tx_ctx_count(), 0);

  ReplayLogEntryFunctor replay_new_leader(n2);
  ASSERT_EQ(OB_SUCCESS, n2->fake_tx_log_adapter_->replay_all(replay_new_leader));

  ObLSTxCtxMgr *ls_tx_ctx_mgr2 = nullptr;
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n2->ls_id_, ls_tx_ctx_mgr2));
  ObTxNode::get_location_adapter_().update_localtion(n2->ls_id_, n2->addr_);
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr2->switch_to_leader());
  n2->wait_all_redolog_applied();
  ASSERT_TRUE(ls_tx_ctx_mgr2->is_master());

  bool primary_waiting_delay_abort = false;
  for (int i = 0; i < 300; i++) {
    ObPartTransCtx *primary_part_ctx = nullptr;
    int get_ret = ls_tx_ctx_mgr2->get_tx_ctx_directly_from_hash_map(primary_tx_id, primary_part_ctx);
    if (OB_TRANS_CTX_NOT_EXIST != get_ret) {
      ASSERT_EQ(OB_SUCCESS, get_ret);
      ASSERT_TRUE(primary_part_ctx != nullptr);
      primary_waiting_delay_abort =
          primary_part_ctx->sub_state_.is_force_abort() && !primary_part_ctx->is_exiting_;
      ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr2->revert_tx_ctx(primary_part_ctx));
      if (primary_waiting_delay_abort) {
        break;
      }
    }
    n2->wait_all_msg_consumed();
    n2->wait_all_redolog_applied();
    usleep(1000);
  }
  ASSERT_TRUE(primary_waiting_delay_abort);

  ObPartTransCtx *new_leader_primary_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr2->get_tx_ctx(primary_tx_id, false /*for_replay*/, new_leader_primary_ctx));
  ASSERT_TRUE(new_leader_primary_ctx != nullptr);
  {
    CtxLockGuard guard(new_leader_primary_ctx->lock_);
    ASSERT_EQ(OB_SUCCESS, new_leader_primary_ctx->abort_(OB_TRANS_ROLLBACKED));
  }
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr2->revert_tx_ctx(new_leader_primary_ctx));
  n2->wait_all_msg_consumed();
  n2->wait_all_redolog_applied();

  ReplayLogEntryFunctor replay_old_leader(n1);
  ASSERT_EQ(OB_SUCCESS, n1->fake_tx_log_adapter_->replay_all(replay_old_leader));
  n1->wait_all_msg_consumed();
  n1->wait_all_redolog_applied();

  ASSERT_EQ(OB_SUCCESS, n1->wait_all_tx_ctx_is_destoryed());
  ASSERT_EQ(OB_SUCCESS, n2->wait_all_tx_ctx_is_destoryed());

  ASSERT_EQ(OB_SUCCESS, n1->abort_tx(primary_tx, OB_TRANS_CTX_NOT_EXIST));
  ASSERT_EQ(OB_SUCCESS, n1->abort_tx(*sec_txs[0], OB_TRANS_CTX_NOT_EXIST));
  ASSERT_EQ(OB_SUCCESS, n1->abort_tx(*sec_txs[1], OB_TRANS_CTX_NOT_EXIST));
  ASSERT_EQ(OB_SUCCESS, primary_guard.release());
  ASSERT_EQ(OB_SUCCESS, sec_guard0.release());
  ASSERT_EQ(OB_SUCCESS, sec_guard1.release());

  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr2));
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr1));
}

// DISABLED_: gtest 默认不执行；恢复时去掉前缀并把下方 #if 0 改为 #if 1。
TEST_F(ObTestHotspotTxLeaderSwitch,
       DISABLED_hotspot_graceful_switch_commit_on_new_leader_releases_ctx_on_both_replicas)
{
  // 本 case 暂时禁用。
  //
  // 目前未 commit 成功的子事务无法切主：子事务在热点聚合场景下无法自行写日志推进状态，
  // 因此不会出现「已开始聚合之后，在新主上把聚合事务提交成功」这一在单测里可稳定复现的路径。
  // 若后续支持子事务在聚合过程中具备与线上一致的日志能力，再打开下方 #if 0 中的实现。

#if 0
  ObTxNode::reset_localtion_adapter();
  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  auto n2 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_);
  DEFER(delete(n1));
  DEFER(delete(n2));
  ASSERT_EQ(OB_SUCCESS, n1->start());
  n2->set_as_follower_replica(*n1);
  ASSERT_EQ(OB_SUCCESS, n2->start());

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
    ASSERT_EQ(OB_SUCCESS, n1->write(*sec_txs[i], snapshot, 600 + i, 700 + i));
  }
  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, snapshot, 5000, 6000));

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

  ObLSTxCtxMgr *ls_tx_ctx_mgr1 = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr1));

  ObTransID fail_tx_id;
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr1->traverse_tx_to_submit_redo_log(fail_tx_id, UINT32_MAX));
  n1->wait_all_msg_consumed();
  n1->wait_all_redolog_applied();

  TRANS_LOG(INFO, "[hotspot leader switch test] before wait hotspot redo migrated", K(primary_tx.tx_id_), K(secondary_tx_ids), K(SECONDARY_TX_COUNT));
  bool hotspot_redo_migrated = false;
  for (int i = 0; i < 300; i++) {
    ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr1->traverse_tx_to_submit_redo_log(fail_tx_id, UINT32_MAX));
    n1->wait_all_msg_consumed();
    n1->wait_all_redolog_applied();

    bool primary_ready = false;
    ObPartTransCtx *primary_part_ctx = nullptr;
    ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr1->get_tx_ctx(primary_tx.tx_id_, false /*for_replay*/, primary_part_ctx));
    ASSERT_TRUE(primary_part_ctx != nullptr);
    primary_ready = primary_part_ctx->redo_flush_status_ == TxRedoFlushStatus::NORMAL_FLUSHED
                    && primary_part_ctx->hotspot_redo_cache_.all_redo_flushed()
                    && primary_part_ctx->hotspot_redo_cache_.all_redo_synced();

    // UT has no trans-service thread to run resp_task after push_response_task. After real
    // aggregation (SECONDARY_MIGRATE_SYNCED), bump secondaries to SECONDARY_MIGRATE_SUCCEEDED under
    // ctx lock to mimic scheduler response already applied (see response_scheduler commit path).
    if (primary_ready) {
      for (int j = 0; j < SECONDARY_TX_COUNT; j++) {
        ObPartTransCtx *secondary_part_ctx = nullptr;
        const int sec_ret =
            ls_tx_ctx_mgr1->get_tx_ctx_directly_from_hash_map(secondary_tx_ids[j], secondary_part_ctx);
        ASSERT_NE(OB_TRANS_CTX_NOT_EXIST, sec_ret);
        ASSERT_EQ(OB_SUCCESS, sec_ret);
        ASSERT_TRUE(secondary_part_ctx != nullptr);
        if (secondary_part_ctx->redo_flush_status_ == TxRedoFlushStatus::SECONDARY_MIGRATE_SYNCED) {
          CtxLockGuard guard(secondary_part_ctx->lock_);
          ASSERT_EQ(TxRedoFlushStatus::SECONDARY_MIGRATE_SYNCED, secondary_part_ctx->redo_flush_status_);
          secondary_part_ctx->redo_flush_status_ = TxRedoFlushStatus::SECONDARY_MIGRATE_SUCCEEDED;
        }
        ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr1->revert_tx_ctx(secondary_part_ctx));
      }
    }

    bool secondary_ready_for_graceful_switch = true;
    for (int j = 0; j < SECONDARY_TX_COUNT && secondary_ready_for_graceful_switch; j++) {
      ObPartTransCtx *secondary_part_ctx = nullptr;
      const int sec_ret = ls_tx_ctx_mgr1->get_tx_ctx_directly_from_hash_map(secondary_tx_ids[j], secondary_part_ctx);
      if (OB_TRANS_CTX_NOT_EXIST == sec_ret) {
        secondary_ready_for_graceful_switch = false;
      } else {
        ASSERT_EQ(OB_SUCCESS, sec_ret);
        ASSERT_TRUE(secondary_part_ctx != nullptr);
        secondary_ready_for_graceful_switch =
            secondary_ready_for_graceful_switch
            && secondary_part_ctx->redo_flush_status_ == TxRedoFlushStatus::SECONDARY_MIGRATE_SUCCEEDED;
        ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr1->revert_tx_ctx(secondary_part_ctx));
      }
    }

    const bool resp_idle = !primary_part_ctx->hotspot_redo_cache_.resp_task_.is_in_queue();
    hotspot_redo_migrated = primary_ready && secondary_ready_for_graceful_switch && resp_idle;
    ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr1->revert_tx_ctx(primary_part_ctx));
    if (hotspot_redo_migrated) {
      break;
    }
    usleep(1000);
  }
  ASSERT_TRUE(hotspot_redo_migrated);

  // Graceful switch happens only after primary/secondary hotspot redo migration is finished.
  // Do not commit/abort the transaction before role switch.
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr1->switch_to_follower_gracefully());
  ASSERT_FALSE(ls_tx_ctx_mgr1->is_master());
  ASSERT_GT(ls_tx_ctx_mgr1->get_tx_ctx_count(), 0);

  ReplayLogEntryFunctor replay_new_leader(n2);
  ASSERT_EQ(OB_SUCCESS, n2->fake_tx_log_adapter_->replay_all(replay_new_leader));

  ObLSTxCtxMgr *ls_tx_ctx_mgr2 = nullptr;
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n2->ls_id_, ls_tx_ctx_mgr2));
  ObTxNode::get_location_adapter_().update_localtion(n2->ls_id_, n2->addr_);
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr2->switch_to_leader());
  ASSERT_TRUE(ls_tx_ctx_mgr2->is_master());
  n2->wait_all_redolog_applied();

  ASSERT_EQ(OB_SUCCESS, n1->commit_tx(primary_tx, n1->ts_after_ms(500)));
  n2->wait_all_msg_consumed();
  n2->wait_all_redolog_applied();

  for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
    const int commit_ret = n1->commit_tx(*sec_txs[i], n1->ts_after_ms(500));
    ASSERT_TRUE(commit_ret == OB_SUCCESS || commit_ret == OB_TRANS_COMMITED)
        << "secondary tx commit failed, tx_id=" << secondary_tx_ids[i]
        << ", commit_ret=" << commit_ret;
  }

  ReplayLogEntryFunctor replay_old_leader(n1);
  ASSERT_EQ(OB_SUCCESS, n1->fake_tx_log_adapter_->replay_all(replay_old_leader));
  n1->wait_all_msg_consumed();
  n1->wait_all_redolog_applied();

  ASSERT_EQ(OB_SUCCESS, primary_guard.release());
  ASSERT_EQ(OB_SUCCESS, sec_guard0.release());
  ASSERT_EQ(OB_SUCCESS, sec_guard1.release());

  ASSERT_EQ(OB_SUCCESS, n2->wait_all_tx_ctx_is_destoryed());
  ASSERT_EQ(OB_SUCCESS, n1->wait_all_tx_ctx_is_destoryed());

  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr2));
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr1));
#endif
}

// Verify that after force revoke, replay of abort log on old leader can
// clean up hotspot_redo_cache even when log CBs are stranded in free list.
//
// Before fix: try_release_idle_log_cb() returns OB_EAGAIN because
// cbs_is_working_() sees free>0, blocking clean_hotspot_redo_cache() and
// preventing tx ctx from exiting (EDIAG at ob_tx_hotspot_helper.cpp:45).
//
// After fix: try_release_idle_log_cb() calls release_free_cbs_to_idle_()
// first, so free CBs are moved to idle and released normally.
TEST_F(ObTestHotspotTxLeaderSwitch,
       hotspot_force_revoke_replay_abort_releases_free_cbs)
{
  ObTxNode::reset_localtion_adapter();
  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  auto n2 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_);
  DEFER(delete(n1));
  DEFER(delete(n2));
  ASSERT_EQ(OB_SUCCESS, n1->start());
  n2->set_as_follower_replica(*n1);
  ASSERT_EQ(OB_SUCCESS, n2->start());

  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000 * 1000 * 1000;
  ObTxDescGuard primary_guard = n1->get_tx_guard();
  ObTxDesc &primary_tx = primary_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary_tx, tx_param));

  ObTxReadSnapshot snapshot;
  ASSERT_EQ(OB_SUCCESS,
            n1->get_read_snapshot(primary_tx, tx_param.isolation_, n1->ts_after_ms(100), snapshot));

  // Use more secondary txs to increase the chance of CBs in free list
  const int SECONDARY_TX_COUNT = 3;
  ObTransID secondary_tx_ids[SECONDARY_TX_COUNT];
  ObTxDescGuard sec_guards[SECONDARY_TX_COUNT] = {
      n1->get_tx_guard(), n1->get_tx_guard(), n1->get_tx_guard()};
  ObTxDesc *sec_txs[SECONDARY_TX_COUNT];
  for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
    sec_txs[i] = &sec_guards[i].get_tx_desc();
    ASSERT_EQ(OB_SUCCESS, n1->start_tx(*sec_txs[i], tx_param));
    secondary_tx_ids[i] = sec_txs[i]->tx_id_;
    ASSERT_EQ(OB_SUCCESS, n1->write(*sec_txs[i], snapshot, 800 + i, 900 + i));
  }
  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, snapshot, 7000, 8000));
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

  ObLSTxCtxMgr *ls_tx_ctx_mgr1 = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr1));

  // Flush redo so CBs are allocated
  ObTransID fail_tx_id;
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr1->traverse_tx_to_submit_redo_log(fail_tx_id, UINT32_MAX));
  n1->wait_all_msg_consumed();
  n1->wait_all_redolog_applied();

  // Force revoke on n1. This may leave CBs in free list if submit fails mid-way.
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr1->switch_to_follower_forcedly());
  ASSERT_FALSE(ls_tx_ctx_mgr1->is_master());
  ASSERT_GT(ls_tx_ctx_mgr1->get_tx_ctx_count(), 0);

  // Replay on n2 to bring it up to date
  ReplayLogEntryFunctor replay_new_leader(n2);
  ASSERT_EQ(OB_SUCCESS, n2->fake_tx_log_adapter_->replay_all(replay_new_leader));

  ObLSTxCtxMgr *ls_tx_ctx_mgr2 = nullptr;
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n2->ls_id_, ls_tx_ctx_mgr2));
  ObTxNode::get_location_adapter_().update_localtion(n2->ls_id_, n2->addr_);
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr2->switch_to_leader());
  n2->wait_all_redolog_applied();
  ASSERT_TRUE(ls_tx_ctx_mgr2->is_master());

  // Wait for new leader to pick up primary tx
  bool primary_waiting_delay_abort = false;
  for (int i = 0; i < 300; i++) {
    ObPartTransCtx *primary_part_ctx = nullptr;
    int get_ret = ls_tx_ctx_mgr2->get_tx_ctx_directly_from_hash_map(primary_tx_id, primary_part_ctx);
    if (OB_TRANS_CTX_NOT_EXIST != get_ret) {
      ASSERT_EQ(OB_SUCCESS, get_ret);
      ASSERT_TRUE(primary_part_ctx != nullptr);
      primary_waiting_delay_abort =
          primary_part_ctx->sub_state_.is_force_abort() && !primary_part_ctx->is_exiting_;
      ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr2->revert_tx_ctx(primary_part_ctx));
      if (primary_waiting_delay_abort) {
        break;
      }
    }
    n2->wait_all_msg_consumed();
    n2->wait_all_redolog_applied();
    usleep(1000);
  }
  ASSERT_TRUE(primary_waiting_delay_abort);

  // New leader aborts primary tx -> writes abort log
  ObPartTransCtx *new_leader_primary_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            ls_tx_ctx_mgr2->get_tx_ctx(primary_tx_id, false, new_leader_primary_ctx));
  ASSERT_TRUE(new_leader_primary_ctx != nullptr);
  {
    CtxLockGuard guard(new_leader_primary_ctx->lock_);
    ASSERT_EQ(OB_SUCCESS, new_leader_primary_ctx->abort_(OB_TRANS_ROLLBACKED));
  }
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr2->revert_tx_ctx(new_leader_primary_ctx));
  n2->wait_all_msg_consumed();
  n2->wait_all_redolog_applied();

  // Inject free CBs into old leader's primary ctx to simulate the scenario
  // where hotspot log submit returned OB_NOT_MASTER during leader switch,
  // leaving CBs stranded in free_hotspot_cbs_ with no clog callback.
  // In production this happens when submit_log_block_out_() fails mid-way
  // during switch_to_follower_forcedly.
  {
    ObPartTransCtx *n1_primary_ctx = nullptr;
    int get_ret = ls_tx_ctx_mgr1->get_tx_ctx_directly_from_hash_map(primary_tx_id, n1_primary_ctx);
    ASSERT_EQ(OB_SUCCESS, get_ret);
    ASSERT_TRUE(n1_primary_ctx != nullptr);

    // Allocate CBs from the tx's log cb pool and place them into free list.
    // This mimics the state after on_failure() moves a CB out of busy list
    // but submit had no clog callback to eventually release it.
    ObTxHotspotRedoCacheHandle &cache = n1_primary_ctx->hotspot_redo_cache_;
    {
      SpinWLockGuard guard(cache.hotspot_lock_);
      ASSERT_TRUE(cache.cache_ != nullptr);
      const int INJECT_FREE_CB_COUNT = 2;
      for (int i = 0; i < INJECT_FREE_CB_COUNT; i++) {
        ObTxLogCb *cb = nullptr;
        int alloc_ret = n1_primary_ctx->prepare_log_cb_(false /*need_final_cb*/, cb);
        if (OB_SUCCESS == alloc_ret && cb != nullptr) {
          cb->reuse();
          cache.cache_->free_hotspot_cbs_.add_last(cb);
          cache.cache_->all_log_cb_cnt_++;
        }
      }
      TRANS_LOG(INFO, "[test] injected free CBs into old leader hotspot_redo_cache",
                K(primary_tx_id),
                K(cache.cache_->free_hotspot_cbs_.get_size()),
                K(cache.cache_->busy_hotspot_cbs_.get_size()),
                K(cache.cache_->idle_hotspot_cbs_.get_size()),
                K(cache.cache_->all_log_cb_cnt_));
      ASSERT_GT(cache.cache_->free_hotspot_cbs_.get_size(), 0);
    }
    ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr1->revert_tx_ctx(n1_primary_ctx));
  }

  // Old leader (n1) replays abort log. This triggers clean_hotspot_redo_cache().
  // Before fix: fails with EDIAG if free_hotspot_cbs_ > 0.
  // After fix: free CBs are moved to idle and released.
  ReplayLogEntryFunctor replay_old_leader(n1);
  ASSERT_EQ(OB_SUCCESS, n1->fake_tx_log_adapter_->replay_all(replay_old_leader));
  n1->wait_all_msg_consumed();
  n1->wait_all_redolog_applied();

  // Both nodes must destroy all tx ctxs. Before fix, n1 would hang here
  // because clean_hotspot_redo_cache() failed and ctx never exited.
  ASSERT_EQ(OB_SUCCESS, n1->wait_all_tx_ctx_is_destoryed());
  ASSERT_EQ(OB_SUCCESS, n2->wait_all_tx_ctx_is_destoryed());

  ASSERT_EQ(OB_SUCCESS, n1->abort_tx(primary_tx, OB_TRANS_CTX_NOT_EXIST));
  for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
    ASSERT_EQ(OB_SUCCESS, n1->abort_tx(*sec_txs[i], OB_TRANS_CTX_NOT_EXIST));
  }
  ASSERT_EQ(OB_SUCCESS, primary_guard.release());
  for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
    ASSERT_EQ(OB_SUCCESS, sec_guards[i].release());
  }

  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr2));
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr1));
}

} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_hotspot_tx_leader_switch.log*");
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_hotspot_tx_leader_switch.log", true, false,
                       "test_hotspot_tx_leader_switch.log",  // rs
                       "test_hotspot_tx_leader_switch.log",  // election
                       "test_hotspot_tx_leader_switch.log"); // audit
  logger.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
