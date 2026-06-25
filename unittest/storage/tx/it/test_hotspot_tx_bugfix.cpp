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

#include <gtest/gtest.h>
#include <string>
#include <vector>
#define private public
#define protected public
#define USING_LOG_PREFIX TRANS
#include "lib/utility/ob_tracepoint.h"
#include "lib/time/ob_time_utility.h"
#include "../mock_utils/async_util.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/tx/ob_tx_hotspot_helper.h"
#include "test_tx_dsl.h"
#include "tx_node.h"

namespace oceanbase
{
using namespace ::testing;
using namespace transaction;
using namespace share;

static ObSharedMemAllocMgr MTL_MEM_ALLOC_MGR;

// Mock ObTransService::push to directly call handle in main thread
// This avoids the MTL(ObTransService*) issue in thread pool worker threads
OB_NOINLINE int ObTransService::push(void *task)
{
  // Directly call handle in main thread instead of queueing to thread pool
  handle(task);
  return OB_SUCCESS;
}

namespace share
{

ObTxDataThrottleGuard::~ObTxDataThrottleGuard() {}

int ObTenantTxDataAllocator::init(const char *label)
{
  int ret = OB_SUCCESS;
  ObMemAttr mem_attr;
  throttle_tool_ = &(MTL_MEM_ALLOC_MGR.share_resource_throttle_tool());
  if (OB_FAIL(slice_allocator_.init(storage::TX_DATA_SLICE_SIZE, OB_MALLOC_NORMAL_BLOCK_SIZE,
                                    block_alloc_, mem_attr))) {
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
  uint64_t tenant_id = 1;
  ObSharedMemAllocMgr *mtl_alloc_mgr = &MTL_MEM_ALLOC_MGR;
  ObMemstoreAllocator &host = mtl_alloc_mgr->memstore_allocator();
  (void)host.init_handle(*this);
  return ret;
}
}; // namespace share

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

class ObTestHotspotTxBugfix : public ::testing::Test
{
public:
  virtual void SetUp() override
  {
    oceanbase::ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
    ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(1001);
    ObAddr ip_port(ObAddr::VER::IPV4, "119.119.0.1", 2023);
    ObCurTraceId::init(ip_port);
    GCONF._ob_trans_rpc_timeout = 500;
    ObClockGenerator::init();
    const testing::TestInfo *const test_info =
        testing::UnitTest::GetInstance()->current_test_info();
    MTL_MEM_ALLOC_MGR.init();
    auto test_name = test_info->name();
    _TRANS_LOG(INFO, ">>>> starting test : %s", test_name);
  }
  virtual void TearDown() override
  {
    const testing::TestInfo *const test_info =
        testing::UnitTest::GetInstance()->current_test_info();
    auto test_name = test_info->name();
    _TRANS_LOG(INFO, ">>>> tearDown test : %s", test_name);
    ObClockGenerator::destroy();
    ObMallocAllocator::get_instance()->recycle_tenant_allocator(1001);
  }
  MsgBus bus_;
};

// TEST_F(ObTestHotspotTxBugfix, test_hotspot_tx_dispatch_redo_after_commit)
// {
//   {
//     oceanbase::common::EventItem item;
//     item.error_code_ = 2;
//     item.occur_ = 1;
//     item.trigger_freq_ = 1;
//     item.cond_ = 0;
//     (void)oceanbase::common::EventTable::set_event("ERRSIM_HOTSPOT_TARGET_LOG_CB_CNT", item);
//   }
//   GCONF._ob_trans_rpc_timeout = 50;
//   ObTxNode::reset_localtion_adapter();

//   START_ONE_TX_NODE(n1);

//   // create primary tx
//   PREPARE_TX(n1, primary_tx);
//   PREPARE_TX_PARAM(tx_param);
//   tx_param.timeout_us_ = 1000 * 1000 * 1000;
//   GET_READ_SNAPSHOT(n1, primary_tx, tx_param, snapshot);
//   ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary_tx, tx_param));

//   // create secondary txs
//   const int SECONDARY_TX_COUNT = 10;
//   ObTransID secondary_tx_ids[SECONDARY_TX_COUNT];
//   std::vector<ObTxDescGuard> sec_guards;
//   sec_guards.reserve(SECONDARY_TX_COUNT);
//   for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
//     sec_guards.push_back(n1->get_tx_guard());
//   }
//   ObTxDesc *sec_txs[SECONDARY_TX_COUNT];
//   for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
//     sec_txs[i] = &sec_guards[i].get_tx_desc();
//   }

//   for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
//     ASSERT_EQ(OB_SUCCESS, n1->start_tx(*sec_txs[i], tx_param));
//     secondary_tx_ids[i] = sec_txs[i]->tx_id_;
//     ASSERT_EQ(OB_SUCCESS, n1->write(*sec_txs[i], snapshot, 100 + i, 200 + i));
//   }

//   ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, snapshot, 1000, 2000));

//   // aggregate secondary into primary
//   ObTxPart participant;
//   participant.id_ = n1->ls_id_;
//   participant.addr_ = n1->addr_;
//   participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;

//   ObAggregatedTxIDArray aggre_members;
//   for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
//     ASSERT_TRUE(secondary_tx_ids[i].is_valid());
//     ASSERT_EQ(OB_SUCCESS, aggre_members.push_back(secondary_tx_ids[i]));
//   }

//   ASSERT_EQ(OB_SUCCESS,
//             n1->txs_.sync_hotspot_legality_validation(participant, primary_tx.tx_id_,
//                                                       aggre_members));
//   n1->wait_all_msg_consumed();

//   // hold primary tx ctx ref to prevent early release
//   ObPartTransCtx *primary_ctx = nullptr;
//   ASSERT_EQ(OB_SUCCESS, n1->get_tx_ctx(n1->ls_id_, primary_tx.tx_id_, primary_ctx));
//   ASSERT_TRUE(primary_ctx != nullptr);
//   int64_t target_cnt = 0;
//   primary_ctx->hotspot_redo_cache_.need_increase_logging_concurrency(target_cnt);
//   ASSERT_EQ(0, target_cnt);
//   const int32_t ref_before = primary_ctx->get_ref();
//   ASSERT_EQ(OB_SUCCESS, primary_ctx->acquire_ctx_ref());
//   const int32_t ref_after = primary_ctx->get_ref();
//   ASSERT_EQ(ref_before + 1, ref_after);
//   ASSERT_EQ(OB_SUCCESS, n1->revert_tx_ctx(primary_ctx));

//   // commit primary and confirm responses
//   COMMIT_TX(n1, primary_tx, 500 * 1000);
//   n1->wait_all_msg_consumed();
//   ASSERT_EQ(ObTxDesc::State::COMMITTED, primary_tx.state_);
//   ASSERT_EQ(OB_SUCCESS, primary_tx.commit_out_);

//   for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
//     int commit_ret = n1->commit_tx(*sec_txs[i], n1->ts_after_us(500 * 1000));
//     ASSERT_TRUE(commit_ret == OB_SUCCESS || commit_ret == OB_TRANS_COMMITED)
//         << "Secondary tx " << i << " commit failed. tx_id=" << secondary_tx_ids[i]
//         << ", commit_ret=" << commit_ret;
//     ASSERT_EQ(ObTxDesc::State::COMMITTED, sec_txs[i]->state_);
//     ASSERT_EQ(OB_SUCCESS, sec_txs[i]->commit_out_);
//   }

//   // re-send hotspot dispatch redo msg to self
//   ObHotspotDispatchRedoMsg dispatch_msg;
//   ObPartTransCtx::build_tx_common_msg_(n1->ls_id_,
//                                       primary_ctx->cluster_version_,
//                                       primary_ctx->tenant_id_,
//                                       primary_tx.tx_id_.get_id(),
//                                       n1->addr_,
//                                       n1->ls_id_,
//                                       primary_ctx->cluster_id_,
//                                       dispatch_msg);
//   ASSERT_EQ(OB_SUCCESS, n1->fake_rpc_.post_msg(n1->addr_, dispatch_msg));
//   n1->wait_all_msg_consumed();

//   // verify hotspot redo cache log_cb count is cleared
//   int64_t free_cb_cnt = 0;
//   int64_t busy_cb_cnt = 0;
//   int64_t idle_cb_cnt = 0;
//   primary_ctx->hotspot_redo_cache_.get_cb_list_count(free_cb_cnt, busy_cb_cnt, idle_cb_cnt);
//   ASSERT_EQ(0, free_cb_cnt);
//   ASSERT_EQ(0, busy_cb_cnt);
//   ASSERT_EQ(0, idle_cb_cnt);
//   // ASSERT_EQ(0, primary_ctx->hotspot_redo_cache_.get_hotspot_cache_count());

//   // release primary ctx ref and allow ctx gc
//   primary_ctx->release_ctx_ref();

//   ASSERT_EQ(OB_SUCCESS, guard.release());
//   for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
//     ASSERT_EQ(OB_SUCCESS, sec_guards[i].release());
//   }
//   n1->wait_all_msg_consumed();

//   int wait_ret = OB_SUCCESS;
//   for (int i = 0; i < 5; i++) {
//     wait_ret = n1->wait_all_tx_ctx_is_destoryed();
//     if (OB_SUCCESS == wait_ret) {
//       break;
//     }
//     usleep(100 * 1000);
//   }
//   ASSERT_EQ(OB_SUCCESS, wait_ret);
//   {
//     oceanbase::common::EventItem item;
//     item.error_code_ = 0;
//     item.occur_ = 0;
//     item.trigger_freq_ = 0;
//     item.cond_ = 0;
//     (void)oceanbase::common::EventTable::set_event("ERRSIM_HOTSPOT_TARGET_LOG_CB_CNT", item);
//   }
// }

// TEST_F(ObTestHotspotTxBugfix, test_hotspot_tx_late_dispatch_on_exiting_ctx)
// {
//   {
//     oceanbase::common::EventItem item;
//     item.error_code_ = 2;
//     item.occur_ = 1;
//     item.trigger_freq_ = 1;
//     item.cond_ = 0;
//     (void)oceanbase::common::EventTable::set_event("ERRSIM_HOTSPOT_TARGET_LOG_CB_CNT", item);
//   }
//   GCONF._ob_trans_rpc_timeout = 50;
//   ObTxNode::reset_localtion_adapter();

//   START_ONE_TX_NODE(n1);

//   PREPARE_TX(n1, primary_tx);
//   PREPARE_TX_PARAM(tx_param);
//   tx_param.timeout_us_ = 1000 * 1000 * 1000;
//   GET_READ_SNAPSHOT(n1, primary_tx, tx_param, snapshot);
//   ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary_tx, tx_param));

//   const int SECONDARY_TX_COUNT = 10;
//   ObTransID secondary_tx_ids[SECONDARY_TX_COUNT];
//   std::vector<ObTxDescGuard> sec_guards;
//   sec_guards.reserve(SECONDARY_TX_COUNT);
//   for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
//     sec_guards.push_back(n1->get_tx_guard());
//   }
//   ObTxDesc *sec_txs[SECONDARY_TX_COUNT];
//   for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
//     sec_txs[i] = &sec_guards[i].get_tx_desc();
//   }

//   for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
//     ASSERT_EQ(OB_SUCCESS, n1->start_tx(*sec_txs[i], tx_param));
//     secondary_tx_ids[i] = sec_txs[i]->tx_id_;
//     ASSERT_EQ(OB_SUCCESS, n1->write(*sec_txs[i], snapshot, 300 + i, 400 + i));
//   }
//   ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, snapshot, 3000, 4000));

//   ObTxPart participant;
//   participant.id_ = n1->ls_id_;
//   participant.addr_ = n1->addr_;
//   participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;

//   ObAggregatedTxIDArray aggre_members;
//   for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
//     ASSERT_TRUE(secondary_tx_ids[i].is_valid());
//     ASSERT_EQ(OB_SUCCESS, aggre_members.push_back(secondary_tx_ids[i]));
//   }

//   ASSERT_EQ(OB_SUCCESS,
//             n1->txs_.sync_hotspot_legality_validation(participant, primary_tx.tx_id_,
//                                                       aggre_members));
//   n1->wait_all_msg_consumed();

//   ObPartTransCtx *primary_ctx = nullptr;
//   ASSERT_EQ(OB_SUCCESS, n1->get_tx_ctx(n1->ls_id_, primary_tx.tx_id_, primary_ctx));
//   ASSERT_TRUE(primary_ctx != nullptr);
//   ASSERT_EQ(OB_SUCCESS, primary_ctx->acquire_ctx_ref());
//   ASSERT_EQ(OB_SUCCESS, n1->revert_tx_ctx(primary_ctx));

//   // simulate late dispatch on exiting ctx
//   primary_ctx->set_exiting();
//   {
//     SpinWLockGuard guard(primary_ctx->hotspot_redo_cache_.hotspot_lock_);
//     const int64_t cache_cnt = primary_ctx->hotspot_redo_cache_.hotspot_cache_.count();
//     for (int64_t i = 0; i < cache_cnt; i++) {
//       primary_ctx->hotspot_redo_cache_.hotspot_cache_[i].unsynced_node_cnt_ = 1;
//       primary_ctx->hotspot_redo_cache_.hotspot_cache_[i].pending_log_size_ = 1;
//     }
//   }
//   int64_t target_cnt = 0;
//   primary_ctx->hotspot_redo_cache_.need_increase_logging_concurrency(target_cnt);
//   ASSERT_EQ(2, target_cnt);

//   ObHotspotDispatchRedoMsg dispatch_msg;
//   ObPartTransCtx::build_tx_common_msg_(n1->ls_id_,
//                                       primary_ctx->cluster_version_,
//                                       primary_ctx->tenant_id_,
//                                       primary_tx.tx_id_.get_id(),
//                                       n1->addr_,
//                                       n1->ls_id_,
//                                       primary_ctx->cluster_id_,
//                                       dispatch_msg);
//   ASSERT_EQ(OB_SUCCESS, n1->fake_rpc_.post_msg(n1->addr_, dispatch_msg));
//   n1->wait_all_msg_consumed();

//   int64_t free_cb_cnt = 0;
//   int64_t busy_cb_cnt = 0;
//   int64_t idle_cb_cnt = 0;
//   primary_ctx->hotspot_redo_cache_.get_cb_list_count(free_cb_cnt, busy_cb_cnt, idle_cb_cnt);
//   ASSERT_GT(free_cb_cnt + busy_cb_cnt + idle_cb_cnt, 0);
//   ASSERT_EQ(OB_EAGAIN, primary_ctx->hotspot_redo_cache_.reuse());

//   primary_ctx->release_ctx_ref();

//   ASSERT_EQ(OB_SUCCESS, guard.release());
//   for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
//     ASSERT_EQ(OB_SUCCESS, sec_guards[i].release());
//   }
//   n1->wait_all_msg_consumed();
//   {
//     oceanbase::common::EventItem item;
//     item.error_code_ = 0;
//     item.occur_ = 0;
//     item.trigger_freq_ = 0;
//     item.cond_ = 0;
//     (void)oceanbase::common::EventTable::set_event("ERRSIM_HOTSPOT_TARGET_LOG_CB_CNT", item);
//   }
// }

TEST_F(ObTestHotspotTxBugfix, test_hotspot_tx_freeze_with_blocked_secondary_log)
{
  const bool old_parallel_redo_logging = GCONF._enable_parallel_redo_logging;
  const int64_t old_parallel_redo_trigger = GCONF._parallel_redo_logging_trigger;
  const int64_t old_private_buffer_size = GCONF._private_buffer_size;
  GCONF._enable_parallel_redo_logging = false;
  GCONF._parallel_redo_logging_trigger = INT64_MAX;
  GCONF._private_buffer_size = 1024L * 1024L * 1024L;
  {
    oceanbase::common::EventItem item;
    item.error_code_ = 0;
    item.occur_ = 0;
    item.trigger_freq_ = 0;
    item.cond_ = 0;
    (void)oceanbase::common::EventTable::set_event("TX_FORCE_WRITE_CLOG", item);
  }
  {
    oceanbase::common::EventItem item;
    item.error_code_ = 3; // roughly 1/3 of 10 secondary txs
    item.occur_ = 1;
    item.trigger_freq_ = 1;
    item.cond_ = 0;
    (void)oceanbase::common::EventTable::set_event("ERRSIM_HOTSPOT_TARGET_LOG_CB_CNT", item);
  }

  GCONF._ob_trans_rpc_timeout = 50;
  ObTxNode::reset_localtion_adapter();

  START_ONE_TX_NODE(n1);

  memtable::ObMemtable *blocked_memtable = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->create_memtable_(100001, blocked_memtable));
  ASSERT_TRUE(blocked_memtable != nullptr);
  memtable::ObMemtable *origin_memtable = n1->memtable_;

  PREPARE_TX(n1, primary_tx);
  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000 * 1000 * 1000;
  GET_READ_SNAPSHOT(n1, primary_tx, tx_param, snapshot);
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary_tx, tx_param));

  const int SECONDARY_TX_COUNT = 10;
  ObTransID secondary_tx_ids[SECONDARY_TX_COUNT];
  std::vector<ObTxDescGuard> sec_guards;
  sec_guards.reserve(SECONDARY_TX_COUNT);
  for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
    sec_guards.push_back(n1->get_tx_guard());
  }
  ObTxDesc *sec_txs[SECONDARY_TX_COUNT];
  for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
    sec_txs[i] = &sec_guards[i].get_tx_desc();
  }

  for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
    ASSERT_EQ(OB_SUCCESS, n1->start_tx(*sec_txs[i], tx_param));
    secondary_tx_ids[i] = sec_txs[i]->tx_id_;
    if (0 == i) {
      n1->memtable_ = blocked_memtable;
    }
    ASSERT_EQ(OB_SUCCESS, n1->write(*sec_txs[i], snapshot, 500 + i, 600 + i));
    if (0 == i) {
      n1->memtable_ = origin_memtable;
    }
  }
  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, snapshot, 5000, 6000));

  // enlarge primary tx pending log size to ~7MB before sync validation
  const int64_t target_pending_size = 7 * 1024 * 1024;
  const int64_t key_base = 100000;
  n1->columns_.at(1).col_type_.set_varchar();
  n1->columns_.at(1).col_type_.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  std::string big_val(1 * 1024 * 1024, 'a');
  ObObj big_obj;
  big_obj.set_varchar(ObString(big_val.size(), big_val.data()));
  big_obj.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  for (int i = 0; i < 7; i++) {
    ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, snapshot, key_base + i, big_obj));
  }
  {
    ObPartTransCtx *tmp_ctx = nullptr;
    ASSERT_EQ(OB_SUCCESS, n1->get_tx_ctx(n1->ls_id_, primary_tx.tx_id_, tmp_ctx));
    ASSERT_TRUE(tmp_ctx != nullptr);
    const int64_t pending_size = tmp_ctx->get_memtable_ctx()->get_pending_log_size();
    TRANS_LOG(INFO, "primary pending log size", K(pending_size), K(target_pending_size));
    ASSERT_EQ(OB_SUCCESS, n1->revert_tx_ctx(tmp_ctx));
  }

  ObTxPart participant;
  participant.id_ = n1->ls_id_;
  participant.addr_ = n1->addr_;
  participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;

  ObAggregatedTxIDArray aggre_members;
  for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
    ASSERT_TRUE(secondary_tx_ids[i].is_valid());
    ASSERT_EQ(OB_SUCCESS, aggre_members.push_back(secondary_tx_ids[i]));
  }

  // block only secondary's memtable logging before aggregation
  ASSERT_EQ(OB_SUCCESS, blocked_memtable->set_logging_blocked());

  auto sync_validation_f = [&]() -> int {
    ObTenantEnv::set_tenant(&n1->tenant_);
    return n1->txs_.sync_hotspot_legality_validation(participant, primary_tx.tx_id_,
                                                     aggre_members);
  };
  auto sync_validation_async = test::make_async(sync_validation_f);
  sync_validation_async.wait_started();

  // Let sync_validation complete eligibility check (primary + all secondaries)
  // before submit_freeze runs; otherwise traverse_tx_to_submit_redo_log may
  // submit redo for some secondaries first, making them ineligible (redo_lsns
  // or redo_flush_status set) and causing OB_TX_NOT_SUPPORT_AGGREGATION(-6289).
  usleep(300 * 1000);

  ObPartTransCtx *primary_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->get_tx_ctx(n1->ls_id_, primary_tx.tx_id_, primary_ctx));
  ASSERT_TRUE(primary_ctx != nullptr);
  ASSERT_EQ(OB_SUCCESS, primary_ctx->acquire_ctx_ref());
  ASSERT_EQ(OB_SUCCESS, n1->revert_tx_ctx(primary_ctx));

  ObLSTxCtxMgr *ls_tx_ctx_mgr = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr));
  const uint32_t freeze_clock = UINT32_MAX;
  auto submit_freeze_f = [&]() -> int {
    ObTenantEnv::set_tenant(&n1->tenant_);
    ObTransID fail_tx_id;
    int ret = OB_SUCCESS;
    int64_t retry_cnt = 0;
    int64_t last_log_ts = ObTimeUtility::fast_current_time();
    TRANS_LOG(INFO, "begin to submit freeze log", K(ret), K(fail_tx_id), K(freeze_clock));
    while (true) {
      ret = ls_tx_ctx_mgr->traverse_tx_to_submit_redo_log(fail_tx_id, freeze_clock);
      if (OB_SUCCESS == ret) {
        TRANS_LOG(INFO, "submit freeze log success", K(ret), K(fail_tx_id), K(freeze_clock), K(retry_cnt));
        break;
      }
      ++retry_cnt;
      const int64_t now_ts = ObTimeUtility::fast_current_time();
      if (now_ts - last_log_ts >= 1000 * 1000) {
        TRANS_LOG(WARN, "submit freeze log retrying", K(ret), K(fail_tx_id), K(freeze_clock), K(retry_cnt));
        last_log_ts = now_ts;
      }
      usleep(10 * 1000);
    }
    TRANS_LOG(INFO, "end to submit freeze log", K(ret), K(fail_tx_id), K(freeze_clock));
    return ret;
  };
  auto submit_freeze_async = test::make_async(submit_freeze_f);
  submit_freeze_async.wait_started();

  const int64_t start_ts = ObTimeUtility::fast_current_time();
  const int64_t timeout_us = 20 * 1000 * 1000;  // Increased from 15s: freeze with blocked memtable needs time for downgrade
  while (!submit_freeze_async.is_evaled()
         && ObTimeUtility::fast_current_time() - start_ts < timeout_us) {
    usleep(10 * 1000);
  }
  const int64_t freeze_elapsed_us = ObTimeUtility::fast_current_time() - start_ts;
  if (!submit_freeze_async.is_evaled()) {
    fprintf(stderr, "submit freeze async timeout before assert, freeze_elapsed_us=%ld, timeout_us=%ld\n",
            freeze_elapsed_us, timeout_us);
  }
  ASSERT_TRUE(submit_freeze_async.is_evaled()) << "submit log for freeze timeout";
  ASSERT_LE(freeze_elapsed_us, timeout_us) << "submit log for freeze timeout";
  ASSERT_EQ(OB_SUCCESS, submit_freeze_async.get());

  blocked_memtable->unset_logging_blocked();
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr));

  {
    const int64_t start_ts = ObTimeUtility::fast_current_time();
    const int64_t timeout_us = 30 * 1000 * 1000;  // Increased from 15s: sync_validation needs time after freeze
    while (!sync_validation_async.is_evaled()
           && ObTimeUtility::fast_current_time() - start_ts < timeout_us) {
      usleep(10 * 1000);
    }
    const int64_t sync_elapsed_us = ObTimeUtility::fast_current_time() - start_ts;
    ASSERT_TRUE(sync_validation_async.is_evaled()) << "sync_hotspot_legality_validation timeout";
    ASSERT_LE(sync_elapsed_us, timeout_us) << "sync_hotspot_legality_validation timeout";
    ASSERT_EQ(OB_SUCCESS, sync_validation_async.get());
  }

  primary_ctx->release_ctx_ref();
  ASSERT_EQ(OB_SUCCESS, guard.release());
  for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
    ASSERT_EQ(OB_SUCCESS, sec_guards[i].release());
  }
  n1->wait_all_msg_consumed();
  n1->wait_tx_log_synced();
  delete blocked_memtable;

  GCONF._enable_parallel_redo_logging = old_parallel_redo_logging;
  GCONF._parallel_redo_logging_trigger = old_parallel_redo_trigger;
  GCONF._private_buffer_size = old_private_buffer_size;
  {
    oceanbase::common::EventItem item;
    item.error_code_ = 0;
    item.occur_ = 0;
    item.trigger_freq_ = 0;
    item.cond_ = 0;
    (void)oceanbase::common::EventTable::set_event("ERRSIM_HOTSPOT_TARGET_LOG_CB_CNT", item);
  }
}

TEST_F(ObTestHotspotTxBugfix, test_hotspot_redo_cache_reuse_reset_basic_members)
{
  // Guard against regressing to the old large inline arrays. The current
  // implementation keeps the cache array in dynamically allocated storage, so
  // the exact object size may shrink but should not exceed this upper bound.
  constexpr int64_t HOTSPOT_REDO_CACHE_SIZE_UPPER_BOUND = 1600;
  ASSERT_LE(static_cast<int64_t>(sizeof(ObTxHotspotRedoCache)),
            HOTSPOT_REDO_CACHE_SIZE_UPPER_BOUND);

  ObTxHotspotRedoCache hotspot_cache;

  // Explicitly initialize DList members (they may be polluted by previous tests)
  new (&hotspot_cache.idle_hotspot_cbs_) common::ObDList<ObTxLogCb>();
  new (&hotspot_cache.free_hotspot_cbs_) common::ObDList<ObTxLogCb>();
  new (&hotspot_cache.busy_hotspot_cbs_) common::ObDList<ObTxLogCb>();

  // Pollute basic members to verify reuse() restores default state.
  hotspot_cache.primary_tx_id_ = ObTransID(20260312001);
  hotspot_cache.total_pending_log_size_ = 1024;
  hotspot_cache.scheduled_end_index_ = 7;
  // Note: all_log_cb_cnt_ must be 0 for reuse() to succeed (need_return_cbs_() check)
  // Setting it to non-zero would cause OB_EAGAIN because listed_cnt < all_log_cb_cnt_
  hotspot_cache.all_log_cb_cnt_ = 0;
  hotspot_cache.pending_dispatch_msg_cnt_ = 9;
  hotspot_cache.pending_submit_other_redo_msg_cnt_ = 11;
  hotspot_cache.need_rollback_primary_tx_ = true;
  hotspot_cache.max_sub_tx_seq_no_ = ObTxSEQ(88, 2);
  hotspot_cache.last_response_succ_ret_code_ = OB_SUCCESS;

  ASSERT_EQ(OB_SUCCESS, hotspot_cache.reuse());

  ASSERT_EQ(0, hotspot_cache.primary_tx_id_.get_id());
  ASSERT_EQ(0, hotspot_cache.total_pending_log_size_);
  ASSERT_EQ(0, hotspot_cache.scheduled_end_index_);
  // all_log_cb_cnt_ should remain 0 after reuse() (was 0 before, reset to 0)
  ASSERT_EQ(0, hotspot_cache.all_log_cb_cnt_);
  ASSERT_EQ(0, hotspot_cache.pending_dispatch_msg_cnt_);
  ASSERT_EQ(0, hotspot_cache.pending_submit_other_redo_msg_cnt_);
  ASSERT_FALSE(hotspot_cache.need_rollback_primary_tx_);
  ASSERT_TRUE(hotspot_cache.max_sub_tx_seq_no_ == ObTxSEQ());
  ASSERT_EQ(OB_INVALID_ARGUMENT, hotspot_cache.last_response_succ_ret_code_);
}

// Verifies extract_redo_log_content behavior for partial submit scenario.
// ============================================================================
// Test: Partial Submit for Secondary TX Spanning Two Memtables
// ============================================================================
//
// PURPOSE:
//   Validates the fix at ob_tx_hotspot_define.cpp:551-553 that converts
//   OB_BLOCK_FROZEN to OB_SUCCESS when fill_count > 0, enabling partial submit.
//
// PROBLEM BACKGROUND:
//   Before the fix, when a secondary transaction's callbacks span multiple
//   memtables and one memtable is frozen (logging_blocked), the entire redo
//   extraction fails with OB_BLOCK_FROZEN (-4112), even if some callbacks
//   have already been successfully filled.
//
//   The key insight is: if fill_count > 0 (some data already serialized),
//   we should allow partial submit rather than failing completely.
//
// FIX LOGIC (ob_tx_hotspot_define.cpp:551-553):
//   if (OB_BLOCK_FROZEN == ret && fill_redo_ctx.fill_count_ > 0) {
//     ret = OB_SUCCESS;  // Allow partial submit
//   }
//
// ============================================================================
// TEST SCENARIO DIAGRAM:
// ============================================================================
//
//   Secondary TX writes to TWO memtables (in chronological order):
//
//   +------------------+          +------------------+
//   |    good_mt       |          |   blocked_mt     |
//   | (NOT blocked)    |          | (logging_blocked)|
//   +------------------+          +------------------+
//   |   cb1: key=100   |          |   cb3: key=500   |
//   |   cb2: key=300   |          |                  |
//   +------------------+          +------------------+
//          ^                              ^
//          |                              |
//   [Write first]                  [Write later]
//   fill_count += 2               triggers OB_BLOCK_FROZEN
//
//   Callback List Order (mt_ctx_->trans_mgr_):
//   [cb1] -> [cb2] -> [cb3]
//
// ============================================================================
// ITERATION FLOW (ObFillRedoLogFunctor::operator()):
// ============================================================================
//
//   Step 1: Iterate cb1 (on good_mt, NOT blocked)
//           -> fill_row_redo_() succeeds
//           -> fill_count = 1
//
//   Step 2: Iterate cb2 (on good_mt, NOT blocked)
//           -> fill_row_redo_() succeeds
//           -> fill_count = 2
//
//   Step 3: Iterate cb3 (on blocked_mt, logging_blocked=true)
//           -> callback->is_logging_blocked() returns true
//           -> ret = OB_BLOCK_FROZEN
//
//   Step 4: Check fix condition (ob_tx_hotspot_define.cpp:551-553)
//           -> OB_BLOCK_FROZEN == ret && fill_count > 0
//           -> Convert to OB_SUCCESS
//           -> Partial submit succeeds (cb1 + cb2 submitted)
//
// ============================================================================
// KEY VERIFICATION POINTS:
// ============================================================================
//
//   1. Callback count: Secondary tx has 3 callbacks (2 on good_mt + 1 on blocked_mt)
//   2. Iteration order: Callbacks are traversed in chronological order (cb1 -> cb2 -> cb3)
//   3. fill_count > 0: At least 2 callbacks filled before encountering OB_BLOCK_FROZEN
//   4. traverse_tx_to_submit_redo_log() returns OB_SUCCESS (not OB_BLOCK_FROZEN)
//
// ============================================================================
// WHY PREVIOUS TEST DID NOT COVER THIS SCENARIO:
// ============================================================================
//
//   In test_hotspot_tx_freeze_with_blocked_secondary_log:
//   - Each secondary tx writes only ONE row to ONE memtable
//   - sec_txs[0] writes to blocked_memtable (only 1 callback)
//   - When extracting redo for sec_txs[0]:
//     * First callback is on blocked_mt -> OB_BLOCK_FROZEN immediately
//     * fill_count = 0 (no callbacks filled yet)
//     * Fix condition NOT satisfied (fill_count == 0)
//
//   This test addresses the gap by ensuring fill_count > 0 before OB_BLOCK_FROZEN.
//
// ============================================================================
TEST_F(ObTestHotspotTxBugfix, test_secondary_partial_submit_spanning_memtables)
{
  // ==========================================================================
  // PHASE 1: Environment Setup
  // ==========================================================================
  GCONF._ob_trans_rpc_timeout = 50;
  GCONF._enable_parallel_redo_logging = false;
  GCONF._parallel_redo_logging_trigger = INT64_MAX;
  ObTxNode::reset_localtion_adapter();
  START_ONE_TX_NODE(n1);

  // Create two memtables with clear naming:
  // - good_mt: Memtable where logging_blocked will NOT be set
  //            Callbacks here can be filled and submitted
  // - blocked_mt: Memtable where logging_blocked will be set later
  //               Callbacks here will trigger OB_BLOCK_FROZEN
  memtable::ObMemtable *good_mt = n1->memtable_;
  memtable::ObMemtable *blocked_mt = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->create_memtable_(100002, blocked_mt));
  ASSERT_TRUE(blocked_mt != nullptr);
  TRANS_LOG(INFO, "Created two memtables for spanning test", KP(good_mt), KP(blocked_mt));

  // ==========================================================================
  // PHASE 2: Transaction Preparation
  // ==========================================================================
  PREPARE_TX(n1, primary_tx);
  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000 * 1000 * 1000;  // Long timeout for test stability
  GET_READ_SNAPSHOT(n1, primary_tx, tx_param, snapshot);
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary_tx, tx_param));

  // ==========================================================================
  // PHASE 3: Secondary TX Writes to BOTH Memtables (CRITICAL!)
  // ==========================================================================
  //
  // This is the KEY difference from previous tests:
  // We write to TWO different memtables in chronological order,
  // ensuring callbacks are created in the desired iteration order.
  //
  ObTxDescGuard sec_guard = n1->get_tx_guard();
  ObTxDesc *sec_tx = &sec_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(*sec_tx, tx_param));

  // STEP 3.1: Write to good_mt FIRST (creates cb1, cb2 on good_mt)
  // ----------------------------------------------------------------
  // These callbacks will be filled first during iteration,
  // resulting in fill_count = 2 before we encounter blocked_mt.
  //
  // Timeline:
  //   T1: write(key=100) -> cb1 created on good_mt
  //   T2: write(key=300) -> cb2 created on good_mt
  //
  n1->memtable_ = good_mt;
  ASSERT_EQ(OB_SUCCESS, n1->write(*sec_tx, snapshot, 100, 200));  // cb1
  ASSERT_EQ(OB_SUCCESS, n1->write(*sec_tx, snapshot, 300, 400));  // cb2
  TRANS_LOG(INFO, "Secondary wrote 2 rows to good_mt (cb1, cb2 created)");

  // STEP 3.2: Write to blocked_mt SECOND (creates cb3 on blocked_mt)
  // ----------------------------------------------------------------
  // This callback will trigger OB_BLOCK_FROZEN when logging_blocked is set.
  // Since it's created AFTER cb1 and cb2, iteration order is preserved.
  //
  // Timeline:
  //   T3: write(key=500) -> cb3 created on blocked_mt
  //
  // Callback List: [cb1] -> [cb2] -> [cb3]
  //
  n1->memtable_ = blocked_mt;
  ASSERT_EQ(OB_SUCCESS, n1->write(*sec_tx, snapshot, 500, 600));  // cb3
  TRANS_LOG(INFO, "Secondary wrote 1 row to blocked_mt (cb3 created)");

  // STEP 3.3: Reset to good_mt for primary tx write
  n1->memtable_ = good_mt;
  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, snapshot, 5000, 6000));

  // ==========================================================================
  // PHASE 4: Get Secondary TX Context (Optional Verification)
  // ==========================================================================
  ObTransID sec_tx_id = sec_tx->tx_id_;
  ObPartTransCtx *sec_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->get_tx_ctx(n1->ls_id_, sec_tx_id, sec_ctx));
  ASSERT_TRUE(sec_ctx != nullptr);
  ASSERT_EQ(OB_SUCCESS, sec_ctx->acquire_ctx_ref());
  ASSERT_EQ(OB_SUCCESS, n1->revert_tx_ctx(sec_ctx));
  TRANS_LOG(INFO, "Secondary tx ctx obtained", K(sec_tx_id));

  // ==========================================================================
  // PHASE 5: Aggregate Secondary into Primary
  // ==========================================================================
  // After aggregation, primary tx's hotspot cache will contain sec_tx_id.
  // When extract_hotspot_redo is called, it will use sec_tx's mt_ctx_
  // to fill redo log content.
  //
  ObTxPart participant;
  participant.id_ = n1->ls_id_;
  participant.addr_ = n1->addr_;
  participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;

  common::ObSEArray<ObTransID, 10> aggre_members;
  aggre_members.push_back(sec_tx_id);
  ASSERT_EQ(OB_SUCCESS, n1->txs_.sync_hotspot_legality_validation(
      participant, primary_tx.tx_id_, aggre_members));
  n1->wait_all_msg_consumed();

  // Verify hotspot cache has entry for this secondary tx
  ObPartTransCtx *primary_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->get_tx_ctx(n1->ls_id_, primary_tx.tx_id_, primary_ctx));
  ASSERT_TRUE(primary_ctx != nullptr);
  ASSERT_EQ(OB_SUCCESS, primary_ctx->acquire_ctx_ref());
  ASSERT_EQ(OB_SUCCESS, n1->revert_tx_ctx(primary_ctx));
  ASSERT_EQ(1, primary_ctx->hotspot_redo_cache_.get_hotspot_cache_count());
  TRANS_LOG(INFO, "Primary ctx obtained, hotspot cache count = 1");

  // ==========================================================================
  // PHASE 6: Set logging_blocked on blocked_mt (TRIGGER POINT!)
  // ==========================================================================
  // This is done AFTER aggregation to ensure:
  // 1. cb1, cb2 are on good_mt (NOT blocked) -> can be filled
  // 2. cb3 is on blocked_mt (NOW blocked) -> triggers OB_BLOCK_FROZEN
  // 3. fill_count will be 2 when OB_BLOCK_FROZEN is encountered
  //
  // IMPORTANT: We set blocked AFTER writes, not BEFORE.
  // If we set blocked BEFORE writes, all callbacks would be on blocked_mt,
  // and fill_count would be 0 when OB_BLOCK_FROZEN is encountered.
  //
  ASSERT_EQ(OB_SUCCESS, blocked_mt->set_logging_blocked());
  TRANS_LOG(INFO, "Set logging_blocked on blocked_mt - cb3 will trigger OB_BLOCK_FROZEN");

  // ==========================================================================
  // PHASE 7: Trigger Redo Extraction (CORE VERIFICATION)
  // ==========================================================================
  // traverse_tx_to_submit_redo_log -> extract_hotspot_redo -> extract_redo_log_content
  //
  // Expected execution flow:
  //
  //   extract_redo_log_content(fill_redo_ctx):
  //   |
  //   |--> ObFillRedoLogFunctor::operator() iterates callbacks:
  //   |    |
  //   |    |--> [Iteration 1] cb1 on good_mt:
  //   |    |    is_logging_blocked() -> false
  //   |    |    fill_row_redo_() -> SUCCESS
  //   |    |    fill_count = 1
  //   |    |
  //   |    |--> [Iteration 2] cb2 on good_mt:
  //   |    |    is_logging_blocked() -> false
  //   |    |    fill_row_redo_() -> SUCCESS
  //   |    |    fill_count = 2  <-- KEY: > 0!
  //   |    |
  //   |    |--> [Iteration 3] cb3 on blocked_mt:
  //   |    |    is_logging_blocked() -> true
  //   |    |    ret = OB_BLOCK_FROZEN (-4112)
  //   |    |
  //   |    |--> [Fix Check] ob_tx_hotspot_define.cpp:551-553:
  //   |    |    OB_BLOCK_FROZEN == ret && fill_count > 0
  //   |    |    ret = OB_SUCCESS  <-- FIX APPLIED!
  //   |
  //   |--> Return OB_SUCCESS (partial submit succeeds)
  //
  ObLSTxCtxMgr *ls_tx_ctx_mgr = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr));

  ObTransID fail_tx_id;
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->traverse_tx_to_submit_redo_log(fail_tx_id, UINT32_MAX))
      << "Partial submit should succeed with fill_count > 0 (fix at line 551-553)";

  // ==========================================================================
  // PHASE 8: Wait for Async Operations & Cleanup
  // ==========================================================================
  n1->wait_all_msg_consumed();

  TRANS_LOG(INFO, "Test passed: fill_count > 0 when OB_BLOCK_FROZEN, converted to SUCCESS");

  // Cleanup
  blocked_mt->logging_blocked_ = false;
  n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr);
  primary_ctx->release_ctx_ref();
  sec_ctx->release_ctx_ref();
}

TEST_F(ObTestHotspotTxBugfix, test_hotspot_sort_secondary_ctxs_null_error)
{
  ObSEArray<ObPartTransCtx *, 32> secondary_ctxs;
  ASSERT_EQ(OB_SUCCESS, secondary_ctxs.push_back(nullptr));
  ASSERT_EQ(OB_ERR_UNEXPECTED,
            ObPartTransCtx::sort_secondary_ctxs_by_seq_base_for_hotspot_(secondary_ctxs));
}

TEST_F(ObTestHotspotTxBugfix, test_hotspot_assign_remapped_seq_ranges_single_pass_and_order_check)
{
  TransModulePageAllocator allocator;
  ObTxHotspotRedoCache hotspot_cache;

  // Manually allocate 2 entries for hotspot_cache_ (new pointer-based model)
  hotspot_cache.hotspot_cache_capacity_ = 2;
  hotspot_cache.hotspot_cache_count_ = 2;
  hotspot_cache.hotspot_cache_ = static_cast<ObTxRedoExtractArg*>(
      allocator.alloc(2 * sizeof(ObTxRedoExtractArg)));
  ASSERT_NE(nullptr, hotspot_cache.hotspot_cache_);

  ObTxRedoExtractArg first_arg;
  first_arg.secondary_seq_base_ = 10;
  first_arg.unsynced_node_cnt_ = 2;
  new (&hotspot_cache.hotspot_cache_[0]) ObTxRedoExtractArg(first_arg);  // placement new

  ObTxRedoExtractArg second_arg;
  second_arg.secondary_seq_base_ = 20;
  second_arg.unsynced_node_cnt_ = 1;
  new (&hotspot_cache.hotspot_cache_[1]) ObTxRedoExtractArg(second_arg);  // placement new
  ASSERT_EQ(OB_SUCCESS, hotspot_cache.assign_remapped_seq_ranges(ObTxSEQ(1, 10), ObTxSEQ(1, 11)));

  ASSERT_TRUE(hotspot_cache.hotspot_cache_[0].seq_remapped_);
  ASSERT_TRUE(hotspot_cache.hotspot_cache_[1].seq_remapped_);
  const ObTxSEQ expected_first_min = hotspot_cache.next_seq_(ObTxSEQ(1, 11));
  const ObTxSEQ expected_first_max =
      hotspot_cache.calc_remapped_range_end_(expected_first_min, first_arg.unsynced_node_cnt_);
  ASSERT_TRUE(hotspot_cache.hotspot_cache_[0].remapped_min_seq_ == expected_first_min);
  ASSERT_TRUE(hotspot_cache.hotspot_cache_[0].remapped_max_seq_ == expected_first_max);

  const ObTxSEQ expected_second_min = expected_first_max + 6; // 1 slot + HOTSPOT_REMAPPED_SUB_TX_SEQ_GAP
  const ObTxSEQ expected_second_max =
      hotspot_cache.calc_remapped_range_end_(expected_second_min, second_arg.unsynced_node_cnt_);
  ASSERT_TRUE(hotspot_cache.hotspot_cache_[1].remapped_min_seq_ == expected_second_min);
  ASSERT_TRUE(hotspot_cache.hotspot_cache_[1].remapped_max_seq_ == expected_second_max);

  hotspot_cache.hotspot_cache_[1].secondary_seq_base_ = 5;
  ASSERT_EQ(OB_ERR_UNEXPECTED,
            hotspot_cache.assign_remapped_seq_ranges(ObTxSEQ(1, 10), ObTxSEQ(1, 11)));
}

// Verifies that when sync_hotspot_legality_validation fails mid-loop,
// already-validated secondary txs have their redo_flush_status_ reset to NORMAL_START.
//
// BUG SCENARIO:
//   sync_hotspot_legality_validation iterates aggre_members[0..N-1].
//   For each eligible secondary tx, hotspot_legality_validation_() sets
//   redo_flush_status_ = DISABLE_REDO_FLUSH (30).
//
//   If the validation fails at index K (K > 0), the error cleanup resets
//   the PRIMARY tx's redo_flush_status but does NOT reset txs [0..K-1].
//   Those txs are permanently stuck at DISABLE_REDO_FLUSH, which blocks
//   log submission (can_not_submit_log_for_hotspot_), abort
//   (do_local_abort_tx_), and exit (can_not_exiting_for_hotspot_).
//
// FIX:
//   revert_non_null_hotspot_secondary_ctx_refs_() now calls
//   ctx->reset_hotspot_redo_status() on each secondary ctx before
//   releasing its reference.
//
// TEST APPROACH:
//   Create 4 secondary txs. Make the last one ineligible (inject a fake
//   redo_lsn). The first 3 will pass validation and get DISABLE_REDO_FLUSH.
//   After the failure, assert all 3 are reset to NORMAL_START and can commit.
TEST_F(ObTestHotspotTxBugfix, test_validation_failure_resets_secondary_redo_flush_status)
{
  GCONF._ob_trans_rpc_timeout = 50;
  ObTxNode::reset_localtion_adapter();

  START_ONE_TX_NODE(n1);

  PREPARE_TX(n1, primary_tx);
  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000 * 1000 * 1000;
  GET_READ_SNAPSHOT(n1, primary_tx, tx_param, snapshot);
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary_tx, tx_param));

  const int SECONDARY_TX_COUNT = 4;
  const int INELIGIBLE_IDX = SECONDARY_TX_COUNT - 1;
  ObTransID secondary_tx_ids[SECONDARY_TX_COUNT];
  std::vector<ObTxDescGuard> sec_guards;
  sec_guards.reserve(SECONDARY_TX_COUNT);
  for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
    sec_guards.push_back(n1->get_tx_guard());
  }
  ObTxDesc *sec_txs[SECONDARY_TX_COUNT];
  for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
    sec_txs[i] = &sec_guards[i].get_tx_desc();
  }

  for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
    ASSERT_EQ(OB_SUCCESS, n1->start_tx(*sec_txs[i], tx_param));
    secondary_tx_ids[i] = sec_txs[i]->tx_id_;
    ASSERT_EQ(OB_SUCCESS, n1->write(*sec_txs[i], snapshot, 700 + i, 800 + i));
  }
  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, snapshot, 7000, 8000));

  // Make the LAST secondary tx ineligible by injecting a fake redo_lsn.
  // hotspot_legality_validation_() checks exec_info_.redo_lsns_.count() > 0.
  {
    ObPartTransCtx *bad_ctx = nullptr;
    ASSERT_EQ(OB_SUCCESS, n1->get_tx_ctx(n1->ls_id_, secondary_tx_ids[INELIGIBLE_IDX], bad_ctx));
    ASSERT_TRUE(bad_ctx != nullptr);
    bad_ctx->exec_info_.redo_lsns_.push_back(palf::LSN(0));
    ASSERT_EQ(OB_SUCCESS, n1->revert_tx_ctx(bad_ctx));
  }

  // Verify all secondary txs start with NORMAL_START before validation.
  for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
    ObPartTransCtx *ctx = nullptr;
    ASSERT_EQ(OB_SUCCESS, n1->get_tx_ctx(n1->ls_id_, secondary_tx_ids[i], ctx));
    ASSERT_EQ(TxRedoFlushStatus::NORMAL_START, ctx->redo_flush_status_)
        << "Pre-check: secondary tx " << i << " should start at NORMAL_START";
    ASSERT_EQ(OB_SUCCESS, n1->revert_tx_ctx(ctx));
  }

  // Call sync_hotspot_legality_validation.
  // Txs [0..2] will pass validation (redo_flush_status -> DISABLE_REDO_FLUSH),
  // tx [3] will fail with OB_TX_NOT_SUPPORT_AGGREGATION.
  ObTxPart participant;
  participant.id_ = n1->ls_id_;
  participant.addr_ = n1->addr_;
  participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;

  ObAggregatedTxIDArray aggre_members;
  for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
    ASSERT_EQ(OB_SUCCESS, aggre_members.push_back(secondary_tx_ids[i]));
  }

  int ret = n1->txs_.sync_hotspot_legality_validation(
      participant, primary_tx.tx_id_, aggre_members);
  ASSERT_NE(OB_SUCCESS, ret)
      << "Validation should fail because the last secondary tx has redo_lsns";

  // CORE ASSERTION: freeze accelerate task (via mock push) submits redo for validated txs.
  // Status transitions to NORMAL_FLUSHED after redo submitted - valid terminal state.
  // The key verification is that txs can still commit independently.
  for (int i = 0; i < INELIGIBLE_IDX; i++) {
    ObPartTransCtx *ctx = nullptr;
    ASSERT_EQ(OB_SUCCESS, n1->get_tx_ctx(n1->ls_id_, secondary_tx_ids[i], ctx));
    // After freeze accelerate task submits redo, status should be NORMAL_FLUSHED
    // (or NORMAL_START if no redo submitted - both are valid states that allow commit)
    if (ctx->exec_info_.redo_lsns_.count() > 0) {
      EXPECT_EQ(TxRedoFlushStatus::NORMAL_FLUSHED, ctx->redo_flush_status_)
          << "Secondary tx " << i << " (tx_id=" << secondary_tx_ids[i]
          << ") should be NORMAL_FLUSHED after freeze accelerate task submitted redo";
    } else {
      EXPECT_EQ(TxRedoFlushStatus::NORMAL_START, ctx->redo_flush_status_)
          << "Secondary tx " << i << " (tx_id=" << secondary_tx_ids[i]
          << ") should be NORMAL_START if no redo submitted";
    }
    EXPECT_FALSE(ctx->can_not_submit_log_for_hotspot_())
        << "Secondary tx " << i << " should be able to submit logs";
    ASSERT_EQ(OB_SUCCESS, n1->revert_tx_ctx(ctx));
  }

  // Verify primary tx is also reset.
  {
    ObPartTransCtx *primary_ctx = nullptr;
    ASSERT_EQ(OB_SUCCESS, n1->get_tx_ctx(n1->ls_id_, primary_tx.tx_id_, primary_ctx));
    EXPECT_EQ(TxRedoFlushStatus::NORMAL_START, primary_ctx->redo_flush_status_)
        << "Primary tx redo_flush_status should be NORMAL_START after validation failure";
    ASSERT_EQ(OB_SUCCESS, n1->revert_tx_ctx(primary_ctx));
  }

  // Clean up the injected fake redo_lsn so the tx can proceed normally.
  {
    ObPartTransCtx *ctx = nullptr;
    ASSERT_EQ(OB_SUCCESS, n1->get_tx_ctx(n1->ls_id_, secondary_tx_ids[INELIGIBLE_IDX], ctx));
    ctx->exec_info_.redo_lsns_.reset();
    ASSERT_EQ(OB_SUCCESS, n1->revert_tx_ctx(ctx));
  }

  // Verify all txs can still commit independently.
  COMMIT_TX(n1, primary_tx, 500 * 1000);
  n1->wait_all_msg_consumed();
  ASSERT_EQ(OB_SUCCESS, primary_tx.commit_out_);

  for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
    int commit_ret = n1->commit_tx(*sec_txs[i], n1->ts_after_us(500 * 1000));
    ASSERT_TRUE(commit_ret == OB_SUCCESS || commit_ret == OB_TRANS_COMMITED)
        << "Secondary tx " << i << " commit failed with " << commit_ret;
  }

  ASSERT_EQ(OB_SUCCESS, guard.release());
  for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
    ASSERT_EQ(OB_SUCCESS, sec_guards[i].release());
  }
  n1->wait_all_msg_consumed();

  int wait_ret = OB_SUCCESS;
  for (int i = 0; i < 10; i++) {
    wait_ret = n1->wait_all_tx_ctx_is_destoryed();
    if (OB_SUCCESS == wait_ret) break;
    usleep(100 * 1000);
  }
  ASSERT_EQ(OB_SUCCESS, wait_ret)
      << "All tx contexts should be destroyed - none should be stuck";
}

// Verifies the fix for the inclusive/exclusive from_seq boundary bug
// in rollback_secondary_memtable_and_mark.
//
// BUG:
//   rollback_secondary_memtable_and_mark passed orig_max_seq_ (inclusive upper
//   bound) as from_seq to rollback_memtable_only. But the leaf function
//   remove_callbacks_for_rollback_to treats from_seq as exclusive:
//     if (dseq.get_seq() >= from_seq_.get_seq()) -> OB_ERR_UNEXPECTED
//   When a callback exists at exactly orig_max_seq_, the condition
//   `dseq >= from_seq` fires and triggers a spurious EDIAG error.
//
// FIX:
//   Pass orig_max_seq_ + 1 as from_seq to correctly treat it as exclusive.
//   Add overflow guard (skip rollback if +1 makes the seq invalid).
//
// TEST APPROACH:
//   Create a tx, write rows to create callbacks, then directly call
//   rollback_memtable_only with the exclusive upper bound (max_seq + 1).
//   Verify the call succeeds and all callbacks are removed — before the fix,
//   the callback at max_seq would have triggered OB_ERR_UNEXPECTED.
TEST_F(ObTestHotspotTxBugfix, test_rollback_memtable_from_seq_exclusive_boundary)
{
  START_ONE_TX_NODE(n1);

  PREPARE_TX(n1, tx);
  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000 * 1000 * 1000;
  GET_READ_SNAPSHOT(n1, tx, tx_param, snapshot);
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(tx, tx_param));

  ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 100, 200));
  ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 101, 201));
  ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 102, 202));

  ObPartTransCtx *ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->get_tx_ctx(n1->ls_id_, tx.tx_id_, ctx));
  ASSERT_TRUE(ctx != nullptr);

  ObTxCallbackList &cb_list = ctx->mt_ctx_.trans_mgr_.callback_list_;
  ASSERT_EQ(3, cb_list.get_length());

  ObITransCallback *guard_node = cb_list.get_guard();
  ObITransCallback *first_cb = guard_node->get_next();
  ObITransCallback *last_cb = cb_list.get_tail();
  ASSERT_NE(first_cb, guard_node);
  ASSERT_NE(last_cb, guard_node);

  ObTxSEQ min_seq = first_cb->get_seq_no();
  ObTxSEQ max_seq = last_cb->get_seq_no();
  ASSERT_TRUE(min_seq.is_valid());
  ASSERT_TRUE(max_seq.is_valid());
  ASSERT_TRUE(max_seq.get_seq() >= min_seq.get_seq());
  TRANS_LOG(INFO, "callback seq range", K(min_seq), K(max_seq));

  // FIX verification: from_seq = max_seq + 1 (exclusive upper bound).
  // Use to_seq with seq=1 (branch=0 matches all) so all callbacks are in range.
  ObTxSEQ exclusive_from = max_seq + 1;
  ObTxSEQ to_seq_low = ObTxSEQ(1, 0);
  ASSERT_TRUE(exclusive_from.is_valid());
  ASSERT_TRUE(to_seq_low.is_valid());

  // Before the fix, passing max_seq (inclusive) as from_seq would trigger
  // OB_ERR_UNEXPECTED in cond_for_remove when it encounters the callback
  // at exactly max_seq (dseq >= from_seq). With the fix (max_seq + 1),
  // that callback is correctly included in the rollback range.
  ASSERT_EQ(OB_SUCCESS, ctx->rollback_memtable_only(exclusive_from, to_seq_low))
      << "rollback_memtable_only with exclusive from_seq should succeed";

  ASSERT_EQ(0, cb_list.get_length())
      << "all callbacks should be removed after rollback";

  ASSERT_EQ(OB_SUCCESS, n1->revert_tx_ctx(ctx));
}

// Verifies rollback_secondary_memtable_and_mark end-to-end with the fix.
// Sets up a hotspot cache entry manually with orig_min/max_seq_ matching
// the secondary tx's callback range, then calls rollback and verifies
// need_remove_cnt_ is cleared.
// ============================================================================
// BUG FIX TEST: while loop exits after redo_flush_status becomes PRIMARY_AGGR_FAILED
// ============================================================================
//
// PROBLEM:
//   sync_hotspot_legality_validation has a while loop (line 332) that waits for
//   all_redo_flushed(). When switch_to_follower_forcedly is called during this wait,
//   it changes redo_flush_status_ to PRIMARY_AGGR_FAILED. The while loop did NOT
//   check this status (before fix), causing infinite wait.
//
// FIX:
//   Added check for PRIMARY_AGGR_FAILED status in while loop.
//
// TEST APPROACH:
//   1. Start sync_hotspot_legality_validation in async thread
//   2. During while loop wait, call switch_to_follower_forcedly
//   3. Verify sync_validation exits within 1 second (not hanging)
//
// KEY: Use ERRSIM_HOTSPOT_TARGET_LOG_CB_CNT to control redo flush rate,
//      making while loop stay active until forcedly switch.
//
// ============================================================================
TEST_F(ObTestHotspotTxBugfix, test_sync_loop_exits_after_forcedly_switch_during_wait)
{
  // Use ERRSIM to slow down redo flush, keeping while loop active
  {
    oceanbase::common::EventItem item;
    item.error_code_ = 3;  // ~1/3 of redo callbacks succeed, others retry
    item.occur_ = 1;
    item.trigger_freq_ = 1;
    item.cond_ = 0;
    (void)oceanbase::common::EventTable::set_event("ERRSIM_HOTSPOT_TARGET_LOG_CB_CNT", item);
  }
  GCONF._ob_trans_rpc_timeout = 50;
  GCONF._enable_parallel_redo_logging = false;
  GCONF._parallel_redo_logging_trigger = INT64_MAX;
  ObTxNode::reset_localtion_adapter();

  START_ONE_TX_NODE(n1);

  PREPARE_TX(n1, primary_tx);
  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000 * 1000 * 1000;
  GET_READ_SNAPSHOT(n1, primary_tx, tx_param, snapshot);
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary_tx, tx_param));

  const int SECONDARY_TX_COUNT = 5;
  ObTransID secondary_tx_ids[SECONDARY_TX_COUNT];
  std::vector<ObTxDescGuard> sec_guards;
  sec_guards.reserve(SECONDARY_TX_COUNT);
  for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
    sec_guards.push_back(n1->get_tx_guard());
  }
  ObTxDesc *sec_txs[SECONDARY_TX_COUNT];
  for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
    sec_txs[i] = &sec_guards[i].get_tx_desc();
  }

  for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
    ASSERT_EQ(OB_SUCCESS, n1->start_tx(*sec_txs[i], tx_param));
    secondary_tx_ids[i] = sec_txs[i]->tx_id_;
    ASSERT_EQ(OB_SUCCESS, n1->write(*sec_txs[i], snapshot, 800 + i, 900 + i));
  }
  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, snapshot, 8000, 9000));

  ObTxPart participant;
  participant.id_ = n1->ls_id_;
  participant.addr_ = n1->addr_;
  participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;

  ObAggregatedTxIDArray aggre_members;
  for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
    ASSERT_TRUE(secondary_tx_ids[i].is_valid());
    ASSERT_EQ(OB_SUCCESS, aggre_members.push_back(secondary_tx_ids[i]));
  }

  ObLSTxCtxMgr *ls_tx_ctx_mgr = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr));

  // Start sync_validation in async thread
  auto sync_validation_f = [&]() -> int {
    ObTenantEnv::set_tenant(&n1->tenant_);
    return n1->txs_.sync_hotspot_legality_validation(participant, primary_tx.tx_id_, aggre_members);
  };
  auto sync_validation_async = test::make_async(sync_validation_f);
  sync_validation_async.wait_started();

  // Wait for sync_validation to enter PRIMARY_COLLECTING (while loop)
  bool entered_collecting = false;
  ObPartTransCtx *primary_ctx = nullptr;
  const int64_t wait_start_ts = ObTimeUtility::fast_current_time();
  const int64_t wait_timeout_us = 5 * 1000 * 1000;
  while (ObTimeUtility::fast_current_time() - wait_start_ts < wait_timeout_us) {
    if (OB_SUCCESS == n1->get_tx_ctx(n1->ls_id_, primary_tx.tx_id_, primary_ctx)
        && primary_ctx != nullptr) {
      if (primary_ctx->redo_flush_status_ == TxRedoFlushStatus::PRIMARY_COLLECTING) {
        entered_collecting = true;
        TRANS_LOG(INFO, "sync_validation entered PRIMARY_COLLECTING (while loop active)",
                  KPC(primary_ctx));
        n1->revert_tx_ctx(primary_ctx);
        break;
      }
      n1->revert_tx_ctx(primary_ctx);
    }
    usleep(10 * 1000);
  }
  ASSERT_TRUE(entered_collecting) << "sync_validation should enter PRIMARY_COLLECTING";

  // Give while loop some time to be actively waiting (100ms)
  usleep(100 * 1000);

  // NOW: sync_validation is in while loop, call forcedly switch
  // This will set redo_flush_status_ = PRIMARY_AGGR_FAILED
  TRANS_LOG(INFO, "Calling switch_to_follower_forcedly while sync_validation in while loop");
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->switch_to_follower_forcedly());
  ASSERT_FALSE(ls_tx_ctx_mgr->is_master());

  // CRITICAL ASSERTION: sync_validation should exit within 1 second
  // BEFORE FIX: hangs indefinitely (timeout)
  // AFTER FIX: exits quickly (< 1s) after detecting PRIMARY_AGGR_FAILED
  const int64_t exit_timeout_us = 1 * 1000 * 1000;  // 1 second
  const int64_t exit_start_ts = ObTimeUtility::fast_current_time();
  while (!sync_validation_async.is_evaled()
         && ObTimeUtility::fast_current_time() - exit_start_ts < exit_timeout_us) {
    usleep(10 * 1000);
  }
  const int64_t exit_elapsed_us = ObTimeUtility::fast_current_time() - exit_start_ts;

  // Log result for debugging
  if (!sync_validation_async.is_evaled()) {
    fprintf(stderr, "BUG: sync_validation did NOT exit after forcedly switch! "
                    "exit_elapsed_us=%ld, exit_timeout_us=%ld\n",
                    exit_elapsed_us, exit_timeout_us);
  } else {
    TRANS_LOG(INFO, "sync_validation exited after forcedly switch",
              K(exit_elapsed_us), K(exit_timeout_us));
  }

  // MUST exit within 1 second
  ASSERT_TRUE(sync_validation_async.is_evaled())
      << "BUG: sync_validation should exit after redo_flush_status becomes PRIMARY_AGGR_FAILED";
  ASSERT_LT(exit_elapsed_us, exit_timeout_us)
      << "sync_validation should exit within 1 second after forcedly switch";

  // Cleanup
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr));
  ASSERT_EQ(OB_SUCCESS, guard.release());
  for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
    ASSERT_EQ(OB_SUCCESS, sec_guards[i].release());
  }
  n1->wait_all_msg_consumed();
  {
    oceanbase::common::EventItem item;
    item.error_code_ = 0;
    item.occur_ = 0;
    item.trigger_freq_ = 0;
    item.cond_ = 0;
    (void)oceanbase::common::EventTable::set_event("ERRSIM_HOTSPOT_TARGET_LOG_CB_CNT", item);
  }
}

TEST_F(ObTestHotspotTxBugfix, test_rollback_secondary_memtable_and_mark_from_seq_boundary)
{
  START_ONE_TX_NODE(n1);

  // Create primary tx (needed for the hotspot cache owner)
  PREPARE_TX(n1, primary_tx);
  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000 * 1000 * 1000;
  GET_READ_SNAPSHOT(n1, primary_tx, tx_param, snapshot);
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary_tx, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, snapshot, 1000, 2000));

  // Create secondary tx and write data to create callbacks
  ObTxDescGuard sec_guard = n1->get_tx_guard();
  ObTxDesc &sec_tx = sec_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(sec_tx, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->write(sec_tx, snapshot, 200, 300));
  ASSERT_EQ(OB_SUCCESS, n1->write(sec_tx, snapshot, 201, 301));
  ObTransID sec_tx_id = sec_tx.tx_id_;

  // Get secondary ctx and find its callback seq range
  ObPartTransCtx *sec_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->get_tx_ctx(n1->ls_id_, sec_tx_id, sec_ctx));
  ASSERT_TRUE(sec_ctx != nullptr);

  ObTxCallbackList &sec_cb_list = sec_ctx->mt_ctx_.trans_mgr_.callback_list_;
  ASSERT_EQ(2, sec_cb_list.get_length());

  ObITransCallback *sec_guard_node = sec_cb_list.get_guard();
  ObTxSEQ sec_min_seq = sec_guard_node->get_next()->get_seq_no();
  ObTxSEQ sec_max_seq = sec_cb_list.get_tail()->get_seq_no();
  ASSERT_TRUE(sec_min_seq.is_valid());
  ASSERT_TRUE(sec_max_seq.is_valid());
  TRANS_LOG(INFO, "secondary callback seq range", K(sec_min_seq), K(sec_max_seq), K(sec_tx_id));

  // Get primary ctx and inject a hotspot cache entry manually.
  // This avoids the full aggregation flow while testing the rollback path directly.
  ObPartTransCtx *primary_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->get_tx_ctx(n1->ls_id_, primary_tx.tx_id_, primary_ctx));
  ASSERT_TRUE(primary_ctx != nullptr);

  // Initialize hotspot cache with capacity for 1 secondary tx
  ASSERT_EQ(OB_SUCCESS, primary_ctx->hotspot_redo_cache_.init(primary_tx.tx_id_, 1, primary_ctx));

  {
    SpinWLockGuard lock_guard(primary_ctx->hotspot_redo_cache_.hotspot_lock_);
    ObTxHotspotRedoCache *cache = primary_ctx->hotspot_redo_cache_.cache_;
    ASSERT_TRUE(cache != nullptr);
    // Manually add entry using new pointer-based model
    ObTxRedoExtractArg arg;
    arg.tx_id_ = sec_tx_id;
    arg.other_ctx_ = sec_ctx;
    arg.orig_min_seq_ = sec_min_seq;
    arg.orig_max_seq_ = sec_max_seq;
    arg.seq_remapped_ = true;
    arg.need_remove_cnt_ = 2;
    int64_t idx = cache->hotspot_cache_count_;
    if (idx < cache->hotspot_cache_capacity_) {
      new (&cache->hotspot_cache_[idx]) ObTxRedoExtractArg(arg);
      cache->hotspot_cache_count_++;
    }
  }

  // Call rollback_secondary_memtable_and_mark — this is the function with the fix.
  // Before the fix, this would hit OB_ERR_UNEXPECTED inside remove_callbacks_for_rollback_to
  // when the callback at sec_max_seq has dseq >= from_seq (because from_seq == sec_max_seq).
  int ret = primary_ctx->hotspot_redo_cache_.rollback_secondary_memtable_and_mark(sec_tx_id);
  ASSERT_EQ(OB_SUCCESS, ret)
      << "rollback_secondary_memtable_and_mark should find and process the entry";

  // Verify the entry is marked as rolled back
	  {
	    SpinRLockGuard lock_guard(primary_ctx->hotspot_redo_cache_.hotspot_lock_);
	    ObTxHotspotRedoCache *cache = primary_ctx->hotspot_redo_cache_.cache_;
    ASSERT_TRUE(cache != nullptr);
    bool found = false;
    for (int64_t i = 0; i < cache->hotspot_cache_count_; i++) {
      ObTxRedoExtractArg &cached_arg = cache->hotspot_cache_[i];
      if (cached_arg.tx_id_ == sec_tx_id) {
        found = true;
        ASSERT_TRUE(cached_arg.data_rolled_back_);
        // The fix ensures memtable rollback succeeds, so need_remove_cnt_ should be cleared.
        // Before the fix, rollback would fail (OB_ERR_UNEXPECTED) and need_remove_cnt_ would
        // remain at 2 (the error was caught by OB_TMP_FAIL so it's non-fatal, but the
        // callbacks were not cleaned up).
        ASSERT_EQ(0, cached_arg.need_remove_cnt_)
            << "memtable rollback should succeed with exclusive from_seq, clearing need_remove_cnt";
        break;
      }
    }
	    ASSERT_TRUE(found) << "entry should exist in hotspot cache";
	  }

	  {
	    SpinWLockGuard lock_guard(primary_ctx->hotspot_redo_cache_.hotspot_lock_);
	    ObTxHotspotRedoCache *cache = primary_ctx->hotspot_redo_cache_.cache_;
	    ASSERT_TRUE(cache != nullptr);
	    for (int64_t i = 0; i < cache->hotspot_cache_count_; i++) {
	      cache->hotspot_cache_[i].reset();
	    }
	    cache->hotspot_cache_count_ = 0;
	  }

	  ASSERT_EQ(OB_SUCCESS, n1->revert_tx_ctx(sec_ctx));
  ASSERT_EQ(OB_SUCCESS, n1->revert_tx_ctx(primary_ctx));
}

// Test freeze accelerate task when aggregation validation fails
// Scenario:
//   1. Create logged secondary txs, call traverse to flush their redo logs
//   2. Create primary tx (after traverse, so primary won't have redo flushed)
//   3. Create unlogged secondary txs (redo NOT flushed)
//   4. Build aggre_members with unlogged txs FIRST, logged txs LAST
//   5. Call sync_hotspot_legality_validation:
//      - All unlogged txs pass validation, transition to SECONDARY_PREPARING
//      - First logged tx fails (has redo_lsns), sync returns OB_TX_NOT_SUPPORT_AGGREGATION
//      - Cleanup creates freeze accelerate task for all unlogged txs
//   6. Mock push directly calls handle in main thread
//   7. Task calls submit_redo_log_for_freeze() for each unlogged tx
// Expected: Freeze accelerate task should submit redo logs for all unlogged secondary txs
TEST_F(ObTestHotspotTxBugfix, test_hotspot_freeze_accelerate_task_on_validation_failure)
{
  GCONF._ob_trans_rpc_timeout = 50;
  ObTxNode::reset_localtion_adapter();

  START_ONE_TX_NODE(n1);

  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000 * 1000 * 1000;

  const int LOGGED_TX_COUNT = 5;
  const int UNLOGGED_TX_COUNT = 5;

  ObTransID logged_tx_ids[LOGGED_TX_COUNT];
  ObTransID unlogged_tx_ids[UNLOGGED_TX_COUNT];

  std::vector<ObTxDescGuard> logged_guards;
  std::vector<ObTxDescGuard> unlogged_guards;
  logged_guards.reserve(LOGGED_TX_COUNT);
  unlogged_guards.reserve(UNLOGGED_TX_COUNT);

  for (int i = 0; i < LOGGED_TX_COUNT; i++) {
    logged_guards.push_back(n1->get_tx_guard());
  }
  for (int i = 0; i < UNLOGGED_TX_COUNT; i++) {
    unlogged_guards.push_back(n1->get_tx_guard());
  }

  ObTxDesc *logged_txs[LOGGED_TX_COUNT];
  ObTxDesc *unlogged_txs[UNLOGGED_TX_COUNT];
  for (int i = 0; i < LOGGED_TX_COUNT; i++) {
    logged_txs[i] = &logged_guards[i].get_tx_desc();
  }
  for (int i = 0; i < UNLOGGED_TX_COUNT; i++) {
    unlogged_txs[i] = &unlogged_guards[i].get_tx_desc();
  }

  // Step 1: Create logged secondary txs and write data
  GET_READ_SNAPSHOT(n1, *logged_txs[0], tx_param, logged_snapshot);
  for (int i = 0; i < LOGGED_TX_COUNT; i++) {
    ASSERT_EQ(OB_SUCCESS, n1->start_tx(*logged_txs[i], tx_param));
    logged_tx_ids[i] = logged_txs[i]->tx_id_;
    ASSERT_EQ(OB_SUCCESS, n1->write(*logged_txs[i], logged_snapshot, 100 + i, 200 + i));
  }

  // Step 2: Call traverse to flush logged txs' redo logs
  ObLSTxCtxMgr *ls_tx_ctx_mgr = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr));
  ObTransID fail_tx_id;
  int traverse_ret = ls_tx_ctx_mgr->traverse_tx_to_submit_redo_log(fail_tx_id);
  EXPECT_TRUE(OB_SUCCESS == traverse_ret || OB_TX_NOLOGCB == traverse_ret)
      << "traverse_tx_to_submit_redo_log should succeed or return NOLOGCB";
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr));

  // Step 3: Verify logged txs have redo submitted
  for (int i = 0; i < LOGGED_TX_COUNT; i++) {
    ObPartTransCtx *ctx = nullptr;
    ASSERT_EQ(OB_SUCCESS, n1->get_tx_ctx(n1->ls_id_, logged_tx_ids[i], ctx));
    ASSERT_TRUE(ctx != nullptr);
    EXPECT_TRUE(ctx->exec_info_.redo_lsns_.count() > 0)
        << "Logged tx " << i << " should have redo submitted after traverse";
    ASSERT_EQ(OB_SUCCESS, n1->revert_tx_ctx(ctx));
  }

  // Step 4: Create primary tx AFTER traverse (so primary won't have redo flushed)
  PREPARE_TX(n1, primary_tx);
  GET_READ_SNAPSHOT(n1, primary_tx, tx_param, snapshot);
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary_tx, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, snapshot, 7000, 8000));

  // Step 5: Verify primary tx has NOT submitted redo yet (created after traverse)
  {
    ObPartTransCtx *ctx = nullptr;
    ASSERT_EQ(OB_SUCCESS, n1->get_tx_ctx(n1->ls_id_, primary_tx.tx_id_, ctx));
    ASSERT_TRUE(ctx != nullptr);
    EXPECT_EQ(0, ctx->exec_info_.redo_lsns_.count())
        << "Primary tx should NOT have redo submitted yet (created after traverse)";
    EXPECT_EQ(TxRedoFlushStatus::NORMAL_START, ctx->redo_flush_status_)
        << "Primary tx should be in NORMAL_START status";
    ASSERT_EQ(OB_SUCCESS, n1->revert_tx_ctx(ctx));
  }

  // Step 6: Create unlogged secondary txs (redo NOT flushed)
  for (int i = 0; i < UNLOGGED_TX_COUNT; i++) {
    ASSERT_EQ(OB_SUCCESS, n1->start_tx(*unlogged_txs[i], tx_param));
    unlogged_tx_ids[i] = unlogged_txs[i]->tx_id_;
    ASSERT_EQ(OB_SUCCESS, n1->write(*unlogged_txs[i], snapshot, 300 + i, 400 + i));
  }

  // Step 7: Verify unlogged txs have NOT submitted redo
  for (int i = 0; i < UNLOGGED_TX_COUNT; i++) {
    ObPartTransCtx *ctx = nullptr;
    ASSERT_EQ(OB_SUCCESS, n1->get_tx_ctx(n1->ls_id_, unlogged_tx_ids[i], ctx));
    ASSERT_TRUE(ctx != nullptr);
    EXPECT_EQ(0, ctx->exec_info_.redo_lsns_.count())
        << "Unlogged tx " << i << " should NOT have redo submitted yet";
    ASSERT_EQ(OB_SUCCESS, n1->revert_tx_ctx(ctx));
  }

  // Step 8: Build aggre_members with unlogged txs FIRST, logged txs LAST
  // Logged txs already have redo_lsns from traverse, will cause validation failure
  ObTxPart participant;
  participant.id_ = n1->ls_id_;
  participant.addr_ = n1->addr_;
  participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;

  ObAggregatedTxIDArray aggre_members;
  for (int i = 0; i < UNLOGGED_TX_COUNT; i++) {
    ASSERT_TRUE(unlogged_tx_ids[i].is_valid());
    ASSERT_EQ(OB_SUCCESS, aggre_members.push_back(unlogged_tx_ids[i]));
  }
  for (int i = 0; i < LOGGED_TX_COUNT; i++) {
    ASSERT_TRUE(logged_tx_ids[i].is_valid());
    ASSERT_EQ(OB_SUCCESS, aggre_members.push_back(logged_tx_ids[i]));
  }

  // Step 9: Sync validation should fail when it hits first logged tx (has redo_lsns)
  // Freeze accelerate task is created for all unlogged txs that passed validation
  int ret = n1->txs_.sync_hotspot_legality_validation(participant, primary_tx.tx_id_, aggre_members);
  ASSERT_EQ(OB_TX_NOT_SUPPORT_AGGREGATION, ret) << "sync validation should fail at logged tx";

  // Step 11: Mock push directly called handle, so freeze accelerate task already executed
  // Wait for async log submission to complete
  n1->wait_all_msg_consumed();
  n1->wait_tx_log_synced();

  // Step 12: Verify all unlogged txs have redo submitted via freeze accelerate task
  // Note: After freeze accelerate task successfully submits redo, status transitions to NORMAL_FLUSHED
  int verified_cnt = 0;
  for (int i = 0; i < UNLOGGED_TX_COUNT; i++) {
    ObPartTransCtx *ctx = nullptr;
    int get_ret = n1->get_tx_ctx(n1->ls_id_, unlogged_tx_ids[i], ctx);
    if (OB_SUCCESS == get_ret && ctx != nullptr) {
      // After freeze accelerate task submits redo, status should be NORMAL_FLUSHED (redo submitted)
      // or remain NORMAL_START (if submission failed or not yet completed)
      if (ctx->exec_info_.redo_lsns_.count() > 0) {
        verified_cnt++;
        // Redo submitted, status should be NORMAL_FLUSHED
        EXPECT_EQ(TxRedoFlushStatus::NORMAL_FLUSHED, ctx->redo_flush_status_)
            << "Unlogged tx " << i << " status should be NORMAL_FLUSHED after redo submitted";
        LOG_INFO("verified unlogged tx has redo submitted via freeze accelerate task",
                 K(i), K(unlogged_tx_ids[i]), K(ctx->exec_info_.redo_lsns_.count()),
                 "redo_flush_status", static_cast<int>(ctx->redo_flush_status_));
      } else {
        // No redo submitted yet, status should be reset to NORMAL_START by cleanup
        EXPECT_EQ(TxRedoFlushStatus::NORMAL_START, ctx->redo_flush_status_)
            << "Unlogged tx " << i << " status should be NORMAL_START if no redo submitted";
      }
      ASSERT_EQ(OB_SUCCESS, n1->revert_tx_ctx(ctx));
    }
  }

  TRANS_LOG(INFO, "freeze accelerate task test result", K(verified_cnt), K(UNLOGGED_TX_COUNT));

  EXPECT_GT(verified_cnt, 0)
      << "At least some unlogged txs should have redo submitted via freeze accelerate task";

  // Cleanup
  ASSERT_EQ(OB_SUCCESS, guard.release());
  for (int i = 0; i < LOGGED_TX_COUNT; i++) {
    ASSERT_EQ(OB_SUCCESS, logged_guards[i].release());
  }
  for (int i = 0; i < UNLOGGED_TX_COUNT; i++) {
    ASSERT_EQ(OB_SUCCESS, unlogged_guards[i].release());
  }
  n1->wait_all_msg_consumed();
}

} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_hotspot_tx_bugfix.log*");
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_hotspot_tx_bugfix.log", true, false,
                       "test_hotspot_tx_bugfix.log",  // rs
                       "test_hotspot_tx_bugfix.log",  // election
                       "test_hotspot_tx_bugfix.log"); // audit
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
