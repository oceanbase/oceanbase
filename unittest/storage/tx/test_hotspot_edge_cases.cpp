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

// Unit tests for hotspot edge cases and memory management
// Covers:
//   1. Memory allocation failure handling (invalid argument)
//   2. Edge conditions and boundary cases

#include <gtest/gtest.h>
#define USING_LOG_PREFIX TRANS
#define private public
#define protected public

#include "storage/tx/ob_tx_hotspot_define.h"
#include "storage/tx/ob_tx_hotspot_helper.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/tx/ob_tx_log_cb_define.h"
#include "storage/tx/ob_trans_submit_log_cb.h"
#include "storage/tx/ob_tx_data_define.h"
#include "storage/tx/ob_gts_rpc.h"
#include "share/ob_errno.h"
#include "lib/utility/utility.h"
#include "lib/allocator/ob_malloc.h"

namespace oceanbase
{
using namespace ::testing;
using namespace transaction;

class ObTestHotspotEdgeCases : public ::testing::Test
{
public:
  virtual void SetUp() override
  {
    oceanbase::ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
  }
  virtual void TearDown() override {}
};

class MockHotspotGtsRequestRpc : public ObIGtsRequestRpc
{
public:
  MockHotspotGtsRequestRpc() : post_count_(0), last_range_size_(0) {}
  virtual ~MockHotspotGtsRequestRpc() {}
  virtual int start() override { return OB_SUCCESS; }
  virtual int stop() override { return OB_SUCCESS; }
  virtual int wait() override { return OB_SUCCESS; }
  virtual void destroy() override {}
  virtual int post(const uint64_t tenant_id,
                   const common::ObAddr &server,
                   const ObGtsRequest &msg) override
  {
    UNUSED(tenant_id);
    UNUSED(server);
    ++post_count_;
    last_range_size_ = msg.range_size_;
    return OB_SUCCESS;
  }
  int64_t post_count_;
  int64_t last_range_size_;
};

class MockHotspotGtsLocationAdapter : public ObILocationAdapter
{
public:
  explicit MockHotspotGtsLocationAdapter(const common::ObAddr &leader) : leader_(leader) {}
  virtual ~MockHotspotGtsLocationAdapter() {}
  virtual int init(share::schema::ObMultiVersionSchemaService *schema_service,
                   share::ObLocationService *location_service) override
  {
    UNUSED(schema_service);
    UNUSED(location_service);
    return OB_SUCCESS;
  }
  virtual void destroy() override {}
  virtual int nonblock_get_leader(const int64_t cluster_id,
                                  const int64_t tenant_id,
                                  const share::ObLSID &ls_id,
                                  common::ObAddr &leader) override
  {
    UNUSED(cluster_id);
    UNUSED(tenant_id);
    UNUSED(ls_id);
    leader = leader_;
    return OB_SUCCESS;
  }
  virtual int nonblock_renew(const int64_t cluster_id,
                             const int64_t tenant_id,
                             const share::ObLSID &ls_id) override
  {
    UNUSED(cluster_id);
    UNUSED(tenant_id);
    UNUSED(ls_id);
    return OB_SUCCESS;
  }
  virtual int nonblock_get(const int64_t cluster_id,
                           const int64_t tenant_id,
                           const share::ObLSID &ls_id,
                           share::ObLSLocation &location) override
  {
    UNUSED(cluster_id);
    UNUSED(tenant_id);
    UNUSED(ls_id);
    UNUSED(location);
    return OB_NOT_SUPPORTED;
  }
private:
  common::ObAddr leader_;
};

// ============================================================================
// TC-01: Verify init() handles invalid count (count=0) gracefully
// ============================================================================
TEST_F(ObTestHotspotEdgeCases, test_init_invalid_count_handling)
{
  // This test verifies that init() properly handles invalid argument:
  // - Returns OB_INVALID_ARGUMENT when count <= 0

  TransModulePageAllocator allocator;
  ObTxHotspotRedoCacheHandle cache(allocator);

  ObTransID primary_id(1);
  int ret = cache.init(primary_id, 0, nullptr);
  EXPECT_EQ(OB_INVALID_ARGUMENT, ret);

  // core should remain nullptr
  EXPECT_TRUE(OB_ISNULL(cache.cache_));

  TRANS_LOG(INFO, "test_init_invalid_count_handling: verified invalid argument handling");
}

// ============================================================================
// TC-02: Verify check_initialized_() guards against null hotspot_cache_
// ============================================================================
TEST_F(ObTestHotspotEdgeCases, test_check_initialized_guards_null_cache)
{
  ObTxHotspotRedoCache cache;

  // Missing cache storage -> OB_NOT_INIT
  EXPECT_EQ(OB_NOT_INIT, cache.check_initialized_());

  TRANS_LOG(INFO, "test_check_initialized_guards_null_cache: verified null check");
}

// ============================================================================
// TC-03: Verify 47-bit seq boundary handling
// ============================================================================
TEST_F(ObTestHotspotEdgeCases, test_47bit_seq_boundary)
{
  // Verify MAX_SEQ_47BIT constant exists and is correct
  // 47-bit seq max = (1 << 47) - 1

  const uint64_t MAX_SEQ_47BIT = (1ULL << 47) - 1ULL;

  // ObTxSEQ should have valid boundary handling
  ObTxSEQ max_seq(MAX_SEQ_47BIT, 0);
  EXPECT_TRUE(max_seq.is_valid());

  TRANS_LOG(INFO, "test_47bit_seq_boundary: verified MAX_SEQ_47BIT", K(MAX_SEQ_47BIT));
}

// ============================================================================
// TC-04: Verify monitoring alert timestamp tracking
// ============================================================================
TEST_F(ObTestHotspotEdgeCases, test_busy_cbs_alert_timestamp)
{
  ObTxHotspotRedoCache cache;

  // Verify last_busy_cbs_alert_ts_ is initialized to 0
  EXPECT_EQ(0, cache.last_busy_cbs_alert_ts_);

  // After setting timestamp, it should be updated
  cache.last_busy_cbs_alert_ts_ = ObClockGenerator::getClock();

  // Verify timestamp is set
  EXPECT_TRUE(cache.last_busy_cbs_alert_ts_ > 0);

  TRANS_LOG(INFO, "test_busy_cbs_alert_timestamp: verified alert timestamp tracking",
            K(cache.last_busy_cbs_alert_ts_));
}

// ============================================================================
// TC-05: Verify empty handle semantics before hotspot cache initialization
// ============================================================================
TEST_F(ObTestHotspotEdgeCases, test_handle_empty_semantics)
{
  TransModulePageAllocator allocator;
  ObTxHotspotRedoCacheHandle cache(allocator);

  EXPECT_EQ(0, cache.get_hotspot_cache_count());
  EXPECT_EQ(0, cache.get_busy_cb_count());
  EXPECT_TRUE(cache.all_redo_flushed());
  EXPECT_TRUE(cache.all_redo_synced());
  EXPECT_TRUE(cache.all_redo_frozen_flushed());
  EXPECT_EQ(OB_SUCCESS, cache.reuse());
  EXPECT_EQ(OB_SUCCESS, cache.try_release_idle_log_cb(nullptr, false));

  TRANS_LOG(INFO, "test_handle_empty_semantics: verified empty handle behavior");
}

// ============================================================================
// TC-06: Verify primary_last_seq_no_ is safe before init and after core release
// ============================================================================
TEST_F(ObTestHotspotEdgeCases, test_handle_primary_last_seq_no_lifetime)
{
  TransModulePageAllocator allocator;
  ObTxHotspotRedoCacheHandle cache(allocator);
  const ObTxSEQ seq(123, 4);

  EXPECT_TRUE(cache.get_primary_last_seq_no() == ObTxSEQ(1, 0));
  cache.set_primary_last_seq_no(seq);
  EXPECT_TRUE(cache.get_primary_last_seq_no() == seq);
  EXPECT_EQ(OB_SUCCESS, cache.reuse());
  EXPECT_TRUE(cache.get_primary_last_seq_no() == ObTxSEQ(1, 0));

  TRANS_LOG(INFO, "test_handle_primary_last_seq_no_lifetime: verified handle-owned seq");
}

// ============================================================================
// TC-07: Verify late response task behavior after core release
// ============================================================================
TEST_F(ObTestHotspotEdgeCases, test_handle_late_response_task_returns_not_init)
{
  TransModulePageAllocator allocator;
  ObTxHotspotRedoCacheHandle cache(allocator);

  EXPECT_EQ(OB_NOT_INIT,
            cache.response_scheduler(OB_TRANS_KILLED, share::SCN::invalid_scn()));

  TRANS_LOG(INFO, "test_handle_late_response_task_returns_not_init: verified terminal late task");
}

// ============================================================================
// TC-08: Verify handle init allocates core and array, then reuse releases both
// ============================================================================
TEST_F(ObTestHotspotEdgeCases, test_handle_one_allocation_init_and_reuse)
{
  TransModulePageAllocator allocator;
  ObTxHotspotRedoCacheHandle cache(allocator);
  const ObTransID primary_id(20260604001);

  EXPECT_EQ(OB_SUCCESS, cache.init(primary_id, 2, nullptr));
  ASSERT_NE(nullptr, cache.cache_);
  EXPECT_NE(nullptr, cache.cache_->hotspot_cache_);
  EXPECT_EQ(2, cache.cache_->hotspot_cache_capacity_);
  EXPECT_EQ(0, cache.cache_->hotspot_cache_count_);

  EXPECT_EQ(OB_SUCCESS, cache.reuse());
  EXPECT_EQ(nullptr, cache.cache_);
  EXPECT_EQ(0, cache.get_hotspot_cache_count());

  TRANS_LOG(INFO, "test_handle_one_allocation_init_and_reuse: verified handle allocation lifecycle");
}

// ============================================================================
// TC-09: Verify insert overflow is rejected before writing past capacity
// ============================================================================
TEST_F(ObTestHotspotEdgeCases, test_handle_insert_capacity_overflow)
{
  TransModulePageAllocator allocator;
  ObTxHotspotRedoCacheHandle cache(allocator);
  const ObTransID primary_id(20260604002);

  ASSERT_EQ(OB_SUCCESS, cache.init(primary_id, 1, nullptr));
  ASSERT_NE(nullptr, cache.cache_);
  cache.cache_->hotspot_cache_count_ = cache.cache_->hotspot_cache_capacity_;

  EXPECT_EQ(OB_SIZE_OVERFLOW, cache.insert_into(nullptr));
  EXPECT_EQ(cache.cache_->hotspot_cache_capacity_, cache.cache_->hotspot_cache_count_);
  cache.cache_->hotspot_cache_count_ = 0;

  TRANS_LOG(INFO, "test_handle_insert_capacity_overflow: verified overflow guard");
}

// ============================================================================
// TC-10: Verify index-based operations reject invalid indexes before array access
// ============================================================================
TEST_F(ObTestHotspotEdgeCases, test_handle_invalid_index_rejection)
{
  TransModulePageAllocator allocator;
  ObTxHotspotRedoCacheHandle cache(allocator);
  const ObTransID primary_id(20260604003);
  bool need_fill_redo_buf = true;
  bool need_submit_log = true;
  int64_t need_remove_count = -1;

  ASSERT_EQ(OB_SUCCESS, cache.init(primary_id, 1, nullptr));
  ASSERT_NE(nullptr, cache.cache_);
  ASSERT_EQ(0, cache.cache_->hotspot_cache_count_);

  EXPECT_EQ(OB_EAGAIN, cache.check_status(-1, need_fill_redo_buf, need_submit_log));
  EXPECT_FALSE(need_fill_redo_buf);
  EXPECT_FALSE(need_submit_log);

  EXPECT_EQ(OB_INVALID_ARGUMENT, cache.check_status(0, need_fill_redo_buf, need_submit_log));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            cache.after_flush_hotspot_redo(0, share::SCN::min_scn(), OB_SUCCESS));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            cache.after_sync_hotspot_redo(0, true, share::SCN::min_scn()));
  EXPECT_EQ(OB_INVALID_ARGUMENT, cache.remove_synced_hotspot_redo(0, need_remove_count));

  TRANS_LOG(INFO, "test_handle_invalid_index_rejection: verified invalid index guards");
}

// ============================================================================
// TC-11: Verify reuse keeps core allocated when free/gap callback state remains
// ============================================================================
TEST_F(ObTestHotspotEdgeCases, test_handle_reuse_blocked_by_callback_state)
{
  TransModulePageAllocator allocator;
  ObTxHotspotRedoCacheHandle cache(allocator);
  const ObTransID primary_id(20260604004);

  ASSERT_EQ(OB_SUCCESS, cache.init(primary_id, 1, nullptr));
  ASSERT_NE(nullptr, cache.cache_);
  ObTxHotspotRedoCache *core = cache.cache_;

  ObTxLogCb free_cb;
  {
    SpinWLockGuard guard(cache.hotspot_lock_);
    core->free_hotspot_cbs_.add_last(&free_cb);
    core->all_log_cb_cnt_ = 1;
  }
  EXPECT_EQ(OB_EAGAIN, cache.reuse());
  EXPECT_EQ(core, cache.cache_);
  {
    SpinWLockGuard guard(cache.hotspot_lock_);
    EXPECT_NE(nullptr, core->free_hotspot_cbs_.remove(&free_cb));
    core->all_log_cb_cnt_ = 1;
  }
  EXPECT_EQ(OB_EAGAIN, cache.reuse());
  EXPECT_EQ(core, cache.cache_);
  {
    SpinWLockGuard guard(cache.hotspot_lock_);
    core->all_log_cb_cnt_ = 0;
  }
  EXPECT_EQ(OB_SUCCESS, cache.reuse());
  EXPECT_EQ(nullptr, cache.cache_);

  TRANS_LOG(INFO, "test_handle_reuse_blocked_by_callback_state: verified free and gap state");
}

// ============================================================================
// TC-12: Verify initialized handle still validates null primary ctx for CB release
// ============================================================================
TEST_F(ObTestHotspotEdgeCases, test_handle_release_idle_rejects_null_ctx_when_inited)
{
  TransModulePageAllocator allocator;
  ObTxHotspotRedoCacheHandle cache(allocator);
  const ObTransID primary_id(20260604005);

  ASSERT_EQ(OB_SUCCESS, cache.init(primary_id, 1, nullptr));
  EXPECT_EQ(OB_INVALID_ARGUMENT, cache.try_release_idle_log_cb(nullptr, false));

  TRANS_LOG(INFO, "test_handle_release_idle_rejects_null_ctx_when_inited: verified argument check");
}

// Regression: free-list membership models a CB reserved for retry after OB_BLOCK_FROZEN.
TEST_F(ObTestHotspotEdgeCases, test_normal_release_preserves_retry_free_cb)
{
  TransModulePageAllocator allocator;
  ObTxHotspotRedoCacheHandle cache(allocator);
  const ObTransID primary_id(20260604007);
  ObTxLogCb free_cb;
  ObPartTransCtx primary_ctx;

  ASSERT_EQ(OB_SUCCESS, cache.init(primary_id, 1, nullptr));
  ASSERT_NE(nullptr, cache.cache_);
  ObTxHotspotRedoCache *core = cache.cache_;
  {
    SpinWLockGuard guard(cache.hotspot_lock_);
    ASSERT_TRUE(core->free_hotspot_cbs_.add_last(&free_cb));
    core->all_log_cb_cnt_ = 1;
  }

  EXPECT_EQ(OB_EAGAIN, cache.try_release_idle_log_cb(&primary_ctx, false));
  {
    SpinRLockGuard guard(cache.hotspot_lock_);
    EXPECT_EQ(1, core->free_hotspot_cbs_.get_size());
    EXPECT_EQ(0, core->busy_hotspot_cbs_.get_size());
    EXPECT_EQ(0, core->idle_hotspot_cbs_.get_size());
    EXPECT_EQ(1, core->all_log_cb_cnt_);
  }

  ObTxLogCb *retry_cb = nullptr;
  EXPECT_EQ(OB_SUCCESS, cache.get_free_cb(retry_cb));
  EXPECT_EQ(&free_cb, retry_cb);
  {
    SpinWLockGuard guard(cache.hotspot_lock_);
    EXPECT_EQ(0, core->free_hotspot_cbs_.get_size());
    EXPECT_EQ(1, core->all_log_cb_cnt_);
    core->all_log_cb_cnt_ = 0;
  }
  EXPECT_EQ(OB_SUCCESS, cache.reuse());
}

// Force release abandons unsynced redo and returns free CBs to the primary context.
TEST_F(ObTestHotspotEdgeCases, test_force_release_returns_free_cb_to_primary)
{
  TransModulePageAllocator allocator;
  ObTxHotspotRedoCacheHandle cache(allocator);
  const ObTransID primary_id(20260604008);
  ObTxLogCb free_cb;
  ObPartTransCtx primary_ctx;

  ASSERT_EQ(OB_SUCCESS, cache.init(primary_id, 1, nullptr));
  ASSERT_NE(nullptr, cache.cache_);
  ObTxHotspotRedoCache *core = cache.cache_;
  {
    SpinWLockGuard guard(cache.hotspot_lock_);
    ASSERT_TRUE(core->free_hotspot_cbs_.add_last(&free_cb));
    core->all_log_cb_cnt_ = 1;
  }

  EXPECT_EQ(OB_SUCCESS, cache.try_release_idle_log_cb(&primary_ctx, true));
  {
    SpinRLockGuard guard(cache.hotspot_lock_);
    EXPECT_EQ(0, core->free_hotspot_cbs_.get_size());
    EXPECT_EQ(0, core->busy_hotspot_cbs_.get_size());
    EXPECT_EQ(0, core->idle_hotspot_cbs_.get_size());
    EXPECT_EQ(0, core->all_log_cb_cnt_);
  }
  {
    ObSpinLockGuard guard(primary_ctx.log_cb_lock_);
    EXPECT_EQ(1, primary_ctx.free_cbs_.get_size());
    EXPECT_EQ(&free_cb, primary_ctx.free_cbs_.remove_first());
  }
  EXPECT_EQ(OB_SUCCESS, cache.reuse());
}

// ============================================================================
// TC-13: Verify active refs block reuse and preserve the core
// ============================================================================
TEST_F(ObTestHotspotEdgeCases, test_handle_reuse_blocked_by_active_ref)
{
  TransModulePageAllocator allocator;
  ObTxHotspotRedoCacheHandle cache(allocator);
  const ObTransID primary_id(20260604006);

  ASSERT_EQ(OB_SUCCESS, cache.init(primary_id, 1, nullptr));
  ObTxHotspotRedoCache *core = cache.cache_;
  ASSERT_NE(nullptr, core);

  ATOMIC_INC(&cache.active_ref_cnt_);
  EXPECT_EQ(OB_EAGAIN, cache.reuse());
  EXPECT_EQ(core, cache.cache_);
  ATOMIC_DEC(&cache.active_ref_cnt_);

  EXPECT_EQ(OB_SUCCESS, cache.reuse());
  EXPECT_EQ(nullptr, cache.cache_);

  TRANS_LOG(INFO, "test_handle_reuse_blocked_by_active_ref: verified active ref guard");
}

// ============================================================================
// TC-14: Secondary prepare version is status-gated, idempotent, and monotonic
// ============================================================================
TEST_F(ObTestHotspotEdgeCases, test_secondary_prepare_version_is_monotonic)
{
  ObPartTransCtx secondary;
  ASSERT_EQ(OB_SUCCESS, secondary.lock_.init(&secondary));
  ASSERT_EQ(OB_SUCCESS, secondary.set_trans_id(ObTransID(20260720001)));
  ASSERT_EQ(OB_SUCCESS, secondary.set_ls_id(share::ObLSID(1001)));

  share::SCN version_100;
  share::SCN version_200;
  share::SCN version_150;
  version_100.convert_for_tx(100);
  version_200.convert_for_tx(200);
  version_150.convert_for_tx(150);

  secondary.redo_flush_status_ = TxRedoFlushStatus::SECONDARY_PREPARING;
  EXPECT_EQ(OB_SUCCESS, secondary.before_prepare_for_hotspot(version_100));
  EXPECT_EQ(version_100, secondary.mt_ctx_.get_trans_version());
  EXPECT_EQ(OB_SUCCESS, secondary.before_prepare_for_hotspot(version_100));

  secondary.redo_flush_status_ = TxRedoFlushStatus::SECONDARY_MIGRATING;
  EXPECT_EQ(OB_SUCCESS, secondary.before_prepare_for_hotspot(version_200));
  EXPECT_EQ(version_200, secondary.mt_ctx_.get_trans_version());
  EXPECT_EQ(OB_ERR_UNEXPECTED, secondary.before_prepare_for_hotspot(version_150));
  EXPECT_EQ(version_200, secondary.mt_ctx_.get_trans_version());

  secondary.redo_flush_status_ = TxRedoFlushStatus::SECONDARY_MIGRATE_FAILED;
  EXPECT_EQ(OB_STATE_NOT_MATCH, secondary.before_prepare_for_hotspot(version_200));
  secondary.lock_.reset();
}

// ============================================================================
// TC-17: Secondary skips commit version generation (returns OB_EAGAIN)
// ============================================================================
TEST_F(ObTestHotspotEdgeCases, test_secondary_skips_commit_version)
{
  ObPartTransCtx secondary;
  ASSERT_EQ(OB_SUCCESS, secondary.lock_.init(&secondary));
  ASSERT_EQ(OB_SUCCESS, secondary.set_trans_id(ObTransID(20260721002)));
  ASSERT_EQ(OB_SUCCESS, secondary.set_ls_id(share::ObLSID(1001)));

  // All secondary statuses must cause generate_commit_version_() to return
  // OB_EAGAIN before checking commit version validity or calling get_gts_().
  const TxRedoFlushStatus secondary_statuses[] = {
    TxRedoFlushStatus::SECONDARY_PREPARING,
    TxRedoFlushStatus::SECONDARY_MIGRATING,
    TxRedoFlushStatus::SECONDARY_MIGRATE_SYNCED,
  };

  for (int64_t i = 0; i < static_cast<int64_t>(sizeof(secondary_statuses) / sizeof(secondary_statuses[0])); ++i) {
    secondary.redo_flush_status_ = secondary_statuses[i];
    EXPECT_EQ(OB_EAGAIN, secondary.generate_commit_version_())
        << "status=" << static_cast<int>(secondary_statuses[i]);
  }

  secondary.lock_.reset();
}

// ============================================================================
// TC-21: before_prepare_secondaries success and partial failure
// ============================================================================
TEST_F(ObTestHotspotEdgeCases, test_before_prepare_secondaries_success_and_partial_failure)
{
  ObPartTransCtx primary;
  ObPartTransCtx sec0;
  ObPartTransCtx sec1;
  ASSERT_EQ(OB_SUCCESS, primary.lock_.init(&primary));
  ASSERT_EQ(OB_SUCCESS, sec0.lock_.init(&sec0));
  ASSERT_EQ(OB_SUCCESS, sec1.lock_.init(&sec1));
  const ObTransID primary_tx_id(20260730001);
  ASSERT_EQ(OB_SUCCESS, primary.set_trans_id(primary_tx_id));
  ASSERT_EQ(OB_SUCCESS, primary.set_ls_id(share::ObLSID(1001)));
  ASSERT_EQ(OB_SUCCESS, sec0.set_trans_id(ObTransID(20260730002)));
  ASSERT_EQ(OB_SUCCESS, sec0.set_ls_id(share::ObLSID(1001)));
  ASSERT_EQ(OB_SUCCESS, sec1.set_trans_id(ObTransID(20260730003)));
  ASSERT_EQ(OB_SUCCESS, sec1.set_ls_id(share::ObLSID(1001)));

  ASSERT_EQ(OB_SUCCESS, primary.hotspot_redo_cache_.init(primary_tx_id, 2, nullptr));
  ASSERT_EQ(OB_SUCCESS, primary.hotspot_redo_cache_.insert_into(&sec0));
  ASSERT_EQ(OB_SUCCESS, primary.hotspot_redo_cache_.insert_into(&sec1));

  share::SCN gts;
  gts.convert_for_tx(500);

  // Case 1: Both secondaries in SECONDARY_MIGRATING -> success
  sec0.redo_flush_status_ = TxRedoFlushStatus::SECONDARY_MIGRATING;
  sec1.redo_flush_status_ = TxRedoFlushStatus::SECONDARY_MIGRATING;
  EXPECT_EQ(OB_SUCCESS, primary.hotspot_redo_cache_.before_prepare_secondaries(gts));
  EXPECT_EQ(gts, sec0.mt_ctx_.get_trans_version());
  EXPECT_EQ(gts, sec1.mt_ctx_.get_trans_version());

  // Case 2: sec0 in SECONDARY_MIGRATE_FAILED -> returns OB_STATE_NOT_MATCH
  // Loop stops on first failure, sec1 is NOT touched
  sec0.redo_flush_status_ = TxRedoFlushStatus::SECONDARY_MIGRATE_FAILED;
  sec0.mt_ctx_.set_trans_version(share::SCN::min_scn());
  sec1.mt_ctx_.set_trans_version(share::SCN::min_scn());
  EXPECT_EQ(OB_STATE_NOT_MATCH, primary.hotspot_redo_cache_.before_prepare_secondaries(gts));
  // sec1 was not processed because loop stopped at sec0
  EXPECT_TRUE(sec1.mt_ctx_.get_trans_version().is_min());

  // Case 3: sec0 SECONDARY_MIGRATING, sec1 SECONDARY_MIGRATE_SYNCED -> success
  sec0.redo_flush_status_ = TxRedoFlushStatus::SECONDARY_MIGRATING;
  sec1.redo_flush_status_ = TxRedoFlushStatus::SECONDARY_MIGRATE_SYNCED;
  EXPECT_EQ(OB_SUCCESS, primary.hotspot_redo_cache_.before_prepare_secondaries(gts));
  EXPECT_EQ(gts, sec0.mt_ctx_.get_trans_version());
  EXPECT_EQ(gts, sec1.mt_ctx_.get_trans_version());

  // Detach raw test pointers for safe cleanup
  if (OB_NOT_NULL(primary.hotspot_redo_cache_.cache_)) {
    SpinWLockGuard guard(primary.hotspot_redo_cache_.hotspot_lock_);
    primary.hotspot_redo_cache_.cache_->hotspot_cache_[0].other_ctx_ = nullptr;
    primary.hotspot_redo_cache_.cache_->hotspot_cache_[1].other_ctx_ = nullptr;
  }
  EXPECT_EQ(OB_SUCCESS, primary.hotspot_redo_cache_.reuse());
  primary.lock_.reset();
  sec0.lock_.reset();
  sec1.lock_.reset();
}

// ============================================================================
// TC-22: before_prepare_secondaries version monotonicity and idempotency
// ============================================================================
TEST_F(ObTestHotspotEdgeCases, test_before_prepare_secondaries_version_monotonicity)
{
  ObPartTransCtx primary;
  ObPartTransCtx secondary;
  ASSERT_EQ(OB_SUCCESS, primary.lock_.init(&primary));
  ASSERT_EQ(OB_SUCCESS, secondary.lock_.init(&secondary));
  const ObTransID primary_tx_id(20260730004);
  ASSERT_EQ(OB_SUCCESS, primary.set_trans_id(primary_tx_id));
  ASSERT_EQ(OB_SUCCESS, primary.set_ls_id(share::ObLSID(1001)));
  ASSERT_EQ(OB_SUCCESS, secondary.set_trans_id(ObTransID(20260730005)));
  ASSERT_EQ(OB_SUCCESS, secondary.set_ls_id(share::ObLSID(1001)));

  ASSERT_EQ(OB_SUCCESS, primary.hotspot_redo_cache_.init(primary_tx_id, 1, nullptr));
  ASSERT_EQ(OB_SUCCESS, primary.hotspot_redo_cache_.insert_into(&secondary));

  secondary.redo_flush_status_ = TxRedoFlushStatus::SECONDARY_MIGRATING;

  // Case 1: Set version 200 first
  share::SCN v200;
  v200.convert_for_tx(200);
  EXPECT_EQ(OB_SUCCESS, primary.hotspot_redo_cache_.before_prepare_secondaries(v200));
  EXPECT_EQ(v200, secondary.mt_ctx_.get_trans_version());

  // Case 2: Try lower version 150 -> OB_ERR_UNEXPECTED (monotonicity violation)
  share::SCN v150;
  v150.convert_for_tx(150);
  EXPECT_EQ(OB_ERR_UNEXPECTED, primary.hotspot_redo_cache_.before_prepare_secondaries(v150));
  // Version unchanged
  EXPECT_EQ(v200, secondary.mt_ctx_.get_trans_version());

  // Case 3: Same version 200 -> success (idempotent, no-op)
  EXPECT_EQ(OB_SUCCESS, primary.hotspot_redo_cache_.before_prepare_secondaries(v200));
  EXPECT_EQ(v200, secondary.mt_ctx_.get_trans_version());

  // Case 4: Higher version 300 -> success, version updated
  share::SCN v300;
  v300.convert_for_tx(300);
  EXPECT_EQ(OB_SUCCESS, primary.hotspot_redo_cache_.before_prepare_secondaries(v300));
  EXPECT_EQ(v300, secondary.mt_ctx_.get_trans_version());

  // Detach raw test pointer for safe cleanup
  if (OB_NOT_NULL(primary.hotspot_redo_cache_.cache_)) {
    SpinWLockGuard guard(primary.hotspot_redo_cache_.hotspot_lock_);
    primary.hotspot_redo_cache_.cache_->hotspot_cache_[0].other_ctx_ = nullptr;
  }
  EXPECT_EQ(OB_SUCCESS, primary.hotspot_redo_cache_.reuse());
  primary.lock_.reset();
  secondary.lock_.reset();
}

// ============================================================================
// TC-23: Shared prepare publication preserves primary/secondary barrier order
// ============================================================================
TEST_F(ObTestHotspotEdgeCases, test_publish_mvcc_prepare_barrier_status_and_order)
{
  ObPartTransCtx primary;
  ObPartTransCtx secondary;
  ASSERT_EQ(OB_SUCCESS, primary.lock_.init(&primary));
  ASSERT_EQ(OB_SUCCESS, secondary.lock_.init(&secondary));
  const ObTransID primary_tx_id(20260806001);
  ASSERT_EQ(OB_SUCCESS, primary.set_trans_id(primary_tx_id));
  ASSERT_EQ(OB_SUCCESS, primary.set_ls_id(share::ObLSID(1001)));
  ASSERT_EQ(OB_SUCCESS, secondary.set_trans_id(ObTransID(20260806002)));
  ASSERT_EQ(OB_SUCCESS, secondary.set_ls_id(share::ObLSID(1001)));

  share::SCN version;
  version.convert_for_tx(600);

  // Normal transactions publish only their local barrier.
  primary.redo_flush_status_ = TxRedoFlushStatus::NORMAL_START;
  ASSERT_EQ(OB_SUCCESS, primary.publish_mvcc_prepare_barrier_(version));
  EXPECT_EQ(version, primary.mt_ctx_.get_trans_version());

  ASSERT_EQ(OB_SUCCESS, primary.hotspot_redo_cache_.init(primary_tx_id, 1, nullptr));
  ASSERT_EQ(OB_SUCCESS, primary.hotspot_redo_cache_.insert_into(&secondary));

  // An aggregating primary must remain unprepared, as must its secondary.
  primary.mt_ctx_.set_trans_version(share::SCN::max_scn());
  secondary.mt_ctx_.set_trans_version(share::SCN::max_scn());
  primary.redo_flush_status_ = TxRedoFlushStatus::PRIMARY_COLLECTING;
  secondary.redo_flush_status_ = TxRedoFlushStatus::SECONDARY_MIGRATING;
  EXPECT_EQ(OB_EAGAIN, primary.publish_mvcc_prepare_barrier_(version));
  EXPECT_TRUE(primary.mt_ctx_.get_trans_version().is_max());
  EXPECT_TRUE(secondary.mt_ctx_.get_trans_version().is_max());

  // A secondary publication failure must not publish the primary barrier.
  primary.redo_flush_status_ = TxRedoFlushStatus::PRIMARY_AGGR_SUCCEEDED;
  secondary.redo_flush_status_ = TxRedoFlushStatus::SECONDARY_MIGRATE_FAILED;
  EXPECT_EQ(OB_STATE_NOT_MATCH, primary.publish_mvcc_prepare_barrier_(version));
  EXPECT_TRUE(primary.mt_ctx_.get_trans_version().is_max());

  // Once aggregation succeeds, secondary is published first and the primary
  // becomes prepared only after that succeeds.
  secondary.redo_flush_status_ = TxRedoFlushStatus::SECONDARY_MIGRATING;
  EXPECT_EQ(OB_SUCCESS, primary.publish_mvcc_prepare_barrier_(version));
  EXPECT_EQ(version, secondary.mt_ctx_.get_trans_version());
  EXPECT_EQ(version, primary.mt_ctx_.get_trans_version());

  if (OB_NOT_NULL(primary.hotspot_redo_cache_.cache_)) {
    SpinWLockGuard guard(primary.hotspot_redo_cache_.hotspot_lock_);
    primary.hotspot_redo_cache_.cache_->hotspot_cache_[0].other_ctx_ = nullptr;
  }
  EXPECT_EQ(OB_SUCCESS, primary.hotspot_redo_cache_.reuse());
  primary.lock_.reset();
  secondary.lock_.reset();
}

// ============================================================================
// TC-24: Unlock refresh target is dynamic and skips replay
// ============================================================================
TEST_F(ObTestHotspotEdgeCases, test_gts_refresh_target_and_replay_filter)
{
  ObPartTransCtx primary;
  storage::ObTxData tx_data;
  primary.ctx_tx_data_.test_init(tx_data, nullptr);

  share::SCN prepare_version;
  share::SCN commit_version;
  ASSERT_EQ(OB_SUCCESS, prepare_version.convert_for_tx(700));
  ASSERT_EQ(OB_SUCCESS, commit_version.convert_for_tx(800));
  primary.exec_info_.prepare_version_ = prepare_version;
  ASSERT_EQ(OB_SUCCESS, primary.ctx_tx_data_.set_commit_version(commit_version));

  // Valid versions alone must not affect normal or unfinished hotspot txs.
  primary.redo_flush_status_ = TxRedoFlushStatus::NORMAL_START;
  EXPECT_FALSE(primary.get_gts_refresh_target_().is_valid_and_not_min());
  primary.redo_flush_status_ = TxRedoFlushStatus::PRIMARY_COLLECTING;
  EXPECT_FALSE(primary.get_gts_refresh_target_().is_valid_and_not_min());

  // A completed hotspot primary uses the greater finite version on every
  // eligible unlock, including unlocks reached from log callbacks. Replay is
  // the only execution-path filter; there is no one-shot path flag.
  primary.redo_flush_status_ = TxRedoFlushStatus::PRIMARY_AGGR_SUCCEEDED;
  EXPECT_EQ(commit_version, primary.get_gts_refresh_target_());
  CtxLockArg unlock_arg;
  primary.before_unlock(unlock_arg);
  EXPECT_EQ(commit_version, unlock_arg.gts_refresh_target_);

  primary.set_for_replay(true);
  EXPECT_FALSE(primary.get_gts_refresh_target_().is_valid_and_not_min());
  primary.set_for_replay(false);

  EXPECT_EQ(commit_version, primary.get_gts_refresh_target_());

  // test_init() points at stack storage without a production tx-data
  // allocator, so detach it directly instead of invoking dec_ref().
  primary.ctx_tx_data_.tx_data_guard_.tx_data_ = nullptr;
}

// ============================================================================
// TC-25: Remote refresh is range=1 and a caught-up cache skips it
// ============================================================================
TEST_F(ObTestHotspotEdgeCases, test_hotspot_gts_refresh_remote_and_skip)
{
  ObGtsSource source;
  const uint64_t tenant_id = 1001;
  common::ObAddr server;
  common::ObAddr leader;
  ASSERT_TRUE(server.set_ip_addr("127.0.0.1", 2881));
  ASSERT_TRUE(leader.set_ip_addr("127.0.0.1", 2882));
  MockHotspotGtsRequestRpc rpc;
  MockHotspotGtsLocationAdapter location_adapter(leader);
  ASSERT_EQ(OB_SUCCESS, source.init(tenant_id, server, &rpc, &location_adapter));

  bool updated = false;
  ASSERT_EQ(OB_SUCCESS, source.update_gts(700, updated));
  ASSERT_TRUE(updated);
  EXPECT_EQ(OB_SUCCESS, source.refresh_gts_if_cache_behind(800));
  EXPECT_EQ(OB_SUCCESS, source.refresh_gts_if_cache_behind(800));
  EXPECT_EQ(2, rpc.post_count_);
  EXPECT_EQ(1, rpc.last_range_size_);

  // Once the cache reaches the target, the same unlock-side check is a no-op.
  updated = false;
  ASSERT_EQ(OB_SUCCESS, source.update_gts(800, updated));
  ASSERT_TRUE(updated);
  EXPECT_EQ(OB_SUCCESS, source.refresh_gts_if_cache_behind(800));
  EXPECT_EQ(2, rpc.post_count_);
  EXPECT_EQ(3, ATOMIC_LOAD(&source.gts_statistics_.hotspot_refresh_check_cnt_));
  EXPECT_EQ(1, ATOMIC_LOAD(&source.gts_statistics_.hotspot_refresh_skip_cnt_));
  EXPECT_EQ(2, ATOMIC_LOAD(&source.gts_statistics_.hotspot_refresh_trigger_cnt_));
  EXPECT_EQ(0, ATOMIC_LOAD(&source.gts_statistics_.hotspot_refresh_local_cnt_));
  EXPECT_EQ(2, ATOMIC_LOAD(&source.gts_statistics_.hotspot_refresh_remote_cnt_));
}

} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_hotspot_edge_cases.log*");
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_hotspot_edge_cases.log", true, false);
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
