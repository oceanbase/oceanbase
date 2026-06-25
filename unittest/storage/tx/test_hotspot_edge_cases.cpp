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
#include "storage/tx/ob_tx_log_cb_define.h"
#include "storage/tx/ob_trans_submit_log_cb.h"
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
  EXPECT_EQ(OB_SUCCESS, cache.try_release_idle_log_cb(nullptr));

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
  EXPECT_EQ(OB_INVALID_ARGUMENT, cache.try_release_idle_log_cb(nullptr));

  TRANS_LOG(INFO, "test_handle_release_idle_rejects_null_ctx_when_inited: verified argument check");
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
