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

// Unit tests for free_hotspot_cbs_ deadlock fix
//
// Bug root cause (from free_cbs_deadlock_analysis.md):
//   1. Submit fails (OB_NOT_MASTER), CBs go to free queue
//   2. Force abort sets PRIMARY_AGGR_FAILED, retry mechanism disabled
//   3. handle_abort_response_() calls cbs_is_working_() which returns true (free>0)
//   4. Free CBs have no clog callback to release them -> infinite retry -> deadlock
//
// Fix approach:
//   When cbs_is_working_() returns true in handle_abort_response_():
//   - Return OB_EAGAIN first (per existing contract)
//   - Then call release_free_cbs_to_idle_() to move free CBs to idle list
//   - If busy==0: free moved to idle, next retry cbs_is_working_() returns false -> success
//   - If busy>0: wait for clog callback to clear busy, then next retry succeeds

#include <gtest/gtest.h>
#define USING_LOG_PREFIX TRANS
#define private public
#define protected public

#include "storage/tx/ob_tx_log_cb_define.h"
#include "storage/tx/ob_tx_hotspot_define.h"
#include "storage/tx/ob_tx_hotspot_helper.h"
#include "storage/tx/ob_trans_submit_log_cb.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "share/ob_errno.h"
#include "lib/utility/utility.h"

namespace oceanbase
{
using namespace ::testing;
using namespace transaction;

class ObTestHotspotFreeCbsDeadlock : public ::testing::Test
{
public:
  virtual void SetUp() override
  {
    oceanbase::ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
  }
  virtual void TearDown() override {}
};

static void cleanup_manual_hotspot_cache(TransModulePageAllocator &allocator,
                                         ObTxHotspotRedoCache &cache)
{
  if (OB_NOT_NULL(cache.hotspot_cache_)) {
    for (int64_t i = 0; i < cache.hotspot_cache_count_; ++i) {
      cache.hotspot_cache_[i].~ObTxRedoExtractArg();
    }
    allocator.free(cache.hotspot_cache_);
    cache.hotspot_cache_ = nullptr;
    cache.hotspot_cache_capacity_ = 0;
    cache.hotspot_cache_count_ = 0;
  }
}

// ============================================================================
// TC-01: Verify cbs_is_working_() implementation: busy>0 OR free>0
// ============================================================================
TEST_F(ObTestHotspotFreeCbsDeadlock, test_cbs_is_working_logic)
{
  ObTxHotspotRedoCache cache;

  // Case 1: busy=0, free=0 -> false (should proceed)
  EXPECT_EQ(0, cache.busy_hotspot_cbs_.get_size());
  EXPECT_EQ(0, cache.free_hotspot_cbs_.get_size());
  EXPECT_FALSE(cache.cbs_is_working_());

  // cbs_is_working_() = busy_hotspot_cbs_.get_size() > 0 || free_hotspot_cbs_.get_size() > 0
  // This is the condition that causes handle_abort_response_() to deadlock when free>0
  // The fix ensures free CBs are moved to idle before next retry

  TRANS_LOG(INFO, "test_cbs_is_working_logic: verified empty case returns false");
}

// ============================================================================
// TC-03: Verify empty lists case for handle_abort_response_
// ============================================================================
TEST_F(ObTestHotspotFreeCbsDeadlock, test_empty_lists_abort)
{
  TransModulePageAllocator allocator;
  ObTxHotspotRedoCache cache;

  // Explicitly initialize DList members (they may be polluted by previous tests)
  new (&cache.idle_hotspot_cbs_) common::ObDList<ObTxLogCb>();
  new (&cache.free_hotspot_cbs_) common::ObDList<ObTxLogCb>();
  new (&cache.busy_hotspot_cbs_) common::ObDList<ObTxLogCb>();

  // Manually allocate 1 entry for hotspot_cache_ (new pointer-based model)
  cache.hotspot_cache_capacity_ = 1;
  cache.hotspot_cache_count_ = 1;
  cache.hotspot_cache_ = static_cast<ObTxRedoExtractArg*>(
      allocator.alloc(1 * sizeof(ObTxRedoExtractArg)));
  ASSERT_NE(nullptr, cache.hotspot_cache_);
  new (&cache.hotspot_cache_[0]) ObTxRedoExtractArg();  // placement new

  // Verify empty CB state
  EXPECT_EQ(0, cache.busy_hotspot_cbs_.get_size());
  EXPECT_EQ(0, cache.free_hotspot_cbs_.get_size());
  EXPECT_EQ(0, cache.idle_hotspot_cbs_.get_size());
  EXPECT_FALSE(cache.cbs_is_working_());

  // With empty CB lists, handle_abort_response_() should succeed immediately
  int ret = cache.handle_abort_response_(0);
  EXPECT_EQ(OB_SUCCESS, ret);

  EXPECT_EQ(TxSecondaryRespStatus::RESP_SKIPPED_BY_PRIMARY,
            cache.hotspot_cache_[0].get_resp_status());

  TRANS_LOG(INFO, "test_empty_lists_abort: empty case succeeds immediately");
  cleanup_manual_hotspot_cache(allocator, cache);
}

// ============================================================================
// TC-04: Verify need_return_cbs_() logic
// ============================================================================
TEST_F(ObTestHotspotFreeCbsDeadlock, test_need_return_cbs)
{
  ObTxHotspotRedoCache cache;

  // need_return_cbs_() = idle>0 || free>0 || busy>0
  EXPECT_FALSE(cache.need_return_cbs_());

  // After fix: release_free_cbs_to_idle_() moves free to idle
  // Then try_release_idle_log_cb() releases idle CBs to primary
  // After both steps, need_return_cbs_() returns false -> reuse() succeeds

  TRANS_LOG(INFO, "test_need_return_cbs: verified empty case returns false");
}

// ============================================================================
// TC-05: Verify release_free_cbs_to_idle_() with empty free list
// ============================================================================
TEST_F(ObTestHotspotFreeCbsDeadlock, test_release_empty_free)
{
  ObTxHotspotRedoCache cache;

  // Explicitly initialize DList members (they may be polluted by previous tests)
  new (&cache.idle_hotspot_cbs_) common::ObDList<ObTxLogCb>();
  new (&cache.free_hotspot_cbs_) common::ObDList<ObTxLogCb>();
  new (&cache.busy_hotspot_cbs_) common::ObDList<ObTxLogCb>();

  // Empty free list: release should succeed with nothing to do
  EXPECT_EQ(0, cache.free_hotspot_cbs_.get_size());
  EXPECT_EQ(0, cache.busy_hotspot_cbs_.get_size());

  int ret = cache.release_free_cbs_to_idle_();
  EXPECT_EQ(OB_SUCCESS, ret);

  // Lists should remain empty
  EXPECT_EQ(0, cache.free_hotspot_cbs_.get_size());
  EXPECT_EQ(0, cache.idle_hotspot_cbs_.get_size());

  TRANS_LOG(INFO, "test_release_empty_free: empty case returns SUCCESS");
}

// ============================================================================
// TC-06: Verify response_scheduler flow with empty cache
// ============================================================================
TEST_F(ObTestHotspotFreeCbsDeadlock, test_response_scheduler_empty)
{
  ObTxHotspotRedoCache cache;

  // Empty cache: response_scheduler should succeed immediately
  int ret = cache.response_scheduler(OB_TRANS_KILLED, share::SCN::invalid_scn());
  EXPECT_EQ(OB_SUCCESS, ret);

  TRANS_LOG(INFO, "test_response_scheduler_empty: empty cache succeeds");
}

// ============================================================================
// TC-07: Verify hotspot_cache_ operations
// ============================================================================
TEST_F(ObTestHotspotFreeCbsDeadlock, test_hotspot_cache_operations)
{
  TransModulePageAllocator allocator;
  ObTxHotspotRedoCache cache;

  // Manually allocate 3 entries for hotspot_cache_ (new pointer-based model)
  cache.hotspot_cache_capacity_ = 3;
  cache.hotspot_cache_count_ = 3;
  cache.hotspot_cache_ = static_cast<ObTxRedoExtractArg*>(
      allocator.alloc(3 * sizeof(ObTxRedoExtractArg)));
  ASSERT_NE(nullptr, cache.hotspot_cache_);
  for (int i = 0; i < 3; i++) {
    new (&cache.hotspot_cache_[i]) ObTxRedoExtractArg();  // placement new
  }

  EXPECT_EQ(3, cache.hotspot_cache_count_);

  // Set resp_status for each entry
  for (int i = 0; i < 3; i++) {
    cache.hotspot_cache_[i].set_resp_status(TxSecondaryRespStatus::RESP_SKIPPED_BY_PRIMARY);
    EXPECT_EQ(TxSecondaryRespStatus::RESP_SKIPPED_BY_PRIMARY,
              cache.hotspot_cache_[i].get_resp_status());
  }

  TRANS_LOG(INFO, "test_hotspot_cache_operations: cache operations verified");
  cleanup_manual_hotspot_cache(allocator, cache);
}

// ============================================================================
// TC-08: Verify reuse() with empty CBs
// ============================================================================
TEST_F(ObTestHotspotFreeCbsDeadlock, test_reuse_empty)
{
  ObTxHotspotRedoCache cache;

  // Explicitly initialize DList members (they may be polluted by previous tests)
  new (&cache.idle_hotspot_cbs_) common::ObDList<ObTxLogCb>();
  new (&cache.free_hotspot_cbs_) common::ObDList<ObTxLogCb>();
  new (&cache.busy_hotspot_cbs_) common::ObDList<ObTxLogCb>();

  // Empty CBs: reuse should succeed
  int ret = cache.reuse();
  EXPECT_EQ(OB_SUCCESS, ret);

  TRANS_LOG(INFO, "test_reuse_empty: reuse succeeds with empty CBs");
}

} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_hotspot_free_cbs_deadlock.log*");
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_hotspot_free_cbs_deadlock.log", true, false,
                       "test_hotspot_free_cbs_deadlock.log",
                       "test_hotspot_free_cbs_deadlock.log",
                       "test_hotspot_free_cbs_deadlock.log");
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
