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
#define private public
#define protected public

#include "storage/tx/ob_tx_replay_executor.h"
#include "storage/ob_storage_table_guard.h"
#include "storage/ob_i_store.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/meta_mem/ob_tablet_handle.h"

#define USING_LOG_PREFIX TRANS

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace storage;
using namespace memtable;
using namespace transaction;

namespace unittest
{

class TestGuardCache : public ::testing::Test
{
public:
  virtual void SetUp() {}
  virtual void TearDown() {}
};

// Helper: after constructing an ObStorageTableGuard via ObReplayHandleCache, disable
// need_control_mem_ so the destructor won't call MTL() (which requires tenant
// context that is unavailable in unit tests).
static void disable_throttle(ObTxReplayExecutor::ObReplayHandleCache &cache)
{
  if (cache.is_storage_guard_constructed_) {
    cache.get_storage_guard_()->need_control_mem_ = false;
  }
}

TEST_F(TestGuardCache, storage_guard_get_tablet)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObStoreCtx store_ctx;
  ObStorageTableGuard guard(nullptr, store_ctx, false, true, SCN::min_scn());
  EXPECT_EQ(nullptr, guard.get_tablet());

  ObTablet fake_tablet;
  guard.tablet_ = &fake_tablet;
  EXPECT_EQ(&fake_tablet, guard.get_tablet());
  guard.tablet_ = nullptr;
}

// =====================================================================
// Test ObReplayHandleCache lifecycle and reuse logic
// =====================================================================

TEST_F(TestGuardCache, guard_cache_init_state)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  ObTxReplayExecutor::ObReplayHandleCache cache;
  EXPECT_FALSE(cache.is_storage_guard_constructed_);
  EXPECT_FALSE(cache.tablet_handle_.is_valid());
}

TEST_F(TestGuardCache, guard_cache_construct_deconstruct_storage_guard)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  ObTxReplayExecutor::ObReplayHandleCache cache;
  ObTablet fake_tablet;
  SCN replay_scn = SCN::min_scn();

  // construct
  cache.construct_storage_guard_(&fake_tablet, replay_scn);
  EXPECT_TRUE(cache.is_storage_guard_constructed_);
  disable_throttle(cache);

  ObStorageTableGuard *guard = cache.get_storage_guard_();
  EXPECT_EQ(&fake_tablet, guard->get_tablet());
  EXPECT_TRUE(guard->for_replay_);

  // double construct should be a no-op (guarded by is_storage_guard_constructed_)
  ObTablet another_tablet;
  cache.construct_storage_guard_(&another_tablet, replay_scn);
  EXPECT_TRUE(cache.is_storage_guard_constructed_);
  EXPECT_EQ(&fake_tablet, guard->get_tablet());

  // deconstruct
  cache.deconstruct_storage_guard_();
  EXPECT_FALSE(cache.is_storage_guard_constructed_);

  // double deconstruct should be safe
  cache.deconstruct_storage_guard_();
  EXPECT_FALSE(cache.is_storage_guard_constructed_);
}

TEST_F(TestGuardCache, guard_cache_get_storage_guard_reuse_same_tablet)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  ObTxReplayExecutor::ObReplayHandleCache cache;
  ObTablet fake_tablet;
  SCN replay_scn = SCN::min_scn();

  // first call: should construct
  cache.construct_storage_guard_(&fake_tablet, replay_scn);
  EXPECT_TRUE(cache.is_storage_guard_constructed_);
  ObStorageTableGuard *guard1 = cache.get_storage_guard_();
  EXPECT_EQ(&fake_tablet, guard1->get_tablet());
  disable_throttle(cache);

  // second call with same tablet: guard stays constructed (reuse)
  ObStorageTableGuard *guard2 = cache.get_storage_guard_();
  EXPECT_TRUE(cache.is_storage_guard_constructed_);
  EXPECT_EQ(guard1, guard2);
  EXPECT_EQ(&fake_tablet, guard2->get_tablet());

  cache.reset();
}

TEST_F(TestGuardCache, guard_cache_get_storage_guard_different_tablet)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  ObTxReplayExecutor::ObReplayHandleCache cache;
  ObTablet tablet_a;
  ObTablet tablet_b;
  SCN replay_scn = SCN::min_scn();

  // first call with tablet_a
  cache.construct_storage_guard_(&tablet_a, replay_scn);
  EXPECT_EQ(&tablet_a, cache.get_storage_guard_()->get_tablet());
  disable_throttle(cache);

  // switch to tablet_b: should deconstruct old and construct new
  cache.deconstruct_storage_guard_();
  cache.construct_storage_guard_(&tablet_b, replay_scn);
  EXPECT_TRUE(cache.is_storage_guard_constructed_);
  EXPECT_EQ(&tablet_b, cache.get_storage_guard_()->get_tablet());
  disable_throttle(cache);

  cache.reset();
}

TEST_F(TestGuardCache, guard_cache_reset)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  ObTxReplayExecutor::ObReplayHandleCache cache;
  ObTablet fake_tablet;
  SCN replay_scn = SCN::min_scn();

  cache.construct_storage_guard_(&fake_tablet, replay_scn);
  EXPECT_TRUE(cache.is_storage_guard_constructed_);
  disable_throttle(cache);

  cache.reset();
  EXPECT_FALSE(cache.is_storage_guard_constructed_);
  EXPECT_FALSE(cache.tablet_handle_.is_valid());
}

TEST_F(TestGuardCache, guard_cache_storage_guard_preserves_replay_props)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  ObTxReplayExecutor::ObReplayHandleCache cache;
  ObTablet fake_tablet;
  SCN replay_scn;
  replay_scn.convert_for_gts(1000);

  cache.construct_storage_guard_(&fake_tablet, replay_scn);
  ObStorageTableGuard *guard = cache.get_storage_guard_();
  disable_throttle(cache);
  EXPECT_TRUE(guard->for_replay_);
  EXPECT_EQ(replay_scn, guard->replay_scn_);

  // reuse with same tablet should keep the guard as-is
  ObStorageTableGuard *guard2 = cache.get_storage_guard_();
  EXPECT_EQ(guard, guard2);

  cache.reset();
}

TEST_F(TestGuardCache, guard_cache_storage_guard_reset_on_tablet_change)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  ObTxReplayExecutor::ObReplayHandleCache cache;
  ObTablet tablet_a;
  ObTablet tablet_b;
  SCN replay_scn;
  replay_scn.convert_for_gts(1000);

  cache.construct_storage_guard_(&tablet_a, replay_scn);
  disable_throttle(cache);

  // switch to a different tablet: old guard is destructed, new one is created fresh
  cache.deconstruct_storage_guard_();
  cache.construct_storage_guard_(&tablet_b, replay_scn);
  ObStorageTableGuard *guard2 = cache.get_storage_guard_();
  disable_throttle(cache);
  EXPECT_EQ(&tablet_b, guard2->get_tablet());

  cache.reset();
}

// Test that multiple construct/deconstruct cycles work correctly
TEST_F(TestGuardCache, guard_cache_multiple_cycles)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  ObTxReplayExecutor::ObReplayHandleCache cache;
  ObTablet tablet_a;
  ObTablet tablet_b;
  ObTablet tablet_c;
  SCN replay_scn = SCN::min_scn();

  // cycle 1: tablet_a
  cache.construct_storage_guard_(&tablet_a, replay_scn);
  disable_throttle(cache);
  EXPECT_EQ(&tablet_a, cache.get_storage_guard_()->get_tablet());

  // cycle 2: switch to tablet_b
  cache.deconstruct_storage_guard_();
  cache.construct_storage_guard_(&tablet_b, replay_scn);
  disable_throttle(cache);
  EXPECT_EQ(&tablet_b, cache.get_storage_guard_()->get_tablet());

  // cycle 3: switch to tablet_c
  cache.deconstruct_storage_guard_();
  cache.construct_storage_guard_(&tablet_c, replay_scn);
  disable_throttle(cache);
  EXPECT_EQ(&tablet_c, cache.get_storage_guard_()->get_tablet());

  // cycle 4: back to tablet_a (should construct fresh, not remember tablet_a)
  cache.deconstruct_storage_guard_();
  cache.construct_storage_guard_(&tablet_a, replay_scn);
  disable_throttle(cache);
  EXPECT_EQ(&tablet_a, cache.get_storage_guard_()->get_tablet());

  cache.reset();
}

// =====================================================================
// Simulate production replay patterns (a)(b)(c)(d)
//
// Because prepare_tablet_for_replay / prepare_memtable_for_replay
// depend on heavy infrastructure (ObLS, ObPartTransCtx, real Memtable),
// we simulate the same state transitions the cache goes through during
// replay_redo_in_memtable_ → replay_one_row_in_memtable_ → replay_row_
// at the construct/deconstruct primitive level, then verify:
//   - Guard is constructed exactly once per distinct tablet
//   - Guard is reused (not re-constructed) for consecutive same-tablet rows
//   - Guard is properly destructed on tablet switch and on final reset
//   - Total construct count == total deconstruct count (lifecycle balance)
// =====================================================================

// Helper that simulates how prepare_memtable_for_replay manages the guard
// for one row: construct if needed, reuse if same tablet, reconstruct on change.
// Returns true if the guard was freshly constructed (not reused).
static bool simulate_prepare_memtable(
    ObTxReplayExecutor::ObReplayHandleCache &cache,
    ObTablet *tablet,
    const SCN &replay_scn,
    int &construct_count,
    int &deconstruct_count)
{
  ObStorageTableGuard *guard = cache.get_storage_guard_();
  bool freshly_constructed = false;
  if (!cache.is_storage_guard_constructed_) {
    cache.construct_storage_guard_(tablet, replay_scn);
    disable_throttle(cache);
    construct_count++;
    freshly_constructed = true;
  } else if (guard->get_tablet() != tablet) {
    cache.deconstruct_storage_guard_();
    deconstruct_count++;
    cache.construct_storage_guard_(tablet, replay_scn);
    disable_throttle(cache);
    construct_count++;
    freshly_constructed = true;
  }
  // else: reuse existing guard (same tablet)
  return freshly_constructed;
}

// Helper that simulates the final guard_cache_.reset() at end of redo entry
static void simulate_redo_end_reset(
    ObTxReplayExecutor::ObReplayHandleCache &cache,
    int &deconstruct_count)
{
  if (cache.is_storage_guard_constructed_) {
    deconstruct_count++;
  }
  cache.reset();
}

// (a) All rows in a single redo go to the same tablet.
// Guard should be constructed once, reused for all subsequent rows,
// then destructed once on reset.
TEST_F(TestGuardCache, replay_pattern_all_rows_same_tablet)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  ObTxReplayExecutor::ObReplayHandleCache cache;
  ObTablet tablet_a;
  SCN replay_scn;
  replay_scn.convert_for_gts(1000);

  int construct_count = 0;
  int deconstruct_count = 0;
  const int ROW_COUNT = 5;

  // simulate 5 rows, all targeting tablet_a (mix of MUTATOR_ROW semantics)
  for (int i = 0; i < ROW_COUNT; i++) {
    bool fresh = simulate_prepare_memtable(cache, &tablet_a, replay_scn,
                                           construct_count, deconstruct_count);
    if (i == 0) {
      EXPECT_TRUE(fresh);  // first row: must construct
    } else {
      EXPECT_FALSE(fresh); // subsequent rows: reuse
    }
    EXPECT_TRUE(cache.is_storage_guard_constructed_);
    EXPECT_EQ(&tablet_a, cache.get_storage_guard_()->get_tablet());
  }

  EXPECT_EQ(1, construct_count);
  EXPECT_EQ(0, deconstruct_count);

  // end of redo entry
  simulate_redo_end_reset(cache, deconstruct_count);
  EXPECT_FALSE(cache.is_storage_guard_constructed_);
  EXPECT_EQ(1, construct_count);
  EXPECT_EQ(1, deconstruct_count);
}

// (b) Rows switch between different tablets mid-redo.
// Pattern: A, A, B, B, A, C — guard should reconstruct on each tablet change.
TEST_F(TestGuardCache, replay_pattern_tablet_switch)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  ObTxReplayExecutor::ObReplayHandleCache cache;
  ObTablet tablet_a, tablet_b, tablet_c;
  SCN replay_scn;
  replay_scn.convert_for_gts(2000);

  int construct_count = 0;
  int deconstruct_count = 0;

  ObTablet *row_tablets[] = {
    &tablet_a, &tablet_a, &tablet_b, &tablet_b, &tablet_a, &tablet_c
  };
  // expected: construct at rows 0, 2, 4, 5 ; reuse at rows 1, 3
  bool expected_fresh[] = {true, false, true, false, true, true};

  for (int i = 0; i < 6; i++) {
    bool fresh = simulate_prepare_memtable(cache, row_tablets[i], replay_scn,
                                           construct_count, deconstruct_count);
    EXPECT_EQ(expected_fresh[i], fresh) << "row " << i;
    EXPECT_TRUE(cache.is_storage_guard_constructed_);
    EXPECT_EQ(row_tablets[i], cache.get_storage_guard_()->get_tablet());
  }

  // 4 constructs (rows 0,2,4,5), 3 mid-switch deconstructs (before rows 2,4,5)
  EXPECT_EQ(4, construct_count);
  EXPECT_EQ(3, deconstruct_count);

  // final reset adds 1 deconstruct
  simulate_redo_end_reset(cache, deconstruct_count);
  EXPECT_EQ(4, construct_count);
  EXPECT_EQ(4, deconstruct_count);
}

// (c) Mix of MUTATOR_ROW and MUTATOR_TABLE_LOCK for the same tablet.
// In production, replay_row_ calls prepare_memtable_for_replay (uses guard),
// while replay_lock_ does NOT use prepare_memtable_for_replay (uses
// tablet->get_active_memtable directly). So the guard stays alive across
// both types and is only constructed once per tablet.
TEST_F(TestGuardCache, replay_pattern_mixed_row_and_lock)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  ObTxReplayExecutor::ObReplayHandleCache cache;
  ObTablet tablet_a, tablet_b;
  SCN replay_scn;
  replay_scn.convert_for_gts(3000);

  int construct_count = 0;
  int deconstruct_count = 0;

  enum RowType { ROW, LOCK };
  struct RowSpec {
    ObTablet *tablet;
    RowType type;
  };
  // A:ROW, A:LOCK, A:ROW, B:ROW, B:LOCK, B:ROW
  RowSpec rows[] = {
    {&tablet_a, ROW}, {&tablet_a, LOCK}, {&tablet_a, ROW},
    {&tablet_b, ROW}, {&tablet_b, LOCK}, {&tablet_b, ROW},
  };

  for (int i = 0; i < 6; i++) {
    // prepare_tablet_for_replay is always called (for both ROW and LOCK)
    // but prepare_memtable_for_replay is only called for MUTATOR_ROW
    // (MUTATOR_TABLE_LOCK goes to replay_lock_ which uses a different path).
    // For guard lifecycle testing, the guard is only touched in replay_row_.
    if (rows[i].type == ROW) {
      simulate_prepare_memtable(cache, rows[i].tablet, replay_scn,
                                construct_count, deconstruct_count);
    }
    // For LOCK type, the guard is simply left as-is (not touched)
    // This verifies that LOCK rows don't disturb the guard state
    EXPECT_EQ(rows[i].tablet, cache.get_storage_guard_()->get_tablet())
        << "row " << i << " guard should still point to current tablet";
  }

  // row 0 (A:ROW): construct for tablet_a => construct_count=1
  // row 1 (A:LOCK): guard untouched, still tablet_a
  // row 2 (A:ROW): same tablet_a, reuse => construct_count=1
  // row 3 (B:ROW): tablet change, deconstruct+construct => construct_count=2, deconstruct_count=1
  // row 4 (B:LOCK): guard untouched, still tablet_b
  // row 5 (B:ROW): same tablet_b, reuse => construct_count=2
  EXPECT_EQ(2, construct_count);
  EXPECT_EQ(1, deconstruct_count);

  simulate_redo_end_reset(cache, deconstruct_count);
  EXPECT_EQ(2, construct_count);
  EXPECT_EQ(2, deconstruct_count);
}

// (d) First row succeeds, then a middle row's replay_check_restore_status
// returns OB_EAGAIN — simulating the retry scenario.
// The guard should remain constructed across the OB_EAGAIN (it's the same
// tablet), and after reset the lifecycle should still be balanced.
// This tests what happens in production when replay_one_row_in_memtable_
// returns OB_EAGAIN on a middle row: the entire redo entry is retried from
// the beginning, and guard_cache_.reset() is called.
TEST_F(TestGuardCache, replay_pattern_eagain_mid_redo)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  ObTxReplayExecutor::ObReplayHandleCache cache;
  ObTablet tablet_a;
  SCN replay_scn;
  replay_scn.convert_for_gts(4000);

  int construct_count = 0;
  int deconstruct_count = 0;

  // --- First attempt: rows 0,1 succeed, row 2 gets OB_EAGAIN ---

  // Row 0: prepare_tablet succeeds, prepare_memtable succeeds
  simulate_prepare_memtable(cache, &tablet_a, replay_scn,
                            construct_count, deconstruct_count);
  EXPECT_EQ(1, construct_count);

  // Row 1: same tablet, reuse
  simulate_prepare_memtable(cache, &tablet_a, replay_scn,
                            construct_count, deconstruct_count);
  EXPECT_EQ(1, construct_count);

  // Row 2: prepare_tablet succeeds (same tablet), but
  // replay_check_restore_status returns OB_EAGAIN.
  // The guard is still constructed (same tablet), but we must reset
  // because the entire redo entry will be retried.
  simulate_prepare_memtable(cache, &tablet_a, replay_scn,
                            construct_count, deconstruct_count);
  EXPECT_EQ(1, construct_count); // still same tablet, reused

  // Simulate OB_EAGAIN: redo entry fails, guard_cache_.reset() is called
  simulate_redo_end_reset(cache, deconstruct_count);
  EXPECT_FALSE(cache.is_storage_guard_constructed_);
  EXPECT_EQ(1, construct_count);
  EXPECT_EQ(1, deconstruct_count); // balanced after first attempt

  // --- Second attempt (retry): all 3 rows succeed ---
  // Guard must be re-constructed from scratch since it was reset

  simulate_prepare_memtable(cache, &tablet_a, replay_scn,
                            construct_count, deconstruct_count);
  EXPECT_EQ(2, construct_count); // new construct

  simulate_prepare_memtable(cache, &tablet_a, replay_scn,
                            construct_count, deconstruct_count);
  EXPECT_EQ(2, construct_count); // reuse

  simulate_prepare_memtable(cache, &tablet_a, replay_scn,
                            construct_count, deconstruct_count);
  EXPECT_EQ(2, construct_count); // reuse

  // Redo entry succeeds, final reset
  simulate_redo_end_reset(cache, deconstruct_count);
  EXPECT_FALSE(cache.is_storage_guard_constructed_);
  EXPECT_EQ(2, construct_count);
  EXPECT_EQ(2, deconstruct_count); // balanced overall
}

// (d-variant) EAGAIN on a different tablet mid-redo: tablet switch + retry.
// Pattern: A, B (EAGAIN), then retry: A, B succeed.
TEST_F(TestGuardCache, replay_pattern_eagain_on_tablet_switch)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  ObTxReplayExecutor::ObReplayHandleCache cache;
  ObTablet tablet_a, tablet_b;
  SCN replay_scn;
  replay_scn.convert_for_gts(5000);

  int construct_count = 0;
  int deconstruct_count = 0;

  // --- First attempt ---
  // Row 0: tablet_a succeeds
  simulate_prepare_memtable(cache, &tablet_a, replay_scn,
                            construct_count, deconstruct_count);
  EXPECT_EQ(1, construct_count);
  EXPECT_EQ(0, deconstruct_count);

  // Row 1: tablet_b — guard reconstructs, then gets OB_EAGAIN
  simulate_prepare_memtable(cache, &tablet_b, replay_scn,
                            construct_count, deconstruct_count);
  EXPECT_EQ(2, construct_count);
  EXPECT_EQ(1, deconstruct_count); // old guard for tablet_a destructed

  // OB_EAGAIN: entire redo fails, reset
  simulate_redo_end_reset(cache, deconstruct_count);
  EXPECT_EQ(2, construct_count);
  EXPECT_EQ(2, deconstruct_count); // balanced

  // --- Second attempt (retry from beginning) ---
  simulate_prepare_memtable(cache, &tablet_a, replay_scn,
                            construct_count, deconstruct_count);
  EXPECT_EQ(3, construct_count);

  simulate_prepare_memtable(cache, &tablet_b, replay_scn,
                            construct_count, deconstruct_count);
  EXPECT_EQ(4, construct_count);
  EXPECT_EQ(3, deconstruct_count);

  simulate_redo_end_reset(cache, deconstruct_count);
  EXPECT_EQ(4, construct_count);
  EXPECT_EQ(4, deconstruct_count); // balanced overall
}

} // end namespace unittest
} // end namespace oceanbase

using namespace oceanbase;
using namespace oceanbase::common;

int main(int argc, char **argv)
{
  int ret = 1;
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_replay_guard_cache.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc, argv);
  ret = RUN_ALL_TESTS();
  return ret;
}
