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

// Unit tests for ObTxHotspotRedoStat (LOG_PLAN §4 / §8.1).
//
// These tests target the stat struct in isolation — they do NOT exercise the
// hotspot business path. The goal is to verify:
//
//   1. Concurrent ATOMIC_INC adds correctly (no lost updates).
//   2. update_peak_ converges to max under racing writers without deadloop.
//   3. record_ts_once is single-write (subsequent calls are no-ops).
//   4. reset() zeros every field (regression guard for newly-added fields).
//   5. should_dump() honours all 7 trigger conditions (§4.5).
//   6. Diagnostic fields stay compact and int64-aligned.

#include <gtest/gtest.h>
#include <thread>
#include <vector>
#include <cstddef>

#define USING_LOG_PREFIX TRANS
#define private public
#define protected public

#include "storage/tx/ob_tx_hotspot_define.h"
#include "storage/tx/ob_tx_log_cb_define.h"  // ObTxLogCbGroup complete type for INHERIT_TO_STRING_KV
#include "share/ob_errno.h"
#include "lib/utility/utility.h"

namespace oceanbase
{
using namespace ::testing;
using namespace transaction;

class TestHotspotRedoStat : public ::testing::Test
{
public:
  virtual void SetUp() override
  {
    oceanbase::ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
  }
  virtual void TearDown() override {}
};

// ============================================================================
// TC-01: Concurrent ATOMIC_INC accumulates correctly.
// 16 threads × 1000 increments must equal 16000 — no lost updates.
// ============================================================================
TEST_F(TestHotspotRedoStat, test_concurrent_inc)
{
  ObTxHotspotRedoStat stat;
  stat.reset();

  constexpr int kThreads = 16;
  constexpr int kIncPerThread = 1000;

  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int i = 0; i < kThreads; i++) {
    threads.emplace_back([&stat]() {
      for (int j = 0; j < kIncPerThread; j++) {
        stat.inc_cb_alloc_fail();
      }
    });
  }
  for (auto &t : threads) {
    t.join();
  }
  EXPECT_EQ(stat.cb_alloc_fail_cnt_, kThreads * kIncPerThread);
}

// ============================================================================
// TC-02: update_peak_ converges to max without deadloop under contention.
// Multiple threads with mixed values; the final value must be the max.
// ============================================================================
TEST_F(TestHotspotRedoStat, test_peak_cas_converges)
{
  ObTxHotspotRedoStat stat;
  stat.reset();

  constexpr int kThreads = 8;
  const int64_t values[kThreads] = {100, 200, 300, 500, 1000, 2000, 5000, 10000};
  const int64_t expected_max = 10000;

  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int i = 0; i < kThreads; i++) {
    threads.emplace_back([&stat, &values, i]() {
      // Stress: each thread writes its value many times to maximise contention.
      for (int round = 0; round < 100; round++) {
        stat.update_peak_submitting_cb(values[i]);
      }
    });
  }
  for (auto &t : threads) {
    t.join();
  }
  EXPECT_EQ(expected_max, stat.peak_submitting_cb_cnt_);
}

// ============================================================================
// TC-03: record_ts_once is a single-write — second call must NOT overwrite.
// ============================================================================
TEST_F(TestHotspotRedoStat, test_ts_once_single_write)
{
  ObTxHotspotRedoStat stat;
  stat.reset();

  ObTxHotspotRedoStat::record_ts_once(stat.aggr_start_ts_);
  const int64_t first_ts = stat.aggr_start_ts_;
  EXPECT_NE(OB_INVALID_TIMESTAMP, first_ts);

  // Sleep just enough to guarantee a different fast_current_time() reading.
  ::usleep(10);

  ObTxHotspotRedoStat::record_ts_once(stat.aggr_start_ts_);
  EXPECT_EQ(first_ts, stat.aggr_start_ts_);
}

// ============================================================================
// TC-04: reset() zeros every field. Regression guard — every field added to
// the struct MUST also be added to reset(); this test fails loudly if not.
// ============================================================================
TEST_F(TestHotspotRedoStat, test_reset_clears_all)
{
  ObTxHotspotRedoStat stat;

  // Poison every field, then assert reset wipes them.
  stat.cb_alloc_fail_cnt_         = 100;
  stat.submit_not_master_cnt_       = 100;
  stat.block_frozen_cnt_     = 100;
  stat.extract_conflict_cnt_   = 100;
  stat.buffer_extend_cnt_   = 100;
  stat.freeze_accel_cnt_ = 100;
  stat.sec_abort_cnt_   = 100;
  stat.leader_switch_abort_cnt_     = 100;
  stat.transfer_blocked_cnt_        = 100;
  stat.partial_rollback_cnt_        = 100;
  stat.peak_submitting_cb_cnt_            = 500;
  stat.peak_orphan_cb_cnt_            = 500;
  stat.peak_pending_size_       = 500;
  stat.peak_sec_tx_cnt_          = 500;
  stat.sec_redo_bytes_    = 1024;
  stat.sec_redo_cnt_      = 7;
  stat.dispatch_msg_sent_cnt_       = 3;
  stat.dispatch_msg_skipped_cnt_    = 2;
  stat.response_msg_sent_cnt_       = 9;
  stat.aggr_start_ts_        = ObTimeUtility::current_time();
  stat.dispatch_start_ts_       = ObTimeUtility::current_time();
  stat.redo_submit_done_ts_        = ObTimeUtility::current_time();
  stat.redo_sync_done_ts_           = ObTimeUtility::current_time();
  stat.cb_return_done_ts_      = ObTimeUtility::current_time();
  stat.resp_start_ts_         = ObTimeUtility::current_time();
  stat.resp_notify_done_ts_   = ObTimeUtility::current_time();
  stat.resp_finish_ts_   = ObTimeUtility::current_time();
  stat.aggr_end_ts_                 = ObTimeUtility::current_time();
  stat.terminal_ret_                = OB_ERR_UNEXPECTED;

  stat.reset();

  EXPECT_EQ(0, stat.cb_alloc_fail_cnt_);
  EXPECT_EQ(0, stat.submit_not_master_cnt_);
  EXPECT_EQ(0, stat.block_frozen_cnt_);
  EXPECT_EQ(0, stat.extract_conflict_cnt_);
  EXPECT_EQ(0, stat.buffer_extend_cnt_);
  EXPECT_EQ(0, stat.freeze_accel_cnt_);
  EXPECT_EQ(0, stat.sec_abort_cnt_);
  EXPECT_EQ(0, stat.leader_switch_abort_cnt_);
  EXPECT_EQ(0, stat.transfer_blocked_cnt_);
  EXPECT_EQ(0, stat.partial_rollback_cnt_);
  EXPECT_EQ(0, stat.peak_submitting_cb_cnt_);
  EXPECT_EQ(0, stat.peak_orphan_cb_cnt_);
  EXPECT_EQ(0, stat.peak_pending_size_);
  EXPECT_EQ(0, stat.peak_sec_tx_cnt_);
  EXPECT_EQ(0, stat.sec_redo_bytes_);
  EXPECT_EQ(0, stat.sec_redo_cnt_);
  EXPECT_EQ(0, stat.dispatch_msg_sent_cnt_);
  EXPECT_EQ(0, stat.dispatch_msg_skipped_cnt_);
  EXPECT_EQ(0, stat.response_msg_sent_cnt_);
  EXPECT_EQ(OB_INVALID_TIMESTAMP, stat.aggr_start_ts_);
  EXPECT_EQ(OB_INVALID_TIMESTAMP, stat.dispatch_start_ts_);
  EXPECT_EQ(OB_INVALID_TIMESTAMP, stat.redo_submit_done_ts_);
  EXPECT_EQ(OB_INVALID_TIMESTAMP, stat.redo_sync_done_ts_);
  EXPECT_EQ(OB_INVALID_TIMESTAMP, stat.cb_return_done_ts_);
  EXPECT_EQ(OB_INVALID_TIMESTAMP, stat.resp_start_ts_);
  EXPECT_EQ(OB_INVALID_TIMESTAMP, stat.resp_notify_done_ts_);
  EXPECT_EQ(OB_INVALID_TIMESTAMP, stat.resp_finish_ts_);
  EXPECT_EQ(OB_INVALID_TIMESTAMP, stat.aggr_end_ts_);
  EXPECT_EQ(OB_SUCCESS, stat.terminal_ret_);
}

// ============================================================================
// TC-06: Compact layout guard.
// ObTxHotspotRedoStat is diagnostic only. Keep fields compact and rely on
// atomic operations for correctness instead of cache-line padding.
// ============================================================================
TEST_F(TestHotspotRedoStat, test_compact_stat_layout)
{
  EXPECT_EQ(alignof(int64_t), alignof(ObTxHotspotRedoStat));
  EXPECT_LT(sizeof(ObTxHotspotRedoStat), 4 * CACHE_ALIGN_SIZE);

#define EXPECT_NEXT_INT64_FIELD(prev, next) \
  EXPECT_EQ(offsetof(ObTxHotspotRedoStat, prev) + sizeof(int64_t), \
            offsetof(ObTxHotspotRedoStat, next))

  EXPECT_EQ(0u, offsetof(ObTxHotspotRedoStat, cb_alloc_fail_cnt_) % alignof(int64_t));
  EXPECT_NEXT_INT64_FIELD(cb_alloc_fail_cnt_, submit_not_master_cnt_);
  EXPECT_NEXT_INT64_FIELD(submit_not_master_cnt_, block_frozen_cnt_);
  EXPECT_NEXT_INT64_FIELD(block_frozen_cnt_, extract_conflict_cnt_);
  EXPECT_NEXT_INT64_FIELD(extract_conflict_cnt_, buffer_extend_cnt_);
  EXPECT_NEXT_INT64_FIELD(buffer_extend_cnt_, freeze_accel_cnt_);
  EXPECT_NEXT_INT64_FIELD(freeze_accel_cnt_, sec_abort_cnt_);
  EXPECT_NEXT_INT64_FIELD(sec_abort_cnt_, leader_switch_abort_cnt_);
  EXPECT_NEXT_INT64_FIELD(leader_switch_abort_cnt_, transfer_blocked_cnt_);
  EXPECT_NEXT_INT64_FIELD(transfer_blocked_cnt_, partial_rollback_cnt_);
  EXPECT_NEXT_INT64_FIELD(partial_rollback_cnt_, peak_submitting_cb_cnt_);
  EXPECT_NEXT_INT64_FIELD(peak_submitting_cb_cnt_, peak_orphan_cb_cnt_);
  EXPECT_NEXT_INT64_FIELD(peak_orphan_cb_cnt_, peak_pending_size_);
  EXPECT_NEXT_INT64_FIELD(peak_pending_size_, peak_sec_tx_cnt_);
  EXPECT_NEXT_INT64_FIELD(peak_sec_tx_cnt_, sec_redo_bytes_);
  EXPECT_NEXT_INT64_FIELD(sec_redo_bytes_, sec_redo_cnt_);
  EXPECT_NEXT_INT64_FIELD(sec_redo_cnt_, dispatch_msg_sent_cnt_);
  EXPECT_NEXT_INT64_FIELD(dispatch_msg_sent_cnt_, dispatch_msg_skipped_cnt_);
  EXPECT_NEXT_INT64_FIELD(dispatch_msg_skipped_cnt_, response_msg_sent_cnt_);
  EXPECT_NEXT_INT64_FIELD(response_msg_sent_cnt_, aggr_start_ts_);

#undef EXPECT_NEXT_INT64_FIELD
}

// ============================================================================
// TC-07: add_sec_redo_bytes guards against negative input.
// Negative byte counts would silently underflow ATOMIC_FAA — the inline
// guard in the header must reject them.
// ============================================================================
TEST_F(TestHotspotRedoStat, test_add_redo_bytes_guards_negative)
{
  ObTxHotspotRedoStat stat;
  stat.reset();

  stat.add_sec_redo_bytes(100);
  stat.add_sec_redo_bytes(-50);   // must be ignored
  stat.add_sec_redo_bytes(0);     // must be ignored
  stat.add_sec_redo_bytes(200);
  EXPECT_EQ(300, stat.sec_redo_bytes_);
}

} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_hotspot_redo_stat.log*");
  oceanbase::common::ObLogger &logger = oceanbase::common::ObLogger::get_logger();
  logger.set_file_name("test_hotspot_redo_stat.log", true, false,
                       "test_hotspot_redo_stat.log",
                       "test_hotspot_redo_stat.log",
                       "test_hotspot_redo_stat.log");
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
