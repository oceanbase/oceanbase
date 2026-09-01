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

#define USING_LOG_PREFIX STORAGETEST

#define private public
#include "storage/tx_storage/ob_log_replica_checkpoint_ctx.h"
#undef private

namespace oceanbase
{
namespace storage
{
namespace checkpoint
{

TEST(TestCheckpointDelayTracker, calculate_disk_pressure_safe_lsn)
{
  const palf::offset_t block_size = palf::PALF_BLOCK_SIZE;
  const palf::LSN begin_lsn(10 * block_size);
  palf::LSN safe_lsn;

  // Clamp to local_end_lsn when the retained range is shorter than the
  // minimum useful two-block advance.
  EXPECT_EQ(OB_SUCCESS,
      ObLogReplicaCheckpointCtx::calculate_disk_pressure_safe_lsn_(
          begin_lsn, begin_lsn + block_size - 1, safe_lsn));
  EXPECT_EQ(begin_lsn.val_ + block_size - 1, safe_lsn.val_);

  // The same clamp applies at exactly one retained block.
  EXPECT_EQ(OB_SUCCESS,
      ObLogReplicaCheckpointCtx::calculate_disk_pressure_safe_lsn_(
          begin_lsn, begin_lsn + block_size, safe_lsn));
  EXPECT_EQ(begin_lsn.val_ + block_size, safe_lsn.val_);

  // The minimum-two-block rule applies until 5% naturally reaches two blocks.
  EXPECT_EQ(OB_SUCCESS,
      ObLogReplicaCheckpointCtx::calculate_disk_pressure_safe_lsn_(
          begin_lsn, begin_lsn + 20 * block_size - 1, safe_lsn));
  EXPECT_EQ(begin_lsn.val_ + 2 * block_size, safe_lsn.val_);

  // A one-block 5% result is also raised to the two-block minimum.
  EXPECT_EQ(OB_SUCCESS,
      ObLogReplicaCheckpointCtx::calculate_disk_pressure_safe_lsn_(
          begin_lsn, begin_lsn + 20 * block_size, safe_lsn));
  EXPECT_EQ(begin_lsn.val_ + 2 * block_size, safe_lsn.val_);

  // Just below the threshold, round up to the two-block minimum.
  EXPECT_EQ(OB_SUCCESS,
      ObLogReplicaCheckpointCtx::calculate_disk_pressure_safe_lsn_(
          begin_lsn, begin_lsn + 40 * block_size - 1, safe_lsn));
  EXPECT_EQ(begin_lsn.val_ + 2 * block_size, safe_lsn.val_);

  // At and above the threshold, retain the byte-precise 5% safe point;
  // locate_by_lsn_coarsely() applies the block granularity at the call site.
  EXPECT_EQ(OB_SUCCESS,
      ObLogReplicaCheckpointCtx::calculate_disk_pressure_safe_lsn_(
          begin_lsn, begin_lsn + 40 * block_size, safe_lsn));
  EXPECT_EQ(begin_lsn.val_ + 2 * block_size, safe_lsn.val_);
  EXPECT_EQ(OB_SUCCESS,
      ObLogReplicaCheckpointCtx::calculate_disk_pressure_safe_lsn_(
          begin_lsn, begin_lsn + 40 * block_size + 20, safe_lsn));
  EXPECT_EQ(begin_lsn.val_ + 2 * block_size + 1, safe_lsn.val_);

  // An empty PALF can report an unaligned tail as both begin and end.
  const palf::LSN unaligned_empty_lsn(begin_lsn.val_ + 12345);
  EXPECT_EQ(OB_SUCCESS,
      ObLogReplicaCheckpointCtx::calculate_disk_pressure_safe_lsn_(
          unaligned_empty_lsn, unaligned_empty_lsn, safe_lsn));
  EXPECT_EQ(unaligned_empty_lsn.val_, safe_lsn.val_);

  // Validate ordering before subtracting unsigned LSN offsets.
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      ObLogReplicaCheckpointCtx::calculate_disk_pressure_safe_lsn_(
          begin_lsn + 1, begin_lsn, safe_lsn));
  EXPECT_FALSE(safe_lsn.is_valid());

  // Dividing the span before adding it to begin also remains safe near the
  // upper end of the valid LSN range.
  const palf::LSN high_begin_lsn(
      (palf::LOG_MAX_BLOCK_ID - 40) * block_size);
  EXPECT_EQ(OB_SUCCESS,
      ObLogReplicaCheckpointCtx::calculate_disk_pressure_safe_lsn_(
          high_begin_lsn, high_begin_lsn + 20 * block_size, safe_lsn));
  EXPECT_EQ(high_begin_lsn.val_ + 2 * block_size, safe_lsn.val_);
}

TEST(TestCheckpointDelayTracker, state_transition)
{
  const share::ObLSID ls_id(1);
  const share::ObLSID another_ls_id(2);
  const int64_t start_ts = 1000;
  const int64_t report_interval_us = 100;
  share::SCN checkpoint_scn;
  share::SCN advanced_checkpoint_scn;
  ASSERT_EQ(OB_SUCCESS, checkpoint_scn.convert_for_logservice(100));
  ASSERT_EQ(OB_SUCCESS, advanced_checkpoint_scn.convert_for_logservice(101));

  ObLogReplicaCheckpointDelayReporter reporter;

  // The first observation only starts tracking this checkpoint.
  EXPECT_FALSE(reporter.need_report_log_replica_checkpoint_delay_(ls_id,
      checkpoint_scn, start_ts, report_interval_us));
  ASSERT_TRUE(reporter.checkpoint_delay_report_info_map_.created());
  ASSERT_EQ(1, reporter.checkpoint_delay_report_info_map_.size());

  // Report only after the checkpoint has remained unchanged for the full threshold.
  EXPECT_FALSE(reporter.need_report_log_replica_checkpoint_delay_(ls_id,
      checkpoint_scn, start_ts + 99, report_interval_us));
  EXPECT_TRUE(reporter.need_report_log_replica_checkpoint_delay_(ls_id,
      checkpoint_scn, start_ts + 100, report_interval_us));

  // Rate limit subsequent reports.
  EXPECT_FALSE(reporter.need_report_log_replica_checkpoint_delay_(ls_id,
      checkpoint_scn, start_ts + 199, report_interval_us));
  EXPECT_TRUE(reporter.need_report_log_replica_checkpoint_delay_(ls_id,
      checkpoint_scn, start_ts + 200, report_interval_us));

  // Advancing the checkpoint starts a new unchanged interval.
  EXPECT_FALSE(reporter.need_report_log_replica_checkpoint_delay_(ls_id,
      advanced_checkpoint_scn, start_ts + 201, report_interval_us));
  EXPECT_FALSE(reporter.need_report_log_replica_checkpoint_delay_(ls_id,
      advanced_checkpoint_scn, start_ts + 300, report_interval_us));
  EXPECT_TRUE(reporter.need_report_log_replica_checkpoint_delay_(ls_id,
      advanced_checkpoint_scn, start_ts + 301, report_interval_us));

  // Treat a clock rollback conservatively as a new observation interval.
  EXPECT_FALSE(reporter.need_report_log_replica_checkpoint_delay_(ls_id,
      advanced_checkpoint_scn, start_ts + 280, report_interval_us));
  EXPECT_TRUE(reporter.need_report_log_replica_checkpoint_delay_(ls_id,
      advanced_checkpoint_scn, start_ts + 380, report_interval_us));

  // Each LS has an independent observation interval.
  EXPECT_FALSE(reporter.need_report_log_replica_checkpoint_delay_(another_ls_id,
      checkpoint_scn, start_ts + 380, report_interval_us));
  EXPECT_TRUE(reporter.need_report_log_replica_checkpoint_delay_(another_ls_id,
      checkpoint_scn, start_ts + 480, report_interval_us));
}

} // namespace checkpoint
} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_checkpoint_delay_tracker.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
