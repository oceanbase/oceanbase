/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE_COMPACTION
#include <gmock/gmock.h>

#define private public
#define protected public
#include "storage/column_store/ob_co_merge_dag.h"
#undef protected
#undef private

namespace oceanbase
{
using namespace common;
using namespace share;

namespace compaction
{
namespace
{

class TestCOMergeDagState : public ::testing::Test
{
protected:
  static ObCOMergeExeDag::CGMergeStatus invalid_cg_status()
  {
    return static_cast<ObCOMergeExeDag::CGMergeStatus>(
        static_cast<int>(ObCOMergeExeDag::CG_SSTABLE_CREATED) + 1);
  }

  static ObCOMergeExeDag::RangeMergeStatus invalid_range_status()
  {
    return static_cast<ObCOMergeExeDag::RangeMergeStatus>(
        static_cast<int>(ObCOMergeExeDag::RANGE_PERSIST_FINISH) + 1);
  }
};

TEST_F(TestCOMergeDagState, schedule_dag_warning_allowlist)
{
  EXPECT_TRUE(ObCOMergeScheduleDag::can_ignore_warning(OB_ALLOCATE_MEMORY_FAILED));
  EXPECT_TRUE(ObCOMergeScheduleDag::can_ignore_warning(OB_EAGAIN));
  EXPECT_TRUE(ObCOMergeScheduleDag::can_ignore_warning(OB_SIZE_OVERFLOW));

  EXPECT_FALSE(ObCOMergeScheduleDag::can_ignore_warning(OB_SUCCESS));
  EXPECT_FALSE(ObCOMergeScheduleDag::can_ignore_warning(OB_INVALID_ARGUMENT));
  EXPECT_FALSE(ObCOMergeScheduleDag::can_ignore_warning(OB_ERR_UNEXPECTED));
}

TEST_F(TestCOMergeDagState, prepare_replay_status_rejects_invalid_dimensions)
{
  {
    ObCOMergeExeDag dag;
    EXPECT_EQ(OB_ERR_UNEXPECTED, dag.prepare_replay_status(0, 1));
    EXPECT_EQ(nullptr, dag.range_status_);
    EXPECT_EQ(nullptr, dag.cg_merge_status_);
  }
  {
    ObCOMergeExeDag dag;
    EXPECT_EQ(OB_ERR_UNEXPECTED, dag.prepare_replay_status(1, 0));
    EXPECT_EQ(nullptr, dag.range_status_);
    EXPECT_EQ(nullptr, dag.cg_merge_status_);
  }
  {
    ObCOMergeExeDag dag;
    EXPECT_EQ(OB_ERR_UNEXPECTED, dag.prepare_replay_status(-1, 1));
    EXPECT_EQ(nullptr, dag.range_status_);
    EXPECT_EQ(nullptr, dag.cg_merge_status_);
  }
  {
    ObCOMergeExeDag dag;
    EXPECT_EQ(OB_ERR_UNEXPECTED, dag.prepare_replay_status(1, -1));
    EXPECT_EQ(nullptr, dag.range_status_);
    EXPECT_EQ(nullptr, dag.cg_merge_status_);
  }
}

TEST_F(TestCOMergeDagState, prepare_replay_status_initializes_every_entry)
{
  ObCOMergeExeDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.prepare_replay_status(2, 3));

  ASSERT_NE(nullptr, dag.range_status_);
  ASSERT_NE(nullptr, dag.cg_merge_status_);
  EXPECT_EQ(2, dag.range_count_);
  EXPECT_EQ(3, dag.cg_count_);
  EXPECT_EQ(0, dag.inner_get_persisted_range_count());
  EXPECT_EQ(0, dag.inner_get_replayed_cg_count());
  EXPECT_FALSE(dag.check_replay_finished());

  for (int64_t range_idx = 0; range_idx < dag.range_count_; ++range_idx) {
    EXPECT_EQ(ObCOMergeExeDag::RANGE_NEED_PERSIST, dag.range_status_[range_idx]);
    for (int64_t cg_idx = 0; cg_idx < dag.cg_count_; ++cg_idx) {
      ObCOMergeExeDag::CGMergeStatus status = ObCOMergeExeDag::CG_SSTABLE_CREATED;
      ASSERT_EQ(OB_SUCCESS, dag.get_cg_merge_status(range_idx, cg_idx, status));
      EXPECT_EQ(ObCOMergeExeDag::CG_NEED_REPLAY, status);
    }
  }
}

TEST_F(TestCOMergeDagState, cg_status_transitions_and_finished_check)
{
  ObCOMergeExeDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.prepare_replay_status(2, 3));

  ASSERT_EQ(OB_SUCCESS, dag.set_cg_merge_status(
      0, 0, 3, ObCOMergeExeDag::CG_REPLAY_FINISH));
  EXPECT_EQ(3, dag.inner_get_replayed_cg_count());
  EXPECT_FALSE(dag.check_replay_finished());

  ASSERT_EQ(OB_SUCCESS, dag.set_cg_merge_status(
      1, 0, 1, ObCOMergeExeDag::CG_SKIP_REPLAY));
  ASSERT_EQ(OB_SUCCESS, dag.set_cg_merge_status(
      1, 1, 2, ObCOMergeExeDag::CG_REPLAY_FAILED));
  ASSERT_EQ(OB_SUCCESS, dag.set_cg_merge_status(
      1, 2, 3, ObCOMergeExeDag::CG_IS_REPLAYING));
  EXPECT_EQ(3, dag.inner_get_replayed_cg_count());
  EXPECT_FALSE(dag.check_replay_finished());

  ASSERT_EQ(OB_SUCCESS, dag.set_cg_merge_status(
      1, 0, 2, ObCOMergeExeDag::CG_REPLAY_FINISH));
  EXPECT_EQ(5, dag.inner_get_replayed_cg_count());
  EXPECT_FALSE(dag.check_replay_finished());

  ASSERT_EQ(OB_SUCCESS, dag.set_cg_merge_status(
      1, 2, 3, ObCOMergeExeDag::CG_SSTABLE_CREATED));
  EXPECT_EQ(6, dag.inner_get_replayed_cg_count());
  EXPECT_TRUE(dag.check_replay_finished());

  ObCOMergeExeDag::CGMergeStatus status = ObCOMergeExeDag::CG_NEED_REPLAY;
  ASSERT_EQ(OB_SUCCESS, dag.get_cg_merge_status(1, 2, status));
  EXPECT_EQ(ObCOMergeExeDag::CG_SSTABLE_CREATED, status);
}

TEST_F(TestCOMergeDagState, cg_status_rejects_invalid_indices_and_status)
{
  ObCOMergeExeDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.prepare_replay_status(2, 3));

  ObCOMergeExeDag::CGMergeStatus status = ObCOMergeExeDag::CG_REPLAY_FINISH;
  EXPECT_EQ(OB_INVALID_ARGUMENT, dag.get_cg_merge_status(-1, 0, status));
  EXPECT_EQ(OB_INVALID_ARGUMENT, dag.get_cg_merge_status(2, 0, status));
  EXPECT_EQ(OB_INVALID_ARGUMENT, dag.get_cg_merge_status(0, -1, status));
  EXPECT_EQ(OB_INVALID_ARGUMENT, dag.get_cg_merge_status(0, 3, status));

  EXPECT_EQ(OB_INVALID_ARGUMENT, dag.set_cg_merge_status(
      -1, 0, 1, ObCOMergeExeDag::CG_REPLAY_FINISH));
  EXPECT_EQ(OB_INVALID_ARGUMENT, dag.set_cg_merge_status(
      2, 0, 1, ObCOMergeExeDag::CG_REPLAY_FINISH));
  EXPECT_EQ(OB_INVALID_ARGUMENT, dag.set_cg_merge_status(
      0, 1, 1, ObCOMergeExeDag::CG_REPLAY_FINISH));
  EXPECT_EQ(OB_INVALID_ARGUMENT, dag.set_cg_merge_status(
      0, 2, 1, ObCOMergeExeDag::CG_REPLAY_FINISH));
  EXPECT_EQ(OB_INVALID_ARGUMENT, dag.set_cg_merge_status(
      0, 0, 4, ObCOMergeExeDag::CG_REPLAY_FINISH));
  EXPECT_EQ(OB_INVALID_ARGUMENT, dag.set_cg_merge_status(
      0, 0, 1, invalid_cg_status()));

  EXPECT_EQ(0, dag.inner_get_replayed_cg_count());
  EXPECT_FALSE(dag.check_replay_finished());
}

// Regression test for the lower-bound check in set_cg_merge_status().  Without
// it, cg_merge_status_[-1] aliases the last byte of range_status_ and corrupts
// range state.
TEST_F(TestCOMergeDagState, cg_status_rejects_negative_start_index)
{
  ObCOMergeExeDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.prepare_replay_status(2, 3));
  ASSERT_EQ(OB_SUCCESS, dag.set_range_merge_status(
      1, ObCOMergeExeDag::RANGE_PERSIST_FINISH));

  EXPECT_EQ(OB_INVALID_ARGUMENT, dag.set_cg_merge_status(
      0, -1, 0, ObCOMergeExeDag::CG_REPLAY_FINISH));
  EXPECT_EQ(ObCOMergeExeDag::RANGE_PERSIST_FINISH, dag.range_status_[1]);

  bool all_cg_finished = false;
  EXPECT_EQ(OB_INVALID_ARGUMENT, dag.finish_replay(0, -1, 0, all_cg_finished));
  EXPECT_FALSE(all_cg_finished);
}

TEST_F(TestCOMergeDagState, range_status_transitions_and_validation)
{
  ObCOMergeExeDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.prepare_replay_status(3, 2));

  EXPECT_EQ(0, dag.inner_get_persisted_range_count());
  ASSERT_EQ(OB_SUCCESS, dag.set_range_merge_status(
      0, ObCOMergeExeDag::RANGE_PERSIST_FINISH));
  ASSERT_EQ(OB_SUCCESS, dag.set_range_merge_status(
      2, ObCOMergeExeDag::RANGE_PERSIST_FINISH));
  EXPECT_EQ(2, dag.inner_get_persisted_range_count());

  ASSERT_EQ(OB_SUCCESS, dag.set_range_merge_status(
      0, ObCOMergeExeDag::RANGE_NEED_PERSIST));
  EXPECT_EQ(1, dag.inner_get_persisted_range_count());

  EXPECT_EQ(OB_INVALID_ARGUMENT, dag.set_range_merge_status(
      -1, ObCOMergeExeDag::RANGE_PERSIST_FINISH));
  EXPECT_EQ(OB_INVALID_ARGUMENT, dag.set_range_merge_status(
      3, ObCOMergeExeDag::RANGE_PERSIST_FINISH));
  EXPECT_EQ(OB_INVALID_ARGUMENT, dag.set_range_merge_status(
      1, invalid_range_status()));
  EXPECT_EQ(1, dag.inner_get_persisted_range_count());
}

TEST_F(TestCOMergeDagState, merge_batch_initialization)
{
  ObCOMergeExeDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.prepare_replay_status(1, 25));

  ASSERT_EQ(OB_SUCCESS, dag.init_merge_batch_size(false));
  EXPECT_EQ(12, dag.merge_batch_size_);

  ASSERT_EQ(OB_SUCCESS, dag.init_merge_batch_size(true));
  EXPECT_EQ(1, dag.merge_batch_size_);
}

TEST_F(TestCOMergeDagState, retry_strategy_classification)
{
  ObCOMergeExeDag dag;
  ObIDag::ObDagRetryStrategy strategy = ObIDag::DAG_SKIP_RETRY;

  ASSERT_EQ(OB_SUCCESS, dag.decide_retry_strategy(OB_SUCCESS, strategy));
  EXPECT_EQ(ObIDag::DAG_CAN_RETRY, strategy);

  ASSERT_EQ(OB_SUCCESS, dag.decide_retry_strategy(OB_EAGAIN, strategy));
  EXPECT_EQ(ObIDag::DAG_CAN_RETRY, strategy);

  ASSERT_EQ(OB_SUCCESS, dag.decide_retry_strategy(OB_TRANS_CTX_NOT_EXIST, strategy));
  EXPECT_EQ(ObIDag::DAG_AND_DAG_NET_SKIP_RETRY, strategy);

  ASSERT_EQ(OB_SUCCESS, dag.decide_retry_strategy(OB_SERVER_OUTOF_DISK_SPACE, strategy));
  EXPECT_EQ(ObIDag::DAG_AND_DAG_NET_SKIP_RETRY, strategy);
}

TEST_F(TestCOMergeDagState, retry_reset_checks_prerequisites_before_mutation)
{
  ObCOMergeExeDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.prepare_replay_status(1, 2));
  ASSERT_EQ(OB_SUCCESS, dag.set_cg_merge_status(
      0, 0, 1, ObCOMergeExeDag::CG_IS_REPLAYING));
  dag.merge_batch_size_ = 8;
  dag.set_need_reduce_batch();

  EXPECT_EQ(OB_NOT_INIT, dag.inner_reset_status_for_retry());
  EXPECT_EQ(8, dag.merge_batch_size_);
  EXPECT_TRUE(dag.need_reduce_batch_);

  ObCOMergeExeDag::CGMergeStatus status = ObCOMergeExeDag::CG_NEED_REPLAY;
  ASSERT_EQ(OB_SUCCESS, dag.get_cg_merge_status(0, 0, status));
  EXPECT_EQ(ObCOMergeExeDag::CG_IS_REPLAYING, status);

  dag.ObTabletMergeDag::is_inited_ = true;
  EXPECT_EQ(OB_ERR_UNEXPECTED, dag.inner_reset_status_for_retry());
  EXPECT_EQ(8, dag.merge_batch_size_);
  EXPECT_TRUE(dag.need_reduce_batch_);
  ASSERT_EQ(OB_SUCCESS, dag.get_cg_merge_status(0, 0, status));
  EXPECT_EQ(ObCOMergeExeDag::CG_IS_REPLAYING, status);
}

} // namespace
} // namespace compaction
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("WARN");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
