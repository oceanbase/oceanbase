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
#include <iostream>
#include <vector>
#include "lib/time/ob_time_utility.h"
#define private public
#define protected public
#include "logservice/replayservice/ob_replay_status.h"
#include "logservice/replayservice/ob_log_replay_service.h"
#undef protected
#undef private

using namespace std;
using namespace oceanbase::common;
namespace oceanbase
{
using namespace common;
using namespace logservice;
using namespace share;
using namespace palf;
using namespace storage;
namespace unittest
{

TEST(TestObReplayService, calc_replay_queue_size)
{
  EXPECT_EQ(MIN_REPLAY_TASK_QUEUE_SIZE, ObLogReplayService::calc_replay_queue_size(-1));
  EXPECT_EQ(MIN_REPLAY_TASK_QUEUE_SIZE, ObLogReplayService::calc_replay_queue_size(0));
  EXPECT_EQ(MIN_REPLAY_TASK_QUEUE_SIZE, ObLogReplayService::calc_replay_queue_size(8));
  EXPECT_EQ(MIN_REPLAY_TASK_QUEUE_SIZE, ObLogReplayService::calc_replay_queue_size(16));
  EXPECT_EQ(80, ObLogReplayService::calc_replay_queue_size(72));
  EXPECT_EQ(MAX_REPLAY_TASK_QUEUE_SIZE, ObLogReplayService::calc_replay_queue_size(128));
  EXPECT_EQ(MAX_REPLAY_TASK_QUEUE_SIZE, ObLogReplayService::calc_replay_queue_size(1024));
}

TEST(TestObReplayService, replay_queue_idx_non_power_of_two)
{
  ObReplayStatus status;
  const int64_t queue_size = 44;
  const int64_t round_count = 10;
  std::vector<int64_t> counts(queue_size, 0);
  ATOMIC_STORE(&status.effective_queue_size_, queue_size);

  for (int64_t replay_hint = 0; replay_hint < queue_size * round_count; ++replay_hint) {
    const int64_t queue_idx = status.calc_replay_queue_idx(replay_hint);
    ASSERT_GE(queue_idx, 0);
    ASSERT_LT(queue_idx, queue_size);
    ++counts[queue_idx];
  }
  for (int64_t i = 0; i < queue_size; ++i) {
    EXPECT_EQ(round_count, counts[i]);
  }
}

TEST(TestObReplayService, pre_barrier_ref_matches_broadcast_queue_count)
{
  ObReplayStatus status;
  const int64_t queue_size = 33;
  const int64_t replay_hint = 100;
  char payload = '\0';
  ObLogReplayBuffer replay_buf;
  ObLogReplayTask task;

  status.is_inited_ = true;
  ATOMIC_STORE(&status.effective_queue_size_, queue_size);
  replay_buf.log_buf_ = &payload;
  task.is_pre_barrier_ = true;
  task.replay_hint_ = replay_hint;
  ASSERT_EQ(OB_SUCCESS, task.init(&replay_buf, queue_size));
  EXPECT_EQ(queue_size, replay_buf.get_replay_ref());

  const int64_t target_queue_idx = status.calc_replay_queue_idx(replay_hint);
  ObLogReplayBuffer *out_buf = NULL;
  bool need_replay = false;
  EXPECT_EQ(OB_EAGAIN, status.check_replay_barrier(&task, out_buf, need_replay, target_queue_idx));
  EXPECT_FALSE(need_replay);
  EXPECT_EQ(queue_size, replay_buf.get_replay_ref());

  for (int64_t i = 0; i < queue_size; ++i) {
    if (i != target_queue_idx) {
      out_buf = NULL;
      need_replay = false;
      EXPECT_EQ(OB_SUCCESS, status.check_replay_barrier(&task, out_buf, need_replay, i));
      EXPECT_EQ(&replay_buf, out_buf);
      EXPECT_FALSE(need_replay);
    }
  }
  EXPECT_EQ(1, replay_buf.get_replay_ref());

  out_buf = NULL;
  need_replay = false;
  EXPECT_EQ(OB_SUCCESS, status.check_replay_barrier(&task, out_buf, need_replay, target_queue_idx));
  EXPECT_EQ(&replay_buf, out_buf);
  EXPECT_TRUE(need_replay);
  EXPECT_EQ(0, replay_buf.get_replay_ref());
  status.is_inited_ = false;
}
class MockReplayService : public ObLogReplayService
{
public:
  int64_t mock_queue_size_;
  MockReplayService() : mock_queue_size_(MIN_REPLAY_TASK_QUEUE_SIZE) {}
  int64_t calc_replay_queue_size() const override { return mock_queue_size_; }
};

static void setup_replay_status_for_reload_test(ObReplayStatus &status,
                                                 MockReplayService &mock_sv,
                                                 const int64_t effective_queue_size)
{
  status.is_inited_ = true;
  status.is_enabled_ = true;
  status.rp_sv_ = &mock_sv;
  ATOMIC_STORE(&status.effective_queue_size_, effective_queue_size);
  status.submit_log_task_.base_scn_ = SCN::min_scn();
  SCN scn;
  scn.convert_for_logservice(1000);
  status.submit_log_task_.next_to_submit_scn_ = scn;
}

TEST(TestObReplayService, reload_barrier_same_size_clears_flag)
{
  ObReplayStatus status;
  MockReplayService mock_sv;
  const int64_t cur_size = 64;
  mock_sv.mock_queue_size_ = cur_size;
  setup_replay_status_for_reload_test(status, mock_sv, cur_size);

  status.need_reload_queue_size_ = true;
  EXPECT_EQ(OB_SUCCESS, status.check_reload_queue_size_before_submit());
  EXPECT_FALSE(status.need_reload_queue_size_);
  EXPECT_EQ(cur_size, ATOMIC_LOAD(&status.effective_queue_size_));
  status.is_inited_ = false;
  status.is_enabled_ = false;
  status.rp_sv_ = NULL;
}

TEST(TestObReplayService, reload_waits_pending_tasks_then_switches)
{
  ObReplayStatus status;
  MockReplayService mock_sv;
  const int64_t old_size = 64;
  const int64_t new_size = 80;
  mock_sv.mock_queue_size_ = new_size;
  setup_replay_status_for_reload_test(status, mock_sv, old_size);

  // Phase 1: pending tasks are not drained, keep waiting.
  status.need_reload_queue_size_ = true;
  ATOMIC_STORE(&status.pending_task_count_, 1);
  EXPECT_EQ(OB_EAGAIN, status.check_reload_queue_size_before_submit());
  EXPECT_EQ(old_size, ATOMIC_LOAD(&status.effective_queue_size_));
  EXPECT_TRUE(status.need_reload_queue_size_);

  // Phase 2: pending tasks are drained, switch effective_queue_size_.
  ATOMIC_STORE(&status.pending_task_count_, 0);
  EXPECT_EQ(OB_SUCCESS, status.check_reload_queue_size_before_submit());
  EXPECT_EQ(new_size, ATOMIC_LOAD(&status.effective_queue_size_));
  EXPECT_FALSE(status.need_reload_queue_size_);

  status.is_inited_ = false;
  status.is_enabled_ = false;
  status.rp_sv_ = NULL;
}

TEST(TestObReplayService, reload_without_pending_tasks_switches_immediately)
{
  ObReplayStatus status;
  MockReplayService mock_sv;
  const int64_t old_size = 64;
  const int64_t new_size = 80;
  mock_sv.mock_queue_size_ = new_size;
  setup_replay_status_for_reload_test(status, mock_sv, old_size);

  status.need_reload_queue_size_ = true;
  ATOMIC_STORE(&status.pending_task_count_, 0);
  EXPECT_EQ(OB_SUCCESS, status.check_reload_queue_size_before_submit());
  EXPECT_FALSE(status.need_reload_queue_size_);
  EXPECT_EQ(new_size, ATOMIC_LOAD(&status.effective_queue_size_));

  status.is_inited_ = false;
  status.is_enabled_ = false;
  status.rp_sv_ = NULL;
}

} // end of unittest
} // end of oceanbase

int main(int argc, char **argv)
{
  UNUSED(argc);
  UNUSED(argv);

  OB_LOGGER.set_file_name("test_ob_replay_service.log", true);
  OB_LOGGER.set_log_level("TRACE");
  PALF_LOG(INFO, "begin unittest::test_ob_replay_service");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
