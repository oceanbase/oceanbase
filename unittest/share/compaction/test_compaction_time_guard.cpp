/**
 * Copyright (c) 2024 OceanBase
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
#define USING_LOG_PREFIX STORAGE
#define private public
#define protected public

#include "share/compaction/ob_compaction_time_guard.h"

namespace oceanbase
{
namespace unittest
{

using namespace compaction;

class TestCompactionTimeGuard : public ::testing::Test
{
public:
  TestCompactionTimeGuard() {}
  virtual ~TestCompactionTimeGuard() {}
};

TEST_F(TestCompactionTimeGuard, basic_time_guard)
{
  const uint16_t CAPACITY = ObCompactionTimeGuard::CAPACITY;
  const uint64_t current_time = common::ObTimeUtility::current_time();
  ObCompactionTimeGuard time_guard;
  // construction
  ASSERT_EQ(CAPACITY, time_guard.capacity_);
  ASSERT_EQ(0, time_guard.size_);
  ASSERT_TRUE(time_guard.is_empty());
  time_guard.set_last_click_ts(current_time);
  ASSERT_EQ(current_time, time_guard.add_time_);
  ASSERT_EQ(current_time, time_guard.last_click_ts_);

  // click
  ASSERT_TRUE(time_guard.click(CAPACITY));
  ASSERT_TRUE(time_guard.is_empty());
  for (uint16_t i = 0; i < CAPACITY; ++i) {
    ASSERT_TRUE(time_guard.click(i));
    ASSERT_EQ(time_guard.size_, i + 1);
    ASSERT_EQ(time_guard.event_times_[i], time_guard.get_specified_cost_time(i));
  }
  ASSERT_TRUE(time_guard.click(CAPACITY + 1));
  ASSERT_EQ(CAPACITY, time_guard.size_);

  // add and assignment
  ObCompactionTimeGuard other_guard;
  ASSERT_EQ(CAPACITY, other_guard.capacity_);
  ASSERT_TRUE(other_guard.is_empty());
  for (uint16_t i = 0; i < CAPACITY; ++i) {
    ASSERT_EQ(0, other_guard.event_times_[i]);
  }
  other_guard = time_guard;
  ASSERT_EQ(time_guard.guard_type_, other_guard.guard_type_);
  ASSERT_EQ(time_guard.warn_threshold_, other_guard.warn_threshold_);
  ASSERT_EQ(time_guard.capacity_, other_guard.capacity_);
  ASSERT_EQ(time_guard.size_, other_guard.size_);
  ASSERT_EQ(time_guard.last_click_ts_, other_guard.last_click_ts_);
  ASSERT_EQ(time_guard.add_time_, other_guard.add_time_);
  for (uint16_t i = 0; i < CAPACITY; ++i) {
    ASSERT_EQ(time_guard.event_times_[i], other_guard.event_times_[i]);
  }

  // add time guard
  ObCompactionTimeGuard summary_guard;
  for (uint16_t i = 0; i < 2 * CAPACITY; i++) {
    summary_guard.add_time_guard(time_guard);
    summary_guard.add_time_guard(other_guard);
  }
  for (uint16_t i = 0; i < CAPACITY; ++i) {
    ASSERT_EQ(summary_guard.event_times_[i], time_guard.event_times_[i] * 4 * CAPACITY);
    ASSERT_EQ(summary_guard.event_times_[i], other_guard.event_times_[i] *  4 * CAPACITY);
  }

  // reuse
  ObCompactionTimeGuard* guards[3] = {&time_guard, &other_guard, &summary_guard};
  for (int i = 0; i < 3; i++) {
    guards[i]->reuse();
    ASSERT_TRUE(guards[i]->is_empty());
    ASSERT_EQ(CAPACITY, guards[i]->capacity_);
    ASSERT_EQ(0, guards[i]->size_);
    for (uint16_t j = 0; j < CAPACITY; ++j) {
      ASSERT_EQ(0, guards[i]->event_times_[j]);
    }
  }
  // out of order click and add time guard with diffrent size_
  time_guard.click(0);
  ASSERT_EQ(1, time_guard.size_);
  time_guard.click(2);
  ASSERT_EQ(3, time_guard.size_);
  time_guard.click(1);
  ASSERT_EQ(3, time_guard.size_);
  time_guard.click(10);
  ASSERT_EQ(11, time_guard.size_);
  other_guard.click(6);
  ASSERT_EQ(7, other_guard.size_);
  summary_guard.add_time_guard(other_guard);
  ASSERT_EQ(summary_guard.size_, other_guard.size_);
  summary_guard.add_time_guard(time_guard);
  ASSERT_EQ(summary_guard.size_, time_guard.size_);
}

TEST_F(TestCompactionTimeGuard, time_guard_to_string)
{
  ObStorageCompactionTimeGuard storage_guard;
  for (uint16_t i = 0; i < ObStorageCompactionTimeGuard::COMPACTION_EVENT_MAX; i++) {
    storage_guard.click(i);
    storage_guard.event_times_[i] += ObStorageCompactionTimeGuard::COMPACTION_SHOW_TIME_THRESHOLD;
    ASSERT_EQ(i + 1, storage_guard.size_);
  }
  ASSERT_EQ(ObStorageCompactionTimeGuard::COMPACTION_EVENT_MAX, storage_guard.size_);
  storage_guard.event_times_[ObStorageCompactionTimeGuard::DAG_WAIT_TO_SCHEDULE] += 2 * ObStorageCompactionTimeGuard::COMPACTION_SHOW_TIME_THRESHOLD;
  storage_guard.event_times_[ObStorageCompactionTimeGuard::DAG_FINISH] += 2 * ObCompactionTimeGuard::WARN_THRESHOLD;
  STORAGE_LOG(INFO, "storage guard is", K(storage_guard));

  ObRSCompactionTimeGuard rs_guard;
  for (uint16_t i = 0; i < ObRSCompactionTimeGuard::COMPACTION_EVENT_MAX; i++) {
    rs_guard.click(i);
    rs_guard.event_times_[i] += 10 * i;
    ASSERT_EQ(i + 1, rs_guard.size_);
  }
  ASSERT_EQ(ObRSCompactionTimeGuard::COMPACTION_EVENT_MAX, rs_guard.size_);
  rs_guard.event_times_[ObRSCompactionTimeGuard::CKM_VERIFICATION] += 2 * ObCompactionTimeGuard::WARN_THRESHOLD;
  STORAGE_LOG(INFO, "rs guard is", K(rs_guard));

  ObCompactionScheduleTimeGuard schedule_guard;
  for (uint16_t i = 0; i < ObCompactionScheduleTimeGuard::COMPACTION_EVENT_MAX; i++) {
    schedule_guard.click(i);
    schedule_guard.event_times_[i] += 10 * i;
    ASSERT_EQ(i + 1, schedule_guard.size_);
  }
  ASSERT_EQ(ObCompactionScheduleTimeGuard::COMPACTION_EVENT_MAX, schedule_guard.size_);
  schedule_guard.event_times_[ObCompactionScheduleTimeGuard::SCHEDULER_NEXT_ROUND] += 2 * ObCompactionTimeGuard::WARN_THRESHOLD;
  STORAGE_LOG(INFO, "schedule guard is", K(schedule_guard));
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  system("rm -f test_compaction_time_guard.log*");
  OB_LOGGER.set_file_name("test_compaction_time_guard.log");
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_max_file_size(256*1024*1024);
  return RUN_ALL_TESTS();
}
