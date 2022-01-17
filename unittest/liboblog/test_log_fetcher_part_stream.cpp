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

#include "gtest/gtest.h"

#include "share/ob_define.h"
#include "storage/ob_storage_log_type.h"
#include "storage/transaction/ob_trans_log.h"

#include "liboblog/src/ob_log_fetcher_part_stream.h"
#include "test_log_fetcher_common_utils.h"

using namespace oceanbase;
using namespace common;
using namespace liboblog;
using namespace fetcher;
using namespace transaction;
using namespace storage;
using namespace clog;

namespace oceanbase
{
namespace unittest
{

/*
 * Basic Function Tests.
 */
/*
 * Half commit, half abort.
 * Fixed redo log cnt.
 */
TEST(PartitionStream, BasicTest1)
{
  int err = OB_SUCCESS;

  ObTransPrepareLog prepare_;

  // Commit half trans, whose has even idx.
  const int64_t trans_cnt = 1000;
  const int64_t commit_trans_cnt = trans_cnt / 2;
  const int64_t redo_cnt = 5;

  // Pkey.
  ObPartitionKey pkey(1000U, 1, 1);
  // Log gen.
  TransLogEntryGenerator1 log_gen(pkey);
  // Task Pool.
  ObConcurrentFIFOAllocator fifo_allocator;
  ObLogTransTaskPool<PartTransTask> task_pool;
  err = fifo_allocator.init(16 * _G_, 16 * _M_, OB_MALLOC_NORMAL_BLOCK_SIZE);
  EXPECT_EQ(OB_SUCCESS, err);
  err = task_pool.init(&fifo_allocator, 10240, 1024, 4 * 1024 * 1024, true);
  EXPECT_EQ(OB_SUCCESS, err);
  // Parser.
  MockParser1 parser;
  FetcherConfig cfg;

  // Init.
  PartitionStream ps;
  err = ps.init(pkey, &parser, &task_pool, &cfg);
  EXPECT_EQ(OB_SUCCESS, err);

  // Read logs.
  ObLogIdArray missing;
  for (int64_t idx = 0; idx < trans_cnt; ++idx) {
    // Commit trans with even idx.
    log_gen.next_trans(redo_cnt, (0 == idx % 2));
    ObLogEntry log_entry;
    while (OB_SUCCESS == log_gen.next_log_entry(log_entry)) {
      err = ps.read(log_entry, missing);
      EXPECT_EQ(OB_SUCCESS, err);
    }
    err = ps.flush();
    EXPECT_EQ(OB_SUCCESS, err);
  }

  // Check.
  EXPECT_EQ(commit_trans_cnt, parser.get_trans_cnt());

  // Destroy.
  err = ps.destroy();
  EXPECT_EQ(OB_SUCCESS, err);
}

/*
 * Half commit, half abort.
 * Commit with Prepare-Commit trans log.
 */
TEST(PartitionStream, BasicTest2)
{
  int err = OB_SUCCESS;

  ObTransPrepareLog prepare_;

  // Commit half trans, whose has even idx.
  const int64_t trans_cnt = 1000;
  const int64_t commit_trans_cnt = trans_cnt / 2;
  const int64_t redo_cnt = 5;

  // Pkey.
  ObPartitionKey pkey(1000U, 1, 1);
  // Log gen.
  TransLogEntryGenerator1 log_gen(pkey);
  // Task Pool.
  ObConcurrentFIFOAllocator fifo_allocator;
  ObLogTransTaskPool<PartTransTask> task_pool;
  err = fifo_allocator.init(16 * _G_, 16 * _M_, OB_MALLOC_NORMAL_BLOCK_SIZE);
  EXPECT_EQ(OB_SUCCESS, err);
  err = task_pool.init(&fifo_allocator, 10240, 1024, 4 * 1024 * 1024, true);
  EXPECT_EQ(OB_SUCCESS, err);
  // Parser.
  MockParser1 parser;
  FetcherConfig cfg;

  // Init.
  PartitionStream ps;
  err = ps.init(pkey, &parser, &task_pool, &cfg);
  EXPECT_EQ(OB_SUCCESS, err);

  // Read logs.
  ObLogIdArray missing;
  for (int64_t idx = 0; idx < trans_cnt; ++idx) {
    // Commit trans with even idx.
    log_gen.next_trans(redo_cnt, (0 == idx % 2));
    ObLogEntry log_entry;
    while (OB_SUCCESS == log_gen.next_log_entry_2(log_entry)) {
      err = ps.read(log_entry, missing);
      EXPECT_EQ(OB_SUCCESS, err);
    }
    err = ps.flush();
    EXPECT_EQ(OB_SUCCESS, err);
  }

  // Check.
  EXPECT_EQ(commit_trans_cnt, parser.get_trans_cnt());

  // Destroy.
  err = ps.destroy();
  EXPECT_EQ(OB_SUCCESS, err);
}


/*
 * Test partition progress tracker.
 */
TEST(PartProgressTracker, BasicTest1)
{
  const int64_t progress_cnt = 4 * 10000;
  PartProgressTracker tracker;

  int err = tracker.init(progress_cnt);
  EXPECT_EQ(OB_SUCCESS, err);

  ObArray<int64_t> indices;
  const int64_t time = get_timestamp();

  // Acquire progresses and update their values.
  for (int64_t idx = 0, cnt = progress_cnt; idx < cnt; ++idx) {
    int64_t progress_idx = 0;
    err = tracker.acquire_progress(progress_idx);
    EXPECT_EQ(OB_SUCCESS, err);
    err = indices.push_back(progress_idx);
    EXPECT_EQ(OB_SUCCESS, err);
    err = tracker.update_progress(progress_idx, time);
    EXPECT_EQ(OB_SUCCESS, err);
  }

  // Get min progress test.
  const int64_t test_cnt = 10000;
  int64_t start = get_timestamp();
  for (int64_t idx = 0, cnt = test_cnt; idx < cnt; ++idx) {
    int64_t min = 0;
    err = tracker.get_min_progress(min);
    EXPECT_EQ(OB_SUCCESS, err);
  }
  const int64_t avg = ((get_timestamp() - start)/ test_cnt);

  // Release.
  while (0 != indices.count()) {
    int64_t progress_idx = indices.at(indices.count() - 1);
    indices.pop_back();
    err = tracker.release_progress(progress_idx);
    EXPECT_EQ(OB_SUCCESS, err);
  }

  err = tracker.destroy();
  EXPECT_EQ(OB_SUCCESS, err);

  // Print result.
  fprintf(stderr, "partition progress tracker get min for %ld progresses costs %s\n",
          progress_cnt, TVAL_TO_STR(avg));
}

// Perf test.
// This test requires at least 3 cores: 1 core tests reading, 2 cores update data.
struct PerfTest1Updater : public Runnable
{
  virtual int routine()
  {
    while (ATOMIC_LOAD(&atomic_run_)) {
      int64_t seed = get_timestamp();
      for (int i = 0; i < 10000; ++i) {
        progress_tracker_->update_progress(indices_->at((seed % (indices_->count()))), seed);
        seed += 777;
      }
    }
    return common::OB_SUCCESS;
  }
  bool atomic_run_;
  PartProgressTracker *progress_tracker_;
  ObArray<int64_t> *indices_;
};
TEST(PartProgressTracker, PerfTest1)
{
  const int64_t progress_cnt = 4 * 10000;
  PartProgressTracker tracker;

  int err = tracker.init(progress_cnt);
  EXPECT_EQ(OB_SUCCESS, err);

  ObArray<int64_t> indices;
  const int64_t time = get_timestamp();

  // Acquire progresses and update their values.
  for (int64_t idx = 0, cnt = progress_cnt; idx < cnt; ++idx) {
    int64_t progress_idx = 0;
    err = tracker.acquire_progress(progress_idx);
    EXPECT_EQ(OB_SUCCESS, err);
    err = indices.push_back(progress_idx);
    EXPECT_EQ(OB_SUCCESS, err);
    err = tracker.update_progress(progress_idx, time);
    EXPECT_EQ(OB_SUCCESS, err);
  }

  // Trigger updaters.
  const int64_t updater_cnt = 2;
  PerfTest1Updater updaters[updater_cnt];
  for (int i = 0; i < updater_cnt; ++i) {
    updaters[i].atomic_run_ = true;
    updaters[i].progress_tracker_ = &tracker;
    updaters[i].indices_ = &indices;
    updaters[i].create();
  }

  // Get min progress test.
  const int64_t test_cnt = 10000;
  int64_t start = get_timestamp();
  for (int64_t idx = 0, cnt = test_cnt; idx < cnt; ++idx) {
    int64_t min = 0;
    err = tracker.get_min_progress(min);
    EXPECT_EQ(OB_SUCCESS, err);
  }
  const int64_t avg = ((get_timestamp() - start)/ test_cnt);

  // Stop updaters.
  for (int i = 0; i < updater_cnt; ++i) {
    ATOMIC_STORE(&(updaters[i].atomic_run_), false);
    updaters[i].join();
  }

  // Release.
  while (0 != indices.count()) {
    int64_t progress_idx = indices.at(indices.count() - 1);
    indices.pop_back();
    err = tracker.release_progress(progress_idx);
    EXPECT_EQ(OB_SUCCESS, err);
  }

  err = tracker.destroy();
  EXPECT_EQ(OB_SUCCESS, err);

  // Print result.
  fprintf(stderr, "partition progress tracker 2 updaters get min for %ld progresses costs %s\n",
          progress_cnt, TVAL_TO_STR(avg));
}
}
}

int main(int argc, char **argv)
{
  //ObLogger::get_logger().set_mod_log_levels("ALL.*:DEBUG, TLOG.*:DEBUG");
  testing::InitGoogleTest(&argc,argv);
  // testing::FLAGS_gtest_filter = "DO_NOT_RUN";
  return RUN_ALL_TESTS();
}
