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

#define USING_LOG_PREFIX OBLOG_FETCHER

#include "gtest/gtest.h"

#include "share/ob_define.h"
#include "storage/ob_storage_log_type.h"
#include "storage/transaction/ob_trans_log.h"
#include "ob_log_fetch_stat_info.h"

#define private public
#include "liboblog/src/ob_log_part_trans_resolver.h"
#include "test_trans_log_generator.h"
#include "test_sp_trans_log_generator.h"

using namespace oceanbase;
using namespace common;
using namespace liboblog;
using namespace transaction;
using namespace storage;
using namespace clog;

namespace oceanbase
{
namespace unittest
{
// Task Pool
static const int64_t PREALLOC_POOL_SIZE = 10 * 1024;
static const int64_t TRANS_TASK_PAGE_SIZE = 1024;
static const int64_t TRANS_TASK_BLOCK_SIZE = 4 * 1024 *1024;
static const int64_t PREALLOC_PAGE_COUNT = 1024;

// For task pool init
ObConcurrentFIFOAllocator fifo_allocator;

// test trans count
static const int64_t TRANS_COUNT = 100;
// redo log count
static const int64_t TRANS_REDO_LOG_COUNT = 100;

int init_task_pool(ObLogTransTaskPool<PartTransTask> &task_pool)
{
  int ret = OB_SUCCESS;

  ret = fifo_allocator.init(16 * _G_, 16 * _M_, OB_MALLOC_NORMAL_BLOCK_SIZE);
  EXPECT_EQ(OB_SUCCESS, ret);

  ret = task_pool.init(&fifo_allocator, PREALLOC_POOL_SIZE, TRANS_TASK_PAGE_SIZE,
      TRANS_TASK_BLOCK_SIZE, true, PREALLOC_PAGE_COUNT);
  EXPECT_EQ(OB_SUCCESS, ret);

  return ret;
}

/*
 * Test scenario.
 * For N transactions, half of which commit, half of which abort
 * Each transaction has a random redo log
 *
 * Log sequence: redo, redo, ... redo, prepare, commit/abort
 *
 * // redo info
 * redo_log_cnt
 * ObLogIdArray redo_log_ids;
 *
 * // prepare info
 * int64_t seq;
 * common::ObPartitionKey partition;
 * int64_t prepare_timestamp;
 * ObTransID trans_id;
 * uint64_t prepare_log_id;
 * uint64_t cluster_id;
 *
 * // commit info
 * int64_t global_trans_version;
 * PartitionLogInfoArray *participants;
 *
 */
TEST(PartTransResolver, BasicTest1)
{
  int err = OB_SUCCESS;

  // Commit half trans, whose has even idx.
  const int64_t trans_cnt = TRANS_COUNT;
  const int64_t commit_trans_cnt = trans_cnt / 2;
  const int64_t abort_trans_cnt = trans_cnt - commit_trans_cnt;
  ObAddr addr(ObAddr::IPV4, "127.0.0.1", 8888);

  TransLogInfo trans_log_info;
  // redo info
  int64_t redo_cnt = 0;
  ObLogIdArray redo_log_ids;
  // prepare info
  int64_t seq = 0;
  ObPartitionKey pkey(1000U, 1, 1);
  int64_t prepare_timestamp = PREPARE_TIMESTAMP;
  ObTransID trans_id(addr);
  uint64_t prepare_log_id = 0;
  uint64_t CLOUSTER_ID = 1000;
  // commit info
  int64_t global_trans_version = GLOBAL_TRANS_VERSION;
  PartitionLogInfoArray ptl_ids;

  // Log gen.
  TransLogEntryGeneratorBase log_gen(pkey, trans_id);
  // Task Pool.
  ObLogTransTaskPool<PartTransTask> task_pool;
  EXPECT_EQ(OB_SUCCESS, init_task_pool(task_pool));
  // Parser.
  MockParser1 parser;
  EXPECT_EQ(OB_SUCCESS, parser.init());

  // Partitioned Transaction Parser
  PartTransResolver pr;
  err = pr.init(pkey, parser, task_pool);
  EXPECT_EQ(OB_SUCCESS, err);

  // Read logs.
  ObLogIdArray missing;
  TransStatInfo tsi;
  volatile bool stop_flag = false;

  for (int64_t idx = 0; idx < trans_cnt; ++idx) {
    redo_cnt = get_timestamp() % TRANS_REDO_LOG_COUNT + 1;
    redo_log_ids.reset();
    for (int64_t cnt = 0; cnt < redo_cnt; ++cnt) {
      EXPECT_EQ(OB_SUCCESS, redo_log_ids.push_back(log_gen.get_log_id() + cnt));
    }
    prepare_log_id = log_gen.get_log_id() + redo_cnt;
    ptl_ids.reset();

    ObPartitionLogInfo ptl_id(pkey, prepare_log_id, PREPARE_TIMESTAMP);
    err = ptl_ids.push_back(ptl_id);
    EXPECT_EQ(OB_SUCCESS, err);
    // push fixed participant information
    for (int64_t idx = 0; idx < FIXED_PART_COUNT; ++idx) {
      err = ptl_ids.push_back(FIXED_PART_INFO[idx]);
      EXPECT_EQ(OB_SUCCESS, err);
    }
    trans_log_info.reset(redo_cnt, redo_log_ids, seq, pkey, prepare_timestamp,
                         trans_id, prepare_log_id, CLOUSTER_ID, global_trans_version, ptl_ids);
    EXPECT_EQ(OB_SUCCESS, parser.push_into_queue(&trans_log_info));
    seq++;

    // Commit trans with even idx.
    log_gen.next_trans(redo_cnt, (0 == idx % 2));
    clog::ObLogEntry log_entry;

    while (OB_SUCCESS == log_gen.next_log_entry(log_entry)) {
      err = pr.read(log_entry, missing, tsi);
      EXPECT_EQ(OB_SUCCESS, err);
    }

    err = pr.flush(stop_flag);
    EXPECT_EQ(OB_SUCCESS, err);

    // Verify the correctness of partition task data
    bool check_result;
    EXPECT_EQ(OB_SUCCESS, parser.get_check_result(check_result));
    EXPECT_TRUE(check_result);
    LOG_DEBUG("debug", K(idx));
  }

  // Check.
  EXPECT_EQ(commit_trans_cnt, parser.get_commit_trans_cnt());
  EXPECT_EQ(abort_trans_cnt, parser.get_abort_trans_cnt());

  // Destroy.
  pr.destroy();
  task_pool.destroy();
  fifo_allocator.destroy();
}

/*
 * Test scenario.
 * For N transactions, half of which commit, half of which abort
 * Each transaction has a random redo log
 * Log sequence: redo, redo... redo-prepare, commit/abort
 * redo-prepare in a log entry
 *
 */
TEST(PartTransResolver, BasicTest2)
{
  int err = OB_SUCCESS;

  // Commit half trans, whose has even idx.
  const int64_t trans_cnt = TRANS_COUNT;
  const int64_t commit_trans_cnt = trans_cnt / 2;
  const int64_t abort_trans_cnt = trans_cnt - commit_trans_cnt;
  ObAddr addr(ObAddr::IPV4, "127.0.0.1", 8888);

  TransLogInfo trans_log_info;
  // redo info
  int64_t redo_cnt = 0;
  ObLogIdArray redo_log_ids;
  // prepare info
  int64_t seq = 0;
  ObPartitionKey pkey(1000U, 1, 1);
  int64_t prepare_timestamp = PREPARE_TIMESTAMP;
  ObTransID trans_id(addr);
  uint64_t prepare_log_id = 0;
  uint64_t CLOUSTER_ID = 1000;
  // commit info
  int64_t global_trans_version = GLOBAL_TRANS_VERSION;
  PartitionLogInfoArray ptl_ids;

  // Log gen.
  TransLogEntryGeneratorBase log_gen(pkey, trans_id);
  // Task Pool.
  ObLogTransTaskPool<PartTransTask> task_pool;
  EXPECT_EQ(OB_SUCCESS, init_task_pool(task_pool));
  // Parser.
  MockParser1 parser;
  EXPECT_EQ(OB_SUCCESS, parser.init());

  // Partitioned Transaction Parser
  PartTransResolver pr;
  err = pr.init(pkey, parser, task_pool);
  EXPECT_EQ(OB_SUCCESS, err);

  // Read logs.
  ObLogIdArray missing;
  TransStatInfo tsi;
  bool stop_flag = false;
  for (int64_t idx = 0; idx < trans_cnt; ++idx) {
    redo_cnt = get_timestamp() % TRANS_REDO_LOG_COUNT + 2;
    redo_log_ids.reset();
    for (int64_t cnt = 0; cnt < redo_cnt; ++cnt) {
      EXPECT_EQ(OB_SUCCESS, redo_log_ids.push_back(log_gen.get_log_id() + cnt));
    }
    prepare_log_id = log_gen.get_log_id() + redo_cnt - 1;

    ptl_ids.reset();
    ObPartitionLogInfo ptl_id(pkey, prepare_log_id, PREPARE_TIMESTAMP);
    err = ptl_ids.push_back(ptl_id);
    EXPECT_EQ(OB_SUCCESS, err);
    // push fixed participant information
    for (int64_t idx = 0; idx < FIXED_PART_COUNT; ++idx) {
      err = ptl_ids.push_back(FIXED_PART_INFO[idx]);
      EXPECT_EQ(OB_SUCCESS, err);
    }
    trans_log_info.reset(redo_cnt, redo_log_ids, seq, pkey, prepare_timestamp,
                         trans_id, prepare_log_id, CLOUSTER_ID, global_trans_version, ptl_ids);
    EXPECT_EQ(OB_SUCCESS, parser.push_into_queue(&trans_log_info));
    seq++;

    // Commit trans with even idx.
    log_gen.next_trans_with_redo_prepare(redo_cnt, (0 == idx % 2));
    clog::ObLogEntry log_entry;

    // read redo, redo... redo-prepare
    for (int64_t log_cnt = 0; log_cnt < redo_cnt; log_cnt++) {
      EXPECT_EQ(OB_SUCCESS, log_gen.next_log_entry_with_redo_prepare(log_entry));
      err = pr.read(log_entry, missing, tsi);
      EXPECT_EQ(OB_SUCCESS, err);
    }

    // read commit/abort log
    EXPECT_EQ(OB_SUCCESS, log_gen.next_log_entry_with_redo_prepare(log_entry));
    err = pr.read(log_entry, missing, tsi);
    EXPECT_EQ(OB_SUCCESS, err);

    err = pr.flush(stop_flag);
    EXPECT_EQ(OB_SUCCESS, err);

    // Verify the correctness of partition task data
    bool check_result;
    EXPECT_EQ(OB_SUCCESS, parser.get_check_result(check_result));
    EXPECT_TRUE(check_result);
  }

  // Check.
  EXPECT_EQ(commit_trans_cnt, parser.get_commit_trans_cnt());
  EXPECT_EQ(abort_trans_cnt, parser.get_abort_trans_cnt());

  // Destroy.
  pr.destroy();
  task_pool.destroy();
  fifo_allocator.destroy();
}

/*
 * Test scenario.
 * Parse to prepare log, find redo log missing, need to read miss log
 * For N transactions, half of them commit, half of them abort
 * Each transaction has a random redo log
 * Two cases.
 * 1. redo, redo, redo...prepare, commit/abort
 * 2. redo, redo, redo...redo-prepare, commit/abort
 *
 */
TEST(PartTransResolver, BasicTest3)
{
  int err = OB_SUCCESS;

  // Commit half trans, whose has even idx.
  const int64_t trans_cnt = TRANS_COUNT;
  const int64_t commit_trans_cnt = trans_cnt / 2;
  int64_t redo_cnt = 0;
  int64_t miss_redo_cnt = 0;
  int64_t can_read_redo_cnt = 0;

  // Pkey.
  ObPartitionKey pkey(1000U, 1, 1);
  // addr
  ObAddr addr(ObAddr::IPV4, "127.0.0.1", 8888);
  ObTransID trans_id(addr);

  // Log gen.
  TransLogEntryGenerator1 log_gen(pkey, trans_id);
  // Task Pool.
  ObLogTransTaskPool<PartTransTask> task_pool;
  EXPECT_EQ(OB_SUCCESS, init_task_pool(task_pool));
  // Parser.
  MockParser2 parser;
  // Partitioned Transaction Parser
  PartTransResolver pr;
  err = pr.init(pkey, parser, task_pool);
  EXPECT_EQ(OB_SUCCESS, err);

  // Read logs.
  ObLogIdArray missing;
  TransStatInfo tsi;
  bool stop_flag = false;

  // case 1: redo, redo, redo...prepare, commit/abort
  // case 2: redo, redo, redo...redo-prepare, commit/abort
  bool is_normal_trans = false;
  bool is_redo_with_prapare_trans = false;
  for (int64_t idx = 0; idx < trans_cnt; ++idx) {
    if (idx < trans_cnt / 2) {
      is_normal_trans = true;
    } else {
      is_redo_with_prapare_trans = true;
    }
    redo_cnt = get_timestamp() % TRANS_REDO_LOG_COUNT + 1;
    if (is_normal_trans) {
      miss_redo_cnt = get_timestamp() % redo_cnt + 1;
      can_read_redo_cnt = redo_cnt - miss_redo_cnt;
    } else if (is_redo_with_prapare_trans){
      miss_redo_cnt = get_timestamp() % redo_cnt;
      can_read_redo_cnt = redo_cnt - miss_redo_cnt - 1;
    } else {
    }

    // Commit trans with even idx.
    if (is_normal_trans) {
      log_gen.next_trans_with_miss_redo(redo_cnt, miss_redo_cnt, (0 == idx % 2), NORMAL_TRAN);
    } else if (is_redo_with_prapare_trans){
      log_gen.next_trans_with_miss_redo(redo_cnt, miss_redo_cnt, (0 == idx % 2), REDO_WITH_PREPARE_TRAN);
    } else {
    }

    uint64_t start_redo_log_id = log_gen.get_log_id();
    clog::ObLogEntry log_entry;

    // First read the can_read_redo_cnt redo log
    for (int64_t log_cnt = 0; log_cnt < can_read_redo_cnt; ++log_cnt) {
      if (is_normal_trans) {
        EXPECT_EQ(OB_SUCCESS, log_gen.next_log_entry_missing_redo(NORMAL_TRAN, log_entry));
      } else if (is_redo_with_prapare_trans){
        EXPECT_EQ(OB_SUCCESS, log_gen.next_log_entry_missing_redo(REDO_WITH_PREPARE_TRAN, log_entry));
      } else {
      }
      err = pr.read(log_entry, missing, tsi);
      EXPECT_EQ(OB_SUCCESS, err);
    }

    // Read prepare log and find miss redo log
    if (is_normal_trans) {
      EXPECT_EQ(OB_SUCCESS, log_gen.next_log_entry_missing_redo(NORMAL_TRAN, log_entry));
    } else if (is_redo_with_prapare_trans){
      EXPECT_EQ(OB_SUCCESS, log_gen.next_log_entry_missing_redo(REDO_WITH_PREPARE_TRAN, log_entry));
    } else {
    }
    err = pr.read(log_entry, missing, tsi);
    EXPECT_EQ(OB_ITEM_NOT_SETTED, err);

    // Verify the misses array and read the misses redo log
    const int64_t miss_array_cnt = missing.count();
    EXPECT_EQ(miss_redo_cnt, miss_array_cnt);
    for (int64_t log_cnt = 0; log_cnt < miss_array_cnt; ++log_cnt) {
      LOG_DEBUG("miss", K(missing[log_cnt]));
      EXPECT_EQ(start_redo_log_id, missing[log_cnt]);
      start_redo_log_id++;

      clog::ObLogEntry miss_log_entry;
      EXPECT_EQ(OB_SUCCESS, log_gen.next_miss_log_entry(missing[log_cnt], miss_log_entry));
      err = pr.read_missing_redo(miss_log_entry);
      EXPECT_EQ(OB_SUCCESS, err);
    }

    // After reading the missing redo log, read the prepare log again to advance the partitioning task
    if (is_normal_trans) {
      EXPECT_EQ(OB_SUCCESS, log_gen.get_prepare_log_entry(NORMAL_TRAN, log_entry));
    } else if (is_redo_with_prapare_trans){
      EXPECT_EQ(OB_SUCCESS, log_gen.get_prepare_log_entry(REDO_WITH_PREPARE_TRAN, log_entry));
    } else {
    }
    err = pr.read(log_entry, missing, tsi);
    EXPECT_EQ(OB_SUCCESS, err);

    // read commit/abort log
    if (is_normal_trans) {
      EXPECT_EQ(OB_SUCCESS, log_gen.next_log_entry_missing_redo(NORMAL_TRAN, log_entry));
    } else if (is_redo_with_prapare_trans){
      EXPECT_EQ(OB_SUCCESS, log_gen.next_log_entry_missing_redo(REDO_WITH_PREPARE_TRAN, log_entry));
    } else {
    }
    err = pr.read(log_entry, missing, tsi);
    EXPECT_EQ(OB_SUCCESS, err);

    err = pr.flush(stop_flag);
    EXPECT_EQ(OB_SUCCESS, err);
  }

  // Check.
  EXPECT_EQ(commit_trans_cnt, parser.get_commit_trans_cnt());

  // Destroy.
  pr.destroy();
  task_pool.destroy();
  fifo_allocator.destroy();
}

/*
 * r stands for redo, p stands for prepare, c stands for commit, a stands for abort)
 * The numbers after r/p/c/a represent the different transactions
 * Log sequence:
 * r1 r2 r2 r2 p2 p1 c1 c2 r3 p3 c3
 * Verifying the correctness of parsing multiple transactions, i.e. constructing different partitioned transaction tasks based on different transaction IDs
 * Verify the output order of transactions: transaction 2 -> transaction 1 -> transaction 3
 */
TEST(PartTransResolver, BasicTest4)
{
  int err = OB_SUCCESS;

  ObPartitionKey pkey(1000U, 1, 1);

  // Task Pool.
  ObLogTransTaskPool<PartTransTask> task_pool;
  EXPECT_EQ(OB_SUCCESS, init_task_pool(task_pool));
  // Parser.
  MockParser1 parser;
  EXPECT_EQ(OB_SUCCESS, parser.init());

  // Partitioned Transaction Parser
  PartTransResolver pr;
  err = pr.init(pkey, parser, task_pool);
  EXPECT_EQ(OB_SUCCESS, err);

  const int64_t commit_trans_cnt = 3;
  // redo info
  int64_t redo_cnt_array[3] = {1, 3, 1};
  ObLogIdArray redo_log_ids_array[3];
  for (int64_t i = 0; i < 3; ++i) {
    for (int64_t j = 0; j < redo_cnt_array[i]; ++j) {
      EXPECT_EQ(OB_SUCCESS, redo_log_ids_array[i].push_back(j));
    }
  }

  // prepare info
  // trans 2 - trans 1 - trans 3->seq: 0, 1, 2
  int64_t seq_array[3] = {1, 0, 2};
  int64_t prepare_timestamp = PREPARE_TIMESTAMP;
  ObAddr addr_array[3];
  for (int64_t idx = 0; idx < 3; idx++) {
    addr_array[idx] = ObAddr(ObAddr::IPV4, "127.0.0.1", static_cast<int32_t>(8888 + idx));
  }
  // trans ID
  ObTransID trans_id_array[3] = {
    ObTransID(addr_array[0]), ObTransID(addr_array[1]), ObTransID(addr_array[2])
  };
  uint64_t prepare_log_id_array[3] = {1, 3, 1};
  uint64_t CLOUSTER_ID = 1000;

  // commit info
  int64_t global_trans_version = GLOBAL_TRANS_VERSION;
  PartitionLogInfoArray ptl_ids_array[3];
  for (int64_t i = 0; i < 3; ++i) {
    ptl_ids_array[i].reset();

    ObPartitionLogInfo ptl_id(pkey, prepare_log_id_array[i], PREPARE_TIMESTAMP);
    err = ptl_ids_array[i].push_back(ptl_id);
    EXPECT_EQ(OB_SUCCESS, err);
    // push fixed participant information
    for (int64_t j = 0; j < FIXED_PART_COUNT; ++j) {
      err = ptl_ids_array[i].push_back(FIXED_PART_INFO[j]);
      EXPECT_EQ(OB_SUCCESS, err);
    }
  }

  TransLogInfo trans_log_info_array[3];
  for (int64_t i = 0; i < 3; ++i) {
    trans_log_info_array[i].reset(redo_cnt_array[i], redo_log_ids_array[i], seq_array[i], pkey, prepare_timestamp,
                                  trans_id_array[i], prepare_log_id_array[i],
                                  CLOUSTER_ID, global_trans_version, ptl_ids_array[i]);
  }

  // Push in the order of transaction 2 - transaction 1 - transaction 3 for subsequent validation of the transaction output order
  EXPECT_EQ(OB_SUCCESS, parser.push_into_queue(&trans_log_info_array[1]));
  EXPECT_EQ(OB_SUCCESS, parser.push_into_queue(&trans_log_info_array[0]));
  EXPECT_EQ(OB_SUCCESS, parser.push_into_queue(&trans_log_info_array[2]));

  // Log gen. Generate logs for transactions 1, 2 and 3 respectively
  TransLogEntryGeneratorBase log_gen_1(pkey, trans_id_array[0]);
  TransLogEntryGeneratorBase log_gen_2(pkey, trans_id_array[1]);
  TransLogEntryGeneratorBase log_gen_3(pkey, trans_id_array[2]);

  log_gen_1.next_trans(redo_cnt_array[0], true);
  log_gen_2.next_trans(redo_cnt_array[1], true);
  log_gen_3.next_trans(redo_cnt_array[2], true);

  // Read logs.
  ObLogIdArray missing1;
  ObLogIdArray missing2;
  ObLogIdArray missing3;
  TransStatInfo tsi;
  volatile bool stop_flag = false;

  // log seq:
  // r1 r2 r2 r2 p2 p1 c1 c2 r3 p3 c3
  clog::ObLogEntry log_entry;

  EXPECT_EQ(0, pr.task_seq_);
  EXPECT_EQ(0, pr.prepare_seq_);
  // r1
  EXPECT_EQ(OB_SUCCESS, log_gen_1.next_log_entry(log_entry));
  err = pr.read(log_entry, missing1, tsi);
  EXPECT_EQ(OB_SUCCESS, err);
  // r2
  EXPECT_EQ(OB_SUCCESS, log_gen_2.next_log_entry(log_entry));
  err = pr.read(log_entry, missing2, tsi);
  EXPECT_EQ(OB_SUCCESS, err);
  // r2
  EXPECT_EQ(OB_SUCCESS, log_gen_2.next_log_entry(log_entry));
  err = pr.read(log_entry, missing2, tsi);
  EXPECT_EQ(OB_SUCCESS, err);
  // r2
  EXPECT_EQ(OB_SUCCESS, log_gen_2.next_log_entry(log_entry));
  err = pr.read(log_entry, missing2, tsi);
  EXPECT_EQ(OB_SUCCESS, err);
  // p2
  EXPECT_EQ(OB_SUCCESS, log_gen_2.next_log_entry(log_entry));
  err = pr.read(log_entry, missing2, tsi);
  EXPECT_EQ(OB_SUCCESS, err);
  EXPECT_EQ(1, pr.prepare_seq_);
  // p1
  EXPECT_EQ(OB_SUCCESS, log_gen_1.next_log_entry(log_entry));
  err = pr.read(log_entry, missing1, tsi);
  EXPECT_EQ(OB_SUCCESS, err);
  EXPECT_EQ(2, pr.prepare_seq_);
  // c1
  EXPECT_EQ(OB_SUCCESS, log_gen_1.next_log_entry(log_entry));
  err = pr.read(log_entry, missing1, tsi);
  EXPECT_EQ(OB_SUCCESS, err);
  // c2
  EXPECT_EQ(OB_SUCCESS, log_gen_2.next_log_entry(log_entry));
  err = pr.read(log_entry, missing2, tsi);
  EXPECT_EQ(OB_SUCCESS, err);
  // r3
  EXPECT_EQ(OB_SUCCESS, log_gen_3.next_log_entry(log_entry));
  err = pr.read(log_entry, missing3, tsi);
  EXPECT_EQ(OB_SUCCESS, err);
  // p3
  EXPECT_EQ(OB_SUCCESS, log_gen_3.next_log_entry(log_entry));
  err = pr.read(log_entry, missing3, tsi);
  EXPECT_EQ(OB_SUCCESS, err);
  EXPECT_EQ(3, pr.prepare_seq_);
  // c3
  EXPECT_EQ(OB_SUCCESS, log_gen_3.next_log_entry(log_entry));
  err = pr.read(log_entry, missing3, tsi);
  EXPECT_EQ(OB_SUCCESS, err);

  err = pr.flush(stop_flag);
  EXPECT_EQ(OB_SUCCESS, err);
  EXPECT_EQ(3, pr.task_seq_);

  // Check.
  EXPECT_EQ(commit_trans_cnt, parser.get_commit_trans_cnt());

  // Verify the correctness of partition task data
  for (int64_t idx = 0; idx < 3; ++idx) {
    bool check_result;
    EXPECT_EQ(OB_SUCCESS, parser.get_check_result(check_result));
    EXPECT_TRUE(check_result);
  }

  // Destroy.
  pr.destroy();
  task_pool.destroy();
  fifo_allocator.destroy();
}

/*
 * Test scenario:
 * For N Sp transactions, half of them commit, half of them abort
 * Each Sp transaction has a random redo log
 *
 * log seq：redo, redo, ... redo, commit/abort
*
 * // redo info
 * redo_log_cnt
 * ObLogIdArray redo_log_ids;
 *
 * // prepare info
 * int64_t seq;
 * common::ObPartitionKey partition;
 * int64_t prepare_timestamp;
 * ObTransID trans_id;
 * uint64_t prepare_log_id;
 * uint64_t cluster_id;
 *
 * // commit info
 * int64_t global_trans_version;
 * PartitionLogInfoArray *participants;
 *
 */
TEST(PartTransResolver, BasicTest5)
{
  int err = OB_SUCCESS;

  // Commit half trans, whose has even idx.
  const int64_t trans_cnt = TRANS_COUNT;
  const int64_t commit_trans_cnt = trans_cnt / 2;
  const int64_t abort_trans_cnt = trans_cnt - commit_trans_cnt;
  ObAddr addr(ObAddr::IPV4, "127.0.0.1", 8888);

  TransLogInfo trans_log_info;

  // redo info
  int64_t redo_cnt = 0;
  ObLogIdArray redo_log_ids;
  // prepare info
  int64_t seq = 0;
  ObPartitionKey pkey(1000U, 1, 1);
  int64_t prepare_timestamp = SP_PREPARE_TIMESTAMP;
  ObTransID trans_id(addr);
  uint64_t prepare_log_id = 0;
  uint64_t CLOUSTER_ID = 1000;
  // commit info
  int64_t global_trans_version = SP_GLOBAL_TRANS_VERSION;
  PartitionLogInfoArray ptl_ids;

  // Log gen.
  SpTransLogEntryGeneratorBase log_gen(pkey, trans_id);
  // Task Pool.
  ObLogTransTaskPool<PartTransTask> task_pool;
  EXPECT_EQ(OB_SUCCESS, init_task_pool(task_pool));
  // Parser.
  MockParser1 parser;
  EXPECT_EQ(OB_SUCCESS, parser.init());

  // Partitioned Transaction Parser
  PartTransResolver pr;
  err = pr.init(pkey, parser, task_pool);
  EXPECT_EQ(OB_SUCCESS, err);

  // Read logs.
  ObLogIdArray missing;
  TransStatInfo tsi;
  volatile bool stop_flag = false;

  for (int64_t idx = 0; idx < trans_cnt; ++idx) {
    redo_cnt = get_timestamp() % TRANS_REDO_LOG_COUNT + 1;
    redo_log_ids.reset();
    for (int64_t cnt = 0; cnt < redo_cnt; ++cnt) {
      EXPECT_EQ(OB_SUCCESS, redo_log_ids.push_back(log_gen.get_log_id() + cnt));
    }
    prepare_log_id = log_gen.get_log_id() + redo_cnt;
    ptl_ids.reset();
    ObPartitionLogInfo ptl_id(pkey, prepare_log_id, PREPARE_TIMESTAMP);
    err = ptl_ids.push_back(ptl_id);
    EXPECT_EQ(OB_SUCCESS, err);

    trans_log_info.reset(redo_cnt, redo_log_ids, seq, pkey, prepare_timestamp,
                         trans_id, prepare_log_id, CLOUSTER_ID, global_trans_version, ptl_ids);
    EXPECT_EQ(OB_SUCCESS, parser.push_into_queue(&trans_log_info));
    seq++;

    // Commit trans with even idx.
    log_gen.next_trans(redo_cnt, (0 == idx % 2));
    clog::ObLogEntry log_entry;

    while (OB_SUCCESS == log_gen.next_log_entry(log_entry)) {
      err = pr.read(log_entry, missing, tsi);
      EXPECT_EQ(OB_SUCCESS, err);
    }

    err = pr.flush(stop_flag);
    EXPECT_EQ(OB_SUCCESS, err);

    // Verify the correctness of partition task data
    bool check_result;
    EXPECT_EQ(OB_SUCCESS, parser.get_check_result(check_result));
    EXPECT_TRUE(check_result);
  }

  // Check.
  EXPECT_EQ(commit_trans_cnt, parser.get_commit_trans_cnt());
  EXPECT_EQ(abort_trans_cnt, parser.get_abort_trans_cnt());

  // Destroy.
  pr.destroy();
  task_pool.destroy();
  fifo_allocator.destroy();
}

/*
 * Test scenario:
 * For N Sp transactions, redo and commit in the same log entry
 * Each Sp transaction has a random redo log
 *
 * log seq：redo, redo, ... redo, redo-commit
 *
 */
TEST(PartTransResolver, BasicTest6)
{
  int err = OB_SUCCESS;

  const int64_t trans_cnt = TRANS_COUNT;
  const int64_t commit_trans_cnt = trans_cnt;
  ObAddr addr(ObAddr::IPV4, "127.0.0.1", 8888);

  TransLogInfo trans_log_info;

  // redo info
  int64_t redo_cnt = 0;
  ObLogIdArray redo_log_ids;
  // prepare info
  int64_t seq = 0;
  ObPartitionKey pkey(1000U, 1, 1);
  int64_t prepare_timestamp = SP_PREPARE_TIMESTAMP;
  ObTransID trans_id(addr);
  uint64_t prepare_log_id = 0;
  uint64_t CLOUSTER_ID = 1000;
  // commit info
  int64_t global_trans_version = SP_GLOBAL_TRANS_VERSION;
  PartitionLogInfoArray ptl_ids;

  // Log gen.
  SpTransLogEntryGeneratorBase log_gen(pkey, trans_id);
  // Task Pool.
  ObLogTransTaskPool<PartTransTask> task_pool;
  EXPECT_EQ(OB_SUCCESS, init_task_pool(task_pool));
  // Parser.
  MockParser1 parser;
  //MockParser2 parser;
  EXPECT_EQ(OB_SUCCESS, parser.init());

  // Partitioned Transaction Parser
  PartTransResolver pr;
  err = pr.init(pkey, parser, task_pool);
  EXPECT_EQ(OB_SUCCESS, err);

  // Read logs.
  ObLogIdArray missing;
  TransStatInfo tsi;
  volatile bool stop_flag = false;

  for (int64_t idx = 0; idx < trans_cnt; ++idx) {
    redo_cnt = get_timestamp() % TRANS_REDO_LOG_COUNT + 1;
    // First test, if redo_cnt=1, only one redo-commit, prepare_log_id=0, illegal
    if (0 == idx && 1 == redo_cnt) {
      redo_cnt++;
    }
    redo_log_ids.reset();
    for (int64_t cnt = 0; cnt < redo_cnt; ++cnt) {
      EXPECT_EQ(OB_SUCCESS, redo_log_ids.push_back(log_gen.get_log_id() + cnt));
    }
    // sp transaction does not have prepare log, prepare log id is the same as commit log id
    prepare_log_id = log_gen.get_log_id() + redo_cnt - 1;
    ptl_ids.reset();
    ObPartitionLogInfo ptl_id(pkey, prepare_log_id, PREPARE_TIMESTAMP);
    err = ptl_ids.push_back(ptl_id);
    EXPECT_EQ(OB_SUCCESS, err);

    trans_log_info.reset(redo_cnt, redo_log_ids, seq, pkey, prepare_timestamp,
                         trans_id, prepare_log_id, CLOUSTER_ID, global_trans_version, ptl_ids);
    EXPECT_EQ(OB_SUCCESS, parser.push_into_queue(&trans_log_info));
    seq++;

    log_gen.next_trans_with_redo_commit(redo_cnt);
    clog::ObLogEntry log_entry;

    while (OB_SUCCESS == log_gen.next_log_entry_with_redo_commit(log_entry)) {
      err = pr.read(log_entry, missing, tsi);
      EXPECT_EQ(OB_SUCCESS, err);
    }

    err = pr.flush(stop_flag);
    EXPECT_EQ(OB_SUCCESS, err);

    // Verify the correctness of partition task data
    bool check_result;
    EXPECT_EQ(OB_SUCCESS, parser.get_check_result(check_result));
    EXPECT_TRUE(check_result);
  }

  // Check.
  EXPECT_EQ(commit_trans_cnt, parser.get_commit_trans_cnt());

  // Destroy.
  pr.destroy();
  task_pool.destroy();
  fifo_allocator.destroy();
}

/*
 * Test scenario:
 * For N Sp transactions, redo and commit in the same log entry
 * Each Sp transaction has a random redo log
 *
 * Log sequence: redo, redo, ... redo, redo-commit
 * Read to redo-commit and find redo log missing, need to read miss log
 *
 */
TEST(PartTransResolver, BasicTest7)
{
  int err = OB_SUCCESS;

  const int64_t trans_cnt = TRANS_COUNT;
  //const int64_t trans_cnt = 2;
  const int64_t commit_trans_cnt = trans_cnt;
  int64_t redo_cnt = 0;
  int64_t miss_redo_cnt = 0;
  int64_t can_read_redo_cnt = 0;

  // Pkey.
  ObPartitionKey pkey(1000U, 1, 1);
  ObAddr addr(ObAddr::IPV4, "127.0.0.1", 8888);
  ObTransID trans_id(addr);

  // Log gen.
  SpTransLogEntryGenerator1 log_gen(pkey, trans_id);
  // Task Pool.
  ObLogTransTaskPool<PartTransTask> task_pool;
  EXPECT_EQ(OB_SUCCESS, init_task_pool(task_pool));
  // Parser.
  MockParser2 parser;

  // Partitioned Transaction Parser
  PartTransResolver pr;
  err = pr.init(pkey, parser, task_pool);
  EXPECT_EQ(OB_SUCCESS, err);

  // Read logs.
  ObLogIdArray missing;
  TransStatInfo tsi;
  volatile bool stop_flag = false;

  // case 1: redo, redo, redo, ... redo, commit
  // case 2: redo, redo, redo, ... redo, redo-commit
  bool is_normal_trans = false;
  bool is_redo_with_commit_trans = false;
  for (int64_t idx = 0; idx < trans_cnt; ++idx) {
    if (idx < trans_cnt / 2) {
      is_normal_trans = true;
    } else {
      is_redo_with_commit_trans = true;
    }

    redo_cnt = get_timestamp() % TRANS_REDO_LOG_COUNT + 1;
    //redo_cnt = 2;
    if (is_normal_trans) {
      miss_redo_cnt = get_timestamp() % redo_cnt + 1;
      can_read_redo_cnt = redo_cnt - miss_redo_cnt;
    } else if (is_redo_with_commit_trans){
      miss_redo_cnt = get_timestamp() % redo_cnt;
      can_read_redo_cnt = redo_cnt - miss_redo_cnt - 1;
    } else {
    }

    if (is_normal_trans) {
      log_gen.next_trans_with_miss_redo(redo_cnt, miss_redo_cnt, SP_NORMAL_TRAN);
    } else if (is_redo_with_commit_trans){
      log_gen.next_trans_with_miss_redo(redo_cnt, miss_redo_cnt, SP_REDO_WITH_COMMIT_TRAN);
    } else {
    }

    uint64_t start_redo_log_id = log_gen.get_log_id();
    clog::ObLogEntry log_entry;

   // First read the can_read_redo_cnt redo log
    for (int64_t log_cnt = 0; log_cnt < can_read_redo_cnt; ++log_cnt) {
      if (is_normal_trans) {
        EXPECT_EQ(OB_SUCCESS, log_gen.next_log_entry_missing_redo(SP_NORMAL_TRAN, log_entry));
      } else if (is_redo_with_commit_trans) {
        EXPECT_EQ(OB_SUCCESS, log_gen.next_log_entry_missing_redo(SP_REDO_WITH_COMMIT_TRAN, log_entry));
      } else {
      }
      err = pr.read(log_entry, missing, tsi);
      EXPECT_EQ(OB_SUCCESS, err);
    }

    // read commit log，发现miss redo log,
    if (is_normal_trans) {
      EXPECT_EQ(OB_SUCCESS, log_gen.next_log_entry_missing_redo(SP_NORMAL_TRAN, log_entry));
    } else if (is_redo_with_commit_trans) {
      EXPECT_EQ(OB_SUCCESS, log_gen.next_log_entry_missing_redo(SP_REDO_WITH_COMMIT_TRAN, log_entry));
    } else {
    }
    err = pr.read(log_entry, missing, tsi);
    EXPECT_EQ(OB_ITEM_NOT_SETTED, err);

    // Verify the misses array and read the misses redo log
    const int64_t miss_array_cnt = missing.count();
    EXPECT_EQ(miss_redo_cnt, miss_array_cnt);
    for (int64_t log_cnt = 0; log_cnt < miss_array_cnt; ++log_cnt) {
      LOG_DEBUG("miss", K(missing[log_cnt]));
      EXPECT_EQ(start_redo_log_id, missing[log_cnt]);
      start_redo_log_id++;

      clog::ObLogEntry miss_log_entry;
      EXPECT_EQ(OB_SUCCESS, log_gen.next_miss_log_entry(missing[log_cnt], miss_log_entry));
      err = pr.read_missing_redo(miss_log_entry);
      EXPECT_EQ(OB_SUCCESS, err);
    }

    // After reading the missing redo log, read the commit log again to advance the partitioning task and free up commit_log_entry memory
    if (is_normal_trans) {
      EXPECT_EQ(OB_SUCCESS, log_gen.get_commit_log_entry(SP_NORMAL_TRAN, log_entry));
    } else if (is_redo_with_commit_trans){
      EXPECT_EQ(OB_SUCCESS, log_gen.get_commit_log_entry(SP_REDO_WITH_COMMIT_TRAN, log_entry));
    } else {
    }
    err = pr.read(log_entry, missing, tsi);
    EXPECT_EQ(OB_SUCCESS, err);

    err = pr.flush(stop_flag);
    EXPECT_EQ(OB_SUCCESS, err);
  }

  // Check.
  EXPECT_EQ(commit_trans_cnt, parser.get_commit_trans_cnt());

  // Destroy.
  pr.destroy();
  task_pool.destroy();
  fifo_allocator.destroy();
}

}
}

int main(int argc, char **argv)
{
  //ObLogger::get_logger().set_mod_log_levels("ALL.*:DEBUG, TLOG.*:DEBUG");
  // testing::FLAGS_gtest_filter = "DO_NOT_RUN";
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_ob_log_part_trans_resolver.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
