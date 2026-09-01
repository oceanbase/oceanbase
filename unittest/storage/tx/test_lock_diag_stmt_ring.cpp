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
#include <atomic>
#include <cstring>
#include <thread>
#include <vector>
#include <gtest/gtest.h>
#define private public
#define protected public
#include "storage/tx/ob_lock_diag_stmt_ring.h"

namespace oceanbase
{
using namespace common;
using namespace transaction;
namespace unittest
{

class TestLockDiagStmtRing : public ::testing::Test
{
public:
  virtual void SetUp() {}
  virtual void TearDown() {}
};

static void fill_lock_diag_sql_id(const int64_t seq, char *sql_id, const int64_t sql_id_buf_len)
{
  snprintf(sql_id, sql_id_buf_len, "SQL_%02ld__________________________", seq);
}

// ==================== need_push_lock_diag_stmt tests ====================

TEST_F(TestLockDiagStmtRing, dml_types_return_true)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  EXPECT_TRUE(need_push_lock_diag_stmt(sql::stmt::T_INSERT, false));
  EXPECT_TRUE(need_push_lock_diag_stmt(sql::stmt::T_INSERT_ALL, false));
  EXPECT_TRUE(need_push_lock_diag_stmt(sql::stmt::T_REPLACE, false));
  EXPECT_TRUE(need_push_lock_diag_stmt(sql::stmt::T_UPDATE, false));
  EXPECT_TRUE(need_push_lock_diag_stmt(sql::stmt::T_DELETE, false));
  EXPECT_TRUE(need_push_lock_diag_stmt(sql::stmt::T_MERGE, false));
}

TEST_F(TestLockDiagStmtRing, select_for_update_true)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  EXPECT_TRUE(need_push_lock_diag_stmt(sql::stmt::T_SELECT, true));
}

TEST_F(TestLockDiagStmtRing, select_not_for_update)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  EXPECT_FALSE(need_push_lock_diag_stmt(sql::stmt::T_SELECT, false));
}

TEST_F(TestLockDiagStmtRing, other_types_return_false)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  EXPECT_FALSE(need_push_lock_diag_stmt(sql::stmt::T_NONE, false));
  EXPECT_FALSE(need_push_lock_diag_stmt(sql::stmt::T_EXPLAIN, false));
  EXPECT_FALSE(need_push_lock_diag_stmt(sql::stmt::T_CREATE_TABLE, false));
  EXPECT_FALSE(need_push_lock_diag_stmt(sql::stmt::T_SHOW_TABLES, false));
}

// ==================== truncate_lock_diag_query_sql_len tests ====================

TEST_F(TestLockDiagStmtRing, null_sql_returns_zero)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  EXPECT_EQ(0, truncate_lock_diag_query_sql_len(NULL, 100));
}

TEST_F(TestLockDiagStmtRing, zero_len_returns_zero)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  EXPECT_EQ(0, truncate_lock_diag_query_sql_len("abc", 0));
}

TEST_F(TestLockDiagStmtRing, negative_len_returns_zero)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  EXPECT_EQ(0, truncate_lock_diag_query_sql_len("abc", -1));
}

TEST_F(TestLockDiagStmtRing, short_sql_returns_actual_len)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  EXPECT_EQ(50, truncate_lock_diag_query_sql_len("abc", 50));
}

TEST_F(TestLockDiagStmtRing, exact_limit_returns_limit)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  char sql[256];
  MEMSET(sql, 'x', sizeof(sql));
  EXPECT_EQ(LOCK_DIAG_QUERY_SQL_LIMIT, truncate_lock_diag_query_sql_len(sql, LOCK_DIAG_QUERY_SQL_LIMIT));
}

TEST_F(TestLockDiagStmtRing, long_sql_truncates_to_limit)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  char sql[512];
  MEMSET(sql, 'x', sizeof(sql));
  EXPECT_EQ(LOCK_DIAG_QUERY_SQL_LIMIT, truncate_lock_diag_query_sql_len(sql, 500));
}

// ==================== ObTxStmtRing tests ====================

TEST_F(TestLockDiagStmtRing, empty_ring_lookup_not_found)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObTxStmtRing ring;
  ring.snapshot_inited_ = true;
  ring.collect_query_sql_ = false;

  ObLockDiagStmtSlot stmt_info;
  ObLockDiagQuerySqlSlot query_sql_info;
  bool has_query_sql = false;
  int ret = ring.lookup_stmt_info(1, stmt_info, query_sql_info, has_query_sql);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, ret);
  EXPECT_FALSE(has_query_sql);
}

TEST_F(TestLockDiagStmtRing, push_one_and_lookup)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObTxStmtRing ring;
  ring.snapshot_inited_ = true;
  ring.collect_query_sql_ = false;

  const char *sql_id = "ABCDEF1234567890ABCDEF1234567890";
  ring.push(1, sql_id, NULL, 0, OB_SERVER_TENANT_ID);

  ObLockDiagStmtSlot stmt_info;
  ObLockDiagQuerySqlSlot query_sql_info;
  bool has_query_sql = false;
  int ret = ring.lookup_stmt_info(1, stmt_info, query_sql_info, has_query_sql);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(1, stmt_info.seq_start_);
  EXPECT_STREQ(sql_id, stmt_info.sql_id_);
  EXPECT_FALSE(has_query_sql);
}

TEST_F(TestLockDiagStmtRing, lookup_seq_too_small)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObTxStmtRing ring;
  ring.snapshot_inited_ = true;
  ring.collect_query_sql_ = false;

  ring.push(5, "SQL_ID_5________________________", NULL, 0, OB_SERVER_TENANT_ID);

  ObLockDiagStmtSlot stmt_info;
  ObLockDiagQuerySqlSlot query_sql_info;
  bool has_query_sql = false;
  int ret = ring.lookup_stmt_info(3, stmt_info, query_sql_info, has_query_sql);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, ret);
}

TEST_F(TestLockDiagStmtRing, lookup_finds_best_match)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObTxStmtRing ring;
  ring.snapshot_inited_ = true;
  ring.collect_query_sql_ = false;

  ring.push(1, "SQL_ID_1________________________", NULL, 0, OB_SERVER_TENANT_ID);
  ring.push(3, "SQL_ID_3________________________", NULL, 0, OB_SERVER_TENANT_ID);
  ring.push(5, "SQL_ID_5________________________", NULL, 0, OB_SERVER_TENANT_ID);
  ring.push(7, "SQL_ID_7________________________", NULL, 0, OB_SERVER_TENANT_ID);

  ObLockDiagStmtSlot stmt_info;
  ObLockDiagQuerySqlSlot query_sql_info;
  bool has_query_sql = false;
  int ret = ring.lookup_stmt_info(6, stmt_info, query_sql_info, has_query_sql);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(5, stmt_info.seq_start_);
  EXPECT_STREQ("SQL_ID_5________________________", stmt_info.sql_id_);
}

TEST_F(TestLockDiagStmtRing, full_ring_wrap_around)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObTxStmtRing ring;
  ring.snapshot_inited_ = true;
  ring.collect_query_sql_ = false;

  // push 20 entries, ring only holds 16
  char sql_id[OB_MAX_SQL_ID_LENGTH + 1];
  for (int64_t i = 1; i <= 20; ++i) {
    fill_lock_diag_sql_id(i, sql_id, sizeof(sql_id));
    ring.push(i, sql_id, NULL, 0, OB_SERVER_TENANT_ID);
  }

  // lookup the latest one (seq=20) should succeed
  ObLockDiagStmtSlot stmt_info;
  ObLockDiagQuerySqlSlot query_sql_info;
  bool has_query_sql = false;
  int ret = ring.lookup_stmt_info(20, stmt_info, query_sql_info, has_query_sql);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(20, stmt_info.seq_start_);

  // lookup seq=18 (still within last 16) should also succeed
  ret = ring.lookup_stmt_info(18, stmt_info, query_sql_info, has_query_sql);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(18, stmt_info.seq_start_);
}

TEST_F(TestLockDiagStmtRing, old_data_overwritten)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObTxStmtRing ring;
  ring.snapshot_inited_ = true;
  ring.collect_query_sql_ = false;

  // push 20 entries
  char sql_id[OB_MAX_SQL_ID_LENGTH + 1];
  for (int64_t i = 1; i <= 20; ++i) {
    fill_lock_diag_sql_id(i, sql_id, sizeof(sql_id));
    ring.push(i, sql_id, NULL, 0, OB_SERVER_TENANT_ID);
  }

  // lookup seq=1 should fail because slot 0 now holds seq_start_=17
  // (entry 17 overwrites entry 1 at pos 0), and 17 > holder_seq=1
  ObLockDiagStmtSlot stmt_info;
  ObLockDiagQuerySqlSlot query_sql_info;
  bool has_query_sql = false;
  int ret = ring.lookup_stmt_info(1, stmt_info, query_sql_info, has_query_sql);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, ret);
}

TEST_F(TestLockDiagStmtRing, concurrent_single_writer_multi_reader)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObTxStmtRing ring;
  ring.snapshot_inited_ = true;
  ring.collect_query_sql_ = false;

  const int64_t push_count = 50000;
  const int64_t reader_count = 4;
  const int64_t reader_rounds_after_done = 2048;

  std::atomic<bool> start(false);
  std::atomic<bool> writer_done(false);
  std::atomic<int64_t> ready_count(0);
  std::atomic<int64_t> latest_seq(0);
  std::atomic<int64_t> success_count(0);
  std::atomic<int64_t> eagain_count(0);
  std::atomic<int64_t> not_exist_count(0);
  std::atomic<int64_t> bad_ret_count(0);
  std::atomic<int64_t> bad_seq_count(0);
  std::atomic<int64_t> bad_sql_id_count(0);
  std::atomic<int64_t> bad_query_sql_count(0);

  auto wait_start = [&]() {
    ready_count.fetch_add(1, std::memory_order_acq_rel);
    while (!start.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
  };

  std::vector<std::thread> threads;
  threads.reserve(reader_count + 1);
  threads.push_back(std::thread([&]() {
    wait_start();
    char sql_id[OB_MAX_SQL_ID_LENGTH + 1];
    for (int64_t seq = 1; seq <= push_count; ++seq) {
      fill_lock_diag_sql_id(seq, sql_id, sizeof(sql_id));
      ring.push(seq, sql_id, NULL, 0, OB_SERVER_TENANT_ID);
      latest_seq.store(seq, std::memory_order_release);
      if (0 == seq % LOCK_DIAG_RING_SIZE) {
        std::this_thread::yield();
      }
    }
    writer_done.store(true, std::memory_order_release);
  }));

  for (int64_t reader_id = 0; reader_id < reader_count; ++reader_id) {
    threads.push_back(std::thread([&, reader_id]() {
      wait_start();
      int64_t round = 0;
      int64_t after_done_round = 0;
      while (after_done_round < reader_rounds_after_done) {
        const bool done = writer_done.load(std::memory_order_acquire);
        const int64_t latest = latest_seq.load(std::memory_order_acquire);
        if (latest <= 0) {
          std::this_thread::yield();
          continue;
        }
        const int64_t offset = (round + reader_id) % (2 * LOCK_DIAG_RING_SIZE);
        const int64_t holder_seq = (latest > offset) ? (latest - offset) : 1;
        ObLockDiagStmtSlot stmt_info;
        ObLockDiagQuerySqlSlot query_sql_info;
        bool has_query_sql = false;
        const int ret = ring.lookup_stmt_info(holder_seq, stmt_info, query_sql_info, has_query_sql);
        if (OB_SUCCESS == ret) {
          success_count.fetch_add(1, std::memory_order_relaxed);
          if (stmt_info.seq_start_ <= 0 || stmt_info.seq_start_ > holder_seq) {
            bad_seq_count.fetch_add(1, std::memory_order_relaxed);
          }
          char expected_sql_id[OB_MAX_SQL_ID_LENGTH + 1];
          fill_lock_diag_sql_id(stmt_info.seq_start_, expected_sql_id, sizeof(expected_sql_id));
          if (0 != strncmp(expected_sql_id, stmt_info.sql_id_, OB_MAX_SQL_ID_LENGTH + 1)) {
            bad_sql_id_count.fetch_add(1, std::memory_order_relaxed);
          }
          if (has_query_sql) {
            bad_query_sql_count.fetch_add(1, std::memory_order_relaxed);
          }
        } else if (OB_EAGAIN == ret) {
          eagain_count.fetch_add(1, std::memory_order_relaxed);
        } else if (OB_ENTRY_NOT_EXIST == ret) {
          not_exist_count.fetch_add(1, std::memory_order_relaxed);
        } else {
          bad_ret_count.fetch_add(1, std::memory_order_relaxed);
        }
        ++round;
        if (done) {
          ++after_done_round;
        } else if (0 == round % LOCK_DIAG_RING_SIZE) {
          std::this_thread::yield();
        }
      }
    }));
  }

  while (ready_count.load(std::memory_order_acquire) < reader_count + 1) {
    std::this_thread::yield();
  }
  start.store(true, std::memory_order_release);
  for (auto &thread : threads) {
    thread.join();
  }

  EXPECT_EQ(push_count, latest_seq.load(std::memory_order_acquire));
  EXPECT_TRUE(writer_done.load(std::memory_order_acquire));
  EXPECT_EQ(0, bad_ret_count.load(std::memory_order_relaxed));
  EXPECT_EQ(0, bad_seq_count.load(std::memory_order_relaxed));
  EXPECT_EQ(0, bad_sql_id_count.load(std::memory_order_relaxed));
  EXPECT_EQ(0, bad_query_sql_count.load(std::memory_order_relaxed));
  EXPECT_GT(success_count.load(std::memory_order_relaxed), 0);
  EXPECT_GT(eagain_count.load(std::memory_order_relaxed)
      + not_exist_count.load(std::memory_order_relaxed)
      + success_count.load(std::memory_order_relaxed), 0);
}

TEST_F(TestLockDiagStmtRing, reset_clears_state)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObTxStmtRing ring;
  ring.snapshot_inited_ = true;
  ring.collect_query_sql_ = false;

  for (int64_t i = 1; i <= 5; ++i) {
    ring.push(i, "SQL_ID_X________________________", NULL, 0, OB_SERVER_TENANT_ID);
  }

  // version before reset = 2*5 = 10
  uint64_t ver_before_reset = ATOMIC_LOAD(&ring.seqlock_version_);
  EXPECT_EQ(10u, ver_before_reset);

  ring.reset();

  // After reset: version should be ver_before_reset + 2 = 12 (still even)
  uint64_t ver_after_reset = ATOMIC_LOAD(&ring.seqlock_version_);
  EXPECT_EQ(ver_before_reset + 2, ver_after_reset);
  EXPECT_EQ(0u, ver_after_reset & 1); // must be even

  ObLockDiagStmtSlot stmt_info;
  ObLockDiagQuerySqlSlot query_sql_info;
  bool has_query_sql = false;
  int ret = ring.lookup_stmt_info(3, stmt_info, query_sql_info, has_query_sql);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, ret);
}

TEST_F(TestLockDiagStmtRing, push_after_reset)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObTxStmtRing ring;
  ring.snapshot_inited_ = true;
  ring.collect_query_sql_ = false;

  ring.push(1, "SQL_ID_1________________________", NULL, 0, OB_SERVER_TENANT_ID);
  ring.reset();

  // After reset, snapshot_inited_ is false; set it again to bypass GCONF
  ring.snapshot_inited_ = true;
  ring.collect_query_sql_ = false;

  ring.push(10, "SQL_ID_10_______________________", NULL, 0, OB_SERVER_TENANT_ID);

  ObLockDiagStmtSlot stmt_info;
  ObLockDiagQuerySqlSlot query_sql_info;
  bool has_query_sql = false;
  int ret = ring.lookup_stmt_info(10, stmt_info, query_sql_info, has_query_sql);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(10, stmt_info.seq_start_);
  EXPECT_STREQ("SQL_ID_10_______________________", stmt_info.sql_id_);
}

TEST_F(TestLockDiagStmtRing, query_sql_not_collected_when_disabled)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObTxStmtRing ring;
  ring.snapshot_inited_ = true;
  ring.collect_query_sql_ = false;
  // query_sql_ring_ stays nullptr

  const char *query_sql = "SELECT * FROM t1 WHERE id = 1";
  ring.push(1, "SQL_ID_A________________________", query_sql, strlen(query_sql), OB_SERVER_TENANT_ID);

  ObLockDiagStmtSlot stmt_info;
  ObLockDiagQuerySqlSlot query_sql_info;
  bool has_query_sql = false;
  int ret = ring.lookup_stmt_info(1, stmt_info, query_sql_info, has_query_sql);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_FALSE(has_query_sql);
}

TEST_F(TestLockDiagStmtRing, query_sql_collected_when_enabled)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObTxStmtRing ring;
  ring.snapshot_inited_ = true;
  ring.collect_query_sql_ = true;

  // Manually allocate query_sql_ring_
  void *buf = ob_malloc(sizeof(ObLockDiagQuerySqlSlot) * LOCK_DIAG_RING_SIZE,
                        lib::ObMemAttr(OB_SERVER_TENANT_ID, "TestDiag"));
  ASSERT_NE(nullptr, buf);
  ring.query_sql_ring_ = reinterpret_cast<ObLockDiagQuerySqlSlot *>(buf);
  MEMSET(ring.query_sql_ring_, 0, sizeof(ObLockDiagQuerySqlSlot) * LOCK_DIAG_RING_SIZE);

  const char *query_sql = "INSERT INTO t1 VALUES(1, 'hello')";
  ring.push(1, "SQL_ID_B________________________", query_sql, strlen(query_sql), OB_SERVER_TENANT_ID);

  ObLockDiagStmtSlot stmt_info;
  ObLockDiagQuerySqlSlot query_sql_info;
  bool has_query_sql = false;
  int ret = ring.lookup_stmt_info(1, stmt_info, query_sql_info, has_query_sql);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_TRUE(has_query_sql);
  EXPECT_STREQ(query_sql, query_sql_info.query_sql_);
  // Destructor will ob_free(query_sql_ring_)
}

TEST_F(TestLockDiagStmtRing, query_sql_truncation)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObTxStmtRing ring;
  ring.snapshot_inited_ = true;
  ring.collect_query_sql_ = true;

  // Manually allocate query_sql_ring_
  void *buf = ob_malloc(sizeof(ObLockDiagQuerySqlSlot) * LOCK_DIAG_RING_SIZE,
                        lib::ObMemAttr(OB_SERVER_TENANT_ID, "TestDiag"));
  ASSERT_NE(nullptr, buf);
  ring.query_sql_ring_ = reinterpret_cast<ObLockDiagQuerySqlSlot *>(buf);
  MEMSET(ring.query_sql_ring_, 0, sizeof(ObLockDiagQuerySqlSlot) * LOCK_DIAG_RING_SIZE);

  // Create a 300-byte SQL string
  char long_sql[301];
  MEMSET(long_sql, 'A', 300);
  long_sql[300] = '\0';

  ring.push(1, "SQL_ID_C________________________", long_sql, 300, OB_SERVER_TENANT_ID);

  ObLockDiagStmtSlot stmt_info;
  ObLockDiagQuerySqlSlot query_sql_info;
  bool has_query_sql = false;
  int ret = ring.lookup_stmt_info(1, stmt_info, query_sql_info, has_query_sql);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_TRUE(has_query_sql);
  // Should be truncated to LOCK_DIAG_QUERY_SQL_LIMIT (200) bytes
  EXPECT_EQ(LOCK_DIAG_QUERY_SQL_LIMIT, static_cast<int64_t>(strlen(query_sql_info.query_sql_)));
  // Verify content is the first 200 chars
  for (int64_t i = 0; i < LOCK_DIAG_QUERY_SQL_LIMIT; ++i) {
    EXPECT_EQ('A', query_sql_info.query_sql_[i]);
  }
}

TEST_F(TestLockDiagStmtRing, null_sql_id_handled)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObTxStmtRing ring;
  ring.snapshot_inited_ = true;
  ring.collect_query_sql_ = false;

  ring.push(1, NULL, NULL, 0, OB_SERVER_TENANT_ID);

  ObLockDiagStmtSlot stmt_info;
  ObLockDiagQuerySqlSlot query_sql_info;
  bool has_query_sql = false;
  int ret = ring.lookup_stmt_info(1, stmt_info, query_sql_info, has_query_sql);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(1, stmt_info.seq_start_);
  EXPECT_EQ('\0', stmt_info.sql_id_[0]);
}

TEST_F(TestLockDiagStmtRing, null_query_sql_handled)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObTxStmtRing ring;
  ring.snapshot_inited_ = true;
  ring.collect_query_sql_ = true;

  // Manually allocate query_sql_ring_
  void *buf = ob_malloc(sizeof(ObLockDiagQuerySqlSlot) * LOCK_DIAG_RING_SIZE,
                        lib::ObMemAttr(OB_SERVER_TENANT_ID, "TestDiag"));
  ASSERT_NE(nullptr, buf);
  ring.query_sql_ring_ = reinterpret_cast<ObLockDiagQuerySqlSlot *>(buf);
  MEMSET(ring.query_sql_ring_, 0, sizeof(ObLockDiagQuerySqlSlot) * LOCK_DIAG_RING_SIZE);

  ring.push(1, "SQL_ID_D________________________", NULL, 0, OB_SERVER_TENANT_ID);

  ObLockDiagStmtSlot stmt_info;
  ObLockDiagQuerySqlSlot query_sql_info;
  bool has_query_sql = false;
  int ret = ring.lookup_stmt_info(1, stmt_info, query_sql_info, has_query_sql);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_FALSE(has_query_sql);
}

TEST_F(TestLockDiagStmtRing, seqlock_version_progression)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObTxStmtRing ring;
  ring.snapshot_inited_ = true;
  ring.collect_query_sql_ = false;

  EXPECT_EQ(0u, ATOMIC_LOAD(&ring.seqlock_version_));

  ring.push(1, "SQL_ID_1________________________", NULL, 0, OB_SERVER_TENANT_ID);
  EXPECT_EQ(2u, ATOMIC_LOAD(&ring.seqlock_version_));

  ring.push(2, "SQL_ID_2________________________", NULL, 0, OB_SERVER_TENANT_ID);
  EXPECT_EQ(4u, ATOMIC_LOAD(&ring.seqlock_version_));

  ring.push(3, "SQL_ID_3________________________", NULL, 0, OB_SERVER_TENANT_ID);
  EXPECT_EQ(6u, ATOMIC_LOAD(&ring.seqlock_version_));

  // After N pushes, version == 2*N; version is always even
  const int N = 10;
  ObTxStmtRing ring2;
  ring2.snapshot_inited_ = true;
  ring2.collect_query_sql_ = false;
  for (int i = 1; i <= N; ++i) {
    ring2.push(i, "ID______________________________", NULL, 0, OB_SERVER_TENANT_ID);
  }
  EXPECT_EQ(static_cast<uint64_t>(2 * N), ATOMIC_LOAD(&ring2.seqlock_version_));
  EXPECT_EQ(0u, ATOMIC_LOAD(&ring2.seqlock_version_) & 1);
}

// ==================== Design review suggestion: OB_EAGAIN simulation ====================

TEST_F(TestLockDiagStmtRing, lookup_eagain_when_version_odd)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObTxStmtRing ring;
  ring.snapshot_inited_ = true;
  ring.collect_query_sql_ = false;

  // Push one entry so there is data to find
  ring.push(1, "SQL_ID_1________________________", NULL, 0, OB_SERVER_TENANT_ID);
  EXPECT_EQ(2u, ATOMIC_LOAD(&ring.seqlock_version_));

  // Manually set version to odd to simulate concurrent write
  ATOMIC_STORE(&ring.seqlock_version_, 3u);

  ObLockDiagStmtSlot stmt_info;
  ObLockDiagQuerySqlSlot query_sql_info;
  bool has_query_sql = false;
  int ret = ring.lookup_stmt_info(1, stmt_info, query_sql_info, has_query_sql);
  // Both retries see odd version, so we get EAGAIN
  EXPECT_EQ(OB_EAGAIN, ret);
  EXPECT_FALSE(has_query_sql);

  // Restore to even so destructor works cleanly
  ATOMIC_STORE(&ring.seqlock_version_, 4u);
}

}//end of unittest
}//end of oceanbase

using namespace oceanbase;
using namespace oceanbase::common;

int main(int argc, char **argv)
{
  int ret = 1;
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_lock_diag_stmt_ring.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc, argv);
  ret = RUN_ALL_TESTS();
  return ret;
}
