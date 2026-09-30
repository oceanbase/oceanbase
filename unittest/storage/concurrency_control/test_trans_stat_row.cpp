/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>

#define private public
#include "storage/memtable/mvcc/ob_mvcc_iterator.h"
#undef private
#include "sql/das/ob_das_dml_ctx_define.h"

namespace oceanbase
{
namespace unittest
{
using namespace common;
using namespace share;
using namespace memtable;
using namespace storage;
using namespace transaction;
using namespace concurrency_control;

// Exercise the same formatter and buffer capacity used by point reads and scans.
static void check_trans_info(const ObTransStatRow &stat,
                            const int64_t expected_version,
                            const int64_t expected_scn,
                            const int64_t expected_tx_id,
                            const int64_t expected_seq,
                            const int64_t expected_snapshot,
                            const void *expected_row,
                            const void *expected_node)
{
  struct {
    char data_[ObTransStatRow::MAX_TRANS_STRING_SIZE];
    char guard_;
  } buffer;
  MEMSET(&buffer, 'x', sizeof(buffer));
  ObTableIterParam param;
  param.need_trans_info_ = true;
  blocksstable::ObDatumRow row;
  row.trans_info_ = buffer.data_;
  build_trans_stat_datum(&param, row, stat);

  ASSERT_EQ('x', buffer.guard_);
  ASSERT_LT(strnlen(buffer.data_, sizeof(buffer.data_)), sizeof(buffer.data_));
  int64_t version = 0, scn = 0, tx_id = 0, seq = 0, snapshot = 0;
  int branch = -1, consumed = 0;
  void *mvcc_row = nullptr;
  void *node = nullptr;
  ASSERT_EQ(8, sscanf(buffer.data_, "[%ld, %ld, %ld, (%d,%ld), %ld, %p, %p]%n",
                      &version, &scn, &tx_id, &branch, &seq, &snapshot, &mvcc_row, &node, &consumed))
      << buffer.data_;
  EXPECT_EQ(strlen(buffer.data_), consumed);
  EXPECT_EQ(expected_version, version);
  EXPECT_EQ(expected_scn, scn);
  EXPECT_EQ(expected_tx_id, tx_id);
  EXPECT_EQ(0, branch);
  EXPECT_EQ(expected_seq, seq);
  EXPECT_EQ(expected_snapshot, snapshot);
  EXPECT_EQ(expected_row, mvcc_row);
  EXPECT_EQ(expected_node, node);

  // The pushdown operator copies this string into the DAS diagnostic payload.
  char das_buffer[sql::ObDASWriteBuffer::DAS_ROW_TRANS_STRING_SIZE];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, databuff_memcpy(das_buffer, sizeof(das_buffer), pos,
                                      strlen(buffer.data_), buffer.data_));
  EXPECT_EQ(0, MEMCMP(das_buffer, buffer.data_, pos));
}

TEST(ObTransStatRowTest, captures_first_visible_node_instead_of_chain_head)
{
  ObMvccAccessCtx ctx;
  ObMvccRow mvcc_row;
  ObMvccTransNode newer_node, visible_node;
  SCN newer_version, visible_version;
  ASSERT_EQ(OB_SUCCESS, newer_version.convert_for_tx(300));
  ASSERT_EQ(OB_SUCCESS, visible_version.convert_for_tx(100));
  ASSERT_EQ(OB_SUCCESS, ctx.snapshot_.version_.convert_for_tx(200));
  newer_node.trans_commit(newer_version, newer_version);
  visible_node.trans_commit(visible_version, visible_version);
  visible_node.scn_ = visible_version;
  visible_node.tx_id_ = ObTransID(42);
  visible_node.seq_no_ = ObTxSEQ::mk_v0(7);
  newer_node.prev_ = &visible_node;
  mvcc_row.list_head_ = &newer_node;

  ObMvccValueIterator iter;
  ObTransStatRow stat;
  ASSERT_EQ(OB_SUCCESS, iter.init(ctx, nullptr, &mvcc_row, ObLSID(1), ObQueryFlag()));
  iter.get_trans_stat_row(stat);
  check_trans_info(stat, 100, 100, 42, 7, 200, &mvcc_row, &visible_node);

  // Reusing the diagnostic row must not retain a node from the previous read.
  ASSERT_EQ(OB_SUCCESS, ctx.snapshot_.version_.convert_for_tx(50));
  ASSERT_EQ(OB_SUCCESS, iter.init(ctx, nullptr, &mvcc_row, ObLSID(1), ObQueryFlag()));
  iter.get_trans_stat_row(stat);
  check_trans_info(stat, INT64_MAX, INT64_MAX, 0, 0, 50, &mvcc_row, nullptr);

  ASSERT_EQ(OB_SUCCESS, iter.init(ctx, nullptr, nullptr, ObLSID(1), ObQueryFlag()));
  iter.get_trans_stat_row(stat);
  check_trans_info(stat, INT64_MAX, INT64_MAX, 0, 0, 50, nullptr, nullptr);

  iter.reset();
  iter.get_trans_stat_row(stat);
  check_trans_info(stat, INT64_MAX, INT64_MAX, 0, 0, INT64_MAX, nullptr, nullptr);
}

// Canonical addresses used only for formatting; they are never dereferenced.
static void prepare_stat(ObTransStatRow &stat, const int64_t sequence)
{
  SCN version, snapshot;
  ASSERT_EQ(OB_SUCCESS, version.convert_for_tx(1790150260000000000L));
  ASSERT_EQ(OB_SUCCESS, snapshot.convert_for_tx(1790150260000000001L));
  stat.set(version, version, ObTransID(1234567890123L), ObTxSEQ(sequence, 0));
  stat.snapshot_ = snapshot;
  stat.mvcc_row_ = reinterpret_cast<const void *>(0x7f1234567890UL);
  stat.first_trans_node_ = reinterpret_cast<const void *>(0x7f12345678a0UL);
}

TEST(ObTransStatRowTest, ordinary_values_fit_in_120_bytes)
{
  ObTransStatRow stat;
  prepare_stat(stat, 1);
  check_trans_info(stat, 1790150260000000000L, 1790150260000000000L,
                   1234567890123L, 1, 1790150260000000001L,
                   stat.mvcc_row_, stat.first_trans_node_);
  struct {
    char data_[120];
    char guard_;
  } buffer;
  MEMSET(&buffer, 'x', sizeof(buffer));
  build_trans_stat_(stat, sizeof(buffer.data_), buffer.data_);
  EXPECT_STREQ("[1790150260000000000, 1790150260000000000, 1234567890123, (0,1), "
               "1790150260000000001, 0x7f1234567890, 0x7f12345678a0]", buffer.data_);
  EXPECT_EQ('x', buffer.guard_);
}

TEST(ObTransStatRowTest, overflow_keeps_prefix_instead_of_clearing_it)
{
  ObTransStatRow stat;
  prepare_stat(stat, 123456);
  const std::string full =
      "[1790150260000000000, 1790150260000000000, 1234567890123, (0,123456), "
      "1790150260000000001, 0x7f1234567890, 0x7f12345678a0]";
  struct {
    char data_[ObTransStatRow::MAX_TRANS_STRING_SIZE];
    char guard_;
  } buffer;
  MEMSET(&buffer, 'x', sizeof(buffer));
  ObTableIterParam param;
  param.need_trans_info_ = true;
  blocksstable::ObDatumRow row;
  row.trans_info_ = buffer.data_;
  build_trans_stat_datum(&param, row, stat);
  ASSERT_EQ('x', buffer.guard_);
  ASSERT_EQ('\0', buffer.data_[119]);
  EXPECT_EQ(full.substr(0, 119), std::string(buffer.data_));
  char das_buffer[128];
  int64_t pos = 0;
  EXPECT_EQ(OB_SUCCESS, databuff_memcpy(das_buffer, sizeof(das_buffer), pos,
                                      strlen(buffer.data_), buffer.data_));
}

TEST(ObTransStatRowTest, respects_zero_tiny_exact_and_short_buffers)
{
  ObTransStatRow stat;
  prepare_stat(stat, 1);
  const std::string full =
      "[1790150260000000000, 1790150260000000000, 1234567890123, (0,1), "
      "1790150260000000001, 0x7f1234567890, 0x7f12345678a0]";
  const int64_t capacities[] = {0, 1, 2, static_cast<int64_t>(full.size()),
      static_cast<int64_t>(full.size()) + 1, 120};
  for (int64_t capacity : capacities) {
    char buffer[121];
    MEMSET(buffer, 'x', sizeof(buffer));
    build_trans_stat_(stat, capacity, buffer);
    if (capacity > 0) {
      ASSERT_LT(strnlen(buffer, capacity), capacity);
      EXPECT_EQ(full.substr(0, capacity - 1), std::string(buffer));
    }
    for (int64_t i = capacity; i < sizeof(buffer); ++i) {
      ASSERT_EQ('x', buffer[i]) << "buffer overwritten at " << i;
    }
  }
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_trans_stat_row.log", true);
  OB_LOGGER.set_log_level("WARN");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
