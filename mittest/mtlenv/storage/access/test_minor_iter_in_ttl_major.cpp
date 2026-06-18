/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#define private public
#define protected public
#define UNITTEST
#include "storage/compaction/ob_partition_merger.h"
#include "storage/test_tablet_helper.h"
#include "test_merge_basic.h"
#include "storage/mockcontainer/mock_ob_merge_iterator.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
using namespace blocksstable;
using namespace compaction;
using namespace unittest;

namespace storage
{

class ObMockMajorMergeIter
{
public:
  explicit ObMockMajorMergeIter(ObPartitionMergeIter *iter)
    : iter_(iter), first_row_(true), allocator_("MockMajorIter"), cached_row_()
  {}

  int get_next_row(const ObDatumRow *&row)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(iter_)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (iter_->is_iter_end()) {
      ret = OB_ITER_END;
    } else if (OB_ISNULL(iter_->get_curr_row())) {
      ret = OB_ERR_UNEXPECTED;
    } else if (first_row_) {
      first_row_ = false;
      row = iter_->get_curr_row();
    } else if (OB_SUCC(iter_->next())) {
      row = iter_->get_curr_row();
    }
    return ret;
  }

private:
  ObPartitionMergeIter *iter_;
  bool first_row_;
  common::ObArenaAllocator allocator_;
  ObDatumRow cached_row_;
};

class TestMinorIterInTTLMajor : public TestMergeBasic
{
public:
  TestMinorIterInTTLMajor();
  virtual ~TestMinorIterInTTLMajor() {}

  static void SetUpTestCase();
  static void TearDownTestCase();
  void SetUp();
  void TearDown();

  void prepare_merge_context(
      const ObMergeType &merge_type,
      const bool is_full_merge,
      const ObVersionRange &trans_version_range,
      ObTabletMajorMergeCtx &merge_context)
  {
    TestMergeBasic::prepare_merge_context(
        merge_type,
        is_full_merge,
        trans_version_range,
        &merge_dag_,
        merge_context,
        ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE);
  }

private:
  compaction::ObLocalArena local_arena_;
  ObTabletMergeExecuteDag merge_dag_;
};

TestMinorIterInTTLMajor::TestMinorIterInTTLMajor()
  : TestMergeBasic("test_minor_iter_in_ttl_major"),
    local_arena_("TTLMajorIter", OB_MALLOC_NORMAL_BLOCK_SIZE)
{}

void TestMinorIterInTTLMajor::SetUpTestCase()
{
  ObMultiVersionSSTableTest::SetUpTestCase();
  ObClockGenerator::init();
  TestMergeBasic::create_tablet();
}

void TestMinorIterInTTLMajor::TearDownTestCase()
{
  ObMultiVersionSSTableTest::TearDownTestCase();
  ObClockGenerator::destroy();
}

void TestMinorIterInTTLMajor::SetUp()
{
  ObMultiVersionSSTableTest::SetUp();
}

void TestMinorIterInTTLMajor::TearDown()
{
  ObMultiVersionSSTableTest::TearDown();
}

TEST_F(TestMinorIterInTTLMajor, test_dump_sstable_iter_for_ttl_major_partial_update)
{
  const int64_t schema_rowkey_cnt = 1;
  const int64_t last_major_compaction_scn = 20;
  const int64_t major_compaction_scn = 40;

  ObTabletMergeDagParam param;
  ObTabletMajorMergeCtx merge_context(param, allocator_);
  ObPartitionMajorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 major_handle;
  const char *major_data[1];
  major_data[0] =
      "bigint   bigint   bigint   bigint   flag    multi_version_row_flag\n"
      "1        -15      0        100      EXIST   N\n"
      "2        -18      0        200      EXIST   N\n"
      "3        -12      0        300      EXIST   N\n"
      "4        -16      0        400      EXIST   N\n";

  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(last_major_compaction_scn);
  prepare_table_schema(major_data, schema_rowkey_cnt, scn_range, last_major_compaction_scn);
  reset_writer(last_major_compaction_scn, MAJOR_MERGE);
  prepare_one_macro(major_data, 1);
  prepare_data_end(major_handle, ObITable::MAJOR_SSTABLE);
  merge_context.static_param_.tables_handle_.add_table(major_handle);

  ObTableHandleV2 dump_handle;
  const char *dump_data[1];
  dump_data[0] =
      "bigint   bigint   bigint   bigint   flag    multi_version_row_flag\n"
      "1        -45      0        145      EXIST   LF\n"
      "2        -45      MIN      245      EXIST   SCF\n"
      "2        -45      0        245      EXIST   C\n"
      "2        -35      0        235      EXIST   CL\n"
      "3        -30      0        330      EXIST   LF\n"
      "4        -18      0        418      EXIST   LF\n"
      "5        -46      MIN      546      EXIST   SCF\n"
      "5        -46      0        546      EXIST   C\n"
      "5        -42      0        542      EXIST   CL\n";

  scn_range.start_scn_.convert_for_tx(last_major_compaction_scn);
  scn_range.end_scn_.convert_for_tx(50);
  table_key_.scn_range_ = scn_range;
  reset_writer(50);
  prepare_one_macro(dump_data, 1);
  prepare_data_end(dump_handle);
  merge_context.static_param_.tables_handle_.add_table(dump_handle);

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = major_compaction_scn;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = last_major_compaction_scn;

  prepare_merge_context(MAJOR_MERGE, false /* is_full_merge */, trans_version_range, merge_context);
  merge_context.static_param_.ttl_major_for_partial_update_upper_snapshot_ = 50;

  ASSERT_EQ(OB_SUCCESS, merger.prepare_merge(merge_context, 0));
  ASSERT_NE(nullptr, merger.merge_helper_);
  ASSERT_EQ(2, merger.merge_helper_->get_merge_iters().count());

  ObPartitionMergeIter *iter = merger.merge_helper_->get_merge_iters().at(0);
  ASSERT_NE(nullptr, iter);
  ASSERT_EQ(1, iter->get_sstable_idx());
  ASSERT_FALSE(iter->is_base_sstable_iter());
  ASSERT_FALSE(iter->is_major_sstable_iter());

  // rowkey=1: only versions > major_compaction_scn, output last row and mark virtual row.
  // rowkey=2: both > major_compaction_scn and <= major_compaction_scn exist,
  //           output fused <= major_compaction_scn row and mark exist_new_committed_row.
  // rowkey=3: only versions in (last_major_compaction_scn, major_compaction_scn], output fused row only.
  // rowkey=4: all versions <= last_major_compaction_scn, output nothing.
  const char *result1 =
      "bigint   bigint   bigint   bigint   flag    multi_version_row_flag   major_merge_flag\n"
      "1        -45      0        145      EXIST   N                        V\n"
      "2        -35      0        235      EXIST   N                        E\n"
      "3        -30      0        330      EXIST   N                        N\n"
      "5        -42      0        542      EXIST   N                        V\n";

  ObMockIterator result_iter;
  ObMockMajorMergeIter major_iter(iter);
  ASSERT_EQ(OB_SUCCESS, result_iter.from_for_datum(result1));
  bool is_equal = result_iter.equals<ObMockMajorMergeIter, ObDatumRow>(
      major_iter, false /* cmp multi version row flag */);
  ASSERT_TRUE(is_equal);

  major_handle.reset();
  dump_handle.reset();
  merger.reset();
}

TEST_F(TestMinorIterInTTLMajor, test_dump_sstable_iter_with_uncommitted_row)
{
  // Transactions and their states:
  //   tx_id=1: COMMIT at version 30 (<=  major_compaction_scn=40) -> readable, output as committed
  //   tx_id=2: COMMIT at version 50 (>   major_compaction_scn=40) -> snapshot(40) < commit(50), skipped
  //   tx_id=3: ABORT                                               -> skipped
  //   tx_id=4: RUNNING                                             -> skipped
  //
  // Uncommitted row format: trans_version=MIN, sql_sequence=negative, flag includes U.
  //
  // Rowkey scenarios:
  //   rowkey=1: single uncommitted row, tx1 (commit at 30) -> output at version -30
  //   rowkey=2: single uncommitted row, tx2 (commit at 50) -> skipped (commit > snapshot)
  //   rowkey=3: single uncommitted row, tx3 (ABORT)        -> skipped
  //   rowkey=4: single uncommitted row, tx4 (RUNNING)      -> skipped
  //   rowkey=5: two uncommitted txns on same rowkey:
  //             tx3 (ABORT, F) + tx1 (commit at 30, L)    -> tx3 skipped, tx1 output at -30
  //   rowkey=6: two uncommitted txns on same rowkey:
  //             tx4 (RUNNING, F) + tx2 (commit at 50, L)  -> both skipped, no output
  const int64_t schema_rowkey_cnt = 1;
  const int64_t last_major_compaction_scn = 20;
  const int64_t major_compaction_scn = 40;

  ObTabletMergeDagParam param;
  ObTabletMajorMergeCtx merge_context(param, allocator_);
  ObPartitionMajorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 major_handle;
  const char *major_data[1];
  major_data[0] =
      "bigint   bigint   bigint   bigint   flag    multi_version_row_flag\n"
      "1        -15      0        100      EXIST   N\n"
      "2        -18      0        200      EXIST   N\n"
      "3        -12      0        300      EXIST   N\n"
      "4        -16      0        400      EXIST   N\n";

  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(last_major_compaction_scn);
  prepare_table_schema(major_data, schema_rowkey_cnt, scn_range, last_major_compaction_scn);
  reset_writer(last_major_compaction_scn, MAJOR_MERGE);
  prepare_one_macro(major_data, 1);
  prepare_data_end(major_handle, ObITable::MAJOR_SSTABLE);
  merge_context.static_param_.tables_handle_.add_table(major_handle);

  ObTableHandleV2 dump_handle;
  const char *dump_data[1];
  // Uncommitted rows: trans_version=MIN, sql_sequence=negative, flag includes U.
  // F = first multi-version row of rowkey, L = last, LF = both (single-row rowkey).
  dump_data[0] =
      "bigint   bigint   bigint   bigint   flag    multi_version_row_flag   trans_id\n"
      "1        MIN      -1       145      EXIST   ULF                      trans_id_1\n"
      "2        MIN      -1       245      EXIST   ULF                      trans_id_2\n"
      "3        MIN      -1       345      EXIST   ULF                      trans_id_3\n"
      "4        MIN      -1       445      EXIST   ULF                      trans_id_4\n"
      "5        MIN      -2       552      EXIST   FU                       trans_id_3\n"
      "5        MIN      -1       551      EXIST   LU                       trans_id_1\n"
      "6        MIN      -2       662      EXIST   FU                       trans_id_4\n"
      "6        MIN      -1       661      EXIST   LU                       trans_id_2\n";

  scn_range.start_scn_.convert_for_tx(last_major_compaction_scn);
  scn_range.end_scn_.convert_for_tx(50);
  table_key_.scn_range_ = scn_range;
  reset_writer(50);
  prepare_one_macro(dump_data, 1);
  prepare_data_end(dump_handle);
  merge_context.static_param_.tables_handle_.add_table(dump_handle);

  // tx1: commit at 30 (<= major_compaction_scn=40), readable
  insert_tx_data(1 /*tx_id*/, 30 /*commit_version*/);
  // tx2: commit at 50 (> major_compaction_scn=40), snapshot(40) < commit(50), skipped
  insert_tx_data(2 /*tx_id*/, 50 /*commit_version*/);
  // tx3: ABORT (commit_version=INT64_MAX triggers ABORT state)
  insert_tx_data(3 /*tx_id*/, INT64_MAX /*commit_version*/);
  // tx4: RUNNING (commit_version=0 triggers RUNNING state)
  insert_tx_data(4 /*tx_id*/, 0 /*commit_version*/);

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = major_compaction_scn;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = last_major_compaction_scn;

  prepare_merge_context(MAJOR_MERGE, false /* is_full_merge */, trans_version_range, merge_context);
  merge_context.static_param_.ttl_major_for_partial_update_upper_snapshot_ = 50;

  ASSERT_EQ(OB_SUCCESS, merger.prepare_merge(merge_context, 0));
  ASSERT_NE(nullptr, merger.merge_helper_);
  ASSERT_EQ(2, merger.merge_helper_->get_merge_iters().count());

  ObPartitionMergeIter *iter = merger.merge_helper_->get_merge_iters().at(0);
  ASSERT_NE(nullptr, iter);
  ASSERT_EQ(1, iter->get_sstable_idx());
  ASSERT_FALSE(iter->is_base_sstable_iter());
  ASSERT_FALSE(iter->is_major_sstable_iter());

  // rowkey=1: tx1 commits at 30 (<=40), readable -> output at version -30, sql_seq=-1 retained
  // rowkey=2: tx2 commits at 50 (>40), snapshot(40)<commit(50) -> output virtual row
  // rowkey=3: tx3 ABORT -> skipped, no output
  // rowkey=4: tx4 RUNNING -> skipped, no output
  // rowkey=5: tx3 ABORT (F, skipped) + tx1 commit at 30 (L, readable) -> output at version -30
  // rowkey=6: tx4 RUNNING (F, skipped) + tx2 commit at 50 (L, skipped) -> output virtual row
  const char *result1 =
      "bigint   bigint   bigint   bigint   flag    multi_version_row_flag   major_merge_flag\n"
      "1        -30      -1       145      EXIST   N                        N\n"
      "2        -50      -1       245      EXIST   N                        V\n"
      "5        -30      -1       551      EXIST   N                        N\n"
      "6        -50      -1       661      EXIST   N                        V\n";

  ObMockIterator result_iter;
  ObMockMajorMergeIter major_iter(iter);
  ASSERT_EQ(OB_SUCCESS, result_iter.from_for_datum(result1));
  bool is_equal = result_iter.equals<ObMockMajorMergeIter, ObDatumRow>(
      major_iter, false /* cmp multi version row flag */);
  ASSERT_TRUE(is_equal);

  clear_tx_data();
  major_handle.reset();
  dump_handle.reset();
  merger.reset();
}

TEST_F(TestMinorIterInTTLMajor, test_dump_sstable_iter_with_uncommitted_row_2)
{
  // Transactions and their states:
  //   tx_id=1: COMMIT at version 30 (<=  major_compaction_scn=40) -> readable, output as committed
  //   tx_id=2: COMMIT at version 50 (>   major_compaction_scn=40) -> snapshot(40) < commit(50), skipped
  //   tx_id=3: ABORT                                               -> skipped
  //   tx_id=4: RUNNING                                             -> skipped
  //
  // Uncommitted row format: trans_version=MIN, sql_sequence=negative, flag includes U.
  //
  // Rowkey scenarios:
  //   rowkey=1: single uncommitted row, tx1 (commit at 30) -> output at version -30
  //   rowkey=2: single uncommitted row, tx2 (commit at 50) -> skipped (commit > snapshot)
  //   rowkey=3: single uncommitted row, tx3 (ABORT)        -> skipped
  //   rowkey=4: single uncommitted row, tx4 (RUNNING)      -> skipped
  //   rowkey=5: two uncommitted txns on same rowkey:
  //             tx3 (ABORT, F) + tx1 (commit at 30, L)    -> tx3 skipped, tx1 output at -30
  //   rowkey=6: two uncommitted txns on same rowkey:
  //             tx4 (RUNNING, F) + tx2 (commit at 50, L)  -> both skipped, no output
  const int64_t schema_rowkey_cnt = 1;
  const int64_t last_major_compaction_scn = 20;
  const int64_t major_compaction_scn = 40;

  ObTabletMergeDagParam param;
  ObTabletMajorMergeCtx merge_context(param, allocator_);
  ObPartitionMajorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 major_handle;
  const char *major_data[1];
  major_data[0] =
      "bigint   bigint   bigint   bigint   flag    multi_version_row_flag\n"
      "1        -15      0        100      EXIST   N\n"
      "2        -18      0        200      EXIST   N\n"
      "3        -12      0        300      EXIST   N\n"
      "4        -16      0        400      EXIST   N\n";

  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(last_major_compaction_scn);
  prepare_table_schema(major_data, schema_rowkey_cnt, scn_range, last_major_compaction_scn);
  reset_writer(last_major_compaction_scn, MAJOR_MERGE);
  prepare_one_macro(major_data, 1);
  prepare_data_end(major_handle, ObITable::MAJOR_SSTABLE);
  merge_context.static_param_.tables_handle_.add_table(major_handle);

  ObTableHandleV2 dump_handle;
  const char *dump_data[1];
  // Uncommitted rows: trans_version=MIN, sql_sequence=negative, flag includes U.
  // F = first multi-version row of rowkey, L = last, LF = both (single-row rowkey).
  dump_data[0] =
      "bigint   bigint   bigint   bigint   flag    multi_version_row_flag   trans_id\n"
      "1        MIN      -1       145      EXIST   ULF                      trans_id_1\n"
      "2        MIN      -1       245      EXIST   FU                       trans_id_2\n"
      "2        -37      0        2        EXIST   L                        trans_id_0\n"
      "3        MIN      -1       345      EXIST   ULF                      trans_id_3\n"
      "4        MIN      -1       445      EXIST   FU                       trans_id_4\n"
      "4        -19      0        4        EXIST   L                        trans_id_0\n"
      "5        MIN      -2       552      EXIST   FU                       trans_id_3\n"
      "5        MIN      -1       551      EXIST   LU                       trans_id_1\n"
      "6        MIN      -2       662      EXIST   FU                       trans_id_4\n"
      "6        MIN      -1       661      EXIST   LU                       trans_id_2\n";

  scn_range.start_scn_.convert_for_tx(last_major_compaction_scn);
  scn_range.end_scn_.convert_for_tx(50);
  table_key_.scn_range_ = scn_range;
  reset_writer(50);
  prepare_one_macro(dump_data, 1);
  prepare_data_end(dump_handle);
  merge_context.static_param_.tables_handle_.add_table(dump_handle);

  // tx1: commit at 30 (<= major_compaction_scn=40), readable
  insert_tx_data(1 /*tx_id*/, 30 /*commit_version*/);
  // tx2: commit at 50 (> major_compaction_scn=40), snapshot(40) < commit(50), skipped
  insert_tx_data(2 /*tx_id*/, 50 /*commit_version*/);
  // tx3: ABORT (commit_version=INT64_MAX triggers ABORT state)
  insert_tx_data(3 /*tx_id*/, INT64_MAX /*commit_version*/);
  // tx4: RUNNING (commit_version=0 triggers RUNNING state)
  insert_tx_data(4 /*tx_id*/, 0 /*commit_version*/);

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = major_compaction_scn;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = last_major_compaction_scn;

  prepare_merge_context(MAJOR_MERGE, false /* is_full_merge */, trans_version_range, merge_context);
  merge_context.static_param_.ttl_major_for_partial_update_upper_snapshot_ = 50;

  ASSERT_EQ(OB_SUCCESS, merger.prepare_merge(merge_context, 0));
  ASSERT_NE(nullptr, merger.merge_helper_);
  ASSERT_EQ(2, merger.merge_helper_->get_merge_iters().count());

  ObPartitionMergeIter *iter = merger.merge_helper_->get_merge_iters().at(0);
  ASSERT_NE(nullptr, iter);
  ASSERT_EQ(1, iter->get_sstable_idx());
  ASSERT_FALSE(iter->is_base_sstable_iter());
  ASSERT_FALSE(iter->is_major_sstable_iter());

  // rowkey=1: tx1 commits at 30 (<=40), readable -> output at version -30, sql_seq=-1 retained
  // rowkey=2: tx2 commits at 50 (>40), snapshot(40)<commit(50) -> output virtual row
  // rowkey=3: tx3 ABORT -> skipped, no output
  // rowkey=4: tx4 RUNNING -> skipped, no output
  // rowkey=5: tx3 ABORT (F, skipped) + tx1 commit at 30 (L, readable) -> output at version -30
  // rowkey=6: tx4 RUNNING (F, skipped) + tx2 commit at 50 (L, skipped) -> output virtual row
  const char *result1 =
      "bigint   bigint   bigint   bigint   flag    multi_version_row_flag   major_merge_flag\n"
      "1        -30      -1       145      EXIST   N                        N\n"
      "2        -37      0        2        EXIST   N                        E\n"
      "5        -30      -1       551      EXIST   N                        N\n"
      "6        -50      -1       661      EXIST   N                        V\n";

  ObMockIterator result_iter;
  ObMockMajorMergeIter major_iter(iter);
  ASSERT_EQ(OB_SUCCESS, result_iter.from_for_datum(result1));
  bool is_equal = result_iter.equals<ObMockMajorMergeIter, ObDatumRow>(
      major_iter, false /* cmp multi version row flag */);
  ASSERT_TRUE(is_equal);

  clear_tx_data();
  major_handle.reset();
  dump_handle.reset();
  merger.reset();
}

// Test case: cross-micro-block major_merge_flag propagation bug.
//
// Data layout:
//   Major sstable (snapshot=20): rowkeys 1,2
//   Dump sstable: 1 macro block with 2 micro blocks
//     micro 1: rowkey=1 versions {45(SCF), 45(C), 35(C with nop data col)}
//     micro 2: rowkey=1 version 25(CL, fills nop), rowkey=2 version 45(LF)
//
// snapshot_version = 40, base_version = 20
//
// Expected scanner output:
//   rowkey=1: version 45 > 40 → skipped, cur_rowkey_exist_new_version_=true
//             version 35 <= 40, first version-fit, read into row_ (nop data col)
//             -- micro block boundary, cache row_ into prev_micro_row_ --
//             version 25 <= 40, fills nop in prev_micro_row_
//             Output: trans=-35, data=125, major_merge_flag=E (exist_new_committed_row)
//   rowkey=2: version 45 > 40, single LF → virtual row
//             Output: trans=-45, data=245, major_merge_flag=V
TEST_F(TestMinorIterInTTLMajor, test_cross_micro_block_major_merge_flag_propagation)
{
  const int64_t schema_rowkey_cnt = 1;
  const int64_t last_major_compaction_scn = 20;
  const int64_t major_compaction_scn = 40;

  ObTabletMergeDagParam param;
  ObTabletMajorMergeCtx merge_context(param, allocator_);
  ObPartitionMajorMerger merger(local_arena_, merge_context.static_param_);

  // 1) Major sstable
  ObTableHandleV2 major_handle;
  const char *major_data[1];
  major_data[0] =
      "bigint   bigint   bigint   bigint   flag    multi_version_row_flag\n"
      "1        -15      0        100      EXIST   N\n"
      "2        -18      0        200      EXIST   N\n";

  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(last_major_compaction_scn);
  prepare_table_schema(major_data, schema_rowkey_cnt, scn_range, last_major_compaction_scn);
  reset_writer(last_major_compaction_scn, MAJOR_MERGE);
  prepare_one_macro(major_data, 1);
  prepare_data_end(major_handle, ObITable::MAJOR_SSTABLE);
  merge_context.static_param_.tables_handle_.add_table(major_handle);

  // 2) Dump sstable: 1 macro block with 2 micro blocks
  // Micro 1: rowkey=1 has versions 45 (>snapshot) and 35 (<=snapshot, nop data col)
  //          The nop data col forces cross-micro fusing when version 25 is in micro 2.
  // Micro 2: rowkey=1 version 25 (<=snapshot, fills nop), rowkey=2 version 45 (virtual)
  //
  // Key: both micro blocks are in the SAME macro block so the scanner handles
  // cross-micro-block fusing via prev_micro_row_.
  ObTableHandleV2 dump_handle;
  const char *dump_data[2];
  dump_data[0] =
      "bigint   bigint   bigint   bigint   flag    multi_version_row_flag\n"
      "1        -45      MIN      145      EXIST   SCF\n"
      "1        -45      0        145      EXIST   C\n"
      "1        -35      0        NOP      EXIST   N\n";

  dump_data[1] =
      "bigint   bigint   bigint   bigint   flag    multi_version_row_flag\n"
      "1        -25      0        125      EXIST   CL\n"
      "2        -45      0        245      EXIST   LF\n";

  scn_range.start_scn_.convert_for_tx(last_major_compaction_scn);
  scn_range.end_scn_.convert_for_tx(50);
  table_key_.scn_range_ = scn_range;
  reset_writer(50);
  prepare_one_macro(dump_data, 2);
  prepare_data_end(dump_handle);
  merge_context.static_param_.tables_handle_.add_table(dump_handle);

  // 3) Merge context
  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = major_compaction_scn;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = last_major_compaction_scn;

  prepare_merge_context(MAJOR_MERGE, false /* is_full_merge */, trans_version_range, merge_context);
  merge_context.static_param_.ttl_major_for_partial_update_upper_snapshot_ = 50;

  ASSERT_EQ(OB_SUCCESS, merger.prepare_merge(merge_context, 0));
  ASSERT_NE(nullptr, merger.merge_helper_);
  ASSERT_EQ(2, merger.merge_helper_->get_merge_iters().count());

  ObPartitionMergeIter *iter = merger.merge_helper_->get_merge_iters().at(0);
  ASSERT_NE(nullptr, iter);
  ASSERT_EQ(1, iter->get_sstable_idx());
  ASSERT_FALSE(iter->is_base_sstable_iter());
  ASSERT_FALSE(iter->is_major_sstable_iter());

  // 4) Verify scanner output
  // rowkey=1: version 45 > snapshot(40), skipped, cur_rowkey_exist_new_version_=true.
  //           version 35 <= snapshot, first version-fit row, read into row_ with nop data col.
  //           Micro block boundary hit. cache_cur_micro_row_forward caches row_ to prev_micro_row_
  //           via copy_row_meta which does NOT copy major_merge_flag_ (BUG).
  //           version 25 <= snapshot, fills nop data col. Output via prev_micro_row_.
  //           Expected flag: E (exist_new_committed_row), because version 45 > snapshot.
  // rowkey=2: version 45 > snapshot, single LF → virtual row.
  //           Expected flag: V (is_virtual_row_for_ttl_major).
  const char *result1 =
      "bigint   bigint   bigint   bigint   flag    multi_version_row_flag   major_merge_flag\n"
      "1        -35      0        125      EXIST   N                        E\n"
      "2        -45      0        245      EXIST   N                        V\n";

  ObMockIterator result_iter;
  ObMockMajorMergeIter major_iter(iter);
  ASSERT_EQ(OB_SUCCESS, result_iter.from_for_datum(result1));
  bool is_equal = result_iter.equals<ObMockMajorMergeIter, ObDatumRow>(
      major_iter, false /* cmp multi version row flag */);
  ASSERT_TRUE(is_equal);

  major_handle.reset();
  dump_handle.reset();
  merger.reset();
}

// Test case: upper_snapshot bound takes effect.
//
// snapshot_version = 40, upper_snapshot = 60.
// dump rows whose trans_version > upper_snapshot must be ignored entirely:
//   - no virtual row is produced
//   - cur_rowkey_exist_new_version_ is not set, so subsequent versions on the
//     same rowkey are not contaminated with the E flag.
//
// Layout:
//   rowkey=1: trans_version=80 (>upper)                           -> ignored, no output
//   rowkey=2: trans_version=50 (in (snapshot, upper])             -> V flag
//   rowkey=3: trans_version=80 (>upper, F) + 35 (<=snapshot, CL)  -> output -35 with N flag
//   rowkey=4: trans_version=50 (in range, F) + 35 (<=snapshot, CL)-> output -35 with E flag
TEST_F(TestMinorIterInTTLMajor, test_dump_sstable_iter_with_upper_snapshot_filter)
{
  const int64_t schema_rowkey_cnt = 1;
  const int64_t last_major_compaction_scn = 20;
  const int64_t major_compaction_scn = 40;
  const int64_t upper_snapshot = 60;

  ObTabletMergeDagParam param;
  ObTabletMajorMergeCtx merge_context(param, allocator_);
  ObPartitionMajorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 major_handle;
  const char *major_data[1];
  major_data[0] =
      "bigint   bigint   bigint   bigint   flag    multi_version_row_flag\n"
      "1        -15      0        100      EXIST   N\n"
      "2        -15      0        200      EXIST   N\n"
      "3        -15      0        300      EXIST   N\n"
      "4        -15      0        400      EXIST   N\n";

  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(last_major_compaction_scn);
  prepare_table_schema(major_data, schema_rowkey_cnt, scn_range, last_major_compaction_scn);
  reset_writer(last_major_compaction_scn, MAJOR_MERGE);
  prepare_one_macro(major_data, 1);
  prepare_data_end(major_handle, ObITable::MAJOR_SSTABLE);
  merge_context.static_param_.tables_handle_.add_table(major_handle);

  ObTableHandleV2 dump_handle;
  const char *dump_data[1];
  dump_data[0] =
      "bigint   bigint   bigint   bigint   flag    multi_version_row_flag\n"
      "1        -80      0        180      EXIST   LF\n"
      "2        -50      0        250      EXIST   LF\n"
      "3        -80      MIN      380      EXIST   SCF\n"
      "3        -80      0        380      EXIST   C\n"
      "3        -35      0        335      EXIST   CL\n"
      "4        -50      MIN      450      EXIST   SCF\n"
      "4        -50      0        450      EXIST   C\n"
      "4        -35      0        435      EXIST   CL\n";

  scn_range.start_scn_.convert_for_tx(last_major_compaction_scn);
  scn_range.end_scn_.convert_for_tx(100);
  table_key_.scn_range_ = scn_range;
  reset_writer(100);
  prepare_one_macro(dump_data, 1);
  prepare_data_end(dump_handle);
  merge_context.static_param_.tables_handle_.add_table(dump_handle);

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = major_compaction_scn;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = last_major_compaction_scn;

  prepare_merge_context(MAJOR_MERGE, false /* is_full_merge */, trans_version_range, merge_context);
  merge_context.static_param_.ttl_major_for_partial_update_upper_snapshot_ = upper_snapshot;

  ASSERT_EQ(OB_SUCCESS, merger.prepare_merge(merge_context, 0));
  ASSERT_NE(nullptr, merger.merge_helper_);
  ASSERT_EQ(2, merger.merge_helper_->get_merge_iters().count());

  ObPartitionMergeIter *iter = merger.merge_helper_->get_merge_iters().at(0);
  ASSERT_NE(nullptr, iter);
  ASSERT_EQ(1, iter->get_sstable_idx());
  ASSERT_FALSE(iter->is_base_sstable_iter());
  ASSERT_FALSE(iter->is_major_sstable_iter());

  const char *result1 =
      "bigint   bigint   bigint   bigint   flag    multi_version_row_flag   major_merge_flag\n"
      "2        -50      0        250      EXIST   N                        V\n"
      "3        -35      0        335      EXIST   N                        N\n"
      "4        -35      0        435      EXIST   N                        E\n";

  ObMockIterator result_iter;
  ObMockMajorMergeIter major_iter(iter);
  ASSERT_EQ(OB_SUCCESS, result_iter.from_for_datum(result1));
  bool is_equal = result_iter.equals<ObMockMajorMergeIter, ObDatumRow>(
      major_iter, false /* cmp multi version row flag */);
  ASSERT_TRUE(is_equal);

  major_handle.reset();
  dump_handle.reset();
  merger.reset();
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_minor_iter_in_ttl_major.log*");
  OB_LOGGER.set_file_name("test_minor_iter_in_ttl_major.log");
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
