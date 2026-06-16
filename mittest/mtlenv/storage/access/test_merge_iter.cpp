/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <algorithm>
#define private public
#define protected public
#define UNITTEST
#include "storage/memtable/ob_memtable_interface.h"
#include "storage/compaction/ob_partition_merger.h"
#include "storage/compaction/ob_partition_merge_iter.h"
#include "storage/ob_micro_block_index_iterator.h"
#include "storage/blocksstable/index_block/ob_sstable_sec_meta_iterator.h"
#include "storage/test_tablet_helper.h"
#include "test_merge_basic.h"
#include "storage/mockcontainer/mock_ob_merge_iterator.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
using namespace blocksstable;
using namespace compaction;
using namespace memtable;
using namespace observer;
using namespace unittest;
using namespace memtable;
using namespace transaction;
using namespace palf;

bool blocksstable::ObSSTableIndexBuilder::satisfies_small_sstable_pre_requisites(
  blocksstable::ObSSTableIndexBuilder::ObSpaceOptimizationMode mode,
  int64_t concurrent_cnt,
  const ObIODevice *device_handle)
{
  return false;
}

namespace storage
{

struct ExpectedBlockInfo
{
  ObDatumRowkey end_key_;
  int64_t row_count_;
  int64_t min_merged_trans_version_;
  int64_t max_merged_trans_version_;
  TO_STRING_KV(K_(end_key), K_(row_count), K_(min_merged_trans_version), K_(max_merged_trans_version));
  ExpectedBlockInfo()
    : end_key_(),
      row_count_(0),
      min_merged_trans_version_(0),
      max_merged_trans_version_(0)
  {}
};

static int build_expected_micro_info(
    const char *micro_data,
    const int64_t schema_rowkey_cnt,
    ObIAllocator &allocator,
    ExpectedBlockInfo &info)
{
  int ret = OB_SUCCESS;
  ObMockIterator iter;
  const ObStoreRow *row = nullptr;
  const int64_t trans_version_idx =
      ObMultiVersionRowkeyHelpper::get_trans_version_col_store_index(schema_rowkey_cnt, true /*is_multi_version*/);
  const int64_t rowkey_cnt =
      schema_rowkey_cnt + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();

  if (OB_FAIL(iter.from(micro_data))) {
    STORAGE_LOG(WARN, "failed to parse micro data", K(ret));
  } else {
    info.row_count_ = iter.count();
    info.min_merged_trans_version_ = INT64_MAX;
    info.max_merged_trans_version_ = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < iter.count(); ++i) {
      if (OB_FAIL(iter.get_row(i, row))) {
        STORAGE_LOG(WARN, "failed to get row from iter", K(ret), K(i));
      } else if (OB_ISNULL(row)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected null row", K(ret), K(i));
      } else {
        const int64_t merged_version = -row->row_val_.get_cell(trans_version_idx).get_int();
        info.min_merged_trans_version_ = std::min(info.min_merged_trans_version_, merged_version);
        info.max_merged_trans_version_ = std::max(info.max_merged_trans_version_, merged_version);
        if (i == iter.count() - 1) {
          ObRowkey tmp_key;
          ObDatumRowkey tmp_datum_key;
          if (FALSE_IT(tmp_key.assign(row->row_val_.cells_, rowkey_cnt))) {
          } else if (OB_FAIL(tmp_datum_key.from_rowkey(tmp_key, allocator))) {
            STORAGE_LOG(WARN, "failed to deep copy rowkey", K(ret));
          } else if (OB_FAIL(tmp_datum_key.deep_copy(info.end_key_/*dst*/, allocator))) {
            STORAGE_LOG(WARN, "failed to deep copy datum key", K(ret));
          }
        }
      }
    }
    if (0 == info.row_count_) {
      info.min_merged_trans_version_ = 0;
      info.max_merged_trans_version_ = 0;
    }
  }
  return ret;
}

static void verify_macro_block_info(
    const blocksstable::ObMacroBlockDesc *macro_desc,
    const ExpectedBlockInfo &expected)
{
  ASSERT_NE(nullptr, macro_desc);
  ASSERT_NE(nullptr, macro_desc->macro_meta_);
  ASSERT_TRUE(macro_desc->range_.end_key_.is_valid());
  LOG_INFO("verify macro block info", K(macro_desc->range_), K(expected));
  ASSERT_TRUE(expected.end_key_ == macro_desc->range_.end_key_);
  ASSERT_EQ(expected.row_count_, macro_desc->row_count_);
  ASSERT_EQ(expected.max_merged_trans_version_, macro_desc->macro_meta_->val_.max_merged_trans_version_);
}

static void verify_micro_block_info(
    const blocksstable::ObMicroBlock &micro_block,
    const ExpectedBlockInfo &expected)
{
  ASSERT_TRUE(micro_block.range_.end_key_.is_valid());
  ASSERT_TRUE(expected.end_key_ == micro_block.range_.end_key_);
  ASSERT_EQ(expected.row_count_, micro_block.header_.row_count_);
  ASSERT_EQ(expected.max_merged_trans_version_, micro_block.header_.max_merged_trans_version_);
  if (micro_block.header_.has_min_merged_trans_version()) {
    ASSERT_EQ(expected.min_merged_trans_version_, micro_block.header_.get_min_merged_trans_version());
  }
}

class TestMergeIter : public TestMergeBasic, public ::testing::WithParamInterface<ObMergeLevel>
{
public:
  TestMergeIter();
  virtual ~TestMergeIter() {}

  void SetUp();
  void TearDown();
  static void SetUpTestCase();
  static void TearDownTestCase();

  void prepare_merge_context(const ObMergeType &merge_type,
                             const bool is_full_merge,
                             const ObVersionRange &trans_version_range,
                             ObTabletExeMergeCtx &merge_context,
                             const ObMergeLevel merge_level = MICRO_BLOCK_MERGE_LEVEL)
  {
    TestMergeBasic::prepare_merge_context(
      merge_type, is_full_merge, trans_version_range, &merge_dag_, merge_context, false/*is_delete_insert_merge*/);
    merge_context.static_param_.merge_level_ = merge_level;
  }

  void verify_iter_basic_info(
      ObPartitionMergeIter *iter,
      int64_t expected_row_count);

  void test_iter_next_range_micro(
      ObPartitionMergeIter *iter,
      const ObIArray<ExpectedBlockInfo> &expected_macros,
      const ObIArray<ExpectedBlockInfo> &expected_micros,
      const ObMergeLevel merge_level = MICRO_BLOCK_MERGE_LEVEL);

  // Helper function to iterate and verify micro blocks in MICRO_BLOCK_MERGE_LEVEL
  void iterate_and_verify_micro_blocks(
      ObPartitionMergeIter *iter,
      const ObIArray<ExpectedBlockInfo> &expected_reused_micros,
      int &ret);
  int prepare_one_macro_with_expected(
      const char **micro_data,
      const int64_t micro_cnt,
      const int64_t schema_rowkey_cnt,
      ObIAllocator &allocator,
      ObIArray<ExpectedBlockInfo> &expected_macros,
      ObIArray<ExpectedBlockInfo> &expected_micros,
      const bool contain_uncommitted = false);
public:
  compaction::ObLocalArena local_arena_;
  ObArenaAllocator allocator_;
  ObTabletMergeExecuteDag merge_dag_;
};

void TestMergeIter::SetUpTestCase()
{
  ObMultiVersionSSTableTest::SetUpTestCase();
  // mock sequence no
  ObClockGenerator::init();
  TestMergeBasic::create_tablet();
}

void TestMergeIter::TearDownTestCase()
{
  ObMultiVersionSSTableTest::TearDownTestCase();
  // reset sequence no
  ObClockGenerator::destroy();
}

TestMergeIter::TestMergeIter()
  : TestMergeBasic("test_merge_iter"),
    local_arena_("MergeIter", OB_MALLOC_NORMAL_BLOCK_SIZE)
{}

void TestMergeIter::SetUp()
{
  ObMultiVersionSSTableTest::SetUp();
}

void TestMergeIter::TearDown()
{
  ObMultiVersionSSTableTest::TearDown();
  TRANS_LOG(INFO, "teardown success");
}

void TestMergeIter::verify_iter_basic_info(
    ObPartitionMergeIter *iter,
    int64_t expected_row_count)
{
  STORAGE_LOG(INFO, "verify iter basic info",
    K(expected_row_count),
    K(iter->iter_row_count_),
    K(iter->iter_row_id_), KPC(iter));
  ASSERT_NE(nullptr, iter);
  ASSERT_TRUE(iter->is_inited_);
  ASSERT_FALSE(iter->iter_end_);
}

void TestMergeIter::test_iter_next_range_micro(
    ObPartitionMergeIter *iter,
    const ObIArray<ExpectedBlockInfo> &expected_macros,
    const ObIArray<ExpectedBlockInfo> &expected_micros,
    const ObMergeLevel merge_level)
{
  int ret = OB_SUCCESS;
  int64_t actual_row_count = 0;
  int64_t actual_macro_count = 0;
  int64_t actual_micro_count = 0;
  while (OB_SUCC(ret)) {
    if (iter->is_iter_end()) {
      break;
    } else {
      if (iter && !iter->is_macro_block_opened()) {
        actual_macro_count++;
        const blocksstable::ObMacroBlockDesc *macro_desc = nullptr;
        ASSERT_EQ(OB_SUCCESS, iter->get_curr_macro_block(macro_desc));
        verify_macro_block_info(macro_desc, expected_macros.at(actual_macro_count - 1));
        if (merge_level == MICRO_BLOCK_MERGE_LEVEL) {
          ASSERT_EQ(OB_SUCCESS, static_cast<ObPartitionMinorMicroMergeIter*>(iter)->init_micro_iter(false/*open_block*/));
        } else {
          ASSERT_EQ(OB_SUCCESS, iter->open_curr_range(false/*for_rewrite*/, false/*for_compare*/));
        }
        STORAGE_LOG(INFO, "open curr macro block", K(actual_macro_count), KPC(iter));
      } else if (iter && iter->is_macro_block_opened() && !iter->is_micro_block_opened()) {
        actual_micro_count++;
        blocksstable::ObDatumRange micro_range;
        ASSERT_EQ(OB_SUCCESS, iter->get_curr_range(micro_range));
        ASSERT_EQ(OB_SUCCESS, iter->open_curr_range(false/*for_rewrite*/, false/*for_compare*/));
        const blocksstable::ObMicroBlock *micro_block = nullptr;
        ASSERT_EQ(OB_SUCCESS, iter->get_curr_micro_block(micro_block));
        ASSERT_NE(nullptr, micro_block);
        STORAGE_LOG(INFO, "verify micro block info", K(actual_micro_count), K(micro_range.end_key_), K(expected_micros.at(actual_micro_count - 1)), KPC(micro_block), KPC(iter));
        ASSERT_TRUE(expected_micros.at(actual_micro_count - 1).end_key_ == micro_block->range_.end_key_);
        verify_micro_block_info(*micro_block, expected_micros.at(actual_micro_count - 1));
      } else {
        actual_row_count++;
        const blocksstable::ObDatumRow *row = iter->get_curr_row();
        STORAGE_LOG(INFO, "get curr row", K(ret), KPC(iter));
        ASSERT_NE(nullptr, row);
        if (OB_FAIL(iter->next())) {
          if (OB_ITER_END != ret) {
            STORAGE_LOG(WARN, "Failed to get next from micro iter", K(ret));
          }
        }
      }
    }
  }
  STORAGE_LOG(INFO, "test micro iter finished",
    K(actual_row_count),
    K(actual_macro_count), K(expected_macros.count()),
    K(actual_micro_count), K(expected_micros.count()),
    K(merge_level));
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(expected_macros.count(), actual_macro_count);
  if (merge_level == MICRO_BLOCK_MERGE_LEVEL) {
    ASSERT_EQ(expected_micros.count(), actual_micro_count);
  } else {
    ASSERT_EQ(0, actual_micro_count);
  }
}

void TestMergeIter::iterate_and_verify_micro_blocks(
    ObPartitionMergeIter *iter,
    const ObIArray<ExpectedBlockInfo> &expected_reused_micros,
    int &ret)
{
  int64_t micro_count = 0;

  while (OB_SUCC(ret)) {
    if (iter->is_iter_end()) {
      break;
    }

    if (!iter->is_macro_block_opened()) {
      const blocksstable::ObMacroBlockDesc *macro_desc = nullptr;
      ASSERT_EQ(OB_SUCCESS, iter->get_curr_macro_block(macro_desc));
      ASSERT_EQ(OB_SUCCESS, iter->open_curr_range(false/*for_rewrite*/, false/*for_compare*/));
    } else if (!iter->is_micro_block_opened()) {
      const blocksstable::ObMicroBlock *micro_block = nullptr;
      ASSERT_EQ(OB_SUCCESS, iter->get_curr_micro_block(micro_block));
      STORAGE_LOG(INFO, "get curr micro block", K(micro_count), KPC(micro_block), KPC(iter));
      ASSERT_NE(nullptr, micro_block);
      ASSERT_TRUE(micro_count < expected_reused_micros.count()) << "micro_count: " << micro_count << ", expected_reused_micros.count(): " << expected_reused_micros.count();
      // Verify this is one of the expected reused micro blocks
      STORAGE_LOG(INFO, "verify micro block", K(micro_count),
                  K(micro_block->range_.end_key_),
                  K(expected_reused_micros.at(micro_count)));
      verify_micro_block_info(*micro_block, expected_reused_micros.at(micro_count));
      micro_count++;
    } else {
      // Skip row iteration in MICRO_BLOCK_MERGE_LEVEL for this test
    }

    ret = iter->next();
  }

  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(expected_reused_micros.count(), micro_count);
  STORAGE_LOG(INFO, "finished iterating micro blocks", K(micro_count));
}

int TestMergeIter::prepare_one_macro_with_expected(
    const char **micro_data,
    const int64_t micro_cnt,
    const int64_t schema_rowkey_cnt,
    ObIAllocator &allocator,
    ObIArray<ExpectedBlockInfo> &expected_macros,
    ObIArray<ExpectedBlockInfo> &expected_micros,
    const bool contain_uncommitted)
{
  int ret = OB_SUCCESS;
  ExpectedBlockInfo macro_info;
  macro_info.row_count_ = 0;
  macro_info.min_merged_trans_version_ = INT64_MAX;
  macro_info.max_merged_trans_version_ = 0;

  for (int64_t i = 0; OB_SUCC(ret) && i < micro_cnt; ++i) {
    ExpectedBlockInfo micro_info;
    if (OB_FAIL(build_expected_micro_info(micro_data[i], schema_rowkey_cnt, allocator, micro_info))) {
      STORAGE_LOG(WARN, "failed to build expected micro info", K(ret), K(i));
    } else if (OB_FAIL(expected_micros.push_back(micro_info))) {
      STORAGE_LOG(WARN, "failed to push expected micro info", K(ret), K(i));
    } else {
      STORAGE_LOG(INFO, "push expected micro info", K(ret), K(micro_info));
      macro_info.row_count_ += micro_info.row_count_;
      macro_info.min_merged_trans_version_ = std::min(macro_info.min_merged_trans_version_, micro_info.min_merged_trans_version_);
      macro_info.max_merged_trans_version_ = std::max(macro_info.max_merged_trans_version_, micro_info.max_merged_trans_version_);
      if (i == micro_cnt - 1) {
        if (OB_FAIL(micro_info.end_key_.deep_copy(macro_info.end_key_, allocator))) {
          STORAGE_LOG(WARN, "failed to deep copy macro end key", K(ret), K(micro_info.end_key_));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (0 == macro_info.row_count_) {
      macro_info.min_merged_trans_version_ = 0;
      macro_info.max_merged_trans_version_ = 0;
    }
    if (OB_FAIL(expected_macros.push_back(macro_info))) {
      STORAGE_LOG(WARN, "failed to push expected macro info", K(ret));
    } else {
      STORAGE_LOG(INFO, "push expected macro info", K(ret), K(macro_info));
    }
  }
  if (OB_SUCC(ret)) {
    prepare_one_macro(micro_data, micro_cnt, contain_uncommitted);
  }
  return ret;
}

TEST_P(TestMergeIter, test_row_iter_basic)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletExeMergeCtx merge_context(param, allocator_);
  ObMergeLevel merge_level = GetParam();

  ObTableHandleV2 handle1;
  const char *micro_data[2];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint dml           flag    multi_version_row_flag\n"
      "0        var0  -8       0        NOP      1    T_DML_UPDATE  EXIST   LF\n"
      "1        var1  -8       0        2        2    T_DML_INSERT  EXIST   CLF\n"
      "2        var2  -8       0        3        3    T_DML_INSERT  EXIST   CLF\n";

  micro_data[1] =
      "bigint   var   bigint   bigint   bigint bigint dml           flag    multi_version_row_flag\n"
      "3        var3  -8       0        4       4     T_DML_INSERT  EXIST   CLF\n"
      "4        var4  -8       0        5       5     T_DML_INSERT  EXIST   CLF\n";

  int schema_rowkey_cnt = 2;
  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);

  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  ObSEArray<ExpectedBlockInfo, 4> expected_macros;
  ObSEArray<ExpectedBlockInfo, 8> expected_micros;
  reset_writer(snapshot_version);
  ASSERT_EQ(OB_SUCCESS, prepare_one_macro_with_expected(micro_data, 2, schema_rowkey_cnt,
    allocator_, expected_macros, expected_micros));
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1", K(merge_level));

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 7;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, true/*is_full_merge*/, trans_version_range, merge_context, merge_level);
  ObPartitionMinorMerger minor_merger(local_arena_, merge_context.static_param_);
  ASSERT_EQ(OB_SUCCESS, minor_merger.prepare_merge(merge_context, 0));
  STORAGE_LOG(INFO, "finish prepare minor merge", K(merge_context));

  ObPartitionMergeIter *iter = minor_merger.merge_helper_->get_merge_iters()[0];
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, iter);

  verify_iter_basic_info(iter, 5);

  test_iter_next_range_micro(iter, expected_macros, expected_micros, merge_level);

  if (nullptr != iter) {
    iter->~ObPartitionMergeIter();
    iter = nullptr;
  }
  handle1.reset();
}

TEST_P(TestMergeIter, test_macro_iter_basic)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletExeMergeCtx merge_context(param, allocator_);
  ObMergeLevel merge_level = GetParam();

  ObTableHandleV2 handle1;
  const char *micro_data[3];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint dml           flag    multi_version_row_flag\n"
      "0        var0  -8       0        NOP      1    T_DML_UPDATE  EXIST   LF\n"
      "1        var1  -8       0        2        2    T_DML_INSERT  EXIST   CLF\n";

  micro_data[1] =
      "bigint   var   bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "2        var2  -8       0        3       3     T_DML_INSERT EXIST   CLF\n"
      "3        var3  -8       0        4       4     T_DML_INSERT EXIST   CLF\n";

  micro_data[2] =
      "bigint   var   bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "4        var4  -8       0        5       5     T_DML_INSERT EXIST   CLF\n"
      "5        var5  -8       0        6       6     T_DML_INSERT EXIST   CLF\n";

  int schema_rowkey_cnt = 2;
  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);

  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  ObSEArray<ExpectedBlockInfo, 4> expected_macros;
  ObSEArray<ExpectedBlockInfo, 8> expected_micros;
  reset_writer(snapshot_version);
  ASSERT_EQ(OB_SUCCESS, prepare_one_macro_with_expected(micro_data, 1, schema_rowkey_cnt,
                                                        allocator_, expected_macros, expected_micros));
  ASSERT_EQ(OB_SUCCESS, prepare_one_macro_with_expected(&micro_data[1], 1, schema_rowkey_cnt,
                                                        allocator_, expected_macros, expected_micros));
  ASSERT_EQ(OB_SUCCESS, prepare_one_macro_with_expected(&micro_data[2], 1, schema_rowkey_cnt,
                                                        allocator_, expected_macros, expected_micros));
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 7;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context, merge_level);
  ObPartitionMinorMerger minor_merger(local_arena_, merge_context.static_param_);
  ASSERT_EQ(OB_SUCCESS, minor_merger.prepare_merge(merge_context, 0));

  ObPartitionMergeIter *iter = minor_merger.merge_helper_->get_merge_iters()[0];
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, iter);

  verify_iter_basic_info(iter, 6);

  test_iter_next_range_micro(iter, expected_macros, expected_micros, merge_level);

  if (nullptr != iter) {
    iter->~ObPartitionMergeIter();
    iter = nullptr;
  }
  handle1.reset();
}

TEST_P(TestMergeIter, test_micro_iter_basic)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletExeMergeCtx merge_context(param, allocator_);
  ObMergeLevel merge_level = GetParam();

  ObTableHandleV2 handle1;
  const char *micro_data[2];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint dml           flag    multi_version_row_flag\n"
      "0        var0  -8       0        NOP      1    T_DML_UPDATE  EXIST   LF\n"
      "1        var1  -8       0        2        2    T_DML_INSERT  EXIST   CLF\n"
      "2        var2  -8       0        3        3    T_DML_INSERT  EXIST   CLF\n";

  micro_data[1] =
      "bigint   var   bigint   bigint   bigint bigint dml           flag    multi_version_row_flag\n"
      "3        var3  -8       0        4       4     T_DML_INSERT  EXIST   CLF\n"
      "4        var4  -8       0        5       5     T_DML_INSERT  EXIST   CLF\n";

  int schema_rowkey_cnt = 2;
  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);

  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  ObSEArray<ExpectedBlockInfo, 4> expected_macros;
  ObSEArray<ExpectedBlockInfo, 8> expected_micros;
  reset_writer(snapshot_version);
  ASSERT_EQ(OB_SUCCESS, prepare_one_macro_with_expected(micro_data, 2, schema_rowkey_cnt,
                                                        allocator_, expected_macros, expected_micros));
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1 with micro blocks", K(merge_level));

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 7;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context, merge_level);
  ObPartitionMinorMerger minor_merger(local_arena_, merge_context.static_param_);
  ASSERT_EQ(OB_SUCCESS, minor_merger.prepare_merge(merge_context, 0));
  STORAGE_LOG(INFO, "finish prepare minor merge", K(merge_context));

  ObPartitionMergeIter *iter = minor_merger.merge_helper_->get_merge_iters()[0];
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, iter);

  // Verify basic iterator info.
  verify_iter_basic_info(iter, 5);
  test_iter_next_range_micro(iter, expected_macros, expected_micros, merge_level);

  // Cleanup.
  if (nullptr != iter) {
    iter->~ObPartitionMergeIter();
    iter = nullptr;
  }
  handle1.reset();
}

// Test case 4: verify iterator info (end_key, trans_version, etc.).
TEST_P(TestMergeIter, test_iter_info_validation)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletExeMergeCtx merge_context(param, allocator_);
  ObMergeLevel merge_level = GetParam();

  // Prepare test data with different transaction versions.
  ObTableHandleV2 handle1;
  const char *micro_data[2];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint dml           flag    multi_version_row_flag\n"
      "0        var0  -10      0        NOP      1    T_DML_UPDATE  EXIST   LF\n"
      "1        var1  -9       0        2        2    T_DML_INSERT  EXIST   CLF\n"
      "2        var2  -8       0        3        3    T_DML_INSERT  EXIST   CLF\n";

  micro_data[1] =
      "bigint   var   bigint   bigint   bigint bigint dml           flag    multi_version_row_flag\n"
      "3        var3  -7       0        4       4     T_DML_INSERT  EXIST   CLF\n"
      "4        var4  -6       0        5       5     T_DML_INSERT  EXIST   CLF\n";

  int schema_rowkey_cnt = 2;
  int64_t snapshot_version = 20;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(20);

  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  ObSEArray<ExpectedBlockInfo, 4> expected_macros;
  ObSEArray<ExpectedBlockInfo, 8> expected_micros;
  reset_writer(snapshot_version);
  ASSERT_EQ(OB_SUCCESS, prepare_one_macro_with_expected(micro_data, 1, schema_rowkey_cnt,
                                                        allocator_, expected_macros, expected_micros));
  ASSERT_EQ(OB_SUCCESS, prepare_one_macro_with_expected(&micro_data[1], 1, schema_rowkey_cnt,
                                                        allocator_, expected_macros, expected_micros));
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable with different trans versions", K(merge_level));

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 5;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context, merge_level);
  ObPartitionMinorMerger minor_merger(local_arena_, merge_context.static_param_);
  ASSERT_EQ(OB_SUCCESS, minor_merger.prepare_merge(merge_context, 0));

  ObPartitionMergeIter *iter = minor_merger.merge_helper_->get_merge_iters()[0];
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, iter);

  test_iter_next_range_micro(iter, expected_macros, expected_micros, merge_level);
  if (nullptr != iter) {
    iter->~ObPartitionMergeIter();
    iter = nullptr;
  }
  handle1.reset();
}

// Test case 5: test recycle version with micro/macro block recycling
TEST_P(TestMergeIter, test_recycle_version)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletExeMergeCtx merge_context(param, allocator_);
  ObMergeLevel merge_level = GetParam();

  ObTableHandleV2 handle1;
  const char *micro_data[3];
  // First macro block: trans_version = 5
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint dml           flag    multi_version_row_flag\n"
      "0        var0  -5       0        NOP      1    T_DML_UPDATE  EXIST   LF\n"
      "1        var1  -5       0        2        2    T_DML_INSERT  EXIST   CLF\n";

  // Second macro block: trans_version = 8
  micro_data[1] =
      "bigint   var   bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "2        var2  -8       0        3       3     T_DML_INSERT EXIST   CLF\n"
      "3        var3  -8       0        4       4     T_DML_INSERT EXIST   CLF\n";

  // Third macro block: trans_version = 12
  micro_data[2] =
      "bigint   var   bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "4        var4  -12      0        5       5     T_DML_INSERT EXIST   CLF\n"
      "5        var5  -12      0        6       6     T_DML_INSERT EXIST   CLF\n";

  int schema_rowkey_cnt = 2;
  int64_t snapshot_version = 20;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(20);

  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  ObSEArray<ExpectedBlockInfo, 4> expected_macros;
  ObSEArray<ExpectedBlockInfo, 8> expected_micros;
  reset_writer(snapshot_version);
  ASSERT_EQ(OB_SUCCESS, prepare_one_macro_with_expected(micro_data, 1, schema_rowkey_cnt,
                                                        allocator_, expected_macros, expected_micros));
  ASSERT_EQ(OB_SUCCESS, prepare_one_macro_with_expected(&micro_data[1], 1, schema_rowkey_cnt,
                                                        allocator_, expected_macros, expected_micros));
  ASSERT_EQ(OB_SUCCESS, prepare_one_macro_with_expected(&micro_data[2], 1, schema_rowkey_cnt,
                                                        allocator_, expected_macros, expected_micros));
  prepare_data_end(handle1);
  ASSERT_EQ(3, expected_macros.count());
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable for recycle version test", K(merge_level));

  // Set recycle_version to 7, which means rows/micro blocks/macro blocks
  // with trans_version <= 7 can be recycled
  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 4;
  trans_version_range.base_version_ = 7;  // This is the recycle_version

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context, merge_level);
  ObPartitionMinorMerger minor_merger(local_arena_, merge_context.static_param_);
  ASSERT_EQ(OB_SUCCESS, minor_merger.prepare_merge(merge_context, 0));
  STORAGE_LOG(INFO, "finish prepare minor merge with recycle_version",
    K(trans_version_range.base_version_), K(merge_context));

  ObPartitionMinorMacroMergeIter *iter = static_cast<ObPartitionMinorMacroMergeIter *>(minor_merger.merge_helper_->get_merge_iters()[0]);
  const blocksstable::ObMacroBlockDesc *macro_desc = nullptr;
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, iter);
  ASSERT_TRUE(iter->rowkey_state_.is_last_row_output());
  ASSERT_TRUE(iter->curr_block_op_.is_none());
  ASSERT_EQ(OB_SUCCESS, iter->get_curr_macro_block(macro_desc));
  verify_macro_block_info(macro_desc, expected_macros.at(1)); // second macro block reused
  ASSERT_EQ(OB_SUCCESS, iter->next());
  ASSERT_TRUE(iter->curr_block_op_.is_none());
  ASSERT_EQ(OB_SUCCESS, iter->get_curr_macro_block(macro_desc));
  verify_macro_block_info(macro_desc, expected_macros.at(2)); // third macro block reused
  ASSERT_EQ(OB_ITER_END, iter->next());

  if (nullptr != iter) {
    iter->~ObPartitionMinorMacroMergeIter();
    iter = nullptr;
  }
  handle1.reset();
}

// Test case 6: test recycle version with multi micro blocks
TEST_P(TestMergeIter, test_recycle_version_multi_micros)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletExeMergeCtx merge_context(param, allocator_);
  ObMergeLevel merge_level = GetParam();

  ObTableHandleV2 handle1;
  const char *micro_data[10];

  // Micro block 1 (odd, should be filtered, trans_version <= 6)
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint dml           flag    multi_version_row_flag\n"
      "0        var0  -3       0        NOP      1    T_DML_UPDATE  EXIST   LF\n"
      "1        var1  -4       0        2        2    T_DML_INSERT  EXIST   CLF\n";

  // Micro block 2 (even, should be reused, trans_version > 6)
  micro_data[1] =
      "bigint   var   bigint   bigint   bigint bigint dml           flag    multi_version_row_flag\n"
      "2        var2  -10      0        3       3     T_DML_INSERT  EXIST   CLF\n"
      "3        var3  -10      0        4       4     T_DML_INSERT  EXIST   CLF\n";

  // Micro block 3 (odd, should be filtered, trans_version <= 6) LIXIA
  micro_data[2] =
      "bigint   var   bigint   bigint   bigint bigint dml           flag    multi_version_row_flag\n"
      "4        var4  -5       0        5       5     T_DML_INSERT  EXIST   CLF\n"
      "5        var5  -6       0        6       6     T_DML_INSERT  EXIST   CLF\n";

  // Micro block 4 (even, should be reused, trans_version > 6)
  micro_data[3] =
      "bigint   var   bigint   bigint   bigint bigint dml           flag    multi_version_row_flag\n"
      "6        var6  -11      0        7       7     T_DML_INSERT  EXIST   CLF\n"
      "7        var7  -11      0        8       8     T_DML_INSERT  EXIST   CLF\n";

  // Micro block 5 (odd, should be filtered, trans_version <= 6)
  micro_data[4] =
      "bigint   var   bigint   bigint   bigint bigint dml           flag    multi_version_row_flag\n"
      "8        var8  -4       0        9       9     T_DML_INSERT  EXIST   CLF\n"
      "9        var9  -5       0        10      10    T_DML_INSERT  EXIST   CLF\n";

  // Micro block 6 (even, should be reused, trans_version > 6)
  micro_data[5] =
      "bigint   var   bigint   bigint   bigint bigint dml           flag    multi_version_row_flag\n"
      "10       var10 -12      0        11      11    T_DML_INSERT  EXIST   CLF\n"
      "11       var11 -12      0        12      12    T_DML_INSERT  EXIST   CLF\n";

  // Micro block 7 (odd, should be filtered, trans_version <= 6)
  micro_data[6] =
      "bigint   var   bigint   bigint   bigint bigint dml           flag    multi_version_row_flag\n"
      "12       var12 -3       0        13      13    T_DML_INSERT  EXIST   CLF\n"
      "13       var13 -4       0        14      14    T_DML_INSERT  EXIST   CLF\n";

  // Micro block 8 (even, should be reused, trans_version > 6)
  micro_data[7] =
      "bigint   var   bigint   bigint   bigint bigint dml           flag    multi_version_row_flag\n"
      "14       var14 -13      0        15      15    T_DML_INSERT  EXIST   CLF\n"
      "15       var15 -13      0        16      16    T_DML_INSERT  EXIST   CLF\n";

  // Micro block 9 (odd, should be filtered, trans_version <= 6)
  micro_data[8] =
      "bigint   var   bigint   bigint   bigint bigint dml           flag    multi_version_row_flag\n"
      "16       var16 -5       0        17      17    T_DML_INSERT  EXIST   CLF\n"
      "17       var17 -6       0        18      18    T_DML_INSERT  EXIST   CLF\n";

  // Micro block 10 (even, should be reused, trans_version > 6)
  micro_data[9] =
      "bigint   var   bigint   bigint   bigint bigint dml           flag    multi_version_row_flag\n"
      "18       var18 -14      0        19      19    T_DML_INSERT  EXIST   CLF\n"
      "19       var19 -14      0        20      20    T_DML_INSERT  EXIST   CLF\n";

  int schema_rowkey_cnt = 2;
  int64_t snapshot_version = 20;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(20);

  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  ObSEArray<ExpectedBlockInfo, 4> expected_macros;
  ObSEArray<ExpectedBlockInfo, 16> expected_micros;
  ObSEArray<ExpectedBlockInfo, 8> expected_reused_micros; // Only even micro blocks
  reset_writer(snapshot_version);

  // Prepare one macro block with 10 micro blocks
  ASSERT_EQ(OB_SUCCESS, prepare_one_macro_with_expected(micro_data, 10, schema_rowkey_cnt,
                                                        allocator_, expected_macros, expected_micros));
  ASSERT_EQ(1, expected_macros.count());
  ASSERT_EQ(10, expected_micros.count());
  // Build expected reused micro blocks (even ones: 2, 4, 6, 8, 10)
  for (int i = 1; i < 10; i += 2) { // indices 1, 3, 5, 7, 9 (which are micro blocks 2, 4, 6, 8, 10)
    ASSERT_EQ(OB_SUCCESS, expected_reused_micros.push_back(expected_micros.at(i)));
  }

  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable for recycle version full merge test", K(merge_level));

  // Set recycle_version to 6, rows with trans_version <= 6 can be recycled
  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 2;
  trans_version_range.base_version_ = 6;  // This is the recycle_version

  prepare_merge_context(MINOR_MERGE, false/*is_full_merge*/, trans_version_range, merge_context, merge_level);
  ObPartitionMinorMerger minor_merger(local_arena_, merge_context.static_param_);
  ASSERT_EQ(OB_SUCCESS, minor_merger.prepare_merge(merge_context, 0));
  STORAGE_LOG(INFO, "finish prepare minor merge with recycle_version",
    K(trans_version_range.base_version_), K(merge_context));

  ObPartitionMergeIter *iter = minor_merger.merge_helper_->get_merge_iters()[0];
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, iter);

  if (merge_level == MACRO_BLOCK_MERGE_LEVEL) {
    // For MACRO_BLOCK_MERGE_LEVEL, the first macro block will be opened,
    // then iterate by rows and count the rows
    int64_t row_count = 0;
    ASSERT_TRUE(iter->is_macro_block_opened());

    // Iterate through all rows and count them
    // Expected: rows from even micro blocks (2,4,6,8,10) = 5 * 2 = 10 rows
    while (OB_SUCC(ret)) {
      if (iter->is_iter_end()) {
        break;
      }
      const blocksstable::ObDatumRow *row = iter->get_curr_row();
      if (row != nullptr) {
        row_count++;
        STORAGE_LOG(INFO, "iterate row", K(row_count), KPC(row));
      }
      ret = iter->next();
    }
    ASSERT_EQ(OB_ITER_END, ret);
    ASSERT_EQ(10, row_count); // 5 even micro blocks * 2 rows each = 10 rows
    STORAGE_LOG(INFO, "MACRO_BLOCK_MERGE_LEVEL: finished iterating rows", K(row_count));
  } else {
    // For MICRO_BLOCK_MERGE_LEVEL, iterate by micro blocks
    // Verify that only even micro blocks are iterated (reused)
    iterate_and_verify_micro_blocks(iter, expected_reused_micros, ret);
  }

  if (nullptr != iter) {
    iter->~ObPartitionMergeIter();
    iter = nullptr;
  }
  handle1.reset();
}

// Test case: Multi-version rowkey spans across macro blocks and micro blocks, and all versions can be recycled
TEST_P(TestMergeIter, test_recycle_multi_version_cross_macro_micro)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletExeMergeCtx merge_context(param, allocator_);
  ObMergeLevel merge_level = GetParam();

  ObTableHandleV2 handle1;
  const char *micro_data[4];

  // First macro block with 2 micro blocks
  // Micro block 1: rowkey=0 (trans_version=10, will not be recycled)
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint dml           flag    multi_version_row_flag\n"
      "0        var0  -10      MIN      2        1    T_DML_UPDATE  EXIST   SCF\n"
      "0        var0  -10      0        NOP      1    T_DML_UPDATE  EXIST   N\n"
      "0        var0  -9       0        2        2    T_DML_INSERT  EXIST   L\n";

  // Micro block 2: rowkey=1 with First flag but no Last flag (trans_version=5, will be recycled)
  // This is the first part of rowkey=1's multi-version rows that spans across macro blocks
  micro_data[1] =
      "bigint   var   bigint   bigint   bigint bigint dml           flag    multi_version_row_flag\n"
      "1        var1  -5       MIN      4       3     T_DML_INSERT  EXIST   SCF\n"
      "1        var1  -5       0        NOP     3     T_DML_UPDATE  EXIST   N\n"
      "1        var1  -4       0        4       4     T_DML_INSERT  EXIST   N\n";

  // Second macro block with 2 micro blocks
  // Micro block 3: rowkey=1 with Last flag (trans_version=3, will be recycled)
  // This is the last part of rowkey=1's multi-version rows
  micro_data[2] =
      "bigint   var   bigint   bigint   bigint bigint dml           flag    multi_version_row_flag\n"
      "1        var1  -3       0        5       5     T_DML_INSERT  EXIST   L\n"
      "2        var2  -11      0        6       6     T_DML_INSERT  EXIST   CLF\n";

  // Micro block 4: rowkey=3 (trans_version=12, will not be recycled)
  micro_data[3] =
      "bigint   var   bigint   bigint   bigint bigint dml           flag    multi_version_row_flag\n"
      "3        var3  -12      0        7       7     T_DML_INSERT  EXIST   CLF\n"
      "4        var4  -12      0        8       8     T_DML_INSERT  EXIST   CLF\n";

  int schema_rowkey_cnt = 2;
  int64_t snapshot_version = 20;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(20);

  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  ObSEArray<ExpectedBlockInfo, 4> expected_macros;
  ObSEArray<ExpectedBlockInfo, 8> expected_micros;
  reset_writer(snapshot_version);

  // Prepare first macro block with 2 micro blocks
  ASSERT_EQ(OB_SUCCESS, prepare_one_macro_with_expected(micro_data, 2, schema_rowkey_cnt,
                                                        allocator_, expected_macros, expected_micros));
  // Prepare second macro block with 2 micro blocks
  ASSERT_EQ(OB_SUCCESS, prepare_one_macro_with_expected(&micro_data[2], 2, schema_rowkey_cnt,
                                                        allocator_, expected_macros, expected_micros));
  ASSERT_EQ(2, expected_macros.count());
  ASSERT_EQ(4, expected_micros.count());

  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable for multi-version cross macro/micro test", K(merge_level));

  // Set recycle_version to 5, so rowkey=1's all versions (trans_version=3,4,5) will be recycled
  // rowkey=0 (trans_version=9,10) and rowkey=2,3,4 (trans_version=11,12) will not be recycled
  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 2;
  trans_version_range.base_version_ = 5;  // This is the recycle_version

  prepare_merge_context(MINOR_MERGE, false/*is_full_merge*/, trans_version_range, merge_context, merge_level);
  ObPartitionMinorMerger minor_merger(local_arena_, merge_context.static_param_);
  ASSERT_EQ(OB_SUCCESS, minor_merger.prepare_merge(merge_context, 0));
  STORAGE_LOG(INFO, "finish prepare minor merge for multi-version cross macro/micro test",
    K(trans_version_range.base_version_), K(merge_context));

  ObPartitionMergeIter *iter = minor_merger.merge_helper_->get_merge_iters()[0];
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, iter);

  if (merge_level == MACRO_BLOCK_MERGE_LEVEL) {
    // For MACRO_BLOCK_MERGE_LEVEL, iterate by rows
    // Expected: rows from rowkey=0 (2 rows), rowkey=2 (1 row), rowkey=3,4 (2 rows) = 5 rows
    // rowkey=1's all versions should be recycled (3 rows filtered out)
    int64_t row_count = 0;

    while (OB_SUCC(ret)) {
      if (iter->is_iter_end()) {
        break;
      }

      if (!iter->is_macro_block_opened()) {
        const blocksstable::ObMacroBlockDesc *macro_desc = nullptr;
        ASSERT_EQ(OB_SUCCESS, iter->get_curr_macro_block(macro_desc));
        ASSERT_EQ(OB_SUCCESS, iter->open_curr_range(false/*for_rewrite*/, false/*for_compare*/));
      } else {
        const blocksstable::ObDatumRow *row = iter->get_curr_row();
        if (row != nullptr) {
          row_count++;
          STORAGE_LOG(INFO, "iterate row", K(row_count), KPC(row));
          // Verify that we don't get rowkey=1 (it should be recycled)
          int64_t rowkey_val = row->storage_datums_[0].get_int();
          ASSERT_NE(1, rowkey_val) << "rowkey=1 should be recycled";
        }
        ret = iter->next();
      }
    }
    ASSERT_EQ(OB_ITER_END, ret);
    ASSERT_EQ(6, row_count); // 3(rowkey=0) + 1(rowkey=2) + 2(rowkey=3,4) = 6 rows
    STORAGE_LOG(INFO, "MACRO_BLOCK_MERGE_LEVEL: finished, rowkey=1 recycled across macro blocks", K(row_count));
  } else {
    // For MICRO_BLOCK_MERGE_LEVEL, iterate by micro blocks
    // Expected: micro blocks containing rowkey=0, rowkey=2, and rowkey=3,4
    // Micro blocks containing only rowkey=1 should be filtered
    ObSEArray<ExpectedBlockInfo, 4> expected_reused_micros;

    // Micro block 0: rowkey=0 (will be reused)
    ASSERT_EQ(OB_SUCCESS, expected_reused_micros.push_back(expected_micros.at(0)));
    // Micro block 1: rowkey=1 (will be filtered, all versions <= 5)
    // Micro block 2: rowkey=1 and rowkey=2 mixed (will be opened and processed row by row)
    // Actually, let me check - if a micro block contains both recyclable and non-recyclable rows,
    // it should be opened and processed row by row
    // Micro block 3: rowkey=3,4 (will be reused)
    ASSERT_EQ(OB_SUCCESS, expected_reused_micros.push_back(expected_micros.at(3)));

    iterate_and_verify_micro_blocks(iter, expected_reused_micros, ret);
    STORAGE_LOG(INFO, "MICRO_BLOCK_MERGE_LEVEL: finished, verified micro blocks");
  }

  if (nullptr != iter) {
    iter->~ObPartitionMergeIter();
    iter = nullptr;
  }
  handle1.reset();
}

// Test case: test with uncommitted rows
TEST_P(TestMergeIter, test_uncommitted_rows)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletExeMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);
  ObMergeLevel merge_level = GetParam();

  ObTableHandleV2 handle1;
  const char *micro_data[2];
  // Micro block 1: contains uncommitted row and committed rows
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint  dml  flag    multi_version_row_flag trans_id\n"
      "0        var0  -8       0        NOP      1     T_DML_UPDATE EXIST    LF  trans_id_0\n"
      "1        var1  MIN      0        10       NOP   T_DML_UPDATE  EXIST   FU  trans_id_1\n"
      "1        var1  -8       MIN      3         3    T_DML_UPDATE  EXIST   SC  trans_id_0\n"
      "1        var1  -8       0        3        NOP   T_DML_UPDATE  EXIST   N   trans_id_0\n";

  // Micro block 2: contains committed rows
  micro_data[1] =
      "bigint   var   bigint   bigint   bigint  bigint dml flag    multi_version_row_flag\n"
      "1        var1  -5       0        NOP     3     T_DML_UPDATE EXIST   L\n"
      "2        var2  -5       0        2       2     T_DML_INSERT EXIST   CLF\n";

  int schema_rowkey_cnt = 2;
  int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);

  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 2, true/*contain_uncommitted*/);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable with uncommitted rows");

  // Insert transaction data for uncommitted transaction (trans_id_1 will commit at version 10)
  for (int64_t i = 1; i <= 1; i++) {
    insert_tx_data(i/*tx_id*/, i * 10/*commit_version*/);
  }

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false/*is_full_merge*/, trans_version_range, merge_context, merge_level);
  ObPartitionMinorMerger minor_merger(local_arena_, merge_context.static_param_);
  ASSERT_EQ(OB_SUCCESS, minor_merger.prepare_merge(merge_context, 0));
  STORAGE_LOG(INFO, "finish prepare minor merge with uncommitted rows", K(merge_context));

  ObPartitionMergeIter *iter = minor_merger.merge_helper_->get_merge_iters()[0];
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, iter);

  verify_iter_basic_info(iter, 6);

  // Expected result after merge: uncommitted row should be committed at version 10
  const char *result1 =
      "bigint   var   bigint   bigint   bigint  bigint  flag    multi_version_row_flag\n"
      "0        var0  -8       0        NOP      1      EXIST   LF\n"
      "1        var1  -10      MIN      10       3      EXIST   N\n"
      "1        var1  -10      0        10       NOP    EXIST   N\n"
      "1        var1  -8       0        3        NOP    EXIST   N\n"
      "1        var1  -5       0        NOP      3      EXIST   L\n"
      "2        var2  -5       0        2        2      EXIST   CLF\n";

  // Prepare expected result iterator
  ObMockIterator res_iter;
  ASSERT_EQ(OB_SUCCESS, res_iter.from_for_datum(result1));

  // Iterate and compare with expected results
  int64_t row_idx = 0;
  const blocksstable::ObDatumRow *expected_datum_row = nullptr;
  while (OB_SUCC(ret)) {
    if (iter->is_iter_end()) {
      break;
    } else {
      if (iter && !iter->is_macro_block_opened()) {
        ASSERT_EQ(OB_SUCCESS, iter->open_curr_range(false/*for_rewrite*/, false/*for_compare*/));
        STORAGE_LOG(INFO, "open macro block for macro block merge level", KPC(iter));
      } else if (iter && iter->is_macro_block_opened() && merge_level == MICRO_BLOCK_MERGE_LEVEL && !iter->is_micro_block_opened()) {
        ASSERT_EQ(OB_SUCCESS, iter->open_curr_range(false/*for_rewrite*/, false/*for_compare*/));
        STORAGE_LOG(INFO, "open micro block for micro block merge level", KPC(iter));
      } else {
        const blocksstable::ObDatumRow *row = iter->get_curr_row();
        ASSERT_NE(nullptr, row);
        ASSERT_EQ(OB_SUCCESS, res_iter.get_next_row(expected_datum_row));
        ASSERT_NE(nullptr, expected_datum_row);

        // Compare current row with expected row using ObMockIterator::equals
        bool is_equal = ObMockIterator::equals(*row, *expected_datum_row, false/*cmp multi version row flag*/);
        if (!is_equal) {
          STORAGE_LOG(WARN, "row not equal", K(row_idx), KPC(row), KPC(expected_datum_row));
        } else {
          STORAGE_LOG(INFO, "row equal", K(row_idx), KPC(row), KPC(expected_datum_row));
        }
        ASSERT_TRUE(is_equal) << "row_idx=" << row_idx;
        row_idx++;
        if (OB_FAIL(iter->next())) {
          if (OB_ITER_END != ret) {
            STORAGE_LOG(WARN, "Failed to get next from iter", K(ret));
          }
        }
      }
    }
  }

  STORAGE_LOG(INFO, "test uncommitted rows finished", K(row_idx), K(merge_level));
  ASSERT_EQ(OB_ITER_END, ret);
  // Verify all expected rows are consumed
  ASSERT_EQ(OB_ITER_END, res_iter.get_next_row(expected_datum_row));

  if (nullptr != iter) {
    iter->~ObPartitionMergeIter();
    iter = nullptr;
  }
  handle1.reset();
}

// Reproduce the minor micro reuse pattern behind the upper-trans-version bug.
// The committed version exists only in B2, a middle micro block that can be reused as a whole;
// B1/B3 and the newer sstable A contain only RUNNING rows, so they legitimately contribute 0.
// If the reuse path does not copy B2's max_merged_trans_version from the source micro header into
// the new ObMicroBlockDesc, the new index minor meta is written as 0 and the sstable-level max also
// collapses to 0. Correct behavior must keep the merged sstable max_merged_trans_version at 18.
TEST_P(TestMergeIter, test_minor_micro_reuse_keep_max_merged_trans_version)
{
  int ret = OB_SUCCESS;
  ObMergeLevel merge_level = GetParam();
  if (MICRO_BLOCK_MERGE_LEVEL != merge_level) {
    STORAGE_LOG(INFO, "skip micro reuse max merged trans version test", K(merge_level));
    return;
  }

  // The committed version carried only by the reused (middle) micro.
  const int64_t kReusedCommittedVersion = 18;
  // Two RUNNING transactions feeding the uncommitted (non-reused) micros.
  const int64_t kRunningTxId1 = 1;
  const int64_t kRunningTxId2 = 2;

  ObTabletMergeDagParam param;
  ObTabletExeMergeCtx merge_context(param, allocator_);
  ObTableHandleV2 handle_b;
  const char *micro_data_b[3];
  // B1 (first micro -> always opened by loser tree): 3 RUNNING uncommitted rows, no overlap with A,
  // so the 3 rows pass through cleanly and flush at min row count, leaving the micro writer empty
  // when B2 arrives -> B2 takes the true reuse byte-copy path. Uncommitted -> contributes 0.
  micro_data_b[0] =
      "bigint   var   bigint   bigint   bigint bigint dml          flag    multi_version_row_flag trans_id\n"
      "0        var0  MIN      0        0      0      T_DML_INSERT EXIST   ULF                    trans_id_1\n"
      "1        var1  MIN      0        1      1      T_DML_INSERT EXIST   ULF                    trans_id_1\n"
      "2        var2  MIN      0        2      2      T_DML_INSERT EXIST   ULF                    trans_id_1\n";
  // B2 (middle micro -> REUSED, range [5,5] does not overlap A at pk 100): the only committed row,
  // version 18. The reuse path must propagate header.max_merged_trans_version_=18 to the new micro.
  micro_data_b[1] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "5        var5  -18      0        5      5      EXIST   CLF\n";
  // B3 (last micro): RUNNING uncommitted row, range [8,8] no overlap with A -> reused too; either
  // way it is uncommitted and contributes 0.
  micro_data_b[2] =
      "bigint   var   bigint   bigint   bigint bigint dml          flag    multi_version_row_flag trans_id\n"
      "8        var8  MIN      0        8      8      T_DML_INSERT EXIST   ULF                    trans_id_1\n";

  const int64_t schema_rowkey_cnt = 2;
  int64_t snapshot_version = 20;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_tx(1);
  scn_range.end_scn_.convert_for_tx(snapshot_version);

  row_store_type_ = FLAT_OPT_ROW_STORE;
  prepare_table_schema(micro_data_b, schema_rowkey_cnt, scn_range, snapshot_version);
  table_schema_.set_row_store_type(FLAT_OPT_ROW_STORE);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data_b, 3, true/*contain_uncommitted*/);
  prepare_data_end(handle_b);
  merge_context.static_param_.tables_handle_.add_table(handle_b);

  // SSTable A (newer): a single RUNNING uncommitted row at pk 100, far from B's key range so it
  // never overlaps the reused micros. It exists only to make this a real minor merge and to keep
  // the whole sstable's contain_uncommitted_row=true (mirrors the freshly-written uncommitted block).
  ObTableHandleV2 handle_a;
  const char *micro_data_a[1];
  micro_data_a[0] =
      "bigint   var    bigint   bigint   bigint bigint dml          flag    multi_version_row_flag trans_id\n"
      "100      var100 MIN      0        100    100    T_DML_INSERT EXIST   ULF                    trans_id_2\n";

  snapshot_version = 30;
  scn_range.start_scn_.convert_for_tx(20);
  scn_range.end_scn_.convert_for_tx(snapshot_version);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data_a, 1, true/*contain_uncommitted*/);
  prepare_data_end(handle_a);
  merge_context.static_param_.tables_handle_.add_table(handle_a);

  // Keep both transactions RUNNING so their rows stay uncommitted through the minor merge and
  // contribute 0 to max_merged_trans_version (commit_version<=0 => ObTxData::RUNNING).
  insert_tx_data(kRunningTxId1, 0/*RUNNING*/);
  insert_tx_data(kRunningTxId2, 0/*RUNNING*/);

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 1000;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false/*is_full_merge*/, trans_version_range, merge_context, merge_level);
  merge_context.merge_info_.get_sstable_build_desc().get_desc().micro_block_size_ = 1;
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));

  ObSSTable *merged_sstable = nullptr;
  bool tmp_bool = false;
  ASSERT_EQ(OB_SUCCESS, merge_context.merge_info_.create_sstable(
      merge_context, merge_context.merged_table_handle_, tmp_bool));
  ASSERT_EQ(OB_SUCCESS, merge_context.merged_table_handle_.get_sstable(merged_sstable));
  ASSERT_NE(nullptr, merged_sstable);

  // Per-micro diagnostics: only the committed (reused) micro should carry version 18; uncommitted
  // micros legitimately report 0. Pre-fix the reused micro also reports 0 (the dropped header value).
  ObMicroBlockIndexIterator *micro_iter = nullptr;
  ObDatumRange range;
  ObMicroIndexInfo micro_index_info;
  ObSEArray<int64_t, 8> micro_max_merged_trans_versions;
  range.set_whole_range();
  ASSERT_EQ(OB_SUCCESS, merged_sstable->scan_micro_block(range, full_read_info_, allocator_, micro_iter));
  ASSERT_NE(nullptr, micro_iter);
  while (OB_SUCCESS == (ret = micro_iter->get_next(micro_index_info))) {
    ASSERT_NE(nullptr, micro_index_info.minor_meta_info_);
    const int64_t cur_max_merged_trans_version = micro_index_info.get_max_merged_trans_version();
    STORAGE_LOG(INFO, "check reused micro index minor meta",
        "micro_idx", micro_max_merged_trans_versions.count(),
        K(cur_max_merged_trans_version), K(micro_index_info));
    ASSERT_EQ(OB_SUCCESS, micro_max_merged_trans_versions.push_back(cur_max_merged_trans_version));
  }
  ASSERT_EQ(OB_ITER_END, ret);
  micro_iter->~ObMicroBlockIndexIterator();
  allocator_.free(micro_iter);

  // The sstable still reports itself as containing uncommitted rows, so at creation
  // upper_trans_version stays INT64_MAX -- it is the SSTableGC recompute (which starts from
  // get_max_merged_trans_version()) that ultimately collapses to 0 in production.
  const int64_t sstable_max_merged_trans_version = merged_sstable->get_max_merged_trans_version();
  STORAGE_LOG(INFO, "check merged minor sstable trans version",
      K(sstable_max_merged_trans_version),
      "upper_trans_version", merged_sstable->get_upper_trans_version(),
      "contain_uncommitted_row", merged_sstable->contain_uncommitted_row());
  EXPECT_TRUE(merged_sstable->contain_uncommitted_row())
      << "scenario invalid: the merged minor must still hold uncommitted rows";
  EXPECT_EQ(kReusedCommittedVersion, sstable_max_merged_trans_version)
      << "sstable max_merged_trans_version should equal the committed version (18) carried by the "
      << "reused micro; the reuse path dropped header.max_merged_trans_version_ so it aggregated to 0, "
      << "which makes SSTableGC compute upper_trans_version=0 and wrongly recycle this minor (4377).";

  merger.reset();
  handle_b.reset();
  handle_a.reset();
}

// This test verifies that last flag (L) is correctly handled when rows span across multiple macro blocks
TEST_P(TestMergeIter, test_last_flag_across_blocks_sstable)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletExeMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);
  ObScnRange scn_range;
  ObMergeLevel merge_level = GetParam();

  ObTableHandleV2 handle1;
  const char *micro_data[4];
  micro_data[0] = // MACRO: OP_NONE (max_version=15 > 11)
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "0        var0  -15      MIN      12      1    EXIST   SCF\n"
      "0        var0  -15      0        12      NOP  EXIST   N\n"
      "0        var0  -13      0        2       2    EXIST   N\n";

  micro_data[1] = // MACRO: OP_OPEN
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "0        var0  -10      0        1       1    EXIST   N\n"
      "0        var0  -8       0        3       3    EXIST   N\n";

  micro_data[2] = // MACRO: OP_OPEN
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "0        var0  -6       0        NOP     2    EXIST   L\n"
      "1        var1  -11      MIN      1       5    EXIST   SCF\n"
      "1        var1  -11      0        NOP     5    EXIST   N\n";

  micro_data[3] = // MACRO: OP_OPEN (max_version=12 > 11)
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "1        var1  -10      0        1       1    EXIST   N\n"
      "1        var1  -8       0        3       3    EXIST   CL\n"
      "2        var2  -12      0        3       3    EXIST   CLF\n";

  const int64_t snapshot_version = 25;
  int schema_rowkey_cnt = 2;
  scn_range.start_scn_.convert_for_tx(1);
  scn_range.end_scn_.convert_for_tx(snapshot_version);

  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 2);
  prepare_one_macro(&micro_data[2], 1);
  prepare_one_macro(&micro_data[3], 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 12;
  trans_version_range.base_version_ = 11;

  prepare_merge_context(MINOR_MERGE, false/*is_full_merge*/, trans_version_range, merge_context, merge_level);
  ObPartitionMinorMerger minor_merger(local_arena_, merge_context.static_param_);
  ASSERT_EQ(OB_SUCCESS, minor_merger.prepare_merge(merge_context, 0));
  STORAGE_LOG(INFO, "finish prepare minor merge", K(merge_context));

  ObPartitionMergeIter *iter = minor_merger.merge_helper_->get_merge_iters()[0];
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, iter);

  // Expected result after recycling versions < base_version=11
  const char *result1 =
      "bigint   var   bigint   bigint   bigint  bigint  flag    multi_version_row_flag\n"
      "0        var0  -15      MIN      12      1    EXIST   SCF\n"
      "0        var0  -15      0        12      NOP  EXIST   N\n"
      "0        var0  -13      0        2       2    EXIST   N\n"
      "0        var0  -10      0        1       1    EXIST   CL\n"
      "2        var2  -12      0        3       3    EXIST   CLF\n";

  // Prepare expected result iterator
  ObMockIterator res_iter;
  ASSERT_EQ(OB_SUCCESS, res_iter.from_for_datum(result1));

  // Iterate and compare with expected results
  int64_t row_idx = 0;
  const blocksstable::ObDatumRow *expected_datum_row = nullptr;
  while (OB_SUCC(ret)) {
    if (iter->is_iter_end()) {
      break;
    } else {
      if (iter && !iter->is_macro_block_opened()) {
        ASSERT_EQ(OB_SUCCESS, iter->open_curr_range(false/*for_rewrite*/, false/*for_compare*/));
      } else if (iter && iter->is_macro_block_opened() && merge_level == MICRO_BLOCK_MERGE_LEVEL && !iter->is_micro_block_opened()) {
        ASSERT_EQ(OB_SUCCESS, iter->open_curr_range(false/*for_rewrite*/, false/*for_compare*/));
      } else {
        const blocksstable::ObDatumRow *row = iter->get_curr_row();
        ASSERT_NE(nullptr, row);
        ASSERT_EQ(OB_SUCCESS, res_iter.get_next_row(expected_datum_row));
        ASSERT_NE(nullptr, expected_datum_row);

        // Compare current row with expected row
        bool is_equal = ObMockIterator::equals(*row, *expected_datum_row, false/*cmp multi version row flag*/);
        if (!is_equal) {
          STORAGE_LOG(WARN, "row not equal", K(row_idx), KPC(row), KPC(expected_datum_row));
        } else {
          STORAGE_LOG(INFO, "row equal", K(row_idx), KPC(row), KPC(expected_datum_row));
        }
        ASSERT_TRUE(is_equal) << "row_idx=" << row_idx;
        row_idx++;
        if (OB_FAIL(iter->next())) {
          if (OB_ITER_END != ret) {
            STORAGE_LOG(WARN, "Failed to get next from iter", K(ret));
          }
        }
      }
    }
  }
  res_iter.get_next_row(expected_datum_row);
  STORAGE_LOG(INFO, "test last flag across blocks finished", K(row_idx), K(merge_level), KPC(expected_datum_row));
  ASSERT_EQ(OB_ITER_END, ret);
  // Verify all expected rows are consumed
  ASSERT_EQ(OB_ITER_END, res_iter.get_next_row(expected_datum_row));

  if (nullptr != iter) {
    iter->~ObPartitionMergeIter();
    iter = nullptr;
  }
  handle1.reset();
}

// Bug repro: micro block reuse does not update last_key_with_L_flag_ in ObMacroBlockWriter.
// Scenario:
//   SSTable B (older): 1 macro, 2 micro blocks
//     B1: pk=5(CLF), pk=6(CLF), pk=7(SCF) — last row has NO L flag (is_last_row_last_flag=0)
//     B2: pk=7(N), pk=7(CL), pk=8(CLF) — pk=7 chain continuation + pk=8
//   SSTable A (newer): pk=3(CLF), pk=8(CLF) — pk=8 forces B2 to open row-by-row
//
// Merge flow at MICRO_BLOCK_MERGE_LEVEL:
//   1. append_row(pk=3, CLF) → last_key_with_L_flag_ = true
//   2. append_micro_block(B1) → last_key_ updated to pk=7, but last_key_with_L_flag_ stays true (BUG)
//   3. B2 can't be reused (pk=8 conflicts with SSTable A), opened row-by-row
//   4. append_row(pk=7 continuation) → check_order sees same schema_rowkey + stale L flag → -4105
//
// Fix: add  last_key_with_L_flag_ = micro_block.header_.is_last_row_last_flag();
//      in ObMacroBlockWriter::append_micro_block after setting is_macro_or_micro_block_reused_.
TEST_P(TestMergeIter, test_micro_reuse_L_flag_bug)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletExeMergeCtx merge_context(param, allocator_);
  ObMergeLevel merge_level = GetParam();

  // ---- SSTable B (older, scn_range [1, 20)) ----
  // 1 macro block with 3 micro blocks:
  //   B1: row 0 CLF, row 1 CLF, row 2 CLF (opened by loser tree since it's first micro)
  //   B2: row 3 SCF, row 3 N     (reused! range [3,3] doesn't overlap A's row 6)
  //   B3: row 3 CL, row 6 CLF    (opened because range [3,6] overlaps A's row 6)
  //
  // With micro_block_size_=1, the adaptive splitter flushes after 3 rows (min_micro_row_count=3).
  // After B1's 3 rows are processed, micro_writer is empty (flushed) and last_key_with_L_flag_=true.
  // B2 arrives at append_micro_block with writer row_count=0 → need_merge=false → true reuse path.
  // save_last_key(rowkey) is called but last_key_with_L_flag_ is NOT updated → stays true.
  // B3 is opened, first row (3, CL) has same schema rowkey as B2's end key.
  // check_order sees: same schema rowkey + stale last_key_with_L_flag_=true → ERROR -4105.
  ObTableHandleV2 handle_b;
  const char *micro_data_b[3];

  micro_data_b[0] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "0        var0  -15      0        0      0      EXIST   CLF\n"
      "1        var1  -15      0        1      1      EXIST   CLF\n"
      "2        var2  -10      0        2      2      EXIST   CLF\n";

  micro_data_b[1] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "3        var3  -18      MIN      3      3      EXIST   SCF\n"
      "3        var3  -18      0        3      NOP    EXIST   N\n";

  micro_data_b[2] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "3        var3  -5       0        3      3      EXIST   CL\n"
      "6        var6  -8       0        6      6      EXIST   CLF\n";

  int schema_rowkey_cnt = 2;
  int64_t snapshot_version = 20;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_tx(1);
  scn_range.end_scn_.convert_for_tx(20);

  // Use FLAT_OPT_ROW_STORE so micro blocks match merge output's format
  // (force_flat_store_type() produces FLAT_OPT_ROW_STORE when format_version >= 2).
  // Without this, row_store_type mismatch causes check_micro_block_need_merge to return need_merge=true.
  row_store_type_ = FLAT_OPT_ROW_STORE;
  prepare_table_schema(micro_data_b, schema_rowkey_cnt, scn_range, snapshot_version);
  table_schema_.set_row_store_type(FLAT_OPT_ROW_STORE);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data_b, 3);
  prepare_data_end(handle_b);
  merge_context.static_param_.tables_handle_.add_table(handle_b);
  STORAGE_LOG(INFO, "finish prepare sstable B (older)");

  // ---- SSTable A (newer, scn_range [20, 30)) ----
  // 1 macro, 1 micro: row 6 CLF
  // A's macro range [6,6] shares end_key with B's macro range [1,6],
  // so the loser tree forces both macro blocks to be opened (ALL_RANGE_NEED_OPEN).
  // But B2 (range [3,3]) is later presented for micro-level reuse since it
  // doesn't overlap with A's row 6 position.
  ObTableHandleV2 handle_a;
  const char *micro_data_a[1];

  micro_data_a[0] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "6        var6  -25      0        6      6      EXIST   CLF\n";

  snapshot_version = 30;
  scn_range.start_scn_.convert_for_tx(20);
  scn_range.end_scn_.convert_for_tx(30);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data_a, 1);
  prepare_data_end(handle_a);
  merge_context.static_param_.tables_handle_.add_table(handle_a);
  STORAGE_LOG(INFO, "finish prepare sstable A (newer)");

  // ---- Run minor merge ----
  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 1000;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false/*is_full_merge*/, trans_version_range, merge_context, merge_level);

  // Force micro_block_size to 1 so the adaptive splitter flushes after 3 rows (MICRO_ROW_MIN_COUNT).
  // This ensures the micro writer is empty when B2 arrives at append_micro_block,
  // making check_micro_block_need_merge return need_merge=false → true reuse path.
  merge_context.merge_info_.get_sstable_build_desc().get_desc().micro_block_size_ = 1;

  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);
  ret = merger.merge_partition(merge_context, 0);

  // After fix: both merge levels succeed.
  ASSERT_EQ(OB_SUCCESS, ret);

  // ---- Verify merge result ----
  ObSSTable *merged_sstable = nullptr;
  {
    bool tmp_bool = false;
    ASSERT_EQ(OB_SUCCESS, merge_context.merge_info_.create_sstable(merge_context, merge_context.merged_table_handle_, tmp_bool));
    ASSERT_EQ(OB_SUCCESS, merge_context.merged_table_handle_.get_sstable(merged_sstable));
  }

  const char *result =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "0        var0  -15      0        0      0      EXIST   CLF\n"
      "1        var1  -15      0        1      1      EXIST   CLF\n"
      "2        var2  -10      0        2      2      EXIST   CLF\n"
      "3        var3  -18      MIN      3      3      EXIST   SCF\n"
      "3        var3  -18      0        3      NOP    EXIST   N\n"
      "3        var3  -5       0        3      3      EXIST   CL\n"
      "6        var6  -25      MIN      6      6      EXIST   SCF\n"
      "6        var6  -25      0        6      6      EXIST   C\n"
      "6        var6  -8       0        6      6      EXIST   CL\n";

  ObMockIterator res_iter;
  ObStoreRowIterator *scanner = NULL;
  ObDatumRange range;
  res_iter.reset();
  range.set_whole_range();
  trans_version_range.base_version_ = 1;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_query_param(trans_version_range);
  ASSERT_EQ(OB_SUCCESS, merged_sstable->scan(iter_param_, context_, range, scanner));
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result));
  ObMockDirectReadIterator sstable_iter;
  ASSERT_EQ(OB_SUCCESS, sstable_iter.init(scanner, allocator_, full_read_info_));
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  scanner->~ObStoreRowIterator();
  merger.reset();
  handle_b.reset();
  handle_a.reset();
}

// Point 1 of the minor micro-reuse bug: GENERATING an abnormal minor (write side).
//
// When a minor merge byte-copy reuses a micro block, ObMacroBlockWriter::build_micro_block_desc's
// common tail only propagates max_merged_trans_version_ / contain_uncommitted_row_ / row_count_delta_
// from the source micro header; it never copies header_.is_last_row_last_flag_ /
// is_first_row_first_flag_ into micro_block_desc. ObAggregateInfo::eval keeps the macro meta's
// is_last_row_last_flag_ = the LAST micro desc flag, so when the byte-reused (flag-dropped) micro is
// the macro's last micro, macro_meta.is_last_row_last_flag_ collapses to 0 -- even though the macro
// ends on a complete CLF rowkey. This is the abnormal minor that Point 2
// (test_minor_reuse_corrupt_flag_cause_primary_key_duplicate) later feeds into a second merge.
//
// Layout: A=pk5 against B=[pk0,1,2 / pk5 / pk8] (micro_block_size_=1 flushes every 3 rows). The loser
// tree opens B's macro at the overlapping pk5 micro and merges it row-by-row, while the trailing pk8
// micro (range [8,8], no overlap with A) is byte-reused and becomes the output macro's LAST micro.
// Post-fix, macro_meta.is_last_row_last_flag_ must stay true.
TEST_P(TestMergeIter, test_minor_micro_reuse_keep_macro_meta_last_row_flag)
{
  int ret = OB_SUCCESS;
  ObMergeLevel merge_level = GetParam();
  if (MICRO_BLOCK_MERGE_LEVEL != merge_level) {
    STORAGE_LOG(INFO, "skip macro_meta last_row_last_flag test", K(merge_level));
    return;
  }

  ObTabletMergeDagParam param;
  ObTabletExeMergeCtx merge_context(param, allocator_);

  // ---- SSTable B (older) : 1 macro / 3 micros ----
  ObTableHandleV2 handle_b;
  const char *micro_data_b[3];
  micro_data_b[0] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "0        var0  -15      0        0      0      EXIST   CLF\n"
      "1        var1  -15      0        1      1      EXIST   CLF\n"
      "2        var2  -10      0        2      2      EXIST   CLF\n";
  micro_data_b[1] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "5        var5  -18      0        5      5      EXIST   CLF\n";
  micro_data_b[2] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "8        var8  -8       0        8      8      EXIST   CLF\n";

  int schema_rowkey_cnt = 2;
  int64_t snapshot_version = 20;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_tx(1);
  scn_range.end_scn_.convert_for_tx(20);

  row_store_type_ = FLAT_OPT_ROW_STORE;
  prepare_table_schema(micro_data_b, schema_rowkey_cnt, scn_range, snapshot_version);
  table_schema_.set_row_store_type(FLAT_OPT_ROW_STORE);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data_b, 3);
  prepare_data_end(handle_b);
  merge_context.static_param_.tables_handle_.add_table(handle_b);
  STORAGE_LOG(INFO, "finish prepare sstable B (older)");

  // ---- SSTable A (newer) : pk5, overlaps only B's middle micro ----
  // A=pk5 (not pk8) so the trailing pk8 micro is byte-reused and becomes the output macro's LAST
  // micro -> macro_meta.is_last_row_last_flag_ is the dropped flag.
  ObTableHandleV2 handle_a;
  const char *micro_data_a[1];
  micro_data_a[0] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "5        var5  -25      0        5      5      EXIST   CLF\n";

  snapshot_version = 30;
  scn_range.start_scn_.convert_for_tx(20);
  scn_range.end_scn_.convert_for_tx(30);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data_a, 1);
  prepare_data_end(handle_a);
  merge_context.static_param_.tables_handle_.add_table(handle_a);
  STORAGE_LOG(INFO, "finish prepare sstable A (newer)");

  // ---- Run minor merge ----
  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 1000;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false/*is_full_merge*/, trans_version_range, merge_context, merge_level);
  merge_context.merge_info_.get_sstable_build_desc().get_desc().micro_block_size_ = 1;
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));

  ObSSTable *merged_sstable = nullptr;
  bool tmp_bool = false;
  ASSERT_EQ(OB_SUCCESS, merge_context.merge_info_.create_sstable(
      merge_context, merge_context.merged_table_handle_, tmp_bool));
  ASSERT_EQ(OB_SUCCESS, merge_context.merged_table_handle_.get_sstable(merged_sstable));
  ASSERT_NE(nullptr, merged_sstable);

  // Iterate the merged sstable's macro metas; the macro that ends on the reused complete pk8 rowkey
  // must report is_last_row_last_flag_ == true. Pre-fix the byte-copy reuse dropped it to false.
  ObSSTableSecMetaIterator meta_iter;
  ObDataMacroBlockMeta data_macro_meta;
  ObDatumRange range;
  range.set_whole_range();
  ASSERT_EQ(OB_SUCCESS, meta_iter.open(
      range, ObMacroBlockMetaType::DATA_BLOCK_META, *merged_sstable, full_read_info_, allocator_));

  int64_t macro_cnt = 0;
  int64_t last_flag_false_cnt = 0;
  int64_t first_flag_false_cnt = 0;
  while (OB_SUCCESS == (ret = meta_iter.get_next(data_macro_meta))) {
    const bool last_flag = data_macro_meta.val_.is_last_row_last_flag_;
    const bool first_flag = data_macro_meta.val_.is_first_row_first_flag_;
    STORAGE_LOG(INFO, "check merged macro meta flags",
        K(macro_cnt), K(last_flag), K(first_flag), K(data_macro_meta));
    if (!last_flag) {
      ++last_flag_false_cnt;
    }
    if (!first_flag) {
      ++first_flag_false_cnt;
    }
    ++macro_cnt;
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_GT(macro_cnt, 0);

  EXPECT_EQ(0, last_flag_false_cnt)
      << "the macro ends on the byte-reused complete pk8 CLF rowkey, so macro_meta."
      << "is_last_row_last_flag_ must be true; the reuse path dropped it, and a later minor merge "
      << "reads this 0 to mis-set rowkey_state -> skip committing-trans compaction -> check_order "
      << "OB_ERR_PRIMARY_KEY_DUPLICATE (-5024).";
  STORAGE_LOG(INFO, "macro meta flag summary",
      K(macro_cnt), K(last_flag_false_cnt), K(first_flag_false_cnt));

  merger.reset();
  handle_b.reset();
  handle_a.reset();
}

// Point 2 of the minor micro-reuse bug: the abnormal minor PARTICIPATING in a later minor merge,
// which is the only place where the dropped flag actually surfaces as -5024.
//
// Stage 1 produces an abnormal minor M with TWO macro blocks in a SINGLE sstable:
//   macro A : ends on a byte-reused complete committed rowkey (pk8 CLF) -> macro_meta
//             is_last_row_last_flag dropped to 0 (pre-fix); all-committed so
//             contain_uncommitted_row = false (whole-reusable by stage 2).
//   macro B : a committing transaction (same uncommitted trans_id, pk9) ->
//             contain_uncommitted_row = true.
// Stage 2 minor-merges M (older) with a newer small sstable whose rowkeys do not overlap macro A,
// so stage 2 whole-reuses macro A (flush_reuse_macro_block, ObBlockOp::OP_NONE). next_macro_block
// then seeds the next rowkey's state from macro A's flag:
//     flag == true  -> set_last_row_output()            (honest)
//     flag == false -> set_first_committed_row_output()  (corrupt)
// The committing-trans compaction gate in try_make_committing_trans_compacted requires
//     is_last_row_output() || is_uncommitted_row_output()
// so the corrupt flag closes the gate, macro B's committing transaction is NOT compacted, its
// INSERT+DELETE are emitted raw, and check_order raises OB_ERR_PRIMARY_KEY_DUPLICATE (-5024).
//
// Pre-fix (ob_macro_block_writer.cpp:1771-1772 still commented): stage 2 returns -5024.
// Post-fix: macro A keeps flag=1, gate stays open, stage 2 returns OB_SUCCESS.
//
// NOTE: this is a fiddly layout (forced 2-macro split, byte-reuse on macro A, whole-reuse of A +
// open of B in stage 2). The macro split point is tuned via macro_store_size_.
TEST_P(TestMergeIter, test_minor_reuse_corrupt_flag_cause_primary_key_duplicate)
{
  int ret = OB_SUCCESS;
  ObMergeLevel merge_level = GetParam();
  if (MICRO_BLOCK_MERGE_LEVEL != merge_level) {
    STORAGE_LOG(INFO, "skip corrupt-flag -5024 test", K(merge_level));
    return;
  }

  // =============================== Stage 1: build abnormal minor M ===============================
  ObTabletMergeDagParam param1;
  ObTabletExeMergeCtx merge_ctx1(param1, allocator_);

  // ---- old sstable B: committed micros + a committing-trans micro ----
  // micro0 pk0,1,2 CLF (committed, first micro -> opened by loser tree)
  // micro1 pk5 CLF      (committed, overlaps A -> rewritten)
  // micro2 pk8 CLF      (committed, no overlap with A -> BYTE-REUSED -> dropped flag)
  // micro3 pk9 FU/...   (uncommitted trans_id_9 -> stays in macro B, contain_uncommitted)
  ObTableHandleV2 handle_b;
  const char *micro_data_b[4];
  micro_data_b[0] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "0        var0  -15      0        0      0      EXIST   CLF\n"
      "1        var1  -15      0        1      1      EXIST   CLF\n"
      "2        var2  -10      0        2      2      EXIST   CLF\n";
  micro_data_b[1] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "5        var5  -18      0        5      5      EXIST   CLF\n";
  micro_data_b[2] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "8        var8  -8       0        8      8      EXIST   CLF\n";
  micro_data_b[3] =
      "bigint   var   bigint   bigint   bigint bigint  dml          flag    multi_version_row_flag trans_id\n"
      "9        var9  MIN      -10      9      NOP     T_DML_INSERT EXIST   FU  trans_id_9\n"
      "9        var9  MIN      -9       NOP    NOP     T_DML_DELETE DELETE  LU  trans_id_9\n";

  int schema_rowkey_cnt = 2;
  int64_t snapshot_version = 20;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_tx(1);
  scn_range.end_scn_.convert_for_tx(20);

  row_store_type_ = FLAT_OPT_ROW_STORE;
  prepare_table_schema(micro_data_b, schema_rowkey_cnt, scn_range, snapshot_version);
  table_schema_.set_row_store_type(FLAT_OPT_ROW_STORE);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data_b, 4, true/*contain_uncommitted*/);
  prepare_data_end(handle_b);
  merge_ctx1.static_param_.tables_handle_.add_table(handle_b);
  STORAGE_LOG(INFO, "stage1: finish prepare sstable B (older)");

  // ---- newer sstable A: pk5, overlaps only B's middle committed micro ----
  ObTableHandleV2 handle_a;
  const char *micro_data_a[1];
  micro_data_a[0] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "5        var5  -25      0        5      5      EXIST   CLF\n";
  snapshot_version = 30;
  scn_range.start_scn_.convert_for_tx(20);
  scn_range.end_scn_.convert_for_tx(30);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data_a, 1);
  prepare_data_end(handle_a);
  merge_ctx1.static_param_.tables_handle_.add_table(handle_a);
  STORAGE_LOG(INFO, "stage1: finish prepare sstable A (newer)");

  // stage1: keep trans_id_9 RUNNING (uncommitted) so the committing rows are written raw into M.
  clear_tx_data();
  insert_tx_data(9/*tx_id*/, 0/*commit_version=0 -> RUNNING*/);

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 1000;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;
  prepare_merge_context(MINOR_MERGE, false/*is_full_merge*/, trans_version_range, merge_ctx1, merge_level);
  merge_ctx1.merge_info_.get_sstable_build_desc().get_desc().micro_block_size_ = 1;
  // Force an early macro split so macro A (committed, byte-reuse tail) and macro B (committing) land
  // in different output macro blocks. Tuned by experiment.
  merge_ctx1.merge_info_.get_sstable_build_desc().get_desc().static_desc_->macro_store_size_ = 200;

  ObPartitionMinorMerger merger1(local_arena_, merge_ctx1.static_param_);
  ASSERT_EQ(OB_SUCCESS, merger1.merge_partition(merge_ctx1, 0));

  ObSSTable *minor_m = nullptr;
  bool tmp_bool = false;
  ASSERT_EQ(OB_SUCCESS, merge_ctx1.merge_info_.create_sstable(
      merge_ctx1, merge_ctx1.merged_table_handle_, tmp_bool));
  ASSERT_EQ(OB_SUCCESS, merge_ctx1.merged_table_handle_.get_sstable(minor_m));
  ASSERT_NE(nullptr, minor_m);

  // ---- inspect M's macro structure (probe) ----
  {
    ObSSTableSecMetaIterator meta_iter;
    ObDataMacroBlockMeta dmm;
    ObDatumRange whole;
    whole.set_whole_range();
    ASSERT_EQ(OB_SUCCESS, meta_iter.open(
        whole, ObMacroBlockMetaType::DATA_BLOCK_META, *minor_m, full_read_info_, allocator_));
    int64_t idx = 0;
    while (OB_SUCCESS == (ret = meta_iter.get_next(dmm))) {
      STORAGE_LOG(INFO, "stage1: M macro meta probe", K(idx),
          "is_last_row_last_flag", dmm.val_.is_last_row_last_flag_,
          "is_first_row_first_flag", dmm.val_.is_first_row_first_flag_,
          "contain_uncommitted_row", dmm.val_.contain_uncommitted_row_,
          "row_count", dmm.val_.row_count_,
          K(dmm));
      ++idx;
    }
    ASSERT_EQ(OB_ITER_END, ret);
    STORAGE_LOG(INFO, "stage1: M macro count", K(idx));
  }

  // =============================== Stage 2: minor-merge M with a newer sstable ===============================
  // Newer sstable C touches only pk5 (overlaps M's macro A1). So:
  //   macro A0 (pk0,1,2) : no overlap -> whole-reused (flush_reuse_macro_block, flag=0 corrupt)
  //   macro A1 (pk5)     : overlaps C -> opened, pk5 committed -> rowkey_state honest
  //   macro A2 (pk8)     : no overlap -> whole-reused -> update_rowkey_state_by_prev_block(none, flag=0)
  //                        -> set_first_committed_row_output (CORRUPT, should have been last_row_output)
  //   macro A3 (pk9)     : contain_uncommitted -> opened; trans_id_9 now committed -> committing trans.
  // The committing-trans compaction gate (is_last_row_output() || is_uncommitted_row_output()) is closed
  // by the corrupt state seeded from A2, so pk9's INSERT+DELETE are emitted raw -> check_order -5024.
  ObTabletMergeDagParam param2;
  ObTabletExeMergeCtx merge_ctx2(param2, allocator_);

  // M (older) carries scn [1,30); add it first.
  merge_ctx2.static_param_.tables_handle_.add_table(merge_ctx1.merged_table_handle_);

  ObTableHandleV2 handle_c;
  const char *micro_data_c[1];
  micro_data_c[0] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "5        var5  -35      0        5      5      EXIST   CLF\n";
  snapshot_version = 40;
  scn_range.start_scn_.convert_for_tx(30);
  scn_range.end_scn_.convert_for_tx(40);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data_c, 1);
  prepare_data_end(handle_c);
  merge_ctx2.static_param_.tables_handle_.add_table(handle_c);
  STORAGE_LOG(INFO, "stage2: finish prepare sstable C (newer)");

  // Commit trans_id_9 so pk9's INSERT+DELETE become a committing transaction during stage2 merge.
  clear_tx_data();
  insert_tx_data(9/*tx_id*/, 25/*commit_version*/);

  ObVersionRange trans_version_range2;
  trans_version_range2.snapshot_version_ = 1000;
  trans_version_range2.multi_version_start_ = 1;
  trans_version_range2.base_version_ = 1;
  prepare_merge_context(MINOR_MERGE, false/*is_full_merge*/, trans_version_range2, merge_ctx2, merge_level);

  ObPartitionMinorMerger merger2(local_arena_, merge_ctx2.static_param_);
  // Pre-fix (ob_macro_block_writer.cpp:1771-1772 commented): A2's corrupt flag=0 skips committing-trans
  // compaction -> check_order returns OB_ERR_PRIMARY_KEY_DUPLICATE (-5024).
  // Post-fix: A2 keeps flag=1, gate stays open, merge succeeds.
  ret = merger2.merge_partition(merge_ctx2, 0);
  STORAGE_LOG(INFO, "stage2: minor merge result", K(ret));
  ASSERT_EQ(OB_SUCCESS, ret)
      << "stage2 minor merge should succeed; pre-fix it returns -5024 because the byte-reused macro A2 "
      << "(pk8 CLF) lost is_last_row_last_flag_, mis-seeding rowkey_state and skipping the pk9 "
      << "committing-transaction compaction.";

  clear_tx_data();
  merger2.reset();
  merger1.reset();
  handle_c.reset();
  handle_b.reset();
  handle_a.reset();
}

// Regression test for the minor micro-block-reuse + instant-add-column bug.
//
// After add-column, the old sstable (6 cols) has fewer columns than the merge schema (7 cols).
// Pre-fix, a non-overlapping old micro block was byte-copy reused, and the column-count check in
// build_micro_block_desc_with_rewrite rejected it (old 6 != new 7) → minor merge fails with -4016.
// Fix: get_block_op() returns set_open() on a column-count mismatch, so the block is opened and
// projected to the new schema instead of byte-copied.
//
// Layout (B = OLD 6-col schema, A = NEW 7-col schema after add-column):
//   SSTable B (scn [1,20)), 1 macro / 3 micros:
//     B1: row 0 CLF, row 1 CLF, row 2 CLF
//     B2: row 3 SCF, row 3 N                ← no overlap with A → pre-fix byte-copy reused
//     B3: row 3 CL, row 6 CLF               ← overlaps A's row 6 → opened
//   SSTable A (scn [20,30)), 1 macro / 1 micro:
//     A1: row 6 CLF
//
// Both merge levels now succeed (pre-fix, MICRO returned -4016).
TEST_P(TestMergeIter, test_micro_reuse_add_column_bug)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletExeMergeCtx merge_context(param, allocator_);
  ObMergeLevel merge_level = GetParam();

  // ---- SSTable B (older, scn_range [1, 20), OLD schema: 2 data cols = 6 total) ----
  ObTableHandleV2 handle_b;
  const char *micro_data_b[3];

  micro_data_b[0] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "0        var0  -15      0        0      0      EXIST   CLF\n"
      "1        var1  -15      0        1      1      EXIST   CLF\n"
      "2        var2  -10      0        2      2      EXIST   CLF\n";

  micro_data_b[1] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "3        var3  -18      MIN      3      3      EXIST   SCF\n"
      "3        var3  -18      0        3      NOP    EXIST   N\n";

  micro_data_b[2] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "3        var3  -5       0        3      3      EXIST   CL\n"
      "6        var6  -8       0        6      6      EXIST   CLF\n";

  int schema_rowkey_cnt = 2;
  int64_t snapshot_version = 20;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_tx(1);
  scn_range.end_scn_.convert_for_tx(20);

  row_store_type_ = FLAT_OPT_ROW_STORE;
  prepare_table_schema(micro_data_b, schema_rowkey_cnt, scn_range, snapshot_version);
  table_schema_.set_row_store_type(FLAT_OPT_ROW_STORE);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data_b, 3);
  prepare_data_end(handle_b);
  merge_context.static_param_.tables_handle_.add_table(handle_b);
  STORAGE_LOG(INFO, "finish prepare sstable B (older, 6 cols)");

  // ---- Add 1 column: schema goes from 6 to 7 total columns ----
  // SSTable A (newer, scn_range [20, 30), NEW schema: 3 data cols = 7 total)
  ObTableHandleV2 handle_a;
  const char *micro_data_a[1];

  micro_data_a[0] =
      "bigint   var   bigint   bigint   bigint bigint bigint  flag    multi_version_row_flag\n"
      "6        var6  -25      0        6      6      60      EXIST   CLF\n";

  snapshot_version = 30;
  scn_range.start_scn_.convert_for_tx(20);
  scn_range.end_scn_.convert_for_tx(30);

  // Re-prepare table schema with new column count; this also re-inits the tablet's storage schema
  prepare_table_schema(micro_data_a, schema_rowkey_cnt, scn_range, snapshot_version);
  table_schema_.set_row_store_type(FLAT_OPT_ROW_STORE);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data_a, 1);
  prepare_data_end(handle_a);
  merge_context.static_param_.tables_handle_.add_table(handle_a);
  STORAGE_LOG(INFO, "finish prepare sstable A (newer, 7 cols after add-column)");

  // ---- Run minor merge ----
  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 1000;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false/*is_full_merge*/, trans_version_range, merge_context, merge_level);

  merge_context.merge_info_.get_sstable_build_desc().get_desc().micro_block_size_ = 1;

  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);
  ret = merger.merge_partition(merge_context, 0);
  STORAGE_LOG(INFO, "test_micro_reuse_add_column_bug: merge result", K(ret), K(merge_level));

  // Tear down all objects BEFORE asserting so a failed assertion does not leak macro-block
  // references (which would hang the tenant IO manager during TearDown).
  merger.reset();
  handle_b.reset();
  handle_a.reset();

  ASSERT_EQ(OB_SUCCESS, ret);
}

// Row-path variant of test_micro_reuse_add_column_bug. Here the reused old micro block hits the
// row-path check_order (ob_macro_block_writer.cpp:1356) — the error point seen in the production
// 4016.log — instead of the byte-copy header check.
//
// Pre-fix mechanism (MICRO level): B1 overlaps A → opened, leaving buffered rows in the micro
// writer. B2 (unique to B) then takes the merge_micro_block path (need_merge=true), whose decoder
// reads at the physical column count → check_order: row.count(6) != row_column_count(7) → -4016.
// The same get_block_op() fix opens B2 and projects it to 7 cols, avoiding that path.
//
// Layout (B = OLD 6-col, A = NEW 7-col):
//   SSTable B (scn [1,20)), 1 macro / 2 micros:
//     B1: row 3 CLF  ← overlaps A → opened
//     B2: row 6 CLF  ← unique to B; pre-fix byte-reuse → -4016, now opened + projected
//   SSTable A (scn [20,30)), 1 macro / 1 micro:
//     A1: row 3 CLF (newer version)
//
// Both merge levels now succeed (pre-fix, MICRO returned -4016).
TEST_P(TestMergeIter, test_micro_reuse_add_column_row_path_bug)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletExeMergeCtx merge_context(param, allocator_);
  ObMergeLevel merge_level = GetParam();

  // ---- SSTable B (older, scn_range [1, 20), OLD schema: 2 data cols = 6 total) ----
  ObTableHandleV2 handle_b;
  const char *micro_data_b[2];

  // B1: row 3, overlaps with A → will be opened
  micro_data_b[0] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "3        var3  -15      0        3      3      EXIST   CLF\n";

  // B2: row 6, unique to B → will be reused (not opened)
  micro_data_b[1] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "6        var6  -8       0        6      6      EXIST   CLF\n";

  int schema_rowkey_cnt = 2;
  int64_t snapshot_version = 20;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_tx(1);
  scn_range.end_scn_.convert_for_tx(20);

  row_store_type_ = FLAT_OPT_ROW_STORE;
  prepare_table_schema(micro_data_b, schema_rowkey_cnt, scn_range, snapshot_version);
  table_schema_.set_row_store_type(FLAT_OPT_ROW_STORE);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data_b, 2);
  prepare_data_end(handle_b);
  merge_context.static_param_.tables_handle_.add_table(handle_b);
  STORAGE_LOG(INFO, "finish prepare sstable B (older, 6 cols) [row-path]");

  // ---- Add 1 column: schema goes from 6 to 7 total columns ----
  ObTableHandleV2 handle_a;
  const char *micro_data_a[1];

  micro_data_a[0] =
      "bigint   var   bigint   bigint   bigint bigint bigint  flag    multi_version_row_flag\n"
      "3        var3  -25      0        3      3      30      EXIST   CLF\n";

  snapshot_version = 30;
  scn_range.start_scn_.convert_for_tx(20);
  scn_range.end_scn_.convert_for_tx(30);

  prepare_table_schema(micro_data_a, schema_rowkey_cnt, scn_range, snapshot_version);
  table_schema_.set_row_store_type(FLAT_OPT_ROW_STORE);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data_a, 1);
  prepare_data_end(handle_a);
  merge_context.static_param_.tables_handle_.add_table(handle_a);
  STORAGE_LOG(INFO, "finish prepare sstable A (newer, 7 cols after add-column) [row-path]");

  // ---- Run minor merge ----
  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 1000;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false/*is_full_merge*/, trans_version_range, merge_context, merge_level);

  // Keep the default (large) micro_block_size so B2 stays small relative to the merge threshold
  // (data_length <= micro_block_size / 2), which keeps need_merge=true once B1 buffered rows.
  // Setting micro_block_size_ = 1 would force need_merge=false (byte-copy only) and miss this path.

  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);
  ret = merger.merge_partition(merge_context, 0);
  STORAGE_LOG(INFO, "test_micro_reuse_add_column_row_path_bug: merge result", K(ret), K(merge_level));

  merger.reset();
  handle_b.reset();
  handle_a.reset();

  ASSERT_EQ(OB_SUCCESS, ret);
}

// Mock filter mimicking ObRowscnFilter's "all rows above recycle version" case: it returns
// `none` (keep + reuse whole block) for every micro block. get_trans_version_col_idx() steers
// inner_get_block_op_from_filter() into the skip-index branch so get_filter_op() is consulted
// rather than an unconditional open(); filter()=KEEP keeps it out of recycling state.
// Used to verify a filter `none` can no longer short-circuit the column-count check in get_block_op().
class MockNoneCompactionFilter : public ObICompactionFilter
{
public:
  explicit MockNoneCompactionFilter(const int64_t trans_version_col_idx)
    : trans_version_col_idx_(trans_version_col_idx) {}
  virtual ~MockNoneCompactionFilter() {}
  virtual int filter(const blocksstable::ObDatumRow &row, ObFilterRet &filter_ret) const override
  {
    UNUSED(row);
    filter_ret = FILTER_RET_KEEP;
    return OB_SUCCESS;
  }
  virtual CompactionFilterType get_filter_type() const override { return ROWSCN_FILTER; }
  virtual int get_filter_op(
      blocksstable::ObAggRowCachedReader &agg_row_cached_reader,
      ObBlockOp &op) const override
  {
    UNUSED(agg_row_cached_reader);
    op.set_none();
    return OB_SUCCESS;
  }
  virtual int64_t get_trans_version_col_idx() const override { return trans_version_col_idx_; }
private:
  int64_t trans_version_col_idx_;
};

// Guards the filter-path gap once flagged in analysis.md ("filter 路径可能短路列数检查").
// Same add-column layout as test_micro_reuse_add_column_bug, plus a mock filter (returning `none`)
// injected into merge_context.filter_ctx_.compaction_filter_ after prepare_merge_context.
//
// Pre-fix, get_block_op() consulted the filter before the column-count check, so the filter's
// `none` on a 6-col micro block bypassed the check → byte reuse → -4016 at MICRO level. The fix
// orders the column-count check ahead of the filter branch, so both levels now succeed.
TEST_P(TestMergeIter, test_micro_reuse_add_column_with_filter_bug)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletExeMergeCtx merge_context(param, allocator_);
  ObMergeLevel merge_level = GetParam();

  // ---- SSTable B (older, scn_range [1, 20), OLD schema: 2 data cols = 6 total) ----
  ObTableHandleV2 handle_b;
  const char *micro_data_b[3];

  micro_data_b[0] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "0        var0  -15      0        0      0      EXIST   CLF\n"
      "1        var1  -15      0        1      1      EXIST   CLF\n"
      "2        var2  -10      0        2      2      EXIST   CLF\n";

  micro_data_b[1] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "3        var3  -18      MIN      3      3      EXIST   SCF\n"
      "3        var3  -18      0        3      NOP    EXIST   N\n";

  micro_data_b[2] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "3        var3  -5       0        3      3      EXIST   CL\n"
      "6        var6  -8       0        6      6      EXIST   CLF\n";

  int schema_rowkey_cnt = 2;
  int64_t snapshot_version = 20;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_tx(1);
  scn_range.end_scn_.convert_for_tx(20);

  row_store_type_ = FLAT_OPT_ROW_STORE;
  prepare_table_schema(micro_data_b, schema_rowkey_cnt, scn_range, snapshot_version);
  table_schema_.set_row_store_type(FLAT_OPT_ROW_STORE);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data_b, 3);
  prepare_data_end(handle_b);
  merge_context.static_param_.tables_handle_.add_table(handle_b);
  STORAGE_LOG(INFO, "finish prepare sstable B (older, 6 cols) [filter]");

  // ---- Add 1 column: schema goes from 6 to 7 total columns ----
  ObTableHandleV2 handle_a;
  const char *micro_data_a[1];

  micro_data_a[0] =
      "bigint   var   bigint   bigint   bigint bigint bigint  flag    multi_version_row_flag\n"
      "6        var6  -25      0        6      6      60      EXIST   CLF\n";

  snapshot_version = 30;
  scn_range.start_scn_.convert_for_tx(20);
  scn_range.end_scn_.convert_for_tx(30);

  prepare_table_schema(micro_data_a, schema_rowkey_cnt, scn_range, snapshot_version);
  table_schema_.set_row_store_type(FLAT_OPT_ROW_STORE);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data_a, 1);
  prepare_data_end(handle_a);
  merge_context.static_param_.tables_handle_.add_table(handle_a);
  STORAGE_LOG(INFO, "finish prepare sstable A (newer, 7 cols after add-column) [filter]");

  // ---- Run minor merge ----
  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 1000;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false/*is_full_merge*/, trans_version_range, merge_context, merge_level);

  merge_context.merge_info_.get_sstable_build_desc().get_desc().micro_block_size_ = 1;

  // Inject the mock filter. trans_version column index in the multi-version row is
  // schema_rowkey_cnt (= 2): after the 2 rowkey columns come trans_version, sql_sequence.
  MockNoneCompactionFilter mock_filter(schema_rowkey_cnt);
  merge_context.filter_ctx_.compaction_filter_ = &mock_filter;
  ASSERT_TRUE(merge_context.has_filter());

  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);
  ret = merger.merge_partition(merge_context, 0);
  STORAGE_LOG(INFO, "test_micro_reuse_add_column_with_filter_bug: merge result", K(ret), K(merge_level));

  merger.reset();
  merge_context.filter_ctx_.compaction_filter_ = nullptr;
  handle_b.reset();
  handle_a.reset();

  // Both levels succeed; pre-fix the filter's `none` short-circuited the check → -4016 at MICRO.
  STORAGE_LOG(INFO, "FILTER REGRESSION RESULT", K(merge_level), K(ret));
  ASSERT_EQ(OB_SUCCESS, ret);
}

INSTANTIATE_TEST_CASE_P(MergeLevelTests, TestMergeIter,
  testing::Values(MACRO_BLOCK_MERGE_LEVEL, MICRO_BLOCK_MERGE_LEVEL));

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_merge_iter.log*");
  OB_LOGGER.set_file_name("test_merge_iter.log");
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
