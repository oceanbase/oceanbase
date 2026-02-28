// owner: dengzhi.ldz
// owner group: storage

/**
 * Test Suite: Multi-Version Merge with Row-Level Recycling
 *
 * Purpose:
 *   This test suite verifies the row-level recycling logic during minor merge operations.
 *   It ensures proper handling of macro block filtering, reuse, and F-flag recycling rules.
 *
 * Core Recycling Logic:
 *   1. Recycle Version: Determined by trans_version_range.base_version_
 *      - If base_version > 1, recycling is enabled
 *      - Rows with version <= base_version are candidates for recycling
 *
 *   2. F-Flag Recycling Rule:
 *      - If an F-flagged rowkey is recycled, ALL versions of that rowkey must be recycled
 *      - This applies even when the rowkey spans multiple micro/macro blocks
 *
 *   3. Block-Level Recycling:
 *      - Macro/Micro blocks should be recycled as a whole when possible
 *      - Decision: Check if max_version of all rows in block <= base_version
 *      - If recyclable as whole, don't open the block (OP_FILTER)
 *      - If reusable as whole, don't open the block (OP_NONE)
 *
 *   4. Avoid Unnecessary Block Opening:
 *      - If a macro/micro block can be entirely reused, don't open it
 *      - Only open blocks that need partial recycling or merging (OP_OPEN)
 *
 * Statistics Verified:
 *   - macro_cnt[OP_FILTER]: Macros filtered at block level
 *   - macro_cnt[OP_NONE]: Macros reused without opening
 *   - micro_cnt[*]: Always 0 for mini/minor sstables (not tracked)
 *   - filter_block_row_cnt: Rows filtered at block level (macro/micro)
 *   - row_cnt[FILTER_RET_REMOVE]: Rows filtered at row level (after opening block)
 *
 * Test Coverage:
 *   - Macro-level complete filtering and reuse
 *   - Micro-level partial filtering within a macro
 *   - F-flag rowkey recycling across micro/macro boundaries
 *   - Mixed scenarios of reuse and recycling
 *   - Uncommitted transaction handling during merge
 */

// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.


#include <gtest/gtest.h>
#define private public
#define protected public
#include "lib/container/ob_iarray.h"
#include "storage/blocksstable/ob_data_file_prepare.h"
#include "storage/blocksstable/ob_row_generate.h"
#include "observer/ob_service.h"
#include "storage/ob_i_store.h"
#include "storage/ob_i_table.h"
#include "storage/compaction/ob_sstable_merge_info_mgr.h"
#include "storage/compaction/ob_partition_merge_iter.h"
#include "storage/compaction/ob_partition_merger.h"
#include "storage/compaction/ob_tablet_merge_ctx.h"
#include "storage/blocksstable/ob_multi_version_sstable_test.h"

#include "storage/memtable/utils_rowkey_builder.h"
#include "storage/memtable/utils_mock_row.h"
#include "storage/tx/ob_mock_tx_ctx.h"
#include "storage/init_basic_struct.h"
#include "storage/test_tablet_helper.h"
#include "storage/tx_table/ob_tx_table.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx/ob_trans_ctx_mgr_v4.h"
#include "mtlenv/storage/access/test_merge_basic.h"

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
namespace storage
{

class TestMultiVersionMergeRecycle : public TestMergeBasic
{
public:
  TestMultiVersionMergeRecycle();
  virtual ~TestMultiVersionMergeRecycle() {}

  void SetUp();
  void TearDown();
  static void SetUpTestCase();
  static void TearDownTestCase();
  void prepare_merge_context(const ObMergeType &merge_type,
                             const bool is_full_merge,
                             const ObVersionRange &trans_version_range,
                             ObTabletExeMergeCtx &merge_context)
  {
    TestMergeBasic::prepare_merge_context(
      merge_type, is_full_merge, trans_version_range, &merge_dag_, merge_context, false/*is_delete_insert_merge*/);
  }
  int64_t schema_rowkey_cnt_;
  ObTabletMergeExecuteDag merge_dag_;
};

void TestMultiVersionMergeRecycle::SetUpTestCase()
{
  ObMultiVersionSSTableTest::SetUpTestCase();
  // mock sequence no
  ObClockGenerator::init();
  TestMergeBasic::create_tablet();
}

void TestMultiVersionMergeRecycle::TearDownTestCase()
{
  ObMultiVersionSSTableTest::TearDownTestCase();
  // reset sequence no
  ObClockGenerator::destroy();
}

TestMultiVersionMergeRecycle::TestMultiVersionMergeRecycle()
  : TestMergeBasic("test_multi_version_merge_recycle"),
    schema_rowkey_cnt_(2)
{}

void TestMultiVersionMergeRecycle::SetUp()
{
  ObMultiVersionSSTableTest::SetUp();
}

void TestMultiVersionMergeRecycle::TearDown()
{
  ObMultiVersionSSTableTest::TearDown();
  TRANS_LOG(INFO, "teardown success");
}

#define PREPARE_SCN_RANGE(start_scn, end_scn) \
  scn_range.start_scn_.convert_for_tx(start_scn); \
  scn_range.end_scn_.convert_for_tx(end_scn); \
  table_key_.scn_range_ = scn_range;

/**
 * TEST 1: recycle_macro - Macro-level Block Recycling
 *
 * Objective: Verify macro block-level filtering and reuse decisions
 *
 * Data Layout:
 *   SSTable1: Macro1(rowkey=0 v-6, rowkey=1 v-6) + Macro2(rowkey=2 v-10)
 *   SSTable2: Macro1(rowkey=3 v-8,v-6) + Macro2(rowkey=5 v-10)
 *
 * Recycle Version: base_version=9
 *
 * Expected Macro Operations:
 *   - SSTable1-Macro1: OP_FILTER (max_version=6 <= 9, all rows recyclable)
 *   - SSTable1-Macro2: OP_NONE (max_version=10 > 9, reuse without opening)
 *   - SSTable2-Macro1: OP_FILTER (max_version=8 <= 9, all rows recyclable)
 *   - SSTable2-Macro2: OP_NONE (max_version=10 > 9, reuse without opening)
 *
 * Expected Statistics:
 *   - macro_cnt[OP_FILTER]=2 (2 macros filtered at block level)
 *   - macro_cnt[OP_NONE]=2 (2 macros reused without opening)
 *   - micro_cnt[*]=0 (mini/minor sstable doesn't count micro stats)
 *   - filter_block_row_cnt=5 (rows filtered at block level: 2+3=5)
 *   - row_cnt[REMOVE]=0 (no row-level filtering needed)
 */
TEST_F(TestMultiVersionMergeRecycle, recycle_macro)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletExeMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);
  ObScnRange scn_range;

  ObTableHandleV2 handle1;
  const char *micro_data[2];
  micro_data[0] = // MACRO: OP_FILTER
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "0        var1  -6       0        NOP      1    EXIST   LF\n"
      "1        var1  -6       0        2        2    EXIST   CLF\n";

  micro_data[1] = // MACRO: OP_NONE
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "2        var1  -10       0        3       NOP   EXIST   LF\n";

  const int64_t snapshot_version = 10;
  PREPARE_SCN_RANGE(1, snapshot_version);
  prepare_table_schema(micro_data, schema_rowkey_cnt_, scn_range, snapshot_version);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[2];
  micro_data2[0] = // MACRO: OP_FILTER
      "bigint   var   bigint   bigint   bigint bigint  flag    multi_version_row_flag\n"
      "3        var3  -8       MIN        2     12    EXIST   SCF\n"
      "3        var3  -8       0          NOP   12    EXIST   N\n"
      "3        var3  -6       0          2     2    EXIST   CL\n";

  micro_data2[1] = // MACRO: OP_NONE
      "bigint   var   bigint   bigint   bigint bigint  flag    multi_version_row_flag\n"
      "5        var5  -10       0        NOP     13    EXIST   LF\n";

  const int64_t snapshot_version_2 = 30;
  PREPARE_SCN_RANGE(snapshot_version, snapshot_version_2);
  reset_writer(snapshot_version_2);
  prepare_one_macro(micro_data2, 1);
  prepare_one_macro(&micro_data2[1], 1);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 10;
  trans_version_range.base_version_ = 9;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint   bigint  bigint  flag    multi_version_row_flag\n"
      "2        var1  -10       0        3       NOP   EXIST   LF\n"
      "5        var5  -10       0        NOP     13    EXIST   LF\n";

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
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
  ObMockDirectReadIterator sstable_iter;
  ASSERT_EQ(OB_SUCCESS, sstable_iter.init(scanner, allocator_, full_read_info_));
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  const ObICompactionFilter::ObFilterStatistics &filter_statistics = merge_context.filter_ctx_.filter_statistics_;
  ASSERT_EQ(2, filter_statistics.macro_cnt_[ObBlockOp::OP_FILTER]);
  ASSERT_EQ(2, filter_statistics.macro_cnt_[ObBlockOp::OP_NONE]);
  ASSERT_EQ(5, filter_statistics.filter_block_row_cnt_);
  ASSERT_EQ(0, filter_statistics.row_cnt_[ObICompactionFilter::FILTER_RET_REMOVE]);
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

/**
 * TEST 2: recycle_after_reuse - Recycle Following Reuse
 *
 * Objective: Verify recycling macros that come after reusable macros
 *
 * Data Layout:
 *   SSTable1: Macro1(rowkey=0 v-15, rowkey=1 v-15,v-7 across 2 micros)
 *            + Macro2(rowkey=2 v-8)
 *   SSTable2: Macro1(rowkey=3 v-8,v-6,v-4 across 2 micros)
 *            + Macro2(rowkey=5 v-10)
 *
 * Recycle Version: base_version=9
 *
 * Expected Macro Operations:
 *   - SSTable1-Macro1: OP_OPEN (rowkey=1 has v-7<=9, need row-level filtering)
 *   - SSTable1-Macro2: OP_FILTER (max_version=8 <= 9)
 *   - SSTable2-Macro1: OP_FILTER (max_version=8 <= 9)
 *   - SSTable2-Macro2: OP_NONE (max_version=10 > 9)
 *
 * Expected Statistics:
 *   - macro_cnt[OP_FILTER]=2, macro_cnt[OP_NONE]=1
 *   - micro_cnt[*]=0 (not counted in mini/minor sstable)
 *   - filter_block_row_cnt=3 (SSTable1-Macro2 + SSTable2-Macro1)
 *   - row_cnt[REMOVE]=2 (rowkey=2 v-8 + partial rows of rowkey=1)
 */
TEST_F(TestMultiVersionMergeRecycle, recycle_after_reuse)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletExeMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);
  ObScnRange scn_range;

  ObTableHandleV2 handle1;
  const char *micro_data[2];
  micro_data[0] = // MACRO: OP_NONE
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "0        var0  -15       0        NOP      1    EXIST   LF\n"
      "1        var1  -15       MIN      12       2    EXIST  SCF\n"
      "1        var1  -15       0        12       NOP  EXIST  N\n";

  micro_data[1] = // MACRO: OP_OPEN
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "1        var1  -7        0        2       2    EXIST   CL\n"
      "2        var2  -8        0        3       NOP   EXIST   LF\n";

  const int64_t snapshot_version = 20;
  PREPARE_SCN_RANGE(1, snapshot_version);
  prepare_table_schema(micro_data, schema_rowkey_cnt_, scn_range, snapshot_version);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[2];
  micro_data2[0] = // MACRO: OP_FILTER
      "bigint   var   bigint   bigint   bigint bigint  flag    multi_version_row_flag\n"
      "3        var3  -8       MIN        12     12    EXIST   SCF\n"
      "3        var3  -8       0          NOP   12    EXIST   N\n"
      "3        var3  -6       0          12    NOP    EXIST  N\n";

  micro_data2[1] = // MACRO: OP_OPEN
      "bigint   var   bigint   bigint   bigint bigint  flag    multi_version_row_flag\n"
      "3        var3  -4       0          2    2      EXIST  CL\n"
      "5        var5  -10      0        NOP    13    EXIST   LF\n";

  const int64_t snapshot_version_2 = 30;
  PREPARE_SCN_RANGE(snapshot_version, snapshot_version_2);
  reset_writer(snapshot_version_2);
  prepare_one_macro(micro_data2, 1);
  prepare_one_macro(&micro_data2[1], 1);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 10;
  trans_version_range.base_version_ = 9;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint   bigint  bigint  flag    multi_version_row_flag\n"
      "0        var0  -15       0        NOP      1     EXIST   LF\n"
      "1        var1  -15       MIN      12       2     EXIST   SCF\n"
      "1        var1  -15       0        12       NOP   EXIST   N\n"
      "1        var1  -7        0        2        2     EXIST   CL\n"
      "5        var5  -10       0        NOP      13    EXIST   LF\n";

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
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
  ObMockDirectReadIterator sstable_iter;
  ASSERT_EQ(OB_SUCCESS, sstable_iter.init(scanner, allocator_, full_read_info_));
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  const ObICompactionFilter::ObFilterStatistics &filter_statistics = merge_context.filter_ctx_.filter_statistics_;
  ASSERT_EQ(1, filter_statistics.macro_cnt_[ObBlockOp::OP_FILTER]);
  ASSERT_EQ(1, filter_statistics.macro_cnt_[ObBlockOp::OP_NONE]);
  ASSERT_EQ(2, filter_statistics.macro_cnt_[ObBlockOp::OP_OPEN]);
  ASSERT_EQ(3, filter_statistics.filter_block_row_cnt_);
  ASSERT_EQ(2, filter_statistics.row_cnt_[ObICompactionFilter::FILTER_RET_REMOVE]);
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

/**
 * TEST 3: reuse_after_recycle - Reuse Following Recycle
 *
 * Objective: Verify reusing macros that come after recyclable macros
 *
 * Data Layout:
 *   SSTable1: Macro1(rowkey=0 v-8, rowkey=1 v-8,v-7 across 2 micros)
 *            + Macro2(rowkey=2 v-15)
 *   SSTable2: Macro1(rowkey=3 v-15,v-8)
 *            + Macro2(rowkey=4 v-4, rowkey=5 v-6)
 *
 * Recycle Version: base_version=9
 *
 * Expected Macro Operations:
 *   - SSTable1-Macro1: OP_FILTER (max_version=8 <= 9)
 *   - SSTable1-Macro2: OP_NONE (max_version=15 > 9)
 *   - SSTable2-Macro1: OP_FILTER (although has v-15, v-8<=9 rows need filtering)
 *   - SSTable2-Macro2: OP_FILTER (max_version=6 <= 9)
 *
 * Expected Statistics:
 *   - macro_cnt[OP_FILTER]=2, macro_cnt[OP_NONE]=1
 *   - micro_cnt[*]=0 (not counted in mini/minor sstable)
 *   - filter_block_row_cnt=1 (SSTable2-Macro2 with 2 rows)
 *   - row_cnt[REMOVE]=6 (SSTable1-Macro1: 4 rows + SSTable2-Macro1: 2 rows)
 */
TEST_F(TestMultiVersionMergeRecycle, reuse_after_recycle)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletExeMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);
  ObScnRange scn_range;

  ObTableHandleV2 handle1;
  const char *micro_data[2];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "0        var0  -8       0        NOP      1    EXIST   LF\n"
      "1        var1  -8       MIN      12       2    EXIST   SCF\n"
      "1        var1  -8       0        12       NOP  EXIST   N\n";

  micro_data[1] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "1        var1  -7        0        2       2     EXIST   CL\n"
      "2        var2  -15       0        3       NOP   EXIST   LF\n";

  const int64_t snapshot_version = 20;
  PREPARE_SCN_RANGE(1, snapshot_version);
  prepare_table_schema(micro_data, schema_rowkey_cnt_, scn_range, snapshot_version);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[2];
  micro_data2[0] =
      "bigint   var   bigint   bigint   bigint bigint  flag    multi_version_row_flag\n"
      "3        var3  -15      MIN       12    12     EXIST   SCF\n"
      "3        var3  -15      0         NOP   12     EXIST   N\n"
      "3        var3  -8       0         12    NOP    EXIST   L\n";

  micro_data2[1] =
      "bigint   var   bigint   bigint   bigint bigint  flag    multi_version_row_flag\n"
      "4        var4  -4       0         2      2     EXIST   CLF\n"
      "5        var5  -6       0         NOP    13    EXIST   LF\n";

  const int64_t snapshot_version_2 = 30;
  PREPARE_SCN_RANGE(snapshot_version, snapshot_version_2);
  reset_writer(snapshot_version_2);
  prepare_one_macro(micro_data2, 1);
  prepare_one_macro(&micro_data2[1], 1);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 10;
  trans_version_range.base_version_ = 9;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint   bigint  bigint  flag    multi_version_row_flag\n"
      "2        var2  -15        0        3       NOP   EXIST   LF\n"
      "3        var3  -15       MIN        12     12    EXIST   SCF\n"
      "3        var3  -15       0          NOP   12    EXIST   N\n"
      "3        var3  -8       0          12    NOP    EXIST  L\n";

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
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
  ObMockDirectReadIterator sstable_iter;
  ASSERT_EQ(OB_SUCCESS, sstable_iter.init(scanner, allocator_, full_read_info_));
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  const ObICompactionFilter::ObFilterStatistics &filter_statistics = merge_context.filter_ctx_.filter_statistics_;
  ASSERT_EQ(2, filter_statistics.macro_cnt_[ObBlockOp::OP_FILTER]);
  ASSERT_EQ(0, filter_statistics.macro_cnt_[ObBlockOp::OP_NONE]);
  ASSERT_EQ(5, filter_statistics.filter_block_row_cnt_);
  ASSERT_EQ(1, filter_statistics.row_cnt_[ObICompactionFilter::FILTER_RET_REMOVE]);
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

/**
 * TEST 4: recycled_micros_after_reuse - Micro-level Recycling After Reuse
 *
 * Objective: Verify partial micro filtering within a reusable macro block
 *
 * Data Layout:
 *   SSTable1: Macro1-Micro1(rowkey=0 v-15, rowkey=1 v-15,v-10)
 *            Macro1-Micro2+3(rowkey=2 v-8, rowkey=3 v-8,v-6, rowkey=4 v-15, rowkey=5 v-15)
 *   SSTable2: Macro1(rowkey=5 v-20)
 *
 * Recycle Version: base_version=9
 *
 * Expected Macro Operations:
 *   - SSTable1-Macro1: OP_OPEN (mixed: some rows reusable v>9, some recyclable v<=9)
 *   - SSTable2-Macro1: OP_OPEN (needs merge with rowkey=5 from SSTable1)
 *
 * Micro-level Operations (if supported):
 *   - Micro1: reusable (all versions > 9)
 *   - Micro2: partially recyclable (rowkey=2,3 v<=9)
 *   - Micro3: reusable (rowkey=4,5 v>9)
 *
 * Expected Statistics:
 *   - macro_cnt[OP_FILTER]=0, macro_cnt[OP_NONE]=1 (Micro1 can be reused)
 *   - micro_cnt[*]=0 (not counted in mini/minor sstable)
 *   - filter_block_row_cnt=3 (rowkey=2: 1 row + rowkey=3: 2 rows at micro level)
 *   - row_cnt[REMOVE]=1 (row-level filtering for partial rowkey)
 */
TEST_F(TestMultiVersionMergeRecycle, recycled_micros_after_reuse)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletExeMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);
  ObScnRange scn_range;

  ObTableHandleV2 handle1;
  const char *micro_data[3];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "0        var0  -15       0        NOP      1    EXIST   LF\n"
      "1        var1  -15       MIN      12       2    EXIST  SCF\n"
      "1        var1  -15       0        12       NOP    EXIST  N\n"
      "1        var1  -10       0        2         2    EXIST  CL\n";

  micro_data[1] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "2        var2  -8        0        3       NOP   EXIST   LF\n"
      "3        var3  -8        MIN      12      12   EXIST   SCF\n"
      "3        var3  -8        0       12      NOP   EXIST   N\n";

  micro_data[2] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "3        var3  -6        0      NOP      12   EXIST   L\n"
      "4        var4  -15       0      2        2    EXIST   CLF\n"
      "5        var5  -15       0      NOP      12   EXIST   LF\n";

  const int64_t snapshot_version = 20;
  PREPARE_SCN_RANGE(1, snapshot_version);
  prepare_table_schema(micro_data, schema_rowkey_cnt_, scn_range, snapshot_version);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 2);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint   bigint   bigint bigint  flag    multi_version_row_flag\n"
      "5        var5  -20      0        NOP    13    EXIST   LF\n";

  const int64_t snapshot_version_2 = 30;
  PREPARE_SCN_RANGE(snapshot_version, snapshot_version_2);
  reset_writer(snapshot_version_2);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 10;
  trans_version_range.base_version_ = 9;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint   bigint  bigint  flag    multi_version_row_flag\n"
      "0        var0  -15       0        NOP      1    EXIST   LF\n"
      "1        var1  -15       MIN      12       2    EXIST  SCF\n"
      "1        var1  -15       0        12       NOP    EXIST  N\n"
      "1        var1  -10       0        2         2    EXIST  CL\n"
      "4        var4  -15       0      2        2    EXIST   CLF\n"
      "5        var5  -20       MIN      NOP      13   EXIST   SF\n"
      "5        var5  -20       0      NOP      13   EXIST   N\n"
      "5        var5  -15       0      NOP      12   EXIST   L\n";

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
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
  ObMockDirectReadIterator sstable_iter;
  ASSERT_EQ(OB_SUCCESS, sstable_iter.init(scanner, allocator_, full_read_info_));
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  const ObICompactionFilter::ObFilterStatistics &filter_statistics = merge_context.filter_ctx_.filter_statistics_;
  ASSERT_EQ(0, filter_statistics.macro_cnt_[ObBlockOp::OP_FILTER]);
  ASSERT_EQ(1, filter_statistics.macro_cnt_[ObBlockOp::OP_NONE]);
  ASSERT_EQ(0, filter_statistics.filter_block_row_cnt_);
  ASSERT_EQ(4, filter_statistics.row_cnt_[ObICompactionFilter::FILTER_RET_REMOVE]);
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

/**
 * TEST 5: rowkeys_across_micros - F-Flag Recycling Across Micros
 *
 * Objective: Verify F-flag row recycling rule when rowkey spans multiple micros
 *
 * Data Layout (all in one macro):
 *   Micro1: rowkey=0 v-10, rowkey=1 v-10,v-8
 *   Micro2: rowkey=1 v-7,v-6 (continues from Micro1)
 *   Micro3: rowkey=1 v-5 (continues), rowkey=2 v-15
 *   Micro4: rowkey=2 v-10,v-8 (continues from Micro3)
 *   Micro5: rowkey=2 v-6 (continues), rowkey=3 v-15, rowkey=5 v-15
 *   SSTable2: rowkey=5 v-20
 *
 * Recycle Version: base_version=12
 *
 * F-Flag Recycling Rule Applied:
 *   - rowkey=0: max_version=10 <= 12, all rows recycled
 *   - rowkey=1: max_version=10 <= 12, ALL rows across 3 micros recycled (F-flag rule)
 *   - rowkey=2: max_version=15 > 12, kept (compacted to v-15,v-10)
 *   - rowkey=3: v-15 > 12, kept
 *   - rowkey=5: merged with SSTable2, kept
 *
 * Expected Statistics:
 *   - macro_cnt[OP_FILTER]=0, macro_cnt[OP_NONE]=0 (macro must be opened)
 *   - micro_cnt[*]=0 (not counted in mini/minor sstable)
 *   - filter_block_row_cnt=3 (rowkey=0: 1 + rowkey=3 micro rows)
 *   - row_cnt[REMOVE]=12 (rowkey=0: 1 + rowkey=1: 7 + rowkey=2 partial: 4)
 */
TEST_F(TestMultiVersionMergeRecycle, rowkeys_across_micros)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletExeMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);
  ObScnRange scn_range;

  ObTableHandleV2 handle1;
  const char *micro_data[5];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "0        var0  -10       0        NOP      1    EXIST   LF\n"
      "1        var1  -10       MIN      12       2    EXIST  SCF\n"
      "1        var1  -10       0        12       NOP    EXIST  N\n"
      "1        var1  -8        0        2         2    EXIST  C\n";

  micro_data[1] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "1        var1  -7        0        1         2    EXIST  C\n"
      "1        var1  -6        0        2         1    EXIST  C\n";

  micro_data[2] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "1        var1  -5        0        1         1    EXIST  CL\n"
      "2        var2  -15       MIN      12      12   EXIST   SCF\n"
      "2        var2  -15       0      NOP     12    EXIST   N\n";

  micro_data[3] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "2        var2  -10       0      12      NOP   EXIST   N\n"
      "2        var2  -8        0      2       2     EXIST   N\n";

  micro_data[4] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "2        var2  -6        0      1       1     EXIST   CL\n"
      "3        var3  -15       0      2       2     EXIST   CLF\n"
      "5        var5  -15      0      2        2    EXIST   CLF\n";


  const int64_t snapshot_version = 20;
  PREPARE_SCN_RANGE(1, snapshot_version);
  prepare_table_schema(micro_data, schema_rowkey_cnt_, scn_range, snapshot_version);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 5);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint   bigint   bigint bigint  flag    multi_version_row_flag\n"
      "5        var5  -20      0        NOP    13    EXIST   LF\n";

  const int64_t snapshot_version_2 = 30;
  PREPARE_SCN_RANGE(snapshot_version, snapshot_version_2);
  reset_writer(snapshot_version_2);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 14;
  trans_version_range.base_version_ = 12;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint   bigint  bigint  flag    multi_version_row_flag\n"
      "2        var2  -15       MIN      12      12   EXIST   SCF\n"
      "2        var2  -15       0      NOP     12    EXIST   N\n"
      "2        var2  -10       0      12      2   EXIST     CL\n"
      "3        var3  -15       0      2       2   EXIST   CLF\n"
      "5        var5  -20       MIN     2     13   EXIST   SCF\n"
      "5        var5  -20       0      NOP     13   EXIST   N\n"
      "5        var5  -15       0      2     2   EXIST   CL\n";

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
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
  ObMockDirectReadIterator sstable_iter;
  ASSERT_EQ(OB_SUCCESS, sstable_iter.init(scanner, allocator_, full_read_info_));
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  const ObICompactionFilter::ObFilterStatistics &filter_statistics = merge_context.filter_ctx_.filter_statistics_;
  ASSERT_EQ(0, filter_statistics.macro_cnt_[ObBlockOp::OP_FILTER]);
  ASSERT_EQ(0, filter_statistics.macro_cnt_[ObBlockOp::OP_NONE]);
  ASSERT_EQ(0, filter_statistics.filter_block_row_cnt_);
  ASSERT_EQ(7, filter_statistics.row_cnt_[ObICompactionFilter::FILTER_RET_REMOVE]);
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

/**
 * TEST 6: rowkeys_across_macro - F-Flag Recycling Across Macros
 *
 * Objective: Verify F-flag row recycling rule when rowkey spans multiple macro blocks
 *
 * Data Layout (5 separate macros):
 *   Macro1: rowkey=0 v-10, rowkey=1 v-15,v-8
 *   Macro2: rowkey=1 v-7,v-6 (continues from Macro1)
 *   Macro3: rowkey=1 v-5 (continues), rowkey=2 v-15
 *   Macro4: rowkey=2 v-10,v-8 (continues from Macro3)
 *   Macro5: rowkey=2 v-6 (continues), rowkey=3 v-15, rowkey=5 v-15
 *   SSTable2-Macro1: rowkey=5 v-20
 *
 * Recycle Version: base_version=12
 *
 * F-Flag Recycling Rule Applied Across Macros:
 *   - rowkey=0: max_version=10 <= 12, Macro1 can be filtered
 *   - rowkey=1: max_version=15 > 12, kept and compacted (v-15,v-8)
 *   - rowkey=2: max_version=15 > 12, kept and compacted (v-15,v-10)
 *   - rowkey=3: v-15 > 12, kept
 *   - rowkey=5: merged with SSTable2, kept
 *
 * Expected Statistics:
 *   - macro_cnt[OP_FILTER]=1 (Macro1 with rowkey=0), macro_cnt[OP_NONE]=0
 *   - micro_cnt[*]=0 (not counted in mini/minor sstable)
 *   - filter_block_row_cnt=1 (rowkey=0 macro filtered)
 *   - row_cnt[REMOVE]=10 (rowkey=1,2 partial versions filtered at row level)
 */
TEST_F(TestMultiVersionMergeRecycle, rowkeys_across_macro)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletExeMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);
  ObScnRange scn_range;

  ObTableHandleV2 handle1;
  const char *micro_data[5];
  micro_data[0] = // MACRO: OP_NONE
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "0        var0  -10       0        NOP      1    EXIST   LF\n"
      "1        var1  -15       MIN      12       2    EXIST  SCF\n"
      "1        var1  -15       0        12       NOP    EXIST  N\n"
      "1        var1  -8        0        2         2    EXIST  C\n";

  micro_data[1] = // MACRO: OP_OPEN
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "1        var1  -7        0        1         2    EXIST  C\n"
      "1        var1  -6        0        2         1    EXIST  C\n";

  micro_data[2] = // MACRO: OP_NONE
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "1        var1  -5        0        1         1    EXIST  CL\n"
      "2        var2  -15       MIN      12      12   EXIST   SCF\n"
      "2        var2  -15       0      NOP     12    EXIST   N\n";

  micro_data[3] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "2        var2  -10       0      12      NOP   EXIST   N\n"
      "2        var2  -8        0      2       2     EXIST   N\n";

  micro_data[4] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "2        var2  -6        0      1       1     EXIST   CL\n"
      "3        var3  -15       0      2       2     EXIST   CLF\n"
      "5        var5  -15      0      2        2    EXIST   CLF\n";

  int64_t snapshot_version = 20;
  PREPARE_SCN_RANGE(1, snapshot_version);
  prepare_table_schema(micro_data, schema_rowkey_cnt_, scn_range, snapshot_version);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1);
  prepare_one_macro(&micro_data[2], 1);
  prepare_one_macro(&micro_data[3], 1);
  prepare_one_macro(&micro_data[4], 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] = // MACRO: OP_NONE
      "bigint   var   bigint   bigint   bigint bigint  flag    multi_version_row_flag\n"
      "5        var5  -20      0        NOP    13    EXIST   LF\n";

  const int64_t snapshot_version_2 = 30;
  PREPARE_SCN_RANGE(snapshot_version, snapshot_version_2);
  reset_writer(snapshot_version_2);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 14;
  trans_version_range.base_version_ = 12;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint   bigint  bigint  flag    multi_version_row_flag\n"
      "1        var1  -15       MIN      12       2    EXIST  SCF\n"
      "1        var1  -15       0        12       NOP    EXIST  N\n"
      "1        var1  -8        0        2         2    EXIST  CL\n"
      "2        var2  -15       MIN      12      12   EXIST   SCF\n"
      "2        var2  -15       0      NOP     12    EXIST   N\n"
      "2        var2  -10       0      12      2   EXIST     CL\n"
      "3        var3  -15       0      2       2   EXIST   CLF\n"
      "5        var5  -20       MIN     2     13   EXIST   SCF\n"
      "5        var5  -20       0      NOP     13   EXIST   N\n"
      "5        var5  -15       0      2     2   EXIST   CL\n";

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
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
  ObMockDirectReadIterator sstable_iter;
  ASSERT_EQ(OB_SUCCESS, sstable_iter.init(scanner, allocator_, full_read_info_));
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  const ObICompactionFilter::ObFilterStatistics &filter_statistics = merge_context.filter_ctx_.filter_statistics_;
  ASSERT_EQ(0, filter_statistics.macro_cnt_[ObBlockOp::OP_FILTER]);
  ASSERT_EQ(0, filter_statistics.macro_cnt_[ObBlockOp::OP_NONE]);
  ASSERT_EQ(0, filter_statistics.filter_block_row_cnt_);
  ASSERT_EQ(1, filter_statistics.row_cnt_[ObICompactionFilter::FILTER_RET_REMOVE]);
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

/**
 * TEST 7: recycle_macro_with_last_row - Recycle Macro with Last Rowkey Spanning Micros
 *
 * Objective: Verify recycling when last rowkey in a macro spans multiple micros (boundary case)
 *
 * Data Layout:
 *   Macro1-Micro1: rowkey=0 v-10, rowkey=1 v-10,v-8
 *   Macro1-Micro2+3: rowkey=2 v-10,v-9,v-8 (spans 2 micros), rowkey=3 v-15, rowkey=5 v-15
 *   SSTable2: rowkey=5 v-20
 *
 * Recycle Version: base_version=12
 *
 * F-Flag Recycling for Last Rowkey:
 *   - rowkey=0: max_version=10 <= 12, filtered
 *   - rowkey=1: max_version=10 <= 12, filtered
 *   - rowkey=2: max_version=10 <= 12, ALL rows across micros recycled (F-flag rule)
 *   - rowkey=3: v-15 > 12, kept
 *   - rowkey=5: merged with SSTable2, kept
 *
 * Special Attention:
 *   This tests the boundary case where the last rowkey in a macro spans micros
 *   and needs complete recycling per F-flag rule.
 *
 * Expected Statistics:
 *   - macro_cnt[OP_FILTER]=1 (Macro1-Micro1), macro_cnt[OP_NONE]=0
 *   - micro_cnt[*]=0 (not counted in mini/minor sstable)
 *   - filter_block_row_cnt=4 (rowkey=0: 1 + rowkey=1: 3 at macro level)
 *   - row_cnt[REMOVE]=4 (rowkey=2: 4 rows across micros at row level)
 */
TEST_F(TestMultiVersionMergeRecycle, recycle_macro_with_last_row)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletExeMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);
  ObScnRange scn_range;

  ObTableHandleV2 handle1;
  const char *micro_data[3];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "0        var0  -10       0        NOP      1    EXIST   LF\n"
      "1        var1  -10       MIN      12       2    EXIST  SCF\n"
      "1        var1  -10       0        12       NOP    EXIST  N\n"
      "1        var1  -8        0        2         2    EXIST  CL\n";

  micro_data[1] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "2        var2  -10       MIN      12      12   EXIST   SCF\n"
      "2        var2  -10       0      NOP     12    EXIST   N\n";

  micro_data[2] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "2        var2  -9        0      12     NOP     EXIST   N\n"
      "2        var2  -8        0      2       2     EXIST   CL\n"
      "3        var3  -15       0      2       2     EXIST   CLF\n"
      "5        var5  -15      0      2        2    EXIST   CLF\n";


  int64_t snapshot_version = 20;
  PREPARE_SCN_RANGE(1, snapshot_version);
  prepare_table_schema(micro_data, schema_rowkey_cnt_, scn_range, snapshot_version);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 2);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint   bigint   bigint bigint  flag    multi_version_row_flag\n"
      "5        var5  -20      0        NOP    13    EXIST   LF\n";

  const int64_t snapshot_version_2 = 30;
  PREPARE_SCN_RANGE(snapshot_version, snapshot_version_2);
  reset_writer(snapshot_version_2);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 14;
  trans_version_range.base_version_ = 12;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint   bigint  bigint  flag    multi_version_row_flag\n"
      "3        var3  -15       0      2       2   EXIST   CLF\n"
      "5        var5  -20       MIN     2     13   EXIST   SCF\n"
      "5        var5  -20       0      NOP     13   EXIST   N\n"
      "5        var5  -15       0      2     2   EXIST   CL\n";

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
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
  ObMockDirectReadIterator sstable_iter;
  ASSERT_EQ(OB_SUCCESS, sstable_iter.init(scanner, allocator_, full_read_info_));
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  const ObICompactionFilter::ObFilterStatistics &filter_statistics = merge_context.filter_ctx_.filter_statistics_;
  ASSERT_EQ(1, filter_statistics.macro_cnt_[ObBlockOp::OP_FILTER]);
  ASSERT_EQ(0, filter_statistics.macro_cnt_[ObBlockOp::OP_NONE]);
  ASSERT_EQ(4, filter_statistics.filter_block_row_cnt_);
  ASSERT_EQ(4, filter_statistics.row_cnt_[ObICompactionFilter::FILTER_RET_REMOVE]);
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

/**
 * TEST 8: reuse_after_recycle_with_last - Reuse After Recycle with Uncommitted Transactions
 *
 * Objective: Verify merging last reusable rowkey with new data including uncommitted transactions
 *
 * Data Layout:
 *   SSTable1-Macro1: rowkey=0 v-10, rowkey=1 v-10,v-8
 *   SSTable1-Macro2: rowkey=2 v-15
 *   SSTable1-Macro3: rowkey=3 v-15
 *   SSTable1-Macro4: rowkey=5 v-15,v-12 (L-flag, last version)
 *   SSTable2: rowkey=5 uncommitted(v-10,v-5) + committed(v-21,v-20)
 *
 * Recycle Version: base_version=12
 *
 * Merge with Uncommitted Transactions:
 *   - rowkey=0,1: max_version=10 <= 12, filtered at macro level
 *   - rowkey=2: v-15 > 12, reused (OP_NONE)
 *   - rowkey=3: v-15 > 12, reused (OP_NONE)
 *   - rowkey=5: needs merge between SSTable1 (v-15,v-12) and SSTable2
 *     * Uncommitted trans resolved to v-25
 *     * Final: v-25,v-21,v-20,v-15 (compact to v-25,v-21,v-20,v-15 per multi-version start=16)
 *
 * Expected Statistics:
 *   - macro_cnt[OP_FILTER]=1, macro_cnt[OP_NONE]=2 (rowkey=2,3 reused)
 *   - micro_cnt[*]=0 (not counted in mini/minor sstable)
 *   - filter_block_row_cnt=4 (rowkey=0,1 filtered at macro level)
 *   - row_cnt[REMOVE]=3 (partial versions of rowkey=5 before base_version)
 */
TEST_F(TestMultiVersionMergeRecycle, reuse_after_recycle_with_last)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletExeMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);
  ObScnRange scn_range;

  ObTableHandleV2 handle1;
  const char *micro_data[4];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "0        var0  -10       0        NOP      1    EXIST   LF\n"
      "1        var1  -10       MIN      12       2    EXIST  SCF\n"
      "1        var1  -10       0        12       NOP    EXIST  N\n"
      "1        var1  -8        0        2         2    EXIST  CL\n";

  micro_data[1] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "2        var2  -15       0        2     2    EXIST   CLF\n";

  micro_data[2] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "3        var3  -15       0      2       2     EXIST   CLF\n";

  micro_data[3] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "5        var5  -15      MIN    2        3    EXIST   SCF\n"
      "5        var5  -15      0      NOP      3    EXIST   N\n"
      "5        var5  -12      0      2        NOP  EXIST   L\n";

  int64_t snapshot_version = 20;
  PREPARE_SCN_RANGE(1, snapshot_version);
  prepare_table_schema(micro_data, schema_rowkey_cnt_, scn_range, snapshot_version);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1);
  prepare_one_macro(&micro_data[2], 1);
  prepare_one_macro(&micro_data[3], 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint   bigint   bigint bigint  flag    multi_version_row_flag  trans_id\n"
      "5        var5  MIN      -10      NOP    25    EXIST   FU  trans_id_1\n"
      "5        var5  MIN      -5       NOP    24    EXIST   U   trans_id_1\n"
      "5        var5  -21      MIN      NOP    21    EXIST   SC  trans_id_0\n"
      "5        var5  -21      0        NOP    21    EXIST   N   trans_id_0\n"
      "5        var5  -20      0        NOP    20    EXIST   L   trans_id_0\n";

  const int64_t snapshot_version_2 = 30;
  PREPARE_SCN_RANGE(snapshot_version, snapshot_version_2);
  reset_writer(snapshot_version_2);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  insert_tx_data(1/*tx_id*/, 25/*commit_version*/);

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 16;
  trans_version_range.base_version_ = 12;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint   bigint  bigint  flag    multi_version_row_flag\n"
      "2        var2  -15       0       2       2    EXIST   CLF\n"
      "3        var3  -15       0       2       2    EXIST   CLF\n"
      "5        var5  -25       MIN     2       25   EXIST   SCF\n"
      "5        var5  -25       0       NOP     25   EXIST   N\n"
      "5        var5  -21       0       NOP     21   EXIST   N\n"
      "5        var5  -20       0       NOP     20   EXIST   N\n"
      "5        var5  -15       0       2       3    EXIST   CL\n";

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
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
  ObMockDirectReadIterator sstable_iter;
  ASSERT_EQ(OB_SUCCESS, sstable_iter.init(scanner, allocator_, full_read_info_));
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  const ObICompactionFilter::ObFilterStatistics &filter_statistics = merge_context.filter_ctx_.filter_statistics_;
  ASSERT_EQ(1, filter_statistics.macro_cnt_[ObBlockOp::OP_FILTER]);
  ASSERT_EQ(2, filter_statistics.macro_cnt_[ObBlockOp::OP_NONE]);
  ASSERT_EQ(4, filter_statistics.filter_block_row_cnt_);
  ASSERT_EQ(0, filter_statistics.row_cnt_[ObICompactionFilter::FILTER_RET_REMOVE]);
  clear_tx_data();
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

/**
 * TEST 9: base_version_boundary - Base Version Boundary Value Test
 *
 * Objective: Verify recycling logic when max_version equals base_version
 *
 * Data Layout:
 *   SSTable1:
 *     Macro1: rowkey=0 v-10,v-9 (F-flag, max_version = base_version)
 *     Macro2: rowkey=1 v-11,v-10 (F-flag, max_version > base_version)
 *     Macro3: rowkey=2 v-9,v-8 (F-flag, max_version < base_version)
 *
 * Recycle Version: base_version=10
 *
 * Expected Behavior:
 *   - rowkey=0: max_version=10 <= 10, ALL versions recycled (F-flag rule)
 *   - rowkey=1: max_version=11 > 10, kept (both v-11 and v-10)
 *   - rowkey=2: max_version=9 <= 10, ALL versions recycled (F-flag rule)
 *
 * Expected Statistics:
 *   - macro_cnt[OP_FILTER]=2 (Macro1 and Macro3)
 *   - macro_cnt[OP_NONE]=1 (Macro2)
 *   - filter_block_row_cnt=6 (rowkey=0: 3 rows + rowkey=2: 3 rows)
 *   - row_cnt[REMOVE]=0 (all filtered at block level)
 */
TEST_F(TestMultiVersionMergeRecycle, base_version_boundary)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletExeMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);
  ObScnRange scn_range;

  ObTableHandleV2 handle1;
  const char *micro_data[3];
  micro_data[0] = // MACRO: OP_FILTER (max_version=10 <= 10)
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "0        var0  -10      MIN      2       1    EXIST   SCF\n"
      "0        var0  -10      0        2       1    EXIST   N\n"
      "0        var0  -9       0        NOP     2    EXIST   L\n";

  micro_data[1] = // MACRO: OP_NONE (max_version=11 > 10)
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "1        var1  -11      MIN      3       1    EXIST   SCF\n"
      "1        var1  -11      0        3       1    EXIST   N\n"
      "1        var1  -10      0        NOP     2    EXIST   L\n";

  micro_data[2] = // MACRO: OP_FILTER (max_version=9 <= 10)
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "2        var2  -9       MIN      4       1    EXIST   SCF\n"
      "2        var2  -9       0        4       1    EXIST   N\n"
      "2        var2  -8       0        NOP     2    EXIST   L\n";

  const int64_t snapshot_version = 20;
  PREPARE_SCN_RANGE(1, snapshot_version);
  prepare_table_schema(micro_data, schema_rowkey_cnt_, scn_range, snapshot_version);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1);
  prepare_one_macro(&micro_data[2], 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  // SSTable2: Add an empty sstable to satisfy minor merge requirement (>= 2 sstables)
  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] = // MACRO: OP_NONE
      "bigint   var   bigint   bigint   bigint bigint  flag    multi_version_row_flag\n"
      "4        var4  -20      0        5       5     EXIST   CLF\n";

  const int64_t snapshot_version2 = 25;
  PREPARE_SCN_RANGE(snapshot_version, snapshot_version2);
  reset_writer(snapshot_version2);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 11;
  trans_version_range.base_version_ = 10;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint   bigint  bigint  flag    multi_version_row_flag\n"
      "1        var1  -11      0        3       1    EXIST   CLF\n"
      "4        var4  -20      0        5       5    EXIST   CLF\n";

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
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
  ObMockDirectReadIterator sstable_iter;
  ASSERT_EQ(OB_SUCCESS, sstable_iter.init(scanner, allocator_, full_read_info_));
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  const ObICompactionFilter::ObFilterStatistics &filter_statistics = merge_context.filter_ctx_.filter_statistics_;
  ASSERT_EQ(2, filter_statistics.macro_cnt_[ObBlockOp::OP_FILTER]);
  ASSERT_EQ(1, filter_statistics.macro_cnt_[ObBlockOp::OP_OPEN]);
  ASSERT_EQ(6, filter_statistics.filter_block_row_cnt_);
  ASSERT_EQ(0, filter_statistics.row_cnt_[ObICompactionFilter::FILTER_RET_REMOVE]);
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

/**
 * TEST 10: last_flag_across_blocks - L-Flag (Non-F-Flag) Across Blocks
 *
 * Objective: Verify L-flag rows do NOT trigger full rowkey recycling
 *
 * Data Layout:
 *   Macro1-Micro1: rowkey=0 v-15,v-12 (L-flag, continues across blocks)
 *   Macro1-Micro2: rowkey=0 v-10,v-8 (continues)
 *   Macro2: rowkey=0 v-6 (CL-flag end), rowkey=1 v-20
 *
 * Recycle Version: base_version=11
 *
 * Expected Behavior:
 *   - rowkey=0: max_version=15 > 11, but v-10,v-8,v-6 <= 11
 *   - NOT F-flag, so only recycle versions <= base_version
 *   - Keep: v-15, v-12; Recycle: v-10, v-8, v-6
 *   - rowkey=1: v-20 > 11, kept
 *
 * Expected Statistics:
 *   - macro_cnt[OP_FILTER]=0, macro_cnt[OP_NONE]=0 (must open for partial recycling)
 *   - macro_cnt[OP_OPEN]=2
 *   - filter_block_row_cnt=0 (no block-level filtering)
 *   - row_cnt[REMOVE]=3 (v-10, v-8, v-6)
 */
TEST_F(TestMultiVersionMergeRecycle, last_flag_across_blocks)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletExeMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);
  ObScnRange scn_range;

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
  PREPARE_SCN_RANGE(1, snapshot_version);
  prepare_table_schema(micro_data, schema_rowkey_cnt_, scn_range, snapshot_version);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 2);
  prepare_one_macro(&micro_data[2], 1);
  prepare_one_macro(&micro_data[3], 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint   bigint   bigint bigint  flag    multi_version_row_flag\n"
      "2        var2  -20      0        NOP     6     EXIST   L\n";

  const int64_t snapshot_version2 = 30;
  PREPARE_SCN_RANGE(snapshot_version, snapshot_version2);
  reset_writer(snapshot_version2);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 12;
  trans_version_range.base_version_ = 11;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint   bigint  bigint  flag    multi_version_row_flag\n"
      "0        var0  -15      MIN      12      1    EXIST   SCF\n"
      "0        var0  -15      0        12      NOP  EXIST   N\n"
      "0        var0  -13      0        2       2    EXIST   N\n"
      "0        var0  -10      0        1       1    EXIST   CL\n"
      "2        var2  -20      MIN      3       6    EXIST   SCF\n"
      "2        var2  -20      0        NOP     6    EXIST   N\n"
      "2        var2  -12      0        3       3    EXIST   CL\n";

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
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
  ObMockDirectReadIterator sstable_iter;
  ASSERT_EQ(OB_SUCCESS, sstable_iter.init(scanner, allocator_, full_read_info_));
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  const ObICompactionFilter::ObFilterStatistics &filter_statistics = merge_context.filter_ctx_.filter_statistics_;
  ASSERT_EQ(0, filter_statistics.macro_cnt_[ObBlockOp::OP_FILTER]);
  ASSERT_EQ(0, filter_statistics.macro_cnt_[ObBlockOp::OP_NONE]);
  ASSERT_EQ(2, filter_statistics.macro_cnt_[ObBlockOp::OP_OPEN]);
  ASSERT_EQ(4, filter_statistics.row_cnt_[ObICompactionFilter::FILTER_RET_REMOVE]);
  ASSERT_EQ(0, filter_statistics.filter_block_row_cnt_);
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

/**
 * TEST 11: delete_flag_recycle - Delete Flag Recycling
 *
 * Objective: Verify DELETE flag rows are properly recycled
 *
 * Data Layout:
 *   SSTable1-Macro1: rowkey=0 v-15,v-12,v-8 (normal inserts)
 *   SSTable2-Macro1: rowkey=0 v-11 (DELETE)
 *   SSTable3-Macro1: rowkey=1 v-8 (DELETE, should be recycled), rowkey=2 v-15
 *
 * Recycle Version: base_version=10
 *
 * Expected Behavior:
 *   - rowkey=0: max_version=15 > 10, merge all versions (DELETE at v-11 between inserts)
 *   - rowkey=1: DELETE at v-8 <= 10, should be recycled
 *   - rowkey=2: v-15 > 10, kept
 *
 * Expected Statistics:
 *   - macro_cnt[OP_FILTER]=1 (SSTable3-Macro1 with rowkey=1)
 *   - row_cnt[REMOVE]=2 (rowkey=0 v-8 + rowkey=1 DELETE)
 */
TEST_F(TestMultiVersionMergeRecycle, delete_flag_recycle)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletExeMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);
  ObScnRange scn_range;

  ObTableHandleV2 handle1;
  const char *micro_data[1];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "0        var0  -15      MIN      12      2    EXIST   SCF\n"
      "0        var0  -15      0        12      NOP  EXIST   N\n"
      "0        var0  -12      0        2       2    EXIST   N\n"
      "0        var0  -8       0        NOP     3    EXIST   L\n"
      "1        var1  -5       0        NOP     NOP  DELETE  CLF\n";

  const int64_t snapshot_version = 18;
  PREPARE_SCN_RANGE(1, snapshot_version);
  prepare_table_schema(micro_data, schema_rowkey_cnt_, scn_range, snapshot_version);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint   bigint   bigint bigint  flag    multi_version_row_flag\n"
      "0        var0  -19      0        NOP     NOP   DELETE  LF\n";

  const int64_t snapshot_version_2 = 19;
  PREPARE_SCN_RANGE(snapshot_version, snapshot_version_2);
  reset_writer(snapshot_version_2);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObTableHandleV2 handle3;
  const char *micro_data3[1];
  micro_data3[0] =
      "bigint   var   bigint   bigint   bigint bigint  flag    multi_version_row_flag\n"
      "1        var1  -20      0        NOP     NOP   DELETE  CLF\n"
      "2        var2  -20      0        5       5     EXIST   CLF\n";

  const int64_t snapshot_version_3 = 20;
  PREPARE_SCN_RANGE(snapshot_version_2, snapshot_version_3);
  reset_writer(snapshot_version_3);
  prepare_one_macro(micro_data3, 1);
  prepare_data_end(handle3);
  merge_context.static_param_.tables_handle_.add_table(handle3);
  STORAGE_LOG(INFO, "finish prepare sstable3");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 11;
  trans_version_range.base_version_ = 10;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint   bigint  bigint  flag    multi_version_row_flag\n"
      "0        var0  -19      MIN      NOP     NOP  DELETE  SCF\n"
      "0        var0  -19      0        NOP     NOP  DELETE  C\n"
      "0        var0  -15      0        12      NOP  EXIST   N\n"
      "0        var0  -12      0        2       2    EXIST   C\n"
      "0        var0  -8       0        NOP     3    EXIST   L\n"
      "1        var1  -20      0        NOP     NOP   DELETE  CLF\n"
      "2        var2  -20      0        5       5     EXIST   CLF\n";

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
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
  ObMockDirectReadIterator sstable_iter;
  ASSERT_EQ(OB_SUCCESS, sstable_iter.init(scanner, allocator_, full_read_info_));
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  handle3.reset();
  merger.reset();
}

/**
 * TEST 12: abort_transaction_recycle - Aborted Transaction Handling
 *
 * Objective: Verify aborted (rollback) transactions are filtered correctly
 *
 * Data Layout:
 *   SSTable1: rowkey=0 v-15,v-12 (committed)
 *   SSTable2: rowkey=0 uncommitted v-10,v-8 (will be aborted), rowkey=1 v-15
 *
 * Recycle Version: base_version=11
 *
 * Expected Behavior:
 *   - rowkey=0 uncommitted rows: transaction aborted, should be filtered
 *   - rowkey=0: only keep v-15, v-12 (v-12 > 11 with multi_version_start=12)
 *   - rowkey=1: v-15 kept
 *
 * Expected Statistics:
 *   - Aborted rows don't participate in version comparison
 *   - row_cnt[REMOVE] includes aborted rows
 */
TEST_F(TestMultiVersionMergeRecycle, abort_transaction_recycle)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletExeMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);
  ObScnRange scn_range;

  ObTableHandleV2 handle1;
  const char *micro_data[1];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "0        var0  -15      MIN      12      1    EXIST   SCF\n"
      "0        var0  -15      0        12      NOP  EXIST   N\n"
      "0        var0  -12      0        2       2    EXIST   L\n";

  const int64_t snapshot_version = 18;
  PREPARE_SCN_RANGE(1, snapshot_version);
  prepare_table_schema(micro_data, schema_rowkey_cnt_, scn_range, snapshot_version);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint   bigint   bigint bigint  flag    multi_version_row_flag  trans_id\n"
      "0        var0  MIN      -10      3       3     EXIST   FU  trans_id_1\n"
      "0        var0  MIN      -8       NOP     4     EXIST   LU   trans_id_1\n"
      "1        var1  -15      0        5       5     EXIST   CLF trans_id_0\n";

  const int64_t snapshot_version_2 = 20;
  PREPARE_SCN_RANGE(snapshot_version, snapshot_version_2);
  reset_writer(snapshot_version_2);
  prepare_one_macro(micro_data2, 1);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  // Mark transaction as ABORTED (rollback) - INT64_MAX means ABORT
  insert_tx_data(1/*tx_id*/, INT64_MAX/*commit_version*/);

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 12;
  trans_version_range.base_version_ = 11;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint   bigint  bigint  flag    multi_version_row_flag\n"
      "0        var0  -15      MIN      12      1    EXIST   SCF\n"
      "0        var0  -15      0        12      NOP  EXIST   N\n"
      "0        var0  -12      0        2       2    EXIST   L\n"
      "1        var1  -15      0        5       5    EXIST   CLF\n";

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
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
  ObMockDirectReadIterator sstable_iter;
  ASSERT_EQ(OB_SUCCESS, sstable_iter.init(scanner, allocator_, full_read_info_));
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  clear_tx_data();
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

/**
 * TEST 13: three_sstables_mixed - Three SSTable Mixed Scenario
 *
 * Objective: Verify correct handling of 3+ SSTable merge with mixed operations
 *
 * Data Layout:
 *   SSTable1-Macro1: rowkey=0 v-8 (recyclable)
 *   SSTable1-Macro2: rowkey=1 v-15 (reusable)
 *   SSTable2-Macro1: rowkey=1 v-12 (needs merge with SSTable1)
 *   SSTable2-Macro2: rowkey=2 v-9 (recyclable)
 *   SSTable3-Macro1: rowkey=2 v-20 (needs merge with SSTable2)
 *   SSTable3-Macro2: rowkey=3 v-8 (recyclable)
 *
 * Recycle Version: base_version=10
 *
 * Expected Behavior:
 *   - rowkey=0: v-8 <= 10, filtered
 *   - rowkey=1: merge v-15 and v-12, keep both
 *   - rowkey=2: merge v-20 and v-9, keep v-20, recycle v-9
 *   - rowkey=3: v-8 <= 10, filtered
 *
 * Expected Statistics:
 *   - macro_cnt[OP_FILTER]=2 (SSTable1-Macro1, SSTable3-Macro2)
 *   - macro_cnt[OP_NONE]=1 (SSTable1-Macro2)
 *   - filter_block_row_cnt=2 (rowkey=0 + rowkey=3)
 *   - row_cnt[REMOVE]=1 (rowkey=2 v-9)
 */
TEST_F(TestMultiVersionMergeRecycle, three_sstables_mixed)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletExeMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);
  ObScnRange scn_range;

  // SSTable1
  ObTableHandleV2 handle1;
  const char *micro_data1[2];
  micro_data1[0] = // MACRO: OP_FILTER
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "0        var0  -8       0        NOP     1    EXIST   CLF\n";

  micro_data1[1] = // MACRO: OP_FILTER
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag\n"
      "1        var1  -15      0        2       2    EXIST   CLF\n";

  const int64_t snapshot_version1 = 10;
  PREPARE_SCN_RANGE(1, snapshot_version1);
  prepare_table_schema(micro_data1, schema_rowkey_cnt_, scn_range, snapshot_version1);
  reset_writer(snapshot_version1);
  prepare_one_macro(micro_data1, 1);
  prepare_one_macro(&micro_data1[1], 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  // SSTable2
  ObTableHandleV2 handle2;
  const char *micro_data2[2];
  micro_data2[0] = // MACRO: OP_NONE
      "bigint   var   bigint   bigint   bigint bigint  flag    multi_version_row_flag\n"
      "1        var1  -18      0        3       3     EXIST   CLF\n";

  micro_data2[1] = // MACRO: OP_FILTER
      "bigint   var   bigint   bigint   bigint bigint  flag    multi_version_row_flag\n"
      "2        var2  -9       0        4       4     EXIST   CLF\n";

  const int64_t snapshot_version2 = 15;
  PREPARE_SCN_RANGE(snapshot_version1, snapshot_version2);
  reset_writer(snapshot_version2);
  prepare_one_macro(micro_data2, 1);
  prepare_one_macro(&micro_data2[1], 1);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  // SSTable3
  ObTableHandleV2 handle3;
  const char *micro_data3[2];
  micro_data3[0] = // MACRO: OP_NONE
      "bigint   var   bigint   bigint   bigint bigint  flag    multi_version_row_flag\n"
      "2        var2  -20      0        5       5     EXIST   CLF\n";

  micro_data3[1] = // MACRO: OP_NONE
      "bigint   var   bigint   bigint   bigint bigint  flag    multi_version_row_flag\n"
      "3        var3  -18       0        NOP     6     EXIST   CLF\n";

  const int64_t snapshot_version3 = 25;
  PREPARE_SCN_RANGE(snapshot_version2, snapshot_version3);
  reset_writer(snapshot_version3);
  prepare_one_macro(micro_data3, 1);
  prepare_one_macro(&micro_data3[1], 1);
  prepare_data_end(handle3);
  merge_context.static_param_.tables_handle_.add_table(handle3);
  STORAGE_LOG(INFO, "finish prepare sstable3");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 17;
  trans_version_range.base_version_ = 15;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint   bigint   bigint  bigint  flag    multi_version_row_flag\n"
      "1        var1  -18      0        3       3     EXIST   CLF\n"
      "2        var2  -20      0        5       5     EXIST   CLF\n"
      "3        var3  -18      0        NOP     6     EXIST   CLF\n";

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
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
  ObMockDirectReadIterator sstable_iter;
  ASSERT_EQ(OB_SUCCESS, sstable_iter.init(scanner, allocator_, full_read_info_));
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true/*cmp multi version row flag*/);
  ASSERT_TRUE(is_equal);
  const ObICompactionFilter::ObFilterStatistics &filter_statistics = merge_context.filter_ctx_.filter_statistics_;
  ASSERT_EQ(3, filter_statistics.macro_cnt_[ObBlockOp::OP_FILTER]);
  ASSERT_EQ(3, filter_statistics.macro_cnt_[ObBlockOp::OP_NONE]);
  ASSERT_EQ(3, filter_statistics.filter_block_row_cnt_);
  ASSERT_EQ(0, filter_statistics.row_cnt_[ObICompactionFilter::FILTER_RET_REMOVE]);
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  handle3.reset();
  merger.reset();
}

TEST_F(TestMultiVersionMergeRecycle, multi_uncommitted_trans_recycle)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletExeMergeCtx merge_context(param, allocator_);
  ObPartitionMinorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  const char *micro_data[1];
  micro_data[0] = // FILTER
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag trans_id\n"
      "0        var0  -10      -1         0       0     INSERT    NORMAL        CLF               trans_id_0\n"
      "1        var1  MIN      -100       NOP     3     INSERT    NORMAL        FU                trans_id_2\n"
      "1        var1  MIN      -80        NOP     2     INSERT    NORMAL        U                 trans_id_2\n"
      "1        var1  MIN      -30        NOP     1     INSERT    NORMAL        U                 trans_id_2\n"
      "1        var1  MIN      -20        30      NOP   INSERT    NORMAL        U                 trans_id_1\n"
      "1        var1  -10      MIN        NOP     9     INSERT    NORMAL        SC                trans_id_0\n"
      "1        var1  -10      0          NOP     9     INSERT    NORMAL        N                 trans_id_0\n"
      "1        var1  -9       0          NOP     1     INSERT    NORMAL        L                 trans_id_0\n";

  int schema_rowkey_cnt = 2;
  int64_t snapshot_version = 30;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(30);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1, true);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[1];
  micro_data2[0] =
      "bigint   var   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag trans_id\n"
      "1        var1  MIN      -500        NOP     NOP   DELETE    NORMAL        FU              trans_id_4\n"
      "1        var1  MIN      -400        NOP     14    UPDATE    NORMAL        U                trans_id_3\n"
      "1        var1  MIN      -300        NOP     3     UPDATE    NORMAL        U                trans_id_3\n"
      "1        var1  -22      0           NOP     4     UPDATE    NORMAL        L                trans_id_0\n"
      "2        var2  -50      0           1       1     INSERT    NORMAL        CLF              trans_id_0\n";

  snapshot_version = 50;
  scn_range.start_scn_.convert_for_tx(30);
  scn_range.end_scn_.convert_for_tx(50);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1, true);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObTxTable *tx_table = nullptr;
  ObTxTableGuard tx_table_guard;
  get_tx_table_guard(tx_table_guard);
  ASSERT_NE(nullptr, tx_table = tx_table_guard.get_tx_table());

  for (int64_t i = 1; i <= 4; i++) {
    ObTxData *tx_data = new ObTxData();
    ASSERT_EQ(OB_SUCCESS, tx_data->init_tx_op());
    transaction::ObTransID tx_id = i;

    // fill in data
    tx_data->tx_id_ = tx_id;
    if (i < 4) {
      tx_data->commit_version_.convert_for_tx(i * 10 + i);
      tx_data->start_scn_.convert_for_tx(i);
      tx_data->end_scn_ = tx_data->commit_version_;
      tx_data->state_ = ObTxData::COMMIT;
    } else {
      tx_data->commit_version_.convert_for_tx(INT64_MAX);
      tx_data->state_ = ObTxData::ABORT;
    }
    ASSERT_EQ(OB_SUCCESS, tx_table->insert(tx_data));
    delete tx_data;
  }

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 40;
  trans_version_range.base_version_ = 40;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  const char *result1 =
      "bigint   var   bigint  bigint      bigint bigint  flag     flag_type  multi_version_row_flag trans_id\n"
      "1        var1  -33      0          NOP     14     UPDATE    NORMAL        LF                trans_id_0\n" // not filter because this row don't have F-flag
      "2        var2  -50      0          1       1      INSERT    NORMAL        CLF                trans_id_0\n";

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
  ASSERT_EQ(OB_SUCCESS, res_iter.from(result1));
  ObMockDirectReadIterator sstable_iter;
  ASSERT_EQ(OB_SUCCESS, sstable_iter.init(scanner, allocator_, full_read_info_));
  bool is_equal = res_iter.equals<ObMockDirectReadIterator, ObStoreRow>(sstable_iter, true, false);
  ASSERT_TRUE(is_equal);
  clear_tx_data();
  scanner->~ObStoreRowIterator();
  handle1.reset();
  handle2.reset();
  merger.reset();
}

}
}

int main(int argc, char **argv)
{
  system("rm -rf test_multi_version_merge_recycle.log*");
  OB_LOGGER.set_file_name("test_multi_version_merge_recycle.log");
  OB_LOGGER.set_log_level("INFO");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
