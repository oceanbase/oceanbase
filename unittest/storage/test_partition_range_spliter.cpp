/**
 * Copyright (c) 2023 OceanBase
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

#define private public
#define protected public

#include "storage/compaction/ob_partition_merger.h"
#include "storage/blocksstable/ob_multi_version_sstable_test.h"
#include "storage/test_tablet_helper.h"
#include "storage/ob_partition_range_spliter.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
using namespace blocksstable;
using namespace compaction;
using namespace memtable;
using namespace observer;
using namespace unittest;
using namespace rpc::frame;
using namespace memtable;
namespace storage
{

static ObArenaAllocator global_allocator_;

class TestRangeSpliter: public ObMultiVersionSSTableTest
{
public:
  static const int64_t MAX_MACRO_CNT = 20;

  TestRangeSpliter() : ObMultiVersionSSTableTest("test_rangespliter") {}

  virtual ~TestRangeSpliter() = default;

  void SetUp() { ObMultiVersionSSTableTest::SetUp(); }

  void TearDown()
  {
    ObMultiVersionSSTableTest::TearDown();
    TRANS_LOG(INFO, "teardown success");
  }

  static void SetUpTestCase();
  static void TearDownTestCase();

  void build_query_range(ObIArray<ObStoreRange> &ranges, const char *rowkey_data);

  void transform_to_datum_range(ObIArray<ObPairStoreAndDatumRange> &datum_ranges, const ObIArray<ObStoreRange> &ranges);

  bool equal_ranges(ObStoreRange &range1, ObStoreRange &range2);

  bool loop_equal_ranges(ObIArray<ObStoreRange> &ranges1, ObIArray<ObStoreRange> &ranges2);

  void prepare_sstable_handle(ObTableHandleV2 &handle,
                              const int64_t macro_idx_start,
                              const int64_t macro_cnt,
                              const int64_t snapshot_version,
                              const int64_t micro_cnt = 1);

  ObArray<ObColDesc> columns_;
  ObTableIterParam param_;
  ObTableAccessContext context_;
  ObArenaAllocator allocator_;
  ObMockIterator range_iter_;
};

void TestRangeSpliter::SetUpTestCase()
{
  ObMultiVersionSSTableTest::SetUpTestCase();
  // mock sequence no
  ObClockGenerator::init();

  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));

  // create tablet
  obrpc::ObBatchCreateTabletArg create_tablet_arg;
  share::schema::ObTableSchema table_schema;
  ASSERT_EQ(OB_SUCCESS, gen_create_tablet_arg(tenant_id_, ls_id, tablet_id, create_tablet_arg, 1, &table_schema));

  ObLSTabletService *ls_tablet_svr = ls_handle.get_ls()->get_tablet_svr();
  ASSERT_EQ(OB_SUCCESS, TestTabletHelper::create_tablet(ls_handle, tablet_id, table_schema, ObMultiVersionSSTableTest::allocator_));
}

void TestRangeSpliter::TearDownTestCase()
{
  ObMultiVersionSSTableTest::TearDownTestCase();
  // reset sequence no
  ObClockGenerator::destroy();
}

bool TestRangeSpliter::equal_ranges(ObStoreRange &range1, ObStoreRange &range2)
{
  bool bret = false;
  if (!range1.get_start_key().simple_equal(range2.get_start_key())) {
  } else if (!range1.get_end_key().simple_equal(range2.get_end_key())) {
  } else {
    bret = range1.get_border_flag().get_data() == range2.get_border_flag().get_data();
  }
  if (!bret) {
    int ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ranges are not equal", K(range1), K(range2));
  }

  return bret;
}

bool TestRangeSpliter::loop_equal_ranges(ObIArray<ObStoreRange> &ranges1, ObIArray<ObStoreRange> &ranges2)
{
  bool bret = false;
  if (ranges1.count() != ranges2.count()) {
    int ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ranges are not equal", K(ranges1), K(ranges2));
  } else {
    bret = true;
    for (int64_t i = 0; bret && i < ranges1.count(); i++) {
      bret = equal_ranges(ranges1.at(i), ranges2.at(i));
    }
  }

  return bret;
}

void TestRangeSpliter::transform_to_datum_range(ObIArray<ObPairStoreAndDatumRange> &datum_ranges,
                                                const ObIArray<ObStoreRange> &ranges)
{
  for (int i = 0; i < ranges.count(); i++) {
    ObPairStoreAndDatumRange range;
    range.origin_store_range_ = &ranges.at(i);
    ASSERT_EQ(OB_SUCCESS, range.datum_range_.from_range(ranges.at(i), allocator_));
    ASSERT_EQ(OB_SUCCESS, datum_ranges.push_back(range));
  }
}

void TestRangeSpliter::build_query_range(ObIArray<ObStoreRange> &ranges, const char *rowkey_data)
{
  const ObStoreRow *row = nullptr;
  const int64_t rowkey_col_cnt = TEST_ROWKEY_COLUMN_CNT;
  ObStoreRange range, dst_range;
  ASSERT_TRUE(nullptr != rowkey_data);
  ranges.reset();
  range_iter_.reset();
  OK(range_iter_.from(rowkey_data));
  ASSERT_TRUE(range_iter_.count() > 0 && range_iter_.count() % 2 == 0);

  for (int64_t i = 0; i < range_iter_.count(); i+=2) {
    OK(range_iter_.get_row(i, row));
    ASSERT_TRUE(nullptr != row);
    range.get_start_key().assign(row->row_val_.cells_, rowkey_col_cnt);
    OK(range_iter_.get_row(i + 1, row));
    ASSERT_TRUE(nullptr != row);
    range.get_end_key().assign(row->row_val_.cells_, rowkey_col_cnt);
    if (range.get_start_key().compare(range.get_end_key()) == 0) {
      range.set_left_closed();
    } else {
      range.set_left_open();
    }
    range.set_right_closed();
    ASSERT_EQ(OB_SUCCESS, range.deep_copy(allocator_, dst_range));
    ASSERT_EQ(OB_SUCCESS, ranges.push_back(dst_range));
    range.reset();
    dst_range.reset();
  }
}

void TestRangeSpliter::prepare_sstable_handle(ObTableHandleV2 &handle,
                                              const int64_t macro_idx_start,
                                              const int64_t macro_cnt,
                                              const int64_t snapshot_version,
                                              const int64_t micro_cnt)
{
  const int64_t rowkey_cnt = TEST_ROWKEY_COLUMN_CNT + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  ASSERT_EQ(true, macro_idx_start + macro_cnt <= MAX_MACRO_CNT);

  const char *macro_data[20];
  macro_data[0] =
      "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
      "0        var1    -9    0     EXIST   CLF\n"
      "1        var1    -9    MIN   EXIST   SCF\n"
      "1        var1    -9    0     EXIST   C\n";

  macro_data[1] =
      "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
      "1        var1    -8     0      EXIST   L\n"
      "2        var1    -9     0      EXIST   CLF\n";

  macro_data[2] =
      "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
      "3        var1    -8     0      EXIST   CLF\n"
      "4        var1   -9    MIN      EXIST   SCF\n"
      "4        var1    -9     0      EXIST   C\n";

  macro_data[3] =
      "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
      "4        var1    -8     0      EXIST   L\n"
      "5        var1    -9     0      EXIST   CLF\n";

  macro_data[4] =
      "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
      "6        var1    -9     0      EXIST   CLF\n"
      "7        var1   -9    MIN      EXIST   SCF\n"
      "7        var1    -9     0      EXIST   C\n";

  macro_data[5] =
      "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
      "7        var1    -8     0      EXIST   L\n"
      "8        var1    -9    0     EXIST   CLF\n";

  macro_data[6] =
      "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
      "9        var1    -8     0      EXIST   CLF\n"
      "10       var1  -9    MIN      EXIST   SCF\n"
      "10       var1    -9    0     EXIST    C\n";

  macro_data[7] =
      "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
      "10       var1    -8     0      EXIST   L\n"
      "11       var1    -9    0     EXIST   CLF\n";

  macro_data[8] =
      "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
      "12       var1     -9    0     EXIST   CLF\n"
      "13       var1   -9    MIN      EXIST   SCF\n"
      "13       var1     -9    0     EXIST   C\n";

  macro_data[9] =
      "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
      "13       var1   -8    MIN      EXIST   L\n"
      "14       var1   -9    MIN      EXIST   CLF\n";

  macro_data[10] =
      "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
      "15       var1     -8     0      EXIST   CLF\n"
      "16       var1   -9    MIN      EXIST   SCF\n"
      "16       var1     -9    0     EXIST     C\n";

  macro_data[11] =
      "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
      "16       var1     -8     0      EXIST   L\n"
      "17       var1     -9    0     EXIST   CLF\n";

  macro_data[12] =
      "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
      "18       var1     -9    0     EXIST   CLF\n"
      "19       var1   -9    MIN      EXIST   SCF\n"
      "19       var1     -9    0     EXIST   C\n";

  macro_data[13] =
      "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
      "19       var1     -8     0      EXIST   L\n"
      "20       var1     -9    0     EXIST   CLF\n";

  macro_data[14] =
      "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
      "21       var1     -8     0      EXIST   CLF\n"
      "22       var1   -9    MIN      EXIST   SCF\n"
      "22       var1     -9    0     EXIST   C\n";

  macro_data[15] =
      "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
      "22       var1    -8     0      EXIST   L\n"
      "23       var1    -9    0     EXIST   CLF\n";

  macro_data[16] =
      "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
      "24       var1    -9    0     EXIST   CLF\n"
      "25       var1    -9    MIN      EXIST  SCF\n"
      "25       var1    -9    0     EXIST   CL\n";

  macro_data[17] =
      "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
      "26       var1    -8     0      EXIST   L\n"
      "27       var1    -9    0     EXIST   CLF\n";

  macro_data[18] =
      "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
      "28       var1    -8     0      EXIST   CLF\n"
      "29       var1   -9    MIN      EXIST   SCF\n"
      "29       var1    -9    0     EXIST   C\n";

  macro_data[19] =
      "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
      "29       var1    -8     0      EXIST   L\n"
      "30       var1    -9    0     EXIST   CLF\n";

  int schema_rowkey_cnt = 2;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  if (!table_schema_.is_valid()) {
    prepare_table_schema(macro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  }
  reset_writer(snapshot_version);
  for (int64_t i = macro_idx_start; i < macro_idx_start + macro_cnt * micro_cnt; i += micro_cnt) {
    prepare_one_macro(&macro_data[i], micro_cnt);
  }
  prepare_data_end(handle);
  ObSSTable *sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, handle.get_sstable(sstable));
  sstable->meta_->basic_meta_.occupy_size_ = (2<<20) * macro_cnt;
  sstable->key_.table_type_ = ObITable::MINOR_SSTABLE;
}

TEST_F(TestRangeSpliter, test_multi_range_estimate)
{
  allocator_.reuse();

  const ObTableReadInfo *index_read_info = &full_read_info_;
  ObTableHandleV2 handle;
  prepare_sstable_handle(handle, 0, 20, 10);
  ObSSTable *sstable = static_cast<ObSSTable *>(handle.table_);

  {
    // single whole range
    ObArray<ObPairStoreAndDatumRange> ranges;
    ObPairStoreAndDatumRange range;
    range.datum_range_.set_whole_range();
    ASSERT_EQ(OB_SUCCESS, ranges.push_back(range));

    ObMultiRangeRowEstimateContext context;
    ObIndexBlockTreeTraverser traverse;

    ASSERT_EQ(OB_SUCCESS, context.init(ranges, *index_read_info));
    ASSERT_EQ(OB_SUCCESS, traverse.init(*sstable, *index_read_info));
    ASSERT_EQ(OB_SUCCESS, traverse.traverse(context));
    ASSERT_EQ(50, context.ranges_.at(0).row_count_);
  }

  {
    ObArray<ObStoreRange> ranges;
    ObArray<ObPairStoreAndDatumRange> datum_ranges;
    const char *rowkey_data = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                              "23       var1   -9    MIN      EXIST   CLF\n"
                              "26       var1   -9    MIN      EXIST   CLF\n"
                              "1        var1   -9    MIN      EXIST   CLF\n"
                              "7        var1   -9    MIN      EXIST   CLF\n"
                              "10       var1   -9    MIN      EXIST   CLF\n"
                              "19       var1   -9    MIN      EXIST   CLF\n";
    build_query_range(ranges, rowkey_data);

    ObMultiRangeRowEstimateContext context;
    ObIndexBlockTreeTraverser traverse;

    transform_to_datum_range(datum_ranges, ranges);
    ASSERT_EQ(OB_SUCCESS, context.init(datum_ranges, *index_read_info));
    ASSERT_EQ(OB_SUCCESS, traverse.init(*sstable, *index_read_info));
    ASSERT_EQ(OB_SUCCESS, traverse.traverse(context));
    ASSERT_LE(7, context.ranges_.at(0).row_count_);
    ASSERT_LE(context.ranges_.at(0).row_count_, 10);
    ASSERT_LE(12, context.ranges_.at(1).row_count_);
    ASSERT_LE(context.ranges_.at(1).row_count_, 15);
    ASSERT_EQ(2, context.ranges_.at(2).row_count_);
  }
}

TEST_F(TestRangeSpliter, test_multi_range_split)
{
  allocator_.reuse();

  const ObTableReadInfo *index_read_info = &full_read_info_;
  ObTableHandleV2 handle;
  prepare_sstable_handle(handle, 0, 20, 10);
  ObSSTable *sstable = static_cast<ObSSTable *>(handle.table_);

  {
    // single whole range
    ObArray<ObPairStoreAndDatumRange> ranges;
    ObPairStoreAndDatumRange range;
    ObArray<ObSplitRangeInfo> split_ranges;
    range.datum_range_.set_whole_range();
    ASSERT_EQ(OB_SUCCESS, ranges.push_back(range));

    ObMultiRangeRowEstimateContext row_context;
    ObMultiRangeSplitContext context;
    ObIndexBlockTreeTraverser traverse;

    ASSERT_EQ(OB_SUCCESS, row_context.init(ranges, *index_read_info));
    ASSERT_EQ(OB_SUCCESS, traverse.init(*sstable, *index_read_info));
    ASSERT_EQ(OB_SUCCESS, traverse.traverse(row_context));
    traverse.reuse();
    ASSERT_EQ(OB_SUCCESS, context.init(ranges, *index_read_info, 10, allocator_, row_context.get_ranges(), split_ranges));
    ASSERT_EQ(OB_SUCCESS, traverse.traverse(context));
    ASSERT_EQ(6, split_ranges.count());
  }

  {
    ObArray<ObStoreRange> ranges;
    ObArray<ObPairStoreAndDatumRange> datum_ranges;
    ObArray<ObSplitRangeInfo> split_ranges;
    const char *rowkey_data = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                              "23       var1   -9    MIN      EXIST   CLF\n"
                              "26       var1   -9    MIN      EXIST   CLF\n"
                              "1        var1   -9    MIN      EXIST   CLF\n"
                              "7        var1   -9    MIN      EXIST   CLF\n"
                              "10       var1   -9    MIN      EXIST   CLF\n"
                              "19       var1   -9    MIN      EXIST   CLF\n";
    build_query_range(ranges, rowkey_data);

    ObMultiRangeRowEstimateContext row_context;
    ObMultiRangeSplitContext context;
    ObIndexBlockTreeTraverser traverse;

    transform_to_datum_range(datum_ranges, ranges);
    ASSERT_EQ(OB_SUCCESS, row_context.init(datum_ranges, *index_read_info));
    ASSERT_EQ(OB_SUCCESS, traverse.init(*sstable, *index_read_info));
    ASSERT_EQ(OB_SUCCESS, traverse.traverse(row_context));
    traverse.reuse();
    ASSERT_EQ(OB_SUCCESS, context.init(datum_ranges, *index_read_info, 4, allocator_, row_context.get_ranges(), split_ranges));
    ASSERT_EQ(OB_SUCCESS, traverse.traverse(context));
    ASSERT_EQ(6, split_ranges.count());
  }
}

TEST_F(TestRangeSpliter, test_partition_multi_range_split)
{
  allocator_.reuse();

  ObArray<ObITable *> sstables;
  const ObTableReadInfo *index_read_info = &full_read_info_;
  ObTableHandleV2 handle;
  prepare_sstable_handle(handle, 0, 20, 10);
  ASSERT_EQ(OB_SUCCESS, sstables.push_back(handle.table_));

  {
    ObArray<ObStoreRange> ranges;
    ObArrayArray<ObStoreRange> multi_range_split_array;
    const char *rowkey_data = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                              "23       var1   -9    MIN      EXIST   CLF\n"
                              "26       var1   -9    MIN      EXIST   CLF\n"
                              "1        var1   -9    MIN      EXIST   CLF\n"
                              "8        var1   -9    MIN      EXIST   CLF\n"
                              "10       var1   -9    MIN      EXIST   CLF\n"
                              "19       var1   -9    MIN      EXIST   CLF\n";
    build_query_range(ranges, rowkey_data);

    ObPartitionMultiRangeSpliter spliter;
    ASSERT_EQ(OB_SUCCESS,
              spliter.build_range_array(
                  ranges, 2, *index_read_info, sstables, allocator_, multi_range_split_array));

    const char *rowkey_data1 = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                              "1        var1   -9    MIN      EXIST   CLF\n"
                              "8        var1   -9    MIN      EXIST   CLF\n";
    build_query_range(ranges, rowkey_data1);
    ASSERT_EQ(true, loop_equal_ranges(ranges, multi_range_split_array.at(0)));

    const char *rowkey_data2 = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                              "10       var1   -9    MIN      EXIST   CLF\n"
                              "19       var1   -9    MIN      EXIST   CLF\n"
                              "23       var1   -9    MIN      EXIST   CLF\n"
                              "26       var1   -9    MIN      EXIST   CLF\n";
    build_query_range(ranges, rowkey_data2);
    ASSERT_EQ(true, loop_equal_ranges(ranges, multi_range_split_array.at(1)));
  }
}

TEST_F(TestRangeSpliter, test_multi_sstable_split)
{
  allocator_.reuse();

  ObArray<ObITable *> sstables;
  const ObTableReadInfo *index_read_info = &full_read_info_;
  ObTableHandleV2 handle, handle1, handle2;
  prepare_sstable_handle(handle, 0, 5, 10);
  prepare_sstable_handle(handle1, 5, 10, 10);
  prepare_sstable_handle(handle2, 15, 5, 10);
  ASSERT_EQ(OB_SUCCESS, sstables.push_back(handle.table_));
  ASSERT_EQ(OB_SUCCESS, sstables.push_back(handle1.table_));
  ASSERT_EQ(OB_SUCCESS, sstables.push_back(handle2.table_));

  {
    ObArray<ObStoreRange> ranges;
    ObArrayArray<ObStoreRange> multi_range_split_array;
    const char *rowkey_data = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                              "23       var1   -9    MIN      EXIST   CLF\n"
                              "26       var1   -9    MIN      EXIST   CLF\n"
                              "1        var1   -9    MIN      EXIST   CLF\n"
                              "8        var1   -9    MIN      EXIST   CLF\n"
                              "10       var1   -9    MIN      EXIST   CLF\n"
                              "19       var1   -9    MIN      EXIST   CLF\n";
    build_query_range(ranges, rowkey_data);

    ObPartitionMultiRangeSpliter spliter;
    ASSERT_EQ(OB_SUCCESS,
              spliter.build_range_array(
                  ranges, 2, *index_read_info, sstables, allocator_, multi_range_split_array));

    const char *rowkey_data1 = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                              "1        var1   -9    MIN      EXIST   CLF\n"
                              "8        var1   -9    MIN      EXIST   CLF\n"
                              "10       var1   -9    MIN      EXIST   CLF\n"
                              "13       var1   -9    MIN      EXIST   CLF\n";
    build_query_range(ranges, rowkey_data1);
    ASSERT_EQ(true, loop_equal_ranges(ranges, multi_range_split_array.at(0)));

    const char *rowkey_data2 = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                              "13       var1   -9    MIN      EXIST   CLF\n"
                              "19       var1   -9    MIN      EXIST   CLF\n"
                              "23       var1   -9    MIN      EXIST   CLF\n"
                              "26       var1   -9    MIN      EXIST   CLF\n";
    build_query_range(ranges, rowkey_data2);
    ASSERT_EQ(true, loop_equal_ranges(ranges, multi_range_split_array.at(1)));
  }

  {
    ObArray<ObStoreRange> ranges;
    ObArrayArray<ObStoreRange> multi_range_split_array;
    const char *rowkey_data = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                              "23       var1   -9    MIN      EXIST   CLF\n"
                              "26       var1   -9    MIN      EXIST   CLF\n"
                              "1        var1   -9    MIN      EXIST   CLF\n"
                              "8        var1   -9    MIN      EXIST   CLF\n"
                              "10       var1   -9    MIN      EXIST   CLF\n"
                              "19       var1   -9    MIN      EXIST   CLF\n";
    build_query_range(ranges, rowkey_data);

    ObPartitionMultiRangeSpliter spliter;
    ASSERT_EQ(OB_SUCCESS,
              spliter.build_range_array(
                  ranges, 3, *index_read_info, sstables, allocator_, multi_range_split_array));

    const char *rowkey_data1 = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                              "1        var1   -9    MIN      EXIST   CLF\n"
                              "8        var1   -9    MIN      EXIST   CLF\n";
    build_query_range(ranges, rowkey_data1);
    ASSERT_EQ(true, loop_equal_ranges(ranges, multi_range_split_array.at(0)));

    const char *rowkey_data2 = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                              "10       var1   -9    MIN      EXIST   CLF\n"
                              "16       var1   -9    MIN      EXIST   CLF\n";
    build_query_range(ranges, rowkey_data2);
    ASSERT_EQ(true, loop_equal_ranges(ranges, multi_range_split_array.at(1)));

    const char *rowkey_data3 = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                              "16       var1   -9    MIN      EXIST   CLF\n"
                              "19       var1   -9    MIN      EXIST   CLF\n"
                              "23       var1   -9    MIN      EXIST   CLF\n"
                              "26       var1   -9    MIN      EXIST   CLF\n";
    build_query_range(ranges, rowkey_data3);
    ASSERT_EQ(true, loop_equal_ranges(ranges, multi_range_split_array.at(2)));
  }
}

TEST_F(TestRangeSpliter, test_micro_level_split)
{
  allocator_.reuse();

  ObArray<ObITable *> sstables;
  const ObTableReadInfo *index_read_info = &full_read_info_;
  ObTableHandleV2 handle;
  prepare_sstable_handle(handle, 0, 2, 10, 10);
  ASSERT_EQ(OB_SUCCESS, sstables.push_back(handle.table_));

  {
    ObArray<ObStoreRange> ranges;
    ObArrayArray<ObStoreRange> multi_range_split_array;

    ObStoreRange whole_range;
    whole_range.set_whole_range();
    ASSERT_EQ(OB_SUCCESS, ranges.push_back(whole_range));

    ObPartitionMultiRangeSpliter spliter;
    ASSERT_EQ(OB_SUCCESS,
              spliter.build_range_array(
                  ranges, 4, *index_read_info, sstables, allocator_, multi_range_split_array));

    const char *rowkey_data1 = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                               "MIN      MIN    -9    MIN      EXIST   CLF\n"
                               "8       var1    -9    MIN      EXIST   CLF\n";
    build_query_range(ranges, rowkey_data1);
    ranges.at(0).get_start_key().set_min();
    ASSERT_EQ(true, loop_equal_ranges(ranges, multi_range_split_array.at(0)));

    const char *rowkey_data2 = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                               "8        var1   -9    MIN      EXIST   CLF\n"
                               "13       var1   -9    MIN      EXIST   CLF\n";
    build_query_range(ranges, rowkey_data2);
    ASSERT_EQ(true, loop_equal_ranges(ranges, multi_range_split_array.at(1)));

    const char *rowkey_data3 = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                               "13       var1   -9    MIN      EXIST   CLF\n"
                               "22       var1   -9    MIN      EXIST   CLF\n";
    build_query_range(ranges, rowkey_data3);
    ASSERT_EQ(true, loop_equal_ranges(ranges, multi_range_split_array.at(2)));

    const char *rowkey_data4 = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                               "22       var1   -9    MIN      EXIST   CLF\n"
                               "MAX      MAX    -9    MIN      EXIST   CLF\n";
    build_query_range(ranges, rowkey_data4);
    ranges.at(0).get_end_key().set_max();
    ranges.at(0).set_right_open();
    ASSERT_EQ(true, loop_equal_ranges(ranges, multi_range_split_array.at(3)));
  }
}

TEST_F(TestRangeSpliter, test_mulit_basic)
{
  allocator_.reuse();

  ObArray<ObITable *> sstables;
  const ObTableReadInfo *index_read_info = &full_read_info_;
  ObTableHandleV2 handle;
  prepare_sstable_handle(handle, 0, 20, 10);
  ASSERT_EQ(OB_SUCCESS, sstables.push_back(handle.table_));

  // single task case
  {
    ObArray<ObStoreRange> ranges;
    ObArrayArray<ObStoreRange> multi_range_split_array;
    const char *rowkey_data = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                              "1        var1   -9    MIN      EXIST   CLF\n"
                              "7        var1   -9    MIN      EXIST   CLF\n"
                              "10       var1   -9    MIN      EXIST   CLF\n"
                              "19       var1   -9    MIN      EXIST   CLF\n"
                              "23       var1   -9    MIN      EXIST   CLF\n"
                              "26       var1   -9    MIN      EXIST   CLF\n";
    build_query_range(ranges, rowkey_data);

    ObPartitionMultiRangeSpliter spliter;
    ASSERT_EQ(OB_SUCCESS,
              spliter.build_range_array(
                  ranges, 1, *index_read_info, sstables, allocator_, multi_range_split_array));

    ASSERT_EQ(1, multi_range_split_array.count());
    ASSERT_EQ(true, loop_equal_ranges(ranges, multi_range_split_array.at(0)));
  }

  // seperate range case, 5 ranges
  {
    ObArray<ObStoreRange> ranges;
    ObArrayArray<ObStoreRange> multi_range_split_array;
    const char *rowkey_data = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                              "1        var1   -9    MIN      EXIST   CLF\n"
                              "8        var1   -9    MIN      EXIST   CLF\n"
                              "10       var1   -9    MIN      EXIST   CLF\n"
                              "12       var1   -9    MIN      EXIST   CLF\n"
                              "13       var1   -9    MIN      EXIST   CLF\n"
                              "15       var1   -9    MIN      EXIST   CLF\n"
                              "16       var1   -9    MIN      EXIST   CLF\n"
                              "18       var1   -9    MIN      EXIST   CLF\n"
                              "22       var1   -9    MIN      EXIST   CLF\n"
                              "28       var1   -9    MIN      EXIST   CLF\n";
    build_query_range(ranges, rowkey_data);

    ObPartitionMultiRangeSpliter spliter;
    ASSERT_EQ(OB_SUCCESS,
              spliter.build_range_array(
                  ranges, 3, *index_read_info, sstables, allocator_, multi_range_split_array));

    ASSERT_EQ(3, multi_range_split_array.count());
    const char *rowkey_data1 = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                               "1        var1   -9    MIN      EXIST   CLF\n"
                               "7        var1   -9    MIN      EXIST   CLF\n";
    build_query_range(ranges, rowkey_data1);
    ASSERT_EQ(true, loop_equal_ranges(ranges, multi_range_split_array.at(0)));

    const char *rowkey_data2 = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                               "7        var1   -9    MIN      EXIST   CLF\n"
                               "8        var1   -9    MIN      EXIST   CLF\n"
                               "10       var1   -9    MIN      EXIST   CLF\n"
                               "12       var1   -9    MIN      EXIST   CLF\n"
                               "13        var1   -9    MIN      EXIST   CLF\n"
                               "15        var1   -9    MIN      EXIST   CLF\n";
    build_query_range(ranges, rowkey_data2);
    ASSERT_EQ(true, loop_equal_ranges(ranges, multi_range_split_array.at(1)));

    const char *rowkey_data3 = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                               "16        var1   -9    MIN      EXIST   CLF\n"
                               "18        var1   -9    MIN      EXIST   CLF\n"
                               "22        var1   -9    MIN      EXIST   CLF\n"
                               "28        var1   -9    MIN      EXIST   CLF\n";
    build_query_range(ranges, rowkey_data3);
    ASSERT_EQ(true, loop_equal_ranges(ranges, multi_range_split_array.at(2)));
  }

  // single task case
  {
    ObArray<ObStoreRange> ranges;
    ObArrayArray<ObStoreRange> multi_range_split_array;
    const char *rowkey_data = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                              "8        var1   -9    MIN      EXIST   CLF\n"
                              "8        var1   -9    MIN      EXIST   CLF\n";
    build_query_range(ranges, rowkey_data);

    ObPartitionMultiRangeSpliter spliter;
    ASSERT_EQ(OB_SUCCESS,
              spliter.build_range_array(
                  ranges, 3, *index_read_info, sstables, allocator_, multi_range_split_array));

    ASSERT_EQ(1, multi_range_split_array.count());
  }
}

}
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_partition_range_spliter.log*");
  OB_LOGGER.set_file_name("test_partition_range_spliter.log", true, true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
