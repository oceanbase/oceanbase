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
#include "lib/container/ob_iarray.h"
#include "lib/container/ob_array_array.h"
#include "storage/compaction/ob_partition_merger.h"
#include "storage/memtable/ob_memtable_interface.h"
#include "storage/ob_partition_component_factory.h"
#include "blocksstable/ob_data_file_prepare.h"
#include "blocksstable/ob_row_generate.h"
#include "observer/ob_service.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/memtable/ob_memtable_iterator.h"
#include "storage/memtable/ob_memtable_mutator.h"

#include "common/cell/ob_cell_reader.h"
#include "lib/allocator/page_arena.h"
#include "lib/container/ob_se_array.h"
#include "storage/ob_i_store.h"
#include "share/ob_srv_rpc_proxy.h"

#include "storage/ob_i_table.h"
#include "storage/blocksstable/ob_multi_version_sstable_test.h"

#include "memtable/utils_rowkey_builder.h"
#include "memtable/utils_mock_row.h"
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

class TestRangeSpliter: public ObMultiVersionSSTableTest
{
public:
  static const int64_t MAX_PARALLEL_DEGREE = 10;
  static const int64_t MAX_MACRO_CNT = 20;
  static const int64_t RANGE_MAX = 0;
  static const int64_t RANGE_MIN = 1;
  static const int64_t RANGE_NORMAL = 2;
  TestRangeSpliter() : ObMultiVersionSSTableTest("test_rangespliter") {}
  virtual ~TestRangeSpliter() {}

	void SetUp()
	{
		ObMultiVersionSSTableTest::SetUp();
	}
	void TearDown()
	{

	ObMultiVersionSSTableTest::TearDown();
		TRANS_LOG(INFO, "teardown success");
	}

  static void SetUpTestCase();
  static void TearDownTestCase();
  void build_store_range(ObIArray<ObStoreRange> &ranges, const char *rowkey_data, const bool compaction = true);
  void build_query_range(ObIArray<ObStoreRange> &ranges, const char *rowkey_data);
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

void TestRangeSpliter::build_store_range(ObIArray<ObStoreRange> &ranges, const char *rowkey_data, const bool compaction)
{
  const ObStoreRow *row = nullptr;
  const int64_t rowkey_col_cnt = TEST_ROWKEY_COLUMN_CNT;
  const int64_t mv_col_cnt = compaction ? ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt() : 0;
  ObStoreRange range, dst_range;
  ASSERT_TRUE(nullptr != rowkey_data);
  ranges.reset();
  range_iter_.reset();
  OK(range_iter_.from(rowkey_data));
  ASSERT_TRUE(range_iter_.count() > 0);

  range.get_start_key().set_min();
  range.set_left_open();
  for (int64_t i = 0; i < range_iter_.count(); i++) {
    OK(range_iter_.get_row(i, row));
    ASSERT_TRUE(nullptr != row);
    for (int64_t i = 0; i < mv_col_cnt; i++) {
      row->row_val_.cells_[rowkey_col_cnt + i].set_max_value();
    }
    range.get_end_key().assign(row->row_val_.cells_, rowkey_col_cnt + mv_col_cnt);
    range.set_right_closed();
    ASSERT_EQ(OB_SUCCESS, range.deep_copy(allocator_, dst_range));
    ASSERT_EQ(OB_SUCCESS, ranges.push_back(dst_range));
    range.reset();
    dst_range.reset();
    range.get_start_key().assign(row->row_val_.cells_, rowkey_col_cnt + mv_col_cnt);
    range.set_left_open();
  }
  range.get_end_key().set_max();
  range.set_right_open();
  ASSERT_EQ(OB_SUCCESS, range.deep_copy(allocator_, dst_range));
  ASSERT_EQ(OB_SUCCESS, ranges.push_back(dst_range));
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
      "1        var1   -9    MIN      EXIST   SCF\n"
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
      "19       var1     -9    0     EXIST   CF\n";

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
      "25       var1    -9    0     EXIST   CF\n";

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


TEST_F(TestRangeSpliter, test_single_basic)
{
  allocator_.reuse();
  ObArray<ObITable *> sstables;
  ObStoreRange split_range;
  ObPartitionRangeSpliter range_spliter;
  ObRangeSplitInfo range_split_info;
  ObArenaAllocator allocator;
  ObArray<ObStoreRange> split_ranges;
  ObTableHandleV2 handle;

  split_range.set_whole_range();
  prepare_sstable_handle(handle, 0, 20, 10);
  const ObTableReadInfo *index_read_info = &full_read_info_;

  ASSERT_EQ(OB_SUCCESS, sstables.push_back(handle.table_));
  ASSERT_EQ(OB_SUCCESS, range_spliter.get_range_split_info(sstables, *index_read_info, split_range, range_split_info));
  range_split_info.set_parallel_target(2);
  ASSERT_EQ(OB_SUCCESS, range_spliter.split_ranges(range_split_info, allocator, true, split_ranges));
  ASSERT_EQ(2, split_ranges.count());
  STORAGE_LOG(INFO, "finish split ranges", K(split_ranges));

  const char *rowkey_data =
      "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
      "10       var1   -9    MIN      EXIST   CLF\n";
  ObArray<ObStoreRange> ranges;
  build_store_range(ranges, rowkey_data);
  ASSERT_EQ(true, loop_equal_ranges(ranges, split_ranges));

  range_spliter.reset();
  ASSERT_EQ(OB_SUCCESS, range_spliter.get_range_split_info(sstables, *index_read_info, split_range, range_split_info));
  range_split_info.set_parallel_target(3);
  ASSERT_EQ(OB_SUCCESS, range_spliter.split_ranges(range_split_info, allocator, true, split_ranges));
  ASSERT_EQ(3, split_ranges.count());
  STORAGE_LOG(INFO, "finish split ranges", K(split_ranges));
  const char *rowkey_data1 =
      "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
      "10       var1   -9    MIN      EXIST   CLF\n"
      "19       var1   -9    MIN      EXIST   CLF\n";
  build_store_range(ranges, rowkey_data1);
  ASSERT_EQ(true, loop_equal_ranges(ranges, split_ranges));

  range_spliter.reset();
  ASSERT_EQ(OB_SUCCESS, range_spliter.get_range_split_info(sstables, *index_read_info, split_range, range_split_info));
  range_split_info.set_parallel_target(4);
  ASSERT_EQ(OB_SUCCESS, range_spliter.split_ranges(range_split_info, allocator, true, split_ranges));
  ASSERT_EQ(4, split_ranges.count());
  STORAGE_LOG(INFO, "finish split ranges", K(split_ranges));
  const char *rowkey_data2 =
      "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
      "7        var1   -9    MIN      EXIST   CLF\n"
      "16       var1   -9    MIN      EXIST   CLF\n"
      "22       var1   -9    MIN      EXIST   CLF\n";
  build_store_range(ranges, rowkey_data2);
  ASSERT_EQ(true, loop_equal_ranges(ranges, split_ranges));

  range_spliter.reset();
  ASSERT_EQ(OB_SUCCESS, range_spliter.get_range_split_info(sstables, *index_read_info, split_range, range_split_info));
  range_split_info.set_parallel_target(9);
  ASSERT_EQ(OB_SUCCESS, range_spliter.split_ranges(range_split_info, allocator, true, split_ranges));
  ASSERT_EQ(9, split_ranges.count());
  STORAGE_LOG(INFO, "finish split ranges", K(split_ranges));
  const char *rowkey_data3 =
      "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
      "4        var1   -9    MIN      EXIST   CLF\n"
      "7        var1   -9    MIN      EXIST   CLF\n"
      "10        var1   -9    MIN      EXIST   CLF\n"
      "13       var1   -9    MIN      EXIST   CLF\n"
      "16        var1   -9    MIN      EXIST   CLF\n"
      "19       var1   -9    MIN      EXIST   CLF\n"
      "22        var1   -9    MIN      EXIST   CLF\n"
      "25       var1   -9    MIN      EXIST   CLF\n";
  build_store_range(ranges, rowkey_data3);
  ASSERT_EQ(true, loop_equal_ranges(ranges, split_ranges));
}

TEST_F(TestRangeSpliter, test_single_multi_sstable)
{
  allocator_.reuse();
  ObArray<ObITable *> sstables;
  ObStoreRange split_range;
  ObPartitionRangeSpliter range_spliter;
  ObRangeSplitInfo range_split_info;
  ObArenaAllocator allocator;
  ObArray<ObStoreRange> split_ranges;
  ObArray<ObStoreRange> ranges;
  ObTableHandleV2 handle, handle1;

  split_range.set_whole_range();
  prepare_sstable_handle(handle, 0, 20, 10);
  ASSERT_EQ(OB_SUCCESS, sstables.push_back(handle.table_));
  prepare_sstable_handle(handle1, 5, 15, 20);
  ASSERT_EQ(OB_SUCCESS, sstables.push_back(handle1.table_));

  const ObTableReadInfo *index_read_info = &full_read_info_;

  ASSERT_EQ(OB_SUCCESS, range_spliter.get_range_split_info(sstables, *index_read_info, split_range, range_split_info));
  range_split_info.set_parallel_target(2);
  ASSERT_EQ(OB_SUCCESS, range_spliter.split_ranges(range_split_info, allocator, true, split_ranges));
  ASSERT_EQ(2, split_ranges.count());
  STORAGE_LOG(INFO, "finish split ranges", K(split_ranges));
  const char *rowkey_data =
      "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
      "17       var1   -9    MIN      EXIST   CLF\n";
  build_store_range(ranges, rowkey_data);
  ASSERT_EQ(true, loop_equal_ranges(ranges, split_ranges));

  ASSERT_EQ(OB_SUCCESS, range_spliter.get_range_split_info(sstables, *index_read_info, split_range, range_split_info));
  range_split_info.set_parallel_target(5);
  ASSERT_EQ(OB_SUCCESS, range_spliter.split_ranges(range_split_info, allocator, true, split_ranges));
  ASSERT_EQ(5, split_ranges.count());
  STORAGE_LOG(INFO, "finish split ranges", K(split_ranges));
  const char *rowkey_data1 =
      "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
      "8       var1   -9    MIN      EXIST   CLF\n"
      "14       var1   -9    MIN      EXIST   CLF\n"
      "20       var1   -9    MIN      EXIST   CLF\n"
      "25       var1   -9    MIN      EXIST   CLF\n";
  build_store_range(ranges, rowkey_data1);
  ASSERT_EQ(true, loop_equal_ranges(ranges, split_ranges));

  ObTableHandleV2 handle2;
  prepare_sstable_handle(handle2, 0, 5, 30);
  ASSERT_EQ(OB_SUCCESS, sstables.push_back(handle2.table_));

  ASSERT_EQ(OB_SUCCESS, range_spliter.get_range_split_info(sstables, *index_read_info, split_range, range_split_info));
  range_split_info.set_parallel_target(3);
  ASSERT_EQ(OB_SUCCESS, range_spliter.split_ranges(range_split_info, allocator, true, split_ranges));
  ASSERT_EQ(3, split_ranges.count());
  STORAGE_LOG(INFO, "finish split ranges", K(split_ranges));
  const char *rowkey_data2 =
      "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
      "8       var1   -9    MIN      EXIST   CLF\n"
      "19       var1   -9    MIN      EXIST   CLF\n";
  build_store_range(ranges, rowkey_data2);
  ASSERT_EQ(true, loop_equal_ranges(ranges, split_ranges));

}

TEST_F(TestRangeSpliter, test_micro_level_split)
{
  allocator_.reuse();
  ObArray<ObITable *> sstables;
  ObStoreRange split_range;
  ObPartitionRangeSpliter range_spliter;
  ObRangeSplitInfo range_split_info;
  ObArenaAllocator allocator;
  ObArray<ObStoreRange> split_ranges;
  ObTableHandleV2 handle;

  split_range.set_whole_range();
  prepare_sstable_handle(handle, 0, 2, 10, 10);
  const ObTableReadInfo *index_read_info = &full_read_info_;

  ASSERT_EQ(OB_SUCCESS, sstables.push_back(handle.table_));
  ASSERT_EQ(OB_SUCCESS, range_spliter.get_range_split_info(sstables, *index_read_info, split_range, range_split_info));
  range_split_info.set_parallel_target(4);
  ASSERT_EQ(OB_SUCCESS, range_spliter.split_ranges(range_split_info, allocator, false, split_ranges));
  ASSERT_EQ(4, split_ranges.count());
  STORAGE_LOG(INFO, "finish split ranges", K(split_ranges));
  const char *rowkey_data =
      "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
      "8       var1   -9    MIN      EXIST   CLF\n"
      "17       var1   -9    MIN      EXIST   CLF\n"
      "23       var1   -9    MIN      EXIST   CLF\n";
  ObArray<ObStoreRange> ranges;
  build_store_range(ranges, rowkey_data, false);
  ASSERT_EQ(true, loop_equal_ranges(ranges, split_ranges));

  range_spliter.reset();
  ASSERT_EQ(OB_SUCCESS, range_spliter.get_range_split_info(sstables, *index_read_info, split_range, range_split_info));
  range_split_info.set_parallel_target(7);
  ASSERT_EQ(OB_SUCCESS, range_spliter.split_ranges(range_split_info, allocator, false, split_ranges));
  ASSERT_EQ(7, split_ranges.count());
  STORAGE_LOG(INFO, "finish split ranges", K(split_ranges));
  const char *rowkey_data1 =
      "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
      "5       var1   -9    MIN      EXIST   CLF\n"
      "11       var1   -9    MIN      EXIST   CLF\n"
      "17       var1   -9    MIN      EXIST   CLF\n"
      "20       var1   -9    MIN      EXIST   CLF\n"
      "23       var1   -9    MIN      EXIST   CLF\n"
      "27       var1   -9    MIN      EXIST   CLF\n";
  build_store_range(ranges, rowkey_data1, false);
  ASSERT_EQ(true, loop_equal_ranges(ranges, split_ranges));
}

//TODO:huronghui.hrh test for mulit range

// TEST_F(TestRangeSpliter, test_mulit_basic)
// {
//   allocator_.reuse();
//   ObTablesHandle tables_handle;
//   ObStoreRange split_range;
//   ObRangeSplitInfo range_split_info;
//   ObArenaAllocator allocator;
//   ObArray<ObStoreRange> split_ranges, ranges;
//   ObArray<ObStoreRange> query_ranges;
//   ObSSTable sstable;
//   ObPartitionMultiRangeSpliter spliter;
//   ObArrayArray<ObStoreRange> ranges_array;
//   int64_t total_size = 0;
//   int64_t parallel_cnt = 0;

//   prepare_sstable_handle(sstable, 0, 20, 10);
//   ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable));

//   //normal case
//   const char *query_rowkey =
//       "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
//       "1        var1   -9    MIN      EXIST   CLF\n"
//       "6        var1   -9    MIN      EXIST   CLF\n"
//       "10       var1   -9    MIN      EXIST   CLF\n"
//       "20       var1   -9    MIN      EXIST   CLF\n";
//   build_query_range(query_ranges, query_rowkey);
//   ASSERT_EQ(OB_SUCCESS, spliter.get_multi_range_size(tables_handle, query_ranges, total_size));
//   parallel_cnt = 3;
//   ASSERT_EQ(OB_SUCCESS, spliter.get_split_multi_ranges(tables_handle, query_ranges, parallel_cnt, allocator, ranges_array));
//   STORAGE_LOG(INFO, "finish split ranges array", K(ranges_array));
//   ASSERT_EQ(3, ranges_array.count());
//   const char *rowkey_data1 =
//       "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
//       "1        var1   -9    MIN      EXIST   CLF\n"
//       "6        var1   -9    MIN      EXIST   CLF\n";
//   build_query_range(ranges, rowkey_data1);
//   ASSERT_EQ(true, loop_equal_ranges(ranges, ranges_array.at(0)));
//   const char *rowkey_data2 =
//       "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
//       "10       var1   -9    MIN      EXIST   CLF\n"
//       "17       var1   -9    MIN      EXIST   CLF\n";
//   build_query_range(ranges, rowkey_data2);
//   ASSERT_EQ(true, loop_equal_ranges(ranges, ranges_array.at(1)));
//   const char *rowkey_data3 =
//       "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
//       "17        var1   -9    MIN      EXIST   CLF\n"
//       "20        var1   -9    MIN      EXIST   CLF\n";
//   build_query_range(ranges, rowkey_data3);
//   ASSERT_EQ(true, loop_equal_ranges(ranges, ranges_array.at(2)));

//   // single task case
//   ASSERT_EQ(OB_SUCCESS, spliter.get_multi_range_size(tables_handle, query_ranges, total_size));
//   parallel_cnt = 1;
//   ASSERT_EQ(OB_SUCCESS, spliter.get_split_multi_ranges(tables_handle, query_ranges, parallel_cnt, allocator, ranges_array));
//   STORAGE_LOG(INFO, "finish split ranges array", K(ranges_array));
//   ASSERT_EQ(1, ranges_array.count());
//   ASSERT_EQ(true, loop_equal_ranges(query_ranges, ranges_array.at(0)));

//   // more task case
//   ASSERT_EQ(OB_SUCCESS, spliter.get_multi_range_size(tables_handle, query_ranges, total_size));
//   parallel_cnt = 5;
//   ASSERT_EQ(OB_SUCCESS, spliter.get_split_multi_ranges(tables_handle, query_ranges, parallel_cnt, allocator, ranges_array));
//   STORAGE_LOG(INFO, "finish split ranges array", K(ranges_array));
//   ASSERT_EQ(5, ranges_array.count());
//   const char *rowkey_data21 =
//       "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
//       "1        var1   -9    MIN      EXIST   CLF\n"
//       "5        var1   -9    MIN      EXIST   CLF\n";
//   build_query_range(ranges, rowkey_data21);
//   ASSERT_EQ(true, loop_equal_ranges(ranges, ranges_array.at(0)));
//   const char *rowkey_data22 =
//       "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
//       "5        var1   -9    MIN      EXIST   CLF\n"
//       "6        var1   -9    MIN      EXIST   CLF\n"
//       "10        var1   -9    MIN      EXIST   CLF\n"
//       "11        var1   -9    MIN      EXIST   CLF\n";
//   build_query_range(ranges, rowkey_data22);
//   ASSERT_EQ(true, loop_equal_ranges(ranges, ranges_array.at(1)));
//   const char *rowkey_data23 =
//       "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
//       "11        var1   -9    MIN      EXIST   CLF\n"
//       "16        var1   -9    MIN      EXIST   CLF\n";
//   build_query_range(ranges, rowkey_data23);
//   ASSERT_EQ(true, loop_equal_ranges(ranges, ranges_array.at(2)));
//   const char *rowkey_data24 =
//       "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
//       "16        var1   -9    MIN      EXIST   CLF\n"
//       "19        var1   -9    MIN      EXIST   CLF\n";
//   build_query_range(ranges, rowkey_data24);
//   ASSERT_EQ(true, loop_equal_ranges(ranges, ranges_array.at(3)));
//   const char *rowkey_data25 =
//       "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
//       "19       var1   -9    MIN      EXIST   CLF\n"
//       "20       var1   -9    MIN      EXIST   CLF\n";
//   build_query_range(ranges, rowkey_data25);
//   ASSERT_EQ(true, loop_equal_ranges(ranges, ranges_array.at(4)));

//   // seperate range case, 5 ranges
//   const char *query_rowkey2 =
//       "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
//       "1        var1   -9    MIN      EXIST   CLF\n"
//       "8        var1   -9    MIN      EXIST   CLF\n"
//       "10       var1   -9    MIN      EXIST   CLF\n"
//       "12       var1   -9    MIN      EXIST   CLF\n"
//       "13       var1   -9    MIN      EXIST   CLF\n"
//       "15       var1   -9    MIN      EXIST   CLF\n"
//       "16       var1   -9    MIN      EXIST   CLF\n"
//       "18       var1   -9    MIN      EXIST   CLF\n"
//       "22       var1   -9    MIN      EXIST   CLF\n"
//       "28       var1   -9    MIN      EXIST   CLF\n";
//   build_query_range(query_ranges, query_rowkey2);
//   ASSERT_EQ(OB_SUCCESS, spliter.get_multi_range_size(tables_handle, query_ranges, total_size));
//   parallel_cnt = 3;
//   ASSERT_EQ(OB_SUCCESS, spliter.get_split_multi_ranges(tables_handle, query_ranges, parallel_cnt, allocator, ranges_array));
//   STORAGE_LOG(INFO, "finish split ranges array", K(ranges_array));
//   ASSERT_EQ(3, ranges_array.count());
//   const char *rowkey_data31 =
//       "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
//       "1        var1   -9    MIN      EXIST   CLF\n"
//       "8        var1   -9    MIN      EXIST   CLF\n";
//   build_query_range(ranges, rowkey_data31);
//   ASSERT_EQ(true, loop_equal_ranges(ranges, ranges_array.at(0)));
//   const char *rowkey_data32 =
//       "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
//       "10       var1   -9    MIN      EXIST   CLF\n"
//       "12       var1   -9    MIN      EXIST   CLF\n"
//       "13        var1   -9    MIN      EXIST   CLF\n"
//       "15        var1   -9    MIN      EXIST   CLF\n";
//   build_query_range(ranges, rowkey_data32);
//   ASSERT_EQ(true, loop_equal_ranges(ranges, ranges_array.at(1)));
//   const char *rowkey_data33 =
//       "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
//       "16        var1   -9    MIN      EXIST   CLF\n"
//       "18        var1   -9    MIN      EXIST   CLF\n"
//       "22        var1   -9    MIN      EXIST   CLF\n"
//       "28        var1   -9    MIN      EXIST   CLF\n";
//   build_query_range(ranges, rowkey_data33);
//   ASSERT_EQ(true, loop_equal_ranges(ranges, ranges_array.at(2)));

//   const char *query_rowkey3 =
//       "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
//       "8        var1   -9    MIN      EXIST   CLF\n"
//       "8        var1   -9    MIN      EXIST   CLF\n";
//   build_query_range(query_ranges, query_rowkey3);
//   ASSERT_EQ(OB_SUCCESS, spliter.get_multi_range_size(tables_handle, query_ranges, total_size));
//   parallel_cnt = 3;
//   ASSERT_EQ(OB_SUCCESS, spliter.get_split_multi_ranges(tables_handle, query_ranges, parallel_cnt, allocator, ranges_array));
//   STORAGE_LOG(INFO, "finish split ranges array", K(ranges_array));
//   ASSERT_EQ(1, ranges_array.count());
// }


// TEST_F(TestRangeSpliter, test_mulit_multi)
// {
//   allocator_.reuse();
//   ObTablesHandle tables_handle;
//   ObStoreRange split_range;
//   ObRangeSplitInfo range_split_info;
//   ObArenaAllocator allocator;
//   ObArray<ObStoreRange> split_ranges, ranges;
//   ObArray<ObStoreRange> query_ranges;
//   ObSSTable sstable, sstable1, sstable2;
//   ObPartitionMultiRangeSpliter spliter;
//   ObArrayArray<ObStoreRange> ranges_array;
//   int64_t total_size = 0;
//   int64_t parallel_cnt = 0;

//   prepare_sstable_handle(sstable, 0, 20, 10);
//   ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable));
//   prepare_sstable_handle(sstable1, 5, 5, 20);
//   ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable1));
//   prepare_sstable_handle(sstable2, 10, 5, 30);
//   ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable2));

//   //normal case
//   const char *query_rowkey =
//       "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
//       "1        var1   -9    MIN      EXIST   CLF\n"
//       "6        var1   -9    MIN      EXIST   CLF\n"
//       "10       var1   -9    MIN      EXIST   CLF\n"
//       "20       var1   -9    MIN      EXIST   CLF\n";
//   build_query_range(query_ranges, query_rowkey);
//   ASSERT_EQ(OB_SUCCESS, spliter.get_multi_range_size(tables_handle, query_ranges, total_size));
//   parallel_cnt = 3;
//   ASSERT_EQ(OB_SUCCESS, spliter.get_split_multi_ranges(tables_handle, query_ranges, parallel_cnt, allocator, ranges_array));
//   STORAGE_LOG(INFO, "finish split ranges array", K(ranges_array));
//   ASSERT_EQ(3, ranges_array.count());
//   const char *rowkey_data1 =
//       "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
//       "1        var1   -9    MIN      EXIST   CLF\n"
//       "6        var1   -9    MIN      EXIST   CLF\n";
//   build_query_range(ranges, rowkey_data1);
//   ASSERT_EQ(true, loop_equal_ranges(ranges, ranges_array.at(0)));
//   const char *rowkey_data2 =
//       "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
//       "10       var1   -9    MIN      EXIST   CLF\n"
//       "14       var1   -9    MIN      EXIST   CLF\n";
//   build_query_range(ranges, rowkey_data2);
//   ASSERT_EQ(true, loop_equal_ranges(ranges, ranges_array.at(1)));
//   const char *rowkey_data3 =
//       "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
//       "14        var1   -9    MIN      EXIST   CLF\n"
//       "20        var1   -9    MIN      EXIST   CLF\n";
//   build_query_range(ranges, rowkey_data3);
//   ASSERT_EQ(true, loop_equal_ranges(ranges, ranges_array.at(2)));

// }


}
}

int main(int argc, char **argv)
{
  system("rm -rf test_partition_range_splite.log*");
  OB_LOGGER.set_file_name("test_partition_range_splite.log");
  OB_LOGGER.set_log_level("DEBUG");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
