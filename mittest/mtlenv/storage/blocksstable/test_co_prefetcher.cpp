/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#define USING_LOG_PREFIX STORAGE
#include <gtest/gtest.h>
#define private public
#define protected public

#include "lib/random/ob_random.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/column_store/ob_co_prefetcher.h"
#include "ob_index_block_data_prepare.h"


namespace oceanbase
{
using namespace storage;
using namespace common;
namespace blocksstable
{
class TestCOPrefetcher : public TestIndexBlockDataPrepare
{
public:
  TestCOPrefetcher();
  virtual ~TestCOPrefetcher();
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
  virtual void prepare_schema();

  void generate_range(
      const int64_t start,
      const int64_t end,
      ObDatumRange &range);
  void generate_border(const int64_t border);
  void prepare_test_case(int level_cnt);
  void prepare_one_block_test_case();
  void clear_test_case();
  void try_prefetch();
  static const int64_t DEFAULT_ROWS_PER_MICRO_BLOCK = 3;
  static const int64_t MIRCO_BLOCKS_PER_MACRO_BLOCK = 2;
  static const int64_t DEFAULT_ROWS_PER_MIDDLE_LAYER_MICRO_BLOCK = 3;
  static const int64_t MAX_LOOP_CNT = 10000;
  static const int64_t ROWKEY_COLUMN_NUM = 1;
  static const int64_t COLUMN_NUM = 2;
  static const int64_t MICRO_BLOCK_SIZE = 500;

public:
  ObArenaAllocator allocator_;
  ObTableAccessParam access_param_;
  ObDatumRow start_row_;
  ObDatumRow end_row_;
  ObDatumRow border_row_;
  ObDatumRowkey border_rowkey_;
  ObDatumRange range_;
  ObCOPrefetcher co_prefetcher_;
};

TestCOPrefetcher::TestCOPrefetcher()
  : TestIndexBlockDataPrepare("Test co prefetcher", compaction::ObMergeType::MAJOR_MERGE)
{
}

TestCOPrefetcher::~TestCOPrefetcher()
{
}

void TestCOPrefetcher::SetUpTestCase()
{
  TestIndexBlockDataPrepare::SetUpTestCase();
}

void TestCOPrefetcher::TearDownTestCase()
{
  TestIndexBlockDataPrepare::TearDownTestCase();
}

void TestCOPrefetcher::SetUp()
{
  TestIndexBlockDataPrepare::SetUp();
  ASSERT_EQ(OB_SUCCESS, start_row_.init(allocator_, COLUMN_NUM));
  ASSERT_EQ(OB_SUCCESS, end_row_.init(allocator_, COLUMN_NUM));
  ASSERT_EQ(OB_SUCCESS, border_row_.init(allocator_, COLUMN_NUM));
}

void TestCOPrefetcher::TearDown()
{
  tablet_handle_.reset();
  TestIndexBlockDataPrepare::TearDown();
}

void TestCOPrefetcher::prepare_schema()
{
  ObColumnSchemaV2 column;
  const int64_t rowkey_column_num = ROWKEY_COLUMN_NUM;
  const int64_t column_num = COLUMN_NUM;
  // Init table schema
  uint64_t table_id = TEST_TABLE_ID;
  table_schema_.reset();
  ASSERT_EQ(OB_SUCCESS, table_schema_.set_table_name("test_index_block"));
  table_schema_.set_tenant_id(1);
  table_schema_.set_tablegroup_id(1);
  table_schema_.set_database_id(1);
  table_schema_.set_table_id(table_id);
  table_schema_.set_rowkey_column_num(rowkey_column_num);
  table_schema_.set_max_used_column_id(column_num);
  table_schema_.set_block_size(2 * 1024);
  table_schema_.set_compress_func_name("none");
  table_schema_.set_row_store_type(row_store_type_);
  table_schema_.set_storage_format_version(OB_STORAGE_FORMAT_VERSION_V4);

  index_schema_.reset();

  // Init column
  char name[OB_MAX_FILE_NAME_LENGTH];
  memset(name, 0, sizeof(name));
  for(int64_t i = 0; i < column_num; ++i) {
    ObObjType obj_type = ObIntType;
    column.reset();
    column.set_table_id(TEST_TABLE_ID);
    column.set_column_id(i + OB_APP_MIN_COLUMN_ID);
    sprintf(name, "test%020ld", i);
    ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
    column.set_data_type(obj_type);
    column.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    column.set_data_length(1);
    if (0 == i) {
      column.set_rowkey_position(1);
    } else {
      column.set_rowkey_position(0);
    }
    ASSERT_EQ(OB_SUCCESS, table_schema_.add_column(column));
  }
}

void TestCOPrefetcher::generate_range(
    const int64_t start,
    const int64_t end,
    ObDatumRange &range)
{
  ObDatumRowkey tmp_rowkey;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(start, start_row_));
  tmp_rowkey.assign(start_row_.storage_datums_, ROWKEY_COLUMN_NUM);
  ASSERT_EQ(OB_SUCCESS, tmp_rowkey.deep_copy(range.start_key_, allocator_));
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(end, end_row_));
  tmp_rowkey.assign(end_row_.storage_datums_, ROWKEY_COLUMN_NUM);
  ASSERT_EQ(OB_SUCCESS, tmp_rowkey.deep_copy(range.end_key_, allocator_));
  range.border_flag_.set_inclusive_start();
  range.border_flag_.set_inclusive_end();
}

void TestCOPrefetcher::generate_border(const int64_t border)
{
  // rowkey value = seed * column_type + column_id;
  // normal column value = seed * column_type + column_id;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(border, border_row_));
  border_rowkey_.assign(border_row_.storage_datums_, ROWKEY_COLUMN_NUM);
}

void TestCOPrefetcher::prepare_test_case(int level_cnt)
{
  rows_per_mirco_block_ = DEFAULT_ROWS_PER_MICRO_BLOCK;
  mirco_blocks_per_macro_block_ = MIRCO_BLOCKS_PER_MACRO_BLOCK;
  max_row_cnt_ = rows_per_mirco_block_ * mirco_blocks_per_macro_block_ * DEFAULT_ROWS_PER_MIDDLE_LAYER_MICRO_BLOCK;
  level_cnt -= 2;
  while (level_cnt-- > 0) {
    max_row_cnt_ *=  DEFAULT_ROWS_PER_MIDDLE_LAYER_MICRO_BLOCK;
  }
  if (is_cg_data_) {
    prepare_cg_data();
  } else {
    prepare_data(MICRO_BLOCK_SIZE);
  }
  ASSERT_EQ(max_row_cnt_, row_cnt_);
}

void TestCOPrefetcher::prepare_one_block_test_case()
{
  rows_per_mirco_block_ = DEFAULT_ROWS_PER_MICRO_BLOCK;
  mirco_blocks_per_macro_block_ = MIRCO_BLOCKS_PER_MACRO_BLOCK;
  max_row_cnt_ = rows_per_mirco_block_;
  if (is_cg_data_) {
    prepare_cg_data();
  } else {
    prepare_data(MICRO_BLOCK_SIZE);
  }
  ASSERT_EQ(max_row_cnt_, row_cnt_);
}

void TestCOPrefetcher::clear_test_case()
{
  TestIndexBlockDataPrepare::TearDown();
}

void TestCOPrefetcher::try_prefetch()
{
  int cnt = 0;
  OK(co_prefetcher_.prefetch());
  while (co_prefetcher_.read_wait()) {
    OK(co_prefetcher_.prefetch());
    if (++cnt > MAX_LOOP_CNT) {
      ASSERT_TRUE(false);
      break;
    }
  }
}

TEST_F(TestCOPrefetcher, test_basic_case)
{
  const int level_cnt = 5; // Total 486 rows.
  prepare_test_case(level_cnt);
  int64_t start = 0;
  int64_t end = max_row_cnt_ - 1;
  generate_range(start, end, range_);
  bool is_reverse_scan = false;
  prepare_query_param(is_reverse_scan);
  int iter_type = ObStoreRowIterator::IteratorScan;
  OK(co_prefetcher_.init(iter_type, sstable_, iter_param_, context_, &range_));
  try_prefetch();
  int64_t border_id1 = max_row_cnt_ / 2;
  generate_border(border_id1);
  // 1. Switch to columnar scan.
  OK(co_prefetcher_.refresh_blockscan_checker_for_column_store(1, border_rowkey_));

  ASSERT_EQ(1, co_prefetcher_.get_cur_level_of_block_scan());
  ASSERT_EQ(rows_per_mirco_block_, co_prefetcher_.block_scan_start_row_id_);
  ASSERT_TRUE(OB_INVALID_CS_ROW_ID != co_prefetcher_.block_scan_border_row_id_);
  ASSERT_EQ(MicroDataBlockScanState::PENDING_BLOCK_SCAN, co_prefetcher_.block_scan_state_);
  ObCSRowId prev_row_id = co_prefetcher_.block_scan_border_row_id_;
  int cnt = 0;
  while (true) {
    OK(co_prefetcher_.prefetch());
    if (MicroDataBlockScanState::IN_END_OF_RANGE == co_prefetcher_.block_scan_state_) {
      ASSERT_TRUE(OB_INVALID_CS_ROW_ID != co_prefetcher_.block_scan_border_row_id_);
      ASSERT_TRUE(prev_row_id <= co_prefetcher_.block_scan_border_row_id_);
      ASSERT_TRUE(border_id1 > co_prefetcher_.block_scan_border_row_id_);
      ASSERT_TRUE((border_id1 - 1 - co_prefetcher_.block_scan_border_row_id_) <= rows_per_mirco_block_);
      break;
    } else if (MicroDataBlockScanState::PENDING_BLOCK_SCAN == co_prefetcher_.block_scan_state_) {
      ASSERT_TRUE(OB_INVALID_CS_ROW_ID != co_prefetcher_.block_scan_border_row_id_);
      ASSERT_TRUE(prev_row_id <= co_prefetcher_.block_scan_border_row_id_);
      prev_row_id = co_prefetcher_.block_scan_border_row_id_;
    } else {
      break;
    }
    if (++cnt > MAX_LOOP_CNT) {
      ASSERT_TRUE(false);
      break;
    }
  }

  ASSERT_EQ(-1, co_prefetcher_.get_cur_level_of_block_scan());
  co_prefetcher_.block_scan_state_ = MicroDataBlockScanState::ROW_STORE_SCAN;
  int border_id2 = max_row_cnt_ / 6 * 5;
  generate_border(border_id2);
  try_prefetch();
  // 2. Switch to columnar scan again.
  OK(co_prefetcher_.refresh_blockscan_checker_for_column_store(co_prefetcher_.cur_micro_data_fetch_idx_ + 1, border_rowkey_));
  ASSERT_EQ(border_id1 + rows_per_mirco_block_, co_prefetcher_.block_scan_start_row_id_);
  ASSERT_TRUE(OB_INVALID_CS_ROW_ID != co_prefetcher_.block_scan_border_row_id_);
  ASSERT_EQ(MicroDataBlockScanState::PENDING_BLOCK_SCAN, co_prefetcher_.block_scan_state_);
  prev_row_id = co_prefetcher_.block_scan_border_row_id_;
  cnt = 0;
  while (true) {
    OK(co_prefetcher_.prefetch());
    if (MicroDataBlockScanState::IN_END_OF_RANGE == co_prefetcher_.block_scan_state_) {
      ASSERT_TRUE(OB_INVALID_CS_ROW_ID != co_prefetcher_.block_scan_border_row_id_);
      ASSERT_TRUE(prev_row_id <= co_prefetcher_.block_scan_border_row_id_);
      ASSERT_TRUE(border_id2 >= co_prefetcher_.block_scan_border_row_id_);
      ASSERT_TRUE((border_id2 - co_prefetcher_.block_scan_border_row_id_) <= rows_per_mirco_block_);
      break;
    } else if (MicroDataBlockScanState::PENDING_BLOCK_SCAN == co_prefetcher_.block_scan_state_) {
      ASSERT_TRUE(OB_INVALID_CS_ROW_ID != co_prefetcher_.block_scan_border_row_id_);
      ASSERT_TRUE(prev_row_id <= co_prefetcher_.block_scan_border_row_id_);
      prev_row_id = co_prefetcher_.block_scan_border_row_id_;
    } else {
      break;
    }
    if (++cnt > MAX_LOOP_CNT) {
      ASSERT_TRUE(false);
      break;
    }
  }

  // 3. Consumed prefetcher
  cnt = 0;
  ASSERT_EQ(-1, co_prefetcher_.get_cur_level_of_block_scan());
  co_prefetcher_.block_scan_state_ = MicroDataBlockScanState::ROW_STORE_SCAN;
  while (true) {
    OK(co_prefetcher_.prefetch());
    if (++cnt > MAX_LOOP_CNT) {
      ASSERT_TRUE(false);
      break;
    }
    if (co_prefetcher_.is_prefetch_end_) {
      break;
    }
    if (co_prefetcher_.read_wait()) {
      continue;
    }
    ++co_prefetcher_.cur_micro_data_fetch_idx_;
  }
  destroy_query_param();
}

TEST_F(TestCOPrefetcher, test_one_micro_block_case)
{
  // Only one data micro block.
  prepare_one_block_test_case();
  int64_t start = 0;
  int64_t end = max_row_cnt_ - 1;
  generate_range(start, end, range_);
  bool is_reverse_scan = false;
  prepare_query_param(is_reverse_scan);
  int iter_type = ObStoreRowIterator::IteratorScan;
  OK(co_prefetcher_.init(iter_type, sstable_, iter_param_, context_, &range_));
  try_prefetch();
  int64_t border_id1 = max_row_cnt_ + 1;
  generate_border(border_id1);
  OK(co_prefetcher_.refresh_blockscan_checker_for_column_store(1, border_rowkey_));
  ASSERT_EQ(2, co_prefetcher_.index_tree_height_);
  ASSERT_EQ(-1, co_prefetcher_.get_cur_level_of_block_scan());
  ASSERT_EQ(OB_INVALID_CS_ROW_ID, co_prefetcher_.block_scan_start_row_id_);
  ASSERT_EQ(OB_INVALID_CS_ROW_ID, co_prefetcher_.block_scan_border_row_id_);
  ASSERT_EQ(MicroDataBlockScanState::ROW_STORE_SCAN, co_prefetcher_.block_scan_state_);
}

TEST_F(TestCOPrefetcher, test_all_columnar_scan_case)
{
  // Border is max, scan all by columnar store.
  const int level_cnt = 5; // Total 486 rows.
  prepare_test_case(level_cnt);
  int64_t start = 0;
  int64_t end = max_row_cnt_ - 1;
  generate_range(start, end, range_);
  bool is_reverse_scan = false;
  prepare_query_param(is_reverse_scan);
  int iter_type = ObStoreRowIterator::IteratorScan;
  OK(co_prefetcher_.init(iter_type, sstable_, iter_param_, context_, &range_));
  try_prefetch();
  int64_t border_id1 = max_row_cnt_;
  generate_border(border_id1);
  OK(co_prefetcher_.refresh_blockscan_checker_for_column_store(1, border_rowkey_));
  ASSERT_EQ(1, co_prefetcher_.get_cur_level_of_block_scan());
  ASSERT_EQ(rows_per_mirco_block_, co_prefetcher_.block_scan_start_row_id_);
  ASSERT_TRUE(OB_INVALID_CS_ROW_ID != co_prefetcher_.block_scan_border_row_id_);
  ASSERT_EQ(MicroDataBlockScanState::PENDING_BLOCK_SCAN, co_prefetcher_.block_scan_state_);
  ObCSRowId prev_row_id = co_prefetcher_.block_scan_border_row_id_;
  int cnt = 0;
  while (true) {
    OK(co_prefetcher_.prefetch());
    if (MicroDataBlockScanState::IN_END_OF_RANGE == co_prefetcher_.block_scan_state_) {
      ASSERT_TRUE(OB_INVALID_CS_ROW_ID != co_prefetcher_.block_scan_border_row_id_);
      ASSERT_TRUE(prev_row_id <= co_prefetcher_.block_scan_border_row_id_);
      ASSERT_TRUE(border_id1 > co_prefetcher_.block_scan_border_row_id_);
      // In range border, so co_prefetcher_.block_scan_border_row_id_ is not max_row_cnt_ - 1.
      ASSERT_EQ(max_row_cnt_ - 1 - rows_per_mirco_block_, co_prefetcher_.block_scan_border_row_id_);
      break;
    } else if (MicroDataBlockScanState::PENDING_BLOCK_SCAN == co_prefetcher_.block_scan_state_) {
      ASSERT_TRUE(OB_INVALID_CS_ROW_ID != co_prefetcher_.block_scan_border_row_id_);
      ASSERT_TRUE(prev_row_id <= co_prefetcher_.block_scan_border_row_id_);
      prev_row_id = co_prefetcher_.block_scan_border_row_id_;
    } else {
      break;
    }
    if (++cnt > MAX_LOOP_CNT) {
      ASSERT_TRUE(false);
      break;
    }
  }
  ASSERT_EQ(-1, co_prefetcher_.get_cur_level_of_block_scan());
  co_prefetcher_.block_scan_state_ = MicroDataBlockScanState::ROW_STORE_SCAN;
  try_prefetch();
  ASSERT_TRUE(co_prefetcher_.is_prefetch_end_);
}

}
}

int main(int argc, char **argv)
{
  system("rm -f test_co_prefetcher.log*");
  OB_LOGGER.set_file_name("test_co_prefetcher.log", true, true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
