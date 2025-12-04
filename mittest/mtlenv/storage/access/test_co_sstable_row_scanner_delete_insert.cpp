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
#include "storage/access/ob_sstable_row_multi_scanner.h"
#include "storage/column_store/ob_co_prefetcher.h"
#include "storage/column_store/ob_co_sstable_row_scanner.h"
#include "storage/tablet/ob_tablet.h"
#include "lib/container/ob_se_array.h"
#include "mtlenv/storage/blocksstable/ob_index_block_data_prepare.h"


namespace oceanbase
{
using namespace storage;
using namespace common;
namespace blocksstable
{
class TestCOSSTableRowScanner : public TestIndexBlockDataPrepare
{
public:
  static const int64_t DEFAULT_ROWS_PER_MICRO_BLOCK = 3;
  static const int64_t MIRCO_BLOCKS_PER_MACRO_BLOCK = 2;
  static const int64_t DEFAULT_ROWS_PER_MIDDLE_LAYER_MICRO_BLOCK = 3;
  static const int64_t MAX_LOOP_CNT = 10000;
  static const int64_t ROWKEY_COLUMN_NUM = 1;
  static const int64_t COLUMN_NUM = 2;
  static const int64_t DEFAULT_RANGE_CNT = 4;
  static const int64_t MICRO_BLOCK_SIZE = 500;

public:
  TestCOSSTableRowScanner();
  virtual ~TestCOSSTableRowScanner();
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
  virtual void prepare_schema();
  void prepare_co_query_param(const bool is_reverse);
  void generate_range(
      const int64_t start,
      const int64_t end,
      ObDatumRange &range);
  void add_range(
      const int64_t start,
      const int64_t end);
  void generate_ranges_case2(const bool is_reverse);
  void generate_border(
      int64_t border,
      const bool is_reverse);
  void prepare_test_case(int level_cnt);
  void prepare_discontinuous_test_case(int level_cnt, int64_t split_seed, const bool is_reverse);
  void get_blockscan_start(
      ObSSTableRowScanner<ObCOPrefetcher> *scanner,
      ObCSRowId &start,
      int32_t &range_idx,
      BlockScanState &block_scan_state,
      const bool is_reverse);
  void forward_blockscan_to_end(
      ObSSTableRowScanner<ObCOPrefetcher> *scanner,
      ObCSRowId &end,
      BlockScanState &block_scan_state,
      const bool is_reverse,
      const ObCSRowId start = 0);
  void consume_rows_by_row_store(
      ObSSTableRowScanner<ObCOPrefetcher> *scanner,
      int64_t start,
      int64_t end,
      const bool is_reverse);
  void check_iter_end(ObSSTableRowScanner<ObCOPrefetcher> *scanner);
  void pushdown_status_changed(ObSSTableRowScanner<ObCOPrefetcher> *scanner);
  int64_t get_index(int64_t i, const bool is_reverse);
  void test_delete_insert_blockscan_empty_border();
  void test_delete_insert_blockscan_consecutive_border();
  void test_delete_insert_blockscan_multi_range();
  void test_delete_insert_blockscan_skip_delete_key_end();
  void test_delete_insert_blockscan_maximum_delete_key();
  void test_delete_insert_blockscan_random();
  void test_delete_insert_blockscan_without_border_rowkey_random();

public:
  ObArenaAllocator allocator_;
  ObTableAccessParam access_param_;
  ObDatumRow start_row_;
  ObDatumRow end_row_;
  ObDatumRow border_row_;
  ObDatumRowkey border_rowkey_;
  ObDatumRange range_;
  ObSEArray<ObDatumRange, 4> ranges_;
  ObSEArray<ObCSRange, 4> range_row_ids_;
  ObBlockRowStore block_row_store_;
  ObSSTableRowScanner<ObCOPrefetcher> scanner_;
  ObSSTableRowMultiScanner<ObCOPrefetcher> multi_scanner_;
};

TestCOSSTableRowScanner::TestCOSSTableRowScanner()
  : TestIndexBlockDataPrepare("Test co sstable row scanner", compaction::ObMergeType::MAJOR_MERGE),
    block_row_store_(context_)
{
}

TestCOSSTableRowScanner::~TestCOSSTableRowScanner()
{
}

void TestCOSSTableRowScanner::SetUpTestCase()
{
  TestIndexBlockDataPrepare::SetUpTestCase();
}

void TestCOSSTableRowScanner::TearDownTestCase()
{
  TestIndexBlockDataPrepare::TearDownTestCase();
}

void TestCOSSTableRowScanner::SetUp()
{
  TestIndexBlockDataPrepare::SetUp();
  ASSERT_EQ(OB_SUCCESS, start_row_.init(allocator_, COLUMN_NUM));
  ASSERT_EQ(OB_SUCCESS, end_row_.init(allocator_, COLUMN_NUM));
  ASSERT_EQ(OB_SUCCESS, border_row_.init(allocator_, COLUMN_NUM));
}

void TestCOSSTableRowScanner::TearDown()
{
  block_row_store_.reset();
  scanner_.reset();
  multi_scanner_.reset();
  tablet_handle_.reset();
  TestIndexBlockDataPrepare::TearDown();
}

void TestCOSSTableRowScanner::prepare_co_query_param(const bool is_reverse)
{
  prepare_query_param(is_reverse);
  iter_param_.pd_storage_flag_.set_blockscan_pushdown(true);
  iter_param_.pd_storage_flag_.set_filter_pushdown(true);
  iter_param_.vectorized_enabled_ = true;
  iter_param_.is_delete_insert_ = true;
}

void TestCOSSTableRowScanner::prepare_schema()
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
  table_schema_.set_micro_index_clustered(false);
  table_schema_.set_merge_engine_type(ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
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

void TestCOSSTableRowScanner::generate_range(
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

void TestCOSSTableRowScanner::add_range(
    const int64_t start,
    const int64_t end)
{
  ObDatumRange range;
  ObCSRange row_id_range;
  row_id_range.start_row_id_ = start;
  row_id_range.end_row_id_ = end;
  generate_range(start, end, range);
  OK(ranges_.push_back(range));
  OK(range_row_ids_.push_back(row_id_range));
}

void TestCOSSTableRowScanner::generate_ranges_case2(const bool is_reverse)
{
  int64_t split_cnt = row_cnt_ / 5;
  if (!is_reverse) {
    add_range(0, split_cnt - 1);
    add_range(split_cnt + 1, split_cnt * 2 - 1);
    add_range(split_cnt * 2 + 1, split_cnt * 2 + 1);
    add_range(split_cnt * 3, split_cnt * 3 + 100);
    add_range(split_cnt * 4, split_cnt * 4 + 100);
  } else {
    add_range(get_index(split_cnt - 1, is_reverse), get_index(0, is_reverse));
    add_range(get_index(split_cnt * 2 - 1, is_reverse), get_index(split_cnt + 1, is_reverse));
    add_range(get_index(split_cnt * 2 + 1, is_reverse), get_index(split_cnt * 2 + 1, is_reverse));
    add_range(get_index(split_cnt * 3 + 100, is_reverse), get_index(split_cnt * 3, is_reverse));
    add_range(get_index(split_cnt * 4 + 100, is_reverse), get_index(split_cnt * 4, is_reverse));
  }
}

void TestCOSSTableRowScanner::generate_border(
    int64_t border,
    const bool is_reverse)
{
  // rowkey value = seed * column_type + column_id;
  // normal column value = seed * column_type + column_id;
  border = get_index(border, is_reverse);
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(border, border_row_));
  border_rowkey_.assign(border_row_.storage_datums_, ROWKEY_COLUMN_NUM);
}

void TestCOSSTableRowScanner::prepare_test_case(int level_cnt)
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

void TestCOSSTableRowScanner::prepare_discontinuous_test_case(int level_cnt, int64_t split_seed, const bool is_reverse)
{
  rows_per_mirco_block_ = DEFAULT_ROWS_PER_MICRO_BLOCK;
  mirco_blocks_per_macro_block_ = MIRCO_BLOCKS_PER_MACRO_BLOCK;
  max_row_cnt_ = rows_per_mirco_block_ * mirco_blocks_per_macro_block_ * DEFAULT_ROWS_PER_MIDDLE_LAYER_MICRO_BLOCK;
  level_cnt -= 2;
  while (level_cnt-- > 0) {
    max_row_cnt_ *=  DEFAULT_ROWS_PER_MIDDLE_LAYER_MICRO_BLOCK;
  }
  if (is_reverse) {
    split_seed = max_row_cnt_ - split_seed - 1;
  }
  prepare_discontinuous_data(MICRO_BLOCK_SIZE, split_seed);
  ASSERT_EQ(max_row_cnt_, row_cnt_);
}

void TestCOSSTableRowScanner::get_blockscan_start(
    ObSSTableRowScanner<ObCOPrefetcher> *scanner,
    ObCSRowId &start,
    int32_t &range_idx,
    BlockScanState &block_scan_state,
    const bool is_reverse)
{
  OK(scanner->get_blockscan_start(start, range_idx, block_scan_state));
  if (OB_INVALID_CS_ROW_ID != start) {
    start = get_index(start, is_reverse);
  }
}

// ROW_STORE_SCAN --> PENDING_BLOCK_SCAN / IN_END_OF_RANGE / PENDING_SWITCH
// PENDING_BLOCK_SCAN --> IN_END_OF_RANGE --> ROW_STORE_SCAN / PENDING_SWITCH
// IN_END_OF_RANGE --> ROW_STORE_SCAN / PENDING_SWITCH
// PENDING_SWITCH --> PENDING_BLOCK_SCAN / IN_END_OF_RANGE / ROW_STORE_SCAN / PENDING_SWITCH
void TestCOSSTableRowScanner::forward_blockscan_to_end(
    ObSSTableRowScanner<ObCOPrefetcher> *scanner,
    ObCSRowId &end,
    BlockScanState &block_scan_state,
    const bool is_reverse,
    const ObCSRowId start)
{
  ObCSRowId prev_end = is_reverse ? INT64_MAX : 0;
  int cnt = 0;
  while (true) {
    OK(scanner->forward_blockscan(end, block_scan_state, start));
    if (!is_reverse) {
      ASSERT_TRUE(prev_end <= end);
    } else {
      ASSERT_TRUE(prev_end >= end);
    }
    prev_end = end;
    if (++cnt > MAX_LOOP_CNT) {
      ASSERT_TRUE(false);
      break;
    }
    ASSERT_TRUE(BlockScanState::BLOCKSCAN_RANGE == block_scan_state
                 || BlockScanState::SWITCH_RANGE == block_scan_state
                 || BlockScanState::BLOCKSCAN_FINISH == block_scan_state);
    if (BlockScanState::SWITCH_RANGE == block_scan_state
         || BlockScanState::BLOCKSCAN_FINISH == block_scan_state) {
      break;
    }
  }
}

void TestCOSSTableRowScanner::consume_rows_by_row_store(
    ObSSTableRowScanner<ObCOPrefetcher> *scanner,
    int64_t start,
    int64_t end,
    const bool is_reverse)
{
  ObDatumRow row;
  OK(row.init(allocator_, COLUMN_NUM));
  const ObDatumRow *iter_row = nullptr;
  int step = is_reverse ? -1 : 1;
  for (int64_t i = start; ; i += step) {
    if (is_reverse && i < end) {
      break;
    }
    if (!is_reverse && i > end) {
      break;
    }
    OK(row_generate_.get_next_row(i, row));
    OK(scanner->inner_get_next_row(iter_row));
    ASSERT_TRUE(row == *iter_row) << i << "index: " << index << " start: " << start
                                  << " end: " << end << " iter_row: " << iter_row;
  }
}

int64_t TestCOSSTableRowScanner::get_index(int64_t i, const bool is_reverse)
{
  int64_t ret_index = i;
  if (is_reverse) {
    ret_index = row_cnt_ - 1 - i;
  }
  return ret_index;
}

void TestCOSSTableRowScanner::check_iter_end(ObSSTableRowScanner<ObCOPrefetcher> *scanner)
{
  const ObDatumRow *iter_row = nullptr;
  ASSERT_EQ(OB_ITER_END, scanner->inner_get_next_row(iter_row));
  scanner->reuse();
}

void TestCOSSTableRowScanner::pushdown_status_changed(ObSSTableRowScanner<ObCOPrefetcher> *scanner)
{
  const ObDatumRow *iter_row = nullptr;
  ASSERT_EQ(OB_PUSHDOWN_STATUS_CHANGED, scanner->inner_get_next_row(iter_row));
}

// case 1: the border rowkey is empty
void TestCOSSTableRowScanner::test_delete_insert_blockscan_empty_border()
{
  const int level_cnt = 5; // Total 486 rows.
  prepare_test_case(level_cnt);
  const int64_t start = 0;
  const int64_t end = row_cnt_ - 1;
  generate_range(start, end, range_);
  prepare_co_query_param(false/*is_reverse*/);
  OK(scanner_.init(iter_param_, context_, &sstable_, &range_));
  scanner_.block_row_store_ = &block_row_store_;
  int64_t border_id1 = 0;
  int32_t range_idx = -1;
  generate_border(border_id1, false/*is_reverse*/);
  OK(scanner_.refresh_blockscan_checker(border_rowkey_));
  ASSERT_EQ(PENDING_SWITCH, scanner_.prefetcher_.block_scan_state_);
  pushdown_status_changed(&scanner_);
  BlockScanState block_scan_state = BlockScanState::BLOCKSCAN_RANGE;
  ObCSRowId start_row_id = OB_INVALID_CS_ROW_ID;
  ObCSRowId end_row_id = OB_INVALID_CS_ROW_ID;
  get_blockscan_start(&scanner_, start_row_id, range_idx, block_scan_state, false/*is_reverse*/);
  ASSERT_EQ(-1, start_row_id);
  ASSERT_EQ(BLOCKSCAN_FINISH, block_scan_state);
  OK(scanner_.try_skip_deleted_row(end_row_id));
  ASSERT_EQ(border_id1 + 1, end_row_id);

  int64_t border_id2 = border_id1 + 10;
  generate_border(border_id2, false/*is_reverse*/);
  OK(scanner_.refresh_blockscan_checker(border_rowkey_));
  ASSERT_EQ(PENDING_SWITCH, scanner_.prefetcher_.block_scan_state_);
  block_scan_state = BlockScanState::BLOCKSCAN_RANGE;
  get_blockscan_start(&scanner_, start_row_id, range_idx, block_scan_state, false/*is_reverse*/);
  ASSERT_EQ(border_id1 + 1, start_row_id);
  ASSERT_EQ(0, range_idx);
  forward_blockscan_to_end(&scanner_, end_row_id, block_scan_state, false/*is_reverse*/, start_row_id);
  ASSERT_EQ(border_id2 - 1, end_row_id);
  ASSERT_EQ(BLOCKSCAN_FINISH, block_scan_state);
  ASSERT_EQ(ROW_STORE_SCAN, scanner_.prefetcher_.block_scan_state_);
  OK(scanner_.try_skip_deleted_row(end_row_id));
  ASSERT_EQ(border_id2 + 1, end_row_id);
}

// case 2: the delete rowkey and border rowkey are consecutive, should return iter end when get blockscan start
void TestCOSSTableRowScanner::test_delete_insert_blockscan_consecutive_border()
{
  const int level_cnt = 5; // Total 486 rows.
  prepare_test_case(level_cnt);
  const int64_t start = 0;
  const int64_t end = row_cnt_ - 1;
  generate_range(start, end, range_);
  prepare_co_query_param(false/*is_reverse*/);
  OK(scanner_.init(iter_param_, context_, &sstable_, &range_));
  scanner_.block_row_store_ = &block_row_store_;

  int64_t border_id1 = row_cnt_ / 2;
  int32_t range_idx = -1;
  ObCSRowId start_row_id = OB_INVALID_CS_ROW_ID;
  ObCSRowId end_row_id = OB_INVALID_CS_ROW_ID;
  generate_border(border_id1, false/*is_reverse*/);
  OK(scanner_.refresh_blockscan_checker(border_rowkey_));
  ASSERT_EQ(PENDING_SWITCH, scanner_.prefetcher_.block_scan_state_);
  pushdown_status_changed(&scanner_);
  BlockScanState block_scan_state = BlockScanState::BLOCKSCAN_RANGE;
  get_blockscan_start(&scanner_, start_row_id, range_idx, block_scan_state, false/*is_reverse*/);
  ASSERT_EQ(0, start_row_id);
  ASSERT_EQ(0, range_idx);
  ASSERT_EQ(BLOCKSCAN_RANGE, block_scan_state);
  forward_blockscan_to_end(&scanner_, end_row_id, block_scan_state, false/*is_reverse*/, start_row_id);
  ASSERT_EQ(border_id1 - 1, end_row_id);
  ASSERT_EQ(BLOCKSCAN_FINISH, block_scan_state);
  OK(scanner_.try_skip_deleted_row(end_row_id));
  ASSERT_EQ(border_id1 + 1, end_row_id);

  int64_t border_id2 = border_id1 + 1;
  generate_border(border_id2, false/*is_reverse*/);
  OK(scanner_.refresh_blockscan_checker(border_rowkey_));
  ASSERT_EQ(PENDING_SWITCH, scanner_.prefetcher_.block_scan_state_);
  pushdown_status_changed(&scanner_);
  block_scan_state = BlockScanState::BLOCKSCAN_RANGE;
  get_blockscan_start(&scanner_, start_row_id, range_idx, block_scan_state, false/*is_reverse*/);
  ASSERT_EQ(-1, start_row_id);
  ASSERT_EQ(BLOCKSCAN_FINISH, block_scan_state);
  OK(scanner_.try_skip_deleted_row(end_row_id));
  ASSERT_EQ(border_id2 + 1, end_row_id);

  int64_t border_id3 = border_id2 + 10;
  generate_border(border_id3, false/*is_reverse*/);
  OK(scanner_.refresh_blockscan_checker(border_rowkey_));
  ASSERT_EQ(PENDING_SWITCH, scanner_.prefetcher_.block_scan_state_);
  pushdown_status_changed(&scanner_);
  block_scan_state = BlockScanState::BLOCKSCAN_RANGE;
  get_blockscan_start(&scanner_, start_row_id, range_idx, block_scan_state, false/*is_reverse*/);
  ASSERT_EQ(border_id2 + 1, start_row_id);
  ASSERT_EQ(0, range_idx);
  ASSERT_EQ(BLOCKSCAN_RANGE, block_scan_state);
  forward_blockscan_to_end(&scanner_, end_row_id, block_scan_state, false/*is_reverse*/, start_row_id);
  ASSERT_EQ(border_id3 - 1, end_row_id);
  ASSERT_EQ(BLOCKSCAN_FINISH, block_scan_state);
  ASSERT_EQ(ROW_STORE_SCAN, scanner_.prefetcher_.block_scan_state_);
  OK(scanner_.try_skip_deleted_row(end_row_id));
  ASSERT_EQ(border_id3 + 1, end_row_id);
}

// case 3: multi range scan
void TestCOSSTableRowScanner::test_delete_insert_blockscan_multi_range()
{
  const int level_cnt = 6; // Total 1458 rows.
  prepare_test_case(level_cnt);
  generate_ranges_case2(false/*is_reverse*/);
  prepare_co_query_param(false/*is_reverse*/);
  OK(multi_scanner_.init(iter_param_, context_, &sstable_, &ranges_));
  multi_scanner_.block_row_store_ = &block_row_store_;
  int64_t border_id1 = range_row_ids_[1].end_row_id_ - 50;
  generate_border(border_id1, false/*always false*/);
  OK(multi_scanner_.refresh_blockscan_checker(border_rowkey_));
  ASSERT_EQ(PENDING_SWITCH, multi_scanner_.prefetcher_.block_scan_state_);
  pushdown_status_changed(&multi_scanner_);
  ObCSRowId start_row_id = OB_INVALID_CS_ROW_ID;
  ObCSRowId end_row_id = OB_INVALID_CS_ROW_ID;
  int32_t range_idx = -1;

  BlockScanState block_scan_state = BlockScanState::BLOCKSCAN_RANGE;
  get_blockscan_start(&multi_scanner_, start_row_id, range_idx, block_scan_state, false);
  ASSERT_EQ(range_row_ids_[0].start_row_id_, start_row_id);
  ASSERT_EQ(0, range_idx);
  forward_blockscan_to_end(&multi_scanner_, end_row_id, block_scan_state, false/*is_reverse*/, start_row_id);
  ASSERT_EQ(range_row_ids_[0].end_row_id_, end_row_id);
  ASSERT_EQ(SWITCH_RANGE, block_scan_state);
  ASSERT_EQ(PENDING_SWITCH, multi_scanner_.prefetcher_.block_scan_state_);

  block_scan_state = BlockScanState::BLOCKSCAN_RANGE;
  get_blockscan_start(&multi_scanner_, start_row_id, range_idx, block_scan_state, false/*always false*/);
  ASSERT_EQ(range_row_ids_[1].start_row_id_, start_row_id);
  ASSERT_EQ(1, range_idx);
  ASSERT_EQ(BLOCKSCAN_RANGE, block_scan_state);
  forward_blockscan_to_end(&multi_scanner_, end_row_id, block_scan_state, false/*is_reverse*/, start_row_id);
  ASSERT_EQ(border_id1 - 1, end_row_id);
  ASSERT_EQ(BLOCKSCAN_FINISH, block_scan_state);
  ASSERT_EQ(ROW_STORE_SCAN, multi_scanner_.prefetcher_.block_scan_state_);
  OK(multi_scanner_.try_skip_deleted_row(end_row_id));
  ASSERT_EQ(border_id1 + 1, end_row_id);

  int64_t border_id2 = range_row_ids_[1].end_row_id_;
  generate_border(border_id2, false/*always false*/);
  OK(multi_scanner_.refresh_blockscan_checker(border_rowkey_));
  ASSERT_EQ(PENDING_SWITCH, multi_scanner_.prefetcher_.block_scan_state_);
  pushdown_status_changed(&multi_scanner_);
  block_scan_state = BlockScanState::BLOCKSCAN_RANGE;
  get_blockscan_start(&multi_scanner_, start_row_id, range_idx, block_scan_state, false/*always false*/);
  ASSERT_EQ(border_id1 + 1, start_row_id);
  ASSERT_EQ(1, range_idx);
  ASSERT_EQ(BLOCKSCAN_RANGE, block_scan_state);
  forward_blockscan_to_end(&multi_scanner_, end_row_id, block_scan_state, false/*is_reverse*/, start_row_id);
  ASSERT_EQ(border_id2 - 1, end_row_id);
  ASSERT_EQ(BLOCKSCAN_FINISH, block_scan_state);
  ASSERT_EQ(ROW_STORE_SCAN, multi_scanner_.prefetcher_.block_scan_state_);
  OK(multi_scanner_.try_skip_deleted_row(end_row_id));
  ASSERT_EQ(border_id2 + 1, end_row_id);
}

// case 4: skip deleted row meet the end of rows, `get_blockscan_start` and `try_skip_deleted_row` should return iter end
void TestCOSSTableRowScanner::test_delete_insert_blockscan_skip_delete_key_end()
{
  rows_per_mirco_block_ = 1;
  mirco_blocks_per_macro_block_ = MIRCO_BLOCKS_PER_MACRO_BLOCK;
  max_row_cnt_ = rows_per_mirco_block_ * 2;
  prepare_data();
  ASSERT_EQ(row_cnt_, max_row_cnt_);
  const int64_t start = 0;
  const int64_t end = row_cnt_ - 1;
  generate_range(start, end, range_);
  prepare_co_query_param(false/*is_reverse*/);
  OK(scanner_.init(iter_param_, context_, &sstable_, &range_));
  scanner_.block_row_store_ = &block_row_store_;
  int32_t range_idx = -1;
  BlockScanState block_scan_state = BlockScanState::BLOCKSCAN_RANGE;
  ObCSRowId start_row_id = OB_INVALID_CS_ROW_ID;
  ObCSRowId end_row_id = OB_INVALID_CS_ROW_ID;

  int64_t border_id1 = 0;
  generate_border(border_id1, false/*is_reverse*/);
  OK(scanner_.refresh_blockscan_checker(border_rowkey_));
  ASSERT_EQ(PENDING_SWITCH, scanner_.prefetcher_.block_scan_state_);
  pushdown_status_changed(&scanner_);
  get_blockscan_start(&scanner_, start_row_id, range_idx, block_scan_state, false/*is_reverse*/);
  ASSERT_EQ(-1, start_row_id);
  ASSERT_EQ(BLOCKSCAN_FINISH, block_scan_state);
  OK(scanner_.try_skip_deleted_row(end_row_id));
  ASSERT_EQ(border_id1 + 1, end_row_id);
  int64_t border_id2 = 1;
  generate_border(border_id2, false/*is_reverse*/);
  OK(scanner_.refresh_blockscan_checker(border_rowkey_));
  ASSERT_EQ(PENDING_SWITCH, scanner_.prefetcher_.block_scan_state_);
  pushdown_status_changed(&scanner_);
  block_scan_state = BlockScanState::BLOCKSCAN_RANGE;
  get_blockscan_start(&scanner_, start_row_id, range_idx, block_scan_state, false/*is_reverse*/);
  ASSERT_EQ(1, start_row_id);
  ASSERT_EQ(BLOCKSCAN_RANGE, block_scan_state);
  forward_blockscan_to_end(&scanner_, end_row_id, block_scan_state, false/*is_reverse*/, start_row_id);
  ASSERT_EQ(BLOCKSCAN_FINISH, block_scan_state);
  ASSERT_EQ(border_id1, end_row_id);
  OK(scanner_.try_skip_deleted_row(end_row_id));
  ASSERT_EQ(border_id2 + 1, end_row_id);
  int64_t border_id3 = 2;
  generate_border(border_id3, false/*is_reverse*/);
  OK(scanner_.refresh_blockscan_checker(border_rowkey_));
  ASSERT_EQ(PENDING_SWITCH, scanner_.prefetcher_.block_scan_state_);
  pushdown_status_changed(&scanner_);
  block_scan_state = BlockScanState::BLOCKSCAN_RANGE;
  get_blockscan_start(&scanner_, start_row_id, range_idx, block_scan_state, false/*is_reverse*/);
  ASSERT_EQ(-1, start_row_id);
  ASSERT_EQ(BLOCKSCAN_FINISH, block_scan_state);
  ASSERT_EQ(OB_ITER_END, scanner_.try_skip_deleted_row(end_row_id));
  ASSERT_EQ(border_id2 + 1, end_row_id);
  check_iter_end(&scanner_);
}

// case 5: forward blockscan meets the end of rows, `get_blockscan_start` and `try_skip_deleted_row` should return iter end
void TestCOSSTableRowScanner::test_delete_insert_blockscan_maximum_delete_key()
{
  const int level_cnt = 5; // Total 486 rows.
  prepare_test_case(level_cnt);
  const int64_t start = 0;
  const int64_t end = row_cnt_ - 1;
  generate_range(start, end, range_);
  prepare_co_query_param(false/*is_reverse*/);
  OK(scanner_.init(iter_param_, context_, &sstable_, &range_));
  scanner_.block_row_store_ = &block_row_store_;
  int32_t range_idx = -1;
  BlockScanState block_scan_state = BlockScanState::BLOCKSCAN_RANGE;
  ObCSRowId start_row_id = OB_INVALID_CS_ROW_ID;
  ObCSRowId end_row_id = OB_INVALID_CS_ROW_ID;

  int64_t border_id = row_cnt_ + 1;
  generate_border(border_id, false/*is_reverse*/);
  OK(scanner_.refresh_blockscan_checker(border_rowkey_));
  ASSERT_EQ(PENDING_SWITCH, scanner_.prefetcher_.block_scan_state_);
  pushdown_status_changed(&scanner_);
  get_blockscan_start(&scanner_, start_row_id, range_idx, block_scan_state, false/*is_reverse*/);
  ASSERT_EQ(0, start_row_id);
  ASSERT_EQ(0, range_idx);
  ASSERT_EQ(BLOCKSCAN_RANGE, block_scan_state);
  forward_blockscan_to_end(&scanner_, end_row_id, block_scan_state, false/*is_reverse*/, start_row_id);
  ASSERT_EQ(end, end_row_id);
  ASSERT_EQ(SWITCH_RANGE, block_scan_state);
  ASSERT_EQ(PENDING_SWITCH, scanner_.prefetcher_.block_scan_state_);
  // after SWITCH_RANGE, `ObCOSSTableRowScanner::get_next_rows` should turn to the BEGIN state, meet BLOCKSCAN_FINISH and finally jump to the END state
  block_scan_state = BLOCKSCAN_RANGE;
  ASSERT_EQ(OB_ITER_END, scanner_.get_blockscan_start(start_row_id, range_idx, block_scan_state));
  ASSERT_EQ(SCAN_FINISH, block_scan_state);
  check_iter_end(&scanner_);
}

void TestCOSSTableRowScanner::test_delete_insert_blockscan_random()
{
  const int level_cnt = 5; // Total 486 rows.
  prepare_test_case(level_cnt);
  const int64_t start = 0;
  const int64_t end = row_cnt_ - 1;
  generate_range(start, end, range_);
  prepare_co_query_param(false/*is_reverse*/);
  const int64_t random_times = 10000;
  for (int64_t time = 0; time < random_times; ++time) {
    OK(scanner_.init(iter_param_, context_, &sstable_, &range_));
    scanner_.block_row_store_ = &block_row_store_;
    int64_t border_id = -1;
    int64_t last_border_id = border_id;
    while (border_id < end) {
      last_border_id = border_id;
      border_id = ObRandom::rand(border_id + 1, end);
      generate_border(border_id, false/*is_reverse*/);
      OK(scanner_.refresh_blockscan_checker(border_rowkey_));
      pushdown_status_changed(&scanner_);
      ASSERT_EQ(PENDING_SWITCH, scanner_.prefetcher_.block_scan_state_);
      ObCSRowId start_row_id = OB_INVALID_CS_ROW_ID;
      ObCSRowId end_row_id = OB_INVALID_CS_ROW_ID;
      int32_t range_idx = -1;
      BlockScanState block_scan_state = BlockScanState::BLOCKSCAN_RANGE;
      get_blockscan_start(&scanner_, start_row_id, range_idx, block_scan_state, false/*is_reverse*/);
      if (BLOCKSCAN_FINISH == block_scan_state) {
        OK(scanner_.try_skip_deleted_row(end_row_id));
        ASSERT_EQ(border_id + 1, end_row_id);
      } else {
        ASSERT_EQ(last_border_id + 1, start_row_id);
        ASSERT_EQ(0, range_idx);
        ASSERT_EQ(BLOCKSCAN_RANGE, block_scan_state);
        ASSERT_TRUE((PENDING_BLOCK_SCAN == scanner_.prefetcher_.block_scan_state_)
                    || (IN_END_OF_RANGE == scanner_.prefetcher_.block_scan_state_)
                    || (ROW_STORE_SCAN == scanner_.prefetcher_.block_scan_state_/*the blockscan finishes*/));
        forward_blockscan_to_end(&scanner_, end_row_id, block_scan_state, false/*is_reverse*/, start_row_id);
        ASSERT_EQ(border_id - 1, end_row_id);
        ASSERT_EQ(BLOCKSCAN_FINISH, block_scan_state);
        ASSERT_EQ(ROW_STORE_SCAN, scanner_.prefetcher_.block_scan_state_);
        OK(scanner_.try_skip_deleted_row(end_row_id));
        ASSERT_EQ(border_id + 1, end_row_id);
      }
    }
    check_iter_end(&scanner_);
  }
}

void TestCOSSTableRowScanner::test_delete_insert_blockscan_without_border_rowkey_random()
{
  const int level_cnt = 5; // Total 486 rows, without the row of border rowkey
  prepare_co_query_param(false/*is_reverse*/);
  ObCSRowId start_row_id = OB_INVALID_CS_ROW_ID;
  ObCSRowId end_row_id = OB_INVALID_CS_ROW_ID;
  int32_t range_idx = -1;
  const int64_t split_seed = ObRandom::rand(1, 484);
  prepare_discontinuous_test_case(level_cnt, split_seed, false/*is_reverse*/);
  const int64_t start = 0;
  const int64_t end = row_cnt_;
  generate_range(start, end, range_);
  OK(scanner_.init(iter_param_, context_, &sstable_, &range_));
  scanner_.block_row_store_ = &block_row_store_;
  int64_t border_id = split_seed;
  generate_border(border_id, false/*is_reverse*/);
  OK(scanner_.refresh_blockscan_checker(border_rowkey_));
  pushdown_status_changed(&scanner_);
  ASSERT_EQ(PENDING_SWITCH, scanner_.prefetcher_.block_scan_state_);
  BlockScanState block_scan_state = BlockScanState::BLOCKSCAN_RANGE;
  get_blockscan_start(&scanner_, start_row_id, range_idx, block_scan_state, false/*is_reverse*/);
  ASSERT_EQ(0, start_row_id);
  ASSERT_EQ(BLOCKSCAN_RANGE, block_scan_state);
  ASSERT_TRUE((PENDING_BLOCK_SCAN == scanner_.prefetcher_.block_scan_state_)
              || (IN_END_OF_RANGE == scanner_.prefetcher_.block_scan_state_)
              || (ROW_STORE_SCAN == scanner_.prefetcher_.block_scan_state_));
  forward_blockscan_to_end(&scanner_, end_row_id, block_scan_state, false/*is_reverse*/, start_row_id);
  ASSERT_EQ(border_id - 1, end_row_id);
  ASSERT_EQ(BLOCKSCAN_FINISH, block_scan_state);
  ASSERT_EQ(ROW_STORE_SCAN, scanner_.prefetcher_.block_scan_state_);
  OK(scanner_.try_skip_deleted_row(end_row_id));
  ASSERT_EQ(border_id, end_row_id);

  border_id = row_cnt_;
  generate_border(border_id, false/*is_reverse*/);
  OK(scanner_.refresh_blockscan_checker(border_rowkey_));
  pushdown_status_changed(&scanner_);
  ASSERT_EQ(PENDING_SWITCH, scanner_.prefetcher_.block_scan_state_);
  block_scan_state = BlockScanState::BLOCKSCAN_RANGE;
  get_blockscan_start(&scanner_, start_row_id, range_idx, block_scan_state, false/*is_reverse*/);
  ASSERT_EQ(split_seed, start_row_id);
  ASSERT_EQ(BLOCKSCAN_RANGE, block_scan_state);
  ASSERT_TRUE((PENDING_BLOCK_SCAN == scanner_.prefetcher_.block_scan_state_)
              || (IN_END_OF_RANGE == scanner_.prefetcher_.block_scan_state_)
              || (ROW_STORE_SCAN == scanner_.prefetcher_.block_scan_state_));
  forward_blockscan_to_end(&scanner_, end_row_id, block_scan_state, false/*is_reverse*/, start_row_id);
  ASSERT_EQ(border_id - 2, end_row_id);
  ASSERT_EQ(BLOCKSCAN_FINISH, block_scan_state);
  ASSERT_EQ(ROW_STORE_SCAN, scanner_.prefetcher_.block_scan_state_);
  OK(scanner_.try_skip_deleted_row(end_row_id));
  ASSERT_EQ(border_id, end_row_id);
  check_iter_end(&scanner_);
}

TEST_F(TestCOSSTableRowScanner, test_delete_insert_blockscan_empty_border)
{
  test_delete_insert_blockscan_empty_border();
}

TEST_F(TestCOSSTableRowScanner, test_delete_insert_blockscan_consecutive_border)
{
  test_delete_insert_blockscan_consecutive_border();
}

TEST_F(TestCOSSTableRowScanner, test_delete_insert_blockscan_multi_range)
{
  test_delete_insert_blockscan_multi_range();
}

TEST_F(TestCOSSTableRowScanner, test_delete_insert_blockscan_skip_delete_key_end)
{
  test_delete_insert_blockscan_skip_delete_key_end();
}

TEST_F(TestCOSSTableRowScanner, test_delete_insert_blockscan_maximum_delete_key)
{
  test_delete_insert_blockscan_maximum_delete_key();
}

TEST_F(TestCOSSTableRowScanner, test_delete_insert_blockscan_random)
{
  test_delete_insert_blockscan_random();
}

TEST_F(TestCOSSTableRowScanner, test_delete_insert_blockscan_without_border_rowkey_random)
{
  test_delete_insert_blockscan_without_border_rowkey_random();
}
}
}

int main(int argc, char **argv)
{
  system("rm -f test_co_sstable_row_scanner_delete_insert.log*");
  OB_LOGGER.set_file_name("test_co_sstable_row_scanner_delete_insert.log", true, true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
