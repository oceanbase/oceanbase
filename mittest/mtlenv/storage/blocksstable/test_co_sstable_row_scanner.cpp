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
#include "ob_index_block_data_prepare.h"
#include "lib/container/ob_se_array.h"


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
  void generate_ranges(int64_t rang_cnt = DEFAULT_RANGE_CNT);
  void generate_ranges_case1(const bool is_reverse);
  void generate_ranges_case2(const bool is_reverse);
  void generate_border(
      int64_t border,
      const bool is_reverse);
  void prepare_test_case(int level_cnt);
  void prepare_one_block_test_case();
  void clear_test_case();
  void forward_blockscan_to_end(
      ObSSTableRowScanner<ObCOPrefetcher> *scanner,
      ObCSRowId &end,
      BlockScanState &block_scan_state,
      const bool is_reverse);
  void consume_rows_by_row_store(
      ObSSTableRowScanner<ObCOPrefetcher> *scanner,
      int64_t start,
      int64_t end,
      const bool is_reverse);
  void check_iter_end(ObSSTableRowScanner<ObCOPrefetcher> *scanner);
  void pushdown_status_changed(ObSSTableRowScanner<ObCOPrefetcher> *scanner);
  int64_t get_index(int64_t i, const bool is_reverse);
  void test_row_scan_only(const bool is_reverse);
  void test_row_scan_and_column_scan(const bool is_reverse);
  void test_row_scan_and_column_scan_with_multi_range1();
  void test_reverse_row_scan_and_column_scan_with_multi_range1();
  void test_row_scan_and_column_scan_with_multi_range2();
  void test_reverse_row_scan_and_column_scan_with_multi_range2();

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
  ObCOPrefetcher co_prefetcher_;
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
  tablet_handle_.reset();
  TestIndexBlockDataPrepare::TearDown();
}

void TestCOSSTableRowScanner::prepare_co_query_param(const bool is_reverse)
{
  prepare_query_param(is_reverse);
  iter_param_.pd_storage_flag_.set_blockscan_pushdown(true);
  iter_param_.pd_storage_flag_.set_filter_pushdown(true);
  iter_param_.vectorized_enabled_ = true;
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

void TestCOSSTableRowScanner::generate_ranges(int64_t range_cnt)
{
  int64_t split_cnt = row_cnt_ / range_cnt;
  for (int i = 0; i < range_cnt; ++i) {
    ObCSRange row_id_range;
    ObDatumRange range;
    int64_t start = i * split_cnt;
    int64_t end = start + split_cnt - 1;
    row_id_range.start_row_id_ = start;
    row_id_range.end_row_id_ = end;
    generate_range(start, end, range);
    OK(ranges_.push_back(range));
    OK(range_row_ids_.push_back(row_id_range));
  }
}

void TestCOSSTableRowScanner::generate_ranges_case1(const bool is_reverse)
{
  if (!is_reverse) {
    add_range(get_index(0, is_reverse), get_index(row_cnt_ - 2, is_reverse));
    add_range(get_index(row_cnt_ - 1, is_reverse), get_index(row_cnt_ - 1, is_reverse));
  } else {
    add_range(get_index(row_cnt_ - 2, is_reverse), get_index(0, is_reverse));
    add_range(get_index(row_cnt_ - 1, is_reverse), get_index(row_cnt_ - 1, is_reverse));
  }
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

void TestCOSSTableRowScanner::prepare_one_block_test_case()
{
  rows_per_mirco_block_ = DEFAULT_ROWS_PER_MICRO_BLOCK;
  mirco_blocks_per_macro_block_ = MIRCO_BLOCKS_PER_MACRO_BLOCK;
  max_row_cnt_ = rows_per_mirco_block_;
}

void TestCOSSTableRowScanner::clear_test_case()
{
  TestIndexBlockDataPrepare::TearDown();
}

// ROW_STORE_SCAN --> PENDING_BLOCK_SCAN / IN_END_OF_RANGE / PENDING_SWITCH
// PENDING_BLOCK_SCAN --> IN_END_OF_RANGE --> ROW_STORE_SCAN / PENDING_SWITCH
// IN_END_OF_RANGE --> ROW_STORE_SCAN / PENDING_SWITCH
// PENDING_SWITCH --> PENDING_BLOCK_SCAN / IN_END_OF_RANGE / ROW_STORE_SCAN / PENDING_SWITCH
void TestCOSSTableRowScanner::forward_blockscan_to_end(
    ObSSTableRowScanner<ObCOPrefetcher> *scanner,
    ObCSRowId &end,
    BlockScanState &block_scan_state,
    const bool is_reverse)
{
  ObCSRowId prev_end = is_reverse ? INT64_MAX : 0;
  int cnt = 0;
  while (true) {
    OK(scanner->forward_blockscan(end, block_scan_state, 0));
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

void TestCOSSTableRowScanner::test_row_scan_only(const bool is_reverse)
{
  const int level_cnt = 5; // Total 486 rows.
  prepare_test_case(level_cnt);
  int64_t start = 0;
  int64_t end = row_cnt_ - 1;
  generate_range(start, end, range_);
  prepare_co_query_param(is_reverse);
  OK(scanner_.init(iter_param_, context_, &sstable_, &range_));
  scanner_.block_row_store_ = &block_row_store_;
  if (is_reverse) {
    consume_rows_by_row_store(&scanner_, end, start, is_reverse);
  } else {
    consume_rows_by_row_store(&scanner_, start, end, is_reverse);
  }
  check_iter_end(&scanner_);
}

void TestCOSSTableRowScanner::test_row_scan_and_column_scan(const bool is_reverse)
{
  const int level_cnt = 5; // Total 486 rows.
  prepare_test_case(level_cnt);
  int64_t start = 0;
  int64_t end = row_cnt_ - 1;
  generate_range(start, end, range_);
  prepare_co_query_param(is_reverse);
  OK(scanner_.init(iter_param_, context_, &sstable_, &range_));
  scanner_.block_row_store_ = &block_row_store_;

  if (!is_reverse) {
    consume_rows_by_row_store(&scanner_, start, start, is_reverse);
  } else {
    consume_rows_by_row_store(&scanner_, end, end, is_reverse);
  }

  int64_t border_id1 = row_cnt_ / 2;
  generate_border(border_id1, is_reverse);
  scanner_.refresh_blockscan_checker(border_rowkey_);

  pushdown_status_changed(&scanner_);

  ObCSRowId start_row_id;
  int32_t range_idx;
  BlockScanState block_scan_state = BlockScanState::BLOCKSCAN_RANGE;
  OK(scanner_.get_blockscan_start(start_row_id, range_idx, block_scan_state));
  start_row_id = get_index(start_row_id, is_reverse);
  ASSERT_EQ(1, start_row_id);
  ASSERT_EQ(0, range_idx);
  ASSERT_EQ(BLOCKSCAN_RANGE, block_scan_state);

  ObCSRowId end_row_id;
  forward_blockscan_to_end(&scanner_, end_row_id, block_scan_state, is_reverse);
  end_row_id = get_index(end_row_id, is_reverse);
  ASSERT_EQ(border_id1 - 1, end_row_id);
  ASSERT_EQ(BLOCKSCAN_FINISH, block_scan_state);

  if (!is_reverse) {
    consume_rows_by_row_store(&scanner_, border_id1, end, is_reverse);
  } else {
    consume_rows_by_row_store(&scanner_, get_index(border_id1, is_reverse), start, is_reverse);
  }
  check_iter_end(&scanner_);
}

void TestCOSSTableRowScanner::test_row_scan_and_column_scan_with_multi_range1()
{
  const int level_cnt = 6; // Total 1458 rows.
  const bool is_reverse = false;
  prepare_test_case(level_cnt);
  generate_ranges_case1(is_reverse);
  prepare_co_query_param(is_reverse);
  OK(multi_scanner_.init(iter_param_, context_, &sstable_, &ranges_));
  multi_scanner_.block_row_store_ = &block_row_store_;
  consume_rows_by_row_store(&multi_scanner_, range_row_ids_[0].start_row_id_,
                             range_row_ids_[0].start_row_id_, is_reverse);
  int64_t border_id1 = range_row_ids_[1].start_row_id_ + 1;
  generate_border(border_id1, is_reverse);

  multi_scanner_.refresh_blockscan_checker(border_rowkey_);

  pushdown_status_changed(&multi_scanner_);

  ObCSRowId start_row_id;
  ObCSRowId end_row_id;
  int32_t range_idx;
  BlockScanState block_scan_state = BlockScanState::BLOCKSCAN_RANGE;
  OK(multi_scanner_.get_blockscan_start(start_row_id, range_idx, block_scan_state));
  start_row_id = get_index(start_row_id, is_reverse);
  ASSERT_EQ(1, start_row_id);
  ASSERT_EQ(0, range_idx);
  ASSERT_EQ(BLOCKSCAN_RANGE, block_scan_state);

  forward_blockscan_to_end(&multi_scanner_, end_row_id, block_scan_state, is_reverse);
  end_row_id = get_index(end_row_id, is_reverse);
  ASSERT_EQ(range_row_ids_[0].end_row_id_, end_row_id);
  ASSERT_EQ(SWITCH_RANGE, block_scan_state);
  block_scan_state = BlockScanState::BLOCKSCAN_RANGE;
  OK(multi_scanner_.get_blockscan_start(start_row_id, range_idx, block_scan_state));
  start_row_id = get_index(start_row_id, is_reverse);
  ASSERT_EQ(range_row_ids_[1].start_row_id_, start_row_id);
  ASSERT_EQ(1, range_idx);
  ASSERT_EQ(ROW_STORE_SCAN, multi_scanner_.prefetcher_.block_scan_state_);
  ASSERT_EQ(BLOCKSCAN_RANGE, block_scan_state);

  forward_blockscan_to_end(&multi_scanner_, end_row_id, block_scan_state, is_reverse);
  end_row_id = get_index(end_row_id, is_reverse);
  ASSERT_EQ(range_row_ids_[1].end_row_id_, end_row_id);
  ASSERT_EQ(ROW_STORE_SCAN, multi_scanner_.prefetcher_.block_scan_state_);
  ASSERT_EQ(BLOCKSCAN_FINISH, block_scan_state);

  check_iter_end(&multi_scanner_);
}

void TestCOSSTableRowScanner::test_reverse_row_scan_and_column_scan_with_multi_range1()
{
  const int level_cnt = 6; // Total 1458 rows.
  const bool is_reverse = true;
  prepare_test_case(level_cnt);
  generate_ranges_case1(is_reverse);
  prepare_co_query_param(is_reverse);
  OK(multi_scanner_.init(iter_param_, context_, &sstable_, &ranges_));
  multi_scanner_.block_row_store_ = &block_row_store_;
  consume_rows_by_row_store(&multi_scanner_, range_row_ids_[0].end_row_id_,
                             range_row_ids_[0].end_row_id_, is_reverse);
  int64_t border_id1 = range_row_ids_[1].end_row_id_ - 1;
  generate_border(border_id1, false);

  multi_scanner_.refresh_blockscan_checker(border_rowkey_);

  pushdown_status_changed(&multi_scanner_);

  ObCSRowId start_row_id;
  ObCSRowId end_row_id;
  int32_t range_idx;
  BlockScanState block_scan_state = BlockScanState::BLOCKSCAN_RANGE;
  OK(multi_scanner_.get_blockscan_start(start_row_id, range_idx, block_scan_state));
  ASSERT_EQ(row_cnt_ - 2, start_row_id);
  ASSERT_EQ(0, range_idx);
  ASSERT_EQ(BLOCKSCAN_RANGE, block_scan_state);

  forward_blockscan_to_end(&multi_scanner_, end_row_id, block_scan_state, is_reverse);
  ASSERT_EQ(range_row_ids_[0].start_row_id_, end_row_id);
  ASSERT_EQ(SWITCH_RANGE, block_scan_state);
  block_scan_state = BlockScanState::BLOCKSCAN_RANGE;
  OK(multi_scanner_.get_blockscan_start(start_row_id, range_idx, block_scan_state));
  ASSERT_EQ(range_row_ids_[1].end_row_id_, start_row_id);
  ASSERT_EQ(1, range_idx);
  ASSERT_EQ(ROW_STORE_SCAN, multi_scanner_.prefetcher_.block_scan_state_);
  ASSERT_EQ(BLOCKSCAN_RANGE, block_scan_state);

  forward_blockscan_to_end(&multi_scanner_, end_row_id, block_scan_state, is_reverse);
  ASSERT_EQ(range_row_ids_[1].start_row_id_, end_row_id);
  ASSERT_EQ(ROW_STORE_SCAN, multi_scanner_.prefetcher_.block_scan_state_);
  ASSERT_EQ(BLOCKSCAN_FINISH, block_scan_state);

  check_iter_end(&multi_scanner_);
}

void TestCOSSTableRowScanner::test_row_scan_and_column_scan_with_multi_range2()
{
  const int level_cnt = 6; // Total 1458 rows.
  const bool is_reverse = false;
  prepare_test_case(level_cnt);
  generate_ranges_case2(is_reverse);
  prepare_co_query_param(is_reverse);
  OK(multi_scanner_.init(iter_param_, context_, &sstable_, &ranges_));
  multi_scanner_.block_row_store_ = &block_row_store_;

  consume_rows_by_row_store(&multi_scanner_, range_row_ids_[0].start_row_id_,
                             range_row_ids_[0].start_row_id_, is_reverse);

  int64_t border_id1 = range_row_ids_[4].start_row_id_ + 10;
  generate_border(border_id1, false);
  multi_scanner_.refresh_blockscan_checker(border_rowkey_);

  pushdown_status_changed(&multi_scanner_);

  ObCSRowId start_row_id;
  ObCSRowId end_row_id;
  int32_t range_idx;
  BlockScanState block_scan_state = BlockScanState::BLOCKSCAN_RANGE;
  OK(multi_scanner_.get_blockscan_start(start_row_id, range_idx, block_scan_state));
  ASSERT_EQ(1, start_row_id);
  ASSERT_EQ(0, range_idx);
  ASSERT_EQ(PENDING_BLOCK_SCAN, multi_scanner_.prefetcher_.block_scan_state_);
  ASSERT_EQ(BLOCKSCAN_RANGE, block_scan_state);

  forward_blockscan_to_end(&multi_scanner_, end_row_id, block_scan_state, is_reverse);
  ASSERT_EQ(range_row_ids_[0].end_row_id_, end_row_id);
  ASSERT_EQ(SWITCH_RANGE, block_scan_state);
  block_scan_state = BlockScanState::BLOCKSCAN_RANGE;
  OK(multi_scanner_.get_blockscan_start(start_row_id, range_idx, block_scan_state));
  ASSERT_EQ(range_row_ids_[1].start_row_id_, start_row_id);
  ASSERT_EQ(1, range_idx);
  ASSERT_EQ(PENDING_BLOCK_SCAN, multi_scanner_.prefetcher_.block_scan_state_);
  ASSERT_EQ(BLOCKSCAN_RANGE, block_scan_state);

  forward_blockscan_to_end(&multi_scanner_, end_row_id, block_scan_state, is_reverse);
  ASSERT_EQ(range_row_ids_[1].end_row_id_, end_row_id);
  ASSERT_EQ(SWITCH_RANGE, block_scan_state);
  block_scan_state = BlockScanState::BLOCKSCAN_RANGE;
  OK(multi_scanner_.get_blockscan_start(start_row_id, range_idx, block_scan_state));
  ASSERT_EQ(range_row_ids_[2].start_row_id_, start_row_id);
  ASSERT_EQ(2, range_idx);
  ASSERT_EQ(PENDING_SWITCH, multi_scanner_.prefetcher_.block_scan_state_);
  ASSERT_EQ(BLOCKSCAN_RANGE, block_scan_state);

  forward_blockscan_to_end(&multi_scanner_, end_row_id, block_scan_state, is_reverse);
  ASSERT_EQ(range_row_ids_[2].end_row_id_, end_row_id);
  ASSERT_EQ(SWITCH_RANGE, block_scan_state);
  block_scan_state = BlockScanState::BLOCKSCAN_RANGE;
  OK(multi_scanner_.get_blockscan_start(start_row_id, range_idx, block_scan_state));
  ASSERT_EQ(range_row_ids_[3].start_row_id_, start_row_id);
  ASSERT_EQ(3, range_idx);
  ASSERT_EQ(PENDING_BLOCK_SCAN, multi_scanner_.prefetcher_.block_scan_state_);
  ASSERT_EQ(BLOCKSCAN_RANGE, block_scan_state);

  forward_blockscan_to_end(&multi_scanner_, end_row_id, block_scan_state, is_reverse);
  ASSERT_EQ(range_row_ids_[3].end_row_id_, end_row_id);
  ASSERT_EQ(SWITCH_RANGE, block_scan_state);
  block_scan_state = BlockScanState::BLOCKSCAN_RANGE;
  OK(multi_scanner_.get_blockscan_start(start_row_id, range_idx, block_scan_state));
  ASSERT_EQ(range_row_ids_[4].start_row_id_, start_row_id);
  ASSERT_EQ(4, range_idx);
  ASSERT_TRUE(PENDING_BLOCK_SCAN == multi_scanner_.prefetcher_.block_scan_state_ || IN_END_OF_RANGE == multi_scanner_.prefetcher_.block_scan_state_);
  ASSERT_EQ(BLOCKSCAN_RANGE, block_scan_state);

  forward_blockscan_to_end(&multi_scanner_, end_row_id, block_scan_state, is_reverse);
  ASSERT_EQ(border_id1 - 1, end_row_id);
  ASSERT_EQ(BLOCKSCAN_FINISH, block_scan_state);
  consume_rows_by_row_store(&multi_scanner_, border_id1, range_row_ids_[4].end_row_id_,
                             is_reverse);
  check_iter_end(&multi_scanner_);
}

void TestCOSSTableRowScanner::test_reverse_row_scan_and_column_scan_with_multi_range2()
{
  const int level_cnt = 6; // Total 1458 rows.
  const bool is_reverse = true;
  prepare_test_case(level_cnt);
  generate_ranges_case2(is_reverse);
  prepare_co_query_param(is_reverse);
  OK(multi_scanner_.init(iter_param_, context_, &sstable_, &ranges_));
  multi_scanner_.block_row_store_ = &block_row_store_;

  consume_rows_by_row_store(&multi_scanner_, range_row_ids_[0].end_row_id_,
                             range_row_ids_[0].end_row_id_, is_reverse);

  int64_t border_id1 = range_row_ids_[4].end_row_id_ - 10;
  generate_border(border_id1, false);
  multi_scanner_.refresh_blockscan_checker(border_rowkey_);

  pushdown_status_changed(&multi_scanner_);

  ObCSRowId start_row_id;
  ObCSRowId end_row_id;
  int32_t range_idx;
  BlockScanState block_scan_state = BlockScanState::BLOCKSCAN_RANGE;
  OK(multi_scanner_.get_blockscan_start(start_row_id, range_idx, block_scan_state));
  ASSERT_EQ(range_row_ids_[0].end_row_id_ - 1, start_row_id);
  ASSERT_EQ(0, range_idx);
  ASSERT_EQ(PENDING_BLOCK_SCAN, multi_scanner_.prefetcher_.block_scan_state_);
  ASSERT_EQ(BLOCKSCAN_RANGE, block_scan_state);

  forward_blockscan_to_end(&multi_scanner_, end_row_id, block_scan_state, is_reverse);
  ASSERT_EQ(range_row_ids_[0].start_row_id_, end_row_id);
  ASSERT_EQ(SWITCH_RANGE, block_scan_state);
  block_scan_state = BlockScanState::BLOCKSCAN_RANGE;
  OK(multi_scanner_.get_blockscan_start(start_row_id, range_idx, block_scan_state));
  ASSERT_EQ(range_row_ids_[1].end_row_id_, start_row_id);
  ASSERT_EQ(1, range_idx);
  ASSERT_EQ(PENDING_BLOCK_SCAN, multi_scanner_.prefetcher_.block_scan_state_);
  ASSERT_EQ(BLOCKSCAN_RANGE, block_scan_state);

  forward_blockscan_to_end(&multi_scanner_, end_row_id, block_scan_state, is_reverse);
  ASSERT_EQ(range_row_ids_[1].start_row_id_, end_row_id);
  ASSERT_EQ(SWITCH_RANGE, block_scan_state);
  block_scan_state = BlockScanState::BLOCKSCAN_RANGE;
  OK(multi_scanner_.get_blockscan_start(start_row_id, range_idx, block_scan_state));
  ASSERT_EQ(range_row_ids_[2].end_row_id_, start_row_id);
  ASSERT_EQ(2, range_idx);
  ASSERT_EQ(PENDING_SWITCH, multi_scanner_.prefetcher_.block_scan_state_);
  ASSERT_EQ(BLOCKSCAN_RANGE, block_scan_state);

  forward_blockscan_to_end(&multi_scanner_, end_row_id, block_scan_state, is_reverse);
  ASSERT_EQ(range_row_ids_[2].start_row_id_, end_row_id);
  ASSERT_EQ(SWITCH_RANGE, block_scan_state);
  block_scan_state = BlockScanState::BLOCKSCAN_RANGE;
  OK(multi_scanner_.get_blockscan_start(start_row_id, range_idx, block_scan_state));
  ASSERT_EQ(range_row_ids_[3].end_row_id_, start_row_id);
  ASSERT_EQ(3, range_idx);
  ASSERT_EQ(PENDING_BLOCK_SCAN, multi_scanner_.prefetcher_.block_scan_state_);
  ASSERT_EQ(BLOCKSCAN_RANGE, block_scan_state);

  forward_blockscan_to_end(&multi_scanner_, end_row_id, block_scan_state, is_reverse);
  ASSERT_EQ(range_row_ids_[3].start_row_id_, end_row_id);
  ASSERT_EQ(SWITCH_RANGE, block_scan_state);
  block_scan_state = BlockScanState::BLOCKSCAN_RANGE;
  OK(multi_scanner_.get_blockscan_start(start_row_id, range_idx, block_scan_state));
  ASSERT_EQ(range_row_ids_[4].end_row_id_, start_row_id);
  ASSERT_EQ(4, range_idx);
  ASSERT_TRUE(PENDING_BLOCK_SCAN == multi_scanner_.prefetcher_.block_scan_state_ || IN_END_OF_RANGE == multi_scanner_.prefetcher_.block_scan_state_);
  ASSERT_EQ(BLOCKSCAN_RANGE, block_scan_state);

  forward_blockscan_to_end(&multi_scanner_, end_row_id, block_scan_state, is_reverse);
  ASSERT_EQ(border_id1 + 1, end_row_id);
  ASSERT_EQ(BLOCKSCAN_FINISH, block_scan_state);
  consume_rows_by_row_store(&multi_scanner_, border_id1, range_row_ids_[4].start_row_id_, is_reverse);
  check_iter_end(&multi_scanner_);
}

TEST_F(TestCOSSTableRowScanner, test_row_scan_only)
{
  test_row_scan_only(false);
}

TEST_F(TestCOSSTableRowScanner, test_row_reverse_scan_only)
{
  test_row_scan_only(true);
}

TEST_F(TestCOSSTableRowScanner, test_row_and_column_scan)
{
  test_row_scan_and_column_scan(false);
}

TEST_F(TestCOSSTableRowScanner, test_row_and_column_reverse_scan)
{
  test_row_scan_and_column_scan(true);
}

TEST_F(TestCOSSTableRowScanner, test_row_scan_and_column_scan_with_multi_range1)
{
  test_row_scan_and_column_scan_with_multi_range1();
}

TEST_F(TestCOSSTableRowScanner, test_row_and_column_reverse_scan_with_multi_range1)
{
  test_reverse_row_scan_and_column_scan_with_multi_range1();
}

TEST_F(TestCOSSTableRowScanner, test_row_scan_and_column_scan_with_multi_range2)
{
  test_row_scan_and_column_scan_with_multi_range2();
}

TEST_F(TestCOSSTableRowScanner, test_reverse_row_scan_and_column_scan_with_multi_range2)
{
  test_reverse_row_scan_and_column_scan_with_multi_range2();
}

}
}

int main(int argc, char **argv)
{
  system("rm -f test_co_sstable_row_scanner.log*");
  OB_LOGGER.set_file_name("test_co_sstable_row_scanner.log", true, true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
