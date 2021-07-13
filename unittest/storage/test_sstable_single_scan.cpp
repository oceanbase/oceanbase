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

#include <gtest/gtest.h>
#include "ob_sstable_test.h"

namespace oceanbase {
using namespace blocksstable;
using namespace common;
using namespace storage;
using namespace share::schema;

namespace unittest {
class TestSSTableSingleScanner : public ObSSTableTest {
public:
  TestSSTableSingleScanner();
  void test_border(const bool is_reverse_scan, const int64_t limit);
  void test_normal(const bool is_reverse_scan, const int64_t limit);
  void test_multi_block_read_continue_io(const bool is_reverse_scan, const int64_t limit);
  void test_multi_block_read_discrete_io(const bool is_reverse_scan, const int64_t limit);
  void generate_range(const int64_t start, const int64_t end, ObStoreRange& range);
  void test_one_case(const ObStoreRange& range, const int64_t start, const int64_t end, const bool is_reverse_scan,
      const int64_t hit_mode);
  void test_skip_range(const bool is_reverse_scan, const common::ObIArray<SkipInfo>& skip_infos);
  virtual ~TestSSTableSingleScanner();

private:
  ObStoreRow start_row_;
  ObStoreRow end_row_;
  ObObj start_cells_[TEST_COLUMN_CNT];
  ObObj end_cells_[TEST_COLUMN_CNT];
};

TestSSTableSingleScanner::TestSSTableSingleScanner() : ObSSTableTest("single_scan_sstable")
{}

TestSSTableSingleScanner::~TestSSTableSingleScanner()
{}

void TestSSTableSingleScanner::generate_range(const int64_t start, const int64_t end, ObStoreRange& range)
{
  int ret = OB_SUCCESS;
  start_row_.row_val_.assign(start_cells_, TEST_COLUMN_CNT);
  end_row_.row_val_.assign(end_cells_, TEST_COLUMN_CNT);
  ret = row_generate_.get_next_row(start, start_row_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = row_generate_.get_next_row(end, end_row_);
  ASSERT_EQ(OB_SUCCESS, ret);
  range.get_start_key().assign(start_cells_, TEST_ROWKEY_COLUMN_CNT);
  range.get_end_key().assign(end_cells_, TEST_ROWKEY_COLUMN_CNT);
  range.get_border_flag().set_inclusive_start();
  range.get_border_flag().set_inclusive_end();
}

void TestSSTableSingleScanner::test_one_case(const ObStoreRange& range, const int64_t start, const int64_t end,
    const bool is_reverse_scan, const int64_t hit_mode)
{
  int ret = OB_SUCCESS;
  ObStoreRowIterator* scanner = NULL;
  ObStoreRow row;
  ObObj cells[TEST_COLUMN_CNT];
  row.row_val_.assign(cells, TEST_COLUMN_CNT);
  const ObStoreRow* prow = NULL;
  ObExtStoreRange ext_range;

  if (HIT_PART == hit_mode) {
    const int64_t part_start = start + (end - start) / 3;
    const int64_t part_end = end - (end - start) / 3;
    ObStoreRange part_range;
    ObStoreRow start_row;
    ObStoreRow end_row;
    ObObj start_cells[TEST_COLUMN_CNT];
    ObObj end_cells[TEST_COLUMN_CNT];
    start_row.row_val_.assign(start_cells, TEST_COLUMN_CNT);
    end_row.row_val_.assign(end_cells, TEST_COLUMN_CNT);
    ret = row_generate_.get_next_row(part_start, start_row);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = row_generate_.get_next_row(part_end, end_row);
    ASSERT_EQ(OB_SUCCESS, ret);
    part_range.get_start_key().assign(start_cells, TEST_ROWKEY_COLUMN_CNT);
    part_range.get_end_key().assign(end_cells, TEST_ROWKEY_COLUMN_CNT);
    part_range.get_border_flag().set_inclusive_start();
    part_range.get_border_flag().set_inclusive_end();
    convert_range(part_range, ext_range, allocator_);
    ret = sstable_.scan(param_, context_, ext_range, scanner);
    ASSERT_EQ(OB_SUCCESS, ret);
    for (int64_t i = part_start; i <= part_end; ++i) {
      if (i < row_cnt_) {
        ret = scanner->get_next_row(prow);
        ASSERT_EQ(OB_SUCCESS, ret) << "i: " << i << " part_start: " << part_start << " part_end: " << part_end
                                   << " prow: " << prow;
      }
    }
    ret = scanner->get_next_row(prow);
    ASSERT_EQ(OB_ITER_END, ret);
  }
  if (nullptr != scanner) {
    scanner->~ObStoreRowIterator();
    scanner = nullptr;
  }

  convert_range(range, ext_range, allocator_);
  ret = sstable_.scan(param_, context_, ext_range, scanner);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = start; i <= end; ++i) {
    int64_t index = 0;
    if (is_reverse_scan) {
      ret = row_generate_.get_next_row(end - i + start, row);
      index = end - i + start;
    } else {
      ret = row_generate_.get_next_row(i, row);
      index = i;
    }
    ASSERT_EQ(OB_SUCCESS, ret);
    if (index < row_cnt_) {
      ret = scanner->get_next_row(prow);
      ASSERT_EQ(OB_SUCCESS, ret) << "index: " << index << " start: " << start << " end: " << end << " prow: " << prow;
      ASSERT_TRUE(row.row_val_ == prow->row_val_);
    }
  }
  ret = scanner->get_next_row(prow);
  ASSERT_EQ(OB_ITER_END, ret);
  ret = scanner->get_next_row(prow);
  ASSERT_EQ(OB_ITER_END, ret);
  if (nullptr != scanner) {
    scanner->~ObStoreRowIterator();
    scanner = nullptr;
  }

  if (HIT_ALL == hit_mode) {
    int64_t index = 0;
    ret = sstable_.scan(param_, context_, ext_range, scanner);
    ASSERT_EQ(OB_SUCCESS, ret);
    for (int64_t i = start; i <= end; ++i) {
      if (is_reverse_scan) {
        ret = row_generate_.get_next_row(end - i + start, row);
        index = end - i + start;
      } else {
        ret = row_generate_.get_next_row(i, row);
        index = i;
      }
      ASSERT_EQ(OB_SUCCESS, ret);
      if (index < row_cnt_) {
        ret = scanner->get_next_row(prow);
        ASSERT_EQ(OB_SUCCESS, ret);
        ASSERT_TRUE(row.row_val_ == prow->row_val_);
      }
    }
    ret = scanner->get_next_row(prow);
    ASSERT_EQ(OB_ITER_END, ret);
    ret = scanner->get_next_row(prow);
    ASSERT_EQ(OB_ITER_END, ret);
  }
  if (nullptr != scanner) {
    scanner->~ObStoreRowIterator();
    scanner = nullptr;
  }
}

void TestSSTableSingleScanner::test_skip_range(const bool is_reverse_scan, const common::ObIArray<SkipInfo>& skip_infos)
{
  int ret = OB_SUCCESS;
  ObStoreRowIterator* scanner = NULL;
  ObStoreRange range;
  ObStoreRow row;
  ObStoreRow gap_row;
  ObStoreRowkey gap_rowkey;
  ObObj cells[TEST_COLUMN_CNT];
  row.row_val_.assign(cells, TEST_COLUMN_CNT);
  ObObj gap_key_cells[TEST_COLUMN_CNT];
  gap_row.row_val_.assign(gap_key_cells, TEST_COLUMN_CNT);
  ObExtStoreRange ext_range;
  const ObStoreRow* prow = NULL;
  int64_t step = is_reverse_scan ? -1 : 1;
  int64_t start = is_reverse_scan ? row_cnt_ - 1 : 0;
  int64_t end = is_reverse_scan ? 0 : row_cnt_ - 1;

  // prepare query param
  const int64_t limit = -1;
  ret = prepare_query_param(is_reverse_scan, limit);
  ASSERT_EQ(OB_SUCCESS, ret);

  // test whole range
  range.set_whole_range();
  convert_range(range, ext_range, allocator_);
  ret = sstable_.scan(param_, context_, ext_range, scanner);
  ASSERT_EQ(OB_SUCCESS, ret);
  STORAGE_LOG(INFO, "test_skip_range", K(is_reverse_scan), K(skip_infos));
  for (int64_t i = start; compare(is_reverse_scan, i, end); i += step) {

    ret = row_generate_.get_next_row(i, row);
    ASSERT_EQ(OB_SUCCESS, ret);
    OB_LOGGER.set_log_level("DEBUG");
    ret = scanner->get_next_row(prow);
    OB_LOGGER.set_log_level("INFO");
    ASSERT_EQ(OB_SUCCESS, ret);
    STORAGE_LOG(INFO, "get_next_row", K(row), K(*prow));
    ASSERT_TRUE(row.row_val_ == prow->row_val_) << i << "\n";
    int64_t j = 0;
    for (j = 0; OB_SUCC(ret) && j < skip_infos.count(); ++j) {
      if (skip_infos.at(j).start_key_ == i) {
        i = skip_infos.at(j).gap_key_ - step;
        break;
      }
    }
    if (j != skip_infos.count()) {
      ret = row_generate_.get_next_row(skip_infos.at(j).gap_key_, gap_row);
      ASSERT_EQ(OB_SUCCESS, ret);
      gap_rowkey.assign(gap_key_cells, TEST_ROWKEY_COLUMN_CNT);
      OB_LOGGER.set_log_level("DEBUG");
      ret = scanner->skip_range(0, &gap_rowkey, true);
      OB_LOGGER.set_log_level("INFO");
      ASSERT_EQ(OB_SUCCESS, ret);
      STORAGE_LOG(INFO, "skip range to", K(gap_rowkey));
    }
  }
  ret = scanner->get_next_row(prow);
  ASSERT_EQ(OB_ITER_END, ret);
  ret = scanner->get_next_row(prow);
  ASSERT_EQ(OB_ITER_END, ret);
  if (nullptr != scanner) {
    scanner->~ObStoreRowIterator();
    scanner = nullptr;
  }
}

void TestSSTableSingleScanner::test_border(const bool is_reverse_scan, const int64_t limit)
{
  int ret = OB_SUCCESS;
  ObStoreRowIterator* scanner = NULL;
  ObStoreRange range;
  ObStoreRow row;
  ObObj cells[TEST_COLUMN_CNT];
  row.row_val_.assign(cells, TEST_COLUMN_CNT);
  ObExtStoreRange ext_range;

  // prepare query param
  ret = prepare_query_param(is_reverse_scan, limit);
  ASSERT_EQ(OB_SUCCESS, ret);

  // full table scan
  range.set_whole_range();
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(range, 0, row_cnt_ - 1, is_reverse_scan, i);
  }

  // the first row of sstable
  generate_range(0, 0, range);
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(range, 0, 0, is_reverse_scan, i);
  }

  // the last row of sstable
  generate_range(row_cnt_ - 1, row_cnt_ - 1, range);
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(range, row_cnt_ - 1, row_cnt_ - 1, is_reverse_scan, i);
  }

  // not exist
  generate_range(row_cnt_, row_cnt_, range);
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(range, row_cnt_, row_cnt_, is_reverse_scan, i);
  }

  // invalid invoke when not inited
  ObSSTable sstable;
  convert_range(range, ext_range, allocator_);
  ret = sstable.scan(param_, context_, ext_range, scanner);
  ASSERT_NE(OB_SUCCESS, ret);
  destroy_query_param();
}

void TestSSTableSingleScanner::test_normal(const bool is_reverse_scan, const int64_t limit)
{
  int ret = OB_SUCCESS;
  ObStoreRange range;
  ObRandom random;
  const int64_t random_start = random.get(0, 10000000) % row_cnt_;
  const int64_t random_end = random.get(0, 100000000) % row_cnt_;
  const int64_t start = std::min(random_start, random_end);
  const int64_t end = std::max(random_start, random_end);

  // prepare query param
  ret = prepare_query_param(is_reverse_scan, limit);
  ASSERT_EQ(OB_SUCCESS, ret);

  // multiple rows exist
  generate_range(start, end, range);
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(range, start, end, is_reverse_scan, i);
  }

  // multiple rows, partial exist
  generate_range(start, row_cnt_ + 10, range);
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(range, start, row_cnt_ + 10, is_reverse_scan, i);
  }

  // single row exist
  generate_range(row_cnt_ / 2, row_cnt_ / 2, range);
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(range, row_cnt_ / 2, row_cnt_ / 2, is_reverse_scan, i);
  }

  // not exist
  generate_range(row_cnt_ + 10, row_cnt_ + 20, range);
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(range, row_cnt_ + 10, row_cnt_ + 20, is_reverse_scan, i);
  }
  destroy_query_param();
}

void TestSSTableSingleScanner::test_multi_block_read_continue_io(const bool is_reverse_scan, const int64_t limit)
{
  int ret = OB_SUCCESS;
  ObStoreRange range;

  // prepare query param
  ret = prepare_query_param(is_reverse_scan, limit);
  ASSERT_EQ(OB_SUCCESS, ret);

  // size < multiblock_read_size
  GCONF.multiblock_read_size = 10 * 1024;
  destroy_all_cache();
  generate_range(0, 10, range);
  test_one_case(range, 0, 10, is_reverse_scan, HIT_NONE);

  // size > multiblock_read_size
  destroy_all_cache();
  generate_range(0, 100, range);
  test_one_case(range, 0, 100, is_reverse_scan, HIT_NONE);
  destroy_query_param();
}

void TestSSTableSingleScanner::test_multi_block_read_discrete_io(const bool is_reverse_scan, const int64_t limit)
{
  int ret = OB_SUCCESS;
  ObStoreRange range;

  // prepare query param
  ret = prepare_query_param(is_reverse_scan, limit);
  ASSERT_EQ(OB_SUCCESS, ret);

  // gap_size < multiblock_read_gap_size
  GCONF.multiblock_read_size = 10 * 1024;
  GCONF.multiblock_read_gap_size = 5 * 1024;
  destroy_all_cache();
  generate_range(10, 25, range);
  test_one_case(range, 10, 25, is_reverse_scan, HIT_NONE);
  generate_range(0, 100, range);
  test_one_case(range, 0, 100, is_reverse_scan, HIT_NONE);

  // gap_size > multiblock_read_gap_size
  destroy_all_cache();
  generate_range(10, 60, range);
  test_one_case(range, 10, 60, is_reverse_scan, HIT_NONE);
  generate_range(0, 100, range);
  test_one_case(range, 0, 100, is_reverse_scan, HIT_NONE);
  destroy_query_param();
}

TEST_F(TestSSTableSingleScanner, test_border)
{
  const bool is_reverse_scan = false;
  int64_t limit = -1;
  test_border(is_reverse_scan, limit);
  limit = 1;
  test_border(is_reverse_scan, limit);
  STORAGE_LOG(INFO, "memory usage", K(lib::get_memory_hold()), K(lib::get_memory_limit()));
  ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(500);
  ObMallocAllocator::get_instance()->print_tenant_memory_usage(500);
  ObMallocAllocator::get_instance()->print_tenant_memory_usage(1);
}

TEST_F(TestSSTableSingleScanner, test_border_reverse_scan)
{
  const bool is_reverse_scan = true;
  int64_t limit = -1;
  test_border(is_reverse_scan, limit);
  limit = 1;
  test_border(is_reverse_scan, limit);
}

TEST_F(TestSSTableSingleScanner, test_normal)
{
  const bool is_reverse_scan = false;
  int64_t limit = -1;
  test_normal(is_reverse_scan, limit);
  limit = 1;
  test_normal(is_reverse_scan, limit);
}

TEST_F(TestSSTableSingleScanner, test_normal_reverse_scan)
{
  const bool is_reverse_scan = true;
  int64_t limit = -1;
  test_normal(is_reverse_scan, limit);
  limit = 1;
  test_normal(is_reverse_scan, limit);
}

TEST_F(TestSSTableSingleScanner, test_scan_mix_columns)
{
  int ret = OB_SUCCESS;
  ObStoreRowIterator* scanner = NULL;
  ObStoreRange range;
  ObExtStoreRange ext_range;
  ObTableAccessParam param;
  const ObStoreRow* prow = NULL;

  // prepare query param
  const bool is_reverse_scan = false;
  const int64_t limit = -1;
  ret = prepare_query_param(is_reverse_scan, limit);
  ASSERT_EQ(OB_SUCCESS, ret);

  columns_.reset();
  ret = table_schema_.get_column_ids(columns_);
  ASSERT_EQ(OB_SUCCESS, ret);

  int64_t column_ids[5] = {3, 5, 3, 23, 12};
  ObColDesc col_desc;
  const share::schema::ObColumnSchemaV2* column = NULL;
  for (int64_t i = 0; i < (int64_t)(sizeof(column_ids) / sizeof(int64_t)); ++i) {
    if (NULL == (column = table_schema_.get_column_schema(column_ids[i]))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "cannot find column.", K(i), K(column_ids[i]));
    } else {
      col_desc.col_id_ = column->get_column_id();
      col_desc.col_type_ = column->get_meta_type();
      if (OB_FAIL(columns_.push_back(col_desc))) {
        STORAGE_LOG(WARN, "push to columns failed.", K(ret));
      }
    }
  }

  param_.out_cols_ = &columns_;

  // whole range
  range.set_whole_range();
  convert_range(range, ext_range, allocator_);
  ret = sstable_.scan(param_, context_, ext_range, scanner);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = 0; i < row_cnt_; ++i) {
    ret = scanner->get_next_row(prow);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(NULL != prow);
  }
  ret = scanner->get_next_row(prow);
  ASSERT_EQ(OB_ITER_END, ret);
}

TEST_F(TestSSTableSingleScanner, test_estimate)
{
  int ret = OB_SUCCESS;
  ObStoreRange range;
  ObExtStoreRange ext_range;
  ObPartitionEst cost_metrics;
  ObStoreRow row;

  // prepare query param
  const bool is_reverse_scan = false;
  const int64_t limit = -1;
  ret = prepare_query_param(is_reverse_scan, limit);
  ASSERT_EQ(OB_SUCCESS, ret);

  range.set_whole_range();
  cost_metrics.reset();
  convert_range(range, ext_range, allocator_);
  ret = sstable_.estimate_scan_row_count(context_.query_flag_, table_key_.table_id_, ext_range, cost_metrics);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(cost_metrics.logical_row_count_ > 2900);

  ObStoreRow start_row;
  ObStoreRow end_row;
  ObObj start_cells[TEST_COLUMN_CNT];
  ObObj end_cells[TEST_COLUMN_CNT];
  start_row.row_val_.assign(start_cells, TEST_COLUMN_CNT);
  end_row.row_val_.assign(end_cells, TEST_COLUMN_CNT);
  ret = row_generate_.get_next_row(0, start_row);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = row_generate_.get_next_row(2, end_row);
  ASSERT_EQ(OB_SUCCESS, ret);
  range.get_start_key().assign(start_cells, TEST_ROWKEY_COLUMN_CNT);
  range.get_end_key().assign(end_cells, TEST_ROWKEY_COLUMN_CNT);
  range.get_border_flag().set_inclusive_start();
  range.get_border_flag().set_inclusive_end();
  convert_range(range, ext_range, allocator_);
  cost_metrics.logical_row_count_ = 0;
  ret = sstable_.estimate_scan_row_count(context_.query_flag_, table_key_.table_id_, ext_range, cost_metrics);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(3, cost_metrics.logical_row_count_);
}

TEST_F(TestSSTableSingleScanner, test_daily_merge)
{
  int ret = OB_SUCCESS;
  ObStoreRowIterator* scanner = NULL;
  ObArray<ObExtStoreRange> ranges;
  ObStoreRange range;
  ObStoreRow row;
  ObObj cells[TEST_COLUMN_CNT];
  row.row_val_.assign(cells, TEST_COLUMN_CNT);
  ObExtStoreRange ext_range;
  const ObStoreRow* prow = NULL;

  // prepare query param
  const bool is_reverse_scan = false;
  const int64_t limit = -1;
  ret = prepare_query_param(is_reverse_scan, limit);
  ASSERT_EQ(OB_SUCCESS, ret);
  context_.query_flag_.daily_merge_ = 1;
  context_.query_flag_.whole_macro_scan_ = 1;

  // test whole range
  range.set_whole_range();
  convert_range(range, ext_range, allocator_);
  ranges.push_back(ext_range);
  ret = sstable_.scan(param_, context_, ext_range, scanner);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = 0; i < row_cnt_; ++i) {
    ret = row_generate_.get_next_row(i, row);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = scanner->get_next_row(prow);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(row.row_val_ == prow->row_val_);
  }
  ret = scanner->get_next_row(prow);
  ASSERT_EQ(OB_ITER_END, ret);
  ret = scanner->get_next_row(prow);
  ASSERT_EQ(OB_ITER_END, ret);
}

TEST_F(TestSSTableSingleScanner, test_skip_single_range)
{
  ObArray<SkipInfo> skip_info_array;
  SkipInfo info;
  info.start_key_ = row_cnt_ / 4;
  info.gap_key_ = row_cnt_ / 2;
  ASSERT_EQ(OB_SUCCESS, skip_info_array.push_back(info));
  test_skip_range(false, skip_info_array);
  STORAGE_LOG(INFO, "memory usage", K(lib::get_memory_hold()), K(lib::get_memory_limit()));
  ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(500);
  ObMallocAllocator::get_instance()->print_tenant_memory_usage(500);
  ObMallocAllocator::get_instance()->print_tenant_memory_usage(1);
}

TEST_F(TestSSTableSingleScanner, test_skip_single_range_reverse_scan)
{
  ObArray<SkipInfo> skip_info_array;
  SkipInfo info;
  info.start_key_ = row_cnt_ / 2;
  info.gap_key_ = row_cnt_ / 4;
  ASSERT_EQ(OB_SUCCESS, skip_info_array.push_back(info));
  test_skip_range(true, skip_info_array);
}

TEST_F(TestSSTableSingleScanner, test_skip_multiple_ranges)
{
  ObArray<SkipInfo> skip_info_array;
  SkipInfo info;
  info.start_key_ = 10;
  info.gap_key_ = 20;
  ASSERT_EQ(OB_SUCCESS, skip_info_array.push_back(info));
  info.start_key_ = 200;
  info.gap_key_ = 205;
  ASSERT_EQ(OB_SUCCESS, skip_info_array.push_back(info));
  info.start_key_ = 1000;
  info.gap_key_ = row_cnt_ / 2;
  ASSERT_EQ(OB_SUCCESS, skip_info_array.push_back(info));
  info.start_key_ = row_cnt_ / 3 * 2;
  info.gap_key_ = row_cnt_ / 4 * 3;
  ASSERT_EQ(OB_SUCCESS, skip_info_array.push_back(info));
  test_skip_range(false, skip_info_array);
}

TEST_F(TestSSTableSingleScanner, test_skip_multiple_ranges_reverse_scan)
{
  ObArray<SkipInfo> skip_info_array;
  SkipInfo info;
  info.start_key_ = row_cnt_ / 4 * 3;
  info.gap_key_ = row_cnt_ / 3 * 2;
  ASSERT_EQ(OB_SUCCESS, skip_info_array.push_back(info));
  info.start_key_ = row_cnt_ / 2;
  info.gap_key_ = 1000;
  ASSERT_EQ(OB_SUCCESS, skip_info_array.push_back(info));
  info.start_key_ = 205;
  info.gap_key_ = 200;
  ASSERT_EQ(OB_SUCCESS, skip_info_array.push_back(info));
  info.start_key_ = 20;
  info.gap_key_ = 10;
  ASSERT_EQ(OB_SUCCESS, skip_info_array.push_back(info));
  test_skip_range(true, skip_info_array);
}

TEST_F(TestSSTableSingleScanner, test_skip_in_same_micro_range)
{
  ObArray<SkipInfo> skip_info_array;
  SkipInfo info;
  info.start_key_ = 10;
  info.gap_key_ = 20;
  ASSERT_EQ(OB_SUCCESS, skip_info_array.push_back(info));
  info.start_key_ = 22;
  info.gap_key_ = 25;
  ASSERT_EQ(OB_SUCCESS, skip_info_array.push_back(info));
  info.start_key_ = 27;
  info.gap_key_ = 30;
  ASSERT_EQ(OB_SUCCESS, skip_info_array.push_back(info));
  info.start_key_ = 1900;
  info.gap_key_ = row_cnt_ + 1;
  ASSERT_EQ(OB_SUCCESS, skip_info_array.push_back(info));
  test_skip_range(false, skip_info_array);
}

TEST_F(TestSSTableSingleScanner, test_skip_in_same_micro_range_reverse_scan)
{
  ObArray<SkipInfo> skip_info_array;
  SkipInfo info;
  info.gap_key_ = 1900;
  info.start_key_ = row_cnt_ - 1;
  ASSERT_EQ(OB_SUCCESS, skip_info_array.push_back(info));
  info.gap_key_ = 27;
  info.start_key_ = 30;
  ASSERT_EQ(OB_SUCCESS, skip_info_array.push_back(info));
  info.gap_key_ = 22;
  info.start_key_ = 25;
  ASSERT_EQ(OB_SUCCESS, skip_info_array.push_back(info));
  info.gap_key_ = 10;
  info.start_key_ = 20;
  ASSERT_EQ(OB_SUCCESS, skip_info_array.push_back(info));
  test_skip_range(true, skip_info_array);
}

TEST_F(TestSSTableSingleScanner, test_skip_random_range)
{
  int ret = OB_SUCCESS;
  const int64_t skip_range_cnt = 10;
  ObArray<SkipInfo> skip_info_array;
  SkipInfo info;
  ObRandom random;
  int64_t last_gap_key = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < skip_range_cnt && last_gap_key <= row_cnt_ - 1; ++i) {
    info.start_key_ = random.get(last_gap_key, row_cnt_ - 1);
    info.gap_key_ = random.get(info.start_key_ + 1, row_cnt_ - 1);
    last_gap_key = info.gap_key_;
    if (info.start_key_ >= row_cnt_ - 1 && info.start_key_ != info.gap_key_) {
      ret = skip_info_array.push_back(info);
      ASSERT_EQ(OB_SUCCESS, ret);
    }
  }
  test_skip_range(false, skip_info_array);
}

TEST_F(TestSSTableSingleScanner, test_skip_random_range_reverse_scan)
{
  int ret = OB_SUCCESS;
  const int64_t skip_range_cnt = 10;
  ObArray<SkipInfo> skip_info_array;
  SkipInfo info;
  ObRandom random;
  int64_t last_gap_key = row_cnt_ - 1;
  for (int64_t i = 0; (OB_SUCC(ret) && i < skip_range_cnt) && last_gap_key > 0; ++i) {
    info.start_key_ = random.get(0, last_gap_key);
    info.gap_key_ = random.get(0, info.start_key_ - 1);
    last_gap_key = info.gap_key_;
    if (last_gap_key >= 0 && info.start_key_ != info.gap_key_) {
      ret = skip_info_array.push_back(info);
      ASSERT_EQ(OB_SUCCESS, ret);
    }
  }
  STORAGE_LOG(INFO, "test skip random range reverse scan", K(skip_info_array));
  test_skip_range(true, skip_info_array);
}

TEST_F(TestSSTableSingleScanner, test_skip_random_range_reverse_scan_bug)
{
  ObArray<SkipInfo> skip_info_array;
  SkipInfo info;
  info.start_key_ = 1390;
  info.gap_key_ = 346;
  ASSERT_EQ(OB_SUCCESS, skip_info_array.push_back(info));
  info.start_key_ = 229;
  info.gap_key_ = 34;
  ASSERT_EQ(OB_SUCCESS, skip_info_array.push_back(info));
  info.start_key_ = 3;
  info.gap_key_ = 0;
  ASSERT_EQ(OB_SUCCESS, skip_info_array.push_back(info));
  test_skip_range(true, skip_info_array);
}

TEST_F(TestSSTableSingleScanner, test_skip_random_range_reverse_scan_bug2)
{
  ObArray<SkipInfo> skip_info_array;
  SkipInfo info;
  info.start_key_ = 1844;
  info.gap_key_ = 653;
  ASSERT_EQ(OB_SUCCESS, skip_info_array.push_back(info));
  info.start_key_ = 351;
  info.gap_key_ = 247;
  ASSERT_EQ(OB_SUCCESS, skip_info_array.push_back(info));
  info.start_key_ = 212;
  info.gap_key_ = 1;
  ASSERT_EQ(OB_SUCCESS, skip_info_array.push_back(info));
  test_skip_range(true, skip_info_array);
}

TEST_F(TestSSTableSingleScanner, test_multi_block_continue_io)
{
  test_multi_block_read_continue_io(false, -1);
}

TEST_F(TestSSTableSingleScanner, test_multi_block_continue_io_reverse_scan)
{
  test_multi_block_read_continue_io(true, -1);
}

TEST_F(TestSSTableSingleScanner, test_multi_block_continue_io_limit)
{
  test_multi_block_read_continue_io(false, 1);
}

TEST_F(TestSSTableSingleScanner, test_multi_block_continue_io_limit_reverse_scan)
{
  test_multi_block_read_continue_io(true, 1);
}

TEST_F(TestSSTableSingleScanner, test_multi_block_read_discrete_io)
{
  test_multi_block_read_discrete_io(false, -1);
}

TEST_F(TestSSTableSingleScanner, test_multi_block_read_discrete_io_reverse_scan)
{
  test_multi_block_read_discrete_io(true, -1);
}

TEST_F(TestSSTableSingleScanner, test_multi_block_read_discrete_io_limit)
{
  test_multi_block_read_discrete_io(false, 1);
}

TEST_F(TestSSTableSingleScanner, test_multi_block_read_discrete_io_limit_reverse_scan)
{
  test_multi_block_read_discrete_io(true, 1);
}

}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char** argv)
{
  system("rm -f test_sstable_single_scan.log*");
  OB_LOGGER.set_file_name("test_sstable_single_scan.log");
  OB_LOGGER.set_log_level("INFO");
  CLOG_LOG(INFO, "begin unittest: test_sstable_single_scan");
  oceanbase::lib::set_memory_limit(30L * 1024 * 1024 * 1024);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  STORAGE_LOG(INFO, "sizeof KVCacheInst", K(sizeof(ObKVCacheInst)));
  STORAGE_LOG(INFO, "sizeof KVCacheInfo", K(sizeof(ObKVCacheInfo)));
  return RUN_ALL_TESTS();
}
