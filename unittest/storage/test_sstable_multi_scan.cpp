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
#include "lib/container/ob_array_iterator.h"

namespace oceanbase {
using namespace blocksstable;
using namespace common;
using namespace storage;
using namespace share::schema;

namespace unittest {
class TestSSTableMultiScanner : public ObSSTableTest {
public:
  TestSSTableMultiScanner();
  virtual ~TestSSTableMultiScanner();
  void test_one_case(const ObIArray<int64_t>& start_seeds, const int64_t count_perf_range, const int64_t hit_mode,
      const bool is_reverse_scan);
  void test_skip_range(const ObIArray<int64_t>& start_seeds, const int64_t count_per_range, const bool is_reverse_scan,
      const ObIArray<SkipInfo>& skip_infos);
  void test_single_get_normal(const bool is_reverse_scan, const int64_t limit);
  void test_single_get_border(const bool is_reverse_scan, const int64_t limit);
  void test_multi_get_normal(const bool is_reverse_scan, const int64_t limit);
  void test_multi_get_border(const bool is_reverse_scan, const int64_t limit);
  void test_single_scan_normal(const bool is_reverse_scan, const int64_t limit);
  void test_single_scan_border(const bool is_reverse_scan, const int64_t limit);
  void test_multi_scan_multi_scan_range(const bool is_reverse_scan, const int64_t limit, const int64_t count_per_range);
  void test_multi_scan_multi_get_with_scan(
      const bool is_reverse_scan, const int64_t limit, const int64_t count_per_range);
  void test_multi_block_read_small_io(const bool is_reverse_scan, const int64_t limit);
  void test_multi_block_read_big_continue_io(const bool is_reverse_scan, const int64_t limit);
  void test_multi_block_read_big_discrete_io(const bool is_reverse_scan, const int64_t limit);
};

TestSSTableMultiScanner::TestSSTableMultiScanner() : ObSSTableTest("multi_scan_sstable")
{}

TestSSTableMultiScanner::~TestSSTableMultiScanner()
{}

void TestSSTableMultiScanner::test_skip_range(const ObIArray<int64_t>& start_seeds, const int64_t count_per_range,
    const bool is_reverse_scan, const ObIArray<SkipInfo>& skip_infos)
{
  int ret = OB_SUCCESS;
  ObStoreRowkey mscan_rowkeys[TEST_MULTI_GET_CNT];
  ObStoreRange mscan_ranges[TEST_MULTI_GET_CNT];
  ObObj mscan_start_cells[TEST_MULTI_GET_CNT][TEST_COLUMN_CNT];
  ObObj mscan_end_cells[TEST_MULTI_GET_CNT][TEST_COLUMN_CNT];
  ObStoreRow mscan_start_rows[TEST_MULTI_GET_CNT];
  ObStoreRow mscan_end_rows[TEST_MULTI_GET_CNT];
  ObArray<ObStoreRange> ranges;
  ObArray<ObExtStoreRange> ext_ranges;
  ObObj check_cells[TEST_COLUMN_CNT];
  ObStoreRow check_row;
  check_row.row_val_.assign(check_cells, TEST_COLUMN_CNT);
  ObStoreRowIterator* scanner;
  const ObStoreRow* prow = NULL;
  const int64_t step = is_reverse_scan ? -1 : 1;
  ObStoreRow gap_row;
  ObStoreRowkey gap_rowkey;
  ObObj gap_key_cells[TEST_COLUMN_CNT];
  gap_row.row_val_.assign(gap_key_cells, TEST_COLUMN_CNT);

  const int64_t limit = -1;
  ret = prepare_query_param(is_reverse_scan, limit);
  ASSERT_EQ(OB_SUCCESS, ret);

  for (int64_t i = 0; i < start_seeds.count(); ++i) {
    mscan_start_rows[i].row_val_.assign(mscan_start_cells[i], TEST_COLUMN_CNT);
    mscan_end_rows[i].row_val_.assign(mscan_end_cells[i], TEST_COLUMN_CNT);
    mscan_ranges[i].get_start_key().assign(mscan_start_cells[i], TEST_ROWKEY_COLUMN_CNT);
    mscan_ranges[i].get_end_key().assign(mscan_end_cells[i], TEST_ROWKEY_COLUMN_CNT);
    mscan_ranges[i].get_border_flag().set_inclusive_start();
    mscan_ranges[i].get_border_flag().set_inclusive_end();
    ret = row_generate_.get_next_row(start_seeds.at(i), mscan_start_rows[i]);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = row_generate_.get_next_row(start_seeds.at(i) + count_per_range - 1, mscan_end_rows[i]);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  for (int64_t i = 0; i < start_seeds.count(); ++i) {
    ret = ranges.push_back(mscan_ranges[i]);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  convert_range(ranges, ext_ranges, allocator_);
  ret = sstable_.multi_scan(param_, context_, ext_ranges, scanner);
  ASSERT_EQ(OB_SUCCESS, ret);
  STORAGE_LOG(INFO, "multi scan ranges", K(ext_ranges));
  for (int64_t i = 0; i < start_seeds.count(); ++i) {
    const int64_t start = is_reverse_scan ? start_seeds.at(i) + count_per_range - 1 : start_seeds.at(i);
    const int64_t end = is_reverse_scan ? start_seeds.at(i) : start_seeds.at(i) + count_per_range - 1;
    for (int64_t j = start; compare(is_reverse_scan, j, end); j += step) {
      int64_t k = 0;
      ret = row_generate_.get_next_row(j, check_row);
      ASSERT_EQ(OB_SUCCESS, ret);
      STORAGE_LOG(INFO, "get_next_row_generate", K(check_row), K(*prow), K(j), K(i), K(end), K(start));
      ret = scanner->get_next_row(prow);
      ASSERT_EQ(OB_SUCCESS, ret);
      STORAGE_LOG(INFO, "get_next_row", K(check_row), K(*prow));
      ASSERT_TRUE(check_row.row_val_ == prow->row_val_);
      for (k = 0; OB_SUCC(ret) && k < skip_infos.count(); ++k) {
        if (skip_infos.at(k).start_key_ == j) {
          j = skip_infos.at(k).gap_key_ - step;
          break;
        }
      }
      if (k != skip_infos.count()) {
        ret = row_generate_.get_next_row(skip_infos.at(k).gap_key_, gap_row);
        ASSERT_EQ(OB_SUCCESS, ret);
        gap_rowkey.assign(gap_key_cells, TEST_ROWKEY_COLUMN_CNT);
        STORAGE_LOG(INFO, "skip range to", K(gap_rowkey), K(skip_infos.at(k)), K(i), K(start_seeds));
        ret = scanner->skip_range(i, &gap_rowkey, true);
        ASSERT_EQ(OB_SUCCESS, ret);
      }
    }
  }
  ret = scanner->get_next_row(prow);
  ASSERT_EQ(OB_ITER_END, ret);
  if (nullptr != scanner) {
    scanner->~ObStoreRowIterator();
    scanner = nullptr;
  }
}

void TestSSTableMultiScanner::test_one_case(const ObIArray<int64_t>& start_seeds, const int64_t count_per_range,
    const int64_t hit_mode, const bool is_reverse_scan)
{
  int ret = OB_SUCCESS;
  ObStoreRowkey mscan_rowkeys[TEST_MULTI_GET_CNT];
  ObStoreRange mscan_ranges[TEST_MULTI_GET_CNT];
  ObObj mscan_start_cells[TEST_MULTI_GET_CNT][TEST_COLUMN_CNT];
  ObObj mscan_end_cells[TEST_MULTI_GET_CNT][TEST_COLUMN_CNT];
  ObStoreRow mscan_start_rows[TEST_MULTI_GET_CNT];
  ObStoreRow mscan_end_rows[TEST_MULTI_GET_CNT];
  ObArray<ObStoreRange> ranges;
  ObArray<ObExtStoreRange> ext_ranges;
  ObObj check_cells[TEST_COLUMN_CNT];
  ObStoreRow check_row;
  ObStoreRowIterator* scanner = nullptr;
  const ObStoreRow* prow = NULL;
  if (HIT_PART == hit_mode) {
    ObArray<ObStoreRange> part_ranges;
    ObArray<ObStoreRange> tmp_ranges;
    ObArray<ObExtStoreRange> part_ext_ranges;
    ret = tmp_ranges.assign(ranges);
    ASSERT_EQ(OB_SUCCESS, ret);
    std::random_shuffle(tmp_ranges.begin(), tmp_ranges.end());
    for (int64_t i = 0; tmp_ranges.count() / 3; ++i) {
      ret = part_ranges.push_back(tmp_ranges.at(i));
      ASSERT_EQ(OB_SUCCESS, ret);
    }
    if (part_ranges.count() > 0) {
      convert_range(part_ranges, part_ext_ranges, allocator_);
      ret = sstable_.multi_scan(param_, context_, part_ext_ranges, scanner);
      ASSERT_EQ(OB_SUCCESS, ret);
      for (int64_t i = 0; OB_SUCC(ret); ++i) {
        ret = scanner->get_next_row(prow);
        ASSERT_EQ(OB_SUCCESS, ret);
      }
      ret = scanner->get_next_row(prow);
      ASSERT_EQ(OB_ITER_END, ret);
    }
  }
  if (nullptr != scanner) {
    scanner->~ObStoreRowIterator();
    scanner = nullptr;
  }
  for (int64_t i = 0; i < start_seeds.count(); ++i) {
    mscan_start_rows[i].row_val_.assign(mscan_start_cells[i], TEST_COLUMN_CNT);
    mscan_end_rows[i].row_val_.assign(mscan_end_cells[i], TEST_COLUMN_CNT);
    mscan_ranges[i].get_start_key().assign(mscan_start_cells[i], TEST_ROWKEY_COLUMN_CNT);
    mscan_ranges[i].get_end_key().assign(mscan_end_cells[i], TEST_ROWKEY_COLUMN_CNT);
    mscan_ranges[i].get_border_flag().set_inclusive_start();
    mscan_ranges[i].get_border_flag().set_inclusive_end();
    ret = row_generate_.get_next_row(start_seeds.at(i), mscan_start_rows[i]);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = row_generate_.get_next_row(start_seeds.at(i) + count_per_range - 1, mscan_end_rows[i]);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  for (int64_t i = 0; i < start_seeds.count(); ++i) {
    ret = ranges.push_back(mscan_ranges[i]);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  convert_range(ranges, ext_ranges, allocator_);
  ret = sstable_.multi_scan(param_, context_, ext_ranges, scanner);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = 0; i < start_seeds.count(); ++i) {
    for (int64_t j = 0; j < count_per_range; ++j) {
      const int64_t k = is_reverse_scan ? start_seeds.at(i) + count_per_range - j - 1 : start_seeds.at(i) + j;
      if (k < row_cnt_ || ext_ranges.at(i).get_range().is_single_rowkey()) {
        check_row.row_val_.assign(check_cells, TEST_COLUMN_CNT);
        ret = row_generate_.get_next_row(k, check_row);
        ASSERT_EQ(OB_SUCCESS, ret);
        ret = scanner->get_next_row(prow);
        ASSERT_EQ(OB_SUCCESS, ret);
        if (k < row_cnt_) {
          ASSERT_TRUE(check_row.row_val_ == prow->row_val_);
        } else {
          ASSERT_TRUE(prow->flag_ == ObActionFlag::OP_ROW_DOES_NOT_EXIST);
        }
      }
    }
  }
  ret = scanner->get_next_row(prow);
  ASSERT_EQ(OB_ITER_END, ret);
  if (nullptr != scanner) {
    scanner->~ObStoreRowIterator();
    scanner = nullptr;
  }

  if (HIT_ALL == hit_mode) {
    ret = sstable_.multi_scan(param_, context_, ext_ranges, scanner);
    ASSERT_EQ(OB_SUCCESS, ret);
    for (int64_t i = 0; i < start_seeds.count(); ++i) {
      for (int64_t j = 0; j < count_per_range; ++j) {
        const int64_t k = is_reverse_scan ? start_seeds.at(i) + count_per_range - j - 1 : start_seeds.at(i) + j;
        if (k < row_cnt_ || ext_ranges.at(i).get_range().is_single_rowkey()) {
          check_row.row_val_.assign(check_cells, TEST_COLUMN_CNT);
          ret = scanner->get_next_row(prow);
          ASSERT_EQ(OB_SUCCESS, ret);
          ret = row_generate_.get_next_row(k, check_row);
          if (k < row_cnt_) {
            ASSERT_TRUE(check_row.row_val_ == prow->row_val_);
          } else {
            ASSERT_TRUE(prow->flag_ == ObActionFlag::OP_ROW_DOES_NOT_EXIST);
          }
        }
      }
    }
    ret = scanner->get_next_row(prow);
    ASSERT_EQ(OB_ITER_END, ret);
    if (nullptr != scanner) {
      scanner->~ObStoreRowIterator();
      scanner = nullptr;
    }
  }
}

void TestSSTableMultiScanner::test_single_get_normal(const bool is_reverse_scan, const int64_t limit)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> seeds;
  // prepare query param and context
  ret = prepare_query_param(is_reverse_scan, limit);
  ASSERT_EQ(OB_SUCCESS, ret);

  // row in first macro
  ret = seeds.push_back(3);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(seeds, 1, i, is_reverse_scan);
  }

  // row in middle macro
  seeds.reset();
  seeds.push_back(row_cnt_ / 2);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(seeds, 1, i, is_reverse_scan);
  }

  // row in last macro, in cache
  seeds.reset();
  seeds.push_back(row_cnt_ - 3);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(seeds, 1, i, is_reverse_scan);
  }
  destroy_query_param();
}

void TestSSTableMultiScanner::test_single_get_border(const bool is_reverse_scan, const int64_t limit)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> seeds;
  // prepare query param and context
  ret = prepare_query_param(is_reverse_scan, limit);
  ASSERT_EQ(OB_SUCCESS, ret);

  // left border rowkey
  ret = seeds.push_back(0);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(seeds, 1, i, is_reverse_scan);
  }

  // right border rowkey
  seeds.reset();
  ret = seeds.push_back(row_cnt_ - 1);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(seeds, 1, i, is_reverse_scan);
  }

  // not exist
  seeds.reset();
  ret = seeds.push_back(row_cnt_);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(seeds, 1, i, is_reverse_scan);
  }
  destroy_query_param();
}

void TestSSTableMultiScanner::test_multi_get_normal(const bool is_reverse_scan, const int64_t limit)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> seeds;

  // prepare query param
  ret = prepare_query_param(is_reverse_scan, limit);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 2 rows exist test
  seeds.reuse();
  for (int64_t i = 0; i < 2; ++i) {
    ret = seeds.push_back(i * 11 + 2);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(seeds, 1, i, is_reverse_scan);
  }

  // 10 rows exist test
  seeds.reuse();
  for (int64_t i = 0; i < 10; ++i) {
    ret = seeds.push_back(i * 11 + 2);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(seeds, 1, i, is_reverse_scan);
  }

  // TEST_MULTI_GET_CNT rows test
  seeds.reuse();
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    ret = seeds.push_back(i);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(seeds, 1, i, is_reverse_scan);
  }

  // single row exist test
  seeds.reuse();
  ret = seeds.push_back(3);
  ASSERT_EQ(OB_SUCCESS, ret);

  // some row exist, while other rows not exist
  seeds.reuse();
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    ret = seeds.push_back(i + (i % 2 ? row_cnt_ : 0));
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(seeds, 1, i, is_reverse_scan);
  }

  // 2 row2 not exist
  seeds.reuse();
  for (int64_t i = 0; i < 2; ++i) {
    ret = seeds.push_back(i * 11 + 2 + row_cnt_);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(seeds, 1, i, is_reverse_scan);
  }

  // 10 rows not exist
  seeds.reuse();
  for (int64_t i = 0; i < 10; ++i) {
    ret = seeds.push_back(i * 11 + 2 + row_cnt_);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(seeds, 1, i, is_reverse_scan);
  }

  // TEST_MULTI_GET_CNT rows not exist
  seeds.reuse();
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    ret = seeds.push_back(i * 11 + 2 + row_cnt_);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(seeds, 1, i, is_reverse_scan);
  }
  destroy_query_param();
}

void TestSSTableMultiScanner::test_multi_get_border(const bool is_reverse_scan, const int64_t limit)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> seeds;
  // prepare query param
  ret = prepare_query_param(is_reverse_scan, limit);
  ASSERT_EQ(OB_SUCCESS, ret);

  // first row of sstable
  ret = seeds.push_back(0);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(seeds, 1, i, false);
  }

  // last row of sstable
  seeds.reset();
  ret = seeds.push_back(row_cnt_ - 1);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(seeds, 1, i, false);
  }

  // single row not exist
  seeds.reset();
  ret = seeds.push_back(row_cnt_);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(seeds, 1, i, false);
  }

  // TEST_MULTI_GET_CNT rows with same rowkey
  seeds.reset();
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    ret = seeds.push_back(row_cnt_ / 2);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(seeds, 1, i, is_reverse_scan);
  }
  destroy_query_param();
}

void TestSSTableMultiScanner::test_single_scan_normal(const bool is_reverse_scan, const int64_t limit)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> seeds;
  ObRandom random;
  const int64_t random_start = random.get(0, 10000000) % row_cnt_;
  const int64_t random_end = random.get(0, 100000000) % row_cnt_;
  const int64_t start = std::min(random_start, random_end);
  const int64_t end = std::max(random_start, random_end);

  // prepare query param
  ret = prepare_query_param(is_reverse_scan, limit);
  ASSERT_EQ(OB_SUCCESS, ret);

  // multiple rows exist
  ret = seeds.push_back(start);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(seeds, end - start, i, is_reverse_scan);
  }

  // multiple rows, partial exist
  seeds.reset();
  ret = seeds.push_back(start);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(seeds, row_cnt_ + 10 - start, i, is_reverse_scan);
  }

  // single row exist
  seeds.reset();
  ret = seeds.push_back(row_cnt_ / 2);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(seeds, 1, i, is_reverse_scan);
  }

  // not exist
  seeds.reset();
  ret = seeds.push_back(row_cnt_ + 10);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(seeds, 10, i, is_reverse_scan);
  }
  destroy_query_param();
}

void TestSSTableMultiScanner::test_single_scan_border(const bool is_reverse_scan, const int64_t limit)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> seeds;
  // prepare query param
  ret = prepare_query_param(is_reverse_scan, limit);
  ASSERT_EQ(OB_SUCCESS, ret);

  // full table scan
  ret = seeds.push_back(0);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(seeds, row_cnt_, i, is_reverse_scan);
  }

  // first row of sstable
  seeds.reset();
  ret = seeds.push_back(0);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(seeds, 1, i, is_reverse_scan);
  }

  // last row of sstable
  seeds.reset();
  ret = seeds.push_back(row_cnt_ - 1);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(seeds, 1, i, is_reverse_scan);
  }

  // not exist
  seeds.reset();
  ret = seeds.push_back(row_cnt_);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(seeds, 1, i, is_reverse_scan);
  }
  destroy_query_param();
}

void TestSSTableMultiScanner::test_multi_block_read_small_io(const bool is_reverse_scan, const int64_t limit)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> seeds;
  GCONF.multiblock_read_size = 10 * 1024;
  ret = prepare_query_param(is_reverse_scan, limit);
  ASSERT_EQ(OB_SUCCESS, ret);
  seeds.reuse();
  destroy_all_cache();
  for (int64_t i = 0; i < 10; ++i) {
    ret = seeds.push_back(i);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  test_one_case(seeds, 2, HIT_NONE, is_reverse_scan);

  destroy_all_cache();
  std::random_shuffle(seeds.begin(), seeds.end());
  test_one_case(seeds, 2, HIT_NONE, is_reverse_scan);
  destroy_query_param();
}

void TestSSTableMultiScanner::test_multi_block_read_big_continue_io(const bool is_reverse_scan, const int64_t limit)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> seeds;
  GCONF.multiblock_read_size = 10 * 1024;
  ret = prepare_query_param(is_reverse_scan, limit);
  ASSERT_EQ(OB_SUCCESS, ret);
  seeds.reuse();
  destroy_all_cache();
  for (int64_t i = 0; i < 100; ++i) {
    ret = seeds.push_back(i);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  test_one_case(seeds, 2, HIT_NONE, is_reverse_scan);

  // gap_size < multiblock_gap_size
  GCONF.multiblock_read_gap_size = 5 * 1024;
  seeds.reuse();
  destroy_all_cache();
  for (int64_t i = 0; i < 100; i += 15) {
    ret = seeds.push_back(i);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  test_one_case(seeds, 2, HIT_NONE, is_reverse_scan);

  // gap_size > multiblock_read_gap_size
  GCONF.multiblock_read_gap_size = 0;
  seeds.reuse();
  destroy_all_cache();
  for (int64_t i = 0; i < 100; i += 15) {
    ret = seeds.push_back(i);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  test_one_case(seeds, 2, HIT_NONE, is_reverse_scan);
  destroy_query_param();
}

void TestSSTableMultiScanner::test_multi_block_read_big_discrete_io(const bool is_reverse_scan, const int64_t limit)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> seeds;
  GCONF.multiblock_read_size = 10 * 1024;
  ret = prepare_query_param(is_reverse_scan, limit);
  ASSERT_EQ(OB_SUCCESS, ret);
  seeds.reuse();
  destroy_all_cache();
  for (int64_t i = 0; i < 100; ++i) {
    ret = seeds.push_back(i);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  std::random_shuffle(seeds.begin(), seeds.end());
  test_one_case(seeds, 2, HIT_NONE, is_reverse_scan);

  // gap_size < multiblock_gap_size
  GCONF.multiblock_read_gap_size = 5 * 1024;
  seeds.reuse();
  destroy_all_cache();
  for (int64_t i = 0; i < 100; i += 15) {
    ret = seeds.push_back(i);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  std::random_shuffle(seeds.begin(), seeds.end());
  test_one_case(seeds, 2, HIT_NONE, is_reverse_scan);

  // gap_size > multiblock_read_gap_size
  GCONF.multiblock_read_gap_size = 0;
  seeds.reuse();
  destroy_all_cache();
  for (int64_t i = 0; i < 100; i += 15) {
    ret = seeds.push_back(i);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  std::random_shuffle(seeds.begin(), seeds.end());
  test_one_case(seeds, 2, HIT_NONE, is_reverse_scan);
  destroy_query_param();
}

TEST_F(TestSSTableMultiScanner, test_single_get_normal)
{
  const bool is_reverse_scan = false;
  test_single_get_normal(is_reverse_scan, -1);
  test_single_get_normal(is_reverse_scan, 1);
}

TEST_F(TestSSTableMultiScanner, test_single_get_normal_reverse_scan)
{
  const bool is_reverse_scan = true;
  test_single_get_normal(is_reverse_scan, -1);
  test_single_get_normal(is_reverse_scan, 1);
}

TEST_F(TestSSTableMultiScanner, test_single_get_border)
{
  const bool is_reverse_scan = false;
  test_single_get_border(is_reverse_scan, -1);
  test_single_get_border(is_reverse_scan, 1);
}

TEST_F(TestSSTableMultiScanner, test_single_get_border_reverse_scan)
{
  const bool is_reverse_scan = true;
  test_single_get_border(is_reverse_scan, -1);
  test_single_get_border(is_reverse_scan, 1);
}

TEST_F(TestSSTableMultiScanner, test_multi_get_normal)
{
  const bool is_reverse_scan = false;
  test_multi_get_normal(is_reverse_scan, -1);
  test_multi_get_normal(is_reverse_scan, 1);
}

TEST_F(TestSSTableMultiScanner, test_multi_get_normal_reverse_scan)
{
  const bool is_reverse_scan = true;
  test_multi_get_normal(is_reverse_scan, -1);
  test_multi_get_normal(is_reverse_scan, 1);
}

TEST_F(TestSSTableMultiScanner, test_single_scan_normal)
{
  const bool is_reverse_scan = false;
  test_single_scan_normal(is_reverse_scan, -1);
  test_single_scan_normal(is_reverse_scan, 1);
}

TEST_F(TestSSTableMultiScanner, test_single_scan_normal_reverse_scan)
{
  const bool is_reverse_scan = true;
  test_single_scan_normal(is_reverse_scan, -1);
  test_single_scan_normal(is_reverse_scan, 1);
}

TEST_F(TestSSTableMultiScanner, test_single_scan_border)
{
  const bool is_reverse_scan = false;
  test_single_scan_border(is_reverse_scan, -1);
  test_single_scan_border(is_reverse_scan, 1);
}

TEST_F(TestSSTableMultiScanner, test_single_scan_border_reverse_scan)
{
  const bool is_reverse_scan = true;
  test_single_scan_border(is_reverse_scan, -1);
  test_single_scan_border(is_reverse_scan, 1);
}

void TestSSTableMultiScanner::test_multi_scan_multi_scan_range(
    const bool is_reverse_scan, const int64_t limit, const int64_t count_per_range)
{
  int ret = OB_SUCCESS;
  ObStoreRowIterator* scanner;
  ObStoreRange range;
  ObArray<ObStoreRange> ranges;
  ObArray<ObExtStoreRange> ext_ranges;
  ObStoreRow row;
  ObStoreRowkey rowkey;
  ObArray<int64_t> seeds;

  // prepare query param
  ret = prepare_query_param(is_reverse_scan, limit);
  ASSERT_EQ(OB_SUCCESS, ret);

  // invalid argument with 0 ranges
  ret = sstable_.multi_scan(param_, context_, ext_ranges, scanner);
  ASSERT_NE(OB_SUCCESS, ret);

  // invalid argument with empty rowkey
  // range.start_key_ = rowkey;
  // range.end_key_ = rowkey;
  // range.border_flag_.set_inclusive_start();
  // range.border_flag_.set_inclusive_end();
  // ret = ext_ranges.push_back(ObExtStoreRange(range));
  // ASSERT_EQ(OB_SUCCESS, ret);
  // ret = sstable_.multi_scan(param_, context_, ext_ranges, scanner);
  // ASSERT_NE(OB_SUCCESS, ret);

  // invalid invoke when not inited
  ranges.reuse();
  ret = ext_ranges.push_back(ObExtStoreRange(range));
  ASSERT_EQ(OB_SUCCESS, ret);
  ObSSTable sstable;
  ret = sstable.multi_scan(param_, context_, ext_ranges, scanner);
  ASSERT_NE(OB_SUCCESS, ret);

  // left border rowkey
  seeds.reset();
  ret = seeds.push_back(0);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(seeds, 1, i, is_reverse_scan);
  }

  // right border rowkey
  seeds.reset();
  ret = seeds.push_back(row_cnt_ - 2);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(seeds, 1, i, is_reverse_scan);
  }

  // not exist rowkey
  seeds.reset();
  ret = seeds.push_back(row_cnt_);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(seeds, 1, i, is_reverse_scan);
  }

  seeds.reset();
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    ret = seeds.push_back(i);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(seeds, count_per_range, i, is_reverse_scan);
  }

  seeds.reset();
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    ret = seeds.push_back(0);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(seeds, count_per_range, i, is_reverse_scan);
  }

  // not exist : all
  seeds.reset();
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    ret = seeds.push_back(row_cnt_ + i);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(seeds, count_per_range, i, is_reverse_scan);
  }

  // not exist : partial
  seeds.reset();
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    ret = seeds.push_back(i % 2 ? i : i + row_cnt_);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(seeds, count_per_range, i, is_reverse_scan);
  }
  destroy_query_param();
}

TEST_F(TestSSTableMultiScanner, test_multi_scan)
{
  const bool is_reverse_scan = false;
  int64_t limit = -1;
  for (int64_t i = 2; i < 20; i += 10) {
    test_multi_scan_multi_scan_range(is_reverse_scan, limit, i);
    limit += 2;
  }
}

TEST_F(TestSSTableMultiScanner, test_reverse_multi_scan)
{
  const bool is_reverse_scan = true;
  int64_t limit = -1;
  for (int64_t i = 2; i < 20; i += 10) {
    test_multi_scan_multi_scan_range(is_reverse_scan, limit, i);
    limit += 2;
  }
}

void TestSSTableMultiScanner::test_multi_scan_multi_get_with_scan(
    const bool is_reverse_scan, const int64_t limit, const int64_t count_per_range)
{
  int ret = OB_SUCCESS;
  ObStoreRowIterator* scanner = NULL;
  ObStoreRange range;
  ObArray<ObStoreRange> ranges;
  ObArray<ObExtStoreRange> ext_ranges;
  ObStoreRow row;
  ObStoreRowkey rowkey;
  const ObStoreRow* prow = NULL;
  ObObj check_cells[TEST_COLUMN_CNT];
  ObStoreRow check_row;
  int64_t row_cnt = 0;
  STORAGE_LOG(INFO, "limit info", K(is_reverse_scan), K(limit));

  // prepare query param
  ret = prepare_query_param(is_reverse_scan, limit);
  ASSERT_EQ(OB_SUCCESS, ret);

  // multi scan interact with multi get
  ObStoreRowkey mget_rowkeys[TEST_MULTI_GET_CNT];
  ObStoreRange mget_ranges[TEST_MULTI_GET_CNT];
  ObObj mget_start_cells[TEST_MULTI_GET_CNT][TEST_COLUMN_CNT];
  ObObj mget_end_cells[TEST_MULTI_GET_CNT][TEST_COLUMN_CNT];
  ObStoreRow mget_start_rows[TEST_MULTI_GET_CNT];
  ObStoreRow mget_end_rows[TEST_MULTI_GET_CNT];
  ranges.reuse();
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    mget_start_rows[i].row_val_.assign(mget_start_cells[i], TEST_COLUMN_CNT);
    mget_end_rows[i].row_val_.assign(mget_end_cells[i], TEST_COLUMN_CNT);
    mget_ranges[i].get_start_key().assign(mget_start_cells[i], TEST_ROWKEY_COLUMN_CNT);
    mget_ranges[i].get_end_key().assign(mget_end_cells[i], TEST_ROWKEY_COLUMN_CNT);
    mget_ranges[i].get_border_flag().set_inclusive_start();
    mget_ranges[i].get_border_flag().set_inclusive_end();
    ret = row_generate_.get_next_row(i, mget_start_rows[i]);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = row_generate_.get_next_row(i + (i % 2 ? count_per_range - 1 : 0), mget_end_rows[i]);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    ret = ranges.push_back(mget_ranges[i]);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  destroy_cache();
  convert_range(ranges, ext_ranges, allocator_);
  STORAGE_LOG(INFO, "multi scan begin");
  ret = sstable_.multi_scan(param_, context_, ext_ranges, scanner);
  ASSERT_EQ(OB_SUCCESS, ret);
  row_cnt = 0;
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    const int64_t p = i;
    if (p % 2) {
      for (int64_t j = 0; j < count_per_range; ++j) {
        const int64_t k = is_reverse_scan ? i + count_per_range - j - 1 : i + j;
        check_row.row_val_.assign(check_cells, TEST_COLUMN_CNT);
        ret = scanner->get_next_row(prow);
        if (OB_SUCCESS != ret) {
          STORAGE_LOG(INFO, "limit info", K(limit));
        }
        ASSERT_EQ(OB_SUCCESS, ret);
        ret = row_generate_.get_next_row(k, check_row);
        ++row_cnt;
        ASSERT_EQ(OB_SUCCESS, ret);
        ASSERT_TRUE(check_row.row_val_ == prow->row_val_);
      }
    } else {
      check_row.row_val_.assign(check_cells, TEST_COLUMN_CNT);
      ret = scanner->get_next_row(prow);
      ASSERT_EQ(OB_SUCCESS, ret);
      ret = row_generate_.get_next_row(p, check_row);
      ++row_cnt;
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_TRUE(check_row.row_val_ == prow->row_val_);
    }
  }
  ret = scanner->get_next_row(prow);
  ASSERT_EQ(OB_ITER_END, ret);

  // TODO: fix here
  // ObCostMetrics cost_metrics;
  // ret = sstable_.estimate_multi_scan_cost(context_.query_flag_, table_key_.table_id_, ext_ranges, cost_metrics);
  // ASSERT_EQ(OB_SUCCESS, ret);
  // ASSERT_EQ(row_cnt, cost_metrics.result_row_count_);

  // first half multi scan, second half multi get
  ranges.reuse();
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    mget_start_rows[i].row_val_.assign(mget_start_cells[i], TEST_COLUMN_CNT);
    mget_end_rows[i].row_val_.assign(mget_end_cells[i], TEST_COLUMN_CNT);
    mget_ranges[i].get_start_key().assign(mget_start_cells[i], TEST_ROWKEY_COLUMN_CNT);
    mget_ranges[i].get_end_key().assign(mget_end_cells[i], TEST_ROWKEY_COLUMN_CNT);
    mget_ranges[i].get_border_flag().set_inclusive_start();
    mget_ranges[i].get_border_flag().set_inclusive_end();
    ret = row_generate_.get_next_row(i, mget_start_rows[i]);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = row_generate_.get_next_row(i + (i < TEST_MULTI_GET_CNT / 2 ? count_per_range - 1 : 0), mget_end_rows[i]);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = ranges.push_back(mget_ranges[i]);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  destroy_cache();
  convert_range(ranges, ext_ranges, allocator_);
  ret = sstable_.multi_scan(param_, context_, ext_ranges, scanner);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    const int64_t p = i;
    if (p < TEST_MULTI_GET_CNT / 2) {
      for (int64_t j = 0; j < count_per_range; ++j) {
        const int64_t k = is_reverse_scan ? i + count_per_range - j - 1 : i + j;
        check_row.row_val_.assign(check_cells, TEST_COLUMN_CNT);
        ret = scanner->get_next_row(prow);
        ASSERT_EQ(OB_SUCCESS, ret);
        ret = row_generate_.get_next_row(k, check_row);
        ASSERT_EQ(OB_SUCCESS, ret);
        ASSERT_TRUE(check_row.row_val_ == prow->row_val_);
      }
    } else {
      check_row.row_val_.assign(check_cells, TEST_COLUMN_CNT);
      ret = scanner->get_next_row(prow);
      ASSERT_EQ(OB_SUCCESS, ret);
      ret = row_generate_.get_next_row(p, check_row);
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_TRUE(check_row.row_val_ == prow->row_val_);
    }
  }
  ret = scanner->get_next_row(prow);
  ASSERT_EQ(OB_ITER_END, ret);

  // first half multi get, second half multi scan
  ranges.reuse();
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    mget_start_rows[i].row_val_.assign(mget_start_cells[i], TEST_COLUMN_CNT);
    mget_end_rows[i].row_val_.assign(mget_end_cells[i], TEST_COLUMN_CNT);
    mget_ranges[i].get_start_key().assign(mget_start_cells[i], TEST_ROWKEY_COLUMN_CNT);
    mget_ranges[i].get_end_key().assign(mget_end_cells[i], TEST_ROWKEY_COLUMN_CNT);
    mget_ranges[i].get_border_flag().set_inclusive_start();
    mget_ranges[i].get_border_flag().set_inclusive_end();
    ret = row_generate_.get_next_row(i, mget_start_rows[i]);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = row_generate_.get_next_row(i + (i > TEST_MULTI_GET_CNT / 2 ? count_per_range - 1 : 0), mget_end_rows[i]);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = ranges.push_back(mget_ranges[i]);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  destroy_cache();
  convert_range(ranges, ext_ranges, allocator_);
  ret = sstable_.multi_scan(param_, context_, ext_ranges, scanner);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    const int64_t p = i;
    if (p > TEST_MULTI_GET_CNT / 2) {
      for (int64_t j = 0; j < count_per_range; ++j) {
        const int64_t k = is_reverse_scan ? i + count_per_range - j - 1 : i + j;
        check_row.row_val_.assign(check_cells, TEST_COLUMN_CNT);
        ret = scanner->get_next_row(prow);
        ASSERT_EQ(OB_SUCCESS, ret);
        ret = row_generate_.get_next_row(k, check_row);
        ASSERT_EQ(OB_SUCCESS, ret);
        ASSERT_TRUE(check_row.row_val_ == prow->row_val_);
      }
    } else {
      check_row.row_val_.assign(check_cells, TEST_COLUMN_CNT);
      ret = scanner->get_next_row(prow);
      ASSERT_EQ(OB_SUCCESS, ret);
      ret = row_generate_.get_next_row(p, check_row);
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_TRUE(check_row.row_val_ == prow->row_val_);
    }
  }
  ret = scanner->get_next_row(prow);
  ASSERT_EQ(OB_ITER_END, ret);

  // first one multi get, others multi scan
  ranges.reuse();
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    mget_start_rows[i].row_val_.assign(mget_start_cells[i], TEST_COLUMN_CNT);
    mget_end_rows[i].row_val_.assign(mget_end_cells[i], TEST_COLUMN_CNT);
    mget_ranges[i].get_start_key().assign(mget_start_cells[i], TEST_ROWKEY_COLUMN_CNT);
    mget_ranges[i].get_end_key().assign(mget_end_cells[i], TEST_ROWKEY_COLUMN_CNT);
    mget_ranges[i].get_border_flag().set_inclusive_start();
    mget_ranges[i].get_border_flag().set_inclusive_end();
    ret = row_generate_.get_next_row(i, mget_start_rows[i]);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = row_generate_.get_next_row(i + (i != 0 ? count_per_range - 1 : 0), mget_end_rows[i]);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = ranges.push_back(mget_ranges[i]);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  convert_range(ranges, ext_ranges, allocator_);
  destroy_cache();
  ret = sstable_.multi_scan(param_, context_, ext_ranges, scanner);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    const int64_t p = i;
    if (p != 0) {
      for (int64_t j = 0; j < count_per_range; ++j) {
        const int64_t k = is_reverse_scan ? i + count_per_range - j - 1 : i + j;
        check_row.row_val_.assign(check_cells, TEST_COLUMN_CNT);
        ret = scanner->get_next_row(prow);
        ASSERT_EQ(OB_SUCCESS, ret);
        ret = row_generate_.get_next_row(k, check_row);
        ASSERT_EQ(OB_SUCCESS, ret);
        ASSERT_TRUE(check_row.row_val_ == prow->row_val_);
      }
    } else {
      check_row.row_val_.assign(check_cells, TEST_COLUMN_CNT);
      ret = scanner->get_next_row(prow);
      ASSERT_EQ(OB_SUCCESS, ret);
      ret = row_generate_.get_next_row(p, check_row);
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_TRUE(check_row.row_val_ == prow->row_val_);
    }
  }
  ret = scanner->get_next_row(prow);
  ASSERT_EQ(OB_ITER_END, ret);

  // first one multi scan, others multi get
  ranges.reuse();
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    mget_start_rows[i].row_val_.assign(mget_start_cells[i], TEST_COLUMN_CNT);
    mget_end_rows[i].row_val_.assign(mget_end_cells[i], TEST_COLUMN_CNT);
    mget_ranges[i].get_start_key().assign(mget_start_cells[i], TEST_ROWKEY_COLUMN_CNT);
    mget_ranges[i].get_end_key().assign(mget_end_cells[i], TEST_ROWKEY_COLUMN_CNT);
    mget_ranges[i].get_border_flag().set_inclusive_start();
    mget_ranges[i].get_border_flag().set_inclusive_end();
    ret = row_generate_.get_next_row(i, mget_start_rows[i]);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = row_generate_.get_next_row(i + (i == 0 ? count_per_range - 1 : 0), mget_end_rows[i]);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = ranges.push_back(mget_ranges[i]);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  destroy_cache();
  convert_range(ranges, ext_ranges, allocator_);
  ret = sstable_.multi_scan(param_, context_, ext_ranges, scanner);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    const int64_t p = i;
    if (p == 0) {
      for (int64_t j = 0; j < count_per_range; ++j) {
        const int64_t k = is_reverse_scan ? i + count_per_range - j - 1 : i + j;
        check_row.row_val_.assign(check_cells, TEST_COLUMN_CNT);
        ret = scanner->get_next_row(prow);
        ASSERT_EQ(OB_SUCCESS, ret);
        ret = row_generate_.get_next_row(k, check_row);
        ASSERT_EQ(OB_SUCCESS, ret);
        ASSERT_TRUE(check_row.row_val_ == prow->row_val_);
      }
    } else {
      check_row.row_val_.assign(check_cells, TEST_COLUMN_CNT);
      ret = scanner->get_next_row(prow);
      ASSERT_EQ(OB_SUCCESS, ret);
      ret = row_generate_.get_next_row(p, check_row);
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_TRUE(check_row.row_val_ == prow->row_val_);
    }
  }
  ret = scanner->get_next_row(prow);
  ASSERT_EQ(OB_ITER_END, ret);

  // multi scan not exist row
  STORAGE_LOG(DEBUG, "multi_scan_not_exist_row");
  ranges.reuse();
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    mget_start_rows[i].row_val_.assign(mget_start_cells[i], TEST_COLUMN_CNT);
    mget_end_rows[i].row_val_.assign(mget_end_cells[i], TEST_COLUMN_CNT);
    mget_ranges[i].get_start_key().assign(mget_start_cells[i], TEST_ROWKEY_COLUMN_CNT);
    mget_ranges[i].get_end_key().assign(mget_end_cells[i], TEST_ROWKEY_COLUMN_CNT);
    mget_ranges[i].get_border_flag().set_inclusive_start();
    mget_ranges[i].get_border_flag().set_inclusive_end();
    ret = row_generate_.get_next_row(i + (i % 2 ? row_cnt_ : 0), mget_start_rows[i]);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = row_generate_.get_next_row(i + (i % 2 ? row_cnt_ + count_per_range - 1 : 0), mget_end_rows[i]);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = ranges.push_back(mget_ranges[i]);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  destroy_cache();
  convert_range(ranges, ext_ranges, allocator_);
  ret = sstable_.multi_scan(param_, context_, ext_ranges, scanner);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    const int64_t p = i;
    if (p % 2) {
      continue;
    } else {
      check_row.row_val_.assign(check_cells, TEST_COLUMN_CNT);
      ret = scanner->get_next_row(prow);
      ASSERT_EQ(OB_SUCCESS, ret);
      ret = row_generate_.get_next_row(p, check_row);
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_TRUE(check_row.row_val_ == prow->row_val_);
    }
  }
  ret = scanner->get_next_row(prow);
  ASSERT_EQ(OB_ITER_END, ret);
  destroy_query_param();
}

TEST_F(TestSSTableMultiScanner, test_multi_get_with_scan)
{
  const bool is_reverse_scan = false;
  int64_t limit = -1;
  for (int64_t i = 2; i < 20; i += 10) {
    test_multi_scan_multi_get_with_scan(is_reverse_scan, limit, i);
    limit += 2;
  }
}

TEST_F(TestSSTableMultiScanner, test_reverse_multi_get_with_scan)
{
  const bool is_reverse_scan = true;
  int64_t limit = -1;
  for (int64_t i = 2; i < 20; i += 10) {
    test_multi_scan_multi_get_with_scan(is_reverse_scan, limit, i);
    limit += 2;
  }
}

TEST_F(TestSSTableMultiScanner, test_multi_block_read_small_io)
{
  test_multi_block_read_small_io(false, -1);
}

TEST_F(TestSSTableMultiScanner, test_multi_block_read_small_io_reverse_scan)
{
  test_multi_block_read_small_io(true, -1);
}

TEST_F(TestSSTableMultiScanner, test_multi_block_read_small_io_limit)
{
  test_multi_block_read_small_io(false, 1);
}

TEST_F(TestSSTableMultiScanner, test_multi_block_read_small_io_limit_reverse_scan)
{
  test_multi_block_read_small_io(true, 1);
}

TEST_F(TestSSTableMultiScanner, test_multi_block_read_big_continue_io)
{
  test_multi_block_read_big_continue_io(false, -1);
}

TEST_F(TestSSTableMultiScanner, test_multi_block_read_big_continue_io_reverse_scan)
{
  test_multi_block_read_big_continue_io(true, -1);
}

TEST_F(TestSSTableMultiScanner, test_multi_block_read_big_continue_io_limit)
{
  test_multi_block_read_big_continue_io(false, 1);
}

TEST_F(TestSSTableMultiScanner, test_multi_block_read_big_continue_io_limit_reverse_scan)
{
  test_multi_block_read_big_continue_io(true, 1);
}

TEST_F(TestSSTableMultiScanner, test_multi_block_read_big_discrete_io)
{
  test_multi_block_read_big_discrete_io(false, -1);
}

TEST_F(TestSSTableMultiScanner, test_multi_block_read_big_discrete_io_reverse_scan)
{
  test_multi_block_read_big_discrete_io(true, -1);
}

TEST_F(TestSSTableMultiScanner, test_multi_block_read_big_discrete_io_limit)
{
  test_multi_block_read_big_discrete_io(false, 1);
}

TEST_F(TestSSTableMultiScanner, test_multi_block_read_big_discrete_io_limit_reverse_scan)
{
  test_multi_block_read_big_discrete_io(true, 1);
}

TEST_F(TestSSTableMultiScanner, test_skip_single_range)
{
  ObArray<int64_t> start_seeds;
  ObArray<SkipInfo> skip_infos;
  const int64_t count_per_range = 1000;
  int64_t start_seed = 20;
  ASSERT_EQ(OB_SUCCESS, start_seeds.push_back(start_seed));
  SkipInfo info;
  info.start_key_ = 205;
  info.gap_key_ = 400;
  ASSERT_EQ(OB_SUCCESS, skip_infos.push_back(info));
  test_skip_range(start_seeds, count_per_range, false, skip_infos);
}

TEST_F(TestSSTableMultiScanner, test_skip_single_range_reverse_scan)
{
  ObArray<int64_t> start_seeds;
  ObArray<SkipInfo> skip_infos;
  const int64_t count_per_range = 1000;
  int64_t start_seed = 20;
  ASSERT_EQ(OB_SUCCESS, start_seeds.push_back(start_seed));
  SkipInfo info;
  info.gap_key_ = 205;
  info.start_key_ = 400;
  ASSERT_EQ(OB_SUCCESS, skip_infos.push_back(info));
  test_skip_range(start_seeds, count_per_range, true, skip_infos);
}

TEST_F(TestSSTableMultiScanner, test_skip_multiple_range)
{
  ObArray<int64_t> start_seeds;
  ObArray<SkipInfo> skip_infos;
  const int64_t count_per_range = 50;
  int64_t start_seed = 20;
  ASSERT_EQ(OB_SUCCESS, start_seeds.push_back(start_seed));
  start_seed = 40;
  ASSERT_EQ(OB_SUCCESS, start_seeds.push_back(start_seed));
  start_seed = 120;
  ASSERT_EQ(OB_SUCCESS, start_seeds.push_back(start_seed));
  start_seed = 300;
  ASSERT_EQ(OB_SUCCESS, start_seeds.push_back(start_seed));
  SkipInfo info;
  info.start_key_ = 25;
  info.gap_key_ = 40;
  ASSERT_EQ(OB_SUCCESS, skip_infos.push_back(info));
  info.start_key_ = 53;
  info.gap_key_ = 55;
  ASSERT_EQ(OB_SUCCESS, skip_infos.push_back(info));
  info.start_key_ = 57;
  info.gap_key_ = 68;
  ASSERT_EQ(OB_SUCCESS, skip_infos.push_back(info));
  info.start_key_ = 72;
  info.gap_key_ = 75;
  ASSERT_EQ(OB_SUCCESS, skip_infos.push_back(info));
  info.start_key_ = 140;
  info.gap_key_ = 155;
  ASSERT_EQ(OB_SUCCESS, skip_infos.push_back(info));
  info.start_key_ = 159;
  info.gap_key_ = 162;
  ASSERT_EQ(OB_SUCCESS, skip_infos.push_back(info));
  info.start_key_ = 301;
  info.gap_key_ = 310;
  ASSERT_EQ(OB_SUCCESS, skip_infos.push_back(info));
  info.start_key_ = 319;
  info.gap_key_ = 323;
  ASSERT_EQ(OB_SUCCESS, skip_infos.push_back(info));
  test_skip_range(start_seeds, count_per_range, false, skip_infos);
}

TEST_F(TestSSTableMultiScanner, test_skip_multiple_range_reverse_scan)
{
  ObArray<int64_t> start_seeds;
  ObArray<SkipInfo> skip_infos;
  const int64_t count_per_range = 50;
  int64_t start_seed = 20;
  ASSERT_EQ(OB_SUCCESS, start_seeds.push_back(start_seed));
  start_seed = 40;
  ASSERT_EQ(OB_SUCCESS, start_seeds.push_back(start_seed));
  start_seed = 120;
  ASSERT_EQ(OB_SUCCESS, start_seeds.push_back(start_seed));
  start_seed = 300;
  ASSERT_EQ(OB_SUCCESS, start_seeds.push_back(start_seed));
  SkipInfo info;
  info.gap_key_ = 25;
  info.start_key_ = 40;
  ASSERT_EQ(OB_SUCCESS, skip_infos.push_back(info));
  info.gap_key_ = 53;
  info.start_key_ = 55;
  ASSERT_EQ(OB_SUCCESS, skip_infos.push_back(info));
  info.gap_key_ = 57;
  info.start_key_ = 68;
  ASSERT_EQ(OB_SUCCESS, skip_infos.push_back(info));
  info.gap_key_ = 72;
  info.start_key_ = 75;
  ASSERT_EQ(OB_SUCCESS, skip_infos.push_back(info));
  info.gap_key_ = 140;
  info.start_key_ = 155;
  ASSERT_EQ(OB_SUCCESS, skip_infos.push_back(info));
  info.gap_key_ = 159;
  info.start_key_ = 162;
  ASSERT_EQ(OB_SUCCESS, skip_infos.push_back(info));
  info.gap_key_ = 301;
  info.start_key_ = 310;
  ASSERT_EQ(OB_SUCCESS, skip_infos.push_back(info));
  info.gap_key_ = 319;
  info.start_key_ = 323;
  ASSERT_EQ(OB_SUCCESS, skip_infos.push_back(info));
  test_skip_range(start_seeds, count_per_range, true, skip_infos);
}

}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char** argv)
{
  system("rm -f test_sstable_multi_scan.log*");
  OB_LOGGER.set_file_name("test_sstable_multi_scan.log");
  OB_LOGGER.set_log_level("INFO");
  CLOG_LOG(INFO, "begin unittest: test_sstable_multi_scan");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
