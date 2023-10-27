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
#define private public
#define protected public

#include "storage/access/ob_index_tree_prefetcher.h"
#include "storage/access/ob_sstable_row_multi_scanner.h"
#include "ob_index_block_data_prepare.h"

namespace oceanbase
{
using namespace storage;
using namespace common;
namespace blocksstable
{
class TestSSTableRowMultiScanner : public TestIndexBlockDataPrepare
{
public:
  TestSSTableRowMultiScanner();
  virtual ~TestSSTableRowMultiScanner();
  static void SetUpTestCase();
  static void TearDownTestCase();

  virtual void SetUp();
  virtual void TearDown();
protected:
  void generate_range(const int64_t start, const int64_t end, ObDatumRange &range);
public:
  void test_one_case(
      const ObIArray<int64_t> &start_seeds,
      const int64_t count_per_range,
      const int64_t hit_mode,
      const bool is_reverse_scan);
  void test_single_get_normal(const bool is_reverse_scan);
  void test_single_get_border(const bool is_reverse_scan);
  void test_multi_get_normal(const bool is_reverse_scan);
  void test_multi_get_border(const bool is_reverse_scan);
  void test_single_scan_normal(const bool is_reverse_scan);
  void test_single_scan_border(const bool is_reverse_scan);
  void test_multi_scan_multi_scan_range(
      const bool is_reverse_scan,
      const int64_t count_per_range);
  void test_multi_scan_multi_get_with_scan(
      const bool is_reverse_scan,
      const int64_t count_per_range);

protected:
  static const int64_t TEST_MULTI_GET_CNT = 2000;
  enum CacheHitMode
  {
    HIT_ALL = 0,
    HIT_NONE,
    HIT_PART,
    HIT_MAX,
  };
private:
  ObArenaAllocator allocator_;
  ObDatumRow start_row_;
  ObDatumRow end_row_;
};

TestSSTableRowMultiScanner::TestSSTableRowMultiScanner()
  : TestIndexBlockDataPrepare("Test sstable row multi scanner")
{
}

TestSSTableRowMultiScanner::~TestSSTableRowMultiScanner()
{
}

void TestSSTableRowMultiScanner::SetUpTestCase()
{
  TestIndexBlockDataPrepare::SetUpTestCase();
}

void TestSSTableRowMultiScanner::TearDownTestCase()
{
  TestIndexBlockDataPrepare::TearDownTestCase();
}

void TestSSTableRowMultiScanner::SetUp()
{
  TestIndexBlockDataPrepare::SetUp();
  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle_));

  ASSERT_EQ(OB_SUCCESS, start_row_.init(allocator_, TEST_COLUMN_CNT));
  ASSERT_EQ(OB_SUCCESS, end_row_.init(allocator_, TEST_COLUMN_CNT));
}

void TestSSTableRowMultiScanner::TearDown()
{
  tablet_handle_.reset();
  TestIndexBlockDataPrepare::TearDown();
}

void TestSSTableRowMultiScanner::generate_range(
    const int64_t start,
    const int64_t end,
    ObDatumRange &range)
{
  ObDatumRowkey tmp_rowkey;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(start, start_row_));
  tmp_rowkey.assign(start_row_.storage_datums_, TEST_ROWKEY_COLUMN_CNT);
  ASSERT_EQ(OB_SUCCESS, tmp_rowkey.deep_copy(range.start_key_, allocator_));
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(end, end_row_));
  tmp_rowkey.assign(end_row_.storage_datums_, TEST_ROWKEY_COLUMN_CNT);
  ASSERT_EQ(OB_SUCCESS, tmp_rowkey.deep_copy(range.end_key_, allocator_));
  range.border_flag_.set_inclusive_start();
  range.border_flag_.set_inclusive_end();
}

void TestSSTableRowMultiScanner::test_one_case(
    const ObIArray<int64_t> &start_seeds,
    const int64_t count_per_range,
    const int64_t hit_mode,
    const bool is_reverse_scan)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator test_allocator(ObMemAttr(MTL_ID(), "TestAlloc"));
  ObDatumRange mscan_ranges[TEST_MULTI_GET_CNT];
  ObSSTableRowMultiScanner scanner;
  ObSSTableRowMultiScanner kv_scanner;
  ObSEArray<ObDatumRange, TEST_MULTI_GET_CNT> ranges;
  const ObDatumRow *prow = NULL;
  const ObDatumRow *kv_prow = NULL;
  //if (HIT_PART == hit_mode) {
    //ObArray<ObDatumRange> part_ranges;
    //ObArray<ObDatumRange> tmp_ranges;
    //ASSERT_EQ(OB_SUCCESS, tmp_ranges.assign(ranges));
    //std::random_shuffle(tmp_ranges.begin(), tmp_ranges.end());
    //for (int64_t i = 0; tmp_ranges.count() / 3; ++i) {
      //ret = part_ranges.push_back(tmp_ranges.at(i));
      //ASSERT_EQ(OB_SUCCESS, ret);
    //}
    //if (part_ranges.count() > 0) {
      //ASSERT_EQ(OB_SUCCESS, scanner.inner_open(
              //iter_param_,
              //context_,
              //&sstable_,
              //&part_ranges));
      //for (int64_t i = 0; OB_SUCC(ret); ++i) {
        //ret = scanner.inner_get_next_row(prow);
        //ASSERT_EQ(OB_SUCCESS, ret);
      //}
      //ret = scanner.inner_get_next_row(prow);
      //ASSERT_EQ(OB_ITER_END, ret);
    //}
    //scanner.reuse();
  //}

  ObDatumRow start_row;
  ObDatumRow end_row;
  ObDatumRow check_row;
  ASSERT_EQ(OB_SUCCESS, start_row.init(test_allocator, TEST_COLUMN_CNT));
  ASSERT_EQ(OB_SUCCESS, end_row.init(test_allocator, TEST_COLUMN_CNT));
  ASSERT_EQ(OB_SUCCESS, check_row.init(test_allocator, TEST_COLUMN_CNT));
  for (int64_t i = 0; i < start_seeds.count(); ++i) {
    ObDatumRowkey tmp_rowkey;
    mscan_ranges[i].border_flag_.set_inclusive_start();
    mscan_ranges[i].border_flag_.set_inclusive_end();
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(start_seeds.at(i), start_row));
    tmp_rowkey.assign(start_row.storage_datums_, TEST_ROWKEY_COLUMN_CNT);
    ASSERT_EQ(OB_SUCCESS, tmp_rowkey.deep_copy(mscan_ranges[i].start_key_, test_allocator));
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(start_seeds.at(i) + count_per_range - 1, end_row));
    tmp_rowkey.assign(end_row.storage_datums_, TEST_ROWKEY_COLUMN_CNT);
    ASSERT_EQ(OB_SUCCESS, tmp_rowkey.deep_copy(mscan_ranges[i].end_key_, test_allocator));
  }
  for (int64_t i = 0; i < start_seeds.count(); ++i) {
    ASSERT_EQ(OB_SUCCESS, ranges.push_back(mscan_ranges[i]));
  }
  ASSERT_EQ(OB_SUCCESS, scanner.inner_open(iter_param_, context_, &sstable_, &ranges));
  ASSERT_EQ(OB_SUCCESS, kv_scanner.inner_open(iter_param_, context_, &ddl_kv_, &ranges));
  for (int64_t i = 0; i < start_seeds.count(); ++i) {
    for (int64_t j = 0; j < count_per_range; ++j) {
      const int64_t k = is_reverse_scan ? start_seeds.at(i) + count_per_range - j - 1 : start_seeds.at(i) + j;
      if (k < row_cnt_ || count_per_range == 1) {
        ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(k, check_row));
        if (k < row_cnt_) {
          ASSERT_EQ(OB_SUCCESS, scanner.inner_get_next_row(prow));
          ASSERT_TRUE(*prow == check_row);
          ASSERT_EQ(OB_SUCCESS, kv_scanner.inner_get_next_row(kv_prow));
          ASSERT_TRUE(*kv_prow == check_row);
        }
      }
    }
  }
  ASSERT_EQ(OB_ITER_END, scanner.inner_get_next_row(prow));
  scanner.reuse();
  ASSERT_EQ(OB_ITER_END, kv_scanner.inner_get_next_row(kv_prow));
  kv_scanner.reuse();

  if (HIT_ALL == hit_mode) {
    ASSERT_EQ(OB_SUCCESS, scanner.inner_open(
            iter_param_,
            context_,
            &sstable_,
            &ranges));
    ASSERT_EQ(OB_SUCCESS, kv_scanner.inner_open(
            iter_param_,
            context_,
            &ddl_kv_,
            &ranges));
    for (int64_t i = 0; i < start_seeds.count(); ++i) {
      for (int64_t j = 0; j < count_per_range; ++j) {
        const int64_t k = is_reverse_scan ? start_seeds.at(i) + count_per_range - j - 1 : start_seeds.at(i) + j;
          if (k < row_cnt_ || count_per_range == 1) {
          ret = row_generate_.get_next_row(k, check_row);
          if (k < row_cnt_) {
            ASSERT_EQ(OB_SUCCESS, scanner.inner_get_next_row(prow));
            ASSERT_TRUE(*prow == check_row);
            ASSERT_EQ(OB_SUCCESS, kv_scanner.inner_get_next_row(kv_prow));
            ASSERT_TRUE(*kv_prow == check_row);
          }
        }
      }
    }
    ret = scanner.inner_get_next_row(prow);
    ASSERT_EQ(OB_ITER_END, ret);
    ret = kv_scanner.inner_get_next_row(kv_prow);
    ASSERT_EQ(OB_ITER_END, ret);
    scanner.reuse();
    kv_scanner.reuse();
  }
}

void TestSSTableRowMultiScanner::test_single_get_normal(const bool is_reverse_scan)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator test_allocator(ObMemAttr(MTL_ID(), "TestAlloc"));
  ObArray<int64_t> seeds;
  // prepare query param and context
  prepare_query_param(is_reverse_scan, &test_allocator);

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

void TestSSTableRowMultiScanner::test_single_get_border(const bool is_reverse_scan)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator test_allocator(ObMemAttr(MTL_ID(), "TestAlloc"));
  ObArray<int64_t> seeds;
  // prepare query param and context
  prepare_query_param(is_reverse_scan, &test_allocator);

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

void TestSSTableRowMultiScanner::test_multi_get_normal(const bool is_reverse_scan)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator test_allocator(ObMemAttr(MTL_ID(), "TestAlloc"));
  ObArray<int64_t> seeds;

  // prepare query param
  prepare_query_param(is_reverse_scan, &test_allocator);

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

void TestSSTableRowMultiScanner::test_multi_get_border(const bool is_reverse_scan)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator test_allocator(ObMemAttr(MTL_ID(), "TestAlloc"));
  ObArray<int64_t> seeds;
  // prepare query param
  prepare_query_param(is_reverse_scan, &test_allocator);

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

void TestSSTableRowMultiScanner::test_single_scan_normal(const bool is_reverse_scan)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator test_allocator(ObMemAttr(MTL_ID(), "TestAlloc"));
  ObArray<int64_t> seeds;
  ObRandom random;
  const int64_t random_start = random.get(0, 10000000) % row_cnt_;
  const int64_t random_end = random.get(0, 100000000) % row_cnt_;
  const int64_t start = std::min(random_start, random_end);
  const int64_t end = std::max(random_start, random_end);

  // prepare query param
  prepare_query_param(is_reverse_scan, &test_allocator);

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

void TestSSTableRowMultiScanner::test_single_scan_border(const bool is_reverse_scan)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator test_allocator(ObMemAttr(MTL_ID(), "TestAlloc"));
  ObArray<int64_t> seeds;
  // prepare query param
  prepare_query_param(is_reverse_scan, &test_allocator);

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

TEST_F(TestSSTableRowMultiScanner, test_single_get_normal)
{
  bool is_reverse_scan = false;
  test_single_get_normal(is_reverse_scan);
  is_reverse_scan = true;
  test_single_get_normal(is_reverse_scan);
}

TEST_F(TestSSTableRowMultiScanner, test_single_get_border)
{
  bool is_reverse_scan = false;
  test_single_get_border(is_reverse_scan);
  is_reverse_scan = true;
  test_single_get_border(is_reverse_scan);
}

TEST_F(TestSSTableRowMultiScanner, test_multi_get_normal)
{
  bool is_reverse_scan = false;
  test_multi_get_normal(is_reverse_scan);
  is_reverse_scan = true;
  test_multi_get_normal(is_reverse_scan);
}

TEST_F(TestSSTableRowMultiScanner, test_single_scan_normal)
{
  bool is_reverse_scan = false;
  test_single_scan_normal(is_reverse_scan);
  is_reverse_scan = true;
  test_single_scan_normal(is_reverse_scan);
}

TEST_F(TestSSTableRowMultiScanner, test_single_scan_border)
{
  bool is_reverse_scan = false;
  test_single_scan_border(is_reverse_scan);
  is_reverse_scan = true;
  test_single_scan_border(is_reverse_scan);
}

void TestSSTableRowMultiScanner::test_multi_scan_multi_scan_range(
    const bool is_reverse_scan,
    const int64_t count_per_range)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator test_allocator(ObMemAttr(MTL_ID(), "TestAlloc"));
  ObStoreRange range;
  ObArray<ObStoreRange> ranges;
  ObStoreRow row;
  ObStoreRowkey rowkey;
  ObArray<int64_t> seeds;
  ObSSTableRowMultiScanner scanner;

  // prepare query param
  prepare_query_param(is_reverse_scan, &test_allocator);

  //invalid argument with 0 ranges
  /*
     ASSERT_EQ(OB_SUCCESS, scanner.inner_open(
     iter_param_,
     context_,
     &sstable_,
     &ranges));
     */

  //invalid argument with empty rowkey
  //range.start_key_ = rowkey;
  //range.end_key_ = rowkey;
  //range.border_flag_.set_inclusive_start();
  //range.border_flag_.set_inclusive_end();
  //ret = ranges.push_back(range);
  //ASSERT_EQ(OB_SUCCESS, ret);
  //ret = sstable_.multi_scan(param_, context_, ranges, scanner);
  //ASSERT_NE(OB_SUCCESS, ret);

  //invalid invoke when not inited
  /*
     ranges.reuse();
     ASSERT_EQ(OB_SUCCESS, ranges.push_back(range));
     ASSERT_EQ(OB_SUCCESS, scanner.inner_open(
     iter_param_,
     context_,
     &sstable_,
     &ranges));
     */

  //left border rowkey
  seeds.reset();
  ret = seeds.push_back(0);
  ASSERT_EQ(OB_SUCCESS, seeds.push_back(0));
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(seeds, 1, i, is_reverse_scan);
  }

  //right border rowkey
  seeds.reset();
  ret = seeds.push_back(row_cnt_ - 2);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_one_case(seeds, 1, i, is_reverse_scan);
  }

  //not exist rowkey
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

TEST_F(TestSSTableRowMultiScanner, test_multi_scan)
{
  bool is_reverse_scan = false;
  for (int64_t i = 2; i < 20; i += 10) {
    test_multi_scan_multi_scan_range(is_reverse_scan, i);
  }
  is_reverse_scan = true;
  for (int64_t i = 2; i < 20; i += 10) {
    test_multi_scan_multi_scan_range(is_reverse_scan, i);
  }
}

void TestSSTableRowMultiScanner::test_multi_scan_multi_get_with_scan(
    const bool is_reverse_scan,
    const int64_t count_per_range)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator test_allocator(ObMemAttr(MTL_ID(), "TestAlloc"));
  ObArenaAllocator query_param_allocator(ObMemAttr(MTL_ID(), "TestAlloc"));
  ObDatumRange range;
  ObArray<ObDatumRange> ranges;
  ObDatumRow row;
  ObDatumRowkey rowkey;
  const ObDatumRow *prow = NULL;
  const ObDatumRow *kv_prow = NULL;
  int64_t row_cnt = 0;
  ObSSTableRowMultiScanner scanner;
  ObSSTableRowMultiScanner kv_scanner;

  // prepare query param
  prepare_query_param(is_reverse_scan, &query_param_allocator);

  // multi scan interact with multi get
  ObDatumRange mget_ranges[TEST_MULTI_GET_CNT];
  ObDatumRow start_row;
  ObDatumRow end_row;
  ObDatumRow check_row;
  ASSERT_EQ(OB_SUCCESS, start_row.init(test_allocator, TEST_COLUMN_CNT));
  ASSERT_EQ(OB_SUCCESS, end_row.init(test_allocator, TEST_COLUMN_CNT));
  ASSERT_EQ(OB_SUCCESS, check_row.init(test_allocator, TEST_COLUMN_CNT));
  ranges.reuse();
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    ObDatumRowkey tmp_rowkey;
    mget_ranges[i].border_flag_.set_inclusive_start();
    mget_ranges[i].border_flag_.set_inclusive_end();
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(i, start_row));
    tmp_rowkey.assign(start_row.storage_datums_, TEST_ROWKEY_COLUMN_CNT);
    ASSERT_EQ(OB_SUCCESS, tmp_rowkey.deep_copy(mget_ranges[i].start_key_, test_allocator));
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(i + (i % 2 ? count_per_range - 1 : 0), end_row));
    tmp_rowkey.assign(end_row.storage_datums_, TEST_ROWKEY_COLUMN_CNT);
    ASSERT_EQ(OB_SUCCESS, tmp_rowkey.deep_copy(mget_ranges[i].end_key_, test_allocator));
  }
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    ret = ranges.push_back(mget_ranges[i]);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  STORAGE_LOG(INFO, "multi scan begin");
  ASSERT_EQ(OB_SUCCESS, scanner.inner_open(
          iter_param_,
          context_,
          &sstable_,
          &ranges));
  ASSERT_EQ(OB_SUCCESS, kv_scanner.inner_open(
          iter_param_,
          context_,
          &ddl_kv_,
          &ranges));
  row_cnt = 0;
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    const int64_t p = i;
    if (p % 2) {
      for (int64_t j = 0; j < count_per_range; ++j) {
        const int64_t k = is_reverse_scan ? i + count_per_range - j - 1 : i + j;
        ASSERT_EQ(OB_SUCCESS, scanner.inner_get_next_row(prow));
        ASSERT_EQ(OB_SUCCESS, kv_scanner.inner_get_next_row(kv_prow));
        ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(k, check_row));
        ++row_cnt;
        ASSERT_TRUE(*prow == check_row);
        ASSERT_TRUE(*kv_prow == check_row);
      }
    } else {
      ASSERT_EQ(OB_SUCCESS, scanner.inner_get_next_row(prow));
      ASSERT_EQ(OB_SUCCESS, kv_scanner.inner_get_next_row(kv_prow));
      ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(p, check_row));
      ++row_cnt;
      ASSERT_TRUE(*prow == check_row);
      ASSERT_TRUE(*kv_prow == check_row);
    }
  }
  ASSERT_EQ(OB_ITER_END, scanner.inner_get_next_row(prow));
  ASSERT_EQ(OB_ITER_END, kv_scanner.inner_get_next_row(kv_prow));
  scanner.reuse();
  kv_scanner.reuse();

  // first half multi scan, second half multi get
  ranges.reuse();
  test_allocator.reuse();
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    ObDatumRowkey tmp_rowkey;
    mget_ranges[i].border_flag_.set_inclusive_start();
    mget_ranges[i].border_flag_.set_inclusive_end();
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(i, start_row));
    tmp_rowkey.assign(start_row.storage_datums_, TEST_ROWKEY_COLUMN_CNT);
    ASSERT_EQ(OB_SUCCESS, tmp_rowkey.deep_copy(mget_ranges[i].start_key_, test_allocator));
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(i + (i < TEST_MULTI_GET_CNT / 2 ? count_per_range - 1 : 0), end_row));
    tmp_rowkey.assign(end_row.storage_datums_, TEST_ROWKEY_COLUMN_CNT);
    ASSERT_EQ(OB_SUCCESS, tmp_rowkey.deep_copy(mget_ranges[i].end_key_, test_allocator));
    ASSERT_EQ(OB_SUCCESS, ranges.push_back(mget_ranges[i]));
  }
  ASSERT_EQ(OB_SUCCESS, scanner.inner_open(
          iter_param_,
          context_,
          &sstable_,
          &ranges));
  ASSERT_EQ(OB_SUCCESS, kv_scanner.inner_open(
          iter_param_,
          context_,
          &ddl_kv_,
          &ranges));
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    const int64_t p = i;
    if (p < TEST_MULTI_GET_CNT / 2) {
      for (int64_t j = 0; j < count_per_range; ++j) {
        const int64_t k = is_reverse_scan ? i + count_per_range - j - 1 : i + j;
        ret = scanner.inner_get_next_row(prow);
        ASSERT_EQ(OB_SUCCESS, ret);
        ret = kv_scanner.inner_get_next_row(kv_prow);
        ASSERT_EQ(OB_SUCCESS, ret);
        ret = row_generate_.get_next_row(k, check_row);
        ASSERT_EQ(OB_SUCCESS, ret);
        ASSERT_TRUE(*prow == check_row);
        ASSERT_TRUE(*kv_prow == check_row);
      }
    } else {
      ret = scanner.inner_get_next_row(prow);
      ASSERT_EQ(OB_SUCCESS, ret);
      ret = kv_scanner.inner_get_next_row(kv_prow);
      ASSERT_EQ(OB_SUCCESS, ret);
      ret = row_generate_.get_next_row(p, check_row);
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_TRUE(*prow == check_row);
      ASSERT_TRUE(*kv_prow == check_row);
    }
  }
  ASSERT_EQ(OB_ITER_END, scanner.inner_get_next_row(prow));
  ASSERT_EQ(OB_ITER_END, kv_scanner.inner_get_next_row(kv_prow));
  scanner.reuse();
  kv_scanner.reuse();

  // first half multi get, second half multi scan
  ranges.reuse();
  test_allocator.reuse();
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    ObDatumRowkey tmp_rowkey;
    mget_ranges[i].border_flag_.set_inclusive_start();
    mget_ranges[i].border_flag_.set_inclusive_end();
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(i, start_row));
    tmp_rowkey.assign(start_row.storage_datums_, TEST_ROWKEY_COLUMN_CNT);
    ASSERT_EQ(OB_SUCCESS, tmp_rowkey.deep_copy(mget_ranges[i].start_key_, test_allocator));
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(i + (i > TEST_MULTI_GET_CNT / 2 ? count_per_range - 1 : 0),
                                                     end_row));
    tmp_rowkey.assign(end_row.storage_datums_, TEST_ROWKEY_COLUMN_CNT);
    ASSERT_EQ(OB_SUCCESS, tmp_rowkey.deep_copy(mget_ranges[i].end_key_, test_allocator));
    ASSERT_EQ(OB_SUCCESS, ranges.push_back(mget_ranges[i]));
  }
  ASSERT_EQ(OB_SUCCESS, scanner.inner_open(
          iter_param_,
          context_,
          &sstable_,
          &ranges));
  ASSERT_EQ(OB_SUCCESS, kv_scanner.inner_open(
          iter_param_,
          context_,
          &ddl_kv_,
          &ranges));

  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    const int64_t p = i;
    if (p > TEST_MULTI_GET_CNT / 2) {
      for (int64_t j = 0; j < count_per_range; ++j) {
        const int64_t k = is_reverse_scan ? i + count_per_range - j - 1 : i + j;
        ret = scanner.inner_get_next_row(prow);
        ASSERT_EQ(OB_SUCCESS, ret);
        ret = kv_scanner.inner_get_next_row(kv_prow);
        ASSERT_EQ(OB_SUCCESS, ret);
        ret = row_generate_.get_next_row(k, check_row);
        ASSERT_EQ(OB_SUCCESS, ret);
        ASSERT_TRUE(*prow == check_row);
        ASSERT_TRUE(*kv_prow == check_row);
      }
    } else {
      ret = scanner.inner_get_next_row(prow);
      ASSERT_EQ(OB_SUCCESS, ret);
      ret = kv_scanner.inner_get_next_row(kv_prow);
      ASSERT_EQ(OB_SUCCESS, ret);
      ret = row_generate_.get_next_row(p, check_row);
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_TRUE(*prow == check_row);
      ASSERT_TRUE(*kv_prow == check_row);
    }
  }
  ret = scanner.inner_get_next_row(prow);
  ASSERT_EQ(OB_ITER_END, ret);
  ret = kv_scanner.inner_get_next_row(kv_prow);
  ASSERT_EQ(OB_ITER_END, ret);
  scanner.reuse();
  kv_scanner.reuse();

  // first one multi get, others multi scan
  ranges.reuse();
  test_allocator.reuse();
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    ObDatumRowkey tmp_rowkey;
    mget_ranges[i].border_flag_.set_inclusive_start();
    mget_ranges[i].border_flag_.set_inclusive_end();
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(i, start_row));
    tmp_rowkey.assign(start_row.storage_datums_, TEST_ROWKEY_COLUMN_CNT);
    ASSERT_EQ(OB_SUCCESS, tmp_rowkey.deep_copy(mget_ranges[i].start_key_, test_allocator));
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(i + (i != 0 ? count_per_range - 1 : 0), end_row));
    tmp_rowkey.assign(end_row.storage_datums_, TEST_ROWKEY_COLUMN_CNT);
    ASSERT_EQ(OB_SUCCESS, tmp_rowkey.deep_copy(mget_ranges[i].end_key_, test_allocator));
    ASSERT_EQ(OB_SUCCESS, ranges.push_back(mget_ranges[i]));
  }
  ASSERT_EQ(OB_SUCCESS, scanner.inner_open(
          iter_param_,
          context_,
          &sstable_,
          &ranges));
  ASSERT_EQ(OB_SUCCESS, kv_scanner.inner_open(
          iter_param_,
          context_,
          &ddl_kv_,
          &ranges));
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    const int64_t p = i;
    if (p != 0) {
      for (int64_t j = 0; j < count_per_range; ++j) {
        const int64_t k = is_reverse_scan ? i + count_per_range - j - 1 : i + j;
        ret = scanner.inner_get_next_row(prow);
        ASSERT_EQ(OB_SUCCESS, ret);
        ret = kv_scanner.inner_get_next_row(kv_prow);
        ASSERT_EQ(OB_SUCCESS, ret);
        ret = row_generate_.get_next_row(k, check_row);
        ASSERT_EQ(OB_SUCCESS, ret);
        ASSERT_TRUE(*prow == check_row);
        ASSERT_TRUE(*kv_prow == check_row);
      }
    } else {
      ret = scanner.inner_get_next_row(prow);
      ASSERT_EQ(OB_SUCCESS, ret);
      ret = kv_scanner.inner_get_next_row(kv_prow);
      ASSERT_EQ(OB_SUCCESS, ret);
      ret = row_generate_.get_next_row(p, check_row);
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_TRUE(*prow == check_row);
      ASSERT_TRUE(*kv_prow == check_row);
    }
  }
  ASSERT_EQ(OB_ITER_END, scanner.inner_get_next_row(prow));
  ASSERT_EQ(OB_ITER_END, kv_scanner.inner_get_next_row(kv_prow));
  scanner.reuse();
  kv_scanner.reuse();

  // first one multi scan, others multi get
  ranges.reuse();
  test_allocator.reuse();
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    ObDatumRowkey tmp_rowkey;
    mget_ranges[i].border_flag_.set_inclusive_start();
    mget_ranges[i].border_flag_.set_inclusive_end();
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(i, start_row));
    tmp_rowkey.assign(start_row.storage_datums_, TEST_ROWKEY_COLUMN_CNT);
    ASSERT_EQ(OB_SUCCESS, tmp_rowkey.deep_copy(mget_ranges[i].start_key_, test_allocator));
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(i + (i == 0 ? count_per_range - 1 : 0), end_row));
    tmp_rowkey.assign(end_row.storage_datums_, TEST_ROWKEY_COLUMN_CNT);
    ASSERT_EQ(OB_SUCCESS, tmp_rowkey.deep_copy(mget_ranges[i].end_key_, test_allocator));
    ASSERT_EQ(OB_SUCCESS, ranges.push_back(mget_ranges[i]));
  }
  ASSERT_EQ(OB_SUCCESS, scanner.inner_open(
          iter_param_,
          context_,
          &sstable_,
          &ranges));
  ASSERT_EQ(OB_SUCCESS, kv_scanner.inner_open(
          iter_param_,
          context_,
          &ddl_kv_,
          &ranges));
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    const int64_t p = i;
    if (p == 0) {
      for (int64_t j = 0; j < count_per_range; ++j) {
        const int64_t k = is_reverse_scan ? i + count_per_range - j - 1 : i + j;
        ret = scanner.inner_get_next_row(prow);
        ASSERT_EQ(OB_SUCCESS, ret);
        ret = kv_scanner.inner_get_next_row(kv_prow);
        ASSERT_EQ(OB_SUCCESS, ret);
        ret = row_generate_.get_next_row(k, check_row);
        ASSERT_EQ(OB_SUCCESS, ret);
        ASSERT_TRUE(*prow == check_row);
        ASSERT_TRUE(*kv_prow == check_row);
      }
    } else {
      ret = scanner.inner_get_next_row(prow);
      ASSERT_EQ(OB_SUCCESS, ret);
      ret = kv_scanner.inner_get_next_row(kv_prow);
      ASSERT_EQ(OB_SUCCESS, ret);
      ret = row_generate_.get_next_row(p, check_row);
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_TRUE(*prow == check_row);
      ASSERT_TRUE(*kv_prow == check_row);
    }
  }
  ASSERT_EQ(OB_ITER_END, scanner.inner_get_next_row(prow));
  ASSERT_EQ(OB_ITER_END, kv_scanner.inner_get_next_row(kv_prow));
  scanner.reuse();
  kv_scanner.reuse();

  // multi scan not exist row
  STORAGE_LOG(DEBUG, "multi_scan_not_exist_row");
  ranges.reuse();
  test_allocator.reuse();
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    ObDatumRowkey tmp_rowkey;
    mget_ranges[i].border_flag_.set_inclusive_start();
    mget_ranges[i].border_flag_.set_inclusive_end();
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(i + (i % 2 ? row_cnt_ : 0), start_row));
    tmp_rowkey.assign(start_row.storage_datums_, TEST_ROWKEY_COLUMN_CNT);
    ASSERT_EQ(OB_SUCCESS, tmp_rowkey.deep_copy(mget_ranges[i].start_key_, test_allocator));
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(i + (i % 2 ? row_cnt_ + count_per_range - 1 : 0), end_row));
    tmp_rowkey.assign(end_row.storage_datums_, TEST_ROWKEY_COLUMN_CNT);
    ASSERT_EQ(OB_SUCCESS, tmp_rowkey.deep_copy(mget_ranges[i].end_key_, test_allocator));
    ASSERT_EQ(OB_SUCCESS, ranges.push_back(mget_ranges[i]));
  }
  ASSERT_EQ(OB_SUCCESS, scanner.inner_open(
          iter_param_,
          context_,
          &sstable_,
          &ranges));
  ASSERT_EQ(OB_SUCCESS, kv_scanner.inner_open(
          iter_param_,
          context_,
          &ddl_kv_,
          &ranges));
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    const int64_t p = i;
    if (p % 2) {
      continue;
    } else {
      ret = scanner.inner_get_next_row(prow);
      ASSERT_EQ(OB_SUCCESS, ret);
      ret = kv_scanner.inner_get_next_row(kv_prow);
      ASSERT_EQ(OB_SUCCESS, ret);
      ret = row_generate_.get_next_row(p, check_row);
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_TRUE(*prow == check_row);
      ASSERT_TRUE(*kv_prow == check_row);
    }
  }
  ASSERT_EQ(OB_ITER_END, scanner.inner_get_next_row(prow));
  ASSERT_EQ(OB_ITER_END, kv_scanner.inner_get_next_row(kv_prow));
  destroy_query_param();
}

TEST_F(TestSSTableRowMultiScanner, test_multi_get_with_scan)
{
  bool is_reverse_scan = false;
  for (int64_t i = 2; i < 20; i += 10) {
    test_multi_scan_multi_get_with_scan(is_reverse_scan, i);
  }
  is_reverse_scan = true;
  for (int64_t i = 2; i < 20; i += 10) {
    test_multi_scan_multi_get_with_scan(is_reverse_scan, i);
  }
}

}
}

int main(int argc, char **argv)
{
  system("rm -f test_sstable_row_multi_scanner.log*");
  OB_LOGGER.set_file_name("test_sstable_row_multi_scanner.log", true, true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
