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
#include "storage/access/ob_sstable_row_multi_getter.h"
#include "ob_index_block_data_prepare.h"

namespace oceanbase
{
using namespace storage;
using namespace common;
namespace blocksstable
{
class TestDDLMergeRowMultiScanner : public TestIndexBlockDataPrepare
{
public:
  TestDDLMergeRowMultiScanner();
  virtual ~TestDDLMergeRowMultiScanner();
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
  static const int64_t TEST_MULTI_GET_CNT = 100;
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

TestDDLMergeRowMultiScanner::TestDDLMergeRowMultiScanner()
  : TestIndexBlockDataPrepare("Test sstable row multi scanner")
{
  is_ddl_merge_data_ = true;
  max_row_cnt_ = 150000;
  max_partial_row_cnt_ = 78881;
  co_sstable_row_offset_ = max_partial_row_cnt_ - 1;
  partial_kv_start_idx_ = 3;
}

TestDDLMergeRowMultiScanner::~TestDDLMergeRowMultiScanner()
{

}

void TestDDLMergeRowMultiScanner::SetUpTestCase()
{
  TestIndexBlockDataPrepare::SetUpTestCase();
}

void TestDDLMergeRowMultiScanner::TearDownTestCase()
{
  TestIndexBlockDataPrepare::TearDownTestCase();
}

void TestDDLMergeRowMultiScanner::SetUp()
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

void TestDDLMergeRowMultiScanner::TearDown()
{
  tablet_handle_.get_obj()->ddl_kv_count_ = 0;
  tablet_handle_.get_obj()->ddl_kvs_ = nullptr;
  tablet_handle_.reset();
  TestIndexBlockDataPrepare::TearDown();
}

void TestDDLMergeRowMultiScanner::generate_range(
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

void TestDDLMergeRowMultiScanner::test_one_case(
    const ObIArray<int64_t> &start_seeds,
    const int64_t count_per_range,
    const bool is_reverse_scan)
{
  int ret = OB_SUCCESS;
  ObDatumRange mscan_ranges[TEST_MULTI_GET_CNT];
  ObSSTableRowMultiScanner<> scanner;
  ObSSTableRowMultiScanner<> merge_ddl_scanner;
  ObSEArray<ObDatumRange, TEST_MULTI_GET_CNT> ranges;
  const ObDatumRow *prow = NULL;
  const ObDatumRow *kv_prow = NULL;

  ObDatumRow start_row;
  ObDatumRow end_row;
  ObDatumRow check_row;
  ASSERT_EQ(OB_SUCCESS, start_row.init(allocator_, TEST_COLUMN_CNT));
  ASSERT_EQ(OB_SUCCESS, end_row.init(allocator_, TEST_COLUMN_CNT));
  ASSERT_EQ(OB_SUCCESS, check_row.init(allocator_, TEST_COLUMN_CNT));
  for (int64_t i = 0; i < start_seeds.count(); ++i) {
    ObDatumRowkey tmp_rowkey;
    mscan_ranges[i].border_flag_.set_inclusive_start();
    mscan_ranges[i].border_flag_.set_inclusive_end();
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(start_seeds.at(i), start_row));
    tmp_rowkey.assign(start_row.storage_datums_, TEST_ROWKEY_COLUMN_CNT);
    ASSERT_EQ(OB_SUCCESS, tmp_rowkey.deep_copy(mscan_ranges[i].start_key_, allocator_));
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(start_seeds.at(i) + count_per_range - 1, end_row));
    tmp_rowkey.assign(end_row.storage_datums_, TEST_ROWKEY_COLUMN_CNT);
    ASSERT_EQ(OB_SUCCESS, tmp_rowkey.deep_copy(mscan_ranges[i].end_key_, allocator_));
  }
  for (int64_t i = 0; i < start_seeds.count(); ++i) {
    ASSERT_EQ(OB_SUCCESS, ranges.push_back(mscan_ranges[i]));
  }
  ASSERT_EQ(OB_SUCCESS, scanner.init(iter_param_, context_, &sstable_, &ranges));
  ASSERT_EQ(OB_SUCCESS, merge_ddl_scanner.init(iter_param_, context_, &partial_sstable_, &ranges));
  for (int64_t i = 0; i < start_seeds.count(); ++i) {
    for (int64_t j = 0; j < count_per_range; ++j) {
      const int64_t k = is_reverse_scan ? start_seeds.at(i) + count_per_range - j - 1 : start_seeds.at(i) + j;
      if (k < row_cnt_ || count_per_range == 1) {
        ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(k, check_row));
        if (k < row_cnt_) {
          ASSERT_EQ(OB_SUCCESS, scanner.inner_get_next_row(prow));
          ASSERT_TRUE(*prow == check_row);
          ASSERT_EQ(OB_SUCCESS, merge_ddl_scanner.inner_get_next_row(kv_prow));
          ASSERT_TRUE(*kv_prow == check_row);
        }
      }
    }
  }
  ASSERT_EQ(OB_ITER_END, scanner.inner_get_next_row(prow));
  scanner.reuse();
  ASSERT_EQ(OB_ITER_END, merge_ddl_scanner.inner_get_next_row(kv_prow));
  merge_ddl_scanner.reuse();
}

void TestDDLMergeRowMultiScanner::test_single_get_normal(const bool is_reverse_scan)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> seeds;
  // prepare query param and context
  prepare_query_param(is_reverse_scan);

  // row in first macro
  ret = seeds.push_back(3);
  ASSERT_EQ(OB_SUCCESS, ret);
  test_one_case(seeds, 1, is_reverse_scan);

  // row in middle macro
  seeds.reset();
  seeds.push_back(row_cnt_ / 2);
  ASSERT_EQ(OB_SUCCESS, ret);
  test_one_case(seeds, 1, is_reverse_scan);

  // row in last macro, in cache
  seeds.reset();
  seeds.push_back(row_cnt_ - 3);
  ASSERT_EQ(OB_SUCCESS, ret);
  test_one_case(seeds, 1, is_reverse_scan);
  destroy_query_param();
}

void TestDDLMergeRowMultiScanner::test_single_get_border(const bool is_reverse_scan)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> seeds;
  // prepare query param and context
  prepare_query_param(is_reverse_scan);

  // left border rowkey
  ret = seeds.push_back(0);
  ASSERT_EQ(OB_SUCCESS, ret);
  test_one_case(seeds, 1, is_reverse_scan);

  // right border rowkey
  seeds.reset();
  ret = seeds.push_back(row_cnt_ - 1);
  ASSERT_EQ(OB_SUCCESS, ret);
  test_one_case(seeds, 1, is_reverse_scan);

  // not exist
  seeds.reset();
  ret = seeds.push_back(row_cnt_);
  ASSERT_EQ(OB_SUCCESS, ret);
  test_one_case(seeds, 1, is_reverse_scan);
  destroy_query_param();
}

void TestDDLMergeRowMultiScanner::test_multi_get_normal(const bool is_reverse_scan)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> seeds;

  // prepare query param
  prepare_query_param(is_reverse_scan);

  // 2 rows exist
  seeds.reuse();
  for (int64_t i = 0; i < 2; ++i) {
    ret = seeds.push_back(i * 11 + 2);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  test_one_case(seeds, 1, is_reverse_scan);

  // TEST_MULTI_GET_CNT rows exist
  seeds.reuse();
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    ret = seeds.push_back(i);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  test_one_case(seeds, 1, is_reverse_scan);

  // 2 row2 not exist
  seeds.reuse();
  for (int64_t i = 0; i < 2; ++i) {
    ret = seeds.push_back(i * 11 + 2 + row_cnt_);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  test_one_case(seeds, 1, is_reverse_scan);

  // TEST_MULTI_GET_CNT rows not exist
  seeds.reuse();
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    ret = seeds.push_back(i * 11 + 2 + row_cnt_);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  test_one_case(seeds, 1, is_reverse_scan);

  // some row exist, while other rows not exist
  seeds.reuse();
  for (int64_t i = 0; i < 10; ++i) {
    ret = seeds.push_back(i + (i % 2 ? row_cnt_ : 0));
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  test_one_case(seeds, 1, is_reverse_scan);

  destroy_query_param();
}

void TestDDLMergeRowMultiScanner::test_multi_get_border(const bool is_reverse_scan)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> seeds;
  // prepare query param
  prepare_query_param(is_reverse_scan);

  // first row of sstable
  ret = seeds.push_back(0);
  ASSERT_EQ(OB_SUCCESS, ret);
  test_one_case(seeds, 1, false);

  // last row of sstable
  seeds.reset();
  ret = seeds.push_back(row_cnt_ - 1);
  ASSERT_EQ(OB_SUCCESS, ret);
  test_one_case(seeds, 1, false);

  // 100 rows with same rowkey
  seeds.reset();
  for (int64_t i = 0; i < 100; ++i) {
    ret = seeds.push_back(row_cnt_ / 2);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  test_one_case(seeds, 1, is_reverse_scan);
  destroy_query_param();
}

void TestDDLMergeRowMultiScanner::test_single_scan_normal(const bool is_reverse_scan)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> seeds;
  ObRandom random;
  const int64_t random_start = random.get(0, 10000000) % row_cnt_;
  const int64_t random_end = random.get(0, 100000000) % row_cnt_;
  const int64_t start = std::min(random_start, random_end);
  const int64_t end = std::max(random_start, random_end);

  // prepare query param
  prepare_query_param(is_reverse_scan);

  // multiple rows exist
  ret = seeds.push_back(start);
  ASSERT_EQ(OB_SUCCESS, ret);
  test_one_case(seeds, end - start, is_reverse_scan);

  // multiple rows, partial exist
  seeds.reset();
  ret = seeds.push_back(start);
  ASSERT_EQ(OB_SUCCESS, ret);
  test_one_case(seeds, row_cnt_ + 10 - start, is_reverse_scan);

  // single row exist
  seeds.reset();
  ret = seeds.push_back(row_cnt_ / 2);
  ASSERT_EQ(OB_SUCCESS, ret);
  test_one_case(seeds, 1, is_reverse_scan);

  // not exist
  seeds.reset();
  ret = seeds.push_back(row_cnt_ + 10);
  ASSERT_EQ(OB_SUCCESS, ret);
  test_one_case(seeds, 10, is_reverse_scan);
  destroy_query_param();
}

void TestDDLMergeRowMultiScanner::test_single_scan_border(const bool is_reverse_scan)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> seeds;
  // prepare query param
  prepare_query_param(is_reverse_scan);

  // full table scan
  ret = seeds.push_back(0);
  ASSERT_EQ(OB_SUCCESS, ret);
  test_one_case(seeds, row_cnt_, is_reverse_scan);

  // first row of sstable
  seeds.reset();
  ret = seeds.push_back(0);
  ASSERT_EQ(OB_SUCCESS, ret);
  test_one_case(seeds, 1, is_reverse_scan);

  // last row of sstable
  seeds.reset();
  ret = seeds.push_back(row_cnt_ - 1);
  ASSERT_EQ(OB_SUCCESS, ret);
  test_one_case(seeds, 1, is_reverse_scan);

  // not exist
  seeds.reset();
  ret = seeds.push_back(row_cnt_);
  ASSERT_EQ(OB_SUCCESS, ret);
  test_one_case(seeds, 1, is_reverse_scan);
  destroy_query_param();
}

TEST_F(TestDDLMergeRowMultiScanner, test_single_get_normal)
{
  bool is_reverse_scan = false;
  test_single_get_normal(is_reverse_scan);
  is_reverse_scan = true;
  test_single_get_normal(is_reverse_scan);
}

TEST_F(TestDDLMergeRowMultiScanner, test_single_get_border)
{
  bool is_reverse_scan = false;
  test_single_get_border(is_reverse_scan);
  is_reverse_scan = true;
  test_single_get_border(is_reverse_scan);
}

TEST_F(TestDDLMergeRowMultiScanner, test_multi_get_normal)
{
  bool is_reverse_scan = false;
  test_multi_get_normal(is_reverse_scan);
  is_reverse_scan = true;
  test_multi_get_normal(is_reverse_scan);
}

TEST_F(TestDDLMergeRowMultiScanner, test_multi_get_border)
{
  bool is_reverse_scan = false;
  test_multi_get_border(is_reverse_scan);
  is_reverse_scan = true;
  test_multi_get_border(is_reverse_scan);
}

TEST_F(TestDDLMergeRowMultiScanner, test_single_scan_normal)
{
  bool is_reverse_scan = false;
  test_single_scan_normal(is_reverse_scan);
  is_reverse_scan = true;
  test_single_scan_normal(is_reverse_scan);
}

TEST_F(TestDDLMergeRowMultiScanner, test_single_scan_border)
{
  bool is_reverse_scan = false;
  test_single_scan_border(is_reverse_scan);
  is_reverse_scan = true;
  test_single_scan_border(is_reverse_scan);
}

void TestDDLMergeRowMultiScanner::test_multi_scan_multi_scan_range(
    const bool is_reverse_scan,
    const int64_t count_per_range)
{
  int ret = OB_SUCCESS;
  ObStoreRange range;
  ObArray<ObStoreRange> ranges;
  ObStoreRow row;
  ObStoreRowkey rowkey;
  ObArray<int64_t> seeds;
  ObSSTableRowMultiScanner<> scanner;

  // prepare query param
  prepare_query_param(is_reverse_scan);

  //left border rowkey
  seeds.reset();
  ret = seeds.push_back(0);
  ASSERT_EQ(OB_SUCCESS, seeds.push_back(0));
  test_one_case(seeds, 1, is_reverse_scan);

  //right border rowkey
  seeds.reset();
  ret = seeds.push_back(row_cnt_ - 1);
  ASSERT_EQ(OB_SUCCESS, ret);
  test_one_case(seeds, 1, is_reverse_scan);

  //not exist rowkey
  seeds.reset();
  ret = seeds.push_back(row_cnt_);
  ASSERT_EQ(OB_SUCCESS, ret);
  test_one_case(seeds, 1, is_reverse_scan);

  //20 exist
  seeds.reset();
  for (int64_t i = 0; i < 50; ++i) {
    ret = seeds.push_back(i);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  test_one_case(seeds, count_per_range, is_reverse_scan);

  //20 dup exist
  seeds.reset();
  for (int64_t i = 0; i < 20; ++i) {
    ret = seeds.push_back(0);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  test_one_case(seeds, count_per_range, is_reverse_scan);

  // 20 not exist
  seeds.reset();
  for (int64_t i = 0; i < 20; ++i) {
    ret = seeds.push_back(row_cnt_ + i);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  test_one_case(seeds, count_per_range, is_reverse_scan);

  // partial not exist
  seeds.reset();
  for (int64_t i = 0; i < 20; ++i) {
    ret = seeds.push_back(i % 2 ? i : i + row_cnt_);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  test_one_case(seeds, count_per_range, is_reverse_scan);
  destroy_query_param();
}

TEST_F(TestDDLMergeRowMultiScanner, test_multi_scan)
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

void TestDDLMergeRowMultiScanner::test_multi_scan_multi_get_with_scan(
    const bool is_reverse_scan,
    const int64_t count_per_range)
{
  int ret = OB_SUCCESS;
  ObDatumRange range;
  ObArray<ObDatumRange> ranges;
  ObDatumRow row;
  ObDatumRowkey rowkey;
  const ObDatumRow *prow = NULL;
  const ObDatumRow *kv_prow = NULL;
  int64_t row_cnt = 0;
  ObSSTableRowMultiScanner<> scanner;
  ObSSTableRowMultiScanner<> merge_ddl_scanner;

  // prepare query param
  prepare_query_param(is_reverse_scan);

  // multi scan interact with multi get
  ObDatumRange mget_ranges[TEST_MULTI_GET_CNT];
  ObDatumRow start_row;
  ObDatumRow end_row;
  ObDatumRow check_row;
  ASSERT_EQ(OB_SUCCESS, start_row.init(allocator_, TEST_COLUMN_CNT));
  ASSERT_EQ(OB_SUCCESS, end_row.init(allocator_, TEST_COLUMN_CNT));
  ASSERT_EQ(OB_SUCCESS, check_row.init(allocator_, TEST_COLUMN_CNT));
  ranges.reuse();
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    ObDatumRowkey tmp_rowkey;
    mget_ranges[i].border_flag_.set_inclusive_start();
    mget_ranges[i].border_flag_.set_inclusive_end();
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(i, start_row));
    tmp_rowkey.assign(start_row.storage_datums_, TEST_ROWKEY_COLUMN_CNT);
    ASSERT_EQ(OB_SUCCESS, tmp_rowkey.deep_copy(mget_ranges[i].start_key_, allocator_));
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(i + (i % 2 ? count_per_range - 1 : 0), end_row));
    tmp_rowkey.assign(end_row.storage_datums_, TEST_ROWKEY_COLUMN_CNT);
    ASSERT_EQ(OB_SUCCESS, tmp_rowkey.deep_copy(mget_ranges[i].end_key_, allocator_));
  }
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    ret = ranges.push_back(mget_ranges[i]);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  STORAGE_LOG(INFO, "multi scan begin");
  ASSERT_EQ(OB_SUCCESS, scanner.init(
          iter_param_,
          context_,
          &sstable_,
          &ranges));
  ASSERT_EQ(OB_SUCCESS, merge_ddl_scanner.init(
          iter_param_,
          context_,
          &partial_sstable_,
          &ranges));
  row_cnt = 0;
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    const int64_t p = i;
    if (p % 2) {
      for (int64_t j = 0; j < count_per_range; ++j) {
        const int64_t k = is_reverse_scan ? i + count_per_range - j - 1 : i + j;
        ASSERT_EQ(OB_SUCCESS, scanner.inner_get_next_row(prow));
        ASSERT_EQ(OB_SUCCESS, merge_ddl_scanner.inner_get_next_row(kv_prow));
        ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(k, check_row));
        ++row_cnt;
        ASSERT_TRUE(*prow == check_row);
        ASSERT_TRUE(*kv_prow == check_row);
      }
    } else {
      ASSERT_EQ(OB_SUCCESS, scanner.inner_get_next_row(prow));
      ASSERT_EQ(OB_SUCCESS, merge_ddl_scanner.inner_get_next_row(kv_prow));
      ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(p, check_row));
      ++row_cnt;
      ASSERT_TRUE(*prow == check_row);
      ASSERT_TRUE(*kv_prow == check_row);
    }
  }
  ASSERT_EQ(OB_ITER_END, scanner.inner_get_next_row(prow));
  ASSERT_EQ(OB_ITER_END, merge_ddl_scanner.inner_get_next_row(kv_prow));
  scanner.reuse();
  merge_ddl_scanner.reuse();

  // first half multi scan, second half multi get
  ranges.reuse();
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    ObDatumRowkey tmp_rowkey;
    mget_ranges[i].border_flag_.set_inclusive_start();
    mget_ranges[i].border_flag_.set_inclusive_end();
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(i, start_row));
    tmp_rowkey.assign(start_row.storage_datums_, TEST_ROWKEY_COLUMN_CNT);
    ASSERT_EQ(OB_SUCCESS, tmp_rowkey.deep_copy(mget_ranges[i].start_key_, allocator_));
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(i + (i < TEST_MULTI_GET_CNT / 2 ? count_per_range - 1 : 0), end_row));
    tmp_rowkey.assign(end_row.storage_datums_, TEST_ROWKEY_COLUMN_CNT);
    ASSERT_EQ(OB_SUCCESS, tmp_rowkey.deep_copy(mget_ranges[i].end_key_, allocator_));
    ASSERT_EQ(OB_SUCCESS, ranges.push_back(mget_ranges[i]));
  }
  ASSERT_EQ(OB_SUCCESS, scanner.init(
          iter_param_,
          context_,
          &sstable_,
          &ranges));
  ASSERT_EQ(OB_SUCCESS, merge_ddl_scanner.init(
          iter_param_,
          context_,
          &partial_sstable_,
          &ranges));
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    const int64_t p = i;
    if (p < TEST_MULTI_GET_CNT / 2) {
      for (int64_t j = 0; j < count_per_range; ++j) {
        const int64_t k = is_reverse_scan ? i + count_per_range - j - 1 : i + j;
        ret = scanner.inner_get_next_row(prow);
        ASSERT_EQ(OB_SUCCESS, ret);
        ret = merge_ddl_scanner.inner_get_next_row(kv_prow);
        ASSERT_EQ(OB_SUCCESS, ret);
        ret = row_generate_.get_next_row(k, check_row);
        ASSERT_EQ(OB_SUCCESS, ret);
        ASSERT_TRUE(*prow == check_row);
        ASSERT_TRUE(*kv_prow == check_row);
      }
    } else {
      ret = scanner.inner_get_next_row(prow);
      ASSERT_EQ(OB_SUCCESS, ret);
      ret = merge_ddl_scanner.inner_get_next_row(kv_prow);
      ASSERT_EQ(OB_SUCCESS, ret);
      ret = row_generate_.get_next_row(p, check_row);
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_TRUE(*prow == check_row);
      ASSERT_TRUE(*kv_prow == check_row);
    }
  }
  ASSERT_EQ(OB_ITER_END, scanner.inner_get_next_row(prow));
  ASSERT_EQ(OB_ITER_END, merge_ddl_scanner.inner_get_next_row(kv_prow));
  scanner.reuse();
  merge_ddl_scanner.reuse();

  // first one multi get, others multi scan
  ranges.reuse();
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    ObDatumRowkey tmp_rowkey;
    mget_ranges[i].border_flag_.set_inclusive_start();
    mget_ranges[i].border_flag_.set_inclusive_end();
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(i, start_row));
    tmp_rowkey.assign(start_row.storage_datums_, TEST_ROWKEY_COLUMN_CNT);
    ASSERT_EQ(OB_SUCCESS, tmp_rowkey.deep_copy(mget_ranges[i].start_key_, allocator_));
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(i + (i != 0 ? count_per_range - 1 : 0), end_row));
    tmp_rowkey.assign(end_row.storage_datums_, TEST_ROWKEY_COLUMN_CNT);
    ASSERT_EQ(OB_SUCCESS, tmp_rowkey.deep_copy(mget_ranges[i].end_key_, allocator_));
    ASSERT_EQ(OB_SUCCESS, ranges.push_back(mget_ranges[i]));
  }
  ASSERT_EQ(OB_SUCCESS, scanner.init(
          iter_param_,
          context_,
          &sstable_,
          &ranges));
  ASSERT_EQ(OB_SUCCESS, merge_ddl_scanner.init(
          iter_param_,
          context_,
          &partial_sstable_,
          &ranges));
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    const int64_t p = i;
    if (p != 0) {
      for (int64_t j = 0; j < count_per_range; ++j) {
        const int64_t k = is_reverse_scan ? i + count_per_range - j - 1 : i + j;
        ret = scanner.inner_get_next_row(prow);
        ASSERT_EQ(OB_SUCCESS, ret);
        ret = merge_ddl_scanner.inner_get_next_row(kv_prow);
        ASSERT_EQ(OB_SUCCESS, ret);
        ret = row_generate_.get_next_row(k, check_row);
        ASSERT_EQ(OB_SUCCESS, ret);
        ASSERT_TRUE(*prow == check_row);
        ASSERT_TRUE(*kv_prow == check_row);
      }
    } else {
      ret = scanner.inner_get_next_row(prow);
      ASSERT_EQ(OB_SUCCESS, ret);
      ret = merge_ddl_scanner.inner_get_next_row(kv_prow);
      ASSERT_EQ(OB_SUCCESS, ret);
      ret = row_generate_.get_next_row(p, check_row);
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_TRUE(*prow == check_row);
      ASSERT_TRUE(*kv_prow == check_row);
    }
  }
  ASSERT_EQ(OB_ITER_END, scanner.inner_get_next_row(prow));
  ASSERT_EQ(OB_ITER_END, merge_ddl_scanner.inner_get_next_row(kv_prow));
  scanner.reuse();
  merge_ddl_scanner.reuse();

  // multi scan not exist row
  STORAGE_LOG(DEBUG, "multi_scan_not_exist_row");
  ranges.reuse();
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    ObDatumRowkey tmp_rowkey;
    mget_ranges[i].border_flag_.set_inclusive_start();
    mget_ranges[i].border_flag_.set_inclusive_end();
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(i + (i % 2 ? row_cnt_ : 0), start_row));
    tmp_rowkey.assign(start_row.storage_datums_, TEST_ROWKEY_COLUMN_CNT);
    ASSERT_EQ(OB_SUCCESS, tmp_rowkey.deep_copy(mget_ranges[i].start_key_, allocator_));
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(i + (i % 2 ? row_cnt_ + count_per_range - 1 : 0), end_row));
    tmp_rowkey.assign(end_row.storage_datums_, TEST_ROWKEY_COLUMN_CNT);
    ASSERT_EQ(OB_SUCCESS, tmp_rowkey.deep_copy(mget_ranges[i].end_key_, allocator_));
    ASSERT_EQ(OB_SUCCESS, ranges.push_back(mget_ranges[i]));
  }
  ASSERT_EQ(OB_SUCCESS, scanner.init(
          iter_param_,
          context_,
          &sstable_,
          &ranges));
  ASSERT_EQ(OB_SUCCESS, merge_ddl_scanner.init(
          iter_param_,
          context_,
          &partial_sstable_,
          &ranges));
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    const int64_t p = i;
    if (p % 2) {
      continue;
    } else {
      ret = scanner.inner_get_next_row(prow);
      ASSERT_EQ(OB_SUCCESS, ret);
      ret = merge_ddl_scanner.inner_get_next_row(kv_prow);
      ASSERT_EQ(OB_SUCCESS, ret);
      ret = row_generate_.get_next_row(p, check_row);
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_TRUE(*prow == check_row);
      ASSERT_TRUE(*kv_prow == check_row);
    }
  }
  ASSERT_EQ(OB_ITER_END, scanner.inner_get_next_row(prow));
  ASSERT_EQ(OB_ITER_END, merge_ddl_scanner.inner_get_next_row(kv_prow));
  scanner.reset();
  destroy_query_param();
}

TEST_F(TestDDLMergeRowMultiScanner, test_multi_get_with_scan)
{
  bool is_reverse_scan = false;
  for (int64_t i = 2; i < 10; i += 10) {
    test_multi_scan_multi_get_with_scan(is_reverse_scan, i);
  }
  is_reverse_scan = true;
  for (int64_t i = 2; i < 10; i += 10) {
    test_multi_scan_multi_get_with_scan(is_reverse_scan, i);
  }
}

}
}

int main(int argc, char **argv)
{
  system("rm -f test_ddl_merge_row_multi_scanner.log*");
  OB_LOGGER.set_file_name("test_ddl_merge_row_multi_scanner.log", true, true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
