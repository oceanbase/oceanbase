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

#include "lib/random/ob_random.h"
#include "storage/access/ob_index_tree_prefetcher.h"
#include "storage/access/ob_sstable_row_getter.h"
#include "storage/access/ob_sstable_row_scanner.h"
#include "ob_index_block_data_prepare.h"

namespace oceanbase
{
using namespace storage;
using namespace common;
namespace blocksstable
{
class TestDDLMergeRowScanner : public TestIndexBlockDataPrepare
{
public:
  TestDDLMergeRowScanner();
  virtual ~TestDDLMergeRowScanner();
  static void SetUpTestCase();
  static void TearDownTestCase();

  virtual void SetUp();
  virtual void TearDown();

  void generate_range(const int64_t start, const int64_t end, ObDatumRange &range);
  void test_one_rowkey(const int64_t seed); //get
  void test_single_case(
      const ObDatumRange &range,
      const int64_t start,
      const int64_t end,
      const bool is_reverse_scan);
  void test_full_case(
      const ObDatumRange &range,
      const int64_t start,
      const int64_t end,
      const bool is_reverse_scan,
      const int64_t hit_mode);
  void test_border(const bool is_reverse_scan);
  void test_basic(const bool is_reverse_scan);

protected:
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

TestDDLMergeRowScanner::TestDDLMergeRowScanner()
  : TestIndexBlockDataPrepare("Test DDL multi row scanner")
{
  is_ddl_merge_data_ = true;
  max_row_cnt_ = 150000;
  max_partial_row_cnt_ = 78881;
  co_sstable_row_offset_ = max_partial_row_cnt_ - 1;
  partial_kv_start_idx_ = 3;
}

TestDDLMergeRowScanner::~TestDDLMergeRowScanner()
{

}

void TestDDLMergeRowScanner::SetUpTestCase()
{
  TestIndexBlockDataPrepare::SetUpTestCase();
}

void TestDDLMergeRowScanner::TearDownTestCase()
{
  TestIndexBlockDataPrepare::TearDownTestCase();
}

void TestDDLMergeRowScanner::SetUp()
{
  TestIndexBlockDataPrepare::SetUp();
  ASSERT_EQ(OB_SUCCESS, start_row_.init(allocator_, TEST_COLUMN_CNT));
  ASSERT_EQ(OB_SUCCESS, end_row_.init(allocator_, TEST_COLUMN_CNT));
  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));

  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle_));
}

void TestDDLMergeRowScanner::TearDown()
{
  tablet_handle_.get_obj()->ddl_kv_count_ = 0;
  tablet_handle_.get_obj()->ddl_kvs_ = nullptr;
  tablet_handle_.reset();
  TestIndexBlockDataPrepare::TearDown();
}

void TestDDLMergeRowScanner::generate_range(
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

void TestDDLMergeRowScanner::test_one_rowkey(const int64_t seed)
{
  ObSSTableRowGetter getter;
  ObSSTableRowGetter merge_ddl_getter;
  ObDatumRow query_row;
  ASSERT_EQ(OB_SUCCESS, query_row.init(allocator_, TEST_COLUMN_CNT));
  row_generate_.get_next_row(seed, query_row);
  ObDatumRowkey query_rowkey;
  query_rowkey.assign(query_row.storage_datums_, TEST_ROWKEY_COLUMN_CNT);
  STORAGE_LOG(INFO, "Query rowkey", K(query_row));
  ASSERT_EQ(OB_SUCCESS, getter.init(iter_param_, context_, &sstable_, &query_rowkey));
  ASSERT_EQ(OB_SUCCESS, merge_ddl_getter.init(iter_param_, context_, &partial_sstable_, &query_rowkey));

  const ObDatumRow *prow = nullptr;
  const ObDatumRow *kv_prow = nullptr;
  ASSERT_EQ(OB_SUCCESS, getter.inner_get_next_row(prow));
  ASSERT_EQ(OB_SUCCESS, merge_ddl_getter.inner_get_next_row(kv_prow));
  STORAGE_LOG(INFO, "debug datum row1", KPC(prow), KPC(kv_prow));
  if (seed >= row_cnt_) {
    ASSERT_TRUE(prow->row_flag_.is_not_exist());
    ASSERT_TRUE(kv_prow->row_flag_.is_not_exist());
  } else {
    ASSERT_TRUE(*prow == query_row);
    if (!(*kv_prow == query_row)) {
      STORAGE_LOG(INFO, "CHECK UNEUQAL", K(query_row), K(*kv_prow));
    }
    ASSERT_TRUE(*kv_prow == query_row);
  }
  ASSERT_EQ(OB_ITER_END, getter.inner_get_next_row(prow));
  ASSERT_EQ(OB_ITER_END, merge_ddl_getter.inner_get_next_row(kv_prow));
  getter.reuse();
  merge_ddl_getter.reuse();
}

void TestDDLMergeRowScanner::test_single_case(
    const ObDatumRange &range,
    const int64_t start,
    const int64_t end,
    const bool is_reverse_scan)
{
  int ret = OB_SUCCESS;
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, TEST_COLUMN_CNT));
  const ObDatumRow *prow = nullptr;
  const ObDatumRow *kv_prow = nullptr;
  ObSSTableRowScanner<> scanner;
  ObSSTableRowScanner<> merge_ddl_scanner;

  ASSERT_EQ(OB_SUCCESS, scanner.init(
          iter_param_,
          context_,
          &sstable_,
          &range));
  ASSERT_EQ(OB_SUCCESS, merge_ddl_scanner.init(
          iter_param_,
          context_,
          &partial_sstable_,
          &range));
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
      ret = scanner.inner_get_next_row(prow);
      ASSERT_EQ(OB_SUCCESS, ret) << i << "index: " << index << " start: " << start
          << " end: " << end << " prow: " << prow;
      ASSERT_TRUE(row == *prow) << i << "index: " << index << " start: " << start
          << " end: " << end << " prow: " << prow;

      ret = merge_ddl_scanner.inner_get_next_row(kv_prow);
      ASSERT_EQ(OB_SUCCESS, ret) << i << "index: " << index << " start: " << start
          << " end: " << end << " kv_prow: " << kv_prow;
      ASSERT_TRUE(row == *kv_prow) << i << "index: " << index << " start: " << start
          << " end: " << end << " kv_prow: " << kv_prow;
    }
  }
  ASSERT_EQ(OB_ITER_END, scanner.inner_get_next_row(prow));
  ASSERT_EQ(OB_ITER_END, merge_ddl_scanner.inner_get_next_row(kv_prow));
  scanner.reuse();
  merge_ddl_scanner.reuse();
}

void TestDDLMergeRowScanner::test_full_case(
    const ObDatumRange &range,
    const int64_t start,
    const int64_t end,
    const bool is_reverse_scan,
    const int64_t hit_mode)
{
  int ret = OB_SUCCESS;
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, TEST_COLUMN_CNT));
  const ObDatumRow *prow = nullptr;
  const ObDatumRow *kv_prow = nullptr;
  ObSSTableRowScanner<> scanner;
  ObSSTableRowScanner<> merge_ddl_scanner;

  if (HIT_PART == hit_mode) {
    const int64_t part_start = start + (end - start) / 3;
    const int64_t part_end = end - (end - start) / 3;
    ObDatumRange part_range;
    ObDatumRow start_row;
    ObDatumRow end_row;
    ObDatumRowkey tmp_rowkey;
    ASSERT_EQ(OB_SUCCESS, start_row.init(allocator_, TEST_COLUMN_CNT));
    ASSERT_EQ(OB_SUCCESS, end_row.init(allocator_, TEST_COLUMN_CNT));
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(part_start, start_row));
    tmp_rowkey.assign(start_row.storage_datums_, TEST_ROWKEY_COLUMN_CNT);
    ASSERT_EQ(OB_SUCCESS, tmp_rowkey.deep_copy(part_range.start_key_, allocator_));
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(part_end, end_row));
    tmp_rowkey.assign(end_row.storage_datums_, TEST_ROWKEY_COLUMN_CNT);
    ASSERT_EQ(OB_SUCCESS, tmp_rowkey.deep_copy(part_range.end_key_, allocator_));
    part_range.border_flag_.set_inclusive_start();
    part_range.border_flag_.set_inclusive_end();
    ASSERT_EQ(OB_SUCCESS, scanner.init(
            iter_param_,
            context_,
            &sstable_,
            &part_range));
    ASSERT_EQ(OB_SUCCESS, merge_ddl_scanner.init(
            iter_param_,
            context_,
            &partial_sstable_,
            &part_range));
    for (int64_t i = part_start; i <= part_end; ++i) {
      if (i < row_cnt_) {
        ret = scanner.inner_get_next_row(prow);
        ASSERT_EQ(OB_SUCCESS, ret) << "i: " << i << " part_start: " << part_start
            << " part_end: " << part_end << " prow: " << prow;
        ret = merge_ddl_scanner.inner_get_next_row(kv_prow);
        ASSERT_EQ(OB_SUCCESS, ret) << "i: " << i << " part_start: " << part_start
            << " part_end: " << part_end << " kv_prow: " << kv_prow;
      }
    }
    ASSERT_EQ(OB_ITER_END, scanner.inner_get_next_row(prow));
    ASSERT_EQ(OB_ITER_END, merge_ddl_scanner.inner_get_next_row(kv_prow));
    scanner.reuse();
    merge_ddl_scanner.reuse();
  }

  ASSERT_EQ(OB_SUCCESS, scanner.init(
          iter_param_,
          context_,
          &sstable_,
          &range));
  ASSERT_EQ(OB_SUCCESS, merge_ddl_scanner.init(
          iter_param_,
          context_,
          &partial_sstable_,
          &range));
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
      ret = scanner.inner_get_next_row(prow);
      ASSERT_EQ(OB_SUCCESS, ret) << i << "index: " << index << " start: " << start
          << " end: " << end << " prow: " << prow;
      ASSERT_TRUE(row == *prow) << i << "index: " << index << " start: " << start
          << " end: " << end << " prow: " << prow;
      ret = merge_ddl_scanner.inner_get_next_row(kv_prow);
      ASSERT_EQ(OB_SUCCESS, ret) << i << "index: " << index << " start: " << start
          << " end: " << end << " kv_prow: " << kv_prow;
      ASSERT_TRUE(row == *kv_prow) << i << "index: " << index << " start: " << start
          << " end: " << end << " kv_prow: " << kv_prow;
    }
  }
  ASSERT_EQ(OB_ITER_END, scanner.inner_get_next_row(prow));
  ASSERT_EQ(OB_ITER_END, scanner.inner_get_next_row(prow));
  ASSERT_EQ(OB_ITER_END, merge_ddl_scanner.inner_get_next_row(kv_prow));
  ASSERT_EQ(OB_ITER_END, merge_ddl_scanner.inner_get_next_row(kv_prow));
  scanner.reuse();
  merge_ddl_scanner.reuse();

  if (HIT_ALL == hit_mode) {
    int64_t index = 0;
    ASSERT_EQ(OB_SUCCESS, scanner.init(
            iter_param_,
            context_,
            &sstable_,
            &range));
    ASSERT_EQ(OB_SUCCESS, merge_ddl_scanner.init(
            iter_param_,
            context_,
            &partial_sstable_,
            &range));
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
        ret = scanner.inner_get_next_row(prow);
        ASSERT_EQ(OB_SUCCESS, ret) << i << "index: " << index << " start: " << start
            << " end: " << end << " prow: " << prow;
        ASSERT_TRUE(row == *prow) << i << "index: " << index << " start: " << start
            << " end: " << end << " prow: " << prow;
        ret = merge_ddl_scanner.inner_get_next_row(kv_prow);
        ASSERT_EQ(OB_SUCCESS, ret) << i << "index: " << index << " start: " << start
            << " end: " << end << " kv_prow: " << kv_prow;
        ASSERT_TRUE(row == *kv_prow) << i << "index: " << index << " start: " << start
            << " end: " << end << " kv_prow: " << kv_prow;
      }
    }
    ASSERT_EQ(OB_ITER_END, scanner.inner_get_next_row(prow));
    ASSERT_EQ(OB_ITER_END, scanner.inner_get_next_row(prow));
    ASSERT_EQ(OB_ITER_END, merge_ddl_scanner.inner_get_next_row(kv_prow));
    ASSERT_EQ(OB_ITER_END, merge_ddl_scanner.inner_get_next_row(kv_prow));
    scanner.reuse();
    merge_ddl_scanner.reuse();
  }
}

void TestDDLMergeRowScanner::test_border(const bool is_reverse_scan)
{
  int ret = OB_SUCCESS;
  ObDatumRange range;

  // prepare query param
  prepare_query_param(is_reverse_scan);

  // full table scan
  range.set_whole_range();
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_full_case(range, 0, row_cnt_ - 1, is_reverse_scan, i);
  }

  // the first row of sstable
  generate_range(0, 0, range);
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_full_case(range, 0, 0, is_reverse_scan, i);
  }

  // the first 100 row of sstable
  generate_range(0, 100, range);
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_full_case(range, 0, 100, is_reverse_scan, i);
  }

  // the last row of sstable
  generate_range(row_cnt_ - 1, row_cnt_ - 1, range);
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_full_case(range, row_cnt_ - 1, row_cnt_ - 1, is_reverse_scan, i);
  }

  // the last 100 row of sstable
  generate_range(row_cnt_ - 100, row_cnt_ - 1, range);
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_full_case(range, row_cnt_ - 100, row_cnt_ - 1, is_reverse_scan, i);
  }

  // not exist
  generate_range(row_cnt_, row_cnt_, range);
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_full_case(range, row_cnt_, row_cnt_, is_reverse_scan, i);
  }

  destroy_query_param();
}

void TestDDLMergeRowScanner::test_basic(const bool is_reverse_scan)
{
  int ret = OB_SUCCESS;
  ObDatumRange range;

  prepare_query_param(is_reverse_scan);
  // full table scan
  range.set_whole_range();
  test_single_case(range, 0, row_cnt_ - 1, is_reverse_scan);

  // the first row of sstable
  generate_range(0, 0, range);
  test_single_case(range, 0, 0, is_reverse_scan);

  // the first 100 row of sstable
  generate_range(0, 100, range);
  test_single_case(range, 0, 100, is_reverse_scan);

  // the last row of sstable
  generate_range(row_cnt_ - 1, row_cnt_ - 1, range);
  test_single_case(range, row_cnt_ - 1, row_cnt_ - 1, is_reverse_scan);

  // the last 100 row of sstable
  generate_range(row_cnt_ - 100, row_cnt_ - 1, range);
  test_single_case(range, row_cnt_ - 100, row_cnt_ - 1, is_reverse_scan);

  // not exist
  generate_range(row_cnt_, row_cnt_, range);
  test_single_case(range, row_cnt_, row_cnt_, is_reverse_scan);
  destroy_query_param();
}

TEST_F(TestDDLMergeRowScanner, test_get)
{
  // forward get
  prepare_query_param(false);
  // left border rowkey
  test_one_rowkey(0);
  // end_key of left border
  test_one_rowkey(15933);
  // first_key of right border
  test_one_rowkey(132361);
  // end_key of right border
  test_one_rowkey(row_cnt_ - 1);
  // mid border rowkey
  test_one_rowkey(row_cnt_ / 2);
  // non-exist rowkey
  test_one_rowkey(row_cnt_);
  destroy_query_param();

  // reverse get
  prepare_query_param(true);
  // left border rowkey
  test_one_rowkey(0);
  // end_key of left border
  test_one_rowkey(15933);
  // first_key of right border
  test_one_rowkey(132361);
  // end_key of right border
  test_one_rowkey(row_cnt_ - 1);
  // mid border rowkey
  test_one_rowkey(row_cnt_ / 2);
  // non-exist rowkey
  test_one_rowkey(row_cnt_);
  destroy_query_param();
}

TEST_F(TestDDLMergeRowScanner, test_basic_scan)
{
  bool is_reverse_scan = false;
  test_basic(is_reverse_scan);
  is_reverse_scan = true;
  test_basic(is_reverse_scan);
  STORAGE_LOG(INFO, "memory usage", K(lib::get_memory_hold()), K(lib::get_memory_limit()));
  ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(500);
  ObMallocAllocator::get_instance()->print_tenant_memory_usage(500);
  ObMallocAllocator::get_instance()->print_tenant_memory_usage(1);
}

TEST_F(TestDDLMergeRowScanner, test_border_scan)
{
  bool is_reverse_scan = false;
  test_border(is_reverse_scan);
  is_reverse_scan = true;
  test_border(is_reverse_scan);
  STORAGE_LOG(INFO, "memory usage", K(lib::get_memory_hold()), K(lib::get_memory_limit()));
  ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(500);
  ObMallocAllocator::get_instance()->print_tenant_memory_usage(500);
  ObMallocAllocator::get_instance()->print_tenant_memory_usage(1);
}

TEST_F(TestDDLMergeRowScanner, test_random_scan)
{
  ObDatumRange range;
  int64_t start = ObRandom::rand(0, row_cnt_ - 1);
  int64_t end = ObRandom::rand(0, row_cnt_ - 1);
  if (start > end) {
    int64_t temp = start;
    start = end;
    end = temp;
  }
  generate_range(start, end, range);

  bool is_reverse_scan = false;
  // prepare query param
  prepare_query_param(is_reverse_scan);
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_full_case(range, start, end, is_reverse_scan, i);
  }
  destroy_query_param();

  is_reverse_scan = true;
  // prepare query param
  prepare_query_param(is_reverse_scan);
  for (int64_t i = HIT_ALL; i < HIT_MAX; ++i) {
    test_full_case(range, start, end, is_reverse_scan, i);
  }
  destroy_query_param();
}

}
}

int main(int argc, char **argv)
{
  system("rm -f test_ddl_merge_row_scanner.log*");
  OB_LOGGER.set_file_name("test_ddl_merge_row_scanner.log", true, true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}