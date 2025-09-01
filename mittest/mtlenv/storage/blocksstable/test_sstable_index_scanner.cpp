/**
 * Copyright (c) 2025 OceanBase
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

#include "mtlenv/storage/blocksstable/ob_index_block_data_prepare.h"
#include "storage/blocksstable/index_block/ob_sstable_index_scanner.h"

namespace oceanbase
{
using namespace storage;
using namespace common;
namespace blocksstable
{

class TestSSTableIndexScanner : public TestIndexBlockDataPrepare
{
public:
  TestSSTableIndexScanner()
    : TestIndexBlockDataPrepare("Test sstable index scanner", ObMergeType::MAJOR_MERGE, true)
  {}
  virtual ~TestSSTableIndexScanner() {}

  virtual void SetUp();
  // virtual void TearDown();

  void prepare_scan_param(
      const bool scan_root_level,
      ObSSTableIndexScanParam &scan_param,
      ObFixedArray<ObSkipIndexColMeta, ObIAllocator> &skip_index_projector);
  void generate_range(const int64_t start_seed, const int64_t end_seed, ObDatumRange &range);
  void generate_key(const int64_t seed, ObDatumRowkey &key);
  void test_range_scan(const ObDatumRange &range, ObSSTableIndexScanner &index_scanner);
private:
  ObArenaAllocator datum_alloc_;
  ObDatumRow start_key_buf_;
  ObDatumRow end_key_buf_;
  ObDatumRow rowkey_buf_;
};

void TestSSTableIndexScanner::SetUp()
{
  TestIndexBlockDataPrepare::SetUp();
  ASSERT_EQ(OB_SUCCESS, start_key_buf_.init(datum_alloc_, TEST_COLUMN_CNT));
  ASSERT_EQ(OB_SUCCESS, end_key_buf_.init(datum_alloc_, TEST_COLUMN_CNT));
  ASSERT_EQ(OB_SUCCESS, rowkey_buf_.init(datum_alloc_, TEST_COLUMN_CNT));
}

void TestSSTableIndexScanner::generate_range(
    const int64_t start_seed,
    const int64_t end_seed,
    ObDatumRange &range)
{
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(start_seed, start_key_buf_));
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(end_seed, end_key_buf_));
  range.start_key_.assign(start_key_buf_.storage_datums_, TEST_ROWKEY_COLUMN_CNT);
  range.end_key_.assign(end_key_buf_.storage_datums_, TEST_ROWKEY_COLUMN_CNT);
  range.border_flag_.set_inclusive_start();
  range.border_flag_.set_inclusive_end();
}

void TestSSTableIndexScanner::generate_key(const int64_t seed, ObDatumRowkey &key)
{
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed, rowkey_buf_));
  key.assign(rowkey_buf_.storage_datums_, TEST_ROWKEY_COLUMN_CNT);
}

void TestSSTableIndexScanner::prepare_scan_param(
    const bool scan_root_level,
    ObSSTableIndexScanParam &scan_param,
    ObFixedArray<ObSkipIndexColMeta, ObIAllocator> &skip_index_projector)
{
  prepare_query_param(false);
  scan_param.query_flag_.set_use_block_cache();
  scan_param.index_read_info_ = &read_info_;
  skip_index_projector.init(0);
  int64_t full_column_count = 0;
  ASSERT_EQ(OB_SUCCESS, table_schema_.get_store_column_count(full_column_count, true));
  const int64_t proj_cnt = 2 * full_column_count;
  skip_index_projector.set_allocator(&allocator_);
  ASSERT_EQ(OB_SUCCESS, skip_index_projector.init(proj_cnt));
  for (int64_t i = 0; i < full_column_count; ++i) {
    ObSkipIndexColMeta col_meta;
    col_meta.col_idx_ = i;
    col_meta.col_type_ = ObSkipIndexColType::SK_IDX_MIN;
    ASSERT_EQ(OB_SUCCESS, skip_index_projector.push_back(col_meta));
    col_meta.col_type_ = ObSkipIndexColType::SK_IDX_MAX;
    ASSERT_EQ(OB_SUCCESS, skip_index_projector.push_back(col_meta));
  }
  scan_param.skip_index_projector_ = &skip_index_projector;
  if (scan_root_level) {
    scan_param.scan_level_ = ObSSTableIndexScanParam::ScanLevel::ROOT;
  } else {
    scan_param.scan_level_ = ObSSTableIndexScanParam::ScanLevel::LEAF;
  }
}

void TestSSTableIndexScanner::test_range_scan(const ObDatumRange &range, ObSSTableIndexScanner &index_scanner)
{
  bool is_last_iter_endkey_beyond_range = false;
  const ObSSTableIndexRow *index_row = nullptr;
  const ObStorageDatumUtils &datum_utils = read_info_.get_datum_utils();
  int tmp_ret = OB_SUCCESS;
  while (OB_SUCCESS == tmp_ret) {
    tmp_ret = index_scanner.get_next(index_row);
    if (OB_SUCCESS == tmp_ret) {
      int start_cmp_ret = 0;
      int end_cmp_ret = 0;
      ASSERT_EQ(OB_SUCCESS, index_row->endkey_->compare(range.start_key_, datum_utils, start_cmp_ret));
      ASSERT_EQ(OB_SUCCESS, index_row->endkey_->compare(range.end_key_, datum_utils, end_cmp_ret));
      ASSERT_TRUE(start_cmp_ret > 0);
      if (end_cmp_ret > 0) {
        ASSERT_FALSE(is_last_iter_endkey_beyond_range);
        is_last_iter_endkey_beyond_range = true;
      }
    } else {
      ASSERT_EQ(OB_ITER_END, tmp_ret);
    }
  }
}

TEST_F(TestSSTableIndexScanner, test_basic_scan_functionality)
{
  ObArenaAllocator tmp_arena;
  ObDatumRange range;
  range.set_whole_range();

  ObSSTableIndexScanParam scan_param;
  ObFixedArray<ObSkipIndexColMeta, ObIAllocator> proj;
  prepare_scan_param(false, scan_param, proj);


  ObSSTableIndexScanner index_scanner;
  ASSERT_EQ(OB_SUCCESS, index_scanner.init(range, scan_param, sstable_, tmp_arena));
  int tmp_ret = OB_SUCCESS;
  int iter_cnt = 0;
  while (OB_SUCCESS == tmp_ret) {
    const ObSSTableIndexRow *index_row = nullptr;
    tmp_ret = index_scanner.get_next(index_row);
    if (tmp_ret != OB_SUCCESS) {
      ASSERT_EQ(OB_ITER_END, tmp_ret);
    } else {
      ASSERT_TRUE(nullptr != index_row);
      ASSERT_EQ(index_row->skip_index_row_.get_column_count(), scan_param.skip_index_projector_->count());
      iter_cnt++;
      LOG_INFO("print index row", KPC(index_row->endkey_), K(index_row->skip_index_row_));
    }
  }
  ASSERT_TRUE(iter_cnt > 0);

  index_scanner.reset();
  scan_param.scan_level_ = ObSSTableIndexScanParam::ScanLevel::ROOT;
  ASSERT_EQ(OB_SUCCESS, index_scanner.init(range, scan_param, sstable_, tmp_arena));
  tmp_ret = OB_SUCCESS;
  iter_cnt = 0;
  while (OB_SUCCESS == tmp_ret) {
    const ObSSTableIndexRow *index_row = nullptr;
    tmp_ret = index_scanner.get_next(index_row);
    if (tmp_ret != OB_SUCCESS) {
      ASSERT_EQ(OB_ITER_END, tmp_ret);
    } else {
      ASSERT_TRUE(nullptr != index_row);
      iter_cnt++;
      LOG_INFO("print index row", KPC(index_row->endkey_), K(index_row->skip_index_row_));
    }
  }
  ASSERT_TRUE(iter_cnt > 0);
}

TEST_F(TestSSTableIndexScanner, test_range_with_advance)
{
  ObArenaAllocator tmp_arena;
  ObDatumRange range;

  ObSSTableIndexScanParam scan_param;
  ObFixedArray<ObSkipIndexColMeta, ObIAllocator> proj;
  prepare_scan_param(false, scan_param, proj);

  ObSSTableIndexScanner index_scanner;
  int tmp_ret = OB_SUCCESS;

  // test range scan
  generate_range(min_row_seed_, max_row_seed_, range);
  ASSERT_EQ(OB_SUCCESS, index_scanner.init(range, scan_param, sstable_, tmp_arena));
  test_range_scan(range, index_scanner);

  index_scanner.reset();
  scan_param.scan_level_ = ObSSTableIndexScanParam::ScanLevel::ROOT;
  ASSERT_EQ(OB_SUCCESS, index_scanner.init(range, scan_param, sstable_, tmp_arena));
  test_range_scan(range, index_scanner);

  generate_range(max_row_seed_ / 2, max_row_seed_, range);
  index_scanner.reset();
  scan_param.scan_level_ = ObSSTableIndexScanParam::ScanLevel::LEAF;
  ASSERT_EQ(OB_SUCCESS, index_scanner.init(range, scan_param, sstable_, tmp_arena));
  test_range_scan(range, index_scanner);

  index_scanner.reset();
  scan_param.scan_level_ = ObSSTableIndexScanParam::ScanLevel::ROOT;
  ASSERT_EQ(OB_SUCCESS, index_scanner.init(range, scan_param, sstable_, tmp_arena));
  test_range_scan(range, index_scanner);

  generate_range(min_row_seed_, max_row_seed_ / 2, range);
  index_scanner.reset();
  scan_param.scan_level_ = ObSSTableIndexScanParam::ScanLevel::LEAF;
  ASSERT_EQ(OB_SUCCESS, index_scanner.init(range, scan_param, sstable_, tmp_arena));
  test_range_scan(range, index_scanner);

  index_scanner.reset();
  scan_param.scan_level_ = ObSSTableIndexScanParam::ScanLevel::ROOT;
  ASSERT_EQ(OB_SUCCESS, index_scanner.init(range, scan_param, sstable_, tmp_arena));
  test_range_scan(range, index_scanner);

  generate_range(max_row_seed_ / 3, (max_row_seed_ * 2)/ 3, range);
  index_scanner.reset();
  scan_param.scan_level_ = ObSSTableIndexScanParam::ScanLevel::LEAF;
  ASSERT_EQ(OB_SUCCESS, index_scanner.init(range, scan_param, sstable_, tmp_arena));
  test_range_scan(range, index_scanner);

  index_scanner.reset();
  scan_param.scan_level_ = ObSSTableIndexScanParam::ScanLevel::ROOT;
  ASSERT_EQ(OB_SUCCESS, index_scanner.init(range, scan_param, sstable_, tmp_arena));
  test_range_scan(range, index_scanner);

  // test advance to interface
  // normal advance
  LOG_INFO("start test advance to interface");

  ObDatumRowkey advance_key;
  const ObSSTableIndexRow *index_row = nullptr;
  int cmp_ret = 0;
  generate_range(min_row_seed_, max_row_seed_, range);
  index_scanner.reset();
  generate_key(max_row_seed_ / 2, advance_key);
  scan_param.scan_level_ = ObSSTableIndexScanParam::ScanLevel::LEAF;
  ASSERT_EQ(OB_SUCCESS, index_scanner.init(range, scan_param, sstable_, tmp_arena));
  ASSERT_EQ(OB_SUCCESS, index_scanner.get_next(index_row));
  ASSERT_EQ(OB_SUCCESS, index_scanner.advance_to(advance_key, true));
  ASSERT_EQ(OB_SUCCESS, index_scanner.get_next(index_row));
  ASSERT_EQ(OB_SUCCESS, index_row->endkey_->compare(advance_key, read_info_.get_datum_utils(), cmp_ret));
  LOG_INFO("print advance key", K(advance_key), KPC(index_row->endkey_));
  ASSERT_TRUE(cmp_ret >= 0);

  generate_key(max_row_seed_, advance_key);
  ASSERT_EQ(OB_SUCCESS, index_scanner.advance_to(advance_key, true));
  ASSERT_EQ(OB_SUCCESS, index_scanner.get_next(index_row));
  cmp_ret = 0;
  ASSERT_EQ(OB_SUCCESS, index_row->endkey_->compare(advance_key, read_info_.get_datum_utils(), cmp_ret));
  ASSERT_TRUE(cmp_ret >= 0);

  generate_key(max_row_seed_ + 1, advance_key);
  ASSERT_EQ(OB_SUCCESS, index_scanner.advance_to(advance_key, true));
  ASSERT_EQ(OB_ITER_END, index_scanner.get_next(index_row));

  index_scanner.reset();
  scan_param.scan_level_ = ObSSTableIndexScanParam::ScanLevel::LEAF;
  ASSERT_EQ(OB_SUCCESS, index_scanner.init(range, scan_param, sstable_, tmp_arena));
  generate_key(min_row_seed_, advance_key);
  ASSERT_EQ(OB_SUCCESS, index_scanner.advance_to(advance_key, true));
  ASSERT_EQ(OB_SUCCESS, index_scanner.get_next(index_row));
  generate_key(max_row_seed_, advance_key);
  ASSERT_EQ(OB_SUCCESS, index_scanner.advance_to(advance_key, true));
  ASSERT_EQ(OB_SUCCESS, index_scanner.get_next(index_row));
  ASSERT_EQ(OB_ITER_END, index_scanner.get_next(index_row));

  index_scanner.reset();
  generate_range(max_row_seed_ / 2, max_row_seed_, range);
  scan_param.scan_level_ = ObSSTableIndexScanParam::ScanLevel::LEAF;
  ASSERT_EQ(OB_SUCCESS, index_scanner.init(range, scan_param, sstable_, tmp_arena));
  generate_key(min_row_seed_, advance_key);
  ASSERT_EQ(OB_SUCCESS, index_scanner.get_next(index_row));
  ObDatumRowkey iter_endkey;
  cmp_ret = 0;
  ASSERT_EQ(OB_SUCCESS, index_row->endkey_->deep_copy(iter_endkey, tmp_arena));
#ifndef OB_BUILD_PACKAGE
  ASSERT_EQ(OB_INVALID_ARGUMENT, index_scanner.advance_to(advance_key, true));
#endif
}


} // namespace blocksstable
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_sstable_index_scanner.log*");
  OB_LOGGER.set_file_name("test_sstable_index_scanner.log", true, true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
