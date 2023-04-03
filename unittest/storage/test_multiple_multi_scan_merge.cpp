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
#include "storage/mock_ob_ss_store.h"
#define private public
#define protected public
#include "storage/access/ob_multiple_multi_scan_merge.h"
#include "lib/allocator/ob_concurrent_fifo_allocator.h"
#include "lib/allocator/page_arena.h"
#include "storage/access/ob_table_scan_iterator.h"
#include "storage/ob_ss_store.h"
#include "blocksstable/ob_row_generate.h"
#include "share/schema/ob_table_schema.h"

namespace oceanbase
{
using namespace blocksstable;
using namespace common;
using namespace storage;
using namespace share::schema;
using namespace ::testing;

namespace unittest
{
static const int64_t TEST_ROWKEY_COLUMN_CNT = 8;
static const int64_t TEST_COLUMN_CNT = ObExtendType - 1;
class ObMockStoreRowIteratorV2 : public storage::ObStoreRowIterator
{
public:
  ObMockStoreRowIteratorV2(ObTableSchema &schema, ObRowGenerate &row_generate);
  void add_get_range(const int64_t seed, const bool is_exist, const bool has_nop);
  void add_scan_range(const int64_t start_seed, const int64_t end_seed, const bool is_exist, const bool has_nop);
  virtual int get_next_row(const ObStoreRow *&row);
  void reset();
  bool is_random_nop(void);
private:
  ObSEArray<int64_t, 512> range_start_;
  ObSEArray<int64_t, 512> range_end_;
  ObSEArray<bool, 512> is_exist_;
  ObSEArray<bool, 512> has_nop_;
  int64_t cur_range_index_;
  int64_t cur_row_index_;
  ObStoreRow cur_row_;
  ObTableSchema &table_schema_;
  ObRowGenerate &row_generate_;
  ObArenaAllocator allocator_;
  bool is_first_scan_;
  ObObj cells_[TEST_COLUMN_CNT];
};

ObMockStoreRowIteratorV2::ObMockStoreRowIteratorV2(ObTableSchema &schema, ObRowGenerate &row_generate) : range_start_(), range_end_(), cur_range_index_(0), cur_row_index_(0),
    cur_row_(), table_schema_(schema), row_generate_(row_generate), allocator_(ObModIds::TEST),
    is_first_scan_(true)
{
  cur_row_.row_val_.assign(cells_, TEST_COLUMN_CNT);
}

void ObMockStoreRowIteratorV2::reset()
{
  range_start_.reset();
  range_end_.reset();
  is_exist_.reset();
  cur_range_index_ = 0;
  cur_row_index_ = 0;
  cur_row_.reset();
  cur_row_.row_val_.assign(cells_, TEST_COLUMN_CNT);
  is_first_scan_ = true;
}

bool ObMockStoreRowIteratorV2::is_random_nop()
{
  return (rand() % 100) < 50;
}

void ObMockStoreRowIteratorV2::add_get_range(const int64_t seed, const bool is_exist, const bool has_nop)
{
  ASSERT_EQ(OB_SUCCESS, range_start_.push_back(seed));
  ASSERT_EQ(OB_SUCCESS, range_end_.push_back(seed));
  ASSERT_EQ(OB_SUCCESS, is_exist_.push_back(is_exist));
  ASSERT_EQ(OB_SUCCESS, has_nop_.push_back(has_nop));
}

void ObMockStoreRowIteratorV2::add_scan_range(const int64_t start_seed, const int64_t end_seed, const bool is_exist, const bool has_nop)
{
  ASSERT_EQ(OB_SUCCESS, range_start_.push_back(start_seed));
  ASSERT_EQ(OB_SUCCESS, range_end_.push_back(end_seed));
  ASSERT_EQ(OB_SUCCESS, is_exist_.push_back(is_exist));
  ASSERT_EQ(OB_SUCCESS, has_nop_.push_back(has_nop));
}

int ObMockStoreRowIteratorV2::get_next_row(const ObStoreRow *&row)
{
  int ret = OB_SUCCESS;
  if (cur_range_index_ >= range_start_.count()) {
    ret = OB_ITER_END;
  } else {
   if (is_first_scan_) {
     cur_row_index_ = range_start_.at(cur_range_index_);
     is_first_scan_ = false;
   }
   if (OB_FAIL(row_generate_.get_next_row(cur_row_index_, cur_row_))) {
     STORAGE_LOG(WARN, "fail to get_next_row", K(ret));
   } else {
     cur_row_.scan_index_ = cur_range_index_;
     cur_row_.is_get_ = range_start_.at(cur_range_index_) == range_end_.at(cur_range_index_);
     if (!is_exist_.at(cur_range_index_)) {
       cur_row_.flag_.set_flag(ObDmlFlag::DF_DELETE);
     }
     if (has_nop_.at(cur_range_index_)) {
       for (int64_t i = TEST_ROWKEY_COLUMN_CNT; i < TEST_COLUMN_CNT; ++i) {
         if (is_random_nop()) {
           cells_[i].set_nop_value();
         }
       }
     }
     row = &cur_row_;
     ++cur_row_index_;
     if (cur_row_index_ > range_end_.at(cur_range_index_)) {
       ++cur_range_index_;
       if (cur_range_index_ < range_start_.count()) {
         cur_row_index_ = range_start_.at(cur_range_index_);
       }
     }
   }
  }
  return ret;
}

class ObMultipleMultiScanMergeTest : public ::testing::Test
{
public:
  static const int64_t TEST_ROWKEY_COLUMN_CNT = 1;
  static const int64_t TEST_COLUMN_CNT = ObExtendType - 1;

public:
  ObMultipleMultiScanMergeTest();
  void prepare_schema(const int64_t rowkey_cnt, const int64_t column_cnt);
  void prepare_store(const ObIArray<ObMockStoreRowIteratorV2 *> &iters);
  virtual void SetUp();
  virtual void TearDown();
public:
  ObArenaAllocator allocator_;
  ObTableSchema table_schema_;
  ObRowGenerate row_generate_;
  common::ObSEArray<ObIStore *, common::MAX_TABLE_CNT_IN_STORAGE> stores_;
  ObTableAccessParam main_table_param_;
  ObTableAccessContext main_table_ctx_;
  ObStoreCtx store_ctx_;
  ColIdxArray out_idxs_;
  ObArray<ObStoreRange> ranges_;
};

ObMultipleMultiScanMergeTest::ObMultipleMultiScanMergeTest()
  : allocator_(ObModIds::TEST), table_schema_(), row_generate_()
{
}

void ObMultipleMultiScanMergeTest::SetUp()
{
  int ret = OB_SUCCESS;
  prepare_schema(TEST_ROWKEY_COLUMN_CNT, TEST_COLUMN_CNT);
  ObArray<ObColDesc> columns;

  OB_STORE_CACHE.init(1, 1, 1, 1, 1, 10);

  main_table_ctx_.query_flag_ = ObQueryFlag();
  main_table_ctx_.query_flag_.query_stat_ = 1;
  main_table_ctx_.store_ctx_ = &store_ctx_;
  main_table_ctx_.allocator_ = &allocator_;
  main_table_ctx_.stmt_allocator_ = &allocator_;
  main_table_ctx_.is_inited_ = true;
  for (int16_t i = 0; i < TEST_COLUMN_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, out_idxs_.push_back(i));
  }

  ret = table_schema_.get_column_ids(columns);
  ASSERT_EQ(OB_SUCCESS, ret);
  main_table_param_.reset();
  ASSERT_EQ(OB_SUCCESS, main_table_param_.out_col_desc_param_.init());
  ASSERT_EQ(OB_SUCCESS, main_table_param_.out_col_desc_param_.assign(columns));
  main_table_param_.iter_param_.table_id_ = table_schema_.get_table_id();
  main_table_param_.iter_param_.rowkey_cnt_ = table_schema_.get_rowkey_column_num();
  main_table_param_.iter_param_.output_project_ = &out_idxs_;
  main_table_param_.iter_param_.out_cols_ = &main_table_param_.out_col_desc_param_.get_col_descs();

  ranges_.reset();
  ObStoreRange range;
  range.set_whole_range();
  ASSERT_EQ(OB_SUCCESS, ranges_.push_back(range));
}

void ObMultipleMultiScanMergeTest::TearDown()
{
  out_idxs_.reset();
  table_schema_.reset();
  for (int64_t i = 0; i < stores_.count(); ++i) {
    delete stores_.at(i);
  }
  stores_.reset();
  OB_STORE_CACHE.destroy();
}

void ObMultipleMultiScanMergeTest::prepare_schema(const int64_t rowkey_cnt, const int64_t column_cnt)
{
  int64_t table_id = combine_id(1, 3001);
  ObColumnSchemaV2 column;
  //init table schema
  table_schema_.reset();
  ASSERT_EQ(OB_SUCCESS, table_schema_.set_table_name("test_sstable"));
  table_schema_.set_tenant_id(1);
  table_schema_.set_tablegroup_id(1);
  table_schema_.set_database_id(1);
  table_schema_.set_table_id(table_id);
  table_schema_.set_rowkey_column_num(rowkey_cnt);
  table_schema_.set_max_used_column_id(column_cnt);
  table_schema_.set_block_size(4 * 1024);
  table_schema_.set_compress_func_name("none");
  //init column
  char name[OB_MAX_FILE_NAME_LENGTH];
  memset(name, 0, sizeof(name));
  ObObjMeta meta_type;
  for(int64_t i = 0; i < column_cnt; ++i){
    ObObjType obj_type = static_cast<ObObjType>(i + 1);
    column.reset();
    column.set_table_id(table_id);
    column.set_column_id(i + OB_APP_MIN_COLUMN_ID);
    column.set_data_length(1);
    sprintf(name, "test%020ld", i);
    ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
    meta_type.set_type(obj_type);
    column.set_meta_type(meta_type);
    if (ob_is_string_type(obj_type) && obj_type != ObHexStringType) {
      meta_type.set_collation_level(CS_LEVEL_IMPLICIT);
      meta_type.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
      column.set_meta_type(meta_type);
    }
    if(obj_type == common::ObVarcharType){
      column.set_rowkey_position(1);
    } else if (obj_type == common::ObCharType){
      column.set_rowkey_position(2);
    } else if (obj_type == common::ObDoubleType){
      column.set_rowkey_position(3);
    } else if (obj_type == common::ObNumberType){
      column.set_rowkey_position(4);
    } else if (obj_type == common::ObUNumberType){
      column.set_rowkey_position(5);
    } else if (obj_type == common::ObIntType){
      column.set_rowkey_position(6);
    } else if (obj_type == common::ObHexStringType){
      column.set_rowkey_position(7);
    } else if (obj_type == common::ObUInt64Type){
      column.set_rowkey_position(8);
    } else {
      column.set_rowkey_position(0);
    }
    ASSERT_EQ(OB_SUCCESS, table_schema_.add_column(column));
  }
  ASSERT_EQ(OB_SUCCESS, row_generate_.init(table_schema_, &allocator_));
}

void ObMultipleMultiScanMergeTest::prepare_store(const ObIArray<ObMockStoreRowIteratorV2 *> &iters)
{
  stores_.reset();
  for (int64_t i = iters.count() - 1; i >= 0; --i) {
    MockObSSStore *store = new MockObSSStore();
    EXPECT_CALL(*store, multi_scan(_, _, _, _))
          .WillOnce(DoAll(SetArgReferee<3>(static_cast<ObStoreRowIterator *>(iters.at(i))),
                Return(OB_SUCCESS)));
    ASSERT_EQ(OB_SUCCESS, stores_.push_back(store));
  }
}

TEST_F(ObMultipleMultiScanMergeTest, test_mock_row_iterator)
{
  const ObStoreRow *row = NULL;
  ObStoreRow check_row;
  ObObj cells[TEST_COLUMN_CNT];
  check_row.row_val_.assign(cells, TEST_COLUMN_CNT);
  ObMockStoreRowIteratorV2 row_iter(table_schema_, row_generate_);
  ASSERT_EQ(OB_ITER_END, row_iter.get_next_row(row));
  row_iter.reset();
  row_iter.add_get_range(10, true, false);
  ASSERT_EQ(OB_SUCCESS, row_iter.get_next_row(row));
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(10, check_row));
  ASSERT_TRUE(check_row.row_val_ == row->row_val_);
  ASSERT_EQ(OB_ITER_END, row_iter.get_next_row(row));
  row_iter.reset();
  row_iter.add_scan_range(12, 15, true, false);
  row_iter.add_get_range(16, true, false);
  row_iter.add_scan_range(17,19, true, false);
  for (int64_t i = 12; i <= 19; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_iter.get_next_row(row));
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(i, check_row));
    ASSERT_TRUE(check_row.row_val_ == row->row_val_);
  }
  ASSERT_EQ(OB_ITER_END, row_iter.get_next_row(row));
}

TEST_F(ObMultipleMultiScanMergeTest, test_empty)
{
  ObMultipleMultiScanMerge merge_iter;
  ObArray<ObMockStoreRowIteratorV2 *> iters;
  ObStoreRow *prow = NULL;
  ObStoreRow check_row;
  ObObj cells[TEST_COLUMN_CNT];
  check_row.row_val_.assign(cells, TEST_COLUMN_CNT);

  ObMockStoreRowIteratorV2 *row_iter1 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter2 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter3 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter4 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter1));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter2));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter3));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter4));
  prepare_store(iters);
  ASSERT_EQ(OB_SUCCESS, merge_iter.init(main_table_param_, main_table_ctx_, stores_));
  ASSERT_EQ(OB_SUCCESS, merge_iter.open(ranges_));
  ASSERT_EQ(OB_ITER_END, merge_iter.get_next_row(prow));
  merge_iter.reset();
}

TEST_F(ObMultipleMultiScanMergeTest, test_single_get)
{
  ObMultipleMultiScanMerge merge_iter;
  ObArray<ObMockStoreRowIteratorV2 *> iters;
  ObStoreRow *prow = NULL;
  ObStoreRow check_row;
  ObObj cells[TEST_COLUMN_CNT];
  check_row.row_val_.assign(cells, TEST_COLUMN_CNT);

  ObMockStoreRowIteratorV2 *row_iter1 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter2 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter3 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter4 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter1));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter2));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter3));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter4));
  prepare_store(iters);
  row_iter1->add_get_range(1, true, false);
  row_iter2->add_get_range(1, true, false);
  row_iter3->add_get_range(1, true, false);
  row_iter4->add_get_range(1, true, false);
  ASSERT_EQ(OB_SUCCESS, merge_iter.init(main_table_param_, main_table_ctx_, stores_));
  ASSERT_EQ(OB_SUCCESS, merge_iter.open(ranges_));
  ASSERT_EQ(OB_SUCCESS, merge_iter.get_next_row(prow));
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(1, check_row));
  ASSERT_TRUE(check_row.row_val_ == prow->row_val_);
  ASSERT_EQ(OB_ITER_END, merge_iter.get_next_row(prow));
  merge_iter.reset();
}

TEST_F(ObMultipleMultiScanMergeTest, test_single_scan)
{
  ObMultipleMultiScanMerge merge_iter;
  ObArray<ObMockStoreRowIteratorV2 *> iters;
  ObStoreRow *prow = NULL;
  ObStoreRow check_row;
  ObObj cells[TEST_COLUMN_CNT];
  check_row.row_val_.assign(cells, TEST_COLUMN_CNT);

  ObMockStoreRowIteratorV2 *row_iter1 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter2 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter3 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter4 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter1));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter2));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter3));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter4));
  prepare_store(iters);
  row_iter1->add_scan_range(1, 6, true, false);
  row_iter2->add_scan_range(2, 3, true, false);
  row_iter3->add_scan_range(4, 5, true, false);
  row_iter4->add_scan_range(3, 6, true, false);
  ASSERT_EQ(OB_SUCCESS, merge_iter.init(main_table_param_, main_table_ctx_, stores_));
  ASSERT_EQ(OB_SUCCESS, merge_iter.open(ranges_));
  for (int64_t i = 1; i <= 6; ++i) {
    ASSERT_EQ(OB_SUCCESS, merge_iter.get_next_row(prow));
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(i, check_row));
    STORAGE_LOG(INFO, "row", K(i), K(prow->row_val_));
    ASSERT_TRUE(check_row.row_val_ == prow->row_val_);
  }
  ASSERT_EQ(OB_ITER_END, merge_iter.get_next_row(prow));
  merge_iter.reset();
}

TEST_F(ObMultipleMultiScanMergeTest, test_multi_get)
{
  ObMultipleMultiScanMerge merge_iter;
  ObArray<ObMockStoreRowIteratorV2 *> iters;
  ObStoreRow *prow = NULL;
  ObStoreRow check_row;
  ObObj cells[TEST_COLUMN_CNT];
  check_row.row_val_.assign(cells, TEST_COLUMN_CNT);

  ObMockStoreRowIteratorV2 *row_iter1 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter2 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter3 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter4 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter1));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter2));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter3));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter4));
  prepare_store(iters);
  for (int64_t i = 0; i < 1000; i += 5) {
    row_iter1->add_get_range(i, true, false);
    row_iter2->add_get_range(i, true, false);
    row_iter3->add_get_range(i, true, false);
    row_iter4->add_get_range(i, true, false);
  }
  ASSERT_EQ(OB_SUCCESS, merge_iter.init(main_table_param_, main_table_ctx_, stores_));
  ASSERT_EQ(OB_SUCCESS, merge_iter.open(ranges_));
  for (int64_t i = 0; i < 1000; i += 5) {
    ASSERT_EQ(OB_SUCCESS, merge_iter.get_next_row(prow));
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(i, check_row));
    STORAGE_LOG(INFO, "row", K(i), K(prow->row_val_));
    ASSERT_TRUE(check_row.row_val_ == prow->row_val_);
  }
  ASSERT_EQ(OB_ITER_END, merge_iter.get_next_row(prow));
  merge_iter.reset();
}

TEST_F(ObMultipleMultiScanMergeTest, test_multi_scan)
{
  ObMultipleMultiScanMerge merge_iter;
  ObArray<ObMockStoreRowIteratorV2 *> iters;
  ObStoreRow *prow = NULL;
  ObStoreRow check_row;
  ObObj cells[TEST_COLUMN_CNT];
  check_row.row_val_.assign(cells, TEST_COLUMN_CNT);

  ObMockStoreRowIteratorV2 *row_iter1 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter2 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter3 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter4 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter1));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter2));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter3));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter4));
  prepare_store(iters);
  for (int64_t i = 0; i < 1000; i += 10) {
    row_iter1->add_scan_range(i, i + 3, true, false);
    row_iter2->add_scan_range(i + 2, i + 5, true, false);
    row_iter3->add_scan_range(i, i + 3, true, false);
    row_iter4->add_scan_range(i + 4, i + 10, true, false);
  }
  ASSERT_EQ(OB_SUCCESS, merge_iter.init(main_table_param_, main_table_ctx_, stores_));
  ASSERT_EQ(OB_SUCCESS, merge_iter.open(ranges_));
  for (int64_t i = 0; i < 1000; i += 10) {
    for (int64_t j = i; j <= i + 10; ++j) {
      ASSERT_EQ(OB_SUCCESS, merge_iter.get_next_row(prow));
      ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(j, check_row));
      ASSERT_TRUE(check_row.row_val_ == prow->row_val_);
    }
  }
  ASSERT_EQ(OB_ITER_END, merge_iter.get_next_row(prow));
  merge_iter.reset();
}

TEST_F(ObMultipleMultiScanMergeTest, test_multi_scan_with_get)
{
  ObMultipleMultiScanMerge merge_iter;
  ObArray<ObMockStoreRowIteratorV2 *> iters;
  ObStoreRow *prow = NULL;
  ObStoreRow check_row;
  ObObj cells[TEST_COLUMN_CNT];
  check_row.row_val_.assign(cells, TEST_COLUMN_CNT);

  ObMockStoreRowIteratorV2 *row_iter1 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter2 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter3 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter4 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter1));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter2));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter3));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter4));
  prepare_store(iters);
  for (int64_t i = 0; i < 100; ++i) {
    if (i % 2) {
      row_iter1->add_scan_range(i, i + 3, true, false);
      row_iter2->add_scan_range(i + 2, i + 5, true, false);
      row_iter3->add_scan_range(i, i + 3, true, false);
      row_iter4->add_scan_range(i + 4, i + 10, true, false);
    } else {
      row_iter1->add_get_range(i, true, false);
      row_iter2->add_get_range(i, true, false);
      row_iter3->add_get_range(i, true, false);
      row_iter4->add_get_range(i, true, false);
    }
  }
  ASSERT_EQ(OB_SUCCESS, merge_iter.init(main_table_param_, main_table_ctx_, stores_));
  ASSERT_EQ(OB_SUCCESS, merge_iter.open(ranges_));
  for (int64_t i = 0; i < 100; ++i) {
    if (i % 2) {
      int64_t j = 0;
      for (j = i; j <= i + 10; ++j) {
        ASSERT_EQ(OB_SUCCESS, merge_iter.get_next_row(prow));
        ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(j, check_row));
        STORAGE_LOG(INFO, "row", K(j), K(prow->row_val_), K(check_row.row_val_));
        ASSERT_TRUE(check_row.row_val_ == prow->row_val_);
      }
    } else {
      ASSERT_EQ(OB_SUCCESS, merge_iter.get_next_row(prow));
      ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(i, check_row));
      STORAGE_LOG(INFO, "row", K(i), K(prow->row_val_));
      ASSERT_TRUE(check_row.row_val_ == prow->row_val_);
    }
  }
  ASSERT_EQ(OB_ITER_END, merge_iter.get_next_row(prow));
  merge_iter.reset();
}

TEST_F(ObMultipleMultiScanMergeTest, test_multi_scan_with_get2)
{
  ObMultipleMultiScanMerge merge_iter;
  ObArray<ObMockStoreRowIteratorV2 *> iters;
  ObStoreRow *prow = NULL;
  ObStoreRow check_row;
  ObObj cells[TEST_COLUMN_CNT];
  check_row.row_val_.assign(cells, TEST_COLUMN_CNT);

  ObMockStoreRowIteratorV2 *row_iter1 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter2 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter3 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter4 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter1));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter2));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter3));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter4));
  prepare_store(iters);
  for (int64_t i = 0; i < 100; ++i) {
    if (i % 2 == 0) {
      row_iter1->add_scan_range(i, i + 3, true, false);
      row_iter2->add_scan_range(i + 2, i + 5, true, false);
      row_iter3->add_scan_range(i, i + 3, true, false);
      row_iter4->add_scan_range(i + 4, i + 10, true, false);
    } else {
      row_iter1->add_get_range(i, true, false);
      row_iter2->add_get_range(i, true, false);
      row_iter3->add_get_range(i, true, false);
      row_iter4->add_get_range(i, true, false);
    }
  }
  ASSERT_EQ(OB_SUCCESS, merge_iter.init(main_table_param_, main_table_ctx_, stores_));
  ASSERT_EQ(OB_SUCCESS, merge_iter.open(ranges_));
  for (int64_t i = 0; i < 100; ++i) {
    if (i % 2 == 0) {
      for (int64_t j = i; j <= i + 10; ++j) {
        ASSERT_EQ(OB_SUCCESS, merge_iter.get_next_row(prow));
        ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(j, check_row));
        STORAGE_LOG(INFO, "row", K(j), K(prow->row_val_));
        ASSERT_TRUE(check_row.row_val_ == prow->row_val_);
      }
    } else {
      ASSERT_EQ(OB_SUCCESS, merge_iter.get_next_row(prow));
      ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(i, check_row));
      STORAGE_LOG(INFO, "row", K(i), K(prow->row_val_));
      ASSERT_TRUE(check_row.row_val_ == prow->row_val_);
    }
  }
  ASSERT_EQ(OB_ITER_END, merge_iter.get_next_row(prow));
  merge_iter.reset();
}

TEST_F(ObMultipleMultiScanMergeTest, test_multi_scan_with_get3)
{
  ObMultipleMultiScanMerge merge_iter;
  ObArray<ObMockStoreRowIteratorV2 *> iters;
  ObStoreRow *prow = NULL;
  ObStoreRow check_row;
  ObObj cells[TEST_COLUMN_CNT];
  check_row.row_val_.assign(cells, TEST_COLUMN_CNT);

  ObMockStoreRowIteratorV2 *row_iter1 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter2 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter3 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter4 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter1));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter2));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter3));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter4));
  prepare_store(iters);
  for (int64_t i = 0; i < 100; ++i) {
    if (i == 0) {
      row_iter1->add_scan_range(i, i + 3, true, false);
      row_iter2->add_scan_range(i + 2, i + 5, true, false);
      row_iter3->add_scan_range(i, i + 3, true, false);
      row_iter4->add_scan_range(i + 4, i + 10, true, false);
    } else {
      row_iter1->add_get_range(i, true, false);
      row_iter2->add_get_range(i, true, false);
      row_iter3->add_get_range(i, true, false);
      row_iter4->add_get_range(i, true, false);
    }
  }
  ASSERT_EQ(OB_SUCCESS, merge_iter.init(main_table_param_, main_table_ctx_, stores_));
  ASSERT_EQ(OB_SUCCESS, merge_iter.open(ranges_));
  for (int64_t i = 0; i < 100; ++i) {
    if (i == 0) {
      for (int64_t j = i; j <= i + 10; ++j) {
        ASSERT_EQ(OB_SUCCESS, merge_iter.get_next_row(prow));
        ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(j, check_row));
        STORAGE_LOG(INFO, "row", K(j), K(prow->row_val_));
        ASSERT_TRUE(check_row.row_val_ == prow->row_val_);
      }
    } else {
      ASSERT_EQ(OB_SUCCESS, merge_iter.get_next_row(prow));
      ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(i, check_row));
      STORAGE_LOG(INFO, "row", K(i), K(prow->row_val_));
      ASSERT_TRUE(check_row.row_val_ == prow->row_val_);
    }
  }
  ASSERT_EQ(OB_ITER_END, merge_iter.get_next_row(prow));
  merge_iter.reset();
}

TEST_F(ObMultipleMultiScanMergeTest, test_multi_scan_with_get4)
{
  ObMultipleMultiScanMerge merge_iter;
  ObArray<ObMockStoreRowIteratorV2 *> iters;
  ObStoreRow *prow = NULL;
  ObStoreRow check_row;
  ObObj cells[TEST_COLUMN_CNT];
  check_row.row_val_.assign(cells, TEST_COLUMN_CNT);

  ObMockStoreRowIteratorV2 *row_iter1 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter2 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter3 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter4 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter1));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter2));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter3));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter4));
  prepare_store(iters);
  for (int64_t i = 0; i < 100; ++i) {
    if (i == 99) {
      row_iter1->add_scan_range(i, i + 3, true, false);
      row_iter2->add_scan_range(i + 2, i + 5, true, false);
      row_iter3->add_scan_range(i, i + 3, true, false);
      row_iter4->add_scan_range(i + 4, i + 10, true, false);
    } else {
      row_iter1->add_get_range(i, true, false);
      row_iter2->add_get_range(i, true, false);
      row_iter3->add_get_range(i, true, false);
      row_iter4->add_get_range(i, true, false);
    }
  }
  ASSERT_EQ(OB_SUCCESS, merge_iter.init(main_table_param_, main_table_ctx_, stores_));
  ASSERT_EQ(OB_SUCCESS, merge_iter.open(ranges_));
  for (int64_t i = 0; i < 100; ++i) {
    if (i == 99) {
      for (int64_t j = i; j <= i + 10; ++j) {
        ASSERT_EQ(OB_SUCCESS, merge_iter.get_next_row(prow));
        ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(j, check_row));
        STORAGE_LOG(INFO, "row", K(j), K(prow->row_val_));
        ASSERT_TRUE(check_row.row_val_ == prow->row_val_);
      }
    } else {
      ASSERT_EQ(OB_SUCCESS, merge_iter.get_next_row(prow));
      ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(i, check_row));
      STORAGE_LOG(INFO, "row", K(i), K(prow->row_val_));
      ASSERT_TRUE(check_row.row_val_ == prow->row_val_);
    }
  }
  ASSERT_EQ(OB_ITER_END, merge_iter.get_next_row(prow));
  merge_iter.reset();
}

TEST_F(ObMultipleMultiScanMergeTest, test_multi_scan_with_get5)
{
  ObMultipleMultiScanMerge merge_iter;
  ObArray<ObMockStoreRowIteratorV2 *> iters;
  ObStoreRow *prow = NULL;
  ObStoreRow check_row;
  ObObj cells[TEST_COLUMN_CNT];
  check_row.row_val_.assign(cells, TEST_COLUMN_CNT);

  ObMockStoreRowIteratorV2 *row_iter1 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter2 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter3 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter4 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter1));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter2));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter3));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter4));
  prepare_store(iters);
  for (int64_t i = 0; i < 100; ++i) {
    if (i > 50) {
      row_iter1->add_scan_range(i, i + 3, true, false);
      row_iter2->add_scan_range(i + 2, i + 5, true, false);
      row_iter3->add_scan_range(i, i + 3, true, false);
      row_iter4->add_scan_range(i + 4, i + 10, true, false);
    } else {
      row_iter1->add_get_range(i, true, false);
      row_iter2->add_get_range(i, true, false);
      row_iter3->add_get_range(i, true, false);
      row_iter4->add_get_range(i, true, false);
    }
  }
  ASSERT_EQ(OB_SUCCESS, merge_iter.init(main_table_param_, main_table_ctx_, stores_));
  ASSERT_EQ(OB_SUCCESS, merge_iter.open(ranges_));
  for (int64_t i = 0; i < 100; ++i) {
    if (i > 50) {
      for (int64_t j = i; j <= i + 10; ++j) {
        ASSERT_EQ(OB_SUCCESS, merge_iter.get_next_row(prow));
        ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(j, check_row));
        STORAGE_LOG(INFO, "row", K(j), K(prow->row_val_));
        ASSERT_TRUE(check_row.row_val_ == prow->row_val_);
      }
    } else {
      ASSERT_EQ(OB_SUCCESS, merge_iter.get_next_row(prow));
      ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(i, check_row));
      STORAGE_LOG(INFO, "row", K(i), K(prow->row_val_));
      ASSERT_TRUE(check_row.row_val_ == prow->row_val_);
    }
  }
  ASSERT_EQ(OB_ITER_END, merge_iter.get_next_row(prow));
  merge_iter.reset();
}

TEST_F(ObMultipleMultiScanMergeTest, test_multi_scan_with_get6)
{
  ObMultipleMultiScanMerge merge_iter;
  ObArray<ObMockStoreRowIteratorV2 *> iters;
  ObStoreRow *prow = NULL;
  ObStoreRow check_row;
  ObObj cells[TEST_COLUMN_CNT];
  check_row.row_val_.assign(cells, TEST_COLUMN_CNT);

  ObMockStoreRowIteratorV2 *row_iter1 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter2 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter3 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter4 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter1));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter2));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter3));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter4));
  prepare_store(iters);
  for (int64_t i = 0; i < 100; ++i) {
    if (i < 50) {
      row_iter1->add_scan_range(i, i + 3, true, false);
      row_iter2->add_scan_range(i + 2, i + 5, true, false);
      row_iter3->add_scan_range(i, i + 3, true, false);
      row_iter4->add_scan_range(i + 4, i + 10, true, false);
    } else {
      row_iter1->add_get_range(i, true, false);
      row_iter2->add_get_range(i, true, false);
      row_iter3->add_get_range(i, true, false);
      row_iter4->add_get_range(i, true, false);
    }
  }
  ASSERT_EQ(OB_SUCCESS, merge_iter.init(main_table_param_, main_table_ctx_, stores_));
  ASSERT_EQ(OB_SUCCESS, merge_iter.open(ranges_));
  for (int64_t i = 0; i < 100; ++i) {
    if (i < 50) {
      for (int64_t j = i; j <= i + 10; ++j) {
        ASSERT_EQ(OB_SUCCESS, merge_iter.get_next_row(prow));
        ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(j, check_row));
        STORAGE_LOG(INFO, "row", K(j), K(prow->row_val_));
        ASSERT_TRUE(check_row.row_val_ == prow->row_val_);
      }
    } else {
      ASSERT_EQ(OB_SUCCESS, merge_iter.get_next_row(prow));
      ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(i, check_row));
      STORAGE_LOG(INFO, "row", K(i), K(prow->row_val_));
      ASSERT_TRUE(check_row.row_val_ == prow->row_val_);
    }
  }
  ASSERT_EQ(OB_ITER_END, merge_iter.get_next_row(prow));
  merge_iter.reset();
}

TEST_F(ObMultipleMultiScanMergeTest, test_multi_scan_with_empty_iterator)
{
  ObMultipleMultiScanMerge merge_iter;
  ObArray<ObMockStoreRowIteratorV2 *> iters;
  ObStoreRow *prow = NULL;
  ObStoreRow check_row;
  ObObj cells[TEST_COLUMN_CNT];
  check_row.row_val_.assign(cells, TEST_COLUMN_CNT);

  ObMockStoreRowIteratorV2 *row_iter1 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter2 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter3 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter4 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter1));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter2));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter3));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter4));
  prepare_store(iters);
  for (int64_t i = 0; i < 100; ++i) {
    if (i < 50) {
      row_iter1->add_scan_range(i, i + 3, true, false);
      row_iter2->add_scan_range(i + 2, i + 5, true, false);
      row_iter4->add_scan_range(i + 4, i + 10, true, false);
    } else {
      row_iter1->add_get_range(i, true, false);
      row_iter2->add_get_range(i, true, false);
      row_iter4->add_get_range(i, true, false);
    }
  }
  ASSERT_EQ(OB_SUCCESS, merge_iter.init(main_table_param_, main_table_ctx_, stores_));
  ASSERT_EQ(OB_SUCCESS, merge_iter.open(ranges_));
  for (int64_t i = 0; i < 100; ++i) {
    if (i < 50) {
      for (int64_t j = i; j <= i + 10; ++j) {
        ASSERT_EQ(OB_SUCCESS, merge_iter.get_next_row(prow));
        ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(j, check_row));
        STORAGE_LOG(INFO, "row", K(j), K(prow->row_val_));
        ASSERT_TRUE(check_row.row_val_ == prow->row_val_);
      }
    } else {
      ASSERT_EQ(OB_SUCCESS, merge_iter.get_next_row(prow));
      ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(i, check_row));
      STORAGE_LOG(INFO, "row", K(i), K(prow->row_val_));
      ASSERT_TRUE(check_row.row_val_ == prow->row_val_);
    }
  }
  ASSERT_EQ(OB_ITER_END, merge_iter.get_next_row(prow));
  merge_iter.reset();
}

TEST_F(ObMultipleMultiScanMergeTest, test_multi_scan_all_empty_iterator)
{
  ObMultipleMultiScanMerge merge_iter;
  ObArray<ObMockStoreRowIteratorV2 *> iters;
  ObStoreRow *prow = NULL;
  ObStoreRow check_row;
  ObObj cells[TEST_COLUMN_CNT];
  check_row.row_val_.assign(cells, TEST_COLUMN_CNT);

  ObMockStoreRowIteratorV2 *row_iter1 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter2 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter3 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter4 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter1));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter2));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter3));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter4));
  prepare_store(iters);
  ASSERT_EQ(OB_SUCCESS, merge_iter.init(main_table_param_, main_table_ctx_, stores_));
  ASSERT_EQ(OB_SUCCESS, merge_iter.open(ranges_));
  ASSERT_EQ(OB_ITER_END, merge_iter.get_next_row(prow));
  merge_iter.reset();
}

TEST_F(ObMultipleMultiScanMergeTest, test_multi_scan_all_deleted_rows)
{
  ObMultipleMultiScanMerge merge_iter;
  ObArray<ObMockStoreRowIteratorV2 *> iters;
  ObStoreRow *prow = NULL;
  ObStoreRow check_row;
  ObObj cells[TEST_COLUMN_CNT];
  check_row.row_val_.assign(cells, TEST_COLUMN_CNT);

  ObMockStoreRowIteratorV2 *row_iter1 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter2 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter3 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter4 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  row_iter1->add_scan_range(0, 1, false, false);
  row_iter2->add_scan_range(0, 1, true, false);
  row_iter3->add_scan_range(0, 1, true, false);
  row_iter4->add_scan_range(0, 1, true, false);
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter1));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter2));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter3));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter4));
  prepare_store(iters);
  ASSERT_EQ(OB_SUCCESS, merge_iter.init(main_table_param_, main_table_ctx_, stores_));
  ASSERT_EQ(OB_SUCCESS, merge_iter.open(ranges_));
  ASSERT_EQ(OB_ITER_END, merge_iter.get_next_row(prow));
  merge_iter.reset();
}

TEST_F(ObMultipleMultiScanMergeTest, test_memtable_exist_ssstore_not_exist)
{
  ObMultipleMultiScanMerge merge_iter;
  ObArray<ObMockStoreRowIteratorV2 *> iters;
  ObStoreRow *prow = NULL;
  ObStoreRow check_row;
  ObObj cells[TEST_COLUMN_CNT];
  check_row.row_val_.assign(cells, TEST_COLUMN_CNT);

  ObMockStoreRowIteratorV2 *row_iter1 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter2 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter3 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter4 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter1));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter2));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter3));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter4));
  prepare_store(iters);
  row_iter1->add_get_range(10, true, false);
  row_iter2->add_get_range(10, false, false);
  row_iter3->add_get_range(10, false, false);
  row_iter4->add_get_range(10, false, false);
  row_iter1->add_scan_range(1, 4, true, false);
  row_iter2->add_scan_range(1, 4, false, false);
  row_iter3->add_scan_range(1, 4, false, false);
  row_iter4->add_scan_range(1, 4, false, false);
  row_iter1->add_get_range(5, true, false);
  row_iter2->add_get_range(5, false, false);
  row_iter3->add_get_range(5, false, false);
  row_iter4->add_get_range(5, false, false);
  row_iter1->add_scan_range(6, 9, true, false);
  row_iter2->add_scan_range(6, 9, false, false);
  row_iter3->add_scan_range(6, 9, false, false);
  row_iter4->add_scan_range(6, 9, false, false);
  ASSERT_EQ(OB_SUCCESS, merge_iter.init(main_table_param_, main_table_ctx_, stores_));
  ASSERT_EQ(OB_SUCCESS, merge_iter.open(ranges_));
  ASSERT_EQ(OB_SUCCESS, merge_iter.get_next_row(prow));
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(10, check_row));
  ASSERT_TRUE(check_row.row_val_ == prow->row_val_);
  for (int64_t i = 1; i <= 9; ++i) {
    ASSERT_EQ(OB_SUCCESS, merge_iter.get_next_row(prow));
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(i, check_row));
    ASSERT_TRUE(check_row.row_val_ == prow->row_val_);
  }
  merge_iter.reset();
}

TEST_F(ObMultipleMultiScanMergeTest, test_memtable_exist_ssstore_exist)
{
  ObMultipleMultiScanMerge merge_iter;
  ObArray<ObMockStoreRowIteratorV2 *> iters;
  ObStoreRow *prow = NULL;
  ObStoreRow check_row;
  ObObj cells[TEST_COLUMN_CNT];
  check_row.row_val_.assign(cells, TEST_COLUMN_CNT);

  ObMockStoreRowIteratorV2 *row_iter1 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter2 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter3 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter4 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter1));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter2));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter3));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter4));
  prepare_store(iters);
  row_iter1->add_get_range(10, true, false);
  row_iter2->add_get_range(10, true, false);
  row_iter3->add_get_range(10, true, false);
  row_iter4->add_get_range(10, true, false);
  row_iter1->add_scan_range(1, 4, true, false);
  row_iter2->add_scan_range(1, 4, true, false);
  row_iter3->add_scan_range(1, 4, true, false);
  row_iter4->add_scan_range(1, 4, true, false);
  row_iter1->add_get_range(5, true, false);
  row_iter2->add_get_range(5, true, false);
  row_iter3->add_get_range(5, true, false);
  row_iter4->add_get_range(5, true, false);
  row_iter1->add_scan_range(6, 9, true, false);
  row_iter2->add_scan_range(6, 9, true, false);
  row_iter3->add_scan_range(6, 9, true, false);
  row_iter4->add_scan_range(6, 9, true, false);
  ASSERT_EQ(OB_SUCCESS, merge_iter.init(main_table_param_, main_table_ctx_, stores_));
  ASSERT_EQ(OB_SUCCESS, merge_iter.open(ranges_));
  ASSERT_EQ(OB_SUCCESS, merge_iter.get_next_row(prow));
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(10, check_row));
  ASSERT_TRUE(check_row.row_val_ == prow->row_val_);
  for (int64_t i = 1; i <= 9; ++i) {
    ASSERT_EQ(OB_SUCCESS, merge_iter.get_next_row(prow));
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(i, check_row));
    ASSERT_TRUE(check_row.row_val_ == prow->row_val_);
  }
  merge_iter.reset();
}

TEST_F(ObMultipleMultiScanMergeTest, test_memtable_exist_ssstore_partial_exist)
{
  ObMultipleMultiScanMerge merge_iter;
  ObArray<ObMockStoreRowIteratorV2 *> iters;
  ObStoreRow *prow = NULL;
  ObStoreRow check_row;
  ObObj cells[TEST_COLUMN_CNT];
  check_row.row_val_.assign(cells, TEST_COLUMN_CNT);

  ObMockStoreRowIteratorV2 *row_iter1 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter2 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter3 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter4 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter1));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter2));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter3));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter4));
  prepare_store(iters);
  row_iter1->add_get_range(10, true, false);
  row_iter2->add_get_range(10, false, false);
  row_iter3->add_get_range(10, true, false);
  row_iter4->add_get_range(10, false, false);
  row_iter1->add_scan_range(1, 4, true, false);
  row_iter2->add_scan_range(1, 4, true, false);
  row_iter3->add_scan_range(1, 4, false, false);
  row_iter4->add_scan_range(1, 4, true, false);
  row_iter1->add_get_range(5, true, false);
  row_iter2->add_get_range(5, false, false);
  row_iter3->add_get_range(5, true, false);
  row_iter4->add_get_range(5, false, false);
  row_iter1->add_scan_range(6, 9, true, false);
  row_iter2->add_scan_range(6, 9, true, false);
  row_iter3->add_scan_range(6, 9, false, false);
  row_iter4->add_scan_range(6, 9, true, false);
  ASSERT_EQ(OB_SUCCESS, merge_iter.init(main_table_param_, main_table_ctx_, stores_));
  ASSERT_EQ(OB_SUCCESS, merge_iter.open(ranges_));
  ASSERT_EQ(OB_SUCCESS, merge_iter.get_next_row(prow));
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(10, check_row));
  ASSERT_TRUE(check_row.row_val_ == prow->row_val_);
  for (int64_t i = 1; i <= 9; ++i) {
    ASSERT_EQ(OB_SUCCESS, merge_iter.get_next_row(prow));
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(i, check_row));
    ASSERT_TRUE(check_row.row_val_ == prow->row_val_);
  }
  merge_iter.reset();
}

TEST_F(ObMultipleMultiScanMergeTest, test_memtable_with_nop_ssstore_exist)
{
  ObMultipleMultiScanMerge merge_iter;
  ObArray<ObMockStoreRowIteratorV2 *> iters;
  ObStoreRow *prow = NULL;
  ObStoreRow check_row;
  ObObj cells[TEST_COLUMN_CNT];
  check_row.row_val_.assign(cells, TEST_COLUMN_CNT);

  ObMockStoreRowIteratorV2 *row_iter1 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter2 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter3 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter4 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter1));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter2));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter3));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter4));
  prepare_store(iters);
  row_iter1->add_get_range(10, true, true);
  row_iter2->add_get_range(10, true, false);
  row_iter3->add_get_range(10, true, false);
  row_iter4->add_get_range(10, true, false);
  row_iter1->add_scan_range(1, 4, true, true);
  row_iter2->add_scan_range(1, 4, true, false);
  row_iter3->add_scan_range(1, 4, true, false);
  row_iter4->add_scan_range(1, 4, true, false);
  row_iter1->add_get_range(5, true, true);
  row_iter2->add_get_range(5, true, false);
  row_iter3->add_get_range(5, true, false);
  row_iter4->add_get_range(5, true, false);
  row_iter1->add_scan_range(6, 9, true, true);
  row_iter2->add_scan_range(6, 9, true, false);
  row_iter3->add_scan_range(6, 9, true, false);
  row_iter4->add_scan_range(6, 9, true, false);
  ASSERT_EQ(OB_SUCCESS, merge_iter.init(main_table_param_, main_table_ctx_, stores_));
  ASSERT_EQ(OB_SUCCESS, merge_iter.open(ranges_));
  ASSERT_EQ(OB_SUCCESS, merge_iter.get_next_row(prow));
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(10, check_row));
  ASSERT_TRUE(check_row.row_val_ == prow->row_val_);
  for (int64_t i = 1; i <= 9; ++i) {
    ASSERT_EQ(OB_SUCCESS, merge_iter.get_next_row(prow));
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(i, check_row));
    ASSERT_TRUE(check_row.row_val_ == prow->row_val_);
  }
  merge_iter.reset();
}

TEST_F(ObMultipleMultiScanMergeTest, test_memtable_with_nop_partial_ssstore_exist)
{
  ObMultipleMultiScanMerge merge_iter;
  ObArray<ObMockStoreRowIteratorV2 *> iters;
  ObStoreRow *prow = NULL;
  ObStoreRow check_row;
  ObObj cells[TEST_COLUMN_CNT];
  check_row.row_val_.assign(cells, TEST_COLUMN_CNT);

  ObMockStoreRowIteratorV2 *row_iter1 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter2 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter3 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter4 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter1));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter2));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter3));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter4));
  prepare_store(iters);
  row_iter1->add_get_range(10, true, true);
  row_iter2->add_get_range(10, true, false);
  row_iter3->add_get_range(10, true, true);
  row_iter4->add_get_range(10, false, false);
  row_iter1->add_scan_range(1, 4, true, true);
  row_iter2->add_scan_range(1, 4, true, false);
  row_iter3->add_scan_range(1, 4, true, true);
  row_iter4->add_scan_range(1, 4, false, false);
  row_iter1->add_get_range(5, true, true);
  row_iter2->add_get_range(5, true, false);
  row_iter3->add_get_range(5, true, true);
  row_iter4->add_get_range(5, false, false);
  row_iter1->add_scan_range(6, 9, true, true);
  row_iter2->add_scan_range(6, 9, true, false);
  row_iter3->add_scan_range(6, 9, true, true);
  row_iter4->add_scan_range(6, 9, false, false);
  ASSERT_EQ(OB_SUCCESS, merge_iter.init(main_table_param_, main_table_ctx_, stores_));
  ASSERT_EQ(OB_SUCCESS, merge_iter.open(ranges_));
  ASSERT_EQ(OB_SUCCESS, merge_iter.get_next_row(prow));
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(10, check_row));
  ASSERT_TRUE(check_row.row_val_ == prow->row_val_);
  for (int64_t i = 1; i <= 9; ++i) {
    ASSERT_EQ(OB_SUCCESS, merge_iter.get_next_row(prow));
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(i, check_row));
    ASSERT_TRUE(check_row.row_val_ == prow->row_val_);
  }
  merge_iter.reset();
}

TEST_F(ObMultipleMultiScanMergeTest, test_memtable_not_all_ssstore_exist)
{
  ObMultipleMultiScanMerge merge_iter;
  ObArray<ObMockStoreRowIteratorV2 *> iters;
  ObStoreRow *prow = NULL;
  ObStoreRow check_row;
  ObObj cells[TEST_COLUMN_CNT];
  check_row.row_val_.assign(cells, TEST_COLUMN_CNT);

  ObMockStoreRowIteratorV2 *row_iter1 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter2 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter3 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter4 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter1));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter2));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter3));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter4));
  prepare_store(iters);
  row_iter1->add_get_range(10, false, false);
  row_iter2->add_get_range(10, true, false);
  row_iter3->add_get_range(10, true, false);
  row_iter4->add_get_range(10, true, false);
  row_iter1->add_scan_range(1, 4, false, false);
  row_iter2->add_scan_range(1, 4, true, false);
  row_iter3->add_scan_range(1, 4, true, false);
  row_iter4->add_scan_range(1, 4, true, false);
  row_iter1->add_get_range(5, false, false);
  row_iter2->add_get_range(5, true, false);
  row_iter3->add_get_range(5, true, false);
  row_iter4->add_get_range(5, true, false);
  row_iter1->add_scan_range(6, 9, false, false);
  row_iter2->add_scan_range(6, 9, true, false);
  row_iter3->add_scan_range(6, 9, true, false);
  row_iter4->add_scan_range(6, 9, true, false);
  ASSERT_EQ(OB_SUCCESS, merge_iter.init(main_table_param_, main_table_ctx_, stores_));
  ASSERT_EQ(OB_SUCCESS, merge_iter.open(ranges_));
  ASSERT_EQ(OB_ITER_END, merge_iter.get_next_row(prow));
  merge_iter.reset();
}

TEST_F(ObMultipleMultiScanMergeTest, test_memtable_not_partial_ssstore_exist)
{
  ObMultipleMultiScanMerge merge_iter;
  ObArray<ObMockStoreRowIteratorV2 *> iters;
  ObStoreRow *prow = NULL;
  ObStoreRow check_row;
  ObObj cells[TEST_COLUMN_CNT];
  check_row.row_val_.assign(cells, TEST_COLUMN_CNT);

  ObMockStoreRowIteratorV2 *row_iter1 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter2 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter3 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ObMockStoreRowIteratorV2 *row_iter4 = new ObMockStoreRowIteratorV2(table_schema_, row_generate_);
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter1));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter2));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter3));
  ASSERT_EQ(OB_SUCCESS, iters.push_back(row_iter4));
  prepare_store(iters);
  row_iter1->add_get_range(10, false, false);
  row_iter2->add_get_range(10, true, false);
  row_iter3->add_get_range(10, false, false);
  row_iter4->add_get_range(10, true, false);
  row_iter1->add_scan_range(1, 4, false, false);
  row_iter2->add_scan_range(1, 4, false, false);
  row_iter3->add_scan_range(1, 4, true, false);
  row_iter4->add_scan_range(1, 4, true, false);
  row_iter1->add_get_range(5, false, false);
  row_iter2->add_get_range(5, true, false);
  row_iter3->add_get_range(5, true, false);
  row_iter4->add_get_range(5, false, false);
  row_iter1->add_scan_range(6, 9, false, false);
  row_iter2->add_scan_range(6, 9, true, false);
  row_iter3->add_scan_range(6, 9, false, false);
  row_iter4->add_scan_range(6, 9, true, false);
  ASSERT_EQ(OB_SUCCESS, merge_iter.init(main_table_param_, main_table_ctx_, stores_));
  ASSERT_EQ(OB_SUCCESS, merge_iter.open(ranges_));
  ASSERT_EQ(OB_ITER_END, merge_iter.get_next_row(prow));
  merge_iter.reset();
}

TEST_F(ObMultipleMultiScanMergeTest, test_estimate_cost)
{
  int ret = OB_SUCCESS;
  ObQueryFlag query_flag;
  const uint64_t table_id = combine_id(1, 3000);
  share::schema::ObColDesc col;
  ObArray<share::schema::ObColDesc> columns;
  ObPartitionEst cost;
  ObMultipleMultiScanMerge merge_iter;
  ret = merge_iter.estimate_cost(query_flag, table_id, ranges_, columns, stores_, cost);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ret = columns.push_back(col);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObPartitionEst cost_metric1;
  ObPartitionEst cost_metric2;
  cost_metric1.logical_row_count_ = 10;
  cost_metric1.total_cost_ = 100;
  MockObSSStore *store1 = new MockObSSStore();
  //EXPECT_CALL(*store1, get_store_type())
    //.WillRepeatedly(Return(MAJOR_SSSTORE));
  EXPECT_CALL(*store1, estimate_multi_scan_cost(_, _, _, _, _))
    .WillRepeatedly(DoAll(SetArgReferee<4>(cost_metric1), Return(OB_SUCCESS)));
  stores_.reset();
  ret = stores_.push_back(store1);
  ASSERT_EQ(OB_SUCCESS, ret);
  cost_metric2.total_cost_ = 120;
  cost_metric2.logical_row_count_ = 11;
  MockObSSStore *store2 = new MockObSSStore();
  //EXPECT_CALL(*store2, get_store_type())
    //.WillRepeatedly(Return(MAJOR_SSSTORE));
  EXPECT_CALL(*store2, estimate_multi_scan_cost(_, _, _, _, _))
    .WillRepeatedly(DoAll(SetArgReferee<4>(cost_metric2), Return(OB_SUCCESS)));
  ret = stores_.push_back(store2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = merge_iter.estimate_cost(query_flag, table_id, ranges_, columns, stores_, cost);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(11, cost.logical_row_count_);
  ASSERT_EQ(0, cost.physical_row_count_);
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_multiple_multi_scan_merge.log*");
  OB_LOGGER.set_file_name("test_multiple_multi_scan_merge.log");
  OB_LOGGER.set_log_level("INFO");
  CLOG_LOG(INFO, "begin unittest: test_multiple_multi_scan_merge");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
