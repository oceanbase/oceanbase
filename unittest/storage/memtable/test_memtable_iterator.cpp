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

#include "share/schema/ob_table_schema.h"
#include "storage/blocksstable/ob_row_generate.h"
#include <gtest/gtest.h>
#define private public
#define protected public
#include "storage/memtable/ob_memtable_iterator.h"
#undef private
#undef protected

namespace oceanbase
{
namespace unittest
{
using namespace blocksstable;
using namespace common;
using namespace storage;
using namespace memtable;
using namespace share::schema;

static const int64_t TEST_ROWKEY_COLUMN_CNT = 4;
static const int64_t TEST_COLUMN_CNT = ObExtendType;
static const int64_t TEST_MULTI_GET_CNT = 2000;

class ObMockStoreRowIterator : public storage::ObStoreRowIterator
{
public:
  ObMockStoreRowIterator(ObTableSchema &schema, ObRowGenerate &row_generate);
  void add_get_range(const int64_t seed);
  void add_scan_range(const int64_t start_seed, const int64_t end_seed);
  virtual int get_next_row(const ObStoreRow *&row);
  void reset();
private:
  ObSEArray<int64_t, 512> range_start_;
  ObSEArray<int64_t, 512> range_end_;
  int64_t cur_range_index_;
  int64_t cur_row_index_;
  ObStoreRow cur_row_;
  ObTableSchema &table_schema_;
  ObRowGenerate &row_generate_;
  ObArenaAllocator allocator_;
  bool is_first_scan_;
  ObObj cells_[TEST_COLUMN_CNT];
};
ObMockStoreRowIterator::ObMockStoreRowIterator(ObTableSchema &schema, ObRowGenerate &row_generate) : range_start_(), range_end_(), cur_range_index_(0), cur_row_index_(0),
    cur_row_(), table_schema_(schema), row_generate_(row_generate), allocator_(ObModIds::TEST),
    is_first_scan_(true)
{
  cur_row_.row_val_.assign(cells_, TEST_COLUMN_CNT);
}

void ObMockStoreRowIterator::reset()
{
  range_start_.reset();
  range_end_.reset();
  cur_range_index_ = 0;
  cur_row_index_ = 0;
  cur_row_.reset();
  cur_row_.row_val_.assign(cells_, TEST_COLUMN_CNT);
  is_first_scan_ = true;
}

void ObMockStoreRowIterator::add_get_range(const int64_t seed)
{
  ASSERT_EQ(OB_SUCCESS, range_start_.push_back(seed));
  ASSERT_EQ(OB_SUCCESS, range_end_.push_back(seed));
}

void ObMockStoreRowIterator::add_scan_range(const int64_t start_seed, const int64_t end_seed)
{
  ASSERT_EQ(OB_SUCCESS, range_start_.push_back(start_seed));
  ASSERT_EQ(OB_SUCCESS, range_end_.push_back(end_seed));
}

int ObMockStoreRowIterator::get_next_row(const ObStoreRow *&row)
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
     row = &cur_row_;
     //fprintf(stdout, "cur_row_index=%lu, range_end=%lu", cur_row_index_, range_end_.at(cur_range_index_));
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

class ObMockMemtableMScanIterator : public ObMemtableMScanIterator
{
public:
  ObMockMemtableMScanIterator(ObTableSchema &schema, ObRowGenerate &row_generate);
  int init(const ObIArray<ObStoreRange> &ranges);
  ObMockStoreRowIterator &get_store_row_iter() { return iter_; }
private:
  int prepare_scan_range(const ObStoreRange &range, const bool is_reverse_scan);
  int get_next_row_for_get(const ObStoreRow *&row, bool &need_retry);
  int inner_get_next_row_for_scan(const ObStoreRow *&row);
  int get_next_row_for_scan(const ObStoreRow *&row, bool &need_retry);
  ObMockStoreRowIterator iter_;
};

ObMockMemtableMScanIterator::ObMockMemtableMScanIterator(ObTableSchema &schema, ObRowGenerate &row_generate)
  : iter_(schema, row_generate)
{
}

int ObMockMemtableMScanIterator::init(const ObIArray<ObStoreRange> &ranges)
{
  int ret = OB_SUCCESS;
  ranges_ = &ranges;
  is_inited_ = true;
  return ret;
}

int ObMockMemtableMScanIterator::prepare_scan_range(const ObStoreRange &range, const bool is_reverse_scan)
{
  int ret = OB_SUCCESS;
  UNUSED(range);
  UNUSED(is_reverse_scan);
  return ret;
}

int ObMockMemtableMScanIterator::get_next_row_for_get(const ObStoreRow *&row, bool &need_retry)
{
  int ret = OB_SUCCESS;
  bool range_scan = false;
  need_retry = false;
  const bool is_reverse_scan = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObMemtableMScanIterator has not been inited", K(ret));
  } else if (cur_range_pos_ >= ranges_->count() || cur_range_pos_ < 0) {
    ret = OB_ITER_END;
  } else {
    if (OB_FAIL(iter_.get_next_row(row))) {
      TRANS_LOG(WARN, "fail to get memtable row", K(ret));
    } else {
      const_cast<ObStoreRow *>(row)->is_get_ = true;
      const_cast<ObStoreRow *>(row)->scan_index_ = cur_range_pos_;
      ++cur_range_pos_;
      if (OB_FAIL(is_range_scan(range_scan))) {
        if (OB_ITER_END != ret) {
          TRANS_LOG(WARN, "fail to get is_range_scan", K(ret), K(cur_range_pos_));
        } else {
          ret = OB_SUCCESS;
        }
      } else if (range_scan && OB_FAIL(prepare_scan_range(ranges_->at(cur_range_pos_).get_range(), is_reverse_scan))) {
        TRANS_LOG(WARN, "fail to prepare for next scan", K(ret), K(cur_range_pos_));
      }
    }
  }
  return ret;
}

int ObMockMemtableMScanIterator::inner_get_next_row_for_scan(const ObStoreRow *&row)
{
  int ret = OB_SUCCESS;
  if (cur_range_pos_ >= ranges_->count()) {
    ret = OB_ITER_END;
  } else {
    if (OB_FAIL(iter_.get_next_row(row))) {
      if (OB_ITER_END != ret) {
        TRANS_LOG(WARN, "fail to get next row", K(ret));
      }
    }
  }
  return ret;
}

int ObMockMemtableMScanIterator::get_next_row_for_scan(const ObStoreRow *&row, bool &need_retry)
{
  int ret = OB_SUCCESS;
  need_retry = false;
  const bool is_reverse_scan = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(ret));
  } else {
    while (OB_SUCCESS == ret && OB_SUCCESS != (ret = inner_get_next_row_for_scan(row))) {
      if (OB_ITER_END != ret) {
        TRANS_LOG(WARN, "fail to get_next_row", K(ret));
      } else {
        bool range_scan = false;
        ++cur_range_pos_;
        if (OB_FAIL(is_range_scan(range_scan))) {
          if (OB_ITER_END != ret) {
            TRANS_LOG(WARN, "fail to check is_range_scan", K(ret));
          }
        } else {
          if (range_scan) {
            if (OB_FAIL(prepare_scan_range(ranges_->at(cur_range_pos_).get_range(), is_reverse_scan))) {
              TRANS_LOG(WARN, "fail to prepare scan range");
            }
          } else {
            need_retry = true; // current range reaches end, and next range is get, need retry
            break;
          }
        }
      }
    }
  }

  if (OB_SUCC(ret) && !need_retry) {
    const_cast<ObStoreRow *>(row)->is_get_ = false;
    const_cast<ObStoreRow *>(row)->scan_index_ = cur_range_pos_;
  }

  return ret;
}

class ObMemtableMScanIteratorTest: public ::testing::Test
{
public:
  static const int64_t TEST_ROWKEY_COLUMN_CNT = 1;
  static const int64_t TEST_COLUMN_CNT = ObExtendType;

public:
  ObMemtableMScanIteratorTest();
  ObMemtableMScanIteratorTest(ObTableSchema &schema, ObRowGenerate &row_generate);
  void prepare_schema(const int64_t rowkey_cnt, const int64_t column_cnt);
  int64_t compute_condition(const int64_t test_type, const int64_t i, const int64_t total);
  void test_multi_get_with_scan(const int64_t test_type);
  virtual void SetUp();
  virtual void TearDown();
public:
  ObArenaAllocator allocator_;
  ObTableSchema table_schema_;
  ObRowGenerate row_generate_;
};

ObMemtableMScanIteratorTest::ObMemtableMScanIteratorTest()
  : allocator_(ObModIds::TEST), table_schema_(), row_generate_()
{
}

void ObMemtableMScanIteratorTest::SetUp()
{
  prepare_schema(TEST_ROWKEY_COLUMN_CNT, TEST_COLUMN_CNT);
}

void ObMemtableMScanIteratorTest::TearDown()
{
  table_schema_.reset();
}

int64_t ObMemtableMScanIteratorTest::compute_condition(const int64_t test_type,
    const int64_t i, const int64_t total)
{
  switch (test_type) {
    case 0:
      return i % 2;
    case 1:
      return !(i % 2);
    case 2:
      return i > total / 2;
    case 3:
      return i < total / 2;
    case 4:
      return i > 0;
    case 5:
      return !(i > 0);
  }
  return 0;
}

void ObMemtableMScanIteratorTest::prepare_schema(const int64_t rowkey_cnt, const int64_t column_cnt)
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
    ObObjType obj_type = static_cast<ObObjType>(i);
    column.reset();
    column.set_table_id(table_id);
    column.set_column_id(i);
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

void ObMemtableMScanIteratorTest::test_multi_get_with_scan(const int64_t test_type)
{
  int ret = OB_SUCCESS;
  const ObStoreRow *prow = NULL;
  ObStoreRow check_row;
  ObStoreRange range;
  ObObj cells[TEST_COLUMN_CNT];
  ObArray<ObStoreRange> ranges;
  ObArray<ObStoreRange> ranges;
  check_row.row_val_.assign(cells, TEST_COLUMN_CNT);
  ObMockMemtableMScanIterator iter(table_schema_, row_generate_);
  ObMockStoreRowIterator &row_iter = iter.get_store_row_iter();
  ObStoreRow start_row;
  ObStoreRow end_row;
  const int64_t count_per_range = 10;

  // multi get test
  ObStoreRowkey mget_rowkeys[TEST_MULTI_GET_CNT];
  ObStoreRange mget_ranges[TEST_MULTI_GET_CNT];
  ObObj mget_start_cells[TEST_MULTI_GET_CNT][TEST_COLUMN_CNT];
  ObObj mget_end_cells[TEST_MULTI_GET_CNT][TEST_COLUMN_CNT];
  ObStoreRow mget_start_rows[TEST_MULTI_GET_CNT];
  ObStoreRow mget_end_rows[TEST_MULTI_GET_CNT];
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    const int64_t condition = compute_condition(test_type, i, TEST_MULTI_GET_CNT);
    mget_start_rows[i].row_val_.assign(mget_start_cells[i], TEST_COLUMN_CNT);
    mget_end_rows[i].row_val_.assign(mget_end_cells[i], TEST_COLUMN_CNT);
    mget_ranges[i].get_start_key().assign(mget_start_cells[i], TEST_ROWKEY_COLUMN_CNT);
    mget_ranges[i].get_end_key().assign(mget_end_cells[i], TEST_ROWKEY_COLUMN_CNT);
    mget_ranges[i].get_border_flag().set_inclusive_start();
    mget_ranges[i].get_border_flag().set_inclusive_end();
    ret = row_generate_.get_next_row(i, mget_start_rows[i]);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = row_generate_.get_next_row(i + condition ? count_per_range - 1 : 0, mget_end_rows[i]);
    ASSERT_EQ(OB_SUCCESS, ret);
    if (condition) {
      row_iter.add_scan_range(i, i + count_per_range - 1);
    } else {
      row_iter.add_get_range(i);
    }
  }

  //larger than 1000 get
  ranges.reuse();
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    ret = ranges.push_back(mget_ranges[i]);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  ret = iter.init(ranges);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    const int64_t condition = compute_condition(test_type, i, TEST_MULTI_GET_CNT);
    if (condition) {
      for (int64_t j = 0; j < count_per_range; ++j) {
        ret = row_generate_.get_next_row(i+j, check_row);
        ASSERT_EQ(OB_SUCCESS, ret);
        ret = iter.get_next_row(prow);
        ASSERT_EQ(OB_SUCCESS, ret);
        ASSERT_TRUE(const_cast<ObStoreRow *>(prow)->row_val_ == check_row.row_val_);
      }
    } else {
      ret = row_generate_.get_next_row(i, check_row);
      ASSERT_EQ(OB_SUCCESS, ret);
      ret = iter.get_next_row(prow);
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_TRUE(const_cast<ObStoreRow *>(prow)->row_val_ == check_row.row_val_);
    }
  }
  ret = iter.get_next_row(prow);
  ASSERT_EQ(OB_ITER_END, ret);
}

TEST_F(ObMemtableMScanIteratorTest, test_single_get)
{
  int ret = OB_SUCCESS;
  const ObStoreRow *row = NULL;
  ObStoreRow check_row;
  ObStoreRange range;
  ObObj cells[TEST_COLUMN_CNT];
  ObArray<ObStoreRange> ranges;
  ObArray<ObStoreRange> ranges;
  check_row.row_val_.assign(cells, TEST_COLUMN_CNT);
  ObMockMemtableMScanIterator iter(table_schema_, row_generate_);
  ObMockStoreRowIterator &row_iter = iter.get_store_row_iter();
  row_iter.add_get_range(10);
  ObStoreRow start_row;
  ObStoreRow end_row;
  ObObj start_cells[TEST_COLUMN_CNT];
  ObObj end_cells[TEST_COLUMN_CNT];
  start_row.row_val_.assign(start_cells, TEST_COLUMN_CNT);
  end_row.row_val_.assign(end_cells, TEST_COLUMN_CNT);
  ret = row_generate_.get_next_row(10, start_row);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = row_generate_.get_next_row(10, end_row);
  ASSERT_EQ(OB_SUCCESS, ret);
  range.get_start_key().assign(start_cells, TEST_ROWKEY_COLUMN_CNT);
  range.get_end_key().assign(end_cells, TEST_ROWKEY_COLUMN_CNT);
  range.get_border_flag().set_inclusive_start();
  range.get_border_flag().set_inclusive_end();
  ASSERT_EQ(OB_SUCCESS, ranges.push_back(range));
  ASSERT_EQ(OB_SUCCESS, iter.init(ranges));
  ASSERT_EQ(OB_SUCCESS, iter.get_next_row(row));
  ASSERT_TRUE(const_cast<ObStoreRow *>(row)->row_val_ == start_row.row_val_);
}

TEST_F(ObMemtableMScanIteratorTest, test_multi_get)
{
  int ret = OB_SUCCESS;
  const ObStoreRow *prow = NULL;
  ObStoreRow check_row;
  ObStoreRange range;
  ObObj cells[TEST_COLUMN_CNT];
  ObArray<ObStoreRange> ranges;
  check_row.row_val_.assign(cells, TEST_COLUMN_CNT);
  ObMockMemtableMScanIterator iter(table_schema_, row_generate_);
  ObMockStoreRowIterator &row_iter = iter.get_store_row_iter();
  ObStoreRow start_row;
  ObStoreRow end_row;

  // multi get test
  ObStoreRowkey mget_rowkeys[TEST_MULTI_GET_CNT];
  ObStoreRange mget_ranges[TEST_MULTI_GET_CNT];
  ObObj mget_start_cells[TEST_MULTI_GET_CNT][TEST_COLUMN_CNT];
  ObObj mget_end_cells[TEST_MULTI_GET_CNT][TEST_COLUMN_CNT];
  ObStoreRow mget_start_rows[TEST_MULTI_GET_CNT];
  ObStoreRow mget_end_rows[TEST_MULTI_GET_CNT];
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    mget_start_rows[i].row_val_.assign(mget_start_cells[i], TEST_COLUMN_CNT);
    mget_end_rows[i].row_val_.assign(mget_end_cells[i], TEST_COLUMN_CNT);
    mget_ranges[i].get_start_key().assign(mget_start_cells[i], TEST_ROWKEY_COLUMN_CNT);
    mget_ranges[i].get_end_key().assign(mget_end_cells[i], TEST_ROWKEY_COLUMN_CNT);
    mget_ranges[i].get_border_flag().set_inclusive_start();
    mget_ranges[i].get_border_flag().set_inclusive_end();
    ret = row_generate_.get_next_row(i, mget_start_rows[i]);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = row_generate_.get_next_row(i, mget_end_rows[i]);
    ASSERT_EQ(OB_SUCCESS, ret);
    row_iter.add_get_range(i);
  }

  //larger than 1000 get
  ranges.reuse();
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    ret = ranges.push_back(mget_ranges[i]);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  ret = iter.init(ranges);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    ret = iter.get_next_row(prow);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(mget_start_rows[i].row_val_ == prow->row_val_);
  }
  ret = iter.get_next_row(prow);
  ASSERT_EQ(OB_ITER_END, ret);
}

TEST_F(ObMemtableMScanIteratorTest, test_single_scan)
{
  int ret = OB_SUCCESS;
  const ObStoreRow *row = NULL;
  ObStoreRow check_row;
  ObStoreRange range;
  ObObj cells[TEST_COLUMN_CNT];
  ObArray<ObStoreRange> ranges;
  check_row.row_val_.assign(cells, TEST_COLUMN_CNT);
  ObMockMemtableMScanIterator iter(table_schema_, row_generate_);
  ObMockStoreRowIterator &row_iter = iter.get_store_row_iter();
  row_iter.add_scan_range(0, TEST_MULTI_GET_CNT-1);
  ObStoreRow start_row;
  ObStoreRow end_row;
  ObObj start_cells[TEST_COLUMN_CNT];
  ObObj end_cells[TEST_COLUMN_CNT];
  start_row.row_val_.assign(start_cells, TEST_COLUMN_CNT);
  end_row.row_val_.assign(end_cells, TEST_COLUMN_CNT);
  ret = row_generate_.get_next_row(0, start_row);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = row_generate_.get_next_row(TEST_MULTI_GET_CNT-1, end_row);
  ASSERT_EQ(OB_SUCCESS, ret);
  range.get_start_key().assign(start_cells, TEST_ROWKEY_COLUMN_CNT);
  range.get_end_key().assign(end_cells, TEST_ROWKEY_COLUMN_CNT);
  range.get_border_flag().set_inclusive_start();
  range.get_border_flag().set_inclusive_end();
  ASSERT_EQ(OB_SUCCESS, ranges.push_back(range));
  ASSERT_EQ(OB_SUCCESS, iter.init(ranges));
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    ret = row_generate_.get_next_row(i, check_row);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = iter.get_next_row(row);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(const_cast<ObStoreRow *>(row)->row_val_ == check_row.row_val_);
  }
  ASSERT_EQ(OB_ITER_END, iter.get_next_row(row));
}

TEST_F(ObMemtableMScanIteratorTest, test_multi_scan)
{
  int ret = OB_SUCCESS;
  const ObStoreRow *prow = NULL;
  ObStoreRow check_row;
  ObStoreRange range;
  ObObj cells[TEST_COLUMN_CNT];
  ObArray<ObStoreRange> ranges;
  check_row.row_val_.assign(cells, TEST_COLUMN_CNT);
  ObMockMemtableMScanIterator iter(table_schema_, row_generate_);
  ObMockStoreRowIterator &row_iter = iter.get_store_row_iter();
  ObStoreRow start_row;
  ObStoreRow end_row;
  const int64_t count_per_range = 10;

  // multi get test
  ObStoreRowkey mget_rowkeys[TEST_MULTI_GET_CNT];
  ObStoreRange mget_ranges[TEST_MULTI_GET_CNT];
  ObObj mget_start_cells[TEST_MULTI_GET_CNT][TEST_COLUMN_CNT];
  ObObj mget_end_cells[TEST_MULTI_GET_CNT][TEST_COLUMN_CNT];
  ObStoreRow mget_start_rows[TEST_MULTI_GET_CNT];
  ObStoreRow mget_end_rows[TEST_MULTI_GET_CNT];
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    mget_start_rows[i].row_val_.assign(mget_start_cells[i], TEST_COLUMN_CNT);
    mget_end_rows[i].row_val_.assign(mget_end_cells[i], TEST_COLUMN_CNT);
    mget_ranges[i].get_start_key().assign(mget_start_cells[i], TEST_ROWKEY_COLUMN_CNT);
    mget_ranges[i].get_end_key().assign(mget_end_cells[i], TEST_ROWKEY_COLUMN_CNT);
    mget_ranges[i].get_border_flag().set_inclusive_start();
    mget_ranges[i].get_border_flag().set_inclusive_end();
    ret = row_generate_.get_next_row(i, mget_start_rows[i]);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = row_generate_.get_next_row(i + count_per_range - 1, mget_end_rows[i]);
    ASSERT_EQ(OB_SUCCESS, ret);
    row_iter.add_scan_range(i, i + count_per_range - 1);
  }

  //larger than 1000 get
  ranges.reuse();
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    ret = ranges.push_back(mget_ranges[i]);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  ret = iter.init(ranges);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = 0; i < TEST_MULTI_GET_CNT; ++i) {
    for (int64_t j = 0; j < count_per_range; ++j) {
      ret = row_generate_.get_next_row(i+j, check_row);
      ASSERT_EQ(OB_SUCCESS, ret);
      ret = iter.get_next_row(prow);
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_TRUE(const_cast<ObStoreRow *>(prow)->row_val_ == check_row.row_val_);
    }
  }
  ret = iter.get_next_row(prow);
  ASSERT_EQ(OB_ITER_END, ret);
}

TEST_F(ObMemtableMScanIteratorTest, test_multi_get_with_scan)
{
  for (int64_t i = 0; i < 6; ++i) {
    test_multi_get_with_scan(i);
  }
}

}
}

int main(int argc, char **argv)
{
  system("rm -f test_memtable_iterator.log*");
  OB_LOGGER.set_file_name("test_memtable_iterator.log", true);
  OB_LOGGER.set_log_level("INFO");
  CLOG_LOG(INFO, "begin unittest: test_memtable_iterator");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
