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

#include "storage/ob_dml_param.h"
#include "storage/ob_range_iterator.h"
#include "lib/container/ob_iarray.h"
#include <gtest/gtest.h>
#include <gmock/gmock.h>

namespace oceanbase {
using namespace common;
using namespace storage;

namespace unittest {

class FakeTableScanParam : public ObTableScanParam {
public:
  FakeTableScanParam(ObOrderType order = ObOrderType::ASC)
  {
    fake_column_orders_.push_back(order);
    column_orders_ = &fake_column_orders_;
  }
  virtual ~FakeTableScanParam()
  {}
  virtual bool is_valid() const
  {
    return true;
  }

private:
  common::ObArray<ObOrderType> fake_column_orders_;
};

int add_get_range(const int64_t val, ObIAllocator& allocator, ObTableScanParam& param)
{
  int ret = OB_SUCCESS;
  ObObj* start_val = NULL;
  ObObj* end_val = NULL;
  if (NULL == (start_val = static_cast<ObObj*>(allocator.alloc(sizeof(ObObj)))) ||
      NULL == (end_val = static_cast<ObObj*>(allocator.alloc(sizeof(ObObj))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "out of memory", K(ret));
  } else {
    start_val->set_int(val);
    end_val->set_int(val);
    ObRowkey start_key(start_val, 1);
    ObRowkey end_key(end_val, 1);
    ObNewRange range;
    range.table_id_ = 3003;
    range.start_key_ = start_key;
    range.end_key_ = end_key;
    range.border_flag_.set_inclusive_start();
    range.border_flag_.set_inclusive_end();
    ret = param.key_ranges_.push_back(range);
  }
  return ret;
}

int add_scan_range(const int64_t s_val, const int64_t e_val, ObIAllocator& allocator, ObTableScanParam& param)
{
  int ret = OB_SUCCESS;
  if (e_val > s_val) {
    ObObj* start_val = NULL;
    ObObj* end_val = NULL;
    if (NULL == (start_val = static_cast<ObObj*>(allocator.alloc(sizeof(ObObj)))) ||
        NULL == (end_val = static_cast<ObObj*>(allocator.alloc(sizeof(ObObj))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "out of memory", K(ret));
    } else {
      start_val->set_int(s_val);
      end_val->set_int(e_val);
      ObRowkey start_key(start_val, 1);
      ObRowkey end_key(end_val, 1);
      ObNewRange range;
      range.table_id_ = 3003;
      range.start_key_ = start_key;
      range.end_key_ = end_key;
      ret = param.key_ranges_.push_back(range);
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
  }
  return ret;
}

void check_get_range(const int64_t val, const ObExtStoreRowkey& rowkey)
{
  ObObj rk_val;
  rk_val.set_int(val);
  ObStoreRowkey rk(&rk_val, 1);
  EXPECT_EQ(rk, rowkey.get_store_rowkey());
}

void check_get_range(const int64_t val, ObBatch& batch)
{
  EXPECT_EQ(ObBatch::T_GET, batch.type_);
  EXPECT_TRUE(NULL != batch.rowkey_);
  check_get_range(val, *batch.rowkey_);
}

void check_multi_get_range(ObIArray<int64_t>& rowkeys, ObBatch& batch)
{
  EXPECT_EQ(ObBatch::T_MULTI_GET, batch.type_);
  EXPECT_TRUE(NULL != batch.rowkeys_);
  EXPECT_EQ(rowkeys.count(), batch.rowkeys_->count());
  for (int64_t i = 0; i < rowkeys.count(); ++i) {
    check_get_range(rowkeys.at(i), batch.rowkeys_->at(i));
  }
}

bool test_equal_store_range(const ObStoreRange& lhs, const ObStoreRange& rhs)
{
  return (rhs.get_table_id() == rhs.get_table_id()) && (lhs.get_start_key().simple_equal(rhs.get_start_key())) &&
         (lhs.get_end_key().simple_equal(rhs.get_end_key())) &&
         (lhs.get_border_flag().get_data() == rhs.get_border_flag().get_data());
}

void check_scan_range(const int64_t s_val, const int64_t e_val, const ObExtStoreRange& single_range)
{
  if (e_val > s_val) {
    ObObj start_val;
    ObObj end_val;
    start_val.set_int(s_val);
    end_val.set_int(e_val);
    ObStoreRowkey start_key(&start_val, 1);
    ObStoreRowkey end_key(&end_val, 1);
    ObStoreRange range;
    range.set_table_id(3003);
    range.set_start_key(start_key);
    range.set_end_key(end_key);
    STORAGE_LOG(INFO, "range", K(single_range), K(range));
    EXPECT_TRUE(test_equal_store_range(single_range.get_range(), range));
  } else {
    BACKTRACE(INFO, true, "backtrace");
    STORAGE_LOG(WARN, "wrong argument(s)", K(s_val), K(e_val));
  }
}

void check_scan_range(const int64_t s_val, const int64_t e_val, ObBatch& batch)
{
  ObObj start_val;
  ObObj end_val;
  start_val.set_int(s_val);
  end_val.set_int(e_val);
  ObStoreRowkey start_key(&start_val, 1);
  ObStoreRowkey end_key(&end_val, 1);
  ObStoreRange range;
  range.set_table_id(3003);
  range.set_start_key(start_key);
  range.set_end_key(end_key);
  EXPECT_EQ(ObBatch::T_SCAN, batch.type_);
  EXPECT_TRUE(NULL != batch.range_);
  EXPECT_TRUE(test_equal_store_range(batch.range_->get_range(), range));
}

void check_multi_scan_range(const ObIArray<int64_t>& s_vals, const ObIArray<int64_t>& e_vals, ObBatch& batch)
{
  EXPECT_EQ(ObBatch::T_MULTI_SCAN, batch.type_);
  ASSERT_TRUE(NULL != batch.ranges_);
  EXPECT_EQ(s_vals.count(), batch.ranges_->count());
  EXPECT_EQ(e_vals.count(), batch.ranges_->count());
  for (int64_t i = 0; i < s_vals.count(); ++i) {
    STORAGE_LOG(INFO, "", "s_val", s_vals.at(i), "e_val", e_vals.at(i));
    if (s_vals.at(i) == e_vals.at(i)) {
      check_get_range(s_vals.at(i), ObExtStoreRowkey(batch.ranges_->at(i).get_range().get_start_key()));
    } else {
      check_scan_range(s_vals.at(i), e_vals.at(i), batch.ranges_->at(i));
    }
  }
}

TEST(ObRowFuseTest, test_empty_range)
{
  ObRangeIterator iter;
  FakeTableScanParam param;
  ObIArray<ObOrderType>* column_orders;
  int64_t rowkey_cnt = 1;
  bool is_whole = false;

  column_orders = param.get_rowkey_column_orders();
  EXPECT_EQ(OB_SUCCESS, iter.set_scan_param(param));
  ObBatch batch;
  EXPECT_EQ(OB_SUCCESS, iter.get_next(batch));
  EXPECT_EQ(ObBatch::T_SCAN, batch.type_);
  EXPECT_EQ(OB_SUCCESS, batch.range_->get_range().is_whole_range(*column_orders, rowkey_cnt, is_whole));
  EXPECT_TRUE(is_whole);
  EXPECT_EQ(OB_ITER_END, iter.get_next(batch));
}

TEST(ObRowFuseTest, test_single_get_range)
{
  ObRangeIterator iter;
  FakeTableScanParam param;
  ModulePageAllocator allocator;
  EXPECT_EQ(OB_SUCCESS, add_get_range(1, allocator, param));
  EXPECT_EQ(OB_SUCCESS, iter.set_scan_param(param));
  ObBatch batch;
  EXPECT_EQ(OB_SUCCESS, iter.get_next(batch));
  check_get_range(1, batch);
  EXPECT_EQ(OB_ITER_END, iter.get_next(batch));

  // test short path of single get
  iter.reset();
  param.is_get_ = true;
  EXPECT_EQ(OB_SUCCESS, iter.set_scan_param(param));
  EXPECT_EQ(OB_SUCCESS, iter.get_next(batch));
  check_get_range(1, batch);
  EXPECT_EQ(OB_ITER_END, iter.get_next(batch));
}

TEST(ObRowFuseTest, test_single_get_range_desc)
{
  ObRangeIterator iter;
  FakeTableScanParam param(ObOrderType::DESC);
  ModulePageAllocator allocator;
  EXPECT_EQ(OB_SUCCESS, add_get_range(1, allocator, param));
  EXPECT_EQ(OB_SUCCESS, iter.set_scan_param(param));
  ObBatch batch;
  EXPECT_EQ(OB_SUCCESS, iter.get_next(batch));
  check_get_range(1, batch);
  EXPECT_EQ(OB_ITER_END, iter.get_next(batch));

  // test short path of single get
  iter.reset();
  param.is_get_ = true;
  EXPECT_EQ(OB_SUCCESS, iter.set_scan_param(param));
  EXPECT_EQ(OB_SUCCESS, iter.get_next(batch));
  check_get_range(1, batch);
  EXPECT_EQ(OB_ITER_END, iter.get_next(batch));
}

TEST(ObRowFuseTest, test_single_scan_range)
{
  ObRangeIterator iter;
  FakeTableScanParam param;
  ModulePageAllocator allocator;
  EXPECT_EQ(OB_SUCCESS, add_scan_range(1, 9, allocator, param));
  EXPECT_EQ(OB_SUCCESS, iter.set_scan_param(param));
  ObBatch batch;
  EXPECT_EQ(OB_SUCCESS, iter.get_next(batch));
  check_scan_range(1, 9, batch);
  EXPECT_EQ(OB_ITER_END, iter.get_next(batch));
}

TEST(ObRowFuseTest, test_single_scan_range_desc)
{
  ObRangeIterator iter;
  FakeTableScanParam param(ObOrderType::DESC);
  ModulePageAllocator allocator;
  EXPECT_EQ(OB_SUCCESS, add_scan_range(1, 9, allocator, param));
  EXPECT_EQ(OB_SUCCESS, iter.set_scan_param(param));
  ObBatch batch;
  EXPECT_EQ(OB_SUCCESS, iter.get_next(batch));
  check_scan_range(9, 1, batch);
  EXPECT_EQ(OB_ITER_END, iter.get_next(batch));
}

TEST(ObRowFuseTest, test_multi_get_range)
{
  ObRangeIterator iter;
  FakeTableScanParam param;
  ModulePageAllocator allocator;
  EXPECT_EQ(OB_SUCCESS, add_get_range(1, allocator, param));
  EXPECT_EQ(OB_SUCCESS, add_get_range(3, allocator, param));
  EXPECT_EQ(OB_SUCCESS, add_get_range(5, allocator, param));
  EXPECT_EQ(OB_SUCCESS, add_get_range(7, allocator, param));
  EXPECT_EQ(OB_SUCCESS, add_get_range(9, allocator, param));
  EXPECT_EQ(OB_SUCCESS, iter.set_scan_param(param));
  ObBatch batch;
  EXPECT_EQ(OB_SUCCESS, iter.get_next(batch));
  ObSEArray<int64_t, 64> rowkeys;
  rowkeys.push_back(1);
  rowkeys.push_back(3);
  rowkeys.push_back(5);
  rowkeys.push_back(7);
  rowkeys.push_back(9);
  check_multi_get_range(rowkeys, batch);
  EXPECT_EQ(OB_ITER_END, iter.get_next(batch));

  // test short path of multi get
  iter.reset();
  param.is_get_ = true;
  EXPECT_EQ(OB_SUCCESS, iter.set_scan_param(param));
  EXPECT_EQ(OB_SUCCESS, iter.get_next(batch));
  check_multi_get_range(rowkeys, batch);
  EXPECT_EQ(OB_ITER_END, iter.get_next(batch));
}

TEST(ObRowFuseTest, test_multi_get_unordered)
{
  ObRangeIterator iter;
  FakeTableScanParam param;
  ModulePageAllocator allocator;
  param.scan_flag_.scan_order_ = ObQueryFlag::Forward;
  EXPECT_EQ(OB_SUCCESS, add_get_range(1, allocator, param));
  EXPECT_EQ(OB_SUCCESS, add_get_range(7, allocator, param));
  EXPECT_EQ(OB_SUCCESS, add_get_range(5, allocator, param));
  EXPECT_EQ(OB_SUCCESS, add_get_range(9, allocator, param));
  EXPECT_EQ(OB_SUCCESS, add_get_range(3, allocator, param));
  EXPECT_EQ(OB_SUCCESS, iter.set_scan_param(param));
  ObBatch batch;
  EXPECT_EQ(OB_SUCCESS, iter.get_next(batch));
  ObSEArray<int64_t, 64> rowkeys;
  rowkeys.push_back(1);
  rowkeys.push_back(3);
  rowkeys.push_back(5);
  rowkeys.push_back(7);
  rowkeys.push_back(9);
  check_multi_get_range(rowkeys, batch);
  EXPECT_EQ(OB_ITER_END, iter.get_next(batch));

  // test short path of multi get
  iter.reset();
  param.is_get_ = true;
  EXPECT_EQ(OB_SUCCESS, iter.set_scan_param(param));
  EXPECT_EQ(OB_SUCCESS, iter.get_next(batch));
  check_multi_get_range(rowkeys, batch);
  EXPECT_EQ(OB_ITER_END, iter.get_next(batch));
}

TEST(ObRowFuseTest, test_multi_get_range_desc)
{
  ObRangeIterator iter;
  FakeTableScanParam param(ObOrderType::DESC);
  ModulePageAllocator allocator;
  EXPECT_EQ(OB_SUCCESS, add_get_range(1, allocator, param));
  EXPECT_EQ(OB_SUCCESS, add_get_range(3, allocator, param));
  EXPECT_EQ(OB_SUCCESS, add_get_range(5, allocator, param));
  EXPECT_EQ(OB_SUCCESS, add_get_range(7, allocator, param));
  EXPECT_EQ(OB_SUCCESS, add_get_range(9, allocator, param));
  EXPECT_EQ(OB_SUCCESS, iter.set_scan_param(param));
  ObBatch batch;
  EXPECT_EQ(OB_SUCCESS, iter.get_next(batch));
  ObSEArray<int64_t, 64> rowkeys;
  rowkeys.push_back(1);
  rowkeys.push_back(3);
  rowkeys.push_back(5);
  rowkeys.push_back(7);
  rowkeys.push_back(9);
  check_multi_get_range(rowkeys, batch);
  EXPECT_EQ(OB_ITER_END, iter.get_next(batch));

  // test short path of multi get
  iter.reset();
  param.is_get_ = true;
  EXPECT_EQ(OB_SUCCESS, iter.set_scan_param(param));
  EXPECT_EQ(OB_SUCCESS, iter.get_next(batch));
  check_multi_get_range(rowkeys, batch);
  EXPECT_EQ(OB_ITER_END, iter.get_next(batch));
}

TEST(ObRowFuseTest, test_single_get_perf)
{
  ObRangeIterator iter;
  FakeTableScanParam param;
  ModulePageAllocator allocator;
  EXPECT_EQ(OB_SUCCESS, add_get_range(1, allocator, param));
  ObBatch batch;

  const int64_t perf_count = 1000L * 1000L;
  int64_t begin_time = 0;
  int64_t end_time = 0;
  int64_t cost_time = 0;

  // perf single get without is_get param
  begin_time = ObTimeUtility::current_time();
  for (int64_t i = 0; i < perf_count; ++i) {
    iter.reset();
    EXPECT_EQ(OB_SUCCESS, iter.set_scan_param(param));
    EXPECT_EQ(OB_SUCCESS, iter.get_next(batch));
    check_get_range(1, batch);
  }
  end_time = ObTimeUtility::current_time();
  cost_time = end_time - begin_time;
  STORAGE_LOG(INFO, "perf single get without is_get", K(perf_count), K(cost_time));

  // perf single get with is_get param
  param.is_get_ = true;
  begin_time = ObTimeUtility::current_time();
  for (int64_t i = 0; i < perf_count; ++i) {
    iter.reset();
    EXPECT_EQ(OB_SUCCESS, iter.set_scan_param(param));
    EXPECT_EQ(OB_SUCCESS, iter.get_next(batch));
    check_get_range(1, batch);
  }
  end_time = ObTimeUtility::current_time();
  cost_time = end_time - begin_time;
  STORAGE_LOG(INFO, "perf single get with is_get", K(perf_count), K(cost_time));
}

TEST(ObRowFuseTest, test_multi_get_perf)
{
  ObRangeIterator iter;
  FakeTableScanParam param;
  param.scan_flag_.scan_order_ = ObQueryFlag::Forward;
  ModulePageAllocator allocator;
  ObBatch batch;
  EXPECT_EQ(OB_SUCCESS, add_get_range(1, allocator, param));
  EXPECT_EQ(OB_SUCCESS, add_get_range(3, allocator, param));
  EXPECT_EQ(OB_SUCCESS, add_get_range(5, allocator, param));
  EXPECT_EQ(OB_SUCCESS, add_get_range(7, allocator, param));
  EXPECT_EQ(OB_SUCCESS, add_get_range(9, allocator, param));
  ObSEArray<int64_t, 64> rowkeys;
  rowkeys.push_back(1);
  rowkeys.push_back(3);
  rowkeys.push_back(5);
  rowkeys.push_back(7);
  rowkeys.push_back(9);

  const int64_t perf_count = 200L * 1000L;
  int64_t begin_time = 0;
  int64_t end_time = 0;
  int64_t cost_time = 0;

  // perf multi get without is_get param
  begin_time = ObTimeUtility::current_time();
  for (int64_t i = 0; i < perf_count; ++i) {
    iter.reset();
    EXPECT_EQ(OB_SUCCESS, iter.set_scan_param(param));
    EXPECT_EQ(OB_SUCCESS, iter.get_next(batch));
    check_multi_get_range(rowkeys, batch);
  }
  end_time = ObTimeUtility::current_time();
  cost_time = end_time - begin_time;
  STORAGE_LOG(INFO, "perf multi get without is_get", K(perf_count), K(cost_time));

  // perf multi get with is_get param
  param.is_get_ = true;
  begin_time = ObTimeUtility::current_time();
  for (int64_t i = 0; i < perf_count; ++i) {
    iter.reset();
    EXPECT_EQ(OB_SUCCESS, iter.set_scan_param(param));
    EXPECT_EQ(OB_SUCCESS, iter.get_next(batch));
    check_multi_get_range(rowkeys, batch);
  }
  end_time = ObTimeUtility::current_time();
  cost_time = end_time - begin_time;
  STORAGE_LOG(INFO, "perf multi get with is_get", K(perf_count), K(cost_time));
}

TEST(ObRowFuseTest, test_multi_range_forward)
{
  ObRangeIterator iter;
  FakeTableScanParam param;
  ModulePageAllocator allocator;
  EXPECT_EQ(OB_SUCCESS, add_get_range(1, allocator, param));
  EXPECT_EQ(OB_SUCCESS, add_get_range(7, allocator, param));
  EXPECT_EQ(OB_SUCCESS, add_get_range(5, allocator, param));
  EXPECT_EQ(OB_SUCCESS, add_get_range(6, allocator, param));
  EXPECT_EQ(OB_SUCCESS, add_scan_range(2, 3, allocator, param));
  EXPECT_EQ(OB_SUCCESS, add_scan_range(11, 13, allocator, param));
  ObQueryFlag flag(ObQueryFlag::Forward, false, false, true, false, false, false);
  param.scan_flag_ = flag;
  EXPECT_EQ(OB_SUCCESS, iter.set_scan_param(param));
  ObBatch batch;
  ObSEArray<int64_t, 64> s_vals;
  ObSEArray<int64_t, 64> e_vals;
  s_vals.push_back(1);
  s_vals.push_back(2);
  s_vals.push_back(5);
  s_vals.push_back(6);
  s_vals.push_back(7);
  s_vals.push_back(11);
  e_vals.push_back(1);
  e_vals.push_back(3);
  e_vals.push_back(5);
  e_vals.push_back(6);
  e_vals.push_back(7);
  e_vals.push_back(13);
  EXPECT_EQ(OB_SUCCESS, iter.get_next(batch));
  check_multi_scan_range(s_vals, e_vals, batch);
}

// When stored in descending order, forward scan should get large value first
TEST(ObRowFuseTest, test_multi_range_forward_desc)
{
  ObRangeIterator iter;
  FakeTableScanParam param(ObOrderType::DESC);
  ModulePageAllocator allocator;
  EXPECT_EQ(OB_SUCCESS, add_get_range(1, allocator, param));
  EXPECT_EQ(OB_SUCCESS, add_get_range(7, allocator, param));
  EXPECT_EQ(OB_SUCCESS, add_get_range(5, allocator, param));
  EXPECT_EQ(OB_SUCCESS, add_get_range(6, allocator, param));
  EXPECT_EQ(OB_SUCCESS, add_scan_range(2, 3, allocator, param));
  EXPECT_EQ(OB_SUCCESS, add_scan_range(11, 13, allocator, param));
  ObQueryFlag flag(ObQueryFlag::Forward, false, false, true, false, false, false);
  param.scan_flag_ = flag;
  EXPECT_EQ(OB_SUCCESS, iter.set_scan_param(param));
  ObBatch batch;
  ObSEArray<int64_t, 64> s_vals;
  ObSEArray<int64_t, 64> e_vals;
  s_vals.push_back(13);
  s_vals.push_back(7);
  s_vals.push_back(6);
  s_vals.push_back(5);
  s_vals.push_back(3);
  s_vals.push_back(1);
  e_vals.push_back(11);
  e_vals.push_back(7);
  e_vals.push_back(6);
  e_vals.push_back(5);
  e_vals.push_back(2);
  e_vals.push_back(1);
  EXPECT_EQ(OB_SUCCESS, iter.get_next(batch));
  check_multi_scan_range(s_vals, e_vals, batch);
}

TEST(ObRowFuseTest, test_multi_range_backward)
{
  ObRangeIterator iter;
  FakeTableScanParam param;
  ModulePageAllocator allocator;
  EXPECT_EQ(OB_SUCCESS, add_get_range(1, allocator, param));
  EXPECT_EQ(OB_SUCCESS, add_get_range(7, allocator, param));
  EXPECT_EQ(OB_SUCCESS, add_get_range(5, allocator, param));
  EXPECT_EQ(OB_SUCCESS, add_get_range(6, allocator, param));
  EXPECT_EQ(OB_SUCCESS, add_scan_range(2, 3, allocator, param));
  EXPECT_EQ(OB_SUCCESS, add_scan_range(11, 13, allocator, param));
  ObQueryFlag flag(ObQueryFlag::Reverse, false, false, true, false, false, false);
  param.scan_flag_ = flag;
  EXPECT_EQ(OB_SUCCESS, iter.set_scan_param(param));
  ObBatch batch;
  ObSEArray<int64_t, 64> s_vals;
  ObSEArray<int64_t, 64> e_vals;
  s_vals.push_back(11);
  s_vals.push_back(7);
  s_vals.push_back(6);
  s_vals.push_back(5);
  s_vals.push_back(2);
  s_vals.push_back(1);
  e_vals.push_back(13);
  e_vals.push_back(7);
  e_vals.push_back(6);
  e_vals.push_back(5);
  e_vals.push_back(3);
  e_vals.push_back(1);
  EXPECT_EQ(OB_SUCCESS, iter.get_next(batch));
  check_multi_scan_range(s_vals, e_vals, batch);
  EXPECT_EQ(OB_ITER_END, iter.get_next(batch));
}

TEST(ObRowFuseTest, test_multi_range_backward_desc)
{
  ObRangeIterator iter;
  FakeTableScanParam param(ObOrderType::DESC);
  ModulePageAllocator allocator;
  EXPECT_EQ(OB_SUCCESS, add_get_range(1, allocator, param));
  EXPECT_EQ(OB_SUCCESS, add_get_range(7, allocator, param));
  EXPECT_EQ(OB_SUCCESS, add_get_range(5, allocator, param));
  EXPECT_EQ(OB_SUCCESS, add_get_range(6, allocator, param));
  EXPECT_EQ(OB_SUCCESS, add_scan_range(2, 3, allocator, param));
  EXPECT_EQ(OB_SUCCESS, add_scan_range(11, 13, allocator, param));
  ObQueryFlag flag(ObQueryFlag::Reverse, false, false, true, false, false, false);
  param.scan_flag_ = flag;
  EXPECT_EQ(OB_SUCCESS, iter.set_scan_param(param));
  ObBatch batch;
  ObSEArray<int64_t, 64> s_vals;
  ObSEArray<int64_t, 64> e_vals;
  s_vals.push_back(1);
  s_vals.push_back(3);
  s_vals.push_back(5);
  s_vals.push_back(6);
  s_vals.push_back(8);
  s_vals.push_back(13);
  e_vals.push_back(1);
  e_vals.push_back(2);
  e_vals.push_back(5);
  e_vals.push_back(6);
  e_vals.push_back(7);
  e_vals.push_back(11);
  EXPECT_EQ(OB_SUCCESS, iter.get_next(batch));
  check_multi_scan_range(s_vals, e_vals, batch);
  EXPECT_EQ(OB_ITER_END, iter.get_next(batch));
}

TEST(ObRowFuseTest, test_multi_range_default)
{
  ObRangeIterator iter;
  FakeTableScanParam param;
  ModulePageAllocator allocator;
  EXPECT_EQ(OB_SUCCESS, add_get_range(1, allocator, param));
  EXPECT_EQ(OB_SUCCESS, add_scan_range(2, 3, allocator, param));
  EXPECT_EQ(OB_SUCCESS, add_get_range(7, allocator, param));
  EXPECT_EQ(OB_SUCCESS, add_get_range(5, allocator, param));
  EXPECT_EQ(OB_SUCCESS, add_get_range(6, allocator, param));
  EXPECT_EQ(OB_SUCCESS, add_scan_range(11, 13, allocator, param));
  EXPECT_EQ(OB_SUCCESS, iter.set_scan_param(param));
  ObBatch batch;
  EXPECT_EQ(OB_SUCCESS, iter.get_next(batch));
  ObSEArray<int64_t, 64> s_vals;
  ObSEArray<int64_t, 64> e_vals;
  s_vals.push_back(1);
  s_vals.push_back(2);
  s_vals.push_back(7);
  s_vals.push_back(5);
  s_vals.push_back(6);
  s_vals.push_back(11);
  e_vals.push_back(1);
  e_vals.push_back(3);
  e_vals.push_back(7);
  e_vals.push_back(5);
  e_vals.push_back(6);
  e_vals.push_back(13);
  check_multi_scan_range(s_vals, e_vals, batch);
  EXPECT_EQ(OB_ITER_END, iter.get_next(batch));
}

TEST(ObRowFuseTest, test_multi_range_default_desc)
{
  ObRangeIterator iter;
  FakeTableScanParam param(ObOrderType::DESC);
  ModulePageAllocator allocator;
  EXPECT_EQ(OB_SUCCESS, add_get_range(1, allocator, param));
  EXPECT_EQ(OB_SUCCESS, add_scan_range(2, 3, allocator, param));
  EXPECT_EQ(OB_SUCCESS, add_get_range(7, allocator, param));
  EXPECT_EQ(OB_SUCCESS, add_get_range(5, allocator, param));
  EXPECT_EQ(OB_SUCCESS, add_get_range(6, allocator, param));
  EXPECT_EQ(OB_SUCCESS, add_scan_range(11, 13, allocator, param));
  EXPECT_EQ(OB_SUCCESS, iter.set_scan_param(param));
  ObBatch batch;
  EXPECT_EQ(OB_SUCCESS, iter.get_next(batch));
  ObSEArray<int64_t, 64> s_vals;
  ObSEArray<int64_t, 64> e_vals;
  s_vals.push_back(1);
  s_vals.push_back(3);
  s_vals.push_back(7);
  s_vals.push_back(5);
  s_vals.push_back(6);
  s_vals.push_back(13);
  e_vals.push_back(1);
  e_vals.push_back(2);
  e_vals.push_back(7);
  e_vals.push_back(5);
  e_vals.push_back(6);
  e_vals.push_back(11);
  check_multi_scan_range(s_vals, e_vals, batch);
  EXPECT_EQ(OB_ITER_END, iter.get_next(batch));
}

TEST(ObRowFuseTest, test_multi_range_keep_order)
{
  ObRangeIterator iter;
  FakeTableScanParam param;
  ModulePageAllocator allocator;
  EXPECT_EQ(OB_SUCCESS, add_get_range(1, allocator, param));
  EXPECT_EQ(OB_SUCCESS, add_scan_range(11, 13, allocator, param));
  EXPECT_EQ(OB_SUCCESS, add_get_range(7, allocator, param));
  EXPECT_EQ(OB_SUCCESS, add_get_range(5, allocator, param));
  EXPECT_EQ(OB_SUCCESS, add_get_range(6, allocator, param));
  EXPECT_EQ(OB_SUCCESS, add_scan_range(2, 3, allocator, param));
  ObQueryFlag flag(ObQueryFlag::KeepOrder, false, false, true, false, false, false);
  param.scan_flag_ = flag;
  EXPECT_EQ(OB_SUCCESS, iter.set_scan_param(param));
  ObBatch batch;
  EXPECT_EQ(OB_SUCCESS, iter.get_next(batch));
  ObSEArray<int64_t, 64> s_vals;
  ObSEArray<int64_t, 64> e_vals;
  s_vals.push_back(1);
  s_vals.push_back(13);
  s_vals.push_back(7);
  s_vals.push_back(5);
  s_vals.push_back(6);
  s_vals.push_back(3);
  e_vals.push_back(1);
  e_vals.push_back(11);
  e_vals.push_back(7);
  e_vals.push_back(5);
  e_vals.push_back(6);
  e_vals.push_back(2);
  check_multi_scan_range(s_vals, e_vals, batch);
  EXPECT_EQ(OB_ITER_END, iter.get_next(batch));
}

TEST(ObRowFuseTest, test_single_scan_range2)
{
  ObRangeIterator iter;
  FakeTableScanParam param;
  ModulePageAllocator allocator;
  EXPECT_EQ(OB_SUCCESS, add_scan_range(1, 9, allocator, param));
  ObQueryFlag flag(ObQueryFlag::Reverse, false, false, true, false, false, false);
  EXPECT_EQ(OB_SUCCESS, iter.set_scan_param(param));
  ObBatch batch;
  EXPECT_EQ(OB_SUCCESS, iter.get_next(batch));
  check_scan_range(1, 9, batch);
  EXPECT_EQ(OB_ITER_END, iter.get_next(batch));
}

TEST(ObRowFuseTest, test_single_scan_range2_desc)
{
  ObRangeIterator iter;
  FakeTableScanParam param(ObOrderType::DESC);
  ModulePageAllocator allocator;
  EXPECT_EQ(OB_SUCCESS, add_scan_range(1, 9, allocator, param));
  ObQueryFlag flag(ObQueryFlag::Reverse, false, false, true, false, false, false);
  EXPECT_EQ(OB_SUCCESS, iter.set_scan_param(param));
  ObBatch batch;
  EXPECT_EQ(OB_SUCCESS, iter.get_next(batch));
  check_scan_range(9, 1, batch);
  EXPECT_EQ(OB_ITER_END, iter.get_next(batch));
}

TEST(ObRowFuseTest, test_single_scan_range3)
{
  ObRangeIterator iter;
  FakeTableScanParam param;
  ModulePageAllocator allocator;
  EXPECT_EQ(OB_SUCCESS, add_scan_range(1, 9, allocator, param));
  ObQueryFlag flag(ObQueryFlag::Forward, false, false, true, false, false, false);
  param.scan_flag_ = flag;
  EXPECT_EQ(OB_SUCCESS, iter.set_scan_param(param));
  ObBatch batch;
  EXPECT_EQ(OB_SUCCESS, iter.get_next(batch));
  check_scan_range(1, 9, batch);
  EXPECT_EQ(OB_ITER_END, iter.get_next(batch));
}

TEST(ObRowFuseTest, test_single_scan_range3_desc)
{
  ObRangeIterator iter;
  FakeTableScanParam param(ObOrderType::DESC);
  ModulePageAllocator allocator;
  EXPECT_EQ(OB_SUCCESS, add_scan_range(1, 9, allocator, param));
  ObQueryFlag flag(ObQueryFlag::Forward, false, false, true, false, false, false);
  param.scan_flag_ = flag;
  EXPECT_EQ(OB_SUCCESS, iter.set_scan_param(param));
  ObBatch batch;
  EXPECT_EQ(OB_SUCCESS, iter.get_next(batch));
  check_scan_range(9, 1, batch);
  EXPECT_EQ(OB_ITER_END, iter.get_next(batch));
}

TEST(ObRowFuseTest, test_single_scan_range4)
{
  ObRangeIterator iter;
  FakeTableScanParam param;
  ModulePageAllocator allocator;
  EXPECT_EQ(OB_SUCCESS, add_scan_range(1, 9, allocator, param));
  ObQueryFlag flag(ObQueryFlag::Forward, false, false, true, false, false, false);
  param.scan_flag_ = flag;
  EXPECT_EQ(OB_SUCCESS, iter.set_scan_param(param));
}

TEST(ObRowFuseTest, test_get_range_array_idx)
{
  ObRangeIterator iter;
  FakeTableScanParam param;
  ModulePageAllocator allocator;
  ObBatch batch;
  EXPECT_EQ(OB_SUCCESS, add_get_range(1, allocator, param));
  EXPECT_EQ(OB_SUCCESS, add_get_range(3, allocator, param));
  EXPECT_EQ(OB_SUCCESS, add_get_range(5, allocator, param));
  EXPECT_EQ(OB_SUCCESS, add_get_range(7, allocator, param));
  EXPECT_EQ(OB_SUCCESS, add_get_range(9, allocator, param));
  param.range_array_pos_.push_back(0);
  param.range_array_pos_.push_back(3);
  param.range_array_pos_.push_back(4);
  EXPECT_EQ(OB_SUCCESS, iter.set_scan_param(param));
  EXPECT_EQ(OB_SUCCESS, iter.get_next(batch));
  ASSERT_EQ(batch.type_, ObBatch::T_MULTI_GET);
  ObSEArray<int64_t, 64> range_array_idx;
  for (int64_t i = 0; i < 5; ++i) {
    int64_t range_idx = batch.rowkeys_->at(i).get_range_array_idx();
    ASSERT_EQ(OB_SUCCESS, range_array_idx.push_back(range_idx));
  }
  EXPECT_EQ(OB_ITER_END, iter.get_next(batch));

  // check range_idx
  EXPECT_EQ(range_array_idx[0], 0);
  EXPECT_EQ(range_array_idx[1], 1);
  EXPECT_EQ(range_array_idx[2], 1);
  EXPECT_EQ(range_array_idx[3], 1);
  EXPECT_EQ(range_array_idx[4], 2);

  // test short path of multi get
  iter.reset();
  param.is_get_ = true;
  EXPECT_EQ(OB_SUCCESS, iter.set_scan_param(param));
  EXPECT_EQ(OB_SUCCESS, iter.get_next(batch));
  for (int64_t i = 0; i < 5; ++i) {
    int64_t range_idx = batch.rowkeys_->at(i).get_range_array_idx();
    EXPECT_EQ(range_idx, range_array_idx.at(i));
  }
  EXPECT_EQ(OB_ITER_END, iter.get_next(batch));
}

}  // namespace unittest
}  // namespace oceanbase

int main(int argc, char** argv)
{
  system("rm -f test_range_iterator.log*");
  OB_LOGGER.set_file_name("test_range_iterator.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  STORAGE_LOG(INFO, "begin unittest: test_row_fuse");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
