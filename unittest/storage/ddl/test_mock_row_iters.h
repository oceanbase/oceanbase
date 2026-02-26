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
#ifndef OCEANBASE_STORAGE_OB_TEST_MOCK_ROW_ITERS_H_
#define OCEANBASE_STORAGE_OB_TEST_MOCK_ROW_ITERS_H_

#define USING_LOG_PREFIX STORAGE

#include "unittest/storage/ddl/test_batch_rows_generater.h"
#include "storage/ddl/ob_tablet_slice_row_iterator.h"
#include "storage/blocksstable/ob_batch_datum_rows.h"

namespace oceanbase
{
namespace unittest
{

class ObMockRowIter : public ObITabletSliceRowIterator
{
public:
  ObMockRowIter() :
    is_inited_(false),
    tablet_id_(0),
    slice_idx_(-1),
    max_batch_size_(0),
    bdrs_(),
    batch_datum_rows_gen_(),
    test_batch_count_(0),
    row_(),
    is_need_next_batch_(true),
    curr_idx_(0) { }
  int init(const ObTabletID &tablet_id,
           const int64_t slice_idx,
           const int64_t max_batch_size,
           ObObjType *col_obj_types,
           int64_t col_count,
           int64_t rowkey_col_count,
           bool is_storage_layer_row = true,
           const int64_t test_batch_count = 512);
  virtual int get_next_row(const blocksstable::ObDatumRow *&row) override;
  virtual int get_next_batch(const blocksstable::ObBatchDatumRows *&datum_rows) override;
  void set_slice_idx(const int64_t slice_idx)
  {
    slice_idx_ = slice_idx;
  }
  virtual int64_t get_slice_idx() const override
  {
    return slice_idx_;
  }
  virtual ObTabletID get_tablet_id() const override
  {
    return tablet_id_;
  }
  virtual int64_t get_max_batch_size() const override { return max_batch_size_;}

private:
  bool is_inited_;
  ObTabletID tablet_id_;
  int64_t slice_idx_;
  int64_t max_batch_size_;
  blocksstable::ObBatchDatumRows bdrs_;
  ObBatchRowsGen batch_datum_rows_gen_;
  int64_t test_batch_count_;
  blocksstable::ObDatumRow row_;
  bool is_need_next_batch_;
  int64_t curr_idx_;
};

class ObMockIters final : public ObITabletSliceRowIterIterator
{
public:
  ObMockIters() :
      is_inited_(false),
      allocator_(ObMemAttr(MTL_ID(), "ObMockIters")),
      mock_cg_row_iters_(allocator_) { }
  ~ObMockIters() = default;
  int init(const int64_t slice_count,
      const ObTabletID &tablet_id,
      const int64_t slice_idx,
      const int64_t max_batch_size,
      ObObjType *col_obj_types,
      int64_t col_count,
      int64_t rowkey_col_count,
      bool is_storage_layer_row,
      const int64_t test_batch_count);
  virtual int get_next_iter(ObITabletSliceRowIterator *&iter) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObMockIters);

private:
  bool is_inited_;
  ObMalloc allocator_;
  ObList<ObMockRowIter *, ObMalloc> mock_cg_row_iters_;
};

int ObMockRowIter::init(
    const ObTabletID &tablet_id,
    const int64_t slice_idx,
    const int64_t max_batch_size,
    ObObjType *col_obj_types,
    int64_t col_count,
    int64_t rowkey_col_count,
    bool is_storage_layer_row,
    const int64_t test_batch_count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("the ObMockRowIter has been initialized", K(ret));
  } else if (OB_UNLIKELY(!tablet_id.is_valid() ||
                  slice_idx < 0 ||
                  max_batch_size <= 0 ||
                  nullptr == col_obj_types ||
                  col_count <= 0 ||
                  test_batch_count <= 0)) {
    LOG_WARN("there are invalid args",
        K(ret), K(tablet_id), K(slice_idx), K(max_batch_size), KP(col_obj_types), K(col_count), K(test_batch_count));
  } else if (OB_FAIL(batch_datum_rows_gen_.init(col_obj_types, col_count, rowkey_col_count, is_storage_layer_row))) {
    LOG_WARN("fail to initialize batch datum rows gen",
        K(ret), KP(col_obj_types), K(col_count), K(rowkey_col_count), K(is_storage_layer_row));
  } else if (OB_FAIL(row_.init(col_count))) {
    LOG_WARN("fail to initialize row", K(ret), K(col_count));
  } else {
    tablet_id_ = tablet_id;
    slice_idx_ = slice_idx;
    max_batch_size_ = max_batch_size;
    test_batch_count_ = test_batch_count;
    is_inited_ = true;
  }
  return ret;
}

int ObMockRowIter::get_next_row(const blocksstable::ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  const blocksstable::ObBatchDatumRows *datum_rows = nullptr; // not use
  if (is_need_next_batch_ && OB_FAIL(get_next_batch(datum_rows))) {
    LOG_WARN("fail to get next batch", K(ret));
  } else {
    is_need_next_batch_ = false;
  }

  if (FAILEDx(bdrs_.to_datum_row(curr_idx_, row_))) {
    LOG_WARN("fail to convert datum row", K(ret), K(curr_idx_));
  } else {
    if(++curr_idx_ >= bdrs_.row_count_) {
      curr_idx_ = 0;
      is_need_next_batch_ = true;
    }
    row = &row_;
  }
  return ret;
}

int ObMockRowIter::get_next_batch(const blocksstable::ObBatchDatumRows *&datum_rows)
{
  int ret = OB_SUCCESS;
  datum_rows = nullptr;
  bdrs_.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("the ObMockRowIter has not been initialized", K(ret));
  } else if (test_batch_count_-- <= 0) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(batch_datum_rows_gen_.get_batch_rows(bdrs_, max_batch_size_))) {
    LOG_WARN("fail to get batch datum rows", K(ret), K(max_batch_size_));
  } else {
    datum_rows = &bdrs_;
  }
  return ret;
}

int ObMockIters::init(
    const int64_t slice_count,
    const ObTabletID &tablet_id,
    const int64_t slice_idx,
    const int64_t max_batch_size,
    ObObjType *col_obj_types,
    int64_t col_count,
    int64_t rowkey_col_count,
    bool is_storage_layer_row,
    const int64_t test_batch_count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("the ObMockIters has been initialized", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < slice_count; ++i) {
      ObMockRowIter *row_iter = OB_NEWx(ObMockRowIter, &allocator_);
      if (OB_UNLIKELY(nullptr == row_iter)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory ", K(ret));
      } else if (row_iter->init(tablet_id, slice_idx + i, max_batch_size, col_obj_types,
                                col_count, rowkey_col_count, is_storage_layer_row, test_batch_count)) {
        LOG_WARN("fail to initialize row iter", K(ret));
      } else if (OB_FAIL(mock_cg_row_iters_.push_back(row_iter))) {
        LOG_WARN("fail to push back row iter", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObMockIters::get_next_iter(ObITabletSliceRowIterator *&iter)
{
  int ret = OB_SUCCESS;
  iter = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("the ObMockIters has not been initialized", K(ret));
  } else if (mock_cg_row_iters_.empty()) {
    ret = OB_ITER_END;
  } else {
    iter = mock_cg_row_iters_.get_first();
    if (OB_FAIL(mock_cg_row_iters_.pop_front())) {
      LOG_WARN("fail to pop front", K(ret));
    }
  }
  return ret;
}

} // end namespace unittest
} // end namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TEST_MOCK_ROW_ITERS_H_