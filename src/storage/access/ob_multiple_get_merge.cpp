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

#include "ob_multiple_get_merge.h"
#include "storage/ob_row_fuse.h"
#include "storage/ob_storage_schema.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/concurrency_control/ob_data_validation_service.h"

namespace oceanbase
{
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
namespace storage
{
ObMultipleGetMerge::ObMultipleGetMerge()
  : rowkeys_(NULL),
    cow_rowkeys_(),
    get_row_range_idx_(0)
{
  type_ = ObQRIterType::T_MULTI_GET;
}

ObMultipleGetMerge::~ObMultipleGetMerge()
{
  reset();
}

int ObMultipleGetMerge::open(const common::ObIArray<ObDatumRowkey> &rowkeys)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(rowkeys.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to do multi get", K(ret), K(rowkeys));
  } else if (OB_FAIL(ObMultipleMerge::open())) {
    STORAGE_LOG(WARN, "Fail to open ObMultipleMerge, ", K(ret));
  } else {
    rowkeys_ = &rowkeys;
    if (OB_FAIL(construct_iters())) {
      STORAGE_LOG(WARN, "fail to construct iters", K(ret));
    } else {
      get_row_range_idx_ = 0;
      STORAGE_LOG(DEBUG, "Success to open ObMultipleGetMerge", K(rowkeys));
    }
  }

  return ret;
}

void ObMultipleGetMerge::reset()
{
  ObMultipleMerge::reset();
  rowkeys_ = NULL;
  cow_rowkeys_.reset();
  get_row_range_idx_ = 0;
}

void ObMultipleGetMerge::reuse()
{
  ObMultipleMerge::reuse();
  get_row_range_idx_ = 0;
}

void ObMultipleGetMerge::reclaim()
{
  ObMultipleMerge::reclaim();
  rowkeys_ = NULL;
  cow_rowkeys_.reuse();
  get_row_range_idx_ = 0;
}

int ObMultipleGetMerge::prepare()
{
  get_row_range_idx_ = 0;
  return OB_SUCCESS;
}

int ObMultipleGetMerge::calc_scan_range()
{
  int ret = OB_SUCCESS;

  if (!curr_rowkey_.is_valid()) {
    // no row has been iterated
  } else if (OB_ISNULL(rowkeys_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "rowkeys is NULL", K(ret));
  } else {
    ObSEArray<ObDatumRowkey, OB_DEFAULT_MULTI_GET_ROWKEY_NUM> tmp_rowkeys;
    for (int64_t i = 0; i < rowkeys_->count() && OB_SUCC(ret); ++i) {
      if (OB_FAIL(tmp_rowkeys.push_back(rowkeys_->at(i)))) {
        STORAGE_LOG(WARN, "push back rowkey failed", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (rowkeys_ != &cow_rowkeys_) {
        cow_rowkeys_.reset();
        rowkeys_ = &cow_rowkeys_;
      }
    }

    if (OB_SUCC(ret)) {
      int64_t l = curr_scan_index_ + 1;
      int64_t r = tmp_rowkeys.count();

      cow_rowkeys_.reuse();
      range_idx_delta_ += curr_scan_index_ + 1;
      for (int64_t i = l; i < r && OB_SUCC(ret); ++i) {
        if (OB_FAIL(cow_rowkeys_.push_back(tmp_rowkeys.at(i)))) {
          STORAGE_LOG(WARN, "push back rowkey failed", K(ret));
        }
      }
      STORAGE_LOG(DEBUG, "skip rowkeys", K(cow_rowkeys_), K(range_idx_delta_));
    }
  }

  return ret;
}

int ObMultipleGetMerge::is_range_valid() const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(rowkeys_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "rowkeys is null", K(ret));
  } else if (0 == rowkeys_->count()) {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObMultipleGetMerge::construct_iters()
{
  int ret = OB_SUCCESS;
  ObStoreRowIterator *iter = NULL;
  const ObTableIterParam *iter_param = nullptr;
  const int64_t iter_cnt = iters_.count();
  int64_t iter_idx = 0;

  for (int64_t i = tables_.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
    ObITable *table = nullptr;
    if (OB_FAIL(tables_.at(i, table))) {
      STORAGE_LOG(WARN, "fail to get table", K(ret));
    } else {
      if (OB_ISNULL(iter_param = get_actual_iter_param(table))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "fail to get iter param", K(ret), K(i), K(*table));
      } else if (iter_idx >= iter_cnt) {
        if (OB_FAIL(table->multi_get(*iter_param, *access_ctx_, *rowkeys_, iter))) {
          STORAGE_LOG(WARN, "fail to multi get", K(ret));
        } else if (OB_FAIL(iters_.push_back(iter))) {
          iter->~ObStoreRowIterator();
          iter = nullptr;
          STORAGE_LOG(WARN, "fail to push back iter", K(ret));
        }
      } else if (OB_FAIL(iters_.at(iter_idx)->init(*iter_param, *access_ctx_, table, rowkeys_))) {
        STORAGE_LOG(WARN, "fail to init iterator", K(ret));
      }
      ++iter_idx;
    }
  }
  return ret;
}

int ObMultipleGetMerge::inner_get_next_row(ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  bool reach_end = false;
  const ObDatumRow *tmp_row = NULL;
  bool final_result = false;

  if (OB_UNLIKELY(0 == iters_.count())) {
    ret = OB_ITER_END;
  } else {
    while (OB_SUCC(ret)) {
      final_result = false;
      ObDatumRow &fuse_row = row;
      nop_pos_.reset();
      fuse_row.count_ = 0;
      fuse_row.row_flag_.set_flag(ObDmlFlag::DF_NOT_EXIST);
      fuse_row.snapshot_version_ = 0L;

      for (int64_t i = 0; OB_SUCC(ret) && i < iters_.count(); ++i) {
        if (OB_FAIL(iters_[i]->get_next_row(tmp_row))) {
          // check if all iterators return OB_ITER_END
          if (OB_ITER_END == ret && (0 == i || reach_end)) {
            reach_end = true;
            if (i < iters_.count() - 1) {
              ret = OB_SUCCESS;
            }
          } else {
            STORAGE_LOG(WARN, "Iterator get next row failed", K(i), K(ret), K(tables_));
          }
        } else if (OB_ISNULL(tmp_row)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "tmp_row is NULL", K(ret));
        } else {
          // fuse working row and result row
          if (!final_result) {
            if (OB_FAIL(ObRowFuse::fuse_row(*tmp_row, fuse_row, nop_pos_, final_result))) {
              STORAGE_LOG(WARN, "failed to merge rows", K(*tmp_row), K(row), K(ret));
            } else {
              STORAGE_LOG(DEBUG, "fuse row, ", K(*tmp_row), K(fuse_row));
            }
          }
        }
      }
      if (OB_SUCCESS == ret) {
        ++get_row_range_idx_;
        if (fuse_row.row_flag_.is_exist_without_delete()) {
          if (get_row_range_idx_ > rowkeys_->count()) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "unexptected range idx", K(ret), K(get_row_range_idx_), K(rowkeys_->count()));
          } else {
            // find result
            fuse_row.scan_index_ = get_row_range_idx_ - 1;
            fuse_row.group_idx_ = rowkeys_->at(get_row_range_idx_ - 1).get_group_idx();
            STORAGE_LOG(DEBUG, "Success to merge get row, ", KP(this), K(fuse_row), K(get_row_range_idx_), K(fuse_row.group_idx_));
          }
          break;
        } else {
          // When the index lookups the rowkeys from the main table, it should exists
          // and if we find that it does not exist, there must be an anomaly
          if (GCONF.enable_defensive_check()
              && access_ctx_->query_flag_.is_lookup_for_4377()) {
            ret = handle_4377("[index lookup]ObMultipleGetMerge::inner_get_next_row");
            STORAGE_LOG(WARN,"[index lookup] row not found", K(ret),
                        K(rowkeys_),
                        K(get_row_range_idx_ - 1),
                        K(rowkeys_->at(get_row_range_idx_ - 1)),
                        K(fuse_row));
          }
        }
      }
    }
  }
  return ret;
}

}
}
