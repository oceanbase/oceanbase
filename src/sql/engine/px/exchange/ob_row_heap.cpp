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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/px/exchange/ob_row_heap.h"
#include "common/row/ob_row.h"


using namespace oceanbase::common;
using namespace oceanbase::sql;


int ObRowComparer::init(const common::ObIArray<ObSortColumn> &columns,
                        const common::ObIArray<const ObNewRow *> &rows)
{
  columns_ = &columns;
  rows_ = &rows;
  return OB_SUCCESS;
}

bool ObRowComparer::operator()(int64_t row_idx1, int64_t row_idx2)
{
  int &ret = ret_;
  int cmp = 0;
  bool cmp_ret = false;
  if (OB_FAIL(ret)) {
    // do nothing if we already have an error,
    // so we can finish the sort process ASAP.
  } else if (OB_ISNULL(columns_) || OB_ISNULL(rows_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("columns or rows is NULL", K(ret), K_(columns), K_(rows));
  } else if (OB_UNLIKELY(!(0 <= row_idx1 && row_idx1 < rows_->count() &&
                           0 <= row_idx2 && row_idx2 < rows_->count()))) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("row idx out of range", K(ret), K(row_idx1), K(row_idx2), K(rows_->count()));
  } else {
    const ObNewRow *row1 = rows_->at(row_idx1);
    const ObNewRow *row2 = rows_->at(row_idx2);
    if (OB_UNLIKELY(OB_ISNULL(row1) || OB_ISNULL(row2))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("row should not be null", K(row_idx1), K(row_idx2), "total", rows_->count(), K(ret));
    }
    ObCompareCtx cmp_ctx;
    cmp_ctx.cmp_type_ = ObMaxType;
    cmp_ctx.cmp_cs_type_ = CS_TYPE_INVALID;
    cmp_ctx.is_null_safe_ = true;
    cmp_ctx.tz_off_ = INVALID_TZ_OFF;
    for (int64_t i = 0; OB_SUCC(ret) && 0 == cmp && i < columns_->count(); i++) {
      int64_t col_idx = columns_->at(i).index_;
      if (OB_UNLIKELY(!(0 <= col_idx && col_idx < row1->count_ && col_idx < row2->count_))) {
        ret = OB_INDEX_OUT_OF_RANGE;
        LOG_WARN("row idx out of range", K(ret), K(col_idx), K(row1->count_), K(row2->count_));
      } else {
        cmp_ctx.cmp_cs_type_ = columns_->at(i).cs_type_;
        cmp_ctx.null_pos_ = columns_->at(i).get_cmp_null_pos();
        cmp = row1->cells_[col_idx].compare(row2->cells_[col_idx], cmp_ctx);
        if (cmp < 0) {
          cmp_ret = !columns_->at(i).is_ascending();
        } else if (cmp > 0) {
          cmp_ret = columns_->at(i).is_ascending();
        }
      }
    }
  }
  return cmp_ret;
}

/************************************* ObDatumRowCompare *********************************/
ObDatumRowCompare::ObDatumRowCompare()
  : ret_(OB_SUCCESS), sort_collations_(nullptr), sort_cmp_funs_(nullptr), rows_(nullptr)
{
}

int ObDatumRowCompare::init(
    const ObIArray<ObSortFieldCollation> *sort_collations,
    const ObIArray<ObSortCmpFunc> *sort_cmp_funs,
    const common::ObIArray<const ObChunkDatumStore::StoredRow*> &rows)
{
  int ret = OB_SUCCESS;
  if (nullptr == sort_collations || nullptr == sort_cmp_funs) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(sort_collations), KP(sort_cmp_funs));
  } else if (sort_cmp_funs->count() != sort_cmp_funs->count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("column count miss match", K(ret),
      K(sort_cmp_funs->count()), K(sort_cmp_funs->count()));
  } else {
    sort_collations_ = sort_collations;
    sort_cmp_funs_ = sort_cmp_funs;
    rows_ = &rows;
  }
  return ret;
}

bool ObDatumRowCompare::operator()(
  int64_t l_idx,
  int64_t r_idx)
{
  bool cmp_ret = false;
  int &ret = ret_;
  const ObChunkDatumStore::StoredRow *l = rows_->at(l_idx);
  const ObChunkDatumStore::StoredRow *r = rows_->at(r_idx);
  if (OB_UNLIKELY(OB_SUCCESS != ret)) {
    // already fail
  } else if (!is_inited() || OB_ISNULL(l) || OB_ISNULL(r)) {
    ret = !is_inited() ? OB_NOT_INIT : OB_INVALID_ARGUMENT;
    LOG_WARN("not init or invalid argument", K(ret), KP(l), KP(r));
  } else {
    const ObDatum *lcells = l->cells();
    const ObDatum *rcells = r->cells();
    int cmp = 0;
    for (int64_t i = 0; OB_SUCC(ret) && 0 == cmp && i < sort_cmp_funs_->count(); i++) {
      const int64_t idx = sort_collations_->at(i).field_idx_;
      if (OB_FAIL(sort_cmp_funs_->at(i).cmp_func_(lcells[idx], rcells[idx], cmp))) {
        LOG_WARN("failed to compare", K(ret));
      } else if (cmp < 0) {
        cmp_ret = !sort_collations_->at(i).is_ascending_;
      } else if (cmp > 0) {
        cmp_ret = sort_collations_->at(i).is_ascending_;
      }
    }
  }
  return cmp_ret;
}

/************************************* ObMaxDatumRowCompare *********************************/
ObMaxDatumRowCompare::ObMaxDatumRowCompare()
  : ret_(OB_SUCCESS), sort_collations_(nullptr), sort_cmp_funs_(nullptr), rows_(nullptr)
{
}

int ObMaxDatumRowCompare::init(
    const ObIArray<ObSortFieldCollation> *sort_collations,
    const ObIArray<ObSortCmpFunc> *sort_cmp_funs,
    const common::ObIArray<const ObChunkDatumStore::LastStoredRow*> &rows)
{
  int ret = OB_SUCCESS;
  if (nullptr == sort_collations || nullptr == sort_cmp_funs) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(sort_collations), KP(sort_cmp_funs));
  } else if (sort_cmp_funs->count() != sort_cmp_funs->count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("column count miss match", K(ret),
      K(sort_cmp_funs->count()), K(sort_cmp_funs->count()));
  } else {
    sort_collations_ = sort_collations;
    sort_cmp_funs_ = sort_cmp_funs;
    rows_ = &rows;
  }
  return ret;
}

bool ObMaxDatumRowCompare::operator()(
  int64_t l_idx,
  int64_t r_idx)
{
  bool cmp_ret = false;
  int &ret = ret_;
  const ObChunkDatumStore::StoredRow *l = rows_->at(l_idx)->store_row_;
  const ObChunkDatumStore::StoredRow *r = rows_->at(r_idx)->store_row_;
  if (OB_UNLIKELY(OB_SUCCESS != ret)) {
    // already fail
  } else if (!is_inited() || OB_ISNULL(l) || OB_ISNULL(r)) {
    ret = !is_inited() ? OB_NOT_INIT : OB_INVALID_ARGUMENT;
    LOG_WARN("not init or invalid argument", K(ret), KP(l), KP(r));
  } else {
    const ObDatum *lcells = l->cells();
    const ObDatum *rcells = r->cells();
    int cmp = 0;
    for (int64_t i = 0; OB_SUCC(ret) && 0 == cmp && i < sort_cmp_funs_->count(); i++) {
      const int64_t idx = sort_collations_->at(i).field_idx_;
      if (OB_FAIL(sort_cmp_funs_->at(i).cmp_func_(lcells[idx], rcells[idx], cmp))) {
        LOG_WARN("failed to compare", K(ret));
      } else if (cmp < 0) {
        cmp_ret = !sort_collations_->at(i).is_ascending_;
      } else if (cmp > 0) {
        cmp_ret = sort_collations_->at(i).is_ascending_;
      }
    }
  }
  return cmp_ret;
}
