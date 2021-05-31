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

#include "lib/allocator/ob_malloc.h"
#include "lib/utility/ob_tracepoint.h"
#include "common/row/ob_row_util.h"
#include "common/object/ob_obj_compare.h"
#include "sql/engine/sort/ob_in_memory_sort.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

ObInMemorySort::ObInMemorySort()
    : ObBaseSort(),
      allocator_(ObModIds::OB_SQL_SORT_ROW, OB_MALLOC_NORMAL_BLOCK_SIZE, OB_SERVER_TENANT_ID, ObCtxIds::WORK_AREA),
      row_store_(allocator_, ObModIds::OB_SQL_SORT_ROW, OB_SERVER_TENANT_ID),
      sort_array_(),
      sort_array_pos_(0)
{
  row_store_.set_ctx_id(ObCtxIds::WORK_AREA);
}

ObInMemorySort::~ObInMemorySort()
{}

void ObInMemorySort::reset()
{
  ObBaseSort::reset();
  row_store_.reset();
  sort_array_.reset();
  sort_array_pos_ = 0;
  allocator_.reset();
}

void ObInMemorySort::reuse()
{
  ObBaseSort::reuse();
  row_store_.reuse();
  sort_array_.reuse();
  sort_array_pos_ = 0;
  allocator_.reuse();
}

void ObInMemorySort::rescan()
{
  sort_array_pos_ = 0;
}

int ObInMemorySort::set_sort_columns(const ObIArray<ObSortColumn>& sort_columns)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObBaseSort::set_sort_columns(sort_columns, 0))) {
    LOG_WARN("ObBaseSort: failed to set sort column", K(ret));
  } else if (sort_columns.count() > 0 && OB_FAIL(row_store_.init_reserved_column_count(sort_columns.count()))) {
    LOG_WARN("failed to init row store", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < sort_columns.count(); ++i) {
    const ObSortColumn& sort_column = sort_columns.at(i);
    if (OB_FAIL(OB_I(t1) row_store_.add_reserved_column(sort_column.index_))) {
      LOG_WARN("failed to add reserved column", K(ret));
    }
  }
  return ret;
}

int ObInMemorySort::add_row(const ObNewRow& row)
{
  int ret = OB_SUCCESS;
  const ObRowStore::StoredRow* stored_row = NULL;
  if (OB_ISNULL(get_sort_columns())) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("sort columns is null", K(ret));
  } else if (OB_FAIL(OB_I(t1) row_store_.add_row(row, stored_row, 0, true))) {
    LOG_WARN("failed to add row into row_store", K(ret), K(row));
  } else if (OB_ISNULL(stored_row)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("stored row is null", K(ret));
  } else if (OB_FAIL(OB_I(t3) sort_array_.push_back(stored_row))) {
    LOG_WARN("failed to push back to array", K(ret));
  }
  return ret;
}

struct ObInMemorySort::Comparer {
  explicit Comparer(const ObIArray<ObSortColumn>& sort_columns, int* err) : sort_columns_(sort_columns), err_(err)
  {}
  bool operator()(const ObRowStore::StoredRow* r1, const ObRowStore::StoredRow* r2)
  {
    int ret = OB_SUCCESS;
    bool bret = false;
    int cmp = 0;
    if (OB_UNLIKELY(OB_SUCCESS != *err_)) {
      // do nothing if we already have an error,
      // so we can finish the sort process ASAP.
    } else {
      if (OB_ISNULL(r1) || OB_ISNULL(r2)) {
        ret = OB_BAD_NULL_ERROR;
        LOG_WARN("stored row is null", K(ret));
      } else {
        if (OB_UNLIKELY(sort_columns_.count() > r1->reserved_cells_count_ ||
                        sort_columns_.count() > r2->reserved_cells_count_)) {
          ret = OB_ARRAY_OUT_OF_RANGE;
          LOG_WARN("sort column count out of range",
              K(ret),
              "sort column count",
              sort_columns_.count(),
              "row1 cell count",
              r1->reserved_cells_count_,
              "row2 cell count",
              r2->reserved_cells_count_);
        }
        ObCompareCtx cmp_ctx(ObMaxType, CS_TYPE_INVALID, true, INVALID_TZ_OFF, default_null_pos());
        for (int32_t i = 0; OB_SUCCESS == ret && 0 == cmp && i < sort_columns_.count(); ++i) {
          cmp_ctx.null_pos_ = sort_columns_.at(i).is_null_first()
                                  ? (sort_columns_.at(i).is_ascending() ? NULL_FIRST : NULL_LAST)
                                  : (sort_columns_.at(i).is_ascending() ? NULL_LAST : NULL_FIRST);
          cmp_ctx.cmp_cs_type_ = sort_columns_.at(i).cs_type_;
          cmp = r1->reserved_cells_[i].compare(r2->reserved_cells_[i], cmp_ctx);
          if (cmp < 0) {
            bret = sort_columns_.at(i).is_ascending();
          } else if (cmp > 0) {
            bret = !sort_columns_.at(i).is_ascending();
          } else {
          }
        }  // end for
      }
      *err_ = ret;
    }
    return bret;
  }
  const ObIArray<ObSortColumn>& sort_columns_;
  int* err_;
};

enum { STATIC_COMPARATOR_MAX_CNT = 16 };
struct ObInMemorySort::StaticComparer : public ObInMemorySort::Comparer {
  explicit StaticComparer(const ObIArray<ObSortColumn>& sort_columns, int* err)
      : Comparer(sort_columns, err), idx_cnt_(sort_columns.count())
  {
    for (int64_t i = 0; i < STATIC_COMPARATOR_MAX_CNT; ++i) {
      if (i < idx_cnt_) {
        idxs_[i] = sort_columns.at(i).index_;
        ascend_[i] = sort_columns.at(i).is_ascending();
        cs_type_[i] = sort_columns.at(i).cs_type_;
      } else {
        idxs_[i] = 0;
        ascend_[i] = true;
        cs_type_[i] = CS_TYPE_UTF8MB4_GENERAL_CI;
      }
    }
  }

  bool operator()(const ObRowStore::StoredRow* r1, const ObRowStore::StoredRow* r2)
  {
    int ret = OB_SUCCESS;
    bool bret = false;
    int cmp = 0;
    if (OB_UNLIKELY(OB_SUCCESS != *err_)) {
      // do nothing if we already have an error,
      // so we can finish the sort process ASAP.
    } else {
      if (OB_ISNULL(r1) || OB_ISNULL(r2)) {
        ret = OB_BAD_NULL_ERROR;
        LOG_WARN("stored row is null", K(ret));
      } else {
        if (OB_UNLIKELY(idx_cnt_ > r1->reserved_cells_count_ || idx_cnt_ > r2->reserved_cells_count_)) {
          ret = OB_ARRAY_OUT_OF_RANGE;
          LOG_WARN("sort column count out of range",
              K(ret),
              "sort column count",
              sort_columns_.count(),
              "row1 cell count",
              r1->reserved_cells_count_,
              "row2 cell count",
              r2->reserved_cells_count_);
        }
        if (OB_SUCC(ret)) {
          for (int32_t i = 0; 0 == cmp && i < idx_cnt_; ++i) {
            cmp = r1->reserved_cells_[i].compare(r2->reserved_cells_[i], cs_type_[i]);
            if (cmp < 0) {
              bret = ascend_[i];
            } else if (cmp > 0) {
              bret = !ascend_[i];
            } else {
            }
          }  // end for
        }
      }
      *err_ = ret;
    }
    return bret;
  }
  int64_t idxs_[STATIC_COMPARATOR_MAX_CNT];
  int64_t idx_cnt_;
  bool ascend_[STATIC_COMPARATOR_MAX_CNT];
  ObCollationType cs_type_[STATIC_COMPARATOR_MAX_CNT];
};

int ObInMemorySort::sort_rows()
{
  int ret = OB_SUCCESS;
  const ObIArray<ObSortColumn>* sort_column = NULL;
  if (OB_ISNULL(sort_column = get_sort_columns())) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("sort columns is null", K(ret));
  } else if (sort_array_.count() > 0) {
    SQL_ENG_LOG(DEBUG, "sort rows", "count", sort_array_.count());
    const ObRowStore::StoredRow** first_row = &sort_array_.at(0);
    int err = OB_SUCCESS;
    if (sort_column->count() <= STATIC_COMPARATOR_MAX_CNT) {
      StaticComparer comparer(*sort_column, &err);
      std::sort(first_row, first_row + sort_array_.count(), comparer);
    } else {
      Comparer comparer(*sort_column, &err);
      std::sort(first_row, first_row + sort_array_.count(), comparer);
    }
    if (OB_FAIL(err)) {
      LOG_WARN("failed to call std::sort", K(ret));
    }
  }
  return ret;
}

int ObInMemorySort::get_next_row(ObNewRow& row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(sort_array_pos_ < 0)) {
    ret = OB_ARRAY_OUT_OF_RANGE;
    LOG_WARN("sort array out of range", K(ret), K_(sort_array_pos));
  } else if (OB_UNLIKELY(sort_array_pos_ >= sort_array_.count()) || OB_UNLIKELY(sort_array_pos_ >= get_topn_cnt())) {
    ret = OB_ITER_END;
    SQL_ENG_LOG(DEBUG, "end of the in-memory run");
  } else {
    if (!row_store_.use_compact_row()) {
      const ObNewRow* res_row = static_cast<const ObNewRow*>(sort_array_.at(sort_array_pos_)->get_compact_row_ptr());
      if (OB_ISNULL(res_row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("row is null", K(ret));
      } else if (OB_ISNULL(res_row->cells_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cells is null", K(ret));
      } else if (OB_UNLIKELY(res_row->count_ > row.count_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("row space not enough", K(res_row->count_), K(row.count_), K(ret));
      } else {
        const ObObj* cells = res_row->cells_;

        // A possible optimization for performance
        //      const ObObj *cells = *reinterpret_cast<ObObj **>(const_cast<char *>(reinterpret_cast<const char *>(
        //          sort_array_.at(sort_array_pos_)) + offsetof(ObRowStore::StoredRow, reserved_cells_)
        //          + sort_col_count_ * sizeof(ObObj) + offsetof(ObNewRow, cells_)));

        for (int64_t i = 0; i < row_store_.get_col_count(); ++i) {
          row.cells_[i] = cells[i];
        }
      }
    } else {
      const ObRowStore::StoredRow* store_row = sort_array_.at(sort_array_pos_);
      if (OB_ISNULL(store_row)) {
        LOG_WARN("store_row is null", K(ret));
      } else {
        const char* compact_row = static_cast<const char*>(store_row->get_compact_row_ptr());
        if (OB_ISNULL(compact_row)) {
          LOG_WARN("compact_row is null", K(ret));
        } else {
          if (OB_FAIL(
                  OB_I(t1) ObRowUtil::convert(compact_row, static_cast<int64_t>(store_row->compact_row_size_), row))) {
            LOG_WARN("failed to convert row", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      ++sort_array_pos_;
    }
  }
  return ret;
}

int ObInMemorySort::get_next_compact_row(ObString& compact_row)
{
  int ret = OB_SUCCESS;
  if (sort_array_pos_ < 0) {
    ret = OB_ARRAY_OUT_OF_RANGE;
    LOG_WARN("sort array out of range", K(ret), K_(sort_array_pos));
  } else if (sort_array_pos_ >= sort_array_.count() || sort_array_pos_ >= get_topn_cnt()) {
    ret = OB_ITER_END;
    SQL_ENG_LOG(INFO, "end of the in-memory run");
  } else {
    compact_row = sort_array_.at(static_cast<int32_t>(sort_array_pos_))->get_compact_row();
    ++sort_array_pos_;
  }
  return ret;
}

int64_t ObInMemorySort::get_row_count() const
{
  return sort_array_.count();
}

int64_t ObInMemorySort::get_used_mem_size() const
{
  return row_store_.get_used_mem_size() + sort_array_.count() * sizeof(void*);
}
