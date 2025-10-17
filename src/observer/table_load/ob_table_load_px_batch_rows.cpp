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

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/ob_table_load_px_batch_rows.h"
#include "storage/direct_load/ob_direct_load_datum_row.h"

namespace oceanbase
{
namespace observer
{
using namespace blocksstable;
using namespace common;
using namespace share::schema;
using namespace sql;
using namespace storage;

ObTableLoadPXBatchRows::ObTableLoadPXBatchRows() : is_inited_(false)
{
  vectors_.set_tenant_id(MTL_ID());
}

ObTableLoadPXBatchRows::~ObTableLoadPXBatchRows() { reset(); }

void ObTableLoadPXBatchRows::reset()
{
  is_inited_ = false;
  vectors_.reset();
  batch_rows_.reset();
}

void ObTableLoadPXBatchRows::reuse() { batch_rows_.reuse(); }

int ObTableLoadPXBatchRows::init(const ObIArray<ObColDesc> &px_col_descs,
                                 const ObIArray<int64_t> &px_column_project_idxs,
                                 const ObIArray<ObColDesc> &col_descs,
                                 const ObBitVector *col_nullables,
                                 const ObDirectLoadRowFlag &row_flag, const int64_t max_batch_size)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadPXBatchRows init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(px_col_descs.empty() ||
                         px_col_descs.count() != px_column_project_idxs.count() ||
                         col_descs.empty() || nullptr == col_nullables || max_batch_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(px_col_descs), K(px_column_project_idxs),
             K(col_descs), KP(col_nullables), K(row_flag), K(max_batch_size));
  } else {
    if (OB_FAIL(vectors_.prepare_allocate(px_col_descs.count()))) {
      LOG_WARN("fail to prepare allocate", KR(ret), K(px_col_descs.count()));
    } else if (OB_FAIL(batch_rows_.init(col_descs, col_nullables, max_batch_size, row_flag))) {
      LOG_WARN("fail to init batch rows", KR(ret));
    } else {
      int64_t column_count = 0;
      const ObIArray<ObDirectLoadVector *> &vectors = batch_rows_.get_vectors();
      for (int64_t i = 0; OB_SUCC(ret) && i < px_column_project_idxs.count(); ++i) {
        const int64_t column_idx = px_column_project_idxs.at(i);
        if (column_idx < 0) {
        } else if (OB_UNLIKELY(column_idx >= vectors.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected column idx", KR(ret), K(i), K(column_idx), K(px_column_project_idxs),
                   K(px_col_descs), K(col_descs));
        } else {
          vectors_.at(i) = vectors.at(column_idx);
          ++column_count;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_UNLIKELY(batch_rows_.get_column_count() != column_count)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected column count", KR(ret), K(column_count), K(batch_rows_));
      }
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTableLoadPXBatchRows::append_batch(const IVectorPtrs &vectors, const int64_t offset,
                                         const int64_t size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadPXBatchRows not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(vectors.count() != vectors_.count() || size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(this), K(vectors), K(size));
  } else if (OB_UNLIKELY(size > remain_size())) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("size overflow", KR(ret), K(remain_size()), K(size));
  } else {
    const int64_t batch_idx = batch_rows_.size();
    for (int64_t i = 0; OB_SUCC(ret) && i < vectors_.count(); ++i) {
      ObDirectLoadVector *vector = vectors_.at(i);
      if (nullptr == vector) {
      } else if (OB_FAIL(vector->append_batch(batch_idx, vectors.at(i), offset, size))) {
        LOG_WARN("fail to append batch", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      batch_rows_.set_size(batch_idx + size);
    }
  }
  return ret;
}

int ObTableLoadPXBatchRows::append_batch(const ObIArray<ObDatumVector> &datum_vectors,
                                         const int64_t offset, const int64_t size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadPXBatchRows not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(datum_vectors.count() != vectors_.count() || size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(this), K(datum_vectors), K(size));
  } else if (OB_UNLIKELY(size > remain_size())) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("size overflow", KR(ret), K(remain_size()), K(size));
  } else {
    const int64_t batch_idx = batch_rows_.size();
    for (int64_t i = 0; OB_SUCC(ret) && i < vectors_.count(); ++i) {
      ObDirectLoadVector *vector = vectors_.at(i);
      if (nullptr == vector) {
      } else if (OB_FAIL(vector->append_batch(batch_idx, datum_vectors.at(i), offset, size))) {
        LOG_WARN("fail to append batch", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      batch_rows_.set_size(batch_idx + size);
    }
  }
  return ret;
}

int ObTableLoadPXBatchRows::append_selective(const IVectorPtrs &vectors, const uint16_t *selector,
                                             int64_t size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadPXBatchRows not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(vectors.count() != vectors_.count() || nullptr == selector || size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(this), K(vectors), KP(selector), K(size));
  } else if (OB_UNLIKELY(size > remain_size())) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("size overflow", KR(ret), K(remain_size()), K(size));
  } else {
    const int64_t batch_idx = batch_rows_.size();
    for (int64_t i = 0; OB_SUCC(ret) && i < vectors_.count(); ++i) {
      ObDirectLoadVector *vector = vectors_.at(i);
      if (nullptr == vector) {
      } else if (OB_FAIL(vector->append_selective(batch_idx, vectors.at(i), selector, size))) {
        LOG_WARN("fail to append selective", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      batch_rows_.set_size(batch_idx + size);
    }
  }
  return ret;
}

int ObTableLoadPXBatchRows::append_selective(const ObIArray<ObDatumVector> &datum_vectors,
                                             const uint16_t *selector, int64_t size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadBatchRows not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(datum_vectors.count() != vectors_.count() || nullptr == selector ||
                         size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(this), K(datum_vectors), KP(selector), K(size));
  } else if (OB_UNLIKELY(size > remain_size())) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("size overflow", KR(ret), K(remain_size()), K(size));
  } else {
    const int64_t batch_idx = batch_rows_.size();
    for (int64_t i = 0; OB_SUCC(ret) && i < vectors_.count(); ++i) {
      ObDirectLoadVector *vector = vectors_.at(i);
      if (nullptr == vector) {
      } else if (OB_FAIL(
                   vector->append_selective(batch_idx, datum_vectors.at(i), selector, size))) {
        LOG_WARN("fail to append selective", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      batch_rows_.set_size(batch_idx + size);
    }
  }
  return ret;
}

int ObTableLoadPXBatchRows::append_row(const ObDirectLoadDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadPXBatchRows not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(datum_row.get_column_count() != vectors_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(this), K(datum_row));
  } else if (OB_UNLIKELY(batch_rows_.remain_size() <= 0)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("size overflow", KR(ret), K(batch_rows_.remain_size()));
  } else {
    const int64_t batch_idx = batch_rows_.size();
    for (int64_t i = 0; OB_SUCC(ret) && i < vectors_.count(); ++i) {
      ObDirectLoadVector *vector = vectors_.at(i);
      if (nullptr == vector) {
      } else {
        const ObDatum &datum = datum_row.storage_datums_[i];
        if (OB_FAIL(vector->append_datum(batch_idx, datum))) {
          LOG_WARN("fail to append datum", KR(ret), K(i), K(datum), KPC(vector));
        }
      }
    }
    if (OB_SUCC(ret)) {
      batch_rows_.set_size(batch_idx + 1);
    }
  }
  return ret;
}

int ObTableLoadPXBatchRows::shallow_copy(const IVectorPtrs &vectors, const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadPXBatchRows not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(vectors.count() != vectors_.count() || batch_size <= 0 ||
                         batch_size > batch_rows_.get_max_batch_size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(this), K(vectors), K(batch_size));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < vectors_.count(); ++i) {
      ObDirectLoadVector *vector = vectors_.at(i);
      if (nullptr == vector) {
      } else {
        if (OB_FAIL(vector->shallow_copy(vectors.at(i), batch_size))) {
          LOG_WARN("fail to shallow copy vector", KR(ret), K(i));
        }
      }
    }
    if (OB_SUCC(ret)) {
      batch_rows_.set_size(batch_size);
    }
  }
  return ret;
}

int ObTableLoadPXBatchRows::shallow_copy(const ObIArray<ObDatumVector> &datum_vectors,
                                         const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadPXBatchRows not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(datum_vectors.count() != vectors_.count() || batch_size <= 0 ||
                         batch_size > batch_rows_.get_max_batch_size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(this), K(datum_vectors), K(batch_size));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < vectors_.count(); ++i) {
      ObDirectLoadVector *vector = vectors_.at(i);
      if (nullptr == vector) {
      } else {
        if (OB_FAIL(vector->shallow_copy(datum_vectors.at(i), batch_size))) {
          LOG_WARN("fail to shallow copy vector", KR(ret), K(i));
        }
      }
    }
    if (OB_SUCC(ret)) {
      batch_rows_.set_size(batch_size);
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
