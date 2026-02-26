/**
 * Copyright (c) 2024 OceanBase
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

#include "storage/direct_load/ob_direct_load_batch_rows.h"
#include "storage/direct_load/ob_direct_load_datum_row.h"

namespace oceanbase
{
namespace storage
{
using namespace blocksstable;
using namespace common;
using namespace share::schema;
using namespace sql;

ObDirectLoadBatchRows::ObDirectLoadBatchRows()
  : allocator_("TLD_BatchRows"),
    vectors_(),
    max_batch_size_(0),
    row_flag_(),
    size_(0),
    is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
  vectors_.set_block_allocator(ModulePageAllocator(allocator_));
}

ObDirectLoadBatchRows::~ObDirectLoadBatchRows() { reset(); }

void ObDirectLoadBatchRows::reset()
{
  is_inited_ = false;
  for (int64_t i = 0; i < vectors_.count(); ++i) {
    ObDirectLoadVector *vector = vectors_.at(i);
    if (nullptr != vector) {
      vector->~ObDirectLoadVector();
    }
  }
  vectors_.reset();
  max_batch_size_ = 0;
  row_flag_.reset();
  size_ = 0;
  allocator_.reset();
}

void ObDirectLoadBatchRows::reuse()
{
  size_ = 0;
  for (int64_t i = 0; i < vectors_.count(); ++i) {
    ObDirectLoadVector *vector = vectors_.at(i);
    if (nullptr != vector) {
      vector->reuse(max_batch_size_);
    }
  }
}

int ObDirectLoadBatchRows::init(const ObIArray<ObColDesc> &col_descs,
                                const ObBitVector *col_nullables, const int64_t max_batch_size,
                                const ObDirectLoadRowFlag &row_flag)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadBatchRows init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(col_descs.empty() || nullptr == col_nullables || max_batch_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(col_descs), KP(col_nullables), K(max_batch_size));
  } else {
    if (OB_FAIL(init_vectors(col_descs, col_nullables, max_batch_size))) {
      LOG_WARN("fail to init vectors", KR(ret));
    } else {
      max_batch_size_ = max_batch_size;
      row_flag_ = row_flag;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadBatchRows::init(const ObIArray<ObColumnSchemaItem> &column_schemas,
                                const int64_t max_batch_size,
                                const ObDirectLoadRowFlag &row_flag)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadBatchRows init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(column_schemas.empty() || max_batch_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(column_schemas), K(max_batch_size));
  } else {
    if (OB_FAIL(init_vectors(column_schemas, max_batch_size))) {
      LOG_WARN("fail to init vectors", KR(ret), K(column_schemas), K(max_batch_size));
    } else {
      max_batch_size_ = max_batch_size;
      row_flag_ = row_flag;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadBatchRows::init_vectors(const ObIArray<ObColDesc> &col_descs,
                                        const ObBitVector *col_nullables, int64_t max_batch_size)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(vectors_.prepare_allocate(col_descs.count()))) {
    LOG_WARN("fail to prepare allocate", KR(ret), K(col_descs.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < col_descs.count(); ++i) {
    const ObColDesc &col_desc = col_descs.at(i);
    const bool is_nullable = col_nullables->at(i);
    ObDirectLoadVector *vector = nullptr;
    if (OB_FAIL(ObDirectLoadVector::create_vector(col_desc, is_nullable, max_batch_size, allocator_,
                                                  vector))) {
      LOG_WARN("fail to create vector", KR(ret), K(col_desc));
    } else {
      vectors_.at(i) = vector;
    }
  }
  return ret;
}

int ObDirectLoadBatchRows::init_vectors(const common::ObIArray<ObColumnSchemaItem> &column_schemas, int64_t max_batch_size)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(vectors_.prepare_allocate(column_schemas.count()))) {
    LOG_WARN("fail to prepare allocate", KR(ret), K(column_schemas.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < column_schemas.count(); ++i) {
    const ObObjMeta &col_type = column_schemas.at(i).col_type_;
    const bool is_nullable = true; // column_schemas.at(i).is_nullable_;// TODO@suzhi:batch rows not check input datum if nullable is false
    ObDirectLoadVector *vector = nullptr;
    if (OB_FAIL(ObDirectLoadVector::create_vector(col_type, is_nullable, max_batch_size, allocator_,
                                                  vector))) {
      LOG_WARN("fail to create vector", KR(ret), K(col_type));
    } else {
      vectors_.at(i) = vector;
    }
  }
  return ret;

}

int ObDirectLoadBatchRows::append_row(const ObDirectLoadDatumRow &datum_row)
{
  return append_row(datum_row.storage_datums_, datum_row.count_);
}

int ObDirectLoadBatchRows::append_row(const ObDatumRow &datum_row)
{
  return append_row(datum_row.storage_datums_, datum_row.count_);
}

int ObDirectLoadBatchRows::append_row(const ObStorageDatum *datums, const int64_t column_count)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadBatchRows not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(column_count != get_column_count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(this), K(column_count));
  } else if (OB_UNLIKELY(size_ >= max_batch_size_)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("size overflow", KR(ret), K(max_batch_size_), K(size_));
  } else {
    for (int64_t src_idx = 0, dest_idx = (row_flag_.uncontain_hidden_pk_ ? 1 : 0);
         OB_SUCC(ret) && src_idx < column_count; ++src_idx, ++dest_idx) {
      const ObDatum &datum = datums[src_idx];
      ObDirectLoadVector *vector = vectors_.at(dest_idx);
      if (OB_FAIL(vector->append_datum(size_, datum))) {
        LOG_WARN("fail to append datum", KR(ret), K(src_idx), K(datum), K(dest_idx), KPC(vector));
      }
    }
    if (OB_SUCC(ret)) {
      ++size_;
    }
  }
  return ret;
}

int ObDirectLoadBatchRows::append_row(const ObIArray<ObDatum *> &datums)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadBatchRows not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(datums.count() != get_column_count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(this), K(datums.count()));
  } else if (OB_UNLIKELY(size_ >= max_batch_size_)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("size overflow", KR(ret), K(max_batch_size_), K(size_));
  } else {
    for (int64_t src_idx = 0, dest_idx = (row_flag_.uncontain_hidden_pk_ ? 1 : 0);
         OB_SUCC(ret) && src_idx < datums.count(); ++src_idx, ++dest_idx) {
      const ObDatum *datum = datums.at(src_idx);
      ObDirectLoadVector *vector = vectors_.at(dest_idx);
      if (OB_ISNULL(datum)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("datum is null", K(ret), K(datum), K(src_idx));
      } else if (OB_FAIL(vector->append_datum(size_, *datum))) {
        LOG_WARN("fail to append datum", KR(ret), K(src_idx), KPC(datum), K(dest_idx), KPC(vector));
      }
    }
    if (OB_SUCC(ret)) {
      ++size_;
    }
  }
  return ret;
}

int ObDirectLoadBatchRows::append_batch(const ObDirectLoadBatchRows &vectors, const int64_t offset,
                                        const int64_t size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadBatchRows not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(vectors.vectors_.count() != vectors_.count() || offset < 0 || size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(this), K(vectors), K(offset), K(size));
  } else if (OB_UNLIKELY(size > remain_size())) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("size overflow", KR(ret), K(max_batch_size_), K(size_), K(size));
  } else {
    for (int64_t i = (row_flag_.uncontain_hidden_pk_ ? 1 : 0); OB_SUCC(ret) && i < vectors_.count();
         ++i) {
      if (OB_FAIL(vectors_.at(i)->append_batch(size_, *vectors.vectors_.at(i), offset, size))) {
        LOG_WARN("fail to append batch", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      size_ += size;
    }
  }
  return ret;
}

int ObDirectLoadBatchRows::append_batch(const IVectorPtrs &vectors, const int64_t offset,
                                        const int64_t size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadBatchRows not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(vectors.count() != vectors_.count() || offset < 0 || size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(this), K(vectors), K(offset), K(size));
  } else if (OB_UNLIKELY(size > remain_size())) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("size overflow", KR(ret), K(max_batch_size_), K(size_), K(size));
  } else {
    for (int64_t i = (row_flag_.uncontain_hidden_pk_ ? 1 : 0); OB_SUCC(ret) && i < vectors_.count();
         ++i) {
      if (OB_FAIL(vectors_.at(i)->append_batch(size_, vectors.at(i), offset, size))) {
        LOG_WARN("fail to append batch", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      size_ += size;
    }
  }
  return ret;
}

int ObDirectLoadBatchRows::append_batch(const ObIArray<ObDatumVector> &datum_vectors,
                                        const int64_t offset, const int64_t size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadBatchRows not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(datum_vectors.count() != vectors_.count() || offset < 0 || size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(this), K(datum_vectors), K(offset), K(size));
  } else if (OB_UNLIKELY(size > remain_size())) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("size overflow", KR(ret), K(max_batch_size_), K(size_), K(size));
  } else {
    for (int64_t i = (row_flag_.uncontain_hidden_pk_ ? 1 : 0); OB_SUCC(ret) && i < vectors_.count();
         ++i) {
      if (OB_FAIL(vectors_.at(i)->append_batch(size_, datum_vectors.at(i), offset, size))) {
        LOG_WARN("fail to append batch", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      size_ += size;
    }
  }
  return ret;
}

int ObDirectLoadBatchRows::append_selective(const ObDirectLoadBatchRows &src,
                                            const uint16_t *selector, const int64_t size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadBatchRows not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(src.vectors_.count() != vectors_.count() || nullptr == selector ||
                         size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(this), K(src), KP(selector), K(size));
  } else if (OB_UNLIKELY(size > remain_size())) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("size overflow", KR(ret), K(max_batch_size_), K(size_), K(size));
  } else {
    for (int64_t i = (row_flag_.uncontain_hidden_pk_ ? 1 : 0); OB_SUCC(ret) && i < vectors_.count();
         ++i) {
      if (OB_FAIL(vectors_.at(i)->append_selective(size_, *src.vectors_.at(i), selector, size))) {
        LOG_WARN("fail to append selective", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      size_ += size;
    }
  }
  return ret;
}

int ObDirectLoadBatchRows::append_selective(const IVectorPtrs &vectors, const uint16_t *selector,
                                            int64_t size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadBatchRows not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(vectors.count() != vectors_.count() || nullptr == selector || size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(this), K(vectors), KP(selector), K(size));
  } else if (OB_UNLIKELY(size > remain_size())) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("size overflow", KR(ret), K(max_batch_size_), K(size_), K(size));
  } else {
    for (int64_t i = (row_flag_.uncontain_hidden_pk_ ? 1 : 0); OB_SUCC(ret) && i < vectors_.count();
         ++i) {
      if (OB_FAIL(vectors_.at(i)->append_selective(size_, vectors.at(i), selector, size))) {
        LOG_WARN("fail to append selective", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      size_ += size;
    }
  }
  return ret;
}

int ObDirectLoadBatchRows::append_selective(const IVectorPtrs &vectors, share::ObBatchSelector &selector)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadBatchRows not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(vectors.count() != vectors_.count() || !selector.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(this), K(vectors), K(selector));
  } else if (OB_UNLIKELY(selector.size() > remain_size())) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("size overflow", KR(ret), K(max_batch_size_), K(size_), K(selector.size()));
  } else {
    int64_t i = 0;
    while (OB_SUCC(ret) && OB_SUCC(selector.get_next(i))) {
      if (OB_FAIL(append_batch(vectors, i, 1))) {
        LOG_WARN("append row failed", K(ret));
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObDirectLoadBatchRows::append_selective(const ObIArray<ObDatumVector> &datum_vectors,
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
    LOG_WARN("size overflow", KR(ret), K(max_batch_size_), K(size_), K(size));
  } else {
    for (int64_t i = (row_flag_.uncontain_hidden_pk_ ? 1 : 0); OB_SUCC(ret) && i < vectors_.count();
         ++i) {
      if (OB_FAIL(vectors_.at(i)->append_selective(size_, datum_vectors.at(i), selector, size))) {
        LOG_WARN("fail to append selective", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      size_ += size;
    }
  }
  return ret;
}

int ObDirectLoadBatchRows::shallow_copy(const IVectorPtrs &vectors, const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadBatchRows not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(vectors.count() != vectors_.count() || batch_size <= 0 ||
                         batch_size > max_batch_size_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(this), K(vectors), K(batch_size));
  } else {
    for (int64_t i = (row_flag_.uncontain_hidden_pk_ ? 1 : 0); OB_SUCC(ret) && i < vectors_.count();
         ++i) {
      if (OB_FAIL(vectors_.at(i)->shallow_copy(vectors.at(i), batch_size))) {
        LOG_WARN("fail to shallow copy vector", KR(ret), K(i));
      }
    }
    if (OB_SUCC(ret)) {
      size_ = batch_size;
    }
  }
  return ret;
}

int ObDirectLoadBatchRows::shallow_copy(const ObIArray<ObDatumVector> &datum_vectors,
                                        const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadBatchRows not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(datum_vectors.count() != vectors_.count() || batch_size <= 0 ||
                         batch_size > max_batch_size_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(this), K(datum_vectors), K(batch_size));
  } else {
    for (int64_t i = (row_flag_.uncontain_hidden_pk_ ? 1 : 0); OB_SUCC(ret) && i < vectors_.count();
         ++i) {
      if (OB_FAIL(vectors_.at(i)->shallow_copy(datum_vectors.at(i), batch_size))) {
        LOG_WARN("fail to shallow copy datum vector", KR(ret), K(i));
      }
    }
    if (OB_SUCC(ret)) {
      size_ = batch_size;
    }
  }
  return ret;
}

int ObDirectLoadBatchRows::get_datum_row(const int64_t batch_idx,
                                         ObDirectLoadDatumRow &datum_row) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadBatchRows not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(batch_idx < 0 || batch_idx >= size_ ||
                         datum_row.get_column_count() != get_column_count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(this), K(batch_idx), K(datum_row));
  } else {
    for (int64_t src_idx = (row_flag_.uncontain_hidden_pk_ ? 1 : 0), dest_idx = 0;
         OB_SUCC(ret) && src_idx < vectors_.count(); ++src_idx, ++dest_idx) {
      ObDirectLoadVector *vector = vectors_.at(src_idx);
      ObDatum &datum = datum_row.storage_datums_[dest_idx];
      if (OB_FAIL(vector->get_datum(batch_idx, datum))) {
        LOG_WARN("fail to get datum", KR(ret));
      }
    }
  }
  return ret;
}

int64_t ObDirectLoadBatchRows::memory_usage() const
{
  int64_t size = 0;
  size += allocator_.total();
  for (int64_t i = 0; i < vectors_.count(); ++i) {
    ObDirectLoadVector *vector = vectors_.at(i);
    if (OB_NOT_NULL(vector)) {
      size += vector->memory_usage();
    }
  }
  return size;
}

int64_t ObDirectLoadBatchRows::bytes_usage() const
{
  int64_t size = 0;
  for (int64_t i = 0; i < vectors_.count(); ++i) {
    ObDirectLoadVector *vector = vectors_.at(i);
    if (OB_NOT_NULL(vector)) {
      size += vector->bytes_usage(size_);
    }
  }
  return size;
}

} // namespace storage
} // namespace oceanbase
