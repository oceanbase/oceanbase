/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_STORAGE_COMPACTION_VECTORIZATION_OB_COMPACTION_VECTOR_H_
#define OB_STORAGE_COMPACTION_VECTORIZATION_OB_COMPACTION_VECTOR_H_

#include "share/datum/ob_datum.h"
#include "share/vector/ob_i_vector.h"
#include "sql/engine/expr/ob_expr.h"
#include "storage/compaction/ob_compaction_memory_context.h"

namespace oceanbase
{
namespace blocksstable
{
struct ObStorageDatum;
} // namespace blocksstable
namespace share
{
namespace schema
{
class ObColDesc;
} // namespace schema
} // namespace share
namespace compaction
{
class ObCompactionGrowableBuffer
{
public:
  ObCompactionGrowableBuffer(const uint64_t tenant_id, const lib::ObLabel &label, const int64_t page_size)
    : capacity_(0),
      size_(0),
      data_(nullptr),
      allocator_(tenant_id, label),
      page_size_(page_size)
  {}
  ~ObCompactionGrowableBuffer()
  {
    reset();
  }

  OB_INLINE void reuse()
  {
    if (capacity_ > 0) {
      MEMSET(data_, 0, capacity_);
    }
    size_ = 0;
  }

  OB_INLINE void reset()
  {
    if (nullptr != data_) {
      allocator_.free(data_);
      data_ = nullptr;
    }
    allocator_.reset();
    capacity_ = 0;
    size_ = 0;
  }

  OB_INLINE int64_t capacity() const { return capacity_; }
  OB_INLINE int64_t size() const { return size_; }
  OB_INLINE char *data() { return data_; }
  OB_INLINE const char *data() const { return data_; }
  OB_INLINE char *data_or_empty() { return nullptr == data_ ? &empty_data_ : data_; }
  OB_INLINE const char *data_or_empty() const { return nullptr == data_ ? &empty_data_ : data_; }
  OB_INLINE void advance(const int64_t len) { size_ += len; }

  int ensure(
      const int64_t need_size,
      int64_t &expand_delta,
      char **ptrs = nullptr,
      const int64_t ptr_cnt = 0);

  OB_INLINE int append_copy(
      const char *src,
      const int64_t len,
      char *&dst,
      char **ptrs = nullptr,
      const int64_t ptr_cnt = 0)
  {
    int ret = OB_SUCCESS;
    int64_t expand_delta = 0; // UNUSED
    dst = nullptr;
    if (OB_UNLIKELY(len < 0)) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid argument", KR(ret), K(len));
    } else if (0 == len) {
      dst = data_or_empty();
    } else if (OB_ISNULL(src)) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid argument", KR(ret), KP(src));
    } else if (OB_FAIL(ensure(len, expand_delta, ptrs, ptr_cnt))) {
      STORAGE_LOG(WARN, "fail to ensure space", KR(ret), K(len), K(size_), K(capacity_));
    } else {
      MEMCPY(data_ + size_, src, len);
      dst = data_ + size_;
      advance(len);
    }
    return ret;
  }

private:
  static char empty_data_;
  int64_t capacity_;
  int64_t size_;
  char *data_;
  compaction::ObLocalAllocator<common::DefaultPageAllocator> allocator_;
  int64_t page_size_;
  DISABLE_COPY_ASSIGN(ObCompactionGrowableBuffer);
};

// nullable
class ObCompactionVector
{
public:
  ObCompactionVector(const VectorFormat format = VEC_INVALID);
  virtual ~ObCompactionVector() = default;

  OB_INLINE sql::VectorHeader &get_vector_header() { return vector_header_; }
  OB_INLINE VectorFormat get_format() const { return vector_header_.get_format(); }
  OB_INLINE ObIVector *get_vector() { return vector_header_.get_vector(); }
  OB_INLINE const ObIVector *get_vector() const { return reinterpret_cast<const ObIVector *>(vector_header_.vector_buf_); }
  virtual void reuse(const int64_t batch_size);
  virtual void init() = 0;
  virtual int64_t get_extra_mem_usage() const { return 0; }

  // --------- append interface --------- //
  OB_INLINE int append_null(const int64_t batch_idx)
  {
    int ret = check_batch_idx(batch_idx);
    if (OB_FAIL(ret)) {
      STORAGE_LOG(WARN, "invalid batch index", KR(ret), K(batch_idx), K_(max_batch_size));
    } else {
      ObBitmapNullVectorBase *base = reinterpret_cast<ObBitmapNullVectorBase *>(get_vector());
      base->set_null(batch_idx);
    }
    return ret;
  }
  virtual int append_datum(const int64_t batch_idx, const blocksstable::ObStorageDatum &datum) = 0;

  // --------- get interface --------- //
  virtual int get_datum(const int64_t batch_idx, ObDatum &datum) = 0;
  OB_INLINE bool is_null(const int64_t batch_idx)
  {
    bool is_null = true;
    if (OB_UNLIKELY(OB_SUCCESS != check_batch_idx(batch_idx))) {
      STORAGE_LOG_RET(WARN, OB_INVALID_ARGUMENT,
                      "invalid batch index", K(batch_idx), K_(max_batch_size));
    } else {
      is_null = reinterpret_cast<ObBitmapNullVectorBase *>(get_vector())->is_null(batch_idx);
    }
    return is_null;
  }

  VIRTUAL_TO_STRING_KV("vector_format", vector_header_.get_format());

  virtual int append_datum_deep_copy(
      const int64_t batch_idx,
      const blocksstable::ObStorageDatum &datum)
  {
    return append_datum(batch_idx, datum);
  }

public:
  static int create_vector(VectorFormat format, VecValueTypeClass value_tc,
                           const int64_t max_batch_size, ObIAllocator &allocator,
                           ObCompactionVector *&vector);

  static int create_vector(const bool is_continuous,
                           const share::schema::ObColDesc &col_desc,
                           const int64_t max_batch_size, ObIAllocator &allocator,
                           ObCompactionVector *&vector);
  static const int64_t PAGE_SIZE = 1024; // 1KB
protected:
  OB_INLINE int check_batch_idx(const int64_t batch_idx) const
  {
    return batch_idx >= 0 && batch_idx < max_batch_size_ ? OB_SUCCESS : OB_INVALID_ARGUMENT;
  }
  OB_INLINE bool is_valid_batch_size(const int64_t batch_size) const
  {
    return batch_size >= 0 && batch_size <= max_batch_size_;
  }
  OB_INLINE void set_max_batch_size(const int64_t max_batch_size)
  {
    max_batch_size_ = max_batch_size;
  }

  // VectorFormat is uint8_t (1 byte), so vector_buf_ sits at offset+1 within VectorHeader.
  // ObCompactionVector has a vptr (8 bytes), putting vector_header_ at offset 8 and
  // vector_buf_ at offset 9 — misaligned for pointer-containing vector objects placed via
  // placement new. This padding shifts vector_header_ to offset 15, so vector_buf_ lands
  // at offset 16 which is 8-byte aligned.
  static_assert(sizeof(VectorFormat) == 1, "adjust padding if VectorFormat size changes");
  char vector_header_align_pad_[7];
  sql::VectorHeader vector_header_;
  int64_t max_batch_size_;
};

template <typename T>
class ObCompactionFixedLengthBase: public ObCompactionVector
{
public:
  ObCompactionFixedLengthBase()
    : ObCompactionVector(VEC_FIXED),
      data_(nullptr)
  {}
  virtual ~ObCompactionFixedLengthBase() override = default;
  virtual void reuse(const int64_t batch_size) override;
  virtual void init() override;
  virtual int append_datum(const int64_t batch_idx, const blocksstable::ObStorageDatum &datum) override;
  virtual int get_datum(const int64_t batch_idx, ObDatum &datum) override;
private:
  T *data_;
};

class ObCompactionDiscreteVector : public ObCompactionVector
{
public:
  ObCompactionDiscreteVector()
    : ObCompactionVector(VEC_DISCRETE),
      lens_(nullptr),
      ptrs_(nullptr),
      buffer_(MTL_ID(), "CompDisc", PAGE_SIZE)
  {}
  ~ObCompactionDiscreteVector() override = default;

  virtual void reuse(const int64_t batch_size) override;
  virtual void init() override;
  virtual int append_datum(const int64_t batch_idx, const blocksstable::ObStorageDatum &datum) override;
  virtual int append_datum_deep_copy(
      const int64_t batch_idx,
      const blocksstable::ObStorageDatum &datum) override;
  virtual int get_datum(const int64_t batch_idx, ObDatum &datum) override;
  virtual int64_t get_extra_mem_usage() const override { return buffer_.capacity(); }

protected:
  int inner_append_datum(
      const int64_t batch_idx,
      const blocksstable::ObStorageDatum &datum,
      const bool force_deep_copy);
  ObLength *lens_;
  char **ptrs_;
  ObCompactionGrowableBuffer buffer_;
};

class ObCompactionContinuousVector : public ObCompactionVector
{
public:
  ObCompactionContinuousVector(const VectorFormat format = VEC_CONTINUOUS);
  virtual ~ObCompactionContinuousVector() override = default;
  virtual void reuse(const int64_t batch_size) override;
  virtual void init() override;
  virtual int append_datum(const int64_t batch_idx, const blocksstable::ObStorageDatum &datum) override;
  virtual int get_datum(const int64_t batch_idx, ObDatum &datum) override;
  virtual int64_t get_extra_mem_usage() const { return buffer_.capacity(); }

private:
  uint32_t *offsets_;
  ObCompactionGrowableBuffer buffer_;
  int64_t next_append_idx_;
};

} // namespace compaction
} // namespace oceanbase
#endif
