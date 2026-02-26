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

#pragma once

#include "share/vector/ob_continuous_base.h"
#include "storage/direct_load/ob_direct_load_vector.h"
#include "common/object/ob_object.h"

namespace oceanbase
{
namespace storage
{
class ObDirectLoadContinuousVector final : public ObDirectLoadVector
{
  static const int64_t PAGE_SIZE = 8LL << 10; // 8K
public:
  ObDirectLoadContinuousVector(ObContinuousBase *continuous_vector);
  ~ObDirectLoadContinuousVector() override;

  ObIVector *get_vector() const override { return continuous_vector_; }
  int64_t memory_usage() const override { return capacity_; }
  int64_t bytes_usage(const int64_t batch_size) const override { return size_; }
  void sum_bytes_usage(int64_t *sum_bytes, const int64_t batch_size) const override
  {
    for (int64_t i = 0; i < batch_size; ++i) {
      sum_bytes[i] += (offsets_[i + 1] - offsets_[i]);
    }
  }

  int sum_lob_length(int64_t *sum_bytes, const int64_t batch_size) const override
  {
    int ret = OB_SUCCESS;

    for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
      const uint32_t offset1 = offsets_[i];
      const uint32_t offset2 = offsets_[i + 1];
      const int32_t len = offset2 - offset1;
      if (len > 0) {
        const char *row_data = data_ + offset1;
        ObLobLocatorV2 locator(ObString(len, row_data), true);
        int64_t lob_length = 0;
        if (OB_FAIL(locator.get_lob_data_byte_len(lob_length))) {
          STORAGE_LOG(WARN, "fail to get lob data byte len", KR(ret), K(locator));
        } else {
          sum_bytes[i] += lob_length + sizeof(ObLobCommon);
        }
      }
    }
    return ret;
  }

  void reuse(const int64_t batch_size) override;

  // --------- append interface --------- //
  int append_default(const int64_t batch_idx) override
  {
    offsets_[batch_idx + 1] = size_;
    return OB_SUCCESS;
  }
  int append_default(const int64_t batch_idx, const int64_t size) override
  {
    for (int64_t i = 0; i < size; ++i) {
      offsets_[batch_idx + i + 1] = size_;
    }
    return OB_SUCCESS;
  }
  int append_datum(const int64_t batch_idx, const ObDatum &datum) override;
  int append_batch(const int64_t batch_idx, const ObDirectLoadVector &src, const int64_t offset,
                   const int64_t size) override;
  int append_batch(const int64_t batch_idx, ObIVector *src, const int64_t offset,
                   const int64_t size) override;
  int append_batch(const int64_t batch_idx, const ObDatumVector &datum_vec, const int64_t offset,
                   const int64_t size) override;
  int append_selective(const int64_t batch_idx, const ObDirectLoadVector &src,
                       const uint16_t *selector, const int64_t size) override;
  int append_selective(const int64_t batch_idx, ObIVector *src, const uint16_t *selector,
                       const int64_t size) override;
  int append_selective(const int64_t batch_idx, const ObDatumVector &datum_vec,
                       const uint16_t *selector, const int64_t size) override;

  // --------- set interface --------- //
  int set_default(const int64_t batch_idx) override { return OB_NOT_SUPPORTED; }
  int set_datum(const int64_t batch_idx, const ObDatum &datum) override { return OB_NOT_SUPPORTED; }

  // --------- shallow copy interface --------- //
  int shallow_copy(ObIVector *src, const int64_t batch_size) override;
  int shallow_copy(const ObDatumVector &datum_vec, const int64_t batch_size) override;

  // --------- get interface --------- //
  int get_datum(const int64_t batch_idx, ObDatum &datum) override
  {
    datum.set_none();
    datum.ptr_ = data_ + offsets_[batch_idx];
    datum.len_ = offsets_[batch_idx + 1] - offsets_[batch_idx];
    return OB_SUCCESS;
  }

  VIRTUAL_TO_STRING_KV(KPC_(continuous_vector), KP_(offsets), KP_(data), K_(capacity), K_(size));

protected:
  int expand(const int64_t need_size);

protected:
  template <typename VEC>
  inline int _append_batch(const int64_t batch_idx, VEC *vec, const int64_t offset,
                           const int64_t size)
  {
    return OB_ERR_UNEXPECTED;
  }
  template <bool IS_CONST>
  inline int _append_batch(const int64_t batch_idx, const ObDatum *datums, const int64_t offset,
                           const int64_t size);
  template <typename VEC>
  inline int _append_selective(const int64_t batch_idx, VEC *vec, const uint16_t *selector,
                               const int64_t size)
  {
    return OB_ERR_UNEXPECTED;
  }
  template <bool IS_CONST>
  inline int _append_selective(const int64_t batch_idx, const ObDatum *datums,
                               const uint16_t *selector, const int64_t size);

  template <typename VEC>
  inline int _shallow_copy(VEC *vec, const int64_t batch_size)
  {
    return OB_ERR_UNEXPECTED;
  }
  template <bool IS_CONST>
  inline int _shallow_copy(const ObDatum *datums, const int64_t batch_size);

protected:
  inline void set_vector(uint32_t *offsets, char *data)
  {
    offsets_ = offsets;
    data_ = data;
    continuous_vector_->set_offsets(offsets);
    continuous_vector_->set_data(data);
  }
  inline void set_data(char *data)
  {
    data_ = data;
    continuous_vector_->set_data(data);
  }

protected:
  ObContinuousBase *const continuous_vector_;
  uint32_t *vec_offsets_;
  uint32_t *offsets_;
  char *data_;
  char *buf_;
  int64_t capacity_;
  int64_t size_;
};

} // namespace storage
} // namespace oceanbase
