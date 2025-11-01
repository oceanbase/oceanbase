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

#include "share/rc/ob_tenant_base.h"
#include "share/vector/ob_continuous_base.h"
#include "share/vector/ob_discrete_base.h"
#include "share/vector/ob_uniform_base.h"
#include "storage/direct_load/ob_direct_load_vector.h"
#include "common/object/ob_object.h"

namespace oceanbase
{
namespace storage
{
class ObDirectLoadDiscreteVector final : public ObDirectLoadVector
{
public:
  ObDirectLoadDiscreteVector(ObDiscreteBase *discrete_vector);
  ~ObDirectLoadDiscreteVector() override = default;

  ObIVector *get_vector() const override { return discrete_vector_; }
  int64_t memory_usage() const override { return allocator_.total(); }
  int64_t bytes_usage(const int64_t batch_size) const override { return allocator_.used(); }
  void sum_bytes_usage(int64_t *sum_bytes, const int64_t batch_size) const override
  {
    for (int64_t i = 0; i < batch_size; ++i) {
      sum_bytes[i] += lens_[i];
    }
  }

  int sum_lob_length(int64_t *sum_bytes, const int64_t batch_size) const override
  {
    int ret = OB_SUCCESS;
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
      const char *data = ptrs_[i];
      const int32_t len = lens_[i];
      if (len > 0 && data != nullptr) {
        ObLobLocatorV2 locator(ObString(len, data), true);
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
  int append_default(const int64_t batch_idx) override { return OB_SUCCESS; }
  int append_default(const int64_t batch_idx, const int64_t size) override { return OB_SUCCESS; }
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
  int set_default(const int64_t batch_idx) override
  {
    lens_[batch_idx] = 0;
    ptrs_[batch_idx] = zero_data_;
    return OB_SUCCESS;
  }
  int set_datum(const int64_t batch_idx, const ObDatum &datum) override
  {
    return append_datum(batch_idx, datum);
  }

  // --------- shallow copy interface --------- //
  int shallow_copy(ObIVector *src, const int64_t batch_size) override;
  int shallow_copy(const ObDatumVector &datum_vec, const int64_t batch_size) override;

  // --------- get interface --------- //
  int get_datum(const int64_t batch_idx, ObDatum &datum) override
  {
    datum.set_none();
    datum.ptr_ = ptrs_[batch_idx];
    datum.len_ = lens_[batch_idx];
    return OB_SUCCESS;
  }

  VIRTUAL_TO_STRING_KV(KPC_(discrete_vector));

protected:
  inline char *alloc_buf(const int64_t size)
  {
    return size > 0 ? static_cast<char *>(allocator_.alloc(size)) : zero_data_;
  }

protected:
  template <typename VEC>
  inline int _append_batch(const int64_t batch_idx, VEC *vec, const int64_t offset,
                           const int64_t size);
  template <bool IS_CONST>
  inline int _append_batch(const int64_t batch_idx, const ObDatum *datums, const int64_t offset,
                           const int64_t size);

  template <typename VEC>
  inline int _append_selective(const int64_t batch_idx, VEC *vec, const uint16_t *selector,
                               const int64_t size);
  template <bool IS_CONST>
  inline int _append_selective(const int64_t batch_idx, const ObDatum *datums,
                               const uint16_t *selector, const int64_t size);

  template <typename VEC>
  inline int _shallow_copy(VEC *vec, const int64_t batch_size);
  template <bool IS_CONST>
  inline int _shallow_copy(const ObDatum *datums, const int64_t batch_size);

protected:
  inline void set_vector(ObLength *lens, char **ptrs)
  {
    lens_ = lens;
    ptrs_ = ptrs;
    discrete_vector_->set_lens(lens);
    discrete_vector_->set_ptrs(ptrs);
  }

protected:
  ObDiscreteBase *const discrete_vector_;
  ObLength *vec_lens_;
  char **vec_ptrs_;
  ObLength *lens_;
  char **ptrs_;
  ObArenaAllocator allocator_;
  char zero_data_[0]; // 长度为0的指针
};

} // namespace storage
} // namespace oceanbase
