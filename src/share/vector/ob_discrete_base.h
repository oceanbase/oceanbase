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

#ifndef OCEANBASE_SHARE_VECTOR_OB_DISCRETE_BASE_H_
#define OCEANBASE_SHARE_VECTOR_OB_DISCRETE_BASE_H_

#include "share/vector/ob_bitmap_null_vector_base.h"

namespace oceanbase
{
namespace common
{

class ObDiscreteBase : public ObBitmapNullVectorBase
{
public:
  ObDiscreteBase(int32_t *lens, char **ptrs, sql::ObBitVector *nulls)
    : ObBitmapNullVectorBase(nulls), lens_(lens), ptrs_(ptrs)
  {}

  static const VectorFormat FORMAT = VEC_DISCRETE;
  DECLARE_TO_STRING;
  inline ObLength *get_lens() { return lens_; }
  inline const ObLength *get_lens() const { return lens_; }
  OB_INLINE void set_lens(ObLength *lens) { lens_ = lens; }
  inline char **get_ptrs() const { return ptrs_; }
  OB_INLINE void set_ptrs(char **ptrs) { ptrs_ = ptrs; }
  OB_INLINE void append_rows_multiple_times(const sql::ObBitVector *src_nulls,
                              char **src_ptrs, ObLength *src_lens,
                              const int64_t src_start_idx,
                              const int64_t src_end_idx,
                              const int64_t times,
                              const int64_t dst_start_idx)
  {
    ObLength *src_len_array = src_lens + src_start_idx;
    char **src_data_ptr = src_ptrs + src_start_idx;
    ObLength *dst_len_array = lens_ + dst_start_idx;
    char **dst_data_ptr = ptrs_ + dst_start_idx;
    const int64_t interval = src_end_idx - src_start_idx;
    if (interval > MEMCPY_THRESHOLD) {
      for (int64_t i = 0; i < times; ++i) {
        MEMCPY(dst_data_ptr + i * interval, src_data_ptr, sizeof(char *) * interval);
        MEMCPY(dst_len_array + i * interval, src_len_array, sizeof(ObLength) * interval);
      }
    } else {
      for (int64_t i = 0; i < interval; ++i) {
        for (int64_t j = 0; j < times; ++j) {
          dst_data_ptr[i + j * interval] = src_data_ptr[i];
          dst_len_array[i + j * interval] = src_len_array[i];
        }
      }
    }
    repeat_nulls_in_append_rows(src_nulls, src_start_idx, src_end_idx, dst_start_idx, times);
  }

  virtual int32_t to_rows(const sql::RowMeta &row_meta,
                       sql::ObCompactRow **stored_rows,
                       const uint16_t selector[],
                       const int64_t size,
                       const int64_t col_idx) const override final;

  virtual int to_rows(const sql::RowMeta &row_meta, sql::ObCompactRow **stored_rows,
                       const int64_t size, const int64_t col_idx) const override final;

protected:
  ObLength *lens_;
  char **ptrs_;
};

}
}
#endif //OCEANBASE_SHARE_VECTOR_OB_DISCRETE_BASE_H_
