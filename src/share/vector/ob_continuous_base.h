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

#ifndef OCEANBASE_SHARE_VECTOR_OB_CONTINUOUS_BASE_H_
#define OCEANBASE_SHARE_VECTOR_OB_CONTINUOUS_BASE_H_

#include "share/vector/ob_bitmap_null_vector_base.h"

namespace oceanbase
{
namespace common
{

class ObContinuousBase: public ObBitmapNullVectorBase
{
public:
  ObContinuousBase(uint32_t *offsets, char *data, sql::ObBitVector *nulls)
    : ObBitmapNullVectorBase(nulls), offsets_(offsets), data_(data)
  {}
  inline uint32_t *get_offsets() { return offsets_; };
  inline const uint32_t *get_offsets() const { return offsets_; };
  OB_INLINE void set_offsets(uint32_t *offsets) { offsets_ = offsets; }
  inline char *get_data() { return data_; }
  inline const char *get_data() const { return data_; }
  inline void set_data(char *ptr) { data_ = ptr; }
  inline void from_continuous_vector(const bool has_null, const sql::ObBitVector &nulls,
                                     uint32_t *offsets, const int64_t start_idx,
                                     const int64_t read_rows, char *data)
  {
    UNUSED(has_null);
    has_null_ = false;
    nulls_->reset(read_rows);
    for (int64_t i = 0; i < read_rows; ++i) {
      if (nulls.at(start_idx + i)) {
        nulls_->set(i);
        has_null_ = true;
      }
    }
    offsets_ = offsets + start_idx;
    data_ = data;
  }
  virtual void to_rows(const sql::RowMeta &row_meta,
                       sql::ObCompactRow **stored_rows,
                       const uint16_t selector[],
                       const int64_t size,
                       const int64_t col_idx) const override final;

  virtual void to_rows(const sql::RowMeta &row_meta, sql::ObCompactRow **stored_rows,
                       const int64_t size, const int64_t col_idx) const override final;
  inline void from(uint32_t *offsets, char *data)
  {
    offsets_ = offsets;
    data_ = data;
  }

protected:
  uint32_t *offsets_;
  char *data_;
};

}
}
#endif // OCEANBASE_SHARE_VECTOR_OB_CONTINUOUS_BASE_H_
