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
#ifndef OCEANBASE_SHARE_VECTOR_OB_FIXED_LENGTH_BASE_H_
#define OCEANBASE_SHARE_VECTOR_OB_FIXED_LENGTH_BASE_H_

#include "share/vector/ob_bitmap_null_vector_base.h"

namespace oceanbase
{
namespace common
{

class ObFixedLengthBase: public ObBitmapNullVectorBase
{
public:
  ObFixedLengthBase(sql::ObBitVector *nulls, ObLength len, char *data)
    : ObBitmapNullVectorBase(nulls), len_(len), data_(data)
  {}

  static const VectorFormat FORMAT = VEC_FIXED;
  DECLARE_TO_STRING;
  inline char *get_data() { return data_; };
  OB_INLINE void set_data(char *data) { data_ = data; }
  inline const char *get_data() const { return data_; };
  inline ObLength get_length() const { return len_; };
  // For coding convenience
  inline ObLength get_length(const int64_t idx) const override {
    UNUSED(idx);
    return len_;
  }
  inline void from_fixed_vector(const bool has_null, const sql::ObBitVector &nulls,
                         const int64_t fixed_len, const int64_t start_idx,
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
    len_ = static_cast<int32_t> (fixed_len);
    data_ = data + (fixed_len * start_idx);
  }
  void set_datum(const int64_t idx, const ObDatum &datum) {
    if (datum.is_null()) {
      set_null(idx);
    } else {
      set_payload(idx, datum.ptr_, datum.len_);
    }
  }
  virtual void to_rows(const sql::RowMeta &row_meta,
                       sql::ObCompactRow **stored_rows,
                       const uint16_t selector[],
                       const int64_t size,
                       const int64_t col_idx) const override final;

  virtual void to_rows(const sql::RowMeta &row_meta, sql::ObCompactRow **stored_rows,
                       const int64_t size, const int64_t col_idx) const override final;

  inline void from(ObLength len, char *data)
  {
    len_ = len;
    data_ = data;
  }

protected:
  ObLength len_; // each cell value length
  char *data_;
};

}
}
#endif // OCEANBASE_SHARE_VECTOR_OB_FIXED_LENGTH_BASE_H_
