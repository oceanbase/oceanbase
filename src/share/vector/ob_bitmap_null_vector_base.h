/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_VECTOR_OB_BITMAP_NULL_VECTOR_BASE_H_
#define OCEANBASE_SHARE_VECTOR_OB_BITMAP_NULL_VECTOR_BASE_H_

#include "share/vector/ob_vector_base.h"
#include "share/vector/ob_uniform_base.h"
#include "sql/engine/ob_bit_vector.h"

namespace oceanbase
{
namespace common
{

class ObBitmapNullVectorBase: public ObVectorBase
{
public:
  ObBitmapNullVectorBase(sql::ObBitVector *nulls) :
    ObVectorBase(), nulls_(nulls), has_null_(false), is_batch_ascii_(UNKNOWN)
  {}

  // Returning true is meaningless, returning false indicates that there is indeed no null.
  OB_INLINE bool has_null() const override final { return has_null_; };
  OB_INLINE void set_has_null() override final { has_null_ = true; };
  inline void set_has_null(bool flag) { has_null_ = flag; };
  OB_INLINE void reset_has_null() override final { has_null_ = false; };

  OB_INLINE bool is_batch_ascii() const override final { return is_batch_ascii_ == ASCII; }
  OB_INLINE void reset_is_batch_ascii() override final { is_batch_ascii_ = UNKNOWN; }
  OB_INLINE void set_is_batch_ascii() override final { is_batch_ascii_ = ASCII; }
  inline void set_is_batch_ascii(CHARSET_FLAG flag) { is_batch_ascii_ = flag; }
  OB_INLINE void set_has_non_ascii() override final { is_batch_ascii_ = NON_ASCII; }
  inline sql::ObBitVector *get_nulls() { return nulls_; }
  OB_INLINE void set_nulls(sql::ObBitVector *nulls) { nulls_ = nulls; }
  inline const sql::ObBitVector *get_nulls() const { return nulls_; }
  inline uint16_t get_flag() const { return flag_; }
  inline void reset_flag()
  {
    has_null_ = false;
    is_batch_ascii_ = UNKNOWN;
  }

  OB_INLINE bool is_null(const int64_t idx) const override final { return nulls_->at(idx); }
  OB_INLINE void set_null(const int64_t idx) override final {
    nulls_->set(idx);
    set_has_null();
  };
  OB_INLINE void unset_null(const int64_t idx) override final {
    nulls_->unset(idx);
  };

  inline void from(sql::ObBitVector *nulls, const uint16_t flag)
  {
    nulls_ = nulls;
    flag_ = flag;
  }

  OB_INLINE int from_vector_bitmap_null(const ObIVector *src_vec, const sql::ObBitVector *skip, const int64_t start, const int64_t end) {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(src_vec)) {
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(from_vector_base(src_vec))) {
      SQL_ENG_LOG(WARN, "failed to copy base", K(ret));
    } else {
      const VectorFormat src_format = src_vec->get_format();

      if (src_format == VEC_UNIFORM || src_format == VEC_UNIFORM_CONST) {
        ret = copy_null_from_uniform(static_cast<const ObUniformBase *>(src_vec), src_format, skip, start, end);
      } else if (src_format == VEC_FIXED || src_format == VEC_DISCRETE || src_format == VEC_CONTINUOUS) {
        ret = copy_null_from_bitmap(static_cast<const ObBitmapNullVectorBase *>(src_vec), skip, start, end);
      } else {
        ret = OB_ERR_UNEXPECTED;
        SQL_ENG_LOG(WARN, "unknown vector format", K(src_format));
      }
    }
    return ret;
  }

private:
  OB_INLINE int copy_null_from_bitmap(const ObBitmapNullVectorBase *src, const sql::ObBitVector *skip, int64_t start, int64_t end) {
    int ret = OB_SUCCESS;
    sql::ObBitVector *dst_nulls = get_nulls();
    const sql::ObBitVector *src_nulls = src->get_nulls();
    if (skip == nullptr) {
      dst_nulls->deep_copy(*src_nulls, start, end);
      if (start == 0 && end == get_max_row_cnt()) {
        src->has_null() ? set_has_null() : reset_has_null();
        is_batch_ascii_ = src->is_batch_ascii_;
      } else {
        set_has_null();
        if (src->is_batch_ascii_ == NON_ASCII) {
          is_batch_ascii_ = NON_ASCII;
        }
      }
    } else {
      for (int64_t i = start; i < end; ++i) {
        if (!skip->at(i)) {
          src_nulls->at(i) ? dst_nulls->set(i) : dst_nulls->unset(i);
        }
      }
      if (src->has_null()) {
        set_has_null();
      }
      if (src->is_batch_ascii_ == NON_ASCII) {
        is_batch_ascii_ = NON_ASCII;
      }
    }
    return ret;
  }

  OB_INLINE int copy_null_from_uniform(const ObUniformBase *src, VectorFormat fmt, const sql::ObBitVector *skip, int64_t start, int64_t end) {
    int ret = OB_SUCCESS;
    const ObDatum *datums = src->get_datums();
    sql::ObBitVector *nulls = get_nulls();
    const bool is_const = (fmt == VEC_UNIFORM_CONST);
    bool has_null = false;
    for (int64_t i = start; i < end; ++i) {
      if (skip && skip->at(i)) {
        continue;
      }
      if (datums[is_const ? 0 : i].is_null()) {
        nulls->set(i);
        has_null = true;
      } else {
        nulls->unset(i);
      }
    }
    if (start == 0 && end == get_max_row_cnt()) {
      has_null ? set_has_null() : reset_has_null();
    } else if (has_null) {
      set_has_null();
    }
    return ret;
  }

public:

  OB_INLINE void repeat_nulls_in_append_rows(const sql::ObBitVector *src_nulls,
                                          const int64_t src_start_idx,
                                          const int64_t src_end_idx,
                                          const int64_t dst_start_idx,
                                          const int64_t times) {
    const int64_t interval = src_end_idx - src_start_idx;
    for (int64_t i = src_start_idx; i < src_end_idx; ++i) {
      if (src_nulls->at(i)) {
        for (int64_t j = 0; j < times; ++j) {
          set_null(dst_start_idx + i - src_start_idx + j * interval);
        }
      } else {
        for (int64_t j = 0; j < times; ++j) {
          unset_null(dst_start_idx + i - src_start_idx + j * interval);
        }
      }
    }
  }

  // Note: if need to add new flag or change the default value of an existing flag,
  // please make sure to synchronize this function accordingly.
  static uint16_t get_default_flag(bool has_null)
  {
    uint16_t flag = 0;
    flag |= has_null;
    flag |= (UNKNOWN << 1);
    return flag;
  }

protected:
  sql::ObBitVector *nulls_;
  union {
		struct {
			uint16_t has_null_:1;
			// is charset ascii
      CHARSET_FLAG is_batch_ascii_:2;
		};
		uint16_t flag_;
	};
};

}
}
#endif // OCEANBASE_SHARE_VECTOR_OB_BITMAP_NULL_VECTOR_BASE_H_
