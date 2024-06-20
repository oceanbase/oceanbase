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

#ifndef OCEANBASE_SHARE_VECTOR_OB_BITMAP_NULL_VECTOR_BASE_H_
#define OCEANBASE_SHARE_VECTOR_OB_BITMAP_NULL_VECTOR_BASE_H_

#include "share/vector/ob_vector_base.h"
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
