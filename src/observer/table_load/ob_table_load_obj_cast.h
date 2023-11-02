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

#pragma once

#include "common/object/ob_object.h"
#include "lib/ob_define.h"
#include "observer/table_load/ob_table_load_time_convert.h"
#include "share/object/ob_obj_cast.h"
#include "share/schema/ob_column_schema.h"
#include "observer/table_load/ob_table_load_struct.h"

namespace oceanbase
{
namespace observer
{
struct ObTableLoadNumberFastCtx
{
public:
  ObTableLoadNumberFastCtx() : is_fast_number_(false), integer_count_(0), decimal_count_(0) {}
  void reset()
  {
    is_fast_number_ = false;
    integer_count_ = 0;
    decimal_count_ = 0;
  }

public:
  bool is_fast_number_;
  int64_t integer_count_;
  int64_t decimal_count_;
};

struct ObTableLoadCastObjCtx
{
public:
  ObTableLoadCastObjCtx(const ObTableLoadParam &param, const ObTableLoadTimeConverter *time_cvrt, common::ObCastCtx *cast_ctx,
                        const bool is_need_check)
    : param_(param), time_cvrt_(time_cvrt), cast_ctx_(cast_ctx), is_need_check_(is_need_check){}

public:
  const ObTableLoadParam &param_;
  const ObTableLoadTimeConverter *time_cvrt_;
  common::ObCastCtx *cast_ctx_;
  ObTableLoadNumberFastCtx number_fast_ctx_;
  const bool is_need_check_;
};

class ObTableLoadObjCaster
{
  static const common::ObObj zero_obj;
  static const common::ObObj null_obj;

public:
  static int cast_obj(ObTableLoadCastObjCtx &cast_obj_ctx,
                      const share::schema::ObColumnSchemaV2 *column_schema,
                      const common::ObObj &src, common::ObObj &dst);

private:
  static int pad_column(const ObAccuracy accuracy, common::ObIAllocator &padding_alloc, common::ObObj &cell);
  static int convert_obj(const common::ObObjType &expect_type, const common::ObObj &src,
                         const common::ObObj *&dest);
  static int handle_string_to_enum_set(ObTableLoadCastObjCtx &cast_obj_ctx,
                                       const share::schema::ObColumnSchemaV2 *column_schema,
                                       const common::ObObj &src, common::ObObj &dst);
  static int string_to_enum(common::ObIAllocator &alloc, const common::ObObj &src,
                            const common::ObCollationType cs_type,
                            const common::ObCastMode cast_mode,
                            const common::ObIArray<ObString> &str_values, int &warning,
                            uint64_t &output_value);
  static int string_to_set(common::ObIAllocator &alloc, const common::ObObj &src,
                           const common::ObCollationType cs_type,
                           const common::ObCastMode cast_mode,
                           const common::ObIArray<ObString> &str_values, int &warning,
                           uint64_t &output_value);
  static int cast_obj_check(ObTableLoadCastObjCtx &cast_obj_ctx,
                            const share::schema::ObColumnSchemaV2 *column_schema,
                            common::ObObj &obj);
  static int to_type(const common::ObObjType &expect_type, const share::schema::ObColumnSchemaV2 *column_schema, ObTableLoadCastObjCtx &cast_obj_ctx,
                     const common::ObAccuracy &accuracy, const common::ObObj &src, common::ObObj &dst);
  static int string_datetime_oracle(const common::ObObjType expect_type,
                                    common::ObObjCastParams &params, const common::ObObj &in,
                                    common::ObObj &out, const common::ObCastMode cast_mode,
                                    const ObTableLoadTimeConverter &time_cvrt);

  // fast path for numbertype cast
  inline static int number_fast_from(const char *str, const int64_t length,
                                     common::ObIAllocator *allocator, ObNumberDesc &d,
                                     uint32_t *&digits, const common::ObAccuracy &accuracy,
                                     ObTableLoadNumberFastCtx &number_fast_ctx)
  {
    int ret = OB_SUCCESS;
    static const uint32_t ROUND_POWS[] = {0,      10,      100,      1000,      10000,
                                          100000, 1000000, 10000000, 100000000, 1000000000};

    if (length > number::ObNumber::DIGIT_LEN) {
      return OB_EAGAIN;
    }

    const char *s = str;

    d.desc_ = 0;
    d.sign_ = number::ObNumber::POSITIVE;
    d.exp_ = (uint8_t)(number::ObNumber::EXP_ZERO);
    if (*s == '+') {
      s++;
    } else if (*s == '-') {
      s++;
      d.sign_ = number::ObNumber::NEGATIVE;
    }

    uint32_t n1 = 0;
    int64_t integer_count = 0;
    while (s < str + length) {
      if (*s >= '0' && *s <= '9') {
        n1 *= 10;
        n1 += (*s - '0');
        s++;
        integer_count++;
      } else if (*s == '.') {
        s++;
        break;
      } else {
        return OB_EAGAIN;
      }
    }
    number_fast_ctx.integer_count_ = integer_count;

    uint32_t n2 = 0;
    int64_t decimal_count = 0;
    while (s < str + length) {
      if (*s >= '0' && *s <= '9') {
        n2 *= 10;
        n2 += (*s - '0');
        s++;
        decimal_count++;
      } else {
        return OB_EAGAIN;
      }
    }
    number_fast_ctx.decimal_count_ = decimal_count;

    d.len_++;
    if (n2 > 0) {
      n2 *= ROUND_POWS[(number::ObNumber::DIGIT_LEN - decimal_count)];
      d.len_++;
    }

    digits = (uint32_t *)(allocator->alloc(sizeof(uint32_t) * d.len_));
    if (digits == nullptr) {
      return OB_EAGAIN;
    }
    digits[0] = n1;
    if (n2 > 0) {
      digits[1] = n2;
    }

    //normalize
    if (d.len_ == 2) {
      if (digits[0] == 0) {
        d.len_ = 1;
        digits[0] = digits[1];
        if (d.sign_ == number::ObNumber::POSITIVE) {
          d.exp_ -= 1;
        } else {
          d.exp_ += 1;
        }
      } else if (digits[1] == 0) {
        d.len_ = 1;
      }
    }

    if (d.len_ == 1) {
      if (digits[0] == 0) {
        d.len_ = 0;
        d.exp_ = 0;
      }
    }

    number_fast_ctx.is_fast_number_ = true;
    return ret;
  }

  // fast path for numbertype cast result check
  inline static int number_fast_cast_check(ObTableLoadNumberFastCtx &number_fast_ctx,
                                           common::ObObj &obj, const common::ObAccuracy &accuracy)
  {
    int ret = OB_SUCCESS;
    static const uint32_t ROUND_POWS[] = {0,      10,      100,      1000,      10000,
                                          100000, 1000000, 10000000, 100000000, 1000000000};

    ObPrecision precision = accuracy.get_precision();
    ObScale scale = accuracy.get_scale();
    if (lib::is_oracle_mode()) {
      if (OB_MAX_NUMBER_PRECISION >= precision && precision >= OB_MIN_NUMBER_PRECISION &&
          number::ObNumber::MAX_SCALE >= scale && scale >= number::ObNumber::MIN_SCALE) {
        // do noting
      } else {
        return OB_EAGAIN;
      }
    } else {
      if (precision >= scale && number::ObNumber::MAX_PRECISION >= precision &&
          precision >= OB_MIN_DECIMAL_PRECISION && number::ObNumber::MAX_SCALE >= scale &&
          scale >= 0) {
        // do noting
      } else {
        return OB_EAGAIN;
      }
    }

    number::ObNumber nmb = obj.get_number();
    ObNumberDesc d = nmb.d_;
    uint32_t *digits = nmb.get_digits();
    if (d.exp_ == (uint8_t)(number::ObNumber::EXP_ZERO) && d.len_ > 0 && d.len_ <= 2) {
      // do nothing
    } else {
      return OB_EAGAIN;
    }
    uint32_t n1 = 0, n2 = 0;
    int64_t integer_count = number_fast_ctx.integer_count_;
    int64_t decimal_count = number_fast_ctx.decimal_count_;
    if (d.len_ > 0) {
      n1 = digits[0];
    } else {
      return OB_EAGAIN;
    }

    if (d.len_ > 1) {
      n2 = digits[1];
    }

    const int32_t decimal_len = precision - scale;
    // precision-scale>0时，整数部分的位数不能超过precision-scale
    // precision-scale<=0时，整数部分必须为0，小数点后-(precision-scale)位也必须为0
    if ((decimal_len > 0 && integer_count <= decimal_len) ||
        (decimal_len <= 0 && n1 == 0 &&
         n2 / ROUND_POWS[(number::ObNumber::DIGIT_LEN + decimal_len)] == 0)) {
      // scale>0时，精度限制在小数点后scale位
      if (scale > 0) {
        if (n2 > 0 && decimal_count > scale) {
          return OB_EAGAIN;
        }
        // scale=0时，小数部分被舍去
      } else if (scale == 0) {
        if (n2 > 0) {
          return OB_EAGAIN;
        }
        // scale<0时，小数部分被舍去且精度限制在小数点前-scale位
      } else {
        if (n2 > 0 || n1 % ROUND_POWS[-scale] == 0) {
          return OB_EAGAIN;
        }
      }
    } else {
      return OB_EAGAIN;
    }
    return ret;
  }
};

} // namespace observer
} // namespace oceanbase
