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

#ifndef OB_WIDE_INTEGER_STR_FUNCS_H_
#define OB_WIDE_INTEGER_STR_FUNCS_H_

namespace oceanbase
{
namespace common
{
struct ObIAllocator;
namespace wide
{
struct str_helper
{
  // NOTE: The maximum and minimum values are not included in the range
  static const int512_t DEC_INT32_MIN;
  static const int512_t DEC_INT32_MAX;
  static const int512_t DEC_INT64_MIN;
  static const int512_t DEC_INT64_MAX;
  static const int512_t DEC_INT128_MIN;
  static const int512_t DEC_INT128_MAX;
  static const int512_t DEC_INT256_MIN;
  static const int512_t DEC_INT256_MAX;
  static const int512_t DEC_INT512_MIN;
  static const int512_t DEC_INT512_MAX;
  static inline void prepend_chars(char *buf, int64_t org_char_size, int64_t offset, const char c)
  {
    char *src_last = buf + org_char_size;
    char *dst_last = src_last + offset;
    while(org_char_size-- > 0) {
      *--dst_last = *--src_last;
    }
    while (offset-- > 0) {
      buf[offset] = c;
    }
  }
  static inline void append_chars(char *buf, int64_t org_char_size, int64_t cnt, const char c)
  {
    while (cnt-- > 0) {
      buf[org_char_size++] = c;
    }
  }

  static inline bool is_zero(const ObDecimalInt *decint, const int32_t int_bytes)
  {
    bool ret = false;
    switch (int_bytes) {
    case 4: ret = *(reinterpret_cast<const int32_t *>(decint)) == 0; break;
    case 8: ret = *(reinterpret_cast<const int64_t *>(decint)) == 0; break;
    case 16: {
      const int128_t *v = reinterpret_cast<const int128_t *>(decint);
      ret = int128_t::_impl::is_zero(*v);
      break;
    }
    case 32: {
      const int256_t *v = reinterpret_cast<const int256_t *>(decint);
      ret = int256_t::_impl::is_zero(*v);
      break;
    }
    case 64: {
      const int512_t *v = reinterpret_cast<const int512_t *>(decint);
      ret = int512_t::_impl::is_zero(*v);
      break;
    }
    default:
      COMMON_LOG(WARN, "invalid integer width", K(int_bytes));
    }
    return ret;
  }
  static inline bool  is_negative(const ObDecimalInt *decint, const int32_t int_bytes)
  {
    bool ret = false;
    switch (int_bytes) {
    case 4: ret = *(reinterpret_cast<const int32_t *>(decint)) < 0; break;
    case 8: ret = *(reinterpret_cast<const int64_t *>(decint)) < 0; break;
    case 16: {
      const int128_t *v = reinterpret_cast<const int128_t *>(decint);
      ret = int128_t::_impl::is_negative(*v);
      break;
    }
    case 32: {
      const int256_t *v = reinterpret_cast<const int256_t *>(decint);
      ret = int256_t::_impl::is_negative(*v);
      break;
    }
    case 64: {
      const int512_t *v = reinterpret_cast<const int512_t *>(decint);
      ret = int512_t::_impl::is_negative(*v);
      break;
    }
    default:
      COMMON_LOG(WARN, "invalid integer width", K(int_bytes));
    }
    return ret;
  }
  static inline unsigned calc_copy_size(const int512_t &x)
  {
    unsigned copy_sz = 0;
    if (x > DEC_INT32_MIN && x < DEC_INT32_MAX) {
      copy_sz = sizeof(int32_t);
    } else if (x > DEC_INT64_MIN && x < DEC_INT64_MAX) {
      copy_sz = sizeof(int64_t);
    } else if (x > DEC_INT128_MIN && x < DEC_INT128_MAX) {
      copy_sz = sizeof(int128_t);
    } else if (x > DEC_INT256_MIN && x < DEC_INT256_MAX) {
      copy_sz = sizeof(int256_t);
    } else {
      copy_sz = sizeof(int512_t);
    }
    return copy_sz;
  }
};

template <unsigned Bits, typename Signed>
int to_string(const ObWideInteger<Bits, Signed> &self, char *buf, const int64_t buf_len, int64_t &pos)
{
  static const uint64_t constexpr DIGITS10_BASE = 10000000000000000000ULL; // 10^19
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(buf_len <= 0)) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    if (ObWideInteger<Bits, Signed>::_impl::is_negative(self)
        && OB_FAIL(databuff_printf(buf, buf_len, pos, "-"))) {
      // do nothing
    }
    if (OB_SUCC(ret)) {
      ObWideInteger<Bits, unsigned> num = ObWideInteger<Bits, Signed>::_impl::make_positive(self);
      ObWideInteger<Bits, unsigned> quo;
      ObWideInteger<Bits, unsigned> remain;
      uint64_t digits[ObWideInteger<Bits, Signed>::ITEM_COUNT * 3] = {0};
      unsigned digit_sz = 0;
      while (OB_SUCC(ret) && num > 0) {
        ObWideInteger<Bits, unsigned>::_impl::template divide<IgnoreOverFlow>(num, DIGITS10_BASE,
                                                                              quo, remain);
        digits[digit_sz++] = remain.items_[0];
        num = quo;
      }
      if (digit_sz == 0) {
        digits[digit_sz++] = 0;
      }
      for (int i = digit_sz - 1; OB_SUCC(ret) && i >= 0; i--) {
        if (i == digit_sz - 1) {
          ret = databuff_printf(buf, buf_len, pos, "%lu", digits[i]);
        } else {
          ret = databuff_printf(buf, buf_len, pos, "%.019lu", digits[i]);
        }
      }
    }
  }
  return ret;
}

int from_string(const char *buf, const int64_t buf_len, ObIAllocator &allocator,
             int16_t &scale, int16_t &precision, int32_t &val_len, ObDecimalInt *&decint);

int to_string(const ObDecimalInt *decint, const int32_t int_bytes, const int64_t scale, char *buf,
              const int64_t buf_len, int64_t &pos, const bool need_to_sci = false);

template<unsigned Bits, typename Signed>
int64_t ObWideInteger<Bits, Signed>::to_string(char *buffer, const int64_t buffer_len) const
{
  int64_t pos = 0;
  int ret = OB_SUCCESS;
  if (OB_FAIL(wide::to_string(*this, buffer, buffer_len, pos))) {
    COMMON_LOG(WARN, "to_string failed", K(ret));
  }
  return pos;
}

} // end namespace wide
} // end namespace common
} // end namespace oceanbase
#endif // !OB_WIDE_INTEGER_STR_FUNCS_H_
