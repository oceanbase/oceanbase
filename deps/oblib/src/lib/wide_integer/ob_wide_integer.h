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

#ifndef OB_WIDE_INTEGER_H_
#define OB_WIDE_INTEGER_H_

#include <limits.h>
#include <type_traits>
#include <initializer_list>

#include "lib/alloc/alloc_assist.h"
#include "common/object/ob_obj_type.h"
#include "lib/number/ob_number_v2.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace lib
{
  struct ObMemAttr;
} // end namespace lib
namespace common
{
struct ObObj;
namespace wide
{
enum
{
  IgnoreOverFlow = 0,
  CheckOverFlow   = 1,
};

template <unsigned Bits, typename Signed = signed>
struct ObWideInteger
{
  using dw_type          = unsigned __int128;

  constexpr static const unsigned   BYTE_COUNT          = Bits / CHAR_BIT;
  constexpr static const unsigned   ITEM_COUNT          = BYTE_COUNT / sizeof(uint64_t);
  constexpr static const unsigned   BASE_BITS           = sizeof(uint64_t) * CHAR_BIT;
  constexpr static const unsigned   BITS                = Bits;
  constexpr static const uint64_t   SIGN_BIT_MASK       = static_cast<uint64_t>(INT64_MIN);
  constexpr static const uint64_t   BASE_MAX            = UINT64_MAX;

  ObWideInteger() {
    MEMSET(items_, 0, sizeof(uint64_t) * ITEM_COUNT);
  }
  ObWideInteger(const ObWideInteger<Bits, Signed> &lhs) {
    MEMCPY(items_, lhs.items_, ITEM_COUNT * sizeof(uint64_t));
  }
  ObWideInteger(std::initializer_list<uint64_t> items)
  {
    int copy_items = items.size() < ITEM_COUNT ? items.size() : ITEM_COUNT;
    MEMCPY(items_, items.begin(), sizeof(uint64_t) * copy_items);
  }
  template<typename T>
  ObWideInteger(const T &rhs)
  {
    *this = rhs;
  }

  template <unsigned Bits2, typename Signed2>
  ObWideInteger<Bits, Signed> &operator=(const ObWideInteger<Bits2, Signed2> &rhs) {
    constexpr const unsigned copy_bits = (Bits < Bits2 ? Bits : Bits2);
    constexpr const unsigned copy_bytes = copy_bits / CHAR_BIT;
    constexpr const unsigned copy_items = copy_bytes / sizeof(uint64_t);

    for (unsigned i = 0; i < copy_items; i++) {
      items_[i] = rhs.items_[i];
    }
    if (Bits > Bits2) {
      if (_impl::is_negative(rhs)) {
        for (unsigned i = copy_items; i < ITEM_COUNT; i++) {
          items_[i] = UINT64_MAX;
        }
      } else {
        for (unsigned i = copy_items; i < ITEM_COUNT; i++) {
          items_[i] = 0;
        }
      }
    }
    return *this;
  }

  template<typename T>
  ObWideInteger<Bits, Signed> &operator=(const T &rhs)
  {
    if (sizeof(T) < BYTE_COUNT) {
      items_[0] = rhs;
      if (_impl::is_negative(rhs)) {
        for (unsigned i = 1; i < ITEM_COUNT; i++) {
          items_[i] = UINT64_MAX;
        }
      } else {
        for (unsigned i = 1; i < ITEM_COUNT; i++) {
          items_[i] = 0;
        }
      }
    } else {
      for (unsigned i = 0; i < sizeof(T) / sizeof(uint64_t); i++) {
        items_[i] = _impl::get_item(rhs, i);
      }
      if (_impl::is_negative(rhs)) {
        for (unsigned i = sizeof(T) / sizeof(uint64_t); i < ITEM_COUNT; i++) {
          items_[i] = UINT64_MAX;
        }
      } else {
        for (unsigned i = sizeof(T) / sizeof(uint64_t); i < ITEM_COUNT; i++) {
          items_[i] = 0;
        }
      }
    }
    return *this;
  }

  operator int64_t() const
  {
    return static_cast<int64_t>(items_[0]);
  }

  template<int check_overflow, typename T>
  int multiply(const T &rhs, ObWideInteger<Bits, Signed> &res) const
  {
    return _impl::template multiply<check_overflow>(*this, rhs, res);
  }

  template<int check_overflow, typename T>
  int divide(const T &rhs, ObWideInteger<Bits, Signed> &res) const
  {
    ObWideInteger<Bits, Signed> rem;
    return _impl::template divide<check_overflow>(*this, rhs, res, rem);
  }

  template<typename T>
  int bitwise_and(const T &rhs, ObWideInteger<Bits, Signed> &res) const
  {
    return _impl::template bitwise_and(*this, rhs, res);
  }

  template<int check_overflow, typename T>
  int percent(const T &rhs, ObWideInteger<Bits, Signed> &rem) const
  {
    ObWideInteger<Bits, Signed> res;
    return _impl::template divide<check_overflow>(*this, rhs, res, rem);
  }

  template<int check_overflow, typename T>
  int add(const T &rhs, ObWideInteger<Bits, Signed> &res) const
  {
    bool l_neg = false, r_neg = false;
    if (check_overflow) {
      l_neg = _impl::is_negative(*this);
      r_neg = _impl::is_negative(rhs);
    }
    if (_impl::is_negative(rhs)) {
      _impl::template sub<check_overflow>(*this, -rhs, res);
    } else {
      _impl::template add<check_overflow>(*this, rhs, res);
    }
    if (check_overflow && _impl::add_overflow(l_neg, r_neg, _impl::is_negative(res))) {
      return OB_OPERATE_OVERFLOW;
    }
    return OB_SUCCESS;
  }

  template<int check_overflow, typename T>
  int sub(const T &rhs, ObWideInteger<Bits, Signed> &res) const
  {
    bool l_neg = false, r_neg = false;
    if (check_overflow) {
      l_neg = _impl::is_negative(*this);
      r_neg = _impl::is_negative(rhs);
    }
    if (_impl::is_negative(rhs)) {
      _impl::template add<check_overflow>(*this, -rhs, res);
    } else {
      _impl::template sub<check_overflow>(*this, rhs, res);
    }
    if (check_overflow && _impl::sub_overflow(l_neg, r_neg, _impl::is_negative(res))) {
      return OB_OPERATE_OVERFLOW;
    }
    return OB_SUCCESS;
  }

  template<typename T>
  int cmp(const T &rhs) const
  {
    return _impl::cmp(*this, rhs);
  }

  ObWideInteger<Bits, Signed> &shift_left(unsigned n) const
  {
    _impl::shift_left(*this, *this, n);
    return *this;
  }

  ObWideInteger<Bits, Signed> &shift_right(unsigned n) const
  {
    _impl::shift_right(*this, *this, n);
    return *this;
  }

  ObWideInteger<Bits, Signed>& operator++()
  {
    _impl::template add<IgnoreOverFlow>(*this, 1, *this);
    return *this;
  }

  template<typename T>
  ObWideInteger<Bits, Signed>& operator+=(const T &rhs)
  {
    if (ObWideInteger<Bits, Signed>::_impl::is_negative(rhs)) {
      ObWideInteger<Bits, Signed>::_impl::template sub<IgnoreOverFlow>(*this, -rhs, *this);
    } else {
      ObWideInteger<Bits, Signed>::_impl::template add<IgnoreOverFlow>(*this, rhs, *this);
    }
    return *this;
  }

  ObWideInteger<Bits, Signed>& operator--()
  {
    _impl::template sub<IgnoreOverFlow>(*this, 1, *this);
    return *this;
  }

  template<typename T>
  ObWideInteger<Bits, Signed>& operator-=(const T &rhs)
  {
    if (ObWideInteger<Bits, Signed>::_impl::is_negative(rhs)) {
      ObWideInteger<Bits, Signed>::_impl::template add<IgnoreOverFlow>(*this, -rhs, *this);
    } else {
      ObWideInteger<Bits, Signed>::_impl::template sub<IgnoreOverFlow>(*this, rhs, *this);
    }
    return *this;
  }

  ObWideInteger<Bits, Signed> operator++(int)
  {
    ObWideInteger<Bits, Signed> ret = *this;
    _impl::template add<IgnoreOverFlow>(*this, 1, *this);
    return ret;
  }

  ObWideInteger<Bits, Signed> operator--(int)
  {
    ObWideInteger<Bits, Signed> ret = *this;
    _impl::template sub<IgnoreOverFlow>(*this, 1, *this);
    return ret;
  }

  // helpers
  bool is_negative() const
  {
    return _impl::is_negative(*this);
  }

  uint64_t items_[ITEM_COUNT];

  struct _impl;
  DECLARE_TO_STRING;
private:
  template <unsigned Bits2, typename Signed2>
  friend class ObWideInteger;
};

// unary operators
template<unsigned Bits, typename Signed, typename T>
bool operator==(const ObWideInteger<Bits, Signed> &lhs, const T &rhs)
{
  return ObWideInteger<Bits, Signed>::_impl::cmp(lhs, rhs) == 0;
}

template<unsigned Bits, typename Signed, typename T>
bool operator==(const T &lhs, const ObWideInteger<Bits, Signed> &rhs)
{
  return ObWideInteger<Bits, Signed>::_impl::cmp(rhs, lhs) == 0;
}

template<unsigned Bits1, typename Signed1, unsigned Bits2, typename Signed2>
bool operator==(const ObWideInteger<Bits1, Signed1> &lhs, const ObWideInteger<Bits2, Signed2> &rhs)
{
  return ObWideInteger<Bits1, Signed1>::_impl::cmp(lhs, rhs) == 0;
}

template<unsigned Bits, typename Signed, typename T>
bool operator>(const ObWideInteger<Bits, Signed> &lhs, const T &rhs)
{
  return ObWideInteger<Bits, Signed>::_impl::cmp(lhs, rhs) > 0;
}

template<unsigned Bits, typename Signed, typename T>
bool operator>(const T &lhs, const ObWideInteger<Bits, Signed> &rhs)
{
  return ObWideInteger<Bits, Signed>::_impl::cmp(rhs, lhs) < 0;
}

template<unsigned Bits1, typename Signed1, unsigned Bits2, typename Signed2>
bool operator>(const ObWideInteger<Bits1, Signed1> &lhs, const ObWideInteger<Bits2, Signed2> &rhs)
{
  return ObWideInteger<Bits1, Signed2>::_impl::cmp(lhs, rhs) > 0;
}

template<unsigned Bits, typename Signed, typename T>
bool operator<(const ObWideInteger<Bits, Signed> &lhs, const T &rhs)
{
  return ObWideInteger<Bits, Signed>::_impl::cmp(lhs, rhs) < 0;
}

template<unsigned Bits, typename Signed, typename T>
bool operator<(const T &lhs, const ObWideInteger<Bits, Signed> &rhs)
{
  return ObWideInteger<Bits, Signed>::_impl::cmp(rhs, lhs) > 0;
}

template<unsigned Bits1, typename Signed1, unsigned Bits2, typename Signed2>
bool operator<(const ObWideInteger<Bits1, Signed1> &lhs, const ObWideInteger<Bits2, Signed2> &rhs)
{
  return ObWideInteger<Bits1, Signed1>::_impl::cmp(lhs, rhs) < 0;
}


template<unsigned Bits, typename Signed, typename T>
bool operator!=(const ObWideInteger<Bits, Signed> &lhs, const T &rhs)
{
  return ObWideInteger<Bits, Signed>::_impl::cmp(lhs, rhs) != 0;
}

template<unsigned Bits, typename Signed, typename T>
bool operator!=(const T &lhs, const ObWideInteger<Bits, Signed> &rhs)
{
  return ObWideInteger<Bits, Signed>::_impl::cmp(rhs, lhs) != 0;
}

template<unsigned Bits1, typename Signed1, unsigned Bits2, typename Signed2>
bool operator!=(const ObWideInteger<Bits1, Signed1> &lhs, const ObWideInteger<Bits2, Signed2> &rhs)
{
  return ObWideInteger<Bits1, Signed1>::_impl::cmp(lhs, rhs) != 0;
}

template<unsigned Bits, typename Signed, typename T>
bool operator<=(const ObWideInteger<Bits, Signed> &lhs, const T &rhs)
{
  int ret = ObWideInteger<Bits, Signed>::_impl::cmp(lhs, rhs);
  return ret <= 0;
}

template<unsigned Bits, typename Signed, typename T>
bool operator<=(const T &lhs, const ObWideInteger<Bits, Signed> &rhs)
{
  int ret = ObWideInteger<Bits, Signed>::_impl::cmp(rhs, lhs);
  return ret >= 0;
}

template<unsigned Bits1, typename Signed1, unsigned Bits2, typename Signed2>
bool operator<=(const ObWideInteger<Bits1, Signed1> &lhs, const ObWideInteger<Bits2, Signed2> &rhs)
{
  int ret = ObWideInteger<Bits1, Signed1>::_impl::cmp(lhs, rhs);
  return ret <= 0;
}

template<unsigned Bits, typename Signed, typename T>
bool operator>=(const ObWideInteger<Bits, Signed> &lhs, const T &rhs)
{
  int ret = ObWideInteger<Bits, Signed>::_impl::cmp(lhs, rhs);
  return ret >= 0;
}

template<unsigned Bits, typename Signed, typename T>
bool operator>=(const T &lhs, const ObWideInteger<Bits, Signed> &rhs)
{
  int ret = ObWideInteger<Bits, Signed>::_impl::cmp(rhs, lhs);
  return ret <= 0;
}

template<unsigned Bits1, typename Signed1, unsigned Bits2, typename Signed2>
bool operator>=(const ObWideInteger<Bits1, Signed1> &lhs,  const ObWideInteger<Bits2, Signed2> &rhs)
{
  int ret = ObWideInteger<Bits1, Signed1>::_impl::cmp(lhs, rhs);
  return ret >= 0;
}

template<unsigned Bits, typename T>
ObWideInteger<Bits> operator+(const ObWideInteger<Bits> &lhs, const T &rhs)
{
  ObWideInteger<Bits> res;
  if (ObWideInteger<Bits>::_impl::is_negative(rhs)) {
    ObWideInteger<Bits>::_impl::template sub<IgnoreOverFlow>(lhs, -rhs, res);
  } else {
    ObWideInteger<Bits>::_impl::template add<IgnoreOverFlow>(lhs, rhs, res);
  }
  return res;
}

template<unsigned Bits, typename T>
ObWideInteger<Bits> operator+(const T &lhs, const ObWideInteger<Bits> &rhs)
{
  ObWideInteger<Bits> res;
  if (ObWideInteger<Bits>::_impl::is_negative(lhs)) {
    ObWideInteger<Bits>::_impl::template sub<IgnoreOverFlow>(rhs, -lhs, res);
  } else {
    ObWideInteger<Bits>::_impl::template add<IgnoreOverFlow>(rhs, lhs, res);
  }
  return res;
}

template <unsigned Bits1, unsigned Bits2>
ObWideInteger<Bits1 >= Bits2 ? Bits1 : Bits2> operator+(const ObWideInteger<Bits1> &lhs,
                                                        const ObWideInteger<Bits2> &rhs)
{
  constexpr unsigned res_bits = (Bits1 >= Bits2 ? Bits1 : Bits2);
  using ResType = ObWideInteger<res_bits>;
  ResType res;
  if (Bits1 >= Bits2) {
    if (ObWideInteger<Bits2>::_impl::is_negative(rhs)) {
      ObWideInteger<res_bits>::_impl::template sub<IgnoreOverFlow>(lhs, -rhs, res);
    } else {
      ObWideInteger<res_bits>::_impl::template add<IgnoreOverFlow>(lhs, rhs, res);
    }
  } else {
    if (ObWideInteger<Bits1>::_impl::is_negative(lhs)) {
      ObWideInteger<res_bits>::_impl::template sub<IgnoreOverFlow>(rhs, -lhs, res);
    } else {
      ObWideInteger<res_bits>::_impl::template add<IgnoreOverFlow>(rhs, lhs, res);
    }
  }
  return res;
}

template<unsigned Bits, typename T>
ObWideInteger<Bits> operator-(const ObWideInteger<Bits> &lhs, const T &rhs)
{
  ObWideInteger<Bits> res;
  if (ObWideInteger<Bits>::_impl::is_negative(rhs)) {
    ObWideInteger<Bits>::_impl::template add<IgnoreOverFlow>(lhs, -rhs, res);
  } else {
    ObWideInteger<Bits>::_impl::template sub<IgnoreOverFlow>(lhs, rhs, res);
  }
  return res;
}

template<unsigned Bits, typename T>
ObWideInteger<Bits> operator-(const T &lhs, const ObWideInteger<Bits> &rhs)
{
  ObWideInteger<Bits> res;
  if (ObWideInteger<Bits>::_impl::is_negative(lhs)) {
    ObWideInteger<Bits>::_impl::template sub<IgnoreOverFlow>(-rhs, -lhs, res);
  } else {
    ObWideInteger<Bits>::_impl::template add<IgnoreOverFlow>(-rhs, lhs, res);
  }
  return res;
}

template <unsigned Bits1, unsigned Bits2>
ObWideInteger<Bits1 >= Bits2 ? Bits1 : Bits2> operator-(const ObWideInteger<Bits1> &lhs,
                                                        const ObWideInteger<Bits2> &rhs)
{
  constexpr unsigned res_bits = (Bits1 >= Bits2 ? Bits1 : Bits2);
  using ResType = ObWideInteger<res_bits>;
  ResType res;
  if (Bits1 >= Bits2) {
    if (ObWideInteger<Bits2>::_impl::is_negative(rhs)) {
      ObWideInteger<res_bits>::_impl::template add<IgnoreOverFlow>(lhs, -rhs, res);
    } else {
      ObWideInteger<res_bits>::_impl::template sub<IgnoreOverFlow>(lhs, rhs, res);
    }
  } else {
    if (ObWideInteger<Bits1>::_impl::is_negative(lhs)) {
      ObWideInteger<res_bits>::_impl::template sub<IgnoreOverFlow>(-rhs, -lhs, res);
    } else {
      ObWideInteger<res_bits>::_impl::template add<IgnoreOverFlow>(-rhs, lhs, res);
    }
  }
  return res;
}

template<unsigned Bits, typename T>
ObWideInteger<Bits> operator*(const ObWideInteger<Bits> &lhs, const T &rhs)
{
  ObWideInteger<Bits> res;
  ObWideInteger<Bits>::_impl::template multiply<IgnoreOverFlow>(lhs, rhs, res);
  return res;
}

template<unsigned Bits, typename T>
ObWideInteger<Bits> operator*(const T &lhs, const ObWideInteger<Bits> &rhs)
{
  ObWideInteger<Bits> res;
  ObWideInteger<Bits>::_impl::template multiply<IgnoreOverFlow>(rhs, lhs, res);
  return res;
}

template <unsigned Bits1, unsigned Bits2>
ObWideInteger<Bits1 >= Bits2 ? Bits1 : Bits2> operator*(const ObWideInteger<Bits1> &lhs,
                                                        const ObWideInteger<Bits2> &rhs)
{
  constexpr unsigned res_bits = (Bits1 >= Bits2 ? Bits1 : Bits2);
  using ResType = ObWideInteger<res_bits>;
  ResType res;
  if (Bits1 >= Bits2) {
    ObWideInteger<res_bits>::_impl::template multiply<IgnoreOverFlow>(lhs, rhs, res);
  } else {
    ObWideInteger<res_bits>::_impl::template multiply<IgnoreOverFlow>(rhs, lhs, res);
  }
  return res;
}

template<unsigned Bits, typename T>
ObWideInteger<Bits> operator/(const ObWideInteger<Bits> &lhs, const T &rhs)
{
  ObWideInteger<Bits> res;
  ObWideInteger<Bits> rem;
  ObWideInteger<Bits>::_impl::template divide<IgnoreOverFlow>(lhs, rhs, res, rem);
  return res;
}

template<unsigned Bits, typename T>
ObWideInteger<Bits> operator/(const T &lhs, const ObWideInteger<Bits> &rhs)
{
  ObWideInteger<Bits> res;
  ObWideInteger<Bits> rem;
  ObWideInteger<Bits>::_impl::template divide<IgnoreOverFlow>(ObWideInteger<Bits>(lhs), rhs, res, rem);
  return res;
}
template <unsigned Bits1, unsigned Bits2>
ObWideInteger<Bits1 >= Bits2 ? Bits1 : Bits2> operator/(const ObWideInteger<Bits1> &lhs,
                                                        const ObWideInteger<Bits2> &rhs)
{
  constexpr unsigned res_bits = (Bits1 >= Bits2 ? Bits1 : Bits2);
  using ResType = ObWideInteger<res_bits>;
  ResType res;
  ResType rem;
  if (Bits1 >= Bits2) {
    ObWideInteger<res_bits>::_impl::template divide<IgnoreOverFlow>(lhs, rhs, res, rem);
  } else {
    ObWideInteger<res_bits>::_impl::template divide<IgnoreOverFlow>(ObWideInteger<Bits2>(lhs), rhs, res, rem);
  }
  return res;
}

template<unsigned Bits, typename T>
ObWideInteger<Bits> operator%(const ObWideInteger<Bits> &lhs, const T &rhs)
{
  ObWideInteger<Bits> res;
  ObWideInteger<Bits> rem;
  ObWideInteger<Bits>::_impl::template divide<IgnoreOverFlow>(lhs, rhs, res, rem);
  return rem;
}

template<unsigned Bits, typename T>
ObWideInteger<Bits> operator%(const T &lhs, const ObWideInteger<Bits> &rhs)
{
  ObWideInteger<Bits> res;
  ObWideInteger<Bits> rem;
  ObWideInteger<Bits>::_impl::template divide<IgnoreOverFlow>(ObWideInteger<Bits>(lhs), rhs, res, rem);
  return rem;
}

template <unsigned Bits1, unsigned Bits2>
ObWideInteger<Bits1 >= Bits2 ? Bits1 : Bits2> operator%(const ObWideInteger<Bits1> &lhs,
                                                        const ObWideInteger<Bits2> &rhs)
{
  constexpr unsigned res_bits = (Bits1 >= Bits2 ? Bits1 : Bits2);
  using ResType = ObWideInteger<res_bits>;
  ResType res;
  ResType rem;
  if (Bits1 >= Bits2) {
    ObWideInteger<res_bits>::_impl::template divide<IgnoreOverFlow>(lhs, rhs, res, rem);
  } else {
    ObWideInteger<res_bits>::_impl::template divide<IgnoreOverFlow>(ObWideInteger<Bits2>(lhs), rhs, res, rem);
  }
  return rem;
}

template<unsigned Bits, typename T>
ObWideInteger<Bits> operator&(const ObWideInteger<Bits> &lhs, const T &rhs)
{
  ObWideInteger<Bits> res;
  ObWideInteger<Bits>::_impl::template bitwise_and(lhs, rhs, res);
  return res;
}

template<unsigned Bits, typename T>
ObWideInteger<Bits> operator&(const T &lhs, const ObWideInteger<Bits> &rhs)
{
  ObWideInteger<Bits> res;
  ObWideInteger<Bits>::_impl::template bitwise_and(rhs, lhs, res);
  return res;
}

template <unsigned Bits1, unsigned Bits2>
ObWideInteger<Bits1 >= Bits2 ? Bits1 : Bits2> operator&(const ObWideInteger<Bits1> &lhs,
                                                        const ObWideInteger<Bits2> &rhs)
{
  constexpr unsigned res_bits = (Bits1 >= Bits2 ? Bits1 : Bits2);
  using ResType = ObWideInteger<res_bits>;
  ResType res;
  if (Bits1 >= Bits2) {
    ObWideInteger<res_bits>::_impl::template bitwise_add(lhs, rhs, res);
  } else {
    ObWideInteger<res_bits>::_impl::template bitwise_add(rhs, lhs, res);
  }
  return res;
}

template<unsigned Bits, typename Signed>
ObWideInteger<Bits, Signed> operator>>(const ObWideInteger<Bits, Signed> &lhs, int n)
{
  ObWideInteger<Bits, Signed> res;
  ObWideInteger<Bits, Signed>::_impl::shift_right(lhs, res, static_cast<unsigned>(n));
  return res;
}

template<unsigned Bits, typename Signed>
ObWideInteger<Bits, Signed> operator<<(const ObWideInteger<Bits, Signed> &lhs, int n)
{
  ObWideInteger<Bits, Signed> res;
  ObWideInteger<Bits, Signed>::_impl::shift_left(lhs, res, static_cast<unsigned>(n));
  return res;
}

// unary operators
template<unsigned Bits, typename Signed>
ObWideInteger<Bits, Signed> operator-(const ObWideInteger<Bits, Signed> &x)
{
  ObWideInteger<Bits, Signed> res;
  ObWideInteger<Bits, Signed>::_impl::template unary_minus<IgnoreOverFlow>(x, res);
  return res;
}
} // end wide namespace
using int128_t  = wide::ObWideInteger<128u, signed>;
using int256_t  = wide::ObWideInteger<256u, signed>;
using int512_t  = wide::ObWideInteger<512u, signed>;
using int1024_t = wide::ObWideInteger<1024u, signed>;

// decimal int
struct ObDecimalInt
{
  union
  {
    int32_t int32_v_[0];
    int64_t int64_v_[0];
    int128_t int128_v_[0];
    int256_t int256_v_[0];
    int512_t int512_v_[0];
  };
} __attribute__((packed));

// helper struct, used to store data for algorithm operations
struct ObDecimalIntBuilder: public common::ObDataBuffer
{
  ObDecimalIntBuilder(): ObDataBuffer(buffer_, sizeof(buffer_)) {}
  template<typename T>
  inline void from(const T &v)
  {
    static_assert(sizeof(T) <= sizeof(int512_t), "");
    int_bytes_ = sizeof(T);
    MEMCPY(buffer_, v.items_, sizeof(T));
  }

  template<unsigned Bits, typename Signed>
  inline void from(const wide::ObWideInteger<Bits, Signed> &v, const int32_t copy_size)
  {
    static_assert(Bits <= 512, "");
    int_bytes_ = MIN(copy_size, sizeof(v));
    MEMCPY(buffer_, v.items_, int_bytes_);
  }

  inline void from(int32_t v)
  {
    int_bytes_ = sizeof(int32_t);
    MEMCPY(buffer_, &v, int_bytes_);
  }

  inline void from(int64_t v)
  {
    int_bytes_ = sizeof(int64_t);
    MEMCPY(buffer_, &v, int_bytes_);
  }

  inline void from(const ObDecimalIntBuilder &other)
  {
    int_bytes_ = other.get_int_bytes();
    MEMCPY(buffer_, other.get_decimal_int(), int_bytes_);
  }

  inline void from(const ObDecimalInt *decint, int32_t int_bytes)
  {
    OB_ASSERT(decint != NULL);
    int_bytes_ = MIN(sizeof(buffer_), int_bytes);
    MEMCPY(buffer_, decint, int_bytes_);
  }

  inline void build(ObDecimalInt *&decint, int32_t &int_bytes)
  {
    decint = reinterpret_cast<ObDecimalInt*>(buffer_);
    int_bytes = int_bytes_;
  }

  inline void build(const ObDecimalInt *&decint, int32_t &int_bytes)
  {
    decint = reinterpret_cast<const ObDecimalInt*>(buffer_);
    int_bytes = int_bytes_;
  }

  inline int32_t get_int_bytes() const
  {
    return int_bytes_;
  }

  inline const ObDecimalInt *get_decimal_int() const
  {
    return reinterpret_cast<const ObDecimalInt *>(buffer_);
  }

  inline void truncate(const int32_t expected_int_bytes)
  {
    if (expected_int_bytes >= int_bytes_) {
      // do nothing
    } else {
      int_bytes_ = expected_int_bytes;
    }
  }

  inline void extend(const int32_t expected_int_bytes)
  {
    bool is_neg = false;
    if (int_bytes_ > 0) {
      is_neg = (buffer_[int_bytes_- 1] & 0x80) > 0;
    }
    int32_t extending = expected_int_bytes - int_bytes_;
    if (extending <= 0) {
      // do nothing
    } else if (is_neg) {
      MEMSET(buffer_ + int_bytes_, 0xFF, extending);
      int_bytes_ = expected_int_bytes;
    } else {
      MEMSET(buffer_ + int_bytes_, 0, extending);
      int_bytes_ = expected_int_bytes;
    }
  }

  inline void set_zero(const int32_t int_bytes)
  {
    int_bytes_ = int_bytes;
    MEMSET(buffer_, 0, int_bytes);
  }

  inline char *get_buffer()
  {
    return buffer_;
  }
private:
  int32_t int_bytes_;
  char buffer_[sizeof(int512_t)];
};

template<typename T>
T get_scale_factor(int16_t scale)
{
  const static int64_t POWERS_OF_TEN[MAX_PRECISION_DECIMAL_INT_64 + 1] =
  {1,
   10,
   100,
   1000,
   10000,
   100000,
   1000000,
   10000000,
   100000000,
   1000000000,
   10000000000,
   100000000000,
   1000000000000,
   10000000000000,
   100000000000000,
   1000000000000000,
   10000000000000000,
   100000000000000000,
   1000000000000000000};

  T sf = 1;
  while (scale > 0) {
    const int16_t step = MIN(scale, MAX_PRECISION_DECIMAL_INT_64);
    sf = static_cast<T>(sf * POWERS_OF_TEN[step]);
    scale -= step;
  }
  return sf;
}

} // end namespace common
} // end namespace oceanbase

#include "lib/wide_integer/ob_wide_integer_helper.h"
#include "lib/wide_integer/ob_wide_integer_impl.h"
#include "lib/wide_integer/ob_wide_integer_cmp_funcs.h"
#include "lib/wide_integer/ob_wide_integer_str_funcs.h"

#endif // !OB_WIDE_INTEGER_
