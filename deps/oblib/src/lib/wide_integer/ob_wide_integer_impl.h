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

#ifndef OB_WIDE_INTEGER_IMPL_H_
#define OB_WIDE_INTEGER_IMPL_H_

#include <type_traits>
#include <cmath>

namespace oceanbase
{
namespace common
{
namespace wide
{

template<unsigned Bits, typename Signed>
struct ObWideInteger<Bits, Signed>::_impl
{
  static unsigned big(unsigned i)
  {
    return ITEM_COUNT - i - 1;
  }

  template<typename T>
  static bool is_negative(const T &x)
  {
    if (std::is_signed<T>::value) {
      return x < 0;
    }
    return false;
  }

  template<unsigned B, typename S>
  static bool is_negative(const ObWideInteger<B, S> &x)
  {
    if (std::is_same<S, signed>::value) {
      if (x.items_[ObWideInteger<B, S>::_impl::big(0)] & SIGN_BIT_MASK) {
        return true;
      }
    }
    return false;
  }

  template<unsigned B, typename S>
  static bool is_zero(const ObWideInteger<B, S> &x)
  {
    for (unsigned i = ObWideInteger<B, S>::ITEM_COUNT; i > 0; --i) {
      if (0 != x.items_[i - 1]) {
        return false;
      }
    }
    return true;
  }

  template<typename T>
  static T make_positive(const T &x)
  {
    if (std::is_signed<T>::value && x < 0) {
      return -x;
    }
    return x;
  }

  template<unsigned B, typename S>
  static ObWideInteger<B, S> make_positive(const ObWideInteger<B, S> &x)
  {
    if (is_negative(x)) {
      ObWideInteger<B, S> res;
      ObWideInteger<B, S>::_impl::template unary_minus<IgnoreOverFlow>(x, res);
      return res;
    }
    return x;
  }

  template<typename T>
  static bool should_keep_size()
  {
    return sizeof(T) <= BYTE_COUNT;
  }

  template<typename T>
  static int nlz(const T &x)
  {
    int cnt_zero = 0;
    if (sizeof(T) <= 4) { // uint32
      cnt_zero = __builtin_clz(x);
    } else if (sizeof(T) <= 8) { // uint64
      cnt_zero = __builtin_clzll(x);
    }
    return cnt_zero;
  }

  template<unsigned B, typename S>
  static int nlz(const ObWideInteger<B, S> &x)
  {
    int cnt_zero = 0;
    for (unsigned i = 0; i < ObWideInteger<B, S>::ITEM_COUNT; i++) {
      unsigned idx = ObWideInteger<B, S>::_impl::big(i);
      if (x.items_[idx]) {
        cnt_zero += __builtin_clzll(x.items_[idx]);
        break;
      } else {
        cnt_zero += ObWideInteger<B, S>::BASE_BITS;
      }
    }
    return cnt_zero;
  }

  template<unsigned B, typename S>
  static unsigned item_size(const ObWideInteger<B, S> &x)
  {
    unsigned i = ObWideInteger<B, S>::ITEM_COUNT;
    for (; i > 0; i--) {
      if (x.items_[i-1] != 0) {
        break;
      }
    }
    return i;
  }
  // check if x > abs(ObWideInteger<Bits, Signed>::MIN_VAL)
  // x is treated as unsigned value
  template<unsigned B, typename S>
  static bool overflow_negative_range(const ObWideInteger<B, S> &x)
  {
    uint64_t big0 = x.items_[ObWideInteger<B, S>::_impl::big(0)];
    if (big0 > ObWideInteger<B, S>::SIGN_BIT_MASK) {
      return true;
    } else if (big0 == ObWideInteger<B, unsigned>::SIGN_BIT_MASK) {
      for (unsigned i = 0; i < ObWideInteger<B, unsigned>::ITEM_COUNT - 1; i++) {
        if (x.items_[i] != 0) {
          return true;
        }
      }
    }
    return false;
  }

  // check if x > ObWideInteger<Bits, Signed>::MAX_VAL
  // x is treated as unsigned value
  template<unsigned B, typename S>
  static bool overflow_positive_range(const ObWideInteger<B, S> &x)
  {
    uint64_t big0 = x.items_[ObWideInteger<B, S>::_impl::big(0)];
    if (big0 >= ObWideInteger<B, unsigned>::SIGN_BIT_MASK) {
      return true;
    }
    return false;
  }

  static bool add_overflow(bool l_neg, bool r_neg, bool res_neg)
  {
    // positive + positive = negative => overflow
    // negative + negative = positive => overflow
    if (l_neg && r_neg) {
      return !res_neg;
    } else if (!l_neg && !r_neg) {
      return res_neg;
    }
    return false;
  }

  static bool sub_overflow(bool l_neg, bool r_neg, bool res_neg)
  {
    // positive - negative = negative => overflow
    // negative - positive = positive => overflow
    if (!l_neg && r_neg) {
      return res_neg;
    } else if (l_neg && !r_neg) {
      return !res_neg;
    }
    return false;
  }

  template <unsigned Bits2, typename Signed2>
  static bool within_limits(const ObWideInteger<Bits2, Signed2> &x)
  {
    return ObWideInteger<Bits2, Signed2>::_impl::cmp(
               x, Limits<ObWideInteger<Bits, Signed>>::max()) <= 0 &&
           ObWideInteger<Bits2, Signed2>::_impl::cmp(
               x, Limits<ObWideInteger<Bits, Signed>>::min()) >= 0;
  }

  template<typename T, class = typename std::enable_if<std::is_integral<T>::value>::type>
  static uint64_t get_item(const T &val, unsigned idx)
  {
    if (sizeof(T) <= sizeof(uint64_t)) {
      if (idx == 0) {
        return static_cast<uint64_t>(val);
      }
      return 0;
    } else if (idx * sizeof(uint64_t) < sizeof(T)) {
      return static_cast<uint64_t>(val >> (idx * BASE_BITS));
    } else {
      return 0;
    }
  }

  template<unsigned B, typename S>
  static uint64_t get_item(const ObWideInteger<B, S> &x, unsigned idx)
  {
    if (idx < ObWideInteger<B, S>::ITEM_COUNT) {
      return x.items_[idx];
    }
    return 0;
  }

  static int shift_left(
    const ObWideInteger<Bits, Signed> &self,
    ObWideInteger<Bits, Signed> &res,
    unsigned n)
  {
    int ret = OB_SUCCESS;
    if ((void *)&res != (void *)&self) {
      res = self;
    }
    if (n >= Bits) {
      MEMSET(res.items_, 0, sizeof(uint64_t) * ITEM_COUNT);
    } else {
      unsigned item_shift = n / BASE_BITS;
      unsigned bits_shift = n % BASE_BITS;
      if (bits_shift) {
        unsigned right_bits = BASE_BITS - bits_shift;
        res.items_[big(0)] = res.items_[big(item_shift)] << bits_shift;
        for (unsigned i = 1; i < ITEM_COUNT - item_shift; i++) {
          res.items_[big(i - 1)] |= res.items_[big(item_shift + i)] >> right_bits;
          res.items_[big(i)] = res.items_[big(i + item_shift)] << bits_shift;
        }
      } else {
        for (unsigned i = 0; i < ITEM_COUNT - item_shift; i++) {
          res.items_[big(i)] = res.items_[big(i + item_shift)];
        }
      }
      for (unsigned i = 0; i < item_shift; i++) {
        res.items_[i] = 0;
      }
    }
    return ret;
  }

  static int shift_right(
    const ObWideInteger<Bits, Signed> &self,
    ObWideInteger<Bits, Signed> &res,
    unsigned n)
  {
    int ret = OB_SUCCESS;
    if ((void *)&res != (void *)&self) {
      res = self;
    }
    bool is_neg = is_negative(self);
    if (n >= Bits) {
      if (is_neg) {
        for (unsigned i = 0; i < ITEM_COUNT; i++) {
          res.items_[i] = UINT64_MAX;
        }
      } else {
        MEMSET(res.items_, 0, sizeof(uint64_t) * ITEM_COUNT);
      }
    } else {
      unsigned item_shift = n / BASE_BITS;
      unsigned bits_shift = n % BASE_BITS;
      if (bits_shift) {
        unsigned left_bits = BASE_BITS - bits_shift;
        res.items_[0] = res.items_[item_shift] >> bits_shift;
        for (unsigned i = 1; i < ITEM_COUNT - item_shift; i++) {
          res.items_[i - 1] |=
              (res.items_[i + item_shift] << left_bits);
          res.items_[i] =
              res.items_[i + item_shift] >> bits_shift;
        }
      } else {
        for (unsigned i = 0; i < ITEM_COUNT - item_shift; i++) {
          res.items_[i] = res.items_[i + item_shift];
        }
      }
      if (is_neg) {
        if (bits_shift) {
          res.items_[big(item_shift)] |= (BASE_MAX << (BASE_BITS - bits_shift));
        }
        for (unsigned i = 0; i < item_shift; i++) {
          res.items_[big(i)] = BASE_MAX;
        }
      } else {
        for (unsigned i = 0; i < item_shift; i++) {
          res.items_[big(i)] = 0;
        }
      }
    }
    return ret;
  }

  template <int check_overflow, unsigned Bits2, typename Signed2>
  static int add(
    const ObWideInteger<Bits, Signed> &lhs,
    const ObWideInteger<Bits2, Signed2> &rhs,
    ObWideInteger<Bits, Signed> &res
  )
  {
    int ret = OB_SUCCESS;
    if (Bits >= Bits2) {
      if (Bits == 128) {
        dw_type l = *reinterpret_cast<const dw_type *>(lhs.items_);
        dw_type r = *reinterpret_cast<const dw_type *>(rhs.items_);
        dw_type result = l + r;
        res.items_[1] = static_cast<uint64_t>(result>>BASE_BITS);
        res.items_[0] = static_cast<uint64_t>(result);
      } else {
        constexpr const unsigned op_items = ObWideInteger<Bits2, Signed2>::ITEM_COUNT;
        bool overflows[ITEM_COUNT] = {false};
        for (unsigned i = 0; i < op_items; i++) {
          res.items_[i] = lhs.items_[i] + rhs.items_[i];
          overflows[i] = (rhs.items_[i] > res.items_[i]);
        }
        for (unsigned i = op_items; i < ITEM_COUNT; i++) { res.items_[i] = lhs.items_[i]; }
        for (unsigned i = 1; i < ITEM_COUNT; i++) {
          if (overflows[i - 1]) {
            res.items_[i]++;
            if (res.items_[i] == 0) { overflows[i] = true; }
          }
        }
      }
    } else {
      using calc_type =
          typename CommonType<ObWideInteger<Bits, Signed>,
                              ObWideInteger<Bits2, Signed2>>::type;
      calc_type xres;
      ret = calc_type::_impl::template add<check_overflow>(calc_type(lhs), rhs, xres);
      res = xres;
    }
    return ret;
  }
  template <int check_overflow, typename T>
  static int add(
    const ObWideInteger<Bits> &self, const T &rhs,
    ObWideInteger<Bits, Signed> &res)
  {
    int ret = OB_SUCCESS;
    if (should_keep_size<T>()) {
      constexpr const unsigned r_items =
          (sizeof(T) < sizeof(uint64_t)) ? 1 : sizeof(T) / sizeof(uint64_t);
      constexpr const unsigned op_items = (r_items < ITEM_COUNT) ? r_items : ITEM_COUNT;

      if ((void *)&res != (void *)&self) {
        res = self;
      }
      bool overflow = false;
      for (unsigned i = 0; i < op_items; i++) {
        uint64_t r_val = get_item(rhs, i);
        if (overflow) {
          res.items_[i]++;
          overflow = (res.items_[i] == 0);
        }
        res.items_[i] += r_val;
        overflow = (overflow || (res.items_[i] < r_val));
      }
      for (unsigned i = op_items; overflow && i < ITEM_COUNT; i++) {
        res.items_[i]++;
        overflow = (res.items_[i] == 0);
      }
    } else {
      using calc_type = typename CommonType<ObWideInteger<Bits, Signed>, T>::type;
      calc_type xres;
      ret = calc_type::_impl::template add<check_overflow>(calc_type(self), rhs, xres);
      res = xres;
    }
    return ret;
  }

  template<int check_overflow, unsigned Bits2, typename Signed2>
  static int sub(
    const ObWideInteger<Bits, Signed> &lhs, const ObWideInteger<Bits2, Signed2> &rhs,
    ObWideInteger<Bits, Signed> &res
  )
  {
    int ret = OB_SUCCESS;
    if (Bits >= Bits2) {
      constexpr const unsigned op_items = ObWideInteger<Bits2, Signed2>::ITEM_COUNT;
      if ((void *)&res != (void *)&lhs) {
        res = lhs;
      }
      bool l_neg = is_negative(lhs);
      bool r_neg = is_negative(rhs);
      bool borrow = false;
      for (unsigned i = 0; i < op_items; i++) {
        uint64_t r_val = rhs.items_[i];
        if (borrow) {
          res.items_[i]--;
          borrow = (res.items_[i] == BASE_MAX);
        }
        borrow = (borrow || (res.items_[i] < r_val));
        res.items_[i] -= r_val;
      }
      for (unsigned i = op_items; borrow && i < ITEM_COUNT; i++) {
        res.items_[i]--;
        borrow = (res.items_[i] == BASE_MAX);
      }
      if (check_overflow && sub_overflow(l_neg, r_neg, is_negative(res))) {
        ret = OB_OPERATE_OVERFLOW;
      }
    } else {
      using calc_type =
          typename CommonType<ObWideInteger<Bits, Signed>,
                              ObWideInteger<Bits2, Signed2>>::type;
      calc_type xres;
      ret = calc_type::_impl::template sub<check_overflow>(calc_type(lhs), rhs, xres);
      res = xres;
    }
    return ret;
  }
  template<int check_overflow, typename T>
  static int sub(
    const ObWideInteger<Bits, Signed> &self,const T &rhs,ObWideInteger<Bits, Signed> &res
  )
  {
    int ret = OB_SUCCESS;
    if (should_keep_size<T>()) {
      constexpr const unsigned r_items =
        (sizeof(T) < sizeof(uint64_t)) ? 1 : sizeof(T) / sizeof(uint64_t);
      constexpr const unsigned op_items = (r_items < ITEM_COUNT) ? r_items : ITEM_COUNT;

      if ((void *)&res != (void *)&self) {
        res = self;
      }
      bool l_neg = is_negative(self);
      bool r_neg = is_negative(rhs);
      bool borrow = false;
      for (unsigned i = 0; i < op_items; i++) {
        uint64_t r_val = get_item(rhs, i);
        if (borrow) {
          res.items_[i]--;
          borrow = (res.items_[i] == BASE_MAX);
        }
        borrow = (borrow || (res.items_[i] < r_val));
        res.items_[i] -= r_val;
      }
      for (unsigned i = op_items; borrow && i < ITEM_COUNT; i++) {
        res.items_[i]--;
        borrow = (res.items_[i] == BASE_MAX);
      }
      if (check_overflow) {
        bool res_neg = is_negative(res);
        // positive - negative = negative => overflow
        // negative - positive = positive => overflow
        if (!l_neg && r_neg) {
          if (res_neg) {
            ret = OB_OPERATE_OVERFLOW;
          }
        } else if (l_neg && !r_neg) {
          if (!res_neg) {
            ret = OB_OPERATE_OVERFLOW;
          }
        }
      }
    } else {
      using calc_type = typename CommonType<ObWideInteger<Bits, Signed>, T>::type;
      calc_type xres;
      ret = calc_type::_impl::template sub<check_overflow>(calc_type(self), rhs, xres);
      res = xres;
    }
    return ret;
  }

  template<int check_overflow>
  static int unary_minus(const ObWideInteger<Bits, Signed> &self, ObWideInteger<Bits, Signed> &res)
  {
    int ret = OB_SUCCESS;

    if ((void *)&res != (void *)&self) {
      res = self;
    }
    bool is_neg = is_negative(res);
    for (unsigned i = 0; i < ITEM_COUNT; i++) {
      res.items_[i] = ~res.items_[i];
    }
    add<IgnoreOverFlow>(res, 1, res);
    if (check_overflow) {
      // abs(min_val) overflow max_val
      if (is_neg && is_negative(res)) {
        ret = OB_OPERATE_OVERFLOW;
      }
    }
    return ret;
  }

  template <int check_overflow, typename T>
  static int multiply(
    const ObWideInteger<Bits, Signed> &lhs, const T &rhs,
    ObWideInteger<Bits, Signed> &res)
  {
    int ret = OB_SUCCESS;
    if (rhs == 0) {
      res = 0;
    } else if (rhs == 1) {
      res = lhs;
    } else if (sizeof(T) <= sizeof(uint64_t)) {
      res = make_positive(lhs);
      dw_type rval = make_positive(rhs);
      dw_type carrier = 0;
      for (unsigned i = 0; i < ITEM_COUNT; i++) {
        carrier += static_cast<dw_type>(res.items_[i]) * rval;
        res.items_[i] = static_cast<uint64_t>(carrier);
        carrier >>= 64;
      }
      bool l_neg = is_negative(lhs);
      bool r_neg = is_negative(rhs);
      if (check_overflow) {
        // if carrier != 0 => must overflow
        // else if result is negative, check if abs(res) >= abs(min_val)
        // else check if abs(res) >= abs(max_val)
        if (carrier) {
          ret = OB_OPERATE_OVERFLOW;
        } else if (l_neg != r_neg && overflow_negative_range(res)) {
          ret = OB_OPERATE_OVERFLOW;
        } else if (l_neg == r_neg && overflow_positive_range(res)) {
          ret = OB_OPERATE_OVERFLOW;
        }
      }
      if (l_neg != r_neg) {
        unary_minus<IgnoreOverFlow>(res, res);
      }
    } else {
      using calc_type = typename CommonType<ObWideInteger<Bits, Signed>, T>::type;
      calc_type xres;
      ret = calc_type::_impl::template multiply<check_overflow>(
        calc_type(lhs), calc_type(rhs), xres);
      res = xres;
      if (check_overflow && ret == OB_SUCCESS) {
        if (!within_limits(xres)) {
          ret = OB_OPERATE_OVERFLOW;
        }
      }
    }
    return ret;
  }


  template <int check_overflow, unsigned Bits2, typename Signed2>
  static int multiply(
    const ObWideInteger<Bits, Signed> &lhs, const ObWideInteger<Bits2, Signed2> &rhs,
    ObWideInteger<Bits, Signed> &res)
  {
    int ret = OB_SUCCESS;
    if (Bits >= Bits2) {
      if (Bits == 128) {
        dw_type lop = *(reinterpret_cast<const dw_type *>(lhs.items_));
        dw_type rop = *(reinterpret_cast<const dw_type *>(rhs.items_));
        dw_type result = lop * rop;
        res.items_[1] = static_cast<uint64_t>(result>>BASE_BITS);
        res.items_[0] = static_cast<uint64_t>(result);
        if (check_overflow) {
          ObWideInteger<Bits, unsigned> tmpl = make_positive(lhs);
          ObWideInteger<Bits, unsigned> tmpr = make_positive(rhs);
          uint64_t h0 = get_item(tmpl, 1);
          uint64_t h1 = get_item(tmpr, 1);
          uint64_t l0 = get_item(tmpl, 0);
          uint64_t l1 = get_item(tmpr, 0);
          uint64_t tmp = 0;
          // when int128 multiplies int128, we have
          //           h0    l0
          //  *        h1    l1
          // ---------------------
          //         h0*l1  l0*l1
          //  h0*h1  h1*l0
          //
          //  if h0 > 0 & h1 > 0 => must overflow
          //  else if h0*l1 overflow uint64 or h1*l0 overflow uint64 => must overflow
          //  otherwise cast to int256 to calculate result and check overflow
          bool overflow1 = false, overflow2 = false;
          if (h0 == 0 && h1 == 0 && (l0 <= (1ULL << 63)) && (l1 <= (1ULL<<63))) {
            // do nothing
          } else {
            using calc_type = typename PromotedInteger<ObWideInteger<Bits, Signed>>::type;
            calc_type xres;
            calc_type::_impl::template multiply<IgnoreOverFlow>(calc_type(lhs), rhs, xres);
            if (!within_limits(xres)) {
              ret = OB_OPERATE_OVERFLOW;
            }
          }
        }
      } else {
        ObWideInteger<Bits, unsigned> lval = make_positive(lhs);
        ObWideInteger<Bits2, unsigned> rval = make_positive(rhs);

        unsigned outter_limit = item_size(lval);
        unsigned inner_limit = item_size(rval);
        res = 0;
        dw_type carrier = 0;
        bool overflow = false;
        for (unsigned j = 0; j < inner_limit; j++) {
          for (unsigned i = 0; i < outter_limit; i++) {
            if (i + j < ITEM_COUNT) {
              carrier += res.items_[i + j] + static_cast<dw_type>(lval.items_[i]) *
                                             static_cast<dw_type>(rval.items_[j]);
              res.items_[i+j] = static_cast<uint64_t>(carrier);
              carrier >>= 64;
            } else {
              overflow = true;
              break;
            }
          } // end inner while
          if (OB_UNLIKELY(overflow)) {
            break;
          } else if (carrier) {
            if (j + outter_limit < ITEM_COUNT) {
              res.items_[j + outter_limit] = static_cast<uint64_t>(carrier);
            } else {
              overflow = true;
              break;
            }
            carrier = 0;
          }
        }
        bool l_neg = is_negative(lhs);
        bool r_neg = is_negative(rhs);
        if (check_overflow) {
          // for example int256 * int256, we have
          //
          //                               l3       l2      l1      l0
          //    *                          r3       r2      r1      r0
          //   -----------------------------------------------------------
          //                             l3*r0     l2*r0   l1*r0  l0*r0
          //                   l3*r1     l2*r1     r1*l1   r1*l0
          //...
          // if any(res_items[i] > 0, i in [item_count, 2*item_count]) => must overflow
          // else if result is positive, check if abs(result) overflow max_val
          // else if result is negative, check if abs(result) overflow abs(min_val)
          // else not overflow
          if (overflow) {
            ret = OB_OPERATE_OVERFLOW;
          } else if (l_neg != r_neg && overflow_negative_range(res)) {
            ret = OB_OPERATE_OVERFLOW;
          } else if (l_neg == r_neg && overflow_positive_range(res)) {
            ret = OB_OPERATE_OVERFLOW;
          }
        }
        if(l_neg != r_neg) {
          unary_minus<IgnoreOverFlow>(res, res);
        }
      }
    } else {
      using calc_type = typename CommonType<ObWideInteger<Bits>, ObWideInteger<Bits2>>::type;
      calc_type xres;
      ret = calc_type::_impl::template multiply<check_overflow>(calc_type(lhs), rhs, xres);
      res = xres;
      if (check_overflow && OB_SUCCESS == ret) {
        if (!within_limits(xres)) {
          ret = OB_OPERATE_OVERFLOW;
        }
      }
    }
    return ret;
  }

  // divide by native integer
  template <int check_overflow, typename T>
  static int divide(
    const ObWideInteger<Bits, Signed> &lhs, const T &rhs,
    ObWideInteger<Bits, Signed> &quotient,
    ObWideInteger<Bits, Signed> &remain)
  {
    int ret = OB_SUCCESS;
    if (check_overflow && rhs == 0) {
      ret = OB_OPERATE_OVERFLOW;
    } else if (lhs == 0) {
      quotient = 0;
      remain = lhs;
    } else {
      ObWideInteger<Bits, unsigned> numerator = make_positive(lhs);
      dw_type denominator;
      if (rhs < 0) {
        denominator = -rhs;
      } else {
        denominator = rhs;
      }
      if (numerator < denominator) {
        quotient = 0;
        remain = lhs;
      } else {
        bool r_neg = is_negative(rhs);
        bool l_neg = is_negative(lhs);
        if (denominator == 1) {
          quotient = numerator;
          remain = 0;
          if (check_overflow) {
            // min_val / -1 => overflow
            if (l_neg && l_neg == r_neg && overflow_positive_range(quotient)) {
              ret = OB_OPERATE_OVERFLOW;
            }
          }
        } else if (Bits == 128) {
          dw_type num = *(reinterpret_cast<const dw_type *>(numerator.items_));
          dw_type den = static_cast<dw_type>(denominator);
          dw_type quo = num / den;
          dw_type rem = num % den;

          quotient.items_[1] = static_cast<uint64_t>(quo >> BASE_BITS);
          quotient.items_[0] = static_cast<uint64_t>(quo);
          remain.items_[1] = static_cast<uint64_t>(rem >> BASE_BITS);
          remain.items_[0] = static_cast<uint64_t>(rem);
        } else {
          dw_type num = 0;
          dw_type den = static_cast<dw_type>(denominator);
          quotient = 0;
          for (unsigned i = 0; i < ITEM_COUNT; i++) {
            num = (num << BASE_BITS) + static_cast<dw_type>(numerator.items_[big(i)]);
            if (num < den) {
              quotient.items_[big(i)] = 0;
            } else {
              quotient.items_[big(i)] = static_cast<uint64_t>(num / den);
              num = num % den;
            }
          }
          remain = 0;
          remain.items_[0] = static_cast<uint64_t>(num);
        }
        if (r_neg != l_neg) {
            unary_minus<IgnoreOverFlow>(quotient, quotient);
        }
        if (l_neg) {
          unary_minus<IgnoreOverFlow>(remain, remain);
        }
      }
    }
    return ret;
  }

  template <int check_overflow, unsigned Bits2, typename Signed2>
  static int divide(
    const ObWideInteger<Bits, Signed> &lhs, const ObWideInteger<Bits2, Signed2> &rhs,
    ObWideInteger<Bits, Signed> &quotient,
    ObWideInteger<Bits, Signed> &remain)
  {
    int ret = OB_SUCCESS;
    if (check_overflow && rhs == 0) {
      ret = OB_OPERATE_OVERFLOW;
    } else if (lhs == 0) {
      quotient = 0;
      remain = lhs;
    } else if (Bits >= Bits2) {
      ObWideInteger<Bits, unsigned> numerator = make_positive(lhs);
      ObWideInteger<Bits, unsigned> denominator = make_positive(rhs);
      if (numerator < denominator) {
        quotient = 0;
        remain = lhs;
      } else if (denominator == 1) {
        bool lhs_neg = is_negative(lhs);
        bool rhs_neg = is_negative(rhs);
        quotient = numerator;
        remain = 0;
        if (check_overflow) {
          // min_val / -1 => overflow
          if (lhs_neg && lhs_neg == rhs_neg && overflow_positive_range(quotient)) {
            ret = OB_OPERATE_OVERFLOW;
          }
        }
        if (lhs_neg != rhs_neg) {
          unary_minus<IgnoreOverFlow>(quotient, quotient);
        }
      } else {
        int nlz_num = nlz(numerator);
        int shift_bits = nlz(denominator) % BASE_BITS;
        /* knuth division algorithm:
         *
         * we have
         *   non negative value `u[m+n-1, ...n-1...0]`(B based number) and
         *   non negative value `v[n-1...0]` (B based number)
         * while v[n-1] != 0 and n > 1 and m >= 1
         *
         * let `quo[m...0]` be quotient, `rem[n-1..0]` be remain
         *
         * Step1. [normalize]
         *   d = nlz(v[n-1]), v <<= d,  u <<= d;
         *
         * Step2.
         *   set j = m;
         *
         * Step3.
         *   qq = (u[j + n] * B + u[j + n - 1]) / v[n-1]
         *   rr =  (u[j + n] * B + u[j + n - 1]) % v[n-1]
         *   if qq == B || qq * v[n-2] > (B * rr + u[j + n - 2]) {
         *     qq -= 1;
         *     rr += v[n-1];
         *   }
         *
         * Step4.
         *   u[j+n..j] = u[j+n..j] - qq*v[n-1..0];
         *   if u[j+n..j] < 0 {
         *     u[j+n..j] += v[n-1..0];
         *     qq -= 1;
         *   }
         *   quo[j] = q;
         *
         * Step5.
         *   j = j - 1;
         *   if j >= 0, goto Step3
         *   else goto Step6
         *
         * Step6.
         *   remain = u[m+n-1..0] >> d;
         */
        if (shift_bits + BASE_BITS > nlz_num) {
          using cast_type = typename PromotedInteger<ObWideInteger<Bits, Signed>>::type;
          cast_type xquo;
          cast_type xrem;
          ret = cast_type::_impl::template divide<check_overflow>(cast_type(lhs), rhs, xquo, xrem);
          quotient = xquo;
          remain = xrem;
        } else {
          ObWideInteger<Bits, unsigned>::_impl::shift_left(numerator, numerator, shift_bits);
          ObWideInteger<Bits, unsigned>::_impl::shift_left(denominator, denominator, shift_bits);

          unsigned xlen = item_size(numerator);
          unsigned ylen = item_size(denominator);
          dw_type base = dw_type(1) << 64;
          unsigned n = ylen, m = xlen - ylen;
          ObWideInteger<Bits, Signed> tmp_ct = 0;
          ObWideInteger<Bits, Signed> tmp_cf = 0;
          ObWideInteger<Bits, unsigned> tmp_vt = 0;

          for (int j = m; j >= 0; j--) {
            dw_type tmp_value =
                static_cast<dw_type>(numerator.items_[j + n]) * base +
                static_cast<dw_type>(numerator.items_[j + n - 1]);
            dw_type qq =
                tmp_value / static_cast<dw_type>(denominator.items_[n - 1]);
            dw_type rr =
                tmp_value % static_cast<dw_type>(denominator.items_[n - 1]);
            if (qq == base || (n >= 2 &&
                              (qq * static_cast<dw_type>(denominator.items_[n - 2]) >
                               base * rr + static_cast<dw_type>(numerator.items_[j + n - 2])))) {
              qq--;
              rr += static_cast<dw_type>(denominator.items_[n - 1]);
            }
            MEMCPY(tmp_ct.items_, numerator.items_ + j, sizeof(uint64_t) * (n + 1));
            ObWideInteger<Bits, unsigned>::_impl::template multiply<IgnoreOverFlow>(
              denominator, static_cast<uint64_t>(qq), tmp_vt);
            sub<IgnoreOverFlow>(tmp_ct, ObWideInteger<Bits, Signed>(tmp_vt), tmp_cf);
            if (is_negative(tmp_cf)) {
              add<IgnoreOverFlow>(tmp_cf, ObWideInteger<Bits, Signed>(denominator), tmp_cf);
              qq--;
            }
            MEMCPY(numerator.items_ + j, tmp_cf.items_,
                   sizeof(uint64_t) * (n + 1));
            quotient.items_[j] = static_cast<uint64_t>(qq);
          }
          // calc remain
          ObWideInteger<Bits, unsigned>::_impl::shift_right(numerator, numerator, shift_bits);
          remain = numerator;
          bool lhs_neg = is_negative(lhs);
          bool rhs_neg = is_negative(rhs);
          if (lhs_neg != rhs_neg) {
            unary_minus<IgnoreOverFlow>(quotient, quotient);
          }
          if (lhs_neg) {
            unary_minus<IgnoreOverFlow>(remain, remain);
          }
        }
      }
    } else {
      using calc_type = typename CommonType<ObWideInteger<Bits>, ObWideInteger<Bits2>>::type;
      calc_type xquo, xrem;
      ret = calc_type::_impl::template divide<check_overflow>(calc_type(lhs), rhs, xquo, xrem);
      quotient = xquo;
      remain = xrem;
    }
    return ret;
  }

  template<typename T>
  static int cmp(const ObWideInteger<Bits, Signed> &lhs, const T &rhs
  )
  {
    int ret = 0;
    bool l_neg = is_negative(lhs);
    bool r_neg = is_negative(rhs);
    if (l_neg != r_neg) {
      ret = (l_neg ? -1 : 1);
    } else if (IsWideInteger<T>::value){
      ret = cmp(lhs, rhs);
    } else {
      ret = cmp(lhs, ObWideInteger<Bits, Signed>(rhs));
    }
    return ret;
  }

  template<unsigned Bits2, typename Signed2>
  static int cmp(const ObWideInteger<Bits, Signed> &lhs, const ObWideInteger<Bits2, Signed2> &rhs
  )
  {
    int ret = 0;
    bool l_neg = is_negative(lhs);
    bool r_neg = is_negative(rhs);
    if (l_neg != r_neg) {
      ret = (l_neg ? -1 : 1);
    } else if (Bits == Bits2) {
      for (unsigned i = 0; (ret == 0) && i < ITEM_COUNT; i++) {
        if (lhs.items_[big(i)] > rhs.items_[big(i)]) {
          ret = 1;
        } else if (lhs.items_[big(i)] < rhs.items_[big(i)]) {
          ret = -1;
        }
      }
    } else {
      using calc_type = typename CommonType<ObWideInteger<Bits, Signed>,
                                            ObWideInteger<Bits2, Signed2>>::type;
      ret = calc_type::_impl::cmp(calc_type(lhs), calc_type(rhs));
    }
    return ret;
  }

  template <unsigned Bits2, typename Signed2>
  static int bitwise_and(
      const ObWideInteger<Bits, Signed> &lhs,
      const ObWideInteger<Bits2, Signed2> &rhs,
      ObWideInteger<Bits, Signed> &res)
  {
    int ret = OB_SUCCESS;
    if (Bits >= Bits2) {
      constexpr const unsigned op_items = ObWideInteger<Bits2, Signed2>::ITEM_COUNT;
      constexpr const unsigned rest_items = ObWideInteger<Bits, Signed>::ITEM_COUNT;
      if ((void *)&res != (void *)&lhs) {
        res = lhs;
      }
      unsigned i = 0;
      for (; i < op_items; i++) {
        res.items_[i] &= rhs.items_[i];
      }
      if (!is_negative(rhs)) {
        // for positive integer, set all rest items to 0
        for (; i < rest_items; i++) {
          res.items_[i] = 0;
        }
      }
    } else {
      using calc_type =
          typename CommonType<ObWideInteger<Bits, Signed>,
                              ObWideInteger<Bits2, Signed2>>::type;
      calc_type xres;
      ret = calc_type::_impl::template bitwise_and(calc_type(lhs), rhs, xres);
      res = xres;
    }
    return ret;
  }

  template <typename T>
  static int bitwise_and(
      const ObWideInteger<Bits> &self,
      const T &rhs,
      ObWideInteger<Bits, Signed> &res)
  {
    int ret = OB_SUCCESS;
    if (should_keep_size<T>()) {
      constexpr const unsigned r_items =
          (sizeof(T) < sizeof(uint64_t)) ? 1 : sizeof(T) / sizeof(uint64_t);
      constexpr const unsigned op_items = (r_items < ITEM_COUNT) ? r_items : ITEM_COUNT;

      if ((void *)&res != (void *)&self) {
        res = self;
      }
      unsigned i = 0;
      for (; i < op_items; i++) {
        uint64_t r_val = get_item(rhs, i);
        res.items_[i] &= r_val;
      }
      if (!is_negative(rhs)) {
        // for positive integer, set all rest items to 0
        for (; i < ITEM_COUNT; i++) {
          res.items_[i] = 0;
        }
      }
    } else {
      using calc_type = typename CommonType<ObWideInteger<Bits, Signed>, T>::type;
      calc_type xres;
      ret = calc_type::_impl::template bitwise_and(calc_type(self), rhs, xres);
      res = xres;
    }
    return ret;
  }
};

template <typename Allocator>
int from_number(const number::ObNumber &nmb, Allocator &allocator, const int16_t scale,
                ObDecimalInt *&decint, int32_t &int_bytes)
{
  static const int64_t pows[5] = {
    10,
    100,
    10000,
    100000000,
    10000000000000000,
  };
  int ret = OB_SUCCESS;
  int512_t res = 0;
  bool is_neg = nmb.is_negative();
  int64_t in_scale = nmb.get_scale();
  uint32_t nmb_base = number::ObNumber::BASE;
  uint32_t *digits = nmb.get_digits();
  uint32_t last_digit = 0, last_base = nmb_base;
  void *cp_data = nullptr;
  if (nmb.is_zero()) {
    if (OB_ISNULL(decint = (ObDecimalInt *)allocator.alloc(sizeof(int32_t)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WARN, "allocate memory failed", K(ret));
    } else {
      *(reinterpret_cast<int32_t *>(decint)) = 0;
      int_bytes = sizeof(int32_t);
    }
  } else {
    if (OB_ISNULL(digits)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "invalid null digits");
    } else {
      last_digit = digits[nmb.get_length() - 1];
      if (in_scale > 0) { // remove trailing zeros for decimal part
        while (last_digit != 0 && last_digit % 10 == 0) { last_digit /= 10; }
        int cnt = in_scale % number::ObNumber::DIGIT_LEN;
        if (cnt != 0) { last_base = 1; }
        for (int i = 0; i < cnt; i++) { last_base *= 10; }
      }
    }
    for (int i = 0; OB_SUCC(ret) && i < nmb.get_length(); i++) {
      if (i == nmb.get_length() - 1) {
        res = res * last_base + last_digit;
      } else {
        res = res * nmb_base + digits[i];
      }
    }
    if (in_scale <= 0) {
      ObNumberDesc desc = nmb.get_desc();
      int32 nmb_exp = (number::ObNumber::POSITIVE == desc.sign_ ?
                         (desc.se_ - number::ObNumber::POSITIVE_EXP_BOUNDARY) :
                         (number::ObNumber::NEGATIVE_EXP_BOUNDARY - desc.se_));
      for (int i = 0; i < nmb_exp - desc.len_ + 1; i++) {
        res = res * nmb_base;
      }
    }
    if (OB_SUCC(ret)) {
      if (in_scale > scale) {
        int64_t scale_down = in_scale - scale;
        while (scale_down > 0 && res != 0) {
          for (int i = ARRAYSIZEOF(pows) - 1; scale_down > 0 && res != 0 && i >= 0; i--) {
            if (scale_down & (1 << i)) {
              res = res / pows[i];
              scale_down -= (1 << i);
            }
          } // for end
          if (scale_down > 0) {
            scale_down -= 1;
            res = res / 10;
          }
        }
      } else if (in_scale < scale) {
        int64_t scale_up = scale - in_scale;
        while (scale_up > 0 && res != 0) {
          for (int i = ARRAYSIZEOF(pows) - 1; scale_up > 0 && res != 0 && i >= 0; i--) {
            if (scale_up & (1 << i)) {
              res = res * pows[i];
              scale_up -= (1 << i);
            }
          } // for end
          if (scale_up > 0) {
            scale_up -= 1;
            res = res * 10;
          }
        }
      }
      if (is_neg) { res = -res; }

      ObDecimalIntBuilder bld;
      if (res <= wide::Limits<int32_t>::max() && res >= wide::Limits<int32_t>::min()) {
        int_bytes = sizeof(int32_t);
      } else if (res <= wide::Limits<int64_t>::max() && res >= wide::Limits<int64_t>::min()) {
        int_bytes = sizeof(int64_t);
      } else if (res <= wide::Limits<int128_t>::max() && res >= wide::Limits<int128_t>::min()) {
        int_bytes = sizeof(int128_t);
      } else if (res <= wide::Limits<int256_t>::max() && res >= wide::Limits<int256_t>::min()) {
        int_bytes = sizeof(int256_t);
      } else {
        int_bytes = sizeof(int512_t);
      }
      if (OB_ISNULL(cp_data = allocator.alloc(int_bytes))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        COMMON_LOG(WARN, "allocate memory failed", K(ret));
      } else {
        MEMCPY(cp_data, res.items_, int_bytes);
        decint = reinterpret_cast<ObDecimalInt *>(cp_data);
      }
    }
  }
  return ret;
}

template <unsigned Bits, typename Signed, typename Allocator>
int to_number(const ObWideInteger<Bits, Signed> &x, int16_t scale, Allocator &allocator,
              number::ObNumber &numb)
{
  int ret = OB_SUCCESS;
  static const uint64_t pows[] = {
    10,
    100,
    1000,
    10000,
    100000,
    1000000,
    10000000,
    100000000,
    1000000000,
  };
  bool is_neg = ObWideInteger<Bits, Signed>::_impl::is_negative(x);
  ObWideInteger<Bits, unsigned> numerator = ObWideInteger<Bits, unsigned>::_impl::make_positive(x);
  ObWideInteger<Bits, unsigned> quo;
  ObWideInteger<Bits, unsigned> rem;
  uint32_t digits[number::ObNumber::MAX_STORE_LEN];
  int idx = number::ObNumber::MAX_STORE_LEN;
  uint32_t nmb_base = number::ObNumber::BASE;
  uint8_t exp, digit_len;
  if (scale >= 0) {
    int64_t calc_exp = -1;
    if ((scale % number::ObNumber::DIGIT_LEN) != 0) {
      int rem_scale = scale % (number::ObNumber::DIGIT_LEN);
      ObWideInteger<Bits, unsigned>::_impl::template divide<IgnoreOverFlow>(
        numerator, pows[rem_scale - 1], quo, rem);
      digits[--idx] = static_cast<uint32_t>(
        static_cast<uint32_t>(rem.items_[0]) * pows[number::ObNumber::DIGIT_LEN - rem_scale - 1]);
      numerator = quo;
      scale -= rem_scale;
    }
    for (; OB_SUCC(ret) && scale > 0 && numerator != 0;) {
      if (OB_UNLIKELY(idx <= 0)) {
        ret = OB_ERROR_OUT_OF_RANGE;
        COMMON_LOG(WARN, "out of number range", K(ret));
      } else {
        ObWideInteger<Bits, unsigned>::_impl::template divide<IgnoreOverFlow>(numerator, nmb_base,
                                                                              quo, rem);
        digits[--idx] = (uint32_t)rem.items_[0];
        numerator = quo;
        scale -= number::ObNumber::DIGIT_LEN;
      }
    }
    while (OB_SUCC(ret) && scale > 0) {
      scale -= number::ObNumber::DIGIT_LEN;
      calc_exp--;
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (numerator == 0) {
      exp = 0x7F & (uint8_t)(number::ObNumber::EXP_ZERO + calc_exp);
      digit_len = (uint8_t)(number::ObNumber::MAX_STORE_LEN - idx);
    } else {
      calc_exp = -1;
      for (; OB_SUCC(ret) && numerator != 0;) {
        if (OB_UNLIKELY(idx <= 0)) {
          ret = OB_ERROR_OUT_OF_RANGE;
          COMMON_LOG(WARN, "out of number range", K(ret));
        } else {
          ObWideInteger<Bits, unsigned>::_impl::template divide<IgnoreOverFlow>(numerator, nmb_base,
                                                                                quo, rem);
          digits[--idx] = (uint32_t)rem.items_[0];
          numerator = quo;
          calc_exp++;
        }
      }
      if(OB_SUCC(ret)) {
        exp = 0x7F & (uint8_t)(number::ObNumber::EXP_ZERO + calc_exp);
        digit_len = (uint8_t)(number::ObNumber::MAX_STORE_LEN - idx);
      }
    }
  } else {
    int64_t calc_exp = 0;
    scale = -scale;
    for (; scale >= number::ObNumber::DIGIT_LEN;) {
      calc_exp += 1;
      scale -= number::ObNumber::DIGIT_LEN;
    }
    if (scale > 0) {
      ObWideInteger<Bits, unsigned>::_impl::template divide<IgnoreOverFlow>(
        numerator, pows[number::ObNumber::DIGIT_LEN - scale - 1], quo, rem);
      digits[--idx] = (uint32_t)rem.items_[0];
      numerator = quo;
      calc_exp++;
    }
    while (OB_SUCC(ret) && numerator != 0) {
      if (OB_UNLIKELY(idx <= 0)) {
        ret = OB_ERROR_OUT_OF_RANGE;
        COMMON_LOG(WARN, "out of number range", K(ret));
      } else {
        ObWideInteger<Bits, unsigned>::_impl::template divide<IgnoreOverFlow>(numerator, nmb_base,
                                                                              quo, rem);
        digits[--idx] = (uint32_t)rem.items_[0];
        numerator = quo;
        calc_exp++;
      }
    }
    if (OB_SUCC(ret)) {
      exp = 0x7F & (uint8_t)(number::ObNumber::EXP_ZERO + calc_exp);
      digit_len = (uint8_t)(number::ObNumber::MAX_STORE_LEN - idx);
    }
  }
  if (OB_SUCC(ret)) {
    ObNumberDesc desc;
    desc.len_ = digit_len;
    desc.exp_ = exp;
    desc.sign_ = is_neg ? number::ObNumber::NEGATIVE : number::ObNumber::POSITIVE;
    if (is_neg) {
      desc.exp_ = 0x7F & (~desc.exp_);
      desc.exp_++;
    }
    uint32_t *copy_digits = nullptr;
    if (digit_len > 0) {
      if (OB_ISNULL(copy_digits = (uint32_t *)allocator.alloc(digit_len * sizeof(uint32_t)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        COMMON_LOG(WARN, "allocate memory failed", K(ret));
      } else {
        numb.assign(desc.desc_, copy_digits);
        if (OB_FAIL(numb.normalize_(digits + idx, digit_len))) {
          COMMON_LOG(WARN, "normalize number failed", K(ret));
        }
      }
    } else if (digit_len == 0) {
      numb.set_zero();
    }
  }
  return ret;
}

template<typename Allocator>
int to_number(const int64_t v, int16_t scale, Allocator &allocator, number::ObNumber &nmb)
{
  int ret = OB_SUCCESS;
  static const uint64_t pows[] = {
    10,
    100,
    1000,
    10000,
    100000,
    1000000,
    10000000,
    100000000,
    1000000000,
  };
  bool is_neg = (v < 0);
  uint64_t numerator = v;
  if (is_neg) {
    numerator = (v == INT64_MIN ? static_cast<uint64_t>(INT64_MAX) + 1 : static_cast<uint64_t>(-v));
  }
  uint32_t digits[number::ObNumber::MAX_STORE_LEN];
  int idx = number::ObNumber::MAX_STORE_LEN;
  uint32_t nmb_base = number::ObNumber::BASE;
  uint8_t exp, digit_len;

  if (scale >= 0) {
    int64_t calc_exp = -1;
    if ((scale % number::ObNumber::DIGIT_LEN) != 0) {
      int16_t rem_scale = scale % number::ObNumber::DIGIT_LEN;
      digits[--idx] = static_cast<uint32_t>(
        (numerator % pows[rem_scale - 1]) * pows[number::ObNumber::DIGIT_LEN - rem_scale - 1]);
      numerator /= pows[rem_scale - 1];
      scale -= rem_scale;
    }
    for (; OB_SUCC(ret) && scale > 0 && numerator > 0;) {
      digits[--idx] = numerator % nmb_base;
      numerator /= nmb_base;
      scale -= number::ObNumber::DIGIT_LEN;
    }
    while (OB_SUCC(ret) && scale > 0) {
      scale -= number::ObNumber::DIGIT_LEN;
      calc_exp--;
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (numerator == 0) {
      exp = 0x7F & (uint8_t)(number::ObNumber::EXP_ZERO + calc_exp);
      digit_len = (uint8_t)(number::ObNumber::MAX_STORE_LEN - idx);
    } else {
      calc_exp = -1;
      while (OB_SUCC(ret) && numerator != 0) {
        digits[--idx] = numerator % nmb_base;
        numerator /= nmb_base;
        calc_exp++;
      }
      exp = 0x7F & (uint8_t)(number::ObNumber::EXP_ZERO + calc_exp);
      digit_len = (uint8_t)(number::ObNumber::MAX_STORE_LEN - idx);
    }
  } else {
    int64_t calc_exp = 0;
    scale = -scale;
    while (scale >= number::ObNumber::DIGIT_LEN) {
      calc_exp += 1;
      scale -= number::ObNumber::DIGIT_LEN;
    }
    if (scale > 0) {
      digits[--idx] =
        static_cast<uint32_t>(numerator % (pows[number::ObNumber::DIGIT_LEN - scale - 1]));
      numerator /= pows[number::ObNumber::DIGIT_LEN - scale - 1];
      calc_exp++;
    }
    while (numerator > 0) {
      digits[--idx] = numerator % nmb_base;
      numerator /= nmb_base;
      calc_exp++;
    }
    exp = 0x7F & (uint8_t)(number::ObNumber::EXP_ZERO + calc_exp);
    digit_len = (uint8_t)(number::ObNumber::MAX_STORE_LEN - idx);
  }
  if (OB_SUCC(ret)) {
    ObNumberDesc desc;
    desc.len_ = digit_len;
    desc.exp_ = exp;
    desc.sign_ = is_neg ? number::ObNumber::NEGATIVE : number::ObNumber::POSITIVE;
    if (is_neg) {
      desc.exp_ = 0x7F & (~desc.exp_);
      desc.exp_++;
    }
    uint32_t *copy_digits = nullptr;
    if (digit_len > 0) {
      if (OB_ISNULL(copy_digits = (uint32_t *)allocator.alloc(digit_len * sizeof(uint32_t)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        COMMON_LOG(WARN, "allocate memory failed", K(ret));
      } else {
        nmb.assign(desc.desc_, copy_digits);
        if (OB_FAIL(nmb.normalize_(digits + idx, digit_len))) {
          COMMON_LOG(WARN, "normalize number failed", K(ret));
        }
      }
    } else if (digit_len == 0) {
      nmb.set_zero();
    }
  }
  return ret;
}

int to_number(const int64_t v, int16_t scale, uint32_t *digits, int32_t digit_len,
              number::ObNumber &nmb);

template <typename Allocator>
int to_number(const ObDecimalInt *decint, const int32_t int_bytes, int16_t scale,
              Allocator &allocator, number::ObNumber &numb)
{
#define ASSIGN_NMB(int_type)\
  const int_type *v = reinterpret_cast<const int_type *>(decint);\
  ret = to_number(*v, scale, allocator, numb);\

  int ret = OB_SUCCESS;
  if (OB_ISNULL(decint) || OB_UNLIKELY(int_bytes <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid decimal int", K(decint), K(int_bytes));
  } else if (OB_UNLIKELY(scale == NUMBER_SCALE_UNKNOWN_YET)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "invalid scale", K(ret), K(scale), K(lib::is_oracle_mode()));
  } else {
    DISPATCH_WIDTH_TASK(int_bytes, ASSIGN_NMB);
  }
  return ret;
#undef ASSIGN_NMB
}

template<typename T, typename Allocator>
int from_integer(const T v, Allocator &allocator, ObDecimalInt *&decint, int32_t &int_bytes)
{
  int ret = OB_SUCCESS;
  static_assert(std::is_integral<T>::value && sizeof(T) <= sizeof(int64_t), "");
  void *data = nullptr;
  if (std::is_signed<T>::value || (sizeof(T) == sizeof(int32_t) && v <= INT32_MAX)
      || (sizeof(T) == sizeof(int64_t) && v <= INT64_MAX)) {
    int32_t alloc_size = sizeof(T);
    T *data = (T *)allocator.alloc(alloc_size);
    if (OB_ISNULL(data)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WARN, "allocate memory failed", K(ret), K(alloc_size));
    } else {
      *data = v;
      decint = (ObDecimalInt *)data;
      int_bytes = alloc_size;
    }
  } else if (sizeof(T) == sizeof(int32_t)) {
    int_bytes = sizeof(int64_t);
    int64_t *data = (int64_t *)allocator.alloc(int_bytes);
    if (OB_ISNULL(data)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WARN, "allocate memory failed", K(ret), K(int_bytes));
    } else {
      *data = v;
      decint = (ObDecimalInt *)data;
    }
  } else {
    int128_t *data = (int128_t *)allocator.alloc(sizeof(int128_t));
    if (OB_ISNULL(data)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WARN, "allocate memory failed", K(ret), K(sizeof(int128_t)));
    } else {
      data->items_[0] = v;
      data->items_[1] = 0;
      decint = (ObDecimalInt *)data;
      int_bytes = sizeof(int128_t);
    }
  }
  return ret;
}

template <typename T, typename Allocator>
int from_integer(const T v, Allocator &allocator, ObDecimalInt *&decint, int32_t &int_bytes,
                 const int16_t in_prec)
{
  int ret = OB_SUCCESS;
  static_assert(std::is_integral<T>::value && sizeof(T) <= sizeof(int64_t), "");
  int_bytes = ObDecimalIntConstValue::get_int_bytes_by_precision(in_prec);
  void *data = nullptr;
  if (OB_UNLIKELY(int_bytes <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "invalid input precision", K(ret), K(in_prec));
  } else if (OB_ISNULL(data = allocator.alloc(int_bytes))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(WARN, "allocate memory failed", K(ret), K(int_bytes));
  } else {
    decint = reinterpret_cast<ObDecimalInt *>(data);
    switch (int_bytes) {
    case sizeof(int32_t):
      *decint->int32_v_ = static_cast<const int32_t>(v);
      break;
    case sizeof(int64_t):
      *decint->int64_v_ = v;
      break;
    case sizeof(int128_t):
      *decint->int128_v_ = v;
      break;
    case sizeof(int256_t):
      *decint->int256_v_ = v;
      break;
    case sizeof(int512_t):
      *decint->int512_v_ = v;
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "invalid int bytes for integer", K(ret), K(int_bytes));
      break;
    }
  }
  return ret;
}

template <typename T, typename Allocator>
int batch_from_integer(const T *val_arr, Allocator &allocator, const ObDecimalInt **decint_arr,
                       int32_t &int_bytes, const int16_t in_prec, const int64_t batch_size)
{
#define ASSIGN_VAL(int_type)                                                                       \
  for (int i = 0; i < batch_size; i++) {                                                           \
    int_type *vals = reinterpret_cast<int_type *>(data);                                           \
    *(vals + i) = val_arr[i];                                                                      \
    decint_arr[i] = reinterpret_cast<const ObDecimalInt *>(vals + i);                              \
  }

  int ret = OB_SUCCESS;
  static_assert(std::is_integral<T>::value && sizeof(T) <= sizeof(int64_t), "");
  int_bytes = ObDecimalIntConstValue::get_int_bytes_by_precision(in_prec);
  void *data = nullptr;
  if (OB_UNLIKELY(int_bytes <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "invalid input precision", K(ret), K(in_prec));
  } else if (OB_ISNULL(data = allocator.alloc(int_bytes * batch_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(WARN, "allocate memory failed", K(ret));
  } else {
    DISPATCH_WIDTH_TASK(int_bytes, ASSIGN_VAL);
  }
  return ret;
#undef ASSIGN_VAL
}

int from_double(const double x, ObIAllocator &allocator, ObDecimalInt *&decint, int32_t &int_bytes,
                int16_t &scale);

template<typename T, class = typename std::enable_if<sizeof(T) <= sizeof(int64_t)>::type>
bool mul_overflow(const T &lhs, const T &rhs, T &res)
{
  return __builtin_mul_overflow(lhs, rhs, &res);
}

template <unsigned Bits, typename Signed>
bool mul_overflow(const ObWideInteger<Bits, Signed> &lhs, const ObWideInteger<Bits, Signed> &rhs,
                  ObWideInteger<Bits, Signed> &res)
{
  int ret = ObWideInteger<Bits, Signed>::_impl::template multiply<CheckOverFlow>(lhs, rhs, res);
  return (ret == OB_OPERATE_OVERFLOW);
}
} // end namespace wide
} // end namespace common
} // end namespace oceanbase

#endif // !OB_WIDE_INTEGER_IMPL_H_
