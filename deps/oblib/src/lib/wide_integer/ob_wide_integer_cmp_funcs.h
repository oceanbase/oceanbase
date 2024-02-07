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

#ifndef OB_WIDE_INTEGER_CMP_FUNCS_
#define OB_WIDE_INTEGER_CMP_FUNCS_

// #include "lib/wide_integer/ob_wide_integer.h"

// typedef int (*decint_obj_cmp_fp)(const oceanbase::common::ObObj &lhs,
//                                  const oceanbase::common::ObObj &rhs);
typedef int (*decint_cmp_fp)(const oceanbase::common::ObDecimalInt *lhs,
                             const oceanbase::common::ObDecimalInt *rhs);
namespace oceanbase
{
namespace common
{
namespace datum_cmp
{
template<ObDecimalIntWideType l, ObDecimalIntWideType r>
struct ObDecintCmp;
} // namespace datum_cmp

namespace wide
{

class ObDecimalIntCmpSet
{
public:
  static const constexpr int32_t DECIMAL_LEN = 64;
  static inline decint_cmp_fp get_decint_decint_cmp_func(int32_t len1, int32_t len2)
  {
    if (len1 > DECIMAL_LEN || len2 > DECIMAL_LEN) {
      return nullptr;
    } else {
      return decint_decint_cmp_set_[len1][len2];
    }
  }
private:
  template<int32_t, int32_t>
  friend struct InitDecimalIntCmpSet;

  template<ObDecimalIntWideType l, ObDecimalIntWideType r>
  friend struct datum_cmp::ObDecintCmp;

  static decint_cmp_fp decint_decint_cmp_set_[DECIMAL_LEN + 1][DECIMAL_LEN +1];
};

// TODO: unittest
// decimal int 比较，scale必须先提升到一样
// helper functions
template<typename T, typename P>
int compare(const T &lhs, const P &rhs, int &result)
{
  int ret = OB_SUCCESS;
  const ObDecimalInt *lhs_decint = lhs.get_decimal_int();
  const ObDecimalInt *rhs_decint = rhs.get_decimal_int();
  int32_t lhs_bytes = lhs.get_int_bytes();
  int32_t rhs_bytes = rhs.get_int_bytes();
  decint_cmp_fp cmp = ObDecimalIntCmpSet::get_decint_decint_cmp_func(lhs_bytes, rhs_bytes);
  if (OB_ISNULL(cmp)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "failed to get compare function", K(ret), K(lhs_bytes), K(rhs_bytes));
  } else {
    result = cmp(lhs_decint, rhs_decint);
  }
  return ret;
}

template<typename T, typename P>
bool abs_equal(const T &lhs, const P &rhs)
{
#define ABS_CMP(ltype, rtype)                                                                      \
  const ltype &lv = *reinterpret_cast<const ltype *>(lhs_decint);                                  \
  const rtype &rv = *reinterpret_cast<const rtype *>(rhs_decint);                                  \
  is_equal = (((lv + rv) == 0) || (lv == rv));

  int ret = OB_SUCCESS;
  bool is_equal = false;
  const ObDecimalInt *lhs_decint = lhs.get_decimal_int();
  const ObDecimalInt *rhs_decint = rhs.get_decimal_int();
  int32_t lhs_bytes = lhs.get_int_bytes();
  int32_t rhs_bytes = rhs.get_int_bytes();

  DISPATCH_INOUT_WIDTH_TASK(lhs_bytes, rhs_bytes, ABS_CMP);
  return is_equal;
#undef ABS_CMP
}
} // end namespace wide
} // end namespace common
} // end namespace oceanbase
#endif // !OB_WIDE_INTEGER_CMP_FUNCS_