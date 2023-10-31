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

#include "lib/wide_integer/ob_wide_integer.h"

#include "common/object/ob_object.h"

namespace oceanbase
{
namespace common
{
namespace wide
{
template<int32_t L0, int32_t L1>
int decimalint_decimalint_cmp_func(const ObDecimalInt *lhs, const ObDecimalInt *rhs)
{
  int ret = 0;
  if (L0 < L1) {
    ret = -decimalint_decimalint_cmp_func<L1, L0>(rhs, lhs);
  }
  return ret;
}

template<int32_t L, int tc>
int decimalint_tc_cmp_func(const ObObj &lhs, const ObObj &obj)
{
  int ret = 0;
  return ret;
}

#define DEFINE_DECINT_DECINT_CMP_FUNC(LEN0, LEN1)                                                  \
  template <>                                                                                      \
  int decimalint_decimalint_cmp_func<LEN0, LEN1>(const ObDecimalInt *lhs_dec,                      \
                                                 const ObDecimalInt *rhs_dec)

#define DEFINE_DECINT_TC_CMP_FUNC(LEN, TC)                                                         \
  template <>                                                                                      \
  int decimalint_tc_cmp_func<LEN, TC>(const ObObj &lhs, const ObObj &rhs)

#define REG_DECINT_DECINT_CMP_FUNC(LEN0, LEN1)                                                     \
  ObDecimalIntCmpSet::decint_decint_cmp_set_[LEN0][LEN1] =                                         \
    decimalint_decimalint_cmp_func<LEN0, LEN1>

decint_cmp_fp ObDecimalIntCmpSet::decint_decint_cmp_set_[ObDecimalIntCmpSet::DECIMAL_LEN + 1]
                                                            [ObDecimalIntCmpSet::DECIMAL_LEN + 1];

#define DEF_DECINT_DECINT_CMP_FUNC(LEN0, LEN1, lhs_type, rhs_type)                                 \
  DEFINE_DECINT_DECINT_CMP_FUNC(LEN0, LEN1) {                                                      \
    int ret = 0;                                                                                   \
    const lhs_type##_t *lop = static_cast<const lhs_type##_t *>(lhs_dec->lhs_type##_v_);           \
    const rhs_type##_t *rop = static_cast<const rhs_type##_t *>(rhs_dec->rhs_type##_v_);           \
    return lop->cmp(*rop);                                                                         \
  }

#define DEF_DECINT_SMALL_DECINT_FUNC(LEN0, LEN1, lhs_type, rhs_type)                               \
  DEFINE_DECINT_DECINT_CMP_FUNC(LEN0, LEN1) {                                                      \
    int ret = 0;                                                                                   \
    const lhs_type##_t *lop = static_cast<const lhs_type##_t *>(lhs_dec->lhs_type##_v_);           \
    rhs_type##_t rop = *(rhs_dec->rhs_type##_v_);                                                  \
    return lop->cmp(rop);                                                                          \
  }

#define DEF_SMALL_DECINT_SMALL_DECINT_FUNC(LEN0, LEN1, lhs_type, rhs_type)                         \
  DEFINE_DECINT_DECINT_CMP_FUNC(LEN0, LEN1) {                                                      \
    int ret = 0;                                                                                   \
    lhs_type##_t lop = *(lhs_dec->lhs_type##_v_);                                                  \
    rhs_type##_t rop = *(rhs_dec->rhs_type##_v_);                                                  \
    if (lop < rop) {                                                                               \
      ret = -1;                                                                                    \
    } else if (lop > rop) {                                                                        \
      ret = 1;                                                                                     \
    }                                                                                              \
    return ret;                                                                                    \
  }
// ================ DecimalInt cmps DecimalInt
DEF_DECINT_DECINT_CMP_FUNC(64, 64, int512, int512);
DEF_DECINT_DECINT_CMP_FUNC(64, 32, int512, int256);
DEF_DECINT_DECINT_CMP_FUNC(64, 16, int512, int128);
DEF_DECINT_SMALL_DECINT_FUNC(64, 8, int512, int64);
DEF_DECINT_SMALL_DECINT_FUNC(64, 4, int512, int32);

DEF_DECINT_DECINT_CMP_FUNC(32, 32, int256, int256);
DEF_DECINT_DECINT_CMP_FUNC(32, 16, int256, int128);
DEF_DECINT_SMALL_DECINT_FUNC(32, 8, int256, int64);
DEF_DECINT_SMALL_DECINT_FUNC(32, 4, int256, int32);

DEF_DECINT_DECINT_CMP_FUNC(16, 16, int128, int128);
DEF_DECINT_SMALL_DECINT_FUNC(16, 8, int128, int64);
DEF_DECINT_SMALL_DECINT_FUNC(16, 4, int128, int32);

DEF_SMALL_DECINT_SMALL_DECINT_FUNC(8, 8, int64, int64);
DEF_SMALL_DECINT_SMALL_DECINT_FUNC(8, 4, int64, int32);

DEF_SMALL_DECINT_SMALL_DECINT_FUNC(4, 4, int32, int32);

#define DEF_DECINT_INTTC_CMP(LEN, tc, lhs_type, rhs_type)                                          \
  DEFINE_DECINT_TC_CMP_FUNC(LEN, tc) {                                                             \
    const ObDecimalInt *lhs_dec = lhs.get_decimal_int();                                           \
    const lhs_type##_t *lop = static_cast<const lhs_type##_t *>(lhs_dec->lhs_type##_v_);           \
    rhs_type##_t rop = rhs.v_.rhs_type##_;                                                         \
    return lop->cmp(rop);                                                                          \
  }

#define DEF_SMALL_DECINT_XXX_CMP(LEN, tc, lhs_type, rhs_type)                                       \
  DEFINE_DECINT_TC_CMP_FUNC(LEN, tc) {                                                       \
    int ret = 0;                                                                                   \
    const ObDecimalInt *lhs_dec = lhs.get_decimal_int();                                           \
    lhs_type##_t lop = *(lhs_dec->lhs_type##_v_);                                                  \
    rhs_type rop = rhs.v_.rhs_type##_;                                                             \
    if (lop < 0 || lop < rop) {                                                                    \
      return -1;                                                                                   \
    } else if (lop > 0 && lop > rop) {                                                             \
      return 1;                                                                                    \
    }                                                                                              \
    return ret;                                                                                    \
  }

template<int32_t L0, int32_t L1>
struct InitDecimalIntCmpSet
{
  static int init()
  {
    REG_DECINT_DECINT_CMP_FUNC(L0, L1);
    if (L1 >= ObDecimalIntCmpSet::DECIMAL_LEN) {
      return InitDecimalIntCmpSet<L0 + L0, 4>::init();
    } else {
      return InitDecimalIntCmpSet<L0, L1+L1>::init();
    }
  }
};

template<int32_t L1>
struct InitDecimalIntCmpSet<ObDecimalIntCmpSet::DECIMAL_LEN * 2, L1>
{
  static int init() { return 0; }
};

template<int32_t L0>
struct InitDecimalIntCmpSet<L0, ObDecimalIntCmpSet::DECIMAL_LEN * 2>
{
  static int init() { return 0; }
};

static int init_decimalint_decimalint_cmpfunc_ret = InitDecimalIntCmpSet<4, 4>::init();
} // end namespace wide
} // end namespace common
} // end namespace oceanbase