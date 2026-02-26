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

#ifndef OCEANBASE_SHARE_VECTOR_VECTOR_OP_UTIL_H_
#define OCEANBASE_SHARE_VECTOR_VECTOR_OP_UTIL_H_
#include "share/vector/vector_basic_op.h"
#include "sql/engine/expr/ob_expr.h"

namespace oceanbase
{
namespace common
{

template <typename T, bool IS_VEC>
struct VectorIter
{
  explicit VectorIter(T *vec) : vec_(vec) {}
  OB_INLINE T &operator[](const int64_t i) const { return IS_VEC ? vec_[i] : vec_[0]; }
private:
  T *vec_;
};

using BatchHashResIter = VectorIter<uint64_t, true>;
using RowHashResIter = VectorIter<uint64_t, false>;

template<typename VectorType>
struct VectorOpUtil
{
  using Op = typename VectorType::VecTCBasicOp;

  template<typename HashMethod, bool hash_v2, typename HashResIter>
  static int hash_dispatch(HashResIter &hash_values,
                           const ObObjMeta &meta,
                           const VectorType &vec,
                           const sql::ObBitVector &skip,
                           const sql::EvalBound &bound,
                           const uint64_t *seeds,
                           const bool is_batch_seed)
  {
    int ret = OB_SUCCESS;
    #define VEC_HASH_PROC(has_null, batch_seed, all_active) \
      using SeedIter = VectorIter<const uint64_t, batch_seed>;    \
      ret = vec_hash<HashMethod, hash_v2, SeedIter, has_null, all_active, HashResIter>( \
                hash_values, meta, vec, skip, bound, SeedIter(seeds));
    if (!vec.has_null() && is_batch_seed && bound.get_all_rows_active()) {
      VEC_HASH_PROC(false, true, true);
    } else if (!vec.has_null() && !is_batch_seed && bound.get_all_rows_active()) {
      VEC_HASH_PROC(false, false, true);
    } else if (vec.has_null() && is_batch_seed && bound.get_all_rows_active()) {
      VEC_HASH_PROC(true, true, true);
    } else if (vec.has_null() && !is_batch_seed && bound.get_all_rows_active()) {
      VEC_HASH_PROC(true, false, true);
    } else if (!vec.has_null() && is_batch_seed && !bound.get_all_rows_active()) {
      VEC_HASH_PROC(false, true, false);
    } else if (!vec.has_null() && !is_batch_seed && !bound.get_all_rows_active()) {
      VEC_HASH_PROC(false, false, false);
    } else if (vec.has_null() && is_batch_seed && !bound.get_all_rows_active()) {
      VEC_HASH_PROC(true, true, false);
    } else if (vec.has_null() && !is_batch_seed && !bound.get_all_rows_active()) {
      VEC_HASH_PROC(true, false, false);
    }
    #undef DEF_FLOATING_HASH_CALC

    return ret;
  }

  template<bool null_first>
  inline static int ns_cmp(const ObObjMeta &meta,
                           const VectorType &vec,
                           const int64_t row_idx,
                           const bool r_null,
                           const void *r_v,
                           const ObLength r_len,
                           int &cmp_ret)
  {
    int ret = OB_SUCCESS;
    bool l_null = vec.is_null(row_idx);
    if (OB_UNLIKELY(l_null && r_null)) {
      cmp_ret = 0;
    } else if (OB_UNLIKELY(l_null)) {
      cmp_ret = null_first? -1 : 1;
    } else if (OB_UNLIKELY(r_null)) {
      cmp_ret = null_first ? 1 : -1;
    } else {
      ret = Op::cmp(meta, vec.get_payload(row_idx), vec.get_length(row_idx), r_v, r_len, cmp_ret);
    }

    return ret;
  }

private:
  template<typename HashMethod, bool hash_v2, typename SeedVec, bool has_null, bool all_active, typename HashResIter>
  static int vec_hash(HashResIter &hash_values,
                      const ObObjMeta &meta,
                      const VectorType &vec,
                      const sql::ObBitVector &skip,
                      const sql::EvalBound &bound,
                      SeedVec seed_vec)
  {
    int ret = OB_SUCCESS;
    if (!has_null && all_active) {
      for (int64_t i = bound.start(); OB_SUCC(ret) && i < bound.end(); i++) {
        ret = Op::template hash<HashMethod, hash_v2>(meta, vec.get_payload(i),
                                                     vec.get_length(i), seed_vec[i],
                                                     hash_values[i]);
      }
    } else if (has_null && all_active) {
      uint64_t v = 0;
      for (int64_t i = bound.start(); OB_SUCC(ret) && i < bound.end(); i++) {
        if (vec.is_null(i)) {
          ret = Op::template null_hash<HashMethod, hash_v2>(seed_vec[i], hash_values[i]);
        } else {
          ret = Op::template hash<HashMethod, hash_v2>(meta, vec.get_payload(i),
                                            vec.get_length(i), seed_vec[i], hash_values[i]);
        }
      }
    } else if (!has_null && !all_active) {
      auto op = [&](const int64_t i) __attribute__((always_inline)) {
        return Op::template hash<HashMethod, hash_v2>(meta, vec.get_payload(i),
                                          vec.get_length(i), seed_vec[i], hash_values[i]);
      };
      ret = sql::ObBitVector::flip_foreach(skip, bound, op);
    } else { /*has_null && !all_active*/
      auto op = [&](const int64_t i) __attribute__((always_inline)) {
        int ret = OB_SUCCESS;
        if (vec.is_null(i)) {
          ret = Op::template null_hash<HashMethod, hash_v2>(seed_vec[i], hash_values[i]);
        } else {
          ret = Op::template hash<HashMethod, hash_v2>(meta, vec.get_payload(i),
                                           vec.get_length(i), seed_vec[i], hash_values[i]);
        }
        return ret;
      };
      ret = sql::ObBitVector::flip_foreach(skip, bound, op);
    }
    return ret;
  }
};

struct VectorRangeUtil
{
  template <typename OP>
  static int lower_bound(sql::ObExpr *expr, sql::ObEvalCtx &ctx, const sql::EvalBound &bound,
                         const sql::ObBitVector &skip, OP cmp_op, int64_t &iter)
  {
    return common_bound<OP, true>(expr, ctx, bound, skip, cmp_op, iter);
  }

  template <typename OP>
  static int upper_bound(sql::ObExpr *expr, sql::ObEvalCtx &ctx, const sql::EvalBound &bound,
                         const sql::ObBitVector &skip, OP cmp_op, int64_t &iter)
  {
    return common_bound<OP, false>(expr, ctx, bound, skip, cmp_op, iter);
  }

  struct NullSafeCmp
  {
    NullSafeCmp(ObObjMeta &obj_meta, const sql::NullSafeRowCmpFunc cmp, const char *value,
                const int32_t len, bool is_null, bool is_ascending) :
      obj_meta_(obj_meta),
      cmp_(cmp), value_(value), len_(len), is_null_(is_null), is_ascending_(is_ascending)
    {}

    OB_INLINE int operator()(const ObObjMeta &other_meta, const char *other, int32_t other_len,
                             const bool other_null, int &cmp_ret) const
    {
      int ret = OB_SUCCESS;
      cmp_ret = 0;
      if (OB_FAIL(cmp_(other_meta, obj_meta_,
                       other, other_len, other_null,
                       value_, len_, is_null_,
                       cmp_ret))) {
        COMMON_LOG(WARN, "compare failed", K(ret));
      } else {
        cmp_ret *= (is_ascending_ ? 1 : -1);
      }
      return ret;
    }
  private:
    ObObjMeta &obj_meta_;
    const sql::NullSafeRowCmpFunc cmp_;
    const char *value_;
    const int32_t len_;
    const bool is_null_;
    const bool is_ascending_;
  };

private:
  template <typename OP, bool is_lower>
  static int common_bound(sql::ObExpr *expr, sql::ObEvalCtx &ctx, const sql::EvalBound &bound,
                          const sql::ObBitVector &skip, OP cmp_op, int64_t &iter);
  template <typename OP, typename VecFmt, bool is_lower>
  static int find_bound(const ObObjMeta &obj_meta, ObIVector *ivector, sql::ObEvalCtx &ctx,
                        const sql::EvalBound &bound, const sql::ObBitVector &skip, OP cmp_op,
                        int64_t &iter);
};

class ObDiscreteFormat;
template<bool is_const>
class ObUniformFormat;
class ObContinuousFormat;
template<typename ValType>
class ObFixedLengthFormat;

#define FIND_BOUND_USE_FMT(fmt)                                                                    \
  ret = find_bound<OP, fmt, is_lower>(expr->obj_meta_, expr->get_vector(ctx), ctx, bound, skip,    \
                                      cmp_op, iter)

#define FIND_BOUND_USE_FIXED_FMT(vec_tc)                                                           \
  case (vec_tc): {                                                                                 \
    ret = FIND_BOUND_USE_FMT(ObFixedLengthFormat<RTCType<vec_tc>>);                                \
  } break

template <typename OP, bool is_lower>
int VectorRangeUtil::common_bound(sql::ObExpr *expr, sql::ObEvalCtx &ctx,
                                 const sql::EvalBound &bound, const sql::ObBitVector &skip,
                                 OP cmp_op, int64_t &iter)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr->eval_vector(ctx, skip, bound))) {
    COMMON_LOG(WARN, "eval vector failed", K(ret));
  } else {
    iter = -1;
    VectorFormat fmt = expr->get_format(ctx);
    VecValueTypeClass vec_tc = expr->get_vec_value_tc();
    switch (fmt) {
    case VEC_DISCRETE: {
      FIND_BOUND_USE_FMT(ObDiscreteFormat);
      break;
    }
    case VEC_FIXED: {
      switch(vec_tc) {
        LST_DO_CODE(FIND_BOUND_USE_FIXED_FMT, FIXED_VEC_LIST);
        default: {
          ret = OB_ERR_UNEXPECTED;
          COMMON_LOG(WARN, "unexpected type class", K(vec_tc));
        }
      }
      break;
    }
    case VEC_CONTINUOUS: {
      FIND_BOUND_USE_FMT(ObContinuousFormat);
      break;
    }
    case VEC_UNIFORM: {
      FIND_BOUND_USE_FMT(ObUniformFormat<false>);
      break;
    }
    case VEC_UNIFORM_CONST: {
      FIND_BOUND_USE_FMT(ObUniformFormat<true>);
      break;
    }
    default: {
      ret = OB_ERR_UNDEFINED;
      COMMON_LOG(WARN, "unexpected data format", K(ret), K(fmt));
    }
    }
    if (OB_FAIL(ret)) {
      COMMON_LOG(WARN, "find bound failed", K(ret));
    }
  }
  return ret;
}

#undef FIND_BOUND_USE_FMT
#undef FIND_BOUND_USE_FIXED_FMT

template <typename OP, typename VecFmt, bool is_lower>
int VectorRangeUtil::find_bound(const ObObjMeta &obj_meta, ObIVector *ivector, sql::ObEvalCtx &ctx,
                                const sql::EvalBound &bound, const sql::ObBitVector &skip,
                                OP cmp_op, int64_t &iter)
{
  // TODO: use binary search
  int ret = OB_SUCCESS;
  int cmp_ret = 0;
  const char *payload = nullptr;
  int32_t len = 0;
  bool is_null = false;
  iter = -1;
  VecFmt *data = static_cast<VecFmt *>(ivector);
  if (std::is_same<VecFmt, ObUniformFormat<true>>::value) {
    bool skip_all = (skip.accumulate_bit_cnt(bound) == bound.range_size());
    if (skip_all) {
    } else {
      is_null = data->is_null(0);
      data->get_payload(0, payload, len);
      if(OB_FAIL(cmp_op(obj_meta, payload,len, is_null, cmp_ret))) {
        COMMON_LOG(WARN, "compare failed", K(ret));
      } else if (is_lower && cmp_ret >= 0) {
        iter = 0;
      } else if (!is_lower && cmp_ret > 0) {
        iter = 0;
      }
    }
  } else if (OB_LIKELY(bound.get_all_rows_active())) {
    for (int i = bound.start(); OB_SUCC(ret) && i < bound.end(); i++) {
      is_null = data->is_null(i);
      data->get_payload(i, payload, len);
      if (OB_FAIL(cmp_op(obj_meta, payload, len, is_null, cmp_ret))) {
        COMMON_LOG(WARN, "compare failed", K(ret));
      } else if (is_lower && cmp_ret >= 0) {
        iter = i;
        break;
      } else if (!is_lower && cmp_ret > 0) {
        iter = i;
        break;
      }
    }
  } else {
    for (int i = bound.start(); OB_SUCC(ret) && i < bound.end(); i++) {
      if (skip.at(i)) {
      } else {
        is_null = data->is_null(i);
        data->get_payload(i, payload, len);
        if (OB_FAIL(cmp_op(obj_meta, payload, len, is_null, cmp_ret))) {
          COMMON_LOG(WARN, "compare failed", K(ret));
        } else if (is_lower && cmp_ret >= 0) {
          iter = i;
          break;
        } else if (!is_lower && cmp_ret > 0) {
          iter = i;
          break;
        }
      }
    }
  }
  return ret;
}
} // end namespace common
} // end namespace oceanbase
#endif // OCEANBASE_SHARE_VECTOR_VECTOR_OP_UTIL_H_
