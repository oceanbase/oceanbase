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

} // end namespace common
} // end namespace oceanbase
#endif // OCEANBASE_SHARE_VECTOR_VECTOR_OP_UTIL_H_
