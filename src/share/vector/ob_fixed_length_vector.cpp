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

#define USING_LOG_PREFIX SHARE
#include "share/vector/ob_fixed_length_vector.h"

namespace oceanbase
{
namespace common
{

template<typename ValueType, typename BasicOp>
int ObFixedLengthVector<ValueType, BasicOp>::default_hash(BATCH_EVAL_HASH_ARGS) const
{
  BatchHashResIter hash_iter(hash_values);
  return VecOpUtil::template hash_dispatch<ObDefaultHash, false, BatchHashResIter>(
    hash_iter, expr.obj_meta_, *this, skip, bound, seeds, is_batch_seed);
}

template<typename ValueType, typename BasicOp>
int ObFixedLengthVector<ValueType, BasicOp>::murmur_hash(BATCH_EVAL_HASH_ARGS) const
{
  BatchHashResIter hash_iter(hash_values);
  return VecOpUtil::template hash_dispatch<ObMurmurHash, false, BatchHashResIter>(
    hash_iter, expr.obj_meta_, *this, skip, bound, seeds, is_batch_seed);
}

template<typename ValueType, typename BasicOp>
OB_INLINE int ObFixedLengthVector<ValueType, BasicOp>::murmur_hash_v3(BATCH_EVAL_HASH_ARGS) const
{
  BatchHashResIter hash_iter(hash_values);
  return VecOpUtil::template hash_dispatch<ObMurmurHash, true, BatchHashResIter>(
    hash_iter, expr.obj_meta_, *this, skip, bound, seeds, is_batch_seed);
}

template<typename ValueType, typename BasicOp>
int ObFixedLengthVector<ValueType, BasicOp>::murmur_hash_v3_for_one_row(EVAL_HASH_ARGS_FOR_ROW) const
{
  RowHashResIter hash_iter(&hash_value);
  sql::EvalBound bound(batch_size, batch_idx, batch_idx + 1, true);
  char mock_skip_data[1] = {0};
  sql::ObBitVector &skip = *sql::to_bit_vector(mock_skip_data);
  return VecOpUtil::template hash_dispatch<ObMurmurHash, true, RowHashResIter>(
    hash_iter, expr.obj_meta_, *this, skip, bound, &seed, false);
}

template<typename ValueType, typename BasicOp>
int ObFixedLengthVector<ValueType, BasicOp>::null_first_cmp(VECTOR_ONE_COMPARE_ARGS) const
{
  return VecOpUtil::template ns_cmp<true>(expr.obj_meta_, *this, row_idx, r_null, r_v, r_len, cmp_ret);
}

template<typename ValueType, typename BasicOp>
int ObFixedLengthVector<ValueType, BasicOp>::null_last_cmp(VECTOR_ONE_COMPARE_ARGS) const
{
  return VecOpUtil::template ns_cmp<false>(expr.obj_meta_, *this, row_idx, r_null, r_v, r_len, cmp_ret);
}

template<typename ValueType, typename BasicOp>
int ObFixedLengthVector<ValueType, BasicOp>::no_null_cmp(VECTOR_NOT_NULL_COMPARE_ARGS) const
{
  return VecOpUtil::Op::cmp(expr.obj_meta_, this->get_payload(row_idx1), this->get_length(row_idx1), this->get_payload(row_idx2), this->get_length(row_idx2), cmp_ret);
}

template<typename ValueType, typename BasicOp>
int ObFixedLengthVector<ValueType, BasicOp>::null_first_mul_cmp(VECTOR_MUL_COMPARE_ARGS) const
{
  int ret = OB_SUCCESS;
  cmp_ret = 0;
  uint16_t start_idx = bound.start();
  uint16_t end_idx = bound.end();
  for (int64_t row_idx = start_idx; OB_SUCC(ret) && 0 == cmp_ret && row_idx < end_idx; row_idx++) {
    if (skip.at(row_idx)) {
      continue;
    } else if (OB_FAIL(null_first_cmp(expr, row_idx, r_null, r_v, r_len, cmp_ret))) {
      LOG_WARN("failed to compare", K(ret));
    } else if (0 != cmp_ret) {
      diff_row_idx = row_idx;
      break;
    }
  }
  if (0 == cmp_ret) {
    diff_row_idx = end_idx;
  }
  return ret;
}

template<typename ValueType, typename BasicOp>
int ObFixedLengthVector<ValueType, BasicOp>::null_last_mul_cmp(VECTOR_MUL_COMPARE_ARGS) const
{
  int ret = OB_SUCCESS;
  cmp_ret = 0;
  uint16_t start_idx = bound.start();
  uint16_t end_idx = bound.end();
  for (int64_t row_idx = start_idx; OB_SUCC(ret) && 0 == cmp_ret && row_idx < end_idx; row_idx++) {
    if (skip.at(row_idx)) {
      continue;
    } else if (OB_FAIL(null_last_cmp(expr, row_idx, r_null, r_v, r_len, cmp_ret))) {
      LOG_WARN("failed to compare", K(ret));
    } else if (0 != cmp_ret) {
      diff_row_idx = row_idx;
      break;
    }
  }
  if (0 == cmp_ret) {
    diff_row_idx = end_idx;
  }
  return ret;
}

template <typename ValueType, typename BasicOp>
int ObFixedLengthVector<ValueType, BasicOp>::null_first_cmp_batch_rows(
    VECTOR_COMPARE_BATCH_ROWS_ARGS) const
{
  int ret = OB_SUCCESS;
  ObLength r_len = 0;
  const char *r_v = NULL;
  int32_t fixed_offset = 0;
  const bool is_fixed_length = row_meta.is_reordered_fixed_expr(row_col_idx);
  if (is_fixed_length) {
    fixed_offset = row_meta.get_fixed_cell_offset(row_col_idx);
    r_len = row_meta.fixed_length(row_col_idx);
    for (int64_t i = 0; OB_SUCC(ret) && i < sel_cnt; i++) {
      uint16_t batch_idx = sel[i];
      r_v = rows[i]->payload() + fixed_offset;
      if (OB_FAIL(null_first_cmp(
              expr, batch_idx, rows[i]->is_null(row_col_idx), r_v, r_len, cmp_ret[i]))) {
        LOG_WARN("failed to compare", K(ret));
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < sel_cnt; i++) {
      uint16_t batch_idx = sel[i];
      rows[i]->get_cell_payload(row_meta, row_col_idx, r_v, r_len);
      if (OB_FAIL(null_first_cmp(
              expr, batch_idx, rows[i]->is_null(row_col_idx), r_v, r_len, cmp_ret[i]))) {
        LOG_WARN("failed to compare", K(ret));
      }
    }
  }
  return ret;
}

template <typename ValueType, typename BasicOp>
int ObFixedLengthVector<ValueType, BasicOp>::no_null_cmp_batch_rows(
    VECTOR_COMPARE_BATCH_ROWS_ARGS) const
{
  int ret = OB_SUCCESS;
  ObLength r_len = 0;
  const char *r_v = NULL;
  int32_t fixed_offset = 0;
  const bool is_fixed_length = row_meta.is_reordered_fixed_expr(row_col_idx);
  if (is_fixed_length) {
    fixed_offset = row_meta.get_fixed_cell_offset(row_col_idx);
    r_len = row_meta.fixed_length(row_col_idx);
    for (int64_t i = 0; OB_SUCC(ret) && i < sel_cnt; i++) {
      uint16_t batch_idx = sel[i];
      r_v = rows[i]->payload() + fixed_offset;
      const ValueType *l_value =
          reinterpret_cast<const ValueType *>(this->get_data() + r_len * batch_idx);
      const ValueType *r_value = reinterpret_cast<const ValueType *>(r_v);
      if (*l_value == *r_value) {
        cmp_ret[i] = 0;
      } else if (*l_value < *r_value) {
        cmp_ret[i] = -1;
      } else {
        cmp_ret[i] = 1;
      }
    }
  } else {
    for (int64_t i = 0; i < sel_cnt; i++) {
      uint16_t batch_idx = sel[i];
      rows[i]->get_cell_payload(row_meta, row_col_idx, r_v, r_len);
      const ValueType *l_value =
          reinterpret_cast<const ValueType *>(this->get_data() + r_len * batch_idx);
      const ValueType *r_value = reinterpret_cast<const ValueType *>(r_v);
      if (*l_value == *r_value) {
        cmp_ret[i] = 0;
      } else if (*l_value < *r_value) {
        cmp_ret[i] = -1;
      } else {
        cmp_ret[i] = 1;
      }
    }
  }
  return ret;
}

template class ObFixedLengthVector<int64_t, VectorBasicOp<VEC_TC_INTEGER>>;
template class ObFixedLengthVector<uint64_t, VectorBasicOp<VEC_TC_UINTEGER>>;
template class ObFixedLengthVector<float, VectorBasicOp<VEC_TC_FLOAT>>;
template class ObFixedLengthVector<double, VectorBasicOp<VEC_TC_DOUBLE>>;
template class ObFixedLengthVector<double, VectorBasicOp<VEC_TC_FIXED_DOUBLE>>;
template class ObFixedLengthVector<int64_t, VectorBasicOp<VEC_TC_DATETIME>>;
template class ObFixedLengthVector<int32_t, VectorBasicOp<VEC_TC_DATE>>;
template class ObFixedLengthVector<int64_t, VectorBasicOp<VEC_TC_TIME>>;
template class ObFixedLengthVector<uint8_t, VectorBasicOp<VEC_TC_YEAR>>;
template class ObFixedLengthVector<int64_t, VectorBasicOp<VEC_TC_UNKNOWN>>;
template class ObFixedLengthVector<uint64_t, VectorBasicOp<VEC_TC_BIT>>;
template class ObFixedLengthVector<uint64_t, VectorBasicOp<VEC_TC_ENUM_SET>>;
template class ObFixedLengthVector<ObOTimestampData, VectorBasicOp<VEC_TC_TIMESTAMP_TZ>>;
template class ObFixedLengthVector<ObOTimestampTinyData, VectorBasicOp<VEC_TC_TIMESTAMP_TINY>>;
template class ObFixedLengthVector<int64_t, VectorBasicOp<VEC_TC_INTERVAL_YM>>;
template class ObFixedLengthVector<ObIntervalDSValue, VectorBasicOp<VEC_TC_INTERVAL_DS>>;
template class ObFixedLengthVector<int32_t, VectorBasicOp<VEC_TC_DEC_INT32>>;
template class ObFixedLengthVector<int64_t, VectorBasicOp<VEC_TC_DEC_INT64>>;
template class ObFixedLengthVector<int128_t, VectorBasicOp<VEC_TC_DEC_INT128>>;
template class ObFixedLengthVector<int256_t, VectorBasicOp<VEC_TC_DEC_INT256>>;
template class ObFixedLengthVector<int512_t, VectorBasicOp<VEC_TC_DEC_INT512>>;
template class ObFixedLengthVector<int64_t, VectorBasicOp<VEC_TC_MYSQL_DATETIME>>;
template class ObFixedLengthVector<int32_t, VectorBasicOp<VEC_TC_MYSQL_DATE>>;


} // end namespace common
} // end namespace oceanbase

#include "share/vector/ob_vector_define.h"