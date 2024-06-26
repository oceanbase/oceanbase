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
#include "lib/wide_integer/ob_wide_integer.h"

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
int ObFixedLengthVector<ValueType, BasicOp>::murmur_hash_v3(BATCH_EVAL_HASH_ARGS) const
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

} // end namespace common
} // end namespace oceanbase

#include "share/vector/ob_vector_define.h"
