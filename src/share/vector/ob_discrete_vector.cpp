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
#include "share/vector/ob_discrete_vector.h"

namespace oceanbase
{
namespace common
{

template<typename BasicOp>
int ObDiscreteVector<BasicOp>::default_hash(BATCH_EVAL_HASH_ARGS) const
{
  BatchHashResIter hash_iter(hash_values);
  return VecOpUtil::template hash_dispatch<ObDefaultHash, false, BatchHashResIter>(
    hash_iter, expr.obj_meta_, *this, skip, bound, seeds, is_batch_seed);
}

template<typename BasicOp>
int ObDiscreteVector<BasicOp>::murmur_hash(BATCH_EVAL_HASH_ARGS) const
{
  BatchHashResIter hash_iter(hash_values);
  return VecOpUtil::template hash_dispatch<ObMurmurHash, false, BatchHashResIter>(
    hash_iter, expr.obj_meta_, *this, skip, bound, seeds, is_batch_seed);
}

template<typename BasicOp>
int ObDiscreteVector<BasicOp>::murmur_hash_v3(BATCH_EVAL_HASH_ARGS) const
{
  BatchHashResIter hash_iter(hash_values);
  return VecOpUtil::template hash_dispatch<ObMurmurHash, true, BatchHashResIter>(
    hash_iter, expr.obj_meta_, *this, skip, bound, seeds, is_batch_seed);
}

template<typename BasicOp>
int ObDiscreteVector<BasicOp>::murmur_hash_v3_for_one_row(EVAL_HASH_ARGS_FOR_ROW) const
{
  RowHashResIter hash_iter(&hash_value);
  sql::EvalBound bound(batch_size, batch_idx, batch_idx + 1, true);
  char mock_skip_data[1] = {0};
  sql::ObBitVector &skip = *sql::to_bit_vector(mock_skip_data);
  return VecOpUtil::template hash_dispatch<ObMurmurHash, true, RowHashResIter>(
    hash_iter, expr.obj_meta_, *this, skip, bound, &seed, false);
}

template<typename BasicOp>
int ObDiscreteVector<BasicOp>::null_first_cmp(VECTOR_ONE_COMPARE_ARGS) const
{
  return VecOpUtil::template ns_cmp<true>(expr.obj_meta_, *this, row_idx, r_null, r_v, r_len, cmp_ret);
}

template<typename BasicOp>
int ObDiscreteVector<BasicOp>::null_last_cmp(VECTOR_ONE_COMPARE_ARGS) const
{
  return VecOpUtil::template ns_cmp<false>(expr.obj_meta_, *this, row_idx, r_null, r_v, r_len, cmp_ret);
}

template<typename BasicOp>
int ObDiscreteVector<BasicOp>::no_null_cmp(VECTOR_NOT_NULL_COMPARE_ARGS) const
{
  return VecOpUtil::Op::cmp(expr.obj_meta_, get_payload(row_idx1), get_length(row_idx1), get_payload(row_idx2), get_length(row_idx2), cmp_ret);
}

template class ObDiscreteVector<VectorBasicOp<VEC_TC_NUMBER>>;
template class ObDiscreteVector<VectorBasicOp<VEC_TC_EXTEND>>;
template class ObDiscreteVector<VectorBasicOp<VEC_TC_STRING>>;
template class ObDiscreteVector<VectorBasicOp<VEC_TC_ENUM_SET_INNER>>;
template class ObDiscreteVector<VectorBasicOp<VEC_TC_RAW>>;
template class ObDiscreteVector<VectorBasicOp<VEC_TC_ROWID>>;
template class ObDiscreteVector<VectorBasicOp<VEC_TC_LOB>>;
template class ObDiscreteVector<VectorBasicOp<VEC_TC_JSON>>;
template class ObDiscreteVector<VectorBasicOp<VEC_TC_GEO>>;
template class ObDiscreteVector<VectorBasicOp<VEC_TC_UDT>>;
template class ObDiscreteVector<VectorBasicOp<VEC_TC_ROARINGBITMAP>>;
} // end namespace common
} // end namespace oceanbase
