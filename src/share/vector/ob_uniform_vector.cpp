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

#include "share/vector/ob_uniform_vector.h"
#include "sql/engine/expr/ob_expr.h"

namespace oceanbase
{
namespace common
{

template<bool IS_CONST, typename BasicOp>
int ObUniformVector<IS_CONST, BasicOp>::default_hash(BATCH_EVAL_HASH_ARGS) const
{
  BatchHashResIter hash_iter(hash_values);
  return VecOpUtil::template hash_dispatch<ObDefaultHash, false, BatchHashResIter>(
    hash_iter, expr.obj_meta_, *this, skip, bound, seeds, is_batch_seed);
}

template<bool IS_CONST, typename BasicOp>
int ObUniformVector<IS_CONST, BasicOp>::murmur_hash(BATCH_EVAL_HASH_ARGS) const
{
  BatchHashResIter hash_iter(hash_values);
  return VecOpUtil::template hash_dispatch<ObMurmurHash, false, BatchHashResIter>(
    hash_iter, expr.obj_meta_, *this, skip, bound, seeds, is_batch_seed);
}

template<bool IS_CONST, typename BasicOp>
int ObUniformVector<IS_CONST, BasicOp>::murmur_hash_v3(BATCH_EVAL_HASH_ARGS) const
{
  BatchHashResIter hash_iter(hash_values);
  return VecOpUtil::template hash_dispatch<ObMurmurHash, true, BatchHashResIter>(
    hash_iter, expr.obj_meta_, *this, skip, bound, seeds, is_batch_seed);
}

template<bool IS_CONST, typename BasicOp>
int ObUniformVector<IS_CONST, BasicOp>::murmur_hash_v3_for_one_row(EVAL_HASH_ARGS_FOR_ROW) const
{

  RowHashResIter hash_iter(&hash_value);
  sql::EvalBound bound(batch_size, batch_idx, batch_idx + 1, true);
  char mock_skip_data[1] = {0};
  sql::ObBitVector &skip = *sql::to_bit_vector(mock_skip_data);
  return VecOpUtil::template hash_dispatch<ObMurmurHash, true, RowHashResIter>(
    hash_iter, expr.obj_meta_, *this, skip, bound, &seed, false);
}

template<bool IS_CONST, typename BasicOp>
int ObUniformVector<IS_CONST, BasicOp>::null_first_cmp(VECTOR_ONE_COMPARE_ARGS) const
{
  return expr.basic_funcs_->null_first_cmp_(this->get_datum(row_idx), ObDatum(r_v, r_len, r_null), cmp_ret);
}

template<bool IS_CONST, typename BasicOp>
int ObUniformVector<IS_CONST, BasicOp>::null_last_cmp(VECTOR_ONE_COMPARE_ARGS) const
{
  return expr.basic_funcs_->null_last_cmp_(this->get_datum(row_idx), ObDatum(r_v, r_len, r_null), cmp_ret);
}

template<bool IS_CONST, typename BasicOp>
int ObUniformVector<IS_CONST, BasicOp>::no_null_cmp(VECTOR_NOT_NULL_COMPARE_ARGS) const
{
  return expr.basic_funcs_->null_last_cmp_(this->get_datum(row_idx1), this->get_datum(row_idx2), cmp_ret);
}

template class ObUniformVector<true, VectorBasicOp<VEC_TC_NULL>>;
template class ObUniformVector<true, VectorBasicOp<VEC_TC_INTEGER>>;
template class ObUniformVector<true, VectorBasicOp<VEC_TC_UINTEGER>>;
template class ObUniformVector<true, VectorBasicOp<VEC_TC_FLOAT>>;
template class ObUniformVector<true, VectorBasicOp<VEC_TC_DOUBLE>>;
template class ObUniformVector<true, VectorBasicOp<VEC_TC_FIXED_DOUBLE>>;
template class ObUniformVector<true, VectorBasicOp<VEC_TC_NUMBER>>;
template class ObUniformVector<true, VectorBasicOp<VEC_TC_DATETIME>>;
template class ObUniformVector<true, VectorBasicOp<VEC_TC_DATE>>;
template class ObUniformVector<true, VectorBasicOp<VEC_TC_TIME>>;
template class ObUniformVector<true, VectorBasicOp<VEC_TC_YEAR>>;
template class ObUniformVector<true, VectorBasicOp<VEC_TC_EXTEND>>;
template class ObUniformVector<true, VectorBasicOp<VEC_TC_UNKNOWN>>;
template class ObUniformVector<true, VectorBasicOp<VEC_TC_STRING>>;
template class ObUniformVector<true, VectorBasicOp<VEC_TC_BIT>>;
template class ObUniformVector<true, VectorBasicOp<VEC_TC_ENUM_SET>>;
template class ObUniformVector<true, VectorBasicOp<VEC_TC_ENUM_SET_INNER>>;
template class ObUniformVector<true, VectorBasicOp<VEC_TC_TIMESTAMP_TZ>>;
template class ObUniformVector<true, VectorBasicOp<VEC_TC_TIMESTAMP_TINY>>;
template class ObUniformVector<true, VectorBasicOp<VEC_TC_RAW>>;
template class ObUniformVector<true, VectorBasicOp<VEC_TC_INTERVAL_YM>>;
template class ObUniformVector<true, VectorBasicOp<VEC_TC_INTERVAL_DS>>;
template class ObUniformVector<true, VectorBasicOp<VEC_TC_ROWID>>;
template class ObUniformVector<true, VectorBasicOp<VEC_TC_LOB>>;
template class ObUniformVector<true, VectorBasicOp<VEC_TC_JSON>>;
template class ObUniformVector<true, VectorBasicOp<VEC_TC_GEO>>;
template class ObUniformVector<true, VectorBasicOp<VEC_TC_UDT>>;
template class ObUniformVector<true, VectorBasicOp<VEC_TC_DEC_INT32>>;
template class ObUniformVector<true, VectorBasicOp<VEC_TC_DEC_INT64>>;
template class ObUniformVector<true, VectorBasicOp<VEC_TC_DEC_INT128>>;
template class ObUniformVector<true, VectorBasicOp<VEC_TC_DEC_INT256>>;
template class ObUniformVector<true, VectorBasicOp<VEC_TC_DEC_INT512>>;
template class ObUniformVector<true, VectorBasicOp<VEC_TC_ROARINGBITMAP>>;

template class ObUniformVector<false, VectorBasicOp<VEC_TC_NULL>>;
template class ObUniformVector<false, VectorBasicOp<VEC_TC_INTEGER>>;
template class ObUniformVector<false, VectorBasicOp<VEC_TC_UINTEGER>>;
template class ObUniformVector<false, VectorBasicOp<VEC_TC_FLOAT>>;
template class ObUniformVector<false, VectorBasicOp<VEC_TC_DOUBLE>>;
template class ObUniformVector<false, VectorBasicOp<VEC_TC_FIXED_DOUBLE>>;
template class ObUniformVector<false, VectorBasicOp<VEC_TC_NUMBER>>;
template class ObUniformVector<false, VectorBasicOp<VEC_TC_DATETIME>>;
template class ObUniformVector<false, VectorBasicOp<VEC_TC_DATE>>;
template class ObUniformVector<false, VectorBasicOp<VEC_TC_TIME>>;
template class ObUniformVector<false, VectorBasicOp<VEC_TC_YEAR>>;
template class ObUniformVector<false, VectorBasicOp<VEC_TC_EXTEND>>;
template class ObUniformVector<false, VectorBasicOp<VEC_TC_UNKNOWN>>;
template class ObUniformVector<false, VectorBasicOp<VEC_TC_STRING>>;
template class ObUniformVector<false, VectorBasicOp<VEC_TC_BIT>>;
template class ObUniformVector<false, VectorBasicOp<VEC_TC_ENUM_SET>>;
template class ObUniformVector<false, VectorBasicOp<VEC_TC_ENUM_SET_INNER>>;
template class ObUniformVector<false, VectorBasicOp<VEC_TC_TIMESTAMP_TZ>>;
template class ObUniformVector<false, VectorBasicOp<VEC_TC_TIMESTAMP_TINY>>;
template class ObUniformVector<false, VectorBasicOp<VEC_TC_RAW>>;
template class ObUniformVector<false, VectorBasicOp<VEC_TC_INTERVAL_YM>>;
template class ObUniformVector<false, VectorBasicOp<VEC_TC_INTERVAL_DS>>;
template class ObUniformVector<false, VectorBasicOp<VEC_TC_ROWID>>;
template class ObUniformVector<false, VectorBasicOp<VEC_TC_LOB>>;
template class ObUniformVector<false, VectorBasicOp<VEC_TC_JSON>>;
template class ObUniformVector<false, VectorBasicOp<VEC_TC_GEO>>;
template class ObUniformVector<false, VectorBasicOp<VEC_TC_UDT>>;
template class ObUniformVector<false, VectorBasicOp<VEC_TC_DEC_INT32>>;
template class ObUniformVector<false, VectorBasicOp<VEC_TC_DEC_INT64>>;
template class ObUniformVector<false, VectorBasicOp<VEC_TC_DEC_INT128>>;
template class ObUniformVector<false, VectorBasicOp<VEC_TC_DEC_INT256>>;
template class ObUniformVector<false, VectorBasicOp<VEC_TC_DEC_INT512>>;
template class ObUniformVector<false, VectorBasicOp<VEC_TC_ROARINGBITMAP>>;
} // end namespace common
} // end namespace oceanbase
