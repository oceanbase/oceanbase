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
#ifndef OCEANBASE_SHARE_VECTOR_OB_FIXED_LENGTH_VECTOR_H_
#define OCEANBASE_SHARE_VECTOR_OB_FIXED_LENGTH_VECTOR_H_

#include "share/vector/ob_fixed_length_format.h"
#include "share/vector/vector_op_util.h"

namespace oceanbase
{
namespace common
{

template<typename ValueType, typename BasicOp>
class ObFixedLengthVector final: public ObFixedLengthFormat<ValueType>
{
public:
  using VecTCBasicOp = BasicOp;
  using VectorType = ObFixedLengthVector<ValueType, VecTCBasicOp>;
  using VecOpUtil = VectorOpUtil<VectorType>;

  ObFixedLengthVector(char *data, sql::ObBitVector *nulls)
    : ObFixedLengthFormat<ValueType>(data, nulls)
  {}

  int default_hash(BATCH_EVAL_HASH_ARGS) const override;
  int murmur_hash(BATCH_EVAL_HASH_ARGS) const override;
  int murmur_hash_v3(BATCH_EVAL_HASH_ARGS) const override;
  int murmur_hash_v3_for_one_row(EVAL_HASH_ARGS_FOR_ROW) const override;
  int null_first_cmp(VECTOR_ONE_COMPARE_ARGS) const override;
  int null_last_cmp(VECTOR_ONE_COMPARE_ARGS) const override final;
  int no_null_cmp(VECTOR_NOT_NULL_COMPARE_ARGS) const override final;
  int null_first_cmp_batch_rows(VECTOR_COMPARE_BATCH_ROWS_ARGS) const override;
  int no_null_cmp_batch_rows(VECTOR_COMPARE_BATCH_ROWS_ARGS) const override;

  int null_first_mul_cmp(VECTOR_MUL_COMPARE_ARGS) const override final;
  int null_last_mul_cmp(VECTOR_MUL_COMPARE_ARGS) const override final;
  // TODO: add not null safe cmp function
};

extern template class ObFixedLengthVector<int64_t, VectorBasicOp<VEC_TC_INTEGER>>;
extern template class ObFixedLengthVector<uint64_t, VectorBasicOp<VEC_TC_UINTEGER>>;
extern template class ObFixedLengthVector<float, VectorBasicOp<VEC_TC_FLOAT>>;
extern template class ObFixedLengthVector<double, VectorBasicOp<VEC_TC_DOUBLE>>;
extern template class ObFixedLengthVector<double, VectorBasicOp<VEC_TC_FIXED_DOUBLE>>;
extern template class ObFixedLengthVector<int64_t, VectorBasicOp<VEC_TC_DATETIME>>;
extern template class ObFixedLengthVector<int32_t, VectorBasicOp<VEC_TC_DATE>>;
extern template class ObFixedLengthVector<int64_t, VectorBasicOp<VEC_TC_TIME>>;
extern template class ObFixedLengthVector<uint8_t, VectorBasicOp<VEC_TC_YEAR>>;
extern template class ObFixedLengthVector<int64_t, VectorBasicOp<VEC_TC_UNKNOWN>>;
extern template class ObFixedLengthVector<uint64_t, VectorBasicOp<VEC_TC_BIT>>;
extern template class ObFixedLengthVector<uint64_t, VectorBasicOp<VEC_TC_ENUM_SET>>;
extern template class ObFixedLengthVector<ObOTimestampData, VectorBasicOp<VEC_TC_TIMESTAMP_TZ>>;
extern template class ObFixedLengthVector<ObOTimestampTinyData, VectorBasicOp<VEC_TC_TIMESTAMP_TINY>>;
extern template class ObFixedLengthVector<int64_t, VectorBasicOp<VEC_TC_INTERVAL_YM>>;
extern template class ObFixedLengthVector<int64_t, VectorBasicOp<VEC_TC_INTERVAL_DS>>;
extern template class ObFixedLengthVector<int32_t, VectorBasicOp<VEC_TC_DEC_INT32>>;
extern template class ObFixedLengthVector<int64_t, VectorBasicOp<VEC_TC_DEC_INT64>>;
extern template class ObFixedLengthVector<int128_t, VectorBasicOp<VEC_TC_DEC_INT128>>;
extern template class ObFixedLengthVector<int256_t, VectorBasicOp<VEC_TC_DEC_INT256>>;
extern template class ObFixedLengthVector<int512_t, VectorBasicOp<VEC_TC_DEC_INT512>>;
extern template class ObFixedLengthVector<int64_t, VectorBasicOp<VEC_TC_MYSQL_DATETIME>>;
extern template class ObFixedLengthVector<int32_t, VectorBasicOp<VEC_TC_MYSQL_DATE>>;
} // end namespace common
} // end namespace oceanbase
#endif // OCEANBASE_SHARE_VECTOR_OB_FIXED_LENGTH_VECTOR_H_
