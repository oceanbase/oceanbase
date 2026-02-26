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

#ifndef OCEANBASE_SHARE_VECTOR_OB_UNIFORM_VECTOR_H_
#define OCEANBASE_SHARE_VECTOR_OB_UNIFORM_VECTOR_H_

#include "share/vector/ob_uniform_format.h"
#include "share/vector/vector_op_util.h"

namespace oceanbase
{
namespace sql
{
  struct ObEvalInfo;
}
namespace common
{

template<bool IS_CONST, typename BasicOp>
class ObUniformVector final: public ObUniformFormat<IS_CONST>
{
public:
  using VecTCBasicOp = BasicOp;
  using VectorType = ObUniformVector<IS_CONST, VecTCBasicOp>;
  using VecOpUtil = VectorOpUtil<VectorType>;

  ObUniformVector(ObDatum *datums, sql::ObEvalInfo *eval_info)
    : ObUniformFormat<IS_CONST>(datums, eval_info)
  {}

  int default_hash(BATCH_EVAL_HASH_ARGS) const override;
  int murmur_hash(BATCH_EVAL_HASH_ARGS) const override;
  int murmur_hash_v3(BATCH_EVAL_HASH_ARGS) const override;
  int murmur_hash_v3_for_one_row(EVAL_HASH_ARGS_FOR_ROW) const override;
  int null_first_cmp(VECTOR_ONE_COMPARE_ARGS) const override;
  int null_last_cmp(VECTOR_ONE_COMPARE_ARGS) const override;
  int no_null_cmp(VECTOR_NOT_NULL_COMPARE_ARGS) const override;
  int null_first_mul_cmp(VECTOR_MUL_COMPARE_ARGS) const override final;
  int null_last_mul_cmp(VECTOR_MUL_COMPARE_ARGS) const override final;
  int null_first_cmp_batch_rows(VECTOR_COMPARE_BATCH_ROWS_ARGS) const override;
  int no_null_cmp_batch_rows(VECTOR_COMPARE_BATCH_ROWS_ARGS) const override;
};

extern template class ObUniformVector<true, VectorBasicOp<VEC_TC_NULL>>;
extern template class ObUniformVector<true, VectorBasicOp<VEC_TC_INTEGER>>;
extern template class ObUniformVector<true, VectorBasicOp<VEC_TC_UINTEGER>>;
extern template class ObUniformVector<true, VectorBasicOp<VEC_TC_FLOAT>>;
extern template class ObUniformVector<true, VectorBasicOp<VEC_TC_DOUBLE>>;
extern template class ObUniformVector<true, VectorBasicOp<VEC_TC_FIXED_DOUBLE>>;
extern template class ObUniformVector<true, VectorBasicOp<VEC_TC_NUMBER>>;
extern template class ObUniformVector<true, VectorBasicOp<VEC_TC_DATETIME>>;
extern template class ObUniformVector<true, VectorBasicOp<VEC_TC_DATE>>;
extern template class ObUniformVector<true, VectorBasicOp<VEC_TC_TIME>>;
extern template class ObUniformVector<true, VectorBasicOp<VEC_TC_YEAR>>;
extern template class ObUniformVector<true, VectorBasicOp<VEC_TC_EXTEND>>;
extern template class ObUniformVector<true, VectorBasicOp<VEC_TC_UNKNOWN>>;
extern template class ObUniformVector<true, VectorBasicOp<VEC_TC_STRING>>;
extern template class ObUniformVector<true, VectorBasicOp<VEC_TC_BIT>>;
extern template class ObUniformVector<true, VectorBasicOp<VEC_TC_ENUM_SET>>;
extern template class ObUniformVector<true, VectorBasicOp<VEC_TC_ENUM_SET_INNER>>;
extern template class ObUniformVector<true, VectorBasicOp<VEC_TC_TIMESTAMP_TZ>>;
extern template class ObUniformVector<true, VectorBasicOp<VEC_TC_TIMESTAMP_TINY>>;
extern template class ObUniformVector<true, VectorBasicOp<VEC_TC_RAW>>;
extern template class ObUniformVector<true, VectorBasicOp<VEC_TC_INTERVAL_YM>>;
extern template class ObUniformVector<true, VectorBasicOp<VEC_TC_INTERVAL_DS>>;
extern template class ObUniformVector<true, VectorBasicOp<VEC_TC_ROWID>>;
extern template class ObUniformVector<true, VectorBasicOp<VEC_TC_LOB>>;
extern template class ObUniformVector<true, VectorBasicOp<VEC_TC_JSON>>;
extern template class ObUniformVector<true, VectorBasicOp<VEC_TC_GEO>>;
extern template class ObUniformVector<true, VectorBasicOp<VEC_TC_UDT>>;
extern template class ObUniformVector<true, VectorBasicOp<VEC_TC_DEC_INT32>>;
extern template class ObUniformVector<true, VectorBasicOp<VEC_TC_DEC_INT64>>;
extern template class ObUniformVector<true, VectorBasicOp<VEC_TC_DEC_INT128>>;
extern template class ObUniformVector<true, VectorBasicOp<VEC_TC_DEC_INT256>>;
extern template class ObUniformVector<true, VectorBasicOp<VEC_TC_DEC_INT512>>;
extern template class ObUniformVector<true, VectorBasicOp<VEC_TC_COLLECTION>>;
extern template class ObUniformVector<true, VectorBasicOp<VEC_TC_MYSQL_DATETIME>>;
extern template class ObUniformVector<true, VectorBasicOp<VEC_TC_MYSQL_DATE>>;
extern template class ObUniformVector<true, VectorBasicOp<VEC_TC_ROARINGBITMAP>>;

} // end namespace common
} // end namespace oceanbase
#endif // OCEANBASE_SHARE_VECTOR_OB_UNIFORM_VECTOR_H_
