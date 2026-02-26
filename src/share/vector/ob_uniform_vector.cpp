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

#include "share/vector/ob_uniform_vector.ipp"
#include "sql/engine/expr/ob_array_expr_utils.h"

namespace oceanbase
{
namespace common
{
DEF_SET_COLLECTION_PAYLOAD(true);
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
template class ObUniformVector<true, VectorBasicOp<VEC_TC_COLLECTION>>;
template class ObUniformVector<true, VectorBasicOp<VEC_TC_MYSQL_DATETIME>>;
template class ObUniformVector<true, VectorBasicOp<VEC_TC_MYSQL_DATE>>;
template class ObUniformVector<true, VectorBasicOp<VEC_TC_ROARINGBITMAP>>;
} // end namespace common
} // end namespace oceanbase
