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

#include "vector_basic_op.h"
#include "sql/engine/expr/ob_array_expr_utils.h"
namespace oceanbase
{
namespace common
{
HashFuncTypeForTc get_hashfunc_by_tc(VecValueTypeClass tc) 
{
  HashFuncTypeForTc res_func;
  switch (tc) {
  case (VEC_TC_NULL) : {
    res_func = VecTCHashCalc<VEC_TC_NULL, ObMurmurHash, true>::hash;
    break;
  }
  case (VEC_TC_INTEGER) : {
    res_func = VecTCHashCalc<VEC_TC_INTEGER, ObMurmurHash, true>::hash;
    break;
  }
  case (VEC_TC_UINTEGER) : {
    res_func = VecTCHashCalc<VEC_TC_UINTEGER, ObMurmurHash, true>::hash;
    break;
  }
  case (VEC_TC_FLOAT) : {
    res_func = VecTCHashCalc<VEC_TC_FLOAT, ObMurmurHash, true>::hash;
    break;
  }
  case (VEC_TC_DOUBLE) : {
    res_func = VecTCHashCalc<VEC_TC_DOUBLE, ObMurmurHash, true>::hash;
    break;
  }
  case (VEC_TC_FIXED_DOUBLE) : {
    res_func = VecTCHashCalc<VEC_TC_FIXED_DOUBLE, ObMurmurHash, true>::hash;
    break;
  }
  case (VEC_TC_NUMBER) : {
    res_func = VecTCHashCalc<VEC_TC_NUMBER, ObMurmurHash, true>::hash;
    break;
  }
  case (VEC_TC_DATETIME) : {
    res_func = VecTCHashCalc<VEC_TC_DATETIME, ObMurmurHash, true>::hash;
    break;
  }
  case (VEC_TC_DATE) : {
    res_func = VecTCHashCalc<VEC_TC_DATE, ObMurmurHash, true>::hash;
    break;
  }
  case (VEC_TC_TIME) : {
    res_func = VecTCHashCalc<VEC_TC_TIME, ObMurmurHash, true>::hash;
    break;
  }
  case (VEC_TC_YEAR) : {
    res_func = VecTCHashCalc<VEC_TC_YEAR, ObMurmurHash, true>::hash;
    break;
  }
  case (VEC_TC_EXTEND) : {
    res_func = VecTCHashCalc<VEC_TC_EXTEND, ObMurmurHash, true>::hash;
    break;
  }
  case (VEC_TC_UNKNOWN) : {
    res_func = VecTCHashCalc<VEC_TC_UNKNOWN, ObMurmurHash, true>::hash;
    break;
  }
  case (VEC_TC_STRING) : {
    res_func = VecTCHashCalc<VEC_TC_STRING, ObMurmurHash, true>::hash;
    break;
  }
  case (VEC_TC_BIT) : {
    res_func = VecTCHashCalc<VEC_TC_BIT, ObMurmurHash, true>::hash;
    break;
  }
  case (VEC_TC_ENUM_SET) : {
    res_func = VecTCHashCalc<VEC_TC_ENUM_SET, ObMurmurHash, true>::hash;
    break;
  }
  case (VEC_TC_ENUM_SET_INNER) : {
    res_func = VecTCHashCalc<VEC_TC_ENUM_SET_INNER, ObMurmurHash, true>::hash;
    break;
  }
  case (VEC_TC_TIMESTAMP_TZ) : {  // ObTimestampTZTyp)
    res_func = VecTCHashCalc<VEC_TC_TIMESTAMP_TZ, ObMurmurHash, true>::hash;
    break;
  }
  case (VEC_TC_TIMESTAMP_TINY) : {
    res_func = VecTCHashCalc<VEC_TC_TIMESTAMP_TINY, ObMurmurHash, true>::hash;
    break;
  }
  case (VEC_TC_RAW) : {
    res_func = VecTCHashCalc<VEC_TC_RAW, ObMurmurHash, true>::hash;
    break;
  }
  case (VEC_TC_INTERVAL_YM) : {
    res_func = VecTCHashCalc<VEC_TC_INTERVAL_YM, ObMurmurHash, true>::hash;
    break;
  }
  case (VEC_TC_INTERVAL_DS) : {
    res_func = VecTCHashCalc<VEC_TC_INTERVAL_DS, ObMurmurHash, true>::hash;
    break;
  }
  case (VEC_TC_ROWID) : {
    res_func = VecTCHashCalc<VEC_TC_ROWID, ObMurmurHash, true>::hash;
    break;
  }
  case (VEC_TC_LOB) : {
    res_func = VecTCHashCalc<VEC_TC_LOB, ObMurmurHash, true>::hash;
    break;
  }
  case (VEC_TC_JSON) : {
    res_func = VecTCHashCalc<VEC_TC_JSON, ObMurmurHash, true>::hash;
    break;
  }
  case (VEC_TC_GEO) : {
    res_func = VecTCHashCalc<VEC_TC_GEO, ObMurmurHash, true>::hash;
    break;
  }
  case (VEC_TC_UDT) : {
    res_func = VecTCHashCalc<VEC_TC_UDT, ObMurmurHash, true>::hash;
    break;
  }
  case (VEC_TC_DEC_INT32) : {
    res_func = VecTCHashCalc<VEC_TC_DEC_INT32, ObMurmurHash, true>::hash;
    break;
  }
  case (VEC_TC_DEC_INT64) : {
    res_func = VecTCHashCalc<VEC_TC_DEC_INT64, ObMurmurHash, true>::hash;
    break;
  }
  case (VEC_TC_DEC_INT128) : {
    res_func = VecTCHashCalc<VEC_TC_DEC_INT128, ObMurmurHash, true>::hash;
    break;
  }
  case (VEC_TC_DEC_INT256) : {
    res_func = VecTCHashCalc<VEC_TC_DEC_INT256, ObMurmurHash, true>::hash;
    break;
  }
  case (VEC_TC_DEC_INT512) : {
    res_func = VecTCHashCalc<VEC_TC_DEC_INT512, ObMurmurHash, true>::hash;
    break;
  }
  case (VEC_TC_COLLECTION) : {
    res_func = VecTCHashCalc<VEC_TC_COLLECTION, ObMurmurHash, true>::hash;
    break;
  }
  case (VEC_TC_MYSQL_DATETIME) : {
    res_func = VecTCHashCalc<VEC_TC_MYSQL_DATETIME, ObMurmurHash, true>::hash;
    break;
  }
  case (VEC_TC_MYSQL_DATE) : {
    res_func = VecTCHashCalc<VEC_TC_MYSQL_DATE, ObMurmurHash, true>::hash;
    break;
  }
  case (VEC_TC_ROARINGBITMAP) : {
    res_func = VecTCHashCalc<VEC_TC_ROARINGBITMAP, ObMurmurHash, true>::hash;
    break;
  }
  case (MAX_VEC_TC) : {
    res_func = VecTCHashCalc<MAX_VEC_TC, ObMurmurHash, true>::hash;
    break;
  }
  }
  return res_func;
}

NullHashFuncTypeForTc get_null_hashfunc_by_tc(VecValueTypeClass tc) 
{
  NullHashFuncTypeForTc res_func;
  switch (tc) {
  case (VEC_TC_NULL) : {
    res_func = VectorBasicOp<VEC_TC_NULL>::null_hash<ObMurmurHash, true>;
    break;
  }
  case (VEC_TC_INTEGER) : {
    res_func = VectorBasicOp<VEC_TC_INTEGER>::null_hash<ObMurmurHash, true>;
    break;
  }
  case (VEC_TC_UINTEGER) : {
    res_func = VectorBasicOp<VEC_TC_UINTEGER>::null_hash<ObMurmurHash, true>;
    break;
  }
  case (VEC_TC_FLOAT) : {
    res_func = VectorBasicOp<VEC_TC_FLOAT>::null_hash<ObMurmurHash, true>;
    break;
  }
  case (VEC_TC_DOUBLE) : {
    res_func = VectorBasicOp<VEC_TC_DOUBLE>::null_hash<ObMurmurHash, true>;
    break;
  }
  case (VEC_TC_FIXED_DOUBLE) : {
    res_func = VectorBasicOp<VEC_TC_FIXED_DOUBLE>::null_hash<ObMurmurHash, true>;
    break;
  }
  case (VEC_TC_NUMBER) : {
    res_func = VectorBasicOp<VEC_TC_NUMBER>::null_hash<ObMurmurHash, true>;
    break;
  }
  case (VEC_TC_DATETIME) : {
    res_func = VectorBasicOp<VEC_TC_DATETIME>::null_hash<ObMurmurHash, true>;
    break;
  }
  case (VEC_TC_DATE) : {
    res_func = VectorBasicOp<VEC_TC_DATE>::null_hash<ObMurmurHash, true>;
    break;
  }
  case (VEC_TC_TIME) : {
    res_func = VectorBasicOp<VEC_TC_TIME>::null_hash<ObMurmurHash, true>;
    break;
  }
  case (VEC_TC_YEAR) : {
    res_func = VectorBasicOp<VEC_TC_YEAR>::null_hash<ObMurmurHash, true>;
    break;
  }
  case (VEC_TC_EXTEND) : {
    res_func = VectorBasicOp<VEC_TC_EXTEND>::null_hash<ObMurmurHash, true>;
    break;
  }
  case (VEC_TC_UNKNOWN) : {
    res_func = VectorBasicOp<VEC_TC_UNKNOWN>::null_hash<ObMurmurHash, true>;
    break;
  }
  case (VEC_TC_STRING) : {
    res_func = VectorBasicOp<VEC_TC_STRING>::null_hash<ObMurmurHash, true>;
    break;
  }
  case (VEC_TC_BIT) : {
    res_func = VectorBasicOp<VEC_TC_BIT>::null_hash<ObMurmurHash, true>;
    break;
  }
  case (VEC_TC_ENUM_SET) : {
    res_func = VectorBasicOp<VEC_TC_ENUM_SET>::null_hash<ObMurmurHash, true>;
    break;
  }
  case (VEC_TC_ENUM_SET_INNER) : {
    res_func = VectorBasicOp<VEC_TC_ENUM_SET_INNER>::null_hash<ObMurmurHash, true>;
    break;
  }
  case (VEC_TC_TIMESTAMP_TZ) : {  // ObTimestampTZTyp)
    res_func = VectorBasicOp<VEC_TC_TIMESTAMP_TZ>::null_hash<ObMurmurHash, true>;
    break;
  }
  case (VEC_TC_TIMESTAMP_TINY) : {
    res_func = VectorBasicOp<VEC_TC_TIMESTAMP_TINY>::null_hash<ObMurmurHash, true>;
    break;
  }
  case (VEC_TC_RAW) : {
    res_func = VectorBasicOp<VEC_TC_RAW>::null_hash<ObMurmurHash, true>;
    break;
  }
  case (VEC_TC_INTERVAL_YM) : {
    res_func = VectorBasicOp<VEC_TC_INTERVAL_YM>::null_hash<ObMurmurHash, true>;
    break;
  }
  case (VEC_TC_INTERVAL_DS) : {
    res_func = VectorBasicOp<VEC_TC_INTERVAL_DS>::null_hash<ObMurmurHash, true>;
    break;
  }
  case (VEC_TC_ROWID) : {
    res_func = VectorBasicOp<VEC_TC_ROWID>::null_hash<ObMurmurHash, true>;
    break;
  }
  case (VEC_TC_LOB) : {
    res_func = VectorBasicOp<VEC_TC_LOB>::null_hash<ObMurmurHash, true>;
    break;
  }
  case (VEC_TC_JSON) : {
    res_func = VectorBasicOp<VEC_TC_JSON>::null_hash<ObMurmurHash, true>;
    break;
  }
  case (VEC_TC_GEO) : {
    res_func = VectorBasicOp<VEC_TC_GEO>::null_hash<ObMurmurHash, true>;
    break;
  }
  case (VEC_TC_UDT) : {
    res_func = VectorBasicOp<VEC_TC_UDT>::null_hash<ObMurmurHash, true>;
    break;
  }
  case (VEC_TC_DEC_INT32) : {
    res_func = VectorBasicOp<VEC_TC_DEC_INT32>::null_hash<ObMurmurHash, true>;
    break;
  }
  case (VEC_TC_DEC_INT64) : {
    res_func = VectorBasicOp<VEC_TC_DEC_INT64>::null_hash<ObMurmurHash, true>;
    break;
  }
  case (VEC_TC_DEC_INT128) : {
    res_func = VectorBasicOp<VEC_TC_DEC_INT128>::null_hash<ObMurmurHash, true>;
    break;
  }
  case (VEC_TC_DEC_INT256) : {
    res_func = VectorBasicOp<VEC_TC_DEC_INT256>::null_hash<ObMurmurHash, true>;
    break;
  }
  case (VEC_TC_DEC_INT512) : {
    res_func = VectorBasicOp<VEC_TC_DEC_INT512>::null_hash<ObMurmurHash, true>;
    break;
  }
  case (VEC_TC_COLLECTION) : {
    res_func = VectorBasicOp<VEC_TC_COLLECTION>::null_hash<ObMurmurHash, true>;
    break;
  }
  case (VEC_TC_MYSQL_DATETIME) : {
    res_func = VectorBasicOp<VEC_TC_MYSQL_DATETIME>::null_hash<ObMurmurHash, true>;
    break;
  }
  case (VEC_TC_MYSQL_DATE) : {
    res_func = VectorBasicOp<VEC_TC_MYSQL_DATE>::null_hash<ObMurmurHash, true>;
    break;
  }
  case (VEC_TC_ROARINGBITMAP) : {
    res_func = VectorBasicOp<VEC_TC_ROARINGBITMAP>::null_hash<ObMurmurHash, true>;
    break;
  }
  case (MAX_VEC_TC) : {
    res_func = VectorBasicOp<MAX_VEC_TC>::null_hash<ObMurmurHash, true>;
    break;
  }
  }
  return res_func;
}

int calc_collection_hash_val(const ObObjMeta &meta, const void *data, ObLength len, hash_algo hash_func, uint64_t seed, uint64_t &hash_val)
{
  return sql::ObArrayExprUtils::calc_collection_hash_val(meta, data, len, hash_func, seed, hash_val);
}

int collection_compare(const ObObjMeta &l_meta, const ObObjMeta &r_meta,
                       const void *l_v, const ObLength l_len,
                       const void *r_v, const ObLength r_len,
                       int &cmp_ret)
{
  return sql::ObArrayExprUtils::collection_compare(l_meta, r_meta, l_v, l_len, r_v, r_len, cmp_ret);
}

} // end namespace common
} // end namespace oceanbase