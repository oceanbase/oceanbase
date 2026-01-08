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

#define USING_LOG_PREFIX SQL_ENG

#include "ob_expr_udf.h"
#include "observer/ob_server.h"
#include "pl/ob_pl_stmt.h"
#include "ob_udf_result_cache.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

template <typename ARG_VEC>
int ObExprUDFUtils::inner_vec_to_obj(ObIVector *in_vec, ObObj &obj, const ObObjMeta &meta, const int64_t batch_idx)
{
  int ret = OB_SUCCESS;

  STATIC_ASSERT(ObMaxType == 55, "check if has new objtype and should adapt udf vectorization");

  ARG_VEC *arg_vec = static_cast<ARG_VEC *>(in_vec);
  if (arg_vec->is_null(batch_idx)) {
    obj.set_null();
    if (!meta.is_null()) {
      obj.set_collation_level(meta.get_collation_level());
      obj.set_collation_type(meta.get_collation_type());
    }
  } else {
    if (OB_UNLIKELY(meta.is_decimal_int() && meta.get_scale() < 0)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "invalid scale for decimal int", K(ret));
    } else {
      switch (meta.get_type()) {
        case ObNullType: {
          obj.set_null();
          break;
        }
        case ObVarcharType:
        case ObCharType:
        case ObHexStringType:
        case ObTinyTextType:
        case ObTextType:
        case ObMediumTextType:
        case ObLongTextType:
        case ObEnumInnerType:
        case ObSetInnerType:
        case ObRawType:
        case ObNVarchar2Type:
        case ObNCharType:
        case ObURowIDType:
        case ObUserDefinedSQLType:
        case ObCollectionSQLType:
        case ObLobType:
        case ObJsonType:
        case ObGeometryType:
        case ObRoaringBitmapType: {
          ObString tmp  = arg_vec->get_string(batch_idx);
          obj.set_string(meta.get_type(), tmp);
          break;
        }
        case ObNumberType:
        case ObUNumberType:
        case ObNumberFloatType: {
          const number::ObCompactNumber& tmp = arg_vec->get_number(batch_idx);
          obj.set_number(meta.get_type(), tmp.desc_, const_cast<uint32_t *>(tmp.digits_));
          break;
        }
        case ObTinyIntType:
        case ObSmallIntType:
        case ObMediumIntType:
        case ObInt32Type:
        case ObIntType:
        case ObDateTimeType:
        case ObTimestampType:
        case ObTimeType:
        case ObUnknownType:
        case ObUTinyIntType:
        case ObUSmallIntType:
        case ObUMediumIntType:
        case ObUInt32Type:
        case ObUInt64Type:
        case ObBitType:
        case ObEnumType:
        case ObSetType:
        case ObMySQLDateTimeType:
        case ObDoubleType:
        case ObUDoubleType:
        case ObIntervalYMType:
        {
          memcpy(&obj.v_.uint64_, arg_vec->get_payload(batch_idx), sizeof(uint64_t));
          break;
        }
        case ObDateType:
        case ObMySQLDateType:
        case ObFloatType:
        case ObUFloatType: {
          obj.v_.uint64_ = 0;
          memcpy(&obj.v_.uint64_, arg_vec->get_payload(batch_idx), sizeof(uint32_t));
          break;
        }
        case ObYearType: {
          obj.v_.uint64_ = 0;
          memcpy(&obj.v_.uint64_, arg_vec->get_payload(batch_idx), sizeof(uint8_t));
          break;
        }

        case ObIntervalDSType:
        case ObTimestampTZType: {
          memcpy(&obj.val_len_, arg_vec->get_payload(batch_idx), sizeof(uint32_t));
          memcpy(&obj.v_.uint64_, arg_vec->get_payload(batch_idx) + sizeof(uint32_t), sizeof(uint64_t));
          break;
        }
        case ObTimestampLTZType:
        case ObTimestampNanoType: {
          obj.val_len_ = 0;
          memcpy(&obj.val_len_, arg_vec->get_payload(batch_idx), sizeof(uint16_t));
          memcpy(&obj.v_.uint64_, arg_vec->get_payload(batch_idx) + sizeof(uint16_t), sizeof(uint64_t));
          break;
        }
        case ObDecimalIntType: {
          ObDecimalInt *tmp = const_cast<ObDecimalInt *>(arg_vec->get_decimal_int(batch_idx));
          obj.set_decimal_int(arg_vec->get_length(batch_idx), meta.get_scale(), tmp);
          break;
        }
        case ObExtendType:
        case ObMaxType: {
          ret = common::OB_ERR_UNEXPECTED;
          LOG_WARN("invalid obj type in vec to obj transfer", K(ret), K(meta.get_type()));
        }
        default: {
          ret = common::OB_NOT_SUPPORTED;
          LOG_WARN("not supported obj type in vec to obj transfer", K(ret), K(meta.get_type()));
        }
      }
      OX (obj.meta_ = meta);
    }
  }
  return ret;
}

template<VecValueTypeClass vec_tc>
int ObExprUDFUtils::dispatch_transfer_vec_to_obj(ObObj& obj, ObIVector *arg_vec, ObObjMeta meta, int64_t idx)
{
  int ret = OB_SUCCESS;
  VectorFormat arg_format = arg_vec->get_format();
  obj.reset();
  switch (arg_format) {
    case VEC_UNIFORM_CONST: {
      ret = inner_vec_to_obj<ObUniformVector<true, VectorBasicOp<vec_tc>>>(arg_vec, obj, meta, idx);
      break;
    }
    case VEC_UNIFORM: {
      ret = inner_vec_to_obj<ObUniformVector<false, VectorBasicOp<vec_tc>>>(arg_vec, obj, meta, idx);
      break;
    }
    case VEC_DISCRETE: {
      ret = inner_vec_to_obj<ObDiscreteVector<VectorBasicOp<vec_tc>>>(arg_vec, obj, meta, idx);
      break;
    }
    case VEC_CONTINUOUS: {
      ret = inner_vec_to_obj<ObContinuousVector<VectorBasicOp<vec_tc>>>(arg_vec, obj, meta, idx);
      break;
    }
    case VEC_FIXED: {
      ret = inner_vec_to_obj<ObFixedLengthVector<RTCType<vec_tc>, VectorBasicOp<vec_tc>>>(arg_vec, obj, meta, idx);
      break;
    }
    default: {
      ret = common::OB_ERR_UNEXPECTED;
      LOG_WARN("invalid udf arg vector data format", K(ret), K(arg_vec), K(meta));
    }
  }
  return ret;
}

template <typename RES_VEC>
int ObExprUDFUtils::inner_obj_to_vec(ObIVector *in_vec,
                                      ObObj& res,
                                      const int64_t batch_idx,
                                      const ObExpr &expr,
                                      ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  RES_VEC *arg_vec = static_cast<RES_VEC *>(in_vec);
  switch (res.meta_.get_type()) {
    case ObNullType: {
      arg_vec->set_null(batch_idx);
      break;
    }
    case ObVarcharType:
    case ObCharType:
    case ObHexStringType:
    case ObTinyTextType:
    case ObTextType:
    case ObMediumTextType:
    case ObLongTextType:
    case ObEnumInnerType:
    case ObSetInnerType:
    case ObRawType:
    case ObNVarchar2Type:
    case ObNCharType:
    case ObURowIDType:
    case ObJsonType:
    case ObGeometryType:
    case ObUserDefinedSQLType:
    case ObCollectionSQLType:
    case ObRoaringBitmapType:
    case ObLobType: {
      const ObString tmp = res.get_string();
      char *buf = expr.get_str_res_mem(eval_ctx, tmp.length());
      if (OB_ISNULL(buf)) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else {
        MEMMOVE(buf, tmp.ptr(), tmp.length());
      }
      OX (arg_vec->set_string(batch_idx, buf, tmp.length()));
      break;
    }
    case ObNumberType:
    case ObUNumberType:
    case ObNumberFloatType: {
      int64_t buf_len = sizeof(res.nmb_desc_);
      int64_t nmb_digits_len = 0;
      if (OB_LIKELY(1 == res.nmb_desc_.len_)) {
        nmb_digits_len = sizeof(*res.v_.nmb_digits_);
      } else {
        nmb_digits_len = sizeof(*res.v_.nmb_digits_) * res.nmb_desc_.len_;
      }
      buf_len += nmb_digits_len;
      char *buf = expr.get_str_res_mem(eval_ctx, buf_len);
      if (OB_ISNULL(buf)) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else {
        MEMMOVE(buf, &res.nmb_desc_, sizeof(res.nmb_desc_));
        MEMMOVE(buf + sizeof(res.nmb_desc_), res.v_.nmb_digits_, nmb_digits_len);
      }
      if (OB_FAIL(ret)) {
      } else {
        const number::ObNumber& tmp = number::ObNumber(res.nmb_desc_.desc_,
                              reinterpret_cast<uint32_t *>(buf + sizeof(res.nmb_desc_)));
        arg_vec->set_number(batch_idx, tmp);
      }
      break;
    }
    case ObTinyIntType:
    case ObSmallIntType:
    case ObInt32Type:
    case ObMediumIntType:
    case ObIntType:
    case ObDateTimeType:
    case ObTimestampType:
    case ObUnknownType:
    case ObTimeType:
    case ObDateType: {
      int64_t tmp = res.get_int();
      arg_vec->set_int(batch_idx, tmp);
      break;
    }
    case ObUTinyIntType:
    case ObUSmallIntType:
    case ObUMediumIntType:
    case ObUInt32Type:
    case ObUInt64Type:
    case ObSetType:
    case ObBitType:
    case ObEnumType: {
      uint64_t tmp = res.get_uint64();
      arg_vec->set_uint(batch_idx, tmp);
      break;
    }
    case ObDoubleType:
    case ObUDoubleType: {
      double tmp = res.get_double();
      arg_vec->set_double(batch_idx, tmp);
      break;
    }
    case ObFloatType:
    case ObUFloatType: {
      float tmp = res.get_float();
      arg_vec->set_float(batch_idx, tmp);
      break;
    }
    case ObMySQLDateTimeType: {
      const ObMySQLDateTime& tmp = res.get_mysql_datetime();
      arg_vec->set_mysql_datetime(batch_idx, tmp);
      break;
    }
    case ObMySQLDateType: {
      const ObMySQLDate& tmp = res.get_mysql_date();
      arg_vec->set_mysql_date(batch_idx, tmp);
      break;
    }
    case ObYearType: {
      uint8_t tmp = res.get_year();
      arg_vec->set_year(batch_idx, tmp);
      break;
    }
    case ObIntervalYMType: {
      const ObIntervalYMValue& tmp = res.get_interval_ym();
      arg_vec->set_interval_ym(batch_idx, tmp.nmonth_);
      break;
    }
    case ObIntervalDSType: {
      const ObIntervalDSValue& tmp = res.get_interval_ds();
      arg_vec->set_interval_ds(batch_idx, tmp);
      break;
    }
    case ObTimestampTZType: {
      const ObOTimestampData& tmp = res.get_otimestamp_value();
      arg_vec->set_otimestamp_tz(batch_idx, tmp);
      break;
    }
    case ObTimestampLTZType:
    case ObTimestampNanoType: {
      const ObOTimestampData& tmp = res.get_otimestamp_value();
      ObOTimestampTinyData v;
      v.from_timestamp_data(tmp);
      arg_vec->set_otimestamp_tiny(batch_idx, v);
      break;
    }
    case ObDecimalIntType: {
      const ObDecimalInt *tmp = res.get_decimal_int();
      arg_vec->set_decimal_int(batch_idx, tmp, res.val_len_);
      break;
    }
    case ObExtendType:
    case ObMaxType:  {
      ret = common::OB_ERR_UNEXPECTED;
      LOG_WARN("invalid obj type in obj to vec transfer", K(ret), K(res));
    }
    default: {
      ret = common::OB_NOT_SUPPORTED;
      LOG_WARN("not supported obj type in obj to vec transfer", K(ret), K(res));
    }
  }
  return ret;
}

template<VecValueTypeClass vec_tc>
int ObExprUDFUtils::dispatch_transfer_obj_to_vec(ObObj& result,
                                                  ObIVector *res_vec,
                                                  int64_t idx,
                                                  const ObExpr &expr,
                                                  ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  VectorFormat res_format = res_vec->get_format();
  switch (res_format) {
    case VEC_UNIFORM_CONST: {
      ret = inner_obj_to_vec<ObUniformVector<true, VectorBasicOp<vec_tc>>>(res_vec, result, idx, expr, eval_ctx);
      break;
    }
    case VEC_UNIFORM: {
      ret = inner_obj_to_vec<ObUniformVector<false, VectorBasicOp<vec_tc>>>(res_vec, result, idx, expr, eval_ctx);
      break;
    }
    case VEC_DISCRETE: {
      ret = inner_obj_to_vec<ObDiscreteVector<VectorBasicOp<vec_tc>>>(res_vec, result, idx, expr, eval_ctx);
      break;
    }
    case VEC_CONTINUOUS: {
      ret = inner_obj_to_vec<ObContinuousVector<VectorBasicOp<vec_tc>>>(res_vec, result, idx, expr, eval_ctx);
      break;
    }
    case VEC_FIXED: {
      ret = inner_obj_to_vec<ObFixedLengthVector<RTCType<vec_tc>, VectorBasicOp<vec_tc>>>(res_vec, result, idx, expr, eval_ctx);
      break;
    }
    default: {
      ret = common::OB_ERR_UNEXPECTED;
      LOG_WARN("invalid obj data format", K(ret), K(result));
    }
  }
  return ret;
}

int ObExprUDFUtils::transfer_vec_to_obj(ObObj *objs, ObIVector **arg_vec, const ObExpr &expr, int64_t idx)
{
  int ret = OB_SUCCESS;

#define HANDLE_VEC_TC_VEC2OBJ(VecType) \
case VecType: \
  ret = dispatch_transfer_vec_to_obj<VecType>(obj, arg_vector, meta, idx); \
  break;

  for (int64_t i = 0; i < expr.arg_cnt_ && OB_SUCC(ret); ++i) {
    const VecValueTypeClass vec_tc = expr.args_[i]->get_vec_value_tc();
    ObObj& obj = objs[i];
    ObIVector *arg_vector = arg_vec[i];
    ObObjMeta meta = expr.args_[i]->obj_meta_;
    switch(vec_tc) {
      HANDLE_VEC_TC_VEC2OBJ(VEC_TC_NULL)
      HANDLE_VEC_TC_VEC2OBJ(VEC_TC_INTEGER)
      HANDLE_VEC_TC_VEC2OBJ(VEC_TC_UINTEGER)
      HANDLE_VEC_TC_VEC2OBJ(VEC_TC_FLOAT)
      HANDLE_VEC_TC_VEC2OBJ(VEC_TC_DOUBLE)
      HANDLE_VEC_TC_VEC2OBJ(VEC_TC_FIXED_DOUBLE)
      HANDLE_VEC_TC_VEC2OBJ(VEC_TC_NUMBER)
      HANDLE_VEC_TC_VEC2OBJ(VEC_TC_DATETIME)
      HANDLE_VEC_TC_VEC2OBJ(VEC_TC_DATE)
      HANDLE_VEC_TC_VEC2OBJ(VEC_TC_TIME)
      HANDLE_VEC_TC_VEC2OBJ(VEC_TC_YEAR)
      HANDLE_VEC_TC_VEC2OBJ(VEC_TC_EXTEND)
      HANDLE_VEC_TC_VEC2OBJ(VEC_TC_UNKNOWN)
      HANDLE_VEC_TC_VEC2OBJ(VEC_TC_STRING)
      HANDLE_VEC_TC_VEC2OBJ(VEC_TC_BIT)
      HANDLE_VEC_TC_VEC2OBJ(VEC_TC_ENUM_SET)
      HANDLE_VEC_TC_VEC2OBJ(VEC_TC_ENUM_SET_INNER)
      HANDLE_VEC_TC_VEC2OBJ(VEC_TC_TIMESTAMP_TZ)
      HANDLE_VEC_TC_VEC2OBJ(VEC_TC_TIMESTAMP_TINY)
      HANDLE_VEC_TC_VEC2OBJ(VEC_TC_RAW)
      HANDLE_VEC_TC_VEC2OBJ(VEC_TC_INTERVAL_YM)
      HANDLE_VEC_TC_VEC2OBJ(VEC_TC_INTERVAL_DS)
      HANDLE_VEC_TC_VEC2OBJ(VEC_TC_ROWID)
      HANDLE_VEC_TC_VEC2OBJ(VEC_TC_LOB)
      HANDLE_VEC_TC_VEC2OBJ(VEC_TC_JSON)
      HANDLE_VEC_TC_VEC2OBJ(VEC_TC_GEO)
      HANDLE_VEC_TC_VEC2OBJ(VEC_TC_UDT)
      HANDLE_VEC_TC_VEC2OBJ(VEC_TC_DEC_INT32)
      HANDLE_VEC_TC_VEC2OBJ(VEC_TC_DEC_INT64)
      HANDLE_VEC_TC_VEC2OBJ(VEC_TC_DEC_INT128)
      HANDLE_VEC_TC_VEC2OBJ(VEC_TC_DEC_INT256)
      HANDLE_VEC_TC_VEC2OBJ(VEC_TC_DEC_INT512)
      HANDLE_VEC_TC_VEC2OBJ(VEC_TC_COLLECTION)
      HANDLE_VEC_TC_VEC2OBJ(VEC_TC_ROARINGBITMAP)
      HANDLE_VEC_TC_VEC2OBJ(VEC_TC_MYSQL_DATE)
      HANDLE_VEC_TC_VEC2OBJ(VEC_TC_MYSQL_DATETIME)
      default : {
        ret = common::OB_ERR_UNEXPECTED;
        LOG_WARN("invalid vec type class", K(ret), K(vec_tc));
      }
    }
  }
#undef HANDLE_VEC_TC_VEC2OBJ
  return ret;
}

int ObExprUDFUtils::transfer_obj_to_vec(ObObj& result,
                                        ObIVector *res_vec,
                                        int64_t idx,
                                        const ObExpr &expr,
                                        ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;

#define HANDLE_VEC_TC_OBJ2VEC(ObjType, VecType) \
  case ObjType: { \
    ret = dispatch_transfer_obj_to_vec<VecType>(result, res_vec, idx, expr, eval_ctx); \
  } break;

  switch(result.meta_.get_type()){
    case ObNullType: {
      res_vec->set_null(idx);
      break;
    }
    HANDLE_VEC_TC_OBJ2VEC(ObTinyIntType, VEC_TC_INTEGER);
    HANDLE_VEC_TC_OBJ2VEC(ObSmallIntType, VEC_TC_INTEGER);
    HANDLE_VEC_TC_OBJ2VEC(ObMediumIntType, VEC_TC_INTEGER);
    HANDLE_VEC_TC_OBJ2VEC(ObInt32Type, VEC_TC_INTEGER);
    HANDLE_VEC_TC_OBJ2VEC(ObIntType, VEC_TC_INTEGER);
    HANDLE_VEC_TC_OBJ2VEC(ObUTinyIntType, VEC_TC_UINTEGER);
    HANDLE_VEC_TC_OBJ2VEC(ObUSmallIntType, VEC_TC_UINTEGER);
    HANDLE_VEC_TC_OBJ2VEC(ObUMediumIntType, VEC_TC_UINTEGER);
    HANDLE_VEC_TC_OBJ2VEC(ObUInt32Type, VEC_TC_UINTEGER);
    HANDLE_VEC_TC_OBJ2VEC(ObUInt64Type, VEC_TC_UINTEGER);
    HANDLE_VEC_TC_OBJ2VEC(ObUFloatType, VEC_TC_FLOAT);
    HANDLE_VEC_TC_OBJ2VEC(ObFloatType, VEC_TC_FLOAT);
    HANDLE_VEC_TC_OBJ2VEC(ObNumberFloatType, VEC_TC_NUMBER);
    HANDLE_VEC_TC_OBJ2VEC(ObNumberType, VEC_TC_NUMBER);
    HANDLE_VEC_TC_OBJ2VEC(ObUNumberType, VEC_TC_NUMBER);
    HANDLE_VEC_TC_OBJ2VEC(ObDateTimeType, VEC_TC_DATETIME);
    HANDLE_VEC_TC_OBJ2VEC(ObTimestampType, VEC_TC_DATETIME);
    HANDLE_VEC_TC_OBJ2VEC(ObDateType, VEC_TC_DATE);
    HANDLE_VEC_TC_OBJ2VEC(ObTimeType, VEC_TC_TIME);
    HANDLE_VEC_TC_OBJ2VEC(ObYearType, VEC_TC_YEAR);
    HANDLE_VEC_TC_OBJ2VEC(ObNCharType, VEC_TC_STRING);
    HANDLE_VEC_TC_OBJ2VEC(ObNVarchar2Type, VEC_TC_STRING);
    HANDLE_VEC_TC_OBJ2VEC(ObCharType, VEC_TC_STRING);
    HANDLE_VEC_TC_OBJ2VEC(ObHexStringType, VEC_TC_STRING);
    HANDLE_VEC_TC_OBJ2VEC(ObTinyTextType, VEC_TC_STRING);
    HANDLE_VEC_TC_OBJ2VEC(ObVarcharType, VEC_TC_STRING);
    HANDLE_VEC_TC_OBJ2VEC(ObUnknownType, VEC_TC_UNKNOWN);
    HANDLE_VEC_TC_OBJ2VEC(ObTextType, VEC_TC_LOB);
    HANDLE_VEC_TC_OBJ2VEC(ObMediumTextType, VEC_TC_LOB);
    HANDLE_VEC_TC_OBJ2VEC(ObLongTextType, VEC_TC_LOB);
    HANDLE_VEC_TC_OBJ2VEC(ObBitType, VEC_TC_BIT);
    HANDLE_VEC_TC_OBJ2VEC(ObSetType, VEC_TC_ENUM_SET);
    HANDLE_VEC_TC_OBJ2VEC(ObEnumType, VEC_TC_ENUM_SET);
    HANDLE_VEC_TC_OBJ2VEC(ObSetInnerType, VEC_TC_ENUM_SET_INNER);
    HANDLE_VEC_TC_OBJ2VEC(ObEnumInnerType, VEC_TC_ENUM_SET_INNER);
    HANDLE_VEC_TC_OBJ2VEC(ObTimestampTZType, VEC_TC_TIMESTAMP_TZ);
    HANDLE_VEC_TC_OBJ2VEC(ObTimestampNanoType, VEC_TC_TIMESTAMP_TINY);
    HANDLE_VEC_TC_OBJ2VEC(ObTimestampLTZType, VEC_TC_TIMESTAMP_TINY);
    HANDLE_VEC_TC_OBJ2VEC(ObRawType, VEC_TC_RAW);
    HANDLE_VEC_TC_OBJ2VEC(ObIntervalYMType, VEC_TC_INTERVAL_YM);
    HANDLE_VEC_TC_OBJ2VEC(ObIntervalDSType, VEC_TC_INTERVAL_DS);
    HANDLE_VEC_TC_OBJ2VEC(ObURowIDType, VEC_TC_ROWID);
    HANDLE_VEC_TC_OBJ2VEC(ObJsonType, VEC_TC_JSON);
    HANDLE_VEC_TC_OBJ2VEC(ObGeometryType, VEC_TC_GEO);
    HANDLE_VEC_TC_OBJ2VEC(ObUserDefinedSQLType, VEC_TC_UDT);
    HANDLE_VEC_TC_OBJ2VEC(ObCollectionSQLType, VEC_TC_COLLECTION);
    HANDLE_VEC_TC_OBJ2VEC(ObMySQLDateType, VEC_TC_MYSQL_DATE);
    HANDLE_VEC_TC_OBJ2VEC(ObMySQLDateTimeType, VEC_TC_MYSQL_DATETIME);
    HANDLE_VEC_TC_OBJ2VEC(ObRoaringBitmapType, VEC_TC_ROARINGBITMAP);

    case ObUDoubleType:
    case ObDoubleType: {
      if (expr.datum_meta_.scale_ > SCALE_UNKNOWN_YET && expr.datum_meta_.scale_ <= OB_MAX_DOUBLE_FLOAT_SCALE) {
        ret = dispatch_transfer_obj_to_vec<VecValueTypeClass::VEC_TC_FIXED_DOUBLE>(result, res_vec, idx, expr, eval_ctx);
      } else {
        ret = dispatch_transfer_obj_to_vec<VecValueTypeClass::VEC_TC_DOUBLE>(result, res_vec, idx, expr, eval_ctx);
      }
      break;
    }
    case ObDecimalIntType: {
      const int16_t precision = expr.datum_meta_.precision_;
      if (precision <= MAX_PRECISION_DECIMAL_INT_32) {
        ret = dispatch_transfer_obj_to_vec<VecValueTypeClass::VEC_TC_DEC_INT32>(result, res_vec, idx, expr, eval_ctx);
      } else if (precision <= MAX_PRECISION_DECIMAL_INT_64) {
        ret = dispatch_transfer_obj_to_vec<VecValueTypeClass::VEC_TC_DEC_INT64>(result, res_vec, idx, expr, eval_ctx);
      } else if (precision <= MAX_PRECISION_DECIMAL_INT_128) {
        ret = dispatch_transfer_obj_to_vec<VecValueTypeClass::VEC_TC_DEC_INT128>(result, res_vec, idx, expr, eval_ctx);
      } else if (precision <= MAX_PRECISION_DECIMAL_INT_256) {
        ret = dispatch_transfer_obj_to_vec<VecValueTypeClass::VEC_TC_DEC_INT256>(result, res_vec, idx, expr, eval_ctx);
      } else if (precision <= MAX_PRECISION_DECIMAL_INT_512) {
        ret = dispatch_transfer_obj_to_vec<VecValueTypeClass::VEC_TC_DEC_INT512>(result, res_vec, idx, expr, eval_ctx);
      }
      break;
    }
    default: {
      ret = common::OB_ERR_UNEXPECTED;
      LOG_WARN("invalid vec type class", K(ret), K(result));
    }
  }
#undef HANDLE_VEC_TC_OBJ2VEC
  return ret;
}

int ObExprUDFUtils::process_in_params(ObExprUDFCtx &udf_ctx, ObIArray<ObObj> &deep_in_objs)
{
  int ret = OB_SUCCESS;
  if (udf_ctx.get_arg_count() > 0) {
    if (udf_ctx.get_info()->is_called_in_sql_) {
      OZ (process_in_params(udf_ctx.get_obj_stack(),
                            udf_ctx.get_arg_count(),
                            udf_ctx.get_info()->params_type_,
                            *(udf_ctx.get_param_store())));
    } else {
      OZ (process_in_params(udf_ctx.get_obj_stack(),
                            udf_ctx.get_arg_count(),
                            udf_ctx.get_info()->params_desc_,
                            udf_ctx.get_info()->params_type_,
                            *(udf_ctx.get_param_store()),
                            udf_ctx.get_allocator(),
                            &deep_in_objs));
    }
    if (udf_ctx.get_info()->is_udt_cons_) {
      pl::ObPLUDTNS ns(*(udf_ctx.get_exec_ctx()->get_sql_ctx()->schema_guard_));
      pl::ObPLDataType pl_type;
      pl_type.set_user_type_id(pl::PL_RECORD_TYPE, udf_ctx.get_info()->udf_package_id_);
      pl_type.set_type_from(pl::PL_TYPE_UDT);
      CK (0 < udf_ctx.get_param_store()->count());
      OZ (ns.init_complex_obj(udf_ctx.get_allocator(),
                              udf_ctx.get_allocator(),
                              pl_type,
                              udf_ctx.get_param_store()->at(0),
                              false,
                              false));
    }
  }
  return ret;
}

// for UDF from sql, only apply parameter to paramstore directly, UDF from sql only has pure IN argument.
int ObExprUDFUtils::process_in_params(const ObObj *objs_stack,
                                      int64_t param_num,
                                      const ObIArray<ObExprResType> &params_type,
                                      ParamStore& iparams)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < param_num; ++i) {
    ObObjParam param;
    param.reset();
    if (params_type.at(i).is_null()) {
      // default value, mock a max obj to tell pl engine here need replace to default value.
      param.set_is_pl_mock_default_param(true);
    } else {
      if (ObExtendType == params_type.at(i).get_type()) {
        if (!objs_stack[i].is_null()) {
          param.set_extend(objs_stack[i].get_ext(), objs_stack[i].get_meta().get_extend_type(), objs_stack[i].get_val_len());
          param.set_param_meta();
        } else {
          objs_stack[i].copy_value_or_obj(param, true);
        }
        OX (param.set_udt_id(params_type.at(i).get_udt_id()));
      } else {
        objs_stack[i].copy_value_or_obj(param, true);
        param.set_param_meta();
      }
    }
    OZ (iparams.push_back(param));
  }
  return ret;
}

int ObExprUDFUtils::process_return_value(ObObj &result,
                                         ObObj &tmp_result,
                                         ObEvalCtx &eval_ctx,
                                         ObExprUDFCtx &udf_ctx,
                                         ObExprUDFEnvGuard &guard)
{
  int ret = OB_SUCCESS;
  if (udf_ctx.get_info()->is_called_in_sql_) { // Call In SQL
    int64_t cur_obj_count = guard.get_cur_obj_count();
    if (tmp_result.is_pl_extend()
        && tmp_result.get_meta().get_extend_type() != pl::PL_REF_CURSOR_TYPE) { // memory of ref cursor on session, do not copy it.
      int tmp_ret = OB_SUCCESS;
      CK (OB_NOT_NULL(eval_ctx.exec_ctx_.get_pl_ctx()));
      OZ (pl::ObUserDefinedType::deep_copy_obj(eval_ctx.exec_ctx_.get_allocator(), tmp_result, result, true));
      if (OB_SUCC(ret)) {
        eval_ctx.exec_ctx_.get_pl_ctx()->reset_obj_range_to_end(cur_obj_count);
        OZ (eval_ctx.exec_ctx_.get_pl_ctx()->add(result));
        if (OB_FAIL(ret)) {
          if ((tmp_ret = pl::ObUserDefinedType::destruct_obj(result, eval_ctx.exec_ctx_.get_my_session())) != OB_SUCCESS) {
            LOG_WARN("failed to destruct result object", K(ret), K(tmp_ret));
          }
        }
      }
      if ((tmp_ret = pl::ObUserDefinedType::destruct_obj(tmp_result, eval_ctx.exec_ctx_.get_my_session())) != OB_SUCCESS) {
        LOG_WARN("failed to destruct tmp result object", K(ret), K(tmp_ret));
        ret = OB_SUCCESS == ret ? tmp_ret : ret;
      }
    } else {
      // Basic result & RefCursor, shadow copy result. What if failed with pl.execute for RefCursor?
      result = tmp_result;
      if (OB_NOT_NULL(eval_ctx.exec_ctx_.get_pl_ctx())) {
        eval_ctx.exec_ctx_.get_pl_ctx()->reset_obj_range_to_end(cur_obj_count);
      }
    }
  } else { // Call IN PL, shadow copy result.
    result = tmp_result;
  }
  return ret;
}

int ObExprUDFUtils::extract_allocator_and_restore_obj(
    const ObObj &obj, ObObj &new_obj, ObIAllocator *&composite_allocator)
{
  int ret = OB_SUCCESS;
  pl::ObPlCompiteWrite *composite_write = nullptr;
  CK (obj.is_ext());
  OX (composite_write = reinterpret_cast<pl::ObPlCompiteWrite *>(obj.get_ext()));
  CK (OB_NOT_NULL(composite_write));
  OX (new_obj.set_extend(composite_write->value_addr_, obj.get_meta().get_extend_type(), obj.get_val_len()));
  OX (composite_allocator = reinterpret_cast<ObIAllocator *>(composite_write->allocator_));

  return ret;
}

// Check current In parameter is Other out parameter or not.
int ObExprUDFUtils::need_deep_copy_in_parameter(const ObObj *objs_stack,
                                                int64_t param_num,
                                                const ObIArray<ObUDFParamDesc> &params_desc,
                                                const ObIArray<ObExprResType> &params_type,
                                                const ObObj &element,
                                                bool &need_deep_copy)
{
  int ret = OB_SUCCESS;
  need_deep_copy = false;
  for (int64_t i = 0; !need_deep_copy && i < param_num; ++i) {
    if (params_desc.at(i).is_out()
        && ObExtendType == params_type.at(i).get_type()) {
      ObObj value = objs_stack[i];
      if (params_desc.at(i).is_obj_access_out()) {
        ObIAllocator *dum_alloc = nullptr;
        OZ (extract_allocator_and_restore_obj(value, value, dum_alloc));
      }
      if (OB_SUCC(ret)) {
        if(element.get_ext() == value.get_ext()
          && value.is_pl_extend()
          && element.is_pl_extend()
          && value.get_meta().get_extend_type() == element.get_meta().get_extend_type()) {
          need_deep_copy = true;
        }
      }
    }
  }
  return ret;
}

// for UDF from PL, may has In/Out parameter, some In Out has same source argument, so need deep copy.
int ObExprUDFUtils::process_in_params(const ObObj *objs_stack,
                                      int64_t param_num,
                                      const ObIArray<ObUDFParamDesc> &params_desc,
                                      const ObIArray<ObExprResType> &params_type,
                                      ParamStore& iparams,
                                      ObIAllocator &allocator,
                                      ObIArray<ObObj> *deep_in_objs)
{
  int ret = OB_SUCCESS;
  CK (0 == param_num || OB_NOT_NULL(objs_stack));
  CK (param_num == params_desc.count());
  for (int64_t i = 0; OB_SUCC(ret) && i < param_num; ++i) {
    ObObjParam param;
    param.reset();
    if (params_type.at(i).is_null()) {
      // default value, mock a max obj to tell pl engine here need replace to default value.
      param.set_is_pl_mock_default_param(true);
    } else if (!params_desc.at(i).is_out()) { // in parameter
      if (ObExtendType == params_type.at(i).get_type()) {
        bool need_copy = false;
        OZ (need_deep_copy_in_parameter(objs_stack, param_num, params_desc, params_type, objs_stack[i], need_copy));
        if (need_copy) {
          OZ (pl::ObUserDefinedType::deep_copy_obj(allocator, objs_stack[i], param, true));
          if (OB_NOT_NULL(deep_in_objs)) {
            OZ (deep_in_objs->push_back(param));
          }
        } else {
          if (!objs_stack[i].is_null()) {
            param.set_extend(objs_stack[i].get_ext(), objs_stack[i].get_meta().get_extend_type(), objs_stack[i].get_val_len());
            param.set_param_meta();
          } else {
            objs_stack[i].copy_value_or_obj(param, true);
          }
        }
      } else {
        objs_stack[i].copy_value_or_obj(param, true);
        param.set_param_meta();
      }
    } else if (params_desc.at(i).is_local_out()
              || params_desc.at(i).is_package_var_out()
              || params_desc.at(i).is_subprogram_var_out()) {
      if (ObExtendType != params_type.at(i).get_type()) {
        OZ (deep_copy_obj(allocator, objs_stack[i], param));
      } else {
        objs_stack[i].copy_value_or_obj(param, true);
      }
    } else {
      ObIAllocator *dum_alloc = nullptr;
      ObObj value = objs_stack[i];
      OZ (extract_allocator_and_restore_obj(value, value, dum_alloc));
      if (OB_SUCC(ret)) {
        if (params_type.at(i).get_type() == ObExtendType) {
          if (params_desc.at(i).is_obj_access_pure_out()) {
            OZ (pl::ObUserDefinedType::deep_copy_obj(allocator, value, param));
            if (OB_NOT_NULL(deep_in_objs)) {
              OZ (deep_in_objs->push_back(param));
            }
          } else {
            param.set_extend(value.get_ext(), value.get_meta().get_extend_type(), value.get_val_len());
            param.set_param_meta();
          }
        } else {
          void *ptr = NULL;
          ObObj *obj = NULL;
          CK (value.is_ext());
          OX (ptr = reinterpret_cast<void*>(value.get_ext()));
          CK (OB_NOT_NULL(ptr));
          OX (obj = reinterpret_cast<ObObj*>(ptr));
          CK (OB_NOT_NULL(obj));
          OX ((*obj).copy_value_or_obj(param, true));
          OX (param.set_param_meta());
        }
      }
    }
    if (OB_SUCC(ret) && params_type.at(i).get_type() == ObExtendType) {
      param.set_udt_id(params_type.at(i).get_udt_id());
    }
    OZ (iparams.push_back(param));
  }
  return ret;
}

int ObExprUDFUtils::process_out_params(ObExprUDFCtx &udf_ctx, ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  if (udf_ctx.get_info()->is_udt_cons_) {
    pl::ObPLComposite *self_argument
      = reinterpret_cast<pl::ObPLComposite *>(udf_ctx.get_param_store()->at(0).get_ext());
    CK (OB_NOT_NULL(self_argument));
    if (OB_SUCC(ret) && self_argument->is_record()) {
      OX (self_argument->set_is_null(false));
    }
  }
  if (udf_ctx.has_out_param()) {
    OZ (process_out_params(udf_ctx.get_obj_stack(),
                                     udf_ctx.get_arg_count(),
                                     *udf_ctx.get_param_store(),
                                     udf_ctx.get_allocator(),
                                     eval_ctx.exec_ctx_,
                                     udf_ctx.get_info()->nocopy_params_,
                                     udf_ctx.get_info()->params_desc_,
                                     udf_ctx.get_info()->out_params_type_));
  }
  return ret;
}

int ObExprUDFUtils::process_out_params(const ObObj *objs_stack,
                                       int64_t param_num,
                                       ParamStore& iparams,
                                       ObIAllocator &alloc,
                                       ObExecContext &exec_ctx,
                                       const ObIArray<int64_t> &nocopy_params,
                                       const ObIArray<ObUDFParamDesc> &params_desc,
                                       const ObIArray<ObExprResType> &params_type)
{
  int ret = OB_SUCCESS;
  UNUSED (param_num);
  CK (iparams.count() == params_desc.count());
  CK (0 == nocopy_params.count() || nocopy_params.count() == iparams.count());
  // 先处理NoCopy参数
  ObSEArray<bool, 16> dones;
  for (int64_t i = 0; OB_SUCC(ret) && i < iparams.count(); ++i) {
    if (!params_desc.at(i).is_out()) {
      OZ (dones.push_back(true));
    } else if (params_desc.at(i).is_local_out() && nocopy_params.at(i) != OB_INVALID_INDEX) {
      pl::ObPLExecCtx *ctx = nullptr;
      ObIAllocator *symbol_alloc = nullptr;
      ObIAllocator *cur_expr_allocator = nullptr;
      const ParamStore &param_store = exec_ctx.get_physical_plan_ctx()->get_param_store();
      int64_t position = params_desc.at(i).get_index();
      ObObjParam *modify = NULL;
      ObObjParam result;
      ObObjParam tmp;
      if (OB_NOT_NULL(exec_ctx.get_my_session()->get_pl_context())) {
        ctx = exec_ctx.get_my_session()->get_pl_context()->get_current_ctx();
        CK (OB_NOT_NULL(ctx));
        OX (symbol_alloc = ctx->allocator_);
        OX (cur_expr_allocator = ctx->get_top_expr_allocator());
        CK (OB_NOT_NULL(cur_expr_allocator));
      }
      CK (position < param_store.count());
      CK (OB_NOT_NULL(modify = const_cast<ObObjParam*>(&(param_store.at(position)))));
      OZ (sql::ObSPIService::spi_convert(exec_ctx.get_my_session(),
                                         nullptr != cur_expr_allocator ? cur_expr_allocator : &alloc,
                                         iparams.at(i),
                                         params_type.at(i),
                                         tmp));
      if (symbol_alloc != nullptr) {
        if (tmp.is_pl_extend() &&
            tmp.get_meta().get_extend_type() != pl::PL_REF_CURSOR_TYPE) {
          OZ (pl::ObUserDefinedType::deep_copy_obj(*symbol_alloc, tmp, result));
        } else {
          OZ (deep_copy_obj(*symbol_alloc, tmp, result));
        }
      } else {
        OX (result = tmp);
      }
      if (OB_SUCC(ret) && symbol_alloc != nullptr) {
        if (modify->is_pl_extend() &&
            modify->get_meta().get_extend_type() != pl::PL_REF_CURSOR_TYPE) {
          pl::ObUserDefinedType::destruct_objparam(*symbol_alloc, *modify, exec_ctx.get_my_session());
        } else if (modify->need_deep_copy()) {
          void *ptr = modify->get_deep_copy_obj_ptr();
          if (nullptr != ptr) {
            symbol_alloc->free(ptr);
          }
        }
      }
      if (result.is_pl_extend()
          && (result.get_meta().get_extend_type() == pl::PL_REF_CURSOR_TYPE || result.get_meta().get_extend_type() == pl::PL_CURSOR_TYPE)) {
        OZ (ObSPIService::spi_copy_ref_cursor(ctx, NULL, &result, modify));
      } else {
        OX (result.copy_value_or_obj(*modify, true));
      }
      OX (modify->set_param_meta());
      if (OB_SUCC(ret) && iparams.at(i).is_ref_cursor_type()) {
        modify->set_is_ref_cursor_type(true);
      }
      OZ (dones.push_back(true));
    } else {
      OZ (dones.push_back(false));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < iparams.count(); ++i) {
    OZ (process_singal_out_param(i,
                                 dones,
                                 objs_stack,
                                 param_num,
                                 iparams,
                                 alloc,
                                 exec_ctx,
                                 nocopy_params,
                                 params_desc,
                                 params_type));
  }
  return ret;
}

int ObExprUDFUtils::process_singal_out_param(int64_t i,
                                             ObIArray<bool> &dones,
                                             const ObObj *objs_stack,
                                             int64_t param_num,
                                             ParamStore& iparams,
                                             ObIAllocator &alloc,
                                             ObExecContext &exec_ctx,
                                             const ObIArray<int64_t> &nocopy_params,
                                             const ObIArray<ObUDFParamDesc> &params_desc,
                                             const ObIArray<ObExprResType> &params_type)
{
  int ret = OB_SUCCESS;
  pl::ObPLExecCtx *ctx = nullptr;
  ObIAllocator *symbol_alloc = nullptr;
  ObIAllocator *cur_expr_allocator = nullptr;
  ObObjParam tmp;
  if (OB_NOT_NULL(exec_ctx.get_my_session()->get_pl_context())) {
    ctx = exec_ctx.get_my_session()->get_pl_context()->get_current_ctx();
    CK (OB_NOT_NULL(ctx));
    OX (symbol_alloc = ctx->allocator_);
    OX (cur_expr_allocator = ctx->get_top_expr_allocator());
    CK (OB_NOT_NULL(cur_expr_allocator));
  }
  if (OB_FAIL(ret)) {
  } else if (dones.at(i)) {
    // already process, do nothing
  } else if (params_desc.at(i).is_local_out()) { //out param in paramstore of caller
    if (nocopy_params.count() > 0 && nocopy_params.at(i) != OB_INVALID_INDEX) {
      // nocopy parameter already process before, do nothing ....
    } else {
      const ParamStore &param_store = exec_ctx.get_physical_plan_ctx()->get_param_store();
      int64_t position = params_desc.at(i).get_index();
      ObObjParam *modify = NULL;
      ObObjParam result;
      CK (position < param_store.count());
      CK (OB_NOT_NULL(modify = const_cast<ObObjParam*>(&(param_store.at(position)))));
      // ext type cannot convert. just copy it.
      if (iparams.at(i).is_ext()) {
        // caller param may ref cursor, which may not allocated.
        if (modify->is_null()) {
          OX (iparams.at(i).copy_value_or_obj(*modify, true));
          if (iparams.at(i).is_ref_cursor_type()) {
            modify->set_is_ref_cursor_type(true);
          }
          OX (modify->set_param_meta());
        } else if (!modify->is_ext()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("process function out param failed, type mismatch", K(ret),
                                                                 K(iparams.at(i)), K(*modify));
        } else {
          if (iparams.at(i).is_pl_extend()
              && (iparams.at(i).get_meta().get_extend_type() == pl::PL_REF_CURSOR_TYPE || iparams.at(i).get_meta().get_extend_type() == pl::PL_CURSOR_TYPE)) {
            OZ (ObSPIService::spi_copy_ref_cursor(ctx, NULL, &iparams.at(i), modify));
          } else {
            OX (iparams.at(i).copy_value_or_obj(*modify, true));
          }
          OX (modify->set_param_meta());
          if (OB_SUCC(ret) && iparams.at(i).is_ref_cursor_type()) {
            modify->set_is_ref_cursor_type(true);
          }
        }
      } else {
        OZ (sql::ObSPIService::spi_convert(exec_ctx.get_my_session(),
                                           nullptr != cur_expr_allocator ? cur_expr_allocator : &alloc,
                                           iparams.at(i),
                                           params_type.at(i),
                                           tmp));
        if (symbol_alloc != nullptr) {
          OZ (deep_copy_obj(*symbol_alloc, tmp, result));
        } else {
          OX (result = tmp);
        }
        if (OB_SUCC(ret) && symbol_alloc != nullptr) {
          void *ptr = modify->get_deep_copy_obj_ptr();
          if (nullptr != ptr) {
            symbol_alloc->free(ptr);
          }
        }
        OX (result.copy_value_or_obj(*modify, true));
        OX (modify->set_param_meta());
        if (OB_SUCC(ret) && iparams.at(i).is_ref_cursor_type()) {
          modify->set_is_ref_cursor_type(true);
        }
      }
      OX (dones.at(i) = true);
    }
  } else if (params_desc.at(i).is_package_var_out()) {
    OZ (sql::ObSPIService::spi_convert(exec_ctx.get_my_session(),
                                        nullptr != cur_expr_allocator ? cur_expr_allocator : &alloc,
                                        iparams.at(i),
                                        params_type.at(i),
                                        tmp));
    CK (OB_NOT_NULL(ctx));
    OZ (ObSPIService::spi_set_package_variable(
      &exec_ctx,
      ctx->guard_,
      params_desc.at(i).get_package_id(),
      params_desc.at(i).get_index(),
      tmp));
    OX (dones.at(i) = true);
  } else if (params_desc.at(i).is_subprogram_var_out()) {
    OZ (sql::ObSPIService::spi_convert(exec_ctx.get_my_session(),
                                        nullptr != cur_expr_allocator ? cur_expr_allocator : &alloc,
                                        iparams.at(i),
                                        params_type.at(i),
                                        tmp));
    OZ (pl::ObPLContext::set_subprogram_var_from_local(
      *exec_ctx.get_my_session(),
      params_desc.at(i).get_package_id(),
      params_desc.at(i).get_subprogram_id(),
      params_desc.at(i).get_index(),
      tmp));
    OX (dones.at(i) = true);
  } else if (params_desc.at(i).is_obj_access_out() &&
             OB_INVALID_ID != params_desc.at(i).get_package_id() &&
             OB_INVALID_ID != params_desc.at(i).get_index() &&
             params_desc.at(i).is_obj_access_package_var_out()) {
    OZ (SMART_CALL(process_package_out_param(
      i, dones, objs_stack, param_num, iparams, alloc, exec_ctx, nocopy_params, params_desc, params_type)));
  } else if (!params_type.at(i).is_ext()) {
    void *ptr = NULL;
    ObObj *obj = NULL;
    ObObjParam result;
    ObObjParam tmp;
    ObArenaAllocator tmp_alloc;
    ObObj value = objs_stack[i];
    ObIAllocator *composite_allocator = nullptr;
    OZ (extract_allocator_and_restore_obj(value, value, composite_allocator));
    CK (value.is_ext());
    OX (ptr = reinterpret_cast<void*>(value.get_ext()));
    CK (OB_NOT_NULL(ptr));
    OX (obj = reinterpret_cast<ObObj*>(ptr));
    CK (OB_NOT_NULL(obj));
    OZ (sql::ObSPIService::spi_convert(exec_ctx.get_my_session(),
                                       &alloc, iparams.at(i),
                                       params_type.at(i),
                                       tmp));
    if (composite_allocator != nullptr) {
      OZ (deep_copy_obj(*composite_allocator, tmp, result));
    } else {
      OX (result = tmp);
    }
    if (OB_SUCC(ret) && composite_allocator != nullptr) {
      void *ptr = obj->get_deep_copy_obj_ptr();
      if (nullptr != ptr) {
        composite_allocator->free(ptr);
      }
    }
    OX (result.copy_value_or_obj(*obj, true));
    OX (result.set_param_meta());
    OX (dones.at(i) = true);
  } else if (params_desc.at(i).is_obj_access_pure_out()) { // objaccess complex type pure out sence
    ObObj &obj = iparams.at(i);
    ObObj origin_value = objs_stack[i];
    ObIAllocator *composite_allocator = nullptr;
    OZ (extract_allocator_and_restore_obj(origin_value, origin_value, composite_allocator));
    if (obj.is_ext() && obj.get_meta().get_extend_type() != pl::PL_REF_CURSOR_TYPE) {
      OZ (pl::ObUserDefinedType::deep_copy_obj(nullptr != composite_allocator ? *composite_allocator : alloc, obj, origin_value, true));
    }
    OX (dones.at(i) = true);
  }
  return ret;
}

int ObExprUDFUtils::process_package_out_param(int64_t idx,
                                              ObIArray<bool> &dones,
                                              const ObObj *objs_stack,
                                              int64_t param_num,
                                              ParamStore& iparams,
                                              ObIAllocator &alloc,
                                              ObExecContext &exec_ctx,
                                              const ObIArray<int64_t> &nocopy_params,
                                              const ObIArray<ObUDFParamDesc> &params_desc,
                                              const ObIArray<ObExprResType> &params_type)
{
  int ret = OB_SUCCESS;
  // check if left out parameter is child of current
  for (int64_t i = idx + 1; OB_SUCC(ret) && i < iparams.count(); ++i) {
    if (!dones.at(i) && iparams.at(i).is_ext()) {
      bool is_child = false;
      ObIAllocator *dum_alloc = nullptr;
      ObObj origin_value_i = objs_stack[i];
      ObObj origin_value_idx = objs_stack[idx];
      OZ (extract_allocator_and_restore_obj(origin_value_i, origin_value_i, dum_alloc));
      OZ (extract_allocator_and_restore_obj(origin_value_idx, origin_value_idx, dum_alloc));
      OZ (is_child_of(origin_value_idx, origin_value_i, is_child));
      if (OB_SUCC(ret) && is_child) {
        OZ (SMART_CALL(process_singal_out_param(
          i, dones, objs_stack, param_num, iparams, alloc, exec_ctx, nocopy_params, params_desc, params_type)));
      }
    }
  }
  ObObj origin_value = objs_stack[idx];
  ObIAllocator *allocator = NULL;
  pl::ObPLExecCtx plctx(nullptr, nullptr, &exec_ctx, nullptr,nullptr,nullptr,nullptr);
  ObIAllocator *composite_allocator = nullptr;
  OZ (exec_ctx.get_package_guard(plctx.guard_));
  OZ (extract_allocator_and_restore_obj(origin_value, origin_value, composite_allocator));
  if (OB_SUCC(ret)) {
    if (OB_NOT_NULL(composite_allocator)) {
      allocator = composite_allocator;
    } else {
      ObIAllocator *pkg_allocator = NULL;
      OZ (ObSPIService::spi_get_package_allocator(&plctx, params_desc.at(idx).get_package_id(), pkg_allocator));
      OX (allocator = pkg_allocator);
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!params_type.at(idx).is_ext()) {
    void *ptr = NULL;
    ObObj *obj = NULL;
    ObObjParam result;
    ObObjParam tmp;
    CK (origin_value.is_ext());
    OX (ptr = reinterpret_cast<void*>(origin_value.get_ext()));
    CK (OB_NOT_NULL(ptr));
    OX (obj = reinterpret_cast<ObObj*>(ptr));
    CK (OB_NOT_NULL(obj));
    OZ (sql::ObSPIService::spi_convert(exec_ctx.get_my_session(),
                                       &alloc,
                                       iparams.at(idx),
                                       params_type.at(idx),
                                       tmp));
    if (allocator != nullptr) {
      OZ (deep_copy_obj(*allocator, tmp, result));
    } else {
      OX (result = tmp);
    }
    if (OB_SUCC(ret) && allocator != nullptr) {
      void *ptr = obj->get_deep_copy_obj_ptr();
      if (nullptr != ptr) {
        allocator->free(ptr);
      }
    }
    OX (result.copy_value_or_obj(*obj, true));
    OX (result.set_param_meta());
  } else {
    ObObj &obj = iparams.at(idx);
    if (OB_SUCC(ret) && nullptr != allocator) {
      if (obj.is_ext() && obj.get_meta().get_extend_type() != pl::PL_REF_CURSOR_TYPE) {
        OZ (pl::ObUserDefinedType::deep_copy_obj(*allocator, obj, origin_value, true));
      }
    }
  }
  OZ (ObSPIService::spi_update_package_change_info(
    &plctx, params_desc.at(idx).get_package_id(), params_desc.at(idx).get_index()));
  OX (dones.at(idx) = true);
  return ret;
}

int ObExprUDFUtils::is_child_of(const ObObj &parent, const ObObj &child, bool &is_child)
{
  int ret = OB_SUCCESS;
  if (parent.is_ext() && child.is_ext() && parent.get_ext() == child.get_ext()) {
    is_child = true;
  } else if (parent.is_pl_extend() && parent.get_ext() != 0) {
    switch (parent.get_meta().get_extend_type()) {
      case pl::PL_NESTED_TABLE_TYPE:
      case pl::PL_ASSOCIATIVE_ARRAY_TYPE:
      case pl::PL_VARRAY_TYPE: {
        pl::ObPLCollection* coll = reinterpret_cast<pl::ObPLCollection*>(parent.get_ext());
        CK (OB_NOT_NULL(coll));
        for (int64_t i = 0; OB_SUCC(ret) && i < coll->get_count(); ++i) {
          CK (OB_NOT_NULL(coll->get_data()));
          if (OB_FAIL(ret)) {
          } else if (!(coll->get_data()[i]).is_ext()) {
            ObObj tmp;
            tmp.set_ext(reinterpret_cast<int64_t>(&(coll->get_data()[i])));
            OZ (SMART_CALL(is_child_of(tmp, child, is_child)));
          } else {
            OZ (SMART_CALL(is_child_of(coll->get_data()[i], child, is_child)));
          }
        }
      } break;
      case pl::PL_RECORD_TYPE: {
        pl::ObPLRecord* record = reinterpret_cast<pl::ObPLRecord*>(parent.get_ext());
        CK (OB_NOT_NULL(record));
        for (int64_t i = 0; OB_SUCC(ret) && i < record->get_count(); ++i) {
          ObObj *obj = NULL;
          OZ (record->get_element(i, obj));
          CK (OB_NOT_NULL(obj));
          if (OB_FAIL(ret)) {
          } else if (!obj->is_ext()) {
            ObObj tmp;
            tmp.set_ext(reinterpret_cast<int64_t>(obj));
            OZ (SMART_CALL(is_child_of(tmp, child, is_child)));
          } else {
            OZ (SMART_CALL(is_child_of(*obj, child, is_child)));
          }
        }
      } break;
      default: {
      } break;
    }
  }
  return ret;
}

// Reference by ob_adjust_lob_datum in ob_expr_lob_utils.cpp
int ObExprUDFUtils::ob_adjust_lob_obj(const ObObj &origin_obj,
                                      const common::ObObjMeta &obj_meta,
                                      ObIAllocator &allocator,
                                      ObObj *out_obj)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(out_obj));
  if (!is_lob_storage(origin_obj.get_type())) { // null & nop is not lob
  } else if (origin_obj.has_lob_header() != obj_meta.has_lob_header()) {
    if (origin_obj.has_lob_header()) { // obj_meta does not have lob header, get data only
      // can avoid allocator if no persist lobs call this function,
      OB_ASSERT(origin_obj.is_persist_lob() == false);
      ObString full_data;
      if (OB_FAIL(ObTextStringHelper::read_real_string_data(&allocator, origin_obj, full_data))) {
        LOG_WARN("Lob: failed to get full data", K(ret));
      } else {
        out_obj->set_string(obj_meta.get_type(), full_data);
      }
    } else { // origin obj does not have lob header, but meta has, build temp lob header
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect for input obj, input obj should has lob header", K(ret), K(origin_obj), K(obj_meta));
    }
  }
  return ret;
}

int ObExprUDFUtils::adjust_return_value(ObObj &result,
                                        const common::ObObjMeta &obj_meta,
                                        ObIAllocator &allocator,
                                        ObExprUDFCtx &udf_ctx)
{
  int ret = OB_SUCCESS;
  if (!result.is_null() && result.get_type() != obj_meta.get_type()) {
    if (ObLobType == obj_meta.get_type()) {
      ObLobLocator *value = NULL;
      char *total_buf = NULL;
      ObString result_data = result.get_string();
      if (is_lob_storage(result.get_type())) {
        if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(&allocator, result, result_data))) {
          LOG_WARN("failed to get real data for lob", K(ret), K(result));
        }
      }
      const int64_t total_buf_len = sizeof(ObLobLocator) + result_data.length();
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(total_buf = reinterpret_cast<char*>(allocator.alloc(total_buf_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Failed to allocate memory for lob locator", K(ret), K(total_buf_len));
      } else if (FALSE_IT(value = reinterpret_cast<ObLobLocator *>(total_buf))) {
      } else if (OB_FAIL(value->init(result_data))) {
        LOG_WARN("Failed to init lob locator", K(ret), K(value));
      } else {
        result.set_lob_locator(*value);
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("get unexpected result type", K(ret), K(result.get_type()), K(obj_meta.get_type()));
    }
  }
  if (OB_SUCC(ret) && is_lob_storage(result.get_type())) {
    if (OB_FAIL(ob_adjust_lob_obj(result, obj_meta, allocator, &result))) {
      LOG_WARN("failed to adjust lob obj", K(ret), K(result), K(obj_meta));
    }
  }
  if (OB_SUCC(ret)
      && udf_ctx.get_info()->is_called_in_sql_
      && result.is_raw() && result.get_raw().length() > OB_MAX_ORACLE_RAW_SQL_COL_LENGTH
      && lib::is_oracle_mode()) {
    ret = OB_ERR_NUMERIC_OR_VALUE_ERROR;
    ObString err_msg("raw variable length too long");
    LOG_WARN("raw variable length too long", K(ret), K(result.get_raw().length()));
    LOG_USER_ERROR(OB_ERR_NUMERIC_OR_VALUE_ERROR, err_msg.length(), err_msg.ptr());
  }
  return ret;
}

int ObExprUDFUtils::transfer_datum_to_objs(const ObExpr &expr, ObEvalCtx &eval_ctx, ObObj *objs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < expr.arg_cnt_ && OB_SUCC(ret); ++i) {
    objs[i].reset();
    ObDatum &param = expr.args_[i]->locate_expr_datum(eval_ctx);
    if (OB_FAIL(param.to_obj(objs[i], expr.args_[i]->obj_meta_))) {
      LOG_WARN("failed to convert obj", K(ret), K(i));
    }
  }
  return ret;
}

}
}