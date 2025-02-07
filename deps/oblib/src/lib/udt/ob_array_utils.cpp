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

#define USING_LOG_PREFIX LIB
#include "ob_array_utils.h"

namespace oceanbase {
namespace common {

int ObArrayUtil::get_type_name(ObNestedType coll_type, const ObDataType &elem_type, char *buf, int buf_len, uint32_t depth)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  for (uint32_t i = 0; OB_SUCC(ret) && i < depth; i++) {
    if (coll_type == ObNestedType::OB_ARRAY_TYPE) {
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, "ARRAY("))) {
        LOG_WARN("failed to convert len to string", K(ret));
      }
    } else if (coll_type == ObNestedType::OB_VECTOR_TYPE) {
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, "VECTOR("))) {
        LOG_WARN("failed to convert len to string", K(ret));
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid collection type", K(ret), K(coll_type));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s", ob_sql_type_str(elem_type.get_obj_type())))) {
    LOG_WARN("failed to convert len to string", K(ret));
  } else if (elem_type.get_obj_type() == ObDecimalIntType
             && OB_FAIL(databuff_printf(buf, buf_len, pos, "(%d,%d)", elem_type.get_precision(), elem_type.get_scale()))) {
    LOG_WARN("failed to add deciaml precision to string", K(ret));
  } else if (ob_is_string_tc(elem_type.get_obj_type())
             && OB_FAIL(databuff_printf(buf, buf_len, pos, "(%d)", elem_type.get_length()))) {
    LOG_WARN("failed to add string len to string", K(ret));
  }
  for (uint32_t i = 0; OB_SUCC(ret) && i < depth; i++) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, ")"))) {
      LOG_WARN("failed to add ) to string", K(ret));
    }
  }
  return ret;
}

int ObArrayUtil::push_back_decimal_int(const ObPrecision prec, const ObDecimalInt *dec_val, bool is_null, ObIArrayType *arr_obj)
{
  int ret = OB_SUCCESS;
  if (get_decimalint_type(prec) == DECIMAL_INT_32) {
    ObArrayFixedSize<int32_t> *arr = static_cast<ObArrayFixedSize<int32_t> *>(arr_obj);
    if (is_null) {
      if (OB_FAIL(arr->push_back(0, true))) {
        LOG_WARN("failed to push back null value", K(ret));
      }
    } else if (OB_FAIL(arr->push_back(dec_val->int32_v_[0]))) {
      LOG_WARN("failed to push back decimal int32 value", K(ret), K(dec_val->int32_v_[0]));
    }
  } else if (get_decimalint_type(prec) == DECIMAL_INT_64) {
    ObArrayFixedSize<int64_t> *arr = static_cast<ObArrayFixedSize<int64_t> *>(arr_obj);
    if (is_null) {
      if (OB_FAIL(arr->push_back(0, true))) {
        LOG_WARN("failed to push back null value", K(ret));
      }
    } else if (OB_FAIL(arr->push_back(dec_val->int64_v_[0]))) {
      LOG_WARN("failed to push back decimal int64 value", K(ret), K(dec_val->int64_v_[0]));
    }
  } else if (get_decimalint_type(prec) == DECIMAL_INT_128) {
    ObArrayFixedSize<int128_t> *arr = static_cast<ObArrayFixedSize<int128_t> *>(arr_obj);
    if (is_null) {
      if (OB_FAIL(arr->push_back(0, true))) {
        LOG_WARN("failed to push back null value", K(ret));
      }
    } else if (OB_FAIL(arr->push_back(dec_val->int128_v_[0]))) {
      LOG_WARN("failed to push back decimal int128 value", K(ret), K(dec_val->int128_v_[0]));
    }
  } else if (get_decimalint_type(prec) == DECIMAL_INT_256) {
    ObArrayFixedSize<int256_t> *arr = static_cast<ObArrayFixedSize<int256_t> *>(arr_obj);
    if (is_null) {
      if (OB_FAIL(arr->push_back(0, true))) {
        LOG_WARN("failed to push back null value", K(ret));
      }
    } else if (OB_FAIL(arr->push_back(dec_val->int256_v_[0]))) {
      LOG_WARN("failed to push back decimal int256 value", K(ret), K(dec_val->int256_v_[0]));
    }
  } else if (get_decimalint_type(prec) == DECIMAL_INT_512) {
    ObArrayFixedSize<int512_t> *arr = static_cast<ObArrayFixedSize<int512_t> *>(arr_obj);
    if (is_null) {
      if (OB_FAIL(arr->push_back(0, true))) {
        LOG_WARN("failed to push back null value", K(ret));
      }
    } else if (OB_FAIL(arr->push_back(dec_val->int512_v_[0]))) {
      LOG_WARN("failed to push back decimal int512 value", K(ret), K(dec_val->int512_v_[0]));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "unexpected precision", K(ret), K(prec));
  }
  return ret;
}

// convert collection bin to string (for liboblog)
int ObArrayUtil::convert_collection_bin_to_string(const ObString &collection_bin,
                                                  const common::ObIArray<common::ObString> &extended_type_info,
                                                  common::ObIAllocator &allocator,
                                                  ObString &res_str)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(extended_type_info.count() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid extended type info for collection type", K(ret), K(extended_type_info.count()));
  } else {
    ObSqlCollectionInfo type_info_parse(allocator);
    ObString collection_type_name = extended_type_info.at(0);
    type_info_parse.set_name(collection_type_name);
    if (OB_FAIL(type_info_parse.parse_type_info())) {
      LOG_WARN("fail to parse type info", K(ret), K(collection_type_name));
    } else if (OB_ISNULL(type_info_parse.collection_meta_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("collection meta is null", K(ret), K(collection_type_name));
    } else {
      ObCollectionArrayType *arr_type = nullptr;
      ObIArrayType *arr_obj = nullptr;
      ObStringBuffer buf(&allocator);
      if (OB_ISNULL(arr_type = static_cast<ObCollectionArrayType *>(type_info_parse.collection_meta_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("collection meta is null", K(ret), K(collection_type_name));
      } else if (OB_FAIL(ObArrayTypeObjFactory::construct(allocator, *arr_type, arr_obj, true))) {
        LOG_WARN("construct array obj failed", K(ret),  K(type_info_parse));
      } else if (OB_ISNULL(arr_obj)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("arr_obj is null", K(ret), K(collection_type_name));
      } else {
        ObString raw_binary = collection_bin;
        if (OB_FAIL(arr_obj->init(raw_binary))) {
          LOG_WARN("failed to init array", K(ret));
        } else if (OB_FAIL(arr_obj->print(buf))) {
          LOG_WARN("failed to format array", K(ret));
        } else {
          res_str.assign_ptr(buf.ptr(), buf.length());
        }
      }
    }
  }
  return ret;
}

// determine a collection type is vector or array
int ObArrayUtil::get_mysql_type(const common::ObIArray<common::ObString> &extended_type_info,
                                obmysql::EMySQLFieldType &type)
{
  int ret = OB_SUCCESS;
  type = obmysql::MYSQL_TYPE_NOT_DEFINED;
  ObArenaAllocator tmp_allocator("OB_ARRAY_UTIL");
  if (OB_UNLIKELY(extended_type_info.count() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid extended type info for collection type", K(ret), K(extended_type_info.count()));
  } else {
    ObSqlCollectionInfo type_info_parse(tmp_allocator);
    ObString collection_type_name = extended_type_info.at(0);
    type_info_parse.set_name(collection_type_name);
    if (OB_FAIL(type_info_parse.parse_type_info())) {
      LOG_WARN("fail to parse type info", K(ret), K(collection_type_name));
    } else if (OB_ISNULL(type_info_parse.collection_meta_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("collection meta is null", K(ret), K(collection_type_name));
    } else {
      uint16_t detail_type = type_info_parse.collection_meta_->type_id_;
      if (detail_type == OB_ARRAY_TYPE) {
        type = obmysql::MYSQL_TYPE_OB_ARRAY;
      } else if (detail_type == OB_VECTOR_TYPE) {
        type = obmysql::MYSQL_TYPE_OB_VECTOR;
      } else {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "unexpected collection type", K(ret), K(detail_type));
      }
    }
  }
  tmp_allocator.reset();
  return ret;
}

#define FIXED_SIZE_ARRAY_APPEND(Element_Type, Get_Func)                                               \
  ObArrayFixedSize<Element_Type> *array_obj = static_cast<ObArrayFixedSize<Element_Type> *>(&array);  \
  if (OB_FAIL(array_obj->push_back(datum->Get_Func()))) {                                             \
    LOG_WARN("failed to push back value", K(ret));                                                    \
  }

int ObArrayUtil::append(ObIArrayType &array, const ObObjType elem_type, const ObDatum *datum)
{
  int ret = OB_SUCCESS;
  if (datum->is_null()) {
    if (OB_FAIL(array.push_null())) {
      LOG_WARN("failed to push back null value", K(ret));
    }
  } else {
    switch (elem_type) {
      case ObNullType: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expect null value", K(ret));
        break;
      }
      case ObTinyIntType: {
        FIXED_SIZE_ARRAY_APPEND(int8_t, get_tinyint);
        break;
      }
      case ObSmallIntType: {
        FIXED_SIZE_ARRAY_APPEND(int16_t, get_smallint);
        break;
      }
      case ObInt32Type: {
        FIXED_SIZE_ARRAY_APPEND(int32_t, get_int32);
        break;
      }
      case ObIntType: {
        FIXED_SIZE_ARRAY_APPEND(int64_t, get_int);
        break;
      }
      case ObUTinyIntType: {
        FIXED_SIZE_ARRAY_APPEND(uint8_t, get_utinyint);
        break;
      }
      case ObUSmallIntType: {
        FIXED_SIZE_ARRAY_APPEND(uint16_t, get_usmallint);
        break;
      }
      case ObUInt32Type: {
        FIXED_SIZE_ARRAY_APPEND(uint32_t, get_uint32);
        break;
      }
      case ObUInt64Type: {
        FIXED_SIZE_ARRAY_APPEND(uint64_t, get_uint64);
        break;
      }
      case ObUFloatType:
      case ObFloatType: {
        FIXED_SIZE_ARRAY_APPEND(float, get_float);
        break;
      }
      case ObUDoubleType:
      case ObDoubleType: {
        FIXED_SIZE_ARRAY_APPEND(double, get_double);
        break;
      }
      case ObVarcharType: {
        ObArrayBinary *binary_array = static_cast<ObArrayBinary *>(&array);
        if (OB_FAIL(binary_array->push_back(datum->get_string()))) {
          LOG_WARN("failed to push back null value", K(ret));
        }
        break;
      }
      default:
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported element type", K(ret), K(elem_type));
    }
  }
  return ret;
}

} // namespace common
} // namespace oceanbase