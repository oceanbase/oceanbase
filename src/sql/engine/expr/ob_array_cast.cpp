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

#include "ob_array_cast.h"
#include "lib/json_type/ob_json_parse.h"
#include <fast_float/fast_float.h>
#include "src/share/object/ob_obj_cast_util.h"

namespace oceanbase {
namespace sql {

int ObVectorDataCast::cast(common::ObIAllocator &alloc, ObIArrayType *src, const ObCollectionTypeBase *elem_type,
                           ObIArrayType *&dst, const ObCollectionTypeBase *dst_elem_type, ObCastMode mode)
{
  int ret = OB_SUCCESS;
  const ObCollectionBasicType *src_type = dynamic_cast<const ObCollectionBasicType *>(elem_type);
  const ObCollectionBasicType *dst_type  = dynamic_cast<const ObCollectionBasicType *>(dst_elem_type);
  if (OB_UNLIKELY(!src_type || !dst_type)) {
    ret = OB_ERR_ARRAY_TYPE_MISMATCH;
    LOG_WARN("unexpected status: invalid argument", K(ret), KP(src_type), KP(dst_type));
  } else if (dim_cnt_ != src->size()) {
    ret = OB_ERR_INVALID_VECTOR_DIM;
    LOG_WARN("invalid array size", K(ret), K(dim_cnt_), K(src->size()));
  }
  for (int64_t i = 0; i < src->size() && OB_SUCC(ret); i++) {
    ObObj src_elem;
    if (src->is_null(i)) {
      if (OB_FAIL(dst->push_null())) {
        LOG_WARN("failed to add null to array", K(ret), K(i));
      }
    } else if (OB_FAIL(ObArrayCastUtils::cast_get_element(src, src_type, i, src_elem))) {
      LOG_WARN("failed to get cast element", K(ret), K(i));
    } else {
      ObObjType dst_obj_type = dst_type->basic_meta_.get_obj_type();
      ObObj res;
      ObCastCtx cast_ctx(&alloc, NULL, mode, ObCharset::get_system_collation());
      if (OB_FAIL(ObObjCaster::to_type(dst_obj_type, cast_ctx, src_elem, res))) {
        LOG_WARN("failed to cast number to double type", K(ret));
      } else if (dst_obj_type == ObFloatType) {
        ObVectorF32Data *dst_arr = static_cast<ObVectorF32Data *>(dst);
        if (OB_FAIL(dst_arr->push_back(res.get_float()))) {
          LOG_WARN("failed to push back array value", K(ret));
        }
      } else if (dst_obj_type == ObUTinyIntType) {
        ObVectorU8Data *dst_arr = static_cast<ObVectorU8Data *>(dst);
        if (OB_FAIL(dst_arr->push_back(res.get_utinyint()))) {
          LOG_WARN("failed to push back array value", K(ret));
        }
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("dest obj type of vector is not supported", K(ret), K(dst_obj_type));
      }
    }
  }
  return ret;
}

int ObArrayFixedSizeCast::cast(common::ObIAllocator &alloc, ObIArrayType *src, const ObCollectionTypeBase *elem_type,
                               ObIArrayType *&dst, const ObCollectionTypeBase *dst_elem_type, ObCastMode mode)
{
  int ret = OB_SUCCESS;
  const ObCollectionBasicType *src_type = dynamic_cast<const ObCollectionBasicType *>(elem_type);
  const ObCollectionBasicType *dst_type  = dynamic_cast<const ObCollectionBasicType *>(dst_elem_type);
  if (OB_UNLIKELY(!src_type || !dst_type)) {
    ret = OB_ERR_ARRAY_TYPE_MISMATCH;
    LOG_WARN("unexpected status: invalid argument", K(ret), KP(src_type), KP(dst_type));
  }
  for (int64_t i = 0; i < src->size() && OB_SUCC(ret); i++) {
    ObObj src_elem;
    if (src->is_null(i)) {
      if (OB_FAIL(dst->push_null())) {
        LOG_WARN("failed to add null to array", K(ret), K(i));
      }
    } else if (OB_FAIL(ObArrayCastUtils::cast_get_element(src, src_type, i, src_elem))) {
      LOG_WARN("failed to get cast element", K(ret), K(i));
    } else if (OB_FAIL(ObArrayCastUtils::cast_add_element(alloc, src_elem, dst, dst_type, mode))) {
      LOG_WARN("failed to cast and add element", K(ret));
    }
  }
  return ret;
}

int ObArrayCastUtils::cast_get_element(ObIArrayType *src, const ObCollectionBasicType *elem_type, uint32_t idx, ObObj &src_elem)
{
  int ret = OB_SUCCESS;
  ObObjType obj_type = elem_type->basic_meta_.get_obj_type();
  switch (obj_type) {
    case ObTinyIntType: {
      ObArrayFixedSize<int8_t> *arr = static_cast<ObArrayFixedSize<int8_t> *>(src);
      src_elem.set_tinyint((*arr)[idx]);
      break;
    }
    case ObSmallIntType: {
      ObArrayFixedSize<int16_t> *arr = static_cast<ObArrayFixedSize<int16_t> *>(src);
      src_elem.set_smallint((*arr)[idx]);
      break;
    }
    case ObIntType: {
      ObArrayFixedSize<int64_t> *arr = static_cast<ObArrayFixedSize<int64_t> *>(src);
      src_elem.set_int((*arr)[idx]);
      break;
    }
    case ObInt32Type: {
      ObArrayFixedSize<int32_t> *arr = static_cast<ObArrayFixedSize<int32_t> *>(src);
      src_elem.set_int32((*arr)[idx]);
      break;
    }
    case ObUTinyIntType: {
      ObArrayFixedSize<uint8_t> *arr = static_cast<ObArrayFixedSize<uint8_t> *>(src);
      src_elem.set_utinyint((*arr)[idx]);
      break;
    }
    case ObUSmallIntType: {
      ObArrayFixedSize<uint16_t> *arr = static_cast<ObArrayFixedSize<uint16_t> *>(src);
      src_elem.set_usmallint((*arr)[idx]);
      break;
    }
    case ObUInt64Type: {
      ObArrayFixedSize<uint64_t> *arr = static_cast<ObArrayFixedSize<uint64_t> *>(src);
      src_elem.set_uint64((*arr)[idx]);
      break;
    }
    case ObUInt32Type: {
      ObArrayFixedSize<uint32_t> *arr = static_cast<ObArrayFixedSize<uint32_t> *>(src);
      src_elem.set_uint32((*arr)[idx]);
      break;
    }
    case ObDecimalIntType: {
      ObPrecision prec = elem_type->basic_meta_.get_precision();
      if (get_decimalint_type(prec) == DECIMAL_INT_32) {
        ObArrayFixedSize<int32_t> *arr = static_cast<ObArrayFixedSize<int32_t> *>(src);
        src_elem.set_decimal_int(sizeof(int32_t), arr->get_scale(), arr->get_decimal_int(idx));
      } else if (get_decimalint_type(prec) == DECIMAL_INT_64) {
        ObArrayFixedSize<int64_t> *arr = static_cast<ObArrayFixedSize<int64_t> *>(src);
        src_elem.set_decimal_int(sizeof(int64_t), arr->get_scale(), arr->get_decimal_int(idx));
      } else if (get_decimalint_type(prec) == DECIMAL_INT_128) {
        ObArrayFixedSize<int128_t> *arr = static_cast<ObArrayFixedSize<int128_t> *>(src);
        src_elem.set_decimal_int(sizeof(int128_t), arr->get_scale(), arr->get_decimal_int(idx));
      } else if (get_decimalint_type(prec) == DECIMAL_INT_256) {
        ObArrayFixedSize<int256_t> *arr = static_cast<ObArrayFixedSize<int256_t> *>(src);
        src_elem.set_decimal_int(sizeof(int256_t), arr->get_scale(), arr->get_decimal_int(idx));
      } else if (get_decimalint_type(prec) == DECIMAL_INT_512) {
        ObArrayFixedSize<int512_t> *arr = static_cast<ObArrayFixedSize<int512_t> *>(src);
        src_elem.set_decimal_int(sizeof(int512_t), arr->get_scale(), arr->get_decimal_int(idx));
      } else {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "unexpected precision", K(ret), K(prec));
      }
      break;
    }
    case ObVarcharType : {
      ObArrayBinary *arr = static_cast<ObArrayBinary *>(src);
      src_elem.set_varchar((*arr)[idx]);
      break;
    }
    case ObUDoubleType:
    case ObDoubleType: {
      ObArrayFixedSize<double> *arr = static_cast<ObArrayFixedSize<double> *>(src);
      src_elem.set_double((*arr)[idx]);
      break;
    }
    case ObUFloatType:
    case ObFloatType: {
      ObArrayFixedSize<float> *arr = static_cast<ObArrayFixedSize<float> *>(src);
      src_elem.set_float((*arr)[idx]);
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "unexpected element type", K(ret), K(elem_type->basic_meta_.get_obj_type()));
    }
  }
  return ret;
}

int ObArrayCastUtils::cast_add_element(common::ObIAllocator &alloc, ObObj &src_elem,
                                       ObIArrayType *dst, const ObCollectionBasicType *dst_elem_type, ObCastMode mode)
{
  int ret = OB_SUCCESS;
  ObCastCtx cast_ctx(&alloc, NULL, mode, ObCharset::get_system_collation());
  ObObjType dst_obj_type = dst_elem_type->basic_meta_.get_obj_type();
  ObObj res;
  ObAccuracy out_acc = dst_elem_type->basic_meta_.get_accuracy();
  const ObCollationType cs_type = dst_elem_type->basic_meta_.meta_.get_collation_type();
  ObObj buf_obj;
  const ObObj *res_obj = &src_elem;
  if (dst_obj_type == ObVarcharType &&
      OB_FAIL(obj_accuracy_check(cast_ctx, out_acc, cs_type, src_elem, buf_obj, res_obj))) {
    LOG_WARN("varchar type length is too long", K(ret), K(src_elem.get_string_len()));
  } else if (OB_FAIL(ObObjCaster::to_type(dst_obj_type, cast_ctx, src_elem, res))) {
    LOG_WARN("failed to cast number to double type", K(ret));
  } else {
    switch (dst_obj_type) {
      case ObTinyIntType : {
        ObArrayFixedSize<int8_t> *dst_arr = static_cast<ObArrayFixedSize<int8_t> *>(dst);
        if (OB_FAIL(dst_arr->push_back(res.get_tinyint()))) {
          LOG_WARN("failed to push back array value", K(ret));
        }
        break;
      }
      case ObSmallIntType : {
        ObArrayFixedSize<int16_t> *dst_arr = static_cast<ObArrayFixedSize<int16_t> *>(dst);
        if (OB_FAIL(dst_arr->push_back(res.get_smallint()))) {
          LOG_WARN("failed to push back array value", K(ret));
        }
        break;
      }
      case ObIntType : {
        ObArrayFixedSize<int64_t> *dst_arr = static_cast<ObArrayFixedSize<int64_t> *>(dst);
        if (OB_FAIL(dst_arr->push_back(res.get_int()))) {
          LOG_WARN("failed to push back array value", K(ret));
        }
        break;
      }
      case ObInt32Type: {
        ObArrayFixedSize<int32_t> *dst_arr = static_cast<ObArrayFixedSize<int32_t> *>(dst);
        if (OB_FAIL(dst_arr->push_back(res.get_int32()))) {
          LOG_WARN("failed to push back array value", K(ret));
        }
        break;
      }
      case ObUTinyIntType : {
        ObArrayFixedSize<uint8_t> *dst_arr = static_cast<ObArrayFixedSize<uint8_t> *>(dst);
        if (OB_FAIL(dst_arr->push_back(res.get_utinyint()))) {
          LOG_WARN("failed to push back array value", K(ret));
        }
        break;
      }
      case ObUSmallIntType : {
        ObArrayFixedSize<uint16_t> *dst_arr = static_cast<ObArrayFixedSize<uint16_t> *>(dst);
        if (OB_FAIL(dst_arr->push_back(res.get_usmallint()))) {
          LOG_WARN("failed to push back array value", K(ret));
        }
        break;
      }
      case ObUInt64Type : {
        ObArrayFixedSize<uint64_t> *dst_arr = static_cast<ObArrayFixedSize<uint64_t> *>(dst);
        if (OB_FAIL(dst_arr->push_back(res.get_uint64()))) {
          LOG_WARN("failed to push back array value", K(ret));
        }
        break;
      }
      case ObUInt32Type: {
        ObArrayFixedSize<uint32_t> *dst_arr = static_cast<ObArrayFixedSize<uint32_t> *>(dst);
        if (OB_FAIL(dst_arr->push_back(res.get_uint32()))) {
          LOG_WARN("failed to push back array value", K(ret));
        }
        break;
      }
      case ObDecimalIntType: {
        ret = OB_NOT_SUPPORTED;
        // to do
        break;
      }
      case ObUFloatType:
      case ObFloatType: {
        ObArrayFixedSize<float> *dst_arr = static_cast<ObArrayFixedSize<float> *>(dst);
        if (OB_FAIL(dst_arr->push_back(res.get_float()))) {
          LOG_WARN("failed to push back array value", K(ret));
        }
        break;
      }
      case ObUDoubleType:
      case ObDoubleType: {
        ObArrayFixedSize<double> *dst_arr = static_cast<ObArrayFixedSize<double> *>(dst);
        if (OB_FAIL(dst_arr->push_back(res.get_double()))) {
          LOG_WARN("failed to push back array value", K(ret));
        }
        break;
      }
      case ObVarcharType: {
        ObArrayBinary *dst_arr = static_cast<ObArrayBinary *>(dst);
        if (OB_FAIL(dst_arr->push_back(res.get_varchar()))) {
          LOG_WARN("failed to push back array value", K(ret));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "unexpected element type", K(ret), K(dst_obj_type));
      }
    }
  }
  return ret;
}

#define ADD_SIGNED_FIXED_ARRAY_OBJ(Element_Type)                                                \
  int64_t val;                                                                                  \
  ObArrayFixedSize<Element_Type> *dst_arr = static_cast<ObArrayFixedSize<Element_Type> *>(dst); \
  if (OB_FAIL(j_node.to_int(val))) {                                                            \
    LOG_WARN("failed to push back array value", K(ret));                                        \
  } else if (OB_FAIL(int_range_check(dst_obj_type, val, val))) {                                \
    LOG_WARN("failed to check value range", K(ret), K(val));                                    \
  } else if (OB_FAIL(dst_arr->push_back(static_cast<Element_Type>(val)))) {                     \
    LOG_WARN("failed to push back array value", K(ret));                                        \
  }

#define ADD_UNSIGNED_FIXED_ARRAY_OBJ(Element_Type)                                              \
  uint64_t val;                                                                                 \
  ObArrayFixedSize<Element_Type> *dst_arr = static_cast<ObArrayFixedSize<Element_Type> *>(dst); \
  if (OB_FAIL(j_node.to_uint(val))) {                                                           \
    LOG_WARN("failed to push back array value", K(ret));                                        \
  } else if (OB_FAIL(uint_range_check(dst_obj_type, val, val))) {                               \
    LOG_WARN("failed to check value range", K(ret), K(val));                                    \
  } else if (OB_FAIL(dst_arr->push_back(static_cast<Element_Type>(val)))) {                     \
    LOG_WARN("failed to push back array value", K(ret));                                        \
  }

int ObArrayCastUtils::add_json_node_to_array(common::ObIAllocator &alloc, ObJsonNode &j_node, const ObCollectionTypeBase *elem_type, ObIArrayType *dst)
{
  int ret = OB_SUCCESS;
  if (j_node.json_type() == ObJsonNodeType::J_NULL) {
    if (OB_FAIL(dst->push_null())) {
      LOG_WARN("failed to push null array value", K(ret));
    }
  } else if (j_node.json_type() == ObJsonNodeType::J_ARRAY) {
    const ObCollectionArrayType *array_type = dynamic_cast<const ObCollectionArrayType *>(elem_type);
    ObIArrayType *child_array = nullptr;
    ObJsonArray *json_arr = static_cast<ObJsonArray *>(&j_node);
    if (OB_ISNULL(array_type)) {
      ret = OB_ERR_ARRAY_TYPE_MISMATCH;
      LOG_WARN("unexpected element type", K(ret), K(elem_type->type_id_));
    } else if (OB_FAIL(ObArrayTypeObjFactory::construct(alloc, *array_type, child_array))) {
      LOG_WARN("failed to add null to array", K(ret));
    }
    for (int i = 0; i < json_arr->element_count() && OB_SUCC(ret); i++) {
      if (OB_FAIL(add_json_node_to_array(alloc, *(*json_arr)[i], array_type->element_type_, child_array))) {
        LOG_WARN("failed to add json node to array", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret)) {
      ObArrayNested *nested_arr = static_cast<ObArrayNested *>(dst);
      if (OB_FAIL(child_array->init())) {
        LOG_WARN("child array init failed", K(ret));
      } else if (OB_FAIL(nested_arr->push_back(*child_array))) {
        LOG_WARN("failed to push back array value", K(ret));
      }
    }
  } else {
    // basic type
    const ObCollectionBasicType *basic_type = dynamic_cast<const ObCollectionBasicType *>(elem_type);
    if (OB_ISNULL(basic_type)) {
      ret = OB_ERR_ARRAY_TYPE_MISMATCH;
      LOG_WARN("unexpected element type", K(ret), K(elem_type->type_id_));
    } else {
      ObObjType dst_obj_type = basic_type->basic_meta_.get_obj_type();
      switch (dst_obj_type) {
        case ObTinyIntType: {
          ADD_SIGNED_FIXED_ARRAY_OBJ(int8_t);
          break;
        }
        case ObSmallIntType: {
          ADD_SIGNED_FIXED_ARRAY_OBJ(int16_t);
          break;
        }
        case ObInt32Type: {
          ADD_SIGNED_FIXED_ARRAY_OBJ(int32_t);
          break;
        }
        case ObIntType: {
          ADD_SIGNED_FIXED_ARRAY_OBJ(int64_t);
          break;
        }
        case ObUTinyIntType: {
          ADD_UNSIGNED_FIXED_ARRAY_OBJ(uint8_t);
          break;
        }
        case ObUSmallIntType: {
          ADD_UNSIGNED_FIXED_ARRAY_OBJ(uint16_t);
          break;
        }
        case ObUInt32Type: {
          ADD_UNSIGNED_FIXED_ARRAY_OBJ(uint32_t);
          break;
        }
        case ObUInt64Type: {
          ADD_SIGNED_FIXED_ARRAY_OBJ(int64_t);
          break;
        }
        case ObDecimalIntType: {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not supported", K(ret));
          break;
        }
        case ObUFloatType:
        case ObFloatType: {
          double val;
          ObArrayFixedSize<float> *dst_arr = static_cast<ObArrayFixedSize<float> *>(dst);
          if (OB_FAIL(j_node.to_double(val))) {
            LOG_WARN("failed to push back array value", K(ret));
          } else if (OB_FAIL(real_range_check(dst_obj_type, val, val))) {
            LOG_WARN("failed to check value range", K(ret));
          } else if (OB_FAIL(dst_arr->push_back(static_cast<float>(val)))) {
            LOG_WARN("failed to push back array value", K(ret));
          }
          break;
        }
        case ObUDoubleType:
        case ObDoubleType: {
          double val;
          ObArrayFixedSize<double> *dst_arr = static_cast<ObArrayFixedSize<double> *>(dst);
          if (OB_FAIL(j_node.to_double(val))) {
            LOG_WARN("failed to push back array value", K(ret));
          } else if (OB_FAIL(real_range_check(dst_obj_type, val, val))) {
            LOG_WARN("failed to check value range", K(ret));
          } else if (OB_FAIL(dst_arr->push_back(val))) {
            LOG_WARN("failed to push back array value", K(ret));
          }
          break;
        }
        case ObVarcharType: {
          ObArrayBinary *dst_arr = static_cast<ObArrayBinary *>(dst);
          ObStringBuffer str_buf(&alloc);
          if (OB_FAIL(j_node.print(str_buf, false))) {
            LOG_WARN("failed to push back array value", K(ret));
          } else if (OB_FAIL(dst_arr->push_back(str_buf.string()))) {
            LOG_WARN("failed to push back array value", K(ret));
          }
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          OB_LOG(WARN, "unexpected element type", K(ret), K(dst_obj_type));
        }
      }
    }
  }
  return ret;
}

static inline bool is_whitespace(const char c) {
  return c == ' ' || c == '\t' || c == '\n' || c == '\v' || c == '\f' || c == '\r';
}

static inline bool is_negative(char ch)
{
  return ch == '-';
}

static inline bool is_positive(char ch)
{
  return ch == '+';
}

static inline bool is_vector_start(char ch)
{
  return (ch == '[');
}

static inline bool is_vector_finish(char ch)
{
  return (ch == ']');
}

static inline bool is_null_string_start(char ch)
{
  return (ch == 'N' || ch == 'n');
}

static bool is_null_const_string(const char* begin, const char* end)
{
  bool bool_ret = false;

  if (end - begin > ObArrayCastUtils::NULL_STR_LEN) {
    ObString tmp(ObArrayCastUtils::NULL_STR_LEN, begin);
    bool_ret = tmp.case_compare("null") == 0;
  }
  return bool_ret;
}


#define ADD_SIGNED_VECTOR_OBJ(Element_Type)                                                     \
  ObArrayFixedSize<Element_Type> *dst_arr = static_cast<ObArrayFixedSize<Element_Type> *>(dst); \
  if (OB_FAIL(dst_arr->push_back(static_cast<Element_Type>(value)))) {                          \
    LOG_WARN("failed to push back array value", K(ret));                                        \
  }

#define ADD_UNSIGNED_VECTOR_OBJ(Element_Type)                                                   \
  ObArrayFixedSize<Element_Type> *dst_arr = static_cast<ObArrayFixedSize<Element_Type> *>(dst); \
  if (OB_FAIL(dst_arr->push_back(static_cast<Element_Type>(value)))) {                          \
    LOG_WARN("failed to push back array value", K(ret));                                        \
  }

int ObArrayCastUtils::add_vector_element(const double value, const ObCollectionTypeBase *elem_type, ObIArrayType *dst)
{
  int ret = OB_SUCCESS;
  const ObCollectionBasicType *basic_type = dynamic_cast<const ObCollectionBasicType *>(elem_type);
  if (OB_ISNULL(basic_type)) {
    ret = OB_ERR_ARRAY_TYPE_MISMATCH;
    LOG_WARN("unexpected element type", K(ret), K(elem_type->type_id_));
  } else {
    ObObjType dst_obj_type = basic_type->basic_meta_.get_obj_type();
    switch (dst_obj_type) {
      case ObTinyIntType: {
        ADD_SIGNED_VECTOR_OBJ(int8_t);
        break;
      }
      case ObSmallIntType: {
        ADD_SIGNED_VECTOR_OBJ(int16_t);
        break;
      }
      case ObInt32Type: {
        ADD_SIGNED_VECTOR_OBJ(int32_t);
        break;
      }
      case ObIntType: {
        ADD_SIGNED_VECTOR_OBJ(int64_t);
        break;
      }
      case ObUTinyIntType: {
        ADD_UNSIGNED_VECTOR_OBJ(uint8_t);
        break;
      }
      case ObUSmallIntType: {
        ADD_UNSIGNED_VECTOR_OBJ(uint16_t);
        break;
      }
      case ObUInt32Type: {
        ADD_UNSIGNED_VECTOR_OBJ(uint32_t);
        break;
      }
      case ObUInt64Type: {
        ADD_SIGNED_VECTOR_OBJ(int64_t);
        break;
      }
      case ObDecimalIntType: {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not supported", K(ret));
        break;
      }
      case ObFloatType: {
        double val = value;
        ObArrayFixedSize<float> *dst_arr = static_cast<ObArrayFixedSize<float> *>(dst);
        if (OB_FAIL(dst_arr->push_back(static_cast<float>(val)))) {
          LOG_WARN("failed to push back array value", K(ret));
        }
        break;
      }
      case ObDoubleType: {
        double val = value;
        ObArrayFixedSize<double> *dst_arr = static_cast<ObArrayFixedSize<double> *>(dst);
        if (OB_FAIL(dst_arr->push_back(val))) {
          LOG_WARN("failed to push back array value", K(ret));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "unexpected element type", K(ret), K(dst_obj_type));
      }
    }
  }

  return  ret;
}

static inline void skip_whitespace(const char*& start, const char* end)
{
  while (start < end && is_whitespace(*start)) {
    ++start;
  }
}

static inline void skip_whitespace_and_spliter(const char*& start, const char* end)
{
  while (start < end && (is_whitespace(*start) || *start == ',')) {
    ++start;
  }
}

int ObArrayCastUtils::string_cast_vector(common::ObIAllocator &alloc, ObString &arr_text, ObIArrayType *&dst, const ObCollectionTypeBase *dst_elem_type)
{
  int ret = OB_SUCCESS;

  const char* begin = arr_text.ptr();
  const char* ptr = begin;
  const char* end = ptr + arr_text.length();
  bool is_end_char = false;

  if (!arr_text.empty()) {
    skip_whitespace(ptr, end);

    if (ptr >= end) {
    } else if (!is_vector_start(*ptr)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("failed to parse array text, No begin char found", K(ret), K(arr_text), K(ptr - begin));
    } else {
      ++ptr;
      skip_whitespace(ptr, end);

      for (; OB_SUCC(ret) && ptr < end; ) {
        double res = 0.0;

        bool is_neg_flag = is_negative(*ptr);
        if (is_neg_flag || is_positive(*ptr)) {
          ++ptr;
        }

        fast_float::from_chars_result parse_ret = fast_float::from_chars(ptr, end, res);
        if (OB_UNLIKELY(parse_ret.ec != std::errc())) {
          if (is_null_string_start(*ptr)) {
            if (!is_null_const_string(ptr, end)) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("failed to parse array", K(ret), K(arr_text), K(ptr - begin));
            } else {
              ptr += NULL_STR_LEN;
              if (OB_FAIL(dst->push_null())) {
                LOG_WARN("failed to push null array value", K(ret));
              }
            }
          } else {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("failed to parse array", K(ret), K(arr_text), K(ptr - begin));
          }
        } else {
          ptr = parse_ret.ptr;
          if (is_neg_flag) {
            res *= -1;
          }

          if (OB_FAIL(add_vector_element(res, dst_elem_type, dst))) {
            LOG_WARN("failed to push store array", K(ret));
          }
        }

        if (OB_FAIL(ret)) {
        } else if (is_vector_finish(*ptr)) {
          ++ptr;
          is_end_char = true;
          break;
        } else {

          skip_whitespace_and_spliter(ptr, end);

          if (ptr < end && is_vector_finish(*ptr)) {
            ++ptr;
            is_end_char = true;
            break;
          }
        }
      }

      if (OB_SUCC(ret)) {
        if (is_end_char) {
          skip_whitespace(ptr, end);
          if (ptr < end && *ptr != 0) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("failed to parse array", K(ret), K(arr_text), K(ptr - begin));
          }
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("failed to parse array", K(ret), K(arr_text), K(ptr - begin));
        }
      }
    }
  }

  return ret;
}

int ObArrayCastUtils::string_cast(common::ObIAllocator &alloc, ObString &arr_text,
                                  ObIArrayType *&dst, const ObCollectionTypeBase *dst_elem_type)
{
  int ret = OB_SUCCESS;
  const char *syntaxerr = NULL;
  uint64_t err_offset = 0;
  ObJsonNode *j_node = NULL;
  uint32_t parse_flag = ObJsonParser::JSN_RELAXED_FLAG;
  if (OB_FAIL(
          ObJsonParser::parse_json_text(&alloc, arr_text.ptr(), arr_text.length(), syntaxerr, &err_offset, j_node, parse_flag))) {
    LOG_WARN("failed to parse array text", K(ret), K(arr_text), KCSTRING(syntaxerr), K(err_offset));
  } else if (j_node->json_type() != ObJsonNodeType::J_ARRAY) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid text. not json type", K(ret), K(arr_text), K(j_node->json_type()));
  } else {
    for (int i = 0; i < j_node->element_count() && OB_SUCC(ret); i++) {
      ObJsonArray *json_arr = static_cast<ObJsonArray *>(j_node);
      if (OB_FAIL(add_json_node_to_array(alloc, *(*json_arr)[i], dst_elem_type, dst))) {
        LOG_WARN("failed to add json node to array", K(ret), K(i));
      }
    }
  }

  return ret;
}

int ObArrayBinaryCast::cast(common::ObIAllocator &alloc, ObIArrayType *src, const ObCollectionTypeBase *elem_type,
                            ObIArrayType *&dst, const ObCollectionTypeBase *dst_elem_type, ObCastMode mode)
{
  int ret = OB_SUCCESS;
  const ObCollectionBasicType *src_type = dynamic_cast<const ObCollectionBasicType *>(elem_type);
  const ObCollectionBasicType *dst_type  = dynamic_cast<const ObCollectionBasicType *>(dst_elem_type);
   if (OB_UNLIKELY(!src_type || !dst_type)) {
    ret = OB_ERR_ARRAY_TYPE_MISMATCH;
    LOG_WARN("unexpected status: invalid argument", K(ret), KP(src_type), KP(dst_type));
  } else {
    ObLength elem_len_max = dst_type->basic_meta_.get_length();
    ObCollationType elem_cs_type = src_type->basic_meta_.get_collation_type();
    ObCollationLevel elem_ncl_type = src_type->basic_meta_.get_collation_level();
    for (int64_t i = 0; i < src->size() && OB_SUCC(ret); i++) {
      ObObj src_elem;
      if (src->is_null(i)) {
        if (OB_FAIL(dst->push_null())) {
          LOG_WARN("failed to add null to array", K(ret), K(i));
        }
      } else if (OB_FAIL(ObArrayCastUtils::cast_get_element(src, src_type, i, src_elem))) {
        LOG_WARN("failed to get cast element", K(ret), K(i));
      } else if (FALSE_IT(src_elem.set_collation_type(elem_cs_type))) {
      } else if (FALSE_IT(src_elem.set_collation_level(elem_ncl_type))) {
      } else if (OB_FAIL(ObArrayCastUtils::cast_add_element(alloc, src_elem, dst, dst_type, mode))) {
        LOG_WARN("failed to cast and add element", K(ret));
      }
    }
  }
  return ret;
}

int ObArrayNestedCast::cast(common::ObIAllocator &alloc, ObIArrayType *src, const ObCollectionTypeBase *elem_type,
                            ObIArrayType *&dst, const ObCollectionTypeBase *dst_elem_type, ObCastMode mode)
{
  int ret = OB_SUCCESS;
  const ObCollectionArrayType *src_type = dynamic_cast<const ObCollectionArrayType *>(elem_type);
  const ObCollectionArrayType *dst_type  = dynamic_cast<const ObCollectionArrayType *>(dst_elem_type);
  ObArrayNested *dst_arr = dynamic_cast<ObArrayNested *>(dst);
   if (OB_UNLIKELY(!src_type || !dst_type || !dst_arr)) {
    ret = OB_ERR_ARRAY_TYPE_MISMATCH;
    LOG_WARN("unexpected status: invalid argument", K(ret), KP(src_type), KP(dst_type), KP(dst_arr));
  } else {
    ObIArrayType *src_elem = nullptr;
    ObIArrayType *dst_elem = nullptr;
    ObArrayTypeCast *arr_cast = nullptr;
    if (OB_FAIL(ObArrayTypeObjFactory::construct(alloc, *src_type, src_elem))) {
      LOG_WARN("failed to add null to array", K(ret));
    } else if (OB_FAIL(ObArrayTypeObjFactory::construct(alloc, *dst_type, dst_elem))) {
      LOG_WARN("failed to add null to array", K(ret));
    } else if (OB_FAIL(ObArrayTypeCastFactory::alloc(alloc, *src_type, *dst_type, arr_cast))) {
      LOG_WARN("alloc array cast failed", K(ret));
    }
    for (int64_t i = 0; i < src->size() && OB_SUCC(ret); i++) {
      if (src->is_null(i)) {
        if (OB_FAIL(dst->push_null())) {
          LOG_WARN("failed to add null to array", K(ret), K(i));
        }
      } else if (OB_FAIL(src->at(i, *src_elem))) {
        LOG_WARN("failed to get elem", K(ret), K(i));
      } else if (OB_FAIL(arr_cast->cast(alloc, src_elem, src_type->element_type_, dst_elem, dst_type->element_type_))) {
        LOG_WARN("array element cast failed", K(ret));
      } else if (OB_FAIL(dst_elem->init())) {
        LOG_WARN("array init failed", K(ret));
      } else if (OB_FAIL(dst_arr->push_back(*dst_elem))) {
        LOG_WARN("array push back failed", K(ret));
      } else {
        src_elem->clear();
        dst_elem->clear();
      }
    }
  }

  return ret;
}

int ObArrayTypeCastFactory::alloc(ObIAllocator &alloc, const ObCollectionTypeBase &src_array_meta,
                                  const ObCollectionTypeBase &dst_array_meta, ObArrayTypeCast *&arr_cast)
{
  int ret = OB_SUCCESS;
  UNUSED(src_array_meta);
  const ObCollectionArrayType *arr_type = dynamic_cast<const ObCollectionArrayType *>(&dst_array_meta);
  if (arr_type->element_type_->type_id_ == ObNestedType::OB_BASIC_TYPE) {
    ObCollectionBasicType *elem_type = static_cast<ObCollectionBasicType *>(arr_type->element_type_);
    if (ob_is_string_tc(elem_type->basic_meta_.get_obj_type())
        && ObCharType != elem_type->basic_meta_.get_obj_type()) {
      arr_cast = OB_NEWx(ObArrayBinaryCast, &alloc);
    } else if (arr_type->type_id_ == ObNestedType::OB_VECTOR_TYPE) {
      arr_cast = OB_NEWx(ObVectorDataCast, &alloc);
      static_cast<ObVectorDataCast *>(arr_cast)->dim_cnt_ = arr_type->dim_cnt_;
    } else {
      arr_cast = OB_NEWx(ObArrayFixedSizeCast, &alloc);
    }
  } else if (arr_type->element_type_->type_id_ == ObNestedType::OB_ARRAY_TYPE
             || arr_type->element_type_->type_id_ == ObNestedType::OB_VECTOR_TYPE) {
    arr_cast = OB_NEWx(ObArrayNestedCast, &alloc);
  } else {
    // to do
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported cast type", K(ret), K(arr_type->element_type_->type_id_));
  }
  if (OB_SUCC(ret) && OB_ISNULL(arr_cast)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "alloc memory failed", K(ret));
  }

  return ret;
}

} // namespace sql
} // namespace oceanbase