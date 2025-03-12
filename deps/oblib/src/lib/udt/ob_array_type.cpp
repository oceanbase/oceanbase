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
#include "ob_array_type.h"
#include "ob_array_fixed_size.h"
#include "ob_array_binary.h"
#include "ob_array_nested.h"
#include "ob_vector_type.h"
#include "lib/ob_errno.h"

namespace oceanbase {
namespace common {

#define CONSTRUCT_FIXED_ARRAY_OBJ(Element_Type)                                              \
  void *buf = alloc.alloc(sizeof(ObArrayFixedSize<Element_Type>));                           \
  if (OB_ISNULL(buf)) {                                                                      \
    ret = OB_ALLOCATE_MEMORY_FAILED;                                                         \
    OB_LOG(WARN, "alloc memory failed", K(ret), K(array_meta.type_id_));                     \
  } else {                                                                                   \
    ObArrayFixedSize<Element_Type> *arr_ptr = new (buf) ObArrayFixedSize<Element_Type>();    \
    if (read_only) {                                                                         \
    } else if (OB_ISNULL(buf = alloc.alloc(sizeof(ObArrayData<Element_Type>)))) {            \
      ret = OB_ALLOCATE_MEMORY_FAILED;                                                       \
      OB_LOG(WARN, "alloc memory failed", K(ret), K(array_meta.type_id_));                   \
    } else {                                                                                 \
      ObArrayData<Element_Type> *arr_data = new (buf) ObArrayData<Element_Type>(alloc);      \
      arr_ptr->set_array_data(arr_data);                                                     \
    }                                                                                        \
    if (OB_SUCC(ret)) {                                                                      \
      arr_obj = arr_ptr;                                                                     \
    }                                                                                        \
  }

#define CONSTRUCT_ARRAY_OBJ(Array_Type, Element_Type)                                        \
  void *buf = alloc.alloc(sizeof(Array_Type));                                               \
  if (OB_ISNULL(buf)) {                                                                      \
    ret = OB_ALLOCATE_MEMORY_FAILED;                                                         \
    OB_LOG(WARN, "alloc memory failed", K(ret), K(array_meta.type_id_));                     \
  } else {                                                                                   \
    Array_Type *arr_ptr = new (buf) Array_Type();                                            \
    if (read_only) {                                                                         \
    } else if (OB_ISNULL(buf = alloc.alloc(sizeof(ObArrayData<Element_Type>)))) {            \
      ret = OB_ALLOCATE_MEMORY_FAILED;                                                       \
      OB_LOG(WARN, "alloc memory failed", K(ret), K(array_meta.type_id_));                   \
    } else {                                                                                 \
      ObArrayData<Element_Type> *arr_data = new (buf) ObArrayData<Element_Type>(alloc);      \
      arr_ptr->set_array_data(arr_data);                                                     \
    }                                                                                        \
    if (OB_SUCC(ret)) {                                                                      \
      arr_obj = arr_ptr;                                                                     \
    }                                                                                        \
  }

int ObArrayTypeObjFactory::construct(common::ObIAllocator &alloc, const ObCollectionTypeBase &array_meta,
                                     ObIArrayType *&arr_obj, bool read_only)
{
  int ret = OB_SUCCESS;
  if (array_meta.type_id_ == ObNestedType::OB_ARRAY_TYPE) {
    const ObCollectionArrayType *arr_type = static_cast<const ObCollectionArrayType *>(&array_meta);
    if (arr_type->element_type_->type_id_ == ObNestedType::OB_BASIC_TYPE) {
      ObCollectionBasicType *elem_type = static_cast<ObCollectionBasicType *>(arr_type->element_type_);
      switch (elem_type->basic_meta_.get_obj_type()) {
        case ObNullType: {
          CONSTRUCT_FIXED_ARRAY_OBJ(int8_t);
          break;
        }
        case ObTinyIntType: {
          CONSTRUCT_FIXED_ARRAY_OBJ(int8_t);
          break;
        }
        case ObSmallIntType: {
          CONSTRUCT_FIXED_ARRAY_OBJ(int16_t);
          break;
        }
        case ObInt32Type: {
          CONSTRUCT_FIXED_ARRAY_OBJ(int32_t);
          break;
        }
        case ObIntType: {
          CONSTRUCT_FIXED_ARRAY_OBJ(int64_t);
          break;
        }
        case ObUTinyIntType: {
          CONSTRUCT_FIXED_ARRAY_OBJ(uint8_t);
          break;
        }
        case ObUSmallIntType: {
          CONSTRUCT_FIXED_ARRAY_OBJ(uint16_t);
          break;
        }
        case ObUInt32Type: {
          CONSTRUCT_FIXED_ARRAY_OBJ(uint32_t);
          break;
        }
        case ObUInt64Type: {
          CONSTRUCT_FIXED_ARRAY_OBJ(uint64_t);
          break;
        }
        case ObUFloatType:
        case ObFloatType: {
          CONSTRUCT_FIXED_ARRAY_OBJ(float);
          break;
        }
        case ObUDoubleType:
        case ObDoubleType: {
          CONSTRUCT_FIXED_ARRAY_OBJ(double);
          break;
        }
        case ObDecimalIntType: {
          ObPrecision preci = elem_type->basic_meta_.get_precision();
          if (get_decimalint_type(preci) == DECIMAL_INT_32) {
            CONSTRUCT_FIXED_ARRAY_OBJ(int32_t);
          } else if (get_decimalint_type(preci) == DECIMAL_INT_64) {
            CONSTRUCT_FIXED_ARRAY_OBJ(int64_t);
          } else if (get_decimalint_type(preci) == DECIMAL_INT_128) {
            CONSTRUCT_FIXED_ARRAY_OBJ(int128_t);
          } else if (get_decimalint_type(preci) == DECIMAL_INT_256) {
            CONSTRUCT_FIXED_ARRAY_OBJ(int256_t);
          } else if (get_decimalint_type(preci) == DECIMAL_INT_512) {
            CONSTRUCT_FIXED_ARRAY_OBJ(int512_t);
          } else {
            ret = OB_ERR_UNEXPECTED;
            OB_LOG(WARN, "unexpected precision", K(ret), K(preci));
          }
          if (OB_SUCC(ret)) {
            arr_obj->set_scale(elem_type->basic_meta_.get_scale());
          }
          break;
        }
        case ObVarcharType : {
          CONSTRUCT_ARRAY_OBJ(ObArrayBinary, char);
          break;
        }
        default: {
          ret = OB_NOT_SUPPORTED;
          OB_LOG(WARN, "unsupported type", K(ret), K(elem_type->basic_meta_.get_obj_type()));
        }
      }
      if (OB_SUCC(ret)) {
        arr_obj->set_element_type(static_cast<int32_t>(elem_type->basic_meta_.get_obj_type()));
      }
    } else if (arr_type->element_type_->type_id_ == ObNestedType::OB_ARRAY_TYPE
               || arr_type->element_type_->type_id_ == ObNestedType::OB_VECTOR_TYPE) {
      CONSTRUCT_ARRAY_OBJ(ObArrayNested, char);
      ObIArrayType *arr_child = NULL;
      if (FAILEDx(construct(alloc, *arr_type->element_type_, arr_child, read_only))) {
        OB_LOG(WARN, "failed to construct child element", K(ret), K(array_meta.type_id_));
      } else {
        arr_obj->set_element_type(static_cast<int32_t>(ObCollectionSQLType));
        ObArrayNested *nested_arr = static_cast<ObArrayNested *>(arr_obj);
        nested_arr->set_child_array(arr_child);
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "unexpected collect info type", K(ret), K(arr_type->element_type_->type_id_));
    }
  } else if (array_meta.type_id_ == ObNestedType::OB_VECTOR_TYPE) {
    const ObCollectionArrayType *arr_type = static_cast<const ObCollectionArrayType *>(&array_meta);
    if (arr_type->element_type_->type_id_ != ObNestedType::OB_BASIC_TYPE) {
      ret = OB_NOT_SUPPORTED;
      OB_LOG(WARN, "not supported vector element type", K(ret), K(arr_type->element_type_->type_id_));
    } else {
      ObCollectionBasicType *elem_type = static_cast<ObCollectionBasicType *>(arr_type->element_type_);
      ObObjType obj_type = elem_type->basic_meta_.get_obj_type();
      if (ObUTinyIntType == obj_type) {
        CONSTRUCT_ARRAY_OBJ(ObVectorU8Data, uint8_t);
        if (OB_SUCC(ret)) {
          arr_obj->set_element_type(static_cast<int32_t>(ObUTinyIntType));
        }
      } else if (ObFloatType == obj_type) {
        CONSTRUCT_ARRAY_OBJ(ObVectorF32Data, float);
        if (OB_SUCC(ret)) {
          arr_obj->set_element_type(static_cast<int32_t>(ObFloatType));
        }
      } else {
        ret = OB_NOT_SUPPORTED;
        OB_LOG(WARN, "not supported vector element type", K(ret), K(obj_type));
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "unexpected collect info type", K(ret), K(array_meta.type_id_));
  }
  if (OB_SUCC(ret)) {
    arr_obj->set_array_type(static_cast<const ObCollectionArrayType *>(&array_meta));
  }
  return ret;
}

#undef CONSTRUCT_ARRAY_OBJ
#undef CONSTRUCT_FIXED_ARRAY_OBJ

} // namespace common
} // namespace oceanbase