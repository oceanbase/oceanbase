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

#ifndef OCEANBASE_OB_ARRAY_UTILS_
#define OCEANBASE_OB_ARRAY_UTILS_

#include "lib/udt/ob_array_type.h"
#include "ob_array_fixed_size.h"
#include "ob_array_binary.h"
#include "ob_array_nested.h"
#include "lib/udt/ob_vector_type.h"
#include "src/share/vector/ob_vector_base.h"

namespace oceanbase {
namespace common {

#define FIXED_ARRAY_OBJ_CONTAINS(Element_Type)                                                                 \
  const ObArrayFixedSize<Element_Type> *arr_ptr = static_cast<const ObArrayFixedSize<Element_Type> *>(&array); \
  if (OB_FAIL(arr_ptr->contains(elem, pos))) {                                                                 \
    OB_LOG(WARN, "array contains failed", K(ret));                                                             \
  }

#define FIXED_ARRAY_OBJ_CLONE_EXCEPT(Element_Type)                                                                 \
  const ObArrayFixedSize<Element_Type> *arr_ptr = static_cast<const ObArrayFixedSize<Element_Type> *>(&src_array); \
  if (OB_FAIL(arr_ptr->clone_except(alloc, elem, is_null, dst_array))) {                                           \
    OB_LOG(WARN, "array clone_except failed", K(ret));                                                             \
  }
class ObArrayUtil
{
public :
  static int get_type_name(ObNestedType coll_type, const ObDataType &elem_type, char *buf, int buf_len, uint32_t depth = 1);
  static int push_back_decimal_int(const ObPrecision prec, const ObDecimalInt *dec_val, bool is_null, ObIArrayType *arr_obj);
  template<typename Elem_Type>
  static int contains(const ObIArrayType &array, const Elem_Type &elem, bool &bret)
  {
    int ret = OB_SUCCESS;
    int pos = -1;
    bret = false;
    if (OB_FAIL(position(array, elem, pos))) {
      OB_LOG(WARN, "array position failed", K(ret));
    } else if (pos >= 0) {
      bret = true;
    }
    return ret;
  }
  template<typename Elem_Type>
  static int position(const ObIArrayType &array, const Elem_Type &elem, int &pos)
  {
    int ret = OB_SUCCESS;
    switch (array.get_format()) {
    case ArrayFormat::Fixed_Size :
      if (static_cast<ObObjType>(array.get_element_type()) == ObTinyIntType) {
        FIXED_ARRAY_OBJ_CONTAINS(int8_t);
      } else if (static_cast<ObObjType>(array.get_element_type()) == ObSmallIntType) {
        FIXED_ARRAY_OBJ_CONTAINS(int16_t);
      } else if (static_cast<ObObjType>(array.get_element_type()) == ObIntType) {
        FIXED_ARRAY_OBJ_CONTAINS(int64_t);
      } else if (static_cast<ObObjType>(array.get_element_type()) == ObInt32Type) {
        FIXED_ARRAY_OBJ_CONTAINS(int32_t);
      } else if (static_cast<ObObjType>(array.get_element_type()) == ObUTinyIntType) {
        FIXED_ARRAY_OBJ_CONTAINS(uint8_t);
      } else if (static_cast<ObObjType>(array.get_element_type()) == ObUSmallIntType) {
        FIXED_ARRAY_OBJ_CONTAINS(uint16_t);
      } else if (static_cast<ObObjType>(array.get_element_type()) == ObUInt64Type) {
        FIXED_ARRAY_OBJ_CONTAINS(int64_t);
      } else if (static_cast<ObObjType>(array.get_element_type()) == ObUInt32Type) {
        FIXED_ARRAY_OBJ_CONTAINS(uint32_t);
      } else if (static_cast<ObObjType>(array.get_element_type()) == ObFloatType) {
        FIXED_ARRAY_OBJ_CONTAINS(float);
      } else if (static_cast<ObObjType>(array.get_element_type()) == ObDoubleType) {
        FIXED_ARRAY_OBJ_CONTAINS(double);
      } else {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "invalid array type", K(ret), K(array.get_element_type()));
      }
      break;
    case ArrayFormat::Binary_Varlen :
      if (OB_FAIL(static_cast<const ObArrayBinary *>(&array)->contains(elem, pos))) {
        OB_LOG(WARN, "failed to do array contains", K(ret));
      }
      break;
    case ArrayFormat::Vector :
      if (static_cast<ObObjType>(array.get_element_type()) == ObUTinyIntType) {
        if (OB_FAIL(static_cast<const ObVectorU8Data *>(&array)->contains(elem, pos))) {
          OB_LOG(WARN, "failed to do array contains", K(ret));
        }
      } else if (static_cast<ObObjType>(array.get_element_type()) == ObFloatType) {
        if (OB_FAIL(static_cast<const ObVectorF32Data *>(&array)->contains(elem, pos))) {
          OB_LOG(WARN, "failed to do array contains", K(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "invalid array type", K(ret), K(array.get_element_type()));
      }
      break;
    case ArrayFormat::Nested_Array :
      if (OB_FAIL(static_cast<const ObArrayNested *>(&array)->contains(elem, pos))) {
        OB_LOG(WARN, "failed to do array contains", K(ret));
      }
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "invalid array type", K(ret), K(array.get_format()));
      break;
    }
    return ret;
  }
  static int convert_collection_bin_to_string(const ObString &collection_bin,
                                              const common::ObIArray<common::ObString> &extended_type_info,
                                              common::ObIAllocator &allocator,
                                              ObString &res_str);
  static int get_mysql_type(const common::ObIArray<common::ObString> &extended_type_info,
                            obmysql::EMySQLFieldType &type);

  static int append(ObIArrayType &array, const ObObjType elem_type, const ObDatum *datum);

  template<typename Elem_Type>
  static int clone_except(ObIAllocator &alloc, const ObIArrayType &src_array, const Elem_Type *elem, bool is_null, ObIArrayType *&dst_array)
  {
    int ret = OB_SUCCESS;
    switch (src_array.get_format()) {
    case ArrayFormat::Fixed_Size :
      if (static_cast<ObObjType>(src_array.get_element_type()) == ObTinyIntType) {
        FIXED_ARRAY_OBJ_CLONE_EXCEPT(int8_t);
      } else if (static_cast<ObObjType>(src_array.get_element_type()) == ObSmallIntType) {
        FIXED_ARRAY_OBJ_CLONE_EXCEPT(int16_t);
      } else if (static_cast<ObObjType>(src_array.get_element_type()) == ObIntType) {
        FIXED_ARRAY_OBJ_CLONE_EXCEPT(int64_t);
      } else if (static_cast<ObObjType>(src_array.get_element_type()) == ObInt32Type) {
        FIXED_ARRAY_OBJ_CLONE_EXCEPT(int32_t);
      } else if (static_cast<ObObjType>(src_array.get_element_type()) == ObUTinyIntType) {
        FIXED_ARRAY_OBJ_CLONE_EXCEPT(uint8_t);
      } else if (static_cast<ObObjType>(src_array.get_element_type()) == ObUSmallIntType) {
        FIXED_ARRAY_OBJ_CLONE_EXCEPT(uint16_t);
      } else if (static_cast<ObObjType>(src_array.get_element_type()) == ObUInt64Type) {
        FIXED_ARRAY_OBJ_CLONE_EXCEPT(uint64_t);
      } else if (static_cast<ObObjType>(src_array.get_element_type()) == ObUInt32Type) {
        FIXED_ARRAY_OBJ_CLONE_EXCEPT(uint32_t);
      } else if (static_cast<ObObjType>(src_array.get_element_type()) == ObFloatType) {
        FIXED_ARRAY_OBJ_CLONE_EXCEPT(float);
      } else if (static_cast<ObObjType>(src_array.get_element_type()) == ObDoubleType) {
        FIXED_ARRAY_OBJ_CLONE_EXCEPT(double);
      } else {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "invalid src_array type", K(ret), K(src_array.get_element_type()));
      }
      break;
    case ArrayFormat::Binary_Varlen :
      if (OB_FAIL(static_cast<const ObArrayBinary *>(&src_array)->clone_except(alloc, elem, is_null, dst_array))) {
        OB_LOG(WARN, "failed to do src_array contains", K(ret));
      }
      break;
    case ArrayFormat::Vector :
      if (static_cast<ObObjType>(src_array.get_element_type()) == ObUTinyIntType) {
        if (OB_FAIL(static_cast<const ObVectorU8Data *>(&src_array)->clone_except(alloc, elem, is_null, dst_array))) {
          OB_LOG(WARN, "failed to do src_array contains", K(ret));
        }
      } else if (static_cast<ObObjType>(src_array.get_element_type()) == ObFloatType) {
        if (OB_FAIL(static_cast<const ObVectorF32Data *>(&src_array)->clone_except(alloc, elem, is_null, dst_array))) {
          OB_LOG(WARN, "failed to do src_array contains", K(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "invalid array type", K(ret), K(src_array.get_element_type()));
      }
      break;
    case ArrayFormat::Nested_Array :
      if (OB_FAIL(static_cast<const ObArrayNested *>(&src_array)->clone_except(alloc, elem, is_null, dst_array))) {
        OB_LOG(WARN, "failed to do src_array contains", K(ret));
      }
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "invalid src_array type", K(ret), K(src_array.get_format()));
      break;
    }
    return ret;
  }

};

#undef FIXED_ARRAY_OBJ_CLONE_EXCEPT
#undef FIXED_ARRAY_OBJ_CONTAINS

} // namespace common
} // namespace oceanbase
#endif // OCEANBASE_OB_ARRAY_UTILS_