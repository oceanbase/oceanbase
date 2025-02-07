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
#include "ob_vector_type.h"
#include "lib/ob_errno.h"

namespace oceanbase {
namespace common {
//////////////////////////////////
// implement of ObVectorF32Data //
//////////////////////////////////
int ObVectorF32Data::print(ObStringBuffer &format_str, uint32_t begin, uint32_t print_size) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(format_str.append("["))) {
    OB_LOG(WARN, "fail to append [", K(ret));
  } else if (OB_FAIL(print_element(format_str, begin, print_size))) {
    OB_LOG(WARN, "fail to print vector element", K(ret));
  } else if (OB_FAIL(format_str.append("]"))) {
    OB_LOG(WARN, "fail to append ]", K(ret));
  }
  return ret;
}

int ObVectorF32Data::print_element(ObStringBuffer &format_str,
                                uint32_t begin, uint32_t print_size,
                                ObString delimiter, bool has_null_str, ObString null_str) const
{
  int ret = OB_SUCCESS;
  if (print_size == 0) {
    // print whole array
    print_size = length_;
  }
  bool is_first_elem = true;
  for (int i = begin; i < begin + print_size && OB_SUCC(ret); i++) {
    if (!is_first_elem && OB_FAIL(format_str.append(delimiter))) {
      OB_LOG(WARN, "fail to append delimiter to buffer", K(ret), K(delimiter));
    } else {
      is_first_elem = false;
      int buf_size = FLOAT_TO_STRING_CONVERSION_BUFFER_SIZE;
      if (OB_FAIL(format_str.reserve(buf_size + 1))) {
        OB_LOG(WARN, "fail to reserve memory for format_str", K(ret));
      } else {
        char *start = format_str.ptr() + format_str.length();
        uint64_t len = ob_gcvt(data_[i], ob_gcvt_arg_type::OB_GCVT_ARG_FLOAT, buf_size, start, NULL);
        if (OB_FAIL(format_str.set_length(format_str.length() + len))) {
          OB_LOG(WARN, "fail to set format_str len", K(ret), K(format_str.length()), K(len));
        }
      }
    }
  }
  return ret;
}

int ObVectorF32Data::clone_empty(ObIAllocator &alloc, ObIArrayType *&output, bool read_only) const
{
  int ret = OB_SUCCESS;
  void *buf = alloc.alloc(sizeof(ObVectorData));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "alloc memory failed", K(ret));
  } else {
    ObVectorF32Data *arr_ptr = new (buf) ObVectorF32Data();
    if (read_only) {
    } else if (OB_ISNULL(buf = alloc.alloc(sizeof(ObArrayData<float>)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "alloc memory failed", K(ret));
    } else {
      ObArrayData<float> *arr_data = new (buf) ObArrayData<float>(alloc);
      arr_ptr->set_array_data(arr_data);
      arr_ptr->set_element_type(this->element_type_);
    }
    if (OB_SUCC(ret)) {
      output = arr_ptr;
    }
  }
  return ret;
}

int ObVectorF32Data::elem_at(uint32_t idx, ObObj &elem_obj) const
{
  elem_obj.set_float(data_[idx]);
  return OB_SUCCESS;
}

/////////////////////////////////
// implement of ObVectorU8Data //
/////////////////////////////////
int ObVectorU8Data::print(ObStringBuffer &format_str, uint32_t begin, uint32_t print_size) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(format_str.append("["))) {
    OB_LOG(WARN, "fail to append [", K(ret));
  } else if (OB_FAIL(print_element(format_str, begin, print_size))) {
    OB_LOG(WARN, "fail to print vector element", K(ret));
  } else if (OB_FAIL(format_str.append("]"))) {
    OB_LOG(WARN, "fail to append ]", K(ret));
  }
  return ret;
}

int ObVectorU8Data::print_element(ObStringBuffer &format_str,
                                uint32_t begin, uint32_t print_size,
                                ObString delimiter, bool has_null_str, ObString null_str) const
{
  int ret = OB_SUCCESS;
  if (print_size == 0) {
    // print whole array
    print_size = length_;
  }
  bool is_first_elem = true;
  for (int i = begin; i < begin + print_size && OB_SUCC(ret); i++) {
    if (!is_first_elem && OB_FAIL(format_str.append(delimiter))) {
      OB_LOG(WARN, "fail to append delimiter to buffer", K(ret), K(delimiter));
    } else {
      is_first_elem = false;
      ObFastFormatInt ffi(data_[i]);
      if (OB_FAIL(format_str.append(ffi.ptr(), ffi.length()))) {
        OB_LOG(WARN, "fail to append format int", K(ret), KP(ffi.ptr()));
      }
    }
  }
  return ret;
}

int ObVectorU8Data::clone_empty(ObIAllocator &alloc, ObIArrayType *&output, bool read_only) const
{
  int ret = OB_SUCCESS;
  void *buf = alloc.alloc(sizeof(ObVectorData));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "alloc memory failed", K(ret));
  } else {
    ObVectorU8Data *arr_ptr = new (buf) ObVectorU8Data();
    if (read_only) {
    } else if (OB_ISNULL(buf = alloc.alloc(sizeof(ObArrayData<uint8_t>)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "alloc memory failed", K(ret));
    } else {
      ObArrayData<uint8_t> *arr_data = new (buf) ObArrayData<uint8_t>(alloc);
      arr_ptr->set_array_data(arr_data);
      arr_ptr->set_element_type(this->element_type_);
    }
    if (OB_SUCC(ret)) {
      output = arr_ptr;
    }
  }
  return ret;
}

int ObVectorU8Data::elem_at(uint32_t idx, ObObj &elem_obj) const
{
  elem_obj.set_utinyint(data_[idx]);
  return OB_SUCCESS;
}

} // namespace common
} // namespace oceanbase