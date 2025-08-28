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
#include "lib/ob_errno.h"
#include "ob_map_type.h"
#include <map>

namespace oceanbase {
namespace common {

int ObMapType::print(ObStringBuffer &format_str, uint32_t begin, uint32_t print_size, bool print_whole) const 
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret) && print_whole) {
    // print whole array
    print_size = length_;
  }
  if (OB_FAIL(format_str.append("{"))) {
    OB_LOG(WARN, "fail to append \"{\"", K(ret));
  }
  for (int i = begin; i < begin + print_size && OB_SUCC(ret); i++) {
    if (i > begin && OB_FAIL(format_str.append(","))) {
      OB_LOG(WARN, "fail to append \",\" to buffer", K(ret));
    } else if (OB_FAIL(print_element_at(format_str, i))) {
      OB_LOG(WARN, "failed to print element", K(ret), K(i));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(format_str.append("}"))) {
    OB_LOG(WARN, "fail to append \"}\" to buffer", K(ret));
  }
  return ret;
}

int ObMapType::print_element_at(ObStringBuffer &format_str, uint32_t idx) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(idx >= length_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "unexpected idx", K(ret), K(idx), K(length_));
  } else if (keys_->is_null(idx)) {
    if (OB_FAIL(format_str.append("NULL"))) {
      OB_LOG(WARN, "fail to append NULL to buffer", K(ret));
    }
  } else if (OB_FAIL(keys_->print_element_at(format_str, idx))) {
    OB_LOG(WARN, "failed to print element", K(ret), K(idx), K(keys_->get_element_type()));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(format_str.append(":"))) {
    OB_LOG(WARN, "fail to append :", K(ret));
  } else if (values_->is_null(idx)) {
    if (OB_FAIL(format_str.append("NULL"))) {
      OB_LOG(WARN, "fail to append NULL to buffer", K(ret));
    }
  } else if (OB_FAIL(values_->print_element_at(format_str, idx))) {
    OB_LOG(WARN, "failed to print element", K(ret), K(idx), K(values_->get_element_type()));
  }
  return ret;
}

int ObMapType::get_raw_binary(char *res_buf, int64_t buf_len)
{
  int ret = OB_SUCCESS;
  if (get_data_binary_len() > buf_len) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "buf len isn't enough", K(ret), K(buf_len));
  } else {
    int64_t pos = 0;
    MEMCPY(res_buf + pos, &this->length_, sizeof(this->length_));
    pos += sizeof(this->length_);
    if (OB_FAIL(get_data_binary(res_buf + pos,  buf_len - pos))) {
      OB_LOG(WARN, "get data binary failed", K(ret), K(buf_len));
    }
  }
  return ret;
}

int ObMapType::get_data_binary(char *res_buf, int64_t buf_len)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (get_data_binary_len() > buf_len) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "buf len isn't enough", K(ret), K(buf_len));
  } else if (OB_FAIL(keys_->get_data_binary(res_buf + pos, buf_len - pos))) {
    OB_LOG(WARN, "get keys data binary failed", K(ret), K(buf_len), K(pos));
  } else if (OB_FALSE_IT(pos += keys_->get_data_binary_len())) {
  } else if (OB_FAIL(values_->get_data_binary(res_buf + pos, buf_len - pos))) {
    OB_LOG(WARN, "get values data binary failed", K(ret), K(buf_len), K(pos));
  }
  return ret;
}

int ObMapType::init(ObString &raw_data)
{
  int ret = OB_SUCCESS;
  char *raw_str = raw_data.ptr();
  if (raw_data.length() < sizeof(length_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "raw data len is invalid", K(ret), K(raw_data.length()));
  } else {
    length_ = *reinterpret_cast<uint32_t *>(raw_str);
    ObString data_str(raw_data.length() - sizeof(length_), raw_data.ptr() + sizeof(length_));
    if (OB_FAIL(init(length_, data_str))) {
      LOG_WARN("init failed", K(ret), K(length_), K(raw_data));
    }
  }
  return ret;
}

int ObMapType::init(uint32_t length, ObString &data_binary)
{
  int ret = OB_SUCCESS;
  char *binary_ptr = data_binary.ptr();
  length_ = length;
  if (OB_FAIL(keys_->init(length_, data_binary))) {
    OB_LOG(WARN, "init keys failed", K(ret), K(length_), K(data_binary));
  } else {
    int64_t pos = keys_->get_data_binary_len();
    ObString value_data_str(data_binary.length() - pos, binary_ptr + pos);
    if (OB_FAIL(values_->init(length_, value_data_str))) {
      OB_LOG(WARN, "init values failed", K(ret), K(length_), K(value_data_str));
    }
  }
  return ret;
}

int ObMapType::init(ObDatum *attrs, uint32_t attr_count, bool with_length)
{
  int ret = OB_SUCCESS;
  uint32_t count = 0;
  uint32_t key_attr_count = 0;
  if (OB_UNLIKELY(OB_ISNULL(attrs))) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "attrs is null", K(ret));
  } else if (keys_->get_format() == ArrayFormat::Fixed_Size) {
    key_attr_count = 2;
  } else if (keys_->get_format() == ArrayFormat::Binary_Varlen) {
    key_attr_count = 3;
  } else {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "unexpected format", K(ret), K(keys_->get_format()));
  }
  if (OB_FAIL(ret)){
  } else if (FALSE_IT(count = with_length ? key_attr_count + 1 : key_attr_count)) {
  } else if (attr_count < count) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "unexpected attrs", K(ret), K(attr_count), K(count));
  } else {
    uint32_t idx = 0;
    if (with_length) {
      length_ = attrs[idx++].get_uint32();
    } else {
      length_ = attrs[0].get_int_bytes() / sizeof(uint8_t);
    }
    if (OB_FAIL(keys_->init(attrs + idx, key_attr_count, false))) {
      OB_LOG(WARN, "failed to init keys attrs", K(ret), K(attr_count), K(key_attr_count));
    } else if (OB_FALSE_IT(idx += key_attr_count)) {
    } else if (OB_FAIL(values_->init(attrs + idx, attr_count - idx, false))) {
      OB_LOG(WARN, "failed to init values attrs", K(ret), K(attr_count), K(count));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(length_ != keys_->size() || length_ != values_->size())) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "keys size or values size is not equal to map size", K(ret), K(length_), K(keys_->size()), K(values_->size()));
  }
  return ret;
}
int ObMapType::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(keys_->init())) {
    OB_LOG(WARN, "init keys failed", K(ret));
  } else if (OB_FAIL(values_->init())) {
    OB_LOG(WARN, "init values failed", K(ret));
  } else if (keys_->size() != values_->size()) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "keys size and values size is not equal", K(ret), K(keys_->size()), K(values_->size()));
  } else {
    length_ = keys_->size();
  }
  return ret;
}

int ObMapType::flatten(ObArrayAttr *attrs, uint32_t attr_count, uint32_t &attr_idx)
{
  int ret = OB_SUCCESS;
  const uint32_t len = 4;
  if (OB_UNLIKELY(OB_ISNULL(attrs))) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "attrs is null", K(ret));
  } else if (OB_FAIL(keys_->flatten(attrs, attr_count, attr_idx))) {
    OB_LOG(WARN, "flatten keys failed", K(ret), K(attr_idx), K(keys_->size()));
  } else if (OB_FAIL(values_->flatten(attrs, attr_count, attr_idx))) {
    OB_LOG(WARN, "flatten values failed", K(ret), K(attr_idx), K(values_->size()));
  }
  return ret;
}

int ObMapType::compare(const ObIArrayType &right, int &cmp_ret) const
{
  int ret = OB_SUCCESS;
  const ObMapType *right_map = dynamic_cast<const ObMapType *>(&right);
  if (OB_ISNULL(right_map)) {
    ret = OB_ERR_ARRAY_TYPE_MISMATCH;
    OB_LOG(WARN, "invalid map type", K(ret), K(right.get_format()), K(this->get_format()));
  } else if (keys_->compare(*right_map->keys_, cmp_ret)) {
    OB_LOG(WARN, "compare keys failed", K(ret), K(cmp_ret));
  } else if (cmp_ret == 0 && values_->compare(*right_map->values_, cmp_ret)) {
    OB_LOG(WARN, "compare values failed", K(ret), K(cmp_ret));
  }
  return ret;
}

int ObMapType::compare_at(uint32_t left_begin, uint32_t left_len,
                          uint32_t right_begin, uint32_t right_len,  
                          const ObIArrayType &right, int &cmp_ret) const
{
  int ret = OB_SUCCESS;
  const ObMapType *right_map = dynamic_cast<const ObMapType *>(&right);
  if (OB_ISNULL(right_map)) {
    ret = OB_ERR_ARRAY_TYPE_MISMATCH;
    OB_LOG(WARN, "invalid map type", K(ret), K(right.get_format()), K(this->get_format()));
  } else if (keys_->compare_at(left_begin, left_len, right_begin, right_len, *right_map->keys_, cmp_ret)) {
    OB_LOG(WARN, "compare keys failed", K(ret), K(cmp_ret));
  } else if (cmp_ret == 0 && values_->compare_at(left_begin, left_len, right_begin, right_len, *right_map->keys_, cmp_ret)) {
    OB_LOG(WARN, "compare values failed", K(ret), K(cmp_ret));
  }
  return ret;
}

int ObMapType::clone_empty(ObIAllocator &alloc, ObIArrayType *&output, bool read_only) const
{
  int ret = OB_SUCCESS;
  void *buf = alloc.alloc(sizeof(ObMapType));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "alloc memory failed", K(ret));
  } else {
    ObMapType *map_ptr = new (buf) ObMapType();
    map_ptr->set_array_type(this->map_type_);
    if (OB_FAIL(keys_->clone_empty(alloc, map_ptr->keys_, read_only))) {
      OB_LOG(WARN, "keys clone empty failed", K(ret));
    } else if (OB_FAIL(values_->clone_empty(alloc, map_ptr->values_, read_only))) {
      OB_LOG(WARN, "values clone empty failed", K(ret));
    } else {
      output = map_ptr;
    }
  }
  return ret;
}

#define CALC_KEY_IDX(Element_Type, Get_Func)                                                      \
  std::map<Element_Type, uint32_t> idx_map;                                                       \
  if (OB_ISNULL(keys_)) {                                                                         \
    ret = OB_ERR_NULL_VALUE;                                                                      \
    LOG_WARN("key array is null", K(ret));                                                        \
  }                                                                                               \
  for (uint32_t i = 0; i < keys_->size() && OB_SUCC(ret); i++) {                                  \
    ObObj elem_obj;                                                                               \
    if (keys_->is_null(i)) {                                                                      \
      idx_arr[0] = i;                                                                             \
      idx_count = 1;                                                                              \
    } else if (OB_FAIL(keys_->elem_at(i, elem_obj))) {                                            \
      LOG_WARN("failed to get element", K(ret), K(i));                                            \
    } else {                                                                                      \
      idx_map[elem_obj.Get_Func()] = i;                                                           \
    }                                                                                             \
  }                                                                                               \
  std::map<Element_Type, uint32_t>::iterator it = idx_map.begin();                                \
  for (; it != idx_map.end() && OB_SUCC(ret); ++it) {                                             \
    idx_arr[idx_count++] = it->second;                                                            \
  }

int ObMapType::distinct(ObIAllocator &alloc, ObIArrayType *&output) const
{
  int ret = OB_SUCCESS;
  ObIArrayType *dst_map = NULL;
  if (OB_FAIL(clone_empty(alloc, dst_map, false))) {
    OB_LOG(WARN, "clone empty failed", K(ret));
  } else if (keys_->get_format() != ArrayFormat::Fixed_Size) {
    ret = OB_NOT_SUPPORTED;
    OB_LOG(WARN, "only support fixed size array for distinct operation", K(ret), K(keys_->get_format()));
  } else if (this->length_ == 0) {
    output = dst_map;
  } else {
    ObIArrayType *dst_key = static_cast<ObMapType *>(dst_map)->keys_;
    ObIArrayType *dst_value = static_cast<ObMapType *>(dst_map)->values_;
    const ObCollectionArrayType *key_array_type = dynamic_cast<const ObCollectionArrayType *>(keys_->get_array_type());
    ObCollectionBasicType *key_elem_type = key_array_type ? dynamic_cast<ObCollectionBasicType *>(key_array_type->element_type_) : NULL;
    uint32_t *idx_arr = NULL;
    uint32_t idx_count = 0;

    if (OB_ISNULL(key_elem_type)) {
      ret = OB_ERR_NULL_VALUE;
      OB_LOG(WARN, "key_elem_type is NULL", K(ret));
    } else if (OB_ISNULL(idx_arr = static_cast<uint32_t *>(alloc.alloc(length_ * sizeof(uint32_t))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for tmpbuf", K(ret), K(length_ * sizeof(uint32_t)));
    } else {
      switch (key_elem_type->basic_meta_.get_obj_type()) {
        case ObNullType: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expect null value", K(ret));
          break;
        }
        case ObTinyIntType: {
          CALC_KEY_IDX(int8_t, get_tinyint);
          break;
        }
        case ObSmallIntType: {
          CALC_KEY_IDX(int16_t, get_smallint);
          break;
        }
        case ObInt32Type: {
          CALC_KEY_IDX(int32_t, get_int32);
          break;
        }
        case ObIntType: {
          CALC_KEY_IDX(int64_t, get_int);
          break;
        }
        case ObUTinyIntType: {
          CALC_KEY_IDX(uint8_t, get_utinyint);
          break;
        }
        case ObUSmallIntType: {
          CALC_KEY_IDX(uint16_t, get_usmallint);
          break;
        }
        case ObUInt32Type: {
          CALC_KEY_IDX(uint32_t, get_uint32);
          break;
        }
        case ObUInt64Type: {
          CALC_KEY_IDX(uint64_t, get_uint64);
          break;
        }
        case ObUFloatType:
        case ObFloatType: {
          CALC_KEY_IDX(float, get_float);
          break;
        }
        case ObUDoubleType:
        case ObDoubleType: {
          CALC_KEY_IDX(double, get_double);
          break;
        }
        default: {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("unsupported element type", K(ret), K(key_elem_type->basic_meta_.get_obj_type()));
        }
      } // end switch
    }

    for (int i = 0; i < idx_count && OB_SUCC(ret); i++) {
      if (OB_FAIL(dst_key->insert_from(*keys_, idx_arr[i]))) {
        OB_LOG(WARN, "append key element failed", K(ret), K(i));
      } else if (OB_FAIL(dst_value->insert_from(*values_, idx_arr[i]))) {
        OB_LOG(WARN, "append value element failed", K(ret), K(i));
      }
    } // end for

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(dst_map->init())) {
      OB_LOG(WARN, "init dst map failed", K(ret));
    } else {
      output = dst_map;
    }
  }

  return ret;
}

} // namespace common
} // namespace oceanbase