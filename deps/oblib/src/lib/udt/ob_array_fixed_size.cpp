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
#include "ob_array_fixed_size.h"

namespace oceanbase {
namespace common {

template<typename T>
int ObArrayFixedSize<T>::push_null()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(this->data_container_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "try to modify read-only array", K(ret));
  } else if (this->length_ + 1 > MAX_ARRAY_ELEMENT_SIZE) {
    ret = OB_SIZE_OVERFLOW;
    OB_LOG(WARN, "array element size exceed max", K(ret), K(this->length_), K(MAX_ARRAY_ELEMENT_SIZE));
  } else if (OB_FAIL(this->data_container_->raw_data_.push_back(0))) {
    OB_LOG(WARN, "failed to push value to array data", K(ret));
  } else if (OB_FAIL(this->data_container_->null_bitmaps_.push_back(1))) {
    // push back null
    OB_LOG(WARN, "failed to push null", K(ret));
  } else if (get_raw_binary_len() > MAX_ARRAY_SIZE) {
    ret = OB_SIZE_OVERFLOW;
    OB_LOG(WARN, "array data length exceed max", K(ret), K(get_raw_binary_len()), K(MAX_ARRAY_SIZE));
  } else {
    this->length_++;
  }
  return ret;
}

template<typename T>
int ObArrayFixedSize<T>::push_back(T value, bool is_null)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(this->data_container_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "try to modify read-only array", K(ret));
  } else if (this->length_ + 1 > MAX_ARRAY_ELEMENT_SIZE) {
    ret = OB_SIZE_OVERFLOW;
    OB_LOG(WARN, "array element size exceed max", K(ret), K(this->length_), K(MAX_ARRAY_ELEMENT_SIZE));
  } else if (is_null) {
    if (OB_FAIL(this->data_container_->raw_data_.push_back(0))) {
      OB_LOG(WARN, "failed to push value to array data", K(ret));
    } else if (OB_FAIL(this->data_container_->null_bitmaps_.push_back(1))) {
      // push back null
      OB_LOG(WARN, "failed to push null", K(ret));
    }
  } else if (OB_FAIL(this->data_container_->raw_data_.push_back(value))) {
    OB_LOG(WARN, "failed to push value to array data", K(ret));
  } else if (OB_FAIL(this->data_container_->null_bitmaps_.push_back(0))) {
    OB_LOG(WARN, "failed to push null", K(ret));
  } else if (get_raw_binary_len() > MAX_ARRAY_SIZE) {
    ret = OB_SIZE_OVERFLOW;
    OB_LOG(WARN, "array data length exceed max", K(ret), K(get_raw_binary_len()), K(MAX_ARRAY_SIZE));
  }
  if (OB_SUCC(ret)) {
    this->length_++;
  }
  return ret;
}

template<typename T>
int ObArrayFixedSize<T>::print(ObStringBuffer &format_str, uint32_t begin, uint32_t print_size, bool print_whole) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(format_str.append("["))) {
    OB_LOG(WARN, "fail to append [", K(ret));
  } else if (OB_FAIL(print_element(format_str, begin, print_size, print_whole))) {
    OB_LOG(WARN, "fail to print element", K(ret));
  } else if (OB_FAIL(format_str.append("]"))) {
    OB_LOG(WARN, "fail to append ]", K(ret));
  }
  return ret;
}

template<typename T>
int ObArrayFixedSize<T>::print_element(ObStringBuffer &format_str, uint32_t begin, uint32_t print_size, bool print_whole,
                                       ObString delimiter, bool has_null_str, ObString null_str) const
{
  int ret = OB_SUCCESS;
  const ObCollectionBasicType *basic_type = dynamic_cast<const ObCollectionBasicType *>(this->get_array_type()->element_type_);
  if (OB_ISNULL(basic_type)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret));
  } else {
    if (print_whole) {
      // print whole element
      print_size = this->length_;
    }
    ObObjType obj_type = basic_type->basic_meta_.get_obj_type();
    bool is_first_elem = true;
    for (int i = begin; i < begin + print_size && OB_SUCC(ret); i++) {
      if (this->null_bitmaps_[i] && !has_null_str) {
        // do nothing
      } else if (!is_first_elem && OB_FAIL(format_str.append(delimiter))) {
        OB_LOG(WARN, "fail to append delimiter to buffer", K(ret), K(delimiter));
      } else if (this->null_bitmaps_[i]) {
        // value is null
        is_first_elem = false;
        if (OB_FAIL(format_str.append(null_str))) {
          OB_LOG(WARN, "fail to append null string to buffer", K(ret), K(null_str));
        }
      } else {
        is_first_elem = false;
        switch (obj_type) {
          case ObTinyIntType:
          case ObSmallIntType:
          case ObIntType:
          case ObInt32Type: {
            char tmp_buf[ObFastFormatInt::MAX_DIGITS10_STR_SIZE] = {0};
            int64_t len = ObFastFormatInt::format_signed(data_[i], tmp_buf);
            if (OB_FAIL(format_str.append(tmp_buf, len))) {
              OB_LOG(WARN, "fail to append int to buffer", K(ret), K(data_[i]), K(print_size));
            }
            break;
          }
          case ObUTinyIntType:
          case ObUSmallIntType:
          case ObUInt64Type:
          case ObUInt32Type: {
            char tmp_buf[ObFastFormatInt::MAX_DIGITS10_STR_SIZE] = {0};
            int64_t len = ObFastFormatInt::format_unsigned(data_[i], tmp_buf);
            if (OB_FAIL(format_str.append(tmp_buf, len))) {
              OB_LOG(WARN, "fail to append int to buffer", K(ret), K(data_[i]), K(print_size));
            }
            break;
          }
          case ObFloatType:
          case ObUFloatType:
          case ObDoubleType:
          case ObUDoubleType: {
            int buf_size = ob_is_float_tc(obj_type) ? FLOAT_TO_STRING_CONVERSION_BUFFER_SIZE : DOUBLE_TO_STRING_CONVERSION_BUFFER_SIZE;
            if (OB_FAIL(format_str.reserve(buf_size + 1))) {
              OB_LOG(WARN, "fail to reserve memory for format_str", K(ret));
            } else {
              char *start = format_str.ptr() + format_str.length();
              uint64_t len = ob_gcvt(data_[i],
                                      ob_is_float_tc(obj_type) ? ob_gcvt_arg_type::OB_GCVT_ARG_FLOAT : ob_gcvt_arg_type::OB_GCVT_ARG_DOUBLE,
                                      buf_size, start, NULL);
              if (OB_FAIL(format_str.set_length(format_str.length() + len))) {
                OB_LOG(WARN, "fail to set format_str len", K(ret), K(format_str.length()), K(len));
              }
            }
            break;
          }
          case ObDecimalIntType: {
            int64_t pos = 0;
            char tmp_buf[ObFastFormatInt::MAX_DIGITS10_STR_SIZE] = {0};
            if (OB_FAIL(wide::to_string(reinterpret_cast<const ObDecimalInt *>(&data_[i]), sizeof(data_[i]), scale_,
                                        tmp_buf, ObFastFormatInt::MAX_DIGITS10_STR_SIZE, pos))) {
              OB_LOG(WARN, "fail to format decimal int to string", K(ret), K(data_[i]), K(print_size));
            } else if (OB_FAIL(format_str.append(tmp_buf, pos))) {
              OB_LOG(WARN, "fail to append decimal int to buffer", K(ret), K(data_[i]), K(print_size));
            }
            break;
          }
          default: {
            ret = OB_ERR_UNEXPECTED;
            OB_LOG(WARN, "unexpected element type", K(ret), K(basic_type->basic_meta_.get_obj_type()));
          }
        }
      }
    }
  }
  return ret;
}

template<typename T>
int32_t ObArrayFixedSize<T>::get_data_binary_len()
{
  if (this->data_container_ == NULL) {
    return this->length_ * sizeof(uint8_t) + this->length_ * sizeof(T);
  }
  return sizeof(uint8_t) * this->data_container_->null_bitmaps_.size() + sizeof(T) * this->data_container_->raw_data_.size();
}

template<typename T>
int ObArrayFixedSize<T>::get_data_binary(char *res_buf, int64_t buf_len)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (get_data_binary_len() > buf_len) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "buf len isn't enough", K(ret), K(buf_len), K(pos));
  } else if (this->data_container_ == NULL) {
    MEMCPY(res_buf + pos, this->null_bitmaps_, sizeof(uint8_t) * this->length_);
    pos += sizeof(uint8_t) * this->length_;
    MEMCPY(res_buf + pos, this->data_, sizeof(T) * this->length_);
  } else {
    MEMCPY(res_buf + pos, reinterpret_cast<char *>(this->data_container_->null_bitmaps_.get_data()), sizeof(uint8_t) * this->data_container_->null_bitmaps_.size());
    pos += sizeof(uint8_t) * this->data_container_->null_bitmaps_.size();
    MEMCPY(res_buf + pos, reinterpret_cast<char *>(this->data_container_->raw_data_.get_data()), sizeof(T) * this->data_container_->raw_data_.size());
  }
  return ret;
}

template<typename T>
int ObArrayFixedSize<T>::get_raw_binary(char *res_buf, int64_t buf_len)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (get_raw_binary_len() > buf_len) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "buf len isn't enough", K(ret), K(buf_len), K(pos));
  } else {
    MEMCPY(res_buf + pos, &this->length_, sizeof(this->length_));
    pos += sizeof(this->length_);
    if (OB_FAIL(get_data_binary(res_buf + pos, buf_len - pos))) {
      OB_LOG(WARN, "get data binary failed", K(ret), K(buf_len), K(pos));
    }
  }
  return ret;
}

template<typename T>
int ObArrayFixedSize<T>::hash(uint64_t &hash_val) const
{
  uint8_t *null_bitmaps = this->null_bitmaps_;
  T *data = this->data_;
  if (this->data_container_ != NULL) {
    null_bitmaps = this->data_container_->null_bitmaps_.get_data();
    data = this->data_container_->raw_data_.get_data();
  }
  hash_val = common::murmurhash(&this->length_, sizeof(this->length_), hash_val);
  if (this->length_ > 0) {
    hash_val = common::murmurhash(null_bitmaps, sizeof(uint8_t) * this->length_, hash_val);
    hash_val = common::murmurhash(data, sizeof(T) * this->length_, hash_val);
  }
  return OB_SUCCESS;
}

template<typename T>
int ObArrayFixedSize<T>::init()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(this->data_container_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "try to modify read-only array", K(ret));
  } else {
    this->length_ = this->data_container_->raw_data_.size();
    data_ = this->data_container_->raw_data_.get_data();
    this->null_bitmaps_ = this->data_container_->null_bitmaps_.get_data();
  }
  return ret;
}

template<typename T>
int ObArrayFixedSize<T>::init(ObString &raw_data)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  char *raw_str = raw_data.ptr();
  if (raw_data.length() < sizeof(this->length_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "raw data len is invalid", K(ret), K(raw_data.length()));
  } else {
    this->length_ = *reinterpret_cast<uint32_t *>(raw_str);
    pos += sizeof(this->length_);
    this->null_bitmaps_ = reinterpret_cast<uint8_t *>(raw_str + pos);
    if (pos + sizeof(uint8_t) * this->length_ > raw_data.length()) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "raw data len is invalid", K(ret), K(pos), K(this->length_), K(raw_data.length()));
    } else {
      pos += sizeof(uint8_t) * this->length_;
      data_ = reinterpret_cast<T *>(raw_str + pos);
      if (pos + sizeof(T) * this->length_ > raw_data.length()) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "raw data len is invalid", K(ret), K(pos), K(this->length_), K(raw_data.length()));
      }
    }
  }
  return ret;
}

template<typename T>
int ObArrayFixedSize<T>::init(ObDatum *attrs, uint32_t attr_count, bool with_length)
{
  int ret = OB_SUCCESS;
  const uint32_t count = with_length ? 3 : 2;
  if (attr_count != count) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "unexpected attrs", K(ret), K(attr_count), K(count));
  } else {
    uint32_t idx = 0;
    if (with_length) {
      this->length_ = attrs[idx++].get_uint32();
    }  else {
      this->length_ = attrs[0].get_int_bytes() / sizeof(uint8_t);
    }
    this->null_bitmaps_ = const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(attrs[idx++].get_string().ptr()));
    data_ = const_cast<T *>(reinterpret_cast<const T *>(attrs[idx++].get_string().ptr()));
    if ((with_length && (this->length_ != attrs[1].get_int_bytes() / sizeof(uint8_t) || this->length_ != attrs[2].get_int_bytes() / sizeof(T)))
        || (!with_length && (this->length_ != attrs[1].get_int_bytes() / sizeof(T)))) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "unexpected attrs", K(ret), K(with_length), K(this->length_));
    }
  }
  return ret;
}

template<typename T>
int ObArrayFixedSize<T>::insert_from(const ObIArrayType &src, uint32_t begin, uint32_t len)
{
  int ret = OB_SUCCESS;
  if (src.get_format() != get_format()
      || src.get_element_type() != this->element_type_) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "inconsistent array type", K(ret), K(src.get_format()), K(src.get_element_type()),
                                            K(get_format()), K(this->element_type_));
  } else if (OB_ISNULL(this->data_container_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "try to modify read-only array", K(ret));
  } else {
    const uint32_t src_data_offset = begin * sizeof(T);
    const uint32_t src_null_offset = begin * sizeof(uint8_t);
    int64_t curr_pos = this->data_container_->raw_data_.size();
    int64_t capacity = curr_pos + len;
    if (OB_FAIL(this->data_container_->raw_data_.prepare_allocate(capacity))) {
      OB_LOG(WARN, "allocate memory failed", K(ret), K(capacity));
    } else {
      char *cur_data = reinterpret_cast<char *>(this->data_container_->raw_data_.get_data() + curr_pos);
      MEMCPY(cur_data, src.get_data() + src_data_offset, len * sizeof(T));
      // insert nullbitmaps
      curr_pos = this->data_container_->null_bitmaps_.size();
      capacity = curr_pos + len;
      if (OB_FAIL(this->data_container_->null_bitmaps_.prepare_allocate(capacity))) {
        OB_LOG(WARN, "allocate memory failed", K(ret), K(capacity));
      } else {
        uint8_t *cur_null_bitmap = this->data_container_->null_bitmaps_.get_data() + curr_pos;
        MEMCPY(cur_null_bitmap, src.get_nullbitmap() + src_null_offset, len * sizeof(uint8_t));
        this->length_ += len;
      }
    }
  }
  return ret;
}

template<typename T>
int ObArrayFixedSize<T>::elem_at(uint32_t idx, ObObj &elem_obj) const
{
  int ret = OB_SUCCESS;
  ObCollectionBasicType *elem_type = static_cast<ObCollectionBasicType *>(this->get_array_type()->element_type_);
  switch (elem_type->basic_meta_.get_obj_type()) {
  case ObTinyIntType: {
    elem_obj.set_tinyint(data_[idx]);
    break;
  }
  case ObSmallIntType: {
    elem_obj.set_smallint(data_[idx]);
    break;
  }
  case ObInt32Type: {
    elem_obj.set_int32(data_[idx]);
    break;
  }    case ObIntType: {
    elem_obj.set_int(data_[idx]);
    break;
  }
  case ObUTinyIntType: {
    elem_obj.set_utinyint(data_[idx]);
    break;
  }
  case ObUSmallIntType: {
    elem_obj.set_usmallint(data_[idx]);
    break;
  }
  case ObUInt64Type: {
    elem_obj.set_uint64(data_[idx]);
    break;
  }
  case ObUInt32Type: {
    elem_obj.set_uint32(data_[idx]);
    break;
  }
  case ObUDoubleType:
  case ObDoubleType: {
    elem_obj.set_double(data_[idx]);
    break;
  }
  case ObUFloatType:
  case ObFloatType: {
    elem_obj.set_float(data_[idx]);
    break;
  }
  default: {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "unexpected element type", K(ret), K(elem_type->basic_meta_.get_obj_type()));
  }
  } // end switch
  return ret;
}

template<typename T>
void ObArrayFixedSize<T>::clear()
{
  data_ = nullptr;
  this->null_bitmaps_ = nullptr;
  this->length_ = 0;
  if (OB_NOT_NULL(this->data_container_)) {
    this->data_container_->clear();
  }
}

template<typename T>
int ObArrayFixedSize<T>::flatten(ObArrayAttr *attrs, uint32_t attr_count, uint32_t &attr_idx)
{
  int ret = OB_SUCCESS;
  const uint32_t len = 2;
  if (len + attr_idx >= attr_count) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "unexpected attr count", K(ret), K(attr_count), K(attr_idx), K(len));
  } else {
    attrs[attr_idx].ptr_ = reinterpret_cast<char *>(this->null_bitmaps_);
    attrs[attr_idx].length_ = sizeof(uint8_t) * this->length_;
    attr_idx++;
    attrs[attr_idx].ptr_ = reinterpret_cast<char *>(data_);
    attrs[attr_idx].length_ = sizeof(T) * this->length_;
    attr_idx++;
  }
  return ret;
}

template<typename T>
int ObArrayFixedSize<T>::compare_at(uint32_t left_begin, uint32_t left_len, uint32_t right_begin, uint32_t right_len,
                                    const ObIArrayType &right, int &cmp_ret) const
{
  int ret = OB_SUCCESS;
  const ObArrayFixedSize<T> *right_data = dynamic_cast<const ObArrayFixedSize<T> *>(&right);
  if (OB_ISNULL(right_data)) {
    ret = OB_ERR_ARRAY_TYPE_MISMATCH;
    OB_LOG(WARN, "invalid array type", K(ret), K(right.get_format()), K(this->get_format()));
  } else {
    uint32_t cmp_len = std::min(left_len, right_len);
    cmp_ret = 0;
    for (uint32_t i = 0; i < cmp_len && !cmp_ret; ++i) {
      if (this->is_null(left_begin + i) && !right.is_null(right_begin + i)) {
        cmp_ret = 1;
      } else if (!this->is_null(left_begin + i) && right.is_null(right_begin + i)) {
        cmp_ret = -1;
      } else if (this->is_null(left_begin + i) && right.is_null(right_begin + i)) {
      } else if (this->data_[left_begin + i] != (*right_data)[right_begin + i]) {
        cmp_ret = this->data_[left_begin + i] > (*right_data)[right_begin + i] ? 1 : -1;
      }
    }
    if (cmp_ret == 0 && left_len != right_len) {
      cmp_ret = left_len > right_len ? 1 : -1;
    }
  }
  return ret;
}

template<typename T>
bool ObArrayFixedSize<T>::sort_cmp(uint32_t idx_l, uint32_t idx_r) const
{
  bool bret = true;
  if (this->is_null(idx_l)) {
    bret = false;
  } else if (this->is_null(idx_r)) {
    bret = true;
  } else {
    bret = operator[](idx_l) < operator[](idx_r);
  }
  return bret;
}

template<typename T>
int ObArrayFixedSize<T>::contains_all(const ObIArrayType &other, bool &bret) const
{
  int ret = OB_SUCCESS;
  const ObArrayFixedSize<T> *right_data = dynamic_cast<const ObArrayFixedSize<T> *>(&other);
  if (OB_ISNULL(right_data)) {
    ret = OB_ERR_ARRAY_TYPE_MISMATCH;
    OB_LOG(WARN, "invalid array type", K(ret), K(other.get_format()), K(this->get_format()));
  } else if (other.contain_null() && !this->contain_null()) {
    bret = false;
  } else {
    bret = true;
    for (uint32_t i = 0; i < other.size() && bret && OB_SUCC(ret); ++i) {
      int pos = -1;
      if (right_data->is_null(i)) {
        // do nothings, checked already
      } else if (OB_FAIL(this->contains((*right_data)[i], pos))) {
        OB_LOG(WARN, "check element contains failed", K(ret), K(i), K((*right_data)[i]));
      } else if (pos < 0) {
        bret = false;
      }
    }
  }
  return ret;
}

template<typename T>
int ObArrayFixedSize<T>::overlaps(const ObIArrayType &other, bool &bret) const
{
  int ret = OB_SUCCESS;
  const ObArrayFixedSize<T> *right_data = dynamic_cast<const ObArrayFixedSize<T> *>(&other);
  if (OB_ISNULL(right_data)) {
    ret = OB_ERR_ARRAY_TYPE_MISMATCH;
    OB_LOG(WARN, "invalid array type", K(ret), K(other.get_format()), K(this->get_format()));
  } else if (other.contain_null() && this->contain_null()) {
    bret = true;
  } else {
    bret = false;
    for (uint32_t i = 0; i < other.size() && !bret && OB_SUCC(ret); ++i) {
      int pos = -1;
      if (right_data->is_null(i)) {
        // do nothings, checked already
      } else if (OB_FAIL(this->contains((*right_data)[i], pos))) {
        OB_LOG(WARN, "check element contains failed", K(ret), K(i), K((*right_data)[i]));
      } else if (pos >= 0) {
        bret = true;
      }
    }
  }
  return ret;
}

template<typename T>
int ObArrayFixedSize<T>::clone_empty(ObIAllocator &alloc, ObIArrayType *&output, bool read_only) const
{
  int ret = OB_SUCCESS;
  void *buf = alloc.alloc(sizeof(ObArrayFixedSize<T>));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "alloc memory failed", K(ret));
  } else {
    ObArrayFixedSize<T> *arr_ptr = new (buf) ObArrayFixedSize<T>();
    if (read_only) {
    } else if (OB_ISNULL(buf = alloc.alloc(sizeof(ObArrayData<T>)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "alloc memory failed", K(ret));
    } else {
      ObArrayData<T> *arr_data = new (buf) ObArrayData<T>(alloc);
      arr_ptr->set_array_data(arr_data);
      arr_ptr->set_element_type(this->element_type_);
      arr_ptr->set_array_type(this->array_type_);
    }
    if (OB_SUCC(ret)) {
      output = arr_ptr;
    }
  }
  return ret;
}

template<typename T>
int ObArrayFixedSize<T>::distinct(ObIAllocator &alloc, ObIArrayType *&output) const
{
  int ret = OB_SUCCESS;
  ObIArrayType *arr_ptr = NULL;
  if (OB_FAIL(clone_empty(alloc, arr_ptr, false))) {
    OB_LOG(WARN, "clone empty failed", K(ret));
  } else if (this->contain_null() && OB_FAIL(arr_ptr->push_null())) {
    OB_LOG(WARN, "push null failed", K(ret));
  } else {
    hash::ObHashSet<ObString> elem_set;
    ObArrayFixedSize<T> *arr_data = dynamic_cast<ObArrayFixedSize<T> *>(arr_ptr);
    if (OB_ISNULL(arr_data)) {
      ret = OB_ERR_ARRAY_TYPE_MISMATCH;
      OB_LOG(WARN, "invalid array type", K(ret), K(this->get_format()));
    } else if (OB_FAIL(elem_set.create(this->length_, ObMemAttr(common::OB_SERVER_TENANT_ID, "ArrayDistSet")))) {
      OB_LOG(WARN, "failed to create cellid set", K(ret));
    } else {
      for (uint32_t i = 0; i < this->length_ && OB_SUCC(ret); ++i) {
        ObString val;
        if (this->is_null(i)) {
          // do nothing
        } else if (FALSE_IT(val.assign_ptr(reinterpret_cast<char *>(&data_[i]), sizeof(T)))) {
        } else if (OB_FAIL(elem_set.exist_refactored(val))) {
          if (ret == OB_HASH_NOT_EXIST) {
            if (OB_FAIL(arr_data->push_back((*this)[i]))) {
              OB_LOG(WARN, "failed to add element", K(ret));
            } else if (OB_FAIL(elem_set.set_refactored(val))) {
              OB_LOG(WARN, "failed to add element into set", K(ret));
            }
          } else if (ret == OB_HASH_EXIST) {
            // duplicate element, do nothing
            ret = OB_SUCCESS;
          } else {
            OB_LOG(WARN, "failed to check element exist", K(ret));
          }
        } else {
          // do nothing
        }
      }
    }
    if (OB_SUCC(ret)) {
      output = arr_ptr;
    }
  }
  return ret;
}

} // namespace common
} // namespace oceanbase