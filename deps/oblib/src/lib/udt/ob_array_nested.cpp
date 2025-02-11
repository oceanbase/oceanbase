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
#include "ob_array_nested.h"

namespace oceanbase {
namespace common {

int ObArrayNested::get_data_binary(char *res_buf, int64_t buf_len)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (get_data_binary_len() > buf_len) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "buf len isn't enough", K(ret), K(buf_len));
  } else if (data_container_ == NULL) {
    MEMCPY(res_buf + pos, reinterpret_cast<char *>(null_bitmaps_), sizeof(uint8_t) * length_);
    pos += sizeof(uint8_t) * length_;
    MEMCPY(res_buf + pos, reinterpret_cast<char *>(offsets_), sizeof(uint32_t) * length_);
    pos += sizeof(uint32_t) * length_;
  } else {
    MEMCPY(res_buf + pos, reinterpret_cast<char *>(data_container_->null_bitmaps_.get_data()), sizeof(uint8_t) * data_container_->null_bitmaps_.size());
    pos += sizeof(uint8_t) * data_container_->null_bitmaps_.size();
    MEMCPY(res_buf + pos, reinterpret_cast<char *>(data_container_->offsets_.get_data()), sizeof(uint32_t) * data_container_->offsets_.size());
    pos += sizeof(uint32_t) * data_container_->offsets_.size();
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(data_->get_data_binary(res_buf + pos,  buf_len - pos))) {
    OB_LOG(WARN, "get data binary failed", K(ret), K(pos), K(length_), K(buf_len));
  }
  return ret;
}

int ObArrayNested::get_raw_binary(char *res_buf, int64_t buf_len)
{
  int ret = OB_SUCCESS;
  if (get_raw_binary_len() > buf_len) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "buf len isn't enough", K(ret), K(buf_len));
  } else {
    int64_t pos = 0;
    MEMCPY(res_buf + pos, &length_, sizeof(length_));
    pos += sizeof(length_);
    if (OB_FAIL(get_data_binary(res_buf + pos,  buf_len - pos))) {
      OB_LOG(WARN, "get data binary failed", K(ret), K(buf_len));
    }
  }
  return ret;
}

int ObArrayNested::hash(uint64_t &hash_val) const
{
  uint8_t *null_bitmaps = this->null_bitmaps_;
  uint32_t *offsets = offsets_;
  if (this->data_container_ != NULL) {
    null_bitmaps = this->data_container_->null_bitmaps_.get_data();
    offsets = data_container_->offsets_.get_data();
  }
  hash_val = common::murmurhash(&length_, sizeof(length_), hash_val);
  if (length_ > 0) {
    hash_val = common::murmurhash(null_bitmaps, sizeof(uint8_t) * length_, hash_val);
    hash_val = common::murmurhash(offsets, sizeof(uint32_t) * length_, hash_val);
    data_->hash(hash_val);
  }
  return OB_SUCCESS;
}

int ObArrayNested::insert_from(const ObIArrayType &src, uint32_t begin, uint32_t len)
{
  int ret = OB_SUCCESS;
  if (src.get_format() != get_format()
      || src.get_element_type() != element_type_) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "inconsistent array type", K(ret), K(src.get_format()), K(src.get_element_type()),
                                            K(get_format()), K(element_type_));
  } else if (OB_ISNULL(data_container_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "try to modify read-only array", K(ret));
  } else {
    // insert offsets
    uint32_t last_offset = offset_at(begin, src.get_offsets());
    uint32_t pre_max_offset = data_container_->offset_at(length_);
    for (uint32_t i = 0; i < len && OB_SUCC(ret); ++i) {
      if (OB_FAIL(data_container_->offsets_.push_back(pre_max_offset + src.get_offsets()[begin + i] - last_offset))) {
        OB_LOG(WARN, "failed to push value to array data", K(ret));
      } else {
        last_offset = src.get_offsets()[begin + i];
        pre_max_offset = data_container_->offset_at(data_container_->offsets_.size());
      }
    }
    // insert nullbitmaps
    for (uint32_t i = 0; i < len && OB_SUCC(ret); ++i) {
      if (OB_FAIL(data_container_->null_bitmaps_.push_back(src.get_nullbitmap()[begin + i]))) {
        OB_LOG(WARN, "failed to push null", K(ret));
      }
    }
    // insert data
    if (OB_SUCC(ret) && len > 0) {
      uint32_t start = offset_at(begin, src.get_offsets());
      uint32_t child_len = src.get_offsets()[begin + len - 1] - start;
      const ObIArrayType *child_arr = static_cast<const ObArrayNested&>(src).get_child_array();
      if (OB_FAIL(data_->insert_from(*child_arr, start, child_len))) {
        OB_LOG(WARN, "failed to insert child array", K(ret));
      } else {
        length_ += len;
      }
    }
  }
  return ret;
}

int ObArrayNested::init()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data_container_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "try to modify read-only array", K(ret));
  } else {
    length_ = data_container_->offsets_.size();
    offsets_ = data_container_->offsets_.get_data();
    null_bitmaps_ = data_container_->null_bitmaps_.get_data();
    if (data_ != NULL) {
      data_->init();
    }
  }
  return ret;
}

int ObArrayNested::init(ObString &raw_data)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  char *raw_str = raw_data.ptr();
  if (raw_data.length() < sizeof(length_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "raw data len is invalid", K(ret), K(raw_data.length()));
  } else {
    length_ = *reinterpret_cast<uint32_t *>(raw_str);
    if (length_ > 0) {
      pos += sizeof(length_);
      // init null bitmap
      null_bitmaps_ = reinterpret_cast<uint8_t *>(raw_str + pos);
      if (pos + sizeof(uint8_t) * length_ > raw_data.length()) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "raw data len is invalid", K(ret), K(pos), K(length_), K(raw_data.length()));
      } else {
        pos += sizeof(uint8_t) * length_;
        if (pos + sizeof(uint32_t) * length_ > raw_data.length()) {
          ret = OB_ERR_UNEXPECTED;
          OB_LOG(WARN, "raw data len is invalid", K(ret), K(pos), K(length_), K(raw_data.length()));
        } else {
          // init offset
          offsets_ = reinterpret_cast<uint32_t *>(raw_str + pos);
          if (data_->get_format() == ArrayFormat::Vector) {
            pos += sizeof(uint32_t) * length_;
          } else {
            // caution : length_ - 1 means : last offset is length of data_(child array)
            pos += sizeof(uint32_t) * (length_ - 1);
          }
          // init data
          ObString data_str(raw_data.length() - pos, raw_str + pos);
          if (OB_FAIL(data_->init(data_str))) {
            OB_LOG(WARN, "data init failed", K(ret), K(pos), K(length_), K(raw_data.length()));
          }
        }
      }
    }
  }
  return ret;
}

int ObArrayNested::init(ObDatum *attrs, uint32_t attr_count, bool with_length)
{
  int ret = OB_SUCCESS;
  const uint32_t count = with_length ? 4 : 3;
  if (attr_count < count) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "unexpected attrs", K(ret), K(attr_count), K(count));
  } else {
    uint32_t idx = 0;
    if (with_length) {
      length_ = attrs[idx++].get_uint32();
    }  else {
      length_ = attrs[0].get_int_bytes() / sizeof(uint8_t);
    }
    null_bitmaps_ = const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(attrs[idx++].get_string().ptr()));
    offsets_ = const_cast<uint32_t *>(reinterpret_cast<const uint32_t *>(attrs[idx++].get_string().ptr()));
    if (OB_FAIL(data_->init(attrs + idx, attr_count - idx, false))) {
      OB_LOG(WARN, "failed to init attrs", K(ret), K(attr_count), K(count));
    }
    if ((with_length && (length_ != attrs[1].get_int_bytes() / sizeof(uint8_t) || length_ != attrs[2].get_int_bytes() / sizeof(uint32_t)))
        || (!with_length && (length_ != attrs[1].get_int_bytes() / sizeof(uint32_t)))) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "unexpected attrs", K(ret), K(with_length), K(length_));
    }
  }
  return ret;
}

int ObArrayNested::print(ObStringBuffer &format_str, uint32_t begin, uint32_t print_size, bool print_whole) const
{
  int ret = OB_SUCCESS;
  const ObCollectionArrayType *array_type = dynamic_cast<const ObCollectionArrayType *>(this->get_array_type()->element_type_);
  if (OB_ISNULL(array_type)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret));
  } else if (OB_FAIL(format_str.append("["))) {
    OB_LOG(WARN, "fail to append [", K(ret));
  } else {
    if (print_whole) {
      // print whole array
      print_size = length_;
    }
    for (int i = begin; i < begin + print_size && OB_SUCC(ret); i++) {
      if (i > begin && OB_FAIL(format_str.append(","))) {
        OB_LOG(WARN, "fail to append \",\" to buffer", K(ret));
      } else if (null_bitmaps_[i]) {
          // value is null
          if (OB_FAIL(format_str.append("NULL"))) {
            OB_LOG(WARN, "fail to append NULL to buffer", K(ret));
          }
      } else {
        uint32_t start = offset_at(i, offsets_);
        uint32_t elem_cnt = offsets_[i] - start;
        if (OB_FAIL(data_->print(format_str, start, elem_cnt, false))) {
           OB_LOG(WARN, "fail to append string to format_str", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(format_str.append("]"))) {
    OB_LOG(WARN, "fail to append ]", K(ret));
  }
  return ret;
}

int ObArrayNested::print_element(ObStringBuffer &format_str, uint32_t begin, uint32_t print_size, bool print_whole,
                                 ObString delimiter, bool has_null_str, ObString null_str) const
{
  int ret = OB_SUCCESS;
  const ObCollectionArrayType *array_type = dynamic_cast<const ObCollectionArrayType *>(this->get_array_type()->element_type_);
  if (OB_ISNULL(array_type)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret));
  } else {
    if (print_whole) {
      // print whole array
      print_size = length_;
    }
    uint64_t last_length = format_str.length();
    for (int i = begin; i < begin + print_size && OB_SUCC(ret); i++) {
      if (this->null_bitmaps_[i]) {
        // do nothing
      } else if (format_str.length() > last_length && OB_FAIL(format_str.append(delimiter))) {
        OB_LOG(WARN, "fail to append delimiter to buffer", K(ret), K(delimiter));
      } else {
        last_length = format_str.length();
        uint32_t start = offset_at(i, offsets_);
        uint32_t elem_cnt = offsets_[i] - start;
        if (OB_FAIL(data_->print_element(format_str, start, elem_cnt, false, delimiter, has_null_str, null_str))) {
          OB_LOG(WARN, "fail to append string to format_str", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObArrayNested::push_back(const ObIArrayType &src, bool is_null)
{
  int ret = OB_SUCCESS;
  if (src.get_format() != data_->get_format()
      || src.get_element_type() != data_->get_element_type()) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "inconsistent array type", K(ret), K(src.get_format()), K(src.get_element_type()),
                                            K(data_->get_format()), K(data_->get_element_type()));
  } else if (OB_ISNULL(data_container_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "try to modify read-only array", K(ret));
  } else if (length_ + 1 > MAX_ARRAY_ELEMENT_SIZE) {
    ret = OB_SIZE_OVERFLOW;
    OB_LOG(WARN, "array element size exceed max", K(ret), K(length_), K(MAX_ARRAY_ELEMENT_SIZE));
  } else if (is_null) {
    if (OB_FAIL(push_null())) {
      OB_LOG(WARN, "failed to push null", K(ret));
    }
  } else {
    uint32_t last_offset = data_container_->offset_at(length_);
    uint32_t cur_offset = last_offset + src.size();
    if (OB_FAIL(data_container_->null_bitmaps_.push_back(false))) {
      OB_LOG(WARN, "failed to push null", K(ret));
    } else if (OB_FAIL(data_container_->offsets_.push_back(cur_offset))) {
      OB_LOG(WARN, "failed to push null", K(ret));
    } else if (OB_FAIL(data_->insert_from(src, 0, src.size()))) {
      OB_LOG(WARN, "failed to insert child array", K(ret));
    } else if (get_raw_binary_len() > MAX_ARRAY_SIZE) {
      ret = OB_SIZE_OVERFLOW;
      OB_LOG(WARN, "array data length exceed max", K(ret), K(get_raw_binary_len()), K(MAX_ARRAY_SIZE));
    } else {
      length_++;
    }
  }

  return ret;
}

int ObArrayNested::push_null()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data_container_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "try to modify read-only array", K(ret));
  } else if (length_ + 1 > MAX_ARRAY_ELEMENT_SIZE) {
    ret = OB_SIZE_OVERFLOW;
    OB_LOG(WARN, "array element size exceed max", K(ret), K(length_), K(MAX_ARRAY_ELEMENT_SIZE));
  } else {
    uint32_t last_offset =  data_container_->offset_at(length_);
    if (OB_FAIL(data_container_->null_bitmaps_.push_back(true))) {
      OB_LOG(WARN, "failed to push null", K(ret));
    } else if (OB_FAIL(data_container_->offsets_.push_back(last_offset))) {
      OB_LOG(WARN, "failed to push null", K(ret));
    } else if (get_raw_binary_len() > MAX_ARRAY_SIZE) {
      ret = OB_SIZE_OVERFLOW;
      OB_LOG(WARN, "array data length exceed max", K(ret), K(get_raw_binary_len()), K(MAX_ARRAY_SIZE));
    } else {
      length_++;
    }
  }
  return ret;
}

void ObArrayNested::clear()
{
  null_bitmaps_ = nullptr;
  offsets_ = nullptr;
  length_ = 0;
  if (OB_NOT_NULL(data_)) {
    data_->clear();
  }
  if (OB_NOT_NULL(data_container_)) {
    data_container_->clear();
  }
}

int ObArrayNested::at(uint32_t idx, ObIArrayType &dest) const
{
  int ret = OB_SUCCESS;
  uint32_t start = offset_at(idx, get_offsets());
  uint32_t child_len = get_offsets()[idx] - start;
  const ObIArrayType *child_arr = get_child_array();
  if (OB_FAIL(dest.insert_from(*child_arr, start, child_len))) {
    OB_LOG(WARN, "failed to insert child array", K(ret), K(idx), K(start), K(child_len));
  } else if (OB_FAIL(dest.init())) {
    OB_LOG(WARN, "failed to init array element", K(ret), K(idx), K(start), K(child_len));
  }
  return ret;
}

int ObArrayNested::flatten(ObArrayAttr *attrs, uint32_t attr_count, uint32_t &attr_idx)
{
  int ret = OB_SUCCESS;
  const uint32_t len = 2;
  if (len  >= attr_count) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "unexpected attr count", K(ret), K(attr_count), K(attr_idx), K(len));
  } else {
    attrs[attr_idx].ptr_ = reinterpret_cast<char *>(null_bitmaps_);
    attrs[attr_idx].length_ = sizeof(uint8_t) * length_;
    attr_idx++;
    attrs[attr_idx].ptr_ = reinterpret_cast<char *>(offsets_);
    attrs[attr_idx].length_ = sizeof(uint32_t) * length_;
    attr_idx++;
    if (OB_FAIL(data_->flatten(attrs, attr_count, attr_idx))) {
      OB_LOG(WARN, "failed to flatten data", K(ret), K(attr_count), K(attr_idx));
    }
  }
  return ret;
}

int ObArrayNested::compare_at(uint32_t left_begin, uint32_t left_len,
                              uint32_t right_begin, uint32_t right_len,
                              const ObIArrayType &right, int &cmp_ret) const
{
  int ret = OB_SUCCESS;
  uint32_t cmp_len = std::min(left_len, right_len);
  cmp_ret = 0;
  for (uint32_t i = 0; i < cmp_len && !cmp_ret && OB_SUCC(ret); ++i) {
    if (this->is_null(left_begin + i) && !right.is_null(right_begin + i)) {
      cmp_ret = 1;
    } else if (!this->is_null(left_begin + i) && right.is_null(right_begin + i)) {
      cmp_ret = -1;
    } else if (this->is_null(left_begin + i) && right.is_null(right_begin + i)) {
    } else {
      const ObArrayNested *right_nested = dynamic_cast<const ObArrayNested *>(&right);
      uint32_t l_start = offset_at(left_begin + i, get_offsets());
      uint32_t l_child_len = get_offsets()[left_begin + i] - l_start;
      uint32_t r_start = right_nested->offset_at(right_begin + i, right_nested->get_offsets());
      uint32_t r_child_len = right_nested->get_offsets()[right_begin + i] - r_start;
      if (OB_ISNULL(right_nested)) {
        ret = OB_ERR_ARRAY_TYPE_MISMATCH;
        OB_LOG(WARN, "invalid array type", K(ret), K(right.get_format()), K(this->get_format()));
      } else if (OB_FAIL(get_child_array()->compare_at(l_start, l_child_len, r_start, r_child_len, *right_nested->get_child_array(), cmp_ret))) {
        OB_LOG(WARN, "failed to do child array compare", K(ret), K(l_start), K(l_child_len), K(r_start), K(r_child_len));
      }
    }
  }
  if (!cmp_ret && OB_SUCC(ret) && left_len != right_len) {
    cmp_ret = (left_len > right_len ? 1 : -1);
  }
  return ret;
}

int ObArrayNested::compare(const ObIArrayType &right, int &cmp_ret) const
{
  return compare_at(0, length_, 0, right.size(), right, cmp_ret);
}

int ObArrayNested::contains_all(const ObIArrayType &other, bool &bret) const
{
  int ret = OB_SUCCESS;
  if (other.contain_null() && !this->contain_null()) {
    bret = false;
  } else {
    bret = true;
    for (uint32_t i = 0; i < other.size() && bret && OB_SUCC(ret); ++i) {
      if (other.is_null(i)) {
        // do nothings, checked already
      } else {
        const ObArrayNested *right_data = dynamic_cast<const ObArrayNested *>(&other);
        uint32_t r_start = right_data->offset_at(i, right_data->get_offsets());
        uint32_t r_child_len = right_data->get_offsets()[i] - r_start;
        bool found = false;
        for (uint32_t j = 0; j < length_ && !found && OB_SUCC(ret); ++j) {
          uint32_t l_start = offset_at(j, get_offsets());
          uint32_t l_child_len = get_offsets()[j] - l_start;
          int cmp_ret = 0;
          if (OB_FAIL(get_child_array()->compare_at(l_start, l_child_len, r_start, r_child_len,
                                                    *right_data->get_child_array(),
                                                    cmp_ret))) {
            OB_LOG(WARN, "failed to do nested array contains", K(ret));
          } else if (cmp_ret == 0) {
            found = true;
          }
        }
        if (OB_SUCC(ret) && !found) {
          bret = false;
        }
      }
    }
  }
  return ret;
}

int ObArrayNested::overlaps(const ObIArrayType &other, bool &bret) const
{
  int ret = OB_SUCCESS;
  if (other.contain_null() && this->contain_null()) {
    bret = true;
  } else {
    bret = false;
    for (uint32_t i = 0; i < other.size() && !bret && OB_SUCC(ret); ++i) {
      if (other.is_null(i)) {
        // do nothings, checked already
      } else {
        const ObArrayNested *right_data = dynamic_cast<const ObArrayNested *>(&other);
        uint32_t r_start = right_data->offset_at(i, right_data->get_offsets());
        uint32_t r_child_len = right_data->get_offsets()[i] - r_start;
        for (uint32_t j = 0; j < length_ && !bret; ++j) {
          uint32_t l_start = offset_at(j, get_offsets());
          uint32_t l_child_len = get_offsets()[j] - l_start;
          int cmp_ret = 0;
          if (OB_FAIL(get_child_array()->compare_at(l_start, l_child_len, r_start, r_child_len,
                                                    *right_data->get_child_array(),
                                                    cmp_ret))) {
            OB_LOG(WARN, "failed to do nested array contains", K(ret));
          } else if (cmp_ret == 0) {
            bret = true;
          }
        }
      }
    }
  }
  return ret;
}

int ObArrayNested::clone_empty(ObIAllocator &alloc, ObIArrayType *&output, bool read_only) const
{
  int ret = OB_SUCCESS;
  void *buf = alloc.alloc(sizeof(ObArrayNested));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "alloc memory failed", K(ret));
  } else {
    ObArrayNested *arr_ptr = new (buf) ObArrayNested();
    if (read_only) {
    } else if (OB_ISNULL(buf = alloc.alloc(sizeof(ObArrayData<char>)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "alloc memory failed", K(ret));
    } else {
      ObArrayData<char> *arr_data = new (buf) ObArrayData<char>(alloc);
      arr_ptr->set_array_data(arr_data);
    }
    if (OB_SUCC(ret)) {
      ObIArrayType *arr_child = NULL;
      if (OB_FAIL(get_child_array()->clone_empty(alloc, arr_child, read_only))) {
        OB_LOG(WARN, "failed to clone child empty array", K(ret));
      } else {
        arr_ptr->set_element_type(this->element_type_);
        arr_ptr->set_child_array(arr_child);
        arr_ptr->set_array_type(this->array_type_);
        output = arr_ptr;
      }
    }
  }
  return ret;
}

int ObArrayNested::distinct(ObIAllocator &alloc, ObIArrayType *&output) const
{
  int ret = OB_SUCCESS;
  ObIArrayType *arr_obj = NULL;
  if (OB_FAIL(clone_empty(alloc, arr_obj, false))) {
    OB_LOG(WARN, "clone empty failed", K(ret));
  } else if (contain_null() && OB_FAIL(arr_obj->push_null())) {
    OB_LOG(WARN, "push null failed", K(ret));
  } else {
    hash::ObHashMap<uint64_t, uint32_t> elem_set;
    ObIArrayType *inner_arr = get_child_array();
    ObIArrayType *child_obj = NULL;
    ObIArrayType *check_obj = NULL;
    ObArrayNested *arr_obj_ptr = dynamic_cast<ObArrayNested *>(arr_obj);
    if (OB_ISNULL(arr_obj_ptr)) {
      ret = OB_ERR_ARRAY_TYPE_MISMATCH;
      OB_LOG(WARN, "invalid array type", K(ret), K(inner_arr->get_format()));
    } else if (OB_FAIL(elem_set.create(length_, ObMemAttr(common::OB_SERVER_TENANT_ID, "ArrayDistSet")))) {
      OB_LOG(WARN, "failed to create cellid set", K(ret), K(length_));
    } else if (OB_FAIL(inner_arr->clone_empty(alloc, child_obj, false))) {
      OB_LOG(WARN, "clone empty failed", K(ret));
    } else {
      for (uint32_t i = 0; i < length_ && OB_SUCC(ret); ++i) {
        uint32_t idx = 0;
        uint64_t hash_val = 0;
        if (is_null(i)) {
          // do nothing
        } else if (OB_FAIL(at(i, *child_obj))) {
          OB_LOG(WARN, "get element failed", K(ret), K(i), K(length_));
        } else if (OB_FAIL(child_obj->hash(hash_val))) {
          OB_LOG(WARN, "get element hash value failed", K(ret), K(i), K(length_));
        } else if (OB_FAIL(elem_set.get_refactored(hash_val, idx))) {
          if (ret == OB_HASH_NOT_EXIST) {
            if (OB_FAIL(arr_obj_ptr->push_back(*child_obj))) {
              OB_LOG(WARN, "failed to add elemen", K(ret));
            } else if (OB_FAIL(elem_set.set_refactored(hash_val, i))) {
              OB_LOG(WARN, "failed to add elemen into set", K(ret));
            }
          } else if (ret == OB_HASH_EXIST) {
            // duplicate element, double check
            if (check_obj == NULL && OB_FAIL(inner_arr->clone_empty(alloc, check_obj, false))) {
              OB_LOG(WARN, "clone empty failed", K(ret));
            } else if (OB_FAIL(at(i, *check_obj))) {
              OB_LOG(WARN, "get element failed", K(ret), K(i), K(length_));
            } else if ((*check_obj) == (*child_obj)) {
              // do nothing
            } else if (OB_FAIL(arr_obj_ptr->push_back(*child_obj))) {
              OB_LOG(WARN, "failed to add elemen", K(ret));
            } else {
              check_obj->clear();
            }
          } else {
            OB_LOG(WARN, "failed to check element exist", K(ret));
          }
        }
        if (child_obj != NULL) {
          child_obj->clear();
        }
      }
    }
    if (OB_SUCC(ret)) {
      output = arr_obj;
    }
  }
  return ret;
}

#undef CONSTRUCT_ARRAY_OBJ
#undef CONSTRUCT_FIXED_ARRAY_OBJ

} // namespace common
} // namespace oceanbase