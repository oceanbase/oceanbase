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
#include "ob_array_binary.h"

namespace oceanbase {
namespace common {

int ObArrayBinary::push_back(const ObString &value, bool is_null)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data_container_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "try to modify read-only array", K(ret));
  } else if (length_ + 1 > MAX_ARRAY_ELEMENT_SIZE) {
    ret = OB_SIZE_OVERFLOW;
    OB_LOG(WARN, "array element size exceed max", K(ret), K(length_), K(MAX_ARRAY_ELEMENT_SIZE));
  } else {
    uint32_t last_offset =  data_container_->raw_data_.size();
    if (is_null) {
      // push back null
      if (OB_FAIL(push_null())) {  
        OB_LOG(WARN, "failed to push null", K(ret));
      }
    } else if (OB_FAIL(data_container_->offsets_.push_back(last_offset + value.length()))) {
        OB_LOG(WARN, "failed to push value to array data", K(ret));
    } else if (OB_FAIL(data_container_->null_bitmaps_.push_back(0))) {
      OB_LOG(WARN, "failed to push null", K(ret));
    } else {
      for (uint32_t i = 0; i < value.length() && OB_SUCC(ret); ++i) {
        if (OB_FAIL(data_container_->raw_data_.push_back(value[i]))) {
          OB_LOG(WARN, "failed to push value to array data", K(ret));
        } 
      }
      if (OB_FAIL(ret)) {
      } else if (get_raw_binary_len() > MAX_ARRAY_SIZE) {
        ret = OB_SIZE_OVERFLOW;
        OB_LOG(WARN, "array data length exceed max", K(ret), K(get_raw_binary_len()), K(MAX_ARRAY_SIZE));
      } else {
        length_++;
      } 
    }
  }
  return ret;
}

int ObArrayBinary::insert_from(const ObIArrayType &src, uint32_t begin, uint32_t len)
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
  } else if (OB_UNLIKELY(begin + len > src.size())) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "unexpected begin or len", K(ret), K(begin), K(len), K(src.size()));
  } else if (len > 0) {
    // insert data
    const uint32_t src_offset = offset_at(begin, src.get_offsets());
    uint32_t src_len = src.get_offsets()[begin + len - 1] - src_offset;
    int64_t curr_pos = data_container_->raw_data_.size();
    int64_t capacity = curr_pos + src_len;
    if (OB_FAIL(data_container_->raw_data_.prepare_allocate(capacity))) {
      OB_LOG(WARN, "allocate memory failed", K(ret), K(capacity));
    } else {
      char *cur_data = data_container_->raw_data_.get_data() + curr_pos;
      MEMCPY(cur_data, src.get_data() + src_offset, src_len);
      // insert offsets
      uint32_t last_offset = src_offset;
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
      if (OB_SUCC(ret)) {
        length_ += len;
      }
    }
  }
  return ret;
}

int ObArrayBinary::elem_at(uint32_t idx, ObObj &elem_obj) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(idx >= this->length_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "unexpected idx", K(ret), K(idx), K(this->length_));
  } else {
    elem_obj.set_varchar(operator[](idx));
  }
  return ret;
}

ObString ObArrayBinary::operator[](const int64_t i) const
{
  ObString str;
  uint32_t last_offset = offset_at(i, offsets_);
  if (i >= 0 && i < length_) {
    uint32_t offset = offsets_[i];
    str.assign_ptr(&data_[last_offset], offset - last_offset);
  }
  return str;
}

int ObArrayBinary::get_data_binary(char *res_buf, int64_t buf_len)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_UNLIKELY(OB_ISNULL(res_buf))) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "res_buf is null", K(ret));
  } else if (get_data_binary_len() > buf_len) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "buf len isn't enough", K(ret), K(buf_len));
  } else if (this->length_ == 0) {
    // do nothing
  } else if (data_container_ == NULL) {
    if (length_ > 0) {
      uint32_t last_idx = length_ - 1;
      MEMCPY(res_buf + pos, reinterpret_cast<char *>(null_bitmaps_), sizeof(uint8_t) * length_);
      pos += sizeof(uint8_t) * length_;
      MEMCPY(res_buf + pos, reinterpret_cast<char *>(offsets_), sizeof(uint32_t) * length_);
      pos += sizeof(uint32_t) * length_;
      MEMCPY(res_buf + pos, data_, offsets_[last_idx]);
    }
  } else {
    MEMCPY(res_buf + pos, reinterpret_cast<char *>(data_container_->null_bitmaps_.get_data()), sizeof(uint8_t) * data_container_->null_bitmaps_.size());
    pos += sizeof(uint8_t) * data_container_->null_bitmaps_.size();
    MEMCPY(res_buf + pos, reinterpret_cast<char *>(data_container_->offsets_.get_data()), sizeof(uint32_t) * data_container_->offsets_.size());
    pos += sizeof(uint32_t) * data_container_->offsets_.size();
    MEMCPY(res_buf + pos, reinterpret_cast<char *>(data_container_->raw_data_.get_data()), data_container_->raw_data_.size());
  }
  return ret;
}

int ObArrayBinary::get_raw_binary(char *res_buf, int64_t buf_len)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_ISNULL(res_buf))) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "res_buf is null", K(ret));
  } else if (get_raw_binary_len() > buf_len) {
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

int ObArrayBinary::hash(uint64_t &hash_val) const
{
  uint8_t *null_bitmaps = this->null_bitmaps_;
  uint32_t *offsets = offsets_;
  char *data = this->data_;
  uint32_t last_idx = length_ > 0 ? length_ - 1 : 0;
  if (this->data_container_ != NULL) {
    null_bitmaps = this->data_container_->null_bitmaps_.get_data();
    offsets = data_container_->offsets_.get_data();
    data = this->data_container_->raw_data_.get_data();
  }
  hash_val = common::murmurhash(&length_, sizeof(length_), hash_val);
  if (length_ > 0) {
    hash_val = common::murmurhash(null_bitmaps, sizeof(uint8_t) * this->length_, hash_val);
    hash_val = common::murmurhash(offsets, sizeof(uint32_t) * this->length_, hash_val);
    hash_val = common::murmurhash(data, offsets_[last_idx], hash_val);
  }
  return OB_SUCCESS;
}

int ObArrayBinary::init()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data_container_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "try to modify read-only array", K(ret));
  } else {
    length_ = data_container_->offsets_.size();
    offsets_ = data_container_->offsets_.get_data();
    null_bitmaps_ = data_container_->null_bitmaps_.get_data();
    data_ = data_container_->raw_data_.get_data();
  }
  if (OB_SUCC(ret) && 
      length_ != 0 && 
      (OB_ISNULL(null_bitmaps_) || OB_ISNULL(offsets_) || (offsets_[length_ - 1] != 0 && OB_ISNULL(data_)))) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "init failed", K(ret));
  }
  return ret;
}

int ObArrayBinary::init(ObString &raw_data)
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

int ObArrayBinary::init(uint32_t length, ObString &data_binary)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  char *binary_ptr = data_binary.ptr();
  length_ = length;
  if (length_ > 0) {
    // init null bitmap
    null_bitmaps_ = reinterpret_cast<uint8_t *>(binary_ptr + pos);
    if (pos + sizeof(uint8_t) * length_ > data_binary.length()) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "raw data len is invalid", K(ret), K(pos), K(length_), K(data_binary.length()));
    } else {
      pos += sizeof(uint8_t) * length_;
      if (pos + sizeof(uint32_t) * length_ > data_binary.length()) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "raw data len is invalid", K(ret), K(pos), K(length_), K(data_binary.length()));
      } else {
        // init offset
        offsets_ = reinterpret_cast<uint32_t *>(binary_ptr + pos);
        pos += sizeof(uint32_t) * length_;
        // init data
        data_ = reinterpret_cast<char *>(binary_ptr + pos);
        // last offset should be equal to data_ length
        if (offsets_[length_ - 1] > data_binary.length() - pos) {
          ret = OB_ERR_UNEXPECTED;
          OB_LOG(WARN, "raw data len is invalid", K(ret), K(pos), K(length_), K(data_binary.length()));
        }
      }
    }
  } else {
    null_bitmaps_ = NULL;
    offsets_ = NULL;
    data_ = NULL;
  }

  if (OB_SUCC(ret) && 
      length_ != 0 && 
      (OB_ISNULL(null_bitmaps_) || OB_ISNULL(offsets_) || (offsets_[length_ - 1] != 0 && OB_ISNULL(data_)))) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "init failed", K(ret));
  }
  return ret;
}

int ObArrayBinary::init(ObDatum *attrs, uint32_t attr_count, bool with_length)
{
  int ret = OB_SUCCESS;
  const uint32_t count = with_length ? 4 : 3;
  if (OB_UNLIKELY(OB_ISNULL(attrs))) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "attrs is null", K(ret));
  } else if (attr_count != count) {
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
    data_ = const_cast<char *>(reinterpret_cast<const char *>(attrs[idx++].get_string().ptr()));
    if ((with_length && (length_ != attrs[1].get_int_bytes() / sizeof(uint8_t) || length_ != attrs[2].get_int_bytes() / sizeof(uint32_t)))
        || (!with_length && (length_ != attrs[1].get_int_bytes() / sizeof(uint32_t)))) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "unexpected attrs", K(ret), K(with_length), K(length_));
    }
  }
  if (OB_SUCC(ret) && 
      length_ != 0 && 
      (OB_ISNULL(null_bitmaps_) || OB_ISNULL(offsets_) || (offsets_[length_ - 1] != 0 && OB_ISNULL(data_)))) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "init failed", K(ret));
  }
  return ret;
}

int ObArrayBinary::push_null()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data_container_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "try to modify read-only array", K(ret));
  } else if (length_ + 1 > MAX_ARRAY_ELEMENT_SIZE) {
    ret = OB_SIZE_OVERFLOW;
    OB_LOG(WARN, "array element size exceed max", K(ret), K(length_), K(MAX_ARRAY_ELEMENT_SIZE));
  } else {
    uint32_t last_offset =  data_container_->raw_data_.size();
    if (OB_FAIL(data_container_->null_bitmaps_.push_back(1))) {
      // push back null
      OB_LOG(WARN, "failed to push null", K(ret));
    } else if (OB_FAIL(data_container_->offsets_.push_back(last_offset))) {
      OB_LOG(WARN, "failed to push value to array data", K(ret));
    } else if (get_raw_binary_len() > MAX_ARRAY_SIZE) {
      ret = OB_SIZE_OVERFLOW;
      OB_LOG(WARN, "array data length exceed max", K(ret), K(get_raw_binary_len()), K(MAX_ARRAY_SIZE));
    } else {
      length_++;
    }
  }
  return ret;
}

int ObArrayBinary::escape_append(ObStringBuffer &format_str, ObString elem_str)
{
  int ret = OB_SUCCESS;
  ObString split_str = elem_str.split_on('\"');
  if (OB_ISNULL(split_str.ptr())) {
    if (OB_FAIL(format_str.append(elem_str))) {
      OB_LOG(WARN, "fail to append string to format_str", K(ret));
    }
  } else {
    if (OB_FAIL(format_str.append(split_str))) {
      OB_LOG(WARN, "fail to append string to format_str", K(ret));
    } else if (OB_FAIL(format_str.append("\\\""))) {
      OB_LOG(WARN, "fail to append \\\" to format_str", K(ret));
    } else if (!elem_str.empty()) {
      ret = escape_append(format_str, elem_str);
    }
  }
  return ret;
}

int ObArrayBinary::print(ObStringBuffer &format_str, uint32_t begin, uint32_t print_size, bool print_whole) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(format_str.append("["))) {
    OB_LOG(WARN, "fail to append [", K(ret));
  } else {
    if (print_whole) {
      // print whole array
      print_size = length_;
    }
    if (OB_UNLIKELY(begin + print_size > length_)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "begin + print_size > length_", K(ret), K(begin), K(print_size), K(length_));
    }
    for (int i = begin; i < begin + print_size && OB_SUCC(ret); i++) {
      if (i > begin && OB_FAIL(format_str.append(","))) {
        OB_LOG(WARN, "fail to append \",\" to buffer", K(ret));
      } else if (null_bitmaps_[i]) {
          // value is null
          if (OB_FAIL(format_str.append("NULL"))) {
            OB_LOG(WARN, "fail to append NULL to buffer", K(ret));
          }
      } else if (OB_FAIL(format_str.append("\""))) {
        OB_LOG(WARN, "fail to append \"\"\" to buffer", K(ret));
      } else if (OB_FAIL(escape_append(format_str, (*this)[i]))) {
        OB_LOG(WARN, "fail to escape_append string to format_str", K(ret));
      } else if (OB_FAIL(format_str.append("\""))) {
        OB_LOG(WARN, "fail to append \"\"\" to buffer", K(ret));
      } 
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(format_str.append("]"))) {
    OB_LOG(WARN, "fail to append ]", K(ret));
  }
  return ret;
}

int ObArrayBinary::print_element(ObStringBuffer &format_str, uint32_t begin, uint32_t print_size, bool print_whole,
                                 ObString delimiter, bool has_null_str, ObString null_str) const
{
  int ret = OB_SUCCESS;
  if (print_whole) {
    // print whole array
    print_size = length_;
  }
  if (OB_UNLIKELY(begin + print_size > length_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "begin + print_size > length_", K(ret), K(begin), K(print_size), K(length_));
  }
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
      if (OB_FAIL(format_str.append((*this)[i]))) {
        OB_LOG(WARN, "fail to append string to format_str", K(ret));
      }
    }
  }
  return ret;
}

int ObArrayBinary::print_element_at(ObStringBuffer &format_str, uint32_t idx) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(idx >= length_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "unexpected idx", K(ret), K(idx), K(length_));
  } else if (null_bitmaps_[idx]) {
    // value is null
    if (OB_FAIL(format_str.append("NULL"))) {
      OB_LOG(WARN, "fail to append NULL to buffer", K(ret));
    }
  } else if (OB_FAIL(format_str.append("\""))) {
    OB_LOG(WARN, "fail to append \"\"\" to buffer", K(ret));
  } else if (OB_FAIL(escape_append(format_str, (*this)[idx]))) {
    OB_LOG(WARN, "fail to escape_append string to format_str", K(ret));
  } else if (OB_FAIL(format_str.append("\""))) {
    OB_LOG(WARN, "fail to append \"\"\" to buffer", K(ret));
  }
  return ret;
}

void ObArrayBinary::clear()
{ 
  data_ = nullptr;
  null_bitmaps_ = nullptr;
  offsets_ = nullptr;
  length_ = 0;
  if (OB_NOT_NULL(data_container_)) {
    data_container_->clear();
  } 
}

int ObArrayBinary::flatten(ObArrayAttr *attrs, uint32_t attr_count, uint32_t &attr_idx)
{
  int ret = OB_SUCCESS;
  const uint32_t len = 3;
  if (OB_UNLIKELY(OB_ISNULL(attrs))) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "attrs is null", K(ret));
  } else if (len + attr_idx >= attr_count) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "unexpected attr count", K(ret), K(attr_count), K(attr_idx), K(len));
  } else {
    attrs[attr_idx].ptr_ = reinterpret_cast<char *>(null_bitmaps_);
    attrs[attr_idx].length_ = sizeof(uint8_t) * length_;
    attr_idx++;
    attrs[attr_idx].ptr_ = reinterpret_cast<char *>(offsets_);
    attrs[attr_idx].length_ = sizeof(uint32_t) * length_;
    attr_idx++;
    attrs[attr_idx].ptr_ = data_;
    attrs[attr_idx].length_ = length_ > 0 ? offsets_[length_ - 1] : 0;
    attr_idx++;
  }
  return ret;
}

int ObArrayBinary::compare_at(uint32_t left_begin, uint32_t left_len,
                              uint32_t right_begin, uint32_t right_len,  
                              const ObIArrayType &right, int &cmp_ret) const
{
  int ret = OB_SUCCESS;
  uint32_t cmp_len = std::min(left_len, right_len);
  cmp_ret = 0;
  if (OB_UNLIKELY(left_begin + left_len > this->length_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "left_begin + left_len > this->length_", K(ret), K(left_begin), K(left_len), K(this->length_));
  } else if (OB_UNLIKELY(right_begin + right_len > right.size())) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "right_begin + right_len > right.size()", K(ret), K(right_begin), K(right_len), K(right.size()));
  }
  for (uint32_t i = 0; i < cmp_len && !cmp_ret && OB_SUCC(ret); ++i) {
    if (this->is_null(left_begin + i) && !right.is_null(right_begin + i)) {
      cmp_ret = 1;
    } else if (!this->is_null(left_begin + i) && right.is_null(right_begin + i)) {
      cmp_ret = -1;
    } else if (this->is_null(left_begin + i) && right.is_null(right_begin + i)) {
    } else {
      const ObArrayBinary *right_data = dynamic_cast<const ObArrayBinary *>(&right);
      uint32_t l_start = offset_at(left_begin + i, get_offsets());
      uint32_t l_child_len = get_offsets()[left_begin + i] - l_start;
      uint32_t r_start = right_data->offset_at(right_begin + i, right_data->get_offsets());
      uint32_t r_child_len = right_data->get_offsets()[right_begin + i] - r_start;
      if (OB_ISNULL(right_data)) {
        ret = OB_ERR_ARRAY_TYPE_MISMATCH;
        OB_LOG(WARN, "invalid array type", K(ret), K(right.get_format()), K(this->get_format()));
      } else {
        uint32_t data_len = std::min(l_child_len, r_child_len);
        cmp_ret = MEMCMP(data_ + l_start, right_data->get_data() + r_start, data_len);
        if (!cmp_ret && l_child_len != r_child_len) {
          cmp_ret = l_child_len > r_child_len ? 1 : -1;
        }
      }
    }
  }
  if (!cmp_ret && OB_SUCC(ret) && left_len != right_len) {
    cmp_ret = left_len > right_len ? 1 : -1;
  }
  return ret;
}

int ObArrayBinary::compare(const ObIArrayType &right, int &cmp_ret) const
{
  return compare_at(0, length_, 0, right.size(), right, cmp_ret);
}

bool ObArrayBinary::sort_cmp(uint32_t idx_l, uint32_t idx_r) const
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
int ObArrayBinary::contains_all(const ObIArrayType &other, bool &bret) const
{
  int ret = OB_SUCCESS;
  const ObArrayBinary *right_data = dynamic_cast<const ObArrayBinary *>(&other);
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

int ObArrayBinary::overlaps(const ObIArrayType &other, bool &bret) const
{
  int ret = OB_SUCCESS;
  const ObArrayBinary *right_data = dynamic_cast<const ObArrayBinary *>(&other);
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

int ObArrayBinary::clone_empty(ObIAllocator &alloc, ObIArrayType *&output, bool read_only) const
{
  int ret = OB_SUCCESS;
  void *buf = alloc.alloc(sizeof(ObArrayBinary));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "alloc memory failed", K(ret));
  } else {
    ObArrayBinary *arr_ptr = new (buf) ObArrayBinary();
    if (read_only) {
    } else if (OB_ISNULL(buf = alloc.alloc(sizeof(ObArrayData<char>)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "alloc memory failed", K(ret));
    } else {
      ObArrayData<char> *arr_data = new (buf) ObArrayData<char>(alloc);
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

int ObArrayBinary::distinct(ObIAllocator &alloc, ObIArrayType *&output) const
{
  int ret = OB_SUCCESS;
  ObIArrayType *arr_ptr = NULL;
  if (OB_FAIL(clone_empty(alloc, arr_ptr, false))) {
    OB_LOG(WARN, "clone empty failed", K(ret));
  } else if (this->contain_null() && OB_FAIL(arr_ptr->push_null())) {
    OB_LOG(WARN, "push null failed", K(ret));
  } else if (this->length_ == 0) {
    output = arr_ptr;
  } else {
    hash::ObHashSet<ObString> elem_set;
    ObArrayBinary *arr_bin_ptr = dynamic_cast<ObArrayBinary *>(arr_ptr);
    if (OB_ISNULL(arr_bin_ptr)) {
      ret = OB_ERR_ARRAY_TYPE_MISMATCH;
      OB_LOG(WARN, "invalid array type", K(ret), K(arr_ptr->get_format()));
    } else if (OB_FAIL(elem_set.create(this->length_, ObMemAttr(common::OB_SERVER_TENANT_ID, "ArrayDistSet")))) {
      OB_LOG(WARN, "failed to create cellid set", K(ret), K(this->length_));
    } else {
      for (uint32_t i = 0; i < this->length_ && OB_SUCC(ret); ++i) {
        if (this->is_null(i)) {
          // do nothing
        } else if (OB_FAIL(elem_set.exist_refactored((*this)[i]))) {
          if (ret == OB_HASH_NOT_EXIST) {
            if (OB_FAIL(arr_bin_ptr->push_back((*this)[i]))) {
              OB_LOG(WARN, "failed to add elemen", K(ret));
            } else if (OB_FAIL(elem_set.set_refactored((*this)[i]))) {
              OB_LOG(WARN, "failed to add elemen into set", K(ret));
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

int ObArrayBinary::push_not_in_set(const ObArrayBinary *arr_bin_ptr, 
                      hash::ObHashSet<ObString> &elem_set, 
                      bool &arr_contain_null,
                      const bool &contain_null)
{
  int ret = OB_SUCCESS;
  for (uint32_t i = 0; i < arr_bin_ptr->length_ && OB_SUCC(ret); ++i) {
    if (arr_bin_ptr->is_null(i)) {
      if (!contain_null && !arr_contain_null) {
        arr_contain_null = true;
        if (OB_FAIL(this->push_null())) {
          OB_LOG(WARN, "push null failed", K(ret));
        }
      }
    } else if (OB_FAIL(elem_set.exist_refactored((*arr_bin_ptr)[i]))) {
      if (ret == OB_HASH_NOT_EXIST) {
        if (OB_FAIL(this->push_back((*arr_bin_ptr)[i]))) {
          OB_LOG(WARN, "failed to add elemen", K(ret));
        } else if (OB_FAIL(elem_set.set_refactored((*arr_bin_ptr)[i]))) {
          OB_LOG(WARN, "failed to add elemen into set", K(ret));
        }
      } else if (ret == OB_HASH_EXIST) {
        // duplicate element, do nothing
        ret = OB_SUCCESS;
      } else {
        OB_LOG(WARN, "failed to check element exist", K(ret));
      }
    }
  }
  return ret;
}

int ObArrayBinary::except(ObIAllocator &alloc, ObIArrayType *arr2, ObIArrayType *&output) const
{
  int ret = OB_SUCCESS;
  ObIArrayType *arr_ptr = NULL;
  bool arr1_contain_null = false;
  bool arr2_contain_null = false;
  hash::ObHashSet<ObString> elem_set;
  ObArrayBinary *arr_bin_ptr = NULL;
  ObArrayBinary *arr2_bin_ptr = dynamic_cast<ObArrayBinary *>(arr2);

  if (OB_FAIL(clone_empty(alloc, arr_ptr, false))) {
    OB_LOG(WARN, "clone empty failed", K(ret));
  } else if (this->size() == 0) {
    output = arr_ptr;
  } else if (OB_ISNULL(arr_bin_ptr = dynamic_cast<ObArrayBinary *>(arr_ptr)) 
            || OB_ISNULL(arr2_bin_ptr)) {
    ret = OB_ERR_ARRAY_TYPE_MISMATCH;
    OB_LOG(WARN, "invalid array type", K(ret), K(arr_ptr->get_format()), K(arr2->get_format()));
  } else if (OB_FAIL(elem_set.create(arr2_bin_ptr->length_ + this->length_, 
                                  ObMemAttr(common::OB_SERVER_TENANT_ID, "ArrayDistSet")))) {
    OB_LOG(WARN, "failed to create cellid set", K(ret), K(arr2_bin_ptr->length_ + this->length_));
  } else {
    for (uint32_t i = 0; i < arr2_bin_ptr->length_ && OB_SUCC(ret); ++i) {
      if (arr2->is_null(i)) {
        arr2_contain_null = true;
      } else if (OB_FAIL(elem_set.exist_refactored((*arr2_bin_ptr)[i]))) {
        if (ret == OB_HASH_NOT_EXIST) {
          if (OB_FAIL(elem_set.set_refactored((*arr2_bin_ptr)[i]))) {
            OB_LOG(WARN, "failed to add elemen into set", K(ret));
          }
        } else if (ret == OB_HASH_EXIST) {
          // duplicate element, do nothing
          ret = OB_SUCCESS;
        } else {
          OB_LOG(WARN, "failed to check element exist", K(ret));
        }
      } 
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(arr_bin_ptr->push_not_in_set(this, elem_set, arr1_contain_null, arr2_contain_null))) {
      OB_LOG(WARN, "failed to push not in set", K(ret));
    } else {
      output = arr_ptr;
    }
  }
  return ret;
}

int ObArrayBinary::unionize(ObIAllocator &alloc, ObIArrayType **arr, uint32_t arr_cnt)
{
  int ret = OB_SUCCESS;
  bool arr_contain_null = false;
  hash::ObHashSet<ObString> elem_set;
  ObArrayBinary *arr_bin_ptr = NULL;

  for (int64_t i = 0; i < arr_cnt && OB_SUCC(ret); ++i) {
    if (OB_ISNULL(arr_bin_ptr = dynamic_cast<ObArrayBinary *>(arr[i]))) {
      ret = OB_ERR_ARRAY_TYPE_MISMATCH;
      OB_LOG(WARN, "invalid array type", K(ret), K(arr[i]->get_format()));
    } else if (arr_bin_ptr->size() == 0) {
      // skip
    } else if (!elem_set.created() && OB_FAIL(elem_set.create(arr_bin_ptr->length_, 
                                            ObMemAttr(common::OB_SERVER_TENANT_ID, "ArrayDistSet")))) {
      OB_LOG(WARN, "failed to create cellid set", K(ret));
    } else if (OB_FAIL(this->push_not_in_set(arr_bin_ptr, elem_set, arr_contain_null, false))) {
      OB_LOG(WARN, "failed to push not in set", K(ret));
    }
  } // end for
  return ret;
}

int ObArrayBinary::intersect(ObIAllocator &alloc, ObIArrayType **arr, uint32_t arr_cnt)
{
  int ret = OB_SUCCESS;
  uint64_t arr_null_number = 0;
  hash::ObHashMap<ObString, uint32_t> elem_map;
  ObArrayBinary *arr_bin_ptr = NULL;
  bool is_null_res = false;

  for (int64_t i = 0; i < arr_cnt && OB_SUCC(ret) && !is_null_res; ++i) {
    if (OB_ISNULL(arr_bin_ptr = dynamic_cast<ObArrayBinary *>(arr[i]))) {
      ret = OB_ERR_ARRAY_TYPE_MISMATCH;
      OB_LOG(WARN, "invalid array type", K(ret), K(arr[i]->get_format()));
    } else if (arr_bin_ptr ->size() == 0) {
      is_null_res = true;
    } else if (!elem_map.created() && elem_map.create(arr_bin_ptr->length_,
                                      ObMemAttr(common::OB_SERVER_TENANT_ID, "ArrayDistMap"))) {
      OB_LOG(WARN, "failed to create cellid map", K(ret));
    } else {
      bool arr_contain_null = false;
      uint32_t cnt = 0;
      for (uint32_t j = 0; j < arr_bin_ptr->length_ && OB_SUCC(ret); ++j) {
        if (arr_bin_ptr->is_null(j)) {
          if (!arr_contain_null) {
            arr_contain_null = true;
            arr_null_number++;
            if (arr_null_number == arr_cnt && OB_FAIL(this->push_null())) {
              OB_LOG(WARN, "push null failed", K(ret));
            }
          }
        } else if (OB_FAIL(elem_map.get_refactored((*arr_bin_ptr)[j], cnt))) {
          if (ret == OB_HASH_NOT_EXIST) {
            if (i == 0 && OB_FAIL(elem_map.set_refactored((*arr_bin_ptr)[j], 1))) {
              OB_LOG(WARN, "failed to add elemen into map", K(ret));
            } else {
              ret = OB_SUCCESS;
            }
          } else {
            OB_LOG(WARN, "failed to check element exist", K(ret));
          }
        } else {
          if (i == cnt) {
            if (i == arr_cnt - 1 && OB_FAIL(this->push_back((*arr_bin_ptr)[j]))) {
              OB_LOG(WARN, "failed to add elemen", K(ret));
            } else if (OB_FAIL(elem_map.erase_refactored((*arr_bin_ptr)[j]))) {
              OB_LOG(WARN, "failed to erase elemen from set", K(ret));
            } else if (OB_FAIL(elem_map.set_refactored((*arr_bin_ptr)[j], cnt + 1))) {
              OB_LOG(WARN, "failed to add elemen into set", K(ret));
            }
          } else if (i + 1 == cnt) {
            // do nothing
          } else if (OB_FAIL(elem_map.erase_refactored((*arr_bin_ptr)[j]))) {
            OB_LOG(WARN, "failed to erase elemen from set", K(ret));
          }
        }
      } // end for 
    }
  } // end for
  return ret;
}

} // namespace common
} // namespace oceanbase