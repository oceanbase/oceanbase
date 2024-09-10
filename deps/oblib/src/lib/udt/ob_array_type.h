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

#ifndef OCEANBASE_OB_ARRAY_TYPE_
#define OCEANBASE_OB_ARRAY_TYPE_
#include <stdint.h>
#include <string.h>
#include "lib/string/ob_string.h"
#include "lib/container/ob_vector.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/udt/ob_collection_type.h"
#include "lib/string/ob_string_buffer.h"
#include "lib/wide_integer/ob_wide_integer_str_funcs.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_fast_convert.h"
#include "rpc/obmysql/ob_mysql_global.h" // DOUBLE_TO_STRING_CONVERSION_BUFFER_SIZE
#include "src/share/datum/ob_datum.h"


namespace oceanbase {
namespace common {

static constexpr int64_t MAX_ARRAY_SIZE = (1 << 20) * 16; // 16M
static constexpr int64_t MAX_ARRAY_ELEMENT_SIZE = 2000000;
enum ArrayAttr {
  ATTR_LENGTH = 0,
  ATTR_NULL_BITMAP = 1,
  ATTR_OFFSETS = 2,
  ATTR_DATA = 3,
};

struct ObArrayAttr {
  const char *ptr_;
  uint32_t length_;
};

template<typename T>
class ObArrayData {
public :
    ObArrayData(ObIAllocator &allocator)
    : raw_data_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator, "ARRAYModule")),
      null_bitmaps_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator, "ARRAYModule")),
      offsets_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator, "ARRAYModule")) {}

  using Container = common::ObArray<T, ModulePageAllocator, false>;
  using NullContainer = common::ObArray<uint8_t, ModulePageAllocator, false>;
  using OffsetContainer = common::ObArray<uint32_t, ModulePageAllocator, false>;
  inline size_t data_length() { return raw_data_.size() * sizeof(T); }
  inline size_t nullbitmaps_length() { return null_bitmaps_.size() * sizeof(uint8_t); }
  inline size_t offsets_length() { return offsets_.size() * sizeof(uint32_t); }
  inline uint32_t offset_at(uint32_t idx) { return idx == 0 ? 0 : offsets_[idx - 1]; }
  void clear() { raw_data_.reset(); null_bitmaps_.reset(); offsets_.reset(); }

  Container raw_data_;
  NullContainer null_bitmaps_;
  OffsetContainer offsets_;
};

enum ArrayFormat {
  Fixed_Size = 0,
  Vector = 1,
  Binary_Varlen = 2,
  Nested_Array = 3,
  Array_MAX_FORMAT
};

class ObIArrayType {
public:
  virtual int print(const ObCollectionTypeBase *elem_type, ObStringBuffer &format_str,
                    uint32_t begin = 0, uint32_t print_size = 0) const = 0;
  virtual int32_t get_raw_binary_len() = 0;
  virtual int get_raw_binary(char *res_buf, int64_t buf_len) = 0;
  // without length_
  virtual int32_t get_data_binary_len() = 0;
  virtual int get_data_binary(char *res_buf, int64_t buf_len) = 0;
  virtual int init(ObString &raw_data) = 0;
  virtual int init(ObDatum *attrs, uint32_t attr_count, bool with_length = true) = 0;
  virtual int init() = 0; // init array with self data_container
  virtual void set_scale(ObScale scale) = 0;  // only for decimalint array
  virtual ArrayFormat get_format() const = 0;
  virtual uint32_t size() const = 0;
  virtual int check_validity(const ObCollectionArrayType &arr_type, const ObIArrayType &array) const = 0;
  virtual bool is_null(uint32_t idx) const = 0; // check if the idx-th element is null or not, idx validity is guaranteed by caller
  virtual int push_null() = 0;
  virtual bool contain_null() const = 0;
  virtual int insert_from(const ObIArrayType &src, uint32_t begin, uint32_t len) = 0;
  virtual int32_t get_element_type() const = 0;
  virtual char *get_data() const = 0;
  virtual uint32_t *get_offsets() const = 0;
  virtual uint8_t *get_nullbitmap() const = 0;
  virtual void set_element_type(int32_t type) = 0;
  virtual int at(uint32_t idx, ObIArrayType &dest) = 0;
  virtual void clear() = 0;
  virtual int flatten(ObArrayAttr *attrs, uint32_t attr_count, uint32_t &attr_idx) = 0;
  virtual int set_null_bitmaps(uint8_t *nulls, int64_t length) = 0;
  virtual int set_offsets(uint32_t *offsets, int64_t length) = 0;
  virtual int compare(const ObIArrayType &right, int &cmp_ret) = 0;
  virtual int compare_at(uint32_t left_begin, uint32_t left_len,
                         uint32_t right_begin, uint32_t right_len,
                         const ObIArrayType &right, int &cmp_ret) = 0;
};

template<typename T>
class ObArrayBase : public ObIArrayType {
public :
  ObArrayBase() : length_(0), element_type_(0), null_bitmaps_(nullptr), data_container_(nullptr) {}
  ObArrayBase(uint32_t length, int32_t elem_type, uint8_t *null_bitmaps)
    : length_(length), element_type_(elem_type), null_bitmaps_(null_bitmaps), data_container_(nullptr) {}

  uint32_t size() const { return length_; }
  bool contain_null() const
  {
    bool bret = false;
    for (int64_t i = 0; null_bitmaps_ != nullptr && !bret && i < length_; ++i) {
      if (null_bitmaps_[i] > 0) {
        bret = true;
      }
    }
    return bret;
  }
  int32_t get_element_type() const { return element_type_; }
  void set_element_type(int32_t type) { element_type_ = type;}
  uint8_t *get_nullbitmap() const { return null_bitmaps_;}
  // make sure null_bitmaps_ isn't nullptr and idx is less than length_
  bool is_null(uint32_t idx) const { return null_bitmaps_[idx] > 0; }
  // make sure offsets isn't nullptr and idx is less than length
  uint32_t offset_at(uint32_t idx, uint32_t *offsets) const { return idx == 0 ? 0 : offsets[idx - 1]; }
  inline void set_array_data(ObArrayData<T> *arr_data) { data_container_ = arr_data;}
  int set_null_bitmaps(uint8_t *nulls, int64_t length)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(data_container_)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "try to modify read-only array", K(ret));
    } else {
      int64_t curr_pos = data_container_->null_bitmaps_.size();
      int64_t capacity = curr_pos + length;
      data_container_->null_bitmaps_.prepare_allocate(capacity);
      uint8_t *cur_null_bitmap = data_container_->null_bitmaps_.get_data() + curr_pos;
      MEMCPY(cur_null_bitmap, nulls, length * sizeof(uint8_t));
    }
    return ret;
  }
  int set_offsets(uint32_t *offsets, int64_t length)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(data_container_)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "try to modify read-only array", K(ret));
    } else {
      int64_t curr_pos = data_container_->offsets_.size();
      int64_t capacity = curr_pos + length;
      data_container_->offsets_.prepare_allocate(capacity);
      char *cur_offsets =  reinterpret_cast<char *>(data_container_->offsets_.get_data() + curr_pos * sizeof(uint32_t));
      MEMCPY(cur_offsets, offsets, length * sizeof(uint32_t));
    }
    return ret;
  }
  int get_reserved_data(int64_t length, T *&data)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(data_container_)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "try to modify read-only array", K(ret));
    } else {
      int64_t curr_pos = data_container_->raw_data_.size();
      int64_t capacity = curr_pos + length;
      data_container_->raw_data_.prepare_allocate(capacity);
      data = reinterpret_cast<T *>(data_container_->raw_data_.get_data() + curr_pos);
    }
    return ret;
  }

protected :
  uint32_t length_;
  int32_t element_type_;
  uint8_t *null_bitmaps_;
  ObArrayData<T> *data_container_;
};

template<typename T>
class ObArrayFixedSize : public ObArrayBase<T> {
public :
  ObArrayFixedSize() : ObArrayBase<T>(), data_(nullptr), scale_(0) {}
  ObArrayFixedSize(uint32_t length, int32_t elem_type, uint8_t *null_bitmaps, T *data, uint32_t scale = 0)
    : ObArrayBase<T>(length, elem_type, null_bitmaps),
      data_(data), scale_(scale) {}
  inline void set_data(T *data, uint32_t len) { data_ = data; this->length_ = len;}
  inline int16_t get_scale() { return scale_; }
  void set_scale(ObScale scale) { scale_ = scale; }  // only for decimalint array
  T operator[](const int64_t i) const { return data_[i]; }
  ObDecimalInt *get_decimal_int(const int64_t i) { return (ObDecimalInt *)(data_ + i); }
  ArrayFormat get_format() const { return ArrayFormat::Fixed_Size; }
  uint32_t *get_offsets() const { return nullptr; }
  char *get_data() const { return reinterpret_cast<char*>(data_);}
  int check_validity(const ObCollectionArrayType &arr_type, const ObIArrayType &array) const { return OB_SUCCESS; }
  int push_null()
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
  int push_back(T value, bool is_null = false)
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

  int print(const ObCollectionTypeBase *elem_type, ObStringBuffer &format_str,
            uint32_t begin = 0, uint32_t print_size = 0) const
  {
    int ret = OB_SUCCESS;
    const ObCollectionBasicType *basic_type = dynamic_cast<const ObCollectionBasicType *>(elem_type);
    if (OB_ISNULL(basic_type)) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "invalid argument", K(ret));
    } else if (OB_FAIL(format_str.append("["))) {
      OB_LOG(WARN, "fail to append [", K(ret));
    } else {
      if (print_size == 0) {
        // print whole element
        print_size = this->length_;
      }
      ObObjType obj_type = basic_type->basic_meta_.get_obj_type();
      for (int i = begin; i < begin + print_size && OB_SUCC(ret); i++) {
        if (i > begin && OB_FAIL(format_str.append(","))) {
          OB_LOG(WARN, "fail to append \",\" to buffer", K(ret));
        } else if (this->null_bitmaps_[i]) {
          // value is null
          if (OB_FAIL(format_str.append("NULL"))) {
            OB_LOG(WARN, "fail to append NULL to buffer", K(ret));
          }
        } else {
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
            case ObFloatType :
            case ObDoubleType : {
              int buf_size = obj_type == ObFloatType ? FLOAT_TO_STRING_CONVERSION_BUFFER_SIZE : DOUBLE_TO_STRING_CONVERSION_BUFFER_SIZE;
              if (OB_FAIL(format_str.reserve(buf_size + 1))) {
                OB_LOG(WARN, "fail to reserve memory for format_str", K(ret));
              } else {
                char *start = format_str.ptr() + format_str.length();
                uint64_t len = ob_gcvt(data_[i],
                                       obj_type == ObFloatType ? ob_gcvt_arg_type::OB_GCVT_ARG_FLOAT : ob_gcvt_arg_type::OB_GCVT_ARG_DOUBLE,
                                       buf_size, start, NULL);
                if (OB_FAIL(format_str.set_length(format_str.length() + len))) {
                  OB_LOG(WARN, "fail to set format_str len", K(ret), K(format_str.length()), K(len));
                }
              }
              break;
            }
            case ObDecimalIntType : {
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
    if (OB_SUCC(ret) && OB_FAIL(format_str.append("]"))) {
      OB_LOG(WARN, "fail to append ]", K(ret));
    }
    return ret;
  }


  int32_t get_data_binary_len()
  {
    if (this->data_container_ == NULL) {
      return this->length_ * sizeof(uint8_t) + this->length_ * sizeof(T);
    }
    return sizeof(uint8_t) * this->data_container_->null_bitmaps_.size()
                                         + sizeof(T) * this->data_container_->raw_data_.size();
  }
  int get_data_binary(char *res_buf, int64_t buf_len)
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
  int32_t get_raw_binary_len() { return sizeof(this->length_) + get_data_binary_len(); }
  int get_raw_binary(char *res_buf, int64_t buf_len)
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

  int init()
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

  int init(ObString &raw_data)
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

  int init(ObDatum *attrs, uint32_t attr_count, bool with_length = true)
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
  int insert_from(const ObIArrayType &src, uint32_t begin, uint32_t len)
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
      this->data_container_->raw_data_.prepare_allocate(capacity);
      char *cur_data = reinterpret_cast<char *>(this->data_container_->raw_data_.get_data() + curr_pos);
      MEMCPY(cur_data, src.get_data() + src_data_offset, len * sizeof(T));
      // insert nullbitmaps
      curr_pos = this->data_container_->null_bitmaps_.size();
      capacity = curr_pos + len;
      this->data_container_->null_bitmaps_.prepare_allocate(capacity);
      uint8_t *cur_null_bitmap = this->data_container_->null_bitmaps_.get_data() + curr_pos;
      MEMCPY(cur_null_bitmap, src.get_nullbitmap() + src_null_offset, len * sizeof(uint8_t));
      this->length_ += len;
    }
    return ret;
  }
  int at(uint32_t idx, ObIArrayType &dest) { return OB_NOT_SUPPORTED; }
  void clear()
  {
    data_ = nullptr;
    this->null_bitmaps_ = nullptr;
    this->length_ = 0;
    if (OB_NOT_NULL(this->data_container_)) {
      this->data_container_->clear();
    }
  }
  int flatten(ObArrayAttr *attrs, uint32_t attr_count, uint32_t &attr_idx)
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

  int compare_at(uint32_t left_begin, uint32_t left_len, uint32_t right_begin, uint32_t right_len,
                 const ObIArrayType &right, int &cmp_ret)
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

  int compare(const ObIArrayType &right, int &cmp_ret)
  {
    return compare_at(0, this->length_, 0, right.size(), right, cmp_ret);
  }
  template<typename Elem_Type>
  int contains(const Elem_Type &elem, bool &bret) const
  {
    int ret = OB_SUCCESS;
    bret = false;
    for (uint32_t i = 0; i < this->length_ && !bret; ++i) {
      if (this->is_null(i)) {
      } else if (static_cast<Elem_Type>(this->data_[i]) == elem) {
        bret = true;
      }
    }
    return ret;
  }
  template <>
  int contains<ObString>(const ObString &elem, bool &bret) const
  {
    return OB_INVALID_ARGUMENT;
  }
  template <>
  int contains<ObIArrayType>(const ObIArrayType &elem, bool &bret) const
  {
    return OB_INVALID_ARGUMENT;
  }

private :
  T *data_;
  int16_t scale_; // only for decimalint type
};

class ObVectorData : public ObArrayBase<float> {
public :
  ObVectorData() : ObArrayBase(), data_(nullptr) {}
  ObVectorData(uint32_t length, float *data)
    : ObArrayBase(length, ObFloatType, nullptr),
      data_(data) {}
  float operator[](const int64_t i) const { return data_[i]; }
  ArrayFormat get_format() const { return ArrayFormat::Vector; }
  int push_back(float value);
  void set_scale(ObScale scale) { UNUSED(scale); }
  int print(const ObCollectionTypeBase *elem_type, ObStringBuffer &format_str,
            uint32_t begin = 0, uint32_t print_size = 0) const;
  uint32_t *get_offsets() const { return nullptr; }
  char *get_data() const { return reinterpret_cast<char*>(data_);}
  int32_t get_raw_binary_len()
  {
    return this->data_container_ == NULL ? (this->length_ * sizeof(float)) : (sizeof(float) * data_container_->raw_data_.size());
  }
  int get_raw_binary(char *res_buf, int64_t buf_len);
  int32_t get_data_binary_len() { return get_raw_binary_len(); }
  int get_data_binary(char *res_buf, int64_t buf_len) { return get_raw_binary(res_buf, buf_len); }
  int init ();
  int init(ObString &raw_data);
  int init(ObDatum *attrs, uint32_t attr_count, bool with_length = true);
  int check_validity(const ObCollectionArrayType &arr_type, const ObIArrayType &array) const;
  int push_null() { return OB_ERR_NULL_VALUE; }
  int insert_from(const ObIArrayType &src, uint32_t begin, uint32_t len);
  int at(uint32_t idx, ObIArrayType &dest) { return OB_NOT_SUPPORTED; }
  void clear();
  int flatten(ObArrayAttr *attrs, uint32_t attr_count, uint32_t &attr_idx);
  int compare_at(uint32_t left_begin, uint32_t left_len, uint32_t right_begin, uint32_t right_len,
                 const ObIArrayType &right, int &cmp_ret);
  int compare(const ObIArrayType &right, int &cmp_ret);
  template<typename Elem_Type>
  int contains(const Elem_Type &elem, bool &bret) const
  {
    int ret = OB_SUCCESS;
    bret = false;
    for (uint32_t i = 0; i < length_ && !bret; ++i) {
      if (static_cast<Elem_Type>(data_[i]) == elem) {
        bret = true;
      }
    }
    return ret;
  }
  template <>
  int contains<ObString>(const ObString &elem, bool &bret) const
  {
    return OB_INVALID_ARGUMENT;
  }
  template <>
  int contains<ObIArrayType>(const ObIArrayType &elem, bool &bret) const
  {
    return OB_INVALID_ARGUMENT;
  }

private :
  float *data_;
};

class ObArrayBinary : public ObArrayBase<char> {
public :
  ObArrayBinary() : ObArrayBase(), offsets_(nullptr), data_(nullptr) {}
  ObArrayBinary(uint32_t length, int32_t elem_type, uint8_t *null_bitmaps, uint32_t *offsets, char *data)
    : ObArrayBase(length, elem_type, null_bitmaps),
      offsets_(offsets), data_(data) {}
  ObString operator[](const int64_t i) const;
  ArrayFormat get_format() const { return ArrayFormat::Binary_Varlen; }
  uint32_t *get_offsets() const { return offsets_; }
  char *get_data() const { return data_;}
  int push_back(const ObString &value, bool is_null = false);
  void set_scale(ObScale scale) { UNUSED(scale); }
  int print(const ObCollectionTypeBase *elem_type, ObStringBuffer &format_str,
            uint32_t begin = 0, uint32_t print_size = 0) const;

  int32_t get_data_binary_len()
  {
    int32_t len = 0;
    if (this->data_container_ == NULL) {
      uint32_t last_idx = this->length_ > 0 ? this->length_ - 1 : 0;
      len = this->length_ * sizeof(uint8_t) + this->length_ * sizeof(uint32_t) + this->offsets_[last_idx];
    } else {
      len = sizeof(uint8_t) * data_container_->null_bitmaps_.size()
                                        + sizeof(uint32_t) * data_container_->offsets_.size()
                                        + data_container_->raw_data_.size();
    }
    return len;
  }
  int get_data_binary(char *res_buf, int64_t buf_len);
  int32_t get_raw_binary_len() { return sizeof(length_) + get_data_binary_len(); }
  int get_raw_binary(char *res_buf, int64_t buf_len);
  int init();
  int init(ObString &raw_data);
  int init(ObDatum *attrs, uint32_t attr_count, bool with_length = true);
  int check_validity(const ObCollectionArrayType &arr_type, const ObIArrayType &array) const { return OB_SUCCESS; }
  int push_null();
  int insert_from(const ObIArrayType &src, uint32_t begin, uint32_t len);
  int at(uint32_t idx, ObIArrayType &dest) { return OB_NOT_SUPPORTED; }
  void clear();
  int flatten(ObArrayAttr *attrs, uint32_t attr_count, uint32_t &attr_idx);
  int compare(const ObIArrayType &right, int &cmp_ret);
  int compare_at(uint32_t left_begin, uint32_t left_len, uint32_t right_begin, uint32_t right_len,
                 const ObIArrayType &right, int &cmp_ret);
  template<typename T>
  int contains(const T &elem, bool &bret) const
  {
    int ret = OB_SUCCESS;
    bret = false;
    const ObString *str = nullptr;
    if (typeid(T) != typeid(ObString)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "invalid data type", K(ret));
    } else {
      str = reinterpret_cast<const ObString *>(&elem);
      for (int i = 0; i < length_ && !bret; i++) {
        if ((*this)[i].compare(*str) == 0) {
          bret = true;
        }
      }
    }
    return ret;
  }

private :
  uint32_t *offsets_;
  char *data_;
};

class ObArrayNested : public ObArrayBase<char> {
public :
  ObArrayNested() : ObArrayBase(), offsets_(nullptr), data_(nullptr) {}
  ObArrayNested(uint32_t length, int32_t elem_type, uint8_t *null_bitmaps, uint32_t *offsets, ObArrayBase *data)
    : ObArrayBase(length, elem_type, null_bitmaps),
      offsets_(offsets), data_(data) {}
  ArrayFormat get_format() const { return ArrayFormat::Nested_Array; }
  uint32_t *get_offsets() const { return offsets_; }
  char *get_data() const { return data_->get_data();}
  void set_scale(ObScale scale) { UNUSED(scale); }
  int print(const ObCollectionTypeBase *elem_type, ObStringBuffer &format_str,
            uint32_t begin = 0, uint32_t print_size = 0) const;

  int32_t get_data_binary_len()
  {
    return this->data_container_ == NULL ? (this->length_ * sizeof(uint8_t) + this->length_ * sizeof(uint32_t) + data_->get_data_binary_len())
      : (sizeof(uint8_t) * data_container_->null_bitmaps_.size()
                                        + sizeof(uint32_t) * data_container_->offsets_.size()
                                        + data_->get_data_binary_len());
  }
  int get_data_binary(char *res_buf, int64_t buf_len);

  int32_t get_raw_binary_len() { return sizeof(length_) + get_data_binary_len(); }
  int get_raw_binary(char *res_buf, int64_t buf_len);
  int init();
  int init(ObString &raw_data);
  int init(ObDatum *attrs, uint32_t attr_count, bool with_length = true);
  int check_validity(const ObCollectionArrayType &arr_type, const ObIArrayType &array) const { return OB_SUCCESS; }
  int push_null();
  inline void set_child_array(ObIArrayType *child) { data_ = static_cast<ObArrayBase *>(child);}
  inline ObIArrayType *get_child_array() const { return static_cast<ObIArrayType *>(data_);}
  int insert_from(const ObIArrayType &src, uint32_t begin, uint32_t len);
  int push_back(const ObIArrayType &src, bool is_null = false);
  int at(uint32_t idx, ObIArrayType &dest);
  void clear();
  int flatten(ObArrayAttr *attrs, uint32_t attr_count, uint32_t &attr_idx);
  int compare_at(uint32_t left_begin, uint32_t left_len, uint32_t right_begin, uint32_t right_len,
                 const ObIArrayType &right, int &cmp_ret);
  int compare(const ObIArrayType &right, int &cmp_ret);
  template<typename T>
  int contains(const T &elem, bool &bret) const
  {
    int ret = OB_SUCCESS;
    bret = false;
    const ObIArrayType *elem_ptr = NULL;
    if (typeid(T) != typeid(ObIArrayType)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "invalid data type", K(ret));
    } else {
      elem_ptr = reinterpret_cast<const ObIArrayType *>(&elem);
    }
    for (uint32_t i = 0; i < length_ && !bret && OB_SUCC(ret); ++i) {
      if (this->is_null(i)) {
      } else {
        uint32_t l_start = offset_at(i, get_offsets());
        uint32_t l_child_len = get_offsets()[i] - l_start;
        int cmp_ret = 0;
        if (OB_FAIL(get_child_array()->compare_at(l_start, l_child_len, 0, elem_ptr->size(), *elem_ptr, cmp_ret))) {
          OB_LOG(WARN, "failed to do nested array contains", K(ret));
        } else if (cmp_ret == 0) {
          bret = true;
        }
      }
    }
    return ret;
  }

private :
  uint32_t *offsets_;
  ObArrayBase *data_;
  // data_container: only maintain null_bitmaps_ and offsets_, raw_data is in the child element
};

class ObArrayTypeObjFactory
{
public:
  ObArrayTypeObjFactory() {};
  virtual ~ObArrayTypeObjFactory() {};
  static int construct(common::ObIAllocator &alloc, const ObCollectionTypeBase  &array_meta, ObIArrayType *&arr_obj, bool read_only = false);
private:
  DISALLOW_COPY_AND_ASSIGN(ObArrayTypeObjFactory);
};


#define FIXED_ARRAY_OBJ_CONTAINS(Element_Type)                                                                 \
  const ObArrayFixedSize<Element_Type> *arr_ptr = static_cast<const ObArrayFixedSize<Element_Type> *>(&array); \
  if (OB_FAIL(arr_ptr->contains(elem, bret))) {                                                                \
    OB_LOG(WARN, "array contains failed", K(ret));                                     \
  }
class ObArrayUtil
{
public :
  static int get_type_name(const ObDataType &elem_type, char *buf, int buf_len, uint32_t depth = 1);
  static int push_back_decimal_int(const ObPrecision prec, const ObDecimalInt *dec_val, bool is_null, ObIArrayType *arr_obj);
  template<typename Elem_Type>
  static int contains(const ObIArrayType &array, const Elem_Type &elem, bool &bret)
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
      if (OB_FAIL(static_cast<const ObArrayBinary *>(&array)->contains(elem, bret))) {
        OB_LOG(WARN, "failed to do array contains", K(ret));
      }
      break;
    case ArrayFormat::Vector :
      if (OB_FAIL(static_cast<const ObVectorData *>(&array)->contains(elem, bret))) {
        OB_LOG(WARN, "failed to do array contains", K(ret));
      }
      break;
    case ArrayFormat::Nested_Array :
      if (OB_FAIL(static_cast<const ObArrayNested *>(&array)->contains(elem, bret))) {
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
};
#undef FIXED_ARRAY_OBJ_CONTAINS

} // namespace common
} // namespace oceanbase
#endif // OCEANBASE_OB_ARRAY_TYPE_
