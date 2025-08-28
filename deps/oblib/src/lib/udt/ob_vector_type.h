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

#ifndef OCEANBASE_OB_VECTOR_TYPE_
#define OCEANBASE_OB_VECTOR_TYPE_
#include "lib/udt/ob_array_type.h"

namespace oceanbase {
namespace common {
template <typename T>
class ObVectorData : public ObArrayBase<T> {
public :
  ObVectorData() : ObArrayBase<T>(), data_(nullptr) {}
  ObVectorData(uint32_t length, int32_t elem_type, T *data)
    : ObArrayBase<T>(length, elem_type, nullptr),
      data_(data) {}
  virtual ~ObVectorData() = default;

  virtual int print(ObStringBuffer &format_str,
            uint32_t begin = 0, uint32_t print_size = 0,
            bool print_whole = true) const = 0;
  virtual int print_element(ObStringBuffer &format_str,
                    uint32_t begin = 0, uint32_t print_size = 0,
                    bool print_whole = true,
                    ObString delimiter = ObString(","),
                    bool has_null_str = true, ObString null_str = ObString("NULL")) const = 0;
  virtual int print_element_at(ObStringBuffer &format_str, uint32_t idx) const = 0;
  virtual int clone_empty(ObIAllocator &alloc, ObIArrayType *&output, bool read_only = true) const = 0;
  virtual int elem_at(uint32_t idx, ObObj &elem_obj) const = 0;

public:
  T operator[](const int64_t i) const { return data_[i]; }
  uint32_t cardinality() const { return this->length_; }
  ArrayFormat get_format() const { return ArrayFormat::Vector; }
  int push_back(T value);
  bool is_null(uint32_t idx) const { return false; }
  void set_scale(ObScale scale) { UNUSED(scale); }
  void set_data(T *data, uint32_t length) { data_ = data; this->length_ = length;}
  
  uint32_t *get_offsets() const { return nullptr; }
  char *get_data() const { return reinterpret_cast<char*>(data_);}
  int32_t get_raw_binary_len()
  { 
    return this->data_container_ == NULL ? 
      (this->length_ * sizeof(T)) : (sizeof(T) * this->data_container_->raw_data_.size());
  }
  int get_raw_binary(char *res_buf, int64_t buf_len);
  int32_t get_data_binary_len() { return get_raw_binary_len(); }
  int get_data_binary(char *res_buf, int64_t buf_len) { return get_raw_binary(res_buf, buf_len); }
  int hash(uint64_t &hash_val) const;
  int init();
  int init(ObString &raw_data);
  int init(uint32_t length, ObString &data_binary);
  int init(ObDatum *attrs, uint32_t attr_count, bool with_length = true);
  int check_validity(const ObCollectionTypeBase &coll_type, const ObIArrayType &array) const;
  int push_null() { return OB_ERR_NULL_VALUE; }
  int insert_from(const ObIArrayType &src, uint32_t begin, uint32_t len);
  int at(uint32_t idx, ObIArrayType &dest) const { return OB_NOT_SUPPORTED; }
  void clear();
  int flatten(ObArrayAttr *attrs, uint32_t attr_count, uint32_t &attr_idx);
  int compare_at(uint32_t left_begin, uint32_t left_len, uint32_t right_begin, uint32_t right_len,  
                 const ObIArrayType &right, int &cmp_ret) const;
  int compare(const ObIArrayType &right, int &cmp_ret) const;
  bool sort_cmp(uint32_t idx_l, uint32_t idx_r) const { return operator[](idx_l) < operator[](idx_r); }
  template<typename Elem_Type>
  int contains(const Elem_Type &elem, int &pos) const
  {
    int ret = OB_SUCCESS;
    pos = -1;
    for (uint32_t i = 0; i < this->length_ && pos < 0; ++i) {
      if (static_cast<Elem_Type>(data_[i]) == elem) {
        pos = i;
      }
    }
    return ret;
  }
  template <>
  int contains<ObString>(const ObString &elem, int &pos) const
  {
    return OB_INVALID_ARGUMENT;
  }
  template <>
  int contains<ObIArrayType>(const ObIArrayType &elem, int &pos) const
  {
    return OB_INVALID_ARGUMENT;
  }
  int contains_all(const ObIArrayType &other, bool &bret) const;
  int overlaps(const ObIArrayType &other, bool &bret) const;
  int distinct(ObIAllocator &alloc, ObIArrayType *&output) const;
  int push_not_in_set(const ObVectorData *arr_bin_ptr, 
          hash::ObHashSet<ObString> &elem_set, 
          bool &arr_contain_null,
          const bool &contain_null);
  int except(ObIAllocator &alloc, ObIArrayType *arr2, ObIArrayType *&output) const;
  int unionize(ObIAllocator &alloc, ObIArrayType **arr, uint32_t arr_cnt);
  int intersect(ObIAllocator &alloc, ObIArrayType **arr, uint32_t arr_cnt);

  template<typename Elem_Type>
  int clone_except(ObIAllocator &alloc, const Elem_Type *elem_except, bool is_null, ObIArrayType *&output) const
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(clone_empty(alloc, output, false))) {
      OB_LOG(WARN, "clone empty failed", K(ret));
    } else if (OB_UNLIKELY(OB_ISNULL(elem_except))) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "elem except is null", K(ret));
    } else {
      ObVectorData *arr_data = dynamic_cast<ObVectorData *>(output);
      if (OB_ISNULL(arr_data)) {
        ret = OB_ERR_ARRAY_TYPE_MISMATCH;
        OB_LOG(WARN, "invalid array type", K(ret), K(this->get_format()));
      }
      for (uint32_t i = 0; i < this->length_ && OB_SUCC(ret); ++i) {
        if (static_cast<Elem_Type>(data_[i]) != *elem_except && OB_FAIL(arr_data->push_back(data_[i]))) {
          OB_LOG(WARN, "failed to add element", K(ret));
        }
      }
    }
    return ret;
  }
  template <>
  int clone_except<ObString>(ObIAllocator &alloc, const ObString *elem_except, bool is_null, ObIArrayType *&output) const
  {
    return OB_INVALID_ARGUMENT;
  }
  template <>
  int clone_except<ObIArrayType>(ObIAllocator &alloc, const ObIArrayType *elem_except, bool is_null, ObIArrayType *&output) const
  {
    return OB_INVALID_ARGUMENT;
  }

protected:
  T *data_;
};

class ObVectorF32Data : public ObVectorData<float> {
public:
  ObVectorF32Data() {}
  ObVectorF32Data(uint32_t length, float *data)
    : ObVectorData(length, ObFloatType, data) {}
  virtual ~ObVectorF32Data() = default;
  virtual int print(ObStringBuffer &format_str,
            uint32_t begin = 0, uint32_t print_size = 0,
            bool print_whole = true) const override;
  virtual int print_element(ObStringBuffer &format_str,
                    uint32_t begin = 0, uint32_t print_size = 0,
                    bool print_whole = true,
                    ObString delimiter = ObString(","),
                    bool has_null_str = true, ObString null_str = ObString("NULL")) const override;
  virtual int print_element_at(ObStringBuffer &format_str, uint32_t idx) const override;
  virtual int clone_empty(ObIAllocator &alloc, ObIArrayType *&output, bool read_only = true) const override;
  virtual int elem_at(uint32_t idx, ObObj &elem_obj) const override;
};

class ObVectorU8Data : public ObVectorData<uint8_t> {
public:
  ObVectorU8Data() {}
  ObVectorU8Data(uint32_t length, uint8_t *data)
    : ObVectorData(length, ObUTinyIntType, data) {}
  virtual ~ObVectorU8Data() = default;
  virtual int print(ObStringBuffer &format_str,
            uint32_t begin = 0, uint32_t print_size = 0,
            bool print_whole = true) const override;
  virtual int print_element(ObStringBuffer &format_str,
                    uint32_t begin = 0, uint32_t print_size = 0,
                    bool print_whole = true,
                    ObString delimiter = ObString(","),
                    bool has_null_str = true, ObString null_str = ObString("NULL")) const override;
  virtual int print_element_at(ObStringBuffer &format_str, uint32_t idx) const override;
  virtual int clone_empty(ObIAllocator &alloc, ObIArrayType *&output, bool read_only = true) const override;
  virtual int elem_at(uint32_t idx, ObObj &elem_obj) const override;
};

//////////////////////////////////
// implement of ObVectorData<T> //
//////////////////////////////////
template <typename T>
int ObVectorData<T>::push_back(T value)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(this->data_container_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "try to modify read-only array", K(ret));
  } else if (this->length_ + 1 > MAX_ARRAY_ELEMENT_SIZE) {
    ret = OB_SIZE_OVERFLOW;
    OB_LOG(WARN, "array element size exceed max", K(ret), K(this->length_), K(MAX_ARRAY_ELEMENT_SIZE));
  } else if (OB_FAIL(this->data_container_->raw_data_.push_back(value))) {
    OB_LOG(WARN, "failed to push value to array data", K(ret));
  } else if (get_raw_binary_len() > MAX_ARRAY_SIZE) {
    ret = OB_SIZE_OVERFLOW;
    OB_LOG(WARN, "vector data length exceed max", K(ret), K(get_raw_binary_len()), K(MAX_ARRAY_SIZE));
  } else {
    this->length_++;
  }
  return ret;
}

template <typename T>
int ObVectorData<T>::get_raw_binary(char *res_buf, int64_t buf_len)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_ISNULL(res_buf))) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "res_buf is null", K(ret));
  } else if (get_raw_binary_len() > buf_len) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "buf len isn't enough", K(ret), K(buf_len));
  } else if (this->data_container_ == NULL) {
    MEMCPY(res_buf, reinterpret_cast<char *>(data_), sizeof(T) * this->length_);
  } else {
    MEMCPY(res_buf,
        reinterpret_cast<char *>(this->data_container_->raw_data_.get_data()),
        sizeof(T) * this->data_container_->raw_data_.size());
  }
  return ret;
}

template <typename T>
int ObVectorData<T>::hash(uint64_t &hash_val) const
{
  T *data = this->data_;
  if (this->data_container_ != NULL) {
    data = this->data_container_->raw_data_.get_data();
  }
  hash_val = common::murmurhash(&this->length_, sizeof(this->length_), hash_val);
  hash_val = common::murmurhash(data, sizeof(T) * this->length_, hash_val);
  return OB_SUCCESS;
}

template <typename T>
int ObVectorData<T>::init()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(this->data_container_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "try to modify read-only array", K(ret));
  } else {
    this->length_ = this->data_container_->raw_data_.size();
    data_ = this->data_container_->raw_data_.get_data();
  }
  if (OB_SUCC(ret) && this->length_ != 0 && OB_ISNULL(data_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "init failed", K(ret));
  }
  return ret;
}

template <typename T>
int ObVectorData<T>::init(ObString &raw_data)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  char *raw_str = raw_data.ptr();
  if (raw_data.length() % sizeof(T) != 0) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "raw data len is invalid", K(ret), K(raw_data.length()));
  } else {
    this->length_ = raw_data.length() / sizeof(T);
    data_ = reinterpret_cast<T *>(raw_str);
  }
  if (OB_SUCC(ret) && this->length_ != 0 && OB_ISNULL(data_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "init failed", K(ret));
  }
  return ret;
}

template <typename T>
int ObVectorData<T>::init(uint32_t length, ObString &data_binary)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObVectorData<T>::init(data_binary))) {
    OB_LOG(WARN, "init failed", K(ret), K(data_binary));
  } else if (this->length_ != length) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "length is invalid", K(ret), K(length), K(this->length_));
  }
  return ret;
}

template <typename T>
int ObVectorData<T>::init(ObDatum *attrs, uint32_t attr_count, bool with_length)
{
  int ret = OB_SUCCESS;
  // attrs of vector are same as array now, maybe optimize later
  const uint32_t count = with_length ? 3 : 2;
  if (OB_UNLIKELY(OB_ISNULL(attrs))) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "attrs is null", K(ret));
  } else if (attr_count != count) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "unexpected attrs", K(ret), K(attr_count), K(count));
  } else {
    data_ = const_cast<T *>(reinterpret_cast<const T *>(attrs[count - 1].get_string().ptr()));
    this->length_ = attrs[count - 1].get_int_bytes() / sizeof(T);
  }
  if (OB_SUCC(ret) && this->length_ != 0 && OB_ISNULL(data_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "init failed", K(ret));
  }
  return ret;
}

template <typename T>
int ObVectorData<T>::check_validity(const ObCollectionTypeBase &coll_type, const ObIArrayType &array) const
{
  int ret = OB_SUCCESS;
  const ObCollectionArrayType *arr_type = dynamic_cast<const ObCollectionArrayType *>(&coll_type);
  if (OB_ISNULL(arr_type)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "invalid array type", K(ret), K(arr_type), K(coll_type));
  } else if (arr_type->dim_cnt_ != array.size()) {
    ret = OB_ERR_INVALID_VECTOR_DIM;
    OB_LOG(WARN, "invalid vector dimension", K(ret), K(arr_type->dim_cnt_), K(array.size()));
  }
  return ret;
}

template <typename T>
int ObVectorData<T>::insert_from(const ObIArrayType &src, uint32_t begin, uint32_t len)
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
  } else if (OB_UNLIKELY(begin + len > src.size())) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "unexpected begin or len", K(ret), K(begin), K(len), K(src.size()));
  } else {
    const uint32_t src_data_offset = begin * sizeof(T);
    int64_t curr_pos = this->data_container_->raw_data_.size();
    int64_t capacity = curr_pos + len;
    if (OB_FAIL(this->data_container_->raw_data_.prepare_allocate(capacity))) {
      OB_LOG(WARN, "allocate memory failed", K(ret), K(capacity));
    } else {
      char *cur_data = reinterpret_cast<char *>(this->data_container_->raw_data_.get_data() + curr_pos);
      MEMCPY(cur_data, src.get_data() + src_data_offset, len * sizeof(T));
      this->length_ += len;
    }
  }
  return ret;
}

template <typename T>
void ObVectorData<T>::clear()
{ 
  data_ = nullptr;
  this->length_ = 0;
  if (OB_NOT_NULL(this->data_container_)) {
    this->data_container_->clear();
  } 
}

template <typename T>
int ObVectorData<T>::flatten(ObArrayAttr *attrs, uint32_t attr_count, uint32_t &attr_idx)
{
  int ret = OB_SUCCESS;
  const uint32_t len = 2;
  if (OB_UNLIKELY(OB_ISNULL(attrs))) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "attrs is null", K(ret));
  } else if (len + attr_idx >= attr_count) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "unexpected attr count", K(ret), K(attr_count), K(attr_idx), K(len));
  } else {
    attrs[attr_idx].ptr_ = nullptr;
    attrs[attr_idx].length_ = 0;
    attr_idx++; // skip null
    attrs[attr_idx].ptr_ = reinterpret_cast<char *>(data_);
    attrs[attr_idx].length_ = sizeof(T) * this->length_;
    attr_idx++;
  }
  return ret;
}

template <typename T>
int ObVectorData<T>::compare_at(uint32_t left_begin, uint32_t left_len, uint32_t right_begin, uint32_t right_len,  
                             const ObIArrayType &right, int &cmp_ret) const
{
  int ret = OB_SUCCESS;
  const ObVectorData *right_data = dynamic_cast<const ObVectorData *>(&right);
  if (OB_ISNULL(right_data)) {
    ret = OB_ERR_ARRAY_TYPE_MISMATCH;
    OB_LOG(WARN, "invalid array type", K(ret), K(right.get_format()), K(this->get_format()));
  } else if (OB_UNLIKELY(left_begin + left_len > this->length_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "left_begin + left_len > this->length_", K(ret), K(left_begin), K(left_len), K(this->length_));
  } else if (OB_UNLIKELY(right_begin + right_len > right.size())) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "right_begin + right_len > right.size()", K(ret), K(right_begin), K(right_len), K(right.size()));
  } else {
    uint32_t cmp_len = std::min(left_len, right_len);
    uint32_t left_max = left_begin + cmp_len;
    uint32_t right_max = right_begin + cmp_len;
    cmp_ret = 0;
    
    if (this->length_ < left_max || this->length_ < right_max) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "invalid argument", K(ret), K(left_begin), K(left_len), K(right_begin), K(right_len), K(this->length_));
    } else {
      for (uint32_t i = 0; i < cmp_len && !cmp_ret; ++i) {
        if (data_[left_begin + i] != (*right_data)[right_begin + i]) {
          cmp_ret = data_[left_begin + i] > (*right_data)[right_begin + i] ? 1 : -1;
        }
      }
      if (cmp_ret == 0 && left_len != right_len) {
        cmp_ret = left_len > right_len ? 1 : -1;
      }
    }
  }
  return ret;
}

template <typename T>
int ObVectorData<T>::compare(const ObIArrayType &right, int &cmp_ret) const
{
  return compare_at(0, this->length_, 0, right.size(), right, cmp_ret);
}

template <typename T>
int ObVectorData<T>::contains_all(const ObIArrayType &other, bool &bret) const
{
  int ret = OB_SUCCESS;
  const ObVectorData *right_data = dynamic_cast<const ObVectorData *>(&other);
  if (OB_ISNULL(right_data)) {
    ret = OB_ERR_ARRAY_TYPE_MISMATCH;
    OB_LOG(WARN, "invalid array type", K(ret), K(other.get_format()), K(this->get_format()));
  } else {
    bret = true;
    for (uint32_t i = 0; i < other.size() && bret && OB_SUCC(ret); ++i) {
      int pos = -1;
      if (OB_FAIL(this->contains((*right_data)[i], pos))) {
        OB_LOG(WARN, "check element contains failed", K(ret), K(i), K((*right_data)[i]));
      } else if (pos < 0) {
        bret = false;
      }
    }
  }
  return ret;
}

template <typename T>
int ObVectorData<T>::overlaps(const ObIArrayType &other, bool &bret) const
{
  int ret = OB_SUCCESS;
  const ObVectorData *right_data = dynamic_cast<const ObVectorData *>(&other);
  if (OB_ISNULL(right_data)) {
    ret = OB_ERR_ARRAY_TYPE_MISMATCH;
    OB_LOG(WARN, "invalid array type", K(ret), K(other.get_format()), K(this->get_format()));
  } else {
    bret = false;
    for (uint32_t i = 0; i < other.size() && !bret && OB_SUCC(ret); ++i) {
      int pos = -1;
      if (OB_FAIL(this->contains((*right_data)[i], pos))) {
        OB_LOG(WARN, "check element contains failed", K(ret), K(i), K((*right_data)[i]));
      } else if (pos >= 0) {
        bret = true;
      }
    }
  }
  return ret;
}

template <typename T>
int ObVectorData<T>::distinct(ObIAllocator &alloc, ObIArrayType *&output) const
{
  int ret = OB_SUCCESS;
  ObIArrayType *arr_ptr = NULL;
  if (OB_FAIL(clone_empty(alloc, arr_ptr, false))) {
    OB_LOG(WARN, "clone empty failed", K(ret));
  } else if (this->length_ == 0) {
    output = arr_ptr;
  } else {
    hash::ObHashSet<ObString> elem_set;
    ObVectorData *vec_ptr = dynamic_cast<ObVectorData *>(arr_ptr);
    if (OB_ISNULL(vec_ptr)) {
      ret = OB_ERR_ARRAY_TYPE_MISMATCH;
      OB_LOG(WARN, "invalid array type", K(ret), K(arr_ptr->get_format()));
    } else if (OB_FAIL(elem_set.create(this->length_, ObMemAttr(common::OB_SERVER_TENANT_ID, "ArrayDistSet")))) {
      OB_LOG(WARN, "failed to create cellid set", K(ret), K(this->length_));
    } else {
      for (uint32_t i = 0; i < this->length_ && OB_SUCC(ret); ++i) {
        ObString val(sizeof(data_[i]), reinterpret_cast<char *>(&data_[i]));
        if (OB_FAIL(elem_set.exist_refactored(val))) {
          if (ret == OB_HASH_NOT_EXIST) {
            if (OB_FAIL(vec_ptr->push_back(data_[i]))) {
              OB_LOG(WARN, "failed to add elemen", K(ret));
            } else if (OB_FAIL(elem_set.set_refactored(val))) {
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

template <typename T>
int ObVectorData<T>::push_not_in_set(const ObVectorData *arr_bin_ptr,
                      hash::ObHashSet<ObString> &elem_set,
                      bool &arr_contain_null,
                      const bool &contain_null)
{
  int ret = OB_SUCCESS;
  for (uint32_t i = 0; i < arr_bin_ptr->length_ && OB_SUCC(ret); ++i) {
    ObString val(sizeof(float), arr_bin_ptr->get_data() + i * sizeof(float));
    if (arr_bin_ptr->is_null(i)) {
      if (!contain_null && !arr_contain_null) {
        arr_contain_null = true;
        if (OB_FAIL(this->push_null())) {
          OB_LOG(WARN, "push null failed", K(ret));
        }
      }
    } else if (OB_FAIL(elem_set.exist_refactored(val))) {
      if (ret == OB_HASH_NOT_EXIST) {
        if (OB_FAIL(this->push_back((*arr_bin_ptr)[i]))) {
          OB_LOG(WARN, "failed to add elemen", K(ret));
        } else if (OB_FAIL(elem_set.set_refactored(val))) {
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

template <typename T>
int ObVectorData<T>::except(ObIAllocator &alloc, ObIArrayType *arr2, ObIArrayType *&output) const
{
  int ret = OB_SUCCESS;
  ObIArrayType *arr_ptr = NULL;
  bool arr1_contain_null = false;
  bool arr2_contain_null = false;
  hash::ObHashSet<ObString> elem_set;
  ObVectorData *arr_bin_ptr = NULL;
  ObVectorData *arr2_bin_ptr = dynamic_cast<ObVectorData *>(arr2);

  if (OB_FAIL(clone_empty(alloc, arr_ptr, false))) {
    OB_LOG(WARN, "clone empty failed", K(ret));
  } else if (this->size() == 0) {
    output = arr_ptr;
  } else if (OB_ISNULL(arr_bin_ptr = dynamic_cast<ObVectorData *>(arr_ptr)) 
            || OB_ISNULL(arr2_bin_ptr)) {
    ret = OB_ERR_ARRAY_TYPE_MISMATCH;
    OB_LOG(WARN, "invalid array type", K(ret), K(arr_ptr->get_format()), K(arr2->get_format()));
  } else if (OB_FAIL(elem_set.create(arr2_bin_ptr->length_ + this->length_, 
                                  ObMemAttr(common::OB_SERVER_TENANT_ID, "ArrayDistSet")))) {
    OB_LOG(WARN, "failed to create cellid set", K(ret), K(arr2_bin_ptr->length_ + this->length_));
  } else {
    for (uint32_t i = 0; i < arr2_bin_ptr->length_ && OB_SUCC(ret); ++i) {
      ObString val(sizeof(float), arr2_bin_ptr->get_data() + i * sizeof(float));
      if (arr2->is_null(i)) {
        arr2_contain_null = true;
      } else if (OB_FAIL(elem_set.exist_refactored(val))) {
        if (ret == OB_HASH_NOT_EXIST) {
          if (OB_FAIL(elem_set.set_refactored(val))) {
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

template <typename T>
int ObVectorData<T>::unionize(ObIAllocator &alloc, ObIArrayType **arr, uint32_t arr_cnt)
{
  int ret = OB_SUCCESS;
  bool arr_contain_null = false;
  hash::ObHashSet<ObString> elem_set;
  ObVectorData *arr_bin_ptr = NULL;
  
  for (int64_t i = 0; i < arr_cnt && OB_SUCC(ret); ++i) {
    if (OB_ISNULL(arr_bin_ptr = dynamic_cast<ObVectorData *>(arr[i]))) {
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
  }
  return ret;
}

template <typename T>
int ObVectorData<T>::intersect(ObIAllocator &alloc, ObIArrayType **arr, uint32_t arr_cnt)
{
  int ret = OB_SUCCESS;
  uint64_t arr_null_number = 0;
  hash::ObHashMap<ObString, uint32_t> elem_map;
  ObVectorData *arr_bin_ptr = NULL;
  bool is_null_res = false;

  for (int64_t i = 0; i < arr_cnt && OB_SUCC(ret) && !is_null_res; ++i) {
    if (OB_ISNULL(arr_bin_ptr = dynamic_cast<ObVectorData *>(arr[i]))) {
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
        ObString val(sizeof(float), arr_bin_ptr->get_data() + i * sizeof(float));
        if (arr_bin_ptr->is_null(j)) {
          if (!arr_contain_null) {
            arr_contain_null = true;
            arr_null_number++;
            if (arr_null_number == arr_cnt && OB_FAIL(this->push_null())) {
              OB_LOG(WARN, "push null failed", K(ret));
            }
          }
        } else if (OB_FAIL(elem_map.get_refactored(val, cnt))) {
          if (ret == OB_HASH_NOT_EXIST) {
            if (i == 0 && OB_FAIL(elem_map.set_refactored(val, 1))) {
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
            } else if (OB_FAIL(elem_map.erase_refactored(val))) {
              OB_LOG(WARN, "failed to erase elemen from set", K(ret));
            } else if (OB_FAIL(elem_map.set_refactored(val, cnt + 1))) {
              OB_LOG(WARN, "failed to add elemen into set", K(ret));
            }
          } else if (i + 1 == cnt) {
            // do nothing
          } else if (OB_FAIL(elem_map.erase_refactored(val))) {
            OB_LOG(WARN, "failed to erase elemen from set", K(ret));
          }
        }        
      } // end for
    }
  } // end for
  return ret;
}

}
}
#endif // OCEANBASE_OB_VECTOR_TYPE_