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

#ifndef OCEANBASE_OB_ARRAY_FIXED_SIZE_
#define OCEANBASE_OB_ARRAY_FIXED_SIZE_
#include "lib/udt/ob_array_type.h"

namespace oceanbase {
namespace common {

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
  uint32_t cardinality() const { return this->length_; }
  ObDecimalInt *get_decimal_int(const int64_t i) { return (ObDecimalInt *)(data_ + i); }
  ArrayFormat get_format() const { return ArrayFormat::Fixed_Size; }
  uint32_t *get_offsets() const { return nullptr; }
  char *get_data() const { return reinterpret_cast<char*>(data_);}
  int check_validity(const ObCollectionArrayType &arr_type, const ObIArrayType &array) const { return OB_SUCCESS; }
  int push_null();
  int push_back(T value, bool is_null = false);
  int print(ObStringBuffer &format_str, uint32_t begin = 0, uint32_t print_size = 0, bool print_whole = true) const;
  int print_element(ObStringBuffer &format_str, uint32_t begin = 0, uint32_t print_size = 0,
                    bool print_whole = true,
                    ObString delimiter = ObString(","),
                    bool has_null_str = true, ObString null_str = ObString("NULL")) const;

  int32_t get_data_binary_len();
  int get_data_binary(char *res_buf, int64_t buf_len);
  int32_t get_raw_binary_len() { return sizeof(this->length_) + get_data_binary_len(); }
  int get_raw_binary(char *res_buf, int64_t buf_len);
  int hash(uint64_t &hash_val) const;
  int init();
  int init(ObString &raw_data);
  int init(ObDatum *attrs, uint32_t attr_count, bool with_length = true);

  int insert_from(const ObIArrayType &src, uint32_t begin, uint32_t len);
  int elem_at(uint32_t idx, ObObj &elem_obj) const;
  int at(uint32_t idx, ObIArrayType &dest) const { return OB_NOT_SUPPORTED; }
  void clear();
  int flatten(ObArrayAttr *attrs, uint32_t attr_count, uint32_t &attr_idx);
  int compare_at(uint32_t left_begin, uint32_t left_len, uint32_t right_begin, uint32_t right_len,
                 const ObIArrayType &right, int &cmp_ret) const;
  int compare(const ObIArrayType &right, int &cmp_ret) const
  {
    return compare_at(0, this->length_, 0, right.size(), right, cmp_ret);
  }
  bool sort_cmp(uint32_t idx_l, uint32_t idx_r) const;
  template<typename Elem_Type>
  int contains(const Elem_Type &elem, int &pos) const
  {
    int ret = OB_SUCCESS;
    pos = -1;
    for (uint32_t i = 0; i < this->length_ && pos < 0; ++i) {
      if (this->is_null(i)) {
      } else if (static_cast<Elem_Type>(this->data_[i]) == elem) {
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
  int clone_empty(ObIAllocator &alloc, ObIArrayType *&output, bool read_only = true) const;

  template<typename Elem_Type>
  int clone_except(ObIAllocator &alloc, const Elem_Type *elem_except, bool is_null, ObIArrayType *&output) const
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(clone_empty(alloc, output, false))) {
      OB_LOG(WARN, "clone empty failed", K(ret));
    } else {
      ObArrayFixedSize<T> *arr_data = dynamic_cast<ObArrayFixedSize<T> *>(output);
      if (OB_ISNULL(arr_data)) {
        ret = OB_ERR_ARRAY_TYPE_MISMATCH;
        OB_LOG(WARN, "invalid array type", K(ret), K(this->get_format()));
      }
      for (uint32_t i = 0; i < this->length_ && OB_SUCC(ret); ++i) {
        if (is_null) {
          if (this->is_null(i)) {
            // do nothing
          } else if (OB_FAIL(arr_data->push_back((*this)[i]))) {
            OB_LOG(WARN, "push null failed", K(ret));
          }
        } else if (this->is_null(i)) {
          if (OB_FAIL(arr_data->push_null())) {
            OB_LOG(WARN, "push null failed", K(ret));
          }
        } else if ((*this)[i] != *elem_except && OB_FAIL(arr_data->push_back((*this)[i]))) {
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

  int distinct(ObIAllocator &alloc, ObIArrayType *&output) const;

private :
  T *data_;
  int16_t scale_; // only for decimalint type
};

} // namespace common
} // namespace oceanbase
#endif // OCEANBASE_OB_ARRAY_FIXED_SIZE_
