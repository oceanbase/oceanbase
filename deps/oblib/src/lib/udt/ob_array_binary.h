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

#ifndef OCEANBASE_OB_ARRAY_BINARY_
#define OCEANBASE_OB_ARRAY_BINARY_
#include "ob_array_type.h"

namespace oceanbase {
namespace common {

class ObArrayBinary : public ObArrayBase<char> {
public :
  ObArrayBinary() : ObArrayBase(), offsets_(nullptr), data_(nullptr) {}
  ObArrayBinary(uint32_t length, int32_t elem_type, uint8_t *null_bitmaps, uint32_t *offsets, char *data)
    : ObArrayBase(length, elem_type, null_bitmaps),
      offsets_(offsets), data_(data) {}
  ObString operator[](const int64_t i) const;
  uint32_t cardinality() const { return this->length_; }
  ArrayFormat get_format() const { return ArrayFormat::Binary_Varlen; }
  uint32_t *get_offsets() const { return offsets_; }
  char *get_data() const { return data_;}
  int push_back(const ObString &value, bool is_null = false);
  void set_scale(ObScale scale) { UNUSED(scale); }
  int static escape_append(ObStringBuffer &format_str, ObString elem_str);
  int print(ObStringBuffer &format_str, uint32_t begin = 0, uint32_t print_size = 0, bool print_whole = true) const;
  int print_element(ObStringBuffer &format_str, uint32_t begin = 0, uint32_t print_size = 0,
                    bool print_whole = true,
                    ObString delimiter = ObString(","),
                    bool has_null_str = true, ObString null_str = ObString("NULL")) const;

  int32_t get_data_binary_len()
  {
    int32_t len = 0;
    if (this->data_container_ == NULL) {
      if (this->length_ > 0) {
        uint32_t last_idx = this->length_ - 1;
        len = this->length_ * sizeof(uint8_t) + this->length_ * sizeof(uint32_t) + this->offsets_[last_idx];
      }
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
  int hash(uint64_t &hash_val) const;
  int init();
  int init(ObString &raw_data);
  int init(ObDatum *attrs, uint32_t attr_count, bool with_length = true);
  int check_validity(const ObCollectionArrayType &arr_type, const ObIArrayType &array) const { return OB_SUCCESS; }
  int push_null();
  int insert_from(const ObIArrayType &src, uint32_t begin, uint32_t len);
  int elem_at(uint32_t idx, ObObj &elem_obj) const;
  int at(uint32_t idx, ObIArrayType &dest) const { return OB_NOT_SUPPORTED; }
  void clear();
  int flatten(ObArrayAttr *attrs, uint32_t attr_count, uint32_t &attr_idx);
  int compare(const ObIArrayType &right, int &cmp_ret) const;
  int compare_at(uint32_t left_begin, uint32_t left_len, uint32_t right_begin, uint32_t right_len,
                 const ObIArrayType &right, int &cmp_ret) const;
  bool sort_cmp(uint32_t idx_l, uint32_t idx_r) const;
  template<typename T>
  int contains(const T &elem, int &pos) const
  {
    int ret = OB_SUCCESS;
    pos = -1;
    const ObString *str = nullptr;
    if (typeid(T) != typeid(ObString)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "invalid data type", K(ret));
    } else {
      str = reinterpret_cast<const ObString *>(&elem);
      for (int i = 0; i < length_ && pos < 0; i++) {
        if ((*this)[i].compare(*str) == 0) {
          pos = i;
        }
      }
    }
    return ret;
  }
  int contains_all(const ObIArrayType &other, bool &bret) const;
  int overlaps(const ObIArrayType &other, bool &bret) const;
  int clone_empty(ObIAllocator &alloc, ObIArrayType *&output, bool read_only = true) const;
  int distinct(ObIAllocator &alloc, ObIArrayType *&output) const;

  template<typename T>
  int clone_except(ObIAllocator &alloc, const T *elem_except, bool is_null, ObIArrayType *&output) const
  {
    int ret = OB_SUCCESS;
    if (typeid(T) != typeid(ObString)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "invalid data type", K(ret));
    } else if (OB_FAIL(clone_empty(alloc, output, false))) {
      OB_LOG(WARN, "clone empty failed", K(ret));
    } else {
      ObArrayBinary *arr_data = dynamic_cast<ObArrayBinary *>(output);
      if (OB_ISNULL(arr_data)) {
        ret = OB_ERR_ARRAY_TYPE_MISMATCH;
        OB_LOG(WARN, "invalid array type", K(ret), K(this->get_format()));
      }
      const ObString *str = reinterpret_cast<const ObString *>(elem_except);
      for (uint32_t i = 0; i < this->length_ && OB_SUCC(ret); ++i) {
        if (is_null) {
          // remove null
          if (this->is_null(i)) {
            // do nothing
          } else if (OB_FAIL(arr_data->push_back((*this)[i]))) {
            OB_LOG(WARN, "push null failed", K(ret));
          }
        } else if (this->is_null(i)) {
          if (OB_FAIL(arr_data->push_null())) {
            OB_LOG(WARN, "push null failed", K(ret));
          }
        } else if ((*this)[i] != *str && OB_FAIL(arr_data->push_back((*this)[i]))) {
          OB_LOG(WARN, "failed to add element", K(ret));
        }
      }
    }
    return ret;
  }

private :
  uint32_t *offsets_;
  char *data_;
};

} // namespace common
} // namespace oceanbase
#endif // OCEANBASE_OB_ARRAY_BINARY_
