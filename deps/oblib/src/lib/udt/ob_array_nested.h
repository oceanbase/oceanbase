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

#ifndef OCEANBASE_OB_ARRAY_NESTED_
#define OCEANBASE_OB_ARRAY_NESTED_
#include "ob_array_type.h"

namespace oceanbase {
namespace common {

class ObArrayNested : public ObArrayBase<char> {
public :
  ObArrayNested() : ObArrayBase(), offsets_(nullptr), data_(nullptr) {}
  ObArrayNested(uint32_t length, int32_t elem_type, uint8_t *null_bitmaps, uint32_t *offsets, ObArrayBase *data)
    : ObArrayBase(length, elem_type, null_bitmaps),
      offsets_(offsets), data_(data) {}
  uint32_t cardinality() const { return data_->cardinality(); };
  ArrayFormat get_format() const { return ArrayFormat::Nested_Array; }
  uint32_t *get_offsets() const { return offsets_; }
  char *get_data() const { return data_->get_data();}
  void set_scale(ObScale scale) { UNUSED(scale); }
  int print(ObStringBuffer &format_str, uint32_t begin = 0, uint32_t print_size = 0, bool print_whole = true) const;
  int print_element(ObStringBuffer &format_str, uint32_t begin = 0, uint32_t print_size = 0,
                    bool print_whole = true,
                    ObString delimiter = ObString(","),
                    bool has_null_str = true, ObString null_str = ObString("NULL")) const;

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
  int hash(uint64_t &hash_val) const;
  int init();
  int init(ObString &raw_data);
  int init(ObDatum *attrs, uint32_t attr_count, bool with_length = true);
  int check_validity(const ObCollectionArrayType &arr_type, const ObIArrayType &array) const { return OB_SUCCESS; }
  int push_null();
  inline void set_child_array(ObIArrayType *child) { data_ = static_cast<ObArrayBase *>(child);}
  inline ObIArrayType *get_child_array() const { return static_cast<ObIArrayType *>(data_);}
  int insert_from(const ObIArrayType &src, uint32_t begin, uint32_t len);
  int push_back(const ObIArrayType &src, bool is_null = false);
  int elem_at(uint32_t idx, ObObj &elem_obj) const { return OB_NOT_SUPPORTED; }
  int at(uint32_t idx, ObIArrayType &dest) const;
  void clear();
  int flatten(ObArrayAttr *attrs, uint32_t attr_count, uint32_t &attr_idx);
  int compare_at(uint32_t left_begin, uint32_t left_len, uint32_t right_begin, uint32_t right_len,
                 const ObIArrayType &right, int &cmp_ret) const;
  int compare(const ObIArrayType &right, int &cmp_ret) const;
  bool sort_cmp(uint32_t idx_l, uint32_t idx_r) const { return true; } // not supported
  template<typename T>
  int contains(const T &elem, int &pos) const
  {
    int ret = OB_SUCCESS;
    pos = -1;
    const ObIArrayType *elem_ptr = NULL;
    if (typeid(T) != typeid(ObIArrayType)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "invalid data type", K(ret));
    } else {
      elem_ptr = reinterpret_cast<const ObIArrayType *>(&elem);
    }
    for (uint32_t i = 0; i < length_ && pos < 0 && OB_SUCC(ret); ++i) {
      if (this->is_null(i)) {
      } else {
        uint32_t l_start = offset_at(i, get_offsets());
        uint32_t l_child_len = get_offsets()[i] - l_start;
        int cmp_ret = 0;
        if (OB_FAIL(get_child_array()->compare_at(l_start, l_child_len, 0, elem_ptr->size(), *elem_ptr, cmp_ret))) {
          OB_LOG(WARN, "failed to do nested array contains", K(ret));
        } else if (cmp_ret == 0) {
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
    const ObIArrayType *elem_ptr = NULL;
    if (!is_null && typeid(T) != typeid(ObIArrayType)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "invalid data type", K(ret));
    } else {
      if (OB_FAIL(clone_empty(alloc, output, false))) {
        OB_LOG(WARN, "clone empty failed", K(ret));
      } else {
        ObArrayNested *arr_data = dynamic_cast<ObArrayNested *>(output);
        if (OB_ISNULL(arr_data)) {
          ret = OB_ERR_ARRAY_TYPE_MISMATCH;
          OB_LOG(WARN, "invalid array type", K(ret), K(this->get_format()));
        } else {
          ObIArrayType *inner_arr = get_child_array();
          ObIArrayType *child_obj = NULL;
          if (OB_FAIL(inner_arr->clone_empty(alloc, child_obj, false))) {
            OB_LOG(WARN, "clone empty failed", K(ret));
          }
          for (uint32_t i = 0; i < this->length_ && OB_SUCC(ret); ++i) {
            if (is_null) {
              // remove null
              if (this->is_null(i)) {
                // do nothing
              } else if (OB_FAIL(at(i, *child_obj))) {
                OB_LOG(WARN, "get element failed", K(ret), K(i), K(length_));
              } else if (OB_FAIL(arr_data->push_back(*child_obj))) {
                OB_LOG(WARN, "push null failed", K(ret));
              } else {
                child_obj->clear();
              }
            } else if (this->is_null(i)) {
              if (OB_FAIL(arr_data->push_null())) {
                OB_LOG(WARN, "push null failed", K(ret));
              }
            } else if (OB_FAIL(at(i, *child_obj))) {
              OB_LOG(WARN, "get element failed", K(ret), K(i), K(length_));
            } else if (FALSE_IT(elem_ptr = reinterpret_cast<const ObIArrayType *>(elem_except))) {
            } else if (*child_obj == *elem_ptr) {
              // do nothing
              child_obj->clear();
            } else if (OB_FAIL(arr_data->push_back(*child_obj))) {
              OB_LOG(WARN, "failed to add element", K(ret));
            } else {
              child_obj->clear();
            }
          }
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

} // namespace common
} // namespace oceanbase
#endif // OCEANBASE_OB_ARRAY_NESTED_
