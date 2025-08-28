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

#ifndef OCEANBASE_OB_MAP_
#define OCEANBASE_OB_MAP_
#include "lib/udt/ob_collection_type.h"
#include "ob_array_type.h"

namespace oceanbase {
namespace common {

class ObMapType : public ObIArrayType {
public :
  ObMapType() : length_(0), keys_(nullptr), values_(nullptr), map_type_(nullptr) {}
  ObMapType(uint32_t length, ObIArrayType *keys, ObIArrayType *values, ObCollectionMapType *map_type)
    : length_(length), keys_(keys), values_(values), map_type_(map_type) {}
  int print(ObStringBuffer &format_str, uint32_t begin = 0, uint32_t print_size = 0, bool print_whole = true) const;
  int print_element(ObStringBuffer &format_str, uint32_t begin = 0, uint32_t print_size = 0,
                    bool print_whole = true,
                    ObString delimiter = ObString(","),
                    bool has_null_str = true, ObString null_str = ObString("NULL")) const { return OB_NOT_SUPPORTED; }
  int print_element_at(ObStringBuffer &format_str, uint32_t idx) const;
  int32_t get_raw_binary_len() { return sizeof(length_) + get_data_binary_len(); }

  int get_raw_binary(char *res_buf, int64_t buf_len);
  int32_t get_data_binary_len()
  { 
    return keys_->get_data_binary_len() + values_->get_data_binary_len();
  }
  int get_data_binary(char *res_buf, int64_t buf_len);
  int init(ObString &raw_data);
  int init(uint32_t length, ObString &data_binary);
  int init(ObDatum *attrs, uint32_t attr_count, bool with_length = true);
  int init();
  void set_scale(ObScale scale) {};  // only for decimalint array
  ArrayFormat get_format() const { return ArrayFormat::MAP; }
  uint32_t size() const { return length_; }
  uint32_t cardinality() const { return values_->cardinality(); }
  int check_validity(const ObCollectionTypeBase &coll_type, const ObIArrayType &array) const
  {
    int ret = OB_SUCCESS;
    const ObCollectionMapType *map_type = dynamic_cast<const ObCollectionMapType *>(&coll_type);
    const ObMapType *map_obj = dynamic_cast<const ObMapType *>(&array);
    if (OB_ISNULL(map_type)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "invalid collection map type", K(ret));
    } else if (OB_ISNULL(map_obj)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "invalid map type", K(ret));
    } else if (OB_FAIL(map_obj->keys_->check_validity(*map_type->key_type_, *map_obj->keys_))) {
      OB_LOG(WARN, "map key check validity failed", K(ret), K(map_obj->keys_), K(map_type->key_type_));
    } else if (OB_FAIL(map_obj->values_->check_validity(*map_type->value_type_, *map_obj->values_))) {
      OB_LOG(WARN, "map value check validity failed", K(ret), K(map_obj->values_), K(map_type->value_type_));
    }
    return OB_SUCCESS;
  }
  bool is_null(uint32_t idx) const { return false; } // no null in map
  int push_null() { return OB_NOT_SUPPORTED; }
  bool contain_null() const { return false; }
  int insert_from(const ObIArrayType &src, uint32_t begin, uint32_t len) { return OB_NOT_SUPPORTED; }
  int32_t get_element_type() const { return values_->get_element_type(); }
  const ObCollectionMapType *get_array_type() const { return map_type_; }

  char *get_data() const { return NULL; }; // not supported now
  uint32_t *get_offsets() const { return keys_->get_offsets(); }
  uint8_t *get_nullbitmap() const { return keys_->get_nullbitmap(); }
  void set_element_type(int32_t type) {} // not supported
  void set_array_type(const ObCollectionTypeBase *array_type) { 
    map_type_ = static_cast<const ObCollectionMapType*>(array_type);
  }
  int elem_at(uint32_t idx, ObObj &elem_obj) const { return OB_NOT_SUPPORTED; }
  int at(uint32_t idx, ObIArrayType &dest) const { return OB_NOT_SUPPORTED; }
  void clear() {}
  int flatten(ObArrayAttr *attrs, uint32_t attr_count, uint32_t &attr_idx);
  int set_null_bitmaps(uint8_t *nulls, int64_t length) { return OB_NOT_SUPPORTED; }
  int set_offsets(uint32_t *offsets, int64_t length) { return OB_NOT_SUPPORTED; }
  int compare(const ObIArrayType &right, int &cmp_ret) const;

  int compare_at(uint32_t left_begin, uint32_t left_len, uint32_t right_begin, uint32_t right_len,  
                 const ObIArrayType &right, int &cmp_ret) const;
  bool sort_cmp(uint32_t idx_l, uint32_t idx_r) const { return true; } // not supported
  int contains_all(const ObIArrayType &other, bool &bret) const { return OB_NOT_SUPPORTED; }
  int overlaps(const ObIArrayType &other, bool &bret) const { return OB_NOT_SUPPORTED; }
  int hash(uint64_t &hash_val) const { return OB_NOT_SUPPORTED; }
  bool operator ==(const ObIArrayType &other) const
  {
    bool b_ret = false;
    int ret = OB_SUCCESS;
    int cmp_ret = 0;
    if (OB_SUCC(compare(other, cmp_ret))) {
      b_ret = (cmp_ret == 0);
    }
    return b_ret;
  }
  int clone_empty(ObIAllocator &alloc, ObIArrayType *&output, bool read_only = true) const;
  int distinct(ObIAllocator &alloc, ObIArrayType *&output) const;
  int except(ObIAllocator &alloc, ObIArrayType *arr2, ObIArrayType *&output) const { return OB_NOT_SUPPORTED; }
  int unionize(ObIAllocator &alloc, ObIArrayType **arr, uint32_t arr_cnt) { return OB_NOT_SUPPORTED; }
  int intersect(ObIAllocator &alloc, ObIArrayType **arr, uint32_t arr_cnt) { return OB_NOT_SUPPORTED; }

  inline void set_size(uint32_t size) { length_ = size; }
  inline ObIArrayType *get_key_array() const { return keys_; }
  inline ObIArrayType *get_value_array() const { return values_; }
  inline ObIArrayType *get_key_array() { return keys_; }
  inline ObIArrayType *get_value_array() { return values_; }
  inline void set_key_array(ObIArrayType *keys) { keys_ = keys; }
  inline void set_value_array(ObIArrayType *values) { values_ = values; }

protected :
  uint32_t length_;
  ObIArrayType *keys_;
  ObIArrayType *values_;
  const ObCollectionMapType *map_type_;

  // private :
  // data_container: only maintain null_bitmaps_ and offsets_, raw_data is in the child element
};

} // namespace common
} // namespace oceanbase
#endif // OCEANBASE_OB_MAP_
