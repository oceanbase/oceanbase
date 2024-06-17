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

#ifndef OCEANBASE_OB_SQL_UDT_TYPE_
#define OCEANBASE_OB_SQL_UDT_TYPE_
#include <stdint.h>
#include <string.h>
#include "common/object/ob_object.h"
#include "lib/string/ob_string.h"
#include "lib/oblog/ob_log_module.h"
#include "common/ob_field.h"

namespace oceanbase {
namespace common {

// build udt meta from ObUDTTypeInfo (flatten_leaf_attrs_meta)
//   1. get all dependent udt schema recursively to build a flattern ObSqlUDTAttrMeta array, assing order sequentially
//   2. sort array built in 1 by type and length order (fix len), by type only (var len)
// encoding from pl extend to sql udt
//   1 build an leaf object point array, in define order, from pl extend
//   2 use order in flatten_leaf_attrs_meta to mapping to a new leaf object
//   3 encoding new object point array in 2 sequentially
// decoding from sql udt to pl extend (need schema guard?)
//   1 build an leaf object point array, in sorted order, from sql udt
//   2 use order in flatten_leaf_attrs_meta to remapping to a new leaf object
//   3 use toplevel_attrs_meta? to build pl extend (looks like a constructor)
// sql udt to string
//   1 same as from sql udt to pl extend
//   2 use root ObSqlUDTMeta to print udt name, to_string top level elements recursively (toplevel_attrs_meta)

const uint32_t nested_udt_bitmap_index = 0; // the first attribute in sql udt is always nest udt bitmap

typedef struct ObSqlUDTAttrMeta
{
public:
  int32_t order_; // only used in flattern leaf attrs meta;
  ObObjMeta type_info_;
  // may need udt id ?
  bool is_same(ObSqlUDTAttrMeta &other) {
    bool is_same = (order_ == other.order_);
    if (is_same) {
      if ((type_info_.is_user_defined_sql_type() || type_info_.is_collection_sql_type())) {
        is_same = (type_info_.get_type() == other.type_info_.get_type()
                   && type_info_.get_scale() == other.type_info_.get_scale());
      } else {
        is_same = (type_info_ == other.type_info_);
      }
    }
    return is_same;
  }

  TO_STRING_KV(K_(order), K_(type_info));
} ObSqlUDTAttrMeta;

typedef struct ObSqlUDTMeta
{
  OB_UNIS_VERSION(1);
public:
  void set_name(ObString &name)
  {
    udt_name_ = name.ptr();
    udt_name_len_ = name.length();
  }

  bool is_same(const ObSqlUDTMeta &other) // test only remove later
  {
    bool is_same = (attribute_cnt_ == other.attribute_cnt_
                    && fixed_attr_cnt_ == other.fixed_attr_cnt_
                    && fixed_offset_ == other.fixed_offset_
                    && pl_type_ == other.pl_type_
                    && udt_id_ == other.udt_id_
                    && child_attr_cnt_ == other.child_attr_cnt_
                    && leaf_attr_cnt_ == other.leaf_attr_cnt_
                    && udt_name_len_ == other.udt_name_len_
                    && strncmp(udt_name_, other.udt_name_, udt_name_len_) == 0
                    && nested_udt_number_ == other.nested_udt_number_
                    && varray_capacity_ == other.varray_capacity_);
    if (is_same) {
      for (int32_t i = 0; is_same && i < child_attr_cnt_; i++) {
        is_same = child_attrs_meta_[i].is_same(other.child_attrs_meta_[i]);
      }
      for (int32_t i = 0; is_same && i < leaf_attr_cnt_; i++) {
        is_same = leaf_attrs_meta_[i].is_same(other.leaf_attrs_meta_[i]);
      }
    }
    return is_same;
  }

  void reset()
  {
    attribute_cnt_ = 0;
    fixed_attr_cnt_ = 0;
    fixed_offset_ = 0;
    pl_type_ = 0;
    udt_id_ = 0;
    child_attr_cnt_ = 0;
    leaf_attr_cnt_ = 0;
    udt_name_len_ = 0;
    udt_name_ = NULL;
    nested_udt_number_ = 0;
    varray_capacity_ = 0;
    child_attrs_meta_ = NULL;
    leaf_attrs_meta_ = NULL;
  }

  int deep_copy(ObIAllocator &allocator, ObSqlUDTMeta *&dst) const;

  TO_STRING_KV(K_(attribute_cnt),
               K_(fixed_attr_cnt),
               K_(fixed_offset),
               K_(pl_type),
               K_(udt_id),
               K_(child_attr_cnt),
               K_(leaf_attr_cnt),
               K(ObString(udt_name_len_, udt_name_)),
               K_(nested_udt_number),
               K_(varray_capacity),
               KP_(child_attrs_meta),
               KP_(leaf_attrs_meta));

  uint32_t attribute_cnt_; // leaf-level attributes count of object type
  uint32_t fixed_attr_cnt_; // fixed size attributes count of object type
  int32_t fixed_offset_;     // offset of fixed attributes

  // new elements for discuss
  int32_t pl_type_; // record/varary
  uint64_t udt_id_;  // orignal udt id in schema

  uint32_t child_attr_cnt_; // top level attributes number for restore
  // leaf attributes number for encoding, the same with attribute_cnt_ ? remove later
  uint32_t leaf_attr_cnt_; // remove this or attribute_cnt_;

  size_t udt_name_len_;
  const char *udt_name_;

  uint32_t nested_udt_number_; // for nested_udt_bitmap
  uint32_t varray_capacity_;
  // bool is_nullable

  // int64_t rowsize_; ? expr_object_construct use this to verify pl handler size

  ObSqlUDTAttrMeta* child_attrs_meta_;  // in attrs define order

  // flattern(encoding) order, fix length attrs first, varray length attrs laster
  // sorted by type, length
  ObSqlUDTAttrMeta* leaf_attrs_meta_;
} ObSqlUDTMeta;

class ObSqlUDT
{
public:
  ObSqlUDT(common::ObIAllocator *allocator = NULL)
      : allocator_(allocator),
        udt_meta_(),
        udt_data_()
  {
  }

  virtual ~ObSqlUDT() {}

  void set_data(const ObString data) { udt_data_ = data; }
  int init_null_bitmap();
  int set_null_bitmap(uint32_t index, bool is_null);
  int set_attribute_offset(uint32_t index, int32_t offset);
  int append_attribute(uint32_t index, const ObString &attr_value);
  static inline uint32_t get_null_bitmap_len(uint32_t attr_cnt) { return (attr_cnt + 7) / 8; }
  void set_udt_meta(const ObSqlUDTMeta &udt_meta) {
    udt_meta_ = udt_meta;
  }

  ObSqlUDTMeta &get_udt_meta() { return udt_meta_; }

  // int update_attribute();

  int access_attribute(uint32_t index, ObString &attr_value, bool is_varray_element = false);

  int get_null_bitmap(ObString &null_bitmap);

  uint32_t get_varray_element_count();
  static uint32_t get_offset_array_len(uint32_t count);
  static int set_null_bitmap_pos(char *bitmap_start, uint32_t bitmap_len, uint32_t pos);
  static int get_null_bitmap_pos(const char *bitmap_start, uint32_t bitmap_len, uint32_t pos, bool &is_set);
  static inline void increase_varray_null_count(char *null_count) { (*reinterpret_cast<int32_t *>(null_count))++; }

  TO_STRING_KV(KP_(allocator), K_(udt_meta), K_(udt_data));

/* data */
private:

  inline int32_t get_attr_offset(uint32_t index, uint32_t null_bitmap_offset, bool is_varray_element = false) {

    int32_t count_offset = is_varray_element ? sizeof(uint32) + sizeof(uint32) : 0;
    return reinterpret_cast<int32_t *>(udt_data_.ptr() + null_bitmap_offset + count_offset)[index];
  }

  common::ObIAllocator *allocator_;

  ObSqlUDTMeta udt_meta_;
  ObString udt_data_;
};

inline bool ob_is_reserved_subschema_id(uint16_t subschema_id)
{
  return subschema_id < ObMaxSystemUDTSqlType;
}
inline bool ob_is_reserved_udt_id(uint64_t udt_id)
{
  return udt_id == T_OBJ_XML;
}
int ob_get_reserved_udt_meta(uint16_t subschema_id, ObSqlUDTMeta &udt_meta);
int ob_get_reserved_subschema(uint64_t udt_id, uint16_t &subschema_id);



} // namespace common
} // namespace oceanbase
#endif // OCEANBASE_OB_SQL_UDT_TYPE_
