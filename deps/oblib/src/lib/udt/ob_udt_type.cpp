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
#include "ob_udt_type.h"


namespace oceanbase {
namespace common {

int ObSqlUDT::init_null_bitmap()
{
  int ret = OB_SUCCESS;
  uint32_t offset = get_null_bitmap_len(udt_meta_.attribute_cnt_);
  uint32_t data_offset = offset + sizeof(int32_t) * udt_meta_.attribute_cnt_;
  MEMSET(udt_data_.ptr(), 0, data_offset);
  udt_data_.set_length(data_offset);
  if (OB_FAIL(set_attribute_offset(0, data_offset))) {
    LOG_WARN("update attribute offset failed", K(ret), K(udt_meta_.attribute_cnt_), K(offset));
  }
  return ret;
}

int ObSqlUDT::access_attribute(uint32_t index, ObString &attr_value, bool is_varray_element)
{
  int ret = OB_SUCCESS;
  uint32_t attribute_cnt = 0;
  if (udt_data_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("udt data is null", K(ret), K(index));
  } else if (FALSE_IT(attribute_cnt = is_varray_element ? get_varray_element_count() : udt_meta_.attribute_cnt_)) {
  } else if (FALSE_IT(attribute_cnt += (!is_varray_element) ? 1 : 0)) {
  } else if (index >= attribute_cnt) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid attribute index", K(ret), K(index), K(attribute_cnt));
  } else {
    uint32_t null_bitmap_offset = get_null_bitmap_len(attribute_cnt);
    int32_t offset = get_attr_offset(index, null_bitmap_offset, is_varray_element);
    uint32_t attr_len = 0;
    if (offset > udt_data_.length()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid udt data", K(ret), K(index), K(udt_data_.length()), K(offset), K(attribute_cnt));
    } else if (index == (attribute_cnt - 1)) {
      // last attribute
      attr_len = udt_data_.length() -  offset;
    } else {
      int32_t next_offset = get_attr_offset(index + 1, null_bitmap_offset, is_varray_element);
      if (next_offset > udt_data_.length() || next_offset < offset) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid udt data", K(ret), K(index), K(udt_data_.length()), K(offset), K(next_offset), K(attribute_cnt));
      } else {
        attr_len = next_offset - offset;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (attr_len == 0) {
      attr_value.reset();
    } else if (offset + attr_len > udt_data_.length()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("udt attribute is out range", K(ret), K(index), K(attribute_cnt), K(attr_len), K(offset), K(udt_data_.length()));
    } else {
      attr_value.assign_ptr(udt_data_.ptr() + offset, attr_len);
    }
  }
  return ret;
}

uint32_t ObSqlUDT::get_offset_array_len(uint32_t count)
{
  uint32_t len = count * sizeof(uint32_t); // offset is 4bytes currently
  return len;
}

uint32_t ObSqlUDT::get_varray_element_count()
{
  uint32_t length = 0;
  if (udt_data_.empty() || udt_data_.length() < sizeof(uint32_t)) {
  } else {
    length = *(reinterpret_cast<uint32_t *>(udt_data_.ptr()));
  }
  return length;
}

int ObSqlUDT::set_null_bitmap_pos(char *bitmap_start, uint32_t bitmap_len, uint32_t pos)
{
  // remove ?
  int ret = OB_SUCCESS;
  uint32 index = pos / 8;
  uint32 byte_index = pos % 8;
  if (index >= bitmap_len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("udt bitmap overflow", K(ret), K(index), K(bitmap_len), K(pos));
  } else {
    bitmap_start[index] |= (1 << byte_index);
  }
  return ret;
}

int ObSqlUDT::append_attribute(uint32_t index, const ObString &attr_value)
{
  int ret = OB_SUCCESS;
  if (index >= udt_meta_.attribute_cnt_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid attribute index", K(ret), K(index), K(udt_meta_.attribute_cnt_));
  } else if (udt_data_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("udt data is null", K(ret), K(index), K(udt_meta_.attribute_cnt_));
  } else if (OB_FAIL(set_null_bitmap(index, attr_value.empty()))) {
    LOG_WARN("set null bitmap failed", K(ret), K(index));
  } else {
    uint32_t null_bitmap_offset = get_null_bitmap_len(udt_meta_.attribute_cnt_);
    int32_t offset = get_attr_offset(index, null_bitmap_offset);
    uint32_t attr_len = attr_value.length();
    if (offset != udt_data_.length()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("append udt attribute failed", K(ret), K(index), K(udt_meta_.attribute_cnt_),
                                              K(attr_len), K(offset), K(udt_data_.length()),
                                              K(udt_data_.size()));
    } else if (offset + attr_len > udt_data_.size()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("udt attribute is out range", K(ret), K(index), K(udt_meta_.attribute_cnt_),
                                              K(attr_len), K(offset), K(udt_data_.length()),
                                              K(udt_data_.size()));
    } else {
      udt_data_.write(attr_value.ptr(), attr_len);
      if (index < udt_meta_.attribute_cnt_ - 1) {
        set_attribute_offset(index + 1, offset + attr_len);
      }
    }
  }
  return ret;
}

int ObSqlUDT::get_null_bitmap_pos(const char *bitmap_start, uint32_t bitmap_len, uint32_t pos, bool &is_set)
{
  int ret = OB_SUCCESS;
  is_set = false;
  uint32 index = pos / 8;
  uint32 byte_pos = pos % 8;
  if (index >= bitmap_len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("udt bitmap overflow", K(ret), K(index), K(bitmap_len), K(pos));
  } else {
    is_set = bitmap_start[index] & (1 << pos);
  }
  return ret;
}

int ObSqlUDT::set_attribute_offset(uint32_t index, int32_t offset)
{
  int ret = OB_SUCCESS;
  uint32_t null_bitmap_offset = get_null_bitmap_len(udt_meta_.attribute_cnt_);
  uint32_t pos = null_bitmap_offset + (sizeof(int32_t) * index);
  if (pos >= udt_data_.length()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("udt attribute is out range", K(ret), K(index), K(null_bitmap_offset), K(offset), K(udt_data_.length()));
  } else {
    reinterpret_cast<int32_t *>(udt_data_.ptr() + null_bitmap_offset)[index] = offset;
  }
  return ret;
}

int ObSqlUDT::set_null_bitmap(uint32_t index, bool is_null)
{
  int ret = OB_SUCCESS;
  uint32_t null_bitmap_len = get_null_bitmap_len(udt_meta_.attribute_cnt_);
  uint32_t byte_pos = static_cast<uint32_t>((index) / 8);
  uint32_t bit_pos  = static_cast<uint32_t>((index) % 8);
  if (byte_pos >= null_bitmap_len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("udt null bitmap is out range", K(ret), K(index), K(udt_meta_.attribute_cnt_), K(null_bitmap_len));
  } else if (udt_data_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("udt data is null", K(ret), K(index), K(udt_meta_.attribute_cnt_));
  } else {
    char *bitmap = udt_data_.ptr();
    if (is_null) {
      bitmap[byte_pos] |= static_cast<char>(1 << bit_pos);
    } else {
      bitmap[byte_pos] &= ~static_cast<char>(1 << bit_pos);
    }
  }
  return ret;
}

// implement udt meta serialization
// Notice: no deep copy in ObSqlUDTMeta serialzie/desrialize,
// deep copy is done when subschema mapping serialzie/desriazlied
int ObSqlUDTMeta::deep_copy(ObIAllocator &allocator, ObSqlUDTMeta *&dst) const
{
  int ret = OB_SUCCESS;
  ObSqlUDTMeta *new_udt_meta = NULL;
  ObSqlUDTAttrMeta *copy_child_attrs_meta = NULL;
  ObSqlUDTAttrMeta *copy_leaf_attrs_meta = NULL;
  char *copy_udt_name = NULL;

  new_udt_meta = reinterpret_cast<ObSqlUDTMeta *>(allocator.alloc(sizeof(ObSqlUDTMeta)));
  if (OB_ISNULL(new_udt_meta)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret), K(sizeof(ObSqlUDTMeta)));
  } else {
    *new_udt_meta = *this;
  }

  if (OB_FAIL(ret)) { // memory free when allocator reset/destory
  } else if (udt_name_len_ == 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("copy udt meta without name", K(ret), K(*this));
  } else if (FALSE_IT(copy_udt_name = reinterpret_cast<char *>(allocator.alloc(udt_name_len_)))) {
  } else if (OB_ISNULL(copy_udt_name)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for udt name", K(ret), K(udt_name_len_));
  } else {
    MEMCPY(copy_udt_name, udt_name_, udt_name_len_);
    new_udt_meta->udt_name_ = copy_udt_name;
  }

  uint32_t child_attrs_size = sizeof(ObSqlUDTAttrMeta) * child_attr_cnt_;
  if (OB_FAIL(ret)) {
  } else if (child_attr_cnt_ == 0) {
    new_udt_meta->child_attrs_meta_ = NULL;
  } else if (FALSE_IT(copy_child_attrs_meta = reinterpret_cast<ObSqlUDTAttrMeta *>(
                                                  allocator.alloc(child_attrs_size)))) {
  } else if (OB_ISNULL(copy_child_attrs_meta)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for child attrs", K(ret), K(child_attrs_size));
  } else {
    MEMCPY(copy_child_attrs_meta, child_attrs_meta_, child_attrs_size);
    new_udt_meta->child_attrs_meta_ = copy_child_attrs_meta;
  }

  uint32_t leaf_attrs_size = sizeof(ObSqlUDTAttrMeta) * leaf_attr_cnt_;
  if (OB_FAIL(ret)) {
  } else if (leaf_attr_cnt_ == 0) {
    new_udt_meta->leaf_attrs_meta_ = NULL;
  } else if (FALSE_IT(copy_leaf_attrs_meta = reinterpret_cast<ObSqlUDTAttrMeta *>(
                                                  allocator.alloc(leaf_attrs_size)))) {
  } else if (OB_ISNULL(copy_leaf_attrs_meta)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for leaf attrs", K(ret), K(leaf_attrs_size));
  } else {
    MEMCPY(copy_leaf_attrs_meta, leaf_attrs_meta_, leaf_attrs_size);
    new_udt_meta->leaf_attrs_meta_ = copy_leaf_attrs_meta;
  }

  if (OB_SUCC(ret)) {
    dst = new_udt_meta;
  }

  return ret;
}

OB_DEF_SERIALIZE(ObSqlUDTMeta)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              attribute_cnt_,
              fixed_attr_cnt_,
              fixed_offset_,
              pl_type_,
              udt_id_,
              child_attr_cnt_,
              leaf_attr_cnt_,
              nested_udt_number_,
              varray_capacity_,
              udt_name_len_);
  if (OB_FAIL(ret)) {
  } else if (udt_name_len_ <= 0 || OB_ISNULL(udt_name_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid udt name length for serialize", K(ret), K(*this));
  } else {
    MEMCPY(buf + pos, udt_name_, udt_name_len_);
    pos += udt_name_len_;
  }
  if (OB_SUCC(ret)) {
    if (child_attr_cnt_ > 0) {
      MEMCPY(buf + pos, reinterpret_cast<char *>(child_attrs_meta_), sizeof(ObSqlUDTAttrMeta) * child_attr_cnt_);
      pos += sizeof(ObSqlUDTAttrMeta) * child_attr_cnt_;
    }
    if (leaf_attr_cnt_ > 0) {
      MEMCPY(buf + pos, reinterpret_cast<char *>(leaf_attrs_meta_), sizeof(ObSqlUDTAttrMeta) * leaf_attr_cnt_);
      pos += sizeof(ObSqlUDTAttrMeta) * leaf_attr_cnt_;
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObSqlUDTMeta)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
              attribute_cnt_,
              fixed_attr_cnt_,
              fixed_offset_,
              pl_type_,
              udt_id_,
              child_attr_cnt_,
              leaf_attr_cnt_,
              nested_udt_number_,
              varray_capacity_,
              udt_name_len_);
  if (OB_FAIL(ret)) {
  } else if (udt_name_len_ <= 0 || pos >= data_len) {
    ret = OB_DESERIALIZE_ERROR;
    LOG_WARN("invalid udt name length for deseriazlie", K(ret), K(*this), K(pos), K(data_len));
  } else {
    udt_name_ = buf + pos;
    pos += udt_name_len_;
  }
  // need copy data after deserialize, since metas are from buffer
  int64_t child_attrs_length = sizeof(ObSqlUDTAttrMeta) * child_attr_cnt_;
  int64_t leaf_attrs_length = sizeof(ObSqlUDTAttrMeta) * leaf_attr_cnt_;
  if (OB_FAIL(ret)) {
  } else if (child_attr_cnt_ == 0) { // no child attrs
  } else if (child_attr_cnt_ < 0 || pos + child_attrs_length > data_len) {
    ret = OB_DESERIALIZE_ERROR;
    LOG_WARN("invalid udt child attrs for deseriazlie", K(ret), K(*this), K(pos), K(data_len));
  } else {
    child_attrs_meta_ = reinterpret_cast<ObSqlUDTAttrMeta *>(const_cast<char *>(buf + pos));
    pos += child_attrs_length;
  }
  if (OB_FAIL(ret)) {
  } else if (leaf_attr_cnt_ == 0) { // no leaf attrs
  } else if (leaf_attr_cnt_ < 0 && pos + leaf_attrs_length > data_len) {
    ret = OB_DESERIALIZE_ERROR;
    LOG_WARN("invalid udt child attrs for deseriazlie", K(ret), K(*this), K(pos), K(data_len));
  } else {
    leaf_attrs_meta_ = reinterpret_cast<ObSqlUDTAttrMeta *>(const_cast<char *>(buf + pos));
    pos += leaf_attrs_length;
  }
  return ret;
}

// serialize size cannot return error code
OB_DEF_SERIALIZE_SIZE(ObSqlUDTMeta)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              attribute_cnt_,
              fixed_attr_cnt_,
              fixed_offset_,
              pl_type_,
              udt_id_,
              child_attr_cnt_,
              leaf_attr_cnt_,
              nested_udt_number_,
              varray_capacity_,
              udt_name_len_);
  len += udt_name_len_;
  len += sizeof(ObSqlUDTAttrMeta) * child_attr_cnt_;
  len += sizeof(ObSqlUDTAttrMeta) * leaf_attr_cnt_;
  return len;
}

int ob_get_reserved_udt_meta(uint16_t subschema_id, ObSqlUDTMeta &udt_meta)
{
  int ret = OB_SUCCESS;
  if (subschema_id == ObXMLSqlType) {
    udt_meta.reset();
    udt_meta.udt_id_ = T_OBJ_XML;
    udt_meta.udt_name_ = "XMLTYPE";
    udt_meta.udt_name_len_ = strlen("XMLTYPE");
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported reserved subschema id", K(ret), K(subschema_id));
  }
  return ret;
}

int ob_get_reserved_subschema(uint64_t udt_id, uint16_t &subschema_id)
{
  int ret = OB_SUCCESS;
  if (udt_id == T_OBJ_XML) {
    subschema_id = ObXMLSqlType;
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported udt id", K(ret), K(udt_id));
  }
  return ret;
}

} // namespace common
} // namespace oceanbase