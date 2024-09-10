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
#include <regex>
#include "ob_collection_type.h"

namespace oceanbase {
namespace common {


int ObCollectionBasicType::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  *reinterpret_cast<uint16_t *>(buf + pos) = type_id_;
  pos += sizeof(type_id_);
  MEMCPY(buf + pos, reinterpret_cast<const char *>(&basic_meta_), sizeof(basic_meta_));
  pos += sizeof(basic_meta_);
  return ret;
}

int ObCollectionBasicType::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  type_id_ = *reinterpret_cast<const uint16_t *>(buf + pos);
  pos += sizeof(type_id_);
  MEMCPY(reinterpret_cast<char *>(&basic_meta_), buf + pos, sizeof(basic_meta_));
  pos += sizeof(basic_meta_);
  return ret;
}

int64_t ObCollectionBasicType::get_serialize_size() const
{
  int64_t len = 0;
  len += sizeof(type_id_);
  len += sizeof(basic_meta_);
  return len;
}

int ObCollectionBasicType::deep_copy(ObIAllocator &allocator, ObCollectionTypeBase *&dst) const
{
  int ret = OB_SUCCESS;
  ObCollectionBasicType *buf = OB_NEWx(ObCollectionBasicType, &allocator);
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc collection basic type memory failed", K(ret));
  } else {
    buf->type_id_ = type_id_;
    buf->basic_meta_ = basic_meta_;
    dst = buf;
  }
  return ret;
}

bool ObCollectionBasicType::has_same_super_type(const ObCollectionBasicType &other) const
{
  bool bret = false;
  if (get_compatiable_type_id() != other.get_compatiable_type_id()) {
  } else if (basic_meta_.meta_ !=  other.basic_meta_.meta_) {
    if (ob_is_null(basic_meta_.meta_.get_type()) || ob_is_null(other.basic_meta_.meta_.get_type())) {
      bret = true;
    } else if (ob_is_numeric_type(basic_meta_.meta_.get_type()) && ob_is_numeric_type(other.basic_meta_.meta_.get_type())) {
      bret = true;
    }
  } else {
    bret = true;
  }
  return bret;
}

int ObCollectionArrayType::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(element_type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid array type for serialize", K(ret));
  } else {
    LST_DO_CODE(OB_UNIS_ENCODE, type_id_);
    LST_DO_CODE(OB_UNIS_ENCODE, dim_cnt_);
    if (OB_FAIL(element_type_->serialize(buf, buf_len, pos))) {
      LOG_WARN("serialize array element type failed", K(ret));
    }
  }
  return ret;
}

int ObCollectionArrayType::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, type_id_);
  LST_DO_CODE(OB_UNIS_DECODE, dim_cnt_);
  if (OB_FAIL(ObSqlCollectionInfo::collection_type_deserialize(allocator_, buf, data_len, pos, element_type_))) {
    LOG_WARN("deserialize element type failed", K(ret));
  }
  return ret;
}

int64_t ObCollectionArrayType::get_serialize_size() const
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, type_id_);
  LST_DO_CODE(OB_UNIS_ADD_LEN, dim_cnt_);
  len += element_type_->get_serialize_size();
  return len;
}

int ObCollectionArrayType::deep_copy(ObIAllocator &allocator, ObCollectionTypeBase *&dst) const
{
  int ret = OB_SUCCESS;
  ObCollectionArrayType *buf = OB_NEWx(ObCollectionArrayType, &allocator, allocator);
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc collection arry type memory failed", K(ret));
  } else if (OB_FAIL(element_type_->deep_copy(allocator, buf->element_type_))) {
    LOG_WARN("do element type deep copy failed", K(ret));
  } else {
    buf->type_id_ = type_id_;
    buf->dim_cnt_ = dim_cnt_;
    dst = buf;
  }
  return ret;
}

bool ObCollectionArrayType::has_same_super_type(const ObCollectionArrayType &other) const
{
  bool bret = false;
  if (type_id_ == ObNestedType::OB_VECTOR_TYPE
      && other.type_id_ == ObNestedType::OB_VECTOR_TYPE && dim_cnt_ != other.dim_cnt_ ) {
    // return false
  } else if (get_compatiable_type_id() != other.get_compatiable_type_id()) {
    // return false
  } else if (OB_NOT_NULL(element_type_) && OB_NOT_NULL(other.element_type_)) {
    if (element_type_->type_id_ != other.element_type_->type_id_) {
    } else if (element_type_->type_id_ == ObNestedType::OB_BASIC_TYPE) {
      bret = static_cast<ObCollectionBasicType*>(element_type_)->has_same_super_type(*static_cast<ObCollectionBasicType*>(other.element_type_));
    } else {
      bret = static_cast<ObCollectionArrayType*>(element_type_)->has_same_super_type(*static_cast<ObCollectionArrayType*>(other.element_type_));
    }
  }
  return bret;
}

OB_DEF_SERIALIZE(ObSqlCollectionInfo)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ret)) {
  } else if (name_len_ <= 0 || OB_ISNULL(name_def_) || OB_ISNULL(collection_meta_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid udt name length for serialize", K(ret), K(*this));
  } else {
    *reinterpret_cast<size_t*>(buf + pos) = name_len_;
    pos += sizeof(name_len_);
    MEMCPY(buf + pos, name_def_, name_len_);
    pos += name_len_;
    if (OB_FAIL(collection_meta_->serialize(buf, buf_len, pos))) {
      LOG_WARN("invalid udt name length for serialize", K(ret), K(*this));
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObSqlCollectionInfo)
{
  int ret = OB_SUCCESS;
  name_len_ = *reinterpret_cast<const size_t*>(buf + pos);
  pos += sizeof(name_len_);
  if (name_len_ <= 0 || pos >= data_len) {
    ret = OB_DESERIALIZE_ERROR;
    LOG_WARN("invalid udt name length for deseriazlie", K(ret), K(*this), K(pos), K(data_len));
  } else {
    name_def_ = buf + pos;
    pos += name_len_;
    if (OB_FAIL(collection_type_deserialize(allocator_, buf, data_len, pos, collection_meta_))) {
      LOG_WARN("deserialize collection meta failed", K(ret), K(*this));
    }
  }
  return ret;
}

// serialize size cannot return error code
OB_DEF_SERIALIZE_SIZE(ObSqlCollectionInfo)
{
  int64_t len = 0;
  len += sizeof(name_len_);
  len += name_len_;
  len += collection_meta_->get_serialize_size();
  return len;
}

int ObSqlCollectionInfo::deep_copy(ObIAllocator &allocator, ObSqlCollectionInfo *&dst) const
{
  int ret = OB_SUCCESS;
  ObSqlCollectionInfo *buf = OB_NEWx(ObSqlCollectionInfo, &allocator, allocator);
  char *copy_name;
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc collection info memory failed", K(ret));
  } else if (OB_ISNULL(copy_name = static_cast<char *>(allocator.alloc(name_len_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc collection type name failed", K(ret));
  } else if (OB_FAIL(collection_meta_->deep_copy(allocator, buf->collection_meta_))) {
    LOG_WARN("do element type deep copy failed", K(ret));
  } else {
    MEMCPY(copy_name, name_def_, name_len_);
    ObString tmp_name(name_len_, copy_name);
    buf->set_name(tmp_name);
    dst = buf;
  }

  return ret;
}

int ObSqlCollectionInfo::collection_type_deserialize(ObIAllocator &allocator, const char* buf, const int64_t data_len,
                                                     int64_t& pos, ObCollectionTypeBase *&collection_meta)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  uint16_t type_id_tmp = 0;
  LST_DO_CODE(OB_UNIS_DECODE, type_id_tmp);
  pos = new_pos;
  if (OB_FAIL(ret)) {
  } else if (type_id_tmp == ObNestedType::OB_BASIC_TYPE) {
    if (OB_ISNULL(collection_meta = OB_NEWx(ObCollectionBasicType, &allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc element type failed", K(ret));
    } else if (OB_FAIL(collection_meta->deserialize(buf, data_len, pos))) {
      LOG_WARN("deserialize element type failed", K(ret));
    }
  } else if (type_id_tmp == ObNestedType::OB_ARRAY_TYPE
             || type_id_tmp == ObNestedType::OB_VECTOR_TYPE) {
   if (OB_ISNULL(collection_meta = OB_NEWx(ObCollectionArrayType, &allocator, allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc element type failed", K(ret));
    } else if (OB_FAIL(collection_meta->deserialize(buf, data_len, pos))) {
      LOG_WARN("deserialize element type failed", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid array type for serialize", K(ret), K(type_id_tmp));
  }
  return ret;
}

int ObSqlCollectionInfo::set_element_meta_unsigned(ObCollectionBasicType *meta_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(meta_info) || meta_info->type_id_ != ObNestedType::OB_BASIC_TYPE) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid meta info", K(ret), K(meta_info));
  } else {
    ObObjType obj_type = meta_info->basic_meta_.get_obj_type();
    switch (obj_type) {
      case ObTinyIntType:
        meta_info->basic_meta_.meta_.set_utinyint();
        break;
      case ObSmallIntType:
        meta_info->basic_meta_.meta_.set_usmallint();
        break;
      case ObInt32Type:
        meta_info->basic_meta_.meta_.set_uint32();
        break;
      case ObIntType:
        meta_info->basic_meta_.meta_.set_uint64();
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid meta info", K(ret), K(meta_info));
    }
  }
  return ret;
}

int ObSqlCollectionInfo::set_element_meta_info(const std::string &name, uint8_t meta_attr_idx, ObCollectionBasicType *meta_info)
{
  /* meta_attr_idx = 0 value is precision
     meta_attr_idx = 1 value is scale
  */
  int ret = OB_SUCCESS;
  int32_t val = std::stoi(name);
  if (OB_ISNULL(meta_info) || meta_info->type_id_ != ObNestedType::OB_BASIC_TYPE) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid meta info", K(ret), K(meta_info));
  } else {
    ObObjType obj_type = meta_info->basic_meta_.get_obj_type();
    ObObjTypeClass tc = meta_info->basic_meta_.get_type_class();
    const ObAccuracy &default_accuracy = ObAccuracy::DDL_DEFAULT_ACCURACY2[0][obj_type];
    switch (tc) {
      case ObIntTC:
      case ObUIntTC:
        // use default precision
        meta_info->basic_meta_.set_precision(default_accuracy.get_precision());
        meta_info->basic_meta_.set_scale(0);
        break;
      case ObFloatTC:
      case ObDoubleTC:
        // use default precision
        meta_info->basic_meta_.set_precision(default_accuracy.get_precision());
        meta_info->basic_meta_.set_scale(default_accuracy.get_scale());
        break;
      case ObStringTC:
        meta_info->basic_meta_.set_length(val);
        break;
      case ObDecimalIntTC :
        if (meta_attr_idx == 0) {
          meta_info->basic_meta_.set_precision(val);
        } else if (meta_attr_idx == 1) {
          meta_info->basic_meta_.set_scale(val);
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected meta_attr_idx", K(ret), K(meta_attr_idx));
        }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid data type", K(ret), K(tc));
    }
  }
  return ret;
}

int ObSqlCollectionInfo::create_meta_info_by_name(const std::string &name, ObCollectionTypeBase *&meta_info, uint8_t &arr_depth)
{
  int ret = OB_SUCCESS;
  if (0 == name.compare("ARRAY") || 0 == name.compare("VECTOR")) {
    if (OB_ISNULL(meta_info = OB_NEWx(ObCollectionArrayType, &allocator_, allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to create array type meta", K(ret));
    } else {
      const uint8_t OB_ARRAY_MAX_NESTED_LEVEL = 6;/* constistent with pg*/
      meta_info->type_id_ = (0 == name.compare("ARRAY")) ?
                            ObNestedType::OB_ARRAY_TYPE : ObNestedType::OB_VECTOR_TYPE;
      if (++arr_depth > OB_ARRAY_MAX_NESTED_LEVEL) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not supported array depth", K(ret), K(arr_depth), K(OB_ARRAY_MAX_NESTED_LEVEL));
      }
    }
  } else {
    if (OB_ISNULL(meta_info = OB_NEWx(ObCollectionBasicType, &allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to create basic element type meta", K(ret));
    } else {
      meta_info->type_id_ = ObNestedType::OB_BASIC_TYPE;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (0 == name.compare("NULL")) {
    static_cast<ObCollectionBasicType *>(meta_info)->basic_meta_.meta_.set_null();
  } else if (0 == name.compare("TINYINT")) {
    static_cast<ObCollectionBasicType *>(meta_info)->basic_meta_.meta_.set_tinyint();
  } else if (0 == name.compare("SMALLINT")) {
    static_cast<ObCollectionBasicType *>(meta_info)->basic_meta_.meta_.set_smallint();
  } else if (0 == name.compare("MEDIUMINT")) {
    static_cast<ObCollectionBasicType *>(meta_info)->basic_meta_.meta_.set_mediumint();
  } else if (0 == name.compare("INT")) {
    static_cast<ObCollectionBasicType *>(meta_info)->basic_meta_.meta_.set_int32();
  } else if (0 == name.compare("BIGINT")) {
    static_cast<ObCollectionBasicType *>(meta_info)->basic_meta_.meta_.set_int();
  } else if (0 == name.compare("FLOAT")) {
    static_cast<ObCollectionBasicType *>(meta_info)->basic_meta_.meta_.set_float();
  } else if (0 == name.compare("DOUBLE")) {
    static_cast<ObCollectionBasicType *>(meta_info)->basic_meta_.meta_.set_double();
  } else if (0 == name.compare("DECIMAL")) {
    static_cast<ObCollectionBasicType *>(meta_info)->basic_meta_.meta_.set_number();
  } else if (0 == name.compare("DATETIME")) {
    static_cast<ObCollectionBasicType *>(meta_info)->basic_meta_.meta_.set_datetime();
  } else if (0 == name.compare("TIMESTAMP")) {
    static_cast<ObCollectionBasicType *>(meta_info)->basic_meta_.meta_.set_timestamp();
  } else if (0 == name.compare("DATE")) {
    static_cast<ObCollectionBasicType *>(meta_info)->basic_meta_.meta_.set_date();
  } else if (0 == name.compare("TIME")) {
    static_cast<ObCollectionBasicType *>(meta_info)->basic_meta_.meta_.set_time();
  } else if (0 == name.compare("YEAR")) {
    static_cast<ObCollectionBasicType *>(meta_info)->basic_meta_.meta_.set_year();
  } else if (0 == name.compare("VARCHAR")) {
    static_cast<ObCollectionBasicType *>(meta_info)->basic_meta_.meta_.set_varchar();
    // use default CS
    static_cast<ObCollectionBasicType *>(meta_info)->basic_meta_.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    static_cast<ObCollectionBasicType *>(meta_info)->basic_meta_.set_collation_level(CS_LEVEL_COERCIBLE);
  } else if (0 == name.compare("VARBINARY")) {
    static_cast<ObCollectionBasicType *>(meta_info)->basic_meta_.meta_.set_varbinary();
  } else if (0 == name.compare("CHAR")) {
    static_cast<ObCollectionBasicType *>(meta_info)->basic_meta_.meta_.set_char();
  } else if (0 == name.compare("BINARY")) {
    static_cast<ObCollectionBasicType *>(meta_info)->basic_meta_.meta_.set_binary();
  } else if (0 == name.compare("BIT")) {
    static_cast<ObCollectionBasicType *>(meta_info)->basic_meta_.meta_.set_bit();
  } else if (0 == name.compare("JSON")) {
    static_cast<ObCollectionBasicType *>(meta_info)->basic_meta_.meta_.set_json();
  } else if (0 == name.compare("GEOMETRY")) {
    static_cast<ObCollectionBasicType *>(meta_info)->basic_meta_.meta_.set_geometry();
  } else if (0 == name.compare("DECIMAL_INT")) {
    static_cast<ObCollectionBasicType *>(meta_info)->basic_meta_.meta_.set_decimal_int();
  } else if (0 == name.compare("ARRAY") || 0 == name.compare("VECTOR")) {
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get type by name failed", K(ret));
  }

  return ret;
}

int ObSqlCollectionInfo::parse_type_info()
{
  int ret = OB_SUCCESS;
  bool is_root = true;
  uint8_t idx = 0;
  ObCollectionTypeBase *curr_meta = NULL;
  const std::string type_info(name_def_, name_len_);
  const std::regex pattern(R"(\d+|\w+)");
  std::smatch matches;
  uint8_t arr_depth = 0;

  std::string::const_iterator searchStart(type_info.cbegin());
  while (OB_SUCC(ret) && std::regex_search(searchStart, type_info.cend(), matches, pattern)) {
    for (std::smatch::iterator it = matches.begin(); it != matches.end() && OB_SUCC(ret); ++it) {
      const auto& match = *it;
      std::string type_name = match.str();
      if (is_root) {
        if (OB_FAIL(create_meta_info_by_name(type_name, collection_meta_, arr_depth))) {
          LOG_WARN("get type by name failed", K(ret));
        } else {
          is_root = false;
          curr_meta = collection_meta_;
        }
      } else if (OB_NOT_NULL(curr_meta) && curr_meta->type_id_ == ObNestedType::OB_ARRAY_TYPE
                && OB_FAIL(create_meta_info_by_name(type_name, static_cast<ObCollectionArrayType *>(curr_meta)->element_type_, arr_depth))) {
        LOG_WARN("create meta info failed", K(ret));
      } else if (OB_NOT_NULL(curr_meta) && curr_meta->type_id_ == ObNestedType::OB_ARRAY_TYPE) {
        curr_meta = static_cast<ObCollectionArrayType *>(curr_meta)->element_type_;
      } else if (OB_NOT_NULL(curr_meta) && curr_meta->type_id_ == ObNestedType::OB_VECTOR_TYPE
                 && isNumber(type_name)) {
        // vector element is float
        std::string vector_elem = "FLOAT";
        if (OB_FAIL(create_meta_info_by_name(vector_elem, static_cast<ObCollectionArrayType *>(curr_meta)->element_type_, arr_depth))) {
          LOG_WARN("create meta info failed", K(ret));
        } else {
          int32_t dim = std::stoi(type_name);
          static_cast<ObCollectionArrayType *>(curr_meta)->dim_cnt_ = dim;
        }
      } else if (isNumber(type_name) && OB_FAIL(set_element_meta_info(type_name, idx++, static_cast<ObCollectionBasicType *>(curr_meta)))) {
        LOG_WARN("set element meta info failed", K(ret));
      } else if (0 == type_name.compare("UNSIGNED") && OB_FAIL(set_element_meta_unsigned(static_cast<ObCollectionBasicType *>(curr_meta)))) {
        LOG_WARN("set element meta unsighed failed", K(ret));
      }
    }
    searchStart = matches.suffix().first;
  }
  return ret;
}

bool ObSqlCollectionInfo::has_same_super_type(const ObSqlCollectionInfo &other) const
{
  bool b_ret = false;
  if (OB_ISNULL(collection_meta_) || OB_ISNULL(other.collection_meta_)) {
    // return false
  } else if (collection_meta_->get_compatiable_type_id() != other.collection_meta_->get_compatiable_type_id()) {
    // return false
  } else {
     if (collection_meta_->type_id_ == ObNestedType::OB_BASIC_TYPE) {
      b_ret = static_cast<ObCollectionBasicType*>(collection_meta_)->has_same_super_type(*static_cast<ObCollectionBasicType*>(other.collection_meta_));
    } else {
      b_ret = static_cast<ObCollectionArrayType*>(collection_meta_)->has_same_super_type(*static_cast<ObCollectionArrayType*>(other.collection_meta_));
    }
  }
  return b_ret;
}

int ObSqlCollectionInfo::get_child_def_string(ObString &child_def) const
{
  int ret = OB_SUCCESS;
  const uint32_t min_len = 7; // array()
  if (name_len_ <= min_len) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(*this));
  } else {
    child_def = ObString(name_len_ - min_len, name_def_ + (min_len - 1));
  }
  return ret;
}

} // namespace common
} // namespace oceanbase