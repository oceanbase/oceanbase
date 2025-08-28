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
#include "common/object/ob_object.h"
#include "ob_collection_type.h"
#include "share/ob_errno.h"
#include "lib/string/ob_string_buffer.h"
#include "lib/utility/ob_fast_convert.h"

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
    if (element_type_->get_compatiable_type_id() != other.element_type_->get_compatiable_type_id()) {
    } else if (element_type_->type_id_ == ObNestedType::OB_BASIC_TYPE) {
      bret = static_cast<ObCollectionBasicType*>(element_type_)->has_same_super_type(*static_cast<ObCollectionBasicType*>(other.element_type_));
    } else {
      bret = static_cast<ObCollectionArrayType*>(element_type_)->has_same_super_type(*static_cast<ObCollectionArrayType*>(other.element_type_));
    }
  }
  return bret;
}

int ObCollectionArrayType::generate_spec_type_info(const ObString &type, ObString &type_info)
{
  int ret = OB_SUCCESS;
  ObStringBuffer buffer(&allocator_);
  ObFastFormatInt ffi(dim_cnt_);
  if (OB_FAIL(buffer.reserve(ffi.length() + type.length() + sizeof("VECTOR(, )")))) {
    LOG_WARN("fail to reserve space for string buffer", K(ret), K(ffi.length()), K(type.length()));
  } else if (OB_FAIL(buffer.append("VECTOR("))) {
    LOG_WARN("fail to append str", K(ret));
  } else if (OB_FAIL(buffer.append(ffi.str()))) {
    LOG_WARN("fail to append str", K(ret), K(ffi.str()));
  } else if (OB_FAIL(buffer.append(", "))) {
    LOG_WARN("fail to append str", K(ret));
  } else if (OB_FAIL(buffer.append(type))) {
    LOG_WARN("fail to append str", K(ret), K(type));
  } else if (OB_FAIL(buffer.append(")"))) {
    LOG_WARN("fail to append str", K(ret));
  } else if (OB_FAIL(buffer.get_result_string(type_info))) {
    LOG_WARN("fail to get result string", K(ret));
  }
  return ret;
}

bool ObCollectionArrayType::check_is_valid_vector() const
{
  bool is_valid = true;
  ObCollectionBasicType *meta_info = reinterpret_cast<ObCollectionBasicType*>(element_type_);
  if (OB_ISNULL(meta_info) || meta_info->type_id_ != ObNestedType::OB_BASIC_TYPE) {
    is_valid = false;
  } else {
    ObObjType obj_type = meta_info->basic_meta_.get_obj_type();
    if (obj_type != ObFloatType && obj_type != ObUTinyIntType) {
      is_valid = false;
    }
  }
  return is_valid;
}

int ObCollectionMapType::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(key_type_) || OB_ISNULL(value_type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid array type for serialize", K(ret));
  } else {
    LST_DO_CODE(OB_UNIS_ENCODE, type_id_);
    if (OB_FAIL(key_type_->serialize(buf, buf_len, pos))) {
      LOG_WARN("serialize array key type failed", K(ret));
    } else if (OB_FAIL(value_type_->serialize(buf, buf_len, pos))) {
      LOG_WARN("serialize array value type failed", K(ret));
    }
  }
  return ret;
}

int ObCollectionMapType::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, type_id_);
  if (OB_FAIL(ObSqlCollectionInfo::collection_type_deserialize(allocator_, buf, data_len, pos, key_type_))) {
    LOG_WARN("deserialize key type failed", K(ret));
  } else if (OB_FAIL(ObSqlCollectionInfo::collection_type_deserialize(allocator_, buf, data_len, pos, value_type_))) {
    LOG_WARN("deserialize value type failed", K(ret));
  }
  return ret;
}

int64_t ObCollectionMapType::get_serialize_size() const
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, type_id_);
  len += key_type_->get_serialize_size();
  len += value_type_->get_serialize_size();
  return len;
}

int ObCollectionMapType::deep_copy(ObIAllocator &allocator, ObCollectionTypeBase *&dst) const
{
  int ret = OB_SUCCESS;
  ObCollectionMapType *buf = OB_NEWx(ObCollectionMapType, &allocator, allocator);
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc collection map type memory failed", K(ret));
  } else if (OB_FAIL(key_type_->deep_copy(allocator, buf->key_type_))) {
    LOG_WARN("do key type deep copy failed", K(ret));
  } else if (OB_FAIL(value_type_->deep_copy(allocator, buf->value_type_))) {
    LOG_WARN("do value type deep copy failed", K(ret));
  } else {
    buf->type_id_ = type_id_;
    dst = buf;
  }
  return ret;
}

bool ObCollectionMapType::has_same_super_type(const ObCollectionMapType &other) const
{
  bool key_ret = false;
  bool value_ret = false;
  if (other.type_id_ != ObNestedType::OB_MAP_TYPE) {
    // return false
  } else if (OB_NOT_NULL(key_type_) && OB_NOT_NULL(value_type_)
             && OB_NOT_NULL(other.key_type_) && OB_NOT_NULL(other.value_type_)) {
    if (key_type_->get_compatiable_type_id() != other.key_type_->get_compatiable_type_id()) {
      // return false
    } else if (key_type_->type_id_ == ObNestedType::OB_BASIC_TYPE) {
      key_ret = static_cast<ObCollectionBasicType*>(key_type_)->has_same_super_type(*static_cast<ObCollectionBasicType*>(other.key_type_));
    } else {
      key_ret = static_cast<ObCollectionArrayType*>(key_type_)->has_same_super_type(*static_cast<ObCollectionArrayType*>(other.key_type_));
    }
    if (value_type_->get_compatiable_type_id() != other.value_type_->get_compatiable_type_id()) {
      // return false
    } else if (value_type_->type_id_ == ObNestedType::OB_BASIC_TYPE) {
      value_ret = static_cast<ObCollectionBasicType*>(value_type_)->has_same_super_type(*static_cast<ObCollectionBasicType*>(other.value_type_));
    } else {
      value_ret = static_cast<ObCollectionArrayType*>(value_type_)->has_same_super_type(*static_cast<ObCollectionArrayType*>(other.value_type_));
    }
  }
  return key_ret && value_ret;
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
  } else if (type_id_tmp == ObNestedType::OB_MAP_TYPE
             || type_id_tmp == ObNestedType::OB_SPARSE_VECTOR_TYPE) {
    if (OB_ISNULL(collection_meta = OB_NEWx(ObCollectionMapType, &allocator, allocator))) {
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
      case ObFloatType:
        meta_info->basic_meta_.meta_.set_ufloat();
        break;
      case ObDoubleType:
        meta_info->basic_meta_.meta_.set_udouble();
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
  int val = std::stoi(name);
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
        if (val <= -1 || val > OB_MAX_VARCHAR_LENGTH / 4) {
          ret = OB_ERR_TOO_LONG_COLUMN_LENGTH;
          LOG_WARN("data length is invalid", K(ret), K(val));
        } else {
          meta_info->basic_meta_.set_length(val);
        }
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

int ObSqlCollectionInfo::set_element_meta(const std::string &name, ObCollectionBasicType *meta_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(meta_info) || meta_info->type_id_ != ObNestedType::OB_BASIC_TYPE) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid meta info", K(ret), K(meta_info));
  } else {
    ObDataType &basic_meta = static_cast<ObCollectionBasicType *>(meta_info)->basic_meta_;
    if (0 == name.compare("NULL")) {
      basic_meta.meta_.set_null();
    } else if (0 == name.compare("UTINYINT")) {
      basic_meta.meta_.set_utinyint();
      basic_meta.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObUTinyIntType].scale_);
      basic_meta.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObUTinyIntType].precision_);
    } else if (0 == name.compare("TINYINT")) {
      basic_meta.meta_.set_tinyint();
      basic_meta.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObTinyIntType].scale_);
      basic_meta.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObTinyIntType].precision_);
    } else if (0 == name.compare("SMALLINT")) {
      basic_meta.meta_.set_smallint();
      basic_meta.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObSmallIntType].scale_);
      basic_meta.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObSmallIntType].precision_);
    } else if (0 == name.compare("MEDIUMINT")) {
      basic_meta.meta_.set_mediumint();
      basic_meta.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObMediumIntType].scale_);
      basic_meta.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObMediumIntType].precision_);
    } else if (0 == name.compare("INT")) {
      basic_meta.meta_.set_int32();
      basic_meta.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObInt32Type].scale_);
      basic_meta.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObInt32Type].precision_);
    } else if (0 == name.compare("BIGINT")) {
      basic_meta.meta_.set_int();
      basic_meta.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].scale_);
      basic_meta.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].precision_);
    } else if (0 == name.compare("FLOAT")) {
      basic_meta.meta_.set_float();
    } else if (0 == name.compare("DOUBLE")) {
      basic_meta.meta_.set_double();
    } else if (0 == name.compare("DECIMAL")) {
      basic_meta.meta_.set_number();
      basic_meta.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObNumberType].scale_);
      basic_meta.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObNumberType].precision_);
    } else if (0 == name.compare("DATETIME")) {
      basic_meta.meta_.set_datetime();
    } else if (0 == name.compare("MYSQL_DATETIME")) {
      basic_meta.meta_.set_mysql_datetime();
    } else if (0 == name.compare("TIMESTAMP")) {
      basic_meta.meta_.set_timestamp();
    } else if (0 == name.compare("DATE")) {
      basic_meta.meta_.set_date();
    } else if (0 == name.compare("MYSQL_DATE")) {
      basic_meta.meta_.set_mysql_date();
    } else if (0 == name.compare("TIME")) {
      basic_meta.meta_.set_time();
    } else if (0 == name.compare("YEAR")) {
      basic_meta.meta_.set_year();
    } else if (0 == name.compare("VARCHAR")) {
      basic_meta.meta_.set_varchar();
      // use default CS
      basic_meta.set_collation_type(CS_TYPE_UTF8MB4_BIN);
      basic_meta.set_collation_level(CS_LEVEL_COERCIBLE);
    } else if (0 == name.compare("VARBINARY")) {
      basic_meta.meta_.set_varbinary();
    } else if (0 == name.compare("CHAR")) {
      basic_meta.meta_.set_char();
    } else if (0 == name.compare("BINARY")) {
      basic_meta.meta_.set_binary();
    } else if (0 == name.compare("BIT")) {
      basic_meta.meta_.set_bit();
    } else if (0 == name.compare("JSON")) {
      basic_meta.meta_.set_json();
    } else if (0 == name.compare("GEOMETRY")) {
      basic_meta.meta_.set_geometry();
    } else if (0 == name.compare("DECIMAL_INT")) {
      basic_meta.meta_.set_decimal_int();
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported element type", K(ret), K(ObString(name.length(), name.c_str())));
    }
  }
  return ret;
}

int ObSqlCollectionInfo::parse_type_info() {
  int ret = OB_SUCCESS;
  const std::string type_info(name_def_, name_len_);
  ObCollectionTypeBase *meta_info = NULL;
  uint8_t arr_depth = 0;
  if (OB_FAIL(parse_collection_info(type_info, meta_info, arr_depth))) {
    LOG_WARN("parse type info failed", K(ret), K(ObString(type_info.length(), type_info.data())));
  } else if (OB_ISNULL(meta_info) && OB_FAIL(parse_element_info(type_info, meta_info, true))) {
    LOG_WARN("parse basic element info failed", K(ret), K(ObString(type_info.length(), type_info.data())));
  } else {
    collection_meta_ = meta_info;
  }
  return ret;
}
int ObSqlCollectionInfo::parse_collection_info(std::string type_info, ObCollectionTypeBase *&meta_info, uint8_t &arr_depth)
{
  int ret = OB_SUCCESS;

  if (0 == type_info.compare("SPARSEVECTOR")) {
    if (OB_ISNULL(meta_info = OB_NEWx(ObCollectionMapType, &allocator_, allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to create array type meta", K(ret));
    } else {
      meta_info->type_id_ = ObNestedType::OB_SPARSE_VECTOR_TYPE;
      if (OB_FAIL(parse_sparse_vector_element_info(static_cast<ObCollectionMapType *>(meta_info)->key_type_,
              static_cast<ObCollectionMapType *>(meta_info)->value_type_))) {
        LOG_WARN("parse sparse value failed", K(ret));
      }
    }
  } else {
    const uint8_t OB_ARRAY_MAX_NESTED_LEVEL = 6;/* constistent with pg*/
    const std::regex pattern(R"((ARRAY|VECTOR|MAP)\((.*)\))", std::regex_constants::icase);
    std::smatch matches;
    if (!std::regex_search(type_info, matches, pattern)) {
      // not collection type, do nothing
    } else {
      // is collection type
      if (matches.size() != 3) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid matches size", K(ret), K(matches.size()), K(ObString(type_info.length(), type_info.data())));
      } else if (++arr_depth > OB_ARRAY_MAX_NESTED_LEVEL) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not supported array depth", K(ret), K(arr_depth), K(OB_ARRAY_MAX_NESTED_LEVEL));
      } else {
        std::string type_name = matches[1];
        std::string type_value = matches[2];
        if (0 == type_name.compare("ARRAY")) {
          if (OB_ISNULL(meta_info = OB_NEWx(ObCollectionArrayType, &allocator_, allocator_))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to create array type meta", K(ret));
          } else {
            meta_info->type_id_ = ObNestedType::OB_ARRAY_TYPE;
            ObCollectionTypeBase *&elem_meta_info =static_cast<ObCollectionArrayType *>(meta_info)->element_type_;
            if (OB_FAIL(parse_collection_info(type_value, elem_meta_info, arr_depth))) {
              LOG_WARN("parse array value failed", K(ret), K(ObString(type_value.length(), type_value.data())));
            } else if (OB_NOT_NULL(elem_meta_info)) {
              // array type
              if (elem_meta_info->type_id_ == ObNestedType::OB_MAP_TYPE || elem_meta_info->type_id_ == ObNestedType::OB_SPARSE_VECTOR_TYPE) {
                // currently not support
                ret = OB_NOT_SUPPORTED;
                LOG_WARN("not supported nested map type", K(ret));
              }
            } else if (OB_FAIL(parse_element_info(type_value, elem_meta_info))) {
              LOG_WARN("parse element info failed", K(ret), K(ObString(type_info.length(), type_info.data())));
            }
          }
        } else if (0 == type_name.compare("VECTOR")) {
          if (OB_ISNULL(meta_info = OB_NEWx(ObCollectionArrayType, &allocator_, allocator_))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to create array type meta", K(ret));
          } else {
            meta_info->type_id_ = ObNestedType::OB_VECTOR_TYPE;
            if (OB_FAIL(parse_vec_element_info(type_value,
                    static_cast<ObCollectionArrayType *>(meta_info)->element_type_,
                    static_cast<ObCollectionArrayType *>(meta_info)->dim_cnt_))) {
              LOG_WARN("parse vector value failed", K(ret), K(ObString(type_value.length(), type_value.data())));
            } else if (!static_cast<ObCollectionArrayType *>(meta_info)->check_is_valid_vector()) {
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("not supported vector meta", K(ret), KPC(meta_info));
            }
          }
        } else if (0 == type_name.compare("MAP")) {
          if (OB_ISNULL(meta_info = OB_NEWx(ObCollectionMapType, &allocator_, allocator_))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to create array type meta", K(ret));
          } else {
            meta_info->type_id_ = ObNestedType::OB_MAP_TYPE;
            if (OB_FAIL(parse_map_element_info(type_value,
                    static_cast<ObCollectionMapType *>(meta_info)->key_type_,
                    static_cast<ObCollectionMapType *>(meta_info)->value_type_,
                    arr_depth))) {
              LOG_WARN("parse map value failed", K(ret), K(ObString(type_value.length(), type_value.data())));
            }
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid type name", K(ret), K(ObString(type_info.length(), type_info.data())));
        }
      }
    }
  }
  return ret;
}

int ObSqlCollectionInfo::parse_vec_element_info(std::string type_info, ObCollectionTypeBase *&meta_info, uint32_t &dim)
{
  int ret = OB_SUCCESS;
  ObCollectionBasicType *basic_meta_info = NULL;
  if (OB_ISNULL(basic_meta_info = OB_NEWx(ObCollectionBasicType, &allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create basic element type meta", K(ret));
  } else {
    meta_info = basic_meta_info;
    meta_info->type_id_ = ObNestedType::OB_BASIC_TYPE;
    basic_meta_info->basic_meta_.meta_.set_float(); // set default type
    const std::regex pattern(R"(\d+|\w+)");
    std::smatch matches;
    while (OB_SUCC(ret) && std::regex_search(type_info, matches, pattern)) {
      std::string type_name = matches[0];
      type_info = matches.suffix().str();
      if (isNumber(type_name)) {
        int32 get_dim = std::stoi(type_name);
        if (get_dim <= 0) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid vector meta", K(ret));
        } else {
          dim = static_cast<uint32_t>(get_dim);
        }
      } else if (0 == type_name.compare("UNSIGNED")) {
        // to parse vector(dim, tinyint unsigned)
        if (OB_FAIL(set_element_meta_unsigned(basic_meta_info))) {
          LOG_WARN("set element meta unsighed failed", K(ret));
        }
      } else if (0 == type_name.compare("UTINYINT")
                 || 0 == type_name.compare("TINYINT")
                 || 0 == type_name.compare("FLOAT")) {
        if (OB_FAIL(set_element_meta(type_name, basic_meta_info))) {
          LOG_WARN("create meta info failed", K(ret));
        }
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("vector only support utinyint/float elem type", K(ret));
      }
    } // end while
  }
  return ret;
}

int ObSqlCollectionInfo::parse_element_info(std::string type_info, ObCollectionTypeBase *&meta_info, bool is_root)
{
  int ret = OB_SUCCESS;
  ObCollectionBasicType *basic_meta_info = NULL;
  if (OB_ISNULL(basic_meta_info = OB_NEWx(ObCollectionBasicType, &allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create basic element type meta", K(ret));
  } else {
    meta_info = basic_meta_info;
    meta_info->type_id_ = ObNestedType::OB_BASIC_TYPE;
    const std::regex pattern(R"(\d+|\w+)");
    std::smatch matches;
    uint8_t idx = 0;
    while (OB_SUCC(ret) && std::regex_search(type_info, matches, pattern)) {
      std::string type_name = matches[0];
      type_info = matches.suffix().str(); // 更新 text 为剩余未匹配的部分
      if (is_root && isNumber(type_name)) {
        // vector element is float
        basic_meta_info->basic_meta_.meta_.set_float();
      } else if (isNumber(type_name)) {
        if (OB_FAIL(set_element_meta_info(type_name, idx++, basic_meta_info))) {
          LOG_WARN("set element meta info failed", K(ret));
        }
      } else if (0 == type_name.compare("UNSIGNED")) {
        if (OB_FAIL(set_element_meta_unsigned(basic_meta_info))) {
          LOG_WARN("set element meta unsighed failed", K(ret));
        }
      } else {
        if (OB_FAIL(set_element_meta(type_name, basic_meta_info))) {
          LOG_WARN("create meta info failed", K(ret));
        }
      }
    } // end while
  }
  return ret;
}

int ObSqlCollectionInfo::parse_map_element_info(std::string type_info, 
                                                ObCollectionTypeBase *&key_meta_info,
                                                ObCollectionTypeBase *&value_meta_info,
                                                uint8_t &arr_depth)
{
  int ret = OB_SUCCESS;
  std::string key_type_info;
  std::string value_type_info;
  size_t comma_pos = type_info.find(',');
  uint8_t key_depth;

  if (arr_depth > 1) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("nested map is not support", K(ret));
  } else if (comma_pos == std::string::npos) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid map element info", K(ret), K(ObString(type_info.length(), type_info.data())));
  } else {
    key_type_info = type_info.substr(0, comma_pos);
    value_type_info = type_info.substr(comma_pos + 1);
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(key_meta_info = OB_NEWx(ObCollectionArrayType, &allocator_, allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create array type meta", K(ret));
  } else if (OB_ISNULL(value_meta_info = OB_NEWx(ObCollectionArrayType, &allocator_, allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create array type meta", K(ret));
  } else {
    key_meta_info->type_id_ = ObNestedType::OB_ARRAY_TYPE;
    value_meta_info->type_id_ = ObNestedType::OB_ARRAY_TYPE;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(parse_element_info(key_type_info, static_cast<ObCollectionArrayType *>(key_meta_info)->element_type_))) {
    LOG_WARN("parse element info failed", K(ret), K(ObString(key_type_info.length(), key_type_info.data())));
  } else if (OB_FAIL(parse_collection_info(value_type_info, static_cast<ObCollectionArrayType *>(value_meta_info)->element_type_, arr_depth))) {
    LOG_WARN("parse array value failed", K(ret), K(ObString(value_type_info.length(), value_type_info.data())));
  } else if (OB_NOT_NULL(static_cast<ObCollectionArrayType *>(value_meta_info)->element_type_)) {
    // array type
    if (static_cast<ObCollectionArrayType *>(value_meta_info)->element_type_->type_id_ == ObNestedType::OB_MAP_TYPE
        || static_cast<ObCollectionArrayType *>(value_meta_info)->element_type_->type_id_ == ObNestedType::OB_SPARSE_VECTOR_TYPE) {
      // currently not support
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported nested map type", K(ret));
    }
  } else if (OB_FAIL(parse_element_info(value_type_info, static_cast<ObCollectionArrayType *>(value_meta_info)->element_type_))) {
    LOG_WARN("parse element info failed", K(ret), K(ObString(value_type_info.length(), value_type_info.data())));
  }
  return ret;
}

int ObSqlCollectionInfo::parse_sparse_vector_element_info(ObCollectionTypeBase *&key_meta_info, ObCollectionTypeBase *&value_meta_info)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(key_meta_info = OB_NEWx(ObCollectionArrayType, &allocator_, allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create array type meta", K(ret));
  } else if (OB_ISNULL(value_meta_info = OB_NEWx(ObCollectionArrayType, &allocator_, allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create array type meta", K(ret));
  } else {
    key_meta_info->type_id_ = ObNestedType::OB_ARRAY_TYPE;
    value_meta_info->type_id_ = ObNestedType::OB_ARRAY_TYPE;
  }

  ObCollectionBasicType *key_basic_meta_info = NULL;
  ObCollectionBasicType *value_basic_meta_info = NULL;
  if (OB_ISNULL(key_basic_meta_info = OB_NEWx(ObCollectionBasicType, &allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create key basic element type meta", K(ret));
  } else if (OB_ISNULL(value_basic_meta_info = OB_NEWx(ObCollectionBasicType, &allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create value basic element type meta", K(ret));
  } else {
    static_cast<ObCollectionArrayType *>(key_meta_info)->element_type_ = key_basic_meta_info;
    key_basic_meta_info->basic_meta_.meta_.set_uint32();
    const ObAccuracy &key_default_accuracy = ObAccuracy::DDL_DEFAULT_ACCURACY2[0][ObUInt32Type];
    key_basic_meta_info->basic_meta_.set_precision(key_default_accuracy.get_precision());
    key_basic_meta_info->basic_meta_.set_scale(0);

    static_cast<ObCollectionArrayType *>(value_meta_info)->element_type_ = value_basic_meta_info;
    value_basic_meta_info->basic_meta_.meta_.set_float();
    const ObAccuracy &value_default_accuracy = ObAccuracy::DDL_DEFAULT_ACCURACY2[0][ObFloatType];
    value_basic_meta_info->basic_meta_.set_precision(value_default_accuracy.get_precision());
    value_basic_meta_info->basic_meta_.set_scale(value_default_accuracy.get_scale());
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
    } else if (collection_meta_->type_id_ == ObNestedType::OB_MAP_TYPE || collection_meta_->type_id_ == ObNestedType::OB_SPARSE_VECTOR_TYPE) {
      b_ret = static_cast<ObCollectionMapType*>(collection_meta_)->has_same_super_type(*static_cast<ObCollectionMapType*>(other.collection_meta_));
    } else {
      b_ret = static_cast<ObCollectionArrayType*>(collection_meta_)->has_same_super_type(*static_cast<ObCollectionArrayType*>(other.collection_meta_));
    }
  }
  return b_ret;
}

int ObSqlCollectionInfo::get_child_def_string(ObString &child_def) const
{
  int ret = OB_SUCCESS;
  if (name_len_ <= 7) { // array()
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(*this));
  } else if (ObString(6, name_def_).compare("ARRAY(") == 0) {
    child_def = ObString(name_len_ - 7, name_def_ + 6);
  } else if (ObString(7, name_def_).compare("VECTOR(") == 0) {
    child_def = ObString(name_len_ - 8, name_def_ + 7);
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(*this));
  }
  return ret;
}

int ObSqlCollectionInfo::get_map_attr_def_string(ObIAllocator &allocator, ObString &def, bool is_values) const
{
  int ret = OB_SUCCESS;
  if (name_len_ <= 5) { // map()
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(*this));
  } else if (ObString(4, name_def_).compare("MAP(") == 0) {
    ObStringBuffer tmp_buf(&allocator);
    if (OB_FAIL(tmp_buf.append("ARRAY("))) {
      LOG_WARN("failed to append prefix to tmp_buf", K(ret), K(*this));
    } else {    
      ObString kv_def = ObString(name_len_ - 5, name_def_ + 4);
      if (!is_values) {
        // key_def
        kv_def = kv_def.split_on(',');
      } else {
        // value_def
        kv_def = kv_def.after(',');
      }
      if (OB_FAIL(tmp_buf.append(kv_def))) {
        LOG_WARN("failed to append subschema to tmp_buf", K(ret), K(*this));
      } else if (OB_FAIL(tmp_buf.append(")"))) {
        LOG_WARN("failed to append postfix to tmp_buf", K(ret), K(*this));
      } else {
        def = tmp_buf.string();
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(*this));
  }
  return ret;
}

} // namespace common
} // namespace oceanbase