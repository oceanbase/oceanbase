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

#define USING_LOG_PREFIX SHARE_SCHEMA
#include "share/schema/ob_objpriv_mysql_schema_struct.h"
namespace oceanbase
{
namespace share
{
namespace schema
{

ObObjMysqlPriv& ObObjMysqlPriv::operator=(const ObObjMysqlPriv &other)
{
  if (this != &other) {
    reset();
    int ret = OB_SUCCESS;
    if (OB_FAIL(ObPriv::assign(other))) {
      LOG_WARN("assign failed", K(ret));
    } else if (OB_FAIL(deep_copy_str(other.obj_name_, obj_name_))) {
      LOG_WARN("Fail to deep copy db", K(ret));
    } else {
      error_ret_ = other.error_ret_;
      obj_type_ = other.obj_type_;
    }
    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }
  return *this;
}

int ObObjMysqlPriv::assign(const ObObjMysqlPriv &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    reset();
    if (OB_FAIL(ObPriv::assign(other))) {
      LOG_WARN("assign failed", K(ret));
    } else if (OB_FAIL(deep_copy_str(other.obj_name_, obj_name_))) {
      LOG_WARN("Fail to deep copy db", K(ret));
    } else {
      obj_type_ = other.obj_type_;
      error_ret_ = other.error_ret_;
    }
    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }
  return ret;
}

bool ObObjMysqlPriv::is_valid() const
{
  return ObSchema::is_valid()
         && ObPriv::is_valid()
         && !obj_name_.empty()
         && obj_type_ != common::OB_INVALID_ID;
}

void ObObjMysqlPriv::reset()
{
  ObSchema::reset();
  ObPriv::reset();
  obj_name_.reset();
  obj_type_ = common::OB_INVALID_ID;
}

int64_t ObObjMysqlPriv::get_convert_size() const
{
  int64_t convert_size = 0;
  convert_size += ObPriv::get_convert_size();
  convert_size += sizeof(ObObjMysqlPriv) - sizeof(ObPriv);
  convert_size += obj_name_.length() + 1;
  return convert_size;
}

OB_DEF_SERIALIZE(ObObjMysqlPriv)
{
  int ret = OB_SUCCESS;
  BASE_SER((, ObPriv));
  LST_DO_CODE(OB_UNIS_ENCODE, obj_name_, obj_type_);
  return ret;
}

OB_DEF_DESERIALIZE(ObObjMysqlPriv)
{
  int ret = OB_SUCCESS;
  ObString obj_name;
  uint64_t obj_type;
  BASE_DESER((, ObPriv));
  LST_DO_CODE(OB_UNIS_DECODE, obj_name, obj_type);
  if (OB_FAIL(ret)) {
    LOG_WARN("Fail to deserialize data", K(ret));
  } else if (OB_FAIL(deep_copy_str(obj_name, obj_name_))) {
    LOG_WARN("Fail to deep copy user_name", K(obj_name), K(ret));
  } else {
    obj_type_ = obj_type;
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObObjMysqlPriv)
{
  int64_t len = ObPriv::get_serialize_size();
  LST_DO_CODE(OB_UNIS_ADD_LEN, obj_name_, obj_type_);
  return len;
}
}
}
}