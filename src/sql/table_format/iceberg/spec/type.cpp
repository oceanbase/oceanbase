/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL

#include "sql/table_format/iceberg/spec/type.h"

#include "sql/table_format/iceberg/spec/schema_field.h"

namespace oceanbase
{
namespace sql
{
namespace iceberg
{

int StructType::add_schema_field(const SchemaField *field)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(field)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("field is null", K(ret));
  } else if (fields_count_ >= MAX_FIELDS_COUNT) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too many fields", K(ret));
  }
  fields_[fields_count_] = field;
  fields_count_++;
  return ret;
}

const SchemaField *StructType::get_field_by_id(int32_t field_id) const
{
  for (int i = 0; i < fields_count_; ++i) {
    if (fields_[i]->field_id() == field_id) {
      return fields_[i];
    }
  }
  return nullptr;
}

const SchemaField *StructType::get_field_by_index(int32_t index) const
{
  if (index < 0 || index >= fields_count_) {
    return nullptr;
  }
  return fields_[index];
}
const SchemaField *StructType::get_field_by_name(const ObString &name) const
{
  for (int i = 0; i < fields_count_; ++i) {
    if (fields_[i]->name() == name) {
      return fields_[i];
    }
  }
  return nullptr;
}

int StructType::to_json_kv_string(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  OZ(J_OBJ_START());
  OZ(databuff_printf(buf, buf_len, pos, R"("type": "struct")"));
  OZ(J_COMMA());
  if (schema_id_.has_value()) {
    OZ(databuff_printf(buf, buf_len, pos, R"("schema-id": %d)", schema_id_.value()));
    OZ(J_COMMA());
  }
  OZ(databuff_printf(buf, buf_len, pos, R"("fields": [)"));
  for (int i = 0; OB_SUCC(ret) && i < fields_count_; ++i) {
    CK(OB_NOT_NULL(fields_[i]));
    OZ(fields_[i]->to_json_kv_string(buf, buf_len, pos));
    if (i != fields_count_ - 1) {
      OZ(J_COMMA());
    }
  }
  OZ(databuff_printf(buf, buf_len, pos, R"(])"));
  OZ(J_OBJ_END());
  return ret;
}

const SchemaField *ListType::get_field_by_id(int32_t field_id) const
{
  if (element_->field_id() == field_id) {
    return element_;
  } else {
    return nullptr;
  }
}

const SchemaField *ListType::get_field_by_index(int32_t index) const
{
  if (index == 0) {
    return element_;
  } else {
    return nullptr;
  }
}

const SchemaField *ListType::get_field_by_name(const ObString &name) const
{
  if (element_->name() == name) {
    return element_;
  } else {
    return nullptr;
  }
}

int ListType::to_json_kv_string(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(element_), OB_NOT_NULL(element_->type()));
  OZ(J_OBJ_START());
  OZ(databuff_printf(buf, buf_len, pos, R"("type": "list")"));
  OZ(J_COMMA());
  OZ(databuff_printf(buf, buf_len, pos, R"("element-id": %d)", element_->field_id()));
  OZ(J_COMMA());
  OZ(databuff_printf(buf, buf_len, pos, R"("element": )"));
  OZ(element_->type()->to_json_kv_string(buf, buf_len, pos));
  OZ(J_COMMA());
  OZ(databuff_printf(buf,
                     buf_len,
                     pos,
                     R"("element-required": %s)",
                     element_->optional() ? "false" : "true"));
  OZ(J_OBJ_END());
  return ret;
}

const SchemaField *MapType::get_field_by_id(int32_t field_id) const
{
  if (field_id == key()->field_id()) {
    return key();
  } else if (field_id == value()->field_id()) {
    return value();
  } else {
    return nullptr;
  }
}

const SchemaField *MapType::get_field_by_index(int32_t index) const
{
  if (index == 0) {
    return key();
  } else if (index == 1) {
    return value();
  } else {
    return nullptr;
  }
}

const SchemaField *MapType::get_field_by_name(const ObString &name) const
{
  if (name == key()->name()) {
    return key();
  } else if (name == value()->name()) {
    return value();
  } else {
    return nullptr;
  }
}

int MapType::to_json_kv_string(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(key_),
     OB_NOT_NULL(value_),
     OB_NOT_NULL(key_->type()),
     OB_NOT_NULL(value_->type()));
  OZ(J_OBJ_START());
  OZ(databuff_printf(buf, buf_len, pos, R"("type": "map")"));
  OZ(J_COMMA());
  OZ(databuff_printf(buf, buf_len, pos, R"("key-id": %d)", key_->field_id()));
  OZ(J_COMMA());
  OZ(databuff_printf(buf, buf_len, pos, R"("key": )"));
  OZ(key_->type()->to_json_kv_string(buf, buf_len, pos));
  OZ(J_COMMA());
  OZ(databuff_printf(buf, buf_len, pos, R"("value-id": %d)", value_->field_id()));
  OZ(J_COMMA());
  OZ(databuff_printf(buf, buf_len, pos, R"("value": )"));
  OZ(value_->type()->to_json_kv_string(buf, buf_len, pos));
  OZ(J_COMMA());
  OZ(databuff_printf(buf,
                     buf_len,
                     pos,
                     R"("value-required": %s)",
                     value_->optional() ? "false" : "true"));
  OZ(J_OBJ_END());
  return ret;
}

BinaryType *BinaryType::get_instance()
{
  static BinaryType instance;
  return &instance;
}

int BinaryType::to_json_kv_string(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  OZ(databuff_printf(buf, buf_len, pos, R"("binary")"));
  return ret;
}

StringType *StringType::get_instance()
{
  static StringType instance;
  return &instance;
}

int StringType::to_json_kv_string(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  OZ(databuff_printf(buf, buf_len, pos, R"("string")"));
  return ret;
}

BooleanType *BooleanType::get_instance()
{
  static BooleanType instance;
  return &instance;
}

int BooleanType::to_json_kv_string(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  OZ(databuff_printf(buf, buf_len, pos, R"("boolean")"));
  return ret;
}

IntType *IntType::get_instance()
{
  static IntType instance;
  return &instance;
}

int IntType::to_json_kv_string(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  OZ(databuff_printf(buf, buf_len, pos, R"("int")"));
  return ret;
}

LongType *LongType::get_instance()
{
  static LongType instance;
  return &instance;
}

int LongType::to_json_kv_string(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  OZ(databuff_printf(buf, buf_len, pos, R"("long")"));
  return ret;
}

} // namespace iceberg

} // namespace sql

} // namespace oceanbase
