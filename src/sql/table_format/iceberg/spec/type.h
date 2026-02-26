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

#ifndef OCEANBASE_TYPE_H
#define OCEANBASE_TYPE_H

#include "sql/table_format/iceberg/ob_iceberg_type_fwd.h"
#include "src/share/ob_define.h"

#include <optional>

namespace oceanbase
{
namespace sql
{
namespace iceberg
{

class Type
{
public:
  Type() = default;
  virtual ~Type() = default;
  virtual TypeId type_id() const = 0;
  virtual bool is_primitive() const = 0;
  virtual bool is_nested() const = 0;
  virtual int to_json_kv_string(char *buf, const int64_t buf_len, int64_t &pos) const = 0;
};

class PrimitiveType : public Type
{
public:
  PrimitiveType() = default;
  virtual ~PrimitiveType() override = default;
  bool is_primitive() const override
  {
    return true;
  }
  bool is_nested() const override
  {
    return false;
  }
};

class NestedType : public Type
{
public:
  NestedType() = default;
  virtual ~NestedType() override = default;

  bool is_primitive() const override
  {
    return false;
  }
  bool is_nested() const override
  {
    return true;
  }

  virtual const SchemaField *get_field_by_id(int32_t field_id) const = 0;
  virtual const SchemaField *get_field_by_index(int32_t index) const = 0;
  virtual const SchemaField *get_field_by_name(const ObString &name) const = 0;
};

// A data type representing a struct with nested fields.
class StructType final : public NestedType
{
public:
  constexpr static const size_t MAX_FIELDS_COUNT = 32;
  typedef const SchemaField *field_ptr_fixed_array[MAX_FIELDS_COUNT];
  explicit StructType(std::initializer_list<const SchemaField *> fields)
  {
    fields_count_ = std::min(fields.size(), MAX_FIELDS_COUNT);
    int i = 0;
    for (const SchemaField *field : fields) {
      if (i >= fields_count_)
        break;
      fields_[i++] = field;
    }
  }
  StructType() = default;
  ~StructType() override = default;

  TypeId type_id() const override
  {
    return TypeId::kStruct;
  }
  const field_ptr_fixed_array &fields() const
  {
    return fields_;
  }
  int fields_count() const
  {
    return fields_count_;
  }
  int add_schema_field(const SchemaField *field);
  const SchemaField *get_field_by_id(int32_t field_id) const override;
  const SchemaField *get_field_by_index(int32_t index) const override;
  const SchemaField *get_field_by_name(const ObString &name) const override;
  int to_json_kv_string(char *buf, const int64_t buf_len, int64_t &pos) const override;
  void set_schema_id(int32_t schema_id)
  {
    schema_id_ = schema_id;
  }

private:
  field_ptr_fixed_array fields_;
  int32_t fields_count_;
  std::optional<int32_t> schema_id_;
};

// A data type representing a list of values.
class ListType final : public NestedType
{
public:
  explicit ListType(const SchemaField *element) : element_(element)
  {
  }
  ~ListType() override = default;

  TypeId type_id() const override
  {
    return TypeId::kList;
  }
  const SchemaField *element() const
  {
    return element_;
  }

  const SchemaField *get_field_by_id(int32_t field_id) const override;
  const SchemaField *get_field_by_index(int32_t index) const override;
  const SchemaField *get_field_by_name(const ObString &name) const override;
  int to_json_kv_string(char *buf, const int64_t buf_len, int64_t &pos) const override;

private:
  const SchemaField *element_;
};

// A data type representing a dictionary of values.
class MapType final : public NestedType
{
public:
  explicit MapType(const SchemaField *key, const SchemaField *value) : key_(key), value_(value)
  {
  }
  ~MapType() override = default;

  const SchemaField *key() const
  {
    return key_;
  }
  const SchemaField *value() const
  {
    return value_;
  }

  TypeId type_id() const override
  {
    return TypeId::kMap;
  }

  const SchemaField *get_field_by_id(int32_t field_id) const override;
  const SchemaField *get_field_by_index(int32_t index) const override;
  const SchemaField *get_field_by_name(const ObString &name) const override;
  int to_json_kv_string(char *buf, const int64_t buf_len, int64_t &pos) const override;

private:
  const SchemaField *key_;
  const SchemaField *value_;
};

// A data type representing an arbitrary-length byte sequence.
class BinaryType final : public PrimitiveType
{
public:
  BinaryType() = default;
  ~BinaryType() override = default;

  TypeId type_id() const override
  {
    return TypeId::kBinary;
  }

  static BinaryType *get_instance();
  int to_json_kv_string(char *buf, const int64_t buf_len, int64_t &pos) const override;
};

// A data type representing an arbitrary-length character sequence (encoded in UTF-8).
class StringType final : public PrimitiveType
{
public:
  StringType() = default;
  ~StringType() override = default;

  TypeId type_id() const override
  {
    return TypeId::kString;
  }

  static StringType *get_instance();
  int to_json_kv_string(char *buf, const int64_t buf_len, int64_t &pos) const override;
};

// A data type representing a boolean (true or false).
class BooleanType final : public PrimitiveType
{
public:
  BooleanType() = default;
  ~BooleanType() override = default;

  TypeId type_id() const override
  {
    return TypeId::kBoolean;
  }

  static BooleanType *get_instance();
  int to_json_kv_string(char *buf, const int64_t buf_len, int64_t &pos) const override;
};

// A data type representing a 32-bit signed integer.
class IntType final : public PrimitiveType
{
public:
  IntType() = default;
  ~IntType() override = default;

  TypeId type_id() const override
  {
    return TypeId::kInt;
  }

  static IntType *get_instance();
  int to_json_kv_string(char *buf, const int64_t buf_len, int64_t &pos) const override;
};

// A data type representing a 64-bit signed integer.
class LongType final : public PrimitiveType
{
public:
  LongType() = default;
  ~LongType() override = default;

  TypeId type_id() const override
  {
    return TypeId::kLong;
  }

  static LongType *get_instance();
  int to_json_kv_string(char *buf, const int64_t buf_len, int64_t &pos) const override;
};

} // namespace iceberg
} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_TYPE_H
