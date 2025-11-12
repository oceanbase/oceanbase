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

#ifndef SCHEMA_H
#define SCHEMA_H

#include "lib/container/ob_array.h"
#include "lib/hash/ob_hashset.h"
#include "lib/json/ob_json.h"
#include "share/schema/ob_column_schema.h"
#include "sql/table_format/iceberg/ob_iceberg_type_fwd.h"
#include "sql/table_format/iceberg/spec/spec.h"

namespace oceanbase
{

namespace sql
{

namespace iceberg
{

namespace schema = share::schema;

class Schema : public SpecWithAllocator
{
public:
  explicit Schema(ObIAllocator &allocator);
  int assign(const Schema &other);
  int init_from_json(const ObJsonObject &json_object);
  int get_column_schema_by_field_id(int32_t field_id,
                                    const schema::ObColumnSchemaV2 *&column_schema) const;

  static int set_column_schema_type_by_type_str(const ObString type_str,
                                                schema::ObColumnSchemaV2 &column_schema);
  static int set_column_schema_complex_type(const ObJsonNode &json_node,
                                            schema::ObColumnSchemaV2 &column_schema);
  static int parse_complex_type(ObIAllocator &allocator,
                                const ObJsonNode &json_node,
                                ObString &return_type);
  static int parse_array(ObIAllocator &allocator,
                         const ObJsonObject &json_object,
                         ObString &return_type);
  static int parse_primitive_type_in_complex_type(ObIAllocator &allocator,
                                                  const ObString &type_str,
                                                  ObString &return_type);
  int32_t schema_id;
  ObFixedArray<int32_t, ObIAllocator> identifier_field_ids;
  ObFixedArray<schema::ObColumnSchemaV2 *, ObIAllocator> fields;

  static constexpr const char *SCHEMA_ID = "schema-id";
  static constexpr const char *IDENTIFIER_FIELD_IDS = "identifier-field-ids";
  static constexpr const char *ID = "id";
  static constexpr const char *NAME = "name";
  static constexpr const char *REQUIRED = "REQUIRED";
  static constexpr const char *TYPE = "type";
  static constexpr const char *DOC = "doc";
  static constexpr const char *FIELDS = "fields";
  static constexpr const char *TYPE_BOOLEAN = "boolean";
  static constexpr const char *TYPE_INT = "int";
  static constexpr const char *TYPE_LONG = "long";
  static constexpr const char *TYPE_FLOAT = "float";
  static constexpr const char *TYPE_DOUBLE = "double";
  static constexpr const char *TYPE_DECIMAL = "decimal";
  static constexpr const char *TYPE_DATE = "date";
  static constexpr const char *TYPE_TIME = "time";
  static constexpr const char *TYPE_TIMESTAMP = "timestamp";
  static constexpr const char *TYPE_TIMESTAMP_TZ = "timestamptz";
  static constexpr const char *TYPE_TIMESTAMP_NS = "timestamp_ns";
  static constexpr const char *TYPE_TIMESTAMP_TZ_NS = "timestamptz_ns";
  static constexpr const char *TYPE_STRING = "string";
  static constexpr const char *TYPE_UUID = "uuid";
  static constexpr const char *TYPE_FIXED = "fixed";
  static constexpr const char *TYPE_BINARY = "binary";
  static constexpr const char *TYPE_STRUCT = "struct";
  static constexpr const char *TYPE_LIST = "list";
  static constexpr const char *TYPE_LIST_ELEMENT = "element";
  static constexpr const char *TYPE_MAP = "map";
  // TODO more types need to handle
private:
  int parse_fields_(const ObJsonArray &json_array, ObIArray<schema::ObColumnSchemaV2 *> &fields);
  int parse_field_(const ObJsonObject &json_object, schema::ObColumnSchemaV2 &field);
  int set_column_properties_(schema::ObColumnSchemaV2 &column_schema);
};
} // namespace iceberg

} // namespace sql

} // namespace oceanbase

#endif // SCHEMA_H
