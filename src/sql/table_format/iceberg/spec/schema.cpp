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

#include "sql/table_format/iceberg/spec/schema.h"

#include "sql/resolver/dml/ob_dml_resolver.h"
#include "sql/table_format/iceberg/ob_iceberg_utils.h"

#include <regex>

namespace oceanbase
{
namespace sql
{
namespace iceberg
{

Schema::Schema(ObIAllocator &allocator)
    : SpecWithAllocator(allocator),
      identifier_field_ids(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator)),
      fields(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator))
{
}

int Schema::assign(const Schema &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    schema_id = other.schema_id;
    if (OB_FAIL(identifier_field_ids.assign(other.identifier_field_ids))) {
      LOG_WARN("failed to assign identifier-field-ids", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < other.fields.count(); i++) {
        const schema::ObColumnSchemaV2 *other_column_schema = other.fields.at(i);
        schema::ObColumnSchemaV2 *copied_column_schema = NULL;
        if (OB_ISNULL(copied_column_schema = OB_NEWx(schema::ObColumnSchemaV2, &allocator_, &allocator_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory for schema", K(ret));
        } else if (OB_FAIL(copied_column_schema->assign(*other_column_schema))) {
          LOG_WARN("failed to assign schema", K(ret));
        } else {
          OZ(fields.push_back(copied_column_schema));
        }
      }
    }
  }
  return ret;
}

int Schema::init_from_json(const ObJsonObject &json_object)
{
  int ret = OB_SUCCESS;

  if (OB_SUCC(ret)) {
    std::optional<int32_t> tmp_schema_id;
    if (OB_FAIL(ObCatalogJsonUtils::get_primitive(json_object, SCHEMA_ID, tmp_schema_id))) {
      LOG_WARN("failed to get schema-id", K(ret));
    } else if (tmp_schema_id.has_value()) {
      schema_id = tmp_schema_id.value();
    } else {
      schema_id = DEFAULT_SCHEMA_ID; // for v1 version
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObCatalogJsonUtils::get_primitive_array(json_object,
                                                        IDENTIFIER_FIELD_IDS,
                                                        identifier_field_ids))) {
      LOG_WARN("failed to get identifier-field-ids", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    const ObJsonNode *fields_json = json_object.get_value(FIELDS);
    if (OB_ISNULL(fields_json)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("schema must have fields", K(ret));
    } else if (ObJsonNodeType::J_ARRAY != fields_json->json_type()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("illegal schema fields", K(ret));
    } else if (OB_FAIL(parse_fields_(*down_cast<const ObJsonArray *>(fields_json), fields))) {
      LOG_WARN("failed to parse fields", K(ret));
    }
  }
  return ret;
}

int Schema::get_column_schema_by_field_id(int32_t field_id,
                                          const schema::ObColumnSchemaV2 *&column_schema) const
{
  int ret = OB_SUCCESS;
  column_schema = NULL;
  for (int32_t i = 0; OB_SUCC(ret) && NULL == column_schema && i < fields.size(); i++) {
    const schema::ObColumnSchemaV2 *tmp_column_schema = fields.at(i);
    if (tmp_column_schema->get_column_id() == ObIcebergUtils::get_ob_column_id(field_id)) {
      column_schema = tmp_column_schema;
    }
  }

  if (OB_SUCC(ret) && NULL == column_schema) {
    ret = OB_NOT_EXIST_COLUMN_ID;
    LOG_WARN("failed to get column schema by id", K(ret), K(field_id));
  }
  return ret;
}

int Schema::parse_fields_(const ObJsonArray &json_array,
                          ObArray<schema::ObColumnSchemaV2 *> &fields)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < json_array.element_count(); i++) {
    ObIJsonBase *json_array_element = NULL;
    schema::ObColumnSchemaV2 *field = NULL;
    if (OB_FAIL(json_array.get_array_element(i, json_array_element))) {
      LOG_WARN("failed to get json array element", K(ret));
    } else if (ObJsonNodeType::J_OBJECT != json_array_element->json_type()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid json array element", K(ret));
    } else if (OB_ISNULL(field = OB_NEWx(schema::ObColumnSchemaV2, &allocator_, &allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret));
    } else if (OB_FAIL(parse_field_(*down_cast<ObJsonObject *>(json_array_element), *field))) {
      LOG_WARN("failed to get field", K(ret));
    } else {
      OZ(fields.push_back(field));
    }
  }
  return ret;
}

int Schema::parse_field_(const ObJsonObject &json_object, schema::ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator;

  if (OB_SUCC(ret)) {
    int32_t id = 0;
    if (OB_FAIL(ObCatalogJsonUtils::get_primitive(json_object, ID, id))) {
      LOG_WARN("failed to get field id", K(ret));
    } else {
      column_schema.set_column_id(ObIcebergUtils::get_ob_column_id(id));
    }
  }

  if (OB_SUCC(ret)) {
    ObString name;
    if (OB_FAIL(ObCatalogJsonUtils::get_string(tmp_allocator, json_object, NAME, name))) {
      LOG_WARN("failed to get field name", K(ret));
    } else {
      column_schema.set_column_name(name);
    }
  }

  if (OB_SUCC(ret)) {
    std::optional<ObString> doc;
    if (OB_FAIL(ObCatalogJsonUtils::get_string(tmp_allocator, json_object, DOC, doc))) {
      LOG_WARN("failed to get field doc", K(ret));
    } else if (doc.has_value()) {
      column_schema.set_comment(doc.value());
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(set_column_properties_(column_schema))) {
      LOG_WARN("failed to set column properties", K(ret));
    }
  }

  // TODO: required/initial-default/write-default not handle yet
  // TODO: complex type not supported
  if (OB_SUCC(ret)) {
    ObJsonNode *json_node = json_object.get_value(TYPE);
    if (OB_ISNULL(json_node)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("failed to get type", K(ret));
    } else if (ObJsonNodeType::J_STRING == json_node->json_type()) {
      // primitive type
      ObString type_str = ObString(json_node->get_data_length(), json_node->get_data());
      if (OB_FAIL(Schema::set_column_schema_type_by_type_str(type_str, column_schema))) {
        LOG_WARN("failed to set column schema type", K(ret));
      }
    } else if (ObJsonNodeType::J_OBJECT == json_node->json_type()) {
      // complex type
      if (OB_FAIL(
              Schema::set_column_schema_complex_type(*down_cast<const ObJsonObject *>(json_node),
                                                     column_schema))) {
        LOG_WARN("failed to set column schema", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid json type", K(ret), K(json_node->json_type()));
    }
  }
  return ret;
}

int Schema::set_column_schema_type_by_type_str(const ObString type_str,
                                               schema::ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  if (0 == type_str.case_compare(TYPE_BOOLEAN)) {
    if (lib::is_oracle_mode()) {
      column_schema.set_data_type(ObDecimalIntType);
      column_schema.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY2[ORACLE_MODE][ObTinyIntType]);
    } else {
      column_schema.set_data_type(ObTinyIntType);
    }
  } else if (0 == type_str.case_compare(TYPE_INT)) {
    if (lib::is_oracle_mode()) {
      column_schema.set_data_type(ObDecimalIntType);
      column_schema.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY2[ORACLE_MODE][ObInt32Type]);
    } else {
      column_schema.set_data_type(ObInt32Type);
    }
  } else if (0 == type_str.case_compare(TYPE_LONG)) {
    if (lib::is_oracle_mode()) {
      column_schema.set_data_type(ObDecimalIntType);
      column_schema.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY2[ORACLE_MODE][ObIntType]);
    } else {
      column_schema.set_data_type(ObIntType);
    }
  } else if (0 == type_str.case_compare(TYPE_FLOAT)) {
    column_schema.set_data_type(ObFloatType);
  } else if (0 == type_str.case_compare(TYPE_DOUBLE)) {
    column_schema.set_data_type(ObDoubleType);
  } else if (type_str.prefix_match_ci(TYPE_DECIMAL)) {
    std::string tmp_type_str(type_str.ptr(), type_str.length());
    std::regex decimal_regex(R"(decimal\(\s*(\d+)\s*,\s*(\d+)\s*\))");
    std::smatch match;
    if (std::regex_match(tmp_type_str, match, decimal_regex)) {
      column_schema.set_data_type(ObDecimalIntType);
      column_schema.set_data_precision(std::stoi(match[1].str()));
      column_schema.set_data_scale(std::stoi(match[2].str()));
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid decimal type", K(ret), K(type_str));
    }
  } else if (0 == type_str.case_compare(TYPE_DATE)) {
    if (lib::is_oracle_mode()) {
      column_schema.set_data_type(ObDateTimeType);
    } else {
      column_schema.set_data_type(ObDateType);
    }
  } else if (0 == type_str.case_compare(TYPE_TIME)) {
    column_schema.set_data_type(ObTimeType);
  } else if (0 == type_str.case_compare(TYPE_TIMESTAMP)) {
    column_schema.set_data_type(lib::is_oracle_mode() ? ObTimestampNanoType : ObDateTimeType);
  } else if (0 == type_str.case_compare(TYPE_TIMESTAMP_TZ)) {
    column_schema.set_data_type(lib::is_oracle_mode() ? ObTimestampLTZType : ObTimestampType);
  } else if (0 == type_str.case_compare(TYPE_TIMESTAMP_NS)) {
    if (lib::is_oracle_mode()) {
      column_schema.set_data_type(ObTimestampNanoType);
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "timestamp_ns not supported yet");
    }
  } else if (0 == type_str.case_compare(TYPE_TIMESTAMP_TZ_NS)) {
    if (lib::is_oracle_mode()) {
      column_schema.set_data_type(ObTimestampLTZType);
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "timestamptz_ns not supported yet");
    }
  } else if (0 == type_str.case_compare(TYPE_STRING)) {
    int64_t varchar_len
        = lib::is_oracle_mode() ? OB_MAX_ORACLE_VARCHAR_LENGTH : OB_MAX_MYSQL_VARCHAR_LENGTH;
    column_schema.set_data_type(ObVarcharType);
    column_schema.set_data_length(varchar_len);
    column_schema.set_length_semantics(LS_CHAR);
    column_schema.set_charset_type(ObCharsetType::CHARSET_UTF8MB4);
    column_schema.set_collation_type(ObCollationType::CS_TYPE_UTF8MB4_BIN);
  } else if (0 == type_str.case_compare(TYPE_BINARY) || 0 == type_str.case_compare(TYPE_UUID)
             || type_str.prefix_match_ci(TYPE_FIXED)) {
    // UUID & FIXED used binary type directly
    int64_t varchar_len
        = lib::is_oracle_mode() ? OB_MAX_ORACLE_VARCHAR_LENGTH : OB_MAX_MYSQL_VARCHAR_LENGTH;
    column_schema.set_data_type(ObVarcharType);
    column_schema.set_data_length(varchar_len);
    column_schema.set_length_semantics(LS_CHAR);
    column_schema.set_charset_type(ObCharsetType::CHARSET_BINARY);
    column_schema.set_collation_type(ObCollationType::CS_TYPE_BINARY);
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported column type", K(type_str));
  }
  return ret;
}

int Schema::set_column_schema_complex_type(const ObJsonNode &json_node,
                                           schema::ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator;
  ObString result_type;
  if (OB_FAIL(parse_complex_type(tmp_allocator, json_node, result_type))) {
    LOG_WARN("failed to parse complex type", K(ret));
  } else {
    column_schema.set_data_type(ObCollectionSQLType);
    ObArray<ObString> complex_type;
    complex_type.push_back(result_type);
    column_schema.set_extended_type_info(complex_type);
  }
  return ret;
}

int Schema::parse_complex_type(ObIAllocator &allocator,
                               const ObJsonNode &json_node,
                               ObString &return_type)
{
  int ret = OB_SUCCESS;
  ObString type_str;
  if (ObJsonNodeType::J_STRING == json_node.json_type()) {
    ObString type_str = ObString(json_node.get_data_length(), json_node.get_data());
    if (OB_FAIL(Schema::parse_primitive_type_in_complex_type(allocator, type_str, return_type))) {
      LOG_WARN("failed to parse primitive type in complex type", K(ret));
    }
  } else if (ObJsonNodeType::J_OBJECT == json_node.json_type()) {
    const ObJsonObject &json_object = down_cast<const ObJsonObject &>(json_node);
    if (OB_FAIL(ObCatalogJsonUtils::get_string(allocator, json_object, TYPE, type_str))) {
      LOG_WARN("failed to get type str", K(ret));
    } else if (0 == type_str.case_compare(TYPE_STRUCT)) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "struct not supported yet");
    } else if (0 == type_str.case_compare(TYPE_LIST)) {
      if (OB_FAIL(parse_array(allocator, json_object, return_type))) {
        LOG_WARN("failed to parse array", K(ret));
      }
    } else if (0 == type_str.case_compare(TYPE_MAP)) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "map not supported yet");
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid type", K(ret), K(type_str));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid type", K(ret), K(json_node.json_type()));
  }
  return ret;
}

int Schema::parse_array(ObIAllocator &allocator,
                        const ObJsonObject &json_object,
                        ObString &return_type)
{
  int ret = OB_SUCCESS;
  // todo extract element_id
  const ObJsonNode *json_element = json_object.get_value(TYPE_LIST_ELEMENT);
  if (OB_ISNULL(json_element)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("array must have element", K(ret));
  } else {
    ObString element_type_str;
    ObSqlString sql_string;
    if (OB_FAIL(Schema::parse_complex_type(allocator, *json_element, element_type_str))) {
      LOG_WARN("failed to parse element type", K(ret));
    } else if (OB_FAIL(sql_string.append_fmt("ARRAY(%.*s)",
                                             element_type_str.length(),
                                             element_type_str.ptr()))) {
      LOG_WARN("failed to append element type", K(ret));
    } else if (OB_FAIL(ob_write_string(allocator, sql_string.string(), return_type))) {
      LOG_WARN("failed to deep copy string", K(ret));
    }
  }
  return ret;
}

int Schema::parse_primitive_type_in_complex_type(ObIAllocator &allocator,
                                                 const ObString &type_str,
                                                 ObString &return_type)
{
  int ret = OB_SUCCESS;
  ObColumnSchemaV2 column_schema;
  if (0 == type_str.case_compare(TYPE_BOOLEAN)) {
    return_type = "BOOLEAN";
  } else if (0 == type_str.case_compare(TYPE_INT)) {
    return_type = "INT";
  } else if (0 == type_str.case_compare(TYPE_LONG)) {
    return_type = "BIGINT";
  } else if (0 == type_str.case_compare(TYPE_FLOAT)) {
    return_type = "FLOAT";
  } else if (0 == type_str.case_compare(TYPE_DOUBLE)) {
    return_type = "DOUBLE";
  } else if (type_str.prefix_match_ci(TYPE_DECIMAL)) {
    std::string tmp_type_str(type_str.ptr(), type_str.length());
    std::regex decimal_regex(R"(decimal\(\s*(\d+)\s*,\s*(\d+)\s*\))");
    std::smatch match;
    if (std::regex_match(tmp_type_str, match, decimal_regex)) {
      ObSqlString sql_string;
      sql_string.append_fmt("DECIMAL(%d, %d)",std::stoi(match[1].str()), std::stoi(match[2].str()));
      OZ(ob_write_string(allocator, sql_string.string(), return_type));
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid decimal type", K(ret), K(type_str));
    }
  } else if (0 == type_str.case_compare(TYPE_DATE)) {
    return_type = "DATE";
  } else if (0 == type_str.case_compare(TYPE_TIME)) {
    return_type = "TIME";
  } else if (0 == type_str.case_compare(TYPE_TIMESTAMP)) {
    return_type = "DATETIME";;
  } else if (0 == type_str.case_compare(TYPE_TIMESTAMP_TZ)) {
    return_type = "TIMESTAMP";
  } else if (0 == type_str.case_compare(TYPE_TIMESTAMP_NS)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("mysql didn't support timestamp_ns", K(ret));
  } else if (0 == type_str.case_compare(TYPE_TIMESTAMP_TZ_NS)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("mysql didn't support timestamp_ns", K(ret));
  } else if (0 == type_str.case_compare(TYPE_STRING)) {
    return_type = "VARCHAR(65535)";
  } else if (0 == type_str.case_compare(TYPE_BINARY) || 0 == type_str.case_compare(TYPE_UUID)
             || type_str.prefix_match_ci(TYPE_FIXED)) {
    // UUID & FIXED used binary type directly
    return_type = "VARBINARY(65535)";
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported column type", K(type_str));
  }
  return ret;
}

int Schema::set_column_properties_(schema::ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator;
  sql::ObExternalFileFormat format;
  format.format_type_ = sql::ObExternalFileFormat::ICEBERG_FORMAT;
  ObString mock_gen_column_str;
  if (OB_FAIL(format.mock_gen_column_def(column_schema, tmp_allocator, mock_gen_column_str))) {
    LOG_WARN("fail to mock gen column def", K(ret));
  } else if (OB_FAIL(
                 ObDMLResolver::set_basic_column_properties(column_schema, mock_gen_column_str))) {
    LOG_WARN("fail to set properties for column", K(ret));
  } else {
    // reset properties from ObDMLResolver::set_basic_column_properties()
    column_schema.set_charset_type(ObCharsetType::CHARSET_UTF8MB4);
    column_schema.set_collation_type(ObCollationType::CS_TYPE_UTF8MB4_BIN);
  }
  return ret;
}

} // namespace iceberg

} // namespace sql
} // namespace oceanbase