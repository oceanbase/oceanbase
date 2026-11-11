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

#define USING_LOG_PREFIX SHARE

#include "share/catalog/jdbc/ob_jdbc_catalog_types.h"

#include <climits>

#include "lib/allocator/page_arena.h"
#include "lib/json_type/ob_json_base.h"
#include "lib/json_type/ob_json_parse.h"
#include "lib/json_type/ob_json_tree.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace share
{
using namespace oceanbase::common;

int ObJdbcTableMetadataParser::get_member(const ObIJsonBase &object,
                                          const char *name,
                                          const ObIJsonBase *&value,
                                          const bool required)
{
  int ret = OB_SUCCESS;
  int64_t matches = 0;
  value = nullptr;
  if (OB_UNLIKELY(ObJsonNodeType::J_OBJECT != object.json_type())
      || OB_ISNULL(name)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    const ObString member_name(name);
    for (uint64_t i = 0; OB_SUCC(ret) && i < object.element_count(); ++i) {
      ObString key;
      ObIJsonBase *member_value = nullptr;
      if (OB_FAIL(object.get_key(i, key))) {
        LOG_WARN("failed to get jdbc metadata member name", K(ret), K(i));
      } else if (OB_FAIL(object.get_object_value(i, member_value))) {
        LOG_WARN("failed to get jdbc metadata member value", K(ret), K(i));
      } else if (OB_ISNULL(member_value)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("jdbc metadata member value is null", K(ret), K(i));
      } else if (key == member_name) {
        ++matches;
        value = member_value;
      }
    }
    if (OB_SUCC(ret) && (matches > 1 || (required && 0 == matches))) {
      ret = OB_INVALID_ARGUMENT;
    }
  }
  return ret;
}

int ObJdbcTableMetadataParser::get_json_string(const ObIJsonBase &value, ObString &string_value)
{
  int ret = OB_SUCCESS;
  string_value.reset();
  if (OB_UNLIKELY(ObJsonNodeType::J_STRING != value.json_type()
                  || value.get_data_length() > INT32_MAX
                  || (value.get_data_length() > 0
                      && OB_ISNULL(value.get_data())))) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    string_value.assign_ptr(value.get_data(),
                            static_cast<int32_t>(value.get_data_length()));
  }
  return ret;
}

int ObJdbcTableMetadataParser::copy_string(const ObIJsonBase &object,
                                           const char *name,
                                           ObIAllocator &allocator,
                                           ObString &destination,
                                           const bool required)
{
  int ret = OB_SUCCESS;
  const ObIJsonBase *value = nullptr;
  ObString string_value;
  destination.reset();
  if (OB_FAIL(get_member(object, name, value, required))) {
    LOG_WARN("failed to find jdbc metadata string member", K(ret), K(name));
  } else if (OB_ISNULL(value)) {
    // Optional member was omitted.
  } else if (ObJsonNodeType::J_NULL == value->json_type() && !required) {
    // Optional member was explicitly null.
  } else if (OB_FAIL(get_json_string(*value, string_value))
             || (required && string_value.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid jdbc metadata string member", K(ret), K(name),
             "type", value->json_type());
  } else if (OB_FAIL(ob_write_string(allocator,
                                     string_value,
                                     destination,
                                     true))) {
    LOG_WARN("failed to copy jdbc metadata string member", K(ret), K(name));
  }
  return ret;
}

int ObJdbcTableMetadataParser::get_number(const ObIJsonBase &object,
                                          const char *name,
                                          int64_t &destination,
                                          const bool required,
                                          const int64_t null_value)
{
  int ret = OB_SUCCESS;
  const ObIJsonBase *value = nullptr;
  destination = null_value;
  if (OB_FAIL(get_member(object, name, value, required))) {
    LOG_WARN("failed to find jdbc metadata number member", K(ret), K(name));
  } else if (OB_ISNULL(value)) {
    // Optional member was omitted.
  } else if (ObJsonNodeType::J_NULL == value->json_type() && !required) {
    // Optional member was explicitly null.
  } else if (ObJsonNodeType::J_INT == value->json_type()) {
    destination = value->get_int();
  } else if (ObJsonNodeType::J_UINT == value->json_type()
             && value->get_uint() <= static_cast<uint64_t>(INT64_MAX)) {
    destination = static_cast<int64_t>(value->get_uint());
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid jdbc metadata number member", K(ret), K(name),
             "type", value->json_type());
  }
  return ret;
}

int ObJdbcTableMetadataParser::get_bool(const ObIJsonBase &object,
                                        const char *name,
                                        int8_t &destination,
                                        const bool required)
{
  int ret = OB_SUCCESS;
  const ObIJsonBase *value = nullptr;
  destination = -1;
  if (OB_FAIL(get_member(object, name, value, required))) {
    LOG_WARN("failed to find jdbc metadata boolean member", K(ret), K(name));
  } else if (OB_ISNULL(value)) {
    // Optional member was omitted.
  } else if (ObJsonNodeType::J_NULL == value->json_type() && !required) {
    // Optional member was explicitly null.
  } else if (ObJsonNodeType::J_BOOLEAN != value->json_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid jdbc metadata boolean member", K(ret), K(name),
             "type", value->json_type());
  } else {
    destination = value->get_boolean() ? 1 : 0;
  }
  return ret;
}

int ObJdbcTableMetadataParser::get_required_object(const ObIJsonBase &object,
                                                   const char *name,
                                                   const ObIJsonBase *&value)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_member(object, name, value, true))) {
    LOG_WARN("failed to find jdbc metadata object member", K(ret), K(name));
  } else if (OB_ISNULL(value)
             || ObJsonNodeType::J_OBJECT != value->json_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid jdbc metadata object member", K(ret), K(name));
  }
  return ret;
}

bool ObJdbcTableMetadataParser::is_one_of(const ObString &value,
                                          const char *const candidates[],
                                          const int64_t candidate_count)
{
  bool found = false;
  for (int64_t i = 0; !found && i < candidate_count; ++i) {
    found = (value == candidates[i]);
  }
  return found;
}

bool ObJdbcTableMetadataParser::is_known_target_name(const ObString &name)
{
  static const char *const NAMES[] = {"TINYINT",
                                      "TINYINT_UNSIGNED",
                                      "SMALLINT",
                                      "INT",
                                      "BIGINT",
                                      "FLOAT",
                                      "DOUBLE",
                                      "DECIMAL",
                                      "NUMBER",
                                      "BINARY_FLOAT",
                                      "BINARY_DOUBLE",
                                      "CHAR",
                                      "VARCHAR",
                                      "VARCHAR2",
                                      "NCHAR",
                                      "NVARCHAR2",
                                      "BINARY",
                                      "VARBINARY",
                                      "RAW",
                                      "DATE",
                                      "TIME",
                                      "DATETIME",
                                      "TIMESTAMP",
                                      "INTERVAL_DAY_TO_SECOND",
                                      "LONGTEXT",
                                      "LONGBLOB",
                                      "CLOB",
                                      "BLOB",
                                      "TIMESTAMP_WITH_TIME_ZONE",
                                      "UNSUPPORTED"};
  return is_one_of(name, NAMES, ARRAYSIZEOF(NAMES));
}

int ObJdbcTableMetadataParser::parse_target_type(const ObIJsonBase &value,
                                                 ObIAllocator &allocator,
                                                 ObJdbcTargetType &target_type)
{
  int ret = OB_SUCCESS;
  bool is_interval_ds = false;
  if (OB_UNLIKELY(ObJsonNodeType::J_OBJECT != value.json_type())) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(copy_string(value, "name", allocator, target_type.name_, true))) {
  } else if (!is_known_target_name(target_type.name_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported jdbc target type name", K(ret), K(target_type.name_));
  } else if (OB_FAIL(get_number(value, "length", target_type.length_, false))) {
  } else if (OB_FAIL(copy_string(value,
                                 "lengthSemantics",
                                 allocator,
                                 target_type.length_semantics_,
                                 false))) {
  } else if (OB_FAIL(get_number(value, "precision", target_type.precision_, false))) {
  } else if (OB_FAIL(get_number(value, "scale", target_type.scale_, false))) {
  } else if (FALSE_IT(is_interval_ds =
                         target_type.name_ == "INTERVAL_DAY_TO_SECOND")) {
  } else if ((target_type.length_ > 0) != !target_type.length_semantics_.empty()
             || target_type.length_ < -1
             || 0 == target_type.length_
             || (!target_type.length_semantics_.empty()
                 && target_type.length_semantics_ != "BYTE"
                 && target_type.length_semantics_ != "CHAR")
             || target_type.precision_ < -1
             || target_type.scale_ < -1
             || (!is_interval_ds
                 && target_type.scale_ > target_type.precision_
                 && target_type.precision_ >= 0)
             || (is_interval_ds
                 && (target_type.length_ >= 0
                     || !target_type.length_semantics_.empty()
                     || target_type.precision_ < 0
                     || target_type.precision_ > MAX_SCALE_FOR_ORACLE_TEMPORAL
                     || target_type.scale_ < 0
                     || target_type.scale_ > MAX_SCALE_FOR_ORACLE_TEMPORAL))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid jdbc target type attributes", K(ret), K(target_type));
  }
  return ret;
}

int ObJdbcTableMetadataParser::parse_column(const ObIJsonBase &value,
                                            ObIAllocator &allocator,
                                            ObJdbcColumnMetadata &column)
{
  int ret = OB_SUCCESS;
  int64_t ordinal_position = 0;
  int8_t source_nullable = -1;
  int8_t target_nullable = -1;
  const ObIJsonBase *target_type = nullptr;
  if (OB_UNLIKELY(ObJsonNodeType::J_OBJECT != value.json_type())) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(copy_string(value, "name", allocator, column.name_, true))) {
  } else if (OB_FAIL(get_number(value,
                                "ordinalPosition",
                                ordinal_position,
                                true,
                                0))) {
  } else if (ordinal_position <= 0 || ordinal_position > INT32_MAX) {
    ret = OB_INVALID_ARGUMENT;
  } else if (FALSE_IT(column.ordinal_position_ =
                          static_cast<int32_t>(ordinal_position))) {
  } else if (OB_FAIL(get_bool(value, "sourceNullable", source_nullable, true))) {
  } else if (OB_FAIL(get_bool(value, "targetNullable", target_nullable, true))) {
  } else if (source_nullable < 0 || target_nullable < 0
             || (1 == source_nullable && 0 == target_nullable)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (FALSE_IT(column.source_nullable_ = (1 == source_nullable))) {
  } else if (FALSE_IT(column.target_nullable_ = (1 == target_nullable))) {
  } else if (OB_FAIL(copy_string(value, "sourceCharset", allocator, column.source_charset_, false))) {
  } else if (OB_FAIL(copy_string(value, "sourceCollation", allocator, column.source_collation_, false))) {
  } else if (OB_FAIL(get_required_object(value, "targetType", target_type))) {
  } else if (OB_FAIL(parse_target_type(*target_type, allocator, column.target_type_))) {
  } else if (column.target_type_.name_ == "UNSUPPORTED") {
    if (column.target_type_.length_ >= 0
        || !column.target_type_.length_semantics_.empty()
        || column.target_type_.precision_ >= 0 || column.target_type_.scale_ >= 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("unsupported jdbc column has target attributes", K(ret), K(column));
    }
  }
  return ret;
}

int ObJdbcTableMetadataParser::parse_columns(const ObIJsonBase &root,
                                             ObIAllocator &allocator,
                                             ObJdbcTableMetadataDesc &metadata)
{
  int ret = OB_SUCCESS;
  const ObIJsonBase *columns = nullptr;
  int32_t previous_ordinal = 0;
  if (OB_FAIL(get_member(root, "columns", columns, true))) {
  } else if (OB_ISNULL(columns)
             || ObJsonNodeType::J_ARRAY != columns->json_type()
             || 0 == columns->element_count()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    for (uint64_t i = 0; OB_SUCC(ret) && i < columns->element_count(); ++i) {
      ObIJsonBase *value = nullptr;
      ObJdbcColumnMetadata column;
      if (OB_FAIL(columns->get_array_element(i, value))) {
        LOG_WARN("failed to get jdbc column metadata", K(ret), K(i));
      } else if (OB_ISNULL(value)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(parse_column(*value, allocator, column))) {
        LOG_WARN("failed to parse jdbc column metadata", K(ret), K(i));
      } else if (ORACLE_MODE == metadata.target_mode_ && !column.target_nullable_
                 && (column.target_type_.name_ == "CHAR"
                     || column.target_type_.name_ == "VARCHAR2"
                     || column.target_type_.name_ == "NCHAR"
                     || column.target_type_.name_ == "NVARCHAR2"
                     || column.target_type_.name_ == "RAW")) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("jdbc target must allow NULL introduced by Oracle conversion", K(ret), K(i));
      } else if (column.ordinal_position_ <= previous_ordinal) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("jdbc column ordinals are not strictly increasing",
                 K(ret), K(previous_ordinal), K(column.ordinal_position_));
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < metadata.columns_.count(); ++j) {
        if (metadata.columns_.at(j).name_ == column.name_) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("duplicate jdbc column name", K(ret), K(column.name_));
        }
      }
      if (OB_SUCC(ret)) {
        previous_ordinal = column.ordinal_position_;
        if (OB_FAIL(metadata.columns_.push_back(column))) {
          LOG_WARN("failed to append jdbc column", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObJdbcTableMetadataDesc::build_scan_parameters(ObIAllocator &allocator,
                                                   ObString &parameters,
                                                   uint64_t catalog_id,
                                                   int64_t catalog_schema_version) const
{
  int ret = OB_SUCCESS;
  ObArenaAllocator json_allocator(ObMemAttr(MTL_ID(), "JdbcMeta"));
  ObJsonObject object(&json_allocator);
  ObJsonBuffer buffer(&json_allocator);
  ObJsonString table(qualified_table_name_);
  ObJsonString target_mode(ORACLE_MODE == target_mode_ ? "ORACLE" : "MYSQL");
  ObJsonUint catalog(catalog_id);
  ObJsonInt version(catalog_schema_version);
  parameters.reset();
  if (OB_UNLIKELY(qualified_table_name_.empty()
                  || catalog_id == OB_INVALID_ID || catalog_schema_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("jdbc scan table or catalog identity is invalid", K(ret));
  } else if (OB_FAIL(object.add("table", &table))) {
    LOG_WARN("failed to set jdbc scan table", K(ret));
  } else if (OB_FAIL(object.add("target_mode", &target_mode))) {
    LOG_WARN("failed to set jdbc scan target mode", K(ret));
  } else if (OB_FAIL(object.add("catalog_id", &catalog))) {
  } else if (OB_FAIL(object.add("catalog_schema_version", &version))) {
  } else if (OB_FAIL(object.print(buffer, true))) {
    LOG_WARN("failed to serialize jdbc scan parameters", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator, buffer.string(), parameters))) {
    LOG_WARN("failed to copy jdbc scan parameters", K(ret));
  }
  return ret;
}

int ObJdbcTableMetadataParser::parse_scan_parameters(const ObString &json_text,
                                                     uint64_t &catalog_id,
                                                     int64_t &catalog_schema_version)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator json_allocator(ObMemAttr(MTL_ID(), "JdbcMeta"));
  ObIJsonBase *root = nullptr;
  const ObIJsonBase *id = nullptr;
  const uint32_t flags = ObJsonParser::JSN_STRICT_FLAG | ObJsonParser::JSN_UNIQUE_FLAG;
  if (json_text.empty() || json_text.length() > MAX_METADATA_JSON_SIZE) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid jdbc scan parameters length", K(ret), K(json_text.length()));
  } else if (OB_FAIL(ObJsonBaseFactory::get_json_base(&json_allocator, json_text,
                 ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, root, flags))) {
    LOG_WARN("failed to parse jdbc scan parameters", K(ret));
  } else if (OB_ISNULL(root)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("jdbc scan parameters root is null", K(ret));
  } else {
    if (OB_FAIL(get_member(*root, "catalog_id", id, true))) {
      LOG_WARN("failed to get jdbc scan catalog id", K(ret));
    } else if (OB_ISNULL(id)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("jdbc scan catalog id is null", K(ret));
    } else if (id->json_type() == ObJsonNodeType::J_UINT) {
      catalog_id = id->get_uint();
    } else if (id->json_type() == ObJsonNodeType::J_INT && id->get_int() > 0) {
      catalog_id = static_cast<uint64_t>(id->get_int());
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid jdbc scan catalog id", K(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(get_number(*root, "catalog_schema_version", catalog_schema_version, true))) {
      LOG_WARN("failed to get jdbc scan catalog schema version", K(ret));
    } else if (catalog_id == 0 || catalog_id == OB_INVALID_ID || catalog_schema_version <= 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid jdbc scan catalog identity", K(ret), K(catalog_id), K(catalog_schema_version));
    }
  }
  return ret;
}

int ObJdbcTableMetadataParser::parse(const ObString &json_text,
                                     ObIAllocator &allocator,
                                     ObJdbcTableMetadataDesc &metadata)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator json_allocator(ObMemAttr(MTL_ID(), "JdbcMeta"));
  ObIJsonBase *root = nullptr;
  int64_t contract_version = 0;
  ObString target_mode;
  const uint32_t parse_flags = ObJsonParser::JSN_STRICT_FLAG
                               | ObJsonParser::JSN_UNIQUE_FLAG;
  metadata.contract_version_ = 0;
  metadata.target_mode_ = OCEANBASE_MODE;
  metadata.qualified_table_name_.reset();
  metadata.columns_.reset();
  if (OB_UNLIKELY(json_text.empty()
                  || json_text.length() > MAX_METADATA_JSON_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid jdbc table metadata json length", K(ret), K(json_text.length()));
  } else if (OB_FAIL(ObJsonBaseFactory::get_json_base(
                         &json_allocator,
                         json_text,
                         ObJsonInType::JSON_TREE,
                         ObJsonInType::JSON_TREE,
                         root,
                         parse_flags))) {
    LOG_WARN("failed to parse jdbc table metadata json", K(ret), K(json_text.length()));
  } else if (OB_ISNULL(root)
             || ObJsonNodeType::J_OBJECT != root->json_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("jdbc table metadata root is not an object", K(ret));
  } else if (OB_FAIL(get_number(*root,
                                "contractVersion",
                                contract_version,
                                true,
                                0))) {
  } else if (ObJdbcTableMetadataDesc::CONTRACT_VERSION != contract_version) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported jdbc table metadata contract version",
             K(ret), K(contract_version));
  } else if (FALSE_IT(metadata.contract_version_ = contract_version)) {
  } else if (OB_FAIL(copy_string(*root,
                                 "targetMode",
                                 allocator,
                                 target_mode,
                                 true))) {
  } else if (target_mode == "MYSQL") {
    metadata.target_mode_ = MYSQL_MODE;
  } else if (target_mode == "ORACLE") {
    metadata.target_mode_ = ORACLE_MODE;
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported jdbc target compatibility mode", K(ret), K(target_mode));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(copy_string(*root, "qualifiedTableName", allocator, metadata.qualified_table_name_, true))) {
  } else if (OB_FAIL(parse_columns(*root, allocator, metadata))) {
  }
  return ret;
}

} // namespace share
} // namespace oceanbase
