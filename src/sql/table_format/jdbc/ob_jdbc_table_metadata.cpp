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

#define USING_LOG_PREFIX SQL

#include "sql/table_format/jdbc/ob_jdbc_table_metadata.h"

#include "share/schema/ob_external_table_column_schema_helper.h"
#include "share/schema/ob_schema_utils.h"
#include "sql/engine/cmd/ob_load_data_parser.h"
#include "sql/parser/ob_char_type.h"
#include "sql/resolver/dml/ob_dml_resolver.h"

namespace oceanbase
{
namespace sql
{
namespace jdbc
{
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

bool ObJdbcTableMetadata::has_no_target_attributes(const ObJdbcTargetType &target_type)
{
  return target_type.length_ < 0
         && target_type.length_semantics_.empty()
         && target_type.precision_ < 0
         && target_type.scale_ < 0;
}

bool ObJdbcTableMetadata::is_mysql_boolean_target(const ObJdbcTargetType &target_type)
{
  return target_type.length_ < 0
         && target_type.length_semantics_.empty()
         && 1 == target_type.precision_
         && 0 == target_type.scale_;
}

ObCollationType ObJdbcTableMetadata::resolve_character_collation(
    const ObJdbcColumnMetadata &column,
    const ObCompatibilityMode target_mode,
    const ObJdbcCharsetContext &context)
{
  ObCollationType collation = context.default_collation_;
  if (ORACLE_MODE == target_mode) {
    if (column.target_type_.name_ == "NCHAR" || column.target_type_.name_ == "NVARCHAR2") {
      collation = context.nchar_collation_;
    }
  } else if (!column.source_charset_.empty() && !column.source_collation_.empty()) {
    const ObCharsetType source_charset = ObCharset::charset_type(column.source_charset_);
    const ObCollationType source_collation = ObCharset::collation_type(column.source_collation_);
    if (ObCharset::is_valid_charset(source_charset)
        && ObCharset::is_valid_collation(source_charset, source_collation)) {
      collation = source_collation;
    }
  }
  return collation;
}

int ObJdbcTableMetadata::validate_length_target(const ObJdbcTargetType &target_type,
                                               const ObString &expected_semantics)
{
  int ret = OB_SUCCESS;
  if (target_type.length_ <= 0
      || target_type.length_semantics_ != expected_semantics
      || target_type.precision_ >= 0
      || target_type.scale_ >= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid jdbc length target attributes", K(ret), K(target_type));
  }
  return ret;
}

int ObJdbcTableMetadata::validate_character_length(const ObJdbcTargetType &target_type,
                                                  const bool is_oracle_mode,
                                                  const ObCollationType collation)
{
  int ret = OB_SUCCESS;
  int64_t mbmaxlen = 0;
  const bool fixed_length = target_type.name_ == "CHAR";
  if (OB_FAIL(validate_length_target(target_type, ObString("CHAR")))) {
  } else if (OB_FAIL(ObCharset::get_mbmaxlen_by_coll(collation, mbmaxlen))) {
    LOG_WARN("failed to get jdbc character width", K(ret), K(collation));
  } else if (mbmaxlen <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid jdbc character width", K(ret), K(target_type), K(collation), K(mbmaxlen));
  } else {
    const int64_t max_length = is_oracle_mode
        ? (fixed_length ? OB_MAX_ORACLE_CHAR_LENGTH_BYTE : OB_MAX_ORACLE_VARCHAR_LENGTH)
        : (fixed_length ? OB_MAX_CHAR_LENGTH : OB_MAX_MYSQL_VARCHAR_LENGTH / mbmaxlen);
    if (target_type.length_ > max_length) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("jdbc character target is too long", K(ret), K(target_type), K(collation), K(max_length));
    }
  }
  return ret;
}

int ObJdbcTableMetadata::validate_decimal_target(const ObJdbcTargetType &target_type)
{
  int ret = OB_SUCCESS;
  if (target_type.length_ >= 0
      || !target_type.length_semantics_.empty()
      || target_type.precision_ <= 0
      || target_type.precision_ > OB_MAX_DECIMAL_PRECISION
      || target_type.scale_ < 0
      || target_type.scale_ > target_type.precision_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid jdbc decimal target attributes", K(ret), K(target_type));
  }
  return ret;
}

int ObJdbcTableMetadata::validate_temporal_target(const ObJdbcTargetType &target_type,
                                                 const int64_t max_scale)
{
  int ret = OB_SUCCESS;
  if (target_type.length_ >= 0
      || !target_type.length_semantics_.empty()
      || target_type.precision_ >= 0
      || target_type.scale_ < 0
      || target_type.scale_ > max_scale) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid jdbc temporal target attributes", K(ret), K(target_type));
  }
  return ret;
}

int ObJdbcTableMetadata::validate_interval_ds_target(const ObJdbcTargetType &target_type)
{
  int ret = OB_SUCCESS;
  if (target_type.length_ >= 0
      || !target_type.length_semantics_.empty()
      || target_type.precision_ < 0
      || target_type.precision_ > MAX_SCALE_FOR_ORACLE_TEMPORAL
      || target_type.scale_ < 0
      || target_type.scale_ > MAX_SCALE_FOR_ORACLE_TEMPORAL) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid jdbc interval day to second target attributes",
             K(ret), K(target_type));
  }
  return ret;
}

void ObJdbcTableMetadata::override_temporal_scale(const int64_t scale,
                                                 ObColumnSchemaV2 &column_schema)
{
  const int16_t old_precision = column_schema.get_data_precision();
  const int16_t old_scale = column_schema.get_data_scale();
  if (old_precision >= 0 && old_scale >= 0) {
    column_schema.set_data_precision(static_cast<int16_t>(
        old_precision - old_scale + scale));
  }
  column_schema.set_data_scale(static_cast<int16_t>(scale));
}

int ObJdbcTableMetadata::setup_oracle_national_character(const ObJdbcTargetType &target_type,
                                                       const bool fixed_length,
                                                       const ObCollationType collation,
                                                       ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  const ObCharsetType charset = ObCharset::charset_type_by_coll(collation);
  int64_t mbminlen = 0;
  if (OB_FAIL(validate_length_target(target_type, ObString("CHAR")))) {
  } else if (OB_FAIL(ObCharset::get_mbminlen_by_coll(collation, mbminlen))) {
    LOG_WARN("failed to get jdbc national character width", K(ret), K(collation));
  } else if (mbminlen <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid jdbc national character width", K(ret), K(target_type), K(collation), K(mbminlen));
  } else if ((fixed_length && target_type.length_ > OB_MAX_ORACLE_CHAR_LENGTH_BYTE / mbminlen)
             || (!fixed_length && target_type.length_ > OB_MAX_ORACLE_VARCHAR_LENGTH)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("jdbc national character target is too long",
             K(ret), K(target_type), K(fixed_length), K(collation), K(mbminlen));
  } else {
    const ObObjType data_type = fixed_length ? ObNCharType : ObNVarchar2Type;
    column_schema.set_data_type(data_type);
    column_schema.set_data_length(target_type.length_);
    column_schema.set_charset_type(charset);
    column_schema.set_collation_type(collation);
    ObExternalTableColumnSchemaHelper::set_column_accuracy(
        ORACLE_MODE, data_type, column_schema);
    column_schema.set_length_semantics(LS_CHAR);
  }
  return ret;
}

int ObJdbcTableMetadata::setup_lob(const ObJdbcTargetType &target_type,
                                  const bool is_oracle_mode,
                                  const ObCollationType text_collation,
                                  ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  const bool binary = target_type.name_ == "BLOB" || target_type.name_ == "LONGBLOB";
  const ObCollationType collation = binary ? CS_TYPE_BINARY : text_collation;
  const ObCharsetType charset = ObCharset::charset_type_by_coll(collation);
  if (target_type.length_ >= 0 || !target_type.length_semantics_.empty()
      || target_type.precision_ >= 0 || target_type.scale_ >= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid jdbc LOB target attributes", K(ret), K(target_type));
  } else {
    column_schema.set_data_type(ObLongTextType);
    column_schema.set_data_length(OB_MAX_LONGTEXT_LENGTH);
    column_schema.set_charset_type(charset);
    column_schema.set_collation_type(collation);
    ObExternalTableColumnSchemaHelper::set_column_accuracy(
        is_oracle_mode ? ORACLE_MODE : MYSQL_MODE, ObLongTextType, column_schema);
    column_schema.set_length_semantics(LS_BYTE);
  }
  return ret;
}

int ObJdbcTableMetadata::setup_oracle_raw(const ObJdbcTargetType &target_type,
                                         ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  const ObCharsetType charset = CHARSET_BINARY;
  const ObCollationType collation = CS_TYPE_BINARY;
  if (OB_FAIL(validate_length_target(target_type, ObString("BYTE")))) {
  } else if (target_type.length_ > OB_MAX_ORACLE_RAW_SQL_COL_LENGTH) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("jdbc RAW target is too long", K(ret), K(target_type), K(OB_MAX_ORACLE_RAW_SQL_COL_LENGTH));
  } else {
    column_schema.set_data_type(ObRawType);
    column_schema.set_data_length(target_type.length_);
    column_schema.set_charset_type(charset);
    column_schema.set_collation_type(collation);
    ObExternalTableColumnSchemaHelper::set_column_accuracy(
        ORACLE_MODE, ObRawType, column_schema);
    column_schema.set_length_semantics(LS_BYTE);
  }
  return ret;
}

int ObJdbcTableMetadata::setup_mysql_binary(const ObJdbcTargetType &target_type,
                                           const bool fixed_length,
                                           ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  const ObCharsetType charset = CHARSET_BINARY;
  const ObCollationType collation = CS_TYPE_BINARY;
  if (OB_FAIL(validate_length_target(target_type, ObString("BYTE")))) {
  } else if ((fixed_length && target_type.length_ > OB_MAX_CHAR_LENGTH)
             || (!fixed_length
                 && target_type.length_ > OB_MAX_MYSQL_VARCHAR_LENGTH)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("jdbc binary target is too long", K(ret), K(target_type), K(fixed_length));
  } else if (fixed_length) {
    ret = ObExternalTableColumnSchemaHelper::setup_char(
        false, target_type.length_, charset, collation, column_schema);
  } else {
    ret = ObExternalTableColumnSchemaHelper::setup_varchar(
        false, target_type.length_, charset, collation, column_schema);
  }
  if (OB_SUCC(ret)) {
    column_schema.set_length_semantics(LS_BYTE);
  }
  return ret;
}

ObLakeTableFormat ObJdbcTableMetadata::get_format_type() const
{
  return ObLakeTableFormat::JDBC;
}

int64_t ObJdbcTableMetadata::get_convert_size() const
{
  return sizeof(ObJdbcTableMetadata) + table_schema_.get_convert_size();
}

int ObJdbcTableMetadata::assign(const ObJdbcTableMetadata &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    if (OB_FAIL(ObILakeTableMetadata::assign(other))) {
      LOG_WARN("failed to assign lake table metadata", K(ret));
    } else if (OB_FAIL(table_schema_.assign(other.table_schema_))) {
      LOG_WARN("failed to assign jdbc table schema", K(ret));
    }
  }
  return ret;
}

int ObJdbcTableMetadata::setup_table_schema(
    const ObString &plugin_name,
    const ObString &scan_parameters,
    const ObJdbcTableMetadataDesc &metadata,
    const ObJdbcCharsetContext &charset_context)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(MYSQL_MODE != metadata.target_mode_
                  && ORACLE_MODE != metadata.target_mode_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid jdbc target mode", K(ret), K(metadata.target_mode_));
  }
  table_schema_.set_table_type(EXTERNAL_TABLE);
  if (OB_SUCC(ret)
      && (!ObCharset::is_valid_collation(charset_context.default_collation_)
          || (ORACLE_MODE == metadata.target_mode_
              && !ObCharset::is_valid_collation(charset_context.nchar_collation_)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid jdbc charset context", K(ret), K(charset_context.default_collation_),
             K(charset_context.nchar_collation_));
  }
  table_schema_.set_collation_type(charset_context.default_collation_);
  table_schema_.set_charset_type(ObCharset::charset_type_by_coll(charset_context.default_collation_));
  table_schema_.set_tenant_id(tenant_id_);
  table_schema_.set_database_id(database_id_);
  table_schema_.set_table_id(table_id_);
  table_schema_.set_catalog_id(catalog_id_);
  table_schema_.set_lake_table_format(ObLakeTableFormat::JDBC);
  table_schema_.set_external_table_auto_refresh(0);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(table_schema_.set_table_name(table_name_))) {
    LOG_WARN("failed to set jdbc table name", K(ret), K(table_name_));
  } else if (OB_FAIL(setup_external_format(plugin_name, scan_parameters))) {
    LOG_WARN("failed to setup jdbc external format", K(ret));
  } else if (OB_FAIL(table_schema_.set_external_file_location("jdbc://"))) {
    LOG_WARN("failed to set jdbc external location", K(ret));
  } else if (OB_FAIL(setup_columns(metadata.columns_, metadata.target_mode_, charset_context))) {
    LOG_WARN("failed to setup jdbc columns", K(ret));
  }
  return ret;
}

int ObJdbcTableMetadata::do_build_table_schema(ObTableSchema *&table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSchemaUtils::alloc_schema(
          allocator_, table_schema_, table_schema))) {
    LOG_WARN("failed to allocate jdbc table schema", K(ret));
  }
  return ret;
}

int ObJdbcTableMetadata::setup_external_format(
    const ObString &plugin_name,
    const ObString &plugin_parameters)
{
  int ret = OB_SUCCESS;
  ObExternalFileFormat format;
  ObString format_str;
  format.format_type_ = ObExternalFileFormat::PLUGIN_FORMAT;
  if (OB_UNLIKELY(plugin_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("jdbc plugin name is empty", K(ret));
  } else if (OB_FAIL(format.plugin_format_.init(allocator_))) {
    LOG_WARN("failed to init jdbc plugin format", K(ret));
  } else if (OB_FAIL(format.plugin_format_.set_type_name(plugin_name))) {
    LOG_WARN("failed to set jdbc plugin name", K(ret), K(plugin_name));
  } else if (OB_FAIL(format.plugin_format_.set_parameters(plugin_parameters))) {
    LOG_WARN("failed to set jdbc plugin parameters", K(ret));
  } else if (OB_FAIL(format.to_string_with_alloc(format_str, allocator_))) {
    LOG_WARN("failed to serialize jdbc plugin format", K(ret));
  } else if (OB_FAIL(table_schema_.set_external_properties(format_str))) {
    LOG_WARN("failed to set jdbc external properties", K(ret));
  }
  return ret;
}

int ObJdbcTableMetadata::setup_columns(
    const ObIArray<ObJdbcColumnMetadata> &columns,
    const ObCompatibilityMode target_mode,
    const ObJdbcCharsetContext &charset_context)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(columns.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("jdbc table metadata has no columns", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
    ObColumnSchemaV2 column_schema;
    const uint64_t column_id = OB_END_RESERVED_COLUMN_ID_NUM + i;
    if (OB_FAIL(setup_one_column(
            columns.at(i), column_id, target_mode, charset_context, column_schema))) {
      LOG_WARN("failed to setup jdbc column", K(ret), K(i), K(columns.at(i)));
    } else if (OB_FAIL(table_schema_.add_column(column_schema))) {
      LOG_WARN("failed to add jdbc column", K(ret), K(column_schema));
    }
  }
  return ret;
}

bool ObJdbcTableMetadata::is_oracle_unquoted_identifier(const ObString &name)
{
  bool is_valid = false;
  if (!name.empty()) {
    const unsigned char first = static_cast<unsigned char>(name.ptr()[0]);
    is_valid = ORACLE_FIRST_IDENTIFIER_FLAGS[first];
  }
  for (int64_t i = 1; is_valid && i < name.length(); ++i) {
    is_valid = ORACLE_IDENTIFIER_FALGS[static_cast<unsigned char>(name.ptr()[i])];
  }
  return is_valid;
}

int ObJdbcTableMetadata::setup_one_column(
    const ObJdbcColumnMetadata &column_metadata,
    const uint64_t column_id,
    const ObCompatibilityMode target_mode,
    const ObJdbcCharsetContext &charset_context,
    ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  const bool is_oracle_mode = ORACLE_MODE == target_mode;
  const ObJdbcTargetType &target_type = column_metadata.target_type_;
  const ObCollationType collation = resolve_character_collation(column_metadata, target_mode, charset_context);
  const ObCharsetType charset = ObCharset::charset_type_by_coll(collation);
  ObExternalFileFormat format;
  ObString mock_gen_column_str;
  bool is_unsupported = false;

  column_schema.set_table_id(table_schema_.get_table_id());
  column_schema.set_column_id(column_id);
  column_schema.set_nullable(column_metadata.target_nullable_);
  if (OB_FAIL(column_schema.set_column_name(column_metadata.name_))) {
    LOG_WARN("failed to set jdbc column name", K(ret), K(column_metadata));
  } else if (target_type.name_ == "UNSUPPORTED") {
    if (!has_no_target_attributes(target_type)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid unsupported jdbc column", K(ret), K(column_metadata));
    } else {
      is_unsupported = true;
    }
  } else if (target_type.name_ == "TINYINT" && MYSQL_MODE == target_mode) {
    if (has_no_target_attributes(target_type)) {
      OZ(ObExternalTableColumnSchemaHelper::setup_tinyint(false, column_schema));
    } else if (is_mysql_boolean_target(target_type)) {
      OZ(ObExternalTableColumnSchemaHelper::setup_bool(false, column_schema));
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid jdbc TINYINT target attributes", K(ret), K(target_type));
    }
  } else if (target_type.name_ == "TINYINT_UNSIGNED"
             && MYSQL_MODE == target_mode
             && has_no_target_attributes(target_type)) {
    column_schema.set_data_type(ObUTinyIntType);
    ObExternalTableColumnSchemaHelper::set_column_accuracy(
        MYSQL_MODE, ObUTinyIntType, column_schema);
  } else if (target_type.name_ == "SMALLINT"
             && MYSQL_MODE == target_mode
             && has_no_target_attributes(target_type)) {
    OZ(ObExternalTableColumnSchemaHelper::setup_smallint(false, column_schema));
  } else if (target_type.name_ == "INT"
             && MYSQL_MODE == target_mode
             && has_no_target_attributes(target_type)) {
    OZ(ObExternalTableColumnSchemaHelper::setup_int(false, column_schema));
  } else if (target_type.name_ == "BIGINT"
             && MYSQL_MODE == target_mode
             && has_no_target_attributes(target_type)) {
    OZ(ObExternalTableColumnSchemaHelper::setup_bigint(false, column_schema));
  } else if (target_type.name_ == "FLOAT"
             && MYSQL_MODE == target_mode
             && has_no_target_attributes(target_type)) {
    OZ(ObExternalTableColumnSchemaHelper::setup_float(false, column_schema));
  } else if (target_type.name_ == "DOUBLE"
             && MYSQL_MODE == target_mode
             && has_no_target_attributes(target_type)) {
    OZ(ObExternalTableColumnSchemaHelper::setup_double(false, column_schema));
  } else if (target_type.name_ == "BINARY_FLOAT"
             && ORACLE_MODE == target_mode
             && has_no_target_attributes(target_type)) {
    OZ(ObExternalTableColumnSchemaHelper::setup_float(true, column_schema));
  } else if (target_type.name_ == "BINARY_DOUBLE"
             && ORACLE_MODE == target_mode
             && has_no_target_attributes(target_type)) {
    OZ(ObExternalTableColumnSchemaHelper::setup_double(true, column_schema));
  } else if ((target_type.name_ == "DECIMAL" && MYSQL_MODE == target_mode)
             || (target_type.name_ == "NUMBER" && ORACLE_MODE == target_mode)) {
    if (OB_FAIL(validate_decimal_target(target_type))) {
    } else {
      OZ(ObExternalTableColumnSchemaHelper::setup_decimal(
          is_oracle_mode,
          static_cast<int16_t>(target_type.precision_),
          static_cast<int16_t>(target_type.scale_),
          column_schema));
    }
  } else if ((target_type.name_ == "CHAR" && MYSQL_MODE == target_mode)
             || (target_type.name_ == "CHAR" && ORACLE_MODE == target_mode)) {
    if (OB_FAIL(validate_character_length(target_type, is_oracle_mode, collation))) {
      is_unsupported = OB_NOT_SUPPORTED == ret;
    } else {
      OZ(ObExternalTableColumnSchemaHelper::setup_char(
          is_oracle_mode,
          target_type.length_,
          charset,
          collation,
          column_schema));
      if (OB_SUCC(ret)) {
        column_schema.set_length_semantics(LS_CHAR);
      }
    }
  } else if ((target_type.name_ == "VARCHAR" && MYSQL_MODE == target_mode)
             || (target_type.name_ == "VARCHAR2" && ORACLE_MODE == target_mode)) {
    if (OB_FAIL(validate_character_length(target_type, is_oracle_mode, collation))) {
      is_unsupported = OB_NOT_SUPPORTED == ret;
    } else {
      OZ(ObExternalTableColumnSchemaHelper::setup_varchar(
          is_oracle_mode,
          target_type.length_,
          charset,
          collation,
          column_schema));
      if (OB_SUCC(ret)) {
        column_schema.set_length_semantics(LS_CHAR);
      }
    }
  } else if (target_type.name_ == "NCHAR" && ORACLE_MODE == target_mode) {
    if (OB_FAIL(setup_oracle_national_character(target_type, true, collation, column_schema))) {
      is_unsupported = OB_NOT_SUPPORTED == ret;
    }
  } else if (target_type.name_ == "NVARCHAR2" && ORACLE_MODE == target_mode) {
    if (OB_FAIL(setup_oracle_national_character(target_type, false, collation, column_schema))) {
      is_unsupported = OB_NOT_SUPPORTED == ret;
    }
  } else if (target_type.name_ == "BINARY" && MYSQL_MODE == target_mode) {
    if (OB_FAIL(setup_mysql_binary(target_type, true, column_schema))) {
      is_unsupported = OB_NOT_SUPPORTED == ret;
    }
  } else if (target_type.name_ == "VARBINARY" && MYSQL_MODE == target_mode) {
    if (OB_FAIL(setup_mysql_binary(target_type, false, column_schema))) {
      is_unsupported = OB_NOT_SUPPORTED == ret;
    }
  } else if (target_type.name_ == "RAW" && ORACLE_MODE == target_mode) {
    if (OB_FAIL(setup_oracle_raw(target_type, column_schema))) {
      is_unsupported = OB_NOT_SUPPORTED == ret;
    }
  } else if (((target_type.name_ == "LONGTEXT" || target_type.name_ == "LONGBLOB")
              && MYSQL_MODE == target_mode)
             || ((target_type.name_ == "CLOB" || target_type.name_ == "BLOB")
                 && ORACLE_MODE == target_mode)) {
    OZ(setup_lob(target_type, is_oracle_mode, collation, column_schema));
  } else if (target_type.name_ == "DATE"
             && has_no_target_attributes(target_type)) {
    OZ(ObExternalTableColumnSchemaHelper::setup_date(
        is_oracle_mode, column_schema));
  } else if (target_type.name_ == "TIME" && MYSQL_MODE == target_mode) {
    if (OB_FAIL(validate_temporal_target(target_type, 6))) {
    } else {
      OZ(ObExternalTableColumnSchemaHelper::setup_time(false, column_schema));
      if (OB_SUCC(ret)) {
        override_temporal_scale(target_type.scale_, column_schema);
      }
    }
  } else if (target_type.name_ == "DATETIME" && MYSQL_MODE == target_mode) {
    if (OB_FAIL(validate_temporal_target(target_type, 6))) {
    } else {
      OZ(ObExternalTableColumnSchemaHelper::setup_datetime(false, column_schema));
      if (OB_SUCC(ret)) {
        override_temporal_scale(target_type.scale_, column_schema);
      }
    }
  } else if (target_type.name_ == "TIMESTAMP" && ORACLE_MODE == target_mode) {
    if (OB_FAIL(validate_temporal_target(target_type, 9))) {
    } else {
      OZ(ObExternalTableColumnSchemaHelper::setup_datetime(true, column_schema));
      if (OB_SUCC(ret)) {
        override_temporal_scale(target_type.scale_, column_schema);
      }
    }
  } else if (target_type.name_ == "TIMESTAMP_WITH_TIME_ZONE" && ORACLE_MODE == target_mode) {
    if (OB_FAIL(validate_temporal_target(target_type, MAX_SCALE_FOR_ORACLE_TEMPORAL))) {
    } else {
      column_schema.set_data_type(ObTimestampTZType);
      ObExternalTableColumnSchemaHelper::set_column_accuracy(
          ORACLE_MODE, ObTimestampTZType, column_schema);
      override_temporal_scale(target_type.scale_, column_schema);
    }
  } else if (target_type.name_ == "INTERVAL_DAY_TO_SECOND"
             && ORACLE_MODE == target_mode) {
    if (OB_FAIL(validate_interval_ds_target(target_type))) {
    } else {
      OZ(ObExternalTableColumnSchemaHelper::setup_interval_ds(
          static_cast<int16_t>(target_type.precision_),
          static_cast<int16_t>(target_type.scale_),
          column_schema));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED,
                   "JDBC target column type for tenant compatibility mode");
    LOG_WARN("unsupported jdbc target column type",
             K(ret), K(target_mode), K(target_type), K(column_metadata.name_));
  }

  // Only recognized capacity limits become a column-level unsupported state.
  if (OB_NOT_SUPPORTED == ret && is_unsupported) {
    ret = OB_SUCCESS;
  }
  if (OB_FAIL(ret)) {
  } else if (is_unsupported) {
    // ObNullType identifies unsupported columns in JDBC mock schemas.
    // JDBC scans reject them before initializing external column conversions.
    column_schema.set_data_type(ObNullType);
    column_schema.set_accuracy(ObAccuracy());
    column_schema.set_charset_type(CHARSET_BINARY);
    column_schema.set_collation_type(CS_TYPE_BINARY);
    column_schema.set_nullable(column_metadata.source_nullable_);
  } else {
    const ObCharsetType mapped_charset = column_schema.get_charset_type();
    const ObCollationType mapped_collation = column_schema.get_collation_type();
    format.format_type_ = ObExternalFileFormat::PLUGIN_FORMAT;
    if (OB_FAIL(format.mock_gen_column_def(
            column_schema, allocator_, mock_gen_column_str))) {
      LOG_WARN("failed to mock jdbc generated column definition", K(ret));
    } else if (OB_FAIL(ObDMLResolver::set_basic_column_properties(
                   column_schema, mock_gen_column_str))) {
      LOG_WARN("failed to set jdbc column properties", K(ret));
    } else {
      column_schema.set_nullable(column_metadata.target_nullable_);
      if (ob_is_string_or_lob_type(column_schema.get_data_type())
          || ob_is_raw(column_schema.get_data_type())) {
        column_schema.set_charset_type(mapped_charset);
        column_schema.set_collation_type(mapped_collation);
      }
    }
  }
  if (OB_SUCC(ret) && is_oracle_mode) {
    // Fold names that are valid Oracle unquoted identifiers; preserve others for quoted access.
    if (is_oracle_unquoted_identifier(column_metadata.name_)
        && OB_FAIL(ObDMLResolver::set_upper_column_name(column_schema))) {
      LOG_WARN("failed to uppercase jdbc column name", K(ret));
    }
  }
  return ret;
}

} // namespace jdbc
} // namespace sql
} // namespace oceanbase
