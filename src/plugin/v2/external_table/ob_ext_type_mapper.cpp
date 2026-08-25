/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "plugin/v2/external_table/ob_ext_type_mapper.h"

#include "lib/ob_define.h"
#include "lib/oblog/ob_log_module.h"
#include "common/object/ob_obj_type.h"  // ObCollectionSQLType
#include "share/schema/ob_external_table_column_schema_helper.h"

#include <algorithm>

namespace oceanbase
{
namespace share
{

namespace
{
// Apply the contract column `precision` as the temporal fsp (sub-second
// digits) on top of the default accuracy the setup_* helper installed. Those
// helpers store accuracy as precision = display_width + fsp, so replacing the
// fsp adjusts both fields. Contract precisions above OB's max fsp
// (OB_MAX_DATETIME_PRECISION == 6, e.g. paimon's 9) are clamped — sub-microsecond
// digits are rounded away by the temporal conversion anyway.
void apply_temporal_fsp(const int32_t precision, schema::ObColumnSchemaV2 &column)
{
  int ret = OB_SUCCESS;  // referenced by LOG_WARN's errcode slot
  if (precision >= 0) {
    const int16_t fsp = static_cast<int16_t>(
        std::min<int32_t>(precision, OB_MAX_DATETIME_PRECISION));
    if (precision > OB_MAX_DATETIME_PRECISION) {
      LOG_WARN("ext schema temporal precision exceeds OB max fsp, clamped",
               K(precision), K(fsp));
    }
    ObAccuracy acc = column.get_accuracy();
    acc.set_precision(static_cast<int16_t>(acc.get_precision() - acc.get_scale() + fsp));
    acc.set_scale(fsp);
    column.set_accuracy(acc);
  }
}
} // namespace

int ext_type_apply_to_column(ob_ext_obj_type ext_type,
                             bool is_oracle_mode,
                             common::ObCharsetType cs_type,
                             common::ObCollationType collation,
                             int32_t precision,
                             int32_t scale,
                             int32_t length,
                             schema::ObColumnSchemaV2 &column)
{
  int ret = OB_SUCCESS;
  switch (ext_type) {
    case OB_EXT_TYPE_BOOL:
      ret = schema::ObExternalTableColumnSchemaHelper::setup_bool(is_oracle_mode, column);
      break;
    case OB_EXT_TYPE_TINYINT:
      ret = schema::ObExternalTableColumnSchemaHelper::setup_tinyint(is_oracle_mode, column);
      break;
    case OB_EXT_TYPE_SMALLINT:
      ret = schema::ObExternalTableColumnSchemaHelper::setup_smallint(is_oracle_mode, column);
      break;
    case OB_EXT_TYPE_INT:
      ret = schema::ObExternalTableColumnSchemaHelper::setup_int(is_oracle_mode, column);
      break;
    case OB_EXT_TYPE_BIGINT:
      ret = schema::ObExternalTableColumnSchemaHelper::setup_bigint(is_oracle_mode, column);
      break;
    case OB_EXT_TYPE_FLOAT:
      ret = schema::ObExternalTableColumnSchemaHelper::setup_float(is_oracle_mode, column);
      break;
    case OB_EXT_TYPE_DOUBLE:
      ret = schema::ObExternalTableColumnSchemaHelper::setup_double(is_oracle_mode, column);
      break;
    case OB_EXT_TYPE_DECIMAL:
      if ((precision < -1) || (scale < -1)
          || (precision > OB_MAX_DECIMAL_PRECISION) || (scale > OB_MAX_DECIMAL_SCALE)
          || (precision >= 0 && scale < 0) || (scale >= 0 && precision < 0)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid DECIMAL precision/scale in ext schema", K(ret), K(precision), K(scale));
      } else {
        ret = schema::ObExternalTableColumnSchemaHelper::setup_decimal(is_oracle_mode,
                                                                 static_cast<int16_t>(precision),
                                                                 static_cast<int16_t>(scale),
                                                                 column);
      }
      break;
    case OB_EXT_TYPE_STRING:
      ret = schema::ObExternalTableColumnSchemaHelper::setup_string(is_oracle_mode, cs_type, collation, column);
      break;
    case OB_EXT_TYPE_BINARY:
    case OB_EXT_TYPE_VARBINARY:
      ret = schema::ObExternalTableColumnSchemaHelper::setup_string(is_oracle_mode, ObCharsetType::CHARSET_BINARY, ObCollationType::CS_TYPE_BINARY, column);
      break;
    case OB_EXT_TYPE_VARCHAR:
      if (length < -1
          || length > (is_oracle_mode ? OB_MAX_ORACLE_VARCHAR_LENGTH : OB_MAX_MYSQL_VARCHAR_LENGTH)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid VARCHAR length in ext schema", K(ret), K(length));
      } else {
        ret = schema::ObExternalTableColumnSchemaHelper::setup_varchar(is_oracle_mode,
                                                                 static_cast<int64_t>(length),
                                                                 cs_type, collation, column);
      }
      break;
    case OB_EXT_TYPE_CHAR:
      if (length < -1
          || length > (is_oracle_mode ? OB_MAX_ORACLE_CHAR_LENGTH_BYTE : OB_MAX_CHAR_LENGTH)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid CHAR length in ext schema", K(ret), K(length));
      } else {
        ret = schema::ObExternalTableColumnSchemaHelper::setup_char(is_oracle_mode,
                                                              static_cast<int64_t>(length),
                                                              cs_type, collation, column);
      }
      break;
    case OB_EXT_TYPE_DATE:
      ret = schema::ObExternalTableColumnSchemaHelper::setup_date(is_oracle_mode, column);
      break;
    case OB_EXT_TYPE_DATETIME:
      ret = schema::ObExternalTableColumnSchemaHelper::setup_datetime(is_oracle_mode, column);
      if (OB_SUCC(ret)) {
        apply_temporal_fsp(precision, column);
      }
      break;
    case OB_EXT_TYPE_TIMESTAMP:
      ret = schema::ObExternalTableColumnSchemaHelper::setup_timestamp(is_oracle_mode, column);
      if (OB_SUCC(ret)) {
        apply_temporal_fsp(precision, column);
      }
      break;
    case OB_EXT_TYPE_TIME:
      ret = schema::ObExternalTableColumnSchemaHelper::setup_time(is_oracle_mode, column);
      break;
    case OB_EXT_TYPE_NULL:
    case OB_EXT_TYPE_ARRAY:
    case OB_EXT_TYPE_MAP:
    case OB_EXT_TYPE_UNKNOWN:
    default:
      // Not covered as primitive columns: ARRAY and MAP go through
      // ext_collection_apply_to_column instead (see ob_ext_schema_parser).
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported ext obj type in bridge", K(ret), K(ext_type));
      break;
  }
  return ret;
}

// OB array elements cannot be LOB strings/binaries, so the unbounded STRING /
// BINARY / VARBINARY protocol types are substituted with a bounded VARCHAR /
// VARBINARY (mirrors iceberg's parse_primitive_type_in_complex_type).
int ext_type_to_element_sql(ob_ext_obj_type ext_type,
                            int32_t precision, int32_t scale, int32_t length,
                            common::ObSqlString &out)
{
  int ret = OB_SUCCESS;
  switch (ext_type) {
    case OB_EXT_TYPE_BOOL:
    case OB_EXT_TYPE_TINYINT:  ret = out.append("TINYINT"); break;
    case OB_EXT_TYPE_SMALLINT: ret = out.append("SMALLINT"); break;
    case OB_EXT_TYPE_INT:      ret = out.append("INT"); break;
    case OB_EXT_TYPE_BIGINT:   ret = out.append("BIGINT"); break;
    case OB_EXT_TYPE_FLOAT:    ret = out.append("FLOAT"); break;
    case OB_EXT_TYPE_DOUBLE:   ret = out.append("DOUBLE"); break;
    case OB_EXT_TYPE_DECIMAL:
      if (precision < 0 || scale < 0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("DECIMAL array element requires precision and scale", K(ret), K(precision), K(scale));
      } else {
        ret = out.append_fmt("DECIMAL(%d, %d)", precision, scale);
      }
      break;
    case OB_EXT_TYPE_STRING:
      // OB array element cannot be LOB string -> substitute bounded VARCHAR.
      ret = out.append("VARCHAR(65535)");
      break;
    case OB_EXT_TYPE_VARCHAR:
      ret = out.append_fmt("VARCHAR(%d)", length > 0 ? length : 65535);
      break;
    case OB_EXT_TYPE_CHAR:
      ret = out.append_fmt("CHAR(%d)", length > 0 ? length : 256);
      break;
    case OB_EXT_TYPE_BINARY:
    case OB_EXT_TYPE_VARBINARY:
      // OB array element cannot be LOB binary -> substitute bounded VARBINARY.
      ret = out.append("VARBINARY(65535)");
      break;
    case OB_EXT_TYPE_DATE:     ret = out.append("DATE"); break;
    case OB_EXT_TYPE_DATETIME: ret = out.append("DATETIME"); break;
    case OB_EXT_TYPE_TIMESTAMP:ret = out.append("TIMESTAMP"); break;
    case OB_EXT_TYPE_TIME:     ret = out.append("TIME"); break;
    case OB_EXT_TYPE_ARRAY:
      // Handled by the schema parser's recursion (which wraps the element in
      // "ARRAY(...)"), not here.
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("ARRAY element must be handled by recursion, not ext_type_to_element_sql", K(ret));
      break;
    case OB_EXT_TYPE_MAP:
    case OB_EXT_TYPE_NULL:
    case OB_EXT_TYPE_UNKNOWN:
    default:
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported ext obj type as array element", K(ret), K(ext_type));
      break;
  }
  return ret;
}

int ext_collection_apply_to_column(const common::ObString &type_str,
                                   schema::ObColumnSchemaV2 &column)
{
  int ret = OB_SUCCESS;
  // OB represents a collection column as ObCollectionSQLType + an extended-type
  // info array holding the full type string ("ARRAY(INT)", ...). Same mechanism
  // iceberg uses (Schema::set_column_schema_complex_type).
  common::ObSEArray<common::ObString, 1> info;
  if (OB_FAIL(info.push_back(type_str))) {
    LOG_WARN("push_back collection type string failed", K(ret));
  } else {
    column.set_data_type(ObCollectionSQLType);
    // set_extended_type_info deep-copies into the column's allocator, so the
    // temp ObString / info array can go out of scope safely.
    if (OB_FAIL(column.set_extended_type_info(info))) {
      LOG_WARN("set_extended_type_info failed", K(ret), K(type_str));
    }
  }
  return ret;
}

} // namespace share
} // namespace oceanbase

#undef USING_LOG_PREFIX
