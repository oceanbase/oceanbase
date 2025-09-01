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

#include "ob_external_table_column_schema_helper.h"

namespace oceanbase
{
namespace share
{
namespace schema
{

int ObExternalTableColumnSchemaHelper::setup_tinyint(const bool &is_oracle_mode,
                                                     ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  if (is_oracle_mode) {
    column_schema.set_data_type(ObDecimalIntType);
    column_schema.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY2[ORACLE_MODE][ObTinyIntType]);
  } else {
    column_schema.set_data_type(ObTinyIntType);
    column_schema.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY2[MYSQL_MODE][ObTinyIntType]);
  }
  return ret;
}

int ObExternalTableColumnSchemaHelper::setup_smallint(const bool &is_oracle_mode,
                                                      ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  if (is_oracle_mode) {
    column_schema.set_data_type(ObDecimalIntType);
    column_schema.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY2[ORACLE_MODE][ObSmallIntType]);
  } else {
    column_schema.set_data_type(ObSmallIntType);
    column_schema.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY2[MYSQL_MODE][ObSmallIntType]);
  }
  return ret;
}

int ObExternalTableColumnSchemaHelper::setup_int(const bool &is_oracle_mode,
                                                 ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  if (is_oracle_mode) {
    column_schema.set_data_type(ObDecimalIntType);
    column_schema.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY2[ORACLE_MODE][ObInt32Type]);
  } else {
    column_schema.set_data_type(ObInt32Type);
    column_schema.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY2[MYSQL_MODE][ObInt32Type]);
  }
  return ret;
}

int ObExternalTableColumnSchemaHelper::setup_bigint(const bool &is_oracle_mode,
                                                    ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  if (is_oracle_mode) {
    column_schema.set_data_type(ObDecimalIntType);
    column_schema.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY2[ORACLE_MODE][ObIntType]);
  } else {
    column_schema.set_data_type(ObIntType);
    column_schema.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY2[MYSQL_MODE][ObIntType]);
  }
  return ret;
}

int ObExternalTableColumnSchemaHelper::setup_float(const bool &is_oracle_mode,
                                                   ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  column_schema.set_data_type(ObFloatType);
  column_schema.set_accuracy(
      ObAccuracy::DDL_DEFAULT_ACCURACY2[is_oracle_mode ? ORACLE_MODE : MYSQL_MODE][ObFloatType]);
  return ret;
}

int ObExternalTableColumnSchemaHelper::setup_double(const bool &is_oracle_mode,
                                                    ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  column_schema.set_data_type(ObDoubleType);
  column_schema.set_accuracy(
      ObAccuracy::DDL_DEFAULT_ACCURACY2[is_oracle_mode ? ORACLE_MODE : MYSQL_MODE][ObDoubleType]);
  return ret;
}

int ObExternalTableColumnSchemaHelper::setup_decimal(const bool &is_oracle_mode,
                                                     const int16_t &precision,
                                                     const int16_t &scale,
                                                     ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(precision > OB_MAX_DECIMAL_PRECISION)) {
    ret = OB_ERR_TOO_BIG_PRECISION;
    LOG_WARN("precision of number overflow", K(ret), K(scale), K(precision));
  } else if (OB_UNLIKELY(scale > OB_MAX_DECIMAL_SCALE)) {
    ret = OB_ERR_TOO_BIG_SCALE;
    LOG_WARN("scale of number overflow", K(ret), K(scale), K(precision));
  } else if (OB_UNLIKELY(precision < scale)) {
    ret = OB_ERR_M_BIGGER_THAN_D;
    LOG_WARN("precision less then scale", K(ret), K(scale), K(precision));
  } else {
    if (precision <= 0 && scale <= 0) {
      column_schema.set_accuracy(
          ObAccuracy::DDL_DEFAULT_ACCURACY2[is_oracle_mode ? ORACLE_MODE : MYSQL_MODE]
                                           [ObDecimalIntType]);
    } else {
      column_schema.set_data_type(ObDecimalIntType);
      column_schema.set_data_precision(precision);
      column_schema.set_data_scale(scale);
    }
  }
  return ret;
}

int ObExternalTableColumnSchemaHelper::setup_varchar(const bool &is_oracle_mode,
                                                     const int64_t &length,
                                                     ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  column_schema.set_data_type(ObVarcharType);
  if (length <= 0) {
    column_schema.set_data_length(is_oracle_mode ? OB_MAX_ORACLE_VARCHAR_LENGTH
                                                 : OB_MAX_MYSQL_VARCHAR_LENGTH);
  } else {
    column_schema.set_data_length(length);
  }
  column_schema.set_length_semantics(LS_CHAR);
  return ret;
}

int ObExternalTableColumnSchemaHelper::setup_char(const bool &is_oracle_mode,
                                                  const int64_t &length,
                                                  ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  column_schema.set_data_type(ObCharType);
  if (length <= 0) {
    column_schema.set_data_length(is_oracle_mode ? OB_MAX_ORACLE_CHAR_LENGTH_BYTE
                                                 : OB_MAX_CHAR_LENGTH);
  } else {
    column_schema.set_data_length(length);
  }
  column_schema.set_length_semantics(LS_CHAR);
  return ret;
}

int ObExternalTableColumnSchemaHelper::setup_timestamp(const bool &is_oracle_mode,
                                                        ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(is_oracle_mode)) {
    column_schema.set_data_type(ObTimestampLTZType);
    column_schema.set_accuracy(
        ObAccuracy::DDL_DEFAULT_ACCURACY2[ORACLE_MODE][ObTimestampLTZType]);
  } else {
    column_schema.set_data_type(ObTimestampType);
    column_schema.set_data_scale(6); // Ensure scale >= 6
    column_schema.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY2[MYSQL_MODE][ObTimestampType]);
  }
  return ret;
}

int ObExternalTableColumnSchemaHelper::setup_date(const bool &is_oracle_mode,
                                                  ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(is_oracle_mode)) {
    column_schema.set_data_type(ObDateTimeType);
    column_schema.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY2[ORACLE_MODE][ObDateTimeType]);
  } else {
    column_schema.set_data_type(ObDateType);
    column_schema.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY2[MYSQL_MODE][ObDateType]);
  }
  return ret;
}

} // namespace schema
} // namespace share
} // namespace oceanbase