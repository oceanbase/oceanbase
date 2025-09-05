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

#ifndef OCEANBASE_SCHEMA_EXTERNAL_TABLE_COLUMN_SCHEMA_HELPER_H_
#define OCEANBASE_SCHEMA_EXTERNAL_TABLE_COLUMN_SCHEMA_HELPER_H_
#include "ob_column_schema.h"

namespace oceanbase
{
namespace share
{
namespace schema
{

class ObExternalTableColumnSchemaHelper
{
public:
  static int setup_bool(const bool &is_oracle_mode, ObColumnSchemaV2 &column_schema);
  static int setup_tinyint(const bool &is_oracle_mode, ObColumnSchemaV2 &column_schema);
  static int setup_smallint(const bool &is_oracle_mode, ObColumnSchemaV2 &column_schema);
  static int setup_int(const bool &is_oracle_mode, ObColumnSchemaV2 &column_schema);
  static int setup_uint(const bool &is_oracle_mode, ObColumnSchemaV2 &column_schema);
  static int setup_bigint(const bool &is_oracle_mode, ObColumnSchemaV2 &column_schema);
  static int setup_ubigint(const bool &is_oracle_mode, ObColumnSchemaV2 &column_schema);
  static int setup_float(const bool &is_oracle_mode, ObColumnSchemaV2 &column_schema);
  static int setup_double(const bool &is_oracle_mode, ObColumnSchemaV2 &column_schema);
  static int setup_decimal(const bool &is_oracle_mode,
                           const int16_t &precision,
                           const int16_t &scale,
                           ObColumnSchemaV2 &column_schema);
  static int setup_varchar(const bool &is_oracle_mode,
                           const int64_t &length,
                           ObCharsetType cs_type,
                           ObCollationType collation,
                           ObColumnSchemaV2 &column_schema);
  static int setup_char(const bool &is_oracle_mode,
                        const int64_t &length,
                        ObCharsetType cs_type,
                        ObCollationType collation,
                        ObColumnSchemaV2 &column_schema);
  static int setup_string(const bool &is_oracle_mode, ObColumnSchemaV2 &column_schema);
  static int setup_timestamp(const bool &is_oracle_mode,
                             ObColumnSchemaV2 &column_schema);
  static int setup_datetime(const bool &is_oracle_mode, ObColumnSchemaV2 &column_schema);
  static int setup_date(const bool &is_oracle_mode,
                        ObColumnSchemaV2 &column_schema);
  static int setup_time(const bool &is_oracle_mode, ObColumnSchemaV2 &column_schema);
  static int setup_timestamp_ns(const bool &is_oracle_mode, ObColumnSchemaV2 &column_schema);
  static int setup_timestamp_tz_ns(const bool &is_oracle_mode, ObColumnSchemaV2 &column_schema);
};

} // namespace schema
} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SCHEMA_EXTERNAL_TABLE_COLUMN_SCHEMA_HELPER_H_