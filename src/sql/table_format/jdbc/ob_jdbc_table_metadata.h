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

#ifndef OCEANBASE_SQL_TABLE_FORMAT_JDBC_OB_JDBC_TABLE_METADATA_H_
#define OCEANBASE_SQL_TABLE_FORMAT_JDBC_OB_JDBC_TABLE_METADATA_H_

#include "share/catalog/jdbc/ob_jdbc_catalog_types.h"
#include "share/catalog/ob_external_catalog.h"

namespace oceanbase
{
namespace sql
{
namespace jdbc
{

struct ObJdbcCharsetContext
{
  common::ObCollationType default_collation_ = common::CS_TYPE_INVALID;
  common::ObCollationType nchar_collation_ = common::CS_TYPE_INVALID;
};

class ObJdbcTableMetadata final : public share::ObILakeTableMetadata
{
public:
  explicit ObJdbcTableMetadata(common::ObIAllocator &allocator)
      : share::ObILakeTableMetadata(allocator), table_schema_(&allocator)
  {
  }
  virtual ~ObJdbcTableMetadata() = default;

  share::ObLakeTableFormat get_format_type() const override;
  int64_t get_convert_size() const override;
  int assign(const ObJdbcTableMetadata &other);

  int setup_table_schema(const common::ObString &plugin_name,
                         const common::ObString &scan_parameters,
                         const share::ObJdbcTableMetadataDesc &metadata,
                         const ObJdbcCharsetContext &charset_context);

protected:
  int do_build_table_schema(share::schema::ObTableSchema *&table_schema) override;

private:
  int setup_external_format(const common::ObString &plugin_name,
                            const common::ObString &plugin_parameters);
  int setup_columns(const common::ObIArray<share::ObJdbcColumnMetadata> &columns,
                    const common::ObCompatibilityMode target_mode,
                    const ObJdbcCharsetContext &charset_context);
  static bool is_oracle_unquoted_identifier(const common::ObString &name);
  int setup_one_column(const share::ObJdbcColumnMetadata &column_metadata,
                       const uint64_t column_id,
                       const common::ObCompatibilityMode target_mode,
                       const ObJdbcCharsetContext &charset_context,
                       share::schema::ObColumnSchemaV2 &column_schema);
  static bool has_no_target_attributes(const share::ObJdbcTargetType &target_type);
  static bool is_mysql_boolean_target(const share::ObJdbcTargetType &target_type);
  static common::ObCollationType resolve_character_collation(
      const share::ObJdbcColumnMetadata &column,
      const common::ObCompatibilityMode target_mode,
      const ObJdbcCharsetContext &context);
  static int validate_length_target(const share::ObJdbcTargetType &target_type,
                                    const common::ObString &expected_semantics);
  static int validate_character_length(const share::ObJdbcTargetType &target_type,
                                       const bool is_oracle_mode,
                                       const common::ObCollationType collation);
  static int validate_decimal_target(const share::ObJdbcTargetType &target_type);
  static int validate_temporal_target(const share::ObJdbcTargetType &target_type,
                                      const int64_t max_scale);
  static int validate_interval_ds_target(const share::ObJdbcTargetType &target_type);
  static void override_temporal_scale(const int64_t scale,
                                      share::schema::ObColumnSchemaV2 &column_schema);
  static int setup_oracle_national_character(const share::ObJdbcTargetType &target_type,
                                             const bool fixed_length,
                                             const common::ObCollationType collation,
                                             share::schema::ObColumnSchemaV2 &column_schema);
  static int setup_lob(const share::ObJdbcTargetType &target_type,
                       const bool is_oracle_mode,
                       const common::ObCollationType text_collation,
                       share::schema::ObColumnSchemaV2 &column_schema);
  static int setup_oracle_raw(const share::ObJdbcTargetType &target_type,
                              share::schema::ObColumnSchemaV2 &column_schema);
  static int setup_mysql_binary(const share::ObJdbcTargetType &target_type,
                                const bool fixed_length,
                                share::schema::ObColumnSchemaV2 &column_schema);

private:
  share::schema::ObTableSchema table_schema_;
  DISALLOW_COPY_AND_ASSIGN(ObJdbcTableMetadata);
};

} // namespace jdbc
} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_TABLE_FORMAT_JDBC_OB_JDBC_TABLE_METADATA_H_
