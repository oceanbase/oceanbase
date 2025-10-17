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

#ifndef OB_SQL_TABLE_FORMAT_HIVE_H
#define OB_SQL_TABLE_FORMAT_HIVE_H

#include "lib/allocator/ob_allocator.h"
#include "lib/string/ob_string.h"
#include "share/catalog/hive/thrift/gen_cpp/hive_metastore_types.h"
#include "share/catalog/ob_external_catalog.h"
#include "share/ob_define.h"
#include "sql/engine/cmd/ob_load_data_parser.h"

namespace oceanbase
{
namespace sql
{
namespace hive
{
namespace ApacheHive = Apache::Hadoop::Hive;

using FieldSchemas = std::vector<ApacheHive::FieldSchema>;
using String = std::string;
using Strings = std::vector<String>;
using PartitionValuesRows = std::vector<ApacheHive::PartitionValuesRow>;

// Hive related types.
const char *const TINYINT = "tinyint";
const char *const SMALLINT = "smallint";
const char *const INT = "int";
const char *const BIGINT = "bigint";
const char *const BOOLEAN = "boolean";
const char *const FLOAT = "float";
const char *const DOUBLE = "double";
// Note: decimal will be filled such as decimal(10, 2).
const char *const DECIMAL = "decimal";
const char *const STRING = "string";
// Note: varchar will be filled such as varchar(50).
const char *const VARCHAR = "varchar";
// Note: char will be filled such as char(5).
const char *const CHAR = "char";
const char *const BINARY = "binary";
const char *const TIMESTAMP = "timestamp";
const char *const DATE = "date";
// Complicated type will delay to support.
const char *const ARRAY = "array";
const char *const MAP = "map";
const char *const STRUCT = "struct";
const char *const UNIONTYPE = "uniontype";

// Hive default delimiters
// Ctrl + A
const char *const DEFAULT_FIELD_DELIMITER = "\x01";
// Ctrl + B
const char *const DEFAULT_COLLECTION_ITEMS_DELIMETER = "\x02";
// Ctrl + C
const char *const DEFAULT_MAP_KEYS_DELIMITER = "\x03";
const char *const DEFAULT_LINE_DELIMITER = "\n";

// Hive csv output default delimiters.
const char *const DEFAULT_SEPARTOR_CHAR = ",";
const char *const DEFAULT_QUATA_CHAR = "\"";
const char *const DEFAULT_ESCAPE_CHAR = "\"";

// parquet format related.
const char *const PARQUET_COMPRESSION = "parquet.compression";
const char *const ROW_GROUP_SIZE = "parquet.block.size";
// orc format related.
const char *const ORC_COMPRESSION = "orc.compress";
const char *const ORC_COMPRESSION_BLOCK_SIZE = "orc.compress.size";
const char *const ORC_STRIPE_SIZE = "orc.stripe.size";
const char *const ORC_ROW_INDEX_STRIDE = "orc.row.index.stride";
const char *const ORC_BLOOM_FILTER_COLUMNS = "orc.bloom.filter.columns";

// Hive paramters keys
const char *const MAPKEY_DELIM = "mapkey.delim";

// Here is for compatibility with Hive 2.x version.
// There is a typo in Hive 2.x version, and fixed in Hive 3.x version.
// https://issues.apache.org/jira/browse/HIVE-16922
const char *const HIVE_2_COLLECTION_DELIM = "colelction.delim";
const char *const COLLECTION_DELIM = "collection.delim";
const char *const SERIALIZATION_FORMAT = "serialization.format";
const char *const LINE_DELIM = "line.delim";
const char *const FIELD_DELIM = "field.delim";

// CSV_OUTPUT_FORMAT specify delims.
const char *const SEPARATOR_CHAR = "separatorChar";
const char *const QUOTA_CHAR = "quoteChar";
const char *const ESCAPE_CHAR = "escapeChar";

// Hive related output formats.
// We will use the output format to read the file by hdfs api.
const char *const CSV_OUTPUT_FORMAT = "org.apache.hadoop.hive.serde2.OpenCSVSerde";
const char *const TEXTFILE_OUTPUT_FORMAT = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat";
const char *const PARQUET_OUTPUT_FORMAT = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat";
const char *const ORC_OUTPUT_FORMAT = "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat";

static constexpr int64_t OB_MAX_ACCESS_INFO_LENGTH = 1600;
const char *const HMS_KEYTAB = "hms_keytab=";
const char *const HMS_PRINCIPAL = "hms_principal=";
const char *const HMS_KRB5CONF = "hms_krb5conf=";

// Collection column type related.
static constexpr uint8_t OB_ARRAY_MAX_NESTED_LEVEL = 6;

class ObHiveTableMetadata final : public share::ObILakeTableMetadata
{
public:
  explicit ObHiveTableMetadata(ObIAllocator &allocator)
      : share::ObILakeTableMetadata(allocator), table_schema_(&allocator)
  {
  }

  virtual ~ObHiveTableMetadata() = default;

  share::ObLakeTableFormat get_format_type() const override;

  int assign(const ObHiveTableMetadata &other);

  int64_t get_convert_size() const override;

  int setup_tbl_schema(const uint64_t tenant_id,
                       const uint64_t database_id,
                       const uint64_t table_id,
                       const Apache::Hadoop::Hive::Table &table,
                       const ObString &properties,
                       const ObString &uri,
                       const ObString &access_info);

  static int calculate_part_val_from_string(const share::schema::ObTableSchema &table_schema,
                                            const bool &is_part_table,
                                            ObArray<ObString> &one_part_vals,
                                            ObIAllocator &allocator,
                                            ObNewRow &ob_part_row);

  const share::schema::ObTableSchema &get_table_schema() const
  {
    return table_schema_;
  }

protected:
  // hive format does not need to build table schema again, it already set up in `setup_tbl_schema`.
  int do_build_table_schema(share::schema::ObTableSchema *&table_schema) override;

private:
  static int setup_external_format(const Apache::Hadoop::Hive::Table &table,
                                   const ObString &output_format,
                                   ObIAllocator &allocator,
                                   share::schema::ObTableSchema &table_schema,
                                   ObExternalFileFormat &format,
                                   bool &is_open_csv);
  static int handle_csv_format(const Apache::Hadoop::Hive::Table &table,
                               const bool &is_open_csv,
                               ObIAllocator &allocator,
                               ObExternalFileFormat &format);
  static int handle_parquet_format(const Apache::Hadoop::Hive::Table &table,
                                   ObIAllocator &allocator,
                                   ObExternalFileFormat &format);
  static int handle_orc_format(const Apache::Hadoop::Hive::Table &table,
                               ObIAllocator &allocator,
                               ObExternalFileFormat &format);
  static int setup_external_location(const Apache::Hadoop::Hive::Table &table,
                                     const ObString &location,
                                     const ObString &uri,
                                     const ObString &access_info,
                                     ObIAllocator &allocator,
                                     share::schema::ObTableSchema &table_schema);
  static int extract_real_location(const ObString &uri,
                                   const ObString &table_location,
                                   ObIAllocator &allocator,
                                   ObString &real_location);
  static int extract_table_location_path(const ObString &table_location,
                                         const int64_t path_len,
                                         char *path);
  static int extract_host_and_port(const ObString &uri,
                                   char *host,
                                   int &port);
  static int build_column_schemas(const FieldSchemas &normal_cols,
                                  const FieldSchemas &part_cols,
                                  const bool &is_open_csv,
                                  ObExternalFileFormat &format,
                                  share::schema::ObTableSchema &table_schema);
  static int handle_column_types(const FieldSchemas &cols,
                                 const bool is_part_cols,
                                 const bool &is_open_csv,
                                 ObExternalFileFormat &format,
                                 share::schema::ObTableSchema &table_schema,
                                 int64_t &col_index,
                                 int64_t &part_index);
  static int handle_column_properties(const bool &is_part_cols,
                                      ObExternalFileFormat &format,
                                      const bool &is_bool_tinyint,
                                      ObIAllocator &allocator,
                                      share::schema::ObColumnSchemaV2 &column_schema,
                                      int64_t &part_index);
  static int set_partition_expr(const FieldSchemas &par_cols,
                                share::schema::ObTableSchema &table_schema);

  static int extract_decimal_precision_and_scale(ObString &decimal,
                                                 int64_t *precision,
                                                 int64_t *scale);
  static int extract_varchar_char_length(ObString &char_or_varchar,
                                         int64_t *length);
  static int convert_collection_type_info(const ObString &src,
                                          ObIAllocator &allocator,
                                          ObString &dst);
  static int handle_timestamp_part_val(const ObString &part_val,
                                       ObIAllocator &allocator,
                                       ObString &out_part_val);
  static int calc_num_from_str(const ObString &part_val, const bool &is_tinyint, int64_t &val);
  static int concat_type_to_collection_type(const ObString &src,
                                            const int32_t &start_index,
                                            const int32_t &end_index,
                                            const ObString &type,
                                            ObIAllocator &allocator,
                                            ObString &dst);

private:
  share::schema::ObTableSchema table_schema_;
};
} // namespace hive
} // namespace sql

} // namespace oceanbase

#endif // OB_SQL_TABLE_FORMAT_HIVE_H
