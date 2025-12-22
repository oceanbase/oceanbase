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
#include "ob_hive_table_metadata.h"

#include "lib/file/ob_string_util.h"
#include "share/external_table/ob_hdfs_storage_info.h"
#include "share/schema/ob_external_table_column_schema_helper.h"
#include "sql/engine/cmd/ob_load_data_parser.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "sql/resolver/dml/ob_dml_resolver.h"

#include <cctype>
#include <orc/Common.hh>
#include <parquet/arrow/schema.h>

namespace oceanbase
{
namespace sql
{
namespace hive
{
using namespace oceanbase::share::schema;

ObLakeTableFormat ObHiveTableMetadata::get_format_type() const
{
  return ObLakeTableFormat::HIVE;
}

int ObHiveTableMetadata::assign(const ObHiveTableMetadata &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    if (OB_FAIL(ObILakeTableMetadata::assign(other))) {
      LOG_WARN("failed to assign ObILakeTableMetadata", K(ret));
    } else {
      OZ(table_schema_.assign(other.table_schema_));
    }
  }
  return ret;
}

int64_t ObHiveTableMetadata::get_convert_size() const
{
  return sizeof(ObHiveTableMetadata) + table_schema_.get_convert_size();
}

int ObHiveTableMetadata::setup_tbl_schema(const uint64_t tenant_id,
                                          const uint64_t database_id,
                                          const uint64_t table_id,
                                          const Apache::Hadoop::Hive::Table &table,
                                          const ObString &properties,
                                          const ObString &uri,
                                          const ObString &access_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(properties) || OB_UNLIKELY(properties.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("properties is null or empty", K(ret));
  } else {
    // Initialization common param for table schema.
    table_schema_.set_table_type(EXTERNAL_TABLE);
    table_schema_.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    table_schema_.set_charset_type(CHARSET_UTF8MB4);
    table_schema_.set_table_name(table_name_);
    table_schema_.set_external_properties(properties);
    table_schema_.set_tenant_id(tenant_id);
    table_schema_.set_database_id(database_id);
    table_schema_.set_table_id(table_id);
    table_schema_.set_lake_table_format(ObLakeTableFormat::HIVE);
  }

  // Setup external table format.
  ObString output_format(table.sd.outputFormat.c_str());
  ObExternalFileFormat format;
  bool is_open_csv = false;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(setup_external_format(table,
                                           output_format,
                                           allocator_,
                                           table_schema_,
                                           format,
                                           is_open_csv))) {
    LOG_WARN("failed to setup external format", K(ret), K(output_format));
  }

  // Setup table external location info.
  ObString location(table.sd.location.c_str());
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(setup_external_location(table,
                                             location,
                                             uri,
                                             access_info,
                                             allocator_,
                                             table_schema_))) {
    LOG_WARN("failed to setup external location", K(ret), K(uri), K(location));
  }

  // Setup columns and partitions.
  FieldSchemas part_keys = table.partitionKeys;
  FieldSchemas normal_cols = table.sd.cols;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(build_column_schemas(normal_cols,
                                          part_keys,
                                          is_open_csv,
                                          format,
                                          table_schema_))) {
    LOG_WARN("failed to setup column schema", K(ret));
  } else if (OB_FAIL(set_partition_expr(part_keys, table_schema_))) {
    LOG_WARN("failed to setup partition expr", K(ret));
  } else {
    std::map<std::string, std::string>::const_iterator params_iter
        = table.parameters.find("transient_lastDdlTime");
    if (OB_UNLIKELY(params_iter != table.parameters.end())) {
      lake_table_metadata_version_
          = ::obsys::ObStringUtil::str_to_int(params_iter->second.c_str(), 0);
    }
  }

  return ret;
}

int ObHiveTableMetadata::do_build_table_schema(std::optional<int32_t> schema_id,  /*not used*/
                                               std::optional<int64_t> snapshot_id,  /*not used*/
                                               ObTableSchema *&table_schema)
{
  int ret = OB_SUCCESS;
  OZ(ObSchemaUtils::alloc_schema(allocator_, table_schema_, table_schema));
  return ret;
}

// ----------------------- utils ---------------------
int ObHiveTableMetadata::setup_external_format(const Apache::Hadoop::Hive::Table &table,
                                               const ObString &output_format,
                                               ObIAllocator &allocator,
                                               ObTableSchema &table_schema,
                                               ObExternalFileFormat &format,
                                               bool &is_open_csv)
{
  int ret = OB_SUCCESS;
  // Setup other needed properties.
  if (output_format.prefix_match(TEXTFILE_OUTPUT_FORMAT)) {
    format.format_type_ = ObExternalFileFormat::CSV_FORMAT;
    ObCollationType file_cs_type = ObCharset::get_system_collation();
    ObString serialization_lib(table.sd.serdeInfo.serializationLib.c_str());
    is_open_csv = serialization_lib.prefix_match(CSV_OUTPUT_FORMAT) ? true : false;
    if (OB_FAIL(format.csv_format_.init_format(ObDataInFileStruct(), 0, file_cs_type))) {
      LOG_WARN("failed to init csv format", K(ret));
    } else if (OB_ISNULL(serialization_lib) || serialization_lib.empty()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("serialization lib is null or empty", K(ret));
    } else if (OB_FAIL(handle_csv_format(table, is_open_csv, allocator, format))) {
      LOG_WARN("failed to handle csv format", K(ret));
    } else {
      // Binary format is default to base64 in hive.
      format.csv_format_.binary_format_ = ObCSVGeneralFormat::ObCSVBinaryFormat::BASE64;
    }
  } else if (output_format.prefix_match(PARQUET_OUTPUT_FORMAT)) {
    format.format_type_ = ObExternalFileFormat::PARQUET_FORMAT;
    if (OB_FAIL(handle_parquet_format(table, allocator, format))) {
      LOG_WARN("failed to handle parquet format", K(ret));
    }
  } else if (output_format.prefix_match(ORC_OUTPUT_FORMAT)) {
    format.format_type_ = ObExternalFileFormat::ORC_FORMAT;
    if (OB_FAIL(handle_orc_format(table, allocator, format))) {
      LOG_WARN("failed to handle orc format", K(ret));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("invalid output format", K(ret), K(output_format));
  }

  if (OB_FAIL(ret)) {
  } else {
    LOG_TRACE("set external file format", K(ret), K(format));
    bool is_valid = true;
    ObString format_str;

    if (OB_FAIL(ObDDLResolver::check_format_valid(format, is_valid))) {
      LOG_WARN("check format valid failed", K(ret));
    } else if (OB_UNLIKELY(!is_valid)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("file format is not valid", K(ret));
    } else if (OB_FAIL(format.to_string_with_alloc(format_str, allocator))) {
      LOG_WARN("failed to convert format to string", K(ret), K(format));
    } else if (OB_FAIL(table_schema.set_external_file_format(format_str))) {
      LOG_WARN("failed to set external file format", K(ret), K(format_str));
    }
  }
  return ret;
}

int ObHiveTableMetadata::handle_csv_format(const Apache::Hadoop::Hive::Table &table,
                                           const bool &is_open_csv,
                                           ObIAllocator &allocator,
                                           ObExternalFileFormat &format)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(format.format_type_ != ObExternalFileFormat::CSV_FORMAT)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid csv format type", K(ret), K(format.format_type_));
  }
  std::map<String, String> parameters = table.sd.serdeInfo.parameters;
  std::map<String, String>::iterator iter;
  // TODO(bitao): support more delim, such as map,struct,array delim ?
  iter = is_open_csv ? parameters.find(SEPARATOR_CHAR) : parameters.find(FIELD_DELIM);
  if (OB_FAIL(ret)) {
  } else if (OB_LIKELY(iter != parameters.end())) {
    const String &value = iter->second;
    if (OB_FAIL(ob_write_string(allocator,
                                ObString(value.c_str()),
                                format.csv_format_.field_term_str_,
                                true))) {
      LOG_WARN("failed to write string", K(ret));
    } else if (OB_UNLIKELY(1 != format.csv_format_.field_term_str_.length())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid length of field term str",
               K(ret),
               K(format.csv_format_.field_term_str_.length()));
    }
  } else {
    LOG_TRACE("enter the default field delim", K(ret));
    if (OB_FAIL(ob_write_string(allocator,
                                ObString(DEFAULT_FIELD_DELIMITER),
                                format.csv_format_.field_term_str_,
                                true))) {
      LOG_WARN("failed to set default field delim", K(ret));
    }
  }

  // Setting original field term str for schema printer.
  if (OB_FAIL(ret)) {
  } else {
    ObString print_field_str = format.csv_format_.field_term_str_;
    if (format.csv_format_.field_term_str_.length() == 1) {
      const unsigned char c
          = static_cast<unsigned char>(format.csv_format_.field_term_str_.ptr()[0]);
      // Convert control characters (non-printable) to char(N) format
      if (!std::isprint(c)) {
        char char_format_buf[32];
        int64_t pos = 0;
        if (OB_FAIL(databuff_printf(char_format_buf,
                                    sizeof(char_format_buf),
                                    pos,
                                    "char(%d)",
                                    static_cast<int>(c)))) {
          LOG_WARN("failed to format char string", K(ret), K(c));
        } else if (OB_FAIL(ob_write_string(allocator,
                                           ObString(static_cast<int64_t>(pos), char_format_buf),
                                           print_field_str,
                                           true))) {
          LOG_WARN("failed to write field term str", K(ret));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ob_write_string(allocator,
                                       print_field_str,
                                       format.origin_file_format_str_.origin_field_term_str_,
                                       true))) {
      LOG_WARN("failed to write default field delim string", K(ret));
    }
  }

  iter = parameters.find(LINE_DELIM);
  if (OB_FAIL(ret)) {
  } else if (OB_LIKELY(iter != parameters.end())) {
    const String &value = iter->second;
    if (OB_FAIL(ob_write_string(allocator,
                                ObString(value.c_str()),
                                format.csv_format_.line_term_str_,
                                true))) {
      LOG_WARN("failed to write string", K(ret));
    } else if (OB_UNLIKELY(0 >= format.csv_format_.line_term_str_.length())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid length of line term str",
               K(ret),
               K(format.csv_format_.line_term_str_),
               K(format.csv_format_.line_term_str_.length()));
    }
  } else {
    LOG_TRACE("enter the default line delim", K(ret));
    if (OB_FAIL(ob_write_string(allocator,
                                ObString(DEFAULT_LINE_DELIMITER),
                                format.csv_format_.line_term_str_,
                                true))) {
      LOG_WARN("failed to write default line delim string", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_LIKELY(is_open_csv)) {
    iter = parameters.find(ESCAPE_CHAR);
    if (OB_LIKELY(iter != parameters.end())) {
      const String &value = iter->second;
      const char *escaped_char = value.c_str();
      format.csv_format_.field_escaped_char_ = static_cast<int64_t>(escaped_char[0]);
    } else {
      LOG_TRACE("enter the default escape char", K(ret));
      format.csv_format_.field_escaped_char_ = static_cast<int64_t>('\\');
    }

    if (OB_FAIL(ret)) {
    } else {
      iter = parameters.find(QUOTA_CHAR);
      if (OB_LIKELY(iter != parameters.end())) {
        const String &value = iter->second;
        const char *enclosed_char = value.c_str();
        format.csv_format_.field_enclosed_char_ = static_cast<int64_t>(enclosed_char[0]);
      } else {
        LOG_TRACE("enter the default quote char", K(ret));
        format.csv_format_.field_enclosed_char_ = static_cast<int64_t>('"');
      }
    }
  }
  return ret;
}

int ObHiveTableMetadata::handle_parquet_format(const Apache::Hadoop::Hive::Table &table,
                                               ObIAllocator &allocator,
                                               ObExternalFileFormat &format)
{
  int ret = OB_SUCCESS;
  UNUSED(allocator);
  if (OB_UNLIKELY(format.format_type_ != ObExternalFileFormat::PARQUET_FORMAT)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid parquet format type", K(ret), K(format.format_type_));
  }

  // Handle compression algorithm.
  std::map<String, String> parameters = table.parameters;
  std::map<String, String>::iterator iter;
  iter = parameters.find(PARQUET_COMPRESSION);
  if (OB_FAIL(ret)) {
  } else if (OB_LIKELY(iter != parameters.end())) {
    const String &value = iter->second;
    ObString compression(value.c_str());
    for (int32_t compress_idx = 0; OB_SUCC(ret) && compress_idx <= parquet::Compression::LZ4_HADOOP;
         compress_idx++) {
      if (0
          == compression.case_compare(
              ObParquetGeneralFormat::COMPRESSION_ALGORITHMS[compress_idx])) {
        format.parquet_format_.compress_type_index_ = compress_idx;
        if (format.parquet_format_.compress_type_index_ == parquet::Compression::LZ4_FRAME
            || format.parquet_format_.compress_type_index_ == parquet::Compression::LZO
            || format.parquet_format_.compress_type_index_ == parquet::Compression::BZ2) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("compress type for parquet file is not supported yet", K(ret), K(compression));
        }
      }
    }
  } else {
    LOG_TRACE("enter the default parquet compress type", K(ret));
    format.parquet_format_.compress_type_index_ = 0;
  }

  // Handle other properties.
  iter = parameters.find(ROW_GROUP_SIZE);
  if (OB_FAIL(ret)) {
  } else if (OB_LIKELY(iter != parameters.end())) {
    const String &value = iter->second;
    OZ(format.parquet_format_.row_group_size_ = atoi(value.c_str()));
  } else {
    LOG_TRACE("enter the default parquet row group size", K(ret));
  }

  return ret;
}

int ObHiveTableMetadata::handle_orc_format(const Apache::Hadoop::Hive::Table &table,
                                           ObIAllocator &allocator,
                                           ObExternalFileFormat &format)
{
  int ret = OB_SUCCESS;
  UNUSED(allocator);
  if (OB_UNLIKELY(format.format_type_ != ObExternalFileFormat::ORC_FORMAT)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid orc format type", K(ret), K(format.format_type_));
  } else {
    format.orc_format_.compress_type_index_ = orc::CompressionKind::CompressionKind_ZLIB;
    // Default to position index type(POSITION) to adapt for hive 1.x.
    format.orc_format_.column_index_type_ = sql::ColumnIndexType::POSITION;
  }

  std::map<String, String> parameters = table.parameters;
  std::map<String, String>::iterator iter;
  iter = parameters.find(ORC_COMPRESSION);
  if (OB_FAIL(ret)) {
  } else if (OB_LIKELY(iter != parameters.end())) {
    const String &value = iter->second;
    ObString compression(value.c_str());
    for (int32_t compress_idx = 0;
         OB_SUCC(ret) && compress_idx <= orc::CompressionKind::CompressionKind_ZSTD;
         compress_idx++) {
      if (0 == compression.case_compare(ObOrcGeneralFormat::COMPRESSION_ALGORITHMS[compress_idx])) {
        format.orc_format_.compress_type_index_ = compress_idx;
        if (OB_UNLIKELY(format.orc_format_.compress_type_index_
                        == orc::CompressionKind::CompressionKind_LZO)) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("compress type for orc file is not supported yet", K(ret), K(compression));
        }
      }
    }
  } else {
    LOG_TRACE("enter the default orc compression", K(ret));
    // Means default is ZLIB.
    format.orc_format_.compress_type_index_ = orc::CompressionKind::CompressionKind_ZLIB;
  }

  // Handle other properties.
  iter = parameters.find(ORC_COMPRESSION_BLOCK_SIZE);
  if (OB_FAIL(ret)) {
  } else if (OB_LIKELY(iter != parameters.end())) {
    const String &value = iter->second;
    OZ(format.orc_format_.compression_block_size_ = atoi(value.c_str()));
  } else {
    LOG_TRACE("enter the default orc compression block size", K(ret));
  }

  iter = parameters.find(ORC_STRIPE_SIZE);
  if (OB_FAIL(ret)) {
  } else if (OB_LIKELY(iter != parameters.end())) {
    const String &value = iter->second;
    OZ(format.orc_format_.stripe_size_ = atoi(value.c_str()));
  } else {
    LOG_TRACE("enter the default orc stripe size", K(ret));
  }

  iter = parameters.find(ORC_ROW_INDEX_STRIDE);
  if (OB_FAIL(ret)) {
  } else if (OB_LIKELY(iter != parameters.end())) {
    const String &value = iter->second;
    OZ(format.orc_format_.compress_type_index_ = atoi(value.c_str()));
  } else {
    LOG_TRACE("enter the default orc row index stride", K(ret));
  }

  iter = parameters.find(ORC_BLOOM_FILTER_COLUMNS);
  if (OB_FAIL(ret)) {
  } else if (OB_LIKELY(iter != parameters.end())) {
    const String &value = iter->second;
    // TODO(bitao): support bloom filter columns
  } else {
    LOG_TRACE("enter the default orc bloom filter columns", K(ret));
  }

  return ret;
}

int ObHiveTableMetadata::setup_external_location(const Apache::Hadoop::Hive::Table &table,
                                                 const ObString &location,
                                                 const ObString &uri,
                                                 const ObString &access_info,
                                                 ObIAllocator &allocator,
                                                 ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  bool is_hdfs_type = location.prefix_match(OB_HDFS_PREFIX);

  ObString real_location;

  // only hdfs need to extract real location
  if (OB_FAIL(ret)) {
  } else if (is_hdfs_type
             && OB_FAIL(extract_real_location(uri, location, allocator, real_location))) {
    LOG_WARN("failed to extract real location", K(ret), K(uri), K(location), K(real_location));
  } else {
    ObString final_location = real_location.empty() ? location : real_location;
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to get storage info str", K(ret));
    } else if (OB_FAIL(table_schema.set_external_file_location(final_location))) {
      LOG_WARN("failed to set external file location", K(ret));
    } else if (OB_FAIL(table_schema.set_external_file_location_access_info(access_info))) {
      LOG_WARN("failed to set external file location access info", K(ret));
    } else {
      LOG_TRACE("get table schema in detail",
                K(ret),
                K(location),
                K(real_location),
                K(final_location));
    }
  }

  return ret;
}

int ObHiveTableMetadata::extract_real_location(const ObString &uri,
                                               const ObString &table_location,
                                               ObIAllocator &allocator,
                                               ObString &real_location)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(uri) || OB_LIKELY(0 == uri.length())) {
    ret = OB_INVALID_HMS_METASTORE;
    LOG_WARN("failed to handle meta uri is null", K(ret), K(uri));
  } else if (OB_ISNULL(table_location) || OB_LIKELY(0 == table_location.length())) {
    ret = OB_INVALID_HMS_TABLE_LOCATION;
    LOG_WARN("failed to handle table location which is null", K(ret), K(table_location));
  } else {
    const int64_t len = table_location.length();
    char path[len];
    const int64_t uri_len = uri.length();
    char host[uri_len];
    int port = 0;
    // Expect to set real location same as followed:
    // hdfs:// + metastore_host ${HDFS port} + path
    if (OB_FAIL(extract_table_location_path(table_location, len, path))) {
      LOG_WARN("failed to extract table location path", K(ret), K(table_location));
    } else if (OB_FAIL(extract_host_and_port(uri, host, port))) {
      LOG_WARN("failed to get namenode and path", K(ret), K(uri));
    } else if (table_location.prefix_match("hdfs://0.0.0.0")) {
      // If hive is loacated same machine as hadoop, then the table location
      // will be as "hdfs://0.0.0.0(:8020)/user/warehouse/${table_name}". So the
      // realocation should be same as host or ip of hdfs.
      LOG_TRACE("resolve table location on same machine by hive and hadoop",
                K(ret),
                K(table_location));
      ObSqlString tmp_table_location;
      if (OB_FAIL(tmp_table_location.append(OB_HDFS_PREFIX))) {
        LOG_WARN("failed to add hdfs prefix", K(ret));
      } else if (OB_FAIL(tmp_table_location.append(host))) {
        LOG_WARN("failed to add host", K(ret));
      } else if (OB_FAIL(tmp_table_location.append(path))) {
        LOG_WARN("failed to add host", K(ret));
      } else if (OB_FAIL(ob_write_string(allocator,
                                         tmp_table_location.string(),
                                         real_location,
                                         true))) {
        LOG_WARN("failed to write string", K(ret));
      } else {
        LOG_TRACE("get real location", K(ret), K(real_location));
      }
    } else {
      LOG_TRACE("resolve table location on different machine by hive and hadoop",
                K(ret),
                K(table_location));
      if (OB_FAIL(ob_write_string(allocator, table_location, real_location, true))) {
        LOG_WARN("failed to write string", K(ret), K(table_location));
      } else {
        LOG_TRACE("get real location", K(ret), K(real_location), K(table_location));
      }
    }
  }
  return ret;
}

int ObHiveTableMetadata::extract_table_location_path(const ObString &table_location,
                                                     const int64_t path_len,
                                                     char *path)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_location) || OB_LIKELY(0 == table_location.length())) {
    ret = OB_INVALID_HMS_TABLE_LOCATION;
    LOG_WARN("unable to handle table location", K(ret));
  } else if (OB_UNLIKELY(!table_location.prefix_match(OB_HDFS_PREFIX))) {
    ret = OB_INVALID_HMS_TABLE_LOCATION;
    LOG_WARN("unable to handle non-hdfs table location", K(ret), K(table_location));
  } else {
    // Full table path is qualified, i.e. "scheme://authority/path/to/table".
    // Extract "/path/to/table".
    const char *ptr = table_location.ptr();
    // Check last slash after "scheme://authority"
    const char *needed_last_slash = strchr(ptr + strlen(OB_HDFS_PREFIX), '/');
    if (OB_ISNULL(needed_last_slash)) {
      ret = OB_HDFS_MALFORMED_URI;
      OB_LOG(WARN, "failed to handle path", K(ret), K(table_location), K(needed_last_slash));
    }

    if (OB_FAIL(ret)) {
    } else {
      // Handle path.
      const int64_t len = strlen(needed_last_slash);
      if (OB_UNLIKELY(len > path_len)) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "expected path len exceed malloc", K(ret), K(len), K(path_len));
      } else {
        strncpy(path, needed_last_slash, len + 1);
        path[len] = '\0';
      }
    }
  }
  return ret;
}

int ObHiveTableMetadata::extract_host_and_port(const ObString &uri, char *host, int &port)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(uri) || OB_LIKELY(0 == uri.length())) {
    ret = OB_INVALID_HMS_METASTORE;
    LOG_WARN("failed to handle meta uri is null", K(ret), K(uri));
  } else if (OB_ISNULL(host)) {
    ret = OB_INVALID_HMS_HOST;
    LOG_WARN("failed to handle host is null", K(ret), K(uri));
  } else if (OB_UNLIKELY(!uri.prefix_match("thrift://"))) {
    ret = OB_INVALID_HMS_METASTORE;
    LOG_WARN("invalid metastore uri without prefix - thrift://", K(ret), K(uri));
  } else {
    // Full path is qualified, i.e. "thrift://host:port".
    // Extract "host" and "port".
    // Skip the "thrift://"
    const char *p = uri.ptr() + 9;
    const int l = uri.length() - 9;
    ObString tmp_uri(l, p);

    LOG_TRACE("get metastore uri in detail", K(ret), K(uri), K(tmp_uri));
    const char *ptr = tmp_uri.ptr();
    const char *needed_colon = strchr(ptr, ':');
    if (OB_ISNULL(needed_colon)) {
      ret = OB_INVALID_HMS_METASTORE;
      LOG_WARN("failed to handle host and port", K(ret), K(uri), K(tmp_uri));
    }

    const int64_t tmp_uri_len = tmp_uri.length();
    if (OB_FAIL(ret)) {
    } else {
      // Handle host.
      const int64_t ip_len = needed_colon - ptr;
      if (OB_UNLIKELY(ip_len > tmp_uri_len)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expected host len execeed malloc", K(ret), K(ip_len), K(tmp_uri_len));
      } else {
        strncpy(host, ptr, ip_len);
        host[ip_len] = '\0';
      }

      if (OB_FAIL(ret)) {
      } else {
        // Handle port.
        const char *port_str = needed_colon + 1;
        const int64_t port_len = tmp_uri_len - ip_len - 1;

        char tmp_port[port_len + 1];
        if (OB_UNLIKELY(port_len > tmp_uri_len)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expected port len exceed malloc",
                   K(ret),
                   K(port_str),
                   K(port_len),
                   K(tmp_uri_len));
        } else {
          strncpy(tmp_port, port_str, port_len);
          tmp_port[port_len] = '\0';
        }
        // Port should be a int number.
        if (OB_FAIL(ret)) {
        } else {
          port = ::obsys::ObStringUtil::str_to_int(tmp_port, 0);
          if (OB_UNLIKELY(!::obsys::ObStringUtil::is_int(tmp_port))) {
            ret = OB_INVALID_HMS_PORT;
            LOG_WARN("port is not int type", K(ret), K(port), K(port_len), K(port_str));
          } else if (OB_LIKELY(port == 0)) {
            ret = OB_INVALID_HMS_PORT;
            LOG_WARN("failed to get port", K(ret));
          } else {
            LOG_TRACE("get port success", K(ret), K(port));
          }
        }
      }
    }
  }

  return ret;
}

int ObHiveTableMetadata::build_column_schemas(const FieldSchemas &normal_cols,
                                              const FieldSchemas &part_cols,
                                              const bool &is_open_csv,
                                              ObExternalFileFormat &format,
                                              ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = lib::is_oracle_mode();

  ObString mock_gen_column_str;
  int64_t col_index = 0;
  int64_t part_index = 0;
  if (OB_FAIL(handle_column_types(normal_cols,
                                  false,
                                  is_open_csv,
                                  format,
                                  table_schema,
                                  col_index,
                                  part_index))) {
    LOG_WARN("failed to build normal columns", K(ret));
  } else if (OB_LIKELY(!part_cols.empty())
             && OB_FAIL(handle_column_types(part_cols,
                                            true,
                                            is_open_csv,
                                            format,
                                            table_schema,
                                            col_index,
                                            part_index))) {
    LOG_WARN("failed to build partition columns", K(ret));
  }

  return ret;
}

int ObHiveTableMetadata::handle_column_types(const FieldSchemas &cols,
                                             const bool is_part_cols,
                                             const bool &is_open_csv,
                                             ObExternalFileFormat &format,
                                             ObTableSchema &table_schema,
                                             int64_t &col_index,
                                             int64_t &part_index)
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = lib::is_oracle_mode();
  // Use temp allocator to alloc because building column will deep copy in
  // table_schema, It is not necessary to holder the allocator all time.
  ObArenaAllocator allocator;
  // Example: FieldSchema(name=xxx, type=xxx, comment=xxx)
  const bool is_csv_format = format.format_type_ == ObExternalFileFormat::CSV_FORMAT;
  for (int64_t i = 0; OB_SUCC(ret) && i < cols.size(); i++, col_index++) {
    schema::ObColumnSchemaV2 column_schema;
    ObString field_name(cols[i].name.c_str());

    // Setup basic properties
    column_schema.set_table_id(table_schema.get_table_id());
    column_schema.set_column_id(col_index + OB_END_RESERVED_COLUMN_ID_NUM);
    column_schema.set_column_name(field_name);

    ObString original_field_type(cols[i].type.c_str());
    // for bool type (csv format only), it will be stored as tinyint(1) in hive.
    bool is_bool_tinyint = false;
    bool is_binary_collation = false;

    if (OB_FAIL(ret)) {
    } else if (original_field_type.prefix_match(MAP) || original_field_type.prefix_match(STRUCT)
               || original_field_type.prefix_match(UNIONTYPE)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not support type", K(ret), K(original_field_type));
    } else if (original_field_type.prefix_match(ARRAY)) {
      if (OB_UNLIKELY(format.format_type_ != ObExternalFileFormat::PARQUET_FORMAT)) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not support type", K(ret), K(original_field_type));
      } else {
        column_schema.set_data_type(ObCollectionSQLType);
        ObString field_type_str;
        OZ(convert_collection_type_info(original_field_type, allocator, field_type_str));
        OZ(column_schema.add_type_info(field_type_str));
      }
    } else if (original_field_type.prefix_match(TINYINT)
               || original_field_type.prefix_match(BOOLEAN)) {
      if (OB_LIKELY(is_csv_format) && original_field_type.prefix_match(BOOLEAN)) {
        is_bool_tinyint = true;
      }
      if (OB_FAIL(
              ObExternalTableColumnSchemaHelper::setup_tinyint(is_oracle_mode, column_schema))) {
        LOG_WARN("failed to setup tinyint", K(ret));
      }
    } else if (original_field_type.prefix_match(SMALLINT)) {
      if (OB_FAIL(
              ObExternalTableColumnSchemaHelper::setup_smallint(is_oracle_mode, column_schema))) {
        LOG_WARN("failed to setup smallint", K(ret));
      }
    } else if (original_field_type.prefix_match(INT)) {
      if (OB_FAIL(ObExternalTableColumnSchemaHelper::setup_int(is_oracle_mode, column_schema))) {
        LOG_WARN("failed to setup int", K(ret));
      }
    } else if (original_field_type.prefix_match(BIGINT)) {
      if (OB_FAIL(ObExternalTableColumnSchemaHelper::setup_bigint(is_oracle_mode, column_schema))) {
        LOG_WARN("failed to setup bigint", K(ret));
      }
    } else if (original_field_type.prefix_match(FLOAT)) {
      if (OB_FAIL(ObExternalTableColumnSchemaHelper::setup_float(is_oracle_mode, column_schema))) {
        LOG_WARN("failed to setup float", K(ret));
      }
    } else if (original_field_type.prefix_match(DOUBLE)) {
      if (OB_FAIL(ObExternalTableColumnSchemaHelper::setup_double(is_oracle_mode, column_schema))) {
        LOG_WARN("failed to setup double", K(ret));
      }
    } else if (original_field_type.prefix_match(DECIMAL)) {
      column_schema.set_data_type(ObDecimalIntType);
      int64_t precision = 0;
      int64_t scale = 0;
      if (OB_FAIL(extract_decimal_precision_and_scale(original_field_type, &precision, &scale))) {
        LOG_WARN("failed to extract decimal precision and scale", K(ret), K(precision), K(scale));
      } else if (OB_FAIL(ObExternalTableColumnSchemaHelper::setup_decimal(is_oracle_mode,
                                                                          precision,
                                                                          scale,
                                                                          column_schema))) {
        LOG_WARN("failed to setup decimal", K(ret), K(precision), K(scale));
      } else {
        LOG_TRACE("setup decimal in detail", K(ret), K(precision), K(scale));
      }
    } else if (original_field_type.prefix_match(STRING)
               || original_field_type.prefix_match(BINARY)) {
      is_binary_collation
          = is_csv_format && !is_open_csv && original_field_type.prefix_match(BINARY);
      if (OB_FAIL(ObExternalTableColumnSchemaHelper::setup_string(is_oracle_mode,
                                                                  CHARSET_UTF8MB4,
                                                                  CS_TYPE_UTF8MB4_BIN,
                                                                  column_schema))) {
        LOG_WARN("failed to setup string", K(ret));
      } else if (OB_LIKELY(is_binary_collation)) {
        // When hive table is csv format, binary type will be stored as base64 string.
        // And when hive table is open csv format, binary type will be stored as normal string.
        column_schema.set_collation_type(CS_TYPE_BINARY);
      }
    } else if (original_field_type.prefix_match(VARCHAR)) {
      int64_t varchar_len = 0;
      if (OB_LIKELY(original_field_type.prefix_match(VARCHAR))
          && OB_FAIL(extract_varchar_char_length(original_field_type, &varchar_len))) {
        LOG_WARN("failed to extract varchar length", K(ret), K(varchar_len));
      } else if (OB_FAIL(ObExternalTableColumnSchemaHelper::setup_varchar(is_oracle_mode,
                                                                          varchar_len,
                                                                          ObCharsetType::CHARSET_UTF8MB4,
                                                                          ObCollationType::CS_TYPE_UTF8MB4_BIN,
                                                                          column_schema))) {
        LOG_WARN("failed to setup varchar", K(ret), K(varchar_len));
      }
    } else if (original_field_type.prefix_match(CHAR)) {
      int64_t char_len = 0;
      if (OB_FAIL(extract_varchar_char_length(original_field_type, &char_len))) {
        LOG_WARN("failed to extract char length", K(ret), K(char_len));
      } else if (OB_FAIL(ObExternalTableColumnSchemaHelper::setup_char(is_oracle_mode,
                                                                       char_len,
                                                                       ObCharsetType::CHARSET_UTF8MB4,
                                                                       ObCollationType::CS_TYPE_UTF8MB4_BIN,
                                                                       column_schema))) {
        LOG_WARN("failed to setup char", K(ret), K(char_len));
      }
    } else if (original_field_type.prefix_match(TIMESTAMP)) {
      if (OB_FAIL(
              ObExternalTableColumnSchemaHelper::setup_timestamp(is_oracle_mode, column_schema))) {
        LOG_WARN("failed to setup timestamp", K(ret));
      }
    } else if (original_field_type.prefix_match(DATE)) {
      if (OB_FAIL(ObExternalTableColumnSchemaHelper::setup_date(is_oracle_mode, column_schema))) {
        LOG_WARN("failed to setup date", K(ret));
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not support type", K(ret), K(original_field_type));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(handle_column_properties(is_part_cols,
                                                format,
                                                is_bool_tinyint,
                                                is_binary_collation,
                                                allocator,
                                                column_schema,
                                                part_index))) {
      LOG_WARN("failed to handle column other properties", K(ret));
    } else if (OB_FAIL(table_schema.add_column(column_schema))) {
      LOG_WARN("failed to add column", K(ret), K(column_schema));
    } else if (is_part_cols
               && OB_FAIL(table_schema.add_partition_key(column_schema.get_column_name()))) {
      LOG_WARN("failed to add partition key", K(ret), K(column_schema.get_column_name()));
    }
  }
  return ret;
}

int ObHiveTableMetadata::handle_column_properties(const bool &is_part_cols,
                                                  ObExternalFileFormat &format,
                                                  const bool &is_bool_tinyint,
                                                  const bool &is_binary_collation,
                                                  ObIAllocator &allocator,
                                                  ObColumnSchemaV2 &column_schema,
                                                  int64_t &part_index)
{
  int ret = OB_SUCCESS;
  ObString mock_gen_column_str;
  if (OB_FAIL(ret)) {
  } else if (OB_LIKELY(is_part_cols)) {
    ObSqlString temp_str;
    if (OB_FAIL(temp_str.assign_fmt("%s%ld", N_PARTITION_LIST_COL, ++part_index))) {
      LOG_WARN("failed to assign fmt", K(ret));
    } else if (OB_FAIL(ob_write_string(allocator, temp_str.string(), mock_gen_column_str))) {
      LOG_WARN("failed to write string", K(ret));
    }
  } else if (format.format_type_ == ObExternalFileFormat::CSV_FORMAT && is_bool_tinyint) {
    if (OB_FAIL(format.mock_gen_bool_tinyint_column_def(column_schema,
                                                        allocator,
                                                        mock_gen_column_str))) {
      LOG_WARN("fail to mock gen bool tinyint column def", K(ret));
    }
  } else if (OB_FAIL(format.mock_gen_column_def(column_schema, allocator, mock_gen_column_str))) {
    LOG_WARN("fail to mock gen column def", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else {
    if (OB_FAIL(ObDMLResolver::set_basic_column_properties(column_schema, mock_gen_column_str))) {
      LOG_WARN("fail to set properties for column", K(ret));
    } else if (OB_LIKELY(is_oracle_mode)
               && OB_FAIL(ObDMLResolver::set_upper_column_name(column_schema))) {
      LOG_WARN("fail to set upper column name in oracle mode", K(ret));
    } else if (is_binary_collation){
      // reset properties from ObDMLResolver::set_basic_column_properties()
      column_schema.set_collation_type(ObCollationType::CS_TYPE_BINARY);
    } else {
      // reset properties from ObDMLResolver::set_basic_column_properties()
      column_schema.set_collation_type(ObCollationType::CS_TYPE_UTF8MB4_BIN);
    }
  }
  return ret;
}

int ObHiveTableMetadata::set_partition_expr(const FieldSchemas &par_cols,
                                            ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;

  ObArenaAllocator allocator;
  bool is_part_table = !par_cols.empty();

  if (OB_LIKELY(is_part_table)) {
    table_schema.set_part_level(share::schema::PARTITION_LEVEL_ONE);
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_LIST);
    // Construct partition columns expr.
    ObString part_cols;
    char *buf = static_cast<char *>(allocator.alloc(OB_MAX_PARTITION_EXPR_LENGTH));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else {
      int64_t pos = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < par_cols.size(); i++) {
        if (i > 0) {
          if (OB_FAIL(databuff_printf(buf, OB_MAX_PARTITION_EXPR_LENGTH, pos, ","))) {
            LOG_WARN("failed to printf", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else {
          ObString part_name(par_cols[i].name.c_str());
          if (OB_FAIL(databuff_printf(buf,
                                      OB_MAX_PARTITION_EXPR_LENGTH,
                                      pos,
                                      "%.*s",
                                      part_name.length(),
                                      part_name.ptr()))) {
            LOG_WARN("failed to printf", K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        part_cols.assign_ptr(buf, static_cast<int32_t>(pos));
        if (OB_FAIL(table_schema.get_part_option().set_part_expr(part_cols))) {
          LOG_WARN("failed to set part expr", K(ret));
        } else {
          table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_LIST_COLUMNS);
        }
      }
    }
  }
  return ret;
}

int ObHiveTableMetadata::calculate_part_val_from_string(const ObTableSchema &table_schema,
                                                        const bool &is_part_table,
                                                        ObIArray<ObString> &one_part_vals,
                                                        ObIAllocator &allocator,
                                                        ObNewRow &ob_part_row)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_part_table)) {
    LOG_TRACE("skip to handle non-partition table", K(ret));
  } else if (OB_UNLIKELY(lib::is_oracle_mode())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support oracle mode now", K(ret));
  } else {
    const ObPartitionKeyInfo &part_key_info = table_schema.get_partition_key_info();
    const int64_t part_size = part_key_info.get_size();
    ObObj *obj_array = nullptr;
    if (OB_ISNULL(obj_array = static_cast<ObObj *>(allocator.alloc(sizeof(ObObj) * part_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", K(ret));
    } else {
      for (ObObj *ptr = obj_array; OB_SUCC(ret) && ptr < obj_array + part_size; ++ptr) {
        new (ptr) ObObj();
      }
      ob_part_row.assign(obj_array, part_size);
    }

    for (int j = 0; OB_SUCC(ret) && j < one_part_vals.count(); j++) {
      const ObRowkeyColumn *part_col = part_key_info.get_column(j);
      ObObjType part_key_type = ObUnknownType;
      ObString part_val(one_part_vals.at(j));
      if (OB_ISNULL(part_col)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null ptr", K(ret), K(j));
      } else if (FALSE_IT(part_key_type = part_col->get_meta_type().get_type())) {
      } else if (part_val.case_compare_equal("__HIVE_DEFAULT_PARTITION__")) {
        // Default partition is displayed as "__HIVE_DEFAULT_PARTITION__" in hive and it will show
        // as "NULL" value.
        ob_part_row.get_cell(j).set_meta_type(part_col->get_meta_type());
        ob_part_row.get_cell(j).set_null();
      } else if (part_key_type == ObVarcharType || part_key_type == ObCharType
                 || part_key_type == ObMediumTextType) {
        common::ObObjMeta meta_type = part_col->get_meta_type();
        ObCollationType coll_dst = static_cast<ObCollationType>(meta_type.get_cs_type());
        ObCollationType coll_src = CS_TYPE_UTF8MB4_BIN;
        int64_t dst_maxblen = 0;
        int64_t src_minblen = 0;
        if (OB_FAIL(ObCharset::get_mbmaxlen_by_coll(coll_dst, dst_maxblen))) {
          LOG_WARN("failed to get dst mb max len", K(ret), K(coll_dst));
        } else if (OB_FAIL(ObCharset::get_mbminlen_by_coll(coll_src, src_minblen))) {
          LOG_WARN("failed to get src mb min len", K(ret), K(coll_src));
        } else {
          void *dst_buf = NULL;
          uint64_t dst_buf_size = (part_val.length() / src_minblen) * dst_maxblen;
          uint32_t dst_len = 0;
          if (OB_ISNULL(dst_buf = allocator.alloc(dst_buf_size))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to alloc buf", K(ret));
          } else if (OB_FAIL(ObCharset::charset_convert(coll_src,
                                                        part_val.ptr(),
                                                        part_val.length(),
                                                        coll_dst,
                                                        static_cast<char *>(dst_buf),
                                                        dst_buf_size,
                                                        dst_len))) {
            LOG_WARN("failed to convert charset", K(ret));
          } else if (part_key_type == ObMediumTextType) { // string type
            ObString lob_data(static_cast<int64_t>(dst_len), static_cast<char *>(dst_buf));
            ObString lob_with_header;
            if (OB_FAIL(ObLobManager::fill_lob_header(allocator, lob_data, lob_with_header))) {
              LOG_WARN("failed to fill lob header", K(ret));
            } else {
              ob_part_row.get_cell(j).set_meta_type(part_col->get_meta_type());
              ob_part_row.get_cell(j).set_string(ObMediumTextType,
                                                 lob_with_header.ptr(),
                                                 lob_with_header.length());
              ob_part_row.get_cell(j).set_has_lob_header();
            }
          } else {
            ob_part_row.get_cell(j).set_meta_type(part_col->get_meta_type());
            ob_part_row.get_cell(j).set_varchar_value(static_cast<char *>(dst_buf),
                                                      static_cast<int64_t>(dst_len));
          }
        }
      } else if (part_key_type == ObTinyIntType || part_key_type == ObSmallIntType
                 || part_key_type == ObMediumIntType || part_key_type == ObInt32Type
                 || part_key_type == ObIntType) {
        int64_t val = 0;
        // Only support boolean type in hive.
        if (part_key_type == ObTinyIntType) {
          if (OB_FAIL(calc_num_from_str(part_val, true, val))) {
            LOG_WARN("failed to calc num from str", K(ret), K(part_val), K(part_key_type), K(j));
          } else {
            ob_part_row.get_cell(j).set_meta_type(part_col->get_meta_type());
            ob_part_row.get_cell(j).set_int(val);
          }
        } else {
          if (OB_FAIL(calc_num_from_str(part_val, false, val))) {
            LOG_WARN("failed to calc num from str", K(ret), K(part_val), K(part_key_type), K(j));
          } else {
            ob_part_row.get_cell(j).set_meta_type(part_col->get_meta_type());
            ob_part_row.get_cell(j).set_int(val);
          }
        }
      } else if (part_key_type == ObDateType) {
        int32_t time_val = 0;
        if (OB_FAIL(ObTimeConverter::str_to_date(part_val, time_val))) {
          LOG_WARN("failed to convert string to date", K(ret), K(part_val));
        } else {
          ob_part_row.get_cell(j).set_meta_type(part_col->get_meta_type());
          ob_part_row.get_cell(j).set_date(time_val);
        }
      } else if (part_key_type == ObTimestampType) {
        int64_t out_val = 0;
        ObScale res_scale; // Useless
        ObSQLSessionInfo *session = THIS_WORKER.get_session();
        const ObTimeZoneInfo *tz_info = session->get_timezone_info();
        ObDateSqlMode sql_mode;
        sql_mode.allow_invalid_dates_ = false;
        sql_mode.no_zero_date_ = false;
        sql_mode.implicit_first_century_year_ = true;
        ObTimeConvertCtx cvrt_ctx(tz_info, true, false);
        ObString out_part_val;
        if (OB_FAIL(handle_timestamp_part_val(part_val, allocator, out_part_val))) {
          LOG_WARN("failed to handle timestamp part val", K(ret), K(part_val));
        } else if (OB_FAIL(ObTimeConverter::str_to_datetime(out_part_val,
                                                            cvrt_ctx,
                                                            out_val,
                                                            &res_scale,
                                                            sql_mode))) {
          LOG_WARN("failed to convert string to timestamp", K(ret), K(part_val));
        } else {
          ob_part_row.get_cell(j).set_meta_type(part_col->get_meta_type());
          ob_part_row.get_cell(j).set_timestamp(out_val);
        }
      } else if (part_key_type == ObDoubleType || part_key_type == ObFloatType) {
        int err = 0;
        char *endptr = NULL;
        double out_val = 0.0;
        out_val = ObCharset::strntodv2(part_val.ptr(), part_val.length(), &endptr, &err);
        if (EOVERFLOW == err && (-DBL_MAX == out_val || DBL_MAX == out_val)) {
          ret = OB_DATA_OUT_OF_RANGE;
          LOG_WARN("double out of range", K(ret), K(part_val), K(part_key_type), K(j));
        } else if (part_key_type == ObDoubleType) {
          ob_part_row.get_cell(j).set_meta_type(part_col->get_meta_type());
          ob_part_row.get_cell(j).set_double(out_val);
        } else {
          // float type should be converted from double.
          ob_part_row.get_cell(j).set_meta_type(part_col->get_meta_type());
          ob_part_row.get_cell(j).set_float(static_cast<float>(out_val));
        }
      } else if (part_key_type == ObDecimalIntType) {
        ObDecimalInt *decint = nullptr;
        ObDecimalIntBuilder res_val;
        // in_precision and in_scale are from field type in hive.
        int32_t val_len = 0;
        int16_t in_precision = 0;
        int16_t in_scale = 0;
        uint64_t column_id = part_col->column_id_;
        const ObColumnSchemaV2 *col_schema = table_schema.get_column_schema(column_id);
        if (OB_ISNULL(col_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null ptr", K(ret), K(j));
        }
        int64_t precision = col_schema->get_data_precision();
        int64_t scale = col_schema->get_data_scale();
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(wide::from_string(part_val.ptr(),
                                             part_val.length(),
                                             allocator,
                                             in_scale,
                                             in_precision,
                                             val_len,
                                             decint))) {
          LOG_WARN("failed to convert string to decimal",
                   K(ret),
                   K(part_val),
                   K(in_precision),
                   K(in_scale));
        } else if (ObDatumCast::need_scale_decimalint(in_scale, in_precision, scale, precision)) {
          if (OB_FAIL(ObDatumCast::common_scale_decimalint(decint,
                                                           val_len,
                                                           in_scale,
                                                           scale,
                                                           precision,
                                                           0,
                                                           res_val))) {
            LOG_WARN("failed to scale decimal",
                     K(ret),
                     K(part_val),
                     K(in_precision),
                     K(in_scale),
                     K(precision),
                     K(scale));
          }
        }
        if (OB_FAIL(ret)) {
        } else {
          ob_part_row.get_cell(j).set_meta_type(part_col->get_meta_type());
          ob_part_row.get_cell(j).set_decimal_int(
              res_val.get_int_bytes(),
              in_scale,
              const_cast<ObDecimalInt *>(res_val.get_decimal_int()));
          // deep copy
          OZ(ob_write_obj(allocator, ob_part_row.get_cell(j), ob_part_row.get_cell(j)));
        }
      } else if (lib::is_oracle_mode() && ObNumberType == part_key_type) {
        number::ObNumber num;
        if (OB_FAIL(num.from(part_val.ptr(), part_val.length(), allocator))) {
          LOG_WARN("cast string to number failed", K(ret), K(part_val), K(part_key_type));
        } else {
          LOG_TRACE("cast string to number success", K(ret));
          ob_part_row.get_cell(j).set_meta_type(part_col->get_meta_type());
          ob_part_row.get_cell(j).set_number(num);
        }
      } else {
        // TODO(bitao): fix support to run in oracle mode and more types
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported part_key_type", K(part_val), K(part_key_type), K(j), K(ret));
      }
    }
  }
  return ret;
}

int ObHiveTableMetadata::extract_decimal_precision_and_scale(ObString &decimal,
                                                             int64_t *precision,
                                                             int64_t *scale)
{
  int ret = OB_SUCCESS;
  const char *p = decimal.ptr();

  // Find out '('
  while (*p && *p != '(') {
    p++;
  }
  if (OB_UNLIKELY(!*p)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid decimal format to handle", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else {
    // Extract the precision
    *precision = 0;
    // Skip '('
    p++;

    while (*p >= '0' && *p <= '9') {
      *precision = *precision * 10 + (*p - '0');
      p++;
    }
  }

  // Find out ','
  while (*p && *p != ',') {
    p++;
  }
  if (OB_UNLIKELY(!*p)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid decimal format to handle", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else {
    // Extract the scale
    *scale = 0;
    // Skip ','
    p++;
    while (*p >= '0' && *p <= '9') {
      *scale = *scale * 10 + (*p - '0');
      p++;
    }
  }
  return ret;
}

int ObHiveTableMetadata::extract_varchar_char_length(ObString &char_or_varchar, int64_t *length)
{
  int ret = OB_SUCCESS;
  const char *p = char_or_varchar.ptr();

  // Find out '('
  while (*p && *p != '(') {
    p++;
  }
  if (OB_UNLIKELY(!*p)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid decimal format to handle", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else {
    // Extract the precision
    *length = 0;
    // Skip '('
    p++;

    while (*p >= '0' && *p <= '9') {
      *length = *length * 10 + (*p - '0');
      p++;
    }
  }
  return ret;
}

int ObHiveTableMetadata::convert_collection_type_info(const ObString &src,
                                                      ObIAllocator &allocator,
                                                      ObString &dst)
{
  int ret = OB_SUCCESS;
  // array<int> -> array(int)
  // array<string> -> array(varchar) -- because varchar is compatible with `string` in hive
  if (OB_ISNULL(src) || OB_UNLIKELY(src.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid collection type info", K(ret));
  } else if (OB_UNLIKELY(src.prefix_match(MAP) || src.prefix_match(STRUCT))) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support map/struct in hive", K(ret), K(src));
  } else if (OB_UNLIKELY(!src.prefix_match(ARRAY))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unable to handle non-collection type", K(ret), K(src));
  } else {
    const int32_t src_len = src.length();
    const char *src_ptr = src.ptr();
    char *dst_ptr = nullptr;
    void *ptr = NULL;
    char letter = '\0';
    if (OB_ISNULL(ptr = allocator.alloc(src_len))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for type info", K(ret));
    } else {
      // Converted lower case to upper case and change the '<>' to '()'.
      dst_ptr = static_cast<char *>(ptr);
      for (int32_t i = 0; OB_SUCC(ret) && i < src_len; i ++) {
        letter = src_ptr[i];

        if (letter >= 'A' && letter <= 'Z') {
          dst_ptr[i] = letter;
        }  else if (letter >= '0' && letter <= '9') {
          dst_ptr[i] = letter;
        } else if (letter == '(' || letter == ')' || letter == ',') {
          dst_ptr[i] = letter;
        } else if (letter >= 'a' && letter <= 'z') {
          dst_ptr[i] = static_cast<char>(letter - 32);
        } else if (letter == '<') {
          dst_ptr[i] = '(';
        } else if (letter == '>') {
          dst_ptr[i] = ')';
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected char", K(ret), K(letter), K(src));
        }
      }
      dst.assign_ptr(dst_ptr, src_len);
      LOG_TRACE("convert collection type info", K(ret), K(src), K(dst));
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    // TODO(bitao): support map/struct.
    // Find the inner element type in `ARRAY` type.
    // Example:
    //  array(array(int)) -> int.
    //  array(array(string)) -> varchar.
    //  array(array(boolean)) -> boolean -> tinyint.
    //  array(array(varchar(10))) -> varchar(10).
    //  array(array(decimal(10, 2))) -> decimal(10, 2).
    const int32_t dst_len = dst.length();
    int32_t start_index = -1;
    int32_t end_index = -1;
    const char *dst_ptr = dst.ptr();
    char letter = '\0';

    for (int32_t index = dst_len - 1; dst_len > 0; index --) {
      letter = dst_ptr[index];
      if (letter == '(' && start_index == -1) {
        start_index = index + 1;
      } else if (letter != ')' && end_index == -1) {
        if (letter >= '0' && letter <= '9') {
          // Such as
          //  array(array(varchar(10))) -> varchar(10).
          //  array(array(decimal(10, 2))) -> decimal(10, 2).
          // And the depth of the array is 2 not 3.
          end_index = index + 1;
        } else {
          end_index = index;
        }
      } else if (start_index != -1 && end_index != -1) {
        break;
      }
    }
    ObString inner_type;
    if (OB_FAIL(ob_sub_str(allocator, dst, start_index, end_index, inner_type))) {
      LOG_WARN("failed to sub str", K(ret), K(start_index), K(end_index), K(dst));
    } else if (inner_type.prefix_match("STRING")) {
      if (OB_FAIL(concat_type_to_collection_type(dst, start_index, end_index, ObString("VARCHAR(65535)"), allocator, dst))) {
        LOG_WARN("failed to concat string to varchar", K(ret), K(dst), K(start_index), K(end_index));
      }
    } else if (inner_type.prefix_match("BOOLEAN")) {
      if (OB_FAIL(concat_type_to_collection_type(dst, start_index, end_index, ObString("TINYINT"), allocator, dst))) {
        LOG_WARN("failed to concat boolean to tinyint", K(ret), K(dst), K(start_index), K(end_index));
      }
    }
    LOG_TRACE("fetch inner type", K(ret), K(inner_type), K(dst));
  }
  return ret;
}

int ObHiveTableMetadata::handle_timestamp_part_val(const ObString &part_val,
                                                   ObIAllocator &allocator,
                                                   ObString &out_part_val)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(part_val.ptr()) || OB_UNLIKELY(part_val.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid timestamp part val", K(ret));
  } else {
    const char *p = part_val.ptr();
    const char *end = p + part_val.length();
    char *buf = nullptr;
    int64_t buf_len = part_val.length(); // 输出buffer长度不会超过输入长度

    if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret), K(buf_len));
    } else {
      char *dest = buf;
      const char *data = p;

      while (data < end) {
        if (data + 2 < end && data[0] == '%' && data[1] == '3'
            && (data[2] == 'A' || data[2] == 'a')) {
          *dest = ':';
          data += 3; // 跳过 %3A
        } else {
          *dest = *data;
          ++data;
        }
        ++dest;
      }

      out_part_val.assign_ptr(buf, static_cast<int32_t>(dest - buf));
    }
  }
  return ret;
}

int ObHiveTableMetadata::calc_num_from_str(const ObString &part_val,
                                           const bool &is_tinyint,
                                           int64_t &val)
{
  int ret = OB_SUCCESS;
  val = INT64_MIN;
  if (OB_ISNULL(part_val) || OB_UNLIKELY(part_val.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid part val", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (is_tinyint && part_val.case_compare_equal("true")) {
    val = 1;
  } else if (is_tinyint && part_val.case_compare_equal("false")) {
    val = 0;
  } else {
    bool is_negative = false;
    for (int64_t k = 0; OB_SUCC(ret) && k < part_val.length(); ++k) {
      if (part_val.ptr()[k] >= '0' && part_val.ptr()[k] <= '9') {
        val = val * 10 + part_val.ptr()[k] - '0';
      } else if (part_val.ptr()[k] == '-') {
        is_negative = true;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected part_val", K(ret), K(is_tinyint), K(part_val));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (is_negative) {
      val = -val;
    }
  }
  return ret;
}

int ObHiveTableMetadata::concat_type_to_collection_type(const ObString &src,
                                                        const int32_t &start_index,
                                                        const int32_t &end_index,
                                                        const ObString &type,
                                                        ObIAllocator &allocator,
                                                        ObString &dst)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src.ptr()) || OB_UNLIKELY(src.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid src", K(ret));
  } else if (OB_ISNULL(type.ptr()) || OB_UNLIKELY(type.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid type", K(ret));
  } else if (start_index < 0 || end_index < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid start_index or end_index", K(ret), K(start_index), K(end_index));
  } else if (start_index >= src.length() || end_index >= src.length()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid start_index or end_index", K(ret), K(start_index), K(end_index));
  } else if (start_index >= end_index) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid start_index or end_index", K(ret), K(start_index), K(end_index));
  } else {
    ObString dst_prefix;
    ObString dst_subfix;
    if (OB_FAIL(ob_sub_str(allocator, src, 0, start_index - 1, dst_prefix))) {
      LOG_WARN("faild to sub dst prefix", K(ret), K(src), K(type));
    } else if (OB_FAIL(ob_sub_str(allocator, src, end_index + 1, dst_subfix))) {
      LOG_WARN("faild to sub dst subfix", K(ret), K(src), K(type));
    } else {
      ObString dst_parts[] = {dst_prefix, type, dst_subfix};
      if (OB_FAIL(ob_concat_string(allocator, dst, 3, dst_parts, true /*c_style*/))) {
        LOG_WARN("failed to create dst by parts", K(ret));
      } else {
        LOG_TRACE("concat type to collection type detail", K(ret), K(dst_prefix), K(type), K(dst_subfix));
      }
    }
  }
  return ret;
}

} // namespace hive
} // namespace sql
} // namespace oceanbase
