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
#include "share/schema/ob_schema_printer.h"

#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/ob_sql_context.h"
#include "sql/parser/ob_parser.h"
#include "pl/ob_pl_resolver.h"
#include "share/schema/ob_mview_info.h"
#include "share/ob_fts_index_builder_util.h"
#include "storage/fts/ob_fts_parser_property.h"
#include "storage/fts/ob_fts_plugin_helper.h"
#include "share/ob_dynamic_partition_manager.h"
#include "sql/resolver/ddl/ob_storage_cache_ddl_util.h"
#include "plugin/interface/ob_plugin_external_intf.h"
#include "share/external_table/ob_external_table_utils.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
using namespace std;
using namespace common;

int ObSchemaPrinter::print_external_table_file_info(const ObTableSchema &table_schema,
                                                    ObIAllocator& allocator,
                                                    char* buf,
                                                    const int64_t& buf_len,
                                                    int64_t& pos) const
{
  int ret = OB_SUCCESS;
  // 1. print file location, pattern
  ObString location;
  uint64_t location_id = table_schema.get_external_location_id();
  const ObString &pattern = table_schema.get_external_file_pattern();
  const ObString &format_string = table_schema.get_external_file_format();
  const ObString &properties_string = table_schema.get_external_properties();
  const ObString &sub_path = table_schema.get_external_sub_path();
  const bool user_specified = table_schema.is_user_specified_partition_for_external_table();
  bool use_properties = false;
  ObExternalFileFormat::FormatType external_table_type = ObExternalFileFormat::INVALID_FORMAT;
  ObString location_name;
  const ObLocationSchema *location_schema = NULL;
  if (OB_INVALID_ID != location_id) {
    if(OB_FAIL(schema_guard_.get_location_schema_by_id(table_schema.get_tenant_id(), location_id, location_schema))) {
      LOG_WARN("failed to get location schema", K(ret));
    } else if (OB_ISNULL(location_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("location schema is null", K(ret));
    } else {
      location_name = location_schema->get_location_name();
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(ObExternalTableUtils::get_external_file_location(table_schema, schema_guard_, allocator, location))) {
    LOG_WARN("failed to get external file location", K(ret));
  } else if (OB_FAIL(ObSQLUtils::get_external_table_type(&table_schema, external_table_type))) {
    LOG_WARN("failed to check is odps table or not", K(ret));
  } else if (ObExternalFileFormat::ODPS_FORMAT != external_table_type &&
      ObExternalFileFormat::PLUGIN_FORMAT != external_table_type) {
    use_properties = false;
  } else {
    use_properties = true;
  }
  if (OB_SUCC(ret) && !use_properties) {
    if (!location_name.empty()) {
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\nLOCATION=@%.*s", location_name.length(), location_name.ptr()))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print LOCATION OBJ", K(ret));
      } else if (!sub_path.empty() && OB_FAIL(databuff_printf(buf, buf_len, pos, "'%.*s'", sub_path.length(), sub_path.ptr()))){
        SHARE_SCHEMA_LOG(WARN, "fail to print SUB_PATH", K(ret));
      }
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\nLOCATION='%.*s'", location.length(), location.ptr()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print LOCATION", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (!use_properties && !pattern.empty() &&
      OB_FAIL(databuff_printf(buf, buf_len, pos, "\nPATTERN='%.*s'", pattern.length(), pattern.ptr()))) {
    SHARE_SCHEMA_LOG(WARN, "fail to print PATTERN", K(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\nAUTO_REFRESH = %s", table_schema.get_external_table_auto_refresh() == 0 ? "OFF" :
                                                                                table_schema.get_external_table_auto_refresh() == 1 ? "IMMEDIATE" : "INTERVAL"))) {
    SHARE_SCHEMA_LOG(WARN, "fail to print AUTO REFRESH", K(ret));
  } else if (user_specified) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\nPARTITION_TYPE=USER_SPECIFIED"))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print PATTERN", K(ret));
    }
  }
  uint64_t tenant_id = table_schema.get_tenant_id();
  uint64_t data_version = 0;
  if (table_schema.is_sys_table() || table_schema.is_vir_table()) {
    // skip for sys/vir table
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("failed to get data version", K(ret), K(tenant_id));
  }
   // 2. print file format
  if (OB_SUCC(ret)) {
    ObExternalFileFormat format;
    const ObString &table_format_or_properties = use_properties ? properties_string : format_string;
    if (table_format_or_properties.empty()) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(WARN, "table_format_or_properties is empty", K(ret));
    } else if (OB_FAIL(format.load_from_string(table_format_or_properties, allocator))) {
      SHARE_SCHEMA_LOG(WARN, "fail to load from json string", K(ret));
    } else if (!(format.format_type_ > ObExternalFileFormat::INVALID_FORMAT
                 && format.format_type_ < ObExternalFileFormat::MAX_FORMAT)) {
      ret = OB_NOT_SUPPORTED;
      SHARE_SCHEMA_LOG(WARN, "unsupported to print file format", K(ret), K(format.format_type_));
    } else if (!use_properties && OB_FAIL(databuff_printf(buf, buf_len, pos, "\nFORMAT (\n"))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print FORMAT (", K(ret));
    } else if (use_properties && OB_FAIL(databuff_printf(buf, buf_len, pos, "\nPROPERTIES (\n"))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print FORMAT (", K(ret));
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "  TYPE = '%s',", ObExternalFileFormat::FORMAT_TYPE_STR[format.format_type_]))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print TYPE", K(ret));
    }
    if (OB_SUCC(ret) && ObExternalFileFormat::CSV_FORMAT == format.format_type_) {
      const ObCSVGeneralFormat &csv = format.csv_format_;
      const ObOriginFileFormat &origin_format = format.origin_file_format_str_;
      const char *compression_name = compression_algorithm_to_string(csv.compression_algorithm_);
      const char *binary_format = binary_format_to_string(csv.binary_format_);
      if (OB_FAIL(0 != csv.line_term_str_.case_compare(ObDataInFileStruct::DEFAULT_LINE_TERM_STR) &&
                        databuff_printf(buf, buf_len, pos, "\n  LINE_DELIMITER = %.*s,", origin_format.origin_line_term_str_.length(), origin_format.origin_line_term_str_.ptr()))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print LINE_DELIMITER", K(ret));
      } else if (OB_FAIL(0 != csv.field_term_str_.case_compare(ObDataInFileStruct::DEFAULT_FIELD_TERM_STR) &&
                        databuff_printf(buf, buf_len, pos, "\n  FIELD_DELIMITER = %.*s,", origin_format.origin_field_term_str_.length(), origin_format.origin_field_term_str_.ptr()))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print FIELD_DELIMITER", K(ret));
      } else if (OB_FAIL(ObDataInFileStruct::DEFAULT_FIELD_ESCAPED_CHAR != csv.field_escaped_char_ &&
                        databuff_printf(buf, buf_len, pos, "\n  ESCAPE = %.*s,", origin_format.origin_field_escaped_str_.length(), origin_format.origin_field_escaped_str_.ptr()))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print ESCAPE", K(ret));
      } else if (OB_FAIL(ObDataInFileStruct::DEFAULT_FIELD_ENCLOSED_CHAR != csv.field_enclosed_char_ &&
                        databuff_printf(buf, buf_len, pos, "\n  FIELD_OPTIONALLY_ENCLOSED_BY = %.*s,", origin_format.origin_field_enclosed_str_.length(), origin_format.origin_field_enclosed_str_.ptr()))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print FIELD_OPTIONALLY_ENCLOSED_BY", K(ret));
      } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\n  ENCODING = '%s',", ObCharset::charset_name(csv.cs_type_)))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print ENCODING", K(ret));
      } else if (OB_FAIL(0 != csv.skip_header_lines_ &&
                        databuff_printf(buf, buf_len, pos, "\n  SKIP_HEADER = %ld,", csv.skip_header_lines_))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print SKIP_HEADER", K(ret));
      } else if (OB_FAIL(csv.skip_blank_lines_ &&
                        databuff_printf(buf, buf_len, pos, "\n  SKIP_BLANK_LINES = TRUE,"))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print SKIP_BLANK_LINES", K(ret));
      } else if (OB_FAIL(csv.trim_space_ &&
                        databuff_printf(buf, buf_len, pos, "\n  TRIM_SPACE = TRUE,"))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print TRIM_SPACE", K(ret));
      } else if (OB_FAIL(csv.empty_field_as_null_ &&
                        databuff_printf(buf, buf_len, pos, "\n  EMPTY_FIELD_AS_NULL = TRUE,"))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print EMPTY_FIELD_AS_NULL", K(ret));
      } else if (OB_FAIL(0 != csv.null_if_.count() &&
                        databuff_printf(buf, buf_len, pos, "\n  NULL_IF = (%.*s),", origin_format.origin_null_if_str_.length(), origin_format.origin_null_if_str_.ptr()))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print NULL_IF", K(ret));
      } else if (ObCSVGeneralFormat::ObCSVCompression::NONE != csv.compression_algorithm_ &&
                 OB_FAIL(databuff_printf(buf, buf_len, pos, "\n  COMPRESSION = %.*s,",
                                         static_cast<int>(STRLEN(compression_name)), compression_name))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print compression", K(ret));
      } else if (OB_FAIL(csv.parse_header_ &&
                        databuff_printf(buf, buf_len, pos, "\n  PARSE_HEADER = TRUE,"))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print PARSE_HEADER", K(ret));
      } else if (ObCSVGeneralFormat::ObCSVBinaryFormat::DEFAULT != csv.binary_format_ &&
        OB_FAIL(databuff_printf(buf, buf_len, pos, "\n  BINARY_FORMAT = %.*s,",
                                         static_cast<int>(STRLEN(binary_format)), binary_format))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print binary format", K(ret));
      } else if (OB_FAIL(!csv.ignore_last_empty_col_ &&
                        databuff_printf(buf, buf_len, pos, "\n  IGNORE_LAST_EMPTY_COLUMN = FALSE,"))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print IGNORE_LAST_EMPTY_COLUMN", K(ret));
      }
    } else if (OB_SUCC(ret) && ObExternalFileFormat::ODPS_FORMAT == format.format_type_) {
      const ObODPSGeneralFormat &odps = format.odps_format_;
      ObString scret_str("********");
      int64_t option_names_idx = 0;
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\n  %s = '%.*s',", ObODPSGeneralFormat::OPTION_NAMES[option_names_idx++], odps.access_type_.length(), odps.access_type_.ptr()))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print ODPS_INFO", K(ret));
      } else if (!odps.access_id_.empty() && OB_FAIL(databuff_printf(buf, buf_len, pos, "\n  %s = '%.*s',", ObODPSGeneralFormat::OPTION_NAMES[option_names_idx++], scret_str.length(), scret_str.ptr()))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print ODPS_INFO", K(ret));
      } else if (!odps.access_key_.empty() && OB_FAIL(databuff_printf(buf, buf_len, pos, "\n  %s = '%.*s',", ObODPSGeneralFormat::OPTION_NAMES[option_names_idx++], scret_str.length(), scret_str.ptr()))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print ODPS_INFO", K(ret));
      } else if (!odps.sts_token_.empty() && OB_FAIL(databuff_printf(buf, buf_len, pos, "\n  %s = '%.*s',", ObODPSGeneralFormat::OPTION_NAMES[option_names_idx++], scret_str.length(), scret_str.ptr()))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print ODPS_INFO", K(ret));
      } else if (odps.sts_token_.empty()) {
        option_names_idx++;
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\n  %s = '%.*s',", ObODPSGeneralFormat::OPTION_NAMES[option_names_idx++], odps.endpoint_.length(), odps.endpoint_.ptr()))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print ODPS_INFO", K(ret));
      } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\n  %s = '%.*s',", ObODPSGeneralFormat::OPTION_NAMES[option_names_idx++], odps.tunnel_endpoint_.length(), odps.tunnel_endpoint_.ptr()))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print ODPS_INFO", K(ret));
      } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\n  %s = '%.*s',", ObODPSGeneralFormat::OPTION_NAMES[option_names_idx++], odps.project_.length(), odps.project_.ptr()))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print ODPS_INFO", K(ret));
      } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\n  %s = '%.*s',", ObODPSGeneralFormat::OPTION_NAMES[option_names_idx++], odps.schema_.length(), odps.schema_.ptr()))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print ODPS_INFO", K(ret));
      } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\n  %s = '%.*s',", ObODPSGeneralFormat::OPTION_NAMES[option_names_idx++], odps.table_.length(), odps.table_.ptr()))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print ODPS_INFO", K(ret));
      } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\n  %s = '%.*s',", ObODPSGeneralFormat::OPTION_NAMES[option_names_idx++], odps.quota_.length(), odps.quota_.ptr()))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print ODPS_INFO", K(ret));
      } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\n  %s = '%.*s',", ObODPSGeneralFormat::OPTION_NAMES[option_names_idx++], odps.compression_code_.length(), odps.compression_code_.ptr()))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print ODPS_INFO", K(ret));
      } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\n  %s = %s,", ObODPSGeneralFormat::OPTION_NAMES[option_names_idx++], odps.collect_statistics_on_create_ ? "TRUE" : "FALSE"))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print ODPS_INFO", K(ret));
      } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\n  %s = '%.*s',", ObODPSGeneralFormat::OPTION_NAMES[option_names_idx++], odps.region_.length(), odps.region_.ptr()))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print ODPS_INFO", K(ret));
      }

      if (OB_SUCC(ret)) {
        ObString str;
        if (odps.api_mode_ == ObODPSGeneralFormat::ApiMode::TUNNEL_API) {
          str = ObODPSGeneralFormatParam::TUNNEL_API;
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\n  %s = '%.*s',", ObODPSGeneralFormat::OPTION_NAMES[option_names_idx++], str.length(), str.ptr()))) {
            SHARE_SCHEMA_LOG(WARN, "fail to print ODPS_INFO", K(ret));
          }
          option_names_idx++;
        } else if (odps.api_mode_ == ObODPSGeneralFormat::ApiMode::BYTE) {
          str = ObODPSGeneralFormatParam::STORAGE_API;
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\n  %s = '%.*s',", ObODPSGeneralFormat::OPTION_NAMES[option_names_idx++], str.length(), str.ptr()))) {
            SHARE_SCHEMA_LOG(WARN, "fail to print ODPS_INFO", K(ret));
          }
          str = ObODPSGeneralFormatParam::BYTE;
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\n  %s = '%.*s',", ObODPSGeneralFormat::OPTION_NAMES[option_names_idx++], str.length(), str.ptr()))) {
            SHARE_SCHEMA_LOG(WARN, "fail to print ODPS_INFO", K(ret));
          }
        } else if (odps.api_mode_ == ObODPSGeneralFormat::ApiMode::ROW) {
          str = ObODPSGeneralFormatParam::STORAGE_API;
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\n  %s = '%.*s',", ObODPSGeneralFormat::OPTION_NAMES[option_names_idx++], str.length(), str.ptr()))) {
            SHARE_SCHEMA_LOG(WARN, "fail to print ODPS_INFO", K(ret));
          }
          str = ObODPSGeneralFormatParam::ROW;
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\n  %s = '%.*s',", ObODPSGeneralFormat::OPTION_NAMES[option_names_idx++], str.length(), str.ptr()))) {
            SHARE_SCHEMA_LOG(WARN, "fail to print ODPS_INFO", K(ret));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to get right api mode", K(ret));
        }
      }
    } else if (OB_SUCC(ret) && ObExternalFileFormat::PLUGIN_FORMAT == format.format_type_) {
      ObString plugin_unavailable("<plugin is not available>");
      ObString parameters;
      plugin::ObExternalDataEngine *engine = nullptr;
      if (OB_FAIL(format.plugin_format_.create_engine(allocator, engine))) {
        LOG_WARN("failed to create engine", K(ret));
        if (OB_FUNCTION_NOT_DEFINED == ret) {
          ret = OB_SUCCESS;
          parameters = plugin_unavailable;
        }
      } else if (OB_FAIL(engine->display(allocator, parameters))) {
        LOG_WARN("failed to get data engine display string", K(ret));
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\n  NAME = '%.*s',",
                                 format.plugin_format_.type_name().length(), format.plugin_format_.type_name().ptr()))) {
        SHARE_SCHEMA_LOG(WARN, "failed to print plugin name", K(ret));
      } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\n  PARAMETERS = '%.*s' ",
                                         parameters.length(), parameters.ptr()))) {
        SHARE_SCHEMA_LOG(WARN, "failed to print parameters", K(ret));
      }
      if (OB_NOT_NULL(engine)) {
        OB_DELETEx(ObExternalDataEngine, &allocator, engine);
        engine = nullptr;
      }
      if (OB_NOT_NULL(parameters.ptr()) && parameters.ptr() != plugin_unavailable.ptr()) {
        allocator.free(parameters.ptr());
        parameters.reset();
      }
    } else if (OB_SUCC(ret) && ObExternalFileFormat::ORC_FORMAT == format.format_type_) {
      const ObOrcGeneralFormat &orc = format.orc_format_;
      const char *column_index_type = column_index_type_to_string(orc.column_index_type_);
      if (sql::ColumnIndexType::NAME != orc.column_index_type_ &&
        OB_FAIL(databuff_printf(buf, buf_len, pos, "\n  COLUMN_INDEX_TYPE = '%.*s',",
                                static_cast<int>(STRLEN(column_index_type)), column_index_type))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print column index type", K(ret));
      } else if (orc.column_name_case_sensitive_
                 && OB_FAIL(databuff_printf(buf,
                                            buf_len,
                                            pos,
                                            "\n  COLUMN_NAME_CASE_SENSITIVE = %s,",
                                            orc.column_name_case_sensitive_ ? "TRUE" : "FALSE"))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print column name case sensitive", K(ret));
      }
    } else if (OB_SUCC(ret) && ObExternalFileFormat::PARQUET_FORMAT == format.format_type_) {
      const ObParquetGeneralFormat &parquet = format.parquet_format_;
      const char *column_index_type = column_index_type_to_string(parquet.column_index_type_);
      if (sql::ColumnIndexType::NAME != parquet.column_index_type_ &&
        OB_FAIL(databuff_printf(buf, buf_len, pos, "\n  COLUMN_INDEX_TYPE = '%.*s',",
                                static_cast<int>(STRLEN(column_index_type)), column_index_type))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print column index type", K(ret));
      } else if (parquet.column_name_case_sensitive_
                 && OB_FAIL(
                     databuff_printf(buf,
                                     buf_len,
                                     pos,
                                     "\n  COLUMN_NAME_CASE_SENSITIVE = %s,",
                                     parquet.column_name_case_sensitive_ ? "TRUE" : "FALSE"))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print column name case sensitive", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      --pos;
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\n) "))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print )", K(ret));
      }
    }
  }

  // add USING ICEBERG/HIVE
  if (OB_SUCC(ret)) {
    switch (table_schema.get_lake_table_format()) {
      case ObLakeTableFormat::ICEBERG: {
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\nUSING ICEBERG\n"))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print using iceberg", K(ret));
        }
        break;
      }
      case ObLakeTableFormat::HIVE: {
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\nUSING HIVE\n"))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print using hive", K(ret));
        }
        break;
      }
      default: {
        // 以前的文件外部表不会被设置 LakeTableFormat
        // odps 暂时也不考虑
        // do nothing
      }
    }
  }
  return ret;
}

} // namespace schema
} // namespace share
} // namespace oceanbase
