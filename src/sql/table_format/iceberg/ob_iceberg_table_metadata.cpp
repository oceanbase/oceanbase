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
#include "ob_iceberg_table_metadata.h"

#include "share/schema/ob_iceberg_table_schema.h"
#include "sql/table_format/iceberg/ob_iceberg_table_metadata.h"
#include "sql/table_format/iceberg/ob_iceberg_utils.h"
#include "sql/table_format/iceberg/scan/task.h"
#include "sql/table_format/iceberg/spec/manifest.h"
#include "sql/table_format/iceberg/spec/manifest_list.h"

namespace oceanbase
{
namespace sql
{
namespace iceberg
{

share::ObLakeTableFormat ObIcebergTableMetadata::get_format_type() const
{
  return share::ObLakeTableFormat::ICEBERG;
}

int ObIcebergTableMetadata::assign(const ObIcebergTableMetadata &other)
{
  // todo
  return OB_NOT_SUPPORTED;
}

int64_t ObIcebergTableMetadata::get_convert_size() const
{
  return -1;
}

int ObIcebergTableMetadata::set_access_info(const ObString &access_info)
{
  int ret = OB_SUCCESS;
  OZ(ob_write_string(allocator_, access_info, this->access_info_, true));
  return ret;
}

int ObIcebergTableMetadata::load_by_table_location(const ObString &table_location)
{
  int ret = OB_SUCCESS;
  ObSqlString temp_sql_string;
  char *version_hint_buf = NULL;
  int64_t version_hint_buf_len = 0;
  int64_t version_number = -1;
  if (OB_FAIL(temp_sql_string.append_fmt("%.*s/metadata/version-hint.text",
                                         table_location.length(),
                                         table_location.ptr()))) {
    LOG_WARN("build path failed", K(ret));
  } else if (OB_FAIL(iceberg::ObIcebergFileIOUtils::read(allocator_,
                                                         temp_sql_string.string(),
                                                         access_info_,
                                                         version_hint_buf,
                                                         version_hint_buf_len,
                                                         false))) {
    LOG_WARN("read version-hint.text failed", K(ret), K(temp_sql_string.string()));
  } else {
    // 如果末尾有 \n，移除它
    if ('\n' == version_hint_buf[version_hint_buf_len - 1]) {
      version_hint_buf_len -= 1;
    }
    ObString version_number_str = ObString(version_hint_buf_len, version_hint_buf);
    int32_t err_number = 0;
    if (!version_number_str.is_numeric()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid version number", K(ret), K(version_number_str));
    } else if (OB_FALSE_IT(version_number = ObCharset::strntoll(version_number_str.ptr(),
                                                                version_number_str.length(),
                                                                10,
                                                                &err_number))) {
    } else if (0 != err_number) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid version number", K(ret), K(version_number_str));
    }
  }

  bool metadata_exists = false;
  ObString existed_metadata_path;

  // try three types: v1.metadata.json / v1.metadata.json.gz / v1.gz.metadata.json
  const char *try_metadata_paths[] = {"%.*s/metadata/v%d.metadata.json",
                                      "%.*s/metadata/v%d.gz.metadata.json",
                                      "%.*s/metadata/v%d.metadata.json.gz"};
  for (int64_t i = 0; OB_SUCC(ret) && !metadata_exists
                      && i < sizeof(try_metadata_paths) / sizeof(try_metadata_paths[0]);
       i++) {
    temp_sql_string.reuse();
    if (OB_FAIL(temp_sql_string.append_fmt(try_metadata_paths[i],
                                           table_location.length(),
                                           table_location.ptr(),
                                           version_number))) {
      LOG_WARN("append path failed", K(ret));
    } else if (OB_FAIL(iceberg::ObIcebergFileIOUtils::is_exist(temp_sql_string.string(),
                                                               access_info_,
                                                               metadata_exists))) {
      LOG_WARN("check existed failed", K(ret));
    } else if (!metadata_exists) {
      LOG_DEBUG("iceberg metadata file not found", K(ret), K(temp_sql_string.string()));
    } else {
      existed_metadata_path = temp_sql_string.string();
      metadata_exists = true;
    }
  }

  if (OB_SUCC(ret) && !metadata_exists) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid iceberg table, can't find metadata.json", K(ret), K(table_location));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(load_by_metadata_location(existed_metadata_path))) {
      LOG_WARN("failed to load table metadata.json", K(ret), K(existed_metadata_path));
    }
  }
  return ret;
}

int ObIcebergTableMetadata::load_by_metadata_location(const ObString &metadata_location)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t read_size = 0;
  ObArenaAllocator tmp_allocator;
  ObJsonNode *json_node = NULL;
  if (OB_FAIL(ObIcebergFileIOUtils::read_table_metadata(tmp_allocator,
                                                        metadata_location,
                                                        access_info_,
                                                        buf,
                                                        read_size))) {
    LOG_WARN("read table metadata failed", K(ret), K(metadata_location));
  } else if (OB_ISNULL(buf) || 0 == read_size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to read table metadata file", K(ret));
  } else if (OB_FAIL(ObJsonParser::get_tree(&tmp_allocator, ObString(read_size, buf), json_node))) {
    LOG_WARN("failed to parse table metadata file", K(ret));
  } else if (OB_ISNULL(json_node) || ObJsonNodeType::J_OBJECT != json_node->json_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid json value", K(ret));
  } else if (OB_FAIL(table_metadata_.init_from_json(*down_cast<ObJsonObject *>(json_node)))) {
    LOG_WARN("fail to init TableMetadata", K(ret));
  } else {
    lake_table_metadata_version_ = table_metadata_.current_snapshot_id;
  }
  return ret;
}

int ObIcebergTableMetadata::do_build_table_schema(share::schema::ObTableSchema *&table_schema)
{
  int ret = OB_SUCCESS;
  const Schema *current_schema = NULL;
  schema::ObIcebergTableSchema *iceberg_table_schema = NULL;
  if (OB_ISNULL(iceberg_table_schema
                = OB_NEWx(schema::ObIcebergTableSchema, &allocator_, &allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else if (OB_FAIL(
                 table_metadata_.get_schema(table_metadata_.current_schema_id, current_schema))) {
    LOG_WARN("get current schema failed", K(ret));
  } else {
    // 这些 id 必须要设置，不然后面的 add column 操作会失败
    iceberg_table_schema->set_tenant_id(tenant_id_);
    iceberg_table_schema->set_database_id(database_id_);
    iceberg_table_schema->set_table_id(table_id_);
    iceberg_table_schema->set_table_name(table_name_);
    iceberg_table_schema->set_schema_version(table_metadata_.current_schema_id);
    iceberg_table_schema->set_lake_table_format(share::ObLakeTableFormat::ICEBERG);

    ObExternalFileFormat format;
    DataFileFormat default_write_format = DataFileFormat::INVALID;
    if (OB_FAIL(table_metadata_.get_table_default_write_format(default_write_format))) {
      LOG_WARN("get default.write.format failed", K(ret));
    } else {
      switch (default_write_format) {
        case DataFileFormat::PARQUET:
          format.format_type_ = ObExternalFileFormat::FormatType::PARQUET_FORMAT;
          break;
        case DataFileFormat::ORC:
          format.format_type_ = ObExternalFileFormat::FormatType::ORC_FORMAT;
          break;
        default:
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("unsupported data file type", K(ret), K(default_write_format));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "Unsupported data file type");
          break;
      }
    }

    ObString format_str;
    ObArenaAllocator tmp_allocator;
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(format.to_string_with_alloc(format_str, tmp_allocator))) {
      LOG_WARN("string alloc failed", K(ret));
    } else if (OB_FAIL(iceberg_table_schema->set_external_file_format(format_str))) {
      LOG_WARN("failed to set external file format", K(ret));
    } else if (OB_FAIL(
                   iceberg_table_schema->set_external_file_location(table_metadata_.location))) {
      LOG_WARN("failed to set external file location", K(ret));
    } else if (OB_FAIL(iceberg_table_schema->set_external_file_location_access_info(access_info_))) {
      LOG_WARN("failed to set external file location access info", K(ret));
    } else {
      FOREACH_CNT_X(it, current_schema->fields, OB_SUCC(ret))
      {
        schema::ObColumnSchemaV2 column_schema;
        if (OB_FAIL(column_schema.assign(**it))) {
          LOG_WARN("assign column schema failed", K(ret));
        } else if (OB_FALSE_IT(column_schema.set_table_id(table_id_))) {
        } else if (OB_FAIL(iceberg_table_schema->add_column(column_schema))) {
          LOG_WARN("add column failed", K(ret));
        }
      }

      const PartitionSpec *spec = NULL;
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(table_metadata_.get_partition_spec(table_metadata_.default_spec_id, spec))) {
        LOG_WARN("get partition spec failed", K(ret), K(table_metadata_.default_spec_id));
      } else if (OB_NOT_NULL(spec)) {
        ObSqlString part_expr;
        for (int64_t i = 0; i < spec->fields.count() && OB_SUCC(ret); ++i) {
          const PartitionField *field = spec->fields.at(i);
          if (!part_expr.empty()) {
            OZ (part_expr.append(", "));
          }
          ObString tmp_str;
          const share::schema::ObColumnSchemaV2 *col_schema = NULL;
          if (OB_FAIL(ret)) {
            // do nothing
          } else if (OB_FAIL(current_schema->get_column_schema_by_field_id(field->source_id, col_schema))) {
            LOG_WARN("get col schema failed", K(ret), K(field->source_id));
          } else if (OB_FAIL(Transform::get_part_expr(tmp_allocator,
                                                      field->transform,
                                                      col_schema->get_column_name_str(),
                                                      tmp_str))) {
            LOG_WARN("get transform str failed", K(ret));
          } else if (OB_FAIL(part_expr.append(tmp_str))) {
            LOG_WARN("append str failed", K(ret), K(tmp_str));
          }
        }
        if (OB_SUCC(ret) && !part_expr.empty()) {
          share::schema::ObPartitionOption &part_opt = iceberg_table_schema->get_part_option();
          OZ (part_opt.set_part_expr(part_expr.string()));
          part_opt.set_part_func_type(share::schema::PARTITION_FUNC_TYPE_LIST_COLUMNS);
          iceberg_table_schema->set_part_level(share::schema::PARTITION_LEVEL_ONE);
        }
      }
    }

    if (OB_SUCC(ret)) {
      table_schema = iceberg_table_schema;
    }
  }
  return ret;
}

} // namespace iceberg
} // namespace sql
} // namespace oceanbase