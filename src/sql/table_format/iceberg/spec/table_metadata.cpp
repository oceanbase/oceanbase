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

#include "sql/table_format/iceberg/spec/table_metadata.h"

#include "share/ob_define.h"
#include "sql/table_format/iceberg/ob_iceberg_utils.h"

namespace oceanbase
{
namespace sql
{

namespace iceberg
{

TableMetadata::TableMetadata(ObIAllocator &allocator)
    : SpecWithAllocator(allocator),
      schemas(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator)),
      partition_specs(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator)),
      properties(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator)),
      snapshots(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator)),
      statistics(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator))
{
}

int TableMetadata::assign(const TableMetadata &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    format_version = other.format_version;
    last_sequence_number = other.last_sequence_number;
    last_updated_ms = other.last_updated_ms;
    last_column_id = other.last_column_id;
    current_schema_id = other.current_schema_id;
    default_spec_id = other.default_spec_id;
    last_partition_id = other.last_partition_id;
    current_snapshot_id = other.current_snapshot_id;

    OZ(ObIcebergUtils::deep_copy_optional_string(allocator_, other.table_uuid, table_uuid));
    OZ(ob_write_string(allocator_, other.location, location));
    OZ(ObIcebergUtils::deep_copy_array_object(allocator_, other.schemas, schemas));
    OZ(ObIcebergUtils::deep_copy_array_object(allocator_, other.partition_specs, partition_specs));
    OZ(ObIcebergUtils::deep_copy_map_string(allocator_, other.properties, properties));
    OZ(ObIcebergUtils::deep_copy_array_object(allocator_, other.snapshots, snapshots));
    OZ(ObIcebergUtils::deep_copy_array_object(allocator_, other.statistics, statistics));
  }
  return ret;
}

int TableMetadata::init_from_json(const ObJsonObject &json_object)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret)) {
    int32_t format_version_num = 0;
    if (OB_FAIL(
            ObCatalogJsonUtils::get_primitive(json_object, FORMAT_VERSION, format_version_num))) {
      LOG_WARN("failed to get format-version", K(ret));
    } else if (format_version_num < static_cast<int32_t>(FormatVersion::V1)
               || format_version_num > static_cast<int32_t>(FormatVersion::V2)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported iceberg version", K(ret), K(format_version_num));
    } else {
      format_version = static_cast<FormatVersion>(format_version_num);
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObCatalogJsonUtils::get_string(allocator_, json_object, TABLE_UUID, table_uuid))) {
      LOG_WARN("failed to get table-uuid", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObCatalogJsonUtils::get_string(allocator_, json_object, LOCATION, location))) {
      LOG_WARN("failed to get location", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (format_version > FormatVersion::V1) {
      if (OB_FAIL(ObCatalogJsonUtils::get_primitive(json_object,
                                                    LAST_SEQUENCE_NUMBER,
                                                    last_sequence_number))) {
        LOG_WARN("failed to get last-sequence-number", K(ret));
      }
    } else {
      last_sequence_number = INITIAL_SEQUENCE_NUMBER;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObCatalogJsonUtils::get_primitive(json_object, LAST_COLUMN_ID, last_column_id))) {
      LOG_WARN("failed to get last-column-id", K(ret));
    }
  }

  // handle schemas
  if (OB_SUCC(ret)) {
    if (OB_FAIL(parse_schemas_(json_object))) {
      LOG_WARN("failed to parse schemas", K(ret));
    }
  }

  // handle partition spec
  if (OB_SUCC(ret)) {
    std::optional<int32> tmp_last_partition_id;
    if (OB_FAIL(parse_partition_specs_(json_object))) {
      LOG_WARN("failed to get partition-specs", K(ret));
    } else if (OB_FAIL(ObCatalogJsonUtils::get_primitive(json_object,
                                                         LAST_PARTITION_ID,
                                                         tmp_last_partition_id))) {
      LOG_WARN("failed to get last-partition-id", K(ret));
    } else if (tmp_last_partition_id.has_value()) {
      last_partition_id = tmp_last_partition_id.value();
    } else {
      if (format_version != FormatVersion::V1) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("last-partition-id not existed", K(ret));
      } else {
        // 在 v1 表中，有一种情况，PartitionSpec 中的 PartitionField 是没有 field_id 的
        // 对于这种情况，我们需要遍历所有的 PartitionSpec 里面的 PartitionField，
        // 从 PARTITION_DATA_ID_START 开始递增赋值，以此推断出 last_partition_id
        for (int64_t i = 0; OB_SUCC(ret) && i < partition_specs.count(); i++) {
          last_partition_id
              = std::max(last_partition_id, partition_specs.at(i)->last_assigned_field_id);
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(parse_sort_order_(json_object))) {
      LOG_WARN("failed to parse sort-order", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ObJsonNode *json_properties = json_object.get_value(PROPERTIES);
    if (NULL == json_properties) {
      if (ObJsonNodeType::J_OBJECT != json_properties->json_type()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid json properties", K(ret));
      } else if (OB_FAIL(ObCatalogJsonUtils::convert_json_object_to_map(
                     allocator_,
                     *down_cast<ObJsonObject *>(json_properties),
                     properties))) {
        LOG_WARN("failed to parse properties", K(ret));
      }
    } else {
      properties.reset();
    }
  }

  if (OB_SUCC(ret)) {
    std::optional<int64_t> tmp_current_snapshot_id;
    if (OB_FAIL(ObCatalogJsonUtils::get_primitive(json_object,
                                                  CURRENT_SNAPSHOT_ID,
                                                  tmp_current_snapshot_id))) {
      LOG_WARN("failed to get current-snapshot-id", K(ret));
    } else {
      current_snapshot_id
          = tmp_current_snapshot_id.has_value() ? tmp_current_snapshot_id.value() : -1L;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObCatalogJsonUtils::get_primitive(json_object,
                                                  LAST_UPDATED_MS,
                                                  last_updated_ms))) {
      LOG_WARN("failed to get last-update-ms", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(parse_refs_(json_object))) {
      LOG_WARN("failed to parse refs", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(parse_snapshots_(json_object))) {
      LOG_WARN("failed to parse snapshots", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(parse_statistics_files_(json_object))) {
      LOG_WARN("failed to parse statistics", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(parse_partition_statistics_files_(json_object))) {
      LOG_WARN("failed to parse partition-statistics", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(parse_snapshot_log_(json_object))) {
      LOG_WARN("failed to parse snapshot-log", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(parse_metadata_log_(json_object))) {
      LOG_WARN("failed to parse metadata-log", K(ret));
    }
  }

  return ret;
}

int TableMetadata::get_current_snapshot(const Snapshot *&snapshot) const
{
  int ret = OB_SUCCESS;
  for (int32_t i = 0; OB_SUCC(ret) && snapshot == NULL && i < snapshots.count(); i++) {
    Snapshot *tmp_snapshot = snapshots.at(i);
    if (OB_ISNULL(tmp_snapshot)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (current_snapshot_id == tmp_snapshot->snapshot_id) {
      snapshot = tmp_snapshot;
    }
  }

  if (OB_ISNULL(snapshot)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("snapshot not existed", K(ret), K(current_snapshot_id));
  }
  return ret;
}

int TableMetadata::get_schema(int32_t schema_id, const Schema *&schema) const
{
  int ret = OB_SUCCESS;
  for (int32_t i = 0; OB_SUCC(ret) && schema == NULL && i < schemas.count(); i++) {
    Schema *tmp_schema = schemas.at(i);
    if (OB_ISNULL(tmp_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (schema_id == tmp_schema->schema_id) {
      schema = tmp_schema;
    }
  }
  if (OB_ISNULL(schema)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("schema not found", K(ret), K(schema_id));
  }
  return ret;
}

int TableMetadata::get_partition_spec(int32_t partition_spec_id,
                                      const PartitionSpec *&partition_spec) const
{
  int ret = OB_SUCCESS;
  for (int32_t i = 0; OB_SUCC(ret) && partition_spec == NULL && i < partition_specs.count(); i++) {
    PartitionSpec *tmp_partition_spec = partition_specs.at(i);
    if (OB_ISNULL(tmp_partition_spec)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (partition_spec_id == tmp_partition_spec->spec_id) {
      partition_spec = tmp_partition_spec;
    }
  }
  if (OB_ISNULL(partition_spec)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("partition spec not found", K(ret), K(partition_spec_id));
  }
  return ret;
}

int TableMetadata::get_table_property(const char *table_property_key, ObString &value) const
{
  int ret = OB_SUCCESS;
  value.reset();
  if (OB_ISNULL(table_property_key)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid key", K(ret), K(table_property_key));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && value.empty() && i < properties.count(); i++) {
      if (properties[i].first.case_compare_equal(table_property_key)) {
        value = properties[i].second;
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (value.empty()) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_DEBUG("specific table property key not found", K(ret), K(table_property_key));
    }
  }
  return ret;
}

int TableMetadata::get_table_default_write_format(DataFileFormat &data_file_format) const
{
  int ret = OB_SUCCESS;
  ObString value;
  if (OB_FAIL(get_table_property(WRITE_FORMAT_DEFAULT, value))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      data_file_format = DataFileFormat::PARQUET;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("get default.write.format failed", K(ret));
    }
  } else {
    if (value.case_compare_equal("PARQUET")) {
      data_file_format = DataFileFormat::PARQUET;
    } else if (value.case_compare_equal("ORC")) {
      data_file_format = DataFileFormat::ORC;
    } else if (value.case_compare_equal("AVRO")) {
      data_file_format = DataFileFormat::AVRO;
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid default.write.format", K(ret), K(value));
    }
  }
  return ret;
}

int TableMetadata::parse_schemas_(const ObJsonObject &json_object)
{
  int ret = OB_SUCCESS;
  const ObJsonNode *json_schemas = json_object.get_value(SCHEMAS);
  if (NULL != json_schemas) {
    // schemas existed, current_schema_id must exist
    if (OB_FAIL(
            ObCatalogJsonUtils::get_primitive(json_object, CURRENT_SCHEMA_ID, current_schema_id))) {
      LOG_WARN("failed to get current-schema-id", K(ret));
    } else if (ObJsonNodeType::J_ARRAY != json_schemas->json_type()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid schemas", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < json_schemas->element_count(); i++) {
        Schema *schema = NULL;
        ObIJsonBase *json_schema = NULL;
        if (OB_FAIL(json_schemas->get_array_element(i, json_schema))) {
          LOG_WARN("failed to parse schema", K(ret));
        } else if (ObJsonNodeType::J_OBJECT != json_schema->json_type()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid json schema", K(ret));
        } else if (OB_ISNULL(schema = OB_NEWx(Schema, &allocator_, allocator_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc", K(ret));
        } else if (OB_FAIL(schema->init_from_json(*static_cast<ObJsonObject*>(json_schema)))) {
          LOG_WARN("parse schema failed", K(ret));
        } else if (current_schema_id == schema->schema_id) {
          OZ(schemas.push_back(schema));
        }
      }
    }
  } else {
    const ObJsonNode *json_schema = json_object.get_value(SCHEMA);
    Schema *schema = NULL;
    if (FormatVersion::V1 != format_version) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("only v1 format is using schema", K(ret), K(format_version));
    } else if (OB_ISNULL(json_schema)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("failed to get schema", K(ret));
    } else if (ObJsonNodeType::J_OBJECT != json_schema->json_type()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid json schema", K(ret));
    } else if (OB_ISNULL(schema = OB_NEWx(Schema, &allocator_, allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc", K(ret));
    } else if (OB_FAIL(schema->init_from_json(*down_cast<const ObJsonObject *>(json_schema)))) {
      LOG_WARN("parse schema failed", K(ret));
    } else if (OB_FAIL(schemas.push_back(schema))) {
      LOG_WARN("failed to add schema", K(ret));
    } else {
      current_schema_id = schema->schema_id;
    }
  }
  return ret;
}

int TableMetadata::parse_partition_specs_(const ObJsonObject &json_object)
{
  int ret = OB_SUCCESS;
  const ObJsonNode *json_partition_specs = json_object.get_value(PARTITION_SPECS);
  if (NULL != json_partition_specs) {
    if (ObJsonNodeType::J_ARRAY != json_partition_specs->json_type()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid partition-specs", K(ret));
    } else if (OB_FAIL(ObCatalogJsonUtils::get_primitive(json_object,
                                                         DEFAULT_SPEC_ID,
                                                         default_spec_id))) {
      LOG_WARN("failed to get default-spec-id", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < json_partition_specs->element_count(); i++) {
        PartitionSpec *partition_spec = NULL;
        ObIJsonBase *json_partition_spec = NULL;
        if (OB_FAIL(json_partition_specs->get_array_element(i, json_partition_spec))) {
          LOG_WARN("failed to get partition-spec", K(ret));
        } else if (ObJsonNodeType::J_OBJECT != json_partition_spec->json_type()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid json partition-spec", K(ret));
        } else if (OB_ISNULL(partition_spec = OB_NEWx(PartitionSpec, &allocator_, allocator_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc", K(ret));
        } else if (OB_FAIL(partition_spec->init_from_json(
                       *down_cast<ObJsonObject *>(json_partition_spec)))) {
          LOG_WARN("failed to parse partition-spec", K(ret));
        } else {
          OZ(partition_specs.push_back(partition_spec));
        }
      }
    }
  } else {
    PartitionSpec *partition_spec = NULL;
    const ObJsonNode *json_partition_spec = json_object.get_value(PARTITION_SPEC);
    if (FormatVersion::V1 != format_version) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("only v1 format is using partition-spec", K(ret));
    } else if (OB_ISNULL(json_partition_spec)
               || ObJsonNodeType::J_ARRAY != json_partition_spec->json_type()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid partition-spec", K(ret));
    } else if (OB_ISNULL(partition_spec = OB_NEWx(PartitionSpec, &allocator_, allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc failed", K(ret));
    } else if (OB_FAIL(partition_spec->init_from_v1_json(
                   PARTITION_DATA_ID_START,
                   *down_cast<const ObJsonArray *>(json_partition_spec)))) {
      LOG_WARN("failed to init v1 partition-spec", K(ret));
    } else if (OB_FAIL(partition_specs.push_back(partition_spec))) {
      LOG_WARN("failed to add partition spec", K(ret));
    } else {
      default_spec_id = partition_spec->spec_id;
    }
  }
  return ret;
}

int TableMetadata::parse_sort_order_(const ObJsonObject &json_object)
{
  int ret = OB_SUCCESS;
  return ret;
}

int TableMetadata::parse_refs_(const ObJsonObject &json_object)
{
  int ret = OB_SUCCESS;
  return ret;
}

int TableMetadata::parse_snapshots_(const ObJsonObject &json_object)
{
  int ret = OB_SUCCESS;
  const ObJsonNode *json_snapshots_array = json_object.get_value(SNAPSHOTS);
  if (NULL == json_snapshots_array) {
    // means empty table
    snapshots.reset();
  } else {
    if (ObJsonNodeType::J_ARRAY != json_snapshots_array->json_type()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid json snapshots", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < json_snapshots_array->element_count(); i++) {
        ObIJsonBase *json_snapshot = NULL;
        Snapshot *snapshot = NULL;
        if (OB_FAIL(json_snapshots_array->get_array_element(i, json_snapshot))) {
          LOG_WARN("failed to get snapshot", K(ret));
        } else if (ObJsonNodeType::J_OBJECT != json_snapshot->json_type()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid json snapshot", K(ret));
        } else if (OB_ISNULL(snapshot = OB_NEWx(Snapshot, &allocator_, allocator_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc failed", K(ret));
        } else if (OB_FAIL(snapshot->init_from_json(*down_cast<ObJsonObject *>(json_snapshot)))) {
          LOG_WARN("failed to parse snapshot");
        } else {
          OZ(snapshots.push_back(snapshot));
        }
      }
    }
  }
  return ret;
}

int TableMetadata::parse_statistics_files_(const ObJsonObject &json_object)
{
  int ret = OB_SUCCESS;
  const ObJsonNode *json_statistics_array = json_object.get_value(STATISTICS);
  if (NULL == json_statistics_array) {
    statistics.reset();
  } else {
    if (ObJsonNodeType::J_ARRAY != json_statistics_array->json_type()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid json statistics array", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < json_statistics_array->element_count(); i++) {
        ObIJsonBase *json_statistics = NULL;
        StatisticsFile *statistics_file = NULL;
        if (OB_FAIL(json_statistics_array->get_array_element(i, json_statistics))) {
          LOG_WARN("failed to get statistics file", K(ret));
        } else if (ObJsonNodeType::J_OBJECT != json_statistics->json_type()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid json statistics", K(ret));
        } else if (OB_ISNULL(statistics_file = OB_NEWx(StatisticsFile, &allocator_, allocator_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc failed", K(ret));
        } else if (OB_FAIL(statistics_file->init_from_json(
                       *down_cast<ObJsonObject *>(json_statistics)))) {
          LOG_WARN("parse statistics failed", K(ret));
        } else {
          OZ(statistics.push_back(statistics_file));
        }
      }
    }
  }
  return ret;
}

int TableMetadata::parse_partition_statistics_files_(const ObJsonObject &json_object)
{
  int ret = OB_SUCCESS;
  return ret;
}

int TableMetadata::parse_snapshot_log_(const ObJsonObject &json_object)
{
  int ret = OB_SUCCESS;
  return ret;
}

int TableMetadata::parse_metadata_log_(const ObJsonObject &json_object)
{
  int ret = OB_SUCCESS;
  return ret;
}

} // namespace iceberg

} // namespace sql
} // namespace oceanbase
