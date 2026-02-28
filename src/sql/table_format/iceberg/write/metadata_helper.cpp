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

#include "sql/table_format/iceberg/write/metadata_helper.h"

#include <avro/Generic.hh>

#include "sql/table_format/iceberg/avro_schema_util.h"
#include "sql/table_format/iceberg/ob_iceberg_utils.h"

namespace oceanbase
{
namespace sql
{
namespace iceberg
{

int V2Metadata::init_manifest_file_node()
{
  int ret = OB_SUCCESS;
  ToAvroNodeVisitor visitor(V2Metadata::MANIFEST_FILE);
  if (!manifest_file_node_) {
    if (OB_FAIL(visitor.visit(manifest_file_type_, manifest_file_node_))) {
      LOG_WARN("failed to visit manifest_file_type", K(ret));
    }
  }
  return ret;
}

int V2Metadata::init_manifest_entry_node()
{
  int ret = OB_SUCCESS;
  ToAvroNodeVisitor visitor(V2Metadata::MANIFEST_ENTRY);
  static const StructType partition_type = StructType({}); //todo: hardcode for now
  if (!manifest_entry_node_) {
    if (OB_FAIL(init_manifest_entry_type(partition_type))) {
      LOG_WARN("failed to init manifest_entry_type", K(ret));
    } else if (OB_FAIL(visitor.visit(*manifest_entry_type_, manifest_entry_node_))) {
      LOG_WARN("failed to visit manifest_entry_type", K(ret));
    }
  }
  return ret;
}

int V2Metadata::init_manifest_entry_type(const StructType& partition_type)
{
  int ret = OB_SUCCESS;
  SchemaField* data_file_field = nullptr;
  if (OB_ISNULL(manifest_entry_type_)) {
    if (OB_FAIL(init_data_file_type(partition_type))) {
      LOG_WARN("failed to init data_file_type", K(ret));
    } else {
      data_file_field = OB_NEWx(SchemaField, &allocator_, 2, ManifestEntry::DATA_FILE, data_file_type_);
      if (OB_ISNULL(data_file_field)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      manifest_entry_type_ = OB_NEWx(StructType, &allocator_,
                                    {&ManifestEntry::STATUS_FIELD,
                                     &ManifestEntry::SNAPSHOT_ID_FIELD,
                                     &ManifestEntry::SEQUENCE_NUMBER_FIELD,
                                     &ManifestEntry::FILE_SEQUENCE_NUMBER_FIELD,
                                     data_file_field});
      if (OB_ISNULL(manifest_entry_type_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      }
    }
  }
  return ret;
}

int V2Metadata::init_data_file_type(const StructType& partition_type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data_file_type_)) {
    SchemaField* partition_field = OB_NEWx(SchemaField, &allocator_,
                                           102, DataFile::PARTITION, &partition_type, false,
                                           "Partition data tuple, schema based on the partition spec");
    if (OB_ISNULL(partition_field)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else {
      data_file_type_ = OB_NEWx(StructType, &allocator_,
                                {&DataFile::REQUIRED_CONTENT_FIELD,
                                &DataFile::FILE_PATH_FIELD,
                                &DataFile::FILE_FORMAT_FIELD,
                                partition_field,
                                &DataFile::RECORD_COUNT_FIELD,
                                &DataFile::FILE_SIZE_FIELD,
                                &DataFile::COLUMN_SIZES_FIELD,
                                &DataFile::VALUE_COUNTS_FIELD,
                                &DataFile::NULL_VALUE_COUNTS_FIELD,
                                &DataFile::NAN_VALUE_COUNTS_FIELD,
                                &DataFile::LOWER_BOUNDS_FIELD,
                                &DataFile::UPPER_BOUNDS_FIELD,
                                &DataFile::KEY_METADATA_FIELD,
                                &DataFile::SPLIT_OFFSETS_FIELD,
                                &DataFile::EQUALITY_IDS_FIELD,
                                &DataFile::SORT_ORDER_ID_FIELD,
                                &DataFile::REFERENCED_DATA_FILE_FIELD});
      if (OB_ISNULL(data_file_type_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      }
    }
  }
  return ret;
}

int V2Metadata::convert_to_avro(const ManifestFile& manifest_file, avro::GenericDatum*& manifest_file_datum)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_manifest_file_node())) {
    LOG_WARN("failed to init manifest_file_node", K(ret));
  } else if (OB_ISNULL(manifest_file_datum = OB_NEWx(avro::GenericDatum, &allocator_, manifest_file_node_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else if (avro::Type::AVRO_RECORD != manifest_file_datum->type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("manifest_file_datum is not a record", K(ret));
  } else {
    avro::GenericRecord& manifest_file_record = manifest_file_datum->value<avro::GenericRecord>();
    manifest_file_record.setFieldAt(manifest_file_record.fieldIndex(ManifestFile::MANIFEST_PATH),
                                    avro::GenericDatum(std::string(manifest_file.manifest_path.ptr(), manifest_file.manifest_path.length())));
    manifest_file_record.setFieldAt(manifest_file_record.fieldIndex(ManifestFile::MANIFEST_LENGTH),
                                    avro::GenericDatum(manifest_file.manifest_length));
    manifest_file_record.setFieldAt(manifest_file_record.fieldIndex(ManifestFile::PARTITION_SPEC_ID),
                                    avro::GenericDatum(manifest_file.partition_spec_id));
    manifest_file_record.setFieldAt(manifest_file_record.fieldIndex(ManifestFile::CONTENT),
                                    avro::GenericDatum(static_cast<int32_t>(manifest_file.content)));
    manifest_file_record.setFieldAt(manifest_file_record.fieldIndex(ManifestFile::SEQUENCE_NUMBER),
                                    avro::GenericDatum(manifest_file.sequence_number));
    manifest_file_record.setFieldAt(manifest_file_record.fieldIndex(ManifestFile::MIN_SEQUENCE_NUMBER),
                                    avro::GenericDatum(manifest_file.min_sequence_number));
    manifest_file_record.setFieldAt(manifest_file_record.fieldIndex(ManifestFile::ADDED_SNAPSHOT_ID),
                                    avro::GenericDatum(manifest_file.added_snapshot_id));
    OZ(ObCatalogAvroUtils::convert_to_avro_primitive(manifest_file.added_files_count,
                                                     manifest_file_record.field(ManifestFile::ADDED_FILES_COUNT)));
    OZ(ObCatalogAvroUtils::convert_to_avro_primitive(manifest_file.existing_files_count,
                                                     manifest_file_record.field(ManifestFile::EXISTING_FILES_COUNT)));
    OZ(ObCatalogAvroUtils::convert_to_avro_primitive(manifest_file.deleted_files_count,
                                                     manifest_file_record.field(ManifestFile::DELETED_FILES_COUNT)));
    OZ(ObCatalogAvroUtils::convert_to_avro_primitive(manifest_file.added_rows_count,
                                                     manifest_file_record.field(ManifestFile::ADDED_ROWS_COUNT)));
    OZ(ObCatalogAvroUtils::convert_to_avro_primitive(manifest_file.existing_rows_count,
                                                     manifest_file_record.field(ManifestFile::EXISTING_ROWS_COUNT)));
    OZ(ObCatalogAvroUtils::convert_to_avro_primitive(manifest_file.deleted_rows_count,
                                                     manifest_file_record.field(ManifestFile::DELETED_ROWS_COUNT)));
    OZ(ObCatalogAvroUtils::convert_to_avro_primitive(manifest_file.key_metadata,
                                                     manifest_file_record.field(ManifestFile::KEY_METADATA)));
  }
  return ret;
}

int V2Metadata::convert_to_avro(const ManifestEntry& manifest_entry, avro::GenericDatum*& manifest_entry_datum)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_manifest_entry_node())) {
    LOG_WARN("failed to init manifest_entry_node", K(ret));
  } else if (OB_ISNULL(manifest_entry_datum = OB_NEWx(avro::GenericDatum, &allocator_, manifest_entry_node_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else if (avro::Type::AVRO_RECORD != manifest_entry_datum->type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("manifest_entry_datum is not a record", K(ret));
  } else {
    avro::GenericRecord& manifest_entry_record = manifest_entry_datum->value<avro::GenericRecord>();
    manifest_entry_record.setFieldAt(manifest_entry_record.fieldIndex(ManifestEntry::STATUS),
                                     avro::GenericDatum(static_cast<int32_t>(manifest_entry.status)));
    OZ(ObCatalogAvroUtils::convert_to_avro_primitive(manifest_entry.snapshot_id,
                                                      manifest_entry_record.field(ManifestEntry::SNAPSHOT_ID)));
    OZ(ObCatalogAvroUtils::convert_to_avro_primitive(manifest_entry.sequence_number,
                                                      manifest_entry_record.field(ManifestEntry::SEQUENCE_NUMBER)));
    OZ(ObCatalogAvroUtils::convert_to_avro_primitive(manifest_entry.file_sequence_number,
                                                      manifest_entry_record.field(ManifestEntry::FILE_SEQUENCE_NUMBER)));
    OZ(convert_to_avro(manifest_entry.data_file, manifest_entry_record.field(ManifestEntry::DATA_FILE)));
  }
  return ret;
}

int V2Metadata::convert_to_avro(const DataFile& data_file, avro::GenericDatum& data_file_datum)
{
  int ret = OB_SUCCESS;
  if (avro::Type::AVRO_RECORD != data_file_datum.type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data_file_datum is not a record", K(ret));
  } else {
    avro::GenericRecord& data_file_record = data_file_datum.value<avro::GenericRecord>();
    data_file_record.setFieldAt(data_file_record.fieldIndex(DataFile::CONTENT),
                                avro::GenericDatum(static_cast<int32_t>(data_file.content)));
    data_file_record.setFieldAt(data_file_record.fieldIndex(DataFile::FILE_PATH),
                                avro::GenericDatum(std::string(data_file.file_path.ptr(),
                                                               data_file.file_path.length())));
    data_file_record.setFieldAt(data_file_record.fieldIndex(DataFile::FILE_FORMAT),
                                avro::GenericDatum(std::string(
                                  DataFile::FILE_FORMAT_NAMES[static_cast<int32_t>(data_file.file_format)])));
    data_file_record.setFieldAt(data_file_record.fieldIndex(DataFile::RECORD_COUNT),
                                avro::GenericDatum(data_file.record_count));
    data_file_record.setFieldAt(data_file_record.fieldIndex(DataFile::FILE_SIZE_IN_BYTES),
                                avro::GenericDatum(data_file.file_size_in_bytes));

    OZ(ObCatalogAvroUtils::convert_to_avro_optional_map(data_file.column_sizes,
                                                        data_file_record.field(DataFile::COLUMN_SIZES)));
    OZ(ObCatalogAvroUtils::convert_to_avro_optional_map(data_file.value_counts,
                                                        data_file_record.field(DataFile::VALUE_COUNTS)));
    OZ(ObCatalogAvroUtils::convert_to_avro_optional_map(data_file.null_value_counts,
                                                        data_file_record.field(DataFile::NULL_VALUE_COUNTS)));
    OZ(ObCatalogAvroUtils::convert_to_avro_optional_map(data_file.nan_value_counts,
                                                        data_file_record.field(DataFile::NAN_VALUE_COUNTS)));
    OZ(ObCatalogAvroUtils::convert_to_avro_optional_map(data_file.lower_bounds,
                                                        data_file_record.field(DataFile::LOWER_BOUNDS)));
    OZ(ObCatalogAvroUtils::convert_to_avro_optional_map(data_file.upper_bounds,
                                                        data_file_record.field(DataFile::UPPER_BOUNDS)));
    OZ(ObCatalogAvroUtils::convert_to_avro_primitive(data_file.key_metadata,
                                                     data_file_record.field(DataFile::KEY_METADATA)));
    OZ(ObCatalogAvroUtils::convert_to_avro_list(data_file.split_offsets,
                                                data_file_record.field(DataFile::SPLIT_OFFSETS)));
    OZ(ObCatalogAvroUtils::convert_to_avro_list(data_file.equality_ids,
                                                data_file_record.field(DataFile::EQUALITY_IDS)));
    OZ(ObCatalogAvroUtils::convert_to_avro_primitive(data_file.sort_order_id,
                                                     data_file_record.field(DataFile::SORT_ORDER_ID)));
    OZ(ObCatalogAvroUtils::convert_to_avro_primitive(data_file.referenced_data_file,
                                                     data_file_record.field(DataFile::REFERENCED_DATA_FILE)));
  }
  return ret;
}

} // namespace iceberg
} // namespace sql
} // namespace oceanbase