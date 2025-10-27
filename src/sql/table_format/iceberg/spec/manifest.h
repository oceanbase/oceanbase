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

#ifndef MANIFEST_H
#define MANIFEST_H

#include "sql/table_format/iceberg/ob_iceberg_type_fwd.h"
#include "sql/table_format/iceberg/ob_iceberg_utils.h"
#include "sql/table_format/iceberg/spec/manifest_list.h"
#include "sql/table_format/iceberg/spec/partition.h"
#include "sql/table_format/iceberg/spec/schema.h"
#include "sql/table_format/iceberg/spec/spec.h"

namespace oceanbase
{
namespace sql
{
namespace iceberg
{

class ManifestMetadata : public SpecWithAllocator
{
public:
  explicit ManifestMetadata(ObIAllocator &allocator);
  int init_from_metadata(const std::map<std::string, std::vector<uint8_t>> &metadata);

  Schema schema;
  int32_t schema_id;
  PartitionSpec partition_spec;
  int32_t partition_spec_id;
  FormatVersion format_version;
  ManifestContent content;

  // fields for avro's header metadata
  static constexpr const char *SCHEMA = "schema";
  static constexpr const char *SCHEMA_ID = "schema-id";
  static constexpr const char *PARTITION_SPEC = "partition-spec";
  static constexpr const char *PARTITION_SPEC_ID = "partition-spec-id";
  static constexpr const char *FORMAT_VERSION = "format-version";
  static constexpr const char *CONTENT = "content";

private:
  int init_schema_from_metadata(const ObString &metadata, Schema &schema);
  int init_partition_fields_from_metadata(const ObString &metadata, PartitionSpec &partition_spec);
};

enum class DataFileContent
{
  DATA = 0,
  POSITION_DELETES = 1,
  EQUALITY_DELETES = 2,
};

class ObSerializableDataFile
{
  OB_UNIS_VERSION(1);
public:
  ObSerializableDataFile() : content_(DataFileContent::DATA),
                             file_format_(DataFileFormat::PARQUET),
                             record_count_(0),
                             file_size_in_bytes_(0) {}
  ~ObSerializableDataFile() = default;
  TO_STRING_KV(K_(content), K_(file_format), K_(record_count), K_(file_size_in_bytes), K_(file_path));

  DataFileContent content_;
  DataFileFormat file_format_;
  int64_t record_count_;
  int64_t file_size_in_bytes_;
  char file_path_[common::OB_MAX_FILE_NAME_LENGTH] = {0};
};

class DataFile : public SpecWithAllocator
{
public:
  explicit DataFile(ObIAllocator &allocator);
  int init_from_avro(const ManifestMetadata &manifest_metadata,
                     const avro::GenericRecord &avro_data_file);
  // PartitionValues 的顺序和 PartitionSpec 里面的列顺序一样
  static int read_partition_values_from_avro(ObIAllocator &allocator,
                                             const ManifestMetadata &manifest_metadata,
                                             const avro::GenericRecord &avro_record_partition,
                                             ObIArray<ObObj> &partition_values);
  static int read_partition_value_from_avro(ObIAllocator &allocator,
                                            const PartitionField *partition_field,
                                            const schema::ObColumnSchemaV2 *column_schema,
                                            const avro::GenericRecord &avro_record_partition,
                                            ObObj &obj);

  DataFileContent content;
  ObString file_path;
  DataFileFormat file_format;
  ObArray<ObObj> partition;
  int64_t record_count;
  int64_t file_size_in_bytes;
  ObArray<std::pair<int32_t, int64_t>> column_sizes;
  ObArray<std::pair<int32_t, int64_t>> value_counts;
  ObArray<std::pair<int32_t, int64_t>> null_value_counts;
  ObArray<std::pair<int32_t, int64_t>> nan_value_counts;
  ObArray<std::pair<int32_t, ObString>> lower_bounds;
  ObArray<std::pair<int32_t, ObString>> upper_bounds;
  std::optional<ObString> key_metadata;
  ObArray<int64_t> split_offsets;
  ObArray<int32_t> equality_ids;
  std::optional<int32_t> sort_order_id;
  std::optional<ObString> referenced_data_file;

  static constexpr const char *CONTENT = "content";
  static constexpr const char *FILE_PATH = "file_path";
  static constexpr const char *FILE_FORMAT = "file_format";
  static constexpr const char *PARTITION = "partition";
  static constexpr const char *RECORD_COUNT = "record_count";
  static constexpr const char *FILE_SIZE_IN_BYTES = "file_size_in_bytes";
  static constexpr const char *COLUMN_SIZES = "column_sizes";
  static constexpr const char *VALUE_COUNTS = "value_counts";
  static constexpr const char *NULL_VALUE_COUNTS = "null_value_counts";
  static constexpr const char *NAN_VALUE_COUNTS = "nan_value_counts";
  static constexpr const char *LOWER_BOUNDS = "lower_bounds";
  static constexpr const char *UPPER_BOUNDS = "upper_bounds";
  static constexpr const char *KEY_METADATA = "key_metadata";
  static constexpr const char *SPLIT_OFFSETS = "split_offsets";
  static constexpr const char *EQUALITY_IDS = "equality_ids";
  static constexpr const char *SORT_ORDER_ID = "sort_order_id";
  static constexpr const char *REFERENCED_DATA_FILE = "referenced_data_file";

private:
  int get_partitions_(const ManifestMetadata &manifest_metadata,
                      const avro::GenericRecord &avro_data_file);
};

enum class ManifestEntryStatus
{
  EXISTING = 0,
  ADDED = 1,
  DELETED = 2,
};

class ManifestEntry : public SpecWithAllocator
{
public:
  explicit ManifestEntry(ObIAllocator &allocator);
  int init_from_avro(const ManifestFile &parent_manifest_file,
                     const ManifestMetadata &manifest_metadata,
                     const avro::GenericRecord &avro_manifest_entry);
  bool is_alive() const;
  bool is_data_file() const;
  bool is_position_delete_file() const;
  bool is_deletion_vector_file() const;
  bool is_equality_delete_file() const;
  bool is_delete_file() const;

  ManifestEntryStatus status;
  int64_t snapshot_id;
  int64_t sequence_number; // aka data_sequence_number
  int64_t file_sequence_number; // do not use to prune delete files
  DataFile data_file;

  int32_t partition_spec_id; // This field is not in manifest, just inherit from manifest metadata
  PartitionSpec partition_spec; // This field is not in manifest, just inherit from manifest metadata

  static constexpr const char *STATUS = "status";
  static constexpr const char *SNAPSHOT_ID = "snapshot_id";
  static constexpr const char *SEQUENCE_NUMBER = "sequence_number";
  static constexpr const char *FILE_SEQUENCE_NUMBER = "file_sequence_number";
  static constexpr const char *DATA_FILE = "data_file";

private:
  int get_data_file_(const ManifestMetadata &manifest_metadata,
                     const avro::GenericRecord &avro_manifest_entry);
};

} // namespace iceberg

} // namespace sql

} // namespace oceanbase

#endif // MANIFEST_H
