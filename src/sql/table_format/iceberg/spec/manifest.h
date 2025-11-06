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

#include "sql/table_format/iceberg/avro_schema_util.h"
#include "sql/table_format/iceberg/ob_iceberg_type_fwd.h"
#include "sql/table_format/iceberg/ob_iceberg_utils.h"
#include "sql/table_format/iceberg/spec/manifest_list.h"
#include "sql/table_format/iceberg/spec/partition.h"
#include "sql/table_format/iceberg/spec/schema.h"
#include "sql/table_format/iceberg/spec/schema_field.h"
#include "sql/table_format/iceberg/spec/spec.h"
#include "sql/table_format/iceberg/spec/type.h"

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
  TO_STRING_KV(K_(content),
               K_(file_format),
               K_(record_count),
               K_(file_size_in_bytes),
               K_(file_path));

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
  int decode_field(const ManifestMetadata &manifest_metadata,
                   const FieldProjection &field_projection,
                   avro::Decoder &decoder);
  // PartitionValues 的顺序和 PartitionSpec 里面的列顺序一样
  // FieldProjection children 的 idx 的顺序也是和 PartitionSpec 一样
  static int read_partition_values_from_avro(ObIAllocator &allocator,
                                             const ManifestMetadata &manifest_metadata,
                                             const FieldProjection &field_projection,
                                             avro::Decoder &decoder,
                                             ObIArray<ObObj> &partition_values);
  static int read_partition_value_from_avro(ObIAllocator &allocator,
                                            const PartitionField *partition_field,
                                            const schema::ObColumnSchemaV2 *column_schema,
                                            const avro::NodePtr &avro_node,
                                            avro::Decoder &decoder,
                                            ObObj &obj);

  DataFileContent content;
  ObString file_path;
  DataFileFormat file_format;
  ObFixedArray<ObObj, ObIAllocator> partition;
  int64_t record_count;
  int64_t file_size_in_bytes;
  ObFixedArray<std::pair<int32_t, int64_t>, ObIAllocator> column_sizes;
  ObFixedArray<std::pair<int32_t, int64_t>, ObIAllocator> value_counts;
  ObFixedArray<std::pair<int32_t, int64_t>, ObIAllocator> null_value_counts;
  ObFixedArray<std::pair<int32_t, int64_t>, ObIAllocator> nan_value_counts;
  ObFixedArray<std::pair<int32_t, ObString>, ObIAllocator> lower_bounds;
  ObFixedArray<std::pair<int32_t, ObString>, ObIAllocator> upper_bounds;
  std::optional<ObString> key_metadata;
  ObFixedArray<int64_t, ObIAllocator> split_offsets;
  ObFixedArray<int32_t, ObIAllocator> equality_ids;
  std::optional<int32_t> sort_order_id;
  std::optional<ObString> referenced_data_file;

  static int get_read_expected_schema(ObIAllocator &allocator,
                                      const ManifestMetadata &manifest_metadata,
                                      StructType *&struct_type);

  static constexpr const char *CONTENT = "content";
  static constexpr const int32_t CONTENT_FIELD_ID = 134;
  inline static const SchemaField REQUIRED_CONTENT_FIELD = SchemaField::make_required(
      CONTENT_FIELD_ID,
      CONTENT,
      IntType::get_instance(),
      "Contents of the file: 0=data, 1=position deletes, 2=equality deletes");
  inline static const SchemaField OPTIONAL_CONTENT_FIELD = SchemaField::make_optional(
      CONTENT_FIELD_ID,
      CONTENT,
      IntType::get_instance(),
      "Contents of the file: 0=data, 1=position deletes, 2=equality deletes");

  static constexpr const char *FILE_PATH = "file_path";
  static constexpr int32_t FILE_PATH_FIELD_ID = 100;
  inline static const SchemaField FILE_PATH_FIELD
      = SchemaField::make_required(FILE_PATH_FIELD_ID,
                                   FILE_PATH,
                                   StringType::get_instance(),
                                   "Location URI with FS scheme");

  static constexpr const char *FILE_FORMAT = "file_format";
  static constexpr int32_t FILE_FORMAT_FIELD_ID = 101;
  inline static const SchemaField FILE_FORMAT_FIELD
      = SchemaField::make_required(FILE_FORMAT_FIELD_ID,
                                   FILE_FORMAT,
                                   StringType::get_instance(),
                                   "File format name: avro, orc, or parquet");

  static constexpr const char *PARTITION = "partition";
  static constexpr int32_t PARTITION_FIELD_ID = 102;

  static constexpr const char *RECORD_COUNT = "record_count";
  static constexpr int32_t RECORD_COUNT_FIELD_ID = 103;
  inline static const SchemaField RECORD_COUNT_FIELD
      = SchemaField::make_required(RECORD_COUNT_FIELD_ID,
                                   RECORD_COUNT,
                                   LongType::get_instance(),
                                   "Number of records in the file");

  static constexpr const char *FILE_SIZE_IN_BYTES = "file_size_in_bytes";
  static constexpr int32_t FILE_SIZE_IN_BYTES_FIELD_ID = 104;
  inline static const SchemaField FILE_SIZE_FIELD
      = SchemaField::make_required(FILE_SIZE_IN_BYTES_FIELD_ID,
                                   FILE_SIZE_IN_BYTES,
                                   LongType::get_instance(),
                                   "Total file size in bytes");

  static constexpr const char *COLUMN_SIZES = "column_sizes";
  static constexpr int32_t COLUMN_SIZE_FIELD_ID = 108;
  inline static const SchemaField COLUMN_SIZES_FIELD = SchemaField::make_optional(
      COLUMN_SIZE_FIELD_ID,
      COLUMN_SIZES,
      new (std::nothrow)
          MapType(new (std::nothrow) SchemaField(117, "key", IntType::get_instance()),
                  new (std::nothrow) SchemaField(118, "value", LongType::get_instance())),
      "Map of column id to total size on disk");

  static constexpr const char *VALUE_COUNTS = "value_counts";
  static constexpr int32_t VALUE_COUNTS_FIELD_ID = 109;
  inline static const SchemaField VALUE_COUNTS_FIELD = SchemaField::make_optional(
      VALUE_COUNTS_FIELD_ID,
      VALUE_COUNTS,
      new (std::nothrow)
          MapType(new (std::nothrow) SchemaField(119, "key", IntType::get_instance()),
                  new (std::nothrow) SchemaField(120, "value", LongType::get_instance())),
      "Map of column id to total count, including null and NaN");

  static constexpr const char *NULL_VALUE_COUNTS = "null_value_counts";
  static constexpr int32_t NULL_VALUE_COUNTS_FIELD_ID = 110;
  inline static const SchemaField NULL_VALUE_COUNTS_FIELD = SchemaField::make_optional(
      NULL_VALUE_COUNTS_FIELD_ID,
      NULL_VALUE_COUNTS,
      new (std::nothrow)
          MapType(new (std::nothrow) SchemaField(121, "key", IntType::get_instance()),
                  new (std::nothrow) SchemaField(122, "value", LongType::get_instance())),
      "Map of column id to null value count");

  static constexpr const char *NAN_VALUE_COUNTS = "nan_value_counts";
  static constexpr int32_t NAN_VALUE_COUNTS_FIELD_ID = 137;
  inline static const SchemaField NAN_VALUE_COUNTS_FIELD = SchemaField::make_optional(
      NAN_VALUE_COUNTS_FIELD_ID,
      NAN_VALUE_COUNTS,
      new (std::nothrow)
          MapType(new (std::nothrow) SchemaField(138, "key", IntType::get_instance()),
                  new (std::nothrow) SchemaField(139, "value", LongType::get_instance())),
      "Map of column id to number of NaN values in the column");

  static constexpr const char *LOWER_BOUNDS = "lower_bounds";
  static constexpr int32_t LOWER_BOUNDS_FIELD_ID = 125;
  inline static const SchemaField LOWER_BOUNDS_FIELD = SchemaField::make_optional(
      LOWER_BOUNDS_FIELD_ID,
      LOWER_BOUNDS,
      new (std::nothrow)
          MapType(new (std::nothrow) SchemaField(126, "key", IntType::get_instance()),
                  new (std::nothrow) SchemaField(127, "value", BinaryType::get_instance())),
      "Map of column id to lower bound");

  static constexpr const char *UPPER_BOUNDS = "upper_bounds";
  static constexpr int32_t UPPER_BOUNDS_FIELD_ID = 128;
  inline static const SchemaField UPPER_BOUNDS_FIELD = SchemaField::make_optional(
      UPPER_BOUNDS_FIELD_ID,
      UPPER_BOUNDS,
      new (std::nothrow)
          MapType(new (std::nothrow) SchemaField(129, "key", IntType::get_instance()),
                  new (std::nothrow) SchemaField(130, "value", BinaryType::get_instance())),
      "Map of column id to upper bound");

  static constexpr const char *KEY_METADATA = "key_metadata";
  static constexpr int32_t KEY_METADATA_FIELD_ID = 131;
  inline static const SchemaField KEY_METADATA_FIELD
      = SchemaField::make_optional(KEY_METADATA_FIELD_ID,
                                   KEY_METADATA,
                                   BinaryType::get_instance(),
                                   "Encryption key metadata blob");

  static constexpr const char *SPLIT_OFFSETS = "split_offsets";
  static constexpr int32_t SPLIT_OFFSETS_FIELD_ID = 132;
  inline static const SchemaField SPLIT_OFFSETS_FIELD = SchemaField::make_optional(
      SPLIT_OFFSETS_FIELD_ID,
      SPLIT_OFFSETS,
      new (std::nothrow)
          ListType(new (std::nothrow) SchemaField(133, "element", LongType::get_instance())),
      "Splittable offsets");

  static constexpr const char *EQUALITY_IDS = "equality_ids";
  static constexpr int32_t EQUALITY_IDS_FIELD_ID = 135;
  inline static const SchemaField EQUALITY_IDS_FIELD = SchemaField::make_optional(
      EQUALITY_IDS_FIELD_ID,
      EQUALITY_IDS,
      new (std::nothrow)
          ListType(new (std::nothrow) SchemaField(136, "element", IntType::get_instance())),
      "Equality comparison field IDs");

  static constexpr const char *SORT_ORDER_ID = "sort_order_id";
  static constexpr int32_t SORT_ORDER_ID_FIELD_ID = 140;
  inline static const SchemaField SORT_ORDER_ID_FIELD
      = SchemaField::make_optional(SORT_ORDER_ID_FIELD_ID,
                                   SORT_ORDER_ID,
                                   IntType::get_instance(),
                                   "Sort order ID");

  static constexpr const char *REFERENCED_DATA_FILE = "referenced_data_file";
  static constexpr int32_t REFERENCED_DATA_FILE_FIELD_ID = 143;
  inline static const SchemaField REFERENCED_DATA_FILE_FIELD = SchemaField::make_optional(
      REFERENCED_DATA_FILE_FIELD_ID,
      REFERENCED_DATA_FILE,
      StringType::get_instance(),
      "Fully qualified location (URI with FS scheme) of a data file that all deletes reference");

private:
  int decode_partitions_(const ManifestMetadata &manifest_metadata,
                         const FieldProjection &field_projection,
                         avro::Decoder &decoder);
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
  int apply(const ManifestMetadata &manifest_metadata);
  int decode_field(const ManifestFile &parent_manifest_file,
                   const ManifestMetadata &manifest_metadata,
                   const FieldProjection &field_projection,
                   avro::Decoder &decoder);
  bool is_alive() const;
  bool is_data_file() const;
  bool is_position_delete_file() const;
  bool is_deletion_vector_file() const;
  bool is_equality_delete_file() const;
  bool is_delete_file() const;

  ManifestEntryStatus status;
  int64_t snapshot_id;
  int64_t sequence_number;      // aka data_sequence_number
  int64_t file_sequence_number; // do not use to prune delete files
  DataFile data_file;

  int32_t partition_spec_id; // This field is not in manifest, just inherit from manifest metadata
  // 浅拷贝，一个 manifest 下面的所有 manifest entry 都共用一份 PartitionSpec 就行了
  PartitionSpec
      partition_spec; // This field is not in manifest, just inherit from manifest metadata

  static int get_read_expected_schema(ObIAllocator &allocator,
                                      const ManifestMetadata &manifest_metadata,
                                      StructType *&struct_type);

  static constexpr const char *STATUS = "status";
  static constexpr const int32_t STATUS_FIELD_ID = 0;
  inline static const SchemaField STATUS_FIELD
      = SchemaField::make_required(STATUS_FIELD_ID, STATUS, IntType::get_instance());

  static constexpr const char *SNAPSHOT_ID = "snapshot_id";
  static constexpr const int32_t SNAPSHOT_ID_FIELD_ID = 1;
  inline static const SchemaField SNAPSHOT_ID_FIELD
      = SchemaField::make_optional(SNAPSHOT_ID_FIELD_ID, SNAPSHOT_ID, LongType::get_instance());

  static constexpr const char *SEQUENCE_NUMBER = "sequence_number";
  static constexpr const int32_t SEQUENCE_NUMBER_FIELD_ID = 3;
  inline static const SchemaField SEQUENCE_NUMBER_FIELD
      = SchemaField::make_optional(SEQUENCE_NUMBER_FIELD_ID,
                                   SEQUENCE_NUMBER,
                                   LongType::get_instance());

  static constexpr const char *FILE_SEQUENCE_NUMBER = "file_sequence_number";
  static constexpr const int32_t FILE_SEQUENCE_NUMBER_FIELD_ID = 4;
  inline static const SchemaField FILE_SEQUENCE_NUMBER_FIELD
      = SchemaField::make_optional(FILE_SEQUENCE_NUMBER_FIELD_ID,
                                   FILE_SEQUENCE_NUMBER,
                                   LongType::get_instance());

  static constexpr const char *DATA_FILE = "data_file";
  static constexpr const int32_t DATA_FILE_FIELD_ID = 2;

private:
  int decode_data_file_(const ManifestMetadata &manifest_metadata,
                        const FieldProjection &field_projection,
                        avro::Decoder &decoder);
};

class ManifestEntryDatum
{
public:
  ManifestEntryDatum(ObIAllocator &allocator,
                     const SchemaProjection &schema_projection,
                     const ManifestFile &parent_manifest_file,
                     const ManifestMetadata &manifest_metadata)
      : allocator_(allocator), schema_projection_(schema_projection),
        parent_manifest_file_(parent_manifest_file), manifest_metadata_(manifest_metadata)
  {
  }
  ObIAllocator &allocator_;
  const SchemaProjection &schema_projection_;
  const ManifestFile &parent_manifest_file_;
  const ManifestMetadata &manifest_metadata_;
  ManifestEntry *manifest_entry_ = NULL;
};

} // namespace iceberg

} // namespace sql

} // namespace oceanbase

namespace avro
{
template <typename T> struct codec_traits;

template <> struct codec_traits<oceanbase::sql::iceberg::ManifestEntryDatum>
{
  static void encode(Encoder &e, const oceanbase::sql::iceberg::ManifestEntryDatum &v);
  static void decode(Decoder &d, oceanbase::sql::iceberg::ManifestEntryDatum &v);
};

} // namespace avro

#endif // MANIFEST_H
