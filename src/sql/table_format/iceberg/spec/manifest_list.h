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

#ifndef MANIFEST_LIST_H
#define MANIFEST_LIST_H

#include "sql/table_format/iceberg/avro_schema_util.h"
#include "sql/table_format/iceberg/ob_iceberg_type_fwd.h"
#include "sql/table_format/iceberg/ob_iceberg_utils.h"
#include "sql/table_format/iceberg/spec/schema_field.h"
#include "sql/table_format/iceberg/spec/spec.h"
#include "sql/table_format/iceberg/spec/type.h"

#include <avro/Decoder.hh>
#include <avro/Encoder.hh>

namespace oceanbase
{
namespace sql
{
namespace iceberg
{

enum class ManifestContent
{
  DATA = 0,
  DELETES = 1
};

class PartitionFieldSummary : public SpecWithAllocator
{
public:
  PartitionFieldSummary(ObIAllocator &allocator);
  int decode_field(const FieldProjection &field_projection, avro::Decoder &decoder);
  bool contains_null;
  std::optional<bool> contains_nan;
  std::optional<ObString> lower_bound;
  std::optional<ObString> upper_bound;

  static constexpr const char *CONTAINS_NULL = "contains_null";
  static constexpr const int32_t CONTAINS_NULL_FIELD_ID = 509;
  inline static const SchemaField CONTAINS_NULL_FIELD
      = SchemaField::make_required(CONTAINS_NULL_FIELD_ID,
                                   CONTAINS_NULL,
                                   IntType::get_instance(),
                                   "Contents of the manifest: 0=data, 1=deletes");

  static constexpr const char *CONTAINS_NAN = "contains_nan";
  static constexpr const int32_t CONTAINS_NAN_FIELD_ID = 518;
  inline static const SchemaField CONTAINS_NAN_FIELD
      = SchemaField::make_optional(CONTAINS_NAN_FIELD_ID,
                                   CONTAINS_NAN,
                                   BooleanType::get_instance(),
                                   "True if any file has a nan partition value");

  static constexpr const char *LOWER_BOUND = "lower_bound";
  static constexpr const int32_t LOWER_BOUND_FIELD_ID = 510;
  inline static const SchemaField LOWER_BOUND_FIELD
      = SchemaField::make_optional(LOWER_BOUND_FIELD_ID,
                                   LOWER_BOUND,
                                   BinaryType::get_instance(),
                                   "Partition lower bound for all files");

  static constexpr const char *UPPER_BOUND = "upper_bound";
  static constexpr const int32_t UPPER_BOUND_FIELD_ID = 511;
  inline static const SchemaField UPPER_BOUND_FIELD
      = SchemaField::make_optional(UPPER_BOUND_FIELD_ID,
                                   UPPER_BOUND,
                                   BinaryType::get_instance(),
                                   "Partition upper bound for all files");

  inline static const StructType PARTITION_SUMMARY_TYPE = StructType(
      {&CONTAINS_NULL_FIELD, &CONTAINS_NAN_FIELD, &LOWER_BOUND_FIELD, &UPPER_BOUND_FIELD});
};

class ManifestFile : public SpecWithAllocator
{
public:
  ManifestFile(ObIAllocator &allocator);
  int decode_field(const FieldProjection &field_projection, avro::Decoder &decoder);
  int get_manifest_entries(const ObString &access_info,
                           ObIArray<ManifestEntry *> &manifest_entries);
  ObIAllocator *get_allocator()
  {
    return &allocator_;
  }
  ObString manifest_path;
  int64_t manifest_length; // for v1 old writer, it will not be set, actually this field not used
  int32_t partition_spec_id;
  ManifestContent content;
  int64_t sequence_number;
  int64_t min_sequence_number;
  int64_t added_snapshot_id;
  std::optional<int32_t> added_files_count;
  std::optional<int32_t> existing_files_count;
  std::optional<int32_t> deleted_files_count;
  std::optional<int64_t> added_rows_count;
  std::optional<int64_t> existing_rows_count;
  std::optional<int64_t> deleted_rows_count;
  ObFixedArray<PartitionFieldSummary *, ObIAllocator> partitions;
  std::optional<ObString> key_metadata;

  static int get_read_expected_schema(StructType *&struct_type);

  static constexpr const char *MANIFEST_PATH = "manifest_path";
  static constexpr const int32_t MANIFEST_PATH_FIELD_ID = 500;
  inline static const SchemaField MANIFEST_PATH_FIELD
      = SchemaField::make_required(MANIFEST_PATH_FIELD_ID,
                                   MANIFEST_PATH,
                                   StringType::get_instance(),
                                   "Location URI with FS scheme");

  static constexpr const char *MANIFEST_LENGTH = "manifest_length";
  static constexpr const int32_t MANIFEST_LENGTH_FIELD_ID = 501;
  inline static const SchemaField MANIFEST_LENGTH_FIELD
      = SchemaField::make_required(MANIFEST_LENGTH_FIELD_ID,
                                   MANIFEST_LENGTH,
                                   LongType::get_instance(),
                                   "Total file size in bytes");

  static constexpr const char *PARTITION_SPEC_ID = "partition_spec_id";
  static constexpr const int32_t PARTITION_SPEC_ID_FIELD_ID = 502;
  inline static const SchemaField PARTITION_SPEC_ID_FIELD
      = SchemaField::make_required(PARTITION_SPEC_ID_FIELD_ID,
                                   PARTITION_SPEC_ID,
                                   IntType::get_instance(),
                                   "Spec ID used to write");

  static constexpr const char *CONTENT = "content";
  static constexpr const int32_t CONTENT_FIELD_ID = 517;
  inline static const SchemaField REQUIRED_CONTENT_FIELD
      = SchemaField::make_required(CONTENT_FIELD_ID,
                                   CONTENT,
                                   IntType::get_instance(),
                                   "Contents of the manifest: 0=data, 1=deletes");
  inline static const SchemaField OPTIONAL_CONTENT_FIELD
      = SchemaField::make_optional(CONTENT_FIELD_ID,
                                   CONTENT,
                                   IntType::get_instance(),
                                   "Contents of the manifest: 0=data, 1=deletes");

  static constexpr const char *SEQUENCE_NUMBER = "sequence_number";
  static constexpr const int32_t SEQUENCE_NUMBER_FIELD_ID = 515;
  inline static const SchemaField REQUIRED_SEQUENCE_NUMBER_FIELD
      = SchemaField::make_required(SEQUENCE_NUMBER_FIELD_ID,
                                   SEQUENCE_NUMBER,
                                   LongType::get_instance(),
                                   "Sequence number when the manifest was added");
  inline static const SchemaField OPTIONAL_SEQUENCE_NUMBER_FIELD
      = SchemaField::make_optional(SEQUENCE_NUMBER_FIELD_ID,
                                   SEQUENCE_NUMBER,
                                   LongType::get_instance(),
                                   "Sequence number when the manifest was added");

  static constexpr const char *MIN_SEQUENCE_NUMBER = "min_sequence_number";
  static constexpr const int32_t MIN_SEQUENCE_NUMBER_FIELD_ID = 516;
  inline static const SchemaField REQUIRED_MIN_SEQUENCE_NUMBER_FIELD
      = SchemaField::make_required(MIN_SEQUENCE_NUMBER_FIELD_ID,
                                   MIN_SEQUENCE_NUMBER,
                                   LongType::get_instance(),
                                   "Lowest sequence number in the manifest");
  inline static const SchemaField OPTIONAL_MIN_SEQUENCE_NUMBER_FIELD
      = SchemaField::make_optional(MIN_SEQUENCE_NUMBER_FIELD_ID,
                                   MIN_SEQUENCE_NUMBER,
                                   LongType::get_instance(),
                                   "Lowest sequence number in the manifest");

  static constexpr const char *ADDED_SNAPSHOT_ID = "added_snapshot_id";
  static constexpr const int32_t ADDED_SNAPSHOT_ID_FIELD_ID = 503;
  inline static const SchemaField ADDED_SNAPSHOT_ID_FIELD
      = SchemaField::make_required(ADDED_SNAPSHOT_ID_FIELD_ID,
                                   ADDED_SNAPSHOT_ID,
                                   LongType::get_instance(),
                                   "Snapshot ID that added the manifest");

  static constexpr const char *ADDED_FILES_COUNT = "added_files_count";
  static constexpr const int32_t ADDED_FILES_COUNT_FIELD_ID = 504;
  inline static const SchemaField REQUIRED_ADDED_FILES_COUNT_FIELD
      = SchemaField::make_required(ADDED_FILES_COUNT_FIELD_ID,
                                   ADDED_FILES_COUNT,
                                   IntType::get_instance(),
                                   "Added entry count");
  inline static const SchemaField OPTIONAL_ADDED_FILES_COUNT_FIELD
      = SchemaField::make_optional(ADDED_FILES_COUNT_FIELD_ID,
                                   ADDED_FILES_COUNT,
                                   IntType::get_instance(),
                                   "Added entry count");

  static constexpr const char *EXISTING_FILES_COUNT = "existing_files_count";
  static constexpr const int32_t EXISTING_FILES_COUNT_FIELD_ID = 505;
  inline static const SchemaField REQUIRED_EXISTING_FILES_COUNT_FIELD
      = SchemaField::make_required(EXISTING_FILES_COUNT_FIELD_ID,
                                   EXISTING_FILES_COUNT,
                                   IntType::get_instance(),
                                   "Existing entry count");
  inline static const SchemaField OPTIONAL_EXISTING_FILES_COUNT_FIELD
      = SchemaField::make_optional(EXISTING_FILES_COUNT_FIELD_ID,
                                   EXISTING_FILES_COUNT,
                                   IntType::get_instance(),
                                   "Existing entry count");

  static constexpr const char *DELETED_FILES_COUNT = "deleted_files_count";
  static constexpr const int32_t DELETED_FILES_COUNT_FIELD_ID = 506;
  inline static const SchemaField REQUIRED_DELETED_FILES_COUNT_FIELD
      = SchemaField::make_required(DELETED_FILES_COUNT_FIELD_ID,
                                   DELETED_FILES_COUNT,
                                   IntType::get_instance(),
                                   "Deleted entry count");
  inline static const SchemaField OPTIONAL_DELETED_FILES_COUNT_FIELD
      = SchemaField::make_optional(DELETED_FILES_COUNT_FIELD_ID,
                                   DELETED_FILES_COUNT,
                                   IntType::get_instance(),
                                   "Deleted entry count");

  static constexpr const char *ADDED_ROWS_COUNT = "added_rows_count";
  static constexpr const int32_t ADDED_ROWS_COUNT_FIELD_ID = 512;
  inline static const SchemaField REQUIRED_ADDED_ROWS_COUNT_FIELD
      = SchemaField::make_required(ADDED_ROWS_COUNT_FIELD_ID,
                                   ADDED_ROWS_COUNT,
                                   LongType::get_instance(),
                                   "Added rows count");
  inline static const SchemaField OPTIONAL_ADDED_ROWS_COUNT_FIELD
      = SchemaField::make_optional(ADDED_ROWS_COUNT_FIELD_ID,
                                   ADDED_ROWS_COUNT,
                                   LongType::get_instance(),
                                   "Added rows count");

  static constexpr const char *EXISTING_ROWS_COUNT = "existing_rows_count";
  static constexpr const int32_t EXISTING_ROWS_COUNT_FIELD_ID = 513;
  inline static const SchemaField REQUIRED_EXISTING_ROWS_COUNT_FIELD
      = SchemaField::make_optional(EXISTING_ROWS_COUNT_FIELD_ID,
                                   EXISTING_ROWS_COUNT,
                                   LongType::get_instance(),
                                   "Existing rows count");
  inline static const SchemaField OPTIONAL_EXISTING_ROWS_COUNT_FIELD
      = SchemaField::make_optional(EXISTING_ROWS_COUNT_FIELD_ID,
                                   EXISTING_ROWS_COUNT,
                                   LongType::get_instance(),
                                   "Existing rows count");

  static constexpr const char *DELETED_ROWS_COUNT = "deleted_rows_count";
  static constexpr const int32_t DELETED_ROWS_COUNT_FIELD_ID = 514;
  inline static const SchemaField REQUIRED_DELETED_ROWS_COUNT_FIELD
      = SchemaField::make_optional(DELETED_ROWS_COUNT_FIELD_ID,
                                   DELETED_ROWS_COUNT,
                                   LongType::get_instance(),
                                   "Deleted rows count");
  inline static const SchemaField OPTIONAL_DELETED_ROWS_COUNT_FIELD
      = SchemaField::make_optional(DELETED_ROWS_COUNT_FIELD_ID,
                                   DELETED_ROWS_COUNT,
                                   LongType::get_instance(),
                                   "Deleted rows count");

  static constexpr const char *PARTITIONS = "partitions";
  static constexpr const int32_t PARTITIONS_FIELD_ID = 507;
  static constexpr const int32_t PARTITION_FIELD_SUMMARY_FIELD_ID = 508;
  inline static const SchemaField PARTITIONS_FIELD = SchemaField::make_optional(
      PARTITIONS_FIELD_ID,
      PARTITIONS,
      new (std::nothrow)
          ListType(new (std::nothrow) SchemaField(PARTITION_FIELD_SUMMARY_FIELD_ID,
                                                  "element",
                                                  &PartitionFieldSummary::PARTITION_SUMMARY_TYPE)),
      "Summary for each partition");

  static constexpr const char *KEY_METADATA = "key_metadata";
  static constexpr const int32_t KEY_METADATA_FIELD_ID = 519;
  inline static const SchemaField KEY_METADATA_FIELD
      = SchemaField::make_optional(KEY_METADATA_FIELD_ID,
                                   KEY_METADATA,
                                   BinaryType::get_instance(),
                                   "Encryption key metadata blob");

private:
  int decode_partitions_(const FieldProjection &field_projection, avro::Decoder &decoder);
  int get_manifest_entries_(const ObString &access_info,
                            ObIArray<ManifestEntry *> &manifest_entries) const;

  // ManifestFile 一旦加载完成过一个 manifest，这个字段就会被填充
  // 后续该 Manifest 读取 ManifestEntry 直接从这个字段返回
  // 有效减少内存开销
  ObFixedArray<ManifestEntry *, ObIAllocator> cached_manifest_entries_;
};

class ManifestFileDatum
{
public:
  ManifestFileDatum(ObIAllocator &allocator, const SchemaProjection &schema_projection)
      : allocator_(allocator), schema_projection_(schema_projection)
  {
  }
  ObIAllocator &allocator_;
  const SchemaProjection &schema_projection_;
  ManifestFile *manifest_file_ = NULL;
};

} // namespace iceberg
} // namespace sql

} // namespace oceanbase

namespace avro
{
template <typename T> struct codec_traits;

template <> struct codec_traits<oceanbase::sql::iceberg::ManifestFileDatum>
{
  static void encode(Encoder &e, const oceanbase::sql::iceberg::ManifestFileDatum &v);
  static void decode(Decoder &d, oceanbase::sql::iceberg::ManifestFileDatum &v);
};

} // namespace avro

#endif // MANIFEST_LIST_H
