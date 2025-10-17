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

#include "sql/table_format/iceberg/ob_iceberg_type_fwd.h"
#include "sql/table_format/iceberg/ob_iceberg_utils.h"
#include "sql/table_format/iceberg/spec/spec.h"

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
  int init_from_avro(const avro::GenericRecord &avro_partition_field_summary);
  bool contains_null;
  std::optional<bool> contains_nan;
  std::optional<ObString> lower_bound;
  std::optional<ObString> upper_bound;

  static constexpr const char *CONTAINS_NULL = "contains_null";
  static constexpr const char *CONTAINS_NAN = "contains_nan";
  static constexpr const char *LOWER_BOUND = "lower_bound";
  static constexpr const char *UPPER_BOUND = "upper_bound";
};

class ManifestFile : public SpecWithAllocator
{
public:
  ManifestFile(ObIAllocator &allocator);
  int init_from_avro(const avro::GenericRecord &avro_manifest_file);
  int get_manifest_entries(const ObString &access_info,
                           ObIArray<ManifestEntry *> &manifest_entries);
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
  ObArray<PartitionFieldSummary *> partitions;
  std::optional<ObString> key_metadata;

  static constexpr const char *MANIFEST_PATH = "manifest_path";
  static constexpr const char *MANIFEST_LENGTH = "manifest_length";
  static constexpr const char *PARTITION_SPEC_ID = "partition_spec_id";
  static constexpr const char *CONTENT = "content";
  static constexpr const char *SEQUENCE_NUMBER = "sequence_number";
  static constexpr const char *MIN_SEQUENCE_NUMBER = "min_sequence_number";
  static constexpr const char *ADDED_SNAPSHOT_ID = "added_snapshot_id";
  static constexpr const char *ADDED_FILES_COUNT = "added_files_count";
  static constexpr const char *EXISTING_FILES_COUNT = "existing_files_count";
  static constexpr const char *DELETED_FILES_COUNT = "deleted_files_count";
  static constexpr const char *ADDED_ROWS_COUNT = "added_rows_count";
  static constexpr const char *EXISTING_ROWS_COUNT = "existing_rows_count";
  static constexpr const char *DELETED_ROWS_COUNT = "deleted_rows_count";
  static constexpr const char *PARTITIONS = "partitions";
  static constexpr const char *KEY_METADATA = "key_metadata";

private:
  int get_partitions_(const avro::GenericRecord &avro_manifest_file);
  int get_manifest_entries_(const ObString &access_info,
                           ObIArray<ManifestEntry *> &manifest_entries) const;

  // ManifestFile 一旦加载完成过一个 manifest，这个字段就会被填充
  // 后续该 Manifest 读取 ManifestEntry 直接从这个字段返回
  // 有效减少内存开销
  ObArray<ManifestEntry *> cached_manifest_entries_;
};

} // namespace iceberg
} // namespace sql

} // namespace oceanbase

#endif // MANIFEST_LIST_H
