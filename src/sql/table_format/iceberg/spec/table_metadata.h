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

#ifndef TABLE_METADATA_H
#define TABLE_METADATA_H

#include "lib/container/ob_array.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/json/ob_json.h"
#include "sql/table_format/iceberg/ob_iceberg_type_fwd.h"
#include "sql/table_format/iceberg/spec/partition.h"
#include "sql/table_format/iceberg/spec/schema.h"
#include "sql/table_format/iceberg/spec/snapshot.h"
#include "sql/table_format/iceberg/spec/spec.h"
#include "sql/table_format/iceberg/spec/statistics.h"

namespace oceanbase
{
namespace sql
{

namespace iceberg
{

enum class FormatVersion
{
  INVALID = 0,
  V1 = 1,
  V2 = 2,
  V3 = 3,
};

struct MetadataLog
{
  TO_STRING_EMPTY();
};

struct SortOrder
{
  TO_STRING_EMPTY();
};

struct SnapshotRef
{
  TO_STRING_EMPTY();
};

struct PartitionStatisticsFile
{
  TO_STRING_EMPTY();
};

class TableMetadata : public SpecWithAllocator
{
public:
  explicit TableMetadata(ObIAllocator &allocator);
  int assign(const TableMetadata &other);

  // An integer version number for the format. Currently, this can be 1 or 2 based on the spec.
  // Implementations must throw an exception if a table's version is higher than the supported
  // version.
  FormatVersion format_version;

  // A UUID that identifies the table, generated when the table is created.
  // Implementations must throw an exception if a table's UUID does not match
  // the expected UUID after refreshing metadata.
  std::optional<ObString> table_uuid;

  // The table's base location. This is used by writers to determine where to
  // store data files, manifest files, and table metadata files.
  ObString location;

  // The table's highest assigned sequence number, a monotonically increasing long that
  // tracks the order of snapshots in a table.
  int64_t last_sequence_number;

  // Timestamp in milliseconds from the unix epoch when the table was last updated.
  // Each table metadata file should update this field just before writing.
  int64_t last_updated_ms;

  // An integer; the highest assigned column ID for the table.
  // This is used to ensure columns are always assigned an unused ID when evolving schemas.
  int32_t last_column_id;

  // The table’s schemas. Only store latest schema
  ObFixedArray<Schema *, ObIAllocator> schemas;

  // ID of the table's current schema.
  int32_t current_schema_id;

  // A list of partition specs, stored as full partition spec objects.
  ObFixedArray<PartitionSpec *, ObIAllocator> partition_specs;

  // ID of the "current" spec that writers should use by default.
  int32_t default_spec_id;

  // An integer; the highest assigned partition field ID across all partition specs for the table.
  // This is used to ensure partition fields are always assigned an unused ID when evolving specs.
  int32_t last_partition_id;

  // A string to string map of table properties.
  ObFixedArray<std::pair<ObString, ObString>, ObIAllocator> properties;

  // long ID of the current table snapshot; must be the same as the current ID of the main branch in
  // refs.
  int64_t current_snapshot_id;

  // A list of valid snapshots.
  ObFixedArray<Snapshot *, ObIAllocator> snapshots;

  // A list (optional) of timestamp and snapshot ID pairs that encodes changes to the
  // current snapshot for the table.
  // common::ObArray<SnapshotLog> snapshot_logs;

  // A list (optional) of timestamp and metadata file location pairs that encodes
  // changes to the previous metadata files for the table.
  // common::ObArray<MetadataLog> metadata_logs;

  // A list of sort orders, stored as full sort order objects.
  // ObArray<SortOrder> sort_orders;

  // Default sort order id of the table.
  // int32_t default_sort_order_id;

  // A map of snapshot references.
  // ObArray<SnapshotRef> refs;

  // Mapping of snapshot ids to statistics files.
  ObFixedArray<StatisticsFile *, ObIAllocator> statistics;

  // Mapping of snapshot ids to partition statistics files.
  // ObArray<PartitionStatisticsFile> partition_statistics;

  int init_from_json(const ObJsonObject &json_object);
  int get_current_snapshot(const Snapshot *&snapshot) const;
  int get_current_snapshot(Snapshot *&snapshot);
  int get_schema(int32_t schema_id, const Schema *&schema) const;
  int get_partition_spec(int32_t partition_spec_id, const PartitionSpec *&partition_spec) const;
  int get_table_property(const char *table_property_key, ObString &value) const;
  int get_table_default_write_format(DataFileFormat &data_file_format) const;

  static constexpr const char *FORMAT_VERSION = "format-version";
  static constexpr const char *TABLE_UUID = "table-uuid";
  static constexpr const char *LOCATION = "location";
  static constexpr const char *LAST_SEQUENCE_NUMBER = "last-sequence-number";
  static constexpr const char *LAST_UPDATED_MS = "last-updated-ms";
  static constexpr const char *LAST_COLUMN_ID = "last-column-id";
  static constexpr const char *SCHEMA = "schema";
  static constexpr const char *SCHEMAS = "schemas";
  static constexpr const char *CURRENT_SCHEMA_ID = "current-schema-id";
  static constexpr const char *PARTITION_SPEC = "partition-spec";
  static constexpr const char *PARTITION_SPECS = "partition-specs";
  static constexpr const char *DEFAULT_SPEC_ID = "default-spec-id";
  static constexpr const char *LAST_PARTITION_ID = "last-partition-id";
  static constexpr const char *PROPERTIES = "properties";
  static constexpr const char *CURRENT_SNAPSHOT_ID = "current-snapshot-id";
  static constexpr const char *SNAPSHOTS = "snapshots";
  static constexpr const char *SNAPSHOT_LOG = "snapshot-log";
  static constexpr const char *METADATA_LOG = "metadata-log";
  static constexpr const char *SORT_ORDERS = "sort-orders";
  static constexpr const char *DEFAULT_SORT_ORDER_ID = "default-sort-order-id";
  static constexpr const char *REFS = "refs";
  static constexpr const char *STATISTICS = "statistics";
  static constexpr const char *PARTITION_STATISTICS = "partition-statistics";


  // Below is table properties
  // https://iceberg.apache.org/docs/1.9.1/configuration/#table-properties
  static constexpr const char *WRITE_FORMAT_DEFAULT = "write.format.default";

private:
  int parse_schemas_(const ObJsonObject &json_object);
  int parse_partition_specs_(const ObJsonObject &json_object);
  int parse_sort_order_(const ObJsonObject &json_object);
  int parse_refs_(const ObJsonObject &json_object);
  int parse_snapshots_(const ObJsonObject &json_object);
  int parse_statistics_files_(const ObJsonObject &json_object);
  int parse_partition_statistics_files_(const ObJsonObject &json_object);
  int parse_snapshot_log_(const ObJsonObject &json_object);
  int parse_metadata_log_(const ObJsonObject &json_object);
};

} // namespace iceberg
} // namespace sql
} // namespace oceanbase

#endif // TABLE_METADATA_H
