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

#ifndef SNAPSHOT_H
#define SNAPSHOT_H
#include "lib/container/ob_array.h"
#include "lib/json/ob_json.h"
#include "sql/table_format/iceberg/ob_iceberg_type_fwd.h"
#include "sql/table_format/iceberg/ob_iceberg_utils.h"
#include "sql/table_format/iceberg/spec/spec.h"

namespace oceanbase
{
namespace sql
{
namespace iceberg
{

class Snapshot : public SpecWithAllocator
{
public:
  explicit Snapshot(ObIAllocator &allocator);
  int init_from_json(const ObJsonObject &json_object);
  int assign(const Snapshot &other);
  int64_t get_convert_size() const;
  int get_manifest_files(ObIAllocator &allocator,
                         const ObString &access_info,
                         ObIArray<ManifestFile *> &manifest_files) const;
  // int plan_files(ObIAllocator &allocator,
  //                const ObString &access_info,
  //                ObIArray<FileScanTask *> &file_scan_tasks) const;

  int64_t snapshot_id;
  std::optional<int64_t> parent_snapshot_id;
  int64_t sequence_number;
  int64_t timestamp_ms;
  std::optional<int32_t> schema_id;
  ObString manifest_list;
  ObArray<ObString> v1_manifests;
  ObArray<std::pair<ObString, ObString>> summary;

  static constexpr const char *SNAPSHOT_ID = "snapshot-id";
  static constexpr const char *PARENT_SNAPSHOT_ID = "parent-snapshot-id";
  static constexpr const char *SEQUENCE_NUMBER = "sequence-number";
  static constexpr const char *TIMESTAMP_MS = "timestamp-ms";
  static constexpr const char *MANIFEST_LIST = "manifest-list";
  static constexpr const char *MANIFESTS = "manifests";
  static constexpr const char *SUMMARY = "summary";
  static constexpr const char *SCHEMA_ID = "schema-id";
};

struct SnapshotLog
{
  TO_STRING_EMPTY();
};

} // namespace iceberg
} // namespace sql
} // namespace oceanbase

#endif // SNAPSHOT_H
