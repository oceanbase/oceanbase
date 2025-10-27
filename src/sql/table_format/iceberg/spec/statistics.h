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

#ifndef STATISTICS_H
#define STATISTICS_H

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

class BlobMetadata : public SpecWithAllocator
{
public:
  explicit BlobMetadata(ObIAllocator &allocator);
  int init_from_json(const ObJsonObject &json_object);
  int assign(const BlobMetadata &other);

  ObString type;
  long snapshot_id;
  long sequence_number;
  ObArray<int32_t> fields;
  ObArray<std::pair<ObString, ObString>> properties;

  static constexpr const char *TYPE = "type";
  static constexpr const char *SNAPSHOT_ID = "snapshot-id";
  static constexpr const char *SEQUENCE_NUMBER = "sequence-number";
  static constexpr const char *FIELDS = "fields";
  static constexpr const char *PROPERTIES = "properties";
};

class StatisticsFile : public SpecWithAllocator
{
public:
  explicit StatisticsFile(ObIAllocator &allocator);
  int init_from_json(const ObJsonObject &json_object);
  int assign(const StatisticsFile &other);

  long snapshot_id;
  ObString statistics_path;
  int64_t file_size_in_bytes;
  int64_t file_footer_size_in_bytes;
  std::optional<ObString> key_metadata;
  ObArray<BlobMetadata *> blob_metadata;

  static constexpr const char *SNAPSHOT_ID = "snapshot-id";
  static constexpr const char *STATISTICS_PATH = "statistics-path";
  static constexpr const char *FILE_SIZE_IN_BYTES = "file-size-in-bytes";
  static constexpr const char *FILE_FOOTER_SIZE_IN_BYTES = "file-footer-size-in-bytes";
  static constexpr const char *KEY_METADATA = "key-metadata";
  static constexpr const char *BLOB_METADATA = "blob-metadata";

private:
  int parse_blob_metadata_(const common::ObJsonObject &json_object);
};

} // namespace iceberg

} // namespace sql
} // namespace oceanbase

#endif // STATISTICS_H
