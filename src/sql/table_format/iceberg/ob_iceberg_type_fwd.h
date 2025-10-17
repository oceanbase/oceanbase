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

#ifndef OB_ICEBERG_TYPE_FWD_H
#define OB_ICEBERG_TYPE_FWD_H

#include <cstdint>

namespace oceanbase
{
namespace sql
{
namespace iceberg
{

static constexpr int32_t INVALID_FIELD_ID = -1;
static constexpr int64_t INITIAL_SEQUENCE_NUMBER = 0L;
static constexpr int32_t DEFAULT_SCHEMA_ID = 0;
static constexpr const int32_t INITIAL_SPEC_ID = 0;
// IDs for partition fields start at 1000
static constexpr const int32_t PARTITION_DATA_ID_START = 1000;

class TableMetadata;
enum class FormatVersion;
class Schema;
class PartitionSpec;
class PartitionField;
class Snapshot;
class StatisticsFile;

class ManifestFile;
class ManifestEntry;
class DataFile;
enum class DataFileContent;
enum class DataFileFormat
{
  INVALID = 0,
  AVRO,
  ORC,
  PARQUET,
  PUFFIN
};


class FileScanTask;

} // namespace iceberg
} // namespace sql
} // namespace oceanbase

#endif // OB_ICEBERG_TYPE_FWD_H