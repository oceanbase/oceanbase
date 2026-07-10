/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "share/ob_catalog_ext_partition_info.h"

namespace oceanbase
{
namespace common
{

int ObCatalogExtPartitionInfo::assign(const ObCatalogExtPartitionInfo &other)
{
  int ret = OB_SUCCESS;
  partition_ = other.partition_;
  path_ = other.path_;
  modify_ts_ = other.modify_ts_;
  data_size_ = other.data_size_;
  file_num_ = other.file_num_;
  schema_version_ = other.schema_version_;
  part_stattype_ = other.part_stattype_;
  iceberg_spec_id_ = other.iceberg_spec_id_;
  if (OB_FAIL(partition_values_.assign(other.partition_values_))) {
    LOG_WARN("failed to assign partition values", K(ret));
  } else if (OB_FAIL(iceberg_part_names_.assign(other.iceberg_part_names_))) {
    LOG_WARN("failed to assign iceberg partition names", K(ret));
  } else if (OB_FAIL(iceberg_part_sql_literals_.assign(other.iceberg_part_sql_literals_))) {
    LOG_WARN("failed to assign iceberg partition sql literals", K(ret));
  }
  return ret;
}

}  // namespace common
}  // namespace oceanbase
