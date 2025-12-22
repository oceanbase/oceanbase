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

#include "sql/table_format/odps/ob_odps_table_metadata.h"

#include "share/schema/ob_schema_utils.h"

namespace oceanbase
{

namespace sql
{

namespace odps
{

share::ObLakeTableFormat ObODPSTableMetadata::get_format_type() const
{
  return share::ObLakeTableFormat::ODPS;
}

int64_t ObODPSTableMetadata::get_convert_size() const
{
  return sizeof(ObODPSTableMetadata) + table_schema_.get_convert_size();
}

int ObODPSTableMetadata::assign(const ObODPSTableMetadata &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    if (OB_FAIL(ObILakeTableMetadata::assign(other))) {
      LOG_WARN("failed to assign ObILakeTableMetadata", K(ret));
    } else {
      OZ(table_schema_.assign(other.table_schema_));
    }
  }
  return ret;
}

int ObODPSTableMetadata::do_build_table_schema(std::optional<int32_t> schema_id, /*not used*/
                                               std::optional<int64_t> snapshot_id, /*not used*/
                                               share::schema::ObTableSchema *&table_schema)
{
  int ret = OB_SUCCESS;
  OZ(share::schema::ObSchemaUtils::alloc_schema(allocator_, table_schema_, table_schema));
  return ret;
}

int ObODPSTableMetadata::get_inner_table_schema(share::schema::ObTableSchema *&table_schema)
{
  int ret = OB_SUCCESS;
  table_schema = &table_schema_;
  return ret;
}

} // namespace odps

} // namespace sql

} // namespace oceanbase