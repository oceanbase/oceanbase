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

#define USING_LOG_PREFIX SHARE_SCHEMA

#include "share/schema/ob_iceberg_table_schema.h"

#include "sql/table_format/iceberg/spec/partition.h"

namespace oceanbase
{

namespace share
{

namespace schema
{

ObIcebergTableSchema::ObIcebergTableSchema(common::ObIAllocator *allocator)
    : ObTableSchema(allocator)
{
}

void ObIcebergTableSchema::reset()
{
  ObTableSchema::reset();
}

int ObIcebergTableSchema::assign(const ObIcebergTableSchema &src_schema)
{
  int ret = common::OB_SUCCESS;
  if (this != &src_schema) {
    reset();
    if (OB_FAIL(ObTableSchema::assign(src_schema))) {
      LOG_WARN("failed to assign table schema", K(ret));
    }
  }
  return ret;
}

int64_t ObIcebergTableSchema::get_convert_size() const
{
  int64_t convert_size = 0;
  convert_size += ObTableSchema::get_convert_size();
  convert_size += sizeof(ObIcebergTableSchema) - sizeof(ObTableSchema);
  return convert_size;
}

} // namespace schema

} // namespace share

} // namespace oceanbase