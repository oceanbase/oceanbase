/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_ICEBERG_TABLE_SCHEMA_H
#define OB_ICEBERG_TABLE_SCHEMA_H

#include "share/schema/ob_table_schema.h"
#include "sql/table_format/iceberg/spec/snapshot.h"

namespace oceanbase
{

namespace share
{

namespace schema
{

class ObIcebergTableSchema : public ObTableSchema
{
public:
  ObIcebergTableSchema(common::ObIAllocator *allocator);
  ~ObIcebergTableSchema() = default;
  void reset();
  int assign(const ObIcebergTableSchema &other);
  int64_t get_convert_size() const override;
};

} // namespace schema

} // namespace share

} // namespace oceanbase

#endif // OB_ICEBERG_TABLE_SCHEMA_H
