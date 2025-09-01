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
