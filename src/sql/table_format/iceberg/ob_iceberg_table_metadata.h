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

#ifndef OB_ICEBERG_TABLE_H
#define OB_ICEBERG_TABLE_H

#include "lib/allocator/ob_allocator.h"
#include "lib/string/ob_string.h"
#include "share/catalog/ob_external_catalog.h"
#include "share/ob_define.h"
#include "sql/table_format/iceberg/ob_iceberg_type_fwd.h"
#include "sql/table_format/iceberg/spec/table_metadata.h"

namespace oceanbase
{
namespace sql
{
namespace iceberg
{

class ObIcebergTableMetadata final : public share::ObILakeTableMetadata
{
public:
  explicit ObIcebergTableMetadata(ObIAllocator &allocator)
      : share::ObILakeTableMetadata(allocator), table_metadata_(allocator)
  {
  }
  share::ObLakeTableFormat get_format_type() const override;
  int assign(const ObIcebergTableMetadata &other);
  int64_t get_convert_size() const override;
  int set_access_info(const ObString &access_info);
  // used by filesystem catalog only
  int load_by_table_location(const ObString &table_location);
  int load_by_metadata_location(const ObString &metadata_location);
  int resolve_time_travel_info(const share::ObTimeTravelInfo *time_travel_info,
                               std::optional<int32_t> &schema_id,
                               std::optional<int64_t> &snapshot_id) override;

  ObString access_info_;
  TableMetadata table_metadata_;

protected:
  int do_build_table_schema(std::optional<int32_t> schema_id,
                            std::optional<int64_t> snapshot_id,
                            share::schema::ObTableSchema *&table_schema) override;
};

} // namespace iceberg
} // namespace sql

} // namespace oceanbase

#endif // OB_ICEBERG_TABLE_H
