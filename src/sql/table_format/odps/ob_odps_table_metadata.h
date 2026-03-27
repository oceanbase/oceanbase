/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_ODPS_TABLE_METADATA_H
#define OB_ODPS_TABLE_METADATA_H

#include "share/catalog/ob_external_catalog.h"

namespace oceanbase
{
namespace sql
{

namespace odps
{

class ObODPSTableMetadata final : public share::ObILakeTableMetadata
{
public:
  explicit ObODPSTableMetadata(ObIAllocator &allocator)
      : share::ObILakeTableMetadata(allocator), table_schema_(&allocator)
  {
  }

  share::ObLakeTableFormat get_format_type() const override;

  int64_t get_convert_size() const override;

  int assign(const ObODPSTableMetadata &other);

  // todo
  // 暂时偷懒
  int get_inner_table_schema(share::schema::ObTableSchema *&table_schema);
protected:
  int do_build_table_schema(std::optional<int32_t> schema_id,
                            std::optional<int64_t> snapshot_id,
                            share::schema::ObTableSchema *&table_schema) override;

private:
  share::schema::ObTableSchema table_schema_;
};

} // namespace odps

} // namespace sql
} // namespace oceanbase

#endif // OB_ODPS_TABLE_METADATA_H
