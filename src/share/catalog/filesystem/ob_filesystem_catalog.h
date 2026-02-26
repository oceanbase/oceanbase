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

#ifndef OB_FILESYSTEM_CATALOG_H
#define OB_FILESYSTEM_CATALOG_H

#include "share/catalog/ob_catalog_properties.h"
#include "share/catalog/ob_external_catalog.h"

namespace oceanbase
{

namespace sql
{
namespace iceberg
{
class ObIcebergTableMetadata;
}
} // namespace sql

namespace share
{

class ObFileSystemCatalog final : public ObIExternalCatalog, ObIExternalTableMetadataOperations
{
public:
  explicit ObFileSystemCatalog(common::ObIAllocator &allocator) : allocator_(allocator) {};

  int list_namespace_names(common::ObIArray<common::ObString> &ns_names) override;

  int list_table_names(const common::ObString &db_name,
                       const ObNameCaseMode case_mode,
                       common::ObIArray<common::ObString> &tbl_names) override;

  int fetch_namespace_schema(const uint64_t database_id,
                             const common::ObString &ns_name,
                             const ObNameCaseMode case_mode,
                             share::schema::ObDatabaseSchema *&database_schema) override;

  int fetch_latest_table_schema_version(const common::ObString &ns_name,
                                        const common::ObString &tbl_name,
                                        const ObNameCaseMode case_mode,
                                        int64_t &schema_version) override;

  int fetch_lake_table_metadata(ObIAllocator &allocator,
                                const uint64_t database_id,
                                const uint64_t table_id,
                                const common::ObString &ns_name,
                                const common::ObString &tbl_name,
                                const ObNameCaseMode case_mode,
                                ObILakeTableMetadata *&table_metadata) override;

  int current(const ObString &ns_name,
              const ObString &tbl_name,
              ObILakeTableMetadata *&metadata) override;

  int commit(const ObString &ns_name,
             const ObString &table_name,
             const ObILakeTableMetadata *previous,
             const ObILakeTableMetadata *latest) override;
private:
  virtual int do_init(const common::ObString &properties) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObFileSystemCatalog);
  int deduce_table_format_(const ObString &tbl_path, ObLakeTableFormat &table_format);
  ObIAllocator &allocator_;
  ObString warehouse_;
  ObString access_info_;
};

} // namespace share

} // namespace oceanbase

#endif // OB_FILESYSTEM_CATALOG_H
