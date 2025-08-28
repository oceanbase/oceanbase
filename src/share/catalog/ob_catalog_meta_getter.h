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

#ifndef __SHARE_OB_CATALOG_META_GETTER_H__
#define __SHARE_OB_CATALOG_META_GETTER_H__

#include "share/catalog/ob_catalog_location_schema_provider.h"
#include "share/catalog/ob_external_catalog.h"
#include "share/schema/ob_schema_getter_guard.h"

namespace oceanbase
{
namespace share
{

class CatalogEntry
{
public:
  CatalogEntry()
  {
  }
  CatalogEntry(const uint64_t tenant_id, const uint64_t catalog_id, ObIExternalCatalog *catalog)
      : tenant_id_(tenant_id), catalog_id_(catalog_id), catalog_(catalog)
  {
  }
  TO_STRING_EMPTY();
  uint64_t tenant_id_ = OB_INVALID_ID;
  uint64_t catalog_id_ = OB_INVALID_ID;
  ObIExternalCatalog *catalog_ = NULL;
};

class ObCatalogMetaGetter final : public ObICatalogMetaGetter
{
public:
  ObCatalogMetaGetter(schema::ObSchemaGetterGuard &schema_getter_guard, ObIAllocator &allocator)
      : schema_getter_guard_(schema_getter_guard), location_schema_provider_(schema_getter_guard_),
        allocator_(allocator)
  {
  }

  ~ObCatalogMetaGetter() override;

  int list_namespace_names(const uint64_t tenant_id,
                           const uint64_t catalog_id,
                           common::ObIArray<common::ObString> &ns_names) override;
  int list_table_names(const uint64_t tenant_id,
                       const uint64_t catalog_id,
                       const common::ObString &ns_name,
                       const ObNameCaseMode case_mode,
                       common::ObIArray<common::ObString> &tbl_names) override;
  int fetch_namespace_schema(const uint64_t tenant_id,
                             const uint64_t catalog_id,
                             const uint64_t database_id,
                             const common::ObString &ns_name,
                             const ObNameCaseMode case_mode,
                             share::schema::ObDatabaseSchema *&database_schema) override;

  // table_schema's table_id/database_id should assign correct before call this function
  int fetch_lake_table_metadata(ObIAllocator &allocator,
                                const uint64_t tenant_id,
                                const uint64_t catalog_id,
                                const uint64_t database_id,
                                const uint64_t table_id,
                                const common::ObString &ns_name,
                                const common::ObString &tbl_name,
                                const ObNameCaseMode case_mode,
                                ObILakeTableMetadata *&table_metadata) override;

  int fetch_latest_table_schema_version(const uint64_t tenant_id,
                                        const uint64_t catalog_id,
                                        const common::ObString &ns_name,
                                        const common::ObString &tbl_name,
                                        const ObNameCaseMode case_mode,
                                        int64_t &schema_version);

  int fetch_table_statistics(ObIAllocator &allocator,
                             const ObILakeTableMetadata *table_metadata,
                             const ObIArray<ObString> &partition_values,
                             const ObIArray<ObString> &column_names,
                             ObOptExternalTableStat *&external_table_stat,
                             ObIArray<ObOptExternalColumnStat *> &external_table_column_stats) override;

  int fetch_partitions(ObIAllocator &allocator,
                       const ObILakeTableMetadata *table_metadata,
                       Partitions &partitions) override;

  int get_cache_refresh_interval_sec(const ObILakeTableMetadata *table_metadata,
                                     int64_t &sec) override;

private:
  schema::ObSchemaGetterGuard &schema_getter_guard_;
  ObCatalogLocationSchemaProvider location_schema_provider_;
  ObArray<CatalogEntry> created_catalog_;
  ObIAllocator &allocator_;

  int get_catalog_(const uint64_t tenant_id, const uint64_t catalog_id, ObIExternalCatalog *&catalog);
};

} // namespace share
} // namespace oceanbase

#endif // __SHARE_OB_CATALOG_META_GETTER_H__
