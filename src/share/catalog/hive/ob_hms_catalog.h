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
#ifndef _SHARE_CATALOG_HIVE_OB_HIVE_CATALOG_H
#define _SHARE_CATALOG_HIVE_OB_HIVE_CATALOG_H

#include "share/catalog/hive/ob_hive_metastore.h"
#include "share/catalog/ob_catalog_properties.h"
#include "share/catalog/ob_external_catalog.h"

#include <optional>

namespace oceanbase
{
namespace share
{
static constexpr const char *VIEW_TABLE_TYPE = "VIRTUAL_VIEW";
static constexpr const char *TRANSACTIONAL = "transactional";

class ObHMSCatalog final : public ObIExternalCatalog
{
public:
  explicit ObHMSCatalog(common::ObIAllocator &allocator)
      : allocator_(allocator), properties_(), uri_(), client_(nullptr)
  {
  }

  ~ObHMSCatalog();
  ObCatalogProperties::CatalogType get_catalog_type() const override
  {
    return ObCatalogProperties::CatalogType::HMS_TYPE;
  }
  virtual int list_namespace_names(common::ObIArray<common::ObString> &ns_names) override;
  virtual int list_table_names(const common::ObString &ns_name,
                               const ObNameCaseMode case_mode,
                               common::ObIArray<common::ObString> &tb_names) override;
  virtual int fetch_namespace_schema(const uint64_t database_id,
                                     const common::ObString &ns_name,
                                     const ObNameCaseMode case_mode,
                                     share::schema::ObDatabaseSchema *&database_schema) override;
  virtual int fetch_lake_table_metadata(ObIAllocator &allocator,
                                        const uint64_t database_id,
                                        const uint64_t table_id,
                                        const common::ObString &ns_name,
                                        const common::ObString &tbl_name,
                                        const ObNameCaseMode case_mode,
                                        ObILakeTableMetadata *&table_metadata) override;
  int fetch_latest_table(const common::ObString &ns_name,
                         const common::ObString &tbl_name,
                         const ObNameCaseMode case_mode,
                         ApacheHive::Table &original_table);
  int fetch_table_statistics(ObIAllocator &allocator,
                             sql::ObSqlSchemaGuard &sql_schema_guard,
                             const ObILakeTableMetadata *table_metadata,
                             const ObIArray<ObString> &partition_values,
                             const ObIArray<ObString> &column_names,
                             ObOptExternalTableStat *&external_table_stat,
                             ObIArray<ObOptExternalColumnStat *> &external_table_column_stats) override;

  int fetch_partitions(ObIAllocator &allocator,
                       const ObILakeTableMetadata *table_metadata,
                       Partitions &partitions) override;

  virtual int fetch_latest_table_schema_version(const common::ObString &ns_name,
                                                const common::ObString &tbl_name,
                                                const ObNameCaseMode case_mode,
                                                int64_t &schema_version) override;

  int fetch_hive_table_statistics(ObIAllocator &allocator,
                                  const ObILakeTableMetadata *table_metadata,
                                  const ObIArray<ObString> &partition_values,
                                  const ObIArray<ObString> &column_names,
                                  ObOptExternalTableStat *&external_table_stat,
                                  ObIArray<ObOptExternalColumnStat *> &external_table_column_stats);

  int fetch_iceberg_table_statistics(ObIAllocator &allocator,
                                     const ObILakeTableMetadata *table_metadata,
                                     const ObIArray<ObString> &partition_values,
                                     const ObIArray<ObString> &column_names,
                                     ObOptExternalTableStat *&external_table_stat,
                                     ObIArray<ObOptExternalColumnStat *> &external_table_column_stats);

  int alter_table_with_lock(const ObString &db_name,
                            const ObString &tb_name,
                            const ObNameCaseMode case_mode,
                            const ApacheHive::Table &new_table);
  static constexpr const char *ICEBERG_METADATA_LOCATION = "metadata_location";
  static constexpr const char *TABLE_TYPE = "table_type";


  int get_cache_refresh_interval_sec(int64_t &sec);

private:
  virtual int do_init(const common::ObString &properties) override;

  static int deduce_lake_table_format(ObIAllocator &allocator,
                                      Apache::Hadoop::Hive::Table &hive_table,
                                      ObLakeTableFormat &table_format,
                                      ObString &metadata_location);



  common::ObIAllocator &allocator_;
  ObString properties_;
  ObString uri_;
  ObHiveMetastoreClient *client_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObHMSCatalog);
};
} // namespace share
} // namespace oceanbase

#endif /* _SHARE_CATALOG_HIVE_OB_HIVE_CATALOG_H  */
