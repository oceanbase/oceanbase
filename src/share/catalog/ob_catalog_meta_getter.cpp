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

#define USING_LOG_PREFIX SHARE

#include "share/catalog/ob_catalog_meta_getter.h"

#include "share/catalog/filesystem/ob_filesystem_catalog.h"
#include "share/catalog/hive/ob_hms_catalog.h"
#include "share/catalog/odps/ob_odps_catalog.h"

namespace oceanbase
{
namespace share
{

ObCatalogMetaGetter::~ObCatalogMetaGetter()
{
  for (int64_t i = 0; i < created_catalog_.count(); i++) {
    OB_DELETEx(ObIExternalCatalog, &allocator_, created_catalog_[i].catalog_);
  }
}

int ObCatalogMetaGetter::list_namespace_names(const uint64_t tenant_id,
                                              const uint64_t catalog_id,
                                              ObIArray<ObString> &ns_names)
{
  int ret = OB_SUCCESS;
  ObIExternalCatalog *catalog = nullptr;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !is_external_catalog_id(catalog_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(catalog_id));
  } else if (OB_FAIL(get_catalog_(tenant_id, catalog_id, catalog))) {
    LOG_WARN("failed to get catalog", K(ret), K(tenant_id), K(catalog_id));
  } else if (OB_ISNULL(catalog)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("catalog is nullptr", K(ret));
  } else {
    ret = catalog->list_namespace_names(ns_names);
  }
  return ret;
}

int ObCatalogMetaGetter::list_table_names(const uint64_t tenant_id,
                                          const uint64_t catalog_id,
                                          const common::ObString &ns_name,
                                          const ObNameCaseMode case_mode,
                                          common::ObIArray<common::ObString> &tbl_names)
{
  int ret = OB_SUCCESS;
  ObIExternalCatalog *catalog = nullptr;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !is_external_catalog_id(catalog_id)
                  || ns_name.empty() || OB_NAME_CASE_INVALID == case_mode)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid case mode", K(ret), K(tenant_id), K(catalog_id), K(ns_name), K(case_mode));
  } else if (OB_FAIL(get_catalog_(tenant_id, catalog_id, catalog))) {
    LOG_WARN("failed to get catalog", K(ret), K(tenant_id), K(catalog_id));
  } else if (OB_ISNULL(catalog)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("catalog is nullptr", K(ret));
  } else {
    ret = catalog->list_table_names(ns_name, case_mode, tbl_names);
  }
  return ret;
}

// database_schema's database_id should assign correct before call this function
int ObCatalogMetaGetter::fetch_namespace_schema(const uint64_t tenant_id,
                                                const uint64_t catalog_id,
                                                const uint64_t database_id,
                                                const common::ObString &ns_name,
                                                const ObNameCaseMode case_mode,
                                                share::schema::ObDatabaseSchema *&database_schema)
{
  int ret = OB_SUCCESS;
  ObIExternalCatalog *catalog = nullptr;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !is_external_catalog_id(catalog_id)
                  || ns_name.empty() || !is_external_object_id(database_id)
                  || OB_NAME_CASE_INVALID == case_mode)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",
             K(ret),
             K(tenant_id),
             K(catalog_id),
             K(database_id),
             K(ns_name),
             K(case_mode));
  } else if (OB_FAIL(get_catalog_(tenant_id, catalog_id, catalog))) {
    LOG_WARN("failed to get catalog", K(ret));
  } else if (OB_ISNULL(catalog)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("catalog is nullptr", K(ret));
  } else if (OB_FAIL(catalog->fetch_namespace_schema(database_id,
                                                     ns_name,
                                                     case_mode,
                                                     database_schema))) {
    LOG_WARN("failed to fetch namespace", K(ret));
  } else {
    database_schema->set_catalog_id(catalog_id);
    database_schema->set_charset_type(common::ObCharsetType::CHARSET_UTF8MB4);
    database_schema->set_collation_type(common::ObCollationType::CS_TYPE_UTF8MB4_GENERAL_CI);
  }
  return ret;
}

// table_id/database_id should assign correct before call this function
int ObCatalogMetaGetter::fetch_lake_table_metadata(ObIAllocator &allocator,
                                                   const uint64_t tenant_id,
                                                   const uint64_t catalog_id,
                                                   const uint64_t database_id,
                                                   const uint64_t table_id,
                                                   const common::ObString &ns_name,
                                                   const common::ObString &tbl_name,
                                                   const ObNameCaseMode case_mode,
                                                   ObILakeTableMetadata *&table_metadata)
{
  int ret = OB_SUCCESS;
  ObIExternalCatalog *catalog = nullptr;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !is_external_catalog_id(catalog_id)
                  || !is_external_object_id(database_id) || !is_external_object_id(table_id)
                  || ns_name.empty() || tbl_name.empty() || OB_NAME_CASE_INVALID == case_mode)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",
             K(ret),
             K(tenant_id),
             K(catalog_id),
             K(database_id),
             K(table_id),
             K(ns_name),
             K(tbl_name),
             K(case_mode));
  } else if (OB_FAIL(get_catalog_(tenant_id, catalog_id, catalog))) {
    LOG_WARN("failed to get catalog", K(ret));
  } else if (OB_ISNULL(catalog)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("catalog is nullptr", K(ret));
  } else if (OB_FAIL(catalog->fetch_lake_table_metadata(allocator,
                                                        database_id,
                                                        table_id,
                                                        ns_name,
                                                        tbl_name,
                                                        case_mode,
                                                        table_metadata))) {
    LOG_WARN("failed to fetch table metadata", K(ret));
  }
  return ret;
}

int ObCatalogMetaGetter::fetch_latest_table_schema_version(const uint64_t tenant_id,
                                                           const uint64_t catalog_id,
                                                           const common::ObString &ns_name,
                                                           const common::ObString &tbl_name,
                                                           const ObNameCaseMode case_mode,
                                                           int64_t &schema_version)
{
  int ret = OB_SUCCESS;
  ObIExternalCatalog *catalog = nullptr;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !is_external_catalog_id(catalog_id)
                  || ns_name.empty() || tbl_name.empty() || OB_NAME_CASE_INVALID == case_mode)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",
             K(ret),
             K(tenant_id),
             K(catalog_id),
             K(ns_name),
             K(tbl_name),
             K(case_mode));
  } else if (OB_FAIL(get_catalog_(tenant_id, catalog_id, catalog))) {
    LOG_WARN("failed to get catalog", K(ret));
  } else if (OB_ISNULL(catalog)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("catalog is nullptr", K(ret));
  } else if (OB_FAIL(catalog->fetch_latest_table_schema_version(ns_name,
                                                                tbl_name,
                                                                case_mode,
                                                                schema_version))) {
    LOG_WARN("fetch latest table schema version failed", K(ret));
  }
  return ret;
}

int ObCatalogMetaGetter::fetch_table_statistics(
    ObIAllocator &allocator,
    const ObILakeTableMetadata *table_metadata,
    const ObIArray<ObString> &partition_values,
    const ObIArray<ObString> &column_names,
    ObOptExternalTableStat *&external_table_stat,
    ObIArray<ObOptExternalColumnStat *> &external_table_column_stats)
{
  int ret = OB_SUCCESS;
  ObIExternalCatalog *catalog = nullptr;
  if (OB_ISNULL(table_metadata)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(
                 get_catalog_(table_metadata->tenant_id_, table_metadata->catalog_id_, catalog))) {
    LOG_WARN("failed to get catalog", K(ret));
  } else if (OB_ISNULL(catalog)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("catalog is nullptr", K(ret));
  } else if (OB_FAIL(catalog->fetch_table_statistics(allocator,
                                                     table_metadata,
                                                     partition_values,
                                                     column_names,
                                                     external_table_stat,
                                                     external_table_column_stats))) {
    LOG_WARN("failed to fetch table statistics", K(ret));
  }
  return ret;
}

int ObCatalogMetaGetter::fetch_partitions(ObIAllocator &allocator,
                                          const ObILakeTableMetadata *table_metadata,
                                          Partitions &partitions)
{
  int ret = OB_SUCCESS;
  ObIExternalCatalog *catalog = nullptr;
  if (OB_ISNULL(table_metadata)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(
                 get_catalog_(table_metadata->tenant_id_, table_metadata->catalog_id_, catalog))) {
    LOG_WARN("failed to get catalog", K(ret));
  } else if (OB_ISNULL(catalog)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("catalog is nullptr", K(ret));
  } else if (OB_FAIL(catalog->fetch_partitions(allocator, table_metadata, partitions))) {
    LOG_WARN("failed to fetch partition values", K(ret));
  }
  return ret;
}

int ObCatalogMetaGetter::get_cache_refresh_interval_sec(const ObILakeTableMetadata *table_metadata,
                                                        int64_t &sec)
{
  int ret = OB_SUCCESS;
  ObIExternalCatalog *catalog = nullptr;
  if (OB_ISNULL(table_metadata)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(
                 get_catalog_(table_metadata->tenant_id_, table_metadata->catalog_id_, catalog))) {
    LOG_WARN("failed to get catalog", K(ret));
  } else if (OB_ISNULL(catalog)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("catalog is nullptr", K(ret));
  } else if (OB_FAIL(catalog->get_cache_refresh_interval_sec(sec))) {
    LOG_WARN("failed to fetch partition values", K(ret));
  }
  return ret;
}

int ObCatalogMetaGetter::get_catalog_(const uint64_t tenant_id,
                                      const uint64_t catalog_id,
                                      ObIExternalCatalog *&catalog)
{
  int ret = OB_SUCCESS;
  catalog = NULL;

  for (int64_t i = 0; OB_SUCC(ret) && catalog == NULL && i < created_catalog_.size(); i++) {
    CatalogEntry &catalog_entry = created_catalog_[i];
    if (catalog_entry.tenant_id_ == tenant_id && catalog_entry.catalog_id_ == catalog_id) {
      catalog = catalog_entry.catalog_;
    }
  }

  if (OB_SUCC(ret) && catalog == NULL) {
    const schema::ObCatalogSchema *schema = nullptr;
    ObCatalogProperties::CatalogType catalog_type = ObCatalogProperties::CatalogType::INVALID_TYPE;
    if (OB_FAIL(schema_getter_guard_.get_catalog_schema_by_id(tenant_id, catalog_id, schema))) {
      LOG_WARN("failed to get catalog schema", K(ret), K(tenant_id), K(catalog_id));
    } else if (OB_ISNULL(schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema is nullptr", K(ret));
    } else if (OB_FAIL(ObCatalogProperties::parse_catalog_type(schema->get_catalog_properties_str(),
                                                               catalog_type))) {
      LOG_WARN("failed to parse catalog type", K(ret));
    } else {
      switch (catalog_type) {
        case ObCatalogProperties::CatalogType::ODPS_TYPE: {
#ifdef OB_BUILD_CPP_ODPS
          catalog = OB_NEWx(ObOdpsCatalog, &allocator_, allocator_);
          if (OB_ISNULL(catalog)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("alloc failed", K(ret));
          } else if (OB_FAIL(catalog->init(schema->get_tenant_id(),
                                           schema->get_catalog_id(),
                                           schema->get_catalog_properties()))) {
            LOG_WARN("failed to init odps catalog", K(ret));
          }
#else
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("ODPS CPP connector is not enabled", K(ret));
#endif
          break;
        }
        case ObCatalogProperties::CatalogType::FILESYSTEM_TYPE: {
          catalog = OB_NEWx(ObFileSystemCatalog, &allocator_, allocator_);
          if (OB_ISNULL(catalog)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("alloc failed", K(ret));
          } else if (OB_FAIL(catalog->init(schema->get_tenant_id(),
                                           schema->get_catalog_id(),
                                           schema->get_catalog_properties(),
                                           &location_schema_provider_))) {
            LOG_WARN("failed to init filesystem catalog", K(ret));
          }
          break;
        }
        case ObCatalogProperties::CatalogType::HMS_TYPE: {
          catalog = OB_NEWx(ObHMSCatalog, &allocator_, allocator_);
          if (OB_ISNULL(catalog)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("hive catalog alloc failed", K(ret));
          } else if (OB_FAIL(catalog->init(schema->get_tenant_id(),
                                           schema->get_catalog_id(),
                                           schema->get_catalog_properties(),
                                           &location_schema_provider_))) {
            LOG_WARN("failed to init hive catalog", K(ret));
          }
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid catalog type", K(ret), K(catalog_type));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(created_catalog_.push_back({tenant_id, catalog_id, catalog}))) {
        // If push back failed, we need to free the catalog.
        catalog->~ObIExternalCatalog();
        allocator_.free(catalog);
        catalog = nullptr;
        LOG_WARN("failed to push back catalog", K(ret));
      }
    }
  }
  return ret;
}

} // namespace share
} // namespace oceanbase
