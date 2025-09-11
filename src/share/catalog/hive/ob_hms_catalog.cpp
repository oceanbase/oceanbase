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

#include "ob_hive_metastore.h"
#include "ob_hms_catalog.h"
#include "ob_hms_client_pool.h"
#include "share/catalog/ob_catalog_location_schema_provider.h"
#include "sql/table_format/hive/ob_hive_table_metadata.h"
#include "sql/table_format/iceberg/ob_iceberg_table_metadata.h"
#include "ob_hive_catalog_stat_helper.h"
#include "ob_iceberg_catalog_stat_helper.h"

namespace oceanbase
{
namespace share
{
ObHMSCatalog::~ObHMSCatalog()
{
  if (OB_NOT_NULL(client_)) {
    // Return client to pool through factory singleton with tenant_id and catalog_id
    client_->release();
    client_ = nullptr;
  }
}

int ObHMSCatalog::do_init(const common::ObString &properties)
{
  int ret = OB_SUCCESS;
  ObHMSCatalogProperties hms_properties_;
  if (OB_FAIL(ob_write_string(allocator_, properties, properties_, true /*c_style*/))) {
    LOG_WARN("failed to write properties", K(ret), K(properties));
  } else if (OB_FAIL(hms_properties_.load_from_string(properties_, allocator_))) {
    LOG_WARN("fail to init hive properties", K(ret), K_(properties));
  } else if (OB_FAIL(hms_properties_.decrypt(allocator_))) {
    LOG_WARN("failed to decrypt", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator_, hms_properties_.uri_, uri_, true /*c_style*/))) {
    LOG_WARN("failed to write metastore uri", K(ret), K(hms_properties_.uri_));
  }
  LOG_TRACE(" ObHMSCatalog do_init end", K(ret), K_(properties));

  if (OB_FAIL(ret)) {
  } else {
    ObHMSClientPoolMgr *hms_client_pool_mgr = MTL(ObHMSClientPoolMgr *);
    if (OB_ISNULL(hms_client_pool_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("hms client pool mgr is null", K(ret));
    } else if (OB_FAIL(hms_client_pool_mgr
                           ->get_client(MTL_ID(), catalog_id_, uri_, properties_, client_))) {
      LOG_WARN("failed to get client from pool", K(ret), K_(catalog_id), K_(uri));
    }
  }
  return ret;
}

int ObHMSCatalog::list_namespace_names(ObIArray<common::ObString> &ns_names)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(client_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("hive metastore client is null", K(ret));
  } else if (OB_FAIL(client_->list_db_names(allocator_, ns_names))) {
    LOG_WARN("failed to list namespace(database) name", K(ret));
  }
  return ret;
}

int ObHMSCatalog::list_table_names(const common::ObString &ns_name,
                                   const ObNameCaseMode case_mode,
                                   ObIArray<common::ObString> &tbl_names)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(client_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("hive metastore client is null", K(ret));
  } else if (OB_FAIL(client_->list_table_names(ns_name, allocator_, tbl_names))) {
    LOG_WARN("failed to list table names", K(ret));
  }
  return ret;
}

int ObHMSCatalog::fetch_namespace_schema(const uint64_t database_id,
                                         const common::ObString &ns_name,
                                         const ObNameCaseMode case_mode,
                                         share::schema::ObDatabaseSchema *&database_schema)
{
  int ret = OB_SUCCESS;
  database_schema = NULL;
  if (OB_ISNULL(database_schema = OB_NEWx(schema::ObDatabaseSchema, &allocator_, &allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory for database schema", K(ret));
  } else if (OB_FALSE_IT(database_schema->set_tenant_id(tenant_id_))) {
  } else if (OB_FALSE_IT(database_schema->set_database_id(database_id))) {
  } else if (OB_ISNULL(client_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("hive metastore client is null", K(ret));
  } else if (OB_FAIL(client_->get_database(ns_name, case_mode, *database_schema))) {
    LOG_WARN("failed to fetch namespace(database) schema", K(ret), K(ns_name));
  }
  return ret;
}

int ObHMSCatalog::fetch_lake_table_metadata(ObIAllocator &allocator,
                                            const uint64_t database_id,
                                            const uint64_t table_id,
                                            const common::ObString &ns_name,
                                            const common::ObString &tbl_name,
                                            const ObNameCaseMode case_mode,
                                            ObILakeTableMetadata *&table_metadata)
{
  int ret = OB_SUCCESS;
  ObLakeTableFormat table_format = ObLakeTableFormat::INVALID;
  ObArenaAllocator tmp_allocator;
  // hive 表返回表的 location, iceberg 表返回 metadata 的 location
  ObString table_location;
  ObString storage_access_info;
  Apache::Hadoop::Hive::Table original_table;

  if (OB_ISNULL(client_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("hive metastore client is null", K(ret));
  } else if (OB_FAIL(client_->get_table(ns_name, tbl_name, case_mode, original_table))) {
    LOG_WARN("failed to get original table", K(ret), K(ns_name), K(tbl_name));
  } else if (0 == strcmp(VIEW_TABLE_TYPE, original_table.tableType.c_str())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support view table", K(ret), K(ns_name), K(tbl_name));
  } else {
    std::map<String, String>::iterator iter;
    iter = original_table.parameters.find(TRANSACTIONAL);
    if (iter != original_table.parameters.end()) {
      String bool_val = iter->second;
      if (0 == strcmp("true", bool_val.c_str())) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not support transactional table", K(ret), K(ns_name), K(tbl_name));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(deduce_lake_table_format(tmp_allocator,
                                              original_table,
                                              table_format,
                                              table_location))) {
    LOG_WARN("failed to deduce lake table format", K(ret));
  } else if (OB_UNLIKELY(table_location.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("empty table location", K(ret));
  } else if (OB_NOT_NULL(location_schema_provider_)) {
    if (OB_FAIL(location_schema_provider_->get_access_info_by_path(allocator_,
                                                                   tenant_id_,
                                                                   table_location,
                                                                   storage_access_info))) {
      LOG_WARN("failed to get storage access info", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (ObLakeTableFormat::HIVE == table_format) {
    Strings partition_names;
    PartitionValuesRows part_values_rows;
    hive::ObHiveTableMetadata *hive_table_metadata = NULL;
    if (OB_ISNULL(hive_table_metadata
                  = OB_NEWx(hive::ObHiveTableMetadata, &allocator, allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc failed", K(ret));
    } else if (OB_FAIL(hive_table_metadata->init(tenant_id_,
                                                 catalog_id_,
                                                 database_id,
                                                 table_id,
                                                 ns_name,
                                                 tbl_name,
                                                 case_mode))) {
      LOG_WARN("failed to init hive table metadata", K(ret));
    } else if (OB_FAIL(hive_table_metadata->setup_tbl_schema(tenant_id_,
                                                             database_id,
                                                             table_id,
                                                             original_table,
                                                             properties_,
                                                             uri_,
                                                             storage_access_info))) {
      LOG_WARN("failed to setup table schema in hive table metadata", K(ret));
    } else {
      table_metadata = hive_table_metadata;
    }
  } else if (ObLakeTableFormat::ICEBERG == table_format) {
    iceberg::ObIcebergTableMetadata *iceberg_table_metadata = NULL;
    if (OB_ISNULL(iceberg_table_metadata
                  = OB_NEWx(iceberg::ObIcebergTableMetadata, &allocator, allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for iceberg table metadata", K(ret));
    } else if (OB_FAIL(iceberg_table_metadata->init(tenant_id_,
                                                    catalog_id_,
                                                    database_id,
                                                    table_id,
                                                    ns_name,
                                                    tbl_name,
                                                    case_mode))) {
      LOG_WARN("failed to init iceberg table metadata", K(ret));
    } else if (OB_FAIL(iceberg_table_metadata->set_access_info(storage_access_info))) {
      LOG_WARN("failed to set access info", K(ret));
    } else if (OB_FAIL(iceberg_table_metadata->load_by_metadata_location(table_location))) {
      LOG_WARN("failed to load iceberg metadata.json", K(ret));
    } else {
      table_metadata = iceberg_table_metadata;
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support table format to get lake table metadata", K(ret), K(table_format));
  }
  return ret;
}

int ObHMSCatalog::fetch_table_statistics(ObIAllocator &allocator,
                                         sql::ObSqlSchemaGuard &sql_schema_guard,
                                         const ObILakeTableMetadata *table_metadata,
                                         const ObIArray<ObString> &partition_values,
                                         const ObIArray<ObString> &column_names,
                                         ObOptExternalTableStat *&external_table_stat,
                                         ObIArray<ObOptExternalColumnStat *> &external_table_column_stats)
{
  int ret = OB_SUCCESS;
  // do some argument check
  if (OB_ISNULL(table_metadata)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table metadata", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (ObLakeTableFormat::HIVE == table_metadata->get_format_type()) {
      if (OB_FAIL(fetch_hive_table_statistics(allocator,
                                              table_metadata,
                                              partition_values,
                                              column_names,
                                              external_table_stat,
                                              external_table_column_stats))) {
        LOG_WARN("failed to fetch hive table statistics", K(ret));
      }
    } else if (ObLakeTableFormat::ICEBERG == table_metadata->get_format_type()) {
      if (OB_FAIL(fetch_iceberg_table_statistics(allocator,
                                                 table_metadata,
                                                 partition_values,
                                                 column_names,
                                                 external_table_stat,
                                                 external_table_column_stats))) {
        LOG_WARN("failed to fetch iceberg table statistics", K(ret));
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported table format in hms catalog",
               K(ret),
               K(table_metadata->get_format_type()));
    }
  }
  return ret;
}

int ObHMSCatalog::fetch_partitions(ObIAllocator &allocator,
                                   const ObILakeTableMetadata *table_metadata,
                                   Partitions &partitions)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(client_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("hive metastore client is null", K(ret));
  } else if (OB_UNLIKELY(ObLakeTableFormat::HIVE != table_metadata->get_format_type())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("only hive format can fetch partition values", K(ret));
  } else if (OB_FAIL(client_->list_partitions(table_metadata->namespace_name_,
                                              table_metadata->table_name_,
                                              partitions))) {
    LOG_WARN("failed to get table schema", K(ret));
  }
  return ret;
}

int ObHMSCatalog::get_cache_refresh_interval_sec(int64_t &sec)
{
  int ret = OB_SUCCESS;
  ObHMSCatalogProperties hms_properties;
  if (OB_FAIL(hms_properties.load_from_string(properties_, allocator_))) {
    LOG_WARN("fail to init hive properties", K(ret), K_(properties));
  } else if (OB_FAIL(hms_properties.decrypt(allocator_))) {
    LOG_WARN("failed to decrypt", K(ret));
  } else {
    sec = hms_properties.get_cache_refresh_interval_sec();
  }

  return ret;
}

int ObHMSCatalog::fetch_latest_table_schema_version(const common::ObString &ns_name,
                                                    const common::ObString &tbl_name,
                                                    const ObNameCaseMode case_mode,
                                                    int64_t &schema_version)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(client_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("hive metastore client is null", K(ret));
  } else if (OB_FAIL(client_->get_latest_schema_version(ns_name,
                                                        tbl_name,
                                                        case_mode,
                                                        schema_version))) {
    LOG_WARN("failed to get table schema", K(ret));
  }
  return ret;
}

int ObHMSCatalog::deduce_lake_table_format(ObIAllocator &allocator,
                                           Apache::Hadoop::Hive::Table &hive_table,
                                           ObLakeTableFormat &table_format,
                                           ObString &metadata_location)
{
  int ret = OB_SUCCESS;
  table_format = ObLakeTableFormat::INVALID;
  metadata_location.reset();
  std::map<std::string, std::string> &parameters = hive_table.parameters;
  const std::map<std::string, std::string>::iterator &iter
      = parameters.find(std::string(ICEBERG_METADATA_LOCATION));
  if (iter != parameters.end()) {
    table_format = ObLakeTableFormat::ICEBERG;
    OZ(ob_write_string(allocator, ObString(iter->second.c_str()), metadata_location, true));
  } else {
    table_format = ObLakeTableFormat::HIVE;
    OZ(ob_write_string(allocator,
                       ObString(hive_table.sd.location.c_str()),
                       metadata_location,
                       true));
  }
  return ret;
}

int ObHMSCatalog::fetch_hive_table_statistics(
    ObIAllocator &allocator,
    const ObILakeTableMetadata *table_metadata,
    const ObIArray<ObString> &partition_values,
    const ObIArray<ObString> &column_names,
    ObOptExternalTableStat *&external_table_stat,
    ObIArray<ObOptExternalColumnStat *> &external_table_column_stats)
{
  int ret = OB_SUCCESS;

  // Use the statistics helper to fetch and convert Hive statistics
  ObHiveCatalogStatHelper stat_helper(allocator);
  if (OB_ISNULL(client_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("hive metastore client is null", K(ret));
  } else if (OB_FAIL(stat_helper.fetch_hive_table_statistics(client_,
                                                             table_metadata,
                                                             partition_values,
                                                             column_names,
                                                             external_table_stat,
                                                             external_table_column_stats))) {
    LOG_WARN("failed to fetch hive table statistics via helper", K(ret));
  }

  return ret;
}

int ObHMSCatalog::fetch_iceberg_table_statistics(
    ObIAllocator &allocator,
    const ObILakeTableMetadata *table_metadata,
    const ObIArray<ObString> &partition_values,
    const ObIArray<ObString> &column_names,
    ObOptExternalTableStat *&external_table_stat,
    ObIArray<ObOptExternalColumnStat *> &external_table_column_stats)
{
  int ret = OB_SUCCESS;

  // Use the iceberg statistics helper to fetch and convert Iceberg statistics
  ObIcebergCatalogStatHelper stat_helper(allocator);
  if (OB_FAIL(stat_helper.fetch_iceberg_table_statistics(table_metadata,
                                                         partition_values,
                                                         column_names,
                                                         external_table_stat,
                                                         external_table_column_stats))) {
    LOG_WARN("failed to fetch iceberg table statistics via helper", K(ret));
  }

  return ret;
}




} // namespace share
} // namespace oceanbase
