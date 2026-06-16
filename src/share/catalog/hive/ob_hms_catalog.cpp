/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX SHARE

#include "ob_hive_metastore.h"
#include "lib/file/ob_string_util.h"
#include "ob_hms_catalog.h"
#include "share/catalog/ob_catalog_location_schema_provider.h"
#include "sql/table_format/hive/ob_hive_table_metadata.h"
#include "sql/table_format/iceberg/ob_iceberg_table_metadata.h"
#include "ob_hive_catalog_stat_helper.h"
#include "ob_iceberg_catalog_stat_helper.h"
#include "share/catalog/rest/client/ob_catalog_client_pool.h"
#include "share/external_table/ob_external_table_utils.h"

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
  ObHMSCatalogProperties hms_properties;
  if (OB_FAIL(ob_write_string(allocator_, properties, properties_, true /*c_style*/))) {
    LOG_WARN("failed to write properties", K(ret), K(properties));
  } else if (OB_FAIL(hms_properties.load_from_string(properties_, allocator_))) {
    LOG_WARN("fail to init hive properties", K(ret), K_(properties));
  } else if (OB_FAIL(hms_properties.decrypt(allocator_))) {
    LOG_WARN("failed to decrypt", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator_, hms_properties.uri_, uri_, true /*c_style*/))) {
    LOG_WARN("failed to write metastore uri", K(ret), K(hms_properties.uri_));
  }
  LOG_TRACE(" ObHMSCatalog do_init end", K(ret), K_(properties));

  if (OB_FAIL(ret)) {
  } else {
    ObCatalogClientPoolMgr<ObHiveMetastoreClient> *client_pool_mgr = MTL(ObCatalogClientPoolMgr<ObHiveMetastoreClient> *);
    if (OB_ISNULL(client_pool_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("hms client pool mgr is null", K(ret));
    } else if (OB_FAIL(client_pool_mgr->get_client(MTL_ID(), catalog_id_, properties_, client_))) {
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

int ObHMSCatalog::fetch_latest_table(const common::ObString &ns_name,
                                     const common::ObString &tbl_name,
                                     const ObNameCaseMode case_mode,
                                     ApacheHive::Table &original_table)
{
  int ret = OB_SUCCESS;

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
  uint64_t location_object_id = OB_INVALID_ID;
  ObString location_object_sub_path;
  Apache::Hadoop::Hive::Table original_table;

  if (OB_FAIL(fetch_latest_table(ns_name, tbl_name, case_mode, original_table))) {
    LOG_WARN("failed to fetch latest table", K(ret), K(ns_name), K(tbl_name));
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
                                                                   storage_access_info,
                                                                   location_object_id,
                                                                   location_object_sub_path))) {
      LOG_WARN("failed to get storage access info", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (ObLakeTableFormat::HIVE == table_format) {
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
                                                             location_object_id,
                                                             location_object_sub_path))) {
      LOG_WARN("failed to setup table schema in hive table metadata", K(ret));
    } else {
      // Using last_modified_time as schema version for hive table.
      int64_t schema_version = -1L;
      std::map<String, String>::iterator iter;
      iter = original_table.parameters.find(LAST_MODIFIED_TIME);
      if (iter != original_table.parameters.end()) {
        String time_str = iter->second;
        if (OB_FAIL(handle_ddl_time(time_str, schema_version))) {
          LOG_WARN("failed to handle ddl time", K(ret));
        }
      }

      if (OB_FAIL(ret)) {
      } else {
        hive_table_metadata->lake_table_metadata_version_ = schema_version;
        table_metadata = hive_table_metadata;
        LOG_TRACE("set schema version to hive table metadata",
                 K(ret),
                 K(schema_version),
                 K(hive_table_metadata->lake_table_metadata_version_));
      }
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
    } else if (OB_FAIL(iceberg_table_metadata->set_access_info(storage_access_info))) {                    // used for read iceberg metadata.json
      LOG_WARN("failed to set access info", K(ret));
    } else if (OB_FALSE_IT(iceberg_table_metadata->set_location_object_id(location_object_id))) {          // used for set ObTableSchema
      LOG_WARN("failed to set location id", K(ret));
    } else if (OB_FAIL(iceberg_table_metadata->set_location_object_sub_path(location_object_sub_path))) {  // used for set ObTableSchema
      LOG_WARN("failed to set sub path", K(ret));
    } else if (OB_FAIL(iceberg_table_metadata->load_by_metadata_location(table_location))) {
      LOG_WARN("failed to load iceberg metadata.json", K(ret));
    } else {
      // iceberg table metadata will set schema version in load_by_metadata_location.
      table_metadata = iceberg_table_metadata;
      LOG_TRACE("set schema version to iceberg table metadata",
               K(ret),
               K(iceberg_table_metadata->lake_table_metadata_version_));
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
                                         ObIArray<ObOptCatalogTableStat *> &catalog_table_stats,
                                         ObIArray<ObOptCatalogColumnStat *> &catalog_table_column_stats)
{
  int ret = OB_SUCCESS;
  catalog_table_stats.reset();
  catalog_table_column_stats.reset();
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
                                              catalog_table_stats,
                                              catalog_table_column_stats))) {
        LOG_WARN("failed to fetch hive table statistics", K(ret));
      }
    } else if (ObLakeTableFormat::ICEBERG == table_metadata->get_format_type()) {
      ObOptCatalogTableStat *catalog_table_stat = nullptr;
      if (OB_FAIL(fetch_iceberg_table_statistics(allocator,
                                                 table_metadata,
                                                 partition_values,
                                                 column_names,
                                                 catalog_table_stat,
                                                 catalog_table_column_stats))) {
        LOG_WARN("failed to fetch iceberg table statistics", K(ret));
      } else if (OB_NOT_NULL(catalog_table_stat)
                 && OB_FAIL(catalog_table_stats.push_back(catalog_table_stat))) {
        LOG_WARN("failed to push back iceberg table stat", K(ret));
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
                                   const ObIArray<ObString> &part_col_names,
                                   ObIArray<common::ObCatalogExtPartitionInfo> &partition_infos)
{
  int ret = OB_SUCCESS;
  Partitions partitions;
  if (OB_ISNULL(client_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("hive metastore client is null", K(ret));
  } else if (OB_UNLIKELY(ObLakeTableFormat::HIVE != table_metadata->get_format_type()
                         && ObLakeTableFormat::ICEBERG != table_metadata->get_format_type())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("only hive or iceberg format can fetch partition values",
             K(ret),
             K(table_metadata->get_format_type()));
  } else if (ObLakeTableFormat::HIVE == table_metadata->get_format_type()) {
    if (OB_FAIL(fetch_hive_table_partitions(allocator,
                                            table_metadata,
                                            part_col_names,
                                            partition_infos))) {
      LOG_WARN("failed to fetch hive table partitions", K(ret));
    }
  } else if (ObLakeTableFormat::ICEBERG == table_metadata->get_format_type()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support to fetch partitions for iceberg table",
             K(ret),
             K(table_metadata->get_format_type()));
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
  UNUSED(ns_name);
  UNUSED(tbl_name);
  UNUSED(case_mode);
  UNUSED(schema_version);
  return OB_NOT_SUPPORTED;
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
    ObIArray<ObOptCatalogTableStat *> &catalog_table_stats,
    ObIArray<ObOptCatalogColumnStat *> &catalog_table_column_stats)
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
                                                             catalog_table_stats,
                                                             catalog_table_column_stats))) {
    LOG_WARN("failed to fetch hive table statistics via helper", K(ret));
  }

  return ret;
}

int ObHMSCatalog::fetch_iceberg_table_statistics(
    ObIAllocator &allocator,
    const ObILakeTableMetadata *table_metadata,
    const ObIArray<ObString> &partition_values,
    const ObIArray<ObString> &column_names,
    ObOptCatalogTableStat *&catalog_table_stat,
    ObIArray<ObOptCatalogColumnStat *> &catalog_table_column_stats)
{
  int ret = OB_SUCCESS;

  // Use the iceberg statistics helper to fetch and convert Iceberg statistics
  ObIcebergCatalogStatHelper stat_helper(allocator);
  if (OB_FAIL(stat_helper.fetch_iceberg_table_statistics(table_metadata,
                                                         partition_values,
                                                         column_names,
                                                         catalog_table_stat,
                                                         catalog_table_column_stats))) {
    LOG_WARN("failed to fetch iceberg table statistics via helper", K(ret));
  }

  return ret;
}

int ObHMSCatalog::alter_table_with_lock(const ObString &db_name,
                                        const ObString &tbl_name,
                                        const ObNameCaseMode case_mode,
                                        const ApacheHive::Table &new_table)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(client_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("hive metastore client is null", K(ret));
  } else if (OB_FAIL(client_->alter_table_with_lock(db_name, tbl_name, new_table, case_mode))) {
    LOG_WARN("failed to alter table", K(ret));
  }
  return ret;
}

int ObHMSCatalog::handle_ddl_time(String &time_str, int64_t &ddl_time)
{
  int ret = OB_SUCCESS;
  ddl_time = 0;
  if (!::obsys::ObStringUtil::is_int(time_str.c_str())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ddl time", K(ret));
  } else {
    ddl_time = ::obsys::ObStringUtil::str_to_int(time_str.c_str(), 0);
  }
  return ret;
}

int ObHMSCatalog::fetch_hive_table_partitions(ObIAllocator &allocator,
                                              const ObILakeTableMetadata *table_metadata,
                                              const ObIArray<ObString> &part_col_names,
                                              ObIArray<common::ObCatalogExtPartitionInfo> &partition_infos)
{
  int ret = OB_SUCCESS;
  Partitions partitions;
  if (OB_ISNULL(client_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("hive metastore client is null", K(ret));
  } else if (OB_ISNULL(table_metadata)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table metadata is null", K(ret));
  } else if (OB_UNLIKELY(ObLakeTableFormat::HIVE != table_metadata->get_format_type())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("only hive format can fetch partitions", K(ret), K(table_metadata->get_format_type()));
  } else if (OB_FAIL(client_->list_partitions(table_metadata->namespace_name_,
                                              table_metadata->table_name_,
                                              partitions))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (partitions.size() == 0) {
    // table without partitions, do nothing
  } else if (part_col_names.count() == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("partition column names is empty", K(ret));
  } else {
    const int64_t part_cols_count = part_col_names.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < partitions.size(); ++i) {
      const Apache::Hadoop::Hive::Partition &hive_part = partitions[i];
      common::ObCatalogExtPartitionInfo p_info;

      for (size_t j = 0; OB_SUCC(ret) && j < hive_part.values.size(); ++j) {
        ObString partition_value;
        OZ(ob_write_string(allocator, ObString(hive_part.values[j].c_str()), partition_value));
        OZ(p_info.partition_values_.push_back(partition_value));
      }
      if (OB_FAIL(ret)) {
      } else {
        ObSqlString part_name;
        for (int64_t k = 0; OB_SUCC(ret) && k < part_cols_count; ++k) {
          OZ(part_name.append(part_col_names.at(k)));
          OZ(part_name.append("="));
          OZ(part_name.append(p_info.partition_values_.at(k)));
          if (k != part_cols_count - 1) {
            OZ(part_name.append("/"));
          }
        }
        OZ(ob_write_string(allocator, part_name.string(), p_info.partition_));
      }
      OZ(ob_write_string(allocator, ObString(hive_part.sd.location.c_str()), p_info.path_));
      OZ(partition_infos.push_back(p_info));
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    const sql::hive::ObHiveTableMetadata *hive_table_metadata = nullptr;
    hive_table_metadata = static_cast<const sql::hive::ObHiveTableMetadata *>(table_metadata);
    if (OB_ISNULL(hive_table_metadata)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null hive table metadata", K(ret));
    } else {
      const schema::ObTableSchema &table_schema = hive_table_metadata->get_table_schema();
      const ObString &location = table_schema.get_external_file_location();
      const uint64_t location_id = table_schema.get_external_location_id();
      ObString access_info;
      if (OB_NOT_NULL(location_schema_provider_)
          && OB_FAIL(location_schema_provider_->get_access_info_by_id(
                 tenant_id_, location_id, access_info))) {
        LOG_WARN("failed to get access info by location id", K(ret),
                 K(location_id), K(tenant_id_));
      } else if (access_info.empty()) {
        access_info = table_schema.get_external_file_location_access_info();
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(fill_partition_stats(table_metadata,
                                              location,
                                              access_info,
                                              partition_infos))) {
        LOG_WARN("failed to fill partition stats", K(ret), K(location), K(access_info));
      } else {
        LOG_INFO("fill partition stats successfully", K(ret), K(location), K(access_info));
      }
    }
  }
  return ret;
}

int ObHMSCatalog::fill_partition_stats(const ObILakeTableMetadata *table_metadata,
                                       const ObString &location,
                                       const ObString &access_info,
                                       ObIArray<common::ObCatalogExtPartitionInfo> &partition_infos)
{
  int ret = OB_SUCCESS;
  if (table_metadata->get_format_type() == ObLakeTableFormat::ICEBERG) {
    // iceberg table does not need to set partition stats.
  } else {
    ObArray<ObString> partition_names;
    ObArray<int64_t> file_nums;
    ObArray<int64_t> data_sizes;
    ObArray<int64_t> modify_times;

    for (int64_t i = 0; OB_SUCC(ret) && i < partition_infos.count(); ++i) {
      OZ(partition_names.push_back(partition_infos.at(i).partition_));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObExternalTableUtils::fetch_external_table_simple_stats(location,
                                                                               access_info,
                                                                               partition_names,
                                                                               file_nums,
                                                                               data_sizes,
                                                                               modify_times))) {
      LOG_WARN("failed to get external table simple stats", K(ret));
    } else if (OB_UNLIKELY(file_nums.count() != partition_infos.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("stats count mismatch", K(ret), K(file_nums.count()), K(partition_infos.count()));
    } else {
      for (int64_t i = 0; i < partition_infos.count(); ++i) {
        partition_infos.at(i).file_num_ = file_nums.at(i);
        partition_infos.at(i).data_size_ = data_sizes.at(i);
        partition_infos.at(i).modify_ts_ = modify_times.at(i);
        partition_infos.at(i).schema_version_ = table_metadata->lake_table_metadata_version_;
      }
    }
  }
  return ret;
}

} // namespace share
} // namespace oceanbase
