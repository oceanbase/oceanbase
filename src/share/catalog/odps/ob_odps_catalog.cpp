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
#include "share/catalog/odps/ob_odps_catalog.h"

#include "share/catalog/odps/ob_odps_catalog_utils.h"
#include "sql/engine/table/ob_odps_table_row_iter.h"
#include "sql/engine/table/ob_odps_jni_table_row_iter.h"
#include "sql/resolver/dml/ob_dml_resolver.h"
#include "sql/table_format/odps/ob_odps_table_metadata.h"
#include "share/external_table/ob_external_table_utils.h"
#include "share/stat/ob_opt_external_table_stat_builder.h"
#ifdef OB_BUILD_CPP_ODPS
#include <odps/odps_table.h>
#endif

namespace oceanbase
{
using namespace common;
namespace share
{
#ifdef OB_BUILD_CPP_ODPS
using namespace apsara::odps;
#endif

int ObOdpsCatalog::do_init(const ObString &properties)
{
  int ret = OB_SUCCESS;
  if (GCONF._use_odps_jni_connector) {
#ifdef OB_BUILD_JNI_ODPS
    if (OB_FAIL(ObJniConnector::java_env_init())) {
      LOG_WARN("failed to env init", K(ret));
    } else if (OB_FAIL(properties_.load_from_string(properties, allocator_))) {
      LOG_WARN("fail to init odps properties", K(ret), K(properties));
    } else if (OB_FAIL(properties_.decrypt(allocator_))) {
      LOG_WARN("failed to decrypt properties", K(ret));
    } else if (OB_ISNULL(jni_catalog_ptr_ = create_odps_jni_catalog())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to create jni catalog", K(ret));
    } else if (OB_FAIL(jni_catalog_ptr_->do_init(allocator_, properties_))) {
      LOG_WARN("failed to init jni catalog", K(ret));
    }
#else
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("jni flag not set in cmakelist", K(ret));
#endif
  } else {
#ifdef OB_BUILD_CPP_ODPS
  try {
    if (OB_FAIL(properties_.load_from_string(properties, allocator_))) {
      LOG_WARN("fail to init odps properties", K(ret), K(properties));
    } else if (OB_FAIL(properties_.decrypt(allocator_))) {
      LOG_WARN("failed to decrypt properties", K(ret));
    } else if (OB_FAIL(ObODPSCatalogUtils::create_odps_conf(properties_, conf_))) {
      LOG_WARN("failed to create odps conf", K(ret));
    } else if (OB_ISNULL(
                  (odps_ = sdk::IODPS::Create(conf_, std::string(properties_.project_.ptr(), properties_.project_.length()))).get())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null ptr", K(ret));
    }
  } catch (const std::exception &ex) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("odps exception occurred", K(ret), K(ex.what()));
      LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
    }
  } catch (...) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("odps exception occurred", K(ret));
    }
  }
#else
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("open source mode not support odps c++ sdk", K(ret));
#endif
  }

  return ret;
}

int ObOdpsCatalog::list_namespace_names(ObIArray<ObString> &ns_names)
{
  int ret = OB_SUCCESS;
  OZ(ns_names.push_back("DEFAULT"));
  return ret;
}

int ObOdpsCatalog::list_table_names(const ObString &ns_name, const ObNameCaseMode case_mode, ObIArray<ObString> &tbl_names)
{
  int ret = OB_SUCCESS;
  if (GCONF._use_odps_jni_connector) {
#ifdef OB_BUILD_JNI_ODPS
    if (OB_ISNULL(jni_catalog_ptr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("jni catalog ptr is null", K(ret));
    } else if (OB_FAIL(jni_catalog_ptr_->do_query_table_list(allocator_, tbl_names))) {
      LOG_WARN("failed to query table list", K(ret));
    }
#else
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("jni flag not set in cmakelist", K(ret));
#endif
  } else {
#ifdef OB_BUILD_CPP_ODPS
    try {
    if (NULL == tables_ && OB_ISNULL((tables_ = odps_->GetTables()).get())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get tables", K(ret));
    } else {
      std::shared_ptr<sdk::Iterator<sdk::ODPSTableBasicInfo>> iter = tables_->ListTables(std::string(ns_name.ptr(), ns_name.length()));
      ObString temp_str;
      for (std::shared_ptr<sdk::ODPSTableBasicInfo> table_info; OB_SUCC(ret) && OB_NOT_NULL(table_info = iter->Next());) {
        OZ(ob_write_string(allocator_, ObString(table_info.get()->mTableName.length(), table_info.get()->mTableName.c_str()), temp_str));
        OZ(tbl_names.push_back(temp_str));
      }
    }
  } catch (const std::exception &ex) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("odps exception occurred", K(ret), K(ex.what()));
      LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
    }
  } catch (...) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("odps exception occurred", K(ret));
    }
  }
#else
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("open source mode not support odps c++ sdk", K(ret));
#endif
  }
  return ret;
}

int ObOdpsCatalog::fetch_latest_table_schema_version(const common::ObString &ns_name,
                                                     const common::ObString &tbl_name,
                                                     const ObNameCaseMode case_mode,
                                                     int64_t &schema_version)
{
  int ret = OB_SUCCESS;
  if (GCONF._use_odps_jni_connector) {
#ifdef OB_BUILD_JNI_ODPS
    int64_t _last_ddl_time_us = 0;
    int64_t _last_modification_time_us = 0;
    if (OB_ISNULL(jni_catalog_ptr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("jni catalog ptr is null", K(ret));
    } else if (OB_FAIL(jni_catalog_ptr_->do_query_table_info(allocator_, tbl_name, _last_ddl_time_us, _last_modification_time_us))) {
      LOG_WARN("failed to query table info", K(ret));
    } else {
      schema_version = _last_modification_time_us;
    }
#else
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("jni flag not set in cmakelist", K(ret));
#endif
  } else {
#ifdef OB_BUILD_CPP_ODPS
    UNUSED(ns_name);
    UNUSED(case_mode);
    sdk::IODPSTablePtr table_ptr = NULL;
    try {
      if (NULL == tables_ && OB_ISNULL((tables_ = odps_->GetTables()).get())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get tables", K(ret));
      } else if (OB_ISNULL(table_ptr = tables_->Get(std::string(tbl_name.ptr(), tbl_name.length())))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get table failed", K(ret));
      }
    } catch (apsara::odps::sdk::OdpsException &ex) {
      if (OB_SUCC(ret)) {
        if (apsara::odps::sdk::NO_SUCH_OBJECT == ex.GetErrorCode() || apsara::odps::sdk::NO_SUCH_TABLE == ex.GetErrorCode()) {
          ret = OB_TABLE_NOT_EXIST;
          LOG_WARN("odps table not found", K(ret), K(ex.what()), KP(this));
        } else {
          ret = OB_ODPS_ERROR;
          LOG_WARN("caught exception when call external driver api", K(ret), K(ex.what()), KP(this));
          LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
        }
      }
    } catch (const std::exception &ex) {
      if (OB_SUCC(ret)) {
        ret = OB_ODPS_ERROR;
        LOG_WARN("odps exception occurred", K(ret), K(ex.what()));
        LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
      }
    } catch (...) {
      if (OB_SUCC(ret)) {
        ret = OB_ODPS_ERROR;
        LOG_WARN("odps exception occurred", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      schema_version = table_ptr->GetLastModifiedTime() * 1000;
    }
#else
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("open source mode not support odps c++ sdk", K(ret));
#endif
  }
  return ret;
}

int ObOdpsCatalog::fetch_namespace_schema(const uint64_t database_id,
                                          const ObString &ns_name,
                                          const ObNameCaseMode case_mode,
                                          ObDatabaseSchema *&database_schema)
{
  int ret = OB_SUCCESS;
  ObArray<ObString> ns_names;
  database_schema = NULL;
  if (OB_ISNULL(database_schema = OB_NEWx(ObDatabaseSchema, &allocator_, &allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory for database schema", K(ret));
  } else if (OB_FALSE_IT(database_schema->set_tenant_id(tenant_id_))) {
  } else if (OB_FALSE_IT(database_schema->set_database_id(database_id))) {
  } else if (OB_FAIL(list_namespace_names(ns_names))) {
    LOG_WARN("list_namespace_names() failed", K(ret));
  } else {
    bool is_found = false;
    for (int64_t i = 0; OB_SUCC(ret) && !is_found && i < ns_names.count(); i++) {
      ObString &tmp_str = ns_names.at(i);
      if (ObCharset::case_mode_equal(case_mode, tmp_str, ns_name)) {
        OZ(database_schema->set_database_name(tmp_str));
        is_found = true;
      }
    }
    if (!is_found) {
      ret = OB_ERR_BAD_DATABASE;
    }
  }
  return ret;
}

int ObOdpsCatalog::fetch_lake_table_metadata(ObIAllocator &allocator,
                                             const uint64_t database_id,
                                             const uint64_t table_id,
                                             const common::ObString &ns_name,
                                             const common::ObString &tbl_name,
                                             const ObNameCaseMode case_mode,
                                             ObILakeTableMetadata *&table_metadata)
{
  int ret = OB_SUCCESS;
  odps::ObODPSTableMetadata *odps_table_metadata = NULL;
  ObTableSchema *inner_table_schema;
  ObExternalFileFormat external_format;
  if (OB_ISNULL(odps_table_metadata = OB_NEWx(odps::ObODPSTableMetadata, &allocator, allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory for table metadata", K(ret));
  } else if (OB_FAIL(odps_table_metadata->init(tenant_id_,
                                               catalog_id_,
                                               database_id,
                                               table_id,
                                               ns_name,
                                               tbl_name,
                                               case_mode))) {
    LOG_WARN("odps table metadata init failed", K(ret));
  } else if (OB_FAIL(odps_table_metadata->get_inner_table_schema(inner_table_schema))) {
    LOG_WARN("failed to get inner table schema", K(ret));
  } else if (OB_ISNULL(inner_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get inner table schema", K(ret));
  } else {
    ObString format_str;
    int64_t latest_schema_version;
    if (OB_FAIL(get_odps_format_str_from_catalog_properties(allocator, properties_, ns_name, tbl_name,  properties_.api_mode_, format_str, external_format))) {
      LOG_WARN("failed to get odps format str from catalog properties", K(ret));
    } else if (OB_FAIL(inner_table_schema->set_table_name(tbl_name))) {
      LOG_WARN("failed to set table name", K(ret));
    } else if (OB_FAIL(inner_table_schema->set_external_properties(format_str))) {
      LOG_WARN("failed to set external properties", K(ret));
    } else if (OB_FAIL(fetch_latest_table_schema_version(ns_name,
                                                         tbl_name,
                                                         case_mode,
                                                         latest_schema_version))) {
      LOG_WARN("failed to get latest table schema version", K(ret));
    } else {
      inner_table_schema->set_tenant_id(tenant_id_);
      inner_table_schema->set_database_id(database_id);
      inner_table_schema->set_table_id(table_id);
      inner_table_schema->set_schema_version(latest_schema_version);
      inner_table_schema->set_lake_table_format(share::ObLakeTableFormat::ODPS);
      odps_table_metadata->lake_table_metadata_version_ = latest_schema_version;
    }
  }

  if (OB_SUCC(ret)) {
    if (GCONF._use_odps_jni_connector) {
#ifdef OB_BUILD_JNI_ODPS
      ObODPSJNITableRowIterator odps_jni_driver;
      if (OB_FAIL(odps_jni_driver.init_jni_schema_scanner(external_format.odps_format_, THIS_WORKER.get_session()))) {
        LOG_WARN("failed to init schema scanner", K(ret));
      } else if (OB_FAIL(odps_jni_driver.pull_data_columns())) {
        LOG_WARN("failed to pull data columns", K(ret));
      } else if (OB_FAIL(odps_jni_driver.pull_partition_columns())) {
        LOG_WARN("failed to pull partition columns", K(ret));
      } else {
        ObSEArray<sql::ObODPSJNITableRowIterator::MirrorOdpsJniColumn, 8> all_column_list;
        ObIArray<sql::ObODPSJNITableRowIterator::MirrorOdpsJniColumn> &nonpart_column_list = odps_jni_driver.get_mirror_nonpart_column_list();
        ObIArray<sql::ObODPSJNITableRowIterator::MirrorOdpsJniColumn> &partition_column_list = odps_jni_driver.get_mirror_partition_column_list();
        ObSEArray<ObString, 8> part_col_names;
        for (int64_t i = 0;  OB_SUCC(ret) && i < partition_column_list.count(); i++) {
          if(OB_FAIL(part_col_names.push_back(partition_column_list.at(i).name_))) {
            LOG_WARN("failed to push back partition column name", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
          // to nothing
        } else if (OB_FAIL(append(all_column_list, nonpart_column_list))) {
          LOG_WARN("failed to append nonpart column list", K(ret));
        } else if (OB_FAIL(append(all_column_list, partition_column_list))) {
          LOG_WARN("failed to append partition column list", K(ret));
        } else if (OB_FAIL(ObDMLResolver::build_column_schemas_for_odps(all_column_list, part_col_names, *inner_table_schema))) {
          LOG_WARN("failed to build column schemas for odps", K(ret));
        } else if (OB_FAIL(ObDMLResolver::set_partition_info_for_odps(*inner_table_schema, part_col_names))) {
          LOG_WARN("failed to set partition info for odps", K(ret));
        }
      }
#else
      ret = OB_NOT_SUPPORTED;
#endif
    } else {
#ifdef OB_BUILD_CPP_ODPS
      ObODPSTableRowIterator odps_driver;
      if (OB_FAIL(odps_driver.init_tunnel(external_format.odps_format_))) {
        LOG_WARN("failed to init tunnel", K(ret));
      } else if (OB_FAIL(odps_driver.pull_all_columns())) {
        LOG_WARN("failed to pull all column", K(ret));
      } else {
        ObIArray<ObString> &part_col_names = odps_driver.get_part_col_names();
        ObIArray<sql::ObODPSTableRowIterator::OdpsColumn> &column_list = odps_driver.get_column_list();
        // here will to create tunnel again, can be optimized
        if (OB_FAIL(ObDMLResolver::build_column_schemas_for_odps(column_list, part_col_names, *inner_table_schema))) {
          LOG_WARN("failed to build column schemas for odps", K(ret));
        } else if (OB_FAIL(ObDMLResolver::set_partition_info_for_odps(*inner_table_schema, part_col_names))) {
          LOG_WARN("failed to set partition info for odps", K(ret));
        }
      }
#else
      ret = OB_NOT_SUPPORTED;
#endif
    }
  }

  if (OB_SUCC(ret)) {
    table_metadata = odps_table_metadata;
  }

  return ret;
}


int ObOdpsCatalog::fetch_table_statistics(ObIAllocator &allocator,
                                          sql::ObSqlSchemaGuard &sql_schema_guard,
                                          const ObILakeTableMetadata *table_metadata,
                                          const ObIArray<ObString> &partition_values,
                                          const ObIArray<ObString> &column_names,
                                          ObOptExternalTableStat *&external_table_stat,
                                          ObIArray<ObOptExternalColumnStat *> &external_table_column_stats)
{
  int ret = OB_SUCCESS;

  // do some argument check
  if (OB_ISNULL(table_metadata)
  || OB_ISNULL(THIS_WORKER.get_session())
  || OB_ISNULL(THIS_WORKER.get_session()->get_cur_exec_ctx())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table metadata", K(ret));
  } else if (OB_UNLIKELY(ObLakeTableFormat::ODPS != table_metadata->get_format_type())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("only support odps table metadata", K(ret));
  }
  int64_t total_partition_count = 0;
  if (OB_SUCC(ret)) {
    external_table_stat = nullptr;
    external_table_column_stats.reset();
    const odps::ObODPSTableMetadata* odps_table_metadata = static_cast<const odps::ObODPSTableMetadata*>(table_metadata);
    ObString format_str;
    const ObTableSchema *table_schema = NULL;
    ObSQLSessionInfo *session = NULL;
    ObExecContext *exec_ctx = NULL;
    const uint64_t ref_table_id = table_metadata->table_id_;
    const uint64_t tenant_id = table_metadata->tenant_id_;
    int64_t row_count = 0;
    int64_t total_data_size = 0;
    ObString max_partition;
    int64_t max_partition_file_size = 0;
    int64_t max_row_count = 0;
    ObExternalFileFormat external_format;
    sql::ObODPSGeneralFormat::ApiMode api_mode = sql::ObODPSGeneralFormat::ApiMode::TUNNEL_API;
    if (properties_.api_mode_ == sql::ObODPSGeneralFormat::ApiMode::TUNNEL_API) {
      api_mode = sql::ObODPSGeneralFormat::ApiMode::TUNNEL_API;
    } else {
      api_mode = sql::ObODPSGeneralFormat::ApiMode::ROW;// 这里有点特殊
    }

    if (partition_values.count() == 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("partition values is empty", K(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(get_session_and_ctx(session, exec_ctx))) {
      LOG_WARN("failed to get session and ctx", K(ret));
    } else if (OB_FAIL(get_odps_format_str_from_catalog_properties(allocator, properties_, table_metadata->namespace_name_,
                                                          table_metadata->table_name_, api_mode, format_str, external_format))) {
      LOG_WARN("failed to get odps format str from catalog properties", K(ret));
    } else if (OB_FAIL(sql_schema_guard.get_table_schema(tenant_id, ref_table_id, table_schema))) {
      LOG_WARN("failed to get table schema", K(ret));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema is null", K(ret));
    }
    if (OB_FAIL(ret)){
    } else if (external_format.odps_format_.api_mode_ == sql::ObODPSGeneralFormat::ApiMode::TUNNEL_API) {
      // 现在有所有分区 和 过滤获得分区
      omt::ObTenantConfigGuard tenant_config(TENANT_CONF(table_metadata->tenant_id_));
      int64_t max_parttition_count_to_collect_statistic = 5;
      if (OB_LIKELY(tenant_config.is_valid())) {
        max_parttition_count_to_collect_statistic = tenant_config->_max_partition_count_to_collect_statistic;
      }
      ObSEArray<ObString, 5> partition_values_to_collect_statistic;
      if (partition_values.count() > max_parttition_count_to_collect_statistic) {
        // do nothing
        LOG_INFO("partition count is too large, do not collect all statistic", K(partition_values.count()), K(max_parttition_count_to_collect_statistic));
        for (int64_t i = 0; OB_SUCC(ret) && i < max_parttition_count_to_collect_statistic; ++i) {
          if (OB_FAIL(partition_values_to_collect_statistic.push_back(partition_values.at(i)))) {
            LOG_WARN("failed to push back partition value", K(ret));
          }
        }
      } else {
        if (OB_FAIL(partition_values_to_collect_statistic.assign(partition_values))) {
          LOG_WARN("failed to assign partition values", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        common::hash::ObHashMap<ObOdpsPartitionKey, int64_t> partition_str_to_file_size;
        // get size for odps partition
        OZ(ObExternalTableUtils::fetch_odps_partition_info(format_str, partition_values_to_collect_statistic, 1,
                            tenant_id, ref_table_id, 1, partition_str_to_file_size, allocator));
        for (int64_t i = 0; OB_SUCC(ret) && i < partition_values_to_collect_statistic.count(); ++i) {
          int64_t file_size = 0;
          OZ(partition_str_to_file_size.get_refactored(ObOdpsPartitionKey(ref_table_id, partition_values_to_collect_statistic.at(i)), file_size));
          if (file_size > max_partition_file_size) {
            max_partition_file_size = file_size;
            max_partition = partition_values_to_collect_statistic.at(i);
          }
          LOG_INFO("ODPS statistics catalog table partition file size", K(partition_values_to_collect_statistic.at(i)), K(file_size));
        }
        // 按照比例计算每个partition的row_count
        if (OB_SUCC(ret)) {
          if (max_partition_file_size == 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("max partition file size is 0", K(ret));
          }
        }
      }

      if (OB_SUCC(ret)) {
        if(!GCONF._use_odps_jni_connector) {
          #if defined(OB_BUILD_CPP_ODPS)
            // get row count for odps partition
            sql::ObODPSTableRowIterator odps_driver;
            if (OB_FAIL(sql::ObOdpsPartitionDownloaderMgr::init_odps_driver(0, session, format_str, odps_driver))) {
              LOG_WARN("failed to init odps driver", K(ret));
            } else if (OB_FAIL(sql::ObOdpsPartitionDownloaderMgr::fetch_row_count(max_partition, 0, odps_driver, max_row_count))) {
              LOG_WARN("failed to fetch row count", K(ret));
            } else if (OB_FAIL(sql::ObOdpsPartitionDownloaderMgr::fetch_row_count(ObString::make_empty_string(), 1, odps_driver, total_data_size))) {
              LOG_WARN("failed to fetch row count", K(ret));
            } else {
              total_partition_count = table_schema->get_partition_num();
            }
          #else
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("not support odps cpp connector", K(ret), K(GCONF._use_odps_jni_connector));
          #endif
        } else {
          #if defined(OB_BUILD_JNI_ODPS)
            ObODPSJNITableRowIterator odps_driver;
            if (OB_FAIL(sql::ObOdpsPartitionJNIDownloaderMgr::init_odps_driver(0, session, format_str, odps_driver))) {
              LOG_WARN("failed to init odps driver", K(ret));
            } else if (OB_FAIL(sql::ObOdpsPartitionJNIDownloaderMgr::fetch_row_count(max_partition, 0, odps_driver, max_row_count))) {
              LOG_WARN("failed to fetch row count", K(ret));
            } else if (OB_FAIL(sql::ObOdpsPartitionJNIDownloaderMgr::fetch_row_count(ObString::make_empty_string(), 1, odps_driver, total_data_size))) {
              LOG_WARN("failed to fetch row count", K(ret));
            } else {
              total_partition_count = table_schema->get_partition_num();
            }
            if (OB_FAIL(ret)) {
              LOG_WARN("failed to fetch row count", K(ret));
            }
          #else
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("not support odps jni connector", K(ret), K(GCONF._use_odps_jni_connector));
          #endif
        }

        if (OB_SUCC(ret)) {
          row_count = max_row_count * (total_data_size * 1.00 / max_partition_file_size);
          LOG_INFO("ODPS statistics catalog table row count estimate size", K(max_partition_file_size), K(max_row_count), K(total_data_size), K(row_count));
        }
      }
    } else { // Storage api mode 谓词过滤不加入
      if (GCONF._use_odps_jni_connector) {
        // use split by row
        ObString part_str = ObString::make_empty_string();
        if (partition_values.count() == 1) {
          ObSqlString part_spec_str;
          int64_t part_count = partition_values.count();
          for (int64_t i = 0; OB_SUCC(ret) && i < part_count; ++i) {
            const ObString &external_info = partition_values.at(i);
            if (0 == external_info.compare(ObExternalTableUtils::dummy_file_name())) {
              // do nothing
            } else if (OB_FAIL(part_spec_str.append(external_info))) {
              LOG_WARN("failed to append file url", K(ret), K(external_info));
            } else if (i < part_count - 1 && OB_FAIL(part_spec_str.append("#"))) {
              LOG_WARN("failed to append comma", K(ret));
            }
          }
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(ob_write_string(allocator, part_spec_str.string(), part_str, true))) {
            LOG_WARN("failed to write string", K(ret), K(part_spec_str));
          }
          OX(total_partition_count = 1);
        } else {
          OX(total_partition_count = table_schema->get_partition_num());
        }

        OZ(ObOdpsPartitionJNIDownloaderMgr::fetch_storage_row_count(
            THIS_WORKER.get_session(), part_str, format_str, row_count));
        LOG_INFO("fetch storage row count", K(row_count));
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not support row api", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      int64_t snapshot_timestamp_ms = common::ObTimeUtil::current_time() / 1000;
      ObOptExternalTableStatBuilder stat_builder;
      if (OB_FAIL(stat_builder.set_basic_info(table_metadata->tenant_id_,
                                              table_metadata->catalog_id_,
                                              table_metadata->namespace_name_,
                                              table_metadata->table_name_,
                                              ObString("")))) {
        LOG_WARN("failed to set basic info for table stat builder", K(ret));
      }  else if (OB_FAIL(stat_builder.set_stat_info(
            row_count, 1, total_data_size,
            snapshot_timestamp_ms))) { // last_analyzed
        LOG_WARN("failed to set stat info for table stat builder", K(ret));
      } else if (OB_FALSE_IT(stat_builder.add_partition_num(total_partition_count))) {
        // 如果是多个分区 odps 通过选中分区进行缩放
        // do nothing
      } else if (OB_FAIL(stat_builder.build(allocator, external_table_stat))) {
        LOG_WARN("failed to build external table stat", K(ret));
      }
    }
  }
  return ret;
}

int ObOdpsCatalog::get_session_and_ctx(ObSQLSessionInfo *&session, ObExecContext *&exec_ctx) {
  int ret = OB_SUCCESS;
  session = THIS_WORKER.get_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else {
    ObExecContext *exec_ctx = session->get_cur_exec_ctx();
    if (OB_ISNULL(exec_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("exec ctx is null", K(ret));
    }
  }
  return ret;
}

int ObOdpsCatalog::get_odps_format_str_from_catalog_properties(common::ObIAllocator &allocator_for_odps_format_str,
    const ObODPSCatalogProperties &properties, const ObString &ns_name,
    const ObString &tbl_name, sql::ObODPSGeneralFormat::ApiMode api_mode,
    ObString &odps_format_str,  ObExternalFileFormat& external_format)
{
  int ret = OB_SUCCESS;
  external_format.format_type_ = ObExternalFileFormat::ODPS_FORMAT;

  ObODPSGeneralFormat &format = external_format.odps_format_;
  format.schema_ = ns_name;
  format.table_ = tbl_name;

  format.access_type_ = properties.access_type_;
  format.access_id_ = properties.access_id_;
  format.access_key_ = properties.access_key_;
  format.sts_token_ = properties.sts_token_;
  format.endpoint_ = properties.endpoint_;
  format.tunnel_endpoint_ = properties.tunnel_endpoint_;
  format.project_ = properties.project_;
  format.quota_ = properties.quota_;
  format.compression_code_ = properties.compression_code_;
  format.region_ = properties.region_;
  format.api_mode_ = api_mode;

  if (OB_FAIL(format.encrypt())) {
      LOG_WARN("failed to encrypt format", K(ret), K(format));
  } else if (OB_FAIL(external_format.to_string_with_alloc(odps_format_str, allocator_for_odps_format_str))) {
    LOG_WARN("failed to convert to string", K(ret));
  }
  return ret;
}



int ObOdpsCatalogUtils::get_partition_odps_str_from_table_schema(common::ObIAllocator &allocator_for_partition_values,
                                              const ObTablePartitionInfo *table_partition_info,
                                              const ObTableSchema *table_schema,
                                              ObIArray<ObString> &partition_values)
{
  int ret = OB_SUCCESS;
  partition_values.reuse();
  if (OB_ISNULL(table_partition_info) || OB_ISNULL(table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_partition_info), K(table_schema));
  } else if (!table_schema->is_partitioned_table()) {
    partition_values.push_back(ObString(""));
  } else {
    ObSEArray<ObString, 4> partition_column_names;

    for (int64_t i = 0; OB_SUCC(ret) && i < table_schema->get_partition_key_column_num(); ++i) {
      ObString *partition_column_name = NULL;
      if (OB_ISNULL(partition_column_name = partition_column_names.alloc_place_holder())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for partition column name", K(ret));
      } else if (OB_FAIL(table_schema->get_part_key_column_name(i, *partition_column_name))) {
        LOG_WARN("failed to get partition column name", K(ret), K(i));
      }
    }
    // Get partition locations from table partition info
    const ObCandiTabletLocIArray &part_loc_info_array =
        table_partition_info->get_phy_tbl_location_info().get_phy_part_loc_info_list();
    // Extract partition names from partition ids
    for (int64_t i = 0; OB_SUCC(ret) && i < part_loc_info_array.count(); ++i) {
      const ObOptTabletLoc &part_loc = part_loc_info_array.at(i).get_partition_location();
      const int64_t partition_id = part_loc.get_partition_id();
      const ObPartition *partition = NULL;
      ObString *partition_value = NULL;

      if (OB_FAIL(table_schema->get_partition_by_part_id(partition_id,
                                                          CHECK_PARTITION_MODE_NORMAL,
                                                          partition))) {
        // do nothing as no partition
        LOG_WARN("failed to get partition by part id", K(ret), K(partition_id));
        ret = OB_SUCCESS;
      } else if (OB_ISNULL(partition)) {
        // do nothing as no partition
        LOG_WARN("partition is null", K(ret), K(partition_id));
        ret = OB_SUCCESS;
      } else if (OB_ISNULL(partition_value = partition_values.alloc_place_holder())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for partition value", K(ret));
      } else {
        const ObIArray<common::ObNewRow> &list_row_values = partition->get_list_row_values();
        if (OB_FAIL(ObOdpsCatalogUtils::construct_partition_values(allocator_for_partition_values,
                                                                  partition_column_names,
                                                                  list_row_values,
                                                                  *partition_value))) {
          LOG_WARN("failed to construct partition values", K(ret));
        }
      }
    }
  }
  return ret;
}


int ObOdpsCatalogUtils::construct_partition_values(common::ObIAllocator &allocator_for_partition_value,
    const common::ObIArray<common::ObString> &partition_column_names,
    const common::ObIArray<common::ObNewRow> &partition_row_values, common::ObString &partition_value)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  const int64_t buf_len = OB_MAX_PARTITION_EXPR_LENGTH;
  int64_t pos = 0;
  if (OB_UNLIKELY(partition_row_values.count() != 1 ||
                  partition_row_values.at(0).get_count() != partition_column_names.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column names count not match row values count",
        K(ret),
        K(partition_column_names.count()),
        K(partition_row_values.count()));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator_for_partition_value.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret), K(buf_len));
  } else {
    const ObNewRow &row = partition_row_values.at(0);
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_column_names.count(); ++i) {
      if (0 != i) {
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, ","))) {
          LOG_WARN("failed to add separator", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        const ObString &col_name = partition_column_names.at(i);

        if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%.*s=", col_name.length(), col_name.ptr()))) {
          LOG_WARN("failed to add column name", K(ret), K(col_name));
        } else {
          const ObObj &tmp_obj = row.get_cell(i);

          if (tmp_obj.is_string_type()) {
            ObString tmp_str = tmp_obj.get_string();
            if (OB_FAIL(tmp_obj.print_sql_literal(buf, buf_len, pos))) {
              LOG_WARN("failed to print varchar literal", K(ret), K(tmp_obj));
            }
          } else if (tmp_obj.is_int() || tmp_obj.is_int32() || tmp_obj.is_tinyint() || tmp_obj.is_uint64() ||
                     tmp_obj.is_uint32()) {
            if (OB_FAIL(databuff_printf(buf, buf_len, pos, "'%ld'", tmp_obj.get_int()))) {
              LOG_WARN("failed to print sql literal", K(ret), K(tmp_obj));
            }
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      partition_value.assign_ptr(buf, static_cast<int32_t>(pos));
    }
  }

  return ret;
}

} // namespace share
} // namespace oceanbase
