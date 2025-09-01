#ifdef OB_BUILD_CPP_ODPS
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

#include <odps/odps_table.h>

namespace oceanbase
{
using namespace common;
namespace share
{
using namespace apsara::odps;
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
    if (OB_ISNULL(jni_catalog_ptr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("jni catalog ptr is null", K(ret));
    } else if (OB_FAIL(jni_catalog_ptr_->do_query_table_info(allocator_, tbl_name, schema_version))) {
      LOG_WARN("failed to query table info", K(ret));
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
      schema_version = table_ptr->GetLastModifiedTime();
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
  ObODPSGeneralFormat format;
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
    format.access_type_ = properties_.access_type_;
    format.access_id_ = properties_.access_id_;
    format.access_key_ = properties_.access_key_;
    format.sts_token_ = properties_.sts_token_;
    format.endpoint_ = properties_.endpoint_;
    format.tunnel_endpoint_ = properties_.tunnel_endpoint_;
    format.project_ = properties_.project_;
    format.schema_ = ns_name;
    format.table_ = tbl_name;
    format.quota_ = properties_.quota_;
    format.compression_code_ = properties_.compression_code_;
    format.region_ = properties_.region_;
    if (OB_FAIL(format.encrypt())) {
      LOG_WARN("failed to encrypt format", K(ret), K(format));
    } else if (OB_FAIL(convert_odps_format_to_str_properties_(format, format_str))) {
      LOG_WARN("failed to convert format", K(ret));
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
      odps_table_metadata->lake_table_metadata_version_ = latest_schema_version;
    }
  }

  if (OB_SUCC(ret)) {
    if (GCONF._use_odps_jni_connector) {
#ifdef OB_BUILD_JNI_ODPS
      ObODPSJNITableRowIterator odps_jni_driver;
      if (OB_FAIL(odps_jni_driver.init_jni_schema_scanner(format, THIS_WORKER.get_session()))) {
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
#elif
      ret = OB_NOT_SUPPORTED;
#endif
    } else {
#ifdef OB_BUILD_CPP_ODPS
      ObODPSTableRowIterator odps_driver;
      if (OB_FAIL(odps_driver.init_tunnel(format))) {
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
#elif
      ret = OB_NOT_SUPPORTED
#endif
    }
  }

  if (OB_SUCC(ret)) {
    table_metadata = odps_table_metadata;
  }

  return ret;
}

int ObOdpsCatalog::fetch_table_statistics(ObIAllocator &allocator,
                                          const ObILakeTableMetadata *table_metadata,
                                          const ObIArray<ObString> &partition_values,
                                          const ObIArray<ObString> &column_names,
                                          ObOptExternalTableStat *&external_table_stat,
                                          ObIArray<ObOptExternalColumnStat *> &external_table_column_stats)
{
  int ret = OB_NOT_SUPPORTED;
  // do some argument check
  if (OB_ISNULL(table_metadata)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table metadata", K(ret));
  } else if (OB_UNLIKELY(ObLakeTableFormat::ODPS != table_metadata->get_format_type())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("only support odps table metadata", K(ret));
  }

  if (OB_SUCC(ret)) {
    // todo down_cast<ODPSTableMetadata*>(table_metadata)
  }
  return ret;
}

int ObOdpsCatalog::convert_odps_format_to_str_properties_(const ObODPSGeneralFormat &odps_format, ObString &str)
{
  int ret = OB_SUCCESS;
  ObExternalFileFormat format;
  if (OB_FAIL(format.odps_format_.deep_copy(odps_format))) {
    LOG_WARN("deep copy failed", K(ret));
  } else if (OB_FALSE_IT(format.format_type_ = ObExternalFileFormat::ODPS_FORMAT)) {
  } else if (OB_FAIL(format.to_string_with_alloc(str, allocator_))) {
    LOG_WARN("failed to convert to string", K(ret));
  }
  return ret;
}

} // namespace share
} // namespace oceanbase
#endif
