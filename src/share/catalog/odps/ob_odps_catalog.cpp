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

#include <odps/odps_table.h>

namespace oceanbase
{
using namespace common;
namespace share
{
using namespace apsara::odps;
int ObOdpsCatalog::init(const ObString &properties)
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

int ObOdpsCatalog::fetch_basic_table_info(const common::ObString &ns_name,
                                          const common::ObString &tbl_name,
                                          const ObNameCaseMode case_mode,
                                          ObCatalogBasicTableInfo &table_info)
{
  int ret = OB_SUCCESS;
  if (GCONF._use_odps_jni_connector) {
#ifdef OB_BUILD_JNI_ODPS
    if (OB_ISNULL(jni_catalog_ptr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("jni catalog ptr is null", K(ret));
    } else if (OB_FAIL(jni_catalog_ptr_->do_query_table_info(allocator_, tbl_name, table_info))) {
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
      table_info.create_time_s = table_ptr->GetCreationTime();
      table_info.last_ddl_time_s = table_ptr->GetLastDDLTime();
      table_info.last_modification_time_s = table_ptr->GetLastModifiedTime();
    }
#else
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("open source mode not support odps c++ sdk", K(ret));
#endif
  }
  return ret;
}

int ObOdpsCatalog::fetch_namespace_schema(const ObString &ns_name, const ObNameCaseMode case_mode, ObDatabaseSchema &database_schema)
{
  int ret = OB_SUCCESS;
  ObArray<ObString> ns_names;
  if (OB_FAIL(list_namespace_names(ns_names))) {
    LOG_WARN("list_namespace_names() failed", K(ret));
  } else {
    bool is_found = false;
    for (const ObString &i : ns_names) {
      if (ObCharset::case_mode_equal(case_mode, i, ns_name)) {
        database_schema.set_database_name(i);
        is_found = true;
        break;
      }
    }
    if (!is_found) {
      ret = OB_ERR_BAD_DATABASE;
    }
  }
  return ret;
}

int ObOdpsCatalog::fetch_table_schema(const ObString &ns_name,
                                      const ObString &tbl_name,
                                      const ObNameCaseMode case_mode,
                                      ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  UNUSED(case_mode);
  ObODPSGeneralFormat format;
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
  ObString format_str;

  if (OB_FAIL(format.encrypt())) {
    LOG_WARN("failed to encrypt format", K(ret), K(format));
  } else if (OB_FAIL(convert_odps_format_to_str_properties_(format, format_str))) {
    LOG_WARN("failed to convert format", K(ret));
  } else {
    if (OB_SUCC(ret)) {
      table_schema.set_table_type(EXTERNAL_TABLE);
      table_schema.set_collation_type(CS_TYPE_UTF8MB4_BIN);
      table_schema.set_charset_type(CHARSET_UTF8MB4);
      table_schema.set_table_name(tbl_name);
      table_schema.set_external_properties(format_str);
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

          } else if (OB_FAIL(append(all_column_list, nonpart_column_list))) {
            LOG_WARN("failed to append nonpart column list", K(ret));
          } else if (OB_FAIL(append(all_column_list, partition_column_list))) {
            LOG_WARN("failed to append partition column list", K(ret));
          } else if (OB_FAIL(ObDMLResolver::build_column_schemas_for_odps(all_column_list, part_col_names, table_schema))) {
            LOG_WARN("failed to build column schemas for odps", K(ret));
          } else if (OB_FAIL(ObDMLResolver::set_partition_info_for_odps(table_schema, part_col_names))) {
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
          if (OB_FAIL(ObDMLResolver::build_column_schemas_for_odps(column_list, part_col_names, table_schema))) {
            LOG_WARN("failed to build column schemas for odps", K(ret));
          } else if (OB_FAIL(ObDMLResolver::set_partition_info_for_odps(table_schema, part_col_names))) {
            LOG_WARN("failed to set partition info for odps", K(ret));
          }
        }
#elif
        ret = OB_NOT_SUPPORTED
#endif
      }
    }
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
