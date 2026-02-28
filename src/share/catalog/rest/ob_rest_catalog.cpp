/**
* Copyright (c) 2021 OceanBase
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

#include "share/catalog/rest/ob_rest_catalog.h"
#include "sql/table_format/iceberg/ob_iceberg_table_metadata.h"
#include "share/catalog/rest/client/ob_catalog_client_pool.h"
#include "share/catalog/hive/ob_iceberg_catalog_stat_helper.h"
#include "share/catalog/rest/auth/ob_rest_auth_mgr.h"

namespace oceanbase
{
namespace share
{

ObRestCatalog::~ObRestCatalog()
{
  if (client_ != nullptr) {
    LOG_TRACE("return client to pool", K(tenant_id_), K(catalog_id_), K(rest_properties_));
    ObCatalogClientPool<ObCurlRestClient> *client_pool = static_cast<ObCatalogClientPool<ObCurlRestClient> *>(client_->get_client_pool());
    ObCurlRestClient *curl_client = static_cast<ObCurlRestClient *>(client_);
    if (OB_SUCCESS != client_->reuse()) {
      LOG_INFO("failed to reuse client", K(tenant_id_), K(catalog_id_), K(rest_properties_));
    }
    if (OB_NOT_NULL(client_pool) && OB_SUCCESS != client_pool->return_client(curl_client)) {
      LOG_INFO("failed to return client to pool", K(tenant_id_), K(catalog_id_), K(rest_properties_));
    }
    client_ = nullptr;
  }
}

int ObRestCatalog::do_init(const ObString &properties)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(rest_properties_.load_from_string(properties, allocator_))) {
    LOG_WARN("fail to init rest properties", K(ret), K(properties));
  } else if (OB_FAIL(rest_properties_.decrypt(allocator_))) {
    LOG_WARN("failed to decrypt", K(ret));
  } else {
    ObCurlRestClient *tmp_client = nullptr;
    ObCatalogClientPoolMgr<ObCurlRestClient> *client_pool_mgr = MTL(ObCatalogClientPoolMgr<ObCurlRestClient> *);
    if (OB_ISNULL(client_pool_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("client pool mgr is null", K(ret));
    } else if (OB_FAIL(client_pool_mgr->get_client(tenant_id_, catalog_id_, properties, tmp_client))) {
      LOG_WARN("failed to get client from pool", K(ret), K_(catalog_id), K(rest_properties_));
    } else if (OB_ISNULL(tmp_client)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("client is null", K(ret));
    } else {
      client_ = tmp_client;
    }
  }
  return ret;
}

ObCatalogProperties::CatalogType ObRestCatalog::get_catalog_type() const
{
  return ObCatalogProperties::CatalogType::REST_TYPE;
}

int ObRestCatalog::list_namespace_names(common::ObIArray<common::ObString> &ns_names)
{
  int ret = OB_SUCCESS;
  ObRestHttpRequest req(allocator_);
  ObRestListNamespacesResponse rsp(allocator_);
  ObSqlString tmp_url;
  ObString auth_info;
  if (OB_FAIL(tmp_url.append_fmt(ENDPOINT_NAMESPACES,
                                 rest_properties_.prefix_.length(), rest_properties_.prefix_.ptr(),
                                 rest_properties_.prefix_.empty() ? "" : "/"))) {
    LOG_WARN("failed to append prefix", K(ret), K(rest_properties_.prefix_));
  } else if (OB_FAIL(req.set_url(tmp_url.string()))) {
    LOG_WARN("failed to set url", K(ret), K(tmp_url.string()));
  } else if (OB_FALSE_IT(req.set_method(ObRestHttpMethod::HTTP_METHOD_GET))) {
  } else if (OB_FAIL(execute_warpper(req, rsp))) {
    LOG_WARN("failed to execute warpper", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rsp.namespaces_.count(); i++) {
      ObString &tmp_str = rsp.namespaces_.at(i);
      if (OB_FAIL(ns_names.push_back(tmp_str))) {
        LOG_WARN("failed to push namespace name to list", K(ret), K(tmp_str));
      }
    }
  }
  return ret;
}

int ObRestCatalog::list_table_names(const common::ObString &ns_name,
                                    const ObNameCaseMode case_mode,
                                    common::ObIArray<common::ObString> &tb_names)
{
  int ret = OB_SUCCESS;
  ObRestHttpRequest req(allocator_);
  ObRestListTablesResponse rsp(allocator_);
  ObSqlString tmp_url;
  ObString auth_info;
  if (OB_FAIL(tmp_url.append_fmt(ENDPOINT_TABLES,
                                 rest_properties_.prefix_.length(), rest_properties_.prefix_.ptr(),
                                 rest_properties_.prefix_.empty() ? "" : "/",
                                 ns_name.length(), ns_name.ptr()))) {
    LOG_WARN("failed to append catalog name", K(ret), K(rest_properties_.prefix_));
  } else if (OB_FAIL(req.set_url(tmp_url.string()))) {
    LOG_WARN("failed to set url", K(ret));
  } else if (OB_FALSE_IT(req.set_method(ObRestHttpMethod::HTTP_METHOD_GET))) {
  } else if (OB_FAIL(execute_warpper(req, rsp))) {
    LOG_WARN("failed to execute warpper", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rsp.tables_.count(); i++) {
      ObString &tmp_str = rsp.tables_.at(i);
      if (OB_FAIL(tb_names.push_back(tmp_str))) {
        LOG_WARN("failed to push table name to list", K(ret), K(tmp_str));
      }
    }
  }
  return ret;
}

int ObRestCatalog::fetch_namespace_schema(const uint64_t database_id,
                                          const common::ObString &ns_name,
                                          const ObNameCaseMode case_mode,
                                          share::schema::ObDatabaseSchema *&database_schema)
{
  int ret = OB_SUCCESS;
  ObRestHttpRequest req(allocator_);
  ObRestGetNamespaceResponse rsp(allocator_);
  ObSqlString tmp_url;
  ObString auth_info;
  if (OB_ISNULL(database_schema = OB_NEWx(schema::ObDatabaseSchema, &allocator_, &allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory for database schema", K(ret));
  } else if (OB_FALSE_IT(database_schema->set_tenant_id(tenant_id_))) {
  } else if (OB_FALSE_IT(database_schema->set_database_id(database_id))) {
  } else if (OB_FAIL(tmp_url.append_fmt(ENDPOINT_NAMESPACE,
                                        rest_properties_.prefix_.length(), rest_properties_.prefix_.ptr(),
                                        rest_properties_.prefix_.empty() ? "" : "/",
                                        ns_name.length(), ns_name.ptr()))) {
    LOG_WARN("failed to append catalog name", K(ret), K(rest_properties_.prefix_));
  } else if (OB_FAIL(req.set_url(tmp_url.string()))) {
    LOG_WARN("failed to set url", K(ret));
  } else if (OB_FALSE_IT(req.set_method(ObRestHttpMethod::HTTP_METHOD_GET))) {
  } else if (OB_FAIL(execute_warpper(req, rsp))) {
    if (ret == OB_HTTP_NOT_FOUND) {  // reset to bad database
      ret = OB_ERR_BAD_DATABASE;
      LOG_WARN("namespace not found", K(ret), K(ns_name));
    } else {
      LOG_WARN("failed to execute warpper", K(ret));
    }
  } else if (OB_FAIL(database_schema->set_database_name(rsp.database_name_))) {
    LOG_WARN("failed to set database name", K(ret), K(rsp.database_name_));
  }
  return ret;
}

int ObRestCatalog::fetch_lake_table_metadata(ObIAllocator &allocator,
                                             const uint64_t database_id,
                                             const uint64_t table_id,
                                             const common::ObString &ns_name,
                                             const common::ObString &tbl_name,
                                             const ObNameCaseMode case_mode,
                                             ObILakeTableMetadata *&table_metadata)
{
  int ret = OB_SUCCESS;
  ObRestHttpRequest req(allocator_);
  ObRestLoadTableResponse rsp(allocator_);
  ObSqlString tmp_url;
  ObString auth_info;
  if (OB_FAIL(tmp_url.append_fmt(ENDPOINT_TABLE,
                                 rest_properties_.prefix_.length(), rest_properties_.prefix_.ptr(),
                                 rest_properties_.prefix_.empty() ? "" : "/",
                                 ns_name.length(), ns_name.ptr(),
                                 tbl_name.length(), tbl_name.ptr()))) {
    LOG_WARN("failed to append catalog name", K(ret), K(rest_properties_.prefix_));
  } else if (OB_FAIL(req.set_url(tmp_url.string()))) {
    LOG_WARN("failed to set url", K(ret));
  } else if (OB_FALSE_IT(req.set_method(ObRestHttpMethod::HTTP_METHOD_GET))) {
  } else if (rest_properties_.vended_credential_enabled_
             && OB_FAIL(req.add_header(VENDED_CREDENTAIL_ENABLED_HEADER, VENDED_CREDENTAIL_ENABLED_VALUE))) {
    LOG_WARN("failed to add header", K(ret));
  } else if (OB_FAIL(execute_warpper(req, rsp))) {
    if (ret == OB_HTTP_NOT_FOUND) {  // reset to table not exist
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("table not found", K(ret), K(tbl_name));
    } else {
      LOG_WARN("failed to execute warpper", K(ret));
    }
  } else {
    ObString table_location = rsp.metadata_location_;
    ObString storage_access_info;
    uint64_t location_object_id = OB_INVALID_ID;
    ObString location_sub_path;
    char storage_info_buf[OB_MAX_BACKUP_STORAGE_INFO_LENGTH] = { 0 };
    if (OB_UNLIKELY(table_location.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("empty table location", K(ret));
    } else if(rest_properties_.vended_credential_enabled_) {
      common::ObStorageType device_type;
      ObSqlString concat_storage_info;
      ObExternalTableStorageInfo external_storage_info;
      if (OB_FAIL(get_storage_type_from_path_for_external_table(table_location, device_type))) {
        LOG_WARN("failed to get storage type from path", K(ret));
      } else if (OB_FAIL(concat_storage_info.append_fmt("access_id=%.*s&access_key=%.*s&host=%.*s",
                                          rsp.accessid_.length(), rsp.accessid_.ptr(),
                                          rsp.accesskey_.length(), rsp.accesskey_.ptr(),
                                          rsp.endpoint_.length(), rsp.endpoint_.ptr()))) {
        LOG_WARN("failed to append storage info", K(ret));
      } else if (OB_STORAGE_S3 == device_type && OB_FAIL(concat_storage_info.append_fmt("&s3_region=%.*s",
                                                  rsp.region_.length(), rsp.region_.ptr()))) {
        LOG_WARN("failed to append storage info", K(ret));
      } else if (OB_FAIL(external_storage_info.set(table_location.ptr(), concat_storage_info.ptr()))) {
        LOG_WARN("failed to set storage info", K(ret));
      } else if (OB_FAIL(external_storage_info.get_storage_info_str(storage_info_buf, sizeof(storage_info_buf)))) {
        LOG_WARN("failed to get storage info str", K(ret));
      } else {
        storage_access_info.assign_ptr(storage_info_buf, strlen(storage_info_buf));
      }
    } else if (OB_NOT_NULL(location_schema_provider_)) {
      if (OB_FAIL(location_schema_provider_->get_access_info_by_path(allocator_,
                                                                     tenant_id_,
                                                                     table_location,
                                                                     storage_access_info,
                                                                     location_object_id,
                                                                     location_sub_path))) {
        LOG_WARN("failed to get storage access info", K(ret), K(table_location));
      }
    }

    sql::iceberg::ObIcebergTableMetadata *iceberg_table_metadata = NULL;
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_ISNULL(iceberg_table_metadata
                  = OB_NEWx(sql::iceberg::ObIcebergTableMetadata, &allocator, allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for iceberg table metadata", K(ret));
    } else if (OB_FAIL(iceberg_table_metadata->init(tenant_id_, catalog_id_,
                                                    database_id, table_id,
                                                    ns_name, tbl_name,
                                                    case_mode))) {
      LOG_WARN("failed to init iceberg table metadata", K(ret));
    } else if (OB_FAIL(iceberg_table_metadata->set_access_info(storage_access_info))) {
      LOG_WARN("failed to set access info", K(ret));
    } else if (OB_FALSE_IT(iceberg_table_metadata->set_location_object_id(location_object_id))) {

    } else if (OB_FAIL(iceberg_table_metadata->set_location_object_sub_path(location_sub_path))) {
      LOG_WARN("failed to set location object sub path", K(ret));
    } else if (OB_FAIL(iceberg_table_metadata->table_metadata_.assign(rsp.table_metadata_))) {
      LOG_WARN("failed to assign iceberg table metadata", K(ret));
    } else {
      table_metadata = iceberg_table_metadata;
    }
  }
  return ret;
}

int ObRestCatalog::fetch_latest_table_schema_version(const common::ObString &ns_name,
                                                     const common::ObString &tbl_name,
                                                     const ObNameCaseMode case_mode,
                                                     int64_t &schema_version)
{
  int ret = OB_SUCCESS;
  // todo@lekou: iceberg表接入缓存后需要实现
  return ret;
}

int ObRestCatalog::fetch_table_statistics(ObIAllocator &allocator,
                                          sql::ObSqlSchemaGuard &sql_schema_guard,
                                          const ObILakeTableMetadata *table_metadata,
                                          const ObIArray<ObString> &partition_values,
                                          const ObIArray<ObString> &column_names,
                                          ObOptExternalTableStat *&external_table_stat,
                                          ObIArray<ObOptExternalColumnStat *> &external_table_column_stats)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_metadata)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table metadata", K(ret));
  } else if (ObLakeTableFormat::ICEBERG == table_metadata->get_format_type()) {
    ObIcebergCatalogStatHelper stat_helper(allocator);
    if (OB_FAIL(stat_helper.fetch_iceberg_table_statistics(table_metadata,
                                                          partition_values,
                                                          column_names,
                                                          external_table_stat,
                                                          external_table_column_stats))) {
      LOG_WARN("failed to fetch iceberg table statistics via helper", K(ret));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported table format in rest catalog",
             K(ret),
             K(table_metadata->get_format_type()));
  }
  return ret;
}

int ObRestCatalog::execute_warpper(ObRestHttpRequest &req, ObRestHttpResponse &rsp)
{
  int ret = OB_SUCCESS;
  bool need_retry = false;
  if (OB_ISNULL(client_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("client is null", K(ret));
  } else if (OB_FAIL(ObRestAuthMgr::get_instance().authenticate(tenant_id_, catalog_id_, req, rest_properties_, allocator_))) {
    LOG_WARN("failed to authenticate", K(ret));
  } else if (OB_FAIL(client_->execute(req, rsp))) {
    if (OB_HTTP_UNAUTHORIZED == ret && ObRestAuthType::OAUTH2_TYPE == rest_properties_.auth_type_) {  // OAUTH2可能是token过期了
      need_retry = true;
    }
    LOG_WARN("failed to execute request", K(ret), K(need_retry));
  }

  if (OB_FAIL(ret) && need_retry) {
    rsp.reset();
    if (OB_FAIL(ObRestAuthMgr::get_instance().authenticate(tenant_id_, catalog_id_, req, rest_properties_, allocator_, true/*force_refresh*/))) {
      LOG_WARN("failed to authenticate", K(ret));
    } else if (OB_FAIL(client_->execute(req, rsp))) {
      LOG_WARN("failed to execute request", K(ret));
    }
  }

  if (OB_SUCC(ret) && OB_FAIL(rsp.parse_from_response())) {
    LOG_WARN("failed to parse response", K(ret));
  }
  return ret;
}

}
}
