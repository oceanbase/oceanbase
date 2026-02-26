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

#include "share/catalog/filesystem/ob_filesystem_catalog.h"

#include "share/catalog/ob_catalog_location_schema_provider.h"
#include "sql/engine/expr/ob_expr_regexp_context.h"
#include "sql/engine/table/ob_external_table_access_service.h"
#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "sql/table_format/iceberg/ob_iceberg_table_metadata.h"
#include "sql/table_format/iceberg/ob_iceberg_utils.h"
#include "sql/table_format/iceberg/scan/task.h"

namespace oceanbase
{
namespace share
{

int ObFileSystemCatalog::do_init(const common::ObString &properties)
{
  int ret = OB_SUCCESS;

  ObFilesystemCatalogProperties fs_catalog_properties;

  ObString tmp_warehouse;
  if (OB_FAIL(fs_catalog_properties.load_from_string(properties, allocator_))) {
    LOG_WARN("failed to load properties", K(ret));
  } else if (OB_FAIL(fs_catalog_properties.decrypt(allocator_))) {
    LOG_WARN("failed to decrypt", K(ret));
  } else if (OB_UNLIKELY((fs_catalog_properties.warehouse_.empty()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("catalog can't be empty", K(ret));
  } else {
    tmp_warehouse = fs_catalog_properties.warehouse_;
    if (tmp_warehouse.ptr()[tmp_warehouse.length() - 1] == '/') {
      // 去除末尾的 '/'
      tmp_warehouse = ObString(tmp_warehouse.length() - 1, tmp_warehouse.ptr());
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ob_write_string(allocator_, tmp_warehouse, warehouse_, true))) {
      LOG_WARN("failed to copy warehouse", K(ret));
    } else if (OB_NOT_NULL(location_schema_provider_)) {
      if (OB_FAIL(location_schema_provider_->get_access_info_by_path(allocator_,
                                                                     tenant_id_,
                                                                     warehouse_,
                                                                     access_info_))) {
        LOG_WARN("failed to get access info", K(ret));
      }
    }
  }
  return ret;
}

int ObFileSystemCatalog::list_namespace_names(common::ObIArray<common::ObString> &ns_names)
{
  int ret = OB_SUCCESS;
  sql::ObExternalDataAccessDriver driver;
  if (OB_FAIL(driver.init(warehouse_, access_info_))) {
    LOG_WARN("init external data access driver failed", K(ret));
  } else if (OB_FAIL(driver.get_directory_list(warehouse_, ns_names, allocator_))) {
    LOG_WARN("list directory failed", K(ret));
  }
  if (driver.is_opened()) {
    driver.close();
  }
  return ret;
}

int ObFileSystemCatalog::list_table_names(const common::ObString &db_name,
                                          const ObNameCaseMode case_mode,
                                          common::ObIArray<common::ObString> &tbl_names)
{
  int ret = OB_SUCCESS;
  sql::ObExternalDataAccessDriver driver;
  ObSqlString db_path;
  if (OB_FAIL(db_path.append_fmt("%.*s/%.*s",
                                 warehouse_.length(),
                                 warehouse_.ptr(),
                                 db_name.length(),
                                 db_name.ptr()))) {
    LOG_WARN("failed to concat db path", K(ret));
  } else if (OB_FAIL(driver.init(warehouse_, access_info_))) {
    LOG_WARN("init external data access driver failed", K(ret));
  } else if (OB_FAIL(driver.get_directory_list(db_path.string(), tbl_names, allocator_))) {
    LOG_WARN("get file urls failed", K(ret));
  }
  if (driver.is_opened()) {
    driver.close();
  }
  return ret;
}

int ObFileSystemCatalog::fetch_namespace_schema(const uint64_t database_id,
                                                const common::ObString &ns_name,
                                                const ObNameCaseMode case_mode,
                                                share::schema::ObDatabaseSchema *&database_schema)
{
  int ret = OB_SUCCESS;
  ObArray<ObString> ns_names;
  database_schema = OB_NEWx(share::schema::ObDatabaseSchema, &allocator_, &allocator_);
  if (OB_ISNULL(database_schema)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate database schema", K(ret));
  } else if (OB_FALSE_IT(database_schema->set_tenant_id(tenant_id_))) {
  } else if (OB_FALSE_IT(database_schema->set_database_id(database_id))) {
  } else if (OB_FAIL(list_namespace_names(ns_names))) {
    LOG_WARN("list_namespace_names() failed", K(ret));
  } else {
    bool is_found = false;
    for (size_t i = 0; OB_SUCC(ret) && !is_found && i < ns_names.size(); ++i) {
      const ObString &tmp_name = ns_names[i];
      if (ObCharset::case_mode_equal(case_mode, tmp_name, ns_name)) {
        OZ(database_schema->set_database_name(tmp_name));
        is_found = true;
      }
    }
    if (!is_found) {
      ret = OB_ERR_BAD_DATABASE;
    }
  }
  return ret;
}

int ObFileSystemCatalog::fetch_latest_table_schema_version(const common::ObString &ns_name,
                                                           const common::ObString &tbl_name,
                                                           const ObNameCaseMode case_mode,
                                                           int64_t &schema_version)
{
  return OB_NOT_SUPPORTED;
}

int ObFileSystemCatalog::fetch_lake_table_metadata(ObIAllocator &allocator,
                                                   const uint64_t database_id,
                                                   const uint64_t table_id,
                                                   const common::ObString &ns_name,
                                                   const common::ObString &tbl_name,
                                                   const ObNameCaseMode case_mode,
                                                   ObILakeTableMetadata *&table_metadata)
{
  int ret = OB_SUCCESS;
  ObLakeTableFormat table_format = ObLakeTableFormat::INVALID;
  ObSqlString tbl_path;
  if (OB_FAIL(tbl_path.append_fmt("%.*s/%.*s/%.*s",
                                  warehouse_.length(),
                                  warehouse_.ptr(),
                                  ns_name.length(),
                                  ns_name.ptr(),
                                  tbl_name.length(),
                                  tbl_name.ptr()))) {
    LOG_WARN("failed to build tbl path", K(ret));
  } else if (OB_FAIL(deduce_table_format_(tbl_path.string(), table_format))) {
    LOG_WARN("deduce table format failed", K(ret));
  } else {
    switch (table_format) {
      case ObLakeTableFormat::ICEBERG: {
        iceberg::ObIcebergTableMetadata *iceberg_table_metadata = NULL;
        if (OB_ISNULL(iceberg_table_metadata
                      = OB_NEWx(iceberg::ObIcebergTableMetadata, &allocator, allocator))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate iceberg table metadata", K(ret));
        } else if (OB_FAIL(iceberg_table_metadata->init(tenant_id_,
                                                        catalog_id_,
                                                        database_id,
                                                        table_id,
                                                        ns_name,
                                                        tbl_name,
                                                        case_mode))) {
          LOG_WARN("failed to init iceberg table metadata", K(ret));
        } else if (OB_FAIL(iceberg_table_metadata->set_access_info(access_info_))) {
          LOG_WARN("failed to set access_info", K(ret));
        } else if (OB_FAIL(iceberg_table_metadata->load_by_table_location(tbl_path.string()))) {
          LOG_WARN("failed to load iceberg table metadata", K(ret));
        } else {
          table_metadata = iceberg_table_metadata;
        }
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported table format", K(ret), K(table_format));
      }
    }
  }
  return ret;
}

int ObFileSystemCatalog::current(const ObString &ns_name,
                                 const ObString &tbl_name,
                                 ObILakeTableMetadata *&metadata)
{
  return OB_NOT_SUPPORTED;
}

int ObFileSystemCatalog::commit(const ObString &ns_name,
                                const ObString &table_name,
                                const ObILakeTableMetadata *previous,
                                const ObILakeTableMetadata *latest)
{
  return OB_NOT_SUPPORTED;
}

int ObFileSystemCatalog::deduce_table_format_(const ObString &tbl_path,
                                              ObLakeTableFormat &table_format)
{
  int ret = OB_SUCCESS;
  ObArray<ObString> table_dirs;
  sql::ObExternalDataAccessDriver driver;
  table_format = ObLakeTableFormat::INVALID;
  if (OB_FAIL(driver.init(warehouse_, access_info_))) {
    LOG_WARN("init external data access driver failed", K(ret));
  } else if (OB_FAIL(driver.get_directory_list(tbl_path, table_dirs, allocator_))) {
    LOG_WARN("get file urls failed", K(ret));
  } else {
    if (table_dirs.count() == 0) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("table not existed", K(ret), K(tbl_path));
    } else if (table_dirs.count() == 1) {  // 创建表, 但还没插入数据时只有meta目录
      if (0 == table_dirs.at(0).case_compare("metadata")) {
        table_format = ObLakeTableFormat::ICEBERG;
      }
    } else if (table_dirs.count() == 2) {
      ObString first_dir = table_dirs.at(0);
      ObString second_dir = table_dirs.at(1);
      if ((0 == first_dir.case_compare("data") && 0 == second_dir.case_compare("metadata"))
          || (0 == first_dir.case_compare("metadata") && 0 == second_dir.case_compare("data"))) {
        table_format = ObLakeTableFormat::ICEBERG;
      }
    }
  }

  if (driver.is_opened()) {
    driver.close();
  }
  return ret;
}
} // namespace share
} // namespace oceanbase