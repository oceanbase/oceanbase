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

#include "share/catalog/odps/ob_odps_catalog.h"

namespace oceanbase
{
namespace share
{

int ObCatalogMetaGetter::list_namespace_names(const uint64_t tenant_id, const uint64_t catalog_id, ObIArray<ObString> &ns_names)
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
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !is_external_catalog_id(catalog_id) || ns_name.empty()
                  || OB_NAME_CASE_INVALID == case_mode)) {
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
                                                const common::ObString &ns_name,
                                                const ObNameCaseMode case_mode,
                                                share::schema::ObDatabaseSchema &database_schema)
{
  int ret = OB_SUCCESS;
  ObIExternalCatalog *catalog = nullptr;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !is_external_catalog_id(catalog_id) || ns_name.empty()
                  || !is_external_object_id(database_schema.get_database_id()) || OB_NAME_CASE_INVALID == case_mode)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(catalog_id), K(ns_name), K(case_mode), K(database_schema.get_database_id()));
  } else if (OB_FALSE_IT(database_schema.set_tenant_id(tenant_id))) {
  } else if (OB_FALSE_IT(database_schema.set_catalog_id(catalog_id))) {
  } else if (OB_FAIL(get_catalog_(tenant_id, catalog_id, catalog))) {
    LOG_WARN("failed to get catalog", K(ret));
  } else if (OB_ISNULL(catalog)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("catalog is nullptr", K(ret));
  } else if (OB_FAIL(catalog->fetch_namespace_schema(ns_name, case_mode, database_schema))) {
    LOG_WARN("failed to fetch namespace", K(ret));
  } else {
    database_schema.set_charset_type(common::ObCharsetType::CHARSET_UTF8MB4);
    database_schema.set_collation_type(common::ObCollationType::CS_TYPE_UTF8MB4_GENERAL_CI);
  }
  return ret;
}

// table_schema's table_id/database_id should assign correct before call this function
int ObCatalogMetaGetter::fetch_table_schema(const uint64_t tenant_id,
                                            const uint64_t catalog_id,
                                            const common::ObString &ns_name,
                                            const common::ObString &tbl_name,
                                            const ObNameCaseMode case_mode,
                                            share::schema::ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  ObIExternalCatalog *catalog = nullptr;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !is_external_catalog_id(catalog_id)
                  || !is_external_object_id(table_schema.get_database_id()) || !is_external_object_id(table_schema.get_table_id())
                  || ns_name.empty() || tbl_name.empty() || OB_NAME_CASE_INVALID == case_mode)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",
             K(ret),
             K(tenant_id),
             K(catalog_id),
             K(ns_name),
             K(tbl_name),
             K(case_mode),
             K(table_schema.get_database_id()),
             K(table_schema.get_table_id()));
  } else if (OB_FALSE_IT(table_schema.set_tenant_id(tenant_id))) {
  } else if (OB_FALSE_IT(table_schema.set_catalog_id(catalog_id))) {
  } else if (OB_FAIL(get_catalog_(tenant_id, catalog_id, catalog))) {
    LOG_WARN("failed to get catalog", K(ret));
  } else if (OB_ISNULL(catalog)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("catalog is nullptr", K(ret));
  } else if (OB_FAIL(catalog->fetch_table_schema(ns_name, tbl_name, case_mode, table_schema))) {
    LOG_WARN("fetch_table_schema failed", K(ret));
  } else {
    // append mocked pk column
    uint64_t COL_IDS[2] = {OB_HIDDEN_FILE_ID_COLUMN_ID, OB_HIDDEN_LINE_NUMBER_COLUMN_ID};
    const char *COL_NAMES[2] = {OB_HIDDEN_FILE_ID_COLUMN_NAME, OB_HIDDEN_LINE_NUMBER_COLUMN_NAME};
    for (int i = 0; OB_SUCC(ret) && i < array_elements(COL_IDS); i++) {
      ObColumnSchemaV2 hidden_pk;
      hidden_pk.reset();
      hidden_pk.set_column_id(COL_IDS[i]);
      hidden_pk.set_data_type(ObIntType);
      hidden_pk.set_nullable(false);
      hidden_pk.set_is_hidden(true);
      hidden_pk.set_charset_type(CHARSET_BINARY);
      hidden_pk.set_collation_type(CS_TYPE_BINARY);
      if (OB_FAIL(hidden_pk.set_column_name(COL_NAMES[i]))) {
        LOG_WARN("failed to set column name", K(ret));
      } else {
        hidden_pk.set_rowkey_position(i + 1);
        if (OB_FAIL(table_schema.add_column(hidden_pk))) {
          LOG_WARN("add column to table_schema failed", K(ret), K(hidden_pk));
        }
      }
    }
    table_schema.set_table_pk_exists_mode(ObTablePrimaryKeyExistsMode::TOM_TABLE_WITHOUT_PK);
  }
  return ret;
}

int ObCatalogMetaGetter::fetch_basic_table_info(const uint64_t tenant_id,
                                                const uint64_t catalog_id,
                                                const common::ObString &ns_name,
                                                const common::ObString &tbl_name,
                                                const ObNameCaseMode case_mode,
                                                ObCatalogBasicTableInfo &table_info)
{
  int ret = OB_SUCCESS;
  ObIExternalCatalog *catalog = nullptr;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !is_external_catalog_id(catalog_id) || ns_name.empty() || tbl_name.empty()
                  || OB_NAME_CASE_INVALID == case_mode)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(catalog_id), K(ns_name), K(tbl_name), K(case_mode));
  } else if (OB_FAIL(get_catalog_(tenant_id, catalog_id, catalog))) {
    LOG_WARN("failed to get catalog", K(ret));
  } else if (OB_ISNULL(catalog)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("catalog is nullptr", K(ret));
  } else if (OB_FAIL(catalog->fetch_basic_table_info(ns_name, tbl_name, case_mode, table_info))) {
    LOG_WARN("fetch_basic_table_info failed", K(ret));
  }
  return ret;
}

int ObCatalogMetaGetter::get_catalog_(const uint64_t tenant_id, const uint64_t catalog_id, ObIExternalCatalog *&catalog)
{
  int ret = OB_SUCCESS;
  catalog = nullptr;
  const ObCatalogSchema *schema = nullptr;
  ObCatalogProperties::CatalogType catalog_type = ObCatalogProperties::CatalogType::INVALID_TYPE;
  if (OB_FAIL(schema_getter_guard_.get_catalog_schema_by_id(tenant_id, catalog_id, schema))) {
    LOG_WARN("failed to get catalog schema", K(ret), K(tenant_id), K(catalog_id));
  } else if (OB_ISNULL(schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema is nullptr", K(ret));
  } else if (OB_FAIL(ObCatalogProperties::parse_catalog_type(schema->get_catalog_properties_str(), catalog_type))) {
    LOG_WARN("failed to parse catalog type", K(ret));
  } else {
    switch (catalog_type) {
      case ObCatalogProperties::CatalogType::ODPS_TYPE: {
#ifdef OB_BUILD_CPP_ODPS
        catalog = OB_NEWx(ObOdpsCatalog, &allocator_, allocator_);
        if (OB_ISNULL(catalog)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("alloc failed", K(ret));
        } else if (OB_FAIL(catalog->init(schema->get_catalog_properties()))) {
          LOG_WARN("failed to init odps catalog", K(ret));
        }
#else
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("ODPS CPP connector is not enabled", K(ret));
#endif
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid catalog type", K(ret));
      }
    }
  }
  return ret;
}

} // namespace share
} // namespace oceanbase
