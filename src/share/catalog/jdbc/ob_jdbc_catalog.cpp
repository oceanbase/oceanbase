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

#include "share/catalog/jdbc/ob_jdbc_catalog.h"

#include "plugin/external_table/ob_jdbc_catalog_plugin.h"
#include "plugin/interface/ob_plugin_external_intf.h"
#include "plugin/sys/ob_plugin_helper.h"
#include "share/catalog/jdbc/ob_jdbc_catalog_types.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "sql/table_format/jdbc/ob_jdbc_table_metadata.h"
#include "share/rc/ob_tenant_base.h"
#include "lib/worker.h"

namespace oceanbase
{
namespace share
{
using namespace oceanbase::common;
using namespace oceanbase::share::schema;

ObJdbcCatalog::~ObJdbcCatalog() = default;

int ObJdbcCatalog::acquire(ObJdbcConnectionGuard &guard)
{
  int ret = OB_SUCCESS;
  if (plugin_name_.empty()) {
    ret = OB_NOT_INIT;
  } else {
    const ObJdbcConnectionPoolKey key(tenant_id_, catalog_id_, catalog_schema_version_);
    ObJdbcConnectionPoolMgr *mgr = MTL(ObJdbcConnectionPoolMgr *);
    if (OB_ISNULL(mgr)) {
      ret = OB_NOT_INIT;
    } else if (OB_FAIL(mgr->acquire(key, THIS_WORKER.get_timeout_ts(), guard))) {
      LOG_WARN("failed to acquire jdbc catalog connection", K(ret), K(key));
    }
  }
  return ret;
}

int ObJdbcCatalog::do_init(const common::ObString &properties)
{
  int ret = OB_SUCCESS;
  ObJdbcCatalogProperties jdbc_properties;
  ObArenaAllocator properties_allocator(ObMemAttr(tenant_id_, "JdbcProps"));
  ObString parsed_plugin_name;
  ObString registered_name;
  plugin::ObIExternalDescriptor *external_desc = nullptr;
  if (OB_FAIL(jdbc_properties.load_from_string(properties, properties_allocator))) {
    LOG_WARN("failed to load jdbc catalog properties", K(ret));
  } else if (OB_FAIL(jdbc_properties.decrypt(properties_allocator))) {
    LOG_WARN("failed to decrypt jdbc catalog properties", K(ret));
  } else if (OB_UNLIKELY(jdbc_properties.url_.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("jdbc catalog url is empty", K(ret));
  } else if (OB_FAIL(ObJdbcCatalogProperties::resolve_plugin_name(jdbc_properties.url_, parsed_plugin_name))) {
    LOG_WARN("failed to resolve jdbc catalog plugin name", K(ret));
  } else if (OB_FAIL(plugin::ObPluginHelper::find_jdbc_catalog(parsed_plugin_name,
                                                               external_desc,
                                                               registered_name))) {
    LOG_WARN("failed to find jdbc catalog plugin", K(ret), K(parsed_plugin_name));
  } else if (OB_ISNULL(external_desc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("jdbc catalog plugin descriptor is null", K(ret), K(parsed_plugin_name));
  } else if (OB_FAIL(ob_write_string(allocator_, registered_name, plugin_name_, true))) {
    LOG_WARN("failed to copy jdbc catalog plugin name", K(ret), K(registered_name));
  }
  return ret;
}

int ObJdbcCatalog::list_namespace_names(ObIArray<ObString> &ns_names)
{
  int ret = OB_SUCCESS;
  {
    ObJdbcConnectionGuard guard;
    plugin::ObJdbcCatalogPlugin *bridge = nullptr;
    bool connection_reusable = false;
    if (OB_FAIL(acquire(guard))) {
      LOG_WARN("failed to acquire jdbc metadata connection", K(ret));
    } else if (OB_ISNULL(bridge = guard.catalog_plugin())) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(bridge->list_namespace_names(guard.connection(), allocator_, ns_names, connection_reusable))) {
      LOG_WARN("failed to list jdbc namespace names", K(ret));
    }
    if (connection_reusable) {
      guard.mark_reusable();
    }
  }
  return ret;
}

int ObJdbcCatalog::list_table_names(const ObString &db_name,
                                    const ObNameCaseMode case_mode,
                                    ObIArray<ObString> &tbl_names)
{
  int ret = OB_SUCCESS;
  bool namespace_found = false;
  UNUSED(case_mode);
  {
    ObJdbcConnectionGuard guard;
    plugin::ObJdbcCatalogPlugin *bridge = nullptr;
    bool connection_reusable = false;
    if (OB_FAIL(acquire(guard))) {
      LOG_WARN("failed to acquire jdbc metadata connection", K(ret));
    } else if (OB_ISNULL(bridge = guard.catalog_plugin())) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(bridge->list_table_names(guard.connection(), allocator_,
                                              db_name, tbl_names, namespace_found, connection_reusable))) {
      LOG_WARN("failed to list jdbc table names", K(ret), K(db_name));
    }
    if (connection_reusable) {
      guard.mark_reusable();
    }
  }
  if (OB_FAIL(ret)) {
  } else if (!namespace_found) {
    ret = OB_ERR_BAD_DATABASE;
    LOG_WARN("jdbc namespace does not exist", K(ret), K(db_name));
  }
  return ret;
}

int ObJdbcCatalog::fetch_namespace_schema(const uint64_t database_id,
                                          const ObString &ns_name,
                                          const ObNameCaseMode case_mode,
                                          ObDatabaseSchema *&database_schema)
{
  int ret = OB_SUCCESS;
  bool namespace_found = false;
  ObString canonical_namespace_name;
  UNUSED(case_mode);
  database_schema = nullptr;
  {
    ObJdbcConnectionGuard guard;
    plugin::ObJdbcCatalogPlugin *bridge = nullptr;
    bool connection_reusable = false;
    if (OB_FAIL(acquire(guard))) {
      LOG_WARN("failed to acquire jdbc metadata connection", K(ret));
    } else if (OB_ISNULL(bridge = guard.catalog_plugin())) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(bridge->fetch_namespace_schema(guard.connection(), allocator_,
                                                    ns_name, canonical_namespace_name,
                                                    namespace_found, connection_reusable))) {
      LOG_WARN("failed to fetch jdbc namespace schema", K(ret), K(ns_name));
    }
    if (connection_reusable) {
      guard.mark_reusable();
    }
  }
  if (OB_FAIL(ret)) {
  } else if (!namespace_found) {
    ret = OB_ERR_BAD_DATABASE;
    LOG_WARN("jdbc namespace does not exist", K(ret), K(ns_name));
  } else if (OB_ISNULL(database_schema = OB_NEWx(ObDatabaseSchema, &allocator_, &allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate jdbc database schema", K(ret));
  } else if (FALSE_IT(database_schema->set_tenant_id(tenant_id_))) {
  } else if (FALSE_IT(database_schema->set_database_id(database_id))) {
  } else if (OB_FAIL(database_schema->set_database_name(canonical_namespace_name))) {
    LOG_WARN("failed to set jdbc database name", K(ret), K(canonical_namespace_name));
  }
  return ret;
}

int ObJdbcCatalog::get_charset_context(const ObCompatibilityMode target_mode,
                                       sql::jdbc::ObJdbcCharsetContext &charset_context)
{
  int ret = OB_SUCCESS;
  charset_context = sql::jdbc::ObJdbcCharsetContext();
  if (MYSQL_MODE == target_mode) {
    const ObSysVarSchema *var_schema = nullptr;
    ObObj collation_value;
    if (OB_FAIL(schema_guard_.get_tenant_system_variable(tenant_id_, SYS_VAR_COLLATION_SERVER, var_schema))) {
      LOG_WARN("failed to get jdbc tenant default collation", K(ret), K(tenant_id_));
    } else if (OB_ISNULL(var_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("jdbc tenant default collation is null", K(ret), K(tenant_id_));
    } else if (OB_FAIL(var_schema->get_value(&allocator_, nullptr, collation_value))) {
      LOG_WARN("failed to read jdbc tenant default collation", K(ret), K(tenant_id_));
    } else if (!collation_value.is_int() || !ObCharset::is_valid_collation(collation_value.get_int())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid jdbc tenant default collation", K(ret), K(collation_value));
    } else {
      charset_context.default_collation_ = static_cast<ObCollationType>(collation_value.get_int());
    }
  } else if (ORACLE_MODE == target_mode) {
    const ObSysVarClassType vars[] = {SYS_VAR_NLS_CHARACTERSET, SYS_VAR_NLS_NCHAR_CHARACTERSET};
    ObCollationType *collations[] = {&charset_context.default_collation_, &charset_context.nchar_collation_};
    for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(vars); ++i) {
      const ObSysVarSchema *var_schema = nullptr;
      if (OB_FAIL(schema_guard_.get_tenant_system_variable(tenant_id_, vars[i], var_schema))) {
        LOG_WARN("failed to get jdbc tenant NLS charset", K(ret), K(tenant_id_), K(vars[i]));
      } else if (OB_ISNULL(var_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("jdbc tenant NLS charset is null", K(ret), K(tenant_id_), K(vars[i]));
      } else {
        *collations[i] = ObCharset::get_default_collation_oracle(
            ObCharset::charset_type_by_name_oracle(var_schema->get_value()));
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
  }
  return ret;
}

int ObJdbcCatalog::fetch_lake_table_metadata(ObIAllocator &allocator,
                                             const uint64_t database_id,
                                             const uint64_t table_id,
                                             const ObString &ns_name,
                                             const ObString &tbl_name,
                                             const ObNameCaseMode case_mode,
                                             ObILakeTableMetadata *&table_metadata)
{
  int ret = OB_SUCCESS;
  bool table_found = false;
  ObString metadata_json;
  ObString scan_parameters;
  ObJdbcTableMetadataDesc metadata_desc;
  sql::jdbc::ObJdbcCharsetContext charset_context;
  const ObCompatibilityMode target_mode = lib::is_oracle_mode() ? ORACLE_MODE : MYSQL_MODE;
  sql::jdbc::ObJdbcTableMetadata *jdbc_table_metadata = nullptr;
  table_metadata = nullptr;
  {
    ObJdbcConnectionGuard guard;
    plugin::ObJdbcCatalogPlugin *bridge = nullptr;
    bool connection_reusable = false;
    if (OB_FAIL(acquire(guard))) {
      LOG_WARN("failed to acquire jdbc metadata connection", K(ret));
    } else if (OB_ISNULL(bridge = guard.catalog_plugin())) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(bridge->fetch_lake_table_metadata(guard.connection(), allocator,
                                                       ns_name, tbl_name, target_mode, metadata_json,
                                                       table_found, connection_reusable))) {
      LOG_WARN("failed to fetch java jdbc table metadata", K(ret), K(ns_name), K(tbl_name));
    }
    if (connection_reusable) {
      guard.mark_reusable();
    }
  }
  if (OB_FAIL(ret)) {
  } else if (!table_found) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("jdbc table does not exist", K(ret), K(ns_name), K(tbl_name));
  } else if (OB_FAIL(ObJdbcTableMetadataParser::parse(metadata_json, allocator, metadata_desc))) {
    LOG_WARN("failed to decode java jdbc table metadata",
             K(ret), K(metadata_json.length()), K(ns_name), K(tbl_name));
  } else if (OB_UNLIKELY(target_mode != metadata_desc.target_mode_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("java jdbc table metadata target mode mismatch",
             K(ret), K(target_mode), K(metadata_desc.target_mode_));
  } else if (OB_FAIL(get_charset_context(target_mode, charset_context))) {
    LOG_WARN("failed to get jdbc charset context", K(ret), K(tenant_id_));
  } else if (OB_FAIL(metadata_desc.build_scan_parameters(allocator, scan_parameters, catalog_id_, catalog_schema_version_))) {
    LOG_WARN("failed to build jdbc scan parameters", K(ret));
  } else if (OB_ISNULL(jdbc_table_metadata = OB_NEWx(sql::jdbc::ObJdbcTableMetadata, &allocator, allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate jdbc table metadata", K(ret));
  } else if (OB_FAIL(jdbc_table_metadata->init(tenant_id_,
                                               catalog_id_,
                                               database_id,
                                               table_id,
                                               ns_name,
                                               tbl_name,
                                               case_mode))) {
    LOG_WARN("failed to initialize jdbc table metadata", K(ret));
  } else if (OB_FAIL(jdbc_table_metadata->setup_table_schema(
                         plugin_name_, scan_parameters, metadata_desc, charset_context))) {
    LOG_WARN("failed to setup jdbc table schema", K(ret));
  } else {
    table_metadata = jdbc_table_metadata;
  }
  return ret;
}

int ObJdbcCatalog::fetch_latest_table_schema_version(const ObString &ns_name,
                                                     const ObString &tbl_name,
                                                     const ObNameCaseMode case_mode,
                                                     int64_t &schema_version)
{
  return OB_NOT_SUPPORTED;
}

} // namespace share
} // namespace oceanbase
