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
 *
 * Schema Getter
 */

#define USING_LOG_PREFIX OBLOG_SCHEMA

#include "ob_log_schema_getter.h"

#include "lib/mysqlclient/ob_mysql_server_provider.h"     // ObMySQLServerProvider
#include "share/config/ob_common_config.h"                // ObCommonConfig
#include "share/ob_get_compat_mode.h"                     // ObCompatModeGetter
#include "share/inner_table/ob_inner_table_schema.h"      // OB_CORE_SCHEMA_VERSION

#include "ob_log_utils.h"                                 // is_mysql_client_errno
#include "ob_log_common.h"                                // GET_SCHEMA_TIMEOUT_ON_START_UP
#include "ob_log_config.h"                                // TCONF

// NOTICE: retry will exist if func return error code OB_TENANT_HAS_BEEN_DROPPED
// errorcode OB_TENANT_NOT_EXIST DOESN'T mean tenant has been dropped: tenant may be not create/deleted, besides,
// sql exectue error or request to a delay rs server may also get this code even if tenant exist(e.g. just created)
// should continue retry until get OB_TENANT_HAS_BEEN_DROPPED.
// should check tenant status by query_tenant_status(INSIDE SCHEMA_SERVICE) when func get OB_TENANT_NOT_EXIST
// (RETRY_ON_FAIL_WITH_TENANT_ID will check tenant status in macro)
//
// currently only function below will use this macro:
// ObLogSchemaGuard::get_database_schema  database_id get from ddl_stmt(__all_ddl_operation) may not real database id
// ObLogSchemaGuard::get_available_tenant_ids  args doesn't contain tenant_id
// auto_switch_mode_and_refresh_schema in ObLogSchemaGetter::init  tenant_id when init may invalid to refresh all_tenant schema
// ObLogSchemaGetter::load_split_schema_version  args doesn't contain tenant_id
#define RETRY_ON_FAIL(timeout, var, func, args...) \
    do { \
      if (OB_SUCC(ret)) { \
        static const int64_t SLEEP_TIME_ON_FAIL = 100L * 1000L; \
        int64_t start_time = get_timestamp(); \
        int64_t end_time = start_time + timeout; \
        int old_err = OB_SUCCESS; \
        while (OB_FAIL((var).func(args))) { \
          old_err = ret; \
          int64_t now = get_timestamp(); \
          if (end_time <= now) { \
            LOG_ERROR(#func " timeout", KR(ret), K(timeout)); \
            ret = OB_TIMEOUT; \
            break; \
          } \
          if (OB_NOT_SUPPORTED == ret || OB_TENANT_HAS_BEEN_DROPPED == ret) { \
            /* retrun if errno needn't retry */ \
            break; \
          } \
          LOG_ERROR(#func " fail, retry later", KR(ret), KR(old_err), K(timeout), \
              "retry_time", now - start_time); \
          ret = OB_SUCCESS; \
          ob_usleep(SLEEP_TIME_ON_FAIL); \
        } \
      } \
    } while (0)

// query tenant status before retry schema service
// function call this should make sure TENANT_ID MUST BE the FIRST one in its ARG list
#define RETRY_ON_FAIL_WITH_TENANT_ID(tenant_id, timeout, var, func, args...) \
    do { \
      if (OB_SUCC(ret)) { \
        static const int64_t SLEEP_TIME_ON_FAIL = 100L * 1000L; \
        if (OB_INVALID_TENANT_ID == tenant_id) {\
          ret = OB_INVALID_ARGUMENT; \
          LOG_ERROR(#func " fail: RETRY_ON_FAIL_WITH_TENANT_ID not support invalid tenant_id", KR(ret), K(tenant_id), K(timeout)); \
        } else { \
          int64_t start_time = get_timestamp(); \
          int64_t end_time = start_time + timeout; \
          int old_err = OB_SUCCESS; \
          LOG_DEBUG(#func " trying with tenant id", K(tenant_id)); \
          bool test_mode_force_check_tenant_status = (1 == TCONF.test_mode_on) && (1 == TCONF.test_mode_force_check_tenant_status); \
          while (OB_FAIL((var).func(args)) || test_mode_force_check_tenant_status) { \
            int64_t now = get_timestamp(); \
            if (end_time <= now) { \
              LOG_ERROR(#func " timeout", KR(ret), K(timeout)); \
              ret = OB_TIMEOUT; \
              break; \
            } \
            old_err = ret; \
            if (test_mode_force_check_tenant_status) { \
              /* force query tenant status only one time for each first time access schema service in test mode */ \
              ret = OB_TENANT_NOT_EXIST; \
              test_mode_force_check_tenant_status = false;  \
            } \
            if (OB_TENANT_NOT_EXIST == ret) { \
              /* retry to ensure tenant status */ \
              TenantStatus tenant_status = TENANT_STATUS_INVALID; \
              if (OB_FAIL(SchemaServiceType::get_instance().query_tenant_status(tenant_id, tenant_status))) { \
                LOG_WARN(#func " veriry tenant_status fail", KR(ret), KR(old_err), K(tenant_id), K(timeout)); \
              } else if (TENANT_DELETED == tenant_status) { \
                ret = OB_TENANT_HAS_BEEN_DROPPED; \
              } else { \
              } \
              LOG_INFO(#func " query tenant status for schema_getter retry", KR(ret), KR(old_err), K(tenant_id), K(tenant_status)); \
            } \
            if (OB_NOT_SUPPORTED == ret || OB_TENANT_HAS_BEEN_DROPPED == ret) { \
              /* retrun if errno needn't retry */ \
              break; \
            } \
            LOG_ERROR(#func " fail, retry later", KR(ret), KR(old_err), K(tenant_id), K(timeout), "retry_time", now - start_time); \
            ret = OB_SUCCESS; \
            ob_usleep(SLEEP_TIME_ON_FAIL); \
          } \
        } \
      } \
    } while (0)

using namespace oceanbase::common;
using namespace oceanbase::common::sqlclient;
using namespace oceanbase::share::schema;

namespace oceanbase
{
namespace libobcdc
{

ObLogSchemaGuard::ObLogSchemaGuard() : tenant_id_(OB_INVALID_TENANT_ID), guard_()
{}

ObLogSchemaGuard::~ObLogSchemaGuard()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
}

int ObLogSchemaGuard::do_check_force_fallback_mode_(const bool is_lazy, const char *func) const
{
  int ret = OB_SUCCESS;
  if (is_lazy) {
    _LOG_ERROR("schema guard should be fallback mode, but is lazy mode, "
        "'%s' not supported, is_lazy=%d, tenant_id=%lu",
        func, is_lazy, tenant_id_);
    ret = OB_NOT_SUPPORTED;
  } else {
    // Not a lazy model, meeting expectations
    LOG_DEBUG("schema guard is fallback mode, check done", K(tenant_id_), K(is_lazy), K(func));
  }
  return ret;
}

// Check if it is fallback mode and report an error if it is not
int ObLogSchemaGuard::check_force_fallback_mode_(const uint64_t tenant_id, const char *func) const
{
  int ret = OB_SUCCESS;
  bool is_lazy = false;
  // First check if the SYS tenant is in lazy mode
  if (OB_FAIL(guard_.is_lazy_mode(tenant_id, is_lazy))) {
    LOG_ERROR("check is_lazy_mode fail", KR(ret), K(tenant_id));
  } else {
    ret = do_check_force_fallback_mode_(is_lazy, func);
  }
  return ret;
}

// Check if it is fallback mode and report an error if it is not
// Requires both SYS tenant and target tenant to be in fallback mode
int ObLogSchemaGuard::check_force_fallback_mode_(const char *func) const
{
  int ret = OB_SUCCESS;
  bool is_lazy = false;
  // First check if the SYS tenant is in lazy mode
  if (OB_FAIL(guard_.is_lazy_mode(OB_SYS_TENANT_ID, is_lazy))) {
    LOG_ERROR("check is_lazy_mode by SYS tenant id fail", KR(ret));
  }
  // If the SYS tenant is not lazy, then check if the normal tenant is in lazy mode
  // Continue checking only when tenant_id is valid
  else if (! is_lazy
      && OB_INVALID_TENANT_ID != tenant_id_
      && OB_SYS_TENANT_ID != tenant_id_
      && OB_FAIL(guard_.is_lazy_mode(tenant_id_, is_lazy))) {
    LOG_ERROR("check is_lazy_mode fail", KR(ret), K(tenant_id_), K(is_lazy));
  } else {
    ret = do_check_force_fallback_mode_(is_lazy, func);
  }
  return ret;
}

int ObLogSchemaGuard::get_table_schema(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const ObTableSchema *&table_schema,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;

  RETRY_ON_FAIL_WITH_TENANT_ID(tenant_id, timeout, guard_, get_table_schema, tenant_id, table_id, table_schema);

  return ret;
}

int ObLogSchemaGuard::get_table_schema(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const ObSimpleTableSchemaV2 *&table_schema,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;

  RETRY_ON_FAIL_WITH_TENANT_ID(tenant_id, timeout, guard_, get_simple_table_schema, tenant_id, table_id, table_schema);

  return ret;
}

int ObLogSchemaGuard::get_database_schema(
    const uint64_t tenant_id,
    uint64_t database_id,
    const ObDatabaseSchema *&database_schema,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;

  RETRY_ON_FAIL(timeout, guard_, get_database_schema, tenant_id, database_id, database_schema);

  return ret;
}

int ObLogSchemaGuard::get_database_schema(
    const uint64_t tenant_id,
    uint64_t database_id,
    const ObSimpleDatabaseSchema *&database_schema,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;

  // The schema guard must be in fallback mode, otherwise the following function may be called with an error, prevented here
  // Database Schema uses the tenant ID of the DB to determine
  ret = check_force_fallback_mode_(tenant_id, "get_database_schema");
  RETRY_ON_FAIL(timeout, guard_, get_database_schema, tenant_id, database_id, database_schema);

  return ret;
}

int ObLogSchemaGuard::get_tenant_info(uint64_t tenant_id,
    const ObTenantSchema *&tenant_info,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;
  RETRY_ON_FAIL_WITH_TENANT_ID(tenant_id, timeout, guard_, get_tenant_info, tenant_id, tenant_info);
  return ret;
}

int ObLogSchemaGuard::get_tenant_info(uint64_t tenant_id,
    const ObSimpleTenantSchema *&tenant_info,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;
  // The schema guard must be in fallback mode, otherwise the following function may be called with an error, so here is a precaution
  // Tenant Schema uses the SYS tenant ID to determine
  ret = check_force_fallback_mode_(OB_SYS_TENANT_ID, "get_tenant_info");
  RETRY_ON_FAIL_WITH_TENANT_ID(tenant_id, timeout, guard_, get_tenant_info, tenant_id, tenant_info);
  return ret;
}

int ObLogSchemaGuard::get_tenant_schema_info(uint64_t tenant_id,
    TenantSchemaInfo &tenant_schema_info,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;
  bool is_lazy = false;

  // First determine if it is Lazy mode
  // Note: The tenant schema is determined using the SYS tenant, as it is used to get the tenant schema from the SYS tenant mgr
  if (OB_FAIL(guard_.is_lazy_mode(OB_SYS_TENANT_ID, is_lazy))) {
    LOG_ERROR("get is_lazy_mode() fail when get_tenant_schema_info", KR(ret));
  } else if (is_lazy) {
    // get Full Tenant Schema in lazy mode
    const ObTenantSchema *tenant_schema = NULL;
    if (OB_FAIL(get_tenant_info(tenant_id, tenant_schema, timeout))) {
      if (OB_TIMEOUT != ret && OB_TENANT_HAS_BEEN_DROPPED != ret) {
        LOG_ERROR("get_tenant_info fail when get tenant name", KR(ret), K(tenant_id), K(is_lazy));
      }
    } else if (OB_ISNULL(tenant_schema)) {
      LOG_WARN("schema error: tenant schema is NULL", K(tenant_id));
      ret = OB_TENANT_HAS_BEEN_DROPPED;
    } else {
      tenant_schema_info.reset(tenant_id,
          tenant_schema->get_schema_version(),
          tenant_schema->get_tenant_name(),
          tenant_schema->is_restore());
    }
  } else {
    // Non-Lazy mode, get Simple Table Schema
    const ObSimpleTenantSchema *simple_tenant_schema = NULL;
    if (OB_FAIL(get_tenant_info(tenant_id, simple_tenant_schema, timeout))) {
      if (OB_TIMEOUT != ret && OB_TENANT_HAS_BEEN_DROPPED != ret) {
        LOG_ERROR("get simple tenant schema fail when get tenant name", KR(ret), K(tenant_id), K(is_lazy));
      }
    } else if (OB_ISNULL(simple_tenant_schema)) {
      LOG_WARN("schema error: simple tenant schema is NULL", K(tenant_id));
      ret = OB_TENANT_HAS_BEEN_DROPPED;
    } else {
      tenant_schema_info.reset(tenant_id,
          simple_tenant_schema->get_schema_version(),
          simple_tenant_schema->get_tenant_name(),
          simple_tenant_schema->is_restore());
    }
  }
  return ret;
}

int ObLogSchemaGuard::get_database_schema_info(
    const uint64_t tenant_id,
    uint64_t database_id,
    DBSchemaInfo &db_schema_info,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;
  bool is_lazy = false;

  // First determine if it is a Lazy schema
  // The DB schema should use the corresponding tenant ID to determine if it is a lazy schema
  if (OB_FAIL(guard_.is_lazy_mode(tenant_id, is_lazy))) {
    LOG_ERROR("get is_lazy_mode() fail when get_database_schema_info", KR(ret), K(tenant_id),
        K(database_id));
  } else if (is_lazy) {
    // get Full Database Schema in lazy mode
    const ObDatabaseSchema *db_schema = NULL;
    if (OB_FAIL(get_database_schema(tenant_id, database_id, db_schema, timeout))) {
      if (OB_TIMEOUT != ret && OB_TENANT_HAS_BEEN_DROPPED != ret) {
        LOG_ERROR("get_database_schema fail when get database name", KR(ret), K(tenant_id), K(database_id), K(is_lazy));
      }
    } else if (OB_ISNULL(db_schema)) {
      LOG_ERROR("schema error: database schema is NULL", K(tenant_id), K(database_id));
      ret = OB_TENANT_HAS_BEEN_DROPPED;
    } else {
      db_schema_info.reset(database_id,
          db_schema->get_schema_version(),
          db_schema->get_database_name());
    }
  } else {
    // get Simple Database Schema in lazy mode
    const ObSimpleDatabaseSchema *simple_db_schema = NULL;
    if (OB_FAIL(get_database_schema(tenant_id, database_id, simple_db_schema, timeout))) {
      if (OB_TIMEOUT != ret && OB_TENANT_HAS_BEEN_DROPPED != ret) {
        LOG_ERROR("get_database_schema fail when get tenant name", KR(ret), K(tenant_id),
            K(database_id), K(is_lazy));
      }
    } else if (OB_ISNULL(simple_db_schema)) {
      LOG_WARN("schema error: simple database schema is NULL", K(database_id));
      ret = OB_TENANT_HAS_BEEN_DROPPED;
    } else {
      db_schema_info.reset(database_id,
          simple_db_schema->get_schema_version(),
          simple_db_schema->get_database_name());
    }
  }
  return ret;
}

int ObLogSchemaGuard::get_available_tenant_ids(common::ObIArray<uint64_t> &tenant_ids,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;
  // The schema guard must be in fallback mode, otherwise the following function may be called with an error, so here is a precaution
  ret = check_force_fallback_mode_("get_available_tenant_ids");
  RETRY_ON_FAIL(timeout, guard_, get_available_tenant_ids, tenant_ids);
  return ret;
}

// Pure memory operations, no retries, no changes here for consistency of code logic
int ObLogSchemaGuard::get_table_schemas_in_tenant(const uint64_t tenant_id,
    ObIArray<const ObSimpleTableSchemaV2 *> &table_schemas,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;
  // The schema guard must be in fallback mode, otherwise the following function may be called with an error, so here is a precaution
  // Check both SYS tenants and target tenants
  ret = check_force_fallback_mode_("get_table_schemas_in_tenant");
  RETRY_ON_FAIL_WITH_TENANT_ID(tenant_id, timeout, guard_, get_table_schemas_in_tenant, tenant_id, table_schemas);
  return ret;
}

int ObLogSchemaGuard::get_schema_version(const uint64_t tenant_id, int64_t &schema_version) const
{
  int ret = OB_SUCCESS;
  schema_version = OB_INVALID_TIMESTAMP;

  if (OB_FAIL(guard_.get_schema_version(tenant_id, schema_version))) {
    LOG_ERROR("get_schema_version fail", KR(ret), K(tenant_id), K(schema_version));
  } else if (OB_UNLIKELY(OB_INVALID_TIMESTAMP == schema_version)) {
    LOG_ERROR("schema_version is invalid", K(tenant_id), K(schema_version));
    ret = OB_ERR_UNEXPECTED;
  } else {
    // succ
  }

  return ret;
}


int ObLogSchemaGuard::get_table_ids_in_tablegroup(const uint64_t tenant_id,
    const uint64_t tablegroup_id,
    common::ObIArray<uint64_t> &table_id_array,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;
  // The schema guard must be in fallback mode, otherwise the following function may be called with an error, so here is a precaution
  // Check both SYS tenants and normal tenants
  ret = check_force_fallback_mode_("get_table_ids_in_tablegroup");
  RETRY_ON_FAIL_WITH_TENANT_ID(tenant_id, timeout, guard_, get_table_ids_in_tablegroup, tenant_id, tablegroup_id, table_id_array);
  return ret;
}

int ObLogSchemaGuard::get_tenant_compat_mode(const uint64_t tenant_id,
    lib::Worker::CompatMode &compat_mode,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;
  RETRY_ON_FAIL_WITH_TENANT_ID(tenant_id, timeout, guard_, get_tenant_compat_mode, tenant_id, compat_mode);
  return ret;
}

///////////////////////////////////////////////////////////////////////////////

ObLogSchemaGetter::ObLogSchemaGetter() : inited_(false),
                                         schema_service_(ObMultiVersionSchemaService::get_instance())
{
}

ObLogSchemaGetter::~ObLogSchemaGetter()
{
  destroy();
}

int ObLogSchemaGetter::init(common::ObMySQLProxy &mysql_proxy,
    common::ObCommonConfig *config,
    const int64_t max_cached_schema_version_count,
    const int64_t max_history_schema_version_count)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("schema getter has been initialized");
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(config)
      || OB_UNLIKELY(max_cached_schema_version_count <= 0)
      || OB_UNLIKELY(max_history_schema_version_count <= 0)) {
    LOG_ERROR("invalid argument", K(config), K(max_cached_schema_version_count), K(max_history_schema_version_count));
    ret = OB_INVALID_ARGUMENT;
  // init Schema Service
  } else if (OB_FAIL(schema_service_.init(&mysql_proxy, NULL, config, max_cached_schema_version_count,
          max_history_schema_version_count))) {
    LOG_ERROR("init schema service fail", KR(ret), K(max_cached_schema_version_count), K(max_history_schema_version_count));
  } else {
    // refresh Schema
    ObSchemaService::g_ignore_column_retrieve_error_ = true;
    ObSchemaService::g_liboblog_mode_ = true;
    ObMultiVersionSchemaService::g_skip_resolve_materialized_view_definition_ = true;
    const uint64_t tenant_id = OB_INVALID_TENANT_ID; // to refresh schema of all tenants
    const int64_t timeout = GET_SCHEMA_TIMEOUT_ON_START_UP;
    bool is_init_succ = false;

    // tenant_id is OB_INVALID_TENANT_ID, can't use RETRY_ON_FAIL_WITH_TENANT_ID
    RETRY_ON_FAIL(timeout, schema_service_, auto_switch_mode_and_refresh_schema, tenant_id);

    if (OB_TENANT_HAS_BEEN_DROPPED == ret) {
      // Indicates that there are some tenant deletions that do not affect refreshing the schema to the latest version
      LOG_WARN("tenant has been dropped when auto_switch_mode_and_refresh_schema, ignore it",
          KR(ret), K(tenant_id));
      is_init_succ = true;
      ret = OB_SUCCESS;
    } else if (OB_SUCCESS != ret) {
      LOG_ERROR("auto_switch_mode_and_refresh_schema failed", KR(ret), K(tenant_id), K(timeout));
    } else {
      is_init_succ = true;
    }

    if (OB_SUCC(ret) && is_init_succ) {
      inited_ = true;
      LOG_INFO("init schema service succ", KR(ret), K(max_cached_schema_version_count), K(max_history_schema_version_count));
    }
  }

  return ret;
}

void ObLogSchemaGetter::destroy()
{
  LOG_INFO("ObCDCSchemaGetter destroy begin");
  inited_ = false;
  schema_service_.stop();
  schema_service_.wait();
  LOG_INFO("ObCDCSchemaGetter destroy succ");
}

int ObLogSchemaGetter::get_lazy_schema_guard(const uint64_t tenant_id,
    const int64_t version,
    const int64_t timeout,
    IObLogSchemaGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  const ObMultiVersionSchemaService::RefreshSchemaMode force_lazy = ObMultiVersionSchemaService::RefreshSchemaMode::FORCE_LAZY;
  const bool specify_version_mode = true; // Specify the version to get the schema schema
  int64_t refreshed_version = OB_INVALID_VERSION;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("schema getter has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(version <= 0)) {
    LOG_ERROR("invalid argument", K(version));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(get_schema_guard_(tenant_id, specify_version_mode, version, timeout,
      schema_guard, force_lazy, refreshed_version))) {
    if (OB_TIMEOUT != ret) {
      LOG_ERROR("get_schema_guard_ fail", KR(ret), K(tenant_id), K(specify_version_mode),
          K(version), K(force_lazy));
    }
  }

  return ret;
}

int ObLogSchemaGetter::get_fallback_schema_guard(const uint64_t tenant_id,
    const int64_t version,
    const int64_t timeout,
    IObLogSchemaGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  const ObMultiVersionSchemaService::RefreshSchemaMode force_fallback = ObMultiVersionSchemaService::RefreshSchemaMode::FORCE_FALLBACK;
  const bool specify_version_mode = true; // Specify the version to get the schema schema
  int64_t refreshed_version = OB_INVALID_VERSION;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("schema getter has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(version <= 0)) {
    LOG_ERROR("invalid argument", K(version));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(get_schema_guard_(tenant_id, specify_version_mode, version, timeout,
      schema_guard, force_fallback, refreshed_version))) {
    if (OB_TIMEOUT != ret) {
      LOG_ERROR("get_schema_guard_ fail", KR(ret), K(tenant_id), K(specify_version_mode),
          K(version), K(force_fallback));
    }
  }

  return ret;
}

int ObLogSchemaGetter::check_schema_guard_suitable_for_table_(IObLogSchemaGuard &schema_guard,
    const uint64_t tenant_id,
    const uint64_t table_id,
    const int64_t expected_version,
    const int64_t refreshed_version,
    const int64_t timeout,
    const ObSimpleTableSchemaV2 *&tb_schema,
    bool &is_suitable)
{
  int ret = OB_SUCCESS;
  // Determine if the latest version of the local schema meets the requirements
  // Here is an optimization: instead of fetching the schema of a specific version, just base it directly on the latest version of the schema.
  //
  // The prerequisite is that the schema information for Table Schema, DB Name, Tenant Name and other dependencies has not changed from the specified
  // version to the latest version. So, the key to correctness here is to determine whether the schema information has changed
  //
  // Criteria: All of the following conditions are met
  // 1. table_schema_version <= expected_version <= latest_schema_version
  // 2. db_schema_version <= expected_version <= latest_schema_version
  // 3. TODO tenant level information changes should also be taken into account, e.g. tenant name change
  //
  // Essentially, in addition to the schema information for tables, the schema for databases and tenants currently only rely on their names, including
  // whitelist matching and setting the DBName field on the binlog record. So, changes to these two pieces of information should also be considered.
  //
  // 1. database name: DB name is currently not supported by RENMAE, but dropping database to recyclebin will change its name
  // 2. tenant name: currently rename is not supported, in the future when tenant rename is supported, tenant name will need to be managed at tenant level, and by then it will be necessary to determine if the tenant name has changed.

  // The default is to meet the requirements
  is_suitable = true;
  const char *reason = "";
  const ObSimpleDatabaseSchema *simple_db_schema = NULL;

  // Get the Simple Table Schema, which is already cached locally, it just fetches the structure from the local cache
  if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, tb_schema, timeout))) {
    LOG_ERROR("get table schema from local schema cache fail", KR(ret), K(tenant_id), K(table_id),
        K(expected_version), K(refreshed_version));
  } else if (OB_ISNULL(tb_schema)) {
    is_suitable = false;
    reason = "table schema is NULL in local refreshed schema, maybe dropped";
  } else if (tb_schema->get_schema_version() > expected_version) {
    // The schema of the table has changed and does not meet the desired version
    is_suitable = false;
    reason = "table schema is changed after expected version";
  }
  // Start get Database information
  else if (OB_FAIL(schema_guard.get_database_schema(tenant_id, tb_schema->get_database_id(),
      simple_db_schema, timeout))) {
    LOG_ERROR("get database schema from local schema cache fail", KR(ret), K(tenant_id),
        K(tb_schema->get_database_id()), K(table_id),
        K(expected_version), K(refreshed_version));
  } else if (OB_ISNULL(simple_db_schema)) {
    is_suitable = false;
    reason = "database schema is NULL in local refreshed schema, maybe dropped";
  } else if (simple_db_schema->get_schema_version() > expected_version) {
    is_suitable = false;
    reason = "database schema is changed after expected version";
  } else {
    // Finally all conditions are met and the latest version of schema guard is available
    is_suitable = true;
    LOG_DEBUG("[GET_SCHEMA] [USE_LATEST_SCHEMA_GUARD]",
        K(table_id), K(expected_version), K(refreshed_version),
        "delta", refreshed_version - expected_version);
  }

  if (OB_SUCCESS == ret && ! is_suitable) {
    LOG_DEBUG("[GET_SCHEMA] [NEED_SPECIFY_VERSION]", K(reason), K(table_id),
        K(expected_version), K(refreshed_version), "delta", refreshed_version - expected_version);
  }

  return ret;
}

int ObLogSchemaGetter::get_schema_guard_and_table_schema(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const int64_t expected_version,
    const int64_t timeout,
    IObLogSchemaGuard &schema_guard,
    const ObSimpleTableSchemaV2 *&tb_schema)
{
  int ret = OB_SUCCESS;
  // Get ObSimpleTableSchemaV2 must use force lazy mode
  const bool force_lazy = true;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("schema getter has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(get_schema_guard_and_simple_table_schema_(tenant_id, table_id, expected_version, timeout, force_lazy,
      schema_guard, tb_schema))) {
    if (OB_TIMEOUT != ret) {
      LOG_ERROR("get_schema_guard_and_simple_table_schema_ fail", KR(ret), K(tenant_id), K(table_id),
          K(expected_version), K(force_lazy), KPC(tb_schema));
    }
  } else {
    // succ
  }

  return ret;
}

int ObLogSchemaGetter::get_schema_guard_and_full_table_schema(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const int64_t expected_version,
    const int64_t timeout,
    IObLogSchemaGuard &schema_guard,
    const oceanbase::share::schema::ObTableSchema *&full_tb_schema)
{
  int ret = OB_SUCCESS;
  // ObTableSchema cannot be obtained by force lazy mode
  const bool force_lazy = false;
  const ObSimpleTableSchemaV2 *tb_schema = NULL;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("schema getter has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(get_schema_guard_and_simple_table_schema_(tenant_id, table_id, expected_version, timeout, force_lazy,
      schema_guard, tb_schema))) {
    if (OB_TIMEOUT != ret) {
      LOG_ERROR("get_schema_guard_and_simple_table_schema_ fail", KR(ret), K(table_id), K(expected_version), K(force_lazy),
          KPC(tb_schema));
    }
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, full_tb_schema, timeout))) {
    if (OB_TIMEOUT != ret) {
      LOG_ERROR("get table schema fail", KR(ret), K(table_id), K(expected_version), K(force_lazy), KPC(full_tb_schema));
    }
  } else {
    // succ
  }

  return ret;
}

int ObLogSchemaGetter::get_schema_guard_and_simple_table_schema_(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const int64_t expected_version,
    const int64_t timeout,
    const bool force_lazy,
    IObLogSchemaGuard &schema_guard,
    const oceanbase::share::schema::ObSimpleTableSchemaV2 *&tb_schema)
{
  int ret = OB_SUCCESS;
  // default not ues force_fallback/force_lazy
  const ObMultiVersionSchemaService::RefreshSchemaMode normal = ObMultiVersionSchemaService::RefreshSchemaMode::NORMAL;
  int64_t refreshed_version = OB_INVALID_VERSION;
  bool schema_guard_suitable = true;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("schema getter has not been initialized");
    ret = OB_NOT_INIT;
  }
  // First get the latest version of the local schema
  // Default to get the latest local schema
  else if (OB_FAIL(get_schema_guard_(tenant_id, false, expected_version, timeout,
      schema_guard, normal, refreshed_version))) {
    if (OB_TIMEOUT != ret) {
      LOG_ERROR("get_schema_guard_ by non-specify_version_mode fail", KR(ret), K(tenant_id),
          K(expected_version), K(normal));
    }
  }
  // check if it is the right schema guard
  // will also get the table schema
  else if (OB_FAIL(check_schema_guard_suitable_for_table_(schema_guard,
      tenant_id,
      table_id,
      expected_version,
      refreshed_version,
      timeout,
      tb_schema,
      schema_guard_suitable))) {
    LOG_WARN("check_schema_guard_suitable_for_table_ fail", KR(ret), K(table_id),
        K(expected_version), K(refreshed_version));
  } else if (schema_guard_suitable) {
    // suitable
  } else {
    ObMultiVersionSchemaService::RefreshSchemaMode refresh_schema_mode = ObMultiVersionSchemaService::RefreshSchemaMode::NORMAL;
    if (force_lazy) {
      refresh_schema_mode = ObMultiVersionSchemaService::RefreshSchemaMode::FORCE_LAZY;
    }

    // The latest local version of the schema guard does not meet expectations, get the specified version of the schema guard
    if (OB_FAIL(get_schema_guard_(tenant_id, true, expected_version, timeout, schema_guard,
        refresh_schema_mode, refreshed_version))) {
      LOG_ERROR("get_schema_guard_ by specify_version_mode fail", KR(ret), K(tenant_id),
          K(expected_version), K(refresh_schema_mode), K(force_lazy));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, tb_schema, timeout))) {
      LOG_ERROR("get_table_schema fail", KR(ret), K(table_id), K(tenant_id), K(expected_version));
    } else {
      // success
    }
  }
  return ret;
}

// @retval OB_SUCCESS                   Success
// @retval OB_TENANT_HAS_BEEN_DROPPED   Tenant has been dropped
// @retval OB_TIMEOUT                   Timeout
// @retval Other error codes            Fail
int ObLogSchemaGetter::refresh_to_expected_version_(const uint64_t tenant_id,
    const int64_t expected_version,
    const int64_t timeout,
    int64_t &latest_version)
{
  int ret = OB_SUCCESS;
  int64_t start_time = get_timestamp();
  latest_version = OB_INVALID_VERSION;

  // Requires valid tenant ID and version
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)
      || OB_UNLIKELY(OB_INVALID_VERSION == expected_version)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(tenant_id), K(expected_version));
  } else if (OB_FAIL(schema_service_.get_tenant_refreshed_schema_version(tenant_id, latest_version))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      // Circumvent the problem of libobcdc reporting an error on the first refresh of the tenant's schema
      ret = OB_SUCCESS;
      latest_version = OB_INVALID_VERSION;
      LOG_INFO("first load tenant schema, force renew schema", KR(ret), K(tenant_id));
    } else {
      LOG_WARN("fail to get refreshed schema version", KR(ret), K(tenant_id));
    }
  }

  if (OB_FAIL(ret)) {
    // fail
  } else if (OB_INVALID_VERSION == latest_version || latest_version < expected_version) {
    // If the tenant version is invalid, or if the desired schema version is not reached, then a refresh of the schema is requested
    LOG_INFO("begin refresh schema to expected version", K(tenant_id), K(expected_version),
        K(latest_version));

    RETRY_ON_FAIL_WITH_TENANT_ID(tenant_id, timeout, schema_service_, auto_switch_mode_and_refresh_schema, tenant_id,
        expected_version);

    if (OB_SUCC(ret)) {
      // Get the latest schema version again
      RETRY_ON_FAIL_WITH_TENANT_ID(tenant_id, timeout, schema_service_, get_tenant_refreshed_schema_version, tenant_id, latest_version);
    }

    int64_t cost_time = get_timestamp() - start_time;
    LOG_INFO("refresh schema to expected version", KR(ret), K(tenant_id),
        K(latest_version), K(expected_version), "delta", latest_version - expected_version,
        "latest_version", TS_TO_STR(latest_version),
        "cost_time", TVAL_TO_STR(cost_time));
  }

  return ret;
}

// Unique interface for obtaining Schema Guard
//
// specify_version_mode indicates whether it is the specified version to get the schema mode.
// if true, it requires the schema of the specified version (expected_version) to be fetched
// If false, the schema is fetched directly from the local refresh and requires a version number greater than or equal to expected_version
//
// @retval OB_SUCCESS                   success
// @retval OB_TENANT_HAS_BEEN_DROPPED   tenant has been dropped
// @retval OB_TIMEOUT                   timeout
// @retval other error code             fail
int ObLogSchemaGetter::get_schema_guard_(const uint64_t tenant_id,
    const bool specify_version_mode,
    const int64_t expected_version,
    const int64_t timeout,
    IObLogSchemaGuard &guard,
    const ObMultiVersionSchemaService::RefreshSchemaMode refresh_schema_mode,
    int64_t &refreshed_version)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard &schema_guard = guard.get_guard();

  // version refreshed locally
  refreshed_version = OB_INVALID_VERSION;

  // First refresh the schema to ensure it is greater than or equal to expected_version
  if (OB_FAIL(refresh_to_expected_version_(tenant_id, expected_version, timeout, refreshed_version))) {
    if (OB_TENANT_HAS_BEEN_DROPPED != ret && OB_TIMEOUT != ret) {
      LOG_ERROR("refresh_to_expected_version_ fail", KR(ret), K(tenant_id), K(expected_version));
    }
  } else {
    int64_t target_version = OB_INVALID_VERSION;;

    if (specify_version_mode) {
      // If the requested version is newer than the latest version, the latest version is used
      if (expected_version > refreshed_version) {
        LOG_INFO("asked schema version is greater than latest schema version", K(tenant_id),
            K(expected_version), K(refreshed_version));
        // Take the current version
        target_version = refreshed_version;
      } else {
        // Otherwise the specified version is used
        target_version = expected_version;
      }
    } else {
      // Take the latest locally refreshed version
      target_version = OB_INVALID_VERSION;
    }

    // The corresponding sys tenant schema version does not need to be provided when fetching tenant level schema, the locally refreshed version is used by default
    const int64_t sys_schema_version = OB_INVALID_VERSION;
    // The get_tenant_schema_guard() function may also return an error, here need to retry until it times out or succeeds
    RETRY_ON_FAIL_WITH_TENANT_ID(tenant_id, timeout, schema_service_, get_tenant_schema_guard, tenant_id, schema_guard,
        target_version,
        sys_schema_version,
        refresh_schema_mode);

    if (OB_FAIL(ret)) {
      LOG_WARN("get_tenant_schema_guard fail", KR(ret), K(tenant_id), K(target_version),
          K(sys_schema_version), K(refresh_schema_mode), K(specify_version_mode), K(refreshed_version));
    }

    if (OB_SUCC(ret)) {
      // set tenant id
      guard.set_tenant_id(tenant_id);
    }
  }

  return ret;
}

int ObLogSchemaGetter::get_schema_version_by_timestamp(const uint64_t tenant_id,
    const int64_t timestamp,
    int64_t &schema_version,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;

  ObRefreshSchemaStatus schema_status;
  schema_status.tenant_id_ = tenant_id;  // use strong read
  RETRY_ON_FAIL_WITH_TENANT_ID(tenant_id, timeout, schema_service_, get_schema_version_by_timestamp,
                schema_status, tenant_id, timestamp, schema_version);

  if (OB_SUCC(ret)) {
    LOG_INFO("get_schema_version_by_timestamp", K(tenant_id), K(timestamp), K(schema_version));
  }

  return ret;
}

int ObLogSchemaGetter::get_first_trans_end_schema_version(const uint64_t tenant_id,
    int64_t &schema_version,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;
  RETRY_ON_FAIL_WITH_TENANT_ID(tenant_id, timeout, schema_service_, get_first_trans_end_schema_version, tenant_id,
      schema_version);
  return ret;
}

int ObLogSchemaGetter::load_split_schema_version(int64_t &split_schema_version,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;
  UNUSEDx(timeout);
  split_schema_version = 0;
  return ret;
}

int ObLogSchemaGetter::get_tenant_refreshed_schema_version(const uint64_t tenant_id, int64_t &version)
{
  int ret = OB_SUCCESS;
  int64_t refreshed_version = 0;
  const int64_t timeout = GET_SCHEMA_TIMEOUT_ON_START_UP;

  RETRY_ON_FAIL_WITH_TENANT_ID(tenant_id, timeout, schema_service_, get_tenant_refreshed_schema_version, tenant_id, refreshed_version);
  if (OB_FAIL(ret)) {
    if (OB_TENANT_HAS_BEEN_DROPPED == ret) {
      LOG_INFO("get tenant refreshed schema version: tenant has been dropped", KR(ret), K(tenant_id), K(refreshed_version));
    } else {
      LOG_WARN("fail to get tenant refreshed schema version", KR(ret), K(tenant_id), K(refreshed_version));
    }
  } else if (refreshed_version <= share::OB_CORE_SCHEMA_VERSION) {
    // When the tenant's schema is not swiped before, its version is invalid and will return less than or equal to OB_CORE_SCHEMA_VERSION
    // We also consider this to be a case where the tenant does not exist
    LOG_INFO("get_tenant_refreshed_schema_version: tenant schema version is not refreshed, "
        "consider tenant not exist",
        K(tenant_id), K(refreshed_version));
    ret = OB_TENANT_HAS_BEEN_DROPPED;
  } else {
    version = refreshed_version;
  }

  return ret;
}

} // namespace libobcdc
} // namespace oceanbase
