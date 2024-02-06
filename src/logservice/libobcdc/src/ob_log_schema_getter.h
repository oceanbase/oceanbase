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

#ifndef OCEANBASE_LIBOBCDC_SCHEMA_GETTER_H__
#define OCEANBASE_LIBOBCDC_SCHEMA_GETTER_H__

#include "lib/mysqlclient/ob_mysql_proxy.h"               // ObMySQLProxy
#include "share/schema/ob_multi_version_schema_service.h" // ObMultiVersionSchemaService
#include "share/schema/ob_schema_getter_guard.h"          // ObSchemaGetterGuard
#include "share/schema/ob_schema_struct.h"                // TableStatus
#include "share/schema/ob_schema_mgr.h"                   // ObSimpleDatabaseSchema, ObSimpleTenantSchema
#include "lib/worker.h"                              // CompatMode

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObTableSchema;
class ObSimpleTableSchemaV2;
class ObDatabaseSchema;
class ObTenantSchema;
class ObTablegroupSchema;
} // namespace schema
} // namespace share
namespace common
{
class ObCommonConfig;

namespace sqlclient
{
class ObMySQLServerProvider;
} // namespace sqlclient
} // namespace common

namespace libobcdc
{

// Database Schema信息
struct DBSchemaInfo
{
  uint64_t              db_id_;
  int64_t               version_;
  const char            *name_;

  TO_STRING_KV(K_(db_id), K_(version), K_(name));

  DBSchemaInfo() { reset(); }

  void reset()
  {
    db_id_ = common::OB_INVALID_ID;
    version_ = common::OB_INVALID_VERSION;
    name_ = NULL;
  }

  void reset(const uint64_t db_id,
      const int64_t version,
      const char *name)
  {
    db_id_ = db_id;
    version_ = version;
    name_ = name;
  }

  bool is_valid() const
  {
    // Only the most basic information is verified here
    return common::OB_INVALID_ID != db_id_
        && common::OB_INVALID_VERSION != version_
        && NULL != name_;
  }
};

// Schema info of tenant
struct TenantSchemaInfo
{
  uint64_t tenant_id_;
  int64_t  version_;
  const char *name_;
  // tenant whether in restore state or not at specified version
  bool is_restore_;

  TO_STRING_KV(K_(tenant_id), K_(version), K_(name), K(is_restore_));

  TenantSchemaInfo() { reset(); }

  void reset()
  {
    tenant_id_ = common::OB_INVALID_TENANT_ID;
    version_ = common::OB_INVALID_VERSION;
    name_ = NULL;
    is_restore_ = false;
  }

  void reset(const uint64_t tenant_id, const int64_t version, const char *name, const bool is_restore)
  {
    tenant_id_ = tenant_id;
    version_ = version;
    name_ = name;
    is_restore_ = is_restore;
  }

  bool is_valid() const
  {
    // Only the most basic information is verified here
    return common::OB_INVALID_TENANT_ID != tenant_id_
        && common::OB_INVALID_VERSION != version_
        && NULL != name_;
  }
};

///////////////////////////////////// IObLogSchemaGuard /////////////////////////////////
class IObLogSchemaGuard
{
  friend class ObLogSchemaGetter;
public:
  virtual ~IObLogSchemaGuard() {}

public:
  virtual oceanbase::share::schema::ObSchemaGetterGuard &get_guard() = 0;
  virtual void set_tenant_id(const uint64_t tenant_id) = 0;

  /// Get Table Schema
  ///
  /// @param [in] table_id      Target table ID
  /// @param [out] table_schema The table schema returned
  /// @param [in] timeout       Timeout timeout
  ///
  /// @retval OB_SUCCESS                   success
  /// @retval OB_TENANT_HAS_BEEN_DROPPED   tenent has been dropped
  /// @retval OB_TIMEOUT                   timeout
  /// @retval other error code             fail
  virtual int get_table_schema(
      const uint64_t tenant_id,
      const uint64_t table_id,
      const oceanbase::share::schema::ObTableSchema *&table_schema,
      const int64_t timeout) = 0;

  /// Get Simple Table Schema (does not contain column information, only partial meta information is stored)
  ///
  /// @param [in] table_id      Target table ID
  /// @param [out] table_schema Returned table schema
  /// @param [in] timeout       timeout
  ///
  /// @retval OB_SUCCESS                   success
  /// @retval OB_TENANT_HAS_BEEN_DROPPED   tenent has been dropped
  /// @retval OB_TIMEOUT                   timeout
  /// @retval other error code             fail
  virtual int get_table_schema(
      const uint64_t tenant_id,
      const uint64_t table_id,
      const oceanbase::share::schema::ObSimpleTableSchemaV2 *&table_schema,
      const int64_t timeout) = 0;

  /// Get Database Schema related information
  /// This function masks the DB Schema details externally for the purpose of internal optimization based on the Schema Guard type
  ///
  /// @param [in] database_id       Target DB ID
  /// @param [out] db_schema_info   The database schema information returned
  /// @param [in] timeout           timeout
  ///
  /// @retval OB_SUCCESS                   success
  /// @retval OB_TENANT_HAS_BEEN_DROPPED   tenent has been dropped
  /// @retval OB_TIMEOUT                   timeout
  /// @retval other error code             fail
  virtual int get_database_schema_info(
      const uint64_t tenant_id,
      uint64_t database_id,
      DBSchemaInfo &db_schema_info,
      const int64_t timeout) = 0;

  /// Get Tenant Schema information
  /// This function is externally masked to get Tenant Schema details for the purpose of internal optimization based on Schema Guard types
  ///
  /// @param [in] tenant_id           Target DB ID
  /// @param [out] tenant_schema_info Tenant Schema information returned
  /// @param [in] timeout             timeout
  ///
  /// @retval OB_SUCCESS                   success
  /// @retval OB_TENANT_HAS_BEEN_DROPPED   tenent has been dropped
  /// @retval OB_TIMEOUT                   timeout
  /// @retval other error code             fail
  virtual int get_tenant_schema_info(uint64_t tenant_id,
      TenantSchemaInfo &tenant_schema_info,
      const int64_t timeout) = 0;

  /// Get all Tenant IDs that have been created successfully and have not been deleted
  ///
  /// @param [out] tenant_ids Returned array of tenant ids
  /// @param [in] timeout     timeout
  ///
  /// @retval OB_SUCCESS                   success
  /// @retval OB_TENANT_HAS_BEEN_DROPPED   tenent has been dropped
  /// @retval OB_TIMEOUT                   timeout
  /// @retval other error code             fail
  virtual int get_available_tenant_ids(common::ObIArray<uint64_t> &tenant_ids,
      const int64_t timeout) = 0;

  /// Get all Simple Table Schema for the tenant
  /// NOTE: The guard must be in fallback mode
  ///
  /// @param [in] tenant_id       Tenant ID
  /// @param [out] table_schemas  Array of returned Table Schema's
  /// @param [in] timeout         timeout
  ///
  /// @retval OB_SUCCESS                   success
  /// @retval OB_TENANT_HAS_BEEN_DROPPED   tenent has been dropped
  /// @retval OB_TIMEOUT                   timeout
  /// @retval other error code             fail
  virtual int get_table_schemas_in_tenant(const uint64_t tenant_id,
      common::ObIArray<const oceanbase::share::schema::ObSimpleTableSchemaV2 *> &table_schemas,
      const int64_t timeout) = 0;

  virtual int get_schema_version(const uint64_t tenant_id, int64_t &schema_version) const = 0;

  /// Get all the table ids in the tablegroup
  /// NOTE: guards must get by fallback mode
  ///
  /// @retval OB_SUCCESS                   success
  /// @retval OB_TENANT_HAS_BEEN_DROPPED   tenent has been dropped
  /// @retval OB_TIMEOUT                   timeout
  /// @retval other error code             fail
  virtual int get_table_ids_in_tablegroup(const uint64_t tenant_id,
      const uint64_t tablegroup_id,
      common::ObIArray<uint64_t> &table_id_array,
      const int64_t timeout) = 0;

  /// Get the working mode of the specified tenant: mysql or oracle
  /// Note: This interface does not return OB_SCHEMA_ERROR/OB_TENANT_HAS_BEEN_DROPPED and does not need to handle such errors
  ///
  /// @param [in]   tenant_id     tenant ID
  /// @param [out]  compat_mode   MYSQL, ORACLE
  ///
  /// @retval OB_SUCCESS                   success
  /// @retval OB_TIMEOUT                   timeout
  /// @retval other error code             fail
  virtual int get_tenant_compat_mode(const uint64_t tenant_id,
      lib::Worker::CompatMode &compat_mode,
      const int64_t timeout) = 0;

protected:
  /// get tenant mode
  ///
  /// @param [in] tenant_id     target tenant id
  /// @param [out] tenant_info  returned tenant schema
  /// @param [in] timeout       timeout
  ///
  /// @retval OB_SUCCESS                   success
  /// @retval OB_TENANT_HAS_BEEN_DROPPED   tenent has been dropped
  /// @retval OB_TIMEOUT                   timeout
  /// @retval other error code             fail
  virtual int get_tenant_info(uint64_t tenant_id,
      const oceanbase::share::schema::ObTenantSchema *&tenant_info,
      const int64_t timeout) = 0;

  /// get Simple Tenant Schema
  ///
  /// @param [in] tenant_id     target tenant id
  /// @param [out] tenant_info  returned tenant schema
  /// @param [in] timeout       timeout
  ///
  /// @retval OB_SUCCESS                   success
  /// @retval OB_TENANT_HAS_BEEN_DROPPED   tenent has been dropped
  /// @retval OB_TIMEOUT                   timeout
  /// @retval other error code             fail
  virtual int get_tenant_info(uint64_t tenant_id,
      const oceanbase::share::schema::ObSimpleTenantSchema *&tenant_info,
      const int64_t timeout) = 0;

  /// get Database Schema
  ///
  /// @param [in]   tenant_id       target tenant id
  /// @param [in]   database_id     target db id
  /// @param [out]  database_schema returned database schema
  /// @param [in]   timeout         timeout
  ///
  /// @retval OB_SUCCESS                   success
  /// @retval OB_TENANT_HAS_BEEN_DROPPED   tenent has been dropped
  /// @retval OB_TIMEOUT                   timeout
  /// @retval other error code             fail
  virtual int get_database_schema(
      const uint64_t tenant_id,
      uint64_t database_id,
      const oceanbase::share::schema::ObDatabaseSchema *&database_schema,
      const int64_t timeout) = 0;

  /// get Database Schema
  ///
  /// @param [in]   database_id     target DB ID
  /// @param [out]  database_schema returned database schema
  /// @param [in]   timeout         timeout
  ///
  /// @retval OB_SUCCESS                   success
  /// @retval OB_TENANT_HAS_BEEN_DROPPED   tenent has been dropped
  /// @retval OB_TIMEOUT                   timeout
  /// @retval other error code             fail
  virtual int get_database_schema(
      const uint64_t tenant_id,
      uint64_t database_id,
      const oceanbase::share::schema::ObSimpleDatabaseSchema *&database_schema,
      const int64_t timeout) = 0;

  ////*********************************************
  //// NOTE: Note that subsequent additions to the interface will have to take into account the return value case when the tenant does not exist.
  //// I: requires the schema module to return a specific error code OB_TENANT_HAS_BEEN_DROPPED when the tenant does not exist, libobcdc will exit the retry loop when it encounters this error code
  ////
  //// II: The use of the place to determine the tenant does not exist error code OB_TENANT_HAS_BEEN_DROPPED, considered normal
};

///////////////////////////////////// IObLogSchemaGetter /////////////////////////////////
class IObLogSchemaGetter
{
public:
  virtual ~IObLogSchemaGetter() {}

public:
  // allow_lazy: lazy mode: all full schemas are supported, ObSimpleTableSchemaV2
  // non-lazy mode: all supported
  // force_fallback: all supported
  // force_lazy: fetching ObSimpleTableSchemaV2, ObTablegroupSchema, full db_schema/tenant_schema supported
  // full table_schema is not supported
  ///
  /// Get Schema Guard in Lazy mode, here use force_lazy mode
  ///
  /// The modules that currently call this interface are.
  /// 1. PartMgr: refresh schema to get ObTablegroupSchema and ObSimpleTableSchemaV2; and query_partition_status
  /// 2. DdlHandler: DDLs that do not require PartMgr processing, need to refresh schema to get tenant_name and db_name
  /// 3. TenantMgr: add tenant, refresh schema to get tenant_schema_info
  ///
  /// @param [in] tenant_id     Tenant ID
  /// @param [in] version       target version
  /// @param [in] timeout       timeout timeout
  /// @param [out] schema_guard The Schema Guard returned
  ///
  /// @retval OB_SUCCESS                   success
  /// @retval OB_TENANT_HAS_BEEN_DROPPED   tenent has been dropped
  /// @retval OB_TIMEOUT                   timeout
  /// @retval other error code             fail
  virtual int get_lazy_schema_guard(const uint64_t tenant_id,
      const int64_t version,
      const int64_t timeout,
      IObLogSchemaGuard &schema_guard) = 0;

  /// Get Schema Guard in fallback mode
  ///
  /// @param [in] tenant_id     Tenant ID
  /// @param [in] version       target version
  /// @param [in] timeout       timeout timeout
  /// @param [out] schema_guard The Schema Guard returned
  ///
  /// @retval OB_SUCCESS                   success
  /// @retval OB_TENANT_HAS_BEEN_DROPPED   tenent has been dropped
  /// @retval OB_TIMEOUT                   timeout
  /// @retval other error code             fail
  virtual int get_fallback_schema_guard(const uint64_t tenant_id,
      const int64_t version,
      const int64_t timeout,
      IObLogSchemaGuard &schema_guard) = 0;

  /// Get Simple Table Schema while getting Schema Guard
  /// NOTE:
  /// 1. Special optimizations can be made inside the function
  /// 2. The returned Schema Guard may or may not be in Lazy mode
  ///
  /// Implementation flow:
  /// 1. Get the latest version of the local schema by default, here it is the full schema already refreshed, no need for force_lazy
  /// 2. Check if the local latest meets the condition, if it does then it can be used
  /// 3. Otherwise force force_lazy mode to fetch schema
  ///
  /// Usage Scenarios:
  /// 1. Formatter formatting data
  /// 2. ParMgr refreshes schema
  ///
  /// @param [in] tenant_id         Tenant ID
  /// @param [in] expected_version  Target version
  /// @param [in] timeout           timeout timeout
  /// @param [out] schema_guard     Returned Schema Guard
  /// @param [out] tb_schema        Returned Simple Table Schema
  ///
  /// @retval OB_SUCCESS                   success
  /// @retval OB_TENANT_HAS_BEEN_DROPPED   tenent has been dropped
  /// @retval OB_TIMEOUT                   timeout
  /// @retval other error code             fail
  virtual int get_schema_guard_and_table_schema(
      const uint64_t tenant_id,
      const uint64_t table_id,
      const int64_t expected_version,
      const int64_t timeout,
      IObLogSchemaGuard &schema_guard,
      const oceanbase::share::schema::ObSimpleTableSchemaV2 *&tb_schema) = 0;

  /// Get the Table Schema while getting the Schema Guard
  ///
  /// @param [in] tenant_id         Tenant ID
  /// @param [in] expected_version  Target version
  /// @param [in] timeout           timeout timeout
  /// @param [out] schema_guard     Returned Schema Guard
  /// @param [out] full_tb_schema   Full Table Schema returned
  ///
  /// @retval OB_SUCCESS                   success
  /// @retval OB_TENANT_HAS_BEEN_DROPPED   tenent has been dropped
  /// @retval OB_TIMEOUT                   timeout
  /// @retval other error code             fail
  virtual int get_schema_guard_and_full_table_schema(
      const uint64_t tenant_id,
      const uint64_t table_id,
      const int64_t expected_version,
      const int64_t timeout,
      IObLogSchemaGuard &schema_guard,
      const oceanbase::share::schema::ObTableSchema *&full_tb_schema) = 0;

  /// Get the corresponding Schema version, based on the timestamp
  ///
  /// @param [in] tenant_id       Tenant ID
  /// @param [in] timestamp       Timestamp
  /// @param [out] schema_version The Schema version to return
  /// @param [in] timeout         timeout
  ///
  /// @retval OB_SUCCESS                   success
  /// @retval OB_TENANT_HAS_BEEN_DROPPED   tenent has been dropped
  /// @retval OB_TIMEOUT                   timeout
  /// @retval other error code             fail
  virtual int get_schema_version_by_timestamp(const uint64_t tenant_id,
      const int64_t timestamp,
      int64_t &schema_version,
      const int64_t timeout) = 0;

  virtual bool is_inited() const = 0;

  /// Get the schema version at the end of the tenant's first DDL transaction, i.e. the first available schema version
  /// Users dynamically add tenants
  ///
  /// @retval OB_SUCCESS                   success
  /// @retval OB_TENANT_HAS_BEEN_DROPPED   tenent has been dropped
  /// @retval OB_TIMEOUT                   timeout
  /// @retval other error code             fail
  virtual int get_first_trans_end_schema_version(const uint64_t tenant_id,
      int64_t &schema_version,
      const int64_t timeout) = 0;

  /// get split schema version
  ///
  /// @retval OB_SUCCESS                   success
  /// @retval OB_TIMEOUT                   timeout
  /// @retval other error code             fail
  virtual int load_split_schema_version(int64_t &split_schema_version, const int64_t timeout) = 0;

  // Print statistics
  virtual void print_stat_info() = 0;

  // Periodic memory recycling
  virtual void try_recycle_memory() = 0;

  // Get the latest schema version that the tenant has been flushed to
  // 1. For version 1.4, the schema is not split, so even though the tenant_id is specified, the latest schema version of the cluster is still returned
  // 2. For version 2.21 and above, the schema service is guaranteed to return the tenant level schema version
  //
  /// @retval OB_SUCCESS                   success
  /// @retval OB_TENANT_HAS_BEEN_DROPPED   tenent has been dropped
  /// @retval other error code             fail
  virtual int get_tenant_refreshed_schema_version(const uint64_t tenant_id, int64_t &version) = 0;

  //// NOTE: Note that all subsequent additions to the interface should consider the return value case when the tenant does not exist.
  //// I: requires the schema module to return a specific error code OB_TENANT_HAS_BEEN_DROPPED when a tenant does not exist. libobcdc encounters this error code and only this error code will determine that the tenant has been deleted and exit the retry loop
  ////
  //// II: Where used to determine that the tenant does not have the error code OB_TENANT_HAS_BEEN_DROPPED, it is considered normal
};


///////////////////////////////////// ObLogSchemaGuard /////////////////////////////////
class ObLogSchemaGuard : public IObLogSchemaGuard
{
public:
  ObLogSchemaGuard();
  virtual ~ObLogSchemaGuard();

public:
  share::schema::ObSchemaGetterGuard &get_guard() { return guard_; }
  void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }

public:
  int get_table_schema(
      const uint64_t tenant_id,
      const uint64_t table_id,
      const share::schema::ObTableSchema *&table_schema,
      const int64_t timeout);
  int get_table_schema(
      const uint64_t tenant_id,
      const uint64_t table_id,
      const share::schema::ObSimpleTableSchemaV2 *&table_schema,
      const int64_t timeout);
  int get_database_schema_info(
      const uint64_t tenant_id,
      uint64_t database_id,
      DBSchemaInfo &db_schema_info,
      const int64_t timeout);
  int get_tenant_schema_info(uint64_t tenant_id,
      TenantSchemaInfo &tenant_schema_info,
      const int64_t timeout);
  int get_tenant_name(uint64_t tenant_id,
      const char *&tenant_name,
      const int64_t timeout);
  int get_available_tenant_ids(common::ObIArray<uint64_t> &tenant_ids,
      const int64_t timeout);
  int get_table_schemas_in_tenant(const uint64_t tenant_id,
      common::ObIArray<const share::schema::ObSimpleTableSchemaV2 *> &table_schemas,
      const int64_t timeout);
  int get_schema_version(const uint64_t tenant_id, int64_t &schema_version) const;
  int get_table_ids_in_tablegroup(const uint64_t tenant_id,
      const uint64_t tablegroup_id,
      common::ObIArray<uint64_t> &table_id_array,
      const int64_t timeout);
  int get_tenant_compat_mode(const uint64_t tenant_id,
      lib::Worker::CompatMode &compat_mode,
      const int64_t timeout);

protected:
  int get_tenant_info(uint64_t tenant_id,
      const share::schema::ObTenantSchema *&tenant_info,
      const int64_t timeout);
  int get_tenant_info(uint64_t tenant_id,
      const oceanbase::share::schema::ObSimpleTenantSchema *&tenant_info,
      const int64_t timeout);
  int get_database_schema(
      const uint64_t tenant_id,
      uint64_t database_id,
      const share::schema::ObDatabaseSchema *&database_schema,
      const int64_t timeout);
  int get_database_schema(
      const uint64_t tenant_id,
      uint64_t database_id,
      const oceanbase::share::schema::ObSimpleDatabaseSchema *&database_schema,
      const int64_t timeout);

private:
  int check_force_fallback_mode_(const char *func) const;
  int check_force_fallback_mode_(const uint64_t tenant_id, const char *func) const;
  int do_check_force_fallback_mode_(const bool is_lazy, const char *func) const;
  int get_is_lazy_mode_(bool &is_lazy) const;

private:
  uint64_t  tenant_id_;
  share::schema::ObSchemaGetterGuard guard_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogSchemaGuard);
};

///////////////////////////////////// ObLogSchemaGetter /////////////////////////////////

typedef oceanbase::share::schema::ObMultiVersionSchemaService SchemaServiceType;

class ObLogSchemaGetter : public IObLogSchemaGetter
{
public:
  static const int64_t RECYCLE_MEMORY_INTERVAL = 60 * 1000L * 1000L;

public:
  ObLogSchemaGetter();
  virtual ~ObLogSchemaGetter();

public:
  int get_lazy_schema_guard(const uint64_t tenant_id,
      const int64_t version,
      const int64_t timeout,
      IObLogSchemaGuard &schema_guard);
  int get_fallback_schema_guard(const uint64_t tenant_id,
      const int64_t version,
      const int64_t timeout,
      IObLogSchemaGuard &schema_guard);
  int get_schema_guard_and_table_schema(
      const uint64_t tenant_id,
      const uint64_t table_id,
      const int64_t expected_version,
      const int64_t timeout,
      IObLogSchemaGuard &schema_guard,
      const oceanbase::share::schema::ObSimpleTableSchemaV2 *&tb_schema);
  int get_schema_guard_and_full_table_schema(
      const uint64_t tenant_id,
      const uint64_t table_id,
      const int64_t expected_version,
      const int64_t timeout,
      IObLogSchemaGuard &schema_guard,
      const oceanbase::share::schema::ObTableSchema *&full_tb_schema);
  int get_schema_version_by_timestamp(const uint64_t tenant_id,
      const int64_t timestamp,
      int64_t &schema_version,
      const int64_t timeout);
  bool is_inited() const { return inited_; }
  int get_first_trans_end_schema_version(const uint64_t tenant_id,
      int64_t &schema_version,
      const int64_t timeout);
  int load_split_schema_version(int64_t &split_schema_version, const int64_t timeout);
  void print_stat_info()
  {
    schema_service_.dump_schema_statistics();
  }
  void try_recycle_memory()
  {
    (void)schema_service_.try_eliminate_schema_mgr();
  }
  int get_tenant_refreshed_schema_version(const uint64_t tenant_id, int64_t &version);

public:
  int init(common::ObMySQLProxy &mysql_proxy,
      common::ObCommonConfig *config,
      const int64_t max_cached_schema_version_count,
      const int64_t max_history_schema_version_count);
  void destroy();

private:
  // 1. ObSimpleTableSchemaV2 must use force lazy mode
  // 2. ObTableSchema cannot be fetched in force lazy mode
  int get_schema_guard_and_simple_table_schema_(
      const uint64_t tenant_id,
      const uint64_t table_id,
      const int64_t expected_version,
      const int64_t timeout,
      const bool force_lazy,
      IObLogSchemaGuard &schema_guard,
      const oceanbase::share::schema::ObSimpleTableSchemaV2 *&tb_schema);
  int get_schema_guard_(const uint64_t tenant_id,
      const bool specify_version_mode,
      const int64_t expected_version,
      const int64_t timeout,
      IObLogSchemaGuard &guard,
      const share::schema::ObMultiVersionSchemaService::RefreshSchemaMode refresh_schema_mode,
      int64_t &refreshed_version);
  int refresh_to_expected_version_(const uint64_t tenant_id,
      const int64_t expected_version,
      const int64_t timeout,
      int64_t &latest_version);
  int check_schema_guard_suitable_for_table_(
      IObLogSchemaGuard &schema_guard,
      const uint64_t tenant_id,
      const uint64_t table_id,
      const int64_t expected_version,
      const int64_t refreshed_version,
      const int64_t timeout,
      const oceanbase::share::schema::ObSimpleTableSchemaV2 *&tb_schema,
      bool &is_suitable);

private:
  bool                  inited_;
  SchemaServiceType     &schema_service_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogSchemaGetter);
};
} // namespace libobcdc
} // namespace oceanbase
#endif /* OCEANBASE_LIBOBCDC_SCHEMA_GETTER_H__ */
