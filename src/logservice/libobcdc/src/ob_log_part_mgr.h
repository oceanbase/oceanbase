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
 * PartMgr is used to manage logstream for OBCDC
 */

#ifndef OCEANBASE_LIBOBCDC_OB_LOG_PART_MGR_H_
#define OCEANBASE_LIBOBCDC_OB_LOG_PART_MGR_H_

#include "lib/lock/ob_thread_cond.h"            // ObThreadCond
#include "share/schema/ob_schema_struct.h"      // PartitionStatus
#include "logservice/data_dictionary/ob_data_dict_struct.h"  // ObDictTableMeta
#include "ob_log_table_id_cache.h"              // GIndexCache, TableIDCache
#include "ob_cdc_tablet_to_table_info.h"        // TabletToTableInfo
#include "ob_log_utils.h"                       // _SEC_

namespace oceanbase
{
namespace share
{
namespace schema {
class ObPartitionSchema;
class ObSimpleTableSchemaV2;
class ObTableSchema;
class ObTablegroupSchema;
} // namespace schema
} // namespace share

using share::schema::ObPartitionSchema;
using share::schema::ObSimpleTableSchemaV2;
using share::schema::ObTableSchema;
using share::schema::ObTablegroupSchema;

namespace libobcdc
{
class ObLogSchemaGuard;

////////////////////////////////////////////////////////////////////////////////////////

class IObLogPartMgr
{
public:
  virtual ~IObLogPartMgr() {}

public:
  /// Init TabletToTableInfo:
  /// add all user tablet(with tenant_id_ and cur_schema_version) into tablet_to_table_info
  /// @param timeout                schema operation timeout
  /// @retval OB_SUCCESS            op success
  virtual int add_all_user_tablets_info(const int64_t timeout) = 0;

  /// Init TabletToTableInfo by Data Dictionary:
  /// add all user tablet(with tenant_id_ and cur_schema_version) into tablet_to_table_info
  /// @param timeout                operation timeout
  /// @retval OB_SUCCESS            op success
  virtual int add_all_user_tablets_info(
      const ObIArray<const datadict::ObDictTableMeta *> &table_metas,
      const int64_t timeout) = 0;

  /// Add a table
  /// @note must be called by a single thread in order according to the Schema version, should not concurrently add table in a random order
  ///
  /// @param table_id               Table ID
  /// @param start_schema_version   The Schema version of the start service
  /// @param start_serve_tstamp     The timestamp of the start service
  /// @param is_create_partition    whether it is a newly created partition
  /// @param [out] is_table_should_ignore_in_committer Whether to filter the DDL of this added table in the committer
  /// @param [out] schema_guard     schema guard
  /// @param [out] tenant_name      Returned tenant name
  /// @param [out] db_name          DB name returned
  /// @param timeout                timeout time
  ////
  /// TODO Consider the specific meaning of table/db/tenant schema when it is NULL, currently unified as TENANT_HAS_BEEN_DROPPED, which has a problematic meaning, if the caller encounters this error code and ignores it, the result is fine
  /// @retval OB_SUCCESS                   success
  /// @retval OB_TIMEOUT                   timeout
  /// @retval OB_TENANT_HAS_BEEN_DROPPED   fail to fetch schema table, database, tenant, tenant may dropped
  /// @retval other error code             fail
  virtual int add_table(const uint64_t table_id,
      const int64_t start_schema_version,
      const int64_t start_serve_tstamp,
      const bool is_create_partition,
      bool &is_table_should_ignore_in_committer,
      ObLogSchemaGuard &schema_guard,
      const char *&tenant_name,
      const char *&db_name,
      const int64_t timeout) = 0;

  /// Add a global unique index table, create index table scenario
  /// @note must be called by a single thread in order by Schema version, not concurrently and in random order
  ////
  /// Support for handling OB_DDL_CREATE_GLOBAL_INDEX global normal indexes and global unique index tables
  /// Support for handling OB_DDL_CREATE_INDEX The process of refreshing the schema will filter out the normal indexes and keep the unique indexes
  /// 1. For globally unique indexes, add partitions if they match, and add TableIDCache
  /// 2. For global common indexes, add global common index cache
  /// 3. Add TableIDCache for unique indexes (not global)
  ///
  /// @param table_id               Table ID
  /// @param start_schema_version   The Schema version of the start service
  /// @param start_serve_tstamp     Timestamp of the start service
  /// @param [out] schema_guard     schema guard
  /// @param [out] tenant_name      The name of the tenant returned
  /// @param [out] db_name          DB name returned
  /// @param timeout                timeout time
  ///
  /// @retval OB_SUCCESS                   success
  /// @retval OB_TIMEOUT                   timeout
  /// @retval OB_TENANT_HAS_BEEN_DROPPED   fail to fetch schema table, database, tenant, tenant may dropped
  /// @retval other error code             fail
  virtual int add_index_table(const uint64_t table_id,
      const int64_t start_schema_version,
      const int64_t start_serve_tstamp,
      ObLogSchemaGuard &schema_guard,
      const char *&tenant_name,
      const char *&db_name,
      const int64_t timeout) = 0;

  /// alter table
  /// @note This function must be called in order by a single thread according to the schema version,
  /// can not concurrently added in random order
  ///
  /// @param table_id                     tableId
  /// @param schema_version_before_alter  old Schema version
  /// @param schema_version_after_alter   new Schema version
  /// @param [out] old_schema_guard       old schema guard
  /// @param [out] new_schema_guard       new schema guard
  /// @param [out] old_tenant_name        return old tenant name
  /// @param [out] old_db_name            return db name
  /// @param timeout                      Timeout
  ///
  /// @retval OB_SUCCESS                   success
  /// @retval OB_TIMEOUT                   timeout
  /// @retval OB_TENANT_HAS_BEEN_DROPPED   fail to fetch schema table, database, tenant, tenant may dropped
  /// @retval other error code             fail
  virtual int alter_table(const uint64_t table_id,
      const int64_t schema_version_before_alter,
      const int64_t schema_version_after_alter,
      const int64_t start_serve_timestamp,
      ObLogSchemaGuard &old_schema_guard,
      ObLogSchemaGuard &new_schema_guard,
      const char *&old_tenant_name,
      const char *&old_db_name,
      const char *event,
      const int64_t timeout) = 0;

  /// Delete a table
  /// @note must be called by a single thread in order by Schema version, no concurrent messy deletions
  ////
  /// @param table_id                       Table ID
  /// @param schema_version_before_drop     Deletes the Schema version before the table is dropped
  /// @param schema_version_after_drop      Deletes the Schema version after the table
  /// @param [out] is_table_should_ignore_in_committer whether to filter the DDL of this drop table in the committer
  /// @param [out] old_schema_guard         old schema guard
  /// @param [out] tenant_name              The old tenant name returned
  /// @param [out] db_name                  Old DB name returned
  /// @param timeout                        timeout time
  ///
  /// @retval OB_SUCCESS                   success
  /// @retval OB_TIMEOUT                   timeout
  /// @retval OB_TENANT_HAS_BEEN_DROPPED   fail to fetch schema table, database, tenant, tenant may dropped
  /// @retval other error code             fail
  virtual int drop_table(const uint64_t table_id,
      const int64_t schema_version_before_drop,
      const int64_t schema_version_after_drop,
      bool &is_table_should_ignore_in_committer,
      ObLogSchemaGuard &old_schema_guard,
      const char *&tenant_name,
      const char *&db_name,
      const int64_t timeout) = 0;

  /// Delete global index table, Delete index table scenario
  /// @note must be called by a single thread in order by Schema version, not concurrently in order
  ////
  /// Support for handling OB_DDL_DROP_GLOBAL_INDEX global normal indexes and global unique index tables
  /// Support for handling OB_DDL_DROP_INDEX The process of refreshing the schema will filter out the normal indexes and keep the unique indexes
  /// 1. For globally unique indexes, delete the corresponding partition
  /// 2. For global common indexes, delete the global common index cache
  /// 3. Delete TableIDCache for unique indexes (not global)
  ////
  /// @param table_id                       TableID
  /// @param schema_version_before_drop     Delete the Schema version before the table
  /// @param schema_version_after_drop      Delete the Schema version after the table
  /// @param [out] old_schema_guard         old schema guard
  /// @param [out] tenant_name              old tenant name returned
  /// @param [out] db_name                  Old DB name returned
  /// @param timeout                        timeout time
  ///
  /// @retval OB_SUCCESS                   success
  /// @retval OB_TIMEOUT                   timeout
  /// @retval OB_TENANT_HAS_BEEN_DROPPED   fail to fetch schema table, database, tenant, tenant may dropped
  /// @retval other error code             fail
  virtual int drop_index_table(const uint64_t table_id,
      const int64_t schema_version_before_drop,
      const int64_t schema_version_after_drop,
      ObLogSchemaGuard &old_schema_guard,
      const char *&tenant_name,
      const char *&db_name,
      const int64_t timeout) = 0;

  /// Add all tables under the current tenant
  ///
  /// @retval OB_SUCCESS                   success
  /// @retval OB_TIMEOUT                   timeout
  /// @retval OB_TENANT_HAS_BEEN_DROPPED   fail to fetch schema table, database, tenant, tenant may dropped
  /// @retval other error code             fail
  virtual int add_all_tables(
      const int64_t start_serve_tstamp,
      const int64_t start_schema_version,
      const int64_t timeout) = 0;

  /// update schema version
  virtual int update_schema_version(const int64_t schema_version) = 0;

  /// Filtering table data within PG, based on table_id to determine if it is in TableIDCache
  ///
  /// @retval OB_SUCCESS                   success
  /// @retval other error code             fail
  virtual int is_exist_table_id_cache(const uint64_t table_id,
      bool &is_exist) = 0;

  /// Filtering table data within PG, handling future table logic based on table_version
  virtual int handle_future_table(const uint64_t table_id,
      const int64_t table_version,
      const int64_t timeout,
      bool &is_exist) = 0;
  virtual int get_table_info_of_tablet_id(
      const common::ObTabletID &tablet_id,
      ObCDCTableInfo &table_info) const = 0;
  virtual int apply_create_tablet_change(const ObCDCTabletChangeInfo &tablet_change_info) = 0;
  virtual int apply_delete_tablet_change(const ObCDCTabletChangeInfo &tablet_change_info) = 0;
};

/////////////////////////////////////////////////////////////////////////////

class ObLogTenant;
class ObLogPartMgr : public IObLogPartMgr
{
private:
  static const int64_t PRINT_LOG_INTERVAL = 10 * _SEC_;

public:
  explicit ObLogPartMgr(ObLogTenant &tenant);
  virtual ~ObLogPartMgr();

public:
  int init(const uint64_t tenant_id,
      const int64_t start_schema_version,
      const bool enable_oracle_mode_match_case_sensitive,
      GIndexCache &gi_cache,
      TableIDCache &table_id_cache);
  void reset();
  int64_t get_schema_version() const { return ATOMIC_LOAD(&cur_schema_version_); }

public:
  virtual int add_all_user_tablets_info(const int64_t timeout);
  virtual int add_all_user_tablets_info(
      const ObIArray<const datadict::ObDictTableMeta *> &table_metas,
      const int64_t timeout);
  virtual int add_table(const uint64_t table_id,
      const int64_t start_schema_version,
      const int64_t start_serve_tstamp,
      const bool is_create_partition,
      bool &is_table_should_ignore_in_committer,
      ObLogSchemaGuard &schema_guard,
      const char *&tenant_name,
      const char *&db_name,
      const int64_t timeout);
  virtual int alter_table(const uint64_t table_id,
      const int64_t schema_version_before_alter,
      const int64_t schema_version_after_alter,
      const int64_t start_serve_timestamp,
      ObLogSchemaGuard &old_schema_guard,
      ObLogSchemaGuard &new_schema_guard,
      const char *&old_tenant_name,
      const char *&old_db_name,
      const char *event,
      const int64_t timeout);
  virtual int drop_table(const uint64_t table_id,
      const int64_t schema_version_before_drop,
      const int64_t schema_version_after_drop,
      bool &is_table_should_ignore_in_committer,
      ObLogSchemaGuard &old_schema_guard,
      const char *&tenant_name,
      const char *&db_name,
      const int64_t timeout);
  virtual int add_index_table(const uint64_t table_id,
      const int64_t start_schema_version,
      const int64_t start_serve_tstamp,
      ObLogSchemaGuard &schema_guard,
      const char *&tenant_name,
      const char *&db_name,
      const int64_t timeout);
  virtual int drop_index_table(const uint64_t table_id,
      const int64_t schema_version_before_drop,
      const int64_t schema_version_after_drop,
      ObLogSchemaGuard &old_schema_guard,
      const char *&tenant_name,
      const char *&db_name,
      const int64_t timeout);
  virtual int add_all_tables(
      const int64_t start_serve_tstamp,
      const int64_t start_schema_version,
      const int64_t timeout);
  virtual int update_schema_version(const int64_t schema_version);
  virtual int is_exist_table_id_cache(const uint64_t table_id,
      bool &is_exist);
  virtual int handle_future_table(const uint64_t table_id,
      const int64_t table_version,
      const int64_t timeout,
      bool &is_exist);
  virtual int get_table_info_of_tablet_id(const common::ObTabletID &tablet_id, ObCDCTableInfo &table_info) const
  {
    return tablet_to_table_info_.get_table_info_of_tablet(tablet_id, table_info);
  }

  virtual int apply_create_tablet_change(const ObCDCTabletChangeInfo &tablet_change_info);
  virtual int apply_delete_tablet_change(const ObCDCTabletChangeInfo &tablet_change_info);

private:
  template<class TableMeta>
  int insert_tablet_table_info_(
      TableMeta &table_meta,
      const common::ObIArray<common::ObTabletID> &tablet_ids);

  // operation:
  // 1. global normal index cache
  // 2. TableIDCache
  int add_table_id_into_cache_(const ObSimpleTableSchemaV2 &tb_schema,
      const char *db_name,
      const uint64_t primary_table_id);
  int clean_table_id_cache_();
  int remove_table_id_from_cache_(const ObSimpleTableSchemaV2 &tb_schema);
  int is_exist_table_id_cache_(const uint64_t table_id,
      const bool is_global_normal_index,
      bool &is_exist);

  int try_get_offline_ddl_origin_table_schema_(const ObSimpleTableSchemaV2 &table_schema,
      ObLogSchemaGuard &schema_guard,
      const int64_t timeout,
      const ObSimpleTableSchemaV2 *&origin_table_schema);
  /// Add a table
  /// When manipulating a global index table, primary_table_schema represents its primary table schema, add_index_table()/do_add_all_tables_()
  /// When handle OFFLINE_DDL scenario, if it is a hidden table, primary_table_schema is the table schema of the origin associated table, add_table()/do_add_all_tables()
  ///
  // The add_table_function is as follows.
  //   (1) Partition changes: add primary table partitions, global unique index table partitions
  //   (2) Global common index cache: if the main table matches, add a global common index cache
  //   (3) TableIDCache: add main table table_id, unique index table_id, global unique index table_id, and the above two functions have intersection, need special attention under
  //
  // In particular.
  // 1. the start-up moment:
  //   (1) Partition addition: Adding main table partition, global unique index table partition
  //   (2) Global common index cache: if the primary table matches, add the global common index cache
  //   (3) TableIDCache.
  //       a. Main table table_id
  //       b. global index table_id
  //       c. get_schemas_based_on_table_schema_no_more_filtering_for_unique_index_tables, where you need to add unique index tables
  //
  // 2. handle OB_DDL_CREATE_TABLE
  //   (1) Add a master table partition that matches the whitelist, and add a TableIDCache
  // 3. Process OB_DDL_CREATE_GLOBAL_INDEX
  // add_table_ supports handling of global normal indexes and global unique index tables
  //   (1) For globally unique indexes, add partitions if they match, and add TableIDCache
  //   (2) For global common indexes, add cache
  int add_table_(const int64_t start_serve_tstamp,
      const bool is_create_partition,
      const ObSimpleTableSchemaV2 *tb_schema,
      const char *tenant_name,
      const char *db_name,
      const int64_t timeout,
      const ObSimpleTableSchemaV2 *primary_table_schema = NULL);

  // drop_table_ functions as follows.
  //   (1) Partition change: delete the main table partition, the global unique index table partition
  //   (2) Global common index cache: delete the corresponding global common index
  //   (3) TableIDCache:
  //        a. Delete unique indexes
  //        b. Delete main table, global unique index
  int drop_table_(const ObSimpleTableSchemaV2 *table_schema);
  // Filtering tables
  // 1. DDL tables will not be filtered
  // 2. Non-user tables are filtered (global unique indexes/unique indexes require special handling)
  // 3. proxy tables will be filtered
  // 4. user tables are matched based on a whitelist
  // where the global unique index table needs to be refreshed with the master schema, so that it is whitelisted based on the master table
  // when handle OFFLINE_DDL scenario, the new hidden table needs to refresh the original table schema to perform whitelist matching based on the original table name
  int filter_table_(const ObSimpleTableSchemaV2 *table_schema,
      const char *tenant_name,
      const char *db_name,
      const lib::Worker::CompatMode &compat_mode,
      bool &chosen,
      bool &is_primary_table_chosen, /* Indicates whether the global index table corresponds to the main table */
      const ObSimpleTableSchemaV2 *primary_table_schema = NULL);
  // is a unique index table (not a global unique index)
  // The unique index table contains.
  // INDEX_TYPE_UNIQUE_LOCAL
  // INDEX_TYPE_UNIQUE_GLOBAL
  // INDEX_TYPE_UNIQUE_GLOBAL_LOCAL_STORAGE
  // where INDEX_TYPE_UNIQUE_GLOBAL is a globally unique index
  bool is_unique_index_table_but_expect_global_unqiue_index_(const ObSimpleTableSchemaV2 &table_schema) const;

  /// Tenant monitoring items: adding and removing tenants
  /// Adding "Tenants" to the service
  int add_served_tenant_for_stat_(const char *tenant_name,
      const uint64_t tenant_id);
  /// delete served tenant
  int del_served_tenant_for_stat_(const uint64_t tenant_id);

  /// filter tenant
  int filter_tenant_(const char *tenant_name,
      bool &chosen);

  // get Schema
  int get_schema_(const int64_t timestamp, const int64_t timeout, ObLogSchemaGuard &schema_guard);

  int check_cur_schema_version_when_handle_future_table_(const int64_t schema_version,
      const int64_t end_time);

  bool is_proxy_table(const char *tenant_name, const char *db_name, const char *tb_name);

  int do_add_all_tables_(
      ObLogSchemaGuard &schema_guard,
      const int64_t start_serve_tstamp,
      const int64_t start_schema_version,
      const int64_t timeout);

// function about schema
private:
  // get Simple Table Schema
  int get_simple_table_schema_(
      const uint64_t table_id,
      const int64_t timeout,
      ObLogSchemaGuard &schema_guard,
      const ObSimpleTableSchemaV2 *&table_schema);
  // get Full Table Schema
  int get_full_table_schema_(const uint64_t table_id,
      const int64_t timeout,
      ObLogSchemaGuard &schema_guard,
      const ObTableSchema *&tb_schema);
  int get_schema_guard_and_table_schema_(const uint64_t table_id,
      const int64_t schema_version,
      const int64_t timeout,
      ObLogSchemaGuard &schema_guard,
      const ObSimpleTableSchemaV2 *&tb_schema);
  int get_lazy_schema_guard_and_tablegroup_schema_(
      const uint64_t tablegroup_id,
      const int64_t schema_version,
      const int64_t timeout,
      ObLogSchemaGuard &schema_guard,
      const ObTablegroupSchema *&tg_schema);
  // get schema info based on Table Schema
  int get_schema_info_based_on_table_schema_(const ObSimpleTableSchemaV2 *tb_schema,
      ObLogSchemaGuard &schema_guard,
      const int64_t timeout,
      bool &table_is_ignored,
      const char *&tenant_name,
      const char *&db_schema);
  // init guard to get Simple Table Schema、db and tenant info
  int get_schema_guard_and_schemas_(const uint64_t table_id,
      const int64_t schema_version,
      const int64_t timeout,
      bool &table_is_ignored,
      ObLogSchemaGuard &schema_guard,
      const ObSimpleTableSchemaV2 *&tb_schema,
      const char *&tenant_name,
      const char *&db_name);

private:
  ObLogTenant       &host_;

  bool              inited_;
  uint64_t          tenant_id_;
  GIndexCache       *global_normal_index_table_cache_; // global normal index cache
  TableIDCache      *table_id_cache_;
  TabletToTableInfo tablet_to_table_info_; // TabletID->TableID

  int64_t           cur_schema_version_ CACHE_ALIGNED;

  // Default whitelist match insensitive
  bool              enable_oracle_mode_match_case_sensitive_;
  bool              enable_check_schema_version_;

  // Conditional
  common::ObThreadCond   schema_cond_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogPartMgr);
};
}
}
#endif
