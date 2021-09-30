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

#ifndef OCEANBASE_LIBOBLOG_OB_LOG_PART_MGR_H_
#define OCEANBASE_LIBOBLOG_OB_LOG_PART_MGR_H_

#include "common/ob_partition_key.h"            // ObPartitionKey
#include "lib/lock/ob_thread_cond.h"            // ObThreadCond
#include "share/schema/ob_schema_struct.h"      // PartitionStatus

#include "ob_log_part_info.h"                   // ObLogPartInfo, PartInfoMap
#include "ob_log_table_id_cache.h"              // GIndexCache, TableIDCache
#include "ob_log_part_callback.h"               // PartCBArray

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

namespace liboblog
{
class ObLogSchemaGuard;

////////////////////////////////////////////////////////////////////////////////////////

class IObLogPartMgr
{
public:
  virtual ~IObLogPartMgr() {}

public:
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

  /// Table splitting
  /// This call corresponds to the "start split" DDL, the main purpose of which is to add the split partition
  ///
  /// @param table_id                       Table ID
  /// @param new_schema_version             The version of the schema after the split
  /// @param start_serve_timestamp          The starting service time of the new partition
  /// @param [out] new_schema_guard         new schema guard
  /// @param [out] tenant_name              The new tenant name returned
  /// @param [out] db_name                  new DB name returned
  /// @param timeout                        timeout
  ///
  /// @retval OB_SUCCESS                   success
  /// @retval OB_TIMEOUT                   timeout
  /// @retval OB_TENANT_HAS_BEEN_DROPPED   fail to fetch schema table, database, tenant, tenant may dropped
  /// @retval other error code             fail
  virtual int split_table(const uint64_t table_id,
      const int64_t new_schema_version,
      const int64_t start_serve_timestamp,
      ObLogSchemaGuard &new_schema_guard,
      const char *&tenant_name,
      const char *&db_name,
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

  /// Supports tablegroup additions
  /// @note must be called by a single thread in order according to the Schema version, not concurrently and in random order
  ///
  /// @param tablegroup_id                  Tablegroup ID
  /// @param schema_version                 Schema version
  /// @param start_serve_timestamp          Start service timestamp
  /// @param [out] schema_guard             schema guard
  /// @param [out] tenant_name              Returned tenant name
  /// @param timeout                        timeout time
  ///
  /// @retval OB_SUCCESS                   success
  /// @retval OB_TIMEOUT                   timeout
  /// @retval OB_TENANT_HAS_BEEN_DROPPED   fail to fetch schema table, database, tenant, tenant may dropped
  /// @retval other error code             fail
  virtual int add_tablegroup_partition(
      const uint64_t tablegroup_id,
      const int64_t schema_version,
      const int64_t start_serve_timestamp,
      ObLogSchemaGuard &schema_guard,
      const char *&tenant_name,
      const int64_t timeout) = 0;

  /// Supports tablegroup deletion
  /// @note must be called by a single thread in order by Schema version, not concurrently added in random order
  /// @param tablegroup_id                  tablegroup ID
  /// @param schema_version_before_drop     The version of the Schema before the drop
  /// @param schema_version_after_drop      Schema version after drop
  /// @param [out] schema_guard             schema guard
  /// @param [out] tenant_name              The tenant name returned
  /// @param timeout                        timeout time
  ///
  /// @retval OB_SUCCESS                   success
  /// @retval OB_TIMEOUT                   timeout
  /// @retval OB_TENANT_HAS_BEEN_DROPPED   fail to fetch schema table, database, tenant, tenant may dropped
  /// @retval other error code             fail
  virtual int drop_tablegroup_partition(
      const uint64_t tablegroup_id,
      const int64_t schema_version_before_drop,
      const int64_t schema_version_after_drop,
      ObLogSchemaGuard &schema_guard,
      const char *&tenant_name,
      const int64_t timeout) = 0;

  /// PG split
  /// This call corresponds to the "start split" DDL, the main purpose of which is to add the split partition
  ///
  /// @param tenant_id              Tenant ID
  /// @param tablegroup_id          tablegroup ID
  /// @param new_schema_version     The version of the schema after the split
  /// @param start_serve_timestamp  Start service time of the new partition
  /// @param [out] schema_guard     schema guard
  /// @param [out] tenant_name      Returned tenant name
  /// @param timeout                timeout time
  ///
  /// @retval OB_SUCCESS                   success
  /// @retval OB_TIMEOUT                   timeout
  /// @retval OB_TENANT_HAS_BEEN_DROPPED   fail to fetch schema table, database, tenant, tenant may dropped
  /// @retval other error code             fail
  virtual int split_tablegroup_partition(
      const uint64_t tablegroup_id,
      const int64_t new_schema_version,
      const int64_t start_serve_timestamp,
      ObLogSchemaGuard &schema_guard,
      const char *&tenant_name,
      const int64_t timeout) = 0;

  /// Modify tablegroup
  /// 1. Support for dynamic partitioning of tablegroups
  /// 2. Support tablegroup splitting
  /// @note must be called by a single thread in order by Schema version, not concurrently added in random order
  ///
  /// @param tablegroup_id                  tablegroup ID
  /// @param schema_version_before_alter    old Schema version
  /// @param schema_version_after_alter     New Schema version
  /// @param start_serve_timestamp          Start service timestamp
  /// @param [out] old_schema_guard         old schema guard
  /// @param [out] new_schema_guard         new schema guard
  /// @param [out] tenant_name              Returned tenant name
  /// @param timeout                        timeout time
  ///
  /// @retval OB_SUCCESS                   success
  /// @retval OB_TIMEOUT                   timeout
  /// @retval OB_TENANT_HAS_BEEN_DROPPED   fail to fetch schema table, database, tenant, tenant may dropped
  /// @retval other error code             fail
  virtual int alter_tablegroup_partition(
      const uint64_t tablegroup_id,
      const int64_t schema_version_before_alter,
      const int64_t schema_version_after_alter,
      const int64_t start_serve_timestamp,
      ObLogSchemaGuard &old_schema_guard,
      ObLogSchemaGuard &new_schema_guard,
      const char *&tenant_name,
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

  /// Adding a general tenant internal table
  ///
  /// @retval OB_SUCCESS                   success
  /// @retval OB_TIMEOUT                   timeout
  /// @retval OB_TENANT_HAS_BEEN_DROPPED   fail to fetch schema table, database, tenant, tenant may dropped
  /// @retval other error code             fail
  virtual int add_inner_tables(const int64_t start_serve_tstamp,
    const int64_t start_schema_version,
    const int64_t timeout) = 0;

  /// Delete all tables under the tenant
  virtual int drop_all_tables() = 0;

  // Add the __all_ddl_operation table for this tenant
  // must be added successfully, otherwise an error is reported
  virtual int add_ddl_table(
      const int64_t start_serve_tstamp,
      const int64_t start_schema_version,
      const bool is_create_tenant) = 0;

  /// update schema version
  virtual int update_schema_version(const int64_t schema_version) = 0;

  /// Print partition service information
  virtual void print_part_info(int64_t &serving_part_count,
      int64_t &offline_part_count,
      int64_t &not_served_part_count) = 0;

  /// Check if a partition transaction is being served and if so, increase the number of running transactions on the partition
  /// Supports DDL and DML partitions
  ///
  /// @param [out] is_serving           return value, identifies whether the partition transaction is being served
  /// @param [in] key                   Partition key
  /// @param [in] prepare_log_id        Prepare log ID
  /// @param [in] prepare_log_timestamp Prepare log timestamp
  /// @param [in] inc_trans_count       Whether to increase the number of ongoing transactions if a partitioned transaction is being served
  /// @param [in] timeout               timeout time
  ////
  /// @retval OB_SUCCESS                Success
  /// @retval OB_TIMEOUT                timeout
  /// @retval Other return values       Fail
  virtual int inc_part_trans_count_on_serving(bool &is_serving,
      const common::ObPartitionKey &key,
      const uint64_t prepare_log_id,
      const int64_t prepare_log_timestamp,
      const bool print_participant_not_serve_info,
      const int64_t timeout) = 0;

  /// Decrement the number of running transactions in a partition
  ///
  /// @param key paritition key
  ///
  /// @retval OB_SUCCESS                Success
  /// @retval Other return values       Fail
  virtual int dec_part_trans_count(const common::ObPartitionKey &key) = 0;

  /// Offline partitions and perform recycling operations
  ///
  /// Calling scenario: The Committer receives the task to downline partitioning, ensures that all previous data has been output, and calls this interface to downline partitioning
  ////
  //// Cautions.
  /// 1. The partition's downline log is parallel to the DDL, and the downline log may be called in parallel with the DDL delete partition
  /// 2. The partition may be deleted before the "delete partition DDL" arrives, but it is guaranteed that no data will be dependent on the partition.
  /// 3. For partition splitting scenarios, the interface can only be relied upon to delete the old partition, the DDL is not responsible for the deletion and the aim is to ensure that all data is output
  /// 4. requires that there are no dependent transactions on the partition, i.e. a transaction count of 0, otherwise the partition cannot be reclaimed
  ///
  /// @retval OB_SUCCESS                   success
  /// @retval OB_TENANT_HAS_BEEN_DROPPED   fail to fetch schema table, database, tenant, tenant may dropped
  /// @retval other error code             fail
  virtual int offline_and_recycle_partition(const common::ObPartitionKey &pkey) = 0;

  /// Only offline partition, no forced recycle required
  ///
  /// @retval OB_SUCCESS                   success
  /// @retval OB_TENANT_HAS_BEEN_DROPPED   fail to fetch schema table, database, tenant, tenant may dropped
  /// @retval other error code             fail
  virtual int offline_partition(const common::ObPartitionKey &pkey) = 0;

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
      PartInfoMap &map,
      GIndexCache &gi_cache,
      TableIDCache &table_id_cache,
      PartCBArray &part_add_cb_array,
      PartCBArray &part_rc_cb_array);
  void reset();
  int64_t get_schema_version() const { return ATOMIC_LOAD(&cur_schema_version_); }

public:
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
  virtual int split_table(const uint64_t table_id,
      const int64_t new_schema_version,
      const int64_t start_serve_timestamp,
      ObLogSchemaGuard &new_schema_guard,
      const char *&tenant_name,
      const char *&db_name,
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
  virtual int add_tablegroup_partition(
      const uint64_t tablegroup_id,
      const int64_t schema_version,
      const int64_t start_serve_timestamp,
      ObLogSchemaGuard &schema_guard,
      const char *&tenant_name,
      const int64_t timeout);
  virtual int drop_tablegroup_partition(
      const uint64_t tablegroup_id,
      const int64_t schema_version_before_drop,
      const int64_t schema_version_after_drop,
      ObLogSchemaGuard &schema_guard,
      const char *&tenant_name,
      const int64_t timeout);
  virtual int split_tablegroup_partition(
      const uint64_t tablegroup_id,
      const int64_t new_schema_version,
      const int64_t start_serve_timestamp,
      ObLogSchemaGuard &schema_guard,
      const char *&tenant_name,
      const int64_t timeout);
  virtual int alter_tablegroup_partition(
      const uint64_t tablegroup_id,
      const int64_t schema_version_before_alter,
      const int64_t schema_version_after_alter,
      const int64_t start_serve_timestamp,
      ObLogSchemaGuard &old_schema_guard,
      ObLogSchemaGuard &new_schema_guard,
      const char *&tenant_name,
      const int64_t timeout);
  int add_all_tables(
      const int64_t start_serve_tstamp,
      const int64_t start_schema_version,
      const int64_t timeout);
  int add_inner_tables(const int64_t start_serve_tstamp,
    const int64_t start_schema_version,
    const int64_t timeout);
  int drop_all_tables();
  int add_ddl_table(
      const int64_t start_serve_tstamp,
      const int64_t start_schema_version,
      const bool is_create_tenant);
  virtual int update_schema_version(const int64_t schema_version);
  virtual int inc_part_trans_count_on_serving(bool &is_serving,
      const common::ObPartitionKey &key,
      const uint64_t prepare_log_id,
      const int64_t prepare_log_timestamp,
      const bool print_participant_not_serve_info,
      const int64_t timeout);
  virtual int dec_part_trans_count(const common::ObPartitionKey &key);
  virtual void print_part_info(int64_t &serving_part_count,
      int64_t &offline_part_count,
      int64_t &not_served_part_count);
  virtual int offline_and_recycle_partition(const common::ObPartitionKey &pkey);
  virtual int offline_partition(const common::ObPartitionKey &pkey);
  virtual int is_exist_table_id_cache(const uint64_t table_id,
      bool &is_exist);
  virtual int handle_future_table(const uint64_t table_id,
      const int64_t table_version,
      const int64_t timeout,
      bool &is_exist);

private:
  int add_tenant_all_tablegroup_(const uint64_t tenant_id,
      const int64_t schema_version,
      const int64_t start_serve_tstamp,
      const int64_t timeout);
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

  int add_tablegroup_partition_(
      const uint64_t tablegroup_id,
      const ObTablegroupSchema &tg_schema,
      const int64_t start_serve_timestamp,
      const bool is_create_partition,
      const char *tenant_name,
      const int64_t timeout);
  int drop_tablegroup_partition_(
      const uint64_t tablegroup_id,
      const ObTablegroupSchema &tg_schema);
  int split_tablegroup_partition_(
      const uint64_t tablegroup_id,
      const ObTablegroupSchema &tg_schema,
      const int64_t start_serve_timestamp);
  int get_table_ids_in_tablegroup_(const uint64_t tenant_id,
      const uint64_t tablegroup_id,
      const int64_t schema_version,
      const int64_t timeout,
      common::ObArray<uint64_t> &table_id_array);
  int alter_tablegroup_partition_when_is_not_binding_(
      const uint64_t tablegroup_id,
      const int64_t schema_version_before_alter,
      ObLogSchemaGuard &old_schema_guard,
      const int64_t schema_version_after_alter,
      ObLogSchemaGuard &new_schema_guard,
      const int64_t start_serve_timestamp,
      const share::ObWorker::CompatMode &compat_mode,
      const int64_t timeout);

  /// add/drop partition dynamicly(means by ddl)
  int alter_table_add_or_drop_partition_(
      const bool is_tablegroup,
      const bool has_physical_part,
      const int64_t start_serve_timestamp,
      const ObPartitionSchema *old_tb_schema,
      const ObPartitionSchema *new_tb_schema,
      const int64_t database_id,
      const char *event);
  int alter_table_drop_partition_(
      const bool is_tablegroup,
      const uint64_t table_id,
      const common::ObArray<int64_t> &drop_part_ids,
      const int64_t partition_cnt);
  int alter_table_add_partition_(
      const bool is_tablegroup,
      const bool has_physical_part,
      const uint64_t table_id,
      const common::ObArray<int64_t> &add_part_ids,
      const int64_t partition_cnt,
      const int64_t start_serve_timestamp,
      const uint64_t tablegroup_id,
      const uint64_t database_id);
  int split_table_(const ObSimpleTableSchemaV2 *tb_schema,
      const char *tenant_name,
      const char *db_name,
      const int64_t start_serve_timestamp,
      const share::ObWorker::CompatMode &compat_mode);
  /// Add a table
  /// When manipulating a global index table, primary_table_schema represents its primary table schema
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
  int filter_table_(const ObSimpleTableSchemaV2 *table_schema,
      const char *tenant_name,
      const char *db_name,
      const share::ObWorker::CompatMode &compat_mode,
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

  /// Filtering PG
  /// tablegroup matches based on whitelist
  int filter_tablegroup_(const ObTablegroupSchema *tg_schema,
      const char *tenant_name,
      const share::ObWorker::CompatMode &compat_mode,
      bool &chosen);
  /// add a partition
  int add_served_partition_(const common::ObPartitionKey &pkey,
      const common::ObPartitionKey &check_serve_info_pkey,
      const int64_t start_serve_tstamp,
      const bool is_create_partition,
      const bool has_physical_part,
      const uint64_t tablegroup_id,
      const uint64_t database_id,
      bool &add_succ);
  int add_served_part_pre_check_(const common::ObPartitionKey &pkey);
  int add_partition_(const common::ObPartitionKey& pkey,
      const int64_t start_tstamp,
      const bool is_create_partition,
      const bool is_served);
  int offline_partition_(const common::ObPartitionKey &pkey,
      const bool ensure_recycled_when_offlined = false);
  /// Recycle a partition: delete all relevant data structures of the partition
  int recycle_partition_(const common::ObPartitionKey &pkey, ObLogPartInfo *info);
  /// call callbacks of add-partition
  int call_add_partition_callbacks_(const common::ObPartitionKey &pkey,
      const int64_t start_tstamp,
      const uint64_t start_log_id);
  /// Notify the modules that the partition is ready for recycling
  int call_recycle_partition_callbacks_(const common::ObPartitionKey &pkey);

  /// Tenant monitoring items: adding and removing tenants
  /// Adding "Tenants" to the service
  int add_served_tenant_for_stat_(const char *tenant_name,
      const uint64_t tenant_id);
  /// delete served tenant
  int del_served_tenant_for_stat_(const uint64_t tenant_id);

  /// filter tenant
  int filter_tenant_(const char *tenant_name,
      bool &chosen);

  // Check if a partition is served
  // 1. partition by default according to the old way of calculating partitions, i.e.: partition tasks by table-group-id + partition-id
  // If table-group-id is invalid, divide tasks by database-id + partition-id
  // 2. When enable_new_partition_hash_algorithm = 1, partition according to the new calculation.
  // partition tasks by table_id + partition_id, avoiding tablegroup_id, database_id dependencies
  // TODO optimization
  // In a multi-instance scenario, this partitioning rule does not guarantee that partitions belonging to the same Partition Group are partitioned into one instance
  bool is_partition_served_(const common::ObPartitionKey &pkey,
      const uint64_t tablegroup_id,
      const uint64_t database_id) const;
  // get Schema
  int get_schema_(const int64_t timestamp, const int64_t timeout, ObLogSchemaGuard &schema_guard);

  /// Check if the partition is serviced, if so, increase the number of partition statements
  /// @retval OB_SUCCESS                   success
  /// @retval OB_TENANT_HAS_BEEN_DROPPED   fail to fetch schema table, database, tenant, tenant may dropped
  /// @retval other error code             fail
  int inc_trans_count_on_serving_(bool &is_serving,
      const common::ObPartitionKey &key,
      const bool print_participant_not_serve_info);

  /// Check partition status
  int check_part_status_(const common::ObPartitionKey &pkey,
    const int64_t schema_version,
    const int64_t timeout,
    share::schema::PartitionStatus &part_status);

  /// Handling future table scenarios
  int handle_future_part_when_inc_trans_count_on_serving_(bool &is_serving,
      const common::ObPartitionKey &key,
      const bool print_participant_not_serve_info,
      const int64_t base_schema_version,
      const int64_t timeout);
  int check_cur_schema_version_when_handle_future_part_(const int64_t schema_version,
      const int64_t end_time);
  int check_cur_schema_version_when_handle_future_table_(const int64_t schema_version,
      const int64_t end_time);

  bool is_proxy_table(const char *tenant_name, const char *db_name, const char *tb_name);

  // Multi-instance scenarios, splitting not supported
  bool need_to_support_split_when_in_multi_instance_() const;

  int get_ddl_pkey_(const uint64_t tenant_id, const int64_t schema_version, ObPartitionKey &pkey);

  int add_ddl_table_(const uint64_t tenant_id,
      const int64_t start_serve_tstamp,
      ObLogSchemaGuard &schema_guard,
      const int64_t timeout);

  template <class PartitionKeyIter, class PartitionSchema>
  int add_table_or_tablegroup_(
      const bool is_tablegroup,
      const uint64_t table_id,
      const uint64_t tablegroup_id,
      const uint64_t db_id,
      const bool has_physical_part,
      const bool is_create_partition,
      const int64_t start_serve_timestamp,
      PartitionKeyIter &pkey_iter,
      PartitionSchema &table_schema,
      int64_t &served_part_count);
  template <class PartitionSchema>
  int drop_table_or_tablegroup_(
      const bool is_tablegroup,
      const uint64_t table_id,
      const char *table_name,
      PartitionSchema &table_schema,
      int64_t &served_part_count);
  template <class PartitionKeyIter, class PartitionSchema>
  int split_table_or_tablegroup_(
      const bool is_tablegroup,
      const uint64_t table_id,
      const uint64_t tablegroup_id,
      const uint64_t db_id,
      const bool has_physical_part,
      const int64_t start_serve_timestamp,
      PartitionKeyIter &pkey_iter,
      PartitionSchema &table_schema);
  int do_add_all_tablegroups_(
      ObLogSchemaGuard &schema_guard,
      const int64_t start_serve_tstamp,
      const int64_t start_schema_version,
      const int64_t timeout);
  int do_add_all_tables_(
      ObLogSchemaGuard &schema_guard,
      const int64_t start_serve_tstamp,
      const int64_t start_schema_version,
      const int64_t timeout);
  int do_add_inner_tables_(
      ObLogSchemaGuard &schema_guard,
      const int64_t start_serve_tstamp,
      const int64_t start_schema_version,
      const int64_t timeout);

// function about schema
private:
  bool has_physical_part_(const ObSimpleTableSchemaV2 &table_schema);
  bool has_physical_part_(const ObTablegroupSchema &tg_schema);
  // get Simple Table Schema
  int get_simple_table_schema_(const uint64_t table_id,
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
  PartInfoMap       *map_;
  GIndexCache       *global_normal_index_table_cache_; // global normal index cache
  TableIDCache      *table_id_cache_;
  PartCBArray       *part_add_cb_array_;
  PartCBArray       *part_rc_cb_array_;

  int64_t           cur_schema_version_ CACHE_ALIGNED;

  // Default whitelist match insensitive
  bool              enable_oracle_mode_match_case_sensitive_;

  // Conditional
  common::ObThreadCond   schema_cond_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogPartMgr);
};
}
}
#endif
